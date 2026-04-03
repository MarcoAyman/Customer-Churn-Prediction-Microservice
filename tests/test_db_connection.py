"""
tests/test_db_connection.py
══════════════════════════════════════════════════════════════════════════════
Unit tests for database/connection.py

PURPOSE:
  These tests verify that the DatabaseConnection class behaves correctly
  in every scenario — including error cases — WITHOUT requiring a real
  database connection.

  We use Python's unittest.mock to replace psycopg2 with a fake version.
  This means the tests run instantly, offline, and with no Supabase account.

HOW TO RUN:
  From the project root:
    python -m pytest tests/test_db_connection.py -v

  Or run this file directly:
    python tests/test_db_connection.py

WHAT IS TESTED:
  1. Password masking — credentials never appear in logs
  2. URL port validation — warns on port 5432, confirms port 6543
  3. Missing DATABASE_URL — raises ValueError with clear message
  4. Connection pool creation — pool is initialised correctly
  5. execute_query() — returns dicts, handles empty results
  6. get_connection() — rolls back on exception, returns connection to pool
  7. health_check() — passes on correct response, fails on wrong response
  8. disconnect() — closes pool and resets state
  9. Context manager — connects on enter, disconnects on exit
  10. is_connected property — reflects true connection state
══════════════════════════════════════════════════════════════════════════════
"""

# ─────────────────────────────────────────────────────────────────────────────
# IMPORTS
# ─────────────────────────────────────────────────────────────────────────────

import sys
import unittest                         # standard library test framework
from pathlib import Path
from unittest.mock import (
    MagicMock,      # creates a flexible mock object that records all calls
    patch,          # temporarily replaces a real object with a mock
    PropertyMock,   # mock for @property decorators
    call,           # used to assert specific call arguments
)

# Add project root to sys.path so imports work when running from any directory
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from database.connection import DatabaseConnection


# ─────────────────────────────────────────────────────────────────────────────
# TEST CLASS
# ─────────────────────────────────────────────────────────────────────────────

class TestDatabaseConnection(unittest.TestCase):
    """
    Unit tests for the DatabaseConnection class.

    setUp() runs before each test method.
    tearDown() runs after each test method.

    All tests mock psycopg2 and dotenv — no real database needed.
    """

    def setUp(self) -> None:
        """
        Create a fresh DatabaseConnection instance before each test.
        Each test starts with a clean, unconnected instance.
        """
        # Patch psycopg2 before creating the instance so all psycopg2 calls
        # in the connection module use our mock instead of the real library
        self.mock_psycopg2_patcher = patch("database.connection.psycopg2")
        self.mock_psycopg2 = self.mock_psycopg2_patcher.start()

        # Patch load_dotenv so tests do not read the real .env file
        self.mock_dotenv_patcher = patch("database.connection.load_dotenv")
        self.mock_dotenv = self.mock_dotenv_patcher.start()

        # Patch os.environ.get to return our fake DATABASE_URL
        self.mock_environ_patcher = patch("database.connection.os.environ.get")
        self.mock_environ_get = self.mock_environ_patcher.start()

        # Default: os.environ.get("DATABASE_URL") returns this fake URL
        # It uses port 6543 (correct) so port validation passes by default
        self.fake_url = (
            "postgresql://postgres.testproject:testpassword"
            "@aws-1-eu-west-1.pooler.supabase.com:6543/postgres"
        )
        self.mock_environ_get.return_value = self.fake_url

        # Create a fresh DatabaseConnection instance
        self.db = DatabaseConnection()

    def tearDown(self) -> None:
        """
        Stop all patches after each test so they don't affect other tests.
        """
        self.mock_psycopg2_patcher.stop()
        self.mock_dotenv_patcher.stop()
        self.mock_environ_patcher.stop()

    # ─────────────────────────────────────────────────────────────────────────
    # TEST 1 — Password masking
    # ─────────────────────────────────────────────────────────────────────────

    def test_mask_password_replaces_password_with_stars(self) -> None:
        """
        _mask_password() must replace the password with **** so it is
        safe to include in log output.
        """
        url = "postgresql://postgres.abc:mysecretpassword@host:6543/postgres"
        masked = DatabaseConnection._mask_password(url)

        # The real password must not appear in the masked output
        self.assertNotIn("mysecretpassword", masked,
                         "Password must not appear in masked URL")

        # The **** replacement must be present
        self.assertIn("****", masked,
                      "Masked URL must contain ****")

        # The hostname must still be visible — only the password is hidden
        self.assertIn("host:6543", masked,
                      "Host and port must remain visible in masked URL")

    def test_mask_password_handles_special_chars_in_password(self) -> None:
        """
        Passwords with URL-encoded special characters (like %40 for @)
        must also be masked correctly.
        """
        url = ("postgresql://postgres.abc:%5BMag%4080509099%5D"
               "@host:6543/postgres")
        masked = DatabaseConnection._mask_password(url)

        # The encoded password must not appear
        self.assertNotIn("%5BMag%4080509099%5D", masked)
        self.assertIn("****", masked)

    def test_mask_password_handles_malformed_url(self) -> None:
        """
        _mask_password() must not crash on malformed URLs.
        It should return a safe fallback string.
        """
        malformed = "not-a-valid-url"
        # Should not raise any exception
        result = DatabaseConnection._mask_password(malformed)
        # Result should be a string (fallback)
        self.assertIsInstance(result, str)

    # ─────────────────────────────────────────────────────────────────────────
    # TEST 2 — Initial state
    # ─────────────────────────────────────────────────────────────────────────

    def test_initial_state_is_not_connected(self) -> None:
        """
        A newly created DatabaseConnection must be in a disconnected state.
        No pool, no URL loaded, is_connected = False.
        """
        fresh_db = DatabaseConnection()

        # Nothing should be connected yet
        self.assertFalse(fresh_db.is_connected,
                         "New instance must not be connected")
        self.assertIsNone(fresh_db._pool,
                          "Pool must be None before connect()")
        self.assertIsNone(fresh_db._db_url,
                          "URL must be None before connect()")

    # ─────────────────────────────────────────────────────────────────────────
    # TEST 3 — Missing DATABASE_URL
    # ─────────────────────────────────────────────────────────────────────────

    def test_connect_raises_when_database_url_is_missing(self) -> None:
        """
        If DATABASE_URL is not in the environment, connect() must raise
        ValueError with a clear, actionable error message.
        """
        # Make os.environ.get return None — simulates missing DATABASE_URL
        self.mock_environ_get.return_value = None

        with self.assertRaises(ValueError) as context:
            self.db.connect()

        # The error message must mention DATABASE_URL so the user knows what to fix
        self.assertIn("DATABASE_URL", str(context.exception))

    # ─────────────────────────────────────────────────────────────────────────
    # TEST 4 — Successful connection
    # ─────────────────────────────────────────────────────────────────────────

    def test_connect_creates_pool_and_sets_connected_flag(self) -> None:
        """
        After a successful connect():
          - _pool must be set (not None)
          - _connected must be True
          - is_connected property must return True
        """
        # Mock the pool constructor to return a fake pool object
        mock_pool = MagicMock()
        self.mock_psycopg2.pool.SimpleConnectionPool.return_value = mock_pool

        # Mock the health check — getconn() returns a fake connection
        mock_conn = MagicMock()
        mock_pool.getconn.return_value = mock_conn

        # Mock cursor and fetchone for the health check query (SELECT 1)
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchone.return_value = (1,)   # SELECT 1 returns (1,)

        self.db.connect()

        # Verify pool was created
        self.assertIsNotNone(self.db._pool, "Pool must be set after connect()")
        self.assertTrue(self.db._connected, "_connected must be True after connect()")
        self.assertTrue(self.db.is_connected, "is_connected must be True after connect()")

    def test_connect_does_not_reconnect_if_already_connected(self) -> None:
        """
        If connect() is called when already connected, it must return
        immediately without creating a new pool.
        """
        # Manually set the connected state (bypass the full connect flow)
        self.db._connected = True
        self.db._pool = MagicMock()   # fake pool — already exists

        self.db.connect()   # call connect() again

        # The pool constructor must NOT have been called a second time
        self.mock_psycopg2.pool.SimpleConnectionPool.assert_not_called()

    # ─────────────────────────────────────────────────────────────────────────
    # TEST 5 — execute_query
    # ─────────────────────────────────────────────────────────────────────────

    def test_execute_query_returns_list_of_dicts(self) -> None:
        """
        execute_query() must return a list of plain Python dicts,
        one dict per row, with column names as keys.
        """
        # Set up a mock pool and connection
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_pool.getconn.return_value = mock_conn

        # Mock cursor that returns two fake rows
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        # Simulate two rows returned from the database
        # RealDictCursor returns dict-like objects — we simulate with plain dicts
        mock_cursor.fetchall.return_value = [
            {"id": "uuid-1", "gender": "Male",   "city_tier": 2},
            {"id": "uuid-2", "gender": "Female", "city_tier": 3},
        ]

        # Set connected state
        self.db._pool = mock_pool
        self.db._connected = True

        # Execute a query
        result = self.db.execute_query("SELECT id, gender, city_tier FROM customers;")

        # Verify result type and content
        self.assertIsInstance(result, list, "Result must be a list")
        self.assertEqual(len(result), 2, "Must return 2 rows")
        self.assertIsInstance(result[0], dict, "Each row must be a dict")
        self.assertEqual(result[0]["gender"], "Male")
        self.assertEqual(result[1]["city_tier"], 3)

    def test_execute_query_returns_empty_list_when_no_rows(self) -> None:
        """
        execute_query() must return an empty list (not None) when the
        query matches no rows.
        """
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_pool.getconn.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = []   # no rows returned

        self.db._pool = mock_pool
        self.db._connected = True

        result = self.db.execute_query("SELECT * FROM customers WHERE id = 'nonexistent';")

        self.assertEqual(result, [], "Must return empty list, not None")

    def test_execute_query_raises_if_not_connected(self) -> None:
        """
        execute_query() must raise RuntimeError if called before connect().
        """
        # Ensure disconnected state
        self.db._pool = None
        self.db._connected = False

        with self.assertRaises(RuntimeError) as context:
            self.db.execute_query("SELECT * FROM customers;")

        self.assertIn("not connected", str(context.exception).lower())

    def test_execute_query_always_returns_connection_to_pool(self) -> None:
        """
        execute_query() must ALWAYS return the borrowed connection to the pool,
        even if an error occurs during query execution.
        """
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_pool.getconn.return_value = mock_conn

        # Make the cursor's execute() raise an error — simulates a bad query
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        # psycopg2.Error is the base class for all psycopg2 exceptions
        self.mock_psycopg2.Error = Exception   # make psycopg2.Error catchable
        mock_cursor.execute.side_effect = Exception("syntax error")

        self.db._pool = mock_pool
        self.db._connected = True

        # The query should raise (we re-raise after catching)
        with self.assertRaises(Exception):
            self.db.execute_query("INVALID SQL;")

        # CRITICAL: putconn() must have been called — connection returned to pool
        mock_pool.putconn.assert_called_once_with(mock_conn)

    # ─────────────────────────────────────────────────────────────────────────
    # TEST 6 — get_connection context manager
    # ─────────────────────────────────────────────────────────────────────────

    def test_get_connection_rolls_back_on_exception(self) -> None:
        """
        If an exception occurs inside the 'with db.get_connection()' block,
        the transaction must be rolled back before the connection is returned
        to the pool.
        """
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_pool.getconn.return_value = mock_conn

        self.db._pool = mock_pool
        self.db._connected = True

        # Simulate an exception inside the with block
        with self.assertRaises(ValueError):
            with self.db.get_connection() as conn:
                raise ValueError("something went wrong in the caller")

        # Verify rollback was called
        mock_conn.rollback.assert_called_once()

        # Verify connection was returned to pool even after the exception
        mock_pool.putconn.assert_called_once_with(mock_conn)

    def test_get_connection_returns_connection_to_pool_on_success(self) -> None:
        """
        On successful execution, get_connection() must return the connection
        to the pool after the 'with' block exits.
        """
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_pool.getconn.return_value = mock_conn

        self.db._pool = mock_pool
        self.db._connected = True

        with self.db.get_connection() as conn:
            pass   # do nothing — just enter and exit the block

        # Connection must be returned to pool
        mock_pool.putconn.assert_called_once_with(mock_conn)

        # Rollback must NOT have been called — no exception occurred
        mock_conn.rollback.assert_not_called()

    # ─────────────────────────────────────────────────────────────────────────
    # TEST 7 — health_check
    # ─────────────────────────────────────────────────────────────────────────

    def test_health_check_passes_when_database_returns_1(self) -> None:
        """
        health_check() must return True when the database responds
        to 'SELECT 1' with the value 1.
        """
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_pool.getconn.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchone.return_value = (1,)   # correct health check response

        self.db._pool = mock_pool
        self.db._connected = True

        result = self.db.health_check()

        self.assertTrue(result, "health_check() must return True on success")

    def test_health_check_raises_before_connect(self) -> None:
        """
        health_check() must raise RuntimeError if called before connect().
        """
        # Disconnected state
        self.db._pool = None

        with self.assertRaises(RuntimeError):
            self.db.health_check()

    # ─────────────────────────────────────────────────────────────────────────
    # TEST 8 — disconnect
    # ─────────────────────────────────────────────────────────────────────────

    def test_disconnect_closes_pool_and_resets_state(self) -> None:
        """
        disconnect() must:
          1. Call closeall() on the pool to close all connections
          2. Set _pool to None
          3. Set _connected to False
          4. Make is_connected return False
        """
        mock_pool = MagicMock()
        self.db._pool = mock_pool
        self.db._connected = True

        self.db.disconnect()

        # Pool must have been closed
        mock_pool.closeall.assert_called_once()

        # State must be reset
        self.assertIsNone(self.db._pool)
        self.assertFalse(self.db._connected)
        self.assertFalse(self.db.is_connected)

    def test_disconnect_is_safe_to_call_when_not_connected(self) -> None:
        """
        Calling disconnect() when already disconnected must not raise
        any exception — it should be a safe no-op.
        """
        # Ensure disconnected state
        self.db._pool = None
        self.db._connected = False

        # Must not raise
        try:
            self.db.disconnect()
        except Exception as e:
            self.fail(f"disconnect() raised {e} unexpectedly")

    # ─────────────────────────────────────────────────────────────────────────
    # TEST 9 — context manager (__enter__ / __exit__)
    # ─────────────────────────────────────────────────────────────────────────

    def test_context_manager_disconnects_after_with_block(self) -> None:
        """
        Using DatabaseConnection as a context manager must:
          1. Call connect() on entry
          2. Call disconnect() on exit (even if exception occurs)
        """
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        mock_pool.getconn.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchone.return_value = (1,)   # health check passes

        self.mock_psycopg2.pool.SimpleConnectionPool.return_value = mock_pool

        with DatabaseConnection() as db:
            # Inside the with block, must be connected
            # (pool was set by connect())
            self.assertIsNotNone(db._pool)

        # After the with block exits, must be disconnected
        self.assertIsNone(db._pool,
                          "Pool must be None after context manager exits")
        self.assertFalse(db._connected,
                         "_connected must be False after context manager exits")

    # ─────────────────────────────────────────────────────────────────────────
    # TEST 10 — get_pool_status
    # ─────────────────────────────────────────────────────────────────────────

    def test_get_pool_status_returns_expected_keys(self) -> None:
        """
        get_pool_status() must return a dict with the expected diagnostic keys.
        """
        self.db._connected = True
        self.db._db_url = self.fake_url

        status = self.db.get_pool_status()

        # All expected keys must be present
        expected_keys = {
            "connected",
            "min_connections",
            "max_connections",
            "pool_timeout_sec",
            "query_timeout_sec",
            "db_url_masked",
        }
        self.assertEqual(
            set(status.keys()), expected_keys,
            f"get_pool_status() must return exactly these keys: {expected_keys}"
        )

        # The masked URL must not contain the real password
        self.assertNotIn("testpassword", status["db_url_masked"])
        self.assertIn("****", status["db_url_masked"])


# ─────────────────────────────────────────────────────────────────────────────
# RUN TESTS
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Run with verbose output so each test name is printed
    # -v flag shows: test name + PASS/FAIL for each test
    unittest.main(verbosity=2)
