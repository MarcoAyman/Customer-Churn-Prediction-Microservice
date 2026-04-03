"""
database/connection.py
══════════════════════════════════════════════════════════════════════════════
THE SINGLE DATABASE CONNECTION MODULE FOR THE ENTIRE PROJECT.

PURPOSE:
  Every part of the system that needs a database connection imports
  from this file. Nobody writes their own connection logic ever again.

  - Operations dashboard  → imports DatabaseConnection
  - Seeding script        → imports DatabaseConnection
  - FastAPI routes        → imports DatabaseConnection
  - Batch scoring         → imports DatabaseConnection
  - Tests                 → imports DatabaseConnection

ONE CLASS. ONE PLACE. ONE SOURCE OF TRUTH.

HOW TO USE (import and call — three lines):
  ─────────────────────────────────────────
  from database.connection import DatabaseConnection

  db = DatabaseConnection()
  db.connect()

  # Option A — run a query directly
  rows = db.execute_query("SELECT * FROM customers LIMIT 5")

  # Option B — borrow a connection for multiple queries
  with db.get_connection() as conn:
      with conn.cursor() as cur:
          cur.execute("SELECT COUNT(*) FROM predictions")
          count = cur.fetchone()[0]

  db.disconnect()
  ─────────────────────────────────────────

  Or use the context manager (auto-connects and disconnects):
  ─────────────────────────────────────────
  with DatabaseConnection() as db:
      rows = db.execute_query("SELECT * FROM customers LIMIT 5")
  ─────────────────────────────────────────

WHAT THIS FILE DOES:
  1. Reads DATABASE_URL from .env file (never hardcoded)
  2. Validates the URL format (warns if wrong port)
  3. Creates a psycopg2 connection pool
  4. Provides execute_query() for simple SELECT queries
  5. Provides get_connection() context manager for transactions
  6. Provides health_check() to verify the connection is alive
  7. Handles retries on transient failures
  8. Masks passwords in all log output
  9. Cleans up properly when disconnecting

WHAT THIS FILE DOES NOT DO:
  - Does not know about any specific table
  - Does not contain any business logic
  - Does not contain any SQL queries specific to this project
  - Does not know about seeding, ML, or the dashboard
══════════════════════════════════════════════════════════════════════════════
"""

# ─────────────────────────────────────────────────────────────────────────────
# IMPORTS
# ─────────────────────────────────────────────────────────────────────────────

import logging          # structured log output with severity levels
import os               # reading DATABASE_URL from environment
import time             # time.sleep() for retry delays
from contextlib import contextmanager   # @contextmanager decorator for get_connection()
from typing import Any, Optional        # type hints

# python-dotenv: reads .env file and injects variables into os.environ
# Install: pip install python-dotenv
try:
    from dotenv import load_dotenv
except ImportError:
    raise ImportError(
        "python-dotenv is not installed. Run: pip install python-dotenv"
    )

# psycopg2: the standard Python PostgreSQL driver
# psycopg2.pool: connection pooling — reuses connections instead of opening new ones
# Install: pip install psycopg2-binary
#
# WHY TRY/EXCEPT AND NOT A HARD RAISE?
#   Tests mock psycopg2 using unittest.mock.patch before importing this module.
#   If we raise immediately on ImportError, the test file cannot even be imported
#   to set up the patch. By setting psycopg2 to None on failure, the module
#   loads successfully. The tests then patch it before any method is called.
#   In production (Render, your machine), psycopg2-binary is installed and this
#   block succeeds normally — the None fallback is never used.
try:
    import psycopg2                         # core PostgreSQL adapter
    import psycopg2.extras                  # execute_values, RealDictCursor
    import psycopg2.pool                    # SimpleConnectionPool
    from psycopg2.extensions import connection as PsycopgConnection  # type hint only
except ImportError:
    # psycopg2 not installed — set to None so tests can patch it
    # Any real connection attempt will fail with a clear error at runtime
    psycopg2 = None   # type: ignore
    import warnings
    warnings.warn(
        "psycopg2 is not installed. Database connections will fail. "
        "Run: pip install psycopg2-binary",
        ImportWarning,
        stacklevel=2,
    )

# All tuneable constants live in config — not hardcoded here
from config.db_connection_config import (
    CONNECT_TIMEOUT_SECONDS,
    ENV_FILE_PATH,
    HEALTH_CHECK_EXPECTED,
    HEALTH_CHECK_QUERY,
    MAX_CONNECTION_RETRIES,
    POOL_MAX_CONNECTIONS,
    POOL_MIN_CONNECTIONS,
    POOL_TIMEOUT_SECONDS,
    QUERY_TIMEOUT_SECONDS,
    RETRY_DELAY_SECONDS,
    SUPABASE_DIRECT_PORT,
    SUPABASE_POOLER_PORT,
)

# ─────────────────────────────────────────────────────────────────────────────
# MODULE LOGGER
# The caller (script, API, dashboard) sets up the log handlers.
# This module just logs — it does not configure where logs go.
# ─────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger(__name__)  # name = 'database.connection'


# ─────────────────────────────────────────────────────────────────────────────
# DATABASE CONNECTION CLASS
# ─────────────────────────────────────────────────────────────────────────────

class DatabaseConnection:
    """
    Manages a psycopg2 connection pool to the Supabase PostgreSQL database.

    This class is the single, reusable database connection for the entire
    ChurnGuard project. Import it wherever a database connection is needed.

    LIFECYCLE:
      1. Instantiate:  db = DatabaseConnection()
      2. Connect:      db.connect()   (or use 'with DatabaseConnection() as db:')
      3. Use:          db.execute_query(...) or db.get_connection()
      4. Disconnect:   db.disconnect() (called automatically by context manager)

    THREAD SAFETY:
      psycopg2.pool.SimpleConnectionPool is thread-safe for borrowing
      and returning connections. Safe to use in multi-threaded FastAPI.

    ATTRIBUTES:
      _pool:      the psycopg2 connection pool (None before connect())
      _db_url:    the DATABASE_URL string (loaded from .env, never logged in full)
      _connected: boolean flag — True after connect() succeeds
    """

    def __init__(self) -> None:
        """
        Initialise the DatabaseConnection instance.

        Does NOT connect to the database yet — connection is deferred
        to connect() so the caller controls when the connection happens.

        WHY DEFERRED CONNECTION?
          If the connection were made in __init__, any import of this class
          would immediately try to reach Supabase — even during testing or
          when running a script that doesn't need the database yet.
          Deferred connection gives the caller full control.
        """
        self._pool: Optional[psycopg2.pool.SimpleConnectionPool] = None
        # _pool is None until connect() is called successfully
        # It becomes a SimpleConnectionPool after connect()

        self._db_url: Optional[str] = None
        # _db_url is None until _load_database_url() is called
        # Stored on the instance so health_check() and reconnect() can reuse it

        self._connected: bool = False
        # Flag used by callers to check connection state before querying

        logger.debug("DatabaseConnection instance created (not yet connected)")

    # ─────────────────────────────────────────────────────────────────────────
    # CONTEXT MANAGER SUPPORT
    # Enables: with DatabaseConnection() as db:
    # Automatically connects on enter, disconnects on exit
    # ─────────────────────────────────────────────────────────────────────────

    def __enter__(self) -> "DatabaseConnection":
        """
        Called when entering a 'with' block.
        Connects to the database and returns self so the caller can use it.

        Example:
            with DatabaseConnection() as db:
                rows = db.execute_query("SELECT * FROM customers")
            # connection automatically closed here
        """
        self.connect()   # establish connection when entering 'with' block
        return self      # return self so 'as db' gets the connected instance

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Called when exiting a 'with' block — whether normally or due to error.
        Always disconnects cleanly, even if an exception was raised inside the block.

        exc_type, exc_val, exc_tb: exception info (None if no exception occurred)
        Returning None (implicitly) means we do not suppress the exception —
        if the 'with' block raised an error, it still propagates to the caller.
        """
        self.disconnect()   # always clean up the pool on exit

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 1 — LOAD DATABASE URL FROM .env
    # ─────────────────────────────────────────────────────────────────────────

    def _load_database_url(self) -> str:
        """
        Load DATABASE_URL from the .env file into the environment,
        then read and return it.

        WHY READ FROM .env AND NOT FROM os.environ DIRECTLY?
          os.environ only contains variables set in the shell session.
          If someone runs the script without manually exporting DATABASE_URL,
          os.environ.get("DATABASE_URL") returns None.
          load_dotenv() reads the .env file and injects its variables into
          os.environ automatically — so the rest of the code just uses os.environ.

        SECURITY:
          The .env file is in .gitignore — it is never committed to git.
          This function logs a masked version of the URL (password replaced
          with ****) so the real password never appears in any log file.

        Returns:
            str: the full DATABASE_URL connection string

        Raises:
            ValueError: if DATABASE_URL is not set in .env or environment
        """
        logger.info("Loading DATABASE_URL from environment...")

        # Load .env file if it exists — injects variables into os.environ
        if ENV_FILE_PATH.exists():
            load_dotenv(ENV_FILE_PATH)   # reads .env and populates os.environ
            logger.info(f"  Loaded .env from: {ENV_FILE_PATH}")
        else:
            # Fall back to current directory .env (useful when running from project root)
            load_dotenv()
            logger.info("  .env file not found at expected path — "
                        "attempting to load from current directory")

        # Read DATABASE_URL from environment (now populated by load_dotenv)
        db_url = os.environ.get("DATABASE_URL")

        # If still not set, fail immediately with clear instructions
        if not db_url:
            logger.error("  DATABASE_URL is not set in .env or environment")
            logger.error("  To fix:")
            logger.error("    1. Copy .env.example to .env")
            logger.error("    2. Fill in your Supabase connection string")
            logger.error("    3. Use port 6543 (pooler), NOT 5432 (direct)")
            raise ValueError(
                "DATABASE_URL is not set. "
                "Copy .env.example to .env and fill in your Supabase URL."
            )

        # Log masked URL — password replaced with **** for security
        masked = self._mask_password(db_url)
        logger.info(f"  DATABASE_URL found: {masked}")

        # Validate the URL uses the correct Supabase pooler port
        self._validate_port(db_url)

        return db_url   # return the FULL url (not masked) for internal use

    def _validate_port(self, db_url: str) -> None:
        """
        Check that the DATABASE_URL uses port 6543 (Supabase pooler).
        Logs a warning if port 5432 (direct) is detected.

        WHY THIS MATTERS:
          Port 5432 (direct): each connection holds a persistent socket to
          the database server. Supabase free tier allows ~20 direct connections.
          If the API, batch job, and seed script all use 5432 simultaneously,
          you hit the connection limit and new connections are rejected.

          Port 6543 (pgBouncer pooler): connections are handled by a proxy
          that multiplexes many application connections into few real DB connections.
          No connection limit issues. This is the correct choice for any
          non-local, production-bound connection.

        Args:
            db_url: the full database URL string to validate
        """
        if f":{SUPABASE_DIRECT_PORT}" in db_url:
            # Port 5432 detected — warn but do not block
            logger.warning(
                f"  ⚠ WARNING: DATABASE_URL uses port {SUPABASE_DIRECT_PORT} "
                f"(direct connection)"
            )
            logger.warning(
                f"  Recommendation: switch to port {SUPABASE_POOLER_PORT} "
                f"(pgBouncer pooler) for production use"
            )
            logger.warning(
                "  Direct connections are fine locally, but will hit "
                "Supabase connection limits on Render/GitHub Actions"
            )
        elif f":{SUPABASE_POOLER_PORT}" in db_url:
            # Port 6543 detected — correct
            logger.info(
                f"  Port {SUPABASE_POOLER_PORT} (pgBouncer pooler) confirmed ✓"
            )
        else:
            # Unknown port — log for visibility but do not block
            logger.warning(
                "  Could not detect port from DATABASE_URL — "
                "verify the URL format is correct"
            )

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 2 — CONNECT (create the pool)
    # ─────────────────────────────────────────────────────────────────────────

    def connect(self) -> None:
        """
        Load DATABASE_URL and create the psycopg2 connection pool.

        This is the main method to call before any database operations.
        It retries up to MAX_CONNECTION_RETRIES times on transient failures.

        After this method returns successfully:
          - self._pool is a live SimpleConnectionPool
          - self._connected is True
          - A health check has been run to confirm the connection works

        Raises:
            ConnectionError: if all retry attempts fail
        """
        logger.info("─" * 50)
        logger.info("Connecting to Supabase database...")
        logger.info("─" * 50)

        # Guard: do not create a second pool if already connected
        if self._connected and self._pool is not None:
            logger.info("Already connected — skipping reconnect")
            return

        # Load the URL first (may raise ValueError if not configured)
        self._db_url = self._load_database_url()

        # Attempt to create the pool, with retries for transient failures
        last_error: Optional[Exception] = None   # stores the last error for logging

        for attempt in range(1, MAX_CONNECTION_RETRIES + 1):
            try:
                logger.info(
                    f"  Creating connection pool "
                    f"(attempt {attempt}/{MAX_CONNECTION_RETRIES})..."
                )

                # SimpleConnectionPool: a thread-safe pool of psycopg2 connections
                # minconn: minimum connections to keep open (always warm)
                # maxconn: maximum connections allowed simultaneously
                # connect_timeout: seconds to wait when opening a new connection
                self._pool = psycopg2.pool.SimpleConnectionPool(
                    minconn=POOL_MIN_CONNECTIONS,          # keep 1 connection always open
                    maxconn=POOL_MAX_CONNECTIONS,          # allow up to 5 simultaneous
                    dsn=self._db_url,                      # the full connection URL
                    connect_timeout=CONNECT_TIMEOUT_SECONDS,  # fail fast if unreachable
                    # options: PostgreSQL runtime parameters sent at connection time
                    # statement_timeout: cancels any query that runs longer than this
                    options=f"-c statement_timeout={QUERY_TIMEOUT_SECONDS * 1000}"
                    # multiply by 1000 because PostgreSQL expects milliseconds
                )

                # Pool created — now verify it actually works by running a health check
                logger.info(
                    f"  Pool created "
                    f"(min={POOL_MIN_CONNECTIONS}, max={POOL_MAX_CONNECTIONS})"
                )
                self._connected = True   # mark as connected before health check
                self.health_check()      # will raise if the connection doesn't work

                logger.info("  ✓ Connected to Supabase successfully")
                return   # success — exit the retry loop

            except psycopg2.OperationalError as e:
                # OperationalError: cannot reach the server, wrong credentials,
                # or connection refused — usually transient or configuration error
                last_error = e
                logger.error(f"  ✗ Connection attempt {attempt} failed: {e}")

                if attempt < MAX_CONNECTION_RETRIES:
                    # Not the last attempt — wait and try again
                    logger.info(
                        f"  Retrying in {RETRY_DELAY_SECONDS} seconds..."
                    )
                    time.sleep(RETRY_DELAY_SECONDS)   # wait before retry
                else:
                    # Last attempt failed — give up and raise
                    logger.error("  All connection attempts failed.")
                    logger.error("  Common causes:")
                    logger.error("    - Wrong DATABASE_URL in .env")
                    logger.error("    - Wrong password (check for special characters)")
                    logger.error("    - Supabase project is paused (free tier)")
                    logger.error("    - Network issue or VPN blocking port 6543")
                    self._connected = False
                    raise ConnectionError(
                        f"Failed to connect to database after "
                        f"{MAX_CONNECTION_RETRIES} attempts. "
                        f"Last error: {last_error}"
                    )

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 3 — HEALTH CHECK
    # ─────────────────────────────────────────────────────────────────────────

    def health_check(self) -> bool:
        """
        Run a lightweight query to verify the connection is alive and working.

        Runs 'SELECT 1' — the universal database ping. If it returns 1,
        the connection is healthy. If it raises any exception, the connection
        is broken or unavailable.

        Called automatically by connect() after creating the pool.
        Can also be called manually at any time to test the connection.

        Returns:
            bool: True if connection is healthy

        Raises:
            ConnectionError: if health check fails
            RuntimeError: if called before connect()
        """
        # Guard: cannot health check without a pool
        if self._pool is None:
            raise RuntimeError(
                "Cannot health check: not connected. Call connect() first."
            )

        logger.info(f"  Running health check: {HEALTH_CHECK_QUERY!r}")

        # Borrow a connection from the pool
        conn = self._pool.getconn()

        try:
            with conn.cursor() as cur:
                cur.execute(HEALTH_CHECK_QUERY)    # run 'SELECT 1'
                result = cur.fetchone()[0]          # get the single returned value

            # Verify the result is what we expect (the number 1)
            if result != HEALTH_CHECK_EXPECTED:
                raise ConnectionError(
                    f"Health check returned unexpected result: {result!r} "
                    f"(expected {HEALTH_CHECK_EXPECTED!r})"
                )

            logger.info(
                f"  ✓ Health check passed — "
                f"database responded with: {result}"
            )
            return True   # connection is healthy

        except psycopg2.Error as e:
            # Any psycopg2 error during health check means the connection is broken
            logger.error(f"  ✗ Health check failed: {e}")
            self._connected = False
            raise ConnectionError(f"Database health check failed: {e}")

        finally:
            # ALWAYS return the connection to the pool
            # If we don't do this, the pool thinks the connection is still in use
            # and will not lend it to the next caller — pool exhaustion
            self._pool.putconn(conn)   # return connection to pool regardless of success/failure

    # ─────────────────────────────────────────────────────────────────────────
    # EXECUTE QUERY — simple SELECT queries that return rows
    # ─────────────────────────────────────────────────────────────────────────

    def execute_query(
        self,
        sql: str,
        params: Optional[tuple] = None,
        as_dict: bool = True,
    ) -> list[dict]:
        """
        Execute a SQL query and return results as a list of dicts.

        USE THIS FOR: SELECT queries that return rows — dashboard KPI queries,
        fetching customer data, reading predictions, checking drift reports.

        DO NOT USE THIS FOR: INSERT, UPDATE, DELETE — use get_connection()
        for write operations that need transaction control and commit/rollback.

        WHY RETURN DICTS AND NOT TUPLES?
          When psycopg2 returns rows as tuples, you access columns by index:
            row[0], row[1], row[2]
          This breaks if column order changes. With RealDictCursor, you access
          by name: row['customer_id'], row['churn_probability']
          Much safer and readable — column names are self-documenting.

        Args:
            sql:     SQL query string. Use %s placeholders for parameters,
                     NEVER format values directly into the SQL string
                     (SQL injection risk).
            params:  Tuple of values to safely substitute into the query.
                     Example: execute_query("SELECT * FROM customers WHERE city_tier = %s", (2,))
            as_dict: If True (default), return list of dicts.
                     If False, return list of raw tuples (faster for large results).

        Returns:
            list[dict]: each dict is one row, keyed by column name.
                        Returns empty list if query returns no rows.

        Raises:
            RuntimeError: if called before connect()
            psycopg2.Error: if the query fails (syntax error, table not found, etc.)
        """
        # Guard: pool must exist before executing queries
        if self._pool is None or not self._connected:
            raise RuntimeError(
                "Cannot execute query: not connected. Call connect() first."
            )

        # Log the query for debugging — truncate long queries to 120 chars
        log_sql = sql.strip().replace("\n", " ")   # single line for cleaner logs
        log_sql = log_sql[:120] + "..." if len(log_sql) > 120 else log_sql
        logger.debug(f"Executing query: {log_sql}")
        if params:
            logger.debug(f"  With params: {params}")

        # Borrow a connection from the pool
        conn = self._pool.getconn()

        try:
            # RealDictCursor: returns rows as RealDictRow objects (behave like dicts)
            # Regular cursor returns plain tuples — less readable
            cursor_factory = (
                psycopg2.extras.RealDictCursor   # dict-like rows (default)
                if as_dict
                else None                         # plain tuples if as_dict=False
            )

            with conn.cursor(cursor_factory=cursor_factory) as cur:
                cur.execute(sql, params)   # safely parameterised — never use %
                                           # string formatting for SQL values

                # fetchall(): retrieve all result rows at once
                # For large result sets consider fetchmany(n) instead
                rows = cur.fetchall()

            # Convert RealDictRow objects to plain Python dicts
            # RealDictRow behaves like a dict but is a special psycopg2 type —
            # converting ensures callers get a standard Python dict they can
            # serialize to JSON, pass to Pydantic models, etc.
            result = [dict(row) for row in rows] if as_dict else list(rows)

            logger.debug(f"  Query returned {len(result)} rows")
            return result   # list of dicts or list of tuples

        except psycopg2.Error as e:
            # Log the failing query and error for debugging
            logger.error(f"Query failed: {e}")
            logger.error(f"  SQL: {log_sql}")
            raise   # re-raise so the caller knows the query failed

        finally:
            # ALWAYS return connection to pool — even if query raised an exception
            self._pool.putconn(conn)

    # ─────────────────────────────────────────────────────────────────────────
    # GET CONNECTION — for write operations and multi-query transactions
    # ─────────────────────────────────────────────────────────────────────────

    @contextmanager
    def get_connection(self):
        """
        Context manager that borrows a connection from the pool.
        Use this for INSERT, UPDATE, DELETE, or any multi-query transaction.

        WHY A CONTEXT MANAGER?
          Write operations need explicit commit() and rollback().
          Wrapping in a context manager ensures:
          1. The connection is returned to the pool after use
          2. Uncommitted changes are rolled back if an exception occurs
          3. The caller cannot forget to return the connection

        USAGE:
          with db.get_connection() as conn:
              with conn.cursor() as cur:
                  cur.execute("INSERT INTO customers (...) VALUES (...)", values)
              conn.commit()   # commit the transaction

        If an exception is raised inside the 'with' block, the transaction
        is automatically rolled back before the connection is returned to pool.

        Yields:
            psycopg2 connection object (borrowed from the pool)

        Raises:
            RuntimeError: if called before connect()
        """
        # Guard: pool must exist
        if self._pool is None or not self._connected:
            raise RuntimeError(
                "Cannot get connection: not connected. Call connect() first."
            )

        # Borrow a connection from the pool
        conn = self._pool.getconn()

        try:
            logger.debug("Connection borrowed from pool")
            yield conn   # hand the connection to the caller's 'with' block

            # If the caller's code completes without exception, we reach here.
            # The caller is responsible for calling conn.commit() explicitly
            # if they made any changes. This is intentional — explicit commits
            # make transaction boundaries clear in the caller's code.

        except Exception as e:
            # Something went wrong inside the caller's 'with' block
            # Roll back any uncommitted changes before returning the connection
            conn.rollback()
            logger.error(f"Transaction rolled back due to error: {e}")
            raise   # re-raise the original exception

        finally:
            # ALWAYS return the connection to the pool
            # This runs whether the try block succeeded or the except block ran
            self._pool.putconn(conn)
            logger.debug("Connection returned to pool")

    # ─────────────────────────────────────────────────────────────────────────
    # DISCONNECT — release all pool connections cleanly
    # ─────────────────────────────────────────────────────────────────────────

    def disconnect(self) -> None:
        """
        Close all connections in the pool and release resources.

        Call this when the application shuts down, or when the database
        connection is no longer needed. The context manager (__exit__)
        calls this automatically.

        After disconnect():
          - self._pool is None
          - self._connected is False
          - All database connections are closed at the network level

        WHY EXPLICIT DISCONNECT?
          Without this, the connections in the pool stay open until the
          process exits or Python garbage-collects them. In a long-running
          server (FastAPI), connections that are not closed become zombie
          connections — they count against Supabase's connection limit
          but do no useful work.
        """
        if self._pool is None:
            # Already disconnected — nothing to do
            logger.debug("disconnect() called but no pool exists — already disconnected")
            return

        logger.info("Disconnecting from database...")

        # closeall(): closes every connection in the pool immediately
        # After this, the pool object is unusable — set to None
        self._pool.closeall()
        logger.info("  All pool connections closed")

        # Reset state
        self._pool = None
        self._connected = False
        logger.info("  ✓ Disconnected cleanly")

    # ─────────────────────────────────────────────────────────────────────────
    # CONNECTION STATUS
    # ─────────────────────────────────────────────────────────────────────────

    @property
    def is_connected(self) -> bool:
        """
        Read-only property — True if the pool exists and connection was verified.
        Use this to check connection state before querying.

        Example:
            if db.is_connected:
                rows = db.execute_query("SELECT 1")
            else:
                db.connect()
        """
        return self._connected and self._pool is not None

    def get_pool_status(self) -> dict:
        """
        Return diagnostic information about the connection pool.
        Useful for health check endpoints in FastAPI.

        Returns:
            dict with keys: connected, min_connections, max_connections, db_url_masked
        """
        return {
            "connected":        self._connected,
            "min_connections":  POOL_MIN_CONNECTIONS,
            "max_connections":  POOL_MAX_CONNECTIONS,
            "pool_timeout_sec": POOL_TIMEOUT_SECONDS,
            "query_timeout_sec": QUERY_TIMEOUT_SECONDS,
            "db_url_masked":    (
                self._mask_password(self._db_url)
                if self._db_url else "not loaded"
            ),
        }

    # ─────────────────────────────────────────────────────────────────────────
    # PRIVATE UTILITY — mask password in URL for safe logging
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _mask_password(url: str) -> str:
        """
        Replace the password in a database URL with **** for safe logging.

        Passwords must NEVER appear in log files. This function produces
        a loggable version of the URL with the password redacted.

        Example:
          Input:  postgresql://postgres.abc:MyP%40ssword@host:6543/postgres
          Output: postgresql://postgres.abc:****@host:6543/postgres

        Args:
            url: the full database connection URL

        Returns:
            str: the URL with the password portion replaced by ****
        """
        if "@" in url and "://" in url:
            try:
                # Split on the last '@' to separate credentials from host
                # Using rsplit('@', 1) handles '@' characters in the password
                credentials_part, host_part = url.rsplit("@", 1)

                # credentials_part = 'postgresql://username:password'
                # Split on '://' first to isolate the scheme
                scheme, rest = credentials_part.split("://", 1)

                # rest = 'username:password'
                # Split on ':' — last segment is the password
                parts = rest.split(":")

                # Replace all parts after the username with ****
                # parts[0] = username, parts[1:] = password (may contain encoded ':')
                parts[1:] = ["****"]   # replace password with ****

                masked_credentials = scheme + "://" + ":".join(parts)
                return masked_credentials + "@" + host_part

            except (ValueError, IndexError):
                # If URL is malformed and cannot be parsed, return fully masked
                return "postgresql://****:****@**** (malformed URL)"

        return url  # return as-is if it doesn't look like a standard DB URL
