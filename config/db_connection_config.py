"""
config/db_connection_config.py
══════════════════════════════════════════════════════════════════════════════
Configuration constants for the database connection module.

WHY THIS FILE EXISTS:
  Every tunable value for the database connection lives here — timeouts,
  pool sizes, retry counts. The connection class (database/connection.py)
  reads from here and contains only logic.

  If you need to change the connection pool size or timeout, you come here.
  You never touch the connection class itself.

WHY SEPARATE FROM db_config.py?
  db_config.py holds seeding rules — table names, column mappings,
  chunk sizes. Those are seeding concerns.
  This file holds connection concerns — how to connect, how many
  connections, how long to wait. Different job, different file.
══════════════════════════════════════════════════════════════════════════════
"""

from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# PROJECT PATHS
# ─────────────────────────────────────────────────────────────────────────────

# Project root — two levels up from this config/ file
PROJECT_ROOT = Path(__file__).parent.parent

# .env file location — where DATABASE_URL lives
# NEVER hardcode the URL in any script — always read from this file
ENV_FILE_PATH = PROJECT_ROOT / ".env"


# ─────────────────────────────────────────────────────────────────────────────
# CONNECTION POOL SETTINGS
#
# WHY A CONNECTION POOL?
#   Opening a new database connection takes ~50–200ms — it involves a
#   TCP handshake, TLS negotiation, and PostgreSQL authentication.
#   A pool keeps a set of connections open and ready to reuse.
#   Instead of opening/closing on every query, you borrow a connection
#   from the pool and return it when done — no overhead.
#
# POOL SIZE FOR SUPABASE FREE TIER:
#   Supabase free tier allows ~20 direct connections (port 5432).
#   Via the pgBouncer pooler (port 6543), the effective limit is higher,
#   but the pool itself should stay small for a portfolio project.
#   min=1 means one connection is always warm and ready.
#   max=5 means at most 5 concurrent queries — more than enough here.
# ─────────────────────────────────────────────────────────────────────────────

# Minimum connections kept open and idle in the pool at all times.
# 1 = always one warm connection ready — avoids cold-start latency.
POOL_MIN_CONNECTIONS = 1

# Maximum connections the pool will open simultaneously.
# Queries beyond this limit wait in queue until a connection is free.
POOL_MAX_CONNECTIONS = 5

# Seconds to wait for a free connection from the pool before raising an error.
# 30s is generous — if a connection is not free in 30s, something is wrong.
POOL_TIMEOUT_SECONDS = 30


# ─────────────────────────────────────────────────────────────────────────────
# QUERY TIMEOUTS
#
# WHY SET TIMEOUTS?
#   Without a timeout, a slow query or network issue makes your entire
#   application hang forever. Timeouts force a failure fast so the
#   caller can handle it gracefully (retry, show error, alert).
# ─────────────────────────────────────────────────────────────────────────────

# Maximum seconds a single query is allowed to run before being cancelled.
# Dashboard queries should complete in <1s — 30s gives ample headroom.
QUERY_TIMEOUT_SECONDS = 30

# Seconds to wait when first establishing the TCP (Transmission Control Protocol) connection to Supabase.
# If Supabase does not respond within 10s, something is wrong upstream.
CONNECT_TIMEOUT_SECONDS = 10


# ─────────────────────────────────────────────────────────────────────────────
# RETRY SETTINGS
#
# WHY RETRY?
#   Supabase free tier occasionally drops idle connections after inactivity.
#   Render cold starts can also cause brief network unavailability.
#   Retrying 3 times with a 2-second wait handles these transient failures
#   without requiring manual intervention.
# ─────────────────────────────────────────────────────────────────────────────

# How many times to retry a failed connection attempt before giving up
MAX_CONNECTION_RETRIES = 3

# Seconds to wait between retry attempts (avoids hammering a stressed server)
RETRY_DELAY_SECONDS = 2


# ─────────────────────────────────────────────────────────────────────────────
# HEALTH CHECK QUERY
#
# A trivial SQL query used to verify the connection is alive.
# "SELECT 1" is the universal database health check — it asks the database
# to return the number 1. If it responds, the connection works.
# No tables are read, no data is touched — it is purely a ping.
# ─────────────────────────────────────────────────────────────────────────────

HEALTH_CHECK_QUERY = "SELECT 1"

# Expected result of the health check query — used to verify correctness
HEALTH_CHECK_EXPECTED = 1


# ─────────────────────────────────────────────────────────────────────────────
# PORT VALIDATION
#
# Supabase must be accessed via the pooler port (6543), not the direct
# connection port (5432). This constant is used to validate the DATABASE_URL
# and warn the user if they are using the wrong port.
# ─────────────────────────────────────────────────────────────────────────────

# The correct Supabase pooler port — must appear in DATABASE_URL
SUPABASE_POOLER_PORT = 6543

# The direct connection port — valid locally but not recommended for deployment
SUPABASE_DIRECT_PORT = 5432
