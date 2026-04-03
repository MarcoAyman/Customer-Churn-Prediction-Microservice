# ══════════════════════════════════════════════════════════════════════════════
# Dockerfile — builds the ChurnGuard FastAPI image for Render deployment
#
# BUILD STAGES: single-stage (simpler for a portfolio project)
#
# HOW RENDER USES THIS:
#   1. Render detects the Dockerfile in the repo root
#   2. Render builds the image on every push to the main branch
#   3. Render runs the container with PORT set to their assigned port
#   4. CMD starts Uvicorn which imports src.api.main:app
#
# LOCAL TESTING:
#   docker build -t churnguard-api .
#   docker run -p 8000:8000 --env-file .env churnguard-api
# ══════════════════════════════════════════════════════════════════════════════

# ── BASE IMAGE ────────────────────────────────────────────────────────────────
# python:3.11-slim: official Python 3.11 with minimal OS footprint.
# slim = no build tools, documentation, or test files — smaller image.
FROM python:3.11-slim

# ── ENVIRONMENT VARIABLES ─────────────────────────────────────────────────────
# PYTHONDONTWRITEBYTECODE: prevents Python from writing .pyc files to disk
#   (not needed in a container — saves space)
ENV PYTHONDONTWRITEBYTECODE=1

# PYTHONUNBUFFERED: ensures Python output is sent straight to the terminal
#   (important for real-time log visibility in Render's dashboard)
ENV PYTHONUNBUFFERED=1

# ── WORKING DIRECTORY ─────────────────────────────────────────────────────────
# All subsequent commands run from this directory inside the container
WORKDIR /app

# ── SYSTEM DEPENDENCIES ───────────────────────────────────────────────────────
# libpq-dev: required by psycopg2 to connect to PostgreSQL
# gcc:       required to compile some Python packages
# We clean up apt cache afterwards to keep the image small
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libpq-dev \
        gcc \
    && rm -rf /var/lib/apt/lists/*

# ── PYTHON DEPENDENCIES ───────────────────────────────────────────────────────
# Copy requirements first (before the rest of the code).
# Docker caches each layer. Copying requirements first means:
#   - If only source code changes, Docker reuses the cached pip install layer
#   - pip install only reruns if requirements.txt changes
#   This makes rebuilds much faster.
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# ── APPLICATION CODE ──────────────────────────────────────────────────────────
# Copy the entire project into the container
COPY . .

# ── PORT ──────────────────────────────────────────────────────────────────────
# Render assigns a PORT environment variable dynamically.
# We EXPOSE 8000 as documentation but the actual port comes from $PORT at runtime.
EXPOSE 8000

# ── START COMMAND ─────────────────────────────────────────────────────────────
# Uvicorn starts the FastAPI application.
# $PORT is set by Render at runtime — defaults to 8000 locally.
#
# --host 0.0.0.0: listen on all interfaces (required in a container)
# --workers 1:    one worker process — free tier has limited memory
# --log-level info: show INFO and above in Render's log dashboard
CMD uvicorn src.api.main:app \
    --host 0.0.0.0 \
    --port ${PORT:-8000} \
    --workers 1 \
    --log-level info
