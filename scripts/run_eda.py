"""
run_eda.py — CLI entry point for Stage 3.1 Supabase EDA.

Follows the project convention: no business logic here, just path/logging
setup + DatabaseConnection + SupabaseEDA.run().

After EDA completes successfully:
    1. ReportsCopier  copies src/ml/eda/reports/ → Customer-Churn-Prediction-Microservice-Reports/eda_reports/
    2. ReportsPublisher  git add . / git commit / git push in the reports repo

Run from the project root:
    python scripts/run_eda.py
    python scripts/run_eda.py --reports-root /tmp/custom
    python scripts/run_eda.py --model-version kaggle_baseline
    python scripts/run_eda.py --skip-sync        ← skip copy + git push

Exit codes:
    0  success, EDA complete + reports synced
    2  EDA completed but sanity check returned 'warn'
    1  unhandled exception — check the traceback in logs/
"""
from __future__ import annotations

import argparse
import logging
import shutil
import subprocess
import sys
import traceback
from datetime import datetime
from pathlib import Path


# ─────────────────────────────────────────────────────────────────────────────
# CLASS 1 — EDARunner
# Handles CLI args, logging setup, DatabaseConnection, and calls SupabaseEDA.
# ─────────────────────────────────────────────────────────────────────────────

class EDARunner:
    """CLI wrapper around SupabaseEDA — handles paths, logging, and exit codes."""

    PROJECT_ROOT       = Path(__file__).resolve().parent.parent
    DEFAULT_REPORTS_ROOT = "src/ml/eda/reports"
    DEFAULT_LOG_DIR    = "logs"
    DEFAULT_REPORTS_REPO = Path.home() / "Desktop" / "Customer-Churn-Prediction-Microservice-Reports"

    def __init__(self) -> None:
        self.args     = self._parse_args()
        self._setup_path()
        self.log_path = self._setup_logging()

    # ---------- setup ----------

    def _parse_args(self) -> argparse.Namespace:
        parser = argparse.ArgumentParser(
            description="Stage 3.1 — Supabase EDA for ChurnGuard",
        )
        parser.add_argument(
            "--reports-root", default=self.DEFAULT_REPORTS_ROOT,
            help="Directory to write EDA artifacts (default: %(default)s)",
        )
        parser.add_argument(
            "--model-version", default="kaggle_baseline",
            help="Row filter on predictions.model_version (default: %(default)s)",
        )
        parser.add_argument(
            "--log-dir", default=self.DEFAULT_LOG_DIR,
            help="Where to write the timestamped log file (default: %(default)s)",
        )
        parser.add_argument(
            "--reports-repo", default=str(self.DEFAULT_REPORTS_REPO),
            help="Path to the reports GitHub repo (default: %(default)s)",
        )
        parser.add_argument(
            "--skip-sync", action="store_true", default=False,
            help="Skip copying reports and git push (useful during development)",
        )
        return parser.parse_args()

    def _setup_path(self) -> None:
        """Ensure imports from src.*, database.* resolve regardless of CWD."""
        if str(self.PROJECT_ROOT) not in sys.path:
            sys.path.insert(0, str(self.PROJECT_ROOT))

    def _setup_logging(self) -> Path:
        """Log to both console and a timestamped file under logs/."""
        log_dir = self.PROJECT_ROOT / self.args.log_dir
        log_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path  = log_dir / f"eda_{timestamp}.log"

        root = logging.getLogger()
        root.setLevel(logging.INFO)
        root.handlers.clear()

        fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
                                datefmt="%H:%M:%S")

        console = logging.StreamHandler(sys.stdout)
        console.setLevel(logging.INFO)
        console.setFormatter(fmt)
        root.addHandler(console)

        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(fmt)
        root.addHandler(file_handler)

        # suppress matplotlib and numexpr noise
        logging.getLogger("matplotlib").setLevel(logging.WARNING)
        logging.getLogger("matplotlib.category").setLevel(logging.WARNING)
        logging.getLogger("numexpr").setLevel(logging.WARNING)

        return log_path

    # ---------- run ----------

    def run(self) -> int:
        # Imports happen AFTER _setup_path() so src.* and database.* resolve.
        from database.connection import DatabaseConnection
        from src.ml.eda import SupabaseEDA

        logging.info("╔" + "═" * 68 + "╗")
        logging.info("║  CHURNGUARD — STAGE 3.1 — SUPABASE EDA" + " " * 29 + "║")
        logging.info(f"║  Log → {str(self.log_path):<61}║")
        logging.info("╚" + "═" * 68 + "╝")

        try:
            # DatabaseConnection is a context manager — connects on enter,
            # disconnects cleanly on exit regardless of exceptions.
            with DatabaseConnection() as db:
                eda = SupabaseEDA(
                    db=db,
                    reports_root=self.args.reports_root,
                    model_version=self.args.model_version,
                )
                summary = eda.run()

            eda_status = summary.get("status", "unknown")

            # ── sync to reports repo (unless --skip-sync) ──
            if self.args.skip_sync:
                logging.info("[EDARunner] --skip-sync flag set — skipping reports sync.")
            else:
                reports_root = self.PROJECT_ROOT / self.args.reports_root
                reports_repo = Path(self.args.reports_repo)

                copier    = ReportsCopier(reports_root, reports_repo)
                publisher = ReportsPublisher(reports_repo)

                copy_ok    = copier.copy()
                publish_ok = publisher.publish() if copy_ok else False

                if not publish_ok:
                    logging.warning(
                        "[EDARunner] Reports sync failed — EDA output is still valid locally."
                    )

            if eda_status == "warn":
                logging.warning("[EDARunner] EDA completed with warnings — inspect sanity.json.")
                return 2

            return 0

        except Exception:
            logging.error("[EDARunner] Uncaught exception during EDA run:")
            logging.error(traceback.format_exc())
            return 1


# ─────────────────────────────────────────────────────────────────────────────
# CLASS 2 — ReportsCopier
# Copies src/ml/eda/reports/ into <reports_repo>/eda_reports/
# ─────────────────────────────────────────────────────────────────────────────

class ReportsCopier:
    """Copy the local EDA reports folder into the reports GitHub repo.

    Source:      <project>/src/ml/eda/reports/
    Destination: <reports_repo>/eda_reports/

    Uses shutil.copytree with dirs_exist_ok=True so existing files are
    overwritten cleanly on every run — no stale artefacts left behind.
    """

    def __init__(self, source: Path, reports_repo: Path) -> None:
        self.source      = Path(source)
        self.destination = Path(reports_repo) / "eda_reports"

    def copy(self) -> bool:
        """Copy reports folder. Returns True on success, False on failure."""
        logging.info("─" * 70)
        logging.info("▸ SYNC — COPY REPORTS TO REPORTS REPO")
        logging.info("─" * 70)

        if not self.source.exists():
            logging.error(
                f"[ReportsCopier] Source not found: {self.source}\n"
                "               Run python scripts/run_eda.py first."
            )
            return False

        if not self.destination.parent.exists():
            logging.error(
                f"[ReportsCopier] Reports repo not found: {self.destination.parent}\n"
                "               Create the repo at that path first:\n"
                f"               mkdir -p {self.destination.parent}"
            )
            return False

        logging.info(f"[ReportsCopier] Reading from   → {self.source}")
        logging.info(f"[ReportsCopier] Writing to     → {self.destination}")

        try:
            shutil.copytree(
                src=self.source,
                dst=self.destination,
                dirs_exist_ok=True,   # overwrite existing files
            )

            # Count what was copied for the log signal
            n_json = len(list(self.destination.rglob("*.json")))
            n_png  = len(list(self.destination.rglob("*.png")))
            logging.info(
                f"[ReportsCopier] DONE           → copied {n_json} JSONs + {n_png} PNGs"
            )
            return True

        except Exception as exc:
            logging.error(f"[ReportsCopier] Copy failed: {exc}")
            return False


# ─────────────────────────────────────────────────────────────────────────────
# CLASS 3 — ReportsPublisher
# Runs git add . / git commit / git push inside the reports repo.
# ─────────────────────────────────────────────────────────────────────────────

class ReportsPublisher:
    """Commit and push the latest EDA reports to the GitHub reports repo.

    Runs three git commands in sequence inside the reports repo:
        git add .
        git commit -m "eda results updated — <timestamp>"
        git push

    Skips the commit step if there are no staged changes (avoids empty commits).
    Each git command is logged clearly so failures are easy to diagnose.
    """

    COMMIT_MESSAGE_PREFIX = "eda results updated"

    def __init__(self, reports_repo: Path) -> None:
        self.repo = Path(reports_repo)

    def publish(self) -> bool:
        """Run git add / commit / push. Returns True on success, False on failure."""
        logging.info("─" * 70)
        logging.info("▸ SYNC — GIT PUSH REPORTS REPO")
        logging.info("─" * 70)

        if not (self.repo / ".git").exists():
            logging.error(
                f"[ReportsPublisher] {self.repo} is not a git repository.\n"
                "                   Run: git init && git remote add origin <url>"
            )
            return False

        timestamp      = datetime.now().strftime("%Y-%m-%d %H:%M")
        commit_message = f"{self.COMMIT_MESSAGE_PREFIX} — {timestamp}"

        # Step 1 — git add .
        if not self._run_git(["git", "add", "."], "git add .", "staged all changes"):
            return False

        # Step 2 — check if there is anything to commit
        result = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=self.repo,
            capture_output=True,
            text=True,
        )
        if not result.stdout.strip():
            logging.info("[ReportsPublisher] Nothing to commit — reports unchanged since last push.")
            return True

        # Step 3 — git commit
        if not self._run_git(
            ["git", "commit", "-m", commit_message],
            "git commit",
            f'committed with message: "{commit_message}"',
        ):
            return False

        # Step 4 — git push
        if not self._run_git(["git", "push"], "git push", "pushed to remote"):
            return False

        logging.info("[ReportsPublisher] DONE           → reports repo updated on GitHub")
        return True

    def _run_git(self, cmd: list, label: str, success_msg: str) -> bool:
        """Run one git command. Logs the result, returns True on success."""
        logging.info(f"[ReportsPublisher] Running        → {label}")
        result = subprocess.run(
            cmd,
            cwd=self.repo,
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            logging.info(f"[ReportsPublisher] ✓              → {success_msg}")
            return True
        else:
            logging.error(
                f"[ReportsPublisher] ✗ {label} failed (exit {result.returncode}):\n"
                f"{result.stderr.strip()}"
            )
            return False


# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    sys.exit(EDARunner().run())
