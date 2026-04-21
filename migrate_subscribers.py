"""Idempotent migration: create `subscribers` table for persisting chat auth.

Phase C.1 — the Telegram bot currently keeps two in-memory sets
(_authenticated, _subscribers) that reset on every restart, forcing every
user to re-login. This migration gives them a durable home.

Usage:
    python migrate_subscribers.py

Safe to re-run. SQLite databases are auto-backed up to
    football_analytics.db.subscribers_backup_{timestamp}
before any CREATE.
"""

from __future__ import annotations

import logging
import shutil
import sys
import time
from pathlib import Path

from sqlalchemy import inspect, text

from src.config import DATABASE_URL
from src.db.models import engine, init_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("migrate_subscribers")

TABLE_NAME = "subscribers"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    chat_id       INTEGER PRIMARY KEY,
    authenticated BOOLEAN DEFAULT 0,
    subscribed    BOOLEAN DEFAULT 0,
    created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_active   DATETIME DEFAULT CURRENT_TIMESTAMP
)
"""


def _backup_sqlite() -> Path | None:
    if not DATABASE_URL.startswith("sqlite:///"):
        return None
    src = Path(DATABASE_URL.replace("sqlite:///", ""))
    if not src.exists():
        return None
    dst = src.with_suffix(src.suffix + f".subscribers_backup_{int(time.time())}")
    shutil.copy2(src, dst)
    log.info("[backup] %s → %s", src, dst)
    return dst


def main() -> int:
    init_db()

    insp = inspect(engine)
    if TABLE_NAME in insp.get_table_names():
        with engine.begin() as conn:
            n = conn.execute(text(f"SELECT COUNT(*) FROM {TABLE_NAME}")).scalar() or 0
        log.info("[migrate_subscribers] table already exists (%d rows) — no-op", n)
        return 0

    bkp = _backup_sqlite()
    try:
        with engine.begin() as conn:
            log.info("[create] running: %s", " ".join(CREATE_SQL.split()))
            conn.execute(text(CREATE_SQL))
    except Exception:
        log.exception("CREATE failed")
        if bkp:
            log.error("restore from backup at %s if needed", bkp)
        return 1

    insp = inspect(engine)
    if TABLE_NAME not in insp.get_table_names():
        log.error("[migrate_subscribers] POST-CHECK failed — table missing after CREATE")
        return 1

    log.info("[migrate_subscribers] OK — table `%s` ready", TABLE_NAME)
    return 0


if __name__ == "__main__":
    sys.exit(main())
