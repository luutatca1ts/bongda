"""Idempotent migration: add home_xg_estimate + away_xg_estimate to predictions.

Usage:
    python migrate_xg.py

Safe to re-run. SQLite databases are auto-backed up to
    football_analytics.db.xg_backup_{timestamp}
before any ALTER. Aborts loud if backup fails.
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
log = logging.getLogger("migrate_xg")

NEW_COLS = {
    "home_xg_estimate": "FLOAT",
    "away_xg_estimate": "FLOAT",
}


def _backup_sqlite() -> Path | None:
    if not DATABASE_URL.startswith("sqlite:///"):
        return None
    src = Path(DATABASE_URL.replace("sqlite:///", ""))
    if not src.exists():
        return None
    dst = src.with_suffix(src.suffix + f".xg_backup_{int(time.time())}")
    shutil.copy2(src, dst)
    log.info("[backup] %s → %s", src, dst)
    return dst


def main() -> int:
    init_db()

    bkp = _backup_sqlite()

    insp = inspect(engine)
    existing = {c["name"] for c in insp.get_columns("predictions")}
    to_add = [(n, t) for n, t in NEW_COLS.items() if n not in existing]
    if not to_add:
        log.info("[migrate_xg] all columns already present — no-op")
        return 0

    with engine.begin() as conn:
        for name, ctype in to_add:
            sql = f"ALTER TABLE predictions ADD COLUMN {name} {ctype}"
            log.info("[migrate_xg] %s", sql)
            conn.execute(text(sql))

    # Verify
    insp = inspect(engine)
    post = {c["name"] for c in insp.get_columns("predictions")}
    missing = [n for n in NEW_COLS if n not in post]
    if missing:
        log.error("[migrate_xg] POST-CHECK failed, missing: %s", missing)
        if bkp:
            log.error("[migrate_xg] restore from backup at %s if needed", bkp)
        return 1

    log.info("[migrate_xg] OK — added: %s", [n for n, _ in to_add])
    return 0


if __name__ == "__main__":
    sys.exit(main())
