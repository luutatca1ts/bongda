"""Migration: thêm 4 columns vào bảng predictions cho injury + weather.

- injury_impact_home  FLOAT DEFAULT 0
- injury_impact_away  FLOAT DEFAULT 0
- weather_adjust      FLOAT DEFAULT 0
- weather_description VARCHAR

Safe chạy nhiều lần — skip column nếu đã tồn tại (SQLite không hỗ trợ IF
NOT EXISTS trên ALTER TABLE, phải check qua PRAGMA table_info).
"""

import logging
import shutil
from datetime import datetime
from pathlib import Path

from sqlalchemy import inspect, text

from src.config import DATABASE_URL
from src.db.models import engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


COLUMNS = [
    ("injury_impact_home",  "FLOAT DEFAULT 0"),
    ("injury_impact_away",  "FLOAT DEFAULT 0"),
    ("weather_adjust",      "FLOAT DEFAULT 0"),
    ("weather_description", "VARCHAR"),
]


def _backup_sqlite():
    if not DATABASE_URL.startswith("sqlite:///"):
        logger.info("DATABASE_URL=%s (not sqlite) — skipping file backup.", DATABASE_URL)
        return
    db_path = DATABASE_URL.replace("sqlite:///", "")
    src = Path(db_path)
    if not src.exists():
        logger.info("No existing DB at %s — nothing to migrate.", src)
        return
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = src.with_name(f"{src.stem}.iw_backup_{ts}{src.suffix}")
    shutil.copy2(src, dst)
    logger.info("Backup: %s -> %s", src, dst)


def _existing_columns(table: str) -> set[str]:
    insp = inspect(engine)
    return {c["name"] for c in insp.get_columns(table)}


def main():
    logger.info("=== migrate_injury_weather.py ===")
    _backup_sqlite()

    insp = inspect(engine)
    if "predictions" not in insp.get_table_names():
        raise SystemExit("FAIL — table 'predictions' not found; run migrate_live.py first.")

    existing = _existing_columns("predictions")
    added = []
    with engine.begin() as conn:
        for col, coltype in COLUMNS:
            if col in existing:
                logger.info("  [skip] predictions.%s already exists", col)
                continue
            sql = f"ALTER TABLE predictions ADD COLUMN {col} {coltype}"
            conn.execute(text(sql))
            logger.info("  [add]  %s", sql)
            added.append(col)

    # Verify
    final = _existing_columns("predictions")
    missing = {c for c, _ in COLUMNS} - final
    if missing:
        raise SystemExit(f"FAIL — columns missing after migration: {missing}")

    logger.info("Migration OK — added=%s total_injury_weather_cols=%d",
                added, sum(1 for c, _ in COLUMNS if c in final))


if __name__ == "__main__":
    main()
