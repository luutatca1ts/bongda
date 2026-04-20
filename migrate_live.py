"""Migration: tạo bảng live_match_states + live_predictions.

Safe chạy nhiều lần — SQLAlchemy create_all chỉ tạo bảng còn thiếu.
"""

import logging
import shutil
from datetime import datetime
from pathlib import Path

from src.config import DATABASE_URL
from src.db.models import Base, engine, init_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _backup_sqlite():
    """Nếu DB là sqlite file, copy sang bản backup trước khi migrate."""
    if not DATABASE_URL.startswith("sqlite:///"):
        logger.info(f"DATABASE_URL={DATABASE_URL} (not sqlite) — skipping file backup.")
        return
    db_path = DATABASE_URL.replace("sqlite:///", "")
    src = Path(db_path)
    if not src.exists():
        logger.info(f"No existing DB at {src} — will be created fresh.")
        return
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = src.with_name(f"{src.stem}.live_backup_{ts}{src.suffix}")
    shutil.copy2(src, dst)
    logger.info(f"Backup: {src} → {dst}")


def main():
    logger.info("=== migrate_live.py ===")
    _backup_sqlite()
    init_db()

    # Verify bảng mới tồn tại
    table_names = set(Base.metadata.tables.keys())
    required = {"live_match_states", "live_predictions"}
    missing = required - table_names
    if missing:
        raise SystemExit(f"FAIL — tables missing in metadata: {missing}")

    from sqlalchemy import inspect
    inspector = inspect(engine)
    db_tables = set(inspector.get_table_names())
    missing_db = required - db_tables
    if missing_db:
        raise SystemExit(f"FAIL — tables missing in DB: {missing_db}")

    logger.info(f"OK — tables in DB: {sorted(db_tables & required)}")
    logger.info("Migration completed.")


if __name__ == "__main__":
    main()
