"""Migration: add odds_history table + CLV columns trong predictions.

Idempotent — chạy lại không lỗi.
"""

import logging

from sqlalchemy import inspect, text

from src.db.models import engine, OddsHistory, init_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("migrate_money_flow")


def main() -> None:
    insp = inspect(engine)

    # 1) Tạo bảng odds_history nếu chưa có
    existing_tables = set(insp.get_table_names())
    if "odds_history" in existing_tables:
        logger.info("odds_history: already exists, skip create")
    else:
        OddsHistory.__table__.create(engine, checkfirst=True)
        logger.info("odds_history: created")

    # Refresh inspector cache sau khi có thể tạo bảng mới
    insp = inspect(engine)

    # 2) Thêm cột vào predictions (nếu chưa có)
    if "predictions" not in insp.get_table_names():
        logger.info("predictions table không tồn tại — chạy init_db() để tạo lần đầu")
        init_db()
        insp = inspect(engine)

    existing_cols = {c["name"] for c in insp.get_columns("predictions")}
    new_cols = [
        ("closing_odds", "FLOAT"),
        ("closing_captured_at", "DATETIME"),
        ("clv", "FLOAT"),
    ]

    with engine.begin() as conn:
        for col_name, col_type in new_cols:
            if col_name in existing_cols:
                logger.info(f"predictions.{col_name}: already exists, skip")
                continue
            conn.execute(text(f"ALTER TABLE predictions ADD COLUMN {col_name} {col_type}"))
            logger.info(f"predictions.{col_name}: added ({col_type})")

    # 3) Final verify
    insp = inspect(engine)
    pred_cols_after = [c["name"] for c in insp.get_columns("predictions")]
    tables_after = insp.get_table_names()
    logger.info("=== MIGRATION DONE ===")
    logger.info(f"Tables: {tables_after}")
    logger.info(f"Prediction cols: {pred_cols_after}")


if __name__ == "__main__":
    main()
