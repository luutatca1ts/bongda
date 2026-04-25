"""One-shot migration: thêm 3 columns vào Match table.
Idempotent — chạy nhiều lần không lỗi (check column tồn tại trước).
"""
import logging
from sqlalchemy import inspect, text
from src.db.models import get_session, Match

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main():
    session = get_session()
    try:
        engine = session.get_bind()
        insp = inspect(engine)
        existing_cols = {c["name"] for c in insp.get_columns("matches")}

        new_cols = [
            ("home_corners", "INTEGER"),
            ("away_corners", "INTEGER"),
            ("api_football_fixture_id", "INTEGER"),
        ]

        added = []
        skipped = []
        for col_name, col_type in new_cols:
            if col_name in existing_cols:
                skipped.append(col_name)
                continue
            sql = f"ALTER TABLE matches ADD COLUMN {col_name} {col_type}"
            session.execute(text(sql))
            added.append(col_name)

        session.commit()
        logger.info(f"Migration done. Added: {added}. Already existed: {skipped}")

        insp = inspect(engine)
        final_cols = {c["name"] for c in insp.get_columns("matches")}
        for col_name, _ in new_cols:
            assert col_name in final_cols, f"Column {col_name} not added!"
        logger.info("All columns verified.")
    finally:
        session.close()


if __name__ == "__main__":
    main()
