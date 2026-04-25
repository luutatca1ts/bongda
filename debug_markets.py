"""Dump tất cả unique (market, outcome pattern) trong DB để biết format thật.

Bypass ORM (schema drift safe) — raw SQL select market+outcome only.
"""
from collections import Counter
from src.db.models import get_session

session = get_session()

rows = session.execute(
    "SELECT market, outcome FROM predictions WHERE is_value_bet = 1"
).fetchall() if False else None

# SQLAlchemy 2.x requires text() wrapper for raw SQL
from sqlalchemy import text
rows = session.execute(
    text("SELECT market, outcome FROM predictions WHERE is_value_bet = 1")
).fetchall()

print(f"Total preds: {len(rows)}")
print()

by_market = Counter((r[0], r[1]) for r in rows)
print("Top 50 (market, outcome) combinations:")
for (market, outcome), count in by_market.most_common(50):
    print(f"  [{count:4d}] market={market!r:30s} outcome={outcome!r}")

print()
print("Markets summary:")
markets = Counter(r[0] for r in rows)
for m, c in markets.most_common():
    print(f"  {m:30s} {c}")

session.close()
