from src.db.models import engine
from sqlalchemy import text

c = engine.connect()

# 1. is_value_bet distribution
print("=== is_value_bet distribution ===")
rows = c.execute(text("SELECT is_value_bet, COUNT(*) FROM predictions GROUP BY is_value_bet")).fetchall()
for r in rows:
    print(f"  is_value_bet={r[0]}: {r[1]} rows")

# 2. Pending value bets (as update_results() filters)
print("\n=== Pending value bets (is_value_bet=True AND result IS NULL) ===")
r = c.execute(text("SELECT COUNT(*) FROM predictions WHERE is_value_bet=1 AND result IS NULL")).fetchone()
print(f"  Pending: {r[0]}")

# 3. Pending value bets JOIN matches — split by match status
print("\n=== Pending VBs by match status ===")
rows = c.execute(text("""
    SELECT m.status, COUNT(*)
    FROM predictions p
    JOIN matches m ON p.match_id = m.match_id
    WHERE p.is_value_bet=1 AND p.result IS NULL
    GROUP BY m.status
""")).fetchall()
for r in rows:
    print(f"  status={r[0]}: {r[1]}")

# 4. Pending VBs with match FINISHED but home_goals NULL
print("\n=== Pending VBs FINISHED but missing goals ===")
r = c.execute(text("""
    SELECT COUNT(*)
    FROM predictions p
    JOIN matches m ON p.match_id = m.match_id
    WHERE p.is_value_bet=1 AND p.result IS NULL
      AND m.status='FINISHED'
      AND (m.home_goals IS NULL OR m.away_goals IS NULL)
""")).fetchone()
print(f"  FINISHED + missing goals: {r[0]}")

# 5. Pending VBs with match FINISHED + goals present (these SHOULD be updatable)
print("\n=== Pending VBs FINISHED + goals present (SHOULD be updatable) ===")
r = c.execute(text("""
    SELECT COUNT(*)
    FROM predictions p
    JOIN matches m ON p.match_id = m.match_id
    WHERE p.is_value_bet=1 AND p.result IS NULL
      AND m.status='FINISHED'
      AND m.home_goals IS NOT NULL AND m.away_goals IS NOT NULL
""")).fetchone()
print(f"  Should be updatable: {r[0]}")