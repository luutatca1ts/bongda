from src.db.models import engine
from sqlalchemy import text

c = engine.connect()

# 1. Sample predictions past - show Match.status
print("=== Sample 10 predictions for past matches ===")
rows = c.execute(text("""
    SELECT p.match_id, p.market, p.outcome, m.status, m.utc_date, m.home_goals, m.away_goals,
           m.home_team, m.away_team
    FROM predictions p
    JOIN matches m ON p.match_id = m.match_id
    WHERE m.utc_date < CURRENT_TIMESTAMP
    LIMIT 10
""")).fetchall()
for r in rows:
    print(f"  match_id={r[0]} status={r[3]} utc={r[4]} goals={r[5]}-{r[6]} | {r[7]} vs {r[8]}")

# 2. Count FINISHED matches by how recently they were updated
print("\n=== FINISHED matches in past (should be ~1130 if all past trans are finished) ===")
r = c.execute(text("""
    SELECT COUNT(*) FROM matches
    WHERE status='FINISHED' AND utc_date < CURRENT_TIMESTAMP
""")).fetchone()
print(f"  FINISHED+past: {r[0]}")

# 3. Matches SCHEDULED but in the past (these SHOULD have been updated to FINISHED)
print("\n=== SCHEDULED but past (data collector missed these) ===")
r = c.execute(text("""
    SELECT COUNT(*) FROM matches
    WHERE status='SCHEDULED' AND utc_date < CURRENT_TIMESTAMP
""")).fetchone()
print(f"  SCHEDULED+past: {r[0]}")

# 4. Show 5 such SCHEDULED+past matches
print("\n=== Sample 5 SCHEDULED+past matches ===")
rows = c.execute(text("""
    SELECT match_id, utc_date, home_team, away_team, home_goals, away_goals
    FROM matches
    WHERE status='SCHEDULED' AND utc_date < CURRENT_TIMESTAMP
    LIMIT 5
""")).fetchall()
for r in rows:
    print(f"  match_id={r[0]} utc={r[1]} | {r[2]} vs {r[3]} | goals={r[4]}-{r[5]}")