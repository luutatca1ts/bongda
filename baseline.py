from src.db.models import engine
from sqlalchemy import text
c = engine.connect()
total = c.execute(text("SELECT COUNT(*) FROM predictions")).fetchone()[0]
wr = c.execute(text("SELECT COUNT(*) FROM predictions WHERE result IS NOT NULL")).fetchone()[0]
nu = c.execute(text("SELECT COUNT(*) FROM predictions WHERE result IS NULL")).fetchone()[0]
sched_null = c.execute(text("SELECT COUNT(*) FROM matches WHERE status='SCHEDULED' AND home_goals IS NULL")).fetchone()[0]
fin = c.execute(text("SELECT COUNT(*) FROM matches WHERE status='FINISHED'")).fetchone()[0]
print(f"Predictions: total={total}, with_result={wr}, null={nu}")
print(f"Matches: SCHEDULED+null_goals={sched_null}, FINISHED={fin}")
