from pathlib import Path
from sqlalchemy import text
from src.db.subscribers import save_subscriber
from src.db.models import engine

file = Path(".authenticated_chats")
chat_ids = [int(ln.strip()) for ln in file.read_text().splitlines() if ln.strip()]
print(f"Found {len(chat_ids)} chat_ids")

for cid in chat_ids:
    ok = save_subscriber(cid, True, True)
    print(f"  {cid}: {'OK' if ok else 'FAIL'}")

with engine.connect() as c:
    rows = c.execute(text("SELECT * FROM subscribers")).fetchall()
    print(f"\nTotal rows: {len(rows)}")
    for r in rows:
        print(f"  {dict(r._mapping)}")