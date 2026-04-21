"""CRUD helpers for the `subscribers` table (Phase C).

Persists Telegram chat auth + subscription state across bot restarts so
users don't have to re-/login every time the process cycles.

All helpers are defensive: any exception is logged and swallowed so a DB
hiccup (file lock, missing table from a DB that hasn't been migrated yet,
stale connection) never takes the bot offline. Callers should assume
"best effort" semantics — the in-memory sets remain the source of truth
for the current process's runtime behavior.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Tuple

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from src.db.models import engine

logger = logging.getLogger(__name__)

_TABLE = "subscribers"


def load_all_subscribers() -> Tuple[set[int], set[int]]:
    """Return (authenticated_set, subscribed_set) from DB.

    If the table doesn't exist yet (migration not run), or the query
    fails for any other reason, returns (set(), set()) and logs a
    warning. The bot continues to work with in-memory state only.
    """
    authed: set[int] = set()
    subbed: set[int] = set()
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                f"SELECT chat_id, authenticated, subscribed FROM {_TABLE}"
            )).fetchall()
        for chat_id, auth, sub in rows:
            if auth:
                authed.add(int(chat_id))
            if sub:
                subbed.add(int(chat_id))
        logger.info("[subscribers] loaded from DB: %d authenticated, %d subscribed",
                    len(authed), len(subbed))
    except SQLAlchemyError as e:
        logger.warning("[subscribers] load failed (table missing or DB error): %s", e)
    except Exception as e:  # noqa: BLE001
        logger.warning("[subscribers] load failed with unexpected error: %s", e)
    return authed, subbed


def save_subscriber(chat_id: int, authenticated: bool, subscribed: bool) -> bool:
    """UPSERT the row and update last_active. Returns True on success."""
    now = datetime.utcnow()
    try:
        with engine.begin() as conn:
            conn.execute(text(f"""
                INSERT INTO {_TABLE} (chat_id, authenticated, subscribed, created_at, last_active)
                VALUES (:cid, :auth, :sub, :ts, :ts)
                ON CONFLICT(chat_id) DO UPDATE SET
                    authenticated = excluded.authenticated,
                    subscribed    = excluded.subscribed,
                    last_active   = excluded.last_active
            """), {"cid": int(chat_id), "auth": bool(authenticated),
                   "sub": bool(subscribed), "ts": now})
        return True
    except SQLAlchemyError as e:
        logger.warning("[subscribers] save_subscriber(%s) failed: %s", chat_id, e)
    except Exception as e:  # noqa: BLE001
        logger.warning("[subscribers] save_subscriber(%s) unexpected error: %s", chat_id, e)
    return False


def remove_subscriber(chat_id: int) -> bool:
    """Hard-delete the row (e.g. user /logout or blocked the bot)."""
    try:
        with engine.begin() as conn:
            conn.execute(
                text(f"DELETE FROM {_TABLE} WHERE chat_id = :cid"),
                {"cid": int(chat_id)},
            )
        return True
    except SQLAlchemyError as e:
        logger.warning("[subscribers] remove_subscriber(%s) failed: %s", chat_id, e)
    except Exception as e:  # noqa: BLE001
        logger.warning("[subscribers] remove_subscriber(%s) unexpected error: %s", chat_id, e)
    return False


def update_last_active(chat_id: int) -> bool:
    """Touch last_active to `now`. No-op (False) if row doesn't exist."""
    try:
        with engine.begin() as conn:
            result = conn.execute(
                text(f"UPDATE {_TABLE} SET last_active = :ts WHERE chat_id = :cid"),
                {"ts": datetime.utcnow(), "cid": int(chat_id)},
            )
            return (result.rowcount or 0) > 0
    except SQLAlchemyError as e:
        logger.warning("[subscribers] update_last_active(%s) failed: %s", chat_id, e)
    except Exception as e:  # noqa: BLE001
        logger.warning("[subscribers] update_last_active(%s) unexpected error: %s", chat_id, e)
    return False
