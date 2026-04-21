"""Idempotent migration: canonical name + API-Football IDs on `matches`.

Phase B2.1 — adds six columns to `matches` and populates from
`artifacts/team_mapping.json` (the output of Phase B1 bootstrap).

Usage:
    python migrate_team_mapping.py

Safe to re-run. SQLite databases are auto-backed up to
    football_analytics.db.team_mapping_backup_{timestamp}
before any ALTER.
"""

from __future__ import annotations

import json
import logging
import shutil
import sys
import time
from pathlib import Path

from sqlalchemy import inspect, text

from src.config import DATABASE_URL
from src.db.models import engine, init_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("migrate_team_mapping")

REPO_ROOT = Path(__file__).resolve().parent
MAPPING_JSON = REPO_ROOT / "artifacts" / "team_mapping.json"

NEW_COLS = {
    "home_canonical":  "TEXT",
    "away_canonical":  "TEXT",
    "home_api_id":     "INTEGER",
    "away_api_id":     "INTEGER",
    "home_league_id":  "INTEGER",
    "away_league_id":  "INTEGER",
}


def _backup_sqlite() -> Path | None:
    if not DATABASE_URL.startswith("sqlite:///"):
        return None
    src = Path(DATABASE_URL.replace("sqlite:///", ""))
    if not src.exists():
        return None
    dst = src.with_suffix(src.suffix + f".team_mapping_backup_{int(time.time())}")
    shutil.copy2(src, dst)
    log.info("[backup] %s → %s", src, dst)
    return dst


def _alter_table() -> list[str]:
    """Add any missing NEW_COLS. Returns the column names actually added."""
    insp = inspect(engine)
    existing = {c["name"] for c in insp.get_columns("matches")}
    to_add = [(n, t) for n, t in NEW_COLS.items() if n not in existing]
    if not to_add:
        log.info("[alter] all canonical columns already present — skipping ALTER")
        return []

    with engine.begin() as conn:
        for name, ctype in to_add:
            sql = f"ALTER TABLE matches ADD COLUMN {name} {ctype}"
            log.info("[alter] %s", sql)
            conn.execute(text(sql))

    insp = inspect(engine)
    post = {c["name"] for c in insp.get_columns("matches")}
    missing = [n for n in NEW_COLS if n not in post]
    if missing:
        raise RuntimeError(f"POST-CHECK failed; missing cols after ALTER: {missing}")
    return [n for n, _ in to_add]


def _load_mapping() -> dict:
    if not MAPPING_JSON.exists():
        raise FileNotFoundError(
            f"{MAPPING_JSON} not found — run bootstrap/team_mapping_bootstrap.py first"
        )
    payload = json.loads(MAPPING_JSON.read_text(encoding="utf-8"))
    mappings = payload.get("mappings") or {}
    if not mappings:
        raise RuntimeError(f"{MAPPING_JSON} has no mappings — cannot populate")
    log.info("[populate] loaded %d entries from %s (schema v%s, generated %s)",
             len(mappings), MAPPING_JSON, payload.get("version"),
             payload.get("generated_at"))
    return mappings


def _populate(mappings: dict) -> None:
    with engine.begin() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM matches")).scalar() or 0
        log.info("[populate] %d rows in `matches`", total)
        if total == 0:
            log.warning("[populate] no rows to populate — done")
            return

        rows = conn.execute(text(
            "SELECT id, home_team, away_team FROM matches"
        )).fetchall()

        home_ok = away_ok = 0
        home_miss: list[str] = []
        away_miss: list[str] = []
        updates: list[dict] = []
        for r in rows:
            mid, h, a = r[0], (r[1] or "").strip(), (r[2] or "").strip()
            h_map = mappings.get(h)
            a_map = mappings.get(a)
            if h_map:
                home_ok += 1
            else:
                home_miss.append(h)
            if a_map:
                away_ok += 1
            else:
                away_miss.append(a)
            updates.append({
                "id":  mid,
                "h_c": (h_map or {}).get("api_name"),
                "h_i": (h_map or {}).get("api_id"),
                "h_l": (h_map or {}).get("league_id"),
                "a_c": (a_map or {}).get("api_name"),
                "a_i": (a_map or {}).get("api_id"),
                "a_l": (a_map or {}).get("league_id"),
            })

        conn.execute(text("""
            UPDATE matches
            SET home_canonical = :h_c,
                home_api_id    = :h_i,
                home_league_id = :h_l,
                away_canonical = :a_c,
                away_api_id    = :a_i,
                away_league_id = :a_l
            WHERE id = :id
        """), updates)

        home_cov = home_ok / total * 100
        away_cov = away_ok / total * 100
        overall  = (home_ok + away_ok) / (2 * total) * 100
        log.info("[populate] home mapped=%d (%.1f%%) unmapped=%d",
                 home_ok, home_cov, len(home_miss))
        log.info("[populate] away mapped=%d (%.1f%%) unmapped=%d",
                 away_ok, away_cov, len(away_miss))
        log.info("[populate] overall coverage %.1f%%", overall)

        # Print the first handful of unmapped names so the operator can spot
        # systematic gaps (e.g. encoding issues, new teams not in B1 bootstrap).
        uniq_miss = sorted(set(home_miss) | set(away_miss))
        if uniq_miss:
            sample = uniq_miss[:15]
            log.info("[populate] unique unmapped names: %d — sample: %s",
                     len(uniq_miss), sample)

        if overall < 60.0:
            log.warning(
                "[populate] coverage %.1f%% is LOW (<60%%) — "
                "bootstrap artifact may be stale; consider re-running B1",
                overall,
            )


def main() -> int:
    init_db()
    bkp = _backup_sqlite()
    try:
        added = _alter_table()
        if added:
            log.info("[alter] added columns: %s", added)
        mappings = _load_mapping()
        _populate(mappings)
    except Exception:
        log.exception("FAILED")
        if bkp:
            log.error("restore from backup at %s if needed", bkp)
        return 1
    log.info("[migrate_team_mapping] OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
