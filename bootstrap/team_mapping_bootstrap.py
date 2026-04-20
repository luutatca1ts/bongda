"""Bootstrap team_name → api_football_id mapping (Phase B1, manual run).

Three-step pipeline:
  1. Query DISTINCT home_team ∪ away_team from SQLite `matches` table.
  2. For each league in src.config.API_FOOTBALL_LEAGUES, GET
     /teams?league={id}&season=2025 with 1s spacing. Cached in one JSON
     file; re-runs reuse the cache unless --force-refresh-api is set.
  3. rapidfuzz.token_set_ratio each DB name against the union of API team
     names. Thresholds: ≥95 auto, 85–94 review, <85 unmatched.

Outputs (in artifacts/):
  - team_mapping.json           auto-matched (score ≥95), machine-readable
  - team_mapping_review.csv     manual review (85–94), user edits `decision`
  - team_mapping_unmatched.csv  <85, shows top-3 candidates per db name

Usage:
    python bootstrap/team_mapping_bootstrap.py            # normal, uses cache
    python bootstrap/team_mapping_bootstrap.py --force-refresh-api
    python bootstrap/team_mapping_bootstrap.py --dry-run  # console only

Does not commit JSON, migrate DB, or touch pipeline. Phase B2 will consume
artifacts/team_mapping.json.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import requests
from rapidfuzz import fuzz, process

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.config import API_FOOTBALL_KEY, API_FOOTBALL_LEAGUES, DATABASE_URL  # noqa: E402

ARTIFACTS_DIR = REPO_ROOT / "artifacts"
CACHE_FILE = ARTIFACTS_DIR / "api_football_teams_raw.json"
OUT_MAPPING = ARTIFACTS_DIR / "team_mapping.json"
OUT_REVIEW = ARTIFACTS_DIR / "team_mapping_review.csv"
OUT_UNMATCHED = ARTIFACTS_DIR / "team_mapping_unmatched.csv"
LOG_FILE = REPO_ROOT / "bootstrap" / "team_mapping_bootstrap.log"

AUTO_THRESHOLD = 95
REVIEW_THRESHOLD = 85
SEASON = 2025
SLEEP_BETWEEN_CALLS = 1.0
API_BASE = "https://v3.football.api-sports.io"

log = logging.getLogger("team_mapping_bootstrap")


def _setup_logging() -> None:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    log.setLevel(logging.INFO)
    log.handlers.clear()
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    log.addHandler(fh)
    log.addHandler(sh)


# -------------------------------------------------------------------
# Step 1: DB team names
# -------------------------------------------------------------------
def _db_path() -> Path:
    if not DATABASE_URL.startswith("sqlite:///"):
        raise RuntimeError(f"Only SQLite DATABASE_URL supported, got: {DATABASE_URL}")
    raw = DATABASE_URL.replace("sqlite:///", "", 1)
    p = Path(raw)
    if not p.is_absolute():
        p = REPO_ROOT / p
    return p


def fetch_db_team_names() -> list[str]:
    db = _db_path()
    if not db.exists():
        raise FileNotFoundError(f"SQLite DB not found: {db}")
    with sqlite3.connect(db) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT home_team FROM matches WHERE home_team IS NOT NULL "
            "UNION "
            "SELECT away_team FROM matches WHERE away_team IS NOT NULL"
        )
        names = {row[0].strip() for row in cur.fetchall() if row[0] and row[0].strip()}
    sorted_names = sorted(names)
    log.info("[step1] %d distinct team names from %s", len(sorted_names), db)
    return sorted_names


# -------------------------------------------------------------------
# Step 2: fetch + cache API-Football teams per league
# -------------------------------------------------------------------
def _api_get(path: str, params: dict) -> dict:
    if not API_FOOTBALL_KEY:
        raise RuntimeError("API_FOOTBALL_KEY missing from env — cannot fetch teams")
    resp = requests.get(
        f"{API_BASE}{path}",
        params=params,
        headers={"x-apisports-key": API_FOOTBALL_KEY},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def fetch_api_teams(force_refresh: bool) -> dict[int, list[dict]]:
    """Return {league_id: [{"id", "name"}]}. Cached in one JSON file."""
    if CACHE_FILE.exists() and not force_refresh:
        log.info("[step2] cache hit: %s", CACHE_FILE)
        raw = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        return {int(k): v for k, v in raw.get("leagues", {}).items()}

    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    leagues_result: dict[int, list[dict]] = {}
    failed: list[tuple[str, int, str]] = []

    items = sorted(API_FOOTBALL_LEAGUES.items(), key=lambda kv: kv[1])
    for idx, (code, league_id) in enumerate(items, start=1):
        try:
            data = _api_get("/teams", {"league": league_id, "season": SEASON})
            teams = [
                {"id": t["team"]["id"], "name": t["team"]["name"]}
                for t in data.get("response", [])
                if t.get("team", {}).get("id") and t.get("team", {}).get("name")
            ]
            leagues_result[league_id] = teams
            log.info("[step2] %3d/%d  %-6s league=%4d  teams=%d",
                     idx, len(items), code, league_id, len(teams))
        except Exception as e:  # noqa: BLE001
            log.warning("[step2] FAILED %s league=%d: %s", code, league_id, e)
            failed.append((code, league_id, str(e)))
        time.sleep(SLEEP_BETWEEN_CALLS)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "season": SEASON,
        "failed": failed,
        "leagues": leagues_result,
    }
    CACHE_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("[step2] cached to %s (%d leagues, %d failed)",
             CACHE_FILE, len(leagues_result), len(failed))
    return leagues_result


# -------------------------------------------------------------------
# Step 3: fuzzy match
# -------------------------------------------------------------------
def _build_candidates(api_teams: dict[int, list[dict]]) -> list[tuple[str, int, int]]:
    """Flatten API teams to (name, api_id, league_id). Duplicates kept — a team
    in multiple leagues (e.g. Liverpool in PL + CL) gives more chances to match."""
    out: list[tuple[str, int, int]] = []
    for league_id, teams in api_teams.items():
        for t in teams:
            out.append((t["name"], t["id"], league_id))
    return out


def _match_one(
    db_name: str,
    candidates: list[tuple[str, int, int]],
    index_names: list[str],
) -> list[tuple[str, int, int, float]]:
    """Return top-3 (api_name, api_id, league_id, score) sorted desc."""
    scored = process.extract(
        db_name,
        index_names,
        scorer=fuzz.token_set_ratio,
        limit=3,
    )
    results: list[tuple[str, int, int, float]] = []
    for name, score, idx in scored:
        api_name, api_id, league_id = candidates[idx]
        results.append((api_name, api_id, league_id, float(score)))
    return results


def classify_matches(
    db_names: list[str], api_teams: dict[int, list[dict]],
) -> tuple[dict, list[dict], list[dict]]:
    candidates = _build_candidates(api_teams)
    if not candidates:
        raise RuntimeError("No API-Football teams fetched — cannot match")
    index_names = [c[0] for c in candidates]

    auto: dict[str, dict] = {}
    review: list[dict] = []
    unmatched: list[dict] = []

    for db_name in db_names:
        top3 = _match_one(db_name, candidates, index_names)
        if not top3:
            unmatched.append({
                "db_name": db_name, "top1_api_name": "", "top1_score": 0.0,
                "top2_api_name": "", "top2_score": 0.0,
                "top3_api_name": "", "top3_score": 0.0,
                "reason": "No candidates",
            })
            continue

        best_name, best_id, best_league, best_score = top3[0]
        reason = "Exact match" if best_score >= 100 else f"token_set_ratio={best_score:.0f}"

        if best_score >= AUTO_THRESHOLD:
            auto[db_name] = {
                "api_id": best_id,
                "api_name": best_name,
                "league_id": best_league,
                "score": round(best_score, 1),
                "reason": reason,
            }
        elif best_score >= REVIEW_THRESHOLD:
            review.append({
                "db_name": db_name,
                "api_team_id": best_id,
                "api_name": best_name,
                "league_id": best_league,
                "score": round(best_score, 1),
                "reason": reason,
                "decision": "",
            })
        else:
            row = {"db_name": db_name, "reason": f"best_score={best_score:.0f} < {REVIEW_THRESHOLD}"}
            for i in range(3):
                if i < len(top3):
                    name, _, _, sc = top3[i]
                    row[f"top{i+1}_api_name"] = name
                    row[f"top{i+1}_score"] = round(sc, 1)
                else:
                    row[f"top{i+1}_api_name"] = ""
                    row[f"top{i+1}_score"] = 0.0
            unmatched.append(row)

    review.sort(key=lambda r: r["score"], reverse=True)
    return auto, review, unmatched


# -------------------------------------------------------------------
# Output writers
# -------------------------------------------------------------------
def write_outputs(
    auto: dict, review: list[dict], unmatched: list[dict],
    total_db: int, dry_run: bool,
) -> None:
    payload = {
        "version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_db_teams": total_db,
        "stats": {"auto": len(auto), "review": len(review), "unmatched": len(unmatched)},
        "mappings": auto,
    }

    if dry_run:
        log.info("[dry-run] mappings JSON preview (first 5):")
        for i, (k, v) in enumerate(list(auto.items())[:5]):
            log.info("  %s -> %s", k, v)
        log.info("[dry-run] review preview (first 5): %s", review[:5])
        log.info("[dry-run] unmatched preview (first 5): %s", unmatched[:5])
        return

    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    OUT_MAPPING.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("[out] wrote %s", OUT_MAPPING)

    _write_csv(OUT_REVIEW,
               ["db_name", "api_team_id", "api_name", "league_id", "score", "reason", "decision"],
               review)
    log.info("[out] wrote %s (%d rows)", OUT_REVIEW, len(review))

    _write_csv(OUT_UNMATCHED,
               ["db_name",
                "top1_api_name", "top1_score",
                "top2_api_name", "top2_score",
                "top3_api_name", "top3_score",
                "reason"],
               unmatched)
    log.info("[out] wrote %s (%d rows)", OUT_UNMATCHED, len(unmatched))


def _write_csv(path: Path, fields: list[str], rows: Iterable[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})


# -------------------------------------------------------------------
# main
# -------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force-refresh-api", action="store_true",
                        help="Ignore cache and re-fetch API-Football teams")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print summary, do not write artifact files")
    args = parser.parse_args()

    _setup_logging()
    log.info("=== team_mapping_bootstrap start (force=%s dry=%s) ===",
             args.force_refresh_api, args.dry_run)

    try:
        db_names = fetch_db_team_names()
        api_teams = fetch_api_teams(force_refresh=args.force_refresh_api)
        auto, review, unmatched = classify_matches(db_names, api_teams)
        write_outputs(auto, review, unmatched, total_db=len(db_names), dry_run=args.dry_run)
    except Exception as e:  # noqa: BLE001
        log.exception("FAILED: %s", e)
        return 1

    print(f"\nMatched: {len(auto)} auto, {len(review)} review, {len(unmatched)} unmatched")
    log.info("=== done — %d auto / %d review / %d unmatched ===",
             len(auto), len(review), len(unmatched))
    return 0


if __name__ == "__main__":
    sys.exit(main())
