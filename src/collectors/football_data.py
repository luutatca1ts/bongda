"""Collector for Football-Data.org API — fixtures, results, standings."""

import time
import requests
from src.config import FOOTBALL_DATA_API_KEY, LEAGUES

BASE_URL = "https://api.football-data.org/v4"
HEADERS = {"X-Auth-Token": FOOTBALL_DATA_API_KEY}

# Rate limit: 10 requests/minute on free tier
_last_request_time = 0


def _get(endpoint: str, params: dict | None = None) -> dict:
    global _last_request_time
    elapsed = time.time() - _last_request_time
    if elapsed < 6.5:  # ~10 req/min safety margin
        time.sleep(6.5 - elapsed)
    _last_request_time = time.time()

    resp = requests.get(f"{BASE_URL}{endpoint}", headers=HEADERS, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def get_upcoming_matches(league_code: str, days: int = 7) -> list[dict]:
    """Get upcoming matches for a league within N days."""
    data = _get(f"/competitions/{league_code}/matches", params={
        "status": "SCHEDULED",
        "dateFrom": _today(),
        "dateTo": _date_offset(days),
    })
    return [_parse_match(m) for m in data.get("matches", [])]


def get_recent_results(league_code: str, days: int = 60) -> list[dict]:
    """Get finished matches for model training/form calculation."""
    data = _get(f"/competitions/{league_code}/matches", params={
        "status": "FINISHED",
        "dateFrom": _date_offset(-days),
        "dateTo": _today(),
    })
    return [_parse_result(m) for m in data.get("matches", [])]


def get_standings(league_code: str) -> list[dict]:
    """Get current league standings."""
    data = _get(f"/competitions/{league_code}/standings")
    table = data.get("standings", [{}])[0].get("table", [])
    return [
        {
            "team": row["team"]["name"],
            "team_id": row["team"]["id"],
            "position": row["position"],
            "played": row["playedGames"],
            "won": row["won"],
            "drawn": row["draw"],
            "lost": row["lost"],
            "goals_for": row["goalsFor"],
            "goals_against": row["goalsAgainst"],
            "points": row["points"],
        }
        for row in table
    ]


def _parse_match(m: dict) -> dict:
    return {
        "match_id": m["id"],
        "competition": m["competition"]["name"],
        "competition_code": m["competition"].get("code", ""),
        "home_team": m["homeTeam"]["name"],
        "home_team_id": m["homeTeam"]["id"],
        "away_team": m["awayTeam"]["name"],
        "away_team_id": m["awayTeam"]["id"],
        "utc_date": m["utcDate"],
        "matchday": m.get("matchday"),
    }


def _parse_result(m: dict) -> dict:
    score = m.get("score", {}).get("fullTime", {})
    return {
        "match_id": m["id"],
        "competition": m["competition"]["name"],
        "competition_code": m["competition"].get("code", ""),
        "home_team": m["homeTeam"]["name"],
        "home_team_id": m["homeTeam"]["id"],
        "away_team": m["awayTeam"]["name"],
        "away_team_id": m["awayTeam"]["id"],
        "home_goals": score.get("home"),
        "away_goals": score.get("away"),
        "utc_date": m["utcDate"],
        "matchday": m.get("matchday"),
    }


def _today() -> str:
    from datetime import date
    return date.today().isoformat()


def _date_offset(days: int) -> str:
    from datetime import date, timedelta
    return (date.today() + timedelta(days=days)).isoformat()


def get_xg_history(league_code: str, days: int = 90) -> list[dict]:
    """Fetch historical xG for a league over the last `days` via API-Football.

    Returns list[{home_team, away_team, home_xg, away_xg, utc_date, xg_source}].
    Entries are NOT index-aligned with get_recent_results — caller must match
    by (home_team, away_team, utc_date). Pipeline does this alignment and
    falls back to integer goals for unmatched fixtures.

    Returns [] if API_FOOTBALL_KEY is missing or league isn't in our id map.

    Quota: 1 call to /fixtures + N calls to /fixtures/statistics per league.
    Typical top-5 league over 90d ≈ 100 finished matches → ≤101 requests/league.
    xg_data.py caches each statistics call for 24h, so repeat invocations
    within a day cost O(1).
    """
    from src.config import API_FOOTBALL_KEY, API_FOOTBALL_LEAGUES
    if not API_FOOTBALL_KEY:
        return []
    af_league_id = API_FOOTBALL_LEAGUES.get(league_code)
    if not af_league_id:
        return []

    from datetime import date, timedelta
    from src.collectors.xg_data import get_xg_for_fixture, _session, BASE_URL

    to_d = date.today().isoformat()
    from_d = (date.today() - timedelta(days=days)).isoformat()

    # API-Football needs a season hint. Europe "2025" covers Aug 2025–May 2026;
    # calendar-year leagues (Brazil/MLS/J1) use the current year. Simple heuristic:
    # if today's month >=7 → season = current year; else current year - 1.
    # Exceptions handled by passing a 2-value attempt list.
    from datetime import datetime as _dt
    now = _dt.utcnow()
    if now.month >= 7:
        seasons_to_try = [now.year, now.year - 1]
    else:
        seasons_to_try = [now.year - 1, now.year]

    fixtures: list[dict] = []
    for season in seasons_to_try:
        try:
            resp = _session.get(
                f"{BASE_URL}/fixtures",
                params={
                    "league": af_league_id,
                    "season": season,
                    "from": from_d,
                    "to": to_d,
                    "status": "FT",
                },
                timeout=25,
            )
            resp.raise_for_status()
            payload = resp.json()
        except Exception:
            continue
        fixtures = payload.get("response", []) or []
        if fixtures:
            break

    out: list[dict] = []
    for fix in fixtures:
        fid = fix.get("fixture", {}).get("id")
        if not fid:
            continue
        teams = fix.get("teams", {}) or {}
        home_blk = teams.get("home") or {}
        away_blk = teams.get("away") or {}
        home = home_blk.get("name", "")
        away = away_blk.get("name", "")
        date_str = fix.get("fixture", {}).get("date")
        xg = get_xg_for_fixture(fid)
        if not xg:
            continue
        out.append({
            "home_team": home,
            "away_team": away,
            # API-Football team ids — passed through so Phase B2 alignment
            # can key on id instead of fuzzy name match.
            "home_team_id": home_blk.get("id"),
            "away_team_id": away_blk.get("id"),
            "home_xg": float(xg["home_xg"]),
            "away_xg": float(xg["away_xg"]),
            "utc_date": date_str,
            "xg_source": xg.get("xg_source", "proxy"),
        })
    return out
