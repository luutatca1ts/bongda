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
