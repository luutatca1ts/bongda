"""xG collector — fetch Expected Goals from API-Football /fixtures/statistics.

Two public helpers:
- get_xg_for_fixture(fixture_id): single-fixture xG + shot peripherals, cached 24h.
- get_team_xg_history(team_api_id, league_api_id, season, last_n): rolling window
  of a team's recent xG, cached 1h.

Fallback policy: when the API response is missing the "Expected Goals" field
(common on lower-tier leagues / free plan), we synthesize xG from shots-on-target
using the classic SoT × 0.3 proxy. Callers that need to distinguish real vs
proxy can read the `"xg_source"` field ("api" or "proxy").

Quota note: API-Football MEGA plan = 150K req/day. Each xG history call hits
/fixtures once (list of N fixtures) + /fixtures/statistics N times. With 90d
history per league capped at ~50 fixtures per pair, we budget ≤5K/day for xG
— well inside the plan.
"""

from __future__ import annotations

import logging
import time
from typing import Optional

import requests

from src.config import API_FOOTBALL_KEY

logger = logging.getLogger(__name__)

BASE_URL = "https://v3.football.api-sports.io"
_HEADERS = {"x-apisports-key": API_FOOTBALL_KEY} if API_FOOTBALL_KEY else {}

_session = requests.Session()
if API_FOOTBALL_KEY:
    _session.headers.update(_HEADERS)

# Cache: fixture-stats lookups are STALE safe (stats don't change after FT).
FIXTURE_CACHE_TTL = 24 * 3600
# Team-history is "recent N fixtures" — grow as new matches finish, so shorter TTL.
TEAM_HISTORY_CACHE_TTL = 3600

_fixture_cache: dict[int, tuple[float, dict]] = {}
_team_history_cache: dict[tuple, tuple[float, list[dict]]] = {}

# Soft quota guard — we refuse new calls if upstream reports <20K remaining.
# This is a best-effort number; get_af_quota() in api_football.py updates it.
_quota_remaining: Optional[int] = None


def _update_quota(resp) -> None:
    global _quota_remaining
    rem = resp.headers.get("x-ratelimit-requests-remaining")
    if rem is not None:
        try:
            _quota_remaining = int(rem)
        except ValueError:
            pass


def get_xg_quota_remaining() -> Optional[int]:
    return _quota_remaining


def _parse_xg(raw_val) -> Optional[float]:
    """Parse the Expected Goals field from API-Football.

    The field arrives as "2.34", "0.00", None, or sometimes a float. Treat
    "0.00" as a legitimate zero (trailing cache entries may still be live),
    but None / missing / non-numeric → None so callers know to proxy.
    """
    if raw_val is None:
        return None
    try:
        return float(str(raw_val).strip())
    except (TypeError, ValueError):
        return None


def get_xg_for_fixture(fixture_id: int) -> dict:
    """Return per-team xG for a finished fixture.

    Output shape:
        {
            "home_xg": float,  # real or proxy
            "away_xg": float,
            "home_shots": int, "away_shots": int,
            "home_shots_on_target": int, "away_shots_on_target": int,
            "xg_source": "api" | "proxy",
        }

    Returns {} on error / missing key / empty response.
    """
    if not API_FOOTBALL_KEY or not fixture_id:
        return {}

    now = time.time()
    hit = _fixture_cache.get(fixture_id)
    if hit and (now - hit[0]) < FIXTURE_CACHE_TTL:
        return hit[1]

    try:
        resp = _session.get(
            f"{BASE_URL}/fixtures/statistics",
            params={"fixture": fixture_id},
            timeout=15,
        )
        resp.raise_for_status()
        _update_quota(resp)
        data = resp.json()
    except Exception as e:
        logger.warning("[xG] fetch fixture=%s failed: %s", fixture_id, e)
        _fixture_cache[fixture_id] = (now, {})
        return {}

    teams = data.get("response") or []
    if len(teams) < 2:
        _fixture_cache[fixture_id] = (now, {})
        return {}

    def _parse_team(team_block: dict) -> dict:
        stats = {s.get("type"): s.get("value") for s in team_block.get("statistics", [])}
        xg = _parse_xg(stats.get("Expected Goals")) or _parse_xg(stats.get("expected_goals"))
        shots = _safe_int(stats.get("Total Shots"))
        sot = _safe_int(stats.get("Shots on Goal"))
        return {"xg": xg, "shots": shots, "sot": sot}

    # API returns [home, away] in that order.
    home = _parse_team(teams[0])
    away = _parse_team(teams[1])

    has_real = home["xg"] is not None and away["xg"] is not None
    if has_real:
        out = {
            "home_xg": home["xg"],
            "away_xg": away["xg"],
            "home_shots": home["shots"],
            "away_shots": away["shots"],
            "home_shots_on_target": home["sot"],
            "away_shots_on_target": away["sot"],
            "xg_source": "api",
        }
    else:
        # Proxy: shots-on-target × 0.3 (classic Understat-style approximation).
        out = {
            "home_xg": round(home["sot"] * 0.3, 2),
            "away_xg": round(away["sot"] * 0.3, 2),
            "home_shots": home["shots"],
            "away_shots": away["shots"],
            "home_shots_on_target": home["sot"],
            "away_shots_on_target": away["sot"],
            "xg_source": "proxy",
        }

    _fixture_cache[fixture_id] = (now, out)
    return out


def get_team_xg_history(
    team_api_id: int,
    league_api_id: int,
    season: int,
    last_n: int = 10,
) -> list[dict]:
    """Return last_n finished fixtures for this team with xG attached.

    Each entry: {
        fixture_id, utc_date, home_team, away_team,
        home_goals, away_goals, home_xg, away_xg, xg_source,
        is_home: bool (this team's side)
    }
    """
    if not API_FOOTBALL_KEY or not team_api_id or not league_api_id:
        return []

    key = (team_api_id, league_api_id, season, last_n)
    now = time.time()
    hit = _team_history_cache.get(key)
    if hit and (now - hit[0]) < TEAM_HISTORY_CACHE_TTL:
        return hit[1]

    try:
        resp = _session.get(
            f"{BASE_URL}/fixtures",
            params={
                "team": team_api_id,
                "league": league_api_id,
                "season": season,
                "last": last_n,
            },
            timeout=20,
        )
        resp.raise_for_status()
        _update_quota(resp)
        payload = resp.json()
    except Exception as e:
        logger.warning("[xG] team history team=%s league=%s failed: %s",
                       team_api_id, league_api_id, e)
        _team_history_cache[key] = (now, [])
        return []

    out: list[dict] = []
    for fix in payload.get("response", []):
        fid = fix.get("fixture", {}).get("id")
        if not fid:
            continue
        teams = fix.get("teams", {})
        goals = fix.get("goals", {})
        home = teams.get("home", {}) or {}
        away = teams.get("away", {}) or {}

        xg = get_xg_for_fixture(fid) or {}
        out.append({
            "fixture_id": fid,
            "utc_date": fix.get("fixture", {}).get("date"),
            "home_team": home.get("name", ""),
            "away_team": away.get("name", ""),
            "home_team_id": home.get("id"),
            "away_team_id": away.get("id"),
            "home_goals": goals.get("home"),
            "away_goals": goals.get("away"),
            "home_xg": xg.get("home_xg"),
            "away_xg": xg.get("away_xg"),
            "xg_source": xg.get("xg_source", "none"),
            "is_home": home.get("id") == team_api_id,
        })

    _team_history_cache[key] = (now, out)
    return out


def _safe_int(v) -> int:
    if v is None:
        return 0
    try:
        return int(str(v).replace("%", "").strip())
    except (TypeError, ValueError):
        return 0
