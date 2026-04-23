"""Starting-lineup collector — API-Football /fixtures/lineups endpoint.

Returns the confirmed starting XI, formation, and coach for both sides of a
fixture. Lineups are typically published ~1 hour before kickoff; before that
the endpoint returns an empty response (has_lineup=False).

Cache 30 minutes — short enough to pick up late swaps (injury during warm-up),
long enough that repeated calls within a single analysis cycle cost O(1).
"""

from __future__ import annotations

import logging
import time
from typing import Optional

import requests

from src.config import API_FOOTBALL_KEY

logger = logging.getLogger(__name__)

BASE_URL = "https://v3.football.api-sports.io"
CACHE_TTL_SECONDS = 1800  # 30 minutes

_cache: dict[int, tuple[float, Optional[dict]]] = {}  # fixture_id -> (ts, payload|None)
_session = requests.Session()
if API_FOOTBALL_KEY:
    _session.headers.update({"x-apisports-key": API_FOOTBALL_KEY})


def _cache_get(fixture_id: int) -> tuple[bool, Optional[dict]]:
    """Return (hit, payload). payload may be None (negative cache entry)."""
    hit = _cache.get(fixture_id)
    if not hit:
        return False, None
    ts, payload = hit
    if (time.time() - ts) > CACHE_TTL_SECONDS:
        _cache.pop(fixture_id, None)
        return False, None
    return True, payload


def _cache_put(fixture_id: int, payload: Optional[dict]) -> None:
    _cache[fixture_id] = (time.time(), payload)


def _parse_team_block(block: dict) -> dict:
    """Extract {team_name, formation, starting_xi, coach} from one response item."""
    team = block.get("team", {}) or {}
    coach = block.get("coach", {}) or {}
    start_xi_raw = block.get("startXI", []) or []

    starting_xi: list[dict] = []
    for entry in start_xi_raw:
        p = (entry or {}).get("player", {}) or {}
        starting_xi.append({
            "player_id": p.get("id"),
            "player_name": p.get("name", "N/A"),
            "number": p.get("number"),
            "position": p.get("pos"),
            "grid": p.get("grid"),
        })

    return {
        "team_id": team.get("id"),
        "team_name": team.get("name", "N/A"),
        "formation": block.get("formation") or "N/A",
        "starting_xi": starting_xi,
        "coach": coach.get("name", "N/A"),
    }


def get_lineup(fixture_id: int) -> Optional[dict]:
    """Fetch starting lineup for fixture. Returns
    {"home": {...}, "away": {...}, "has_lineup": bool} or None on error.

    Each team block: {team_id, team_name, formation, starting_xi, coach}.
    `has_lineup` is False when the API responded with an empty list (lineup
    not yet published) — both team blocks will still be present but with
    empty starting_xi.

    Team ordering heuristic: first distinct team_id seen → "home", second →
    "away". API-Football typically returns the home side first. If the
    caller knows the real home/away team_ids it should remap.

    Returns None (logged) on network errors, non-2xx, JSON parse failure, or
    missing API key — caller must tolerate None and fall back gracefully.
    """
    if not API_FOOTBALL_KEY or not fixture_id:
        return None

    hit, cached = _cache_get(fixture_id)
    if hit:
        return cached

    try:
        resp = _session.get(
            f"{BASE_URL}/fixtures/lineups",
            params={"fixture": fixture_id},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning("[Lineup] fixture=%s fetch failed: %s", fixture_id, e)
        _cache_put(fixture_id, None)
        return None

    try:
        rows = data.get("response", []) or []
        if not rows:
            out = {
                "home": {
                    "team_id": None, "team_name": "N/A", "formation": "N/A",
                    "starting_xi": [], "coach": "N/A",
                },
                "away": {
                    "team_id": None, "team_name": "N/A", "formation": "N/A",
                    "starting_xi": [], "coach": "N/A",
                },
                "has_lineup": False,
            }
            _cache_put(fixture_id, out)
            return out

        home_block = _parse_team_block(rows[0])
        away_block = _parse_team_block(rows[1]) if len(rows) > 1 else {
            "team_id": None, "team_name": "N/A", "formation": "N/A",
            "starting_xi": [], "coach": "N/A",
        }
        out = {
            "home": home_block,
            "away": away_block,
            "has_lineup": bool(home_block["starting_xi"] or away_block["starting_xi"]),
        }
    except Exception as e:
        logger.warning("[Lineup] parse failed fixture=%s: %s", fixture_id, e)
        _cache_put(fixture_id, None)
        return None

    _cache_put(fixture_id, out)
    return out
