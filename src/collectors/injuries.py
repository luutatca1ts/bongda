"""Injury data collector — API-Football /injuries endpoint.

Returns which players are Missing or Questionable for a given fixture.
Caches 1 hour because injury lists don't change often within match day.
"""

from __future__ import annotations

import logging
import time
from typing import Optional

import requests

from src.config import API_FOOTBALL_KEY

logger = logging.getLogger(__name__)

BASE_URL = "https://v3.football.api-sports.io"
CACHE_TTL_SECONDS = 3600  # 1 hour

_cache: dict[int, tuple[float, dict]] = {}  # fixture_id -> (ts, payload)
_session = requests.Session()
if API_FOOTBALL_KEY:
    _session.headers.update({"x-apisports-key": API_FOOTBALL_KEY})


def _cache_get(fixture_id: int) -> Optional[dict]:
    hit = _cache.get(fixture_id)
    if not hit:
        return None
    ts, payload = hit
    if (time.time() - ts) > CACHE_TTL_SECONDS:
        _cache.pop(fixture_id, None)
        return None
    return payload


def _cache_put(fixture_id: int, payload: dict) -> None:
    _cache[fixture_id] = (time.time(), payload)


def _classify_position(pos: str) -> str:
    """Normalize API-Football position strings to the 4 canonical buckets."""
    p = (pos or "").lower()
    if "goalkeeper" in p or p == "g":
        return "Goalkeeper"
    if "defender" in p or "defence" in p or p == "d":
        return "Defender"
    if "midfielder" in p or "midfield" in p or p == "m":
        return "Midfielder"
    if "attacker" in p or "forward" in p or "striker" in p or p == "f":
        return "Attacker"
    return "Midfielder"  # safe default — avg weight


def get_injuries(fixture_id: int) -> dict:
    """Fetch injury list for fixture. Returns {"home": [...], "away": [...]}.

    Each player entry: {player_name, position, reason, status}.
    Only players with status "Missing Fixture" or "Questionable" are returned.
    Empty {"home": [], "away": []} on any error or missing API key —
    caller must tolerate empty result (model falls back to no adjustment).
    """
    empty = {"home": [], "away": []}
    if not API_FOOTBALL_KEY or not fixture_id:
        return empty

    cached = _cache_get(fixture_id)
    if cached is not None:
        return cached

    try:
        resp = _session.get(
            f"{BASE_URL}/injuries",
            params={"fixture": fixture_id},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning("[Injuries] fixture=%s fetch failed: %s", fixture_id, e)
        _cache_put(fixture_id, empty)
        return empty

    out = {"home": [], "away": []}
    try:
        rows = data.get("response", []) or []
        if not rows:
            _cache_put(fixture_id, out)
            return out
        # Determine home/away team id from first row's fixture.teams section is
        # not exposed — API-Football returns player+team separately. We group
        # by team_id; home/away mapping is resolved by the caller if needed.
        # For convenience here, we pick the *first distinct team_id seen* as
        # home and the second as away. The caller should pass fixture_id for
        # which it already knows home/away team_ids and can re-map if order
        # differs. Most /injuries responses DO encode the home team first in
        # the list due to API ordering, so this is a safe heuristic.
        team_order: list[int] = []
        for row in rows:
            team = row.get("team", {}) or {}
            player = row.get("player", {}) or {}
            tid = team.get("id")
            if tid is None:
                continue
            if tid not in team_order:
                team_order.append(tid)
            status = (player.get("reason") or "").strip()
            # API-Football: "reason" holds the free-text reason. The "type"
            # field inside player holds "Missing Fixture" / "Questionable".
            play_type = (player.get("type") or "").strip()
            if play_type not in ("Missing Fixture", "Questionable"):
                continue
            entry = {
                "player_name": player.get("name", "N/A"),
                "position": _classify_position(player.get("position") or ""),
                "reason": status or play_type,
                "status": play_type,
                "team_id": tid,
            }
            bucket_key = "home" if team_order and tid == team_order[0] else "away"
            out[bucket_key].append(entry)
    except Exception as e:
        logger.warning("[Injuries] parse failed fixture=%s: %s", fixture_id, e)
        _cache_put(fixture_id, empty)
        return empty

    _cache_put(fixture_id, out)
    return out


def get_injuries_by_team(fixture_id: int, home_team_id: int, away_team_id: int) -> dict:
    """Same as get_injuries but uses explicit team_ids to disambiguate
    home/away instead of relying on response ordering.

    Prefer this variant when the caller knows the team_ids from fixture
    lookup — it's robust to API-Football re-ordering the response.
    """
    raw = get_injuries(fixture_id)
    if not raw.get("home") and not raw.get("away"):
        return {"home": [], "away": []}
    # Rebuild using team_id
    out = {"home": [], "away": []}
    for bucket in ("home", "away"):
        for entry in raw.get(bucket, []):
            tid = entry.get("team_id")
            if tid == home_team_id:
                out["home"].append(entry)
            elif tid == away_team_id:
                out["away"].append(entry)
            # else: drop — stale / unmatched
    return out
