"""Live state collector — lấy state trực tiếp của 1 fixture từ API-Football."""

from __future__ import annotations

import logging

from src.collectors.api_football import (
    get_fixture_events,
    get_fixture_stats,
    get_live_fixtures,
    parse_events,
)
from src.config import LIVE_LEAGUE_IDS, LIVE_XG_AVAILABLE

logger = logging.getLogger(__name__)


def _parse_xg(val) -> float:
    """expected_goals trong response có thể là str ('0.82') hoặc None."""
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    try:
        return float(str(val).strip())
    except (TypeError, ValueError):
        return 0.0


def _xg_proxy(stats: dict) -> float:
    """xG proxy khi API không trả expected_goals.

    shots_on × 0.25  (~strong opportunity)
    shots_off × 0.05 (~weak opportunity)
    """
    if not stats:
        return 0.0
    sot = int(stats.get("shots_on") or 0)
    sof = int(stats.get("shots_off") or 0)
    return sot * 0.25 + sof * 0.05


def get_live_match_state(fixture_id: int) -> dict:
    """Lấy state hiện tại của 1 fixture (score, phút, xG, thẻ đỏ, shots)."""
    if not fixture_id:
        return {}

    # Một call /fixtures/statistics đã có đủ shots + red cards + (có thể) xG.
    stats = get_fixture_stats(fixture_id)
    if not stats:
        logger.info(f"[LiveStats] No stats for fixture {fixture_id}")
        return {}

    home_stats = stats.get("home", {}) or {}
    away_stats = stats.get("away", {}) or {}

    # Lấy xG thật nếu plan có, else dùng proxy
    home_xg_real = _parse_xg(home_stats.get("expected_goals"))
    away_xg_real = _parse_xg(away_stats.get("expected_goals"))
    if LIVE_XG_AVAILABLE and (home_xg_real > 0 or away_xg_real > 0):
        home_xg = home_xg_real
        away_xg = away_xg_real
        xg_source = "api"
    elif home_xg_real > 0 or away_xg_real > 0:
        # Plan không được mark available nhưng response vẫn có → dùng
        home_xg = home_xg_real
        away_xg = away_xg_real
        xg_source = "api_untagged"
    else:
        home_xg = _xg_proxy(home_stats)
        away_xg = _xg_proxy(away_stats)
        xg_source = "proxy"

    # Red cards — stats cho số card có thể thiếu, lấy từ events cho chắc
    home_reds = int(home_stats.get("red") or 0)
    away_reds = int(away_stats.get("red") or 0)
    if home_reds == 0 and away_reds == 0:
        events = get_fixture_events(fixture_id)
        home_team_id = home_stats.get("team_id", 0)
        parsed = parse_events(events, home_team_id)
        home_reds = parsed["home_reds"]
        away_reds = parsed["away_reds"]

    return {
        "fixture_id": fixture_id,
        "home_team": home_stats.get("team_name", ""),
        "away_team": away_stats.get("team_name", ""),
        "home_team_id": home_stats.get("team_id", 0),
        "away_team_id": away_stats.get("team_id", 0),
        "home_xg": home_xg,
        "away_xg": away_xg,
        "home_shots_on_target": int(home_stats.get("shots_on") or 0),
        "away_shots_on_target": int(away_stats.get("shots_on") or 0),
        "home_red_cards": home_reds,
        "away_red_cards": away_reds,
        "xg_source": xg_source,
    }


def get_all_live_matches() -> list[dict]:
    """Tất cả fixture đang live trong các giải top (LIVE_LEAGUE_IDS).

    Return list theo format của get_live_fixtures() nhưng đã filter.
    """
    fixtures = get_live_fixtures()  # filter theo API_FOOTBALL_LEAGUES
    if not fixtures:
        return []
    if LIVE_LEAGUE_IDS:
        fixtures = [f for f in fixtures if f.get("league_id") in LIVE_LEAGUE_IDS]
    return fixtures
