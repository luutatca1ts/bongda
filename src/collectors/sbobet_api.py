"""Collector for Sbobet odds via OddsPapi (api.oddspapi.io) — free tier."""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from src.config import ODDSPAPI_KEY

logger = logging.getLogger(__name__)

BASE_URL = "https://api.oddspapi.io/v4"

# Reusable session for connection pooling (keeps TCP connections alive)
_session = requests.Session()
_session.headers.update({"Accept": "application/json"})

# OddsPapi tournament IDs for football (sportId=10)
# These need to be discovered via /v4/tournaments?sportId=10
# Common IDs (will be auto-discovered if wrong)
SBOBET_TOURNAMENTS = {
    "PL": 17,       # English Premier League
    "PD": 8,        # Spanish La Liga
    "BL1": 35,      # German Bundesliga
    "SA": 23,       # Italian Serie A
    "FL1": 34,      # French Ligue 1
    "CL": 7,        # UEFA Champions League
}

# OddsPapi market IDs
MARKET_MONEYLINE = "101"       # 1X2 (outcomes: 101=home, 102=draw, 103=away)
MARKET_OVER_UNDER = "1010"     # O/U 2.5
# Asian Handicap market IDs range from 1074-1090


# Bookmakers to try in order: Sbobet first, then Pinnacle as fallback
BOOKMAKER_PRIORITY = ["sbobet", "pinnacle"]


def _fetch_fixture_odds(fixture: dict) -> dict | None:
    """Fetch odds for a single fixture (called in parallel). Returns result dict or None."""
    fid = fixture.get("fixtureId", fixture.get("id", ""))
    home = fixture.get("participant1Name", "")
    away = fixture.get("participant2Name", "")
    start = fixture.get("startTime", "")

    if not fid or not home:
        return None

    for bk in BOOKMAKER_PRIORITY:
        try:
            resp = _session.get(
                f"{BASE_URL}/odds",
                params={
                    "apiKey": ODDSPAPI_KEY,
                    "fixtureId": fid,
                    "bookmaker": bk,
                    "oddsFormat": "decimal",
                },
                timeout=15,
            )
            resp.raise_for_status()
            odds_data = resp.json()

            if not odds_data.get("hasOdds"):
                continue

            bk_odds = odds_data.get("bookmakerOdds", {}).get(bk, {})
            markets = bk_odds.get("markets", {})
            if markets:
                odds = _parse_markets(markets)
                if odds.get("h2h") or odds.get("asian_handicap"):
                    return {
                        "home_team": home,
                        "away_team": away,
                        "kick_off": start,
                        "sbobet_odds": odds,
                        "bookmaker_source": bk.upper(),
                    }
        except Exception:
            continue
    return None


def get_sbobet_league_odds(league_code: str) -> list[dict]:
    """
    Get Sbobet/Pinnacle odds for upcoming matches in a league.
    First tries Sbobet, falls back to Pinnacle if no data.
    Uses /v4/fixtures + /v4/odds per fixture (parallel).
    Returns: [{home_team, away_team, sbobet_odds: {h2h, asian_handicap, over_under}, bookmaker_source}]
    """
    tournament_id = SBOBET_TOURNAMENTS.get(league_code)
    if not tournament_id or not ODDSPAPI_KEY:
        return []

    try:
        # Step 1: Get upcoming fixtures (1 API call)
        resp = _session.get(
            f"{BASE_URL}/fixtures",
            params={
                "apiKey": ODDSPAPI_KEY,
                "tournamentId": tournament_id,
                "statusId": 0,  # not started
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        fixtures = data if isinstance(data, list) else data.get("data", [])
        if not fixtures:
            return []

        # Step 2: Fetch odds for fixtures IN PARALLEL (5 workers)
        to_fetch = fixtures[:10]  # Limit to save API quota (250 free/month)
        results = []

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(_fetch_fixture_odds, f): f for f in to_fetch}
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                except Exception:
                    pass

        logger.info(f"[Sbobet] Got {len(results)}/{len(fixtures)} events for {league_code}")
        return results

    except Exception as e:
        logger.error(f"[Sbobet] Failed for {league_code}: {e}")
        return []


def _parse_oddspapi_fixture(fixture: dict) -> dict | None:
    """Parse OddsPapi fixture into our standard Sbobet odds format."""
    # Get team names from participants
    home_team = fixture.get("participant1Name", fixture.get("home", ""))
    away_team = fixture.get("participant2Name", fixture.get("away", ""))

    if not home_team or not away_team:
        # Try alternative field names
        participants = fixture.get("participants", {})
        if participants:
            home_team = participants.get("1", {}).get("name", "")
            away_team = participants.get("2", {}).get("name", "")

    if not home_team:
        return None

    # Parse Sbobet odds from bookmakerOdds
    bk_odds = fixture.get("bookmakerOdds", {})
    sbobet_data = bk_odds.get("sbobet", bk_odds.get("Sbobet", {}))

    if not sbobet_data:
        # Maybe odds are directly in the fixture
        sbobet_data = bk_odds
        if not sbobet_data:
            return None

    markets = sbobet_data.get("markets", sbobet_data)
    odds = _parse_markets(markets)

    if not odds["h2h"] and not odds["asian_handicap"] and not odds["over_under"]:
        return None

    return {
        "home_team": home_team,
        "away_team": away_team,
        "kick_off": fixture.get("startTime", ""),
        "sbobet_odds": odds,
    }


def _parse_markets(markets: dict) -> dict:
    """Parse OddsPapi market structure into our standard format."""
    odds = {
        "h2h": {},
        "asian_handicap": [],
        "over_under": [],
    }

    if not isinstance(markets, dict):
        return odds

    for market_id, market_data in markets.items():
        outcomes = market_data.get("outcomes", {})

        # === MONEYLINE (1X2) — market 101 ===
        if market_id == "101":
            odds["h2h"] = _parse_moneyline(outcomes)

        # === OVER/UNDER — markets 1010, 1012, 10166-10178 ===
        elif market_id in ("1010", "1012") or (market_id.startswith("101") and len(market_id) > 3):
            ou = _parse_over_under(outcomes, market_id)
            if ou:
                odds["over_under"].append(ou)

        # === ASIAN HANDICAP — markets 1074-1090+ ===
        elif market_id.startswith("107") or market_id.startswith("108") or market_id.startswith("109"):
            ah = _parse_asian_handicap(outcomes, market_id)
            if ah:
                odds["asian_handicap"].append(ah)

    # Sort AH by absolute handicap value
    odds["asian_handicap"].sort(key=lambda x: abs(x["hdp"]))

    # Keep only the main O/U line (closest to 2.5)
    if odds["over_under"]:
        odds["over_under"].sort(key=lambda x: abs(x["line"] - 2.5))

    return odds


def _parse_moneyline(outcomes: dict) -> dict:
    """Parse 1X2 moneyline outcomes."""
    h2h = {}
    for outcome_id, outcome_data in outcomes.items():
        players = outcome_data.get("players", {})
        player = players.get("0", {})
        price = _to_float(player.get("price", 0))
        if not price:
            continue

        bk_id = player.get("bookmakerOutcomeId", "")
        if outcome_id == "101" or bk_id == "home":
            h2h["Home"] = price
        elif outcome_id == "102" or bk_id == "draw":
            h2h["Draw"] = price
        elif outcome_id == "103" or bk_id == "away":
            h2h["Away"] = price

    return h2h


def _parse_over_under(outcomes: dict, market_id: str) -> dict | None:
    """Parse over/under outcomes."""
    over_price = 0.0
    under_price = 0.0
    line = 2.5  # default

    # Try to determine line from market_id
    line_map = {
        "1010": 2.5, "1012": 3.5,
        "10166": 1.75, "10168": 2.0, "10170": 2.25, "10172": 2.75,
        "10174": 3.0, "10176": 3.25, "10178": 3.75,
    }
    line = line_map.get(market_id, 2.5)

    for outcome_id, outcome_data in outcomes.items():
        players = outcome_data.get("players", {})
        player = players.get("0", {})
        price = _to_float(player.get("price", 0))
        bk_id = player.get("bookmakerOutcomeId", "")

        if outcome_id == "104" or bk_id == "over":
            over_price = price
        elif outcome_id == "105" or bk_id == "under":
            under_price = price

    if over_price and under_price:
        return {"line": line, "over_price": over_price, "under_price": under_price}
    return None


def _parse_asian_handicap(outcomes: dict, market_id: str) -> dict | None:
    """Parse Asian Handicap outcomes."""
    home_price = 0.0
    away_price = 0.0
    hdp = 0.0

    # AH market IDs encode the handicap line
    # 1074=-0.25, 1076=-0.5, 1078=-0.75, 1080=-1.0, 1082=-1.25, etc.
    # This mapping may vary — we extract hdp from outcome data if possible

    for outcome_id, outcome_data in outcomes.items():
        players = outcome_data.get("players", {})
        player = players.get("0", {})
        price = _to_float(player.get("price", 0))
        bk_id = player.get("bookmakerOutcomeId", "")

        # Try to get handicap from player data
        player_hdp = player.get("handicap", player.get("hdp", player.get("point")))
        if player_hdp is not None:
            hdp = _to_float(player_hdp)

        if bk_id == "home" or outcome_id in ("106", "108"):
            home_price = price
        elif bk_id == "away" or outcome_id in ("107", "109"):
            away_price = price

    if home_price and away_price:
        return {"hdp": hdp, "home_price": home_price, "away_price": away_price}
    return None


def _to_float(val) -> float:
    """Safe convert to float."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0
