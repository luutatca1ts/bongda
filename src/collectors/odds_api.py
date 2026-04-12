"""Collector for The Odds API — live odds from multiple bookmakers."""

import logging
import requests
from src.config import ODDS_API_KEY, ODDS_SPORTS

logger = logging.getLogger(__name__)

BASE_URL = "https://api.the-odds-api.com/v4"

# Track API quota globally
_quota = {"remaining": None, "used": None}


def get_quota() -> dict:
    """Return current API quota status."""
    return dict(_quota)


def _update_quota(resp):
    """Update quota from API response headers."""
    remaining = resp.headers.get("x-requests-remaining")
    used = resp.headers.get("x-requests-used")
    if remaining is not None:
        try:
            _quota["remaining"] = int(remaining)
        except ValueError:
            pass
    if used is not None:
        try:
            _quota["used"] = int(used)
        except ValueError:
            pass
    logger.info(f"[OddsAPI] Quota — remaining: {_quota['remaining']}, used: {_quota['used']}")


def get_live_scores(league_code: str) -> list[dict]:
    """Get live scores for in-play matches."""
    sport_key = ODDS_SPORTS.get(league_code)
    if not sport_key:
        return []
    try:
        resp = requests.get(
            f"{BASE_URL}/sports/{sport_key}/scores",
            params={"apiKey": ODDS_API_KEY, "daysFrom": 1},
            timeout=30,
        )
        resp.raise_for_status()
        _update_quota(resp)
        results = []
        for ev in resp.json():
            if not ev.get("completed") and ev.get("scores"):
                scores = {s["name"]: int(s["score"]) for s in ev["scores"] if s.get("score")}
                results.append({
                    "event_id": ev["id"],
                    "home_team": ev["home_team"],
                    "away_team": ev["away_team"],
                    "commence_time": ev["commence_time"],
                    "home_score": scores.get(ev["home_team"], 0),
                    "away_score": scores.get(ev["away_team"], 0),
                })
        return results
    except Exception as e:
        logger.error(f"[OddsAPI] Live scores failed: {e}")
        return []


def get_live_odds(league_code: str, live_event_ids: list[str] = None, markets: str = "h2h,totals,spreads") -> list[dict]:
    """
    Get odds for in-play matches by fetching all odds and filtering
    to only events that are currently live (based on live_event_ids from scores).
    """
    sport_key = ODDS_SPORTS.get(league_code)
    if not sport_key or not live_event_ids:
        return []
    try:
        resp = requests.get(
            f"{BASE_URL}/sports/{sport_key}/odds",
            params={
                "apiKey": ODDS_API_KEY,
                "regions": "eu,uk",
                "markets": markets,
                "oddsFormat": "decimal",
                "eventIds": ",".join(live_event_ids),
            },
            timeout=30,
        )
        resp.raise_for_status()
        _update_quota(resp)
        return [_parse_event(event) for event in resp.json()]
    except Exception as e:
        logger.error(f"[OddsAPI] Live odds failed: {e}")
        return []


def get_odds(league_code: str, markets: str = "h2h,totals,spreads") -> list[dict]:
    """
    Get odds for upcoming matches in a league.
    markets: h2h (1X2), totals (over/under), spreads (asian handicap)
    """
    sport_key = ODDS_SPORTS.get(league_code)
    if not sport_key:
        return []

    resp = requests.get(
        f"{BASE_URL}/sports/{sport_key}/odds",
        params={
            "apiKey": ODDS_API_KEY,
            "regions": "eu,uk",
            "markets": markets,
            "oddsFormat": "decimal",
        },
        timeout=30,
    )
    resp.raise_for_status()

    _update_quota(resp)

    return [_parse_event(event) for event in resp.json()]


def _parse_event(event: dict) -> dict:
    bookmakers_data = {}
    for bk in event.get("bookmakers", []):
        bk_key = bk["key"]
        bk_name = bk["title"]
        markets = {}
        for market in bk.get("markets", []):
            if market["key"] in ("totals", "spreads"):
                # Include point (handicap line or O/U line) for both
                outcomes = {}
                for o in market.get("outcomes", []):
                    outcomes[o["name"]] = {
                        "price": o.get("price"),
                        "point": o.get("point"),
                    }
                markets[market["key"]] = outcomes
            elif market["key"] == "alternate_totals_corners":
                # Corner O/U — multiple lines (8.5, 9.5, 10.5, etc.)
                # Group by point to keep all lines
                corners = markets.get("corners_totals", [])
                for o in market.get("outcomes", []):
                    corners.append({
                        "name": o["name"],  # Over / Under
                        "price": o.get("price"),
                        "point": o.get("point"),
                    })
                markets["corners_totals"] = corners
            else:
                # h2h — just price
                outcomes = {o["name"]: o.get("price") for o in market.get("outcomes", [])}
                markets[market["key"]] = outcomes
        bookmakers_data[bk_key] = {"name": bk_name, "markets": markets}

    return {
        "event_id": event["id"],
        "sport": event["sport_key"],
        "home_team": event["home_team"],
        "away_team": event["away_team"],
        "commence_time": event["commence_time"],
        "bookmakers": bookmakers_data,
    }


# Priority bookmakers: Pinnacle first (sharpest odds), then best price
PRIORITY_BOOKMAKERS = ["pinnacle"]


def get_best_odds(event: dict, market: str = "h2h") -> dict:
    """
    Find the best odds across all bookmakers for each outcome.
    Prioritizes Pinnacle — uses Pinnacle odds if available,
    falls back to best price across all bookmakers.
    """
    best = {}
    pinnacle = {}

    for bk_key, bk_data in event.get("bookmakers", {}).items():
        mkt = bk_data.get("markets", {}).get(market, {})
        is_priority = bk_key in PRIORITY_BOOKMAKERS

        for outcome_name, value in mkt.items():
            price = value if isinstance(value, (int, float)) else value.get("price", 0)
            point = value.get("point") if isinstance(value, dict) else None
            entry = {
                "price": price,
                "bookmaker": bk_data["name"],
                "bookmaker_key": bk_key,
                "point": point,
            }

            # Track Pinnacle separately
            if is_priority:
                pinnacle[outcome_name] = entry

            # Track best price overall
            if outcome_name not in best or price > best[outcome_name]["price"]:
                best[outcome_name] = entry

    # Prefer Pinnacle odds when available (override best price)
    for outcome_name, entry in pinnacle.items():
        best[outcome_name] = entry

    return best


def get_corner_odds(league_code: str, event_ids: list[str] | None = None) -> dict:
    """
    Fetch corner odds for all events in a league.
    Uses per-event endpoint (bulk doesn't support alternate corner markets).
    If event_ids provided, skip the events list call to save quota.
    Returns: {"home__away": {"totals": {line: {...}}, "spreads": [{...}],
              "h1_totals": {...}, "h1_spreads": [...]}}
    """
    sport_key = ODDS_SPORTS.get(league_code)
    if not sport_key:
        return {}

    # Only these 2 markets are supported by the API; _1st_half variants return 422
    CORNER_MARKETS = "alternate_totals_corners,alternate_spreads_corners"

    try:
        # Get event list if not provided
        if not event_ids:
            resp = requests.get(
                f"{BASE_URL}/sports/{sport_key}/events",
                params={"apiKey": ODDS_API_KEY},
                timeout=30,
            )
            resp.raise_for_status()
            _update_quota(resp)
            events = resp.json()
        else:
            events = [{"id": eid} for eid in event_ids]

        if not events:
            return {}

        result = {}
        for ev in events:
            eid = ev.get("id", "")
            if not eid:
                continue
            try:
                resp2 = requests.get(
                    f"{BASE_URL}/sports/{sport_key}/events/{eid}/odds",
                    params={
                        "apiKey": ODDS_API_KEY,
                        "regions": "eu,uk",
                        "markets": CORNER_MARKETS,
                        "oddsFormat": "decimal",
                    },
                    timeout=20,
                )
                resp2.raise_for_status()
                _update_quota(resp2)
                data = resp2.json()

                corners_totals = []
                corners_spreads = []

                for bk in data.get("bookmakers", []):
                    bk_name = bk.get("title", bk.get("key", "?"))
                    for market in bk.get("markets", []):
                        mk = market.get("key", "")
                        for o in market.get("outcomes", []):
                            entry = {
                                "name": o["name"],
                                "price": o.get("price", 0),
                                "point": o.get("point"),
                                "bk": bk_name,
                            }
                            if "totals_corners" in mk:
                                corners_totals.append(entry)
                            elif "spreads_corners" in mk:
                                corners_spreads.append(entry)

                home_team = data.get("home_team", ev.get("home_team", ""))
                away_team = data.get("away_team", ev.get("away_team", ""))
                key = f"{home_team}__{away_team}"

                match_data = {}
                if corners_totals:
                    match_data["totals"] = _build_corner_best(corners_totals)
                if corners_spreads:
                    match_data["spreads"] = _build_corner_spreads(corners_spreads)
                if match_data:
                    result[key] = match_data

            except Exception as exc:
                logger.warning(f"[OddsAPI] Corner event {eid} error: {exc}")
                continue

        logger.info(f"[OddsAPI] Corner odds: {len(result)}/{len(events)} events for {league_code}")
        return result

    except Exception as e:
        logger.error(f"[OddsAPI] Corner odds failed for {league_code}: {e}")
        return {}


def _build_corner_best(corners_list: list, target_line: float = 9.5) -> dict:
    """Build best corner odds from raw list."""
    lines = {}
    for c in corners_list:
        point = c.get("point")
        if point is None:
            continue
        if point not in lines:
            lines[point] = {"over": [], "under": []}
        if c["name"] == "Over":
            lines[point]["over"].append((c["price"], c["bk"]))
        elif c["name"] == "Under":
            lines[point]["under"].append((c["price"], c["bk"]))

    sorted_lines = sorted(lines.keys(), key=lambda x: abs(x - target_line))
    result = {}
    for line in sorted_lines[:6]:
        data = lines[line]
        best_over = max(data["over"], key=lambda x: x[0]) if data["over"] else (0, "?")
        best_under = max(data["under"], key=lambda x: x[0]) if data["under"] else (0, "?")
        if best_over[0] and best_under[0]:
            result[line] = {
                "over_price": best_over[0],
                "over_bk": best_over[1],
                "under_price": best_under[0],
                "under_bk": best_under[1],
            }
    return result


def _build_corner_spreads(spreads_list: list) -> list:
    """
    Build corner Asian Handicap pairs from raw spread outcomes.
    Pairs MUST have:
      - Same absolute point (opposite signs)
      - DIFFERENT team names
    Returns sorted by line "balance" (most balanced = main line first):
      [{home_name, away_name, home_point, away_point, home_price, away_price, bk}]
    """
    pairs = []
    bk_outcomes = {}
    for s in spreads_list:
        bk = s["bk"]
        if bk not in bk_outcomes:
            bk_outcomes[bk] = []
        bk_outcomes[bk].append(s)

    for bk, outcomes in bk_outcomes.items():
        seen = set()
        for i, o1 in enumerate(outcomes):
            if i in seen:
                continue
            for j, o2 in enumerate(outcomes):
                if j in seen or j == i:
                    continue
                if o1["point"] is None or o2["point"] is None:
                    continue
                # Must be opposite points
                if abs(o1["point"] + o2["point"]) >= 0.01:
                    continue
                # Must be DIFFERENT teams (avoid Chelsea/Chelsea cross-pair bug)
                if o1["name"].strip().lower() == o2["name"].strip().lower():
                    continue
                seen.add(i)
                seen.add(j)
                # Team with smaller (more negative) point = favorite = "home" label
                if o1["point"] < o2["point"]:
                    home, away = o1, o2
                else:
                    home, away = o2, o1
                pairs.append({
                    "home_name": home["name"],
                    "away_name": away["name"],
                    "home_point": home["point"],
                    "away_point": away["point"],
                    "home_price": home["price"],
                    "away_price": away["price"],
                    "bk": bk,
                })
                break

    # Sort by line balance — most balanced (closest to 50/50) = main line first.
    # Main line is what bookmaker considers fair and is what users see by default.
    pairs.sort(key=lambda x: abs(x["home_price"] - x["away_price"]))
    return pairs[:5]  # Top 5 to allow value-bet search across alternate lines


def get_best_corners(event: dict, target_line: float = 9.5) -> dict:
    """
    Get best corner O/U odds across all bookmakers.
    Returns dict with lines closest to target_line.
    Format: {line: {over_price, over_bk, under_price, under_bk}}
    """
    # Collect all corner lines from all bookmakers
    lines = {}  # line -> {over: [(price, bk)], under: [(price, bk)]}

    for bk_key, bk_data in event.get("bookmakers", {}).items():
        corners = bk_data.get("markets", {}).get("corners_totals", [])
        for c in corners:
            point = c.get("point")
            if point is None:
                continue
            if point not in lines:
                lines[point] = {"over": [], "under": []}
            if c["name"] == "Over":
                lines[point]["over"].append((c["price"], bk_data["name"]))
            elif c["name"] == "Under":
                lines[point]["under"].append((c["price"], bk_data["name"]))

    if not lines:
        return {}

    # Pick lines closest to target, return top 3
    sorted_lines = sorted(lines.keys(), key=lambda x: abs(x - target_line))
    result = {}
    for line in sorted_lines[:6]:
        data = lines[line]
        best_over = max(data["over"], key=lambda x: x[0]) if data["over"] else (0, "?")
        best_under = max(data["under"], key=lambda x: x[0]) if data["under"] else (0, "?")
        if best_over[0] and best_under[0]:
            result[line] = {
                "over_price": best_over[0],
                "over_bk": best_over[1],
                "under_price": best_under[0],
                "under_bk": best_under[1],
            }
    return result


def get_spread_pairs(event: dict) -> list[dict]:
    """
    Get Asian Handicap as paired lines from SAME bookmaker.
    Returns list of pairs sorted by bookmaker quality.
    Each pair: {home_team, away_team, home_point, away_point,
                home_price, away_price, bookmaker}
    """
    pairs = []
    home_team = event.get("home_team", "Home")
    away_team = event.get("away_team", "Away")

    for bk_key, bk_data in event.get("bookmakers", {}).items():
        spreads = bk_data.get("markets", {}).get("spreads", {})
        if len(spreads) < 2:
            continue

        outcomes = list(spreads.values())
        names = list(spreads.keys())

        # First outcome is usually home team
        home_info = outcomes[0]
        away_info = outcomes[1]
        home_name = names[0]
        away_name = names[1]

        if not isinstance(home_info, dict) or not isinstance(away_info, dict):
            continue
        if home_info.get("point") is None or away_info.get("point") is None:
            continue

        pairs.append({
            "home_name": home_name,
            "away_name": away_name,
            "home_point": home_info["point"],
            "away_point": away_info["point"],
            "home_price": home_info["price"],
            "away_price": away_info["price"],
            "bookmaker": bk_data["name"],
        })

    # Sort: Pinnacle first, then by home_price descending
    pairs.sort(key=lambda x: (0 if x["bookmaker"].lower() == "pinnacle" else 1, -x["home_price"]))
    return pairs
