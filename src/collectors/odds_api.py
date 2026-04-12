"""Collector for The Odds API — live odds from multiple bookmakers."""

import logging
import time
from threading import Lock
import requests
from src.config import ODDS_API_KEY, ODDS_SPORTS

logger = logging.getLogger(__name__)

BASE_URL = "https://api.the-odds-api.com/v4"

# Track API quota globally
_quota = {"remaining": None, "used": None}

# === Adaptive Pinnacle call gate ===
# Base interval 18s. On error/429 widen to 30s. After 5 min clean → restore 18s.
_PIN_BASE_INTERVAL = 18
_PIN_BACKOFF_INTERVAL = 30
_PIN_RECOVERY_SECONDS = 300
_pin_gate = {
    "current_interval": _PIN_BASE_INTERVAL,
    "last_error": 0.0,
    "lock": Lock(),
}
# Per-event cache: {(sport_key, eid): (timestamp, result_dict)}
_corner_cache: dict = {}


def _pin_gate_recover_if_clean():
    """If 5 min have passed since last error, drop interval back to base."""
    with _pin_gate["lock"]:
        if (
            _pin_gate["current_interval"] != _PIN_BASE_INTERVAL
            and (time.time() - _pin_gate["last_error"]) > _PIN_RECOVERY_SECONDS
        ):
            _pin_gate["current_interval"] = _PIN_BASE_INTERVAL
            logger.info(f"[OddsAPI] Pinnacle gate recovered → {_PIN_BASE_INTERVAL}s")


def _pin_gate_register_error(reason: str):
    """Widen interval to backoff on any error / rate limit."""
    with _pin_gate["lock"]:
        if _pin_gate["current_interval"] != _PIN_BACKOFF_INTERVAL:
            logger.warning(
                f"[OddsAPI] Pinnacle gate widened {_pin_gate['current_interval']}s "
                f"→ {_PIN_BACKOFF_INTERVAL}s ({reason})"
            )
        _pin_gate["current_interval"] = _PIN_BACKOFF_INTERVAL
        _pin_gate["last_error"] = time.time()


def get_pinnacle_gate_status() -> dict:
    """Diagnostic helper: returns current interval + last error info."""
    return {
        "current_interval": _pin_gate["current_interval"],
        "base_interval": _PIN_BASE_INTERVAL,
        "backoff_interval": _PIN_BACKOFF_INTERVAL,
        "seconds_since_last_error": (
            int(time.time() - _pin_gate["last_error"]) if _pin_gate["last_error"] else None
        ),
    }


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
    STRICT Pinnacle-only odds. Returns Pinnacle's listed line and price.
    Returns empty dict if Pinnacle is not available — no fallback.
    """
    result = {}
    for bk_key, bk_data in event.get("bookmakers", {}).items():
        if bk_key != "pinnacle":
            continue
        mkt = bk_data.get("markets", {}).get(market, {})
        for outcome_name, value in mkt.items():
            price = value if isinstance(value, (int, float)) else value.get("price", 0)
            point = value.get("point") if isinstance(value, dict) else None
            result[outcome_name] = {
                "price": price,
                "bookmaker": bk_data["name"],
                "bookmaker_key": bk_key,
                "point": point,
            }
    return result


# MAIN markets carry the live active line; alternates carry alternative lines.
# Pinnacle may live in us2 region for some sports — query all major regions.
CORNER_MARKETS_FULL = "totals_corners,spreads_corners,alternate_totals_corners,alternate_spreads_corners"
CORNER_MARKETS_MAIN_ONLY = "totals_corners,spreads_corners"
CORNER_REGIONS = "eu,uk,us,us2,au"


def _parse_corner_response(data: dict, ev_hint: dict | None = None) -> dict:
    """
    Parse one event's corner odds response into structured form.
    Returns: {
        "home_team": str, "away_team": str,
        "totals": {line: {over_price, under_price, ...}},  # main only
        "spreads": [pair_dict],                             # main only
        "had_pinnacle": bool,
        "had_main_market": bool,
        "alt_only_lines": [list of alt lines seen — for log only],
    }
    """
    corners_totals: list = []
    corners_spreads: list = []
    bk_market_log: dict = {}

    for bk in data.get("bookmakers", []):
        bk_key = bk.get("key", "?")
        bk_name = bk.get("title", bk_key)
        bk_markets = []
        for market in bk.get("markets", []):
            mk = market.get("key", "")
            bk_markets.append(f"{mk}({len(market.get('outcomes', []))})")
            is_main = not mk.startswith("alternate_")
            for o in market.get("outcomes", []):
                entry = {
                    "name": o["name"],
                    "price": o.get("price", 0),
                    "point": o.get("point"),
                    "bk": bk_name,
                    "bk_key": bk_key,
                    "is_main": is_main,
                }
                if "totals_corners" in mk:
                    corners_totals.append(entry)
                elif "spreads_corners" in mk:
                    corners_spreads.append(entry)
        bk_market_log[bk_key] = bk_markets

    home_team = data.get("home_team", (ev_hint or {}).get("home_team", ""))
    away_team = data.get("away_team", (ev_hint or {}).get("away_team", ""))

    pinnacle_totals = [c for c in corners_totals if c.get("bk_key") == "pinnacle"]
    pinnacle_spreads = [c for c in corners_spreads if c.get("bk_key") == "pinnacle"]
    pinnacle_main_totals = [c for c in pinnacle_totals if c.get("is_main")]
    pinnacle_main_spreads = [c for c in pinnacle_spreads if c.get("is_main")]
    alt_lines = sorted({
        c.get("point") for c in (pinnacle_totals + pinnacle_spreads)
        if not c.get("is_main") and c.get("point") is not None
    })

    logger.info(
        f"[OddsAPI] Corner parse {home_team} vs {away_team}: "
        f"books={list(bk_market_log.keys())} | "
        f"Pin totals all={len(pinnacle_totals)} main={len(pinnacle_main_totals)} | "
        f"Pin spreads all={len(pinnacle_spreads)} main={len(pinnacle_main_spreads)}"
    )

    parsed: dict = {
        "home_team": home_team,
        "away_team": away_team,
        "totals": {},
        "spreads": [],
        "had_pinnacle": bool(pinnacle_totals or pinnacle_spreads),
        "had_main_market": bool(pinnacle_main_totals or pinnacle_main_spreads),
        "alt_only_lines": alt_lines,
    }
    if corners_totals:
        parsed["totals"] = _build_corner_best(corners_totals)
    if corners_spreads:
        parsed["spreads"] = _build_corner_spreads(corners_spreads)
    return parsed


def _http_get_corner(sport_key: str, eid: str, markets_str: str):
    return requests.get(
        f"{BASE_URL}/sports/{sport_key}/events/{eid}/odds",
        params={
            "apiKey": ODDS_API_KEY,
            "regions": CORNER_REGIONS,
            "markets": markets_str,
            "oddsFormat": "decimal",
        },
        headers={"Cache-Control": "no-cache", "Pragma": "no-cache"},
        timeout=20,
    )


def fetch_pinnacle_corners(sport_key: str, eid: str, ev_hint: dict | None = None,
                           max_retries: int = 3) -> dict:
    """
    ISOLATED function to fetch Pinnacle corner odds for one event.

    Independent from other odds-fetch logic — edits to other code must NOT
    touch this function. It is the single source of truth for corner data.

    Behaviour:
      1. Cache: if a result was fetched within `current_interval` seconds, return it.
      2. Retry up to `max_retries` (default 3) on empty/error.
      3. Each attempt fetches FULL markets (main + alternates).
      4. On 422 (markets unsupported), retries with main-only.
      5. On any error/429: register error → gate widens to 30s.
      6. Returns parsed dict; never raises.
    """
    _pin_gate_recover_if_clean()

    cache_key = (sport_key, eid)
    cached = _corner_cache.get(cache_key)
    if cached:
        ts, result = cached
        if (time.time() - ts) < _pin_gate["current_interval"]:
            logger.info(
                f"[OddsAPI] Corner cache HIT {eid} "
                f"(age={int(time.time() - ts)}s, ttl={_pin_gate['current_interval']}s)"
            )
            return result

    last_parsed: dict = {}
    for attempt in range(1, max_retries + 1):
        try:
            resp = _http_get_corner(sport_key, eid, CORNER_MARKETS_FULL)
            if resp.status_code == 422:
                logger.warning(f"[OddsAPI] Corner FULL 422 for {eid}, retrying main-only")
                resp = _http_get_corner(sport_key, eid, CORNER_MARKETS_MAIN_ONLY)
            if resp.status_code == 429:
                _pin_gate_register_error("429 rate limit")
                logger.warning(f"[OddsAPI] Corner 429 for {eid} attempt {attempt}/{max_retries}")
                time.sleep(0.6)
                continue
            resp.raise_for_status()
            _update_quota(resp)
            data = resp.json()

            # === RAW DEBUG DUMP ===
            # Print everything the API returned for this corner request so we
            # can definitively diagnose the "Chưa có kèo" issue.
            import json as _json
            try:
                home_dbg = data.get("home_team", "?")
                away_dbg = data.get("away_team", "?")
                bks_dbg = data.get("bookmakers", []) or []
                logger.warning(
                    f"[RAW-CORNER] {eid} {home_dbg} vs {away_dbg} | "
                    f"status={resp.status_code} | bookmakers_count={len(bks_dbg)} | "
                    f"bookmaker_keys={[b.get('key') for b in bks_dbg]}"
                )
                for b in bks_dbg:
                    bk_key = b.get("key", "?")
                    bk_title = b.get("title", "?")
                    mkts = b.get("markets", []) or []
                    mkt_summary = [
                        f"{m.get('key')}({len(m.get('outcomes', []))})"
                        for m in mkts
                    ]
                    logger.warning(
                        f"[RAW-CORNER] {eid} bk={bk_key} ({bk_title}) markets={mkt_summary}"
                    )
                    # FULL outcome dump for Pinnacle only — that's what we care about.
                    if bk_key == "pinnacle":
                        for m in mkts:
                            mk = m.get("key", "?")
                            outcomes = m.get("outcomes", []) or []
                            for o in outcomes:
                                logger.warning(
                                    f"[RAW-CORNER-PIN] {eid} {mk}: "
                                    f"name={o.get('name')!r} point={o.get('point')} price={o.get('price')}"
                                )
                if not bks_dbg:
                    raw_blob = _json.dumps(data, ensure_ascii=False)[:800]
                    logger.warning(f"[RAW-CORNER] {eid} EMPTY bookmakers. Raw: {raw_blob}")
            except Exception as _dbg_exc:
                logger.warning(f"[RAW-CORNER] dump failed for {eid}: {_dbg_exc}")
            # === END RAW DEBUG DUMP ===

            parsed = _parse_corner_response(data, ev_hint)
            last_parsed = parsed

            has_data = bool(parsed.get("totals") or parsed.get("spreads"))
            if has_data:
                logger.info(
                    f"[OddsAPI] fetch_pinnacle_corners OK {eid} attempt {attempt}: "
                    f"totals={list(parsed['totals'].keys())} "
                    f"spreads={[p.get('home_point') for p in parsed['spreads']]}"
                )
                _corner_cache[cache_key] = (time.time(), parsed)
                return parsed

            # Empty: no Pinnacle main lines parsed. Log + retry.
            logger.warning(
                f"[OddsAPI] fetch_pinnacle_corners EMPTY {eid} attempt {attempt}/{max_retries}: "
                f"had_pinnacle={parsed['had_pinnacle']} had_main={parsed['had_main_market']} "
                f"alt_lines={parsed['alt_only_lines']}"
            )
            time.sleep(0.5)
        except Exception as exc:
            _pin_gate_register_error(f"exception: {type(exc).__name__}")
            logger.warning(
                f"[OddsAPI] fetch_pinnacle_corners ERROR {eid} attempt {attempt}/{max_retries}: {exc}"
            )
            time.sleep(0.6)

    # All retries exhausted. Cache empty briefly so we don't hammer API.
    _corner_cache[cache_key] = (time.time(), last_parsed)
    logger.warning(
        f"[OddsAPI] fetch_pinnacle_corners GAVE UP {eid} after {max_retries} attempts"
    )
    return last_parsed


def get_corner_odds(league_code: str, event_ids: list[str] | None = None) -> dict:
    """
    Fetch corner odds for all events in a league.
    Thin wrapper that delegates per-event work to `fetch_pinnacle_corners`.
    Returns: {"home__away": {"totals": {line: {...}}, "spreads": [{...}]}}
    """
    sport_key = ODDS_SPORTS.get(league_code)
    if not sport_key:
        return {}

    try:
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

        result: dict = {}
        for ev in events:
            eid = ev.get("id", "")
            if not eid:
                continue
            parsed = fetch_pinnacle_corners(sport_key, eid, ev_hint=ev)
            if not parsed:
                continue
            home_team = parsed.get("home_team") or ev.get("home_team", "")
            away_team = parsed.get("away_team") or ev.get("away_team", "")
            key = f"{home_team}__{away_team}"
            match_data = {}
            if parsed.get("totals"):
                match_data["totals"] = parsed["totals"]
            if parsed.get("spreads"):
                match_data["spreads"] = parsed["spreads"]
            if match_data:
                result[key] = match_data

        logger.info(
            f"[OddsAPI] Corner odds: {len(result)}/{len(events)} events for {league_code} "
            f"(gate={_pin_gate['current_interval']}s)"
        )
        return result

    except Exception as e:
        _pin_gate_register_error(f"top-level: {type(e).__name__}")
        logger.error(f"[OddsAPI] Corner odds failed for {league_code}: {e}")
        return {}


def _build_corner_best(corners_list: list, target_line: float = 9.5) -> dict:
    """
    STRICT Pinnacle-only corner totals.
    Rule: take Pinnacle's MAIN market line in API order (FIRST entry).
    No "most balanced" picking. No alternate fallback for line selection —
    if Pinnacle main isn't there, return empty (caller shows "Chưa có kèo").
    """
    pinnacle_main = {}   # insertion order = API order
    pinnacle_alt = {}
    for c in corners_list:
        if c.get("bk_key") != "pinnacle" and str(c.get("bk", "")).lower() != "pinnacle":
            continue
        point = c.get("point")
        if point is None:
            continue
        bucket = pinnacle_main if c.get("is_main") else pinnacle_alt
        if point not in bucket:
            bucket[point] = {"over": None, "under": None}
        if c["name"] == "Over":
            bucket[point]["over"] = c["price"]
        elif c["name"] == "Under":
            bucket[point]["under"] = c["price"]

    # Verbose dump — every Pinnacle line we received, in API order.
    logger.info(f"[OddsAPI] _build_corner_best: Pinnacle MAIN lines (API order): {list(pinnacle_main.keys())}")
    logger.info(f"[OddsAPI] _build_corner_best: Pinnacle ALT  lines (API order): {list(pinnacle_alt.keys())}")

    main_complete = {l: d for l, d in pinnacle_main.items() if d["over"] and d["under"]}

    if main_complete:
        # FIRST entry by insertion order = first line API returned for the main market.
        main_line = next(iter(main_complete))
        d = main_complete[main_line]
        logger.info(f"[OddsAPI] _build_corner_best: PICKED Pinnacle MAIN line = {main_line} (over={d['over']}, under={d['under']})")
        return {
            main_line: {
                "over_price": d["over"],
                "over_bk": "Pinnacle",
                "under_price": d["under"],
                "under_bk": "Pinnacle",
            }
        }

    # No main from Pinnacle — refuse to guess from alternates. Caller shows "Chưa có kèo".
    all_books = sorted({str(c.get("bk", "?")) for c in corners_list})
    logger.warning(
        f"[OddsAPI] _build_corner_best: Pinnacle has NO MAIN line. "
        f"alt_lines={list(pinnacle_alt.keys())}, books_seen={all_books}. "
        f"Returning empty (no fallback to alternates)."
    )
    return {}


def _build_corner_spreads(spreads_list: list) -> list:
    """
    STRICT Pinnacle-only corner Asian Handicap.
    Rule: take Pinnacle's MAIN spreads_corners market in API order (FIRST pair).
    No "most balanced" picking. No alternate fallback — return [] if main absent.
    """
    pinnacle_main = [
        s for s in spreads_list
        if (s.get("bk_key") == "pinnacle" or str(s.get("bk", "")).lower() == "pinnacle")
        and s.get("is_main") is True
    ]
    pinnacle_alt = [
        s for s in spreads_list
        if (s.get("bk_key") == "pinnacle" or str(s.get("bk", "")).lower() == "pinnacle")
        and s.get("is_main") is not True
    ]

    logger.info(
        f"[OddsAPI] _build_corner_spreads: Pinnacle MAIN outcomes = "
        f"{[(o.get('name'), o.get('point'), o.get('price')) for o in pinnacle_main]}"
    )
    logger.info(
        f"[OddsAPI] _build_corner_spreads: Pinnacle ALT count = {len(pinnacle_alt)}"
    )

    if not pinnacle_main and not pinnacle_alt:
        all_books = sorted({str(s.get("bk", "?")) for s in spreads_list})
        logger.warning(f"[OddsAPI] _build_corner_spreads: NO Pinnacle. Total entries: {len(spreads_list)}, books: {all_books}")
        return []

    def _pair_outcomes_first(outcomes: list) -> list:
        """Pair outcomes preserving API order. Return FIRST complete pair only."""
        seen = set()
        for i, o1 in enumerate(outcomes):
            if i in seen:
                continue
            for j, o2 in enumerate(outcomes):
                if j in seen or j == i:
                    continue
                if o1["point"] is None or o2["point"] is None:
                    continue
                if abs(o1["point"] + o2["point"]) >= 0.01:
                    continue
                if o1["name"].strip().lower() == o2["name"].strip().lower():
                    continue
                if o1["point"] < o2["point"]:
                    home, away = o1, o2
                else:
                    home, away = o2, o1
                return [{
                    "home_name": home["name"],
                    "away_name": away["name"],
                    "home_point": home["point"],
                    "away_point": away["point"],
                    "home_price": home["price"],
                    "away_price": away["price"],
                    "bk": "Pinnacle",
                }]
        return []

    if pinnacle_main:
        main_pair = _pair_outcomes_first(pinnacle_main)
        if main_pair:
            p = main_pair[0]
            logger.info(
                f"[OddsAPI] _build_corner_spreads: PICKED Pinnacle MAIN line = "
                f"home {p['home_point']} ({p['home_price']}) / away {p['away_point']} ({p['away_price']})"
            )
            return main_pair
        logger.warning(f"[OddsAPI] _build_corner_spreads: Pinnacle MAIN present but no complete pair")

    logger.warning(
        f"[OddsAPI] _build_corner_spreads: Pinnacle has NO MAIN pair. "
        f"alt_count={len(pinnacle_alt)}. Returning empty (no fallback to alternates)."
    )
    return []


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
    STRICT Pinnacle-only Asian Handicap pair.
    Returns single-element list with Pinnacle's listed line, or empty.
    """
    pairs = []
    for bk_key, bk_data in event.get("bookmakers", {}).items():
        if bk_key != "pinnacle":
            continue
        spreads = bk_data.get("markets", {}).get("spreads", {})
        if len(spreads) < 2:
            continue

        outcomes = list(spreads.values())
        names = list(spreads.keys())
        home_info = outcomes[0]
        away_info = outcomes[1]

        if not isinstance(home_info, dict) or not isinstance(away_info, dict):
            continue
        if home_info.get("point") is None or away_info.get("point") is None:
            continue

        pairs.append({
            "home_name": names[0],
            "away_name": names[1],
            "home_point": home_info["point"],
            "away_point": away_info["point"],
            "home_price": home_info["price"],
            "away_price": away_info["price"],
            "bookmaker": bk_data["name"],
        })
        break
    return pairs
