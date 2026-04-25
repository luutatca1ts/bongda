"""Collector for API-Football (api-sports.io) — live match statistics."""

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests
from src.config import API_FOOTBALL_KEY, API_FOOTBALL_LEAGUES

logger = logging.getLogger(__name__)

BASE_URL = "https://v3.football.api-sports.io"
HEADERS = {"x-apisports-key": API_FOOTBALL_KEY}

_session = requests.Session()
_session.headers.update(HEADERS)

# Track API quota
_af_quota = {"current": None, "limit": None}


def get_af_quota() -> dict:
    """Return current API-Football quota status."""
    return dict(_af_quota)


def _update_af_quota(resp):
    """Update quota from API-Football response headers."""
    current = resp.headers.get("x-ratelimit-requests-remaining")
    limit = resp.headers.get("x-ratelimit-requests-limit")
    if current is not None:
        try:
            _af_quota["current"] = int(current)
        except ValueError:
            pass
    if limit is not None:
        try:
            _af_quota["limit"] = int(limit)
        except ValueError:
            pass


def get_live_fixtures(league_code: str = None) -> list[dict]:
    """
    Get all currently live fixtures, optionally filtered by league.
    Returns: [{fixture_id, home, away, home_score, away_score, minute, league}]
    """
    if not API_FOOTBALL_KEY:
        return []
    try:
        params = {"live": "all"}
        if league_code and league_code in API_FOOTBALL_LEAGUES:
            params = {"league": API_FOOTBALL_LEAGUES[league_code], "season": 2025, "live": "all"}

        resp = _session.get(f"{BASE_URL}/fixtures", params=params, timeout=20)
        resp.raise_for_status()
        _update_af_quota(resp)
        data = resp.json()

        results = []
        for fix in data.get("response", []):
            fixture = fix.get("fixture", {})
            teams = fix.get("teams", {})
            goals = fix.get("goals", {})
            league = fix.get("league", {})

            results.append({
                "fixture_id": fixture.get("id"),
                "home": teams.get("home", {}).get("name", ""),
                "away": teams.get("away", {}).get("name", ""),
                "home_score": goals.get("home", 0) or 0,
                "away_score": goals.get("away", 0) or 0,
                "minute": fixture.get("status", {}).get("elapsed", 0) or 0,
                "status": fixture.get("status", {}).get("short", ""),
                "league_name": league.get("name", ""),
                "league_id": league.get("id"),
            })

        # Filter to our supported leagues if not already filtered
        if not league_code:
            supported_ids = set(API_FOOTBALL_LEAGUES.values())
            results = [r for r in results if r["league_id"] in supported_ids]

        return results
    except Exception as e:
        logger.error(f"[API-Football] Live fixtures failed: {e}")
        return []


# ------------------------------------------------------------------
# Pre-match fixture_id resolver (Phase 2.1)
# ------------------------------------------------------------------
# In-memory cache keyed by (home_api_id, away_api_id, kickoff_date_iso).
# Both hits and misses are cached — negative caching prevents quota waste
# when the same /chot cycle re-checks a pick we already know is unresolvable.
_PREMATCH_FIXTURE_CACHE: dict[tuple[int, int, str], tuple[Optional[int], float]] = {}
_PREMATCH_CACHE_TTL_SEC = 3600  # 1 hour


def resolve_fixture_id_prematch(
    home_api_id: int,
    away_api_id: int,
    kickoff_utc: datetime,
    league_api_id: Optional[int] = None,
) -> Optional[int]:
    """Resolve API-Football fixture_id for a scheduled (pre-match) fixture.

    Queries `/fixtures?date=YYYY-MM-DD[&league=X]&season=Y` and matches on
    the home/away API-Football team IDs. Tries the kickoff's UTC date, then
    ±1 day to cover timezone-drift edge cases (a 23:00 UTC kickoff can be
    the "next day" in API-Football's local time).

    Season rolls over on 1 July: month ≥ 7 uses the current year, otherwise
    year - 1 (matches the typical Aug→May European season convention).
    Leagues with a calendar-year season may miss — acceptable; resolver
    returns None and caller falls back to skipping lineup/injury signals.

    Caching:
        Results (including None) are cached for 1 hour by
        (home_api_id, away_api_id, kickoff_date).

    Args:
        home_api_id: API-Football team ID (Match.home_api_id).
        away_api_id: API-Football team ID (Match.away_api_id).
        kickoff_utc: scheduled kickoff (Match.utc_date). Naive datetimes
            are assumed to already be UTC.
        league_api_id: API-Football league ID (Match.home_league_id).
            Optional but strongly recommended — narrows the query and
            saves quota.

    Returns:
        int fixture_id on match, None on miss / API error / missing key.
    """
    if not API_FOOTBALL_KEY:
        return None
    if not home_api_id or not away_api_id:
        return None

    # Normalise kickoff → UTC date.
    if kickoff_utc.tzinfo is None:
        kickoff_utc = kickoff_utc.replace(tzinfo=timezone.utc)
    kickoff_date = kickoff_utc.astimezone(timezone.utc).date()
    date_iso = kickoff_date.isoformat()

    cache_key = (int(home_api_id), int(away_api_id), date_iso)
    now = time.time()
    cached = _PREMATCH_FIXTURE_CACHE.get(cache_key)
    if cached is not None:
        fid, cached_at = cached
        if now - cached_at < _PREMATCH_CACHE_TTL_SEC:
            logger.debug(
                f"[prematch_resolver] cache HIT {cache_key} → {fid}"
            )
            return fid

    dates_to_try = [
        date_iso,
        (kickoff_date - timedelta(days=1)).isoformat(),
        (kickoff_date + timedelta(days=1)).isoformat(),
    ]
    # European convention: Aug-May spans 2 calendar years (Aug 2025-May 2026 = season 2025).
    # Calendar-year leagues (Brazilian, Sudamericana, Libertadores, MLS, Argentina):
    # season = year of kickoff. Try both to handle either case.
    season_european = kickoff_date.year if kickoff_date.month >= 7 else kickoff_date.year - 1
    season_calendar = kickoff_date.year
    seasons_to_try = [season_european]
    if season_calendar != season_european:
        seasons_to_try.append(season_calendar)

    found_fid: Optional[int] = None
    for season in seasons_to_try:
        if found_fid:
            break
        for d in dates_to_try:
            try:
                params: dict = {"date": d, "season": season}
                if league_api_id:
                    params["league"] = int(league_api_id)
                resp = _session.get(
                    f"{BASE_URL}/fixtures",
                    params=params,
                    timeout=20,
                )
                if resp.status_code != 200:
                    logger.warning(
                        f"[prematch_resolver] /fixtures HTTP {resp.status_code} "
                        f"for date={d} season={season} league={league_api_id}"
                    )
                    continue
                _update_af_quota(resp)
                data = resp.json()
                for item in data.get("response", []) or []:
                    teams = item.get("teams", {}) or {}
                    h_id = (teams.get("home") or {}).get("id")
                    a_id = (teams.get("away") or {}).get("id")
                    if h_id == int(home_api_id) and a_id == int(away_api_id):
                        fid = (item.get("fixture") or {}).get("id")
                        if fid:
                            found_fid = int(fid)
                            logger.info(
                                f"[prematch_resolver] HIT home={home_api_id} "
                                f"away={away_api_id} date={d} season={season} "
                                f"→ fixture_id={found_fid}"
                            )
                            break
                if found_fid:
                    break
            except Exception as e:  # noqa: BLE001
                logger.debug(
                    f"[prematch_resolver] error for date={d} season={season}: {e}"
                )
                continue

    if found_fid is None:
        logger.debug(
            f"[prematch_resolver] MISS home={home_api_id} "
            f"away={away_api_id} date={date_iso} league={league_api_id}"
        )

    _PREMATCH_FIXTURE_CACHE[cache_key] = (found_fid, now)
    return found_fid


def get_fixture_stats(fixture_id: int) -> dict:
    """
    Get detailed statistics for a live fixture.
    Returns: {
        home: {shots, shots_on, possession, passes, pass_accuracy,
               fouls, corners, offsides, saves, attacks, dangerous_attacks},
        away: {same fields}
    }
    """
    if not API_FOOTBALL_KEY or not fixture_id:
        return {}
    try:
        resp = _session.get(
            f"{BASE_URL}/fixtures/statistics",
            params={"fixture": fixture_id},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        stats = {}
        for team_data in data.get("response", []):
            team_name = team_data.get("team", {}).get("name", "")
            team_id = team_data.get("team", {}).get("id", 0)
            raw = {s["type"]: s["value"] for s in team_data.get("statistics", [])}

            parsed = {
                "team_name": team_name,
                "team_id": team_id,
                "shots": _int(raw.get("Total Shots")),
                "shots_on": _int(raw.get("Shots on Goal")),
                "shots_off": _int(raw.get("Shots off Goal")),
                "blocked": _int(raw.get("Blocked Shots")),
                "possession": raw.get("Ball Possession", "50%"),
                "passes": _int(raw.get("Total passes")),
                "pass_accuracy": raw.get("Passes accurate", "0"),
                "pass_pct": raw.get("Passes %", "0%"),
                "fouls": _int(raw.get("Fouls")),
                "corners": _int(raw.get("Corner Kicks")),
                "offsides": _int(raw.get("Offsides")),
                "saves": _int(raw.get("Goalkeeper Saves")),
                "yellow": _int(raw.get("Yellow Cards")),
                "red": _int(raw.get("Red Cards")),
                "shots_insidebox": _int(raw.get("Shots insidebox")),
                "shots_outsidebox": _int(raw.get("Shots outsidebox")),
                "expected_goals": raw.get("expected_goals", raw.get("Expected goals")),
                "goals_prevented": raw.get("goals_prevented"),
            }

            # Determine home/away by order (first = home)
            if "home" not in stats:
                stats["home"] = parsed
            else:
                stats["away"] = parsed

        return stats
    except Exception as e:
        logger.error(f"[API-Football] Stats for fixture {fixture_id} failed: {e}")
        return {}


def get_fixture_events(fixture_id: int) -> list[dict]:
    """
    Get match events/timeline: goals, cards, substitutions, VAR.
    Returns: [{minute, type, detail, team_name, player, assist}]
    """
    if not API_FOOTBALL_KEY or not fixture_id:
        return []
    try:
        resp = _session.get(
            f"{BASE_URL}/fixtures/events",
            params={"fixture": fixture_id},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        events = []
        for ev in data.get("response", []):
            time_info = ev.get("time", {})
            minute = (time_info.get("elapsed") or 0) + (time_info.get("extra") or 0)
            events.append({
                "minute": minute,
                "type": ev.get("type", ""),        # Goal, Card, subst, Var
                "detail": ev.get("detail", ""),     # Normal Goal, Yellow Card, Red Card, Substitution 1, etc.
                "team_name": ev.get("team", {}).get("name", ""),
                "team_id": ev.get("team", {}).get("id", 0),
                "player": ev.get("player", {}).get("name", ""),
                "assist": ev.get("assist", {}).get("name", ""),
            })
        events.sort(key=lambda x: x["minute"])
        return events
    except Exception as e:
        logger.error(f"[API-Football] Events for fixture {fixture_id} failed: {e}")
        return []


def parse_events(events: list[dict], home_team_id: int = 0) -> dict:
    """
    Parse events into structured data for analysis.
    Returns: {
        red_cards: [{minute, team, player, is_home}],
        substitutions: [{minute, team, player_in, player_out, is_home}],
        goals: [{minute, team, player, is_home}],
        corners_timeline: [{minute, is_home}],
        home_reds: int, away_reds: int,
        home_subs: int, away_subs: int,
        last_sub_minute: int,
        sub_intent: "attacking"/"defensive"/"neutral",
    }
    """
    result = {
        "red_cards": [],
        "substitutions": [],
        "goals": [],
        "home_reds": 0,
        "away_reds": 0,
        "home_subs": 0,
        "away_subs": 0,
        "last_sub_minute": 0,
        "sub_intent": "neutral",
    }

    recent_subs = []  # subs in last 5 minutes for intent detection

    for ev in events:
        is_home = ev.get("team_id") == home_team_id if home_team_id else True
        minute = ev["minute"]

        if ev["type"] == "Card" and "Red" in ev.get("detail", ""):
            result["red_cards"].append({
                "minute": minute, "team": ev["team_name"],
                "player": ev["player"], "is_home": is_home,
            })
            if is_home:
                result["home_reds"] += 1
            else:
                result["away_reds"] += 1

        elif ev["type"] == "subst":
            sub = {
                "minute": minute, "team": ev["team_name"],
                "player_in": ev.get("assist", ""),  # assist = player coming in
                "player_out": ev["player"],          # player = player going out
                "is_home": is_home,
            }
            result["substitutions"].append(sub)
            if is_home:
                result["home_subs"] += 1
            else:
                result["away_subs"] += 1
            result["last_sub_minute"] = max(result["last_sub_minute"], minute)
            recent_subs.append(sub)

        elif ev["type"] == "Goal":
            result["goals"].append({
                "minute": minute, "team": ev["team_name"],
                "player": ev["player"], "is_home": is_home,
            })

    # Detect substitution intent from trailing team
    # Multiple subs in short window by trailing team = attacking push
    if len(recent_subs) >= 2:
        last_3 = recent_subs[-3:] if len(recent_subs) >= 3 else recent_subs
        min_span = max(s["minute"] for s in last_3) - min(s["minute"] for s in last_3)
        if min_span <= 5 and len(last_3) >= 2:
            # Batch subs = tactical change
            home_batch = sum(1 for s in last_3 if s["is_home"])
            away_batch = sum(1 for s in last_3 if not s["is_home"])
            if home_batch >= 2 or away_batch >= 2:
                result["sub_intent"] = "attacking"  # aggressive change

    return result


def get_live_stats_batch(league_code: str = None) -> list[dict]:
    """
    Get live fixtures + stats + events in one flow.
    Returns: [{fixture_id, home, away, scores, minute, stats, events, parsed_events}]
    """
    fixtures = get_live_fixtures(league_code)
    if not fixtures:
        return []

    results = []
    for fix in fixtures:
        fid = fix["fixture_id"]
        stats = get_fixture_stats(fid) if fid else {}
        events = get_fixture_events(fid) if fid else []

        # Get home team_id for event parsing
        home_team_id = stats.get("home", {}).get("team_id", 0)
        parsed = parse_events(events, home_team_id)

        results.append({
            **fix,
            "stats": stats,
            "events": events,
            "parsed_events": parsed,
        })

    return results


def _int(val) -> int:
    """Safe convert to int."""
    if val is None:
        return 0
    try:
        return int(str(val).replace("%", "").strip())
    except (TypeError, ValueError):
        return 0
