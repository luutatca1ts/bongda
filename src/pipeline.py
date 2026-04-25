"""Main analysis pipeline — collect data, predict, find value bets, alert."""

import json
import logging
import unicodedata
import re
from datetime import datetime, date, timedelta

import hashlib

from src.config import (
    LEAGUES, ODDS_SPORTS, FOOTBALL_DATA_LEAGUES, LOW_CONFIDENCE_LEAGUES,
    USE_DIXON_COLES, USE_BIVARIATE_POISSON, BIVARIATE_POISSON_LEAGUES,
    API_FOOTBALL_QUOTA_FLOOR, USE_MATCH_CONTEXT, USE_TEAM_MAPPING,
)
from src.collectors.football_data import get_upcoming_matches, get_recent_results, get_xg_history
from src.collectors.odds_api import get_odds, get_best_odds, get_corner_odds
from src.collectors.injuries import get_injuries_by_team
from src.collectors.weather import get_weather_forecast, get_venue_coords, is_weather_enabled
from src.collectors.api_football import get_af_quota
from src.analytics.injury_impact import summarize_injuries, count_key_players_out
from src.analytics.weather_impact import calculate_weather_adjustment
from src.analytics.match_context import classify_match, context_summary
from src.analytics.team_mapping import lookup_api_id
from src.models.poisson import PoissonModel, find_value_bets, get_confidence_tier
from src.models.dixon_coles import DixonColesModel
from src.models.bivariate_poisson import BivariatePoissonModel

# Factory: default model family when nothing league-specific applies.
# Per-league choice (BP vs DC) happens in _select_model().
ModelClass = DixonColesModel if USE_DIXON_COLES else PoissonModel
from src.db.models import get_session, Match, Prediction
from src.bot.formatters import format_value_bet_alert, format_daily_report
from src.analytics.line_movement import save_odds_snapshot
from src.analytics.steam_detector import detect_steam_moves


_ALIASES = {
    "internazionale": "inter",
    "atletico madrid": "atletico",
    "atletico de madrid": "atletico",
    "club atletico": "atletico",
    "borussia monchengladbach": "gladbach",
    "monchengladbach": "gladbach",
    "wolverhampton wanderers": "wolves",
    "tottenham hotspur": "tottenham",
    "manchester united": "man utd",
    "manchester city": "man city",
    "nottingham forest": "nott forest",
    "newcastle united": "newcastle",
    "west ham united": "west ham",
    "sheffield united": "sheffield utd",
    "paris saint germain": "psg",
    "paris saint-germain": "psg",
    "real sociedad de futbol": "real sociedad",
    "real betis balompie": "real betis",
    "rcd espanyol": "espanyol",
    "espanyol de barcelona": "espanyol",
    "real club deportivo espanyol": "espanyol",
    "athletic club": "athletic bilbao",
    "athletic club de bilbao": "athletic bilbao",
}


def _normalize(name: str) -> str:
    """Normalize team name for fuzzy matching: remove accents, FC/CF/SC, lowercase."""
    # Unicode normalize — remove accents (ö→o, é→e, etc.)
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    name = name.lower()
    # Remove common suffixes/prefixes
    for token in ["fc", "cf", "sc", "ac", "ss", "us", "as", "ssc", "1.", "1846", "1910", "1907", "de futbol", "calcio"]:
        name = name.replace(token, "")
    name = name.replace("-", " ")
    name = re.sub(r"[^a-z0-9 ]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    # Apply aliases
    for alias_from, alias_to in _ALIASES.items():
        if alias_from in name:
            name = alias_to
            break
    return name


def _match_teams(home1: str, away1: str, home2: str, away2: str) -> bool:
    """Check if two team pairs refer to the same match using fuzzy matching."""
    h1, a1 = _normalize(home1), _normalize(away1)
    h2, a2 = _normalize(home2), _normalize(away2)

    def _similar(a: str, b: str) -> bool:
        # One contains the other, or share a significant word
        if a in b or b in a:
            return True
        words_a = set(a.split())
        words_b = set(b.split())
        # Share at least one word with 4+ chars
        common = words_a & words_b
        return any(len(w) >= 4 for w in common)

    return _similar(h1, h2) and _similar(a1, a2)


def _match_event(db_home: str, db_away: str, db_utc, ev: dict, max_hours: float = 6.0) -> bool:
    """
    Match a DB match against an Odds API event.
    Requires BOTH team names AND kickoff time within `max_hours` to match.
    Prevents stale/wrong odds events being paired with the wrong fixture.
    """
    if not _match_teams(db_home, db_away, ev.get("home_team", ""), ev.get("away_team", "")):
        return False
    if db_utc is None:
        return True
    ev_ct = ev.get("commence_time")
    if not ev_ct:
        return True
    try:
        ev_dt = datetime.fromisoformat(ev_ct.replace("Z", "+00:00")).replace(tzinfo=None)
        m_dt = db_utc.replace(tzinfo=None) if getattr(db_utc, "tzinfo", None) else db_utc
        return abs((ev_dt - m_dt).total_seconds()) <= max_hours * 3600
    except Exception:
        return True

logger = logging.getLogger(__name__)


def _is_ev_suspicious(vb: dict) -> tuple[bool, str]:
    """Return (is_suspicious, reason).

    Why: EV quá cao thường là dấu hiệu model sai (xG thấp bất hợp lý) hoặc odds
    lỗi (line sai, stale). Threshold Pinnacle được nới sau khi chuyển sang
    Dixon-Coles (τ + time-decay → 1X2 chính xác hơn ~5-10% so với plain
    Poisson, đặc biệt giảm bias đánh giá thấp Draw). Corner và EV>25% giữ
    nguyên vì DC không trực tiếp cải thiện corner model.
    """
    ev = vb.get("ev", 0)
    bk = (vb.get("bookmaker") or "").lower()
    market = (vb.get("market") or "").lower()

    if ev > 0.25:
        return True, f"EV {ev*100:.1f}% quá cao (>25%), model/data có thể sai"
    if "pinnacle" in bk and ev > 0.15:
        return True, f"EV {ev*100:.1f}% trên Pinnacle (sharp book) bất thường"
    if "corner" in market and ev > 0.10:
        return True, f"EV {ev*100:.1f}% trên corner (model kém chính xác)"
    # Rule 4: giải data hạn chế (non Football-Data hoặc fallback implied prob).
    # Why: fallback dùng implied probability — EV >8% đồng nghĩa đang bắt
    # against odds của sharp book (không khả thi) → nhiều khả năng ảo.
    if vb.get("low_confidence_league") and ev > 0.08:
        return True, f"EV {ev*100:.1f}% trên giải nhỏ (data hạn chế) — model có thể ảo"
    return False, ""


def _synthetic_match_id(event_id: str) -> int:
    """Stable 31-bit int từ Odds API event_id (UUID) cho non-FD leagues.
    Why: Match.match_id là Integer → hash xuống range an toàn Integer(32-bit)."""
    h = hashlib.sha1(event_id.encode("utf-8")).hexdigest()
    return int(h[:8], 16) & 0x7FFFFFFF


def _synthesize_match_from_event(ev: dict, league_code: str) -> dict:
    """Tạo match dict từ Odds API event — dùng cho giải không có trong
    Football-Data (không có get_upcoming_matches endpoint)."""
    eid = ev.get("event_id") or f"{ev.get('home_team', '')}__{ev.get('away_team', '')}"
    return {
        "match_id": _synthetic_match_id(eid),
        "competition": LEAGUES.get(league_code, league_code),
        "competition_code": league_code,
        "home_team": ev.get("home_team", ""),
        "home_team_id": 0,
        "away_team": ev.get("away_team", ""),
        "away_team_id": 0,
        "utc_date": ev.get("commence_time", ""),
        "_synthetic": True,
    }


def _pinnacle_implied_h2h(odds_event: dict) -> dict | None:
    """Devig Pinnacle h2h odds → implied probabilities.

    Why: với giải không có historical data (non-FD), lấy Pinnacle prob
    làm proxy — Pinnacle là sharpest book nên implied prob gần true prob.
    So sánh với best odds của soft books → tìm value bet thực sự.

    Returns {Home, Draw, Away} normalized hoặc None nếu thiếu data.
    """
    pin = odds_event.get("bookmakers", {}).get("pinnacle")
    if not pin:
        return None
    h2h = pin.get("markets", {}).get("h2h") or {}
    probs = {}
    for outcome in ("Home", "Draw", "Away"):
        entry = h2h.get(outcome)
        if entry is None:
            continue
        price = entry if isinstance(entry, (int, float)) else entry.get("price", 0)
        if price and price > 1.01:
            probs[outcome] = 1.0 / price
    if len(probs) < 2:
        return None
    total = sum(probs.values())
    if total <= 0:
        return None
    return {k: v / total for k, v in probs.items()}


def _predict_with_context(
    model, home: str, away: str,
    injury_data: dict | None = None,
    weather_data: dict | None = None,
    match_context: dict | None = None,
) -> dict:
    """Call model.predict() with injury+weather+match_context kwargs only when
    the model supports them (DC, BP). PoissonModel.predict() has no such
    parameters so we degrade gracefully.
    """
    if isinstance(model, (DixonColesModel, BivariatePoissonModel)):
        return model.predict(
            home, away,
            injury_data=injury_data,
            weather_data=weather_data,
            match_context=match_context,
        )
    return model.predict(home, away)


def _select_model(league_code: str, n_results: int):
    """Pick the best-fit model class for this league.

    BivariatePoisson only when:
      1. USE_BIVARIATE_POISSON flag is on, AND
      2. league is in BIVARIATE_POISSON_LEAGUES (top-5 European), AND
      3. ≥100 matches in the 90-day window (λ3 needs data to identify).
    Otherwise fall back to DC (or PoissonModel if DC flag is off).

    Why restrict: λ3 is a shared extra parameter. On small or noisy samples
    it collapses to ~0 and adds nothing while hurting optimizer stability.
    """
    if (
        USE_BIVARIATE_POISSON
        and league_code in BIVARIATE_POISSON_LEAGUES
        and n_results >= 100
    ):
        return BivariatePoissonModel
    return ModelClass


def _align_xg_to_results(results: list[dict], xg_history: list[dict]) -> tuple[list, float]:
    """Produce an xg_data list index-aligned with `results`.

    Matches by fuzzy home/away team name (reuses pipeline's _normalize) —
    dates are not strict-matched because FD and API-Football timestamps can
    differ by a few minutes. First name match wins; unmatched rows become
    None so DixonColesModel.fit() falls back to integer goals for them.

    Phase B2 (USE_TEAM_MAPPING):
      - "off":      name match only (legacy).
      - "log_only": also build an id-keyed bucket, compare per row, log
                    disagreements. Return value stays the name-match result.
      - "on":       prefer id match, fall back to name match.

    Returns (aligned_list, coverage_fraction).
    """
    if not xg_history:
        return [None] * len(results), 0.0

    # Name bucket (legacy)
    name_bucket: dict[tuple[str, str], dict] = {}
    for x in xg_history:
        h = _normalize(x.get("home_team", ""))
        a = _normalize(x.get("away_team", ""))
        if h and a:
            name_bucket[(h, a)] = x

    # Id bucket — only populated when team ids are present in xg_history
    id_bucket: dict[tuple[int, int], dict] = {}
    if USE_TEAM_MAPPING != "off":
        for x in xg_history:
            hi = x.get("home_team_id")
            ai = x.get("away_team_id")
            if hi and ai:
                id_bucket[(int(hi), int(ai))] = x

    def _name_match(r: dict) -> dict | None:
        rh = _normalize(r.get("home_team", ""))
        ra = _normalize(r.get("away_team", ""))
        hit = name_bucket.get((rh, ra))
        if hit:
            return hit
        for (xh, xa), xv in name_bucket.items():
            if (rh in xh or xh in rh) and (ra in xa or xa in ra):
                return xv
        return None

    def _id_match(r: dict) -> dict | None:
        if not id_bucket:
            return None
        hi = r.get("home_api_id") or lookup_api_id(r.get("home_team", ""))
        ai = r.get("away_api_id") or lookup_api_id(r.get("away_team", ""))
        if hi and ai:
            return id_bucket.get((int(hi), int(ai)))
        return None

    aligned: list = []
    matched = 0

    # log_only comparison counters (per-league summary at the end)
    n_both_match = n_only_name = n_only_id = n_disagree = 0

    for r in results:
        name_hit = _name_match(r)
        id_hit = _id_match(r) if USE_TEAM_MAPPING != "off" else None

        if USE_TEAM_MAPPING != "off":
            if name_hit and id_hit:
                # Compare by (fixture_id if both have it) or (xg values)
                same = (
                    name_hit.get("utc_date") == id_hit.get("utc_date")
                    and name_hit.get("home_xg") == id_hit.get("home_xg")
                    and name_hit.get("away_xg") == id_hit.get("away_xg")
                )
                if same:
                    n_both_match += 1
                else:
                    n_disagree += 1
                    logger.info(
                        "[MAPPING] %s vs %s: name/id disagree — "
                        "by_name=(%s,%s xg=%.2f/%.2f) by_id=(%s,%s xg=%.2f/%.2f)",
                        r.get("home_team"), r.get("away_team"),
                        name_hit.get("home_team"), name_hit.get("away_team"),
                        float(name_hit.get("home_xg") or 0),
                        float(name_hit.get("away_xg") or 0),
                        id_hit.get("home_team"), id_hit.get("away_team"),
                        float(id_hit.get("home_xg") or 0),
                        float(id_hit.get("away_xg") or 0),
                    )
            elif id_hit and not name_hit:
                n_only_id += 1
            elif name_hit and not id_hit:
                n_only_name += 1

        if USE_TEAM_MAPPING == "on":
            hit = id_hit or name_hit
        else:
            # "off" and "log_only" both return the legacy name-match result
            hit = name_hit

        if hit:
            aligned.append({"home_xg": hit["home_xg"], "away_xg": hit["away_xg"]})
            matched += 1
        else:
            aligned.append(None)

    if USE_TEAM_MAPPING != "off" and results:
        logger.info(
            "[MAPPING] xG align summary mode=%s: both=%d name_only=%d id_only=%d disagree=%d of %d",
            USE_TEAM_MAPPING, n_both_match, n_only_name, n_only_id, n_disagree, len(results),
        )

    coverage = matched / len(results) if results else 0.0
    return aligned, coverage


def _fetch_match_context(match: dict, is_fd: bool) -> tuple[dict | None, dict | None, dict | None]:
    """Fetch injury + weather context best-effort.

    Returns:
        injury_summary: {home: {attack_mult, defense_mult, key_out, names}, away: {...}}
                        or None if no fixture_id available / API disabled.
        weather_raw: raw OpenWeatherMap slice (temp, rain_mm_h, wind, condition) or None.
        weather_adj: {total_goals_adjust, description} or None.

    Robust to any single-source failure — returns None for the failed
    component but never raises, so the predict path always runs.
    """
    # --- INJURIES ---
    injury_summary = None
    fixture_id = match.get("fixture_id") or match.get("api_football_id")
    h_tid = match.get("home_team_af_id")
    a_tid = match.get("away_team_af_id")
    if fixture_id and h_tid and a_tid:
        try:
            raw = get_injuries_by_team(int(fixture_id), int(h_tid), int(a_tid))
            if raw.get("home") or raw.get("away"):
                injury_summary = summarize_injuries(raw)
                for bucket in ("home", "away"):
                    team_name = match["home_team"] if bucket == "home" else match["away_team"]
                    names = injury_summary[bucket].get("names", [])
                    if names:
                        logger.info(
                            "[Injuries] %s OUT/QUESTIONABLE: %s (key_out=%d)",
                            team_name, ", ".join(names[:5]),
                            injury_summary[bucket].get("key_out", 0),
                        )
        except Exception as e:
            logger.debug("[Injuries] skip %s vs %s: %s",
                         match.get("home_team"), match.get("away_team"), e)

    # --- WEATHER ---
    weather_raw = None
    weather_adj = None
    if is_weather_enabled():
        lat, lon = get_venue_coords(match.get("home_team", ""))
        if lat is not None and lon is not None:
            try:
                weather_raw = get_weather_forecast(lat, lon, match.get("utc_date"))
                if weather_raw:
                    weather_adj = calculate_weather_adjustment(weather_raw)
            except Exception as e:
                logger.debug("[Weather] skip %s: %s", match.get("home_team"), e)

    return injury_summary, weather_raw, weather_adj


def _fit_or_fallback(
    model: "PoissonModel | DixonColesModel | None",
    league_code: str,
    home: str,
    away: str,
    odds_event: dict,
    session,
    injury_data: dict | None = None,
    weather_data: dict | None = None,
    match_context: dict | None = None,
) -> tuple[dict, bool]:
    """Return (prediction, low_confidence).

    Order of preference:
    1. Fitted Poisson model (from FD historical) — high confidence
    2. DB historical ≥20 matches cùng league → fit Poisson on the fly (low conf)
    3. Devigged Pinnacle implied probability (h2h only) — low confidence
    4. Default prediction — low confidence

    low_confidence=True khiến pipeline dùng min_ev=0.05 và tag VB để
    _is_ev_suspicious có thể filter aggressively.
    """
    if model is not None and model._fitted:
        return _predict_with_context(
            model, home, away, injury_data, weather_data, match_context
        ), False

    try:
        hist = (
            session.query(Match)
            .filter(
                Match.competition_code == league_code,
                Match.status == "FINISHED",
                Match.home_goals.isnot(None),
            )
            .order_by(Match.utc_date.desc())
            .limit(300)
            .all()
        )
        if len(hist) >= 20:
            results = [
                {
                    "home_team": m.home_team,
                    "away_team": m.away_team,
                    "home_goals": m.home_goals,
                    "away_goals": m.away_goals,
                    "utc_date": m.utc_date.isoformat() if m.utc_date else None,
                }
                for m in hist
            ]
            m2 = ModelClass()
            m2.fit(results)
            if m2._fitted:
                return _predict_with_context(
                    m2, home, away, injury_data, weather_data, match_context
                ), True
    except Exception as e:
        logger.debug(f"[Pipeline] DB historical fit failed for {league_code}: {e}")

    impl = _pinnacle_implied_h2h(odds_event)
    if impl:
        return {
            "home_xg": None,
            "away_xg": None,
            "h2h": {k: round(v, 4) for k, v in impl.items()},
            "totals": {},
            "btts": {},
            "asian_handicap": {},
            "corners": {"lines": {}, "asian_handicap": {}},
            "corners_h1": {"lines": {}, "asian_handicap": {}},
        }, True

    return ModelClass()._default_prediction(), True


def run_analysis_pipeline() -> list[str]:
    """
    Full pipeline:
    1. Fetch recent results per league
    2. Fit Poisson model per league
    3. Fetch upcoming matches + odds
    4. Find value bets
    5. Save to DB
    6. Return formatted alert messages
    """
    alerts = []
    session = get_session()
    filtered_suspicious = 0   # vb bị chặn bởi _is_ev_suspicious
    leagues_processed = 0     # giải có odds_events
    matches_analyzed = 0      # trận có odds_event + prediction
    low_conf_matches = 0      # trận dùng fallback (implied prob / DB hist)

    # Reset per-league corner data — always fetch fresh from API
    run_analysis_pipeline._corner_per_league = {}

    try:
        for league_code, league_name in LEAGUES.items():
            if league_code not in ODDS_SPORTS:
                continue

            logger.info(f"[Pipeline] Processing {league_name}...")

            is_fd = league_code in FOOTBALL_DATA_LEAGUES
            league_low_conf = (league_code in LOW_CONFIDENCE_LEAGUES) or not is_fd

            # 1. Historical + model fit — only FD leagues have fixture API.
            # Non-FD leagues fall back to DB history or Pinnacle-implied per match.
            model: "PoissonModel | DixonColesModel | None" = None
            if is_fd:
                try:
                    results = get_recent_results(league_code, days=90)
                except Exception as e:
                    logger.error(f"[Pipeline] Failed to get results for {league_name}: {e}")
                    results = []

                if results:
                    # Per-league model selection: BP for top-5 with ≥100 matches,
                    # else DC (or Poisson if DC flag is off).
                    Chosen = _select_model(league_code, len(results))
                    m = Chosen()

                    # Optional xG — gated by API-Football quota floor.
                    xg_raw: list[dict] = []
                    if USE_DIXON_COLES:
                        af_q = get_af_quota().get("current")
                        if af_q is None or af_q >= API_FOOTBALL_QUOTA_FLOOR:
                            try:
                                xg_raw = get_xg_history(league_code, days=90)
                            except Exception as e:
                                logger.debug("[xG] fetch failed for %s: %s", league_code, e)
                                xg_raw = []
                        else:
                            logger.warning(
                                "[xG] skip fetch for %s — AF quota %s < floor %s",
                                league_code, af_q, API_FOOTBALL_QUOTA_FLOOR,
                            )
                    xg_aligned, xg_cov = _align_xg_to_results(results, xg_raw)
                    logger.info(
                        "[MODEL] %s: %s (%d matches, xG coverage %.0f%%)",
                        league_code, Chosen.__name__, len(results), xg_cov * 100,
                    )
                    if USE_DIXON_COLES and isinstance(m, DixonColesModel):
                        m.fit(results, xg_data=xg_aligned)
                    else:
                        m.fit(results)
                    if m._fitted:
                        model = m
                        for r in results:
                            existing = session.query(Match).filter(Match.match_id == r["match_id"]).first()
                            if not existing:
                                session.add(Match(
                                    match_id=r["match_id"],
                                    competition=r["competition"],
                                    competition_code=r.get("competition_code", league_code),
                                    home_team=r["home_team"],
                                    home_team_id=r["home_team_id"],
                                    away_team=r["away_team"],
                                    away_team_id=r["away_team_id"],
                                    home_goals=r["home_goals"],
                                    away_goals=r["away_goals"],
                                    utc_date=datetime.fromisoformat(r["utc_date"].replace("Z", "+00:00")),
                                    status="FINISHED",
                                ))
                    else:
                        logger.warning(f"[Pipeline] Model not fitted for {league_name}, will use fallback")

            # 2. Get odds (ALWAYS — needed for matching + fallback prediction)
            try:
                odds_events = get_odds(league_code)
            except Exception as e:
                logger.error(f"[Pipeline] Failed to get odds for {league_name}: {e}")
                odds_events = []

            if not odds_events:
                logger.info(f"[Pipeline] No odds for {league_name}, skipping")
                continue

            leagues_processed += 1

            try:
                save_odds_snapshot(odds_events)
            except Exception as e:
                logger.error(f"[Pipeline] save_odds_snapshot failed: {e}")

            # 3. Build upcoming list — FD fixtures API for big leagues,
            # synthetic-from-odds for everyone else.
            upcoming: list[dict] = []
            if is_fd:
                try:
                    upcoming = get_upcoming_matches(league_code, days=3)
                except Exception as e:
                    logger.error(f"[Pipeline] Failed to get upcoming for {league_name}: {e}")
                    upcoming = []
            if not upcoming:
                upcoming = [_synthesize_match_from_event(ev, league_code) for ev in odds_events]

            # 4. Analyze each upcoming match
            for match in upcoming:
                # Check if already predicted today
                existing_pred = (
                    session.query(Prediction)
                    .filter(
                        Prediction.match_id == match["match_id"],
                        Prediction.created_at >= datetime.combine(date.today(), datetime.min.time()),
                    )
                    .first()
                )
                if existing_pred:
                    continue

                # Save match to DB
                existing_match = session.query(Match).filter(Match.match_id == match["match_id"]).first()
                if not existing_match:
                    try:
                        utc_dt = datetime.fromisoformat(match["utc_date"].replace("Z", "+00:00"))
                    except Exception:
                        utc_dt = None
                    session.add(Match(
                        match_id=match["match_id"],
                        competition=match["competition"],
                        competition_code=match.get("competition_code", league_code),
                        home_team=match["home_team"],
                        home_team_id=match.get("home_team_id", 0),
                        away_team=match["away_team"],
                        away_team_id=match.get("away_team_id", 0),
                        utc_date=utc_dt,
                        status="SCHEDULED",
                    ))

                # Match with odds first — require team name AND kickoff time match
                try:
                    m_utc = datetime.fromisoformat(match["utc_date"].replace("Z", "+00:00")).replace(tzinfo=None)
                except Exception:
                    m_utc = None
                odds_event = None
                for ev in odds_events:
                    if _match_event(match["home_team"], match["away_team"], m_utc, ev):
                        odds_event = ev
                        break

                if not odds_event:
                    logger.info(f"[Pipeline] No odds found for {match['home_team']} vs {match['away_team']}")
                    continue

                # --- Injury + Weather context (best-effort, skip on any failure) ---
                # Returns ({injury_summary or None}, {weather_summary or None},
                #         {weather_adj_dict}, fixture_id).
                inj_summary, weather_raw, weather_adj = _fetch_match_context(
                    match, is_fd
                )

                # --- Special-match context (derby / cup final / knockout) ---
                # 3-mode gate via USE_MATCH_CONTEXT: "off" skips entirely,
                # "log_only" classifies + logs + saves to DB but does NOT feed
                # the model, "on" also adjusts λ.
                match_ctx: dict | None = None
                model_ctx: dict | None = None
                if USE_MATCH_CONTEXT != "off":
                    match_ctx = classify_match(
                        match["home_team"],
                        match["away_team"],
                        competition_code=match.get("competition_code", league_code),
                        stage=odds_event.get("stage"),
                    )
                    if match_ctx and any(
                        match_ctx.get(k) for k in (
                            "is_derby", "is_cup_final", "is_knockout",
                            "is_relegation_6pointer",
                        )
                    ):
                        logger.info(
                            "[SPECIAL] %s vs %s — %s (mode=%s)",
                            match["home_team"], match["away_team"],
                            context_summary(match_ctx), USE_MATCH_CONTEXT,
                        )
                    if USE_MATCH_CONTEXT == "on":
                        model_ctx = match_ctx

                # Serialize once for every Prediction() save in this match
                # (h2h + corners). match_ctx is truthy for every classified
                # match — even no-flag matches store their tournament_stage.
                match_ctx_json = json.dumps(match_ctx) if match_ctx else None

                # Predict — fallback chain: fitted FD model → DB history → Pinnacle devig
                prediction, match_low_conf = _fit_or_fallback(
                    model, league_code, match["home_team"], match["away_team"], odds_event, session,
                    injury_data=inj_summary, weather_data=weather_adj,
                    match_context=model_ctx,
                )
                is_low_confidence = league_low_conf or match_low_conf
                dynamic_min_ev = 0.05 if is_low_confidence else 0.01
                matches_analyzed += 1
                if is_low_confidence:
                    low_conf_matches += 1

                # Log key outs (≥3) and weather shift for observability
                if inj_summary:
                    h_key = inj_summary["home"].get("key_out", 0)
                    a_key = inj_summary["away"].get("key_out", 0)
                    if h_key >= 3 or a_key >= 3:
                        logger.info(
                            "[Injuries] %s vs %s — key_out home=%d away=%d",
                            match["home_team"], match["away_team"], h_key, a_key,
                        )
                if weather_adj and weather_adj.get("total_goals_adjust", 0) != 0:
                    logger.info(
                        "[Weather] %s vs %s — %s",
                        match["home_team"], match["away_team"],
                        weather_adj.get("description", ""),
                    )

                # Get best odds for each market
                best_h2h = get_best_odds(odds_event, "h2h")
                best_totals = get_best_odds(odds_event, "totals")
                best_spreads = get_best_odds(odds_event, "spreads")

                combined_odds = {"h2h": best_h2h, "totals": best_totals, "spreads": best_spreads}

                # Find value bets
                value_bets = find_value_bets(prediction, combined_odds, min_ev=dynamic_min_ev)

                for vb in value_bets:
                    confidence = get_confidence_tier(vb["ev"], vb["probability"])
                    if confidence == "SKIP":
                        continue

                    vb["confidence"] = confidence
                    vb["low_confidence_league"] = is_low_confidence

                    # Safety filter: chặn vb EV ảo (quá cao / sharp book / corner).
                    is_susp, susp_reason = _is_ev_suspicious(vb)
                    if is_susp:
                        filtered_suspicious += 1
                        logger.warning(
                            f"[Pipeline] FILTERED suspicious VB — "
                            f"{match['home_team']} vs {match['away_team']} | "
                            f"{vb['market']}:{vb['outcome']} @ {vb['odds']} "
                            f"(EV {vb['ev']*100:+.1f}%, bk={vb.get('bookmaker', 'N/A')}) "
                            f"— {susp_reason}"
                        )
                        continue

                    # Save prediction (with injury + weather + xG metadata)
                    pred = Prediction(
                        match_id=match["match_id"],
                        market=vb["market"],
                        outcome=vb["outcome"],
                        model_probability=vb["probability"],
                        best_odds=vb["odds"],
                        best_bookmaker=vb.get("bookmaker", "N/A"),
                        expected_value=vb["ev"],
                        confidence=confidence,
                        is_value_bet=True,
                        injury_impact_home=(inj_summary or {}).get("home", {}).get("offensive_drop", 0.0) if inj_summary else 0.0,
                        injury_impact_away=(inj_summary or {}).get("away", {}).get("offensive_drop", 0.0) if inj_summary else 0.0,
                        weather_adjust=(weather_adj or {}).get("total_goals_adjust", 0.0) if weather_adj else 0.0,
                        weather_description=(weather_adj or {}).get("description") if weather_adj else None,
                        home_xg_estimate=prediction.get("home_xg"),
                        away_xg_estimate=prediction.get("away_xg"),
                        match_context=match_ctx_json,
                    )
                    session.add(pred)

                    # Build bookmaker odds comparison for alert
                    bk_odds_comparison = _get_bookmaker_comparison(
                        odds_event, vb["market"], vb["outcome"]
                    )

                    # Check for steam move cùng hướng với value bet hiện tại.
                    # Value bet = bắt cửa có prob cao hơn odds implied → nếu odds
                    # cửa đó đang shortening (giảm) nghĩa là sharp cũng vào cùng → bullish signal.
                    steam_info = None
                    try:
                        steams = detect_steam_moves(match_id_filter=match["match_id"])
                        for s in steams:
                            if (
                                s["market"] == vb["market"]
                                and s["outcome"] == vb["outcome"]
                                and s["direction"] == "shortening"
                            ):
                                steam_info = s
                                break
                    except Exception as _e:
                        logger.debug(f"[Pipeline] steam filter failed: {_e}")

                    alert_msg = format_value_bet_alert(
                        match, vb, prediction, bk_odds_comparison, steam_info=steam_info,
                        injury_summary=inj_summary, weather_adj=weather_adj,
                        match_context=match_ctx,
                    )
                    alerts.append(alert_msg)

                # === CORNER VALUE BETS ===
                corners_pred = prediction.get("corners", {})
                corner_lines = corners_pred.get("lines", {})
                corner_ah_pred = corners_pred.get("asian_handicap", {})

                # Fetch corner odds fresh per league (no cache — always live data)
                if not hasattr(run_analysis_pipeline, '_corner_per_league'):
                    run_analysis_pipeline._corner_per_league = {}
                if league_code not in run_analysis_pipeline._corner_per_league:
                    try:
                        eids = [ev["event_id"] for ev in odds_events if ev.get("event_id")]
                        run_analysis_pipeline._corner_per_league[league_code] = get_corner_odds(league_code, event_ids=eids or None)
                    except Exception:
                        run_analysis_pipeline._corner_per_league[league_code] = {}
                league_corners = run_analysis_pipeline._corner_per_league[league_code]

                # Find corner data for this match
                corner_key = f"{match['home_team']}__{match['away_team']}"
                corner_data = league_corners.get(corner_key, {})
                if not corner_data:
                    for ck, cv in league_corners.items():
                        parts = ck.split("__")
                        if len(parts) == 2 and _match_teams(match['home_team'], match['away_team'], parts[0], parts[1]):
                            corner_data = cv
                            break

                corner_totals_odds = corner_data.get("totals", {})
                corner_spreads = corner_data.get("spreads", [])

                # Corner O/U value bets
                for line in [8.5, 9.5, 10.5, 11.5]:
                    cl = corner_lines.get(line, {})
                    co = corner_totals_odds.get(line, {})
                    o_prob = cl.get("over", 0)
                    u_prob = cl.get("under", 0)
                    if co.get("over_price") and o_prob > 0:
                        ev = o_prob * co["over_price"] - 1
                        if ev > 0.01:
                            conf = get_confidence_tier(ev, o_prob)
                            if conf != "SKIP":
                                _vb = {"ev": ev, "bookmaker": co.get("over_bk", "?"),
                                       "market": "corners_totals", "outcome": f"Over {line}", "odds": co["over_price"]}
                                _vb["low_confidence_league"] = is_low_confidence
                                _susp, _r = _is_ev_suspicious(_vb)
                                if _susp:
                                    filtered_suspicious += 1
                                    logger.warning(
                                        f"[Pipeline] FILTERED suspicious VB — "
                                        f"{match['home_team']} vs {match['away_team']} | "
                                        f"corners_totals:Over {line} @ {co['over_price']} "
                                        f"(EV {ev*100:+.1f}%, bk={co.get('over_bk', '?')}) — {_r}"
                                    )
                                else:
                                    session.add(Prediction(
                                        match_id=match["match_id"], market="corners_totals",
                                        outcome=f"Over {line}", model_probability=o_prob,
                                        best_odds=co["over_price"], best_bookmaker=co.get("over_bk", "?"),
                                        expected_value=ev, confidence=conf, is_value_bet=True,
                                        match_context=match_ctx_json,
                                    ))
                    if co.get("under_price") and u_prob > 0:
                        ev = u_prob * co["under_price"] - 1
                        if ev > 0.01:
                            conf = get_confidence_tier(ev, u_prob)
                            if conf != "SKIP":
                                _vb = {"ev": ev, "bookmaker": co.get("under_bk", "?"),
                                       "market": "corners_totals", "outcome": f"Under {line}", "odds": co["under_price"]}
                                _vb["low_confidence_league"] = is_low_confidence
                                _susp, _r = _is_ev_suspicious(_vb)
                                if _susp:
                                    filtered_suspicious += 1
                                    logger.warning(
                                        f"[Pipeline] FILTERED suspicious VB — "
                                        f"{match['home_team']} vs {match['away_team']} | "
                                        f"corners_totals:Under {line} @ {co['under_price']} "
                                        f"(EV {ev*100:+.1f}%, bk={co.get('under_bk', '?')}) — {_r}"
                                    )
                                else:
                                    session.add(Prediction(
                                        match_id=match["match_id"], market="corners_totals",
                                        outcome=f"Under {line}", model_probability=u_prob,
                                        best_odds=co["under_price"], best_bookmaker=co.get("under_bk", "?"),
                                        expected_value=ev, confidence=conf, is_value_bet=True,
                                        match_context=match_ctx_json,
                                    ))

                # Corner AH value bets — main line only (matches what bookmaker displays)
                if corner_spreads:
                    cs = corner_spreads[0]
                    hp = cs.get("home_point", 0)
                    ap = cs.get("away_point", 0)
                    mk = f"{hp:+g}" if hp != 0 else "0"
                    ah_p = corner_ah_pred.get(mk, {})
                    h_prob = ah_p.get("home", 0)
                    a_prob = ah_p.get("away", 0)
                    if h_prob > 0 and cs.get("home_price"):
                        ev = h_prob * cs["home_price"] - 1
                        if ev > 0.01:
                            conf = get_confidence_tier(ev, h_prob)
                            if conf != "SKIP":
                                _out = f"{cs.get('home_name', 'Home')} {hp:+g}"
                                _vb = {"ev": ev, "bookmaker": cs.get("bk", "?"),
                                       "market": "corners_spreads", "outcome": _out, "odds": cs["home_price"]}
                                _vb["low_confidence_league"] = is_low_confidence
                                _susp, _r = _is_ev_suspicious(_vb)
                                if _susp:
                                    filtered_suspicious += 1
                                    logger.warning(
                                        f"[Pipeline] FILTERED suspicious VB — "
                                        f"{match['home_team']} vs {match['away_team']} | "
                                        f"corners_spreads:{_out} @ {cs['home_price']} "
                                        f"(EV {ev*100:+.1f}%, bk={cs.get('bk', '?')}) — {_r}"
                                    )
                                else:
                                    session.add(Prediction(
                                        match_id=match["match_id"], market="corners_spreads",
                                        outcome=_out,
                                        model_probability=h_prob, best_odds=cs["home_price"],
                                        best_bookmaker=cs.get("bk", "?"), expected_value=ev,
                                        confidence=conf, is_value_bet=True,
                                        match_context=match_ctx_json,
                                    ))
                    if a_prob > 0 and cs.get("away_price"):
                        ev = a_prob * cs["away_price"] - 1
                        if ev > 0.01:
                            conf = get_confidence_tier(ev, a_prob)
                            if conf != "SKIP":
                                _out = f"{cs.get('away_name', 'Away')} {ap:+g}"
                                _vb = {"ev": ev, "bookmaker": cs.get("bk", "?"),
                                       "market": "corners_spreads", "outcome": _out, "odds": cs["away_price"]}
                                _vb["low_confidence_league"] = is_low_confidence
                                _susp, _r = _is_ev_suspicious(_vb)
                                if _susp:
                                    filtered_suspicious += 1
                                    logger.warning(
                                        f"[Pipeline] FILTERED suspicious VB — "
                                        f"{match['home_team']} vs {match['away_team']} | "
                                        f"corners_spreads:{_out} @ {cs['away_price']} "
                                        f"(EV {ev*100:+.1f}%, bk={cs.get('bk', '?')}) — {_r}"
                                    )
                                else:
                                    session.add(Prediction(
                                        match_id=match["match_id"], market="corners_spreads",
                                        outcome=_out,
                                        model_probability=a_prob, best_odds=cs["away_price"],
                                        best_bookmaker=cs.get("bk", "?"), expected_value=ev,
                                        confidence=conf, is_value_bet=True,
                                        match_context=match_ctx_json,
                                    ))

                # === FIRST HALF CORNER VALUE BETS ===
                h1c_pred = prediction.get("corners_h1", {})
                h1c_lines = h1c_pred.get("lines", {})
                h1c_ah_pred = h1c_pred.get("asian_handicap", {})

                h1c_totals_odds = corner_data.get("h1_totals", {})
                h1c_spreads = corner_data.get("h1_spreads", [])

                # H1 corner O/U value bets
                for line in [3.5, 4.5, 5.5, 6.5]:
                    cl = h1c_lines.get(line, {})
                    co = h1c_totals_odds.get(line, {})
                    o_prob = cl.get("over", 0)
                    u_prob = cl.get("under", 0)
                    if co.get("over_price") and o_prob > 0:
                        ev = o_prob * co["over_price"] - 1
                        if ev > 0.01:
                            conf = get_confidence_tier(ev, o_prob)
                            if conf != "SKIP":
                                _vb = {"ev": ev, "bookmaker": co.get("over_bk", "?"),
                                       "market": "corners_h1_totals", "outcome": f"Over {line}", "odds": co["over_price"]}
                                _vb["low_confidence_league"] = is_low_confidence
                                _susp, _r = _is_ev_suspicious(_vb)
                                if _susp:
                                    filtered_suspicious += 1
                                    logger.warning(
                                        f"[Pipeline] FILTERED suspicious VB — "
                                        f"{match['home_team']} vs {match['away_team']} | "
                                        f"corners_h1_totals:Over {line} @ {co['over_price']} "
                                        f"(EV {ev*100:+.1f}%, bk={co.get('over_bk', '?')}) — {_r}"
                                    )
                                else:
                                    session.add(Prediction(
                                        match_id=match["match_id"], market="corners_h1_totals",
                                        outcome=f"Over {line}", model_probability=o_prob,
                                        best_odds=co["over_price"], best_bookmaker=co.get("over_bk", "?"),
                                        expected_value=ev, confidence=conf, is_value_bet=True,
                                        match_context=match_ctx_json,
                                    ))
                    if co.get("under_price") and u_prob > 0:
                        ev = u_prob * co["under_price"] - 1
                        if ev > 0.01:
                            conf = get_confidence_tier(ev, u_prob)
                            if conf != "SKIP":
                                _vb = {"ev": ev, "bookmaker": co.get("under_bk", "?"),
                                       "market": "corners_h1_totals", "outcome": f"Under {line}", "odds": co["under_price"]}
                                _vb["low_confidence_league"] = is_low_confidence
                                _susp, _r = _is_ev_suspicious(_vb)
                                if _susp:
                                    filtered_suspicious += 1
                                    logger.warning(
                                        f"[Pipeline] FILTERED suspicious VB — "
                                        f"{match['home_team']} vs {match['away_team']} | "
                                        f"corners_h1_totals:Under {line} @ {co['under_price']} "
                                        f"(EV {ev*100:+.1f}%, bk={co.get('under_bk', '?')}) — {_r}"
                                    )
                                else:
                                    session.add(Prediction(
                                        match_id=match["match_id"], market="corners_h1_totals",
                                        outcome=f"Under {line}", model_probability=u_prob,
                                        best_odds=co["under_price"], best_bookmaker=co.get("under_bk", "?"),
                                        expected_value=ev, confidence=conf, is_value_bet=True,
                                        match_context=match_ctx_json,
                                    ))

                # H1 corner AH value bets — main line only
                if h1c_spreads:
                    cs = h1c_spreads[0]
                    hp = cs.get("home_point", 0)
                    ap = cs.get("away_point", 0)
                    mk = f"{hp:+g}" if hp != 0 else "0"
                    ah_p = h1c_ah_pred.get(mk, {})
                    h_prob = ah_p.get("home", 0)
                    a_prob = ah_p.get("away", 0)
                    if h_prob > 0 and cs.get("home_price"):
                        ev = h_prob * cs["home_price"] - 1
                        if ev > 0.01:
                            conf = get_confidence_tier(ev, h_prob)
                            if conf != "SKIP":
                                _out = f"{cs.get('home_name', 'Home')} {hp:+g}"
                                _vb = {"ev": ev, "bookmaker": cs.get("bk", "?"),
                                       "market": "corners_h1_spreads", "outcome": _out, "odds": cs["home_price"]}
                                _vb["low_confidence_league"] = is_low_confidence
                                _susp, _r = _is_ev_suspicious(_vb)
                                if _susp:
                                    filtered_suspicious += 1
                                    logger.warning(
                                        f"[Pipeline] FILTERED suspicious VB — "
                                        f"{match['home_team']} vs {match['away_team']} | "
                                        f"corners_h1_spreads:{_out} @ {cs['home_price']} "
                                        f"(EV {ev*100:+.1f}%, bk={cs.get('bk', '?')}) — {_r}"
                                    )
                                else:
                                    session.add(Prediction(
                                        match_id=match["match_id"], market="corners_h1_spreads",
                                        outcome=_out,
                                        model_probability=h_prob, best_odds=cs["home_price"],
                                        best_bookmaker=cs.get("bk", "?"), expected_value=ev,
                                        confidence=conf, is_value_bet=True,
                                        match_context=match_ctx_json,
                                    ))
                    if a_prob > 0 and cs.get("away_price"):
                        ev = a_prob * cs["away_price"] - 1
                        if ev > 0.01:
                            conf = get_confidence_tier(ev, a_prob)
                            if conf != "SKIP":
                                _out = f"{cs.get('away_name', 'Away')} {ap:+g}"
                                _vb = {"ev": ev, "bookmaker": cs.get("bk", "?"),
                                       "market": "corners_h1_spreads", "outcome": _out, "odds": cs["away_price"]}
                                _vb["low_confidence_league"] = is_low_confidence
                                _susp, _r = _is_ev_suspicious(_vb)
                                if _susp:
                                    filtered_suspicious += 1
                                    logger.warning(
                                        f"[Pipeline] FILTERED suspicious VB — "
                                        f"{match['home_team']} vs {match['away_team']} | "
                                        f"corners_h1_spreads:{_out} @ {cs['away_price']} "
                                        f"(EV {ev*100:+.1f}%, bk={cs.get('bk', '?')}) — {_r}"
                                    )
                                else:
                                    session.add(Prediction(
                                        match_id=match["match_id"], market="corners_h1_spreads",
                                        outcome=_out,
                                        model_probability=a_prob, best_odds=cs["away_price"],
                                        best_bookmaker=cs.get("bk", "?"), expected_value=ev,
                                        confidence=conf, is_value_bet=True,
                                        match_context=match_ctx_json,
                                    ))

            session.commit()
            logger.info(f"[Pipeline] {league_name} done. Found {len(alerts)} value bets total.")

    except Exception as e:
        logger.error(f"[Pipeline] Error: {e}", exc_info=True)
        session.rollback()
    finally:
        session.close()

    logger.info(
        f"[Pipeline] Cycle summary — leagues_processed={leagues_processed}, "
        f"matches_analyzed={matches_analyzed} (low_conf={low_conf_matches}), "
        f"alerts={len(alerts)}, filtered_suspicious={filtered_suspicious}"
    )
    return alerts


def _compute_pred_result(pred, home_goals: int, away_goals: int) -> str | None:
    """Compute WIN/LOSE/PUSH cho 1 prediction dựa trên kết quả thực tế.

    Handle markets: h2h, totals, btts, asian_handicap (spreads),
    corners_totals, corners_spreads, corners_h1_totals, corners_h1_spreads.

    Returns None nếu market hoặc outcome không parse được — caller increments
    unknown_market counter để monitoring.

    Lưu ý: corners_* markets KHÔNG resolve được vì DB không lưu corner counts
    của trận đấu — return None để skip chứ không gây result sai.
    """
    market = (pred.market or "").lower()
    outcome = (pred.outcome or "").strip()
    total_goals = home_goals + away_goals

    # 1X2 / Moneyline
    if market == "h2h":
        if outcome == "Home":
            return "WIN" if home_goals > away_goals else "LOSE"
        if outcome == "Draw":
            return "WIN" if home_goals == away_goals else "LOSE"
        if outcome == "Away":
            return "WIN" if away_goals > home_goals else "LOSE"
        return None

    # Goal totals (Over/Under X.X)
    if market == "totals":
        try:
            threshold = float(outcome.split()[-1])
        except (ValueError, IndexError):
            return None
        if "Over" in outcome:
            if total_goals > threshold:
                return "WIN"
            if total_goals < threshold:
                return "LOSE"
            return "PUSH"
        if "Under" in outcome:
            if total_goals < threshold:
                return "WIN"
            if total_goals > threshold:
                return "LOSE"
            return "PUSH"
        return None

    # Both Teams To Score
    if market == "btts":
        both_scored = home_goals > 0 and away_goals > 0
        if outcome == "Yes":
            return "WIN" if both_scored else "LOSE"
        if outcome == "No":
            return "WIN" if not both_scored else "LOSE"
        return None

    # Asian Handicap / Spreads
    if market in ("asian_handicap", "spreads"):
        return _resolve_asian_handicap(outcome, home_goals, away_goals, pred)

    # Corner markets — DB không lưu corner counts cho match → không resolve được
    if market.startswith("corners_"):
        return None

    return None


def _resolve_asian_handicap(outcome: str, home_goals: int, away_goals: int, pred) -> str | None:
    """Resolve Asian Handicap. Outcome dạng '<side> <handicap>' với side là tên
    team hoặc 'Home'/'Away', handicap là số float (có thể là 0.25 / 0.75 → quarter line).

    AH logic (handicap từ perspective của side):
      - side = 'home': adjusted_margin = (home_goals - away_goals) + handicap
      - side = 'away': adjusted_margin = (away_goals - home_goals) + handicap
      - adjusted_margin > 0 → WIN
      - adjusted_margin < 0 → LOSE
      - adjusted_margin == 0 → PUSH (refund)

    Quarter lines (.25, .75): split bet — half WIN/half PUSH or half LOSE/half PUSH.
    Để đơn giản, resolve sang WIN/LOSE/PUSH gần nhất theo majority (không tracking
    half-stake refund — TODO sau nếu cần).
    """
    parts = outcome.rsplit(" ", 1)
    if len(parts) != 2:
        return None
    side_str, hcap_str = parts[0].strip(), parts[1].strip()
    try:
        handicap = float(hcap_str)
    except ValueError:
        return None

    side_lower = side_str.lower()
    if side_lower == "home":
        is_home = True
    elif side_lower == "away":
        is_home = False
    else:
        from src.db.models import get_session, Match
        from src.bot.telegram_bot import _canonical_team_key
        s = get_session()
        try:
            m = s.query(Match).filter(Match.match_id == pred.match_id).first()
            if not m:
                return None
            ck_home = _canonical_team_key(m.home_team or "")
            ck_away = _canonical_team_key(m.away_team or "")
            ck_outcome = _canonical_team_key(side_str)
            if ck_outcome == ck_home or ck_outcome in ck_home or ck_home in ck_outcome:
                is_home = True
            elif ck_outcome == ck_away or ck_outcome in ck_away or ck_away in ck_outcome:
                is_home = False
            else:
                return None
        finally:
            s.close()

    base_margin = (home_goals - away_goals) if is_home else (away_goals - home_goals)
    adjusted = base_margin + handicap

    # Quarter line handling: 0.25 / 0.75 split between two adjacent lines
    frac = abs(handicap) - int(abs(handicap))
    if abs(frac - 0.25) < 0.01 or abs(frac - 0.75) < 0.01:
        if adjusted > 0.25:
            return "WIN"
        if adjusted < -0.25:
            return "LOSE"
        if adjusted >= 0:
            return "WIN"
        return "LOSE"

    # Whole or half lines (.0, .5)
    if adjusted > 0.01:
        return "WIN"
    if adjusted < -0.01:
        return "LOSE"
    return "PUSH"


def update_results() -> list[str]:
    """Pull recent results from API, match to DB Match rows by team name +
    kickoff time, flip Match.status, then resolve preds.

    NOTE: Football-Data API và DB dùng match_id KHÁC nhau (DB thường lấy
    từ Odds API), nên ta match qua (canonical_home, canonical_away,
    kickoff rounded to minute) thay vì match_id.
    """
    from src.collectors.football_data import get_recent_results
    from src.bot.telegram_bot import _canonical_team_key

    session = get_session()
    updated: list[str] = []

    try:
        pending_preds = (
            session.query(Prediction)
            .filter(Prediction.is_value_bet == True, Prediction.result.is_(None))  # noqa: E712
            .all()
        )
        if not pending_preds:
            return updated

        # ---------- PHASE 1: refresh Match rows from API (team-name match) ----------
        league_codes: set[str] = set()
        for pred in pending_preds:
            m = session.query(Match).filter(Match.match_id == pred.match_id).first()
            if m and m.competition_code:
                league_codes.add(m.competition_code)

        # Football-Data free tier chỉ support 13 league codes — filter để tránh 403/404 spam
        FD_FREE_TIER = {"PL", "BL1", "SA", "PD", "FL1", "DED", "PPL", "ELC", "CL", "EC", "WC", "BSA", "CLI"}
        invalid_codes = league_codes - FD_FREE_TIER
        if invalid_codes:
            logger.info(
                f"[update_results] skipping non-FD league codes: {sorted(invalid_codes)}"
            )
        league_codes = league_codes & FD_FREE_TIER

        if not league_codes:
            logger.warning("[update_results] no FD-supported leagues in pending preds — skip API pull")
            # vẫn chạy Phase 2 để handle case data đã có sẵn trong DB

        # Pull all API results, keyed by (home_canon, away_canon, kickoff_min)
        api_index: dict[tuple, dict] = {}
        for lc in league_codes:
            try:
                results = get_recent_results(lc, days=60) or []
                for r in results:
                    h = _canonical_team_key(r.get("home_team") or "")
                    a = _canonical_team_key(r.get("away_team") or "")
                    raw_dt = r.get("utc_date") or ""
                    try:
                        from datetime import datetime
                        dt = datetime.fromisoformat(raw_dt.replace("Z", "+00:00")).replace(tzinfo=None)
                        ko_min = dt.replace(second=0, microsecond=0).isoformat()
                    except Exception:
                        ko_min = ""
                    if h and a and ko_min:
                        api_index[(h, a, ko_min)] = r
                logger.info(
                    f"[update_results] {lc}: pulled {len(results)} results from API"
                )
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    f"[update_results] get_recent_results({lc}) failed: {e}"
                )

        # Walk DB Match rows for each league, look up by (home, away, ko_min)
        flipped = 0
        for lc in league_codes:
            db_matches = (
                session.query(Match)
                .filter(Match.competition_code == lc)
                .all()
            )
            for m in db_matches:
                h = _canonical_team_key(m.home_team or "")
                a = _canonical_team_key(m.away_team or "")
                ko_min = (
                    m.utc_date.replace(second=0, microsecond=0).isoformat()
                    if m.utc_date else ""
                )
                if not (h and a and ko_min):
                    continue
                r = api_index.get((h, a, ko_min))
                if not r:
                    continue
                changed = False
                if r.get("status") and m.status != r["status"]:
                    m.status = r["status"]
                    changed = True
                if r.get("home_goals") is not None and m.home_goals != r["home_goals"]:
                    m.home_goals = r["home_goals"]
                    changed = True
                if r.get("away_goals") is not None and m.away_goals != r["away_goals"]:
                    m.away_goals = r["away_goals"]
                    changed = True
                if changed:
                    flipped += 1
        if flipped:
            session.commit()
            logger.info(
                f"[update_results] phase 1: flipped {flipped} Match rows"
            )

        # ---------- PHASE 2: resolve pending preds ----------
        # pred_match có thể đã được flip FINISHED bởi Phase 1 hoặc từ trước.
        # Compute result cho mọi market: h2h, totals, btts, asian_handicap,
        # corners_totals, corners_spreads, corners_h1_totals, corners_h1_spreads.
        unresolved_no_match = 0
        unresolved_unfinished = 0
        unresolved_unknown_market = 0

        for pred in pending_preds:
            match = session.query(Match).filter(Match.match_id == pred.match_id).first()
            if not match:
                unresolved_no_match += 1
                continue
            if match.status != "FINISHED" or match.home_goals is None or match.away_goals is None:
                unresolved_unfinished += 1
                continue

            result = _compute_pred_result(pred, match.home_goals, match.away_goals)
            if result is None:
                unresolved_unknown_market += 1
                continue
            pred.result = result

            icon = "✅" if pred.result == "WIN" else ("↩️" if pred.result == "PUSH" else "❌")
            updated.append(
                f"{icon} {match.home_team} vs {match.away_team} "
                f"{pred.market}/{pred.outcome} → {pred.result}"
            )

        if updated:
            session.commit()
        logger.info(
            f"[update_results] phase 2: resolved {len(updated)}/{len(pending_preds)} "
            f"(no_match={unresolved_no_match}, not_finished_yet={unresolved_unfinished}, "
            f"unknown_market={unresolved_unknown_market})"
        )
    finally:
        session.close()

    return updated


def generate_eod_summary() -> str | None:
    """Generate end-of-day summary message for Telegram.

    Returns formatted string with W/L/Push/Pending counts, win rate, ROI
    for all preds whose Match kicked off today (UTC). Returns None if no
    preds for today.
    """
    from datetime import datetime, date, timedelta

    session = get_session()
    try:
        today = date.today()
        day_start = datetime(today.year, today.month, today.day)
        day_end = day_start + timedelta(days=1)

        rows = (
            session.query(Prediction, Match)
            .join(Match, Prediction.match_id == Match.match_id)
            .filter(
                Prediction.is_value_bet == True,  # noqa: E712
                Match.utc_date >= day_start,
                Match.utc_date < day_end,
            )
            .all()
        )

        if not rows:
            return None

        total = len(rows)
        wins = sum(1 for p, _ in rows if p.result == "WIN")
        losses = sum(1 for p, _ in rows if p.result == "LOSE")
        pushes = sum(1 for p, _ in rows if p.result == "PUSH")
        pending = sum(1 for p, _ in rows if p.result is None)

        decided = wins + losses
        win_rate = (wins / decided * 100) if decided > 0 else 0.0
        stake = wins + losses + pushes
        gross = sum((p.best_odds or 0) for p, _ in rows if p.result == "WIN")
        roi = ((gross - stake) / stake * 100) if stake > 0 else 0.0

        lines = [
            "📊 TỔNG KẾT HÔM NAY",
            "━━━━━━━━━━━━━━━━━",
            f"📅 {today.strftime('%d/%m/%Y')}",
            f"📌 Tổng kèo: {total}",
            f"✅ Thắng: {wins}",
            f"❌ Thua: {losses}",
        ]
        if pushes:
            lines.append(f"↩️ Push: {pushes}")
        if pending:
            lines.append(f"⏳ Chờ kết quả: {pending}")
        if decided:
            lines.append("")
            lines.append(f"🎯 Tỉ lệ thắng: {win_rate:.1f}% ({wins}W/{losses}L)")
            lines.append(f"💰 ROI: {roi:+.1f}%")
        return "\n".join(lines)
    finally:
        session.close()


def _compute_lesson_learned(session, day_start_utc, day_end_utc) -> str:
    """Produce a Vietnamese 'rút kinh nghiệm' section for settled predictions
    in [day_start_utc, day_end_utc). Returns "" on empty window or any error —
    the daily report must never crash over this optional section.

    Tags are computed from existing Prediction fields only (no NLG):
      * line_drift_ngược:   closing_odds drifted >5% from best_odds
      * CLV_âm:             clv < -2.0
      * chấn_thương_nặng:   injury_impact_home/away >= 0.2 goals
      * thời_tiết_xấu:      |weather_adjust| >= 0.2 goals

    Thresholds are intentionally conservative — false positives erode trust.
    """
    try:
        from collections import defaultdict

        settled = (
            session.query(Prediction)
            .filter(
                Prediction.created_at >= day_start_utc,
                Prediction.created_at < day_end_utc,
                Prediction.result.isnot(None),
                Prediction.result.in_(["WIN", "LOSE", "PUSH"]),
            )
            .all()
        )

        if not settled:
            return ""

        losses = [p for p in settled if p.result == "LOSE"]

        lines = []
        lines.append("")
        lines.append("━━━━━━━━━━━━━━━━━━━")
        lines.append("⚠️ RÚT KINH NGHIỆM HÔM NAY:")

        # --- 1. Auto-tag each LOSS with reasons ---
        loss_tags: dict[str, int] = {}
        for p in losses:
            if p.closing_odds and p.best_odds and p.best_odds > 0:
                drift = (p.closing_odds - p.best_odds) / p.best_odds
                if drift > 0.05:
                    loss_tags["line_drift_ngược"] = loss_tags.get("line_drift_ngược", 0) + 1

            if p.clv is not None and p.clv < -2.0:
                loss_tags["CLV_âm"] = loss_tags.get("CLV_âm", 0) + 1

            if (p.injury_impact_home or 0) >= 0.2 or (p.injury_impact_away or 0) >= 0.2:
                loss_tags["chấn_thương_nặng"] = loss_tags.get("chấn_thương_nặng", 0) + 1

            if abs(p.weather_adjust or 0) >= 0.2:
                loss_tags["thời_tiết_xấu"] = loss_tags.get("thời_tiết_xấu", 0) + 1

        # --- 2. Market-level breakdown ---
        market_stats: dict = defaultdict(lambda: {"win": 0, "lose": 0, "push": 0})
        for p in settled:
            if p.result == "WIN":
                market_stats[p.market]["win"] += 1
            elif p.result == "LOSE":
                market_stats[p.market]["lose"] += 1
            elif p.result == "PUSH":
                market_stats[p.market]["push"] += 1

        bad_markets = []
        good_markets = []
        for market, st in market_stats.items():
            total = st["win"] + st["lose"]
            if total < 2:
                continue
            loss_rate = st["lose"] / total
            win_rate = st["win"] / total
            if st["lose"] >= 2 and loss_rate >= 0.6:
                bad_markets.append((market, st["win"], st["lose"], loss_rate))
            if st["win"] >= 3 and win_rate >= 0.7:
                good_markets.append((market, st["win"], st["lose"], win_rate))

        # --- 3. Confidence breakdown ---
        conf_stats: dict = defaultdict(lambda: {"win": 0, "lose": 0})
        for p in settled:
            if p.result == "WIN":
                conf_stats[p.confidence]["win"] += 1
            elif p.result == "LOSE":
                conf_stats[p.confidence]["lose"] += 1

        # --- 4. Compose output ---
        if loss_tags:
            lines.append("")
            lines.append("🔍 Lý do thua phổ biến:")
            top_tags = sorted(loss_tags.items(), key=lambda x: -x[1])[:3]
            for tag, count in top_tags:
                pct = count / len(losses) * 100 if losses else 0
                lines.append(f"   • {tag}: {count}/{len(losses)} kèo thua ({pct:.0f}%)")

        if bad_markets:
            lines.append("")
            lines.append("📉 Market cần thận trọng:")
            for market, w, l, lr in sorted(bad_markets, key=lambda x: -x[3])[:3]:
                lines.append(f"   • {market}: {w}W / {l}L (thua {lr*100:.0f}%)")

        if good_markets:
            lines.append("")
            lines.append("📈 Market đang tốt:")
            for market, w, l, wr in sorted(good_markets, key=lambda x: -x[3])[:3]:
                lines.append(f"   • {market}: {w}W / {l}L (thắng {wr*100:.0f}%)")

        conf_line_parts = []
        for conf_level in ["HIGH", "MEDIUM", "LOW"]:
            st = conf_stats.get(conf_level, {"win": 0, "lose": 0})
            total = st["win"] + st["lose"]
            if total >= 2:
                wr = st["win"] / total * 100
                conf_line_parts.append(f"{conf_level} {wr:.0f}%")
        if conf_line_parts:
            lines.append("")
            lines.append(f"🎯 Theo confidence: {' | '.join(conf_line_parts)}")

        # --- 5. Suggestion for tomorrow (conservative) ---
        suggestions = []
        if bad_markets:
            worst = sorted(bad_markets, key=lambda x: -x[3])[0]
            suggestions.append(f"Cẩn trọng với {worst[0]} (track record xấu hôm nay)")
        if good_markets:
            best = sorted(good_markets, key=lambda x: -x[3])[0]
            suggestions.append(f"Market {best[0]} đang có form tốt — ưu tiên cân nhắc")

        if suggestions:
            lines.append("")
            lines.append("📌 GỢI Ý MAI:")
            for sug in suggestions[:3]:
                lines.append(f"   • {sug}")

        # Header alone (no substantive content) → skip the section entirely.
        if len(lines) <= 3:
            return ""

        return "\n".join(lines)

    except Exception as e:  # noqa: BLE001
        logger.debug(f"[daily_report] lesson learned compute failed: {e}")
        return ""


def generate_daily_report() -> str:
    """Generate daily performance report."""
    session = get_session()
    try:
        today = date.today().isoformat()
        preds = (
            session.query(Prediction)
            .filter(Prediction.is_value_bet == True, Prediction.created_at >= today)
            .all()
        )

        report = {
            "date": today,
            "total_picks": len(preds),
            "correct": len([p for p in preds if p.result == "WIN"]),
            "wrong": len([p for p in preds if p.result == "LOSE"]),
            "pending": len([p for p in preds if p.result is None]),
            "high_correct": len([p for p in preds if p.confidence == "HIGH" and p.result == "WIN"]),
            "high_total": len([p for p in preds if p.confidence == "HIGH" and p.result in ("WIN", "LOSE")]),
            "medium_correct": len([p for p in preds if p.confidence == "MEDIUM" and p.result == "WIN"]),
            "medium_total": len([p for p in preds if p.confidence == "MEDIUM" and p.result in ("WIN", "LOSE")]),
            "low_correct": len([p for p in preds if p.confidence == "LOW" and p.result == "WIN"]),
            "low_total": len([p for p in preds if p.confidence == "LOW" and p.result in ("WIN", "LOSE")]),
        }

        msg = format_daily_report(report)

        # Append CLV summary (7 ngày)
        try:
            from src.analytics.clv import get_clv_stats, format_clv_report
            clv_stats = get_clv_stats(days=7)
            msg += "\n\n" + format_clv_report(clv_stats)
        except Exception as e:
            logger.warning(f"[Report] CLV append failed: {e}")

        # Append "Rút kinh nghiệm" section. Reuses the existing day boundary —
        # the stats block above compares `created_at >= today` (server-local
        # midnight); we bound the window at +1 day for a clean range on the
        # helper's side. Same timezone convention, no drift.
        try:
            day_start = datetime.combine(date.today(), datetime.min.time())
            day_end = day_start + timedelta(days=1)
            lesson_section = _compute_lesson_learned(session, day_start, day_end)
            if lesson_section:
                msg += lesson_section + "\n"
        except Exception as e:
            logger.debug(f"[daily_report] could not append lesson learned: {e}")

        return msg
    finally:
        session.close()


def _get_bookmaker_comparison(odds_event: dict, market: str, outcome: str) -> dict:
    """Get odds from all bookmakers for a specific outcome."""
    comparison = {}
    for bk_key, bk_data in odds_event.get("bookmakers", {}).items():
        mkt = bk_data.get("markets", {}).get(market, {})
        val = mkt.get(outcome)
        if val is not None:
            price = val if isinstance(val, (int, float)) else val.get("price", 0)
            comparison[bk_data["name"]] = price
    # Sort by odds descending
    return dict(sorted(comparison.items(), key=lambda x: x[1], reverse=True))
