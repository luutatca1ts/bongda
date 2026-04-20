"""Main analysis pipeline — collect data, predict, find value bets, alert."""

import logging
import unicodedata
import re
from datetime import datetime, date

import hashlib

from src.config import LEAGUES, ODDS_SPORTS, FOOTBALL_DATA_LEAGUES, LOW_CONFIDENCE_LEAGUES, USE_DIXON_COLES
from src.collectors.football_data import get_upcoming_matches, get_recent_results, get_xg_history
from src.collectors.odds_api import get_odds, get_best_odds, get_corner_odds
from src.models.poisson import PoissonModel, find_value_bets, get_confidence_tier
from src.models.dixon_coles import DixonColesModel

# Factory: swap model family via config flag without touching call sites.
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


def _fit_or_fallback(
    model: "PoissonModel | DixonColesModel | None",
    league_code: str,
    home: str,
    away: str,
    odds_event: dict,
    session,
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
        return model.predict(home, away), False

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
                return m2.predict(home, away), True
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
                    m = ModelClass()
                    # Optional xG — empty list today (stub); DC falls back to goals.
                    xg = get_xg_history(league_code, days=90) if USE_DIXON_COLES else []
                    m.fit(results, xg_data=xg) if USE_DIXON_COLES else m.fit(results)
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

                # Predict — fallback chain: fitted FD model → DB history → Pinnacle devig
                prediction, match_low_conf = _fit_or_fallback(
                    model, league_code, match["home_team"], match["away_team"], odds_event, session
                )
                is_low_confidence = league_low_conf or match_low_conf
                dynamic_min_ev = 0.05 if is_low_confidence else 0.01
                matches_analyzed += 1
                if is_low_confidence:
                    low_conf_matches += 1

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

                    # Save prediction
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
                        match, vb, prediction, bk_odds_comparison, steam_info=steam_info
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


def update_results() -> list[str]:
    """Check finished matches and update prediction results."""
    session = get_session()
    updated = []

    try:
        pending_preds = (
            session.query(Prediction)
            .filter(Prediction.is_value_bet == True, Prediction.result.is_(None))
            .all()
        )

        for pred in pending_preds:
            match = session.query(Match).filter(Match.match_id == pred.match_id).first()
            if not match or match.status != "FINISHED":
                # Try to update match result from API
                if match and match.home_goals is None:
                    continue
                if not match:
                    continue

            if match.home_goals is None:
                continue

            # Determine result
            total_goals = match.home_goals + match.away_goals

            if pred.market == "h2h":
                if pred.outcome == "Home":
                    pred.result = "WIN" if match.home_goals > match.away_goals else "LOSE"
                elif pred.outcome == "Draw":
                    pred.result = "WIN" if match.home_goals == match.away_goals else "LOSE"
                elif pred.outcome == "Away":
                    pred.result = "WIN" if match.away_goals > match.home_goals else "LOSE"

            elif pred.market == "totals":
                if "Over" in pred.outcome:
                    threshold = float(pred.outcome.split()[-1])
                    pred.result = "WIN" if total_goals > threshold else "LOSE"
                elif "Under" in pred.outcome:
                    threshold = float(pred.outcome.split()[-1])
                    pred.result = "WIN" if total_goals < threshold else "LOSE"

            elif pred.market == "btts":
                both_scored = match.home_goals > 0 and match.away_goals > 0
                if pred.outcome == "Yes":
                    pred.result = "WIN" if both_scored else "LOSE"
                elif pred.outcome == "No":
                    pred.result = "WIN" if not both_scored else "LOSE"

            if pred.result:
                updated.append(
                    f"{'✅' if pred.result == 'WIN' else '❌'} "
                    f"{match.home_team} vs {match.away_team} | "
                    f"{pred.outcome} @ {pred.best_odds} → {pred.result}"
                )

        session.commit()
    except Exception as e:
        logger.error(f"[Results] Error: {e}", exc_info=True)
        session.rollback()
    finally:
        session.close()

    return updated


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
