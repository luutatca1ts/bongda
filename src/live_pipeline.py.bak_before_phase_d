"""Live (in-play) pipeline — phát hiện value bet real-time từ LivePoissonModel.

Chạy mỗi 2 phút trong cửa sổ giờ có nhiều trận live. Với mỗi trận live trong các
giải top, pipeline sẽ:

1. Match fixture_id (API-Football) với match_id trong DB qua fuzzy team-name.
2. Lấy state hiện tại (score, minute, xG, red cards) từ /fixtures/statistics.
3. Lưu snapshot vào LiveMatchState.
4. Dựng pregame λ bằng PoissonModel refit trên recent results của league đó.
5. Gọi LivePoissonModel.predict_at_state() → model probs.
6. Fetch live odds (h2h + totals) từ The Odds API.
7. Tính EV = prob × odds - 1; lọc EV ≥ LIVE_MIN_EV (5%).
8. Skip outcome nào vừa alert trong 10 phút (anti-spam).
9. Lưu LivePrediction + tạo message alert.

Quota protection (Part 6):
- Check Odds API remaining ≥ LIVE_QUOTA_MIN_THRESHOLD trước khi chạy.
- Cap số trận xử lý mỗi cycle = LIVE_MAX_MATCHES_PER_CYCLE.
- Ưu tiên trận có pregame value bet trong 24h gần nhất.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from src.config import (
    API_FOOTBALL_LEAGUES,
    FOOTBALL_DATA_LEAGUES,
    LIVE_MAX_MATCHES_PER_CYCLE,
    LIVE_QUOTA_MIN_THRESHOLD,
    ODDS_SPORTS,
)
from src.collectors.live_stats import get_all_live_matches, get_live_match_state
from src.collectors.football_data import get_recent_results
from src.collectors.odds_api import (
    get_live_odds,
    get_live_scores,
    get_quota,
)
from src.config import USE_DIXON_COLES
from src.models.poisson import PoissonModel
from src.models.dixon_coles import DixonColesModel
from src.models.live_poisson import LivePoissonModel

ModelClass = DixonColesModel if USE_DIXON_COLES else PoissonModel
from src.db.models import (
    LiveMatchState,
    LivePrediction,
    Match,
    Prediction,
    get_session,
)
from src.pipeline import _match_event, _match_teams
from src.bot.formatters import format_live_alert

logger = logging.getLogger(__name__)


# EV threshold cao hơn pregame (1%) vì live noise lớn hơn, risk cao hơn.
LIVE_MIN_EV = 0.05
# Không alert lại cùng match+market+outcome trong window này.
ALERT_COOLDOWN_MIN = 10


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _league_id_to_code() -> dict[int, str]:
    """Reverse map API-Football league_id → league_code."""
    return {lid: code for code, lid in API_FOOTBALL_LEAGUES.items()}


def _priority_match_ids(session, hours: int = 24) -> set[int]:
    """match_ids có pregame value bet alert trong N giờ gần nhất."""
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    rows = (
        session.query(Prediction.match_id)
        .filter(
            Prediction.is_value_bet.is_(True),
            Prediction.created_at >= cutoff,
        )
        .distinct()
        .all()
    )
    return {r[0] for r in rows if r[0]}


def _find_db_match(session, fixture: dict) -> Match | None:
    """Match fixture API-Football với row Match đang SCHEDULED hoặc mới live."""
    home = fixture.get("home", "")
    away = fixture.get("away", "")
    if not home or not away:
        return None

    # Ưu tiên trận trong 12h xung quanh now (live là hiện tại)
    now = datetime.utcnow()
    window_start = now - timedelta(hours=6)
    window_end = now + timedelta(hours=6)

    candidates = (
        session.query(Match)
        .filter(Match.utc_date >= window_start, Match.utc_date <= window_end)
        .all()
    )
    for m in candidates:
        if _match_teams(m.home_team, m.away_team, home, away):
            return m
    return None


def _build_pregame_lambdas(
    home_team: str,
    away_team: str,
    league_code: str,
    session=None,
) -> tuple[float, float, bool]:
    """Refit Poisson để lấy λ 90-phút. Return (h, a, low_conf).

    Fallback tier:
    1. Football-Data recent results (nếu league ∈ FOOTBALL_DATA_LEAGUES).
    2. DB history: Match rows FINISHED cùng competition_code (≥20 trận).
    3. Default (1.3, 1.1) + low_conf=True.
    """
    results = []
    if league_code in FOOTBALL_DATA_LEAGUES:
        try:
            results = get_recent_results(league_code, days=90)
        except Exception as e:
            logger.warning(f"[LivePipeline] get_recent_results failed for {league_code}: {e}")
            results = []

    if results:
        model = ModelClass()
        model.fit(results)
        if model._fitted:
            pred = model.predict(home_team, away_team)
            h = float(pred.get("home_xg", 1.3))
            a = float(pred.get("away_xg", 1.1))
            return (h, a, False)

    # Tier 2: DB history
    if session is not None:
        try:
            rows = (
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
            if len(rows) >= 20:
                hist = [
                    {
                        "home_team": r.home_team,
                        "away_team": r.away_team,
                        "home_goals": r.home_goals,
                        "away_goals": r.away_goals,
                        "utc_date": r.utc_date.isoformat() if r.utc_date else None,
                    }
                    for r in rows
                ]
                model = ModelClass()
                model.fit(hist)
                if model._fitted:
                    pred = model.predict(home_team, away_team)
                    h = float(pred.get("home_xg", 1.3))
                    a = float(pred.get("away_xg", 1.1))
                    return (h, a, True)
        except Exception as e:
            logger.warning(f"[LivePipeline] DB history fallback failed {league_code}: {e}")

    return (1.3, 1.1, True)


def _already_alerted(session, match_id: int, market: str, outcome: str) -> bool:
    """Đã có LivePrediction alert cùng match+market+outcome trong ALERT_COOLDOWN_MIN."""
    cutoff = datetime.utcnow() - timedelta(minutes=ALERT_COOLDOWN_MIN)
    row = (
        session.query(LivePrediction)
        .filter(
            LivePrediction.match_id == match_id,
            LivePrediction.market == market,
            LivePrediction.outcome == outcome,
            LivePrediction.alerted.is_(True),
            LivePrediction.created_at >= cutoff,
        )
        .first()
    )
    return row is not None


def _best_live_odds(odds_event: dict, market: str) -> dict:
    """Giống get_best_odds nhưng dùng được cho live — strict Pinnacle."""
    result = {}
    for bk_key, bk_data in odds_event.get("bookmakers", {}).items():
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


def _h2h_outcome_to_team(outcome_name: str, home: str, away: str) -> str | None:
    """Odds API h2h outcomes là tên team hoặc 'Draw'. Map → 'Home'/'Draw'/'Away'."""
    if outcome_name == "Draw":
        return "Draw"
    from src.pipeline import _normalize
    n = _normalize(outcome_name)
    if n == _normalize(home):
        return "Home"
    if n == _normalize(away):
        return "Away"
    return None


# ---------------------------------------------------------------------------
# Main entry
# ---------------------------------------------------------------------------


def run_live_pipeline() -> list[str]:
    """Tìm live value bet trong cycle hiện tại. Return list alert messages."""
    # Quota guard (Part 6)
    q = get_quota()
    remaining = q.get("remaining")
    if remaining is not None and remaining < LIVE_QUOTA_MIN_THRESHOLD:
        logger.warning(
            f"[LivePipeline] Skip — Odds API remaining={remaining} "
            f"< threshold {LIVE_QUOTA_MIN_THRESHOLD}"
        )
        return []

    fixtures = get_all_live_matches()
    if not fixtures:
        logger.info("[LivePipeline] No live fixtures in top leagues.")
        return []

    alerts: list[str] = []
    session = get_session()
    id_to_code = _league_id_to_code()

    try:
        priority = _priority_match_ids(session)

        # Sắp xếp: priority matches trước, rồi cap tại LIVE_MAX_MATCHES_PER_CYCLE.
        def _is_priority(fix):
            m = _find_db_match(session, fix)
            return 0 if (m and m.match_id in priority) else 1

        fixtures_sorted = sorted(fixtures, key=_is_priority)
        fixtures_capped = fixtures_sorted[:LIVE_MAX_MATCHES_PER_CYCLE]

        # Cache live odds per league_code để tránh gọi lặp.
        odds_cache: dict[str, list[dict]] = {}

        for fix in fixtures_capped:
            fixture_id = fix.get("fixture_id")
            league_id = fix.get("league_id")
            league_code = id_to_code.get(league_id)
            if not league_code or league_code not in ODDS_SPORTS:
                continue

            db_match = _find_db_match(session, fix)
            if not db_match:
                logger.info(
                    f"[LivePipeline] No DB match for {fix.get('home')} vs "
                    f"{fix.get('away')} (fixture {fixture_id})"
                )
                continue

            # b. Live state
            state = get_live_match_state(fixture_id)
            if not state:
                continue

            # Override minute/score từ fixture (nguồn tin cậy hơn stats)
            state["minute"] = fix.get("minute", state.get("minute", 0))
            state["home_score"] = fix.get("home_score", 0)
            state["away_score"] = fix.get("away_score", 0)

            # c. Save snapshot
            session.add(LiveMatchState(
                match_id=db_match.match_id,
                fixture_id=fixture_id,
                minute=int(state["minute"] or 0),
                home_score=int(state["home_score"] or 0),
                away_score=int(state["away_score"] or 0),
                home_red_cards=int(state.get("home_red_cards", 0) or 0),
                away_red_cards=int(state.get("away_red_cards", 0) or 0),
                home_xg=float(state.get("home_xg", 0.0) or 0.0),
                away_xg=float(state.get("away_xg", 0.0) or 0.0),
                home_shots_on_target=int(state.get("home_shots_on_target", 0) or 0),
                away_shots_on_target=int(state.get("away_shots_on_target", 0) or 0),
            ))

            # d. Pregame λ — fallback qua DB history nếu league không có FD data
            h_lambda, a_lambda, low_conf = _build_pregame_lambdas(
                db_match.home_team, db_match.away_team, league_code, session=session,
            )

            # e+f. Live Poisson predict
            live_model = LivePoissonModel(h_lambda, a_lambda)
            model_probs = live_model.predict_at_state({
                "minute": state["minute"],
                "home_score": state["home_score"],
                "away_score": state["away_score"],
                "home_xg": state.get("home_xg", 0.0),
                "away_xg": state.get("away_xg", 0.0),
                "home_red_cards": state.get("home_red_cards", 0),
                "away_red_cards": state.get("away_red_cards", 0),
            })

            # g. Live odds cho league này (cache per-league)
            if league_code not in odds_cache:
                try:
                    scores = get_live_scores(league_code)
                    live_ids = [s["event_id"] for s in scores]
                    odds_cache[league_code] = (
                        get_live_odds(league_code, live_ids) if live_ids else []
                    )
                except Exception as e:
                    logger.warning(f"[LivePipeline] live odds fetch failed {league_code}: {e}")
                    odds_cache[league_code] = []
            league_odds = odds_cache[league_code]

            # Match event với match hiện tại
            m_utc = db_match.utc_date
            odds_event = None
            for ev in league_odds:
                if _match_event(db_match.home_team, db_match.away_team, m_utc, ev):
                    odds_event = ev
                    break
            if not odds_event:
                continue

            # h. So model probs vs live odds → value bets
            # Low-conf leagues: bump min EV lên 0.10 để tránh noise cao từ model không fit tốt
            min_ev_effective = 0.10 if low_conf else LIVE_MIN_EV
            value_bets = _find_live_value_bets(
                model_probs, odds_event, db_match.home_team, db_match.away_team,
                min_ev=min_ev_effective,
            )

            # i. Save + alert
            for vb in value_bets:
                if _already_alerted(session, db_match.match_id, vb["market"], vb["outcome"]):
                    continue

                pred = LivePrediction(
                    match_id=db_match.match_id,
                    minute=int(state["minute"] or 0),
                    market=vb["market"],
                    outcome=vb["outcome"],
                    model_probability=vb["probability"],
                    live_odds=vb["odds"],
                    best_bookmaker=vb.get("bookmaker", "Pinnacle"),
                    expected_value=vb["ev"],
                    confidence=vb["confidence"],
                    is_value_bet=True,
                    alerted=True,
                )
                session.add(pred)

                match_dict = {
                    "home_team": db_match.home_team,
                    "away_team": db_match.away_team,
                    "competition": db_match.competition,
                    "match_id": db_match.match_id,
                }
                alerts.append(format_live_alert(match_dict, vb, state, model_probs))

        session.commit()
    except Exception as e:
        logger.error(f"[LivePipeline] Error: {e}", exc_info=True)
        session.rollback()
    finally:
        session.close()

    logger.info(f"[LivePipeline] Cycle done — {len(alerts)} live value bets.")
    return alerts


def _find_live_value_bets(
    model_probs: dict,
    odds_event: dict,
    home_team: str,
    away_team: str,
    min_ev: float = LIVE_MIN_EV,
) -> list[dict]:
    """So xác suất model với live odds (Pinnacle). Return list vb thỏa EV ≥ min_ev."""
    value_bets: list[dict] = []

    # --- h2h ---
    h2h_probs = model_probs.get("h2h", {})
    h2h_odds = _best_live_odds(odds_event, "h2h")
    for outcome_name, od in h2h_odds.items():
        mapped = _h2h_outcome_to_team(outcome_name, home_team, away_team)
        if not mapped:
            continue
        prob = h2h_probs.get(mapped, 0.0)
        price = od.get("price", 0.0) or 0.0
        if prob <= 0 or price <= 1.01:
            continue
        ev = prob * price - 1
        if ev >= min_ev:
            value_bets.append({
                "market": "h2h",
                "outcome": mapped,
                "probability": prob,
                "odds": price,
                "bookmaker": od.get("bookmaker", "Pinnacle"),
                "ev": ev,
                "confidence": _live_confidence(ev),
            })

    # --- totals ---
    totals_model = model_probs.get("totals", {})
    totals_odds = _best_live_odds(odds_event, "totals")
    for outcome_name, od in totals_odds.items():
        # Pinnacle trả "Over"/"Under" + point
        point = od.get("point")
        price = od.get("price", 0.0) or 0.0
        if point is None or price <= 1.01:
            continue
        line_probs = totals_model.get(float(point))
        if not line_probs:
            continue
        prob = line_probs.get(outcome_name, 0.0)
        if prob <= 0:
            continue
        ev = prob * price - 1
        if ev >= min_ev:
            value_bets.append({
                "market": "totals",
                "outcome": f"{outcome_name} {point}",
                "probability": prob,
                "odds": price,
                "bookmaker": od.get("bookmaker", "Pinnacle"),
                "ev": ev,
                "confidence": _live_confidence(ev),
            })

    return value_bets


def _live_confidence(ev: float) -> str:
    """Tier ngưỡng cao hơn pregame vì live volatile hơn."""
    if ev >= 0.12:
        return "HIGH"
    if ev >= 0.08:
        return "MEDIUM"
    return "LOW"
