"""Main analysis pipeline — collect data, predict, find value bets, alert."""

import logging
import unicodedata
import re
from datetime import datetime, date

from src.config import LEAGUES, ODDS_SPORTS, FOOTBALL_DATA_LEAGUES
from src.collectors.football_data import get_upcoming_matches, get_recent_results
from src.collectors.odds_api import get_odds, get_best_odds, get_corner_odds
from src.models.poisson import PoissonModel, find_value_bets, get_confidence_tier
from src.db.models import get_session, Match, Prediction
from src.bot.formatters import format_value_bet_alert, format_daily_report


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

logger = logging.getLogger(__name__)


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

    try:
        for league_code, league_name in LEAGUES.items():
            if league_code not in ODDS_SPORTS:
                continue

            logger.info(f"[Pipeline] Processing {league_name}...")

            # 1. Get historical results & fit model
            if league_code not in FOOTBALL_DATA_LEAGUES:
                logger.info(f"[Pipeline] {league_name} not on Football-Data.org free tier, skipping fixture fetch")
                # Still fetch odds-only analysis for non-FD leagues
                # (model won't be fitted, but odds data still useful)
                continue
            try:
                results = get_recent_results(league_code, days=90)
            except Exception as e:
                logger.error(f"[Pipeline] Failed to get results for {league_name}: {e}")
                continue

            model = PoissonModel()
            model.fit(results)

            if not model._fitted:
                logger.warning(f"[Pipeline] Model not fitted for {league_name}, skipping")
                continue

            # Save results to DB
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

            # 2. Get upcoming matches
            try:
                upcoming = get_upcoming_matches(league_code, days=3)
            except Exception as e:
                logger.error(f"[Pipeline] Failed to get upcoming for {league_name}: {e}")
                continue

            # 3. Get odds
            try:
                odds_events = get_odds(league_code)
            except Exception as e:
                logger.error(f"[Pipeline] Failed to get odds for {league_name}: {e}")
                odds_events = []

            # Build odds lookup by team names
            odds_lookup = {}
            for ev in odds_events:
                key = f"{ev['home_team']}_{ev['away_team']}".lower()
                odds_lookup[key] = ev

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
                    session.add(Match(
                        match_id=match["match_id"],
                        competition=match["competition"],
                        competition_code=match.get("competition_code", league_code),
                        home_team=match["home_team"],
                        home_team_id=match["home_team_id"],
                        away_team=match["away_team"],
                        away_team_id=match["away_team_id"],
                        utc_date=datetime.fromisoformat(match["utc_date"].replace("Z", "+00:00")),
                        status="SCHEDULED",
                    ))

                # Predict
                prediction = model.predict(match["home_team"], match["away_team"])

                # Match with odds (fuzzy match on team names)
                odds_event = None
                for ev in odds_events:
                    if _match_teams(match["home_team"], match["away_team"], ev["home_team"], ev["away_team"]):
                        odds_event = ev
                        break

                if not odds_event:
                    logger.info(f"[Pipeline] No odds found for {match['home_team']} vs {match['away_team']}")
                    continue

                # Get best odds for each market
                best_h2h = get_best_odds(odds_event, "h2h")
                best_totals = get_best_odds(odds_event, "totals")
                best_spreads = get_best_odds(odds_event, "spreads")

                combined_odds = {"h2h": best_h2h, "totals": best_totals, "spreads": best_spreads}

                # Find value bets
                value_bets = find_value_bets(prediction, combined_odds, min_ev=0.01)

                for vb in value_bets:
                    confidence = get_confidence_tier(vb["ev"], vb["probability"])
                    if confidence == "SKIP":
                        continue

                    vb["confidence"] = confidence

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

                    alert_msg = format_value_bet_alert(match, vb, prediction, bk_odds_comparison)
                    alerts.append(alert_msg)

                # === CORNER VALUE BETS ===
                corners_pred = prediction.get("corners", {})
                corner_lines = corners_pred.get("lines", {})
                corner_ah_pred = corners_pred.get("asian_handicap", {})

                # Fetch corner odds for this league (cached per league)
                if not hasattr(run_analysis_pipeline, '_corner_cache'):
                    run_analysis_pipeline._corner_cache = {}
                if league_code not in run_analysis_pipeline._corner_cache:
                    try:
                        eids = [ev["event_id"] for ev in odds_events if ev.get("event_id")]
                        run_analysis_pipeline._corner_cache[league_code] = get_corner_odds(league_code, event_ids=eids or None)
                    except Exception:
                        run_analysis_pipeline._corner_cache[league_code] = {}
                league_corners = run_analysis_pipeline._corner_cache[league_code]

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
                                session.add(Prediction(
                                    match_id=match["match_id"], market="corners_totals",
                                    outcome=f"Under {line}", model_probability=u_prob,
                                    best_odds=co["under_price"], best_bookmaker=co.get("under_bk", "?"),
                                    expected_value=ev, confidence=conf, is_value_bet=True,
                                ))

                # Corner AH value bets — main line only
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
                                session.add(Prediction(
                                    match_id=match["match_id"], market="corners_spreads",
                                    outcome=f"{cs.get('home_name', 'Home')} {hp:+g}",
                                    model_probability=h_prob, best_odds=cs["home_price"],
                                    best_bookmaker=cs.get("bk", "?"), expected_value=ev,
                                    confidence=conf, is_value_bet=True,
                                ))
                    if a_prob > 0 and cs.get("away_price"):
                        ev = a_prob * cs["away_price"] - 1
                        if ev > 0.01:
                            conf = get_confidence_tier(ev, a_prob)
                            if conf != "SKIP":
                                session.add(Prediction(
                                    match_id=match["match_id"], market="corners_spreads",
                                    outcome=f"{cs.get('away_name', 'Away')} {ap:+g}",
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
                                session.add(Prediction(
                                    match_id=match["match_id"], market="corners_h1_spreads",
                                    outcome=f"{cs.get('home_name', 'Home')} {hp:+g}",
                                    model_probability=h_prob, best_odds=cs["home_price"],
                                    best_bookmaker=cs.get("bk", "?"), expected_value=ev,
                                    confidence=conf, is_value_bet=True,
                                ))
                    if a_prob > 0 and cs.get("away_price"):
                        ev = a_prob * cs["away_price"] - 1
                        if ev > 0.01:
                            conf = get_confidence_tier(ev, a_prob)
                            if conf != "SKIP":
                                session.add(Prediction(
                                    match_id=match["match_id"], market="corners_h1_spreads",
                                    outcome=f"{cs.get('away_name', 'Away')} {ap:+g}",
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

        return format_daily_report(report)
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
