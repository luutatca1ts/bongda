"""One-shot backfill: pull historical results from Football-Data,
match to DB rows by team name + kickoff time, flip stale matches,
resolve all pending preds.

NOTE: DB Match.match_id != Football-Data API match_id. Match rows
are joined via canonical team name + kickoff time rounded to minute.

Run AFTER applying patches:

    python backfill_results.py
"""

import logging
from datetime import datetime

from src.db.models import get_session, Match, Prediction
from src.collectors.football_data import get_recent_results
from src.bot.telegram_bot import _canonical_team_key

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    session = get_session()
    try:
        now = datetime.utcnow()
        stale = (
            session.query(Match)
            .filter(Match.status == "SCHEDULED", Match.utc_date < now)
            .all()
        )
        logger.info(f"[backfill] {len(stale)} stale SCHEDULED matches with past kickoff")

        league_codes_all = {m.competition_code for m in stale if m.competition_code}

        # Football-Data free tier chỉ support 13 league codes — filter để tránh 403/404 spam
        FD_FREE_TIER = {"PL", "BL1", "SA", "PD", "FL1", "DED", "PPL", "ELC", "CL", "EC", "WC", "BSA", "CLI"}
        invalid_codes = league_codes_all - FD_FREE_TIER
        if invalid_codes:
            logger.info(
                f"[backfill] skipping non-FD league codes: {sorted(invalid_codes)}"
            )
        league_codes = sorted(league_codes_all & FD_FREE_TIER)
        logger.info(f"[backfill] leagues to fetch: {league_codes}")

        # Build API index keyed by (home_canon, away_canon, kickoff_min)
        api_index: dict[tuple, dict] = {}
        for lc in league_codes:
            try:
                results = get_recent_results(lc, days=120) or []
                for r in results:
                    h = _canonical_team_key(r.get("home_team") or "")
                    a = _canonical_team_key(r.get("away_team") or "")
                    raw_dt = r.get("utc_date") or ""
                    try:
                        dt = datetime.fromisoformat(raw_dt.replace("Z", "+00:00")).replace(tzinfo=None)
                        ko_min = dt.replace(second=0, microsecond=0).isoformat()
                    except Exception:
                        ko_min = ""
                    if h and a and ko_min:
                        api_index[(h, a, ko_min)] = r
                logger.info(f"[backfill] {lc}: pulled {len(results)} results")
            except Exception as e:
                logger.warning(f"[backfill] get_recent_results({lc}) failed: {e}")

        # Apply API results to DB Match rows by team-name match
        flipped = 0
        unmatched = 0
        for m in stale:
            h = _canonical_team_key(m.home_team or "")
            a = _canonical_team_key(m.away_team or "")
            ko_min = (
                m.utc_date.replace(second=0, microsecond=0).isoformat()
                if m.utc_date else ""
            )
            if not (h and a and ko_min):
                unmatched += 1
                continue
            r = api_index.get((h, a, ko_min))
            if not r:
                unmatched += 1
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
        session.commit()
        logger.info(
            f"[backfill] Phase 1 done: flipped {flipped} Match rows "
            f"(unmatched: {unmatched})"
        )

        # ---------- PHASE 2: resolve pending preds via finished_index ----------
        # Match qua (canonical_home, canonical_away, ko_min) — không dùng pred.match_id
        # vì DB có thể có 2 Match record cho cùng 1 trận (Odds API + Football-Data).
        pending_preds = (
            session.query(Prediction)
            .filter(Prediction.is_value_bet == True, Prediction.result.is_(None))  # noqa: E712
            .all()
        )

        finished_index: dict[tuple, Match] = {}
        all_finished = (
            session.query(Match)
            .filter(
                Match.status == "FINISHED",
                Match.home_goals.isnot(None),
                Match.away_goals.isnot(None),
            )
            .all()
        )
        for fm in all_finished:
            h = _canonical_team_key(fm.home_team or "")
            a = _canonical_team_key(fm.away_team or "")
            ko_min = (
                fm.utc_date.replace(second=0, microsecond=0).isoformat()
                if fm.utc_date else ""
            )
            if h and a and ko_min:
                finished_index[(h, a, ko_min)] = fm

        logger.info(
            f"[backfill] Phase 2: indexed {len(finished_index)} finished matches"
        )

        updated: list[str] = []
        unresolved_no_match = 0
        unresolved_unfinished = 0
        for pred in pending_preds:
            pred_match = session.query(Match).filter(Match.match_id == pred.match_id).first()
            if not pred_match:
                unresolved_no_match += 1
                continue

            h = _canonical_team_key(pred_match.home_team or "")
            a = _canonical_team_key(pred_match.away_team or "")
            ko_min = (
                pred_match.utc_date.replace(second=0, microsecond=0).isoformat()
                if pred_match.utc_date else ""
            )
            if not (h and a and ko_min):
                unresolved_no_match += 1
                continue

            finished_match = finished_index.get((h, a, ko_min))
            if not finished_match:
                unresolved_unfinished += 1
                continue

            match = finished_match
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
                icon = "✅" if pred.result == "WIN" else "❌"
                updated.append(
                    f"{icon} {match.home_team} vs {match.away_team} "
                    f"{pred.market}/{pred.outcome} → {pred.result}"
                )

        if updated:
            session.commit()
        logger.info(
            f"[backfill] Phase 2 done: resolved {len(updated)}/{len(pending_preds)} "
            f"(no_match={unresolved_no_match}, not_finished_yet={unresolved_unfinished})"
        )

        total_preds = session.query(Prediction).count()
        resolved = (
            session.query(Prediction).filter(Prediction.result.isnot(None)).count()
        )
        wins = session.query(Prediction).filter(Prediction.result == "WIN").count()
        losses = session.query(Prediction).filter(Prediction.result == "LOSE").count()
        pushes = session.query(Prediction).filter(Prediction.result == "PUSH").count()
        pending = total_preds - resolved
        win_rate = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0.0

        print()
        print("=" * 50)
        print("BACKFILL DONE")
        print("=" * 50)
        print(f"Total predictions: {total_preds}")
        print(f"Resolved:          {resolved}")
        print(f"  WIN:             {wins}")
        print(f"  LOSE:            {losses}")
        print(f"  PUSH:            {pushes}")
        print(f"Still pending:     {pending}")
        print(f"Win rate:          {win_rate:.1f}%")
        print("=" * 50)
    finally:
        session.close()


if __name__ == "__main__":
    main()
