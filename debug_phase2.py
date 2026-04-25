"""Debug: tại sao Phase 2 không match pred_match với finished_match.
Dump 5 sample preds chưa resolve, kèm pred_match info + 5 finished match
gần nhất cùng team để so sánh canonical key + kickoff."""
from datetime import datetime
from src.db.models import get_session, Match, Prediction
from src.bot.telegram_bot import _canonical_team_key

session = get_session()

print("\n" + "=" * 70)
print("STEP 1: Distribution of competition_code in DB")
print("=" * 70)
from sqlalchemy import func, case
rows = (
    session.query(Match.competition_code, func.count(Match.id),
                  func.sum(case((Match.status == "FINISHED", 1), else_=0)))
    .group_by(Match.competition_code)
    .order_by(func.count(Match.id).desc())
    .all()
)
for code, total, finished in rows:
    print(f"  {code or '(NULL)':30s} total={total:5d} finished={finished or 0:5d}")

print("\n" + "=" * 70)
print("STEP 2: Sample 5 unresolved preds + their pred_match")
print("=" * 70)

pending = (
    session.query(Prediction)
    .filter(Prediction.is_value_bet == True, Prediction.result.is_(None))
    .limit(5)
    .all()
)

for pred in pending:
    pm = session.query(Match).filter(Match.match_id == pred.match_id).first()
    if not pm:
        print(f"\nPred {pred.id}: NO MATCH FOUND for match_id={pred.match_id}")
        continue
    print(f"\nPred {pred.id}: {pred.market}/{pred.outcome}")
    print(f"  pred_match: id={pm.id}, match_id={pm.match_id}")
    print(f"    home={pm.home_team!r} away={pm.away_team!r}")
    print(f"    canon_h={_canonical_team_key(pm.home_team or '')!r}")
    print(f"    canon_a={_canonical_team_key(pm.away_team or '')!r}")
    print(f"    utc_date={pm.utc_date} status={pm.status} comp_code={pm.competition_code}")
    print(f"    home_goals={pm.home_goals} away_goals={pm.away_goals}")

    # Tìm Match khác có cùng team (fuzzy) để xem có version FINISHED không
    if pm.home_team and pm.away_team:
        canon_h = _canonical_team_key(pm.home_team)
        canon_a = _canonical_team_key(pm.away_team)
        candidates = (
            session.query(Match)
            .filter(Match.id != pm.id, Match.status == "FINISHED")
            .all()
        )
        siblings = [
            m for m in candidates
            if _canonical_team_key(m.home_team or "") == canon_h
            and _canonical_team_key(m.away_team or "") == canon_a
        ]
        if siblings:
            print(f"  -> FOUND {len(siblings)} sibling FINISHED match(es) with same canonical teams:")
            for s in siblings[:3]:
                delta_min = None
                if s.utc_date and pm.utc_date:
                    delta_min = abs((s.utc_date - pm.utc_date).total_seconds()) / 60
                print(f"     sibling id={s.id} match_id={s.match_id} comp={s.competition_code}")
                print(f"       utc_date={s.utc_date} (delta_min={delta_min})")
                print(f"       goals={s.home_goals}-{s.away_goals}")
        else:
            print(f"  -> NO sibling FINISHED match with same canonical teams")

session.close()
