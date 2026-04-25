"""One-shot script: fetch corner data cho mọi FINISHED Match chưa có corners,
sau đó re-run update_results() để resolve corner preds."""
import logging
from src.collectors.corner_fetcher import fetch_corners_batch
from src.pipeline import update_results

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main():
    print("=" * 60)
    print("PHASE A: Fetching corner stats from API-Football")
    print("=" * 60)
    counters = fetch_corners_batch(limit=2000)
    print(f"\nFetch summary: {counters}")

    print()
    print("=" * 60)
    print("PHASE B: Re-running update_results to resolve corner preds")
    print("=" * 60)
    updated = update_results()
    print(f"\nupdate_results resolved {len(updated)} new predictions")

    from src.db.models import get_session, Prediction
    session = get_session()
    try:
        total = session.query(Prediction).filter(Prediction.is_value_bet == True).count()
        resolved = session.query(Prediction).filter(
            Prediction.is_value_bet == True,
            Prediction.result.isnot(None)
        ).count()
        wins = session.query(Prediction).filter(Prediction.result == "WIN").count()
        losses = session.query(Prediction).filter(Prediction.result == "LOSE").count()
        pushes = session.query(Prediction).filter(Prediction.result == "PUSH").count()
        win_rate = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0
    finally:
        session.close()

    print()
    print("=" * 60)
    print("FINAL STATS")
    print("=" * 60)
    print(f"Total predictions: {total}")
    print(f"Resolved: {resolved}")
    print(f"  WIN:  {wins}")
    print(f"  LOSE: {losses}")
    print(f"  PUSH: {pushes}")
    print(f"Still pending: {total - resolved}")
    print(f"Win rate: {win_rate:.1f}%")


if __name__ == "__main__":
    main()
