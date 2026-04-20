"""Closing Line Value — đo chất lượng bet bằng cách so giá khi đánh vs giá đóng.

CLV = (best_odds_khi_đánh - closing_odds) / closing_odds * 100
- CLV > 0: ta lấy giá tốt hơn thị trường → dài hạn có lợi thế (beat the market).
- CLV < 0: thị trường dịch ngược với ta → long-run edge yếu.
Sharp bettor thường đạt CLV trung bình +2% đến +5%.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from sqlalchemy import func

from src.db.models import get_session, Match, Prediction, OddsHistory

logger = logging.getLogger(__name__)


def capture_closing_lines() -> int:
    """Capture closing odds cho các trận sắp kickoff (trong ~45 phút tới).

    Với mỗi Prediction chưa có closing_odds: tìm snapshot OddsHistory mới nhất
    cho (match_id, market, outcome) ưu tiên best_bookmaker của pred, fallback
    pinnacle.

    Return số prediction đã capture.
    """
    now = datetime.utcnow()
    window_end = now + timedelta(minutes=45)

    session = get_session()
    captured = 0
    try:
        matches = (
            session.query(Match)
            .filter(
                Match.status == "SCHEDULED",
                Match.utc_date >= now,
                Match.utc_date <= window_end,
            )
            .all()
        )

        for m in matches:
            preds = (
                session.query(Prediction)
                .filter(
                    Prediction.match_id == m.match_id,
                    Prediction.closing_odds.is_(None),
                )
                .all()
            )
            for pred in preds:
                # Ưu tiên đúng bookmaker đã đánh, fallback pinnacle
                bk_keys = []
                if pred.best_bookmaker:
                    # best_bookmaker lưu tên hiển thị (vd "Pinnacle"), OddsHistory
                    # lưu bookmaker_key (vd "pinnacle"). So sánh cả 2 dạng.
                    bk_keys.append(pred.best_bookmaker)
                    bk_keys.append(pred.best_bookmaker.lower().replace(" ", ""))
                bk_keys.append("pinnacle")

                snapshot = None
                for bk in bk_keys:
                    q = (
                        session.query(OddsHistory)
                        .filter(
                            OddsHistory.match_id == pred.match_id,
                            OddsHistory.market == pred.market,
                            OddsHistory.outcome == pred.outcome,
                        )
                    )
                    # Match theo bookmaker_key (case-insensitive approx)
                    q = q.filter(func.lower(OddsHistory.bookmaker_key) == bk.lower())
                    snapshot = q.order_by(OddsHistory.captured_at.desc()).first()
                    if snapshot is not None:
                        break

                # Fallback cuối: bất kỳ bookmaker nào có record
                if snapshot is None:
                    snapshot = (
                        session.query(OddsHistory)
                        .filter(
                            OddsHistory.match_id == pred.match_id,
                            OddsHistory.market == pred.market,
                            OddsHistory.outcome == pred.outcome,
                        )
                        .order_by(OddsHistory.captured_at.desc())
                        .first()
                    )

                if snapshot is None or snapshot.odds <= 0:
                    continue

                pred.closing_odds = snapshot.odds
                pred.closing_captured_at = now
                if pred.best_odds and pred.best_odds > 0:
                    pred.clv = (pred.best_odds - snapshot.odds) / snapshot.odds * 100
                captured += 1

        session.commit()
    except Exception as e:
        logger.error(f"[CLV] capture_closing_lines error: {e}", exc_info=True)
        session.rollback()
    finally:
        session.close()

    if captured:
        logger.info(f"[CLV] Captured closing odds for {captured} predictions")
    return captured


def get_clv_stats(days: int = 30) -> dict:
    """Thống kê CLV trong N ngày gần nhất.

    Return: {count, avg_clv, positive_count, positive_pct,
             by_confidence: {HIGH/MEDIUM/LOW: avg},
             by_market:     {market: avg}}
    """
    cutoff = datetime.utcnow() - timedelta(days=days)
    session = get_session()
    try:
        preds = (
            session.query(Prediction)
            .filter(
                Prediction.clv.isnot(None),
                Prediction.created_at >= cutoff,
            )
            .all()
        )

        count = len(preds)
        if count == 0:
            return {
                "count": 0,
                "avg_clv": 0.0,
                "positive_count": 0,
                "positive_pct": 0.0,
                "by_confidence": {},
                "by_market": {},
                "days": days,
            }

        clvs = [p.clv for p in preds]
        avg_clv = sum(clvs) / count
        positive = [c for c in clvs if c > 0]
        positive_count = len(positive)
        positive_pct = positive_count / count * 100

        by_conf: dict[str, list[float]] = {}
        by_market: dict[str, list[float]] = {}
        for p in preds:
            if p.confidence:
                by_conf.setdefault(p.confidence, []).append(p.clv)
            if p.market:
                by_market.setdefault(p.market, []).append(p.clv)

        def _avg(vals: list[float]) -> float:
            return sum(vals) / len(vals) if vals else 0.0

        return {
            "count": count,
            "avg_clv": avg_clv,
            "positive_count": positive_count,
            "positive_pct": positive_pct,
            "by_confidence": {k: _avg(v) for k, v in by_conf.items()},
            "by_market": {k: _avg(v) for k, v in by_market.items()},
            "days": days,
        }
    finally:
        session.close()


def format_clv_report(stats: dict) -> str:
    """Format báo cáo CLV thành message Telegram."""
    days = stats.get("days", 30)
    count = stats.get("count", 0)
    if count == 0:
        return (
            f"\U0001f4c8 CLOSING LINE VALUE ({days} ng\u00e0y)\n"
            f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
            f"Ch\u01b0a c\u00f3 bet n\u00e0o c\u00f3 CLV trong kho\u1ea3ng th\u1eddi gian n\u00e0y.\n"
            f"\n\U0001f4a1 CLV \u0111\u01b0\u1ee3c capture t\u1ef1 \u0111\u1ed9ng ~45' tr\u01b0\u1edbc kickoff.\n"
            f"\U0001f4a1 CLV > 0 ngh\u0129a l\u00e0 bot \u0111ang \u0111\u00e1nh b\u1ea1i th\u1ecb tr\u01b0\u1eddng\n"
            f"\U0001f4a1 Sharp bettor x\u1ecbn c\u00f3 CLV trung b\u00ecnh +2% \u0111\u1ebfn +5%"
        )

    conf_map = {"HIGH": "\U0001f534", "MEDIUM": "\U0001f7e1", "LOW": "\U0001f7e2"}
    conf_lines = ""
    for level in ("HIGH", "MEDIUM", "LOW"):
        if level in stats["by_confidence"]:
            v = stats["by_confidence"][level]
            conf_lines += f"  {conf_map[level]} {level:6s}: {v:+.2f}%\n"

    market_lines = ""
    for market, v in sorted(stats["by_market"].items(), key=lambda x: x[1], reverse=True):
        market_lines += f"  \u2022 {market}: {v:+.2f}%\n"

    msg = (
        f"\U0001f4c8 CLOSING LINE VALUE ({days} ng\u00e0y)\n"
        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        f"\U0001f4ca T\u1ed5ng s\u1ed1 bet c\u00f3 CLV: {count}\n"
        f"\U0001f4b0 CLV trung b\u00ecnh: {stats['avg_clv']:+.2f}%\n"
        f"\u2705 T\u1ef7 l\u1ec7 CLV d\u01b0\u01a1ng: {stats['positive_pct']:.1f}% "
        f"({stats['positive_count']}/{count})\n"
    )
    if conf_lines:
        msg += f"\nTheo confidence:\n{conf_lines}"
    if market_lines:
        msg += f"\nTheo market:\n{market_lines}"
    msg += (
        f"\n\U0001f4a1 CLV > 0 ngh\u0129a l\u00e0 bot \u0111ang \u0111\u00e1nh b\u1ea1i th\u1ecb tr\u01b0\u1eddng\n"
        f"\U0001f4a1 Sharp bettor x\u1ecbn c\u00f3 CLV trung b\u00ecnh +2% \u0111\u1ebfn +5%"
    )
    return msg
