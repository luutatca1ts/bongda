"""Line Movement Tracking — lưu snapshot odds mỗi chu kỳ, truy vấn lịch sử dịch kèo.

Cấu trúc event đầu vào (từ src.collectors.odds_api.get_odds._parse_event):
    {
        "event_id", "home_team", "away_team", "commence_time",
        "bookmakers": {
            bk_key: {
                "name": str,
                "markets": {
                    "h2h": {outcome: price},
                    "totals": {outcome: {price, point}},
                    "spreads": {outcome: {price, point}},
                    "corners_totals": [{name, price, point}, ...],
                }
            }
        }
    }
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func

from src.db.models import get_session, Match, OddsHistory

logger = logging.getLogger(__name__)

# Ngưỡng thay đổi odds tối thiểu để insert snapshot mới (tránh ghi trùng lặp)
_MIN_ODDS_DELTA = 0.02


# ---------------------------------------------------------------------------
# Team-name fuzzy match (reuse logic từ pipeline để đồng bộ)
# ---------------------------------------------------------------------------

def _normalize(name: str) -> str:
    """Normalize team name — mỏng hơn src.pipeline._normalize, chỉ cần cho match_id lookup."""
    import re
    import unicodedata
    name = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode("ascii")
    name = name.lower()
    for token in ["fc", "cf", "sc", "ac", "ss", "us", "as", "ssc", "1.", "de futbol", "calcio"]:
        name = name.replace(token, "")
    name = name.replace("-", " ")
    name = re.sub(r"[^a-z0-9 ]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def _teams_match(db_home: str, db_away: str, ev_home: str, ev_away: str) -> bool:
    """Fuzzy match: hai cặp đội có share ít nhất 1 từ ≥4 ký tự cho cả home và away."""
    def _similar(a: str, b: str) -> bool:
        a_n, b_n = _normalize(a), _normalize(b)
        if not a_n or not b_n:
            return False
        if a_n in b_n or b_n in a_n:
            return True
        wa = {w for w in a_n.split() if len(w) >= 4}
        wb = {w for w in b_n.split() if len(w) >= 4}
        return bool(wa & wb)
    return _similar(db_home, ev_home) and _similar(db_away, ev_away)


def _find_match_id(session, ev: dict) -> int | None:
    """Tìm Match.match_id khớp với 1 event odds (team fuzzy + kickoff ≤6h)."""
    ev_ct = ev.get("commence_time")
    ev_dt = None
    if ev_ct:
        try:
            ev_dt = datetime.fromisoformat(ev_ct.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            ev_dt = None

    # Lấy candidate trong khoảng ±6h nếu có commence_time, else tất cả SCHEDULED
    q = session.query(Match)
    if ev_dt is not None:
        q = q.filter(
            Match.utc_date >= ev_dt - timedelta(hours=6),
            Match.utc_date <= ev_dt + timedelta(hours=6),
        )
    candidates = q.all()
    ev_home = ev.get("home_team", "")
    ev_away = ev.get("away_team", "")
    for m in candidates:
        if _teams_match(m.home_team, m.away_team, ev_home, ev_away):
            return m.match_id
    return None


# ---------------------------------------------------------------------------
# Iterator parse các cấu trúc market khác nhau
# ---------------------------------------------------------------------------

def _iter_market_entries(market_key: str, market_data: Any):
    """Yield (outcome, point, price) cho mọi cấu trúc market.

    - h2h: dict {outcome: price}
    - totals/spreads: dict {outcome: {price, point}}
    - corners_totals (list): [{name, price, point}, ...]
    """
    if isinstance(market_data, list):
        # corners_totals
        for entry in market_data:
            name = entry.get("name")
            price = entry.get("price")
            point = entry.get("point")
            if name is None or price is None:
                continue
            yield name, point, price
        return

    if not isinstance(market_data, dict):
        return

    for outcome, value in market_data.items():
        if isinstance(value, dict):
            price = value.get("price")
            point = value.get("point")
        else:
            price = value
            point = None
        if price is None:
            continue
        yield outcome, point, price


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def save_odds_snapshot(odds_events: list[dict]) -> int:
    """Lưu snapshot odds vào OddsHistory.

    Chỉ insert khi odds mới lệch ≥ _MIN_ODDS_DELTA so với snapshot gần nhất của
    cùng (match_id, bookmaker_key, market, outcome, point).

    Return số records đã insert.
    """
    if not odds_events:
        return 0

    session = get_session()
    inserted = 0
    skipped_no_match = 0
    now = datetime.utcnow()

    try:
        for ev in odds_events:
            match_id = _find_match_id(session, ev)
            if match_id is None:
                skipped_no_match += 1
                continue

            for bk_key, bk_data in (ev.get("bookmakers") or {}).items():
                bk_name = bk_data.get("name", bk_key)
                for market_key, market_data in (bk_data.get("markets") or {}).items():
                    for outcome, point, price in _iter_market_entries(market_key, market_data):
                        try:
                            odds_val = float(price)
                        except (TypeError, ValueError):
                            continue
                        if odds_val <= 0:
                            continue

                        # Lấy snapshot mới nhất cho cùng key
                        q = (
                            session.query(OddsHistory)
                            .filter(
                                OddsHistory.match_id == match_id,
                                OddsHistory.bookmaker_key == bk_key,
                                OddsHistory.market == market_key,
                                OddsHistory.outcome == outcome,
                            )
                        )
                        if point is None:
                            q = q.filter(OddsHistory.point.is_(None))
                        else:
                            q = q.filter(OddsHistory.point == point)
                        last = q.order_by(OddsHistory.captured_at.desc()).first()

                        if last is not None and abs(last.odds - odds_val) < _MIN_ODDS_DELTA:
                            continue

                        session.add(OddsHistory(
                            match_id=match_id,
                            bookmaker_key=bk_key,
                            bookmaker_name=bk_name,
                            market=market_key,
                            outcome=outcome,
                            point=point,
                            odds=odds_val,
                            captured_at=now,
                        ))
                        inserted += 1

        session.commit()
    except Exception as e:
        logger.error(f"[LineMovement] save_odds_snapshot error: {e}", exc_info=True)
        session.rollback()
    finally:
        session.close()

    if skipped_no_match:
        logger.info(
            f"[LineMovement] Saved {inserted} snapshots "
            f"(skipped {skipped_no_match} events — no match_id in DB)"
        )
    else:
        logger.info(f"[LineMovement] Saved {inserted} snapshots")
    return inserted


def _query_snapshot(match_id: int, market: str, outcome: str,
                    bookmaker_key: str, point: float | None,
                    order: str = "asc") -> dict | None:
    session = get_session()
    try:
        q = (
            session.query(OddsHistory)
            .filter(
                OddsHistory.match_id == match_id,
                OddsHistory.market == market,
                OddsHistory.outcome == outcome,
                OddsHistory.bookmaker_key == bookmaker_key,
            )
        )
        if point is None:
            q = q.filter(OddsHistory.point.is_(None))
        else:
            q = q.filter(OddsHistory.point == point)
        if order == "desc":
            row = q.order_by(OddsHistory.captured_at.desc()).first()
        else:
            row = q.order_by(OddsHistory.captured_at.asc()).first()
        if row is None:
            return None
        return {
            "odds": row.odds,
            "point": row.point,
            "captured_at": row.captured_at,
            "bookmaker": row.bookmaker_name,
        }
    finally:
        session.close()


def get_opening_odds(match_id: int, market: str, outcome: str,
                     bookmaker_key: str = "pinnacle",
                     point: float | None = None) -> dict | None:
    """Snapshot cũ nhất của (match_id, market, outcome, bookmaker, point)."""
    return _query_snapshot(match_id, market, outcome, bookmaker_key, point, order="asc")


def get_current_odds(match_id: int, market: str, outcome: str,
                     bookmaker_key: str = "pinnacle",
                     point: float | None = None) -> dict | None:
    """Snapshot mới nhất của (match_id, market, outcome, bookmaker, point)."""
    return _query_snapshot(match_id, market, outcome, bookmaker_key, point, order="desc")


def compute_drift(match_id: int, market: str, outcome: str,
                  bookmaker_key: str = "pinnacle",
                  point: float | None = None) -> dict | None:
    """Tính drift opening → current cho 1 outcome.

    Trả {opening_odds, current_odds, drift_pct, implied_prob_shift, direction,
         opening_time, current_time}.
    - shortening: odds giảm → tiền đổ vào cửa này.
    - drifting:   odds tăng → tiền rút khỏi cửa này.
    - stable:     |drift_pct| < 1%.
    """
    opening = get_opening_odds(match_id, market, outcome, bookmaker_key, point)
    current = get_current_odds(match_id, market, outcome, bookmaker_key, point)
    if opening is None or current is None:
        return None
    o, c = opening["odds"], current["odds"]
    if o <= 0 or c <= 0:
        return None
    drift_pct = (c - o) / o * 100
    implied_shift = (1.0 / c) - (1.0 / o)
    if drift_pct < -1.0:
        direction = "shortening"
    elif drift_pct > 1.0:
        direction = "drifting"
    else:
        direction = "stable"
    return {
        "opening_odds": o,
        "current_odds": c,
        "drift_pct": drift_pct,
        "implied_prob_shift": implied_shift,
        "direction": direction,
        "opening_time": opening["captured_at"],
        "current_time": current["captured_at"],
    }


def get_movement_timeline(match_id: int, market: str, outcome: str,
                          bookmaker_key: str = "pinnacle",
                          point: float | None = None,
                          limit: int = 50) -> list[dict]:
    """Timeline snapshots theo thứ tự thời gian tăng dần."""
    session = get_session()
    try:
        q = (
            session.query(OddsHistory)
            .filter(
                OddsHistory.match_id == match_id,
                OddsHistory.market == market,
                OddsHistory.outcome == outcome,
                OddsHistory.bookmaker_key == bookmaker_key,
            )
        )
        if point is None:
            q = q.filter(OddsHistory.point.is_(None))
        else:
            q = q.filter(OddsHistory.point == point)
        rows = q.order_by(OddsHistory.captured_at.asc()).limit(limit).all()
        return [
            {
                "odds": r.odds,
                "point": r.point,
                "captured_at": r.captured_at,
                "bookmaker": r.bookmaker_name,
            }
            for r in rows
        ]
    finally:
        session.close()


def get_all_bookmakers_current(match_id: int, market: str, outcome: str,
                               point: float | None = None) -> dict:
    """Snapshot mới nhất của từng bookmaker cho 1 outcome.

    Return: {bk_key: {odds, captured_at, bookmaker_name, point}}
    """
    session = get_session()
    try:
        base_filters = [
            OddsHistory.match_id == match_id,
            OddsHistory.market == market,
            OddsHistory.outcome == outcome,
        ]
        if point is None:
            base_filters.append(OddsHistory.point.is_(None))
        else:
            base_filters.append(OddsHistory.point == point)

        subq = (
            session.query(
                OddsHistory.bookmaker_key.label("bk"),
                func.max(OddsHistory.captured_at).label("max_ts"),
            )
            .filter(*base_filters)
            .group_by(OddsHistory.bookmaker_key)
            .subquery()
        )

        rows = (
            session.query(OddsHistory)
            .join(
                subq,
                (OddsHistory.bookmaker_key == subq.c.bk)
                & (OddsHistory.captured_at == subq.c.max_ts),
            )
            .filter(*base_filters)
            .all()
        )

        out: dict = {}
        for r in rows:
            out[r.bookmaker_key] = {
                "odds": r.odds,
                "captured_at": r.captured_at,
                "bookmaker_name": r.bookmaker_name,
                "point": r.point,
            }
        return out
    finally:
        session.close()


def cleanup_old_history(days: int = 30) -> int:
    """Xóa record cũ hơn N ngày. Return số row đã xóa."""
    cutoff = datetime.utcnow() - timedelta(days=days)
    session = get_session()
    try:
        n = (
            session.query(OddsHistory)
            .filter(OddsHistory.captured_at < cutoff)
            .delete(synchronize_session=False)
        )
        session.commit()
        logger.info(f"[LineMovement] Cleanup — removed {n} records older than {days} days")
        return n
    except Exception as e:
        logger.error(f"[LineMovement] cleanup error: {e}")
        session.rollback()
        return 0
    finally:
        session.close()


def get_stats() -> dict:
    """Thống kê tổng về bảng odds_history."""
    session = get_session()
    try:
        total = session.query(func.count(OddsHistory.id)).scalar() or 0
        by_market_rows = (
            session.query(OddsHistory.market, func.count(OddsHistory.id))
            .group_by(OddsHistory.market)
            .all()
        )
        by_market = {m: c for m, c in by_market_rows}
        oldest = session.query(func.min(OddsHistory.captured_at)).scalar()
        newest = session.query(func.max(OddsHistory.captured_at)).scalar()
        return {
            "total_records": total,
            "by_market": by_market,
            "oldest": oldest,
            "newest": newest,
        }
    finally:
        session.close()
