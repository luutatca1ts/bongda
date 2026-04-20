"""Steam Move Detection — phát hiện nhiều bookmaker cùng dịch odds 1 hướng.

Steam move = dấu hiệu sharp money: khi ≥N bookmaker độc lập cùng shorten/drift
1 outcome trong thời gian ngắn (~15 phút), thường do sharp bettor lớn vào cửa.
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from datetime import datetime, timedelta

from src.db.models import get_session, Match, OddsHistory

logger = logging.getLogger(__name__)

# Cache chống trùng alert — {(match_id, market, outcome, point): last_alerted_ts}
_ALERT_CACHE: dict[tuple, float] = {}
_ALERT_TTL_SECONDS = 30 * 60  # 30 phút


def _prune_alert_cache() -> None:
    now = time.time()
    expired = [k for k, ts in _ALERT_CACHE.items() if now - ts > _ALERT_TTL_SECONDS]
    for k in expired:
        _ALERT_CACHE.pop(k, None)


def detect_steam_moves(window_minutes: int = 15,
                       min_bookmakers: int = 3,
                       min_drift_pct: float = 3.0,
                       match_id_filter: int | None = None) -> list[dict]:
    """Phát hiện steam move trong cửa sổ thời gian gần nhất.

    Args:
        window_minutes: khoảng thời gian xét (phút).
        min_bookmakers: số bookmaker tối thiểu cùng hướng để coi là steam.
        min_drift_pct: ngưỡng |drift_pct| mỗi bookmaker phải vượt.
        match_id_filter: nếu có, chỉ xét 1 match (dùng trong pipeline).

    Return list: [{match_id, home_team, away_team, market, outcome, point,
                   direction, avg_drift_pct, bookmakers_count, bookmakers, detected_at}]
    """
    _prune_alert_cache()

    cutoff = datetime.utcnow() - timedelta(minutes=window_minutes)
    session = get_session()
    try:
        q = session.query(OddsHistory).filter(OddsHistory.captured_at >= cutoff)
        if match_id_filter is not None:
            q = q.filter(OddsHistory.match_id == match_id_filter)
        rows = q.all()

        # Group: (match_id, market, outcome, point) -> {bk_key: [rows sorted by time]}
        groups: dict[tuple, dict[str, list]] = defaultdict(lambda: defaultdict(list))
        for r in rows:
            key = (r.match_id, r.market, r.outcome, r.point)
            groups[key][r.bookmaker_key].append(r)

        results: list[dict] = []
        now = datetime.utcnow()

        for (mid, market, outcome, point), bk_map in groups.items():
            shortening_bks: list[dict] = []
            drifting_bks: list[dict] = []
            for bk_key, bk_rows in bk_map.items():
                if len(bk_rows) < 2:
                    continue
                bk_rows_sorted = sorted(bk_rows, key=lambda x: x.captured_at)
                first, last = bk_rows_sorted[0], bk_rows_sorted[-1]
                if first.odds <= 0:
                    continue
                drift_pct = (last.odds - first.odds) / first.odds * 100
                if abs(drift_pct) < min_drift_pct:
                    continue
                entry = {
                    "bookmaker_key": bk_key,
                    "bookmaker_name": last.bookmaker_name,
                    "drift_pct": drift_pct,
                }
                if drift_pct < 0:
                    shortening_bks.append(entry)
                else:
                    drifting_bks.append(entry)

            # Pick dominant direction if it meets threshold
            chosen: list[dict] | None = None
            direction = None
            if len(shortening_bks) >= min_bookmakers:
                chosen = shortening_bks
                direction = "shortening"
            elif len(drifting_bks) >= min_bookmakers:
                chosen = drifting_bks
                direction = "drifting"
            if not chosen:
                continue

            # Suppress if already alerted within TTL
            cache_key = (mid, market, outcome, point)
            last_alerted = _ALERT_CACHE.get(cache_key)
            if last_alerted and (time.time() - last_alerted) < _ALERT_TTL_SECONDS:
                continue

            match = session.query(Match).filter(Match.match_id == mid).first()
            if match is None:
                continue

            avg_drift = sum(b["drift_pct"] for b in chosen) / len(chosen)
            results.append({
                "match_id": mid,
                "home_team": match.home_team,
                "away_team": match.away_team,
                "market": market,
                "outcome": outcome,
                "point": point,
                "direction": direction,
                "avg_drift_pct": avg_drift,
                "bookmakers_count": len(chosen),
                "bookmakers": [b["bookmaker_name"] for b in chosen],
                "detected_at": now,
            })
            _ALERT_CACHE[cache_key] = time.time()

        if results:
            logger.info(f"[SteamDetector] Detected {len(results)} steam moves")
        return results
    finally:
        session.close()


def format_steam_alert(steam: dict) -> str:
    """Format steam move thành message Telegram."""
    direction = steam.get("direction", "stable")
    arrow = "\U0001f53b" if direction == "shortening" else "\U0001f53a"
    side_hint = (
        "Sharp money \u0111ang v\u00e0o c\u1eeda n\u00e0y"
        if direction == "shortening"
        else "Sharp money \u0111ang r\u00fat kh\u1ecfi c\u1eeda n\u00e0y"
    )

    point = steam.get("point")
    outcome = steam["outcome"]
    if point is not None:
        outcome_disp = f"{outcome} {point:g}"
    else:
        outcome_disp = outcome

    bks = steam.get("bookmakers", [])
    bk_count = steam.get("bookmakers_count", len(bks))
    bk_list = ", ".join(bks[:8])
    if len(bks) > 8:
        bk_list += f" (+{len(bks) - 8})"

    msg = (
        f"\U0001f525 STEAM MOVE DETECTED\n"
        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        f"\u26bd {steam['home_team']} vs {steam['away_team']}\n"
        f"\U0001f4ca Market: {steam['market']} \u2014 {outcome_disp}\n"
        f"{arrow} Odds d\u1ecbch trung b\u00ecnh {steam['avg_drift_pct']:+.1f}% "
        f"t\u1ea1i {bk_count} bookmaker:\n"
        f"  \u2022 {bk_list}\n"
        f"\U0001f4a1 {side_hint}"
    )
    return msg
