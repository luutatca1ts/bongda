"""Fetch corner data for Match rows from API-Football.

Strategy:
1. Find FINISHED Matches có home_api_id+away_api_id but home_corners IS NULL
2. resolve_fixture_id_prematch(home_api_id, away_api_id, utc_date, league_api_id)
3. get_fixture_stats(fid) → extract corners
4. Save: api_football_fixture_id, home_corners, away_corners

Robust: skip on any error per match (don't fail batch).
"""
import logging

from src.db.models import get_session, Match
from src.collectors.api_football import (
    resolve_fixture_id_prematch, get_fixture_stats, get_af_quota
)

logger = logging.getLogger(__name__)


def fetch_corners_for_match(match: Match, session) -> tuple[bool, str]:
    """Fetch + save corner data for a single Match. Returns (success, reason).

    Skip nếu:
    - home_api_id hoặc away_api_id NULL
    - utc_date NULL
    - resolve_fixture_id_prematch trả None
    - get_fixture_stats không có corner data
    """
    if not match.home_api_id or not match.away_api_id:
        return False, "missing_api_ids"
    if not match.utc_date:
        return False, "missing_utc_date"

    # Step 1: Resolve fixture_id (skip nếu đã có)
    fid = match.api_football_fixture_id
    if not fid:
        league_api = match.home_league_id or match.away_league_id
        try:
            fid = resolve_fixture_id_prematch(
                match.home_api_id, match.away_api_id,
                match.utc_date, league_api
            )
        except Exception as e:
            return False, f"resolve_failed: {e}"
        if not fid:
            return False, "fixture_not_found"
        match.api_football_fixture_id = fid

    # Step 2: Fetch stats
    try:
        stats = get_fixture_stats(fid)
    except Exception as e:
        return False, f"fetch_stats_failed: {e}"

    if not stats:
        return False, "empty_stats"

    home_stats = stats.get("home") or {}
    away_stats = stats.get("away") or {}
    home_corners = home_stats.get("corners")
    away_corners = away_stats.get("corners")

    if home_corners is None and away_corners is None:
        return False, "no_corner_data"

    match.home_corners = home_corners or 0
    match.away_corners = away_corners or 0
    return True, "ok"


def fetch_corners_batch(limit: int = 200, dry_run: bool = False) -> dict:
    """Batch fetch corners cho FINISHED Matches chưa có corner data.

    Returns counters dict.
    """
    session = get_session()
    counters = {
        "total": 0, "success": 0, "skipped": 0,
        "no_api_ids": 0, "no_fixture": 0, "no_stats": 0, "errors": 0,
    }
    try:
        matches = (
            session.query(Match)
            .filter(
                Match.status == "FINISHED",
                Match.home_corners.is_(None),
                Match.home_api_id.isnot(None),
                Match.away_api_id.isnot(None),
            )
            .order_by(Match.utc_date.desc())
            .limit(limit)
            .all()
        )
        counters["total"] = len(matches)
        logger.info(f"[corner_fetch] processing {len(matches)} matches (limit={limit})")

        for i, m in enumerate(matches, 1):
            success, reason = fetch_corners_for_match(m, session)
            if success:
                counters["success"] += 1
            elif "missing_api_ids" in reason:
                counters["no_api_ids"] += 1
            elif "fixture_not_found" in reason:
                counters["no_fixture"] += 1
            elif "no_corner_data" in reason or "empty_stats" in reason:
                counters["no_stats"] += 1
            else:
                counters["errors"] += 1

            if i % 20 == 0:
                logger.info(
                    f"[corner_fetch] progress {i}/{len(matches)} — "
                    f"success={counters['success']}, errors={counters['errors']}"
                )
                if not dry_run:
                    session.commit()

        if not dry_run:
            session.commit()
    finally:
        session.close()

    logger.info(f"[corner_fetch] DONE: {counters}")
    return counters
