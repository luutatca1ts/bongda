"""Pre-match odds re-check pipeline (`/chot`).

Phase 1 scope (kept narrow on purpose):
  * Find Predictions with kickoff in 30-90 min and `is_value_bet=True`
    that have NOT yet been re-checked (no row in `chot_reanalysis`).
  * Re-fetch latest odds from The Odds API (per league, batched).
  * Re-compute EV using the SAVED model probability and the new odds —
    no model re-run, no lineup/xG/injury/weather adjust (that's Phase 2).
  * Decide keep / better / worse / drop, broadcast to subscribers,
    persist a ChotReanalysis row.

Markets supported in Phase 1: `h2h`, `totals`. Other markets (spreads,
asian_handicap, corners_*) are skipped — line/handicap re-matching needs
the corner pipeline + spread-pair logic and is deferred to Phase 2.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional

from src.db.models import (
    ChotReanalysis,
    Match,
    Prediction,
    get_session,
)

logger = logging.getLogger(__name__)


# Phase 1: only these markets are re-checked. Others are logged + skipped.
_SUPPORTED_MARKETS = {"h2h", "totals"}

# Decision thresholds (absolute EV diff in fraction units, e.g. 0.02 = 2%).
_EV_BAND = 0.02

# Same suffix-stripping idea as telegram_bot._canonical_team_key — duplicated
# here to avoid a circular import.
_TEAM_SUFFIXES = (" fc", " afc", " cf", " sc", " ac", " fk", " sk", " ck",
                  " hc", " bk", " if", " ff", " kf")


def _norm_team(name: str) -> str:
    if not name:
        return ""
    s = name.strip().lower()
    changed = True
    while changed:
        changed = False
        for suf in _TEAM_SUFFIXES:
            if s.endswith(suf):
                s = s[: -len(suf)].rstrip()
                changed = True
    return s


def _collect_phase2_signals(match: Match, pred: Prediction) -> dict:
    """Best-effort fetch of context signals for a re-check. Display-only —
    never raises, never adjusts EV.

    Returns a dict with three optional sub-blocks:
      * `injuries`: {"home_key_out": int, "away_key_out": int} or None
      * `lineup`:   {"has_lineup": bool, "home_formation": str,
                     "away_formation": str} or None
      * `xg`:       {"home": float, "away": float} or None

    Injuries + lineup need the API-Football fixture_id, which the Match
    model does NOT currently persist. We attempt to resolve it from the
    LiveMatchState table (live collector may have seen it), and skip the
    signal if no id is available. xG comes from Prediction.home_xg_estimate
    / away_xg_estimate populated at Poisson time.
    """
    out: dict = {"injuries": None, "lineup": None, "xg": None}

    # --- xG form (from saved prediction λ) ---
    try:
        h_xg = pred.home_xg_estimate
        a_xg = pred.away_xg_estimate
        if h_xg is not None and a_xg is not None and (h_xg > 0 or a_xg > 0):
            out["xg"] = {"home": float(h_xg), "away": float(a_xg)}
    except Exception as e:  # noqa: BLE001
        logger.debug(f"[chot] xg signal skipped pred_id={pred.id}: {e}")

    # --- Try to resolve API-Football fixture_id from LiveMatchState ---
    fixture_id: Optional[int] = None
    try:
        from src.db.models import LiveMatchState, get_session as _gs
        s = _gs()
        try:
            row = (
                s.query(LiveMatchState)
                .filter(LiveMatchState.match_id == match.match_id)
                .filter(LiveMatchState.fixture_id.isnot(None))
                .order_by(LiveMatchState.captured_at.desc())
                .first()
            )
            if row and row.fixture_id:
                fixture_id = int(row.fixture_id)
        finally:
            s.close()
    except Exception as e:  # noqa: BLE001
        logger.debug(f"[chot] fixture_id lookup failed for match_id={match.match_id}: {e}")

    # --- Lineup (needs fixture_id) ---
    if fixture_id:
        try:
            from src.collectors.lineup import get_lineup
            lu = get_lineup(fixture_id)
            if lu:
                home_tid = match.home_api_id
                away_tid = match.away_api_id
                # Remap home/away if explicit team_ids are known and differ
                # from the first-row heuristic used by the collector.
                h_block = lu.get("home") or {}
                a_block = lu.get("away") or {}
                if home_tid and away_tid:
                    if (h_block.get("team_id") == away_tid and
                            a_block.get("team_id") == home_tid):
                        h_block, a_block = a_block, h_block
                out["lineup"] = {
                    "has_lineup": bool(lu.get("has_lineup")),
                    "home_formation": h_block.get("formation") or "N/A",
                    "away_formation": a_block.get("formation") or "N/A",
                }
        except Exception as e:  # noqa: BLE001
            logger.debug(f"[chot] lineup signal skipped fixture={fixture_id}: {e}")

    # --- Injuries (needs fixture_id + team_ids) ---
    if fixture_id and match.home_api_id and match.away_api_id:
        try:
            from src.collectors.injuries import get_injuries_by_team
            inj = get_injuries_by_team(fixture_id,
                                        int(match.home_api_id),
                                        int(match.away_api_id))
            # "key out" = players with status "Missing Fixture"
            def _key_out(bucket: list[dict]) -> int:
                return sum(1 for p in (bucket or [])
                           if (p.get("status") or "") == "Missing Fixture")
            out["injuries"] = {
                "home_key_out": _key_out(inj.get("home", [])),
                "away_key_out": _key_out(inj.get("away", [])),
            }
        except Exception as e:  # noqa: BLE001
            logger.debug(f"[chot] injuries signal skipped fixture={fixture_id}: {e}")

    return out


def _format_signals_block(signals: dict) -> str:
    """Render the 'SIGNALS TỔNG HỢP' block. Empty string if all sub-signals None."""
    if not signals or all(v is None for v in signals.values()):
        return ""
    lines = ["", "📊 SIGNALS TỔNG HỢP"]

    lu = signals.get("lineup")
    if lu is None:
        lines.append("⚠️ Lineup: chưa có fixture mapping")
    elif lu.get("has_lineup"):
        lines.append(
            f"✅ Lineup: {lu.get('home_formation', 'N/A')} vs "
            f"{lu.get('away_formation', 'N/A')}"
        )
    else:
        lines.append("⚠️ Lineup: chưa công bố")

    xg = signals.get("xg")
    if xg is None:
        lines.append("⚠️ xG form: N/A")
    else:
        lines.append(f"✅ xG form: {xg['home']:.2f} - {xg['away']:.2f}")

    inj = signals.get("injuries")
    if inj is None:
        lines.append("⚠️ Injuries: N/A")
    else:
        total = inj["home_key_out"] + inj["away_key_out"]
        emoji = "✅" if total <= 1 else "⚠️"
        lines.append(
            f"{emoji} Injuries: {inj['home_key_out']} nhà / "
            f"{inj['away_key_out']} khách (key out)"
        )
    return "\n".join(lines)


def _decide(old_ev: float, new_ev: float) -> tuple[str, str]:
    """Return (decision_code, vietnamese_label). See module docstring for rules."""
    if new_ev <= 0:
        return "drop", "❌ Bỏ KÈO"
    diff = new_ev - old_ev
    if diff >= _EV_BAND:
        return "better", "✅ ODDS TỐT HƠN"
    if abs(diff) < _EV_BAND:
        return "keep", "✅ GIỮ KÈO"
    # diff < -_EV_BAND and new_ev > 0
    return "worse", "⚠️ ODDS XẤU ĐI"


def _decision_note(decision: str, old_ev: float, new_ev: float) -> str:
    diff_pct = (new_ev - old_ev) * 100
    if decision == "drop":
        return f"EV mới {new_ev*100:+.1f}% ≤ 0"
    if decision == "better":
        return f"EV tăng {diff_pct:+.1f} điểm"
    if decision == "worse":
        return f"EV giảm {diff_pct:+.1f} điểm"
    return "EV gần như không đổi"


def _find_event(events: list[dict], home: str, away: str,
                kickoff: Optional[datetime]) -> Optional[dict]:
    """Match a Match row to an Odds-API event by team names + commence_time.

    Falls back to canonical (suffix-stripped) match if exact names don't hit.
    Kickoff is checked within ±30 min as a sanity guard.
    """
    h_norm = _norm_team(home)
    a_norm = _norm_team(away)
    for ev in events:
        ev_home = ev.get("home_team", "") or ""
        ev_away = ev.get("away_team", "") or ""
        if (_norm_team(ev_home) == h_norm and _norm_team(ev_away) == a_norm):
            if kickoff:
                ct = ev.get("commence_time")
                if ct:
                    try:
                        ev_dt = datetime.fromisoformat(
                            ct.replace("Z", "+00:00")
                        ).replace(tzinfo=None)
                    except Exception:
                        return ev
                    if abs((ev_dt - kickoff).total_seconds()) > 30 * 60:
                        continue
            return ev
    return None


def _extract_new_odds(event: dict, market: str, outcome: str) -> tuple[Optional[float], Optional[str]]:
    """Phase 1: only h2h + totals. Returns (price, bookmaker_name) or (None, None)."""
    from src.collectors.odds_api import get_best_odds

    if market not in _SUPPORTED_MARKETS:
        return None, None
    book_dict = get_best_odds(event, market)
    if not book_dict:
        return None, None
    info = book_dict.get(outcome)
    if not info:
        return None, None
    price = info.get("price")
    if not price:
        return None, None
    return float(price), info.get("bookmaker")


def _get_candidates(session, now: datetime) -> list[tuple[Prediction, Match]]:
    """Predictions with kickoff in 30-90 min, value-bet flagged,
    supported market, not yet re-checked."""
    win_start = now + timedelta(minutes=30)
    win_end = now + timedelta(minutes=90)

    rows = (
        session.query(Prediction, Match)
        .join(Match, Prediction.match_id == Match.match_id)
        .outerjoin(ChotReanalysis,
                   ChotReanalysis.prediction_id == Prediction.id)
        .filter(
            Match.utc_date >= win_start,
            Match.utc_date <= win_end,
            Match.status == "SCHEDULED",
            Prediction.is_value_bet == True,  # noqa: E712
            Prediction.market.in_(list(_SUPPORTED_MARKETS)),
            ChotReanalysis.id.is_(None),
        )
        .all()
    )
    return rows


def _format_message(match: Match, pred: Prediction, new_odds: float,
                    new_bookmaker: Optional[str], new_ev: float,
                    decision: str, decision_label: str, decision_note: str,
                    minutes_to_kickoff: int,
                    signals: Optional[dict] = None) -> str:
    from src.bot.telegram_bot import _MKT_NAMES  # local import to avoid cycle
    mkt = _MKT_NAMES.get(pred.market, pred.market)
    league = match.competition_code or "?"
    old_ev = float(pred.expected_value or 0)
    old_odds = float(pred.best_odds or 0)
    msg = (
        f"\U0001f3af RE-CHECK KÈO\n"
        f"⚽ {match.home_team} vs {match.away_team}\n"
        f"⏰ Còn {minutes_to_kickoff} phút | \U0001f3c6 {league}\n"
        f"➜ {pred.outcome} ({mkt}) @ {new_odds:.2f} "
        f"(trước: @{old_odds:.2f})\n"
        f"\U0001f4ca EV: {new_ev*100:+.1f}% (trước: {old_ev*100:+.1f}%)\n"
        f"\U0001f4cd {new_bookmaker or pred.best_bookmaker or '?'}\n"
        f"{decision_label} — {decision_note}"
    )
    if signals:
        block = _format_signals_block(signals)
        if block:
            msg += "\n" + block
    return msg


async def _reanalyze_pick(session, app, pred: Prediction, match: Match,
                          event: dict, now: datetime) -> bool:
    """Returns True if a ChotReanalysis row was written."""
    new_odds, new_bookmaker = _extract_new_odds(event, pred.market, pred.outcome or "")
    if new_odds is None:
        logger.info(
            f"[chot] no fresh odds for pred_id={pred.id} "
            f"{match.home_team} vs {match.away_team} {pred.market}/{pred.outcome}"
        )
        return False

    model_p = float(pred.model_probability or 0)
    new_ev = (new_odds * model_p) - 1
    old_ev = float(pred.expected_value or 0)
    old_odds = float(pred.best_odds or 0)
    decision, label = _decide(old_ev, new_ev)
    note = _decision_note(decision, old_ev, new_ev)

    # Persist BEFORE pushing — even if Telegram fails, DB stays consistent so
    # the next cycle won't re-check the same pred.
    row = ChotReanalysis(
        prediction_id=pred.id,
        match_id=pred.match_id,
        old_odds=old_odds,
        new_odds=new_odds,
        old_ev=old_ev,
        new_ev=new_ev,
        old_bookmaker=pred.best_bookmaker,
        new_bookmaker=new_bookmaker,
        decision=decision,
        reanalyzed_at=now,
    )
    session.add(row)
    session.commit()

    minutes_to_kickoff = max(0, int((match.utc_date - now).total_seconds() // 60)) \
        if match.utc_date else 0

    # Phase 2 signals (display-only — never affects decide/EV).
    signals = _collect_phase2_signals(match, pred)

    msg = _format_message(match, pred, new_odds, new_bookmaker, new_ev,
                          decision, label, note, minutes_to_kickoff,
                          signals=signals)
    try:
        from src.bot.telegram_bot import send_alert
        await send_alert(app, msg)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[chot] send_alert failed for pred_id={pred.id}: {e}")

    logger.info(
        f"[chot] pred_id={pred.id} {match.home_team} vs {match.away_team} "
        f"{pred.market}/{pred.outcome}: old_odds={old_odds:.2f} "
        f"new_odds={new_odds:.2f} old_ev={old_ev*100:+.1f}% "
        f"new_ev={new_ev*100:+.1f}% → {decision}"
    )
    return True


async def run_chot_cycle(app) -> None:
    """Entry point — called every 5 min from main.py scheduler."""
    from src.collectors.odds_api import get_odds
    from src.config import ODDS_SPORTS

    now = datetime.utcnow()
    session = get_session()
    written = 0
    skipped = 0
    try:
        candidates = _get_candidates(session, now)
        if not candidates:
            logger.info("[chot] no candidates in 30-90 min window")
            return

        # Group by league so we batch one get_odds call per league.
        by_league: dict[str, list[tuple[Prediction, Match]]] = {}
        for pred, match in candidates:
            lc = match.competition_code or ""
            by_league.setdefault(lc, []).append((pred, match))

        logger.info(
            f"[chot] {len(candidates)} candidate predictions across "
            f"{len(by_league)} league(s)"
        )

        for lc, items in by_league.items():
            if lc not in ODDS_SPORTS:
                logger.info(f"[chot] skip league {lc} (no ODDS_SPORTS mapping) "
                            f"— {len(items)} preds")
                skipped += len(items)
                continue
            try:
                events = await asyncio.wait_for(
                    asyncio.to_thread(get_odds, lc), timeout=30,
                )
            except Exception as e:  # noqa: BLE001
                logger.warning(f"[chot] get_odds failed for {lc}: {e}")
                skipped += len(items)
                continue
            if not events:
                logger.info(f"[chot] empty odds payload for {lc} "
                            f"— {len(items)} preds")
                skipped += len(items)
                continue

            # Dedup candidates that point to the same fixture (same canonical
            # teams + kickoff + market + outcome) — mirror /ancan dedup so we
            # don't push two notifications for one bet.
            seen: set[tuple] = set()
            for pred, match in items:
                key = (
                    _norm_team(match.home_team or ""),
                    _norm_team(match.away_team or ""),
                    match.utc_date.replace(second=0, microsecond=0).isoformat()
                    if match.utc_date else "",
                    pred.market,
                    pred.outcome or "",
                )
                if key in seen:
                    skipped += 1
                    continue
                seen.add(key)

                event = _find_event(events, match.home_team or "",
                                    match.away_team or "", match.utc_date)
                if not event:
                    skipped += 1
                    logger.info(
                        f"[chot] no event match for pred_id={pred.id} "
                        f"{match.home_team} vs {match.away_team} in {lc}"
                    )
                    continue
                try:
                    ok = await _reanalyze_pick(session, app, pred, match, event, now)
                    if ok:
                        written += 1
                    else:
                        skipped += 1
                except Exception as e:  # noqa: BLE001
                    session.rollback()
                    skipped += 1
                    logger.warning(
                        f"[chot] reanalyze failed pred_id={pred.id}: {e}"
                    )
    finally:
        session.close()

    logger.info(f"[chot] cycle done — written={written} skipped={skipped}")
