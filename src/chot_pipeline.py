"""Pre-match odds re-check pipeline (`/chot`).

Phase 1 scope (kept narrow on purpose):
  * Find Predictions with kickoff in 30-90 min and `is_value_bet=True`
    that have NOT yet been re-checked (no row in `chot_reanalysis`).
  * Re-fetch latest odds from The Odds API (per league, batched).
  * Re-compute EV using the SAVED model probability and the new odds —
    no model re-run, no lineup/xG/injury/weather adjust (that's Phase 2).
  * Decide keep / better / worse / drop, broadcast to subscribers,
    persist a ChotReanalysis row.

Phase 2.2: Now supports h2h, totals, spreads, asian_handicap,
corners_totals, corners_spreads. Line drift handled: if the original
line is unavailable, picks the nearest line and flags in decision_note.
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


# Markets re-checked by /chot. Goals AH has two legacy keys (`spreads` and
# `asian_handicap`) produced by different code paths — both map onto
# get_spread_pairs(). H1 corners (`corners_h1_*`) are NOT included: The Odds
# API plan does not expose a separate H1-corner endpoint, so we'd have no
# fresh odds to re-check against.
_SUPPORTED_MARKETS = {
    "h2h", "totals",
    "spreads", "asian_handicap",
    "corners_totals", "corners_spreads",
}

_AH_MARKETS = {"spreads", "asian_handicap"}
_CORNER_MARKETS = {"corners_totals", "corners_spreads"}

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

    Returns a dict with four optional sub-blocks:
      * `injuries`: {"home_key_out": int, "away_key_out": int} or None
      * `lineup`:   {"has_lineup": bool, "home_formation": str,
                     "away_formation": str} or None
      * `xg`:       {"home": float, "away": float} or None
      * `steam`:    {"market", "outcome", "direction", "bookmakers_count",
                     "avg_drift_pct", "detected_at"} or None — only populated
                     when detect_steam_moves() finds a "shortening" move that
                     matches this pick's market+outcome (sharp money agrees
                     with the pick). "drifting" direction is intentionally
                     ignored to match pipeline.py semantics.

    Injuries + lineup need the API-Football fixture_id, which the Match
    model does NOT currently persist. We attempt to resolve it from the
    LiveMatchState table (live collector may have seen it), and skip the
    signal if no id is available. xG comes from Prediction.home_xg_estimate
    / away_xg_estimate populated at Poisson time.
    """
    out: dict = {"injuries": None, "lineup": None, "xg": None, "steam": None}

    # --- xG form (from saved prediction λ) ---
    try:
        h_xg = pred.home_xg_estimate
        a_xg = pred.away_xg_estimate
        if h_xg is not None and a_xg is not None and (h_xg > 0 or a_xg > 0):
            out["xg"] = {"home": float(h_xg), "away": float(a_xg)}
    except Exception as e:  # noqa: BLE001
        logger.debug(f"[chot] xg signal skipped pred_id={pred.id}: {e}")

    # --- Tier 1: LiveMatchState (authoritative for live, populated by live pipeline) ---
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

    # --- Tier 2 (Phase 2.1): pre-match resolver via API-Football /fixtures ---
    # Only runs when LiveMatchState missed (pre-match scenario). Gated by flag:
    #   log_only = call resolver + log result, but DON'T assign fixture_id.
    #   on       = call resolver + assign fixture_id → lineup/injuries fire.
    if fixture_id is None:
        try:
            from src.config import USE_PREMATCH_FIXTURE_RESOLVER
            flag = USE_PREMATCH_FIXTURE_RESOLVER
        except Exception:
            flag = "off"
        if flag in ("log_only", "on") and match.home_api_id and match.away_api_id and match.utc_date:
            try:
                from src.collectors.api_football import resolve_fixture_id_prematch
                resolved = resolve_fixture_id_prematch(
                    home_api_id=int(match.home_api_id),
                    away_api_id=int(match.away_api_id),
                    kickoff_utc=match.utc_date,
                    league_api_id=int(match.home_league_id) if match.home_league_id else None,
                )
                if flag == "log_only":
                    logger.info(
                        f"[chot][prematch_resolver][LOG_ONLY] "
                        f"match_id={match.match_id} "
                        f"{match.home_team} vs {match.away_team} "
                        f"→ resolved={resolved} (NOT assigned)"
                    )
                elif flag == "on" and resolved:
                    fixture_id = resolved
                    logger.info(
                        f"[chot][prematch_resolver][ON] "
                        f"match_id={match.match_id} "
                        f"fixture_id={resolved} assigned"
                    )
            except Exception as e:  # noqa: BLE001
                logger.debug(
                    f"[chot][prematch_resolver] error match_id={match.match_id}: {e}"
                )

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

    # --- Steam Move (Phase 3): match-scoped, same market+outcome, "shortening" only ---
    # Mirrors pipeline.py:786 — sharp-money agreement with the pick. "drifting"
    # is ignored here because a drifting pick is the opposite signal. First
    # matching entry wins (detector already orders by drift magnitude).
    try:
        from src.analytics.steam_detector import detect_steam_moves
        steams = detect_steam_moves(match_id_filter=match.match_id)
        for s in steams or []:
            if (s.get("market") == pred.market
                    and s.get("outcome") == pred.outcome
                    and s.get("direction") == "shortening"):
                out["steam"] = {
                    "market": s.get("market"),
                    "outcome": s.get("outcome"),
                    "direction": s.get("direction"),
                    "bookmakers_count": s.get("bookmakers_count", 0),
                    "avg_drift_pct": s.get("avg_drift_pct"),
                    "detected_at": s.get("detected_at"),
                }
                break
    except Exception as e:  # noqa: BLE001
        logger.debug(f"[chot] steam signal skipped match_id={match.match_id}: {e}")

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

    sm = signals.get("steam")
    if sm:
        bk = sm.get("bookmakers_count", 0)
        drift = sm.get("avg_drift_pct")
        drift_str = f"{drift:+.1f}%" if isinstance(drift, (int, float)) else "n/a"
        lines.append(f"🔥 Steam Move: {bk} BK cùng hướng ({drift_str}) — ủng hộ pick")
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


def _decision_note(decision: str, old_ev: float, new_ev: float,
                   drift: Optional[dict] = None) -> str:
    diff_pct = (new_ev - old_ev) * 100
    if decision == "drop":
        base = f"EV mới {new_ev*100:+.1f}% ≤ 0"
    elif decision == "better":
        base = f"EV tăng {diff_pct:+.1f} điểm"
    elif decision == "worse":
        base = f"EV giảm {diff_pct:+.1f} điểm"
    else:
        base = "EV gần như không đổi"
    if drift:
        old_ln = drift.get("old_line")
        new_ln = drift.get("new_line")
        base += f" | line {old_ln:+g} → {new_ln:+g}"
    return base


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


import re as _re

# Accept "Home +0.5", "Away -0.25", "Oviedo +0.5", "Rayo -0.25", and the
# fallback "AH +0.5 Oviedo" format emitted by find_value_bets when the
# bookmaker outcome can't be mapped to Home/Away.
_AH_RE_SUFFIX = _re.compile(r"^(.*?)[\s]+([+-]?\d+(?:\.\d+)?)\s*$")
_AH_RE_PREFIX = _re.compile(r"^\s*AH\s+([+-]?\d+(?:\.\d+)?)\s+(.+?)\s*$", _re.IGNORECASE)
_TOTALS_RE = _re.compile(r"^\s*(over|under)\s+([+-]?\d+(?:\.\d+)?)\s*$", _re.IGNORECASE)


def _parse_ah_outcome(outcome: str) -> Optional[tuple[str, float]]:
    """Parse an Asian-Handicap outcome string.

    Accepts:
      * "Home +0.5" / "Away -0.25"            → ("Home", 0.5) / ("Away", -0.25)
      * "Oviedo +0.5" / "Rayo -0.25"          → ("Oviedo", 0.5) / ("Rayo", -0.25)
      * "AH +0.5 Oviedo"  (legacy fallback)   → ("Oviedo", 0.5)
    Returns None if the string doesn't parse.
    """
    if not outcome:
        return None
    m = _AH_RE_PREFIX.match(outcome)
    if m:
        try:
            return m.group(2).strip(), float(m.group(1))
        except ValueError:
            return None
    m = _AH_RE_SUFFIX.match(outcome)
    if m:
        try:
            return m.group(1).strip(), float(m.group(2))
        except ValueError:
            return None
    return None


def _parse_totals_outcome(outcome: str) -> Optional[tuple[str, float]]:
    """Parse an Over/Under outcome string: "Over 9.5" → ("over", 9.5)."""
    if not outcome:
        return None
    m = _TOTALS_RE.match(outcome)
    if not m:
        return None
    try:
        return m.group(1).lower(), float(m.group(2))
    except ValueError:
        return None


def _nearest(lines: list[float], target: float) -> Optional[float]:
    """Return the element of `lines` with the smallest |x - target|, or None."""
    if not lines:
        return None
    return min(lines, key=lambda x: abs(x - target))


def _extract_new_odds(event: dict, market: str, outcome: str
                      ) -> tuple[Optional[float], Optional[str], Optional[dict]]:
    """Return (price, bookmaker_name, drift_info) or (None, None, None).

    `drift_info` (when present) describes a line/point swap when the
    original pick's line is no longer quoted and the nearest-line
    fallback is used. Shape:
        {"old_line": 0.5, "new_line": 0.25, "side": "Home"}
    `drift_info` is None when the line matched exactly (or when the
    market has no notion of a line — h2h).

    Markets handled:
      * h2h, totals        → get_best_odds
      * spreads / asian_handicap → get_spread_pairs (Pinnacle pair)
      * corners_totals     → event["_corners"]["totals"]  (set by caller)
      * corners_spreads    → event["_corners"]["spreads"] (set by caller)

    Returns (None, None, None) on any failure — caller treats it as "no
    fresh odds, skip this pick".
    """
    from src.collectors.odds_api import get_best_odds, get_spread_pairs

    if market not in _SUPPORTED_MARKETS:
        return None, None, None

    # --- h2h + totals: direct best-odds lookup ---
    if market in ("h2h", "totals"):
        book_dict = get_best_odds(event, market) or {}
        info = book_dict.get(outcome)
        if not info:
            return None, None, None
        price = info.get("price")
        if not price:
            return None, None, None
        return float(price), info.get("bookmaker"), None

    # --- Goals AH: Pinnacle pair ---
    if market in _AH_MARKETS:
        parsed = _parse_ah_outcome(outcome)
        if not parsed:
            return None, None, None
        side_hint, pick_line = parsed
        pairs = get_spread_pairs(event) or []
        if not pairs:
            return None, None, None
        pair = pairs[0]
        # Decide which side of the pair matches the original pick.
        side_label_lc = side_hint.lower()
        home_name = (pair.get("home_name") or "").lower()
        away_name = (pair.get("away_name") or "").lower()
        home_ev_team = (event.get("home_team") or "").lower()
        away_ev_team = (event.get("away_team") or "").lower()

        chosen_side = None  # "home" | "away"
        if side_label_lc in ("home",) or side_label_lc == home_ev_team or side_label_lc == home_name:
            chosen_side = "home"
        elif side_label_lc in ("away",) or side_label_lc == away_ev_team or side_label_lc == away_name:
            chosen_side = "away"
        else:
            # Fall back to point-sign: favourite (negative point) is typically home
            # when points aren't symmetric. Use abs-point match as a secondary
            # tiebreaker: the side whose point is closer to pick_line wins.
            if abs(pair.get("home_point", 0) - pick_line) <= abs(pair.get("away_point", 0) - pick_line):
                chosen_side = "home"
            else:
                chosen_side = "away"

        new_point = pair.get(f"{chosen_side}_point")
        new_price = pair.get(f"{chosen_side}_price")
        if new_price is None or new_point is None:
            return None, None, None
        drift = None
        if abs(new_point - pick_line) >= 0.01:
            drift = {
                "old_line": pick_line,
                "new_line": float(new_point),
                "side": side_hint,
            }
        return float(new_price), pair.get("bookmaker"), drift

    # --- Corner totals: nearest line ---
    if market == "corners_totals":
        parsed = _parse_totals_outcome(outcome)
        if not parsed:
            return None, None, None
        ou, pick_line = parsed
        corners = (event.get("_corners") or {}).get("totals") or {}
        if not corners:
            return None, None, None
        if pick_line in corners:
            new_line = pick_line
        else:
            new_line = _nearest(list(corners.keys()), pick_line)
            if new_line is None:
                return None, None, None
        info = corners.get(new_line) or {}
        if ou == "over":
            price = info.get("over_price")
            bk = info.get("over_bk")
        else:
            price = info.get("under_price")
            bk = info.get("under_bk")
        if not price:
            return None, None, None
        drift = None
        if abs(new_line - pick_line) >= 0.01:
            drift = {
                "old_line": pick_line,
                "new_line": float(new_line),
                "side": ou.capitalize(),
            }
        return float(price), bk, drift

    # --- Corner AH: single pair (bookmaker-fallback picker) ---
    if market == "corners_spreads":
        parsed = _parse_ah_outcome(outcome)
        if not parsed:
            return None, None, None
        side_hint, pick_line = parsed
        spreads = (event.get("_corners") or {}).get("spreads") or []
        if not spreads:
            return None, None, None
        pair = spreads[0]
        home_name = (pair.get("home_name") or "").lower()
        away_name = (pair.get("away_name") or "").lower()
        side_lc = side_hint.lower()
        if side_lc == home_name or side_lc == "home":
            chosen = "home"
        elif side_lc == away_name or side_lc == "away":
            chosen = "away"
        else:
            # Fallback on nearest point.
            if abs(pair.get("home_point", 0) - pick_line) <= abs(pair.get("away_point", 0) - pick_line):
                chosen = "home"
            else:
                chosen = "away"
        new_point = pair.get(f"{chosen}_point")
        new_price = pair.get(f"{chosen}_price")
        if new_price is None or new_point is None:
            return None, None, None
        drift = None
        if abs(new_point - pick_line) >= 0.01:
            drift = {
                "old_line": pick_line,
                "new_line": float(new_point),
                "side": side_hint,
            }
        return float(new_price), pair.get("bk") or pair.get("bookmaker"), drift

    return None, None, None


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
                    signals: Optional[dict] = None,
                    drift: Optional[dict] = None) -> str:
    from src.bot.telegram_bot import _MKT_NAMES  # local import to avoid cycle
    mkt = _MKT_NAMES.get(pred.market, pred.market)
    league = match.competition_code or "?"
    old_ev = float(pred.expected_value or 0)
    old_odds = float(pred.best_odds or 0)
    drift_line = ""
    if drift:
        drift_line = (
            f"⚠️ LINE DRIFT: pick gốc {drift.get('side', '?')} "
            f"{drift.get('old_line', 0):+g}, odds mới "
            f"{drift.get('side', '?')} {drift.get('new_line', 0):+g} (gần nhất)\n"
            f"EV tính trên line mới: {new_ev*100:+.1f}%\n"
        )
    msg = (
        f"\U0001f3af RE-CHECK KÈO\n"
        f"⚽ {match.home_team} vs {match.away_team}\n"
        f"⏰ Còn {minutes_to_kickoff} phút | \U0001f3c6 {league}\n"
        f"➜ {pred.outcome} ({mkt}) @ {new_odds:.2f} "
        f"(trước: @{old_odds:.2f})\n"
        f"{drift_line}"
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
    new_odds, new_bookmaker, drift = _extract_new_odds(
        event, pred.market, pred.outcome or ""
    )
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
    note = _decision_note(decision, old_ev, new_ev, drift=drift)

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
                          signals=signals, drift=drift)
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
    from src.collectors.odds_api import get_odds, get_corner_odds
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

            # Only fetch corner odds (separate endpoint, rate-limited) if this
            # league has any corner pick in the current window. Best-effort —
            # corner picks still get skipped cleanly if the call fails.
            needs_corners = any(
                (pred.market in _CORNER_MARKETS) for pred, _ in items
            )
            corner_data: dict = {}
            if needs_corners:
                try:
                    corner_data = await asyncio.wait_for(
                        asyncio.to_thread(get_corner_odds, lc, None),
                        timeout=60,
                    )
                except Exception as e:  # noqa: BLE001
                    logger.warning(
                        f"[chot] get_corner_odds failed for {lc}: {e}"
                    )
                    corner_data = {}

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

                # Attach corner payload (if any) for this event so
                # _extract_new_odds can read event["_corners"] directly.
                if corner_data and pred.market in _CORNER_MARKETS:
                    ckey = f"{event.get('home_team', '')}__{event.get('away_team', '')}"
                    event["_corners"] = corner_data.get(ckey) or {}

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
