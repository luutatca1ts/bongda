"""One-shot patch for Step 4d — replace totals block in live_pipeline.py.

Finds the totals block (from '# --- totals ---' to 'return value_bets')
and replaces with new version containing 2 filters + summary log.
"""
from pathlib import Path

PATH = Path("src/live_pipeline.py")
src = PATH.read_text(encoding="utf-8")

# Find start of totals block
start_marker = "    # --- totals ---"
start_idx = src.find(start_marker)
if start_idx == -1:
    raise SystemExit("ERROR: totals marker not found")

# Find end: 'return value_bets' AFTER start
end_marker = "    return value_bets"
end_idx = src.find(end_marker, start_idx)
if end_idx == -1:
    raise SystemExit("ERROR: return value_bets not found after totals")
end_idx += len(end_marker)

old_block = src[start_idx:end_idx]
print(f"Found old totals block, {len(old_block)} chars, {old_block.count(chr(10))} lines")

new_block = '''    # --- totals ---
    totals_model = model_probs.get("totals", {})
    totals_odds = _best_live_odds(odds_event, "totals")
    for outcome_name, od in totals_odds.items():
        # Pinnacle tra "Over"/"Under" + point
        point = od.get("point")
        price = od.get("price", 0.0) or 0.0
        if point is None or price <= 1.01:
            continue
        line_probs = totals_model.get(float(point))
        if not line_probs:
            continue
        prob = line_probs.get(outcome_name, 0.0)
        if prob <= 0:
            continue
        # Filter 1: prob threshold
        if prob < min_prob:
            filtered_low_prob += 1
            continue
        ev = prob * price - 1
        if ev < min_ev:
            continue
        # Filter 2: suspicious VB
        _outcome_label = f"{outcome_name} {point}"
        _vb_check = {
            "ev": ev,
            "bookmaker": od.get("bookmaker", "Pinnacle") or "",
            "market": "totals",
            "outcome": _outcome_label,
        }
        _susp, _reason = _is_ev_suspicious(_vb_check)
        if _susp:
            filtered_suspicious += 1
            logger.warning(
                f"[LivePipeline] FILTERED suspicious VB - "
                f"{home_team} vs {away_team} | totals:{_outcome_label} @ {price} "
                f"(EV {ev*100:+.1f}%, bk={od.get('bookmaker', 'N/A')}) - {_reason}"
            )
            continue
        value_bets.append({
            "market": "totals",
            "outcome": _outcome_label,
            "probability": prob,
            "odds": price,
            "bookmaker": od.get("bookmaker", "Pinnacle"),
            "ev": ev,
            "confidence": _live_confidence(ev),
        })

    # Summary log if filters caught anything
    if filtered_low_prob > 0 or filtered_suspicious > 0:
        logger.info(
            f"[LivePipeline] {home_team} vs {away_team}: "
            f"kept={len(value_bets)}, filtered_low_prob={filtered_low_prob}, "
            f"filtered_suspicious={filtered_suspicious}"
        )

    return value_bets'''

new_src = src[:start_idx] + new_block + src[end_idx:]

# Verify syntax before writing
import ast
try:
    ast.parse(new_src)
    print("New source parses OK")
except SyntaxError as e:
    print(f"ERROR: new source has SyntaxError: {e}")
    raise SystemExit(1)

PATH.write_text(new_src, encoding="utf-8")
print(f"Patched {PATH} successfully")
print(f"Old block: {len(old_block)} chars, new block: {len(new_block)} chars")