import sys
import io
import re
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

LOG_PATH = r"C:\Users\Administrator\.pm2\logs\main-error.log"

def main():
    with open(LOG_PATH, "r", encoding="utf-8", errors="replace") as f:
        lines = f.readlines()
    
    live_debug_lines = [l for l in lines if "[LiveDebug]" in l]
    kept_lines = [l for l in lines if "[LivePipeline]" in l and "kept=" in l]
    filtered_susp_lines = [l for l in lines if "[LivePipeline] FILTERED suspicious" in l]
    cycle_done_lines = [l for l in lines if "[LivePipeline] Cycle done" in l]
    
    print(f"=== LiveDebug lines: {len(live_debug_lines)} ===")
    print(f"=== Cycle done: {len(cycle_done_lines)} ===")
    print(f"=== kept= summary: {len(kept_lines)} ===")
    print(f"=== FILTERED suspicious: {len(filtered_susp_lines)} ===")
    print()
    
    kept_pattern = re.compile(r"\[LivePipeline\] (.+?): kept=(\d+), filtered_low_prob=(\d+), filtered_suspicious=(\d+)")
    total_kept = 0
    total_low = 0
    total_susp = 0
    matches_analyzed = 0
    per_match_kept = {}
    for line in kept_lines:
        m = kept_pattern.search(line)
        if m:
            match_name = m.group(1)
            kept = int(m.group(2))
            low = int(m.group(3))
            susp = int(m.group(4))
            total_kept += kept
            total_low += low
            total_susp += susp
            matches_analyzed += 1
            per_match_kept.setdefault(match_name, {"kept": 0, "low": 0, "susp": 0, "cycles": 0})
            per_match_kept[match_name]["kept"] += kept
            per_match_kept[match_name]["low"] += low
            per_match_kept[match_name]["susp"] += susp
            per_match_kept[match_name]["cycles"] += 1
    
    print(f"=== AGGREGATE ({matches_analyzed} match-cycles) ===")
    print(f"Total kept=:       {total_kept}")
    print(f"Total low_prob:    {total_low}")
    print(f"Total suspicious:  {total_susp}")
    print()
    print("=== PER MATCH ===")
    for match, stats in sorted(per_match_kept.items()):
        print(f"  {match}:")
        print(f"    cycles={stats['cycles']}, kept={stats['kept']}, low={stats['low']}, susp={stats['susp']}")
    print()
    
    print("=== LAST 5 LIVEDEBUG ===")
    for line in live_debug_lines[-5:]:
        print(line.strip()[:300])
        print()
    
    print("=== LAST 10 FILTERED SUSPICIOUS (live pipeline) ===")
    for line in filtered_susp_lines[-10:]:
        m = re.search(r"FILTERED suspicious VB - (.+?) \| (.+?) - EV ([\d.]+)", line)
        if m:
            print(f"  {m.group(1)} | {m.group(2)} | EV {m.group(3)}%")
        else:
            print(f"  {line.strip()[:200]}")

main()