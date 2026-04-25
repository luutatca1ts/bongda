import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

INPUT_PATH = "src/live_pipeline.py"
OUTPUT_PATH = "src/live_pipeline.py.with_debug"

DEBUG_LOG_TEMPLATE = [
    'logger.info(',
    '    f"[LiveDebug] {db_match.home_team} vs {db_match.away_team} | "',
    '    f"state: min={state[\'minute\']} score={state[\'home_score\']}-{state[\'away_score\']} "',
    '    f"xg={state.get(\'home_xg\', 0):.2f}-{state.get(\'away_xg\', 0):.2f} "',
    '    f"reds={state.get(\'home_red_cards\', 0)}-{state.get(\'away_red_cards\', 0)} | "',
    '    f"lambdas: h={h_lambda:.2f} a={a_lambda:.2f} low_conf={low_conf} | "',
    '    f"probs: {model_probs}"',
    ')',
]

def main():
    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        lines = f.readlines()
    print(f"Read {len(lines)} lines from {INPUT_PATH}")
    
    target_idx = None
    for i, line in enumerate(lines):
        if "# g. Live odds" in line and "league" in line:
            target_idx = i
            print(f"Found target at line {i+1}: {line.rstrip()}")
            break
    if target_idx is None:
        print("ERROR: Target line not found. Aborting.")
        sys.exit(1)
    
    target_line = lines[target_idx]
    indent = len(target_line) - len(target_line.lstrip())
    indent_str = " " * indent
    print(f"Detected indent: {indent} spaces")
    
    preceding = "".join(lines[max(0, target_idx-25):target_idx])
    checks = {
        "h_lambda": "h_lambda" in preceding,
        "a_lambda": "a_lambda" in preceding,
        "low_conf": "low_conf" in preceding,
        "model_probs": "model_probs" in preceding,
        "state": "state" in preceding,
        "db_match": "db_match" in preceding,
    }
    print()
    print("Variable scope check:")
    all_ok = True
    for name, ok in checks.items():
        marker = "OK" if ok else "MISSING"
        print(f"  {name}: {marker}")
        if not ok:
            all_ok = False
    if not all_ok:
        print("ERROR: Some variables not in scope. Aborting.")
        sys.exit(1)
    
    debug_block_lines = []
    for tmpl_line in DEBUG_LOG_TEMPLATE:
        debug_block_lines.append(indent_str + tmpl_line + "\n")
    debug_block_lines.append("\n")
    
    new_lines = lines[:target_idx] + debug_block_lines + lines[target_idx:]
    
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.writelines(new_lines)
    print()
    print(f"Wrote {len(new_lines)} lines to {OUTPUT_PATH}")
    print(f"Inserted debug log block BEFORE line {target_idx+1}")
    
    import ast
    try:
        with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
            ast.parse(f.read())
        print("Syntax OK")
    except SyntaxError as e:
        print(f"SYNTAX ERROR: {e}")
        sys.exit(1)

main()