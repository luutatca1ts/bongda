"""Patch pipeline.py — add UPDATE branch to sync Match results (v2, regex-based)."""
from pathlib import Path
import re
import ast

PATH = Path("src/pipeline.py")
src = PATH.read_text(encoding="utf-8")

# Already applied?
if "# UPDATE: sync result when match already exists" in src:
    raise SystemExit("ERROR: patch already applied, aborting")

# Regex pattern:
# Match the closing of session.add(Match(...)) in the training branch
# specifically the one with status="FINISHED" followed by )) then newline
# then next non-whitespace line (not part of this block)
pattern = re.compile(
    r'(status="FINISHED",\s*\r?\n\s*\)\))',
    re.MULTILINE
)

matches = list(pattern.finditer(src))
print(f"Found {len(matches)} candidate locations")
for i, m in enumerate(matches):
    # Show context 
    start = max(0, m.start() - 80)
    end = min(len(src), m.end() + 20)
    print(f"\n--- Match {i} at {m.start()}-{m.end()} ---")
    print(src[start:end])

if len(matches) == 0:
    raise SystemExit("ERROR: no marker found at all")
if len(matches) > 1:
    raise SystemExit(f"ERROR: multiple matches ({len(matches)}), need unique")

# Single match - insert else block after it
m = matches[0]
end_pos = m.end()

else_block = """
            else:
                # UPDATE: sync result when match already exists but has no goals yet
                if existing.home_goals is None and r.get("home_goals") is not None:
                    existing.home_goals = r["home_goals"]
                    existing.away_goals = r["away_goals"]
                    existing.status = "FINISHED"
                    logger.info(
                        f"[Pipeline] Updated match {r['match_id']} "
                        f"({r.get('home_team')} vs {r.get('away_team')}): "
                        f"{r['home_goals']}-{r['away_goals']}"
                    )"""

new_src = src[:end_pos] + else_block + src[end_pos:]

# Syntax check
try:
    ast.parse(new_src)
    print("\nNew source parses OK")
except SyntaxError as e:
    print(f"\nERROR: SyntaxError after patch: {e}")
    raise SystemExit(1)

PATH.write_text(new_src, encoding="utf-8")
print(f"Patched {PATH} successfully")
print(f"Added {len(else_block)} chars")