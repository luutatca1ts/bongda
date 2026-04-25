import re

with open(r"src\bot\telegram_bot.py", encoding="utf-8") as f:
    lines = f.readlines()

# Find f-strings with \u escape inside {...}
pattern = re.compile(r'f["\'].*\{[^}]*\\u[0-9a-fA-F]{4}[^}]*\}')

bad = []
for i, line in enumerate(lines, start=1):
    if pattern.search(line):
        bad.append((i, line.rstrip()))

print(f"Found {len(bad)} lines with backslash in f-string expression:")
for n, l in bad:
    print(f"  line {n}: {l[:150]}")