import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
import json

RAW_PATH = "artifacts/api_football_teams_raw.json"

# List search terms để tra api_id đúng
# Format: (hint_name, league_hint_id hoặc None)
SEARCHES = [
    ("Paris Saint", 61),          # PSG ở Ligue 1
    ("Espanyol", 140),             # La Liga
    ("Lens", 61),                  # Ligue 1
    ("Santos Laguna", None),       # Liga MX
    ("Queens Park Rangers", None), # QPR (Championship hoặc Premier)
    ("Independiente Medell", None),
    ("Independiente Rivadavia", None),
    ("Independiente Santa Fe", None),
    ("Independiente del Valle", None),
    ("Independiente Petrolero", None),
    ("Barcelona SC", None),        # Ecuador
    ("Everton", None),             # có thể có nhiều Everton
    ("Athletic Club", None),       # Brazil (MG) vs Bilbao
]

def main():
    with open(RAW_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    leagues = data.get("leagues", {})
    print(f"Loaded {len(leagues)} leagues")
    print()
    
    for hint_name, league_hint in SEARCHES:
        print(f"=== Searching: '{hint_name}'", end="")
        if league_hint:
            print(f" (league={league_hint})", end="")
        print(" ===")
        
        matches = []
        for lid, teams in leagues.items():
            if league_hint and str(lid) != str(league_hint):
                continue
            for t in teams:
                name = t.get("name", "")
                if hint_name.lower() in name.lower():
                    matches.append((lid, t.get("id"), name))
        
        # Dedupe by (id, name)
        seen = set()
        unique = []
        for lid, tid, name in matches:
            key = (tid, name)
            if key not in seen:
                seen.add(key)
                unique.append((lid, tid, name))
        
        if not unique:
            print("  NO MATCH")
        else:
            for lid, tid, name in unique[:10]:  # max 10
                print(f"  league={lid} id={tid} name='{name}'")
        print()

main()