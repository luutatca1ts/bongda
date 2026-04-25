import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
import json
from datetime import datetime

INPUT_PATH = "artifacts/team_mapping.json"
OUTPUT_PATH = "artifacts/team_mapping.json.fixed"

REMAPS = {
    "Paris Saint-Germain FC": {"api_id": 85,    "api_name": "Paris Saint Germain"},
    "RCD Espanyol de Barcelona": {"api_id": 540, "api_name": "Espanyol", "league_id": 140},
    "Racing Club de Lens": {"api_id": 116,  "api_name": "Lens", "league_id": 61},
    "Santos Laguna": {"api_id": 2285, "api_name": "Santos Laguna", "league_id": 262},
    "Independiente del Valle": {"api_id": 1153, "api_name": "Independiente del Valle", "league_id": 11},
    "Barcelona SC": {"api_id": 1152, "api_name": "Barcelona SC", "league_id": 13},
    "Everton de Viña del Mar": {"api_id": 2325, "api_name": "Everton de Vina", "league_id": 11},
    "Athletic Club (MG)": {"api_id": 13975, "api_name": "Athletic Club", "league_id": 72},
}

DELETES = [
    "Queens Park Rangers",
    "Queens Park Rangers FC",
    "Independiente Medellín",
    "Independiente Rivadavia",
    "Independiente Santa Fe",
    "Club Independiente Petrolero",
    "England",
]

def main():
    print(f"Reading {INPUT_PATH}...")
    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    mappings = data.get("mappings", {})
    print(f"Loaded {len(mappings)} mappings")
    print()
    
    print("=" * 60)
    print("APPLYING REMAPS")
    print("=" * 60)
    remap_done = 0
    remap_missing = []
    for key, changes in REMAPS.items():
        if key not in mappings:
            remap_missing.append(key)
            print(f"  [MISS] key not found: '{key}'")
            continue
        before = dict(mappings[key])
        for field, new_val in changes.items():
            mappings[key][field] = new_val
        mappings[key]["reason"] = "Manual fix 2026-04-22"
        after = dict(mappings[key])
        print(f"  [OK]   '{key}':")
        print(f"           BEFORE: api_id={before.get('api_id')} api_name='{before.get('api_name')}' league_id={before.get('league_id')}")
        print(f"           AFTER:  api_id={after.get('api_id')} api_name='{after.get('api_name')}' league_id={after.get('league_id')}")
        remap_done += 1
    
    print()
    print("=" * 60)
    print("APPLYING DELETES")
    print("=" * 60)
    delete_done = 0
    delete_missing = []
    for key in DELETES:
        if key not in mappings:
            delete_missing.append(key)
            print(f"  [MISS] key not found: '{key}'")
            continue
        removed = mappings.pop(key)
        print(f"  [OK]   deleted '{key}' (was api_id={removed.get('api_id')} api_name='{removed.get('api_name')}')")
        delete_done += 1
    
    data["mappings"] = mappings
    if "stats" in data and isinstance(data["stats"], dict):
        data["stats"]["manual_fix"] = data["stats"].get("manual_fix", 0) + remap_done + delete_done
    data["last_manual_fix"] = datetime.utcnow().isoformat() + "Z"
    
    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Remaps applied: {remap_done} / {len(REMAPS)}")
    if remap_missing:
        print(f"Remaps missed:  {remap_missing}")
    print(f"Deletes applied: {delete_done} / {len(DELETES)}")
    if delete_missing:
        print(f"Deletes missed:  {delete_missing}")
    print(f"Final mappings count: {len(mappings)}")
    
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print()
    print(f"Wrote output to: {OUTPUT_PATH}")

main()