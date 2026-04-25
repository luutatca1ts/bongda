import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
import json
import re
from collections import defaultdict

MAPPING_PATH = "artifacts/team_mapping.json"

def normalize(name):
    s = name.lower()
    for token in [" fc", " cf", " sc", " afc", " ac", " bk", " if", " nfc", " nk", " fk"]:
        if s.endswith(token):
            s = s[:-len(token)]
        if s.startswith(token.strip() + " "):
            s = s[len(token.strip())+1:]
    s = re.sub(r"[\.\-_']", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def main():
    with open(MAPPING_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    mapping = data["mappings"]
    
    print(f"Loaded {len(mapping)} entries")
    print()
    
    print("=" * 60)
    print("CHECK 1: Self-mismatch (key vs api_name)")
    print("=" * 60)
    self_mismatch = []
    for key, info in mapping.items():
        api_name = info.get("api_name", "")
        if not api_name:
            continue
        n_key = normalize(key)
        n_api = normalize(api_name)
        if n_key != n_api:
            self_mismatch.append((key, api_name, info.get("api_id"), info.get("league_id")))
    
    print(f"Found {len(self_mismatch)} self-mismatches:")
    print()
    for key, api_name, api_id, league_id in self_mismatch:
        print(f"  key=[{key}] api_name=[{api_name}] api_id={api_id} league_id={league_id}")
    
    print()
    print("=" * 60)
    print("CHECK 2: Substring collision (same api_id, keys nested)")
    print("=" * 60)
    
    by_api_id = defaultdict(list)
    for key, info in mapping.items():
        aid = info.get("api_id")
        if aid is not None:
            by_api_id[aid].append((key, info))
    
    collisions = []
    for aid, entries in by_api_id.items():
        if len(entries) < 2:
            continue
        keys = [e[0] for e in entries]
        found_pair = False
        for i, k1 in enumerate(keys):
            for k2 in keys[i+1:]:
                n1, n2 = normalize(k1), normalize(k2)
                if n1 != n2 and (n1 in n2 or n2 in n1):
                    collisions.append((aid, entries))
                    found_pair = True
                    break
            if found_pair:
                break
    
    print(f"Found {len(collisions)} suspect collisions:")
    print()
    for aid, entries in collisions:
        print(f"  api_id={aid}:")
        for key, info in entries:
            print(f"    key=[{key}] api_name=[{info.get('api_name')}] league_id={info.get('league_id')}")
        print()
    
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Self-mismatches: {len(self_mismatch)}")
    print(f"Substring collisions: {len(collisions)}")
    print(f"Total entries: {len(mapping)}")

main()