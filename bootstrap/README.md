# bootstrap/

Manual, one-shot utilities. **Not** registered with pm2/apscheduler.

## team_mapping_bootstrap.py — Phase B1

Builds `team_name → api_football_id` mapping from the `matches` table
against API-Football `/teams?league={id}&season=2025`.

### Run

```bash
pip install -r requirements.txt            # rapidfuzz added in B1
python bootstrap/team_mapping_bootstrap.py             # normal
python bootstrap/team_mapping_bootstrap.py --dry-run   # console only
python bootstrap/team_mapping_bootstrap.py --force-refresh-api
```

Expects `.env` with `API_FOOTBALL_KEY`. Reuses the cache at
`artifacts/api_football_teams_raw.json` across runs — a crash during
step 2 means the whole step is redone next run (quota is plentiful on
MEGA plan, no checkpointing needed).

### Outputs (in `artifacts/`)

| File | Contents |
|---|---|
| `team_mapping.json` | auto-matched (token_set_ratio ≥ 95) — machine-readable |
| `team_mapping_review.csv` | 85–94 — fill `decision` column with Y/N |
| `team_mapping_unmatched.csv` | < 85 — top-3 candidates shown for manual decision |

### Tuning

- `AUTO_THRESHOLD = 95`, `REVIEW_THRESHOLD = 85` — edit in the script.
- Scorer is `rapidfuzz.fuzz.token_set_ratio` (handles "Manchester United
  FC" vs "Manchester Utd" well).

### Scope (B1)

Writes artifacts only. Does **not** migrate DB, touch pipeline, or edit
`Match` / `Prediction`. Consuming the mapping lives in Phase B2.

## Next (Phase B2, not yet implemented)

- Add `api_football_id` column to `matches` (idempotent migration)
- Backfill from `artifacts/team_mapping.json`
- Wire collectors (injuries, weather, live xG) to use the ID instead of
  string team names
