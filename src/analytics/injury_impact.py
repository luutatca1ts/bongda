"""Injury impact → λ multipliers for the prediction model.

Converts a list of injured players per team into attack/defense
multipliers that are applied to Dixon-Coles λ_home / λ_away.

Weights are conservative — single player rarely accounts for >15%
shift in goals. Sum is capped at 0.30 (30%) to avoid over-adjustment
when teams report many non-starters.
"""

from __future__ import annotations

# Per-position impact. Attack weight subtracts from that team's OFFENSIVE
# multiplier; Defense weight adds to the *opponent's* attack multiplier
# via a 1 + sum(defense) factor applied in DixonColesModel.predict().
POSITION_WEIGHTS = {
    "Goalkeeper":  {"attack": 0.00, "defense": 0.15},
    "Defender":    {"attack": 0.00, "defense": 0.08},
    "Midfielder":  {"attack": 0.03, "defense": 0.03},  # split: partial both sides
    "Attacker":    {"attack": 0.10, "defense": 0.00},
}

MAX_ATTACK_DROP = 0.30   # cap on cumulative attack loss (-30%)
MAX_DEFENSE_DROP = 0.30  # cap on cumulative defense loss (-30%)


def calculate_injury_adjustment(injuries: list[dict]) -> dict:
    """Compute {attack_mult, defense_mult, key_player_count} from injury list.

    injuries: list of {player_name, position, reason, status}.
    Returns: {
        "attack_mult":  float  (multiplicative; 1.0 = no effect, <1.0 weaker),
        "defense_mult": float  (multiplicative applied to team's own defense
                                strength; <1.0 means defense WEAKER so the
                                opponent scores more),
        "count":        int    (number of injured players factored in),
        "offensive_drop": float  (total offensive weight, pre-cap),
        "defensive_drop": float  (total defensive weight, pre-cap),
    }

    Usage in DixonColesModel.predict():
      λ_home_new = λ_home * home.attack_mult * (2 - away.defense_mult)
      λ_away_new = λ_away * away.attack_mult * (2 - home.defense_mult)

    Equivalently, DC.predict() expects the caller to pass:
      home = {"attack_mult": 1 - offensive_drop, "defense_mult": 1 + defensive_drop}
    so the sign convention is already folded in.
    """
    if not injuries:
        return {"attack_mult": 1.0, "defense_mult": 1.0, "count": 0,
                "offensive_drop": 0.0, "defensive_drop": 0.0}

    off_drop = 0.0
    def_drop = 0.0
    for inj in injuries:
        pos = inj.get("position", "Midfielder")
        w = POSITION_WEIGHTS.get(pos, POSITION_WEIGHTS["Midfielder"])
        off_drop += w["attack"]
        def_drop += w["defense"]

    off_drop_capped = min(off_drop, MAX_ATTACK_DROP)
    def_drop_capped = min(def_drop, MAX_DEFENSE_DROP)

    return {
        "attack_mult": 1.0 - off_drop_capped,
        "defense_mult": 1.0 + def_drop_capped,  # >1.0 means weaker defense
        "count": len(injuries),
        "offensive_drop": off_drop_capped,
        "defensive_drop": def_drop_capped,
    }


def count_key_players_out(injuries: list[dict]) -> int:
    """Count 'key' outs — goalkeeper, striker, centre-back injuries are
    heavier than generic squad rotations. Used for alert warning thresholds.
    """
    key = 0
    for inj in injuries:
        pos = inj.get("position", "")
        status = inj.get("status", "")
        if status == "Missing Fixture" and pos in ("Goalkeeper", "Defender", "Attacker"):
            key += 1
    return key


def summarize_injuries(injuries_dict: dict) -> dict:
    """Convenience: summarize both home+away for alert rendering.

    injuries_dict: {"home": [...], "away": [...]}
    Returns: {
        "home": {attack_mult, defense_mult, count, names},
        "away": {attack_mult, defense_mult, count, names},
    }
    """
    out = {}
    for bucket in ("home", "away"):
        lst = injuries_dict.get(bucket, []) or []
        adj = calculate_injury_adjustment(lst)
        adj["names"] = [inj.get("player_name", "?") for inj in lst]
        adj["key_out"] = count_key_players_out(lst)
        out[bucket] = adj
    return out
