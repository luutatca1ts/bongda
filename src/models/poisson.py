"""Poisson-based football prediction model.

Uses historical goal data to estimate attack/defense strengths per team,
then predicts match outcome probabilities via Poisson distribution.
"""

import numpy as np
from scipy.stats import poisson
from collections import defaultdict


class PoissonModel:
    def __init__(self):
        self.attack_strength = {}   # team -> attack rating
        self.defense_strength = {}  # team -> defense rating
        self.league_avg_goals = 0.0
        self._fitted = False

    def fit(self, results: list[dict]):
        """
        Fit model on historical results.
        Each result: {home_team, away_team, home_goals, away_goals}
        """
        if not results:
            return

        # Filter out results with missing goals
        results = [r for r in results if r.get("home_goals") is not None]
        if not results:
            return

        total_home_goals = sum(r["home_goals"] for r in results)
        total_away_goals = sum(r["away_goals"] for r in results)
        n = len(results)

        avg_home = total_home_goals / n
        avg_away = total_away_goals / n
        self.league_avg_goals = (avg_home + avg_away) / 2

        # Count goals scored/conceded per team
        home_scored = defaultdict(list)
        home_conceded = defaultdict(list)
        away_scored = defaultdict(list)
        away_conceded = defaultdict(list)

        for r in results:
            home_scored[r["home_team"]].append(r["home_goals"])
            home_conceded[r["home_team"]].append(r["away_goals"])
            away_scored[r["away_team"]].append(r["away_goals"])
            away_conceded[r["away_team"]].append(r["home_goals"])

        all_teams = set(home_scored.keys()) | set(away_scored.keys())

        for team in all_teams:
            h_scored = home_scored.get(team, [])
            a_scored = away_scored.get(team, [])
            h_conceded = home_conceded.get(team, [])
            a_conceded = away_conceded.get(team, [])

            goals_scored = h_scored + a_scored
            goals_conceded = h_conceded + a_conceded

            if goals_scored:
                self.attack_strength[team] = np.mean(goals_scored) / max(self.league_avg_goals, 0.01)
            else:
                self.attack_strength[team] = 1.0

            if goals_conceded:
                self.defense_strength[team] = np.mean(goals_conceded) / max(self.league_avg_goals, 0.01)
            else:
                self.defense_strength[team] = 1.0

        self._fitted = True

    def predict(self, home_team: str, away_team: str, home_advantage: float = 1.25) -> dict:
        """
        Predict match probabilities.
        Returns probabilities for: Home, Draw, Away, Over/Under 2.5, BTTS
        """
        if not self._fitted:
            return self._default_prediction()

        home_atk = self.attack_strength.get(home_team, 1.0)
        away_def = self.defense_strength.get(away_team, 1.0)
        away_atk = self.attack_strength.get(away_team, 1.0)
        home_def = self.defense_strength.get(home_team, 1.0)

        # Expected goals
        home_xg = home_atk * away_def * self.league_avg_goals * home_advantage
        away_xg = away_atk * home_def * self.league_avg_goals

        # Clamp to reasonable range
        home_xg = np.clip(home_xg, 0.2, 5.0)
        away_xg = np.clip(away_xg, 0.2, 5.0)

        # Build goal probability matrix (0-6 goals each)
        max_goals = 7
        home_probs = [poisson.pmf(i, home_xg) for i in range(max_goals)]
        away_probs = [poisson.pmf(i, away_xg) for i in range(max_goals)]

        # Joint probability matrix
        matrix = np.outer(home_probs, away_probs)

        # 1X2 probabilities
        p_home = np.sum(np.tril(matrix, -1))  # home goals > away goals
        p_draw = np.trace(matrix)
        p_away = np.sum(np.triu(matrix, 1))   # away goals > home goals

        # Normalize
        total = p_home + p_draw + p_away
        p_home /= total
        p_draw /= total
        p_away /= total

        # Over/Under — all common lines
        # Half-goal lines: prob = P(total > line) since no push
        # Quarter-goal lines (e.g. 2.25, 2.75): split bet
        #   Over 2.25 = half bet on Over 2.0 + half bet on Over 2.5
        #   Over 2.75 = half bet on Over 2.5 + half bet on Over 3.0
        ou_probs = {}
        for threshold in range(0, 7):  # 0,1,2,3,4,5,6
            ou_probs[threshold] = sum(
                matrix[i][j] for i in range(max_goals) for j in range(max_goals) if i + j <= threshold
            )

        # Half-goal lines (no push possible)
        p_under_15 = ou_probs[1]
        p_over_15 = 1.0 - p_under_15
        p_under_25 = ou_probs[2]
        p_over_25 = 1.0 - p_under_25
        p_under_35 = ou_probs[3]
        p_over_35 = 1.0 - p_under_35

        # Whole-goal lines (push = exact total)
        # P(Under N.0) = P(total < N), P(Over N.0) = P(total > N), push = P(total == N)
        # Quarter-goal lines (split bet: average of adjacent)
        ou_all = {}
        for line_x10 in [15, 20, 25, 27, 30, 32, 35, 37, 40, 45]:
            line = line_x10 / 10.0
            if line == int(line):
                # Whole number: under = total < line, over = total > line
                n = int(line)
                p_exact = sum(matrix[i][j] for i in range(max_goals) for j in range(max_goals) if i + j == n)
                p_under = ou_probs.get(n - 1, 0)
                p_over = 1.0 - ou_probs.get(n, 0)
                ou_all[line] = {"over": round(p_over, 4), "under": round(p_under, 4)}
            elif line % 1 == 0.5:
                # Half-goal: clean, no push
                n = int(line)
                p_under = ou_probs.get(n, 0)
                p_over = 1.0 - p_under
                ou_all[line] = {"over": round(p_over, 4), "under": round(p_under, 4)}
            else:
                # Quarter-goal: average of adjacent half lines
                lower = line - 0.25  # e.g. 2.75 -> 2.5
                upper = line + 0.25  # e.g. 2.75 -> 3.0
                n_lower = int(lower)
                n_upper = int(upper)
                p_over_lower = 1.0 - ou_probs.get(n_lower, 0)
                p_over_upper = 1.0 - ou_probs.get(n_upper, 0)
                p_over = (p_over_lower + p_over_upper) / 2
                p_under = 1.0 - p_over
                ou_all[line] = {"over": round(p_over, 4), "under": round(p_under, 4)}

        # BTTS (Both Teams To Score)
        p_btts_yes = 1.0 - sum(matrix[0, :]) - sum(matrix[:, 0]) + matrix[0][0]
        p_btts_no = 1.0 - p_btts_yes

        # Asian Handicap probabilities
        # Calculate P(home wins by exactly N goals) for various handicaps
        asian_handicap = {}
        for line in [-2.5, -2.25, -2.0, -1.75, -1.5, -1.25, -1.0, -0.75, -0.5, -0.25, 0, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0, 2.25, 2.5]:
            # Handicap applied to HOME team
            # line = -1.5 means home gives 1.5 goals → home must win by 2+
            p_home_cover = 0.0
            p_away_cover = 0.0
            p_push = 0.0
            for i in range(max_goals):
                for j in range(max_goals):
                    diff = (i + line) - j  # home goals + handicap - away goals
                    if abs(diff) < 1e-9:  # push (whole number handicaps)
                        p_push += matrix[i][j]
                    elif diff > 0:
                        p_home_cover += matrix[i][j]
                    else:
                        p_away_cover += matrix[i][j]

            # For half-goal handicaps, no push possible
            # Use +g format to get clean keys: -1.25, -1.5, -1, +0.5, etc.
            key = f"{line:+g}" if line != 0 else "0"
            asian_handicap[key] = {
                "home": round(p_home_cover, 4),
                "away": round(p_away_cover, 4),
                "push": round(p_push, 4),
            }

        # === CORNERS prediction (estimated from xG) ===
        # Average ~10.5 corners per match in top leagues
        # More attacking teams (higher xG) tend to win more corners
        avg_corners = 10.5
        home_corner_xg = (home_atk * 0.6 + away_def * 0.4) * (avg_corners / 2) * 1.05  # slight home advantage
        away_corner_xg = (away_atk * 0.6 + home_def * 0.4) * (avg_corners / 2)
        total_corner_xg = np.clip(home_corner_xg + away_corner_xg, 6.0, 18.0)

        # Clamp individual corner xG
        home_corner_xg = np.clip(home_corner_xg, 2.0, 10.0)
        away_corner_xg = np.clip(away_corner_xg, 2.0, 10.0)

        # Use Poisson for corner totals
        corner_probs = {}
        for line in [7.5, 8.5, 9.5, 10.5, 11.5, 12.5]:
            # P(total corners > line) using Poisson CDF
            p_under = sum(poisson.pmf(i, total_corner_xg) for i in range(int(line) + 1))
            p_over = 1.0 - p_under
            corner_probs[line] = {
                "over": round(p_over, 4),
                "under": round(p_under, 4),
            }

        # Corner Asian Handicap (home corners - away corners + handicap)
        max_corners = 15
        home_corner_probs = [poisson.pmf(i, home_corner_xg) for i in range(max_corners)]
        away_corner_probs = [poisson.pmf(i, away_corner_xg) for i in range(max_corners)]
        corner_matrix = np.outer(home_corner_probs, away_corner_probs)

        corner_ah = {}
        for hdp in [-8.5, -8, -7.5, -7, -6.5, -6, -5.5, -5, -4.5, -4, -3.5, -3, -2.5, -2, -1.5, -1, -0.5, 0, 0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5, 5, 5.5, 6, 6.5, 7, 7.5, 8, 8.5]:
            p_home_cover = 0.0
            p_away_cover = 0.0
            for i in range(max_corners):
                for j in range(max_corners):
                    diff = (i + hdp) - j  # home corners + handicap - away corners
                    if diff > 0:
                        p_home_cover += corner_matrix[i][j]
                    elif diff < 0:
                        p_away_cover += corner_matrix[i][j]
            key = f"{hdp:+g}"
            corner_ah[key] = {
                "home": round(p_home_cover, 4),
                "away": round(p_away_cover, 4),
            }

        # === FIRST HALF CORNERS prediction ===
        # Empirically ~43-47% of full-match corners occur in the 1st half
        h1_ratio = 0.45
        h1_total_xc = total_corner_xg * h1_ratio
        h1_home_xc = home_corner_xg * h1_ratio
        h1_away_xc = away_corner_xg * h1_ratio
        h1_total_xc = np.clip(h1_total_xc, 2.5, 9.0)
        h1_home_xc = np.clip(h1_home_xc, 1.0, 5.5)
        h1_away_xc = np.clip(h1_away_xc, 1.0, 5.5)

        # H1 corner O/U
        h1_corner_probs = {}
        for line in [3.5, 4.5, 5.5, 6.5]:
            p_under = sum(poisson.pmf(i, h1_total_xc) for i in range(int(line) + 1))
            p_over = 1.0 - p_under
            h1_corner_probs[line] = {
                "over": round(p_over, 4),
                "under": round(p_under, 4),
            }

        # H1 corner AH
        max_h1c = 10
        h1_home_probs = [poisson.pmf(i, h1_home_xc) for i in range(max_h1c)]
        h1_away_probs = [poisson.pmf(i, h1_away_xc) for i in range(max_h1c)]
        h1_corner_matrix = np.outer(h1_home_probs, h1_away_probs)

        h1_corner_ah = {}
        for hdp in [-3.5, -3, -2.5, -2, -1.5, -1, -0.5, 0, 0.5, 1, 1.5, 2, 2.5, 3, 3.5]:
            p_hc = 0.0
            p_ac = 0.0
            for i in range(max_h1c):
                for j in range(max_h1c):
                    diff = (i + hdp) - j
                    if diff > 0:
                        p_hc += h1_corner_matrix[i][j]
                    elif diff < 0:
                        p_ac += h1_corner_matrix[i][j]
            key = f"{hdp:+g}"
            h1_corner_ah[key] = {
                "home": round(p_hc, 4),
                "away": round(p_ac, 4),
            }

        return {
            "home_xg": round(home_xg, 2),
            "away_xg": round(away_xg, 2),
            "h2h": {
                "Home": round(p_home, 4),
                "Draw": round(p_draw, 4),
                "Away": round(p_away, 4),
            },
            "totals": {
                **{f"Over {line}": data["over"] for line, data in ou_all.items()},
                **{f"Under {line}": data["under"] for line, data in ou_all.items()},
            },
            "btts": {
                "Yes": round(p_btts_yes, 4),
                "No": round(p_btts_no, 4),
            },
            "asian_handicap": asian_handicap,
            "corners": {
                "xg": round(total_corner_xg, 1),
                "home_xc": round(home_corner_xg, 1),
                "away_xc": round(away_corner_xg, 1),
                "lines": corner_probs,
                "asian_handicap": corner_ah,
            },
            "corners_h1": {
                "xg": round(h1_total_xc, 1),
                "home_xc": round(h1_home_xc, 1),
                "away_xc": round(h1_away_xc, 1),
                "lines": h1_corner_probs,
                "asian_handicap": h1_corner_ah,
            },
        }

    def _default_prediction(self) -> dict:
        return {
            "home_xg": 1.3,
            "away_xg": 1.0,
            "h2h": {"Home": 0.45, "Draw": 0.27, "Away": 0.28},
            "totals": {
                "Over 1.5": 0.72, "Under 1.5": 0.28,
                "Over 2.5": 0.50, "Under 2.5": 0.50,
                "Over 3.5": 0.28, "Under 3.5": 0.72,
            },
            "btts": {"Yes": 0.48, "No": 0.52},
            "asian_handicap": {
                "-0.5": {"home": 0.45, "away": 0.55, "push": 0},
                "0": {"home": 0.45, "away": 0.28, "push": 0.27},
                "+0.5": {"home": 0.72, "away": 0.28, "push": 0},
            },
            "corners": {
                "xg": 10.5,
                "home_xc": 5.5,
                "away_xc": 5.0,
                "lines": {
                    9.5: {"over": 0.58, "under": 0.42},
                    10.5: {"over": 0.50, "under": 0.50},
                    11.5: {"over": 0.39, "under": 0.61},
                },
                "asian_handicap": {
                    "-1.5": {"home": 0.42, "away": 0.58},
                    "-0.5": {"home": 0.52, "away": 0.48},
                    "+0.5": {"home": 0.60, "away": 0.40},
                    "+1.5": {"home": 0.70, "away": 0.30},
                },
            },
        }


def calculate_expected_value(probability: float, odds: float) -> float:
    """EV = (probability * odds) - 1. Positive = value bet."""
    return round(probability * odds - 1.0, 4)


def find_value_bets(prediction: dict, odds_data: dict, min_ev: float = 0.01) -> list[dict]:
    """
    Compare model probabilities with market odds to find value bets.
    Returns list of value bets with EV > min_ev.
    """
    value_bets = []

    # Check h2h market
    if "h2h" in odds_data:
        for outcome, prob in prediction["h2h"].items():
            odds_info = odds_data["h2h"].get(outcome)
            if not odds_info:
                continue
            price = odds_info if isinstance(odds_info, (int, float)) else odds_info.get("price", 0)
            ev = calculate_expected_value(prob, price)
            if ev > min_ev:
                value_bets.append({
                    "market": "h2h",
                    "outcome": outcome,
                    "probability": prob,
                    "odds": price,
                    "ev": ev,
                    "bookmaker": odds_info.get("bookmaker", "N/A") if isinstance(odds_info, dict) else "N/A",
                })

    # Check totals (over/under) — match by point.
    # prediction['totals'] keys = "Over 2.5", "Under 3.5" (with point baked in).
    # odds_data['totals'] keys = "Over", "Under" (point in dict).
    if "totals" in odds_data:
        for outcome_key, odds_info in odds_data["totals"].items():
            if not isinstance(odds_info, dict):
                continue
            point = odds_info.get("point")
            price = odds_info.get("price", 0) or 0
            if point is None or price <= 0:
                continue
            # Build prediction key with the point: "Over 2.75", "Under 2.75".
            pred_key = f"{outcome_key} {point}"
            prob = prediction.get("totals", {}).get(pred_key)
            if prob is None:
                # Fallback: try default 2.5 line if model doesn't have this point.
                prob = prediction.get("totals", {}).get(f"{outcome_key} 2.5")
                if prob is None:
                    continue
            ev = calculate_expected_value(prob, price)
            if ev > min_ev:
                value_bets.append({
                    "market": "totals",
                    "outcome": pred_key,  # Save with point: "Over 2.75"
                    "probability": prob,
                    "odds": price,
                    "ev": ev,
                    "bookmaker": odds_info.get("bookmaker", "N/A"),
                })

    # Check BTTS
    if "btts" in odds_data:
        for outcome, prob in prediction["btts"].items():
            odds_info = odds_data["btts"].get(outcome)
            if not odds_info:
                continue
            price = odds_info if isinstance(odds_info, (int, float)) else odds_info.get("price", 0)
            ev = calculate_expected_value(prob, price)
            if ev > min_ev:
                value_bets.append({
                    "market": "btts",
                    "outcome": outcome,
                    "probability": prob,
                    "odds": price,
                    "ev": ev,
                    "bookmaker": odds_info.get("bookmaker", "N/A") if isinstance(odds_info, dict) else "N/A",
                })

    # Check Asian Handicap (spreads)
    if "spreads" in odds_data and "asian_handicap" in prediction:
        for bk_outcome, odds_info in odds_data["spreads"].items():
            if not isinstance(odds_info, dict) or "price" not in odds_info:
                continue
            price = odds_info["price"]
            point = odds_info.get("point", 0)

            # Map odds outcome to our handicap prediction
            # The Odds API: home team spread point (e.g., -1.5)
            handicap_key = f"{point:+g}" if point != 0 else "0"
            ah = prediction["asian_handicap"].get(handicap_key)
            if not ah:
                continue

            # Determine which side this odds is for
            if bk_outcome == "Home" or (isinstance(bk_outcome, str) and "home" in bk_outcome.lower()):
                prob = ah["home"]
                outcome_label = f"Home {handicap_key}"
            elif bk_outcome == "Away" or (isinstance(bk_outcome, str) and "away" in bk_outcome.lower()):
                prob = ah["away"]
                outcome_label = f"Away {handicap_key}"
            else:
                # Try to match by team name in outcome
                prob = ah["home"]
                outcome_label = f"AH {handicap_key} {bk_outcome}"

            ev = calculate_expected_value(prob, price)
            if ev > min_ev:
                value_bets.append({
                    "market": "asian_handicap",
                    "outcome": outcome_label,
                    "probability": prob,
                    "odds": price,
                    "ev": ev,
                    "bookmaker": odds_info.get("bookmaker", "N/A"),
                })

    # Sort by EV descending
    value_bets.sort(key=lambda x: x["ev"], reverse=True)
    return value_bets


def get_confidence_tier(ev: float, probability: float) -> str:
    """Assign confidence tier based on EV and model probability."""
    from src.config import CONFIDENCE
    if ev >= CONFIDENCE["HIGH"]["min_ev"] and probability >= CONFIDENCE["HIGH"]["min_agreement"]:
        return "HIGH"
    if ev >= CONFIDENCE["MEDIUM"]["min_ev"] and probability >= CONFIDENCE["MEDIUM"]["min_agreement"]:
        return "MEDIUM"
    if ev >= CONFIDENCE["LOW"]["min_ev"] and probability >= CONFIDENCE["LOW"]["min_agreement"]:
        return "LOW"
    return "SKIP"
