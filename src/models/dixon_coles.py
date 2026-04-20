"""Dixon-Coles football prediction model.

Addresses 3 weaknesses of plain Poisson:
1. Home/away goals are NOT independent → apply τ correction for low scores
   (0-0, 1-0, 0-1, 1-1), learned jointly via correlation parameter ρ.
2. No recency weighting → apply exponential time decay weights (ξ=0.0065/day,
   half-life ≈ 107 days) when fitting.
3. No xG support → fit() accepts optional `xg_data` to replace integer goals
   with Expected Goals as the observed target.

Output shape of predict() is identical to PoissonModel.predict() so the
pipeline and formatter can swap models with zero downstream changes.

Reference: Dixon & Coles (1997), "Modelling Association Football Scores and
Inefficiencies in the Football Betting Market".
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, timezone
from typing import Optional

import numpy as np
from scipy.optimize import minimize
from scipy.stats import poisson

logger = logging.getLogger(__name__)

XI = 0.0065             # Time-decay rate (per day). Half-life = ln 2 / ξ ≈ 107d.
INITIAL_GAMMA = 0.3     # log home advantage; exp(0.3) ≈ 1.35×
INITIAL_RHO = -0.08     # mild negative correlation — typical for top leagues


def _parse_date(s) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


class DixonColesModel:
    def __init__(self):
        self.teams: list[str] = []
        self.team_index: dict[str, int] = {}
        self.alpha: np.ndarray = np.array([])   # attack (log scale)
        self.beta: np.ndarray = np.array([])    # defense weakness (log scale)
        self.gamma: float = INITIAL_GAMMA       # log home advantage
        self.rho: float = INITIAL_RHO           # low-score correlation
        self.league_avg_goals: float = 0.0
        # Backward-compat display fields (multiplicative scale, like PoissonModel).
        self.attack_strength: dict[str, float] = {}
        self.defense_strength: dict[str, float] = {}
        self._fitted: bool = False
        self._used_xg: bool = False

    # ---- Fitting -------------------------------------------------------

    def fit(
        self,
        results: list[dict],
        xg_data: Optional[list[dict]] = None,
    ) -> None:
        """Weighted MLE fit via L-BFGS-B.

        results: list of {home_team, away_team, home_goals, away_goals,
                          utc_date?}. utc_date (ISO string) enables time decay.
        xg_data: optional list aligned 1:1 with results, containing
                 {home_xg, away_xg}. When provided, fit targets xG (floats)
                 instead of integer goals — better signal for team strength.
        """
        results = [
            r for r in results
            if r.get("home_goals") is not None and r.get("away_goals") is not None
        ]
        if len(results) < 20:
            logger.warning("[DC] need ≥20 matches to fit, got %d — staying unfitted", len(results))
            return

        teams = sorted({r["home_team"] for r in results} | {r["away_team"] for r in results})
        n = len(teams)
        idx = {t: i for i, t in enumerate(teams)}
        self.teams = teams
        self.team_index = idx

        h_idx = np.array([idx[r["home_team"]] for r in results], dtype=int)
        a_idx = np.array([idx[r["away_team"]] for r in results], dtype=int)

        # xg_data (if given) is a list aligned 1:1 with results. Each entry is
        # either {"home_xg", "away_xg"} or None (no xG for that match). When
        # ≥50% entries have real xG, we use continuous Gamma log-likelihood
        # for those slots and fall back to integer Poisson for the rest.
        xg_mask = np.zeros(len(results), dtype=bool)
        hg_xg = np.zeros(len(results), dtype=float)
        ag_xg = np.zeros(len(results), dtype=float)
        if xg_data is not None and len(xg_data) == len(results):
            for i, x in enumerate(xg_data):
                if not x:
                    continue
                h_xg = x.get("home_xg")
                a_xg = x.get("away_xg")
                if h_xg is None or a_xg is None:
                    continue
                xg_mask[i] = True
                hg_xg[i] = max(0.05, float(h_xg))
                ag_xg[i] = max(0.05, float(a_xg))

        coverage = float(xg_mask.sum()) / max(1, len(results))
        using_xg = coverage >= 0.5
        # Integer goals are ALWAYS kept as backup — even in xG mode we use
        # them on the ~50% of rows where xG is missing.
        hg_int = np.array([r["home_goals"] for r in results], dtype=float)
        ag_int = np.array([r["away_goals"] for r in results], dtype=float)

        if using_xg:
            # Mixed target: xG where available, integer goals elsewhere.
            hg = np.where(xg_mask, hg_xg, hg_int)
            ag = np.where(xg_mask, ag_xg, ag_int)
        else:
            hg = hg_int
            ag = ag_int

        # Time-decay weights
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        weights = np.ones(len(results))
        for i, r in enumerate(results):
            d = _parse_date(r.get("utc_date"))
            if d is not None:
                days = max(0.0, (now - d).total_seconds() / 86400.0)
                weights[i] = math.exp(-XI * days)

        self.league_avg_goals = float((hg.mean() + ag.mean()) / 2.0)

        # Param layout: [α_0..α_{n-2}, β_0..β_{n-2}, γ, ρ]
        # Last α and β are -sum(others) to enforce sum-to-zero identifiability.
        init = np.concatenate([
            np.zeros(n - 1),
            np.zeros(n - 1),
            [INITIAL_GAMMA],
            [INITIAL_RHO],
        ])
        bounds = (
            [(-2.0, 2.0)] * (n - 1)
            + [(-2.0, 2.0)] * (n - 1)
            + [(-0.5, 1.0)]
            + [(-0.2, 0.05)]  # ρ clamped so τ stays positive for plausible λ/μ
        )

        # Pre-compute log(Γ(x+1)) for both integer-goal and xG targets. For
        # continuous xG this generalizes log(k!) — Γ(n+1)=n! for integers.
        log_gamma_h = np.array([math.lgamma(float(x) + 1.0) for x in hg], dtype=float)
        log_gamma_a = np.array([math.lgamma(float(x) + 1.0) for x in ag], dtype=float)
        # τ correction is defined on integer score cells; for rows where we use
        # xG as target (float), we skip τ. Integer rows in mixed mode keep τ.
        tau_eligible = ~xg_mask if using_xg else np.ones(len(results), dtype=bool)

        def nll(params):
            a = np.empty(n)
            b = np.empty(n)
            a[:-1] = params[: n - 1]
            b[:-1] = params[n - 1 : 2 * (n - 1)]
            a[-1] = -a[:-1].sum()
            b[-1] = -b[:-1].sum()
            g = params[-2]
            rho = params[-1]

            lam = np.exp(a[h_idx] + b[a_idx] + g)
            mu = np.exp(a[a_idx] + b[h_idx])
            lam = np.clip(lam, 0.05, 8.0)
            mu = np.clip(mu, 0.05, 8.0)

            # Continuous Poisson-like log-likelihood: x·log(λ) - λ - log(Γ(x+1)).
            # Works for both integer k (exact Poisson pmf) and float xG.
            log_poi_h = hg * np.log(lam) - lam - log_gamma_h
            log_poi_a = ag * np.log(mu) - mu - log_gamma_a

            tau = np.ones(len(results))
            m00 = tau_eligible & (hg == 0) & (ag == 0)
            m01 = tau_eligible & (hg == 0) & (ag == 1)
            m10 = tau_eligible & (hg == 1) & (ag == 0)
            m11 = tau_eligible & (hg == 1) & (ag == 1)
            tau[m00] = 1.0 - lam[m00] * mu[m00] * rho
            tau[m01] = 1.0 + lam[m01] * rho
            tau[m10] = 1.0 + mu[m10] * rho
            tau[m11] = 1.0 - rho
            tau = np.clip(tau, 1e-9, None)

            ll = weights * (np.log(tau) + log_poi_h + log_poi_a)
            return -float(np.sum(ll))

        res = minimize(
            nll, init, method="L-BFGS-B", bounds=bounds,
            options={"maxiter": 250, "ftol": 1e-7},
        )
        if not res.success:
            logger.warning("[DC] optimizer non-converge: %s", res.message)

        a = np.empty(n)
        b = np.empty(n)
        a[:-1] = res.x[: n - 1]
        b[:-1] = res.x[n - 1 : 2 * (n - 1)]
        a[-1] = -a[:-1].sum()
        b[-1] = -b[:-1].sum()
        self.alpha = a
        self.beta = b
        self.gamma = float(res.x[-2])
        self.rho = float(res.x[-1])

        self.attack_strength = {t: float(math.exp(a[i])) for t, i in idx.items()}
        self.defense_strength = {t: float(math.exp(b[i])) for t, i in idx.items()}
        self._fitted = True
        self._used_xg = using_xg

        logger.info(
            "[DC] fit OK: teams=%d matches=%d rho=%.3f gamma=%.3f avg_goals=%.2f xG=%s (coverage=%.0f%%)",
            n, len(results), self.rho, self.gamma, self.league_avg_goals, using_xg, coverage * 100,
        )

    # ---- Prediction ----------------------------------------------------

    def get_home_away_lambdas(self, home_team: str, away_team: str) -> tuple[float, float]:
        """Return (λ_home, λ_away) for downstream live model re-use."""
        if not self._fitted:
            return 1.3, 1.0
        i = self.team_index.get(home_team)
        j = self.team_index.get(away_team)
        if i is None or j is None:
            lag = self.league_avg_goals or 1.2
            return float(np.clip(lag * 1.15, 0.2, 5.0)), float(np.clip(lag * 0.85, 0.2, 5.0))
        lam = math.exp(self.alpha[i] + self.beta[j] + self.gamma)
        mu = math.exp(self.alpha[j] + self.beta[i])
        return float(np.clip(lam, 0.2, 5.0)), float(np.clip(mu, 0.2, 5.0))

    def predict(
        self,
        home_team: str,
        away_team: str,
        home_advantage: float = 1.25,
        injury_data: Optional[dict] = None,
        weather_data: Optional[dict] = None,
        match_context: Optional[dict] = None,
    ) -> dict:
        """Build τ-corrected joint matrix and derive full market dict.

        home_advantage arg kept for signature-compat with PoissonModel —
        DC uses its own learned γ instead.

        injury_data: output of analytics.injury_impact.summarize_injuries, shape:
            {"home": {attack_mult, defense_mult, ...}, "away": {...}}.
            None → no injury adjustment.
        weather_data: output of analytics.weather_impact.calculate_weather_adjustment:
            {"total_goals_adjust": float, "description": str}.
            None → no weather adjustment.
        match_context: output of analytics.match_context.classify_match +
            the USE_MATCH_CONTEXT=="on" gate already applied by the caller.
            When passed, additive λ adjustments from LAMBDA_ADJUSTMENTS are
            applied AFTER weather + injuries. None (or flags all False) →
            no adjustment.
        """
        if not self._fitted:
            return self._default_prediction()

        lam, mu = self.get_home_away_lambdas(home_team, away_team)

        # --- Weather: shift total goals, split evenly between both sides ---
        # Applied FIRST because it's an additive baseline shift (pitch
        # playability), then injuries tweak multiplicatively per team.
        if weather_data and weather_data.get("total_goals_adjust"):
            shift = float(weather_data["total_goals_adjust"])
            lam = max(0.1, lam + shift / 2.0)
            mu = max(0.1, mu + shift / 2.0)

        # --- Injuries: per-team attack/defense multipliers ---
        if injury_data:
            h = injury_data.get("home", {}) or {}
            a = injury_data.get("away", {}) or {}
            h_atk = float(h.get("attack_mult", 1.0))
            h_def = float(h.get("defense_mult", 1.0))
            a_atk = float(a.get("attack_mult", 1.0))
            a_def = float(a.get("defense_mult", 1.0))
            # λ_home = home_attack × away_defense_weakness
            # defense_mult >1 means weaker defense → scale λ UP for opponent.
            lam = max(0.1, lam * h_atk * a_def)
            mu = max(0.1, mu * a_atk * h_def)

        # --- Match context: additive λ adjustments (derby/final/knockout) ---
        # Applied LAST so weather/injury baselines are preserved and context
        # nudges the post-adjusted rates. Caller must gate on
        # USE_MATCH_CONTEXT=="on"; DC model does not read config itself.
        if match_context:
            from src.analytics.match_context import apply_lambda_adjustment
            lam, mu = apply_lambda_adjustment(lam, mu, match_context)

        max_goals = 8
        home_p = np.array([poisson.pmf(i, lam) for i in range(max_goals)])
        away_p = np.array([poisson.pmf(j, mu) for j in range(max_goals)])
        matrix = np.outer(home_p, away_p)

        # τ correction on low-score cells.
        rho = self.rho
        matrix[0, 0] *= max(1e-9, 1.0 - lam * mu * rho)
        matrix[0, 1] *= max(1e-9, 1.0 + lam * rho)
        matrix[1, 0] *= max(1e-9, 1.0 + mu * rho)
        matrix[1, 1] *= max(1e-9, 1.0 - rho)
        s = matrix.sum()
        if s > 0:
            matrix /= s

        # 1X2
        p_home = float(np.sum(np.tril(matrix, -1)))
        p_draw = float(np.trace(matrix))
        p_away = float(np.sum(np.triu(matrix, 1)))
        total = p_home + p_draw + p_away
        if total > 0:
            p_home /= total; p_draw /= total; p_away /= total

        # Totals: pre-compute P(total <= k) once.
        ou_probs: dict[int, float] = {}
        for threshold in range(0, max_goals + 1):
            ou_probs[threshold] = float(sum(
                matrix[i][j]
                for i in range(max_goals) for j in range(max_goals)
                if i + j <= threshold
            ))
        ou_all: dict[float, dict[str, float]] = {}
        for line_x10 in [15, 20, 25, 27, 30, 32, 35, 37, 40, 45]:
            line = line_x10 / 10.0
            if line == int(line):
                k = int(line)
                p_under = ou_probs.get(k - 1, 0.0)
                p_over = 1.0 - ou_probs.get(k, 0.0)
                ou_all[line] = {"over": round(p_over, 4), "under": round(p_under, 4)}
            elif line % 1 == 0.5:
                k = int(line)
                p_under = ou_probs.get(k, 0.0)
                p_over = 1.0 - p_under
                ou_all[line] = {"over": round(p_over, 4), "under": round(p_under, 4)}
            else:
                lower = int(line - 0.25)
                upper = int(line + 0.25)
                p_over_l = 1.0 - ou_probs.get(lower, 0.0)
                p_over_u = 1.0 - ou_probs.get(upper, 0.0)
                p_over = (p_over_l + p_over_u) / 2
                ou_all[line] = {"over": round(p_over, 4), "under": round(1.0 - p_over, 4)}

        # BTTS
        p_btts_yes = 1.0 - float(np.sum(matrix[0, :])) - float(np.sum(matrix[:, 0])) + float(matrix[0][0])
        p_btts_no = 1.0 - p_btts_yes

        # Asian Handicap (match level)
        asian_handicap = {}
        ah_lines = [-2.5, -2.25, -2.0, -1.75, -1.5, -1.25, -1.0, -0.75, -0.5, -0.25,
                    0, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0, 2.25, 2.5]
        for line in ah_lines:
            p_h = p_a = p_push = 0.0
            for i in range(max_goals):
                for j in range(max_goals):
                    diff = (i + line) - j
                    if abs(diff) < 1e-9:
                        p_push += matrix[i][j]
                    elif diff > 0:
                        p_h += matrix[i][j]
                    else:
                        p_a += matrix[i][j]
            key = f"{line:+g}" if line != 0 else "0"
            asian_handicap[key] = {
                "home": round(float(p_h), 4),
                "away": round(float(p_a), 4),
                "push": round(float(p_push), 4),
            }

        # ---- Corners (same heuristic as PoissonModel) ----
        home_atk = self.attack_strength.get(home_team, 1.0)
        away_atk = self.attack_strength.get(away_team, 1.0)
        home_def = self.defense_strength.get(home_team, 1.0)
        away_def = self.defense_strength.get(away_team, 1.0)

        avg_corners = 10.5
        home_corner_xg = (home_atk * 0.6 + away_def * 0.4) * (avg_corners / 2) * 1.05
        away_corner_xg = (away_atk * 0.6 + home_def * 0.4) * (avg_corners / 2)
        total_corner_xg = float(np.clip(home_corner_xg + away_corner_xg, 6.0, 18.0))
        home_corner_xg = float(np.clip(home_corner_xg, 2.0, 10.0))
        away_corner_xg = float(np.clip(away_corner_xg, 2.0, 10.0))

        corner_probs = {}
        for line in [7.5, 8.5, 9.5, 10.5, 11.5, 12.5]:
            p_under = float(sum(poisson.pmf(i, total_corner_xg) for i in range(int(line) + 1)))
            corner_probs[line] = {
                "over": round(1.0 - p_under, 4),
                "under": round(p_under, 4),
            }

        max_c = 15
        hp = np.array([poisson.pmf(i, home_corner_xg) for i in range(max_c)])
        ap = np.array([poisson.pmf(i, away_corner_xg) for i in range(max_c)])
        cm = np.outer(hp, ap)
        corner_ah = {}
        for hdp in [-8.5, -8, -7.5, -7, -6.5, -6, -5.5, -5, -4.5, -4, -3.5, -3, -2.5,
                    -2, -1.5, -1, -0.5, 0, 0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5, 5,
                    5.5, 6, 6.5, 7, 7.5, 8, 8.5]:
            p_h = p_a = 0.0
            for i in range(max_c):
                for j in range(max_c):
                    d = (i + hdp) - j
                    if d > 0: p_h += cm[i][j]
                    elif d < 0: p_a += cm[i][j]
            corner_ah[f"{hdp:+g}"] = {"home": round(float(p_h), 4), "away": round(float(p_a), 4)}

        # H1 corners (~45% of match)
        h1_total = float(np.clip(total_corner_xg * 0.45, 2.5, 9.0))
        h1_home = float(np.clip(home_corner_xg * 0.45, 1.0, 5.5))
        h1_away = float(np.clip(away_corner_xg * 0.45, 1.0, 5.5))
        h1_corner_probs = {}
        for line in [3.5, 4.5, 5.5, 6.5]:
            p_under = float(sum(poisson.pmf(i, h1_total) for i in range(int(line) + 1)))
            h1_corner_probs[line] = {"over": round(1.0 - p_under, 4), "under": round(p_under, 4)}
        max_h1c = 10
        h1_hp = np.array([poisson.pmf(i, h1_home) for i in range(max_h1c)])
        h1_ap = np.array([poisson.pmf(i, h1_away) for i in range(max_h1c)])
        h1_cm = np.outer(h1_hp, h1_ap)
        h1_corner_ah = {}
        for hdp in [-3.5, -3, -2.5, -2, -1.5, -1, -0.5, 0, 0.5, 1, 1.5, 2, 2.5, 3, 3.5]:
            p_h = p_a = 0.0
            for i in range(max_h1c):
                for j in range(max_h1c):
                    d = (i + hdp) - j
                    if d > 0: p_h += h1_cm[i][j]
                    elif d < 0: p_a += h1_cm[i][j]
            h1_corner_ah[f"{hdp:+g}"] = {"home": round(float(p_h), 4), "away": round(float(p_a), 4)}

        return {
            "home_xg": round(lam, 2),
            "away_xg": round(mu, 2),
            "h2h": {
                "Home": round(p_home, 4),
                "Draw": round(p_draw, 4),
                "Away": round(p_away, 4),
            },
            "totals": {
                **{f"Over {line}": data["over"] for line, data in ou_all.items()},
                **{f"Under {line}": data["under"] for line, data in ou_all.items()},
            },
            "btts": {"Yes": round(p_btts_yes, 4), "No": round(p_btts_no, 4)},
            "asian_handicap": asian_handicap,
            "corners": {
                "xg": round(total_corner_xg, 1),
                "home_xc": round(home_corner_xg, 1),
                "away_xc": round(away_corner_xg, 1),
                "lines": corner_probs,
                "asian_handicap": corner_ah,
            },
            "corners_h1": {
                "xg": round(h1_total, 1),
                "home_xc": round(h1_home, 1),
                "away_xc": round(h1_away, 1),
                "lines": h1_corner_probs,
                "asian_handicap": h1_corner_ah,
            },
        }

    def _default_prediction(self) -> dict:
        return {
            "home_xg": 1.3, "away_xg": 1.0,
            "h2h": {"Home": 0.45, "Draw": 0.26, "Away": 0.29},
            "totals": {
                "Over 1.5": 0.72, "Under 1.5": 0.28,
                "Over 2.5": 0.50, "Under 2.5": 0.50,
                "Over 3.5": 0.28, "Under 3.5": 0.72,
            },
            "btts": {"Yes": 0.48, "No": 0.52},
            "asian_handicap": {
                "-0.5": {"home": 0.45, "away": 0.55, "push": 0},
                "0": {"home": 0.45, "away": 0.29, "push": 0.26},
                "+0.5": {"home": 0.71, "away": 0.29, "push": 0},
            },
            "corners": {
                "xg": 10.5, "home_xc": 5.5, "away_xc": 5.0,
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
