"""Bivariate Poisson model for football (Karlis & Ntzoufras, 2003).

The key insight over Dixon-Coles: goals are correlated via a *shared* latent
component Y3 rather than a τ-patched Poisson product. If you think of Y3 as
"shared game tempo" (open, attacking match → both sides benefit), the model
captures positive correlation naturally and produces materially higher draw
probabilities (+2-4%) and inflated 1-1/2-2 cells — which matches observed
football scoring distributions better than DC on top leagues with ≥100
matches in 90 days.

Construction
------------
    X_home = Y1 + Y3
    X_away = Y2 + Y3
    Y1 ~ Pois(λ1), Y2 ~ Pois(λ2), Y3 ~ Pois(λ3), independent.

With λ1 = exp(α_h + β_a + γ), λ2 = exp(α_a + β_h), λ3 a free league-wide
dispersion parameter (shared across all matches; could be extended to
match-level covariates but we keep it constant for stability).

Joint PMF
---------
    P(X=x, Y=y) = exp(-(λ1+λ2+λ3)) ·
                  Σ_{k=0}^{min(x,y)} [λ1^(x-k) · λ2^(y-k) · λ3^k
                                       / ((x-k)! · (y-k)! · k!)]

Parameter layout (length 2n):
    [α_0..α_{n-2}, β_0..β_{n-2}, γ, log_λ3]
Last α and β set so sum(α)=sum(β)=0 (sum-to-zero identifiability).

Why log_λ3 instead of λ3 directly: keeps it strictly positive without an
explicit bound, and tends to stabilize L-BFGS-B near 0.

Interface is identical to DixonColesModel.predict() so pipeline swaps are
a one-line change.
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

XI = 0.0065
INITIAL_GAMMA = 0.30
INITIAL_LAMBDA3 = 0.10
MAX_GOALS = 8


def _parse_date(s) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def _bp_pmf(x: int, y: int, lam1: float, lam2: float, lam3: float) -> float:
    """Bivariate Poisson joint PMF at (x, y). Finite sum, safe for small inputs."""
    base = math.exp(-(lam1 + lam2 + lam3))
    total = 0.0
    k_max = min(x, y)
    for k in range(k_max + 1):
        num = (lam1 ** (x - k)) * (lam2 ** (y - k)) * (lam3 ** k)
        den = math.factorial(x - k) * math.factorial(y - k) * math.factorial(k)
        total += num / den
    return base * total


class BivariatePoissonModel:
    def __init__(self):
        self.teams: list[str] = []
        self.team_index: dict[str, int] = {}
        self.alpha: np.ndarray = np.array([])
        self.beta: np.ndarray = np.array([])
        self.gamma: float = INITIAL_GAMMA
        self.lambda3: float = INITIAL_LAMBDA3
        self.league_avg_goals: float = 0.0
        self.attack_strength: dict[str, float] = {}
        self.defense_strength: dict[str, float] = {}
        self._fitted: bool = False

    # ---- Fitting -------------------------------------------------------

    def fit(self, results: list[dict], xg_data: Optional[list[dict]] = None) -> None:
        """Weighted MLE fit via L-BFGS-B.

        Target is integer goals — using continuous xG with a bivariate latent
        component is still open research; for xG fits we recommend DC.
        """
        results = [
            r for r in results
            if r.get("home_goals") is not None and r.get("away_goals") is not None
        ]
        if len(results) < 30:
            logger.warning("[BP] need ≥30 matches to fit, got %d — staying unfitted", len(results))
            return

        teams = sorted({r["home_team"] for r in results} | {r["away_team"] for r in results})
        n = len(teams)
        idx = {t: i for i, t in enumerate(teams)}
        self.teams = teams
        self.team_index = idx

        h_idx = np.array([idx[r["home_team"]] for r in results], dtype=int)
        a_idx = np.array([idx[r["away_team"]] for r in results], dtype=int)
        hg = np.array([r["home_goals"] for r in results], dtype=int)
        ag = np.array([r["away_goals"] for r in results], dtype=int)

        now = datetime.now(timezone.utc).replace(tzinfo=None)
        weights = np.ones(len(results))
        for i, r in enumerate(results):
            d = _parse_date(r.get("utc_date"))
            if d is not None:
                days = max(0.0, (now - d).total_seconds() / 86400.0)
                weights[i] = math.exp(-XI * days)

        self.league_avg_goals = float((hg.mean() + ag.mean()) / 2.0)

        init = np.concatenate([
            np.zeros(n - 1),
            np.zeros(n - 1),
            [INITIAL_GAMMA],
            [math.log(INITIAL_LAMBDA3)],
        ])
        bounds = (
            [(-2.0, 2.0)] * (n - 1)
            + [(-2.0, 2.0)] * (n - 1)
            + [(-0.5, 1.0)]
            + [(-6.0, 1.0)]  # log_λ3 ∈ roughly [0.0025, 2.7]
        )

        def nll(params):
            a = np.empty(n); b = np.empty(n)
            a[:-1] = params[: n - 1]
            b[:-1] = params[n - 1 : 2 * (n - 1)]
            a[-1] = -a[:-1].sum()
            b[-1] = -b[:-1].sum()
            g = params[-2]
            lam3 = math.exp(params[-1])

            lam1 = np.exp(a[h_idx] + b[a_idx] + g)
            lam2 = np.exp(a[a_idx] + b[h_idx])
            lam1 = np.clip(lam1, 0.05, 8.0)
            lam2 = np.clip(lam2, 0.05, 8.0)

            total = 0.0
            for i in range(len(results)):
                p = _bp_pmf(int(hg[i]), int(ag[i]), float(lam1[i]), float(lam2[i]), lam3)
                if p <= 0.0:
                    p = 1e-12
                total += weights[i] * math.log(p)
            return -total

        res = minimize(
            nll, init, method="L-BFGS-B", bounds=bounds,
            options={"maxiter": 200, "ftol": 1e-6},
        )
        if not res.success:
            logger.warning("[BP] optimizer non-converge: %s", res.message)

        a = np.empty(n); b = np.empty(n)
        a[:-1] = res.x[: n - 1]
        b[:-1] = res.x[n - 1 : 2 * (n - 1)]
        a[-1] = -a[:-1].sum()
        b[-1] = -b[:-1].sum()
        self.alpha = a
        self.beta = b
        self.gamma = float(res.x[-2])
        self.lambda3 = float(math.exp(res.x[-1]))

        self.attack_strength = {t: float(math.exp(a[i])) for t, i in idx.items()}
        self.defense_strength = {t: float(math.exp(b[i])) for t, i in idx.items()}
        self._fitted = True
        logger.info(
            "[BP] fit OK: teams=%d matches=%d gamma=%.3f lambda3=%.3f avg_goals=%.2f",
            n, len(results), self.gamma, self.lambda3, self.league_avg_goals,
        )

    # ---- Prediction ----------------------------------------------------

    def get_home_away_lambdas(self, home_team: str, away_team: str) -> tuple[float, float]:
        """Return (λ_home_marginal, λ_away_marginal) = (λ1+λ3, λ2+λ3)."""
        if not self._fitted:
            return 1.3, 1.0
        i = self.team_index.get(home_team)
        j = self.team_index.get(away_team)
        if i is None or j is None:
            lag = self.league_avg_goals or 1.2
            return float(np.clip(lag * 1.15, 0.2, 5.0)), float(np.clip(lag * 0.85, 0.2, 5.0))
        lam1 = math.exp(self.alpha[i] + self.beta[j] + self.gamma)
        lam2 = math.exp(self.alpha[j] + self.beta[i])
        return float(np.clip(lam1 + self.lambda3, 0.2, 5.0)), float(np.clip(lam2 + self.lambda3, 0.2, 5.0))

    def predict(
        self,
        home_team: str,
        away_team: str,
        home_advantage: float = 1.25,
        injury_data: Optional[dict] = None,
        weather_data: Optional[dict] = None,
        match_context: Optional[dict] = None,
    ) -> dict:
        """Same output shape as DixonColesModel.predict().

        match_context semantics identical to DixonColesModel.predict — applied
        on λ1/λ2 after weather+injuries; λ3 is NOT adjusted (it models
        structural correlation, not goal volume).
        """
        if not self._fitted:
            return self._default_prediction()

        i = self.team_index.get(home_team)
        j = self.team_index.get(away_team)
        if i is None or j is None:
            return self._default_prediction()

        lam1 = math.exp(self.alpha[i] + self.beta[j] + self.gamma)
        lam2 = math.exp(self.alpha[j] + self.beta[i])
        lam3 = self.lambda3

        # Weather: additive shift on total goals → split evenly across λ1 & λ2.
        # λ3 stays fixed (it models structural correlation, not goal volume).
        if weather_data and weather_data.get("total_goals_adjust"):
            shift = float(weather_data["total_goals_adjust"])
            lam1 = max(0.1, lam1 + shift / 2.0)
            lam2 = max(0.1, lam2 + shift / 2.0)

        # Injuries: multiplicative per-side.
        if injury_data:
            h = injury_data.get("home", {}) or {}
            a = injury_data.get("away", {}) or {}
            h_atk = float(h.get("attack_mult", 1.0))
            h_def = float(h.get("defense_mult", 1.0))
            a_atk = float(a.get("attack_mult", 1.0))
            a_def = float(a.get("defense_mult", 1.0))
            lam1 = max(0.1, lam1 * h_atk * a_def)
            lam2 = max(0.1, lam2 * a_atk * h_def)

        # Match context: additive λ adjustments (caller gates on USE_MATCH_CONTEXT=="on").
        if match_context:
            from src.analytics.match_context import apply_lambda_adjustment
            lam1, lam2 = apply_lambda_adjustment(lam1, lam2, match_context)

        # Build joint score matrix via bivariate PMF.
        matrix = np.zeros((MAX_GOALS, MAX_GOALS))
        for x in range(MAX_GOALS):
            for y in range(MAX_GOALS):
                matrix[x, y] = _bp_pmf(x, y, lam1, lam2, lam3)
        s = matrix.sum()
        if s > 0:
            matrix /= s

        # Marginals for display (xG = lam1+lam3, mu = lam2+lam3).
        lam_disp = lam1 + lam3
        mu_disp = lam2 + lam3

        # 1X2
        p_home = float(np.sum(np.tril(matrix, -1)))
        p_draw = float(np.trace(matrix))
        p_away = float(np.sum(np.triu(matrix, 1)))
        tot = p_home + p_draw + p_away
        if tot > 0:
            p_home /= tot; p_draw /= tot; p_away /= tot

        # Totals
        ou_probs: dict[int, float] = {}
        for k in range(0, MAX_GOALS + 1):
            ou_probs[k] = float(sum(
                matrix[i][j] for i in range(MAX_GOALS) for j in range(MAX_GOALS)
                if i + j <= k
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

        # Asian handicap
        asian_handicap = {}
        ah_lines = [-2.5, -2.25, -2.0, -1.75, -1.5, -1.25, -1.0, -0.75, -0.5, -0.25,
                    0, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0, 2.25, 2.5]
        for line in ah_lines:
            p_h = p_a = p_push = 0.0
            for x in range(MAX_GOALS):
                for y in range(MAX_GOALS):
                    diff = (x + line) - y
                    if abs(diff) < 1e-9:
                        p_push += matrix[x][y]
                    elif diff > 0:
                        p_h += matrix[x][y]
                    else:
                        p_a += matrix[x][y]
            key = f"{line:+g}" if line != 0 else "0"
            asian_handicap[key] = {
                "home": round(float(p_h), 4),
                "away": round(float(p_a), 4),
                "push": round(float(p_push), 4),
            }

        # Corners (reuse DC heuristic — BP doesn't model corners directly)
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
            corner_probs[line] = {"over": round(1.0 - p_under, 4), "under": round(p_under, 4)}

        max_c = 15
        hp = np.array([poisson.pmf(i, home_corner_xg) for i in range(max_c)])
        ap = np.array([poisson.pmf(i, away_corner_xg) for i in range(max_c)])
        cm = np.outer(hp, ap)
        corner_ah = {}
        for hdp in [-8.5, -8, -7.5, -7, -6.5, -6, -5.5, -5, -4.5, -4, -3.5, -3, -2.5,
                    -2, -1.5, -1, -0.5, 0, 0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5, 5,
                    5.5, 6, 6.5, 7, 7.5, 8, 8.5]:
            p_h = p_a = 0.0
            for x in range(max_c):
                for y in range(max_c):
                    d = (x + hdp) - y
                    if d > 0: p_h += cm[x][y]
                    elif d < 0: p_a += cm[x][y]
            corner_ah[f"{hdp:+g}"] = {"home": round(float(p_h), 4), "away": round(float(p_a), 4)}

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
            for x in range(max_h1c):
                for y in range(max_h1c):
                    d = (x + hdp) - y
                    if d > 0: p_h += h1_cm[x][y]
                    elif d < 0: p_a += h1_cm[x][y]
            h1_corner_ah[f"{hdp:+g}"] = {"home": round(float(p_h), 4), "away": round(float(p_a), 4)}

        return {
            "home_xg": round(lam_disp, 2),
            "away_xg": round(mu_disp, 2),
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
