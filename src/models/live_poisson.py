"""Live (in-play) Poisson model — dự đoán kết quả dựa trên state hiện tại."""

from __future__ import annotations

import logging

from scipy.stats import poisson

logger = logging.getLogger(__name__)


class LivePoissonModel:
    """Poisson model cho in-play betting.

    Khởi tạo từ pregame expected goals (λ 90 phút). Với mỗi state (minute, score,
    xG, red cards), blend xG thực tế với rate pregame, adjust cho thẻ đỏ + game
    state, rồi tính xác suất cho h2h / totals / next goal trên số phút còn lại.
    """

    def __init__(self, home_lambda_full: float, away_lambda_full: float):
        self.home_lambda_full = max(0.01, float(home_lambda_full))
        self.away_lambda_full = max(0.01, float(away_lambda_full))

    def predict_at_state(self, state: dict) -> dict:
        elapsed = max(0, int(state.get("minute", 0)))
        remaining_regular = max(0, 90 - elapsed)
        stoppage = 3 if elapsed >= 45 else 0
        remaining_minutes = remaining_regular + stoppage

        pregame_rate_home = self.home_lambda_full / 90.0
        pregame_rate_away = self.away_lambda_full / 90.0

        if elapsed > 10:
            xg_rate_home = max(0.0, state.get("home_xg", 0.0)) / elapsed
            xg_rate_away = max(0.0, state.get("away_xg", 0.0)) / elapsed
            w_xg = min(elapsed / 60.0, 0.7)
            w_pregame = 1.0 - w_xg
            home_rate = xg_rate_home * w_xg + pregame_rate_home * w_pregame
            away_rate = xg_rate_away * w_xg + pregame_rate_away * w_pregame
        else:
            home_rate = pregame_rate_home
            away_rate = pregame_rate_away

        # Red card adjustment
        away_reds = int(state.get("away_red_cards", 0) or 0)
        home_reds = int(state.get("home_red_cards", 0) or 0)
        if away_reds > 0:
            home_rate *= 1.35 ** away_reds
            away_rate *= 0.65 ** away_reds
        if home_reds > 0:
            home_rate *= 0.65 ** home_reds
            away_rate *= 1.35 ** home_reds

        # Game state effect: leader defends
        home_score = int(state.get("home_score", 0) or 0)
        away_score = int(state.get("away_score", 0) or 0)
        goal_diff = home_score - away_score
        if goal_diff > 0:
            home_rate *= max(0.8, 1.0 - 0.05 * goal_diff)
            away_rate *= min(1.3, 1.0 + 0.08 * goal_diff)
        elif goal_diff < 0:
            # goal_diff âm; dùng |goal_diff| cho hệ số
            abs_diff = -goal_diff
            away_rate *= max(0.8, 1.0 - 0.05 * abs_diff)
            home_rate *= min(1.3, 1.0 + 0.08 * abs_diff)

        home_rate = max(0.0, home_rate)
        away_rate = max(0.0, away_rate)

        lambda_home_remaining = home_rate * remaining_minutes
        lambda_away_remaining = away_rate * remaining_minutes

        # Distribution of remaining goals (truncate tại 6)
        home_dist = [poisson.pmf(k, lambda_home_remaining) for k in range(7)]
        away_dist = [poisson.pmf(k, lambda_away_remaining) for k in range(7)]

        p_home = p_draw = p_away = 0.0
        for h_add in range(7):
            for a_add in range(7):
                final_h = home_score + h_add
                final_a = away_score + a_add
                prob = home_dist[h_add] * away_dist[a_add]
                if final_h > final_a:
                    p_home += prob
                elif final_h < final_a:
                    p_away += prob
                else:
                    p_draw += prob

        # Totals
        totals: dict = {}
        current_total = home_score + away_score
        lambda_total = lambda_home_remaining + lambda_away_remaining
        for line in (0.5, 1.5, 2.5, 3.5, 4.5):
            needed = line - current_total
            if needed < 0:
                # current already > line → chắc chắn Over
                p_over, p_under = 1.0, 0.0
            else:
                # P(remaining goals ≥ ceil(needed))
                # With half lines (.5), threshold = floor(needed) + 1
                threshold = int(needed) + 1
                p_over = 1.0 - poisson.cdf(threshold - 1, lambda_total)
                p_under = 1.0 - p_over
            totals[line] = {"Over": p_over, "Under": p_under}

        # Next goal
        if lambda_total > 0:
            p_no_more = poisson.pmf(0, lambda_total)
            split_home = lambda_home_remaining / lambda_total
            split_away = lambda_away_remaining / lambda_total
            p_next_home = split_home * (1.0 - p_no_more)
            p_next_away = split_away * (1.0 - p_no_more)
        else:
            p_next_home = 0.0
            p_next_away = 0.0
            p_no_more = 1.0

        return {
            "h2h": {"Home": p_home, "Draw": p_draw, "Away": p_away},
            "totals": totals,
            "next_goal": {"Home": p_next_home, "Away": p_next_away, "None": p_no_more},
            "remaining_minutes": remaining_minutes,
            "lambda_home_remaining": lambda_home_remaining,
            "lambda_away_remaining": lambda_away_remaining,
        }
