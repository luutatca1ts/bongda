"""Weather → expected-goals adjustment.

Adverse weather (heavy rain, strong wind, extreme temps) suppresses
goal scoring. The adjustment is a total-goals shift that the prediction
layer splits evenly between home and away λ.
"""

from __future__ import annotations

MAX_TOTAL_ADJUST = -0.6  # cap on cumulative negative shift (avoid predicting impossibly low totals)


def calculate_weather_adjustment(weather: dict) -> dict:
    """Return {total_goals_adjust, description} given an OpenWeatherMap slice.

    weather: {temp, rain_mm_h, wind_speed, condition, ...}
    Returns:
        total_goals_adjust: float (goals, applied to sum of λ_home + λ_away)
        description:        short VN text for alerts (empty if no adjustment)
    """
    if not weather:
        return {"total_goals_adjust": 0.0, "description": ""}

    adj = 0.0
    notes: list[str] = []

    rain = float(weather.get("rain_mm_h", 0) or 0)
    if rain > 5:
        adj -= 0.4
        notes.append(f"Mưa to ({rain:.1f}mm/h)")
    elif rain >= 2:
        adj -= 0.2
        notes.append(f"Mưa vừa ({rain:.1f}mm/h)")

    wind = float(weather.get("wind_speed", 0) or 0)
    if wind > 15:
        adj -= 0.3
        notes.append(f"Gió mạnh ({wind:.0f}m/s)")

    temp = float(weather.get("temp", 20) or 20)
    if temp > 32:
        adj -= 0.2
        notes.append(f"Nóng ({temp:.0f}°C)")
    elif temp < 0:
        adj -= 0.3
        notes.append(f"Lạnh ({temp:.0f}°C)")

    adj = max(adj, MAX_TOTAL_ADJUST)

    description = ""
    if notes and adj < 0:
        description = ", ".join(notes) + f" → giảm {abs(adj):.1f} bàn"

    return {"total_goals_adjust": adj, "description": description}
