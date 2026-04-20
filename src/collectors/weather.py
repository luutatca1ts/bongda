"""Weather forecast collector — OpenWeatherMap /forecast endpoint.

Fetches forecast-for-timestamp (closest 3h window) given venue lat/lon.
Caches 6 hours because weather forecasts rarely shift materially within
that window and the 5-day/3h forecast re-runs ~every 3-6 hours upstream.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Optional

import requests

from src.config import OPENWEATHER_API_KEY

logger = logging.getLogger(__name__)

BASE_URL = "https://api.openweathermap.org/data/2.5/forecast"
CACHE_TTL_SECONDS = 6 * 3600  # 6 hours

_cache: dict[tuple, tuple[float, dict]] = {}  # (lat_r, lon_r, ts_bucket) -> (fetched_ts, payload)


def is_weather_enabled() -> bool:
    return bool(OPENWEATHER_API_KEY)


# Seed dict: major stadium coords, team_name → (lat, lon). Team matching is
# fuzzy (contains, case-insensitive). Unknown teams → (None, None) → pipeline
# skips weather for that match.
#
# TODO: bootstrap this from API-Football /venues endpoint on demand and
# persist to DB so we can cover all Pinnacle leagues automatically.
_VENUE_COORDS = {
    # EPL
    "manchester city": (53.4831, -2.2004),
    "manchester united": (53.4631, -2.2913),
    "liverpool": (53.4308, -2.9608),
    "arsenal": (51.5549, -0.1084),
    "chelsea": (51.4817, -0.1910),
    "tottenham": (51.6043, -0.0667),
    "newcastle": (54.9756, -1.6216),
    "west ham": (51.5388, -0.0166),
    "everton": (53.4388, -2.9667),
    "aston villa": (52.5092, -1.8847),
    # La Liga
    "real madrid": (40.4531, -3.6884),
    "barcelona": (41.3809, 2.1228),
    "atletico": (40.4362, -3.5994),
    "sevilla": (37.3838, -5.9706),
    "real betis": (37.3564, -5.9817),
    "valencia": (39.4748, -0.3582),
    # Serie A
    "juventus": (45.1096, 7.6411),
    "inter": (45.4781, 9.1240),
    "milan": (45.4781, 9.1240),
    "roma": (41.9341, 12.4547),
    "lazio": (41.9341, 12.4547),
    "napoli": (40.8280, 14.1930),
    # Bundesliga
    "bayern": (48.2188, 11.6247),
    "dortmund": (51.4926, 7.4519),
    "leverkusen": (51.0380, 7.0020),
    "leipzig": (51.3458, 12.4126),
    # Ligue 1
    "psg": (48.8414, 2.2530),
    "marseille": (43.2700, 5.3959),
    "lyon": (45.7653, 4.9822),
    # Dutch Eredivisie
    "ajax": (52.3144, 4.9414),
    "psv": (51.4416, 5.4682),
    "feyenoord": (51.8939, 4.5233),
}


def get_venue_coords(home_team: str) -> tuple:
    """Resolve home team → (lat, lon) from seed dict. Returns (None, None)
    if team isn't in the starter list — caller should then skip weather.
    """
    if not home_team:
        return (None, None)
    key = home_team.lower()
    for k, coords in _VENUE_COORDS.items():
        if k in key or key in k:
            return coords
    return (None, None)


def _cache_key(lat: float, lon: float, target_ts: int) -> tuple:
    # Round coords to 2 decimals (~1km) and timestamp to 3h buckets — matches API granularity.
    return (round(lat, 2), round(lon, 2), target_ts // (3 * 3600))


def _cache_get(key: tuple) -> Optional[dict]:
    hit = _cache.get(key)
    if not hit:
        return None
    ts, payload = hit
    if (time.time() - ts) > CACHE_TTL_SECONDS:
        _cache.pop(key, None)
        return None
    return payload


def _cache_put(key: tuple, payload: dict) -> None:
    _cache[key] = (time.time(), payload)


def get_weather_forecast(lat: float, lon: float, timestamp) -> dict:
    """Return closest-forecast weather for given lat/lon and match kickoff.

    timestamp: int (unix seconds) or datetime or ISO string.
    Returns: {temp, rain_mm_h, wind_speed, condition} or {} on error / missing key.
    """
    if not OPENWEATHER_API_KEY:
        return {}
    if lat is None or lon is None:
        return {}

    # Normalize timestamp to epoch seconds
    if isinstance(timestamp, (int, float)):
        target_ts = int(timestamp)
    elif isinstance(timestamp, datetime):
        target_ts = int(timestamp.timestamp())
    elif isinstance(timestamp, str):
        try:
            target_ts = int(datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp())
        except Exception:
            return {}
    else:
        return {}

    key = _cache_key(float(lat), float(lon), target_ts)
    cached = _cache_get(key)
    if cached is not None:
        return cached

    try:
        resp = requests.get(
            BASE_URL,
            params={
                "lat": lat, "lon": lon,
                "appid": OPENWEATHER_API_KEY,
                "units": "metric",
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning("[Weather] fetch failed lat=%s lon=%s: %s", lat, lon, e)
        _cache_put(key, {})
        return {}

    # Pick forecast slot closest to target_ts
    slots = data.get("list", []) or []
    if not slots:
        _cache_put(key, {})
        return {}
    best = min(slots, key=lambda s: abs(s.get("dt", 0) - target_ts))

    main = best.get("main", {}) or {}
    wind = best.get("wind", {}) or {}
    rain = best.get("rain", {}) or {}
    weather_arr = best.get("weather") or [{}]
    # `rain.3h` is mm over 3h — convert to mm/h.
    rain_3h = rain.get("3h", 0) or rain.get("1h", 0) or 0

    out = {
        "temp": float(main.get("temp", 20.0)),
        "rain_mm_h": float(rain_3h) / 3.0 if rain.get("3h") else float(rain_3h),
        "wind_speed": float(wind.get("speed", 0.0)),
        "condition": (weather_arr[0] or {}).get("main", "Clear"),
        "condition_detail": (weather_arr[0] or {}).get("description", ""),
    }
    _cache_put(key, out)
    return out
