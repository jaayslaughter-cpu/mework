"""
open_meteo_weather.py
=====================
Weather data service using Open-Meteo (https://open-meteo.com).

✅ Completely FREE — no API key required
✅ Forecast API: https://api.open-meteo.com/v1/forecast
✅ Historical Archive API: https://archive-api.open-meteo.com/v1/archive
✅ Returns temperature, wind speed/direction, precipitation probability,
   humidity — all fields needed for PropIQ's WeatherAgent

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Open-Meteo endpoint constants
# ---------------------------------------------------------------------------

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

HOURLY_VARS = (
    "temperature_2m,"
    "windspeed_10m,"
    "winddirection_10m,"
    "precipitation_probability,"
    "relativehumidity_2m,"
    "precipitation"
)

REQUEST_TIMEOUT = 15
MAX_RETRIES = 3
RETRY_BACKOFF = 1.5

# ---------------------------------------------------------------------------
# MLB Stadium coordinates (lat, lon, elevation_m, timezone)
# ---------------------------------------------------------------------------

MLB_STADIUMS: dict[str, dict[str, Any]] = {
    # American League East
    "yankee_stadium":        {"lat": 40.8296,  "lon": -73.9262,  "tz": "America/New_York",     "elev": 15,   "team": "NYY", "name": "Yankee Stadium"},
    "fenway_park":           {"lat": 42.3467,  "lon": -71.0972,  "tz": "America/New_York",     "elev": 9,    "team": "BOS", "name": "Fenway Park"},
    "oriole_park":           {"lat": 39.2839,  "lon": -76.6217,  "tz": "America/New_York",     "elev": 9,    "team": "BAL", "name": "Oriole Park at Camden Yards"},
    "rogers_centre":         {"lat": 43.6414,  "lon": -79.3894,  "tz": "America/Toronto",      "elev": 76,   "team": "TOR", "name": "Rogers Centre"},
    "tropicana_field":       {"lat": 27.7683,  "lon": -82.6534,  "tz": "America/New_York",     "elev": 9,    "team": "TB",  "name": "Tropicana Field"},
    # American League Central
    "guaranteed_rate_field": {"lat": 41.8300,  "lon": -87.6339,  "tz": "America/Chicago",      "elev": 182,  "team": "CWS", "name": "Guaranteed Rate Field"},
    "progressive_field":     {"lat": 41.4962,  "lon": -81.6852,  "tz": "America/New_York",     "elev": 197,  "team": "CLE", "name": "Progressive Field"},
    "comerica_park":         {"lat": 42.3390,  "lon": -83.0485,  "tz": "America/Detroit",      "elev": 192,  "team": "DET", "name": "Comerica Park"},
    "kauffman_stadium":      {"lat": 39.0517,  "lon": -94.4803,  "tz": "America/Chicago",      "elev": 330,  "team": "KC",  "name": "Kauffman Stadium"},
    "target_field":          {"lat": 44.9817,  "lon": -93.2778,  "tz": "America/Chicago",      "elev": 264,  "team": "MIN", "name": "Target Field"},
    # American League West
    "minute_maid_park":      {"lat": 29.7573,  "lon": -95.3555,  "tz": "America/Chicago",      "elev": 12,   "team": "HOU", "name": "Minute Maid Park"},
    "angel_stadium":         {"lat": 33.8003,  "lon": -117.8827, "tz": "America/Los_Angeles",  "elev": 47,   "team": "LAA", "name": "Angel Stadium"},
    "oakland_coliseum":      {"lat": 37.7516,  "lon": -122.2005, "tz": "America/Los_Angeles",  "elev": 2,    "team": "OAK", "name": "Oakland Coliseum"},
    "t_mobile_park":         {"lat": 47.5914,  "lon": -122.3325, "tz": "America/Los_Angeles",  "elev": 0,    "team": "SEA", "name": "T-Mobile Park"},
    "globe_life_field":      {"lat": 32.7473,  "lon": -97.0837,  "tz": "America/Chicago",      "elev": 182,  "team": "TEX", "name": "Globe Life Field"},
    # National League East
    "truist_park":           {"lat": 33.8908,  "lon": -84.4678,  "tz": "America/New_York",     "elev": 302,  "team": "ATL", "name": "Truist Park"},
    "citizens_bank_park":    {"lat": 39.9061,  "lon": -75.1665,  "tz": "America/New_York",     "elev": 9,    "team": "PHI", "name": "Citizens Bank Park"},
    "nationals_park":        {"lat": 38.8730,  "lon": -77.0074,  "tz": "America/New_York",     "elev": 9,    "team": "WSH", "name": "Nationals Park"},
    "citi_field":            {"lat": 40.7571,  "lon": -73.8458,  "tz": "America/New_York",     "elev": 4,    "team": "NYM", "name": "Citi Field"},
    "marlins_park":          {"lat": 25.7781,  "lon": -80.2197,  "tz": "America/New_York",     "elev": 3,    "team": "MIA", "name": "loanDepot park"},
    # National League Central
    "wrigley_field":         {"lat": 41.9484,  "lon": -87.6553,  "tz": "America/Chicago",      "elev": 182,  "team": "CHC", "name": "Wrigley Field"},
    "great_american":        {"lat": 39.0978,  "lon": -84.5082,  "tz": "America/New_York",     "elev": 201,  "team": "CIN", "name": "Great American Ball Park"},
    "american_family_field": {"lat": 43.0280,  "lon": -87.9712,  "tz": "America/Chicago",      "elev": 185,  "team": "MIL", "name": "American Family Field"},
    "pnc_park":              {"lat": 40.4469,  "lon": -80.0057,  "tz": "America/New_York",     "elev": 222,  "team": "PIT", "name": "PNC Park"},
    "busch_stadium":         {"lat": 38.6226,  "lon": -90.1928,  "tz": "America/Chicago",      "elev": 142,  "team": "STL", "name": "Busch Stadium"},
    # National League West
    "chase_field":           {"lat": 33.4453,  "lon": -112.0667, "tz": "America/Phoenix",      "elev": 331,  "team": "ARI", "name": "Chase Field"},
    "coors_field":           {"lat": 39.7559,  "lon": -104.9942, "tz": "America/Denver",       "elev": 1580, "team": "COL", "name": "Coors Field"},
    "dodger_stadium":        {"lat": 34.0739,  "lon": -118.2400, "tz": "America/Los_Angeles",  "elev": 163,  "team": "LAD", "name": "Dodger Stadium"},
    "petco_park":            {"lat": 32.7076,  "lon": -117.1570, "tz": "America/Los_Angeles",  "elev": 15,   "team": "SD",  "name": "Petco Park"},
    "oracle_park":           {"lat": 37.7786,  "lon": -122.3893, "tz": "America/Los_Angeles",  "elev": 1,    "team": "SF",  "name": "Oracle Park"},
}

# Team abbreviation → stadium key (for quick lookup by team)
_TEAM_TO_STADIUM: dict[str, str] = {
    v["team"]: k for k, v in MLB_STADIUMS.items()
}


# ---------------------------------------------------------------------------
# Data class
# ---------------------------------------------------------------------------


@dataclass
class GameWeather:
    """Parsed weather snapshot for a specific MLB game."""

    stadium_name: str
    game_date: str          # YYYY-MM-DD
    game_hour_local: int    # 0–23 local time
    temp_c: float
    temp_f: float
    wind_speed_kph: float
    wind_speed_mph: float
    wind_direction_deg: float
    wind_cardinal: str      # "N", "NE", "E", …
    precip_pct: int         # precipitation probability 0–100
    precipitation_mm: float
    humidity_pct: int
    elevation_m: int
    # PropIQ-specific derived fields
    is_dome: bool
    wind_blowing_out: bool  # True if wind aids offense (toward CF)
    wind_blowing_in: bool   # True if wind suppresses offense
    hr_boost: float         # multiplicative HR probability modifier
    offense_boost: float    # multiplicative general offense modifier
    data_source: str = "open-meteo"


# ---------------------------------------------------------------------------
# Service class
# ---------------------------------------------------------------------------


class OpenMeteoWeatherService:
    """
    Fetches weather data for MLB games using the Open-Meteo API.

    No API key required.  Supports both game-day forecasts (up to 16 days
    ahead) and historical lookups (archive endpoint, 1940-present).

    Parameters
    ----------
    timeout:
        HTTP request timeout in seconds.
    max_retries:
        Number of retry attempts on transient network errors.
    """

    # Retractable roof / dome stadiums — weather is irrelevant
    DOME_STADIUMS = {
        "tropicana_field",
        "rogers_centre",
        "globe_life_field",
        "american_family_field",
        "chase_field",
        "marlins_park",
        "minute_maid_park",
    }

    def __init__(
        self,
        timeout: int = REQUEST_TIMEOUT,
        max_retries: int = MAX_RETRIES,
    ) -> None:
        self._timeout = timeout
        self._max_retries = max_retries
        self._session = requests.Session()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def get_game_weather(
        self,
        stadium: str,
        game_date: str,
        hour: int = 13,
    ) -> GameWeather | None:
        """
        Fetch weather forecast for a game.

        Parameters
        ----------
        stadium:
            Stadium key (e.g. ``"wrigley_field"``) OR team abbreviation
            (e.g. ``"CHC"``).  Case-insensitive.
        game_date:
            Date string ``"YYYY-MM-DD"``.
        hour:
            Local hour of first pitch (0–23). Default 13 = 1 PM.

        Returns
        -------
        :class:`GameWeather` or ``None`` if the stadium is unknown.
        """
        info = self._resolve_stadium(stadium)
        if info is None:
            logger.warning("Unknown stadium/team: %s", stadium)
            return None

        params = {
            "latitude": info["lat"],
            "longitude": info["lon"],
            "hourly": HOURLY_VARS,
            "timezone": info["tz"],
            "forecast_days": 16,
        }
        raw = self._fetch(FORECAST_URL, params)
        if raw is None:
            return None

        return self._extract_hour(
            raw, info, game_date, hour, is_forecast=True
        )

    def get_historical_weather(
        self,
        stadium: str,
        game_date: str,
        hour: int = 13,
    ) -> GameWeather | None:
        """
        Fetch historical weather for a past game (for backtesting).

        Parameters
        ----------
        stadium:
            Stadium key or team abbreviation.
        game_date:
            ``"YYYY-MM-DD"`` — must be in the past.
        hour:
            Local first-pitch hour.

        Returns
        -------
        :class:`GameWeather` or ``None`` on failure.
        """
        info = self._resolve_stadium(stadium)
        if info is None:
            logger.warning("Unknown stadium/team: %s", stadium)
            return None

        params = {
            "latitude": info["lat"],
            "longitude": info["lon"],
            "hourly": HOURLY_VARS,
            "timezone": info["tz"],
            "start_date": game_date,
            "end_date": game_date,
        }
        raw = self._fetch(ARCHIVE_URL, params)
        if raw is None:
            return None

        return self._extract_hour(
            raw, info, game_date, hour, is_forecast=False
        )

    def get_weather_features(
        self,
        stadium: str,
        game_date: str,
        hour: int = 13,
        historical: bool = False,
    ) -> dict[str, float]:
        """
        Return a flat dict of weather features suitable for ML model input.

        Keys:
          temp_f, wind_speed_mph, wind_direction_deg,
          precip_pct, humidity_pct, elevation_m,
          is_dome, wind_blowing_out, hr_boost, offense_boost
        """
        fn = self.get_historical_weather if historical else self.get_game_weather
        wx = fn(stadium, game_date, hour)
        if wx is None:
            return self._null_features()
        return {
            "temp_f": wx.temp_f,
            "wind_speed_mph": wx.wind_speed_mph,
            "wind_direction_deg": wx.wind_direction_deg,
            "precip_pct": float(wx.precip_pct),
            "humidity_pct": float(wx.humidity_pct),
            "elevation_m": float(wx.elevation_m),
            "is_dome": 1.0 if wx.is_dome else 0.0,
            "wind_blowing_out": 1.0 if wx.wind_blowing_out else 0.0,
            "wind_blowing_in": 1.0 if wx.wind_blowing_in else 0.0,
            "hr_boost": wx.hr_boost,
            "offense_boost": wx.offense_boost,
        }

    def list_stadiums(self) -> list[dict[str, Any]]:
        """Return metadata for all 30 MLB stadiums."""
        return [
            {
                "key": k,
                "name": v["name"],
                "team": v["team"],
                "lat": v["lat"],
                "lon": v["lon"],
                "elevation_m": v["elev"],
                "timezone": v["tz"],
                "is_dome": k in self.DOME_STADIUMS,
            }
            for k, v in MLB_STADIUMS.items()
        ]

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _resolve_stadium(self, stadium: str) -> dict[str, Any] | None:
        """Resolve stadium key or team abbreviation to stadium metadata."""
        key = stadium.lower().replace(" ", "_")
        if key in MLB_STADIUMS:
            return {**MLB_STADIUMS[key], "key": key}
        # Try team abbreviation
        team_key = _TEAM_TO_STADIUM.get(stadium.upper())
        if team_key:
            return {**MLB_STADIUMS[team_key], "key": team_key}
        return None

    def _fetch(
        self, url: str, params: dict[str, Any]
    ) -> dict[str, Any] | None:
        """HTTP GET with retry logic."""
        last_exc: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                resp = self._session.get(url, params=params, timeout=self._timeout)
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.RequestException as exc:
                last_exc = exc
                logger.warning("OpenMeteo attempt %d failed: %s", attempt, exc)
                if attempt < self._max_retries:
                    time.sleep(RETRY_BACKOFF * attempt)
        logger.error("OpenMeteo: all retries exhausted. Last error: %s", last_exc)
        return None

    def _extract_hour(
        self,
        raw: dict[str, Any],
        info: dict[str, Any],
        game_date: str,
        hour: int,
        is_forecast: bool,
    ) -> GameWeather | None:
        """
        Pull the hourly slice matching ``game_date`` at ``hour`` from the
        Open-Meteo response and construct a :class:`GameWeather` instance.
        """
        hourly = raw.get("hourly", {})
        times: list[str] = hourly.get("time", [])

        target = f"{game_date}T{hour:02d}:00"
        try:
            idx = times.index(target)
        except ValueError:
            # Try to find closest hour in that day
            idx = self._find_closest_index(times, game_date, hour)
            if idx is None:
                logger.warning(
                    "OpenMeteo: no data for %s hour %d at %s",
                    game_date,
                    hour,
                    info.get("name"),
                )
                return None

        def val(key: str, default: float = 0.0) -> float:
            lst = hourly.get(key, [])
            return float(lst[idx]) if idx < len(lst) and lst[idx] is not None else default

        temp_c = val("temperature_2m")
        temp_f = temp_c * 9 / 5 + 32
        wind_kph = val("windspeed_10m")
        wind_mph = wind_kph * 0.621371
        wind_dir = val("winddirection_10m")
        precip_pct = int(val("precipitation_probability", 0))
        precipitation = val("precipitation", 0.0)
        humidity = int(val("relativehumidity_2m", 50))

        stadium_key = info.get("key", "")
        is_dome = stadium_key in self.DOME_STADIUMS

        wind_out, wind_in = self._classify_wind_direction(wind_dir, wind_mph)
        hr_boost = self._calc_hr_boost(wind_mph, wind_out, info.get("elev", 0))
        offense_boost = self._calc_offense_boost(
            temp_f, wind_mph, wind_out, is_dome
        )

        return GameWeather(
            stadium_name=info.get("name", stadium_key),
            game_date=game_date,
            game_hour_local=hour,
            temp_c=round(temp_c, 1),
            temp_f=round(temp_f, 1),
            wind_speed_kph=round(wind_kph, 1),
            wind_speed_mph=round(wind_mph, 1),
            wind_direction_deg=round(wind_dir, 1),
            wind_cardinal=self._deg_to_cardinal(wind_dir),
            precip_pct=precip_pct,
            precipitation_mm=round(precipitation, 2),
            humidity_pct=humidity,
            elevation_m=info.get("elev", 0),
            is_dome=is_dome,
            wind_blowing_out=wind_out,
            wind_blowing_in=wind_in,
            hr_boost=round(hr_boost, 3),
            offense_boost=round(offense_boost, 3),
            data_source="open-meteo-archive" if not is_forecast else "open-meteo-forecast",
        )

    @staticmethod
    def _find_closest_index(
        times: list[str], game_date: str, hour: int
    ) -> int | None:
        """Find the index of the closest time slot on the target date."""
        day_slots = [
            (i, t) for i, t in enumerate(times) if t.startswith(game_date)
        ]
        if not day_slots:
            return None
        # Find slot closest to desired hour
        target_min = hour * 60
        best_idx, best_diff = None, float("inf")
        for i, t in day_slots:
            try:
                slot_hour = int(t[11:13])
                diff = abs(slot_hour * 60 - target_min)
                if diff < best_diff:
                    best_diff = diff
                    best_idx = i
            except (IndexError, ValueError):
                continue
        return best_idx

    # ------------------------------------------------------------------
    # Wind and weather calculation helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _deg_to_cardinal(deg: float) -> str:
        """Convert meteorological wind direction degrees to cardinal."""
        dirs = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
                "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
        idx = round(deg / 22.5) % 16
        return dirs[idx]

    @staticmethod
    def _classify_wind_direction(
        wind_dir_deg: float, wind_mph: float, threshold_mph: float = 8.0
    ) -> tuple[bool, bool]:
        """
        Classify wind as blowing out vs in.

        Convention: most MLB parks have CF roughly to the North (from
        home plate perspective).  Wind FROM the South (≈180°) blows
        toward CF → blowing out.  Wind FROM the North (≈0°/360°) blows
        toward home plate → blowing in.

        This is a reasonable approximation; park-specific orientation
        would require individual vectors.

        Returns (wind_blowing_out, wind_blowing_in).
        """
        if wind_mph < threshold_mph:
            return False, False
        # Blowing out: wind from roughly S/SW/SE sector (135°–225°)
        wind_blowing_out = 135 <= wind_dir_deg <= 225
        # Blowing in: wind from roughly N/NW/NE sector (315°–360° or 0°–45°)
        wind_blowing_in = wind_dir_deg >= 315 or wind_dir_deg <= 45
        return wind_blowing_out, wind_blowing_in

    @staticmethod
    def _calc_hr_boost(
        wind_mph: float, wind_out: bool, elevation_m: int
    ) -> float:
        """
        Estimate a multiplicative home run probability modifier.

        Factors:
        - Wind blowing out ≥ 15 mph → +10% per 5 mph above threshold
        - Altitude (Coors effect) — every 300m above sea level ≈ +3%
        - Wind blowing in ≥ 15 mph → suppresses HRs
        """
        modifier = 1.0
        # Altitude boost (e.g. Coors at 1580m ≈ +15%)
        altitude_boost = (elevation_m / 300) * 0.03
        modifier += altitude_boost

        if wind_out and wind_mph >= 15:
            wind_boost = ((wind_mph - 15) / 5) * 0.10
            modifier += min(wind_boost, 0.30)  # cap at +30%
        elif not wind_out and wind_mph >= 15:
            wind_penalty = ((wind_mph - 15) / 5) * 0.07
            modifier -= min(wind_penalty, 0.20)  # cap at -20%

        return max(0.5, min(modifier, 2.0))

    @staticmethod
    def _calc_offense_boost(
        temp_f: float,
        wind_mph: float,
        wind_out: bool,
        is_dome: bool,
    ) -> float:
        """
        Estimate multiplicative general offense modifier (hits, TB, runs).

        Factors:
        - Temperature: below 50°F suppresses offense
        - Wind blowing out: boosts
        - Dome: neutral
        """
        if is_dome:
            return 1.0

        modifier = 1.0

        # Temperature effect
        if temp_f < 50:
            cold_penalty = (50 - temp_f) * 0.004  # -0.4% per degree below 50
            modifier -= min(cold_penalty, 0.15)
        elif temp_f > 80:
            heat_boost = (temp_f - 80) * 0.003  # +0.3% per degree above 80
            modifier += min(heat_boost, 0.10)

        # Wind effect
        if wind_out and wind_mph >= 10:
            modifier += min((wind_mph - 10) / 10 * 0.05, 0.12)

        return max(0.7, min(modifier, 1.20))

    @staticmethod
    def _null_features() -> dict[str, float]:
        """Return neutral features when weather data is unavailable."""
        return {
            "temp_f": 72.0,
            "wind_speed_mph": 0.0,
            "wind_direction_deg": 0.0,
            "precip_pct": 0.0,
            "humidity_pct": 50.0,
            "elevation_m": 100.0,
            "is_dome": 0.0,
            "wind_blowing_out": 0.0,
            "wind_blowing_in": 0.0,
            "hr_boost": 1.0,
            "offense_boost": 1.0,
        }
