"""
test_open_meteo_weather.py
==========================
Unit tests for OpenMeteoWeatherService.

Run with: pytest tests/test_open_meteo_weather.py -v
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from api.services.open_meteo_weather import (
    MLB_STADIUMS,
    OpenMeteoWeatherService,
    GameWeather,
    _TEAM_TO_STADIUM,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_hourly_response(
    date: str = "2026-04-05",
    hour: int = 13,
    temp_c: float = 15.0,
    wind_kph: float = 20.0,
    wind_dir: float = 180.0,
    precip_pct: int = 10,
    humidity: int = 55,
    precip_mm: float = 0.0,
) -> dict:
    """Build a minimal Open-Meteo hourly response with the target hour."""
    times = [f"{date}T{h:02d}:00" for h in range(24)]
    idx = hour

    def _series(val):
        return [val if i == idx else 0.0 for i in range(24)]

    return {
        "latitude": 41.9484,
        "longitude": -87.6553,
        "timezone": "America/Chicago",
        "hourly": {
            "time": times,
            "temperature_2m": _series(temp_c),
            "windspeed_10m": _series(wind_kph),
            "winddirection_10m": _series(wind_dir),
            "precipitation_probability": _series(precip_pct),
            "relativehumidity_2m": _series(humidity),
            "precipitation": _series(precip_mm),
        },
    }


@pytest.fixture
def svc():
    return OpenMeteoWeatherService()


# ---------------------------------------------------------------------------
# Stadium catalogue
# ---------------------------------------------------------------------------


class TestStadiumCatalogue:
    def test_all_30_stadiums(self, svc):
        assert len(MLB_STADIUMS) == 30

    def test_all_fields_present(self, svc):
        required = {"lat", "lon", "tz", "elev", "team", "name"}
        for key, info in MLB_STADIUMS.items():
            missing = required - set(info.keys())
            assert not missing, f"{key} missing: {missing}"

    def test_team_to_stadium_map(self):
        # Every team abbreviation in stadiums should be in the lookup map
        for key, info in MLB_STADIUMS.items():
            assert info["team"] in _TEAM_TO_STADIUM

    def test_list_stadiums_returns_30(self, svc):
        items = svc.list_stadiums()
        assert len(items) == 30

    def test_coors_field_elevation(self, svc):
        assert MLB_STADIUMS["coors_field"]["elev"] > 1000

    def test_dome_flags(self, svc):
        domes = {s["key"] for s in svc.list_stadiums() if s["is_dome"]}
        assert "tropicana_field" in domes
        assert "wrigley_field" not in domes


# ---------------------------------------------------------------------------
# Stadium resolution
# ---------------------------------------------------------------------------


class TestStadiumResolution:
    def test_resolve_by_key(self, svc):
        info = svc._resolve_stadium("wrigley_field")
        assert info is not None
        assert info["team"] == "CHC"

    def test_resolve_by_team_abbreviation(self, svc):
        info = svc._resolve_stadium("CHC")
        assert info is not None
        assert "wrigley" in info["key"]

    def test_resolve_case_insensitive_key(self, svc):
        info = svc._resolve_stadium("WRIGLEY_FIELD")
        assert info is not None

    def test_resolve_unknown_returns_none(self, svc):
        assert svc._resolve_stadium("mystery_park") is None

    def test_resolve_unknown_team_returns_none(self, svc):
        assert svc._resolve_stadium("ZZZ") is None


# ---------------------------------------------------------------------------
# Forecast weather
# ---------------------------------------------------------------------------


class TestForecastWeather:
    def test_returns_game_weather(self, svc):
        raw = _make_hourly_response(temp_c=20.0, wind_kph=15.0, wind_dir=180.0)
        with patch.object(svc, "_fetch", return_value=raw):
            wx = svc.get_game_weather("wrigley_field", "2026-04-05", hour=13)
        assert isinstance(wx, GameWeather)

    def test_temperature_conversion(self, svc):
        raw = _make_hourly_response(temp_c=20.0)
        with patch.object(svc, "_fetch", return_value=raw):
            wx = svc.get_game_weather("wrigley_field", "2026-04-05", hour=13)
        assert abs(wx.temp_f - 68.0) < 0.5

    def test_wind_mph_conversion(self, svc):
        raw = _make_hourly_response(wind_kph=16.093)  # exactly 10 mph
        with patch.object(svc, "_fetch", return_value=raw):
            wx = svc.get_game_weather("wrigley_field", "2026-04-05", hour=13)
        assert abs(wx.wind_speed_mph - 10.0) < 0.5

    def test_wind_direction_stored(self, svc):
        raw = _make_hourly_response(wind_dir=270.0)
        with patch.object(svc, "_fetch", return_value=raw):
            wx = svc.get_game_weather("wrigley_field", "2026-04-05", hour=13)
        assert wx.wind_direction_deg == 270.0

    def test_precip_pct_stored(self, svc):
        raw = _make_hourly_response(precip_pct=45)
        with patch.object(svc, "_fetch", return_value=raw):
            wx = svc.get_game_weather("wrigley_field", "2026-04-05", hour=13)
        assert wx.precip_pct == 45

    def test_humidity_stored(self, svc):
        raw = _make_hourly_response(humidity=70)
        with patch.object(svc, "_fetch", return_value=raw):
            wx = svc.get_game_weather("wrigley_field", "2026-04-05", hour=13)
        assert wx.humidity_pct == 70

    def test_data_source_forecast(self, svc):
        raw = _make_hourly_response()
        with patch.object(svc, "_fetch", return_value=raw):
            wx = svc.get_game_weather("wrigley_field", "2026-04-05", hour=13)
        assert wx.data_source == "open-meteo-forecast"

    def test_unknown_stadium_returns_none(self, svc):
        wx = svc.get_game_weather("nonexistent_park", "2026-04-05")
        assert wx is None

    def test_fetch_failure_returns_none(self, svc):
        with patch.object(svc, "_fetch", return_value=None):
            wx = svc.get_game_weather("wrigley_field", "2026-04-05")
        assert wx is None

    def test_team_abbreviation_works(self, svc):
        raw = _make_hourly_response()
        with patch.object(svc, "_fetch", return_value=raw):
            wx = svc.get_game_weather("CHC", "2026-04-05", hour=13)
        assert wx is not None
        assert "Wrigley" in wx.stadium_name


# ---------------------------------------------------------------------------
# Historical weather
# ---------------------------------------------------------------------------


class TestHistoricalWeather:
    def test_data_source_archive(self, svc):
        # Mock response must use same date as requested
        raw = _make_hourly_response(date="2025-09-01", hour=13)
        with patch.object(svc, "_fetch", return_value=raw):
            wx = svc.get_historical_weather("yankee_stadium", "2025-09-01", hour=13)
        assert wx is not None
        assert wx.data_source == "open-meteo-archive"

    def test_yankee_stadium_metadata(self, svc):
        raw = _make_hourly_response(date="2025-09-01", hour=19)
        with patch.object(svc, "_fetch", return_value=raw):
            wx = svc.get_historical_weather("yankee_stadium", "2025-09-01", hour=19)
        assert wx is not None
        assert "Yankee" in wx.stadium_name


# ---------------------------------------------------------------------------
# Wind classification
# ---------------------------------------------------------------------------


class TestWindClassification:
    def test_blowing_out_south_wind(self, svc):
        out, inp = svc._classify_wind_direction(180.0, 15.0)
        assert out is True
        assert inp is False

    def test_blowing_in_north_wind(self, svc):
        out, inp = svc._classify_wind_direction(0.0, 15.0)
        assert out is False
        assert inp is True

    def test_no_significant_wind(self, svc):
        out, inp = svc._classify_wind_direction(180.0, 5.0)
        assert out is False
        assert inp is False

    def test_crosswind_neither_out_nor_in(self, svc):
        out, inp = svc._classify_wind_direction(90.0, 20.0)
        assert out is False
        assert inp is False


# ---------------------------------------------------------------------------
# HR boost calculation
# ---------------------------------------------------------------------------


class TestHRBoost:
    def test_neutral_conditions(self, svc):
        boost = svc._calc_hr_boost(0.0, False, 0)
        assert boost == pytest.approx(1.0, abs=0.05)

    def test_wind_blowing_out_increases_boost(self, svc):
        boost_no_wind = svc._calc_hr_boost(0.0, False, 0)
        boost_wind_out = svc._calc_hr_boost(20.0, True, 0)
        assert boost_wind_out > boost_no_wind

    def test_coors_altitude_boost(self, svc):
        # Coors Field at 1580m
        boost_coors = svc._calc_hr_boost(0.0, False, 1580)
        boost_sea = svc._calc_hr_boost(0.0, False, 0)
        assert boost_coors > boost_sea + 0.10  # at least 10% more

    def test_boost_capped_at_2(self, svc):
        boost = svc._calc_hr_boost(50.0, True, 2000)
        assert boost <= 2.0

    def test_boost_floor_at_0_5(self, svc):
        boost = svc._calc_hr_boost(50.0, False, 0)
        assert boost >= 0.5


# ---------------------------------------------------------------------------
# Offense boost calculation
# ---------------------------------------------------------------------------


class TestOffenseBoost:
    def test_dome_returns_1(self, svc):
        boost = svc._calc_offense_boost(70.0, 15.0, True, is_dome=True)
        assert boost == 1.0

    def test_cold_suppresses_offense(self, svc):
        warm = svc._calc_offense_boost(75.0, 0.0, False, is_dome=False)
        cold = svc._calc_offense_boost(35.0, 0.0, False, is_dome=False)
        assert cold < warm

    def test_hot_boosts_offense(self, svc):
        cool = svc._calc_offense_boost(65.0, 0.0, False, is_dome=False)
        hot = svc._calc_offense_boost(95.0, 0.0, False, is_dome=False)
        assert hot > cool

    def test_floor_and_ceiling(self, svc):
        # Worst conditions
        low = svc._calc_offense_boost(0.0, 0.0, False, is_dome=False)
        assert low >= 0.7
        # Best conditions
        high = svc._calc_offense_boost(110.0, 30.0, True, is_dome=False)
        assert high <= 1.20


# ---------------------------------------------------------------------------
# Weather features dict
# ---------------------------------------------------------------------------


class TestWeatherFeatures:
    def test_required_keys_present(self, svc):
        raw = _make_hourly_response()
        with patch.object(svc, "_fetch", return_value=raw):
            features = svc.get_weather_features("wrigley_field", "2026-04-05")
        required = {
            "temp_f", "wind_speed_mph", "wind_direction_deg",
            "precip_pct", "humidity_pct", "elevation_m",
            "is_dome", "wind_blowing_out", "wind_blowing_in",
            "hr_boost", "offense_boost",
        }
        assert required.issubset(features.keys())

    def test_all_values_are_float(self, svc):
        raw = _make_hourly_response()
        with patch.object(svc, "_fetch", return_value=raw):
            features = svc.get_weather_features("wrigley_field", "2026-04-05")
        for k, v in features.items():
            assert isinstance(v, float), f"{k} is not float: {type(v)}"

    def test_null_features_on_failure(self, svc):
        with patch.object(svc, "_fetch", return_value=None):
            features = svc.get_weather_features("wrigley_field", "2026-04-05")
        assert features["hr_boost"] == 1.0
        assert features["offense_boost"] == 1.0

    def test_dome_feature_flag(self, svc):
        raw = _make_hourly_response()
        with patch.object(svc, "_fetch", return_value=raw):
            features = svc.get_weather_features("tropicana_field", "2026-04-05")
        assert features["is_dome"] == 1.0

    def test_outdoor_feature_flag(self, svc):
        raw = _make_hourly_response()
        with patch.object(svc, "_fetch", return_value=raw):
            features = svc.get_weather_features("wrigley_field", "2026-04-05")
        assert features["is_dome"] == 0.0


# ---------------------------------------------------------------------------
# Cardinal direction
# ---------------------------------------------------------------------------


class TestCardinalDirection:
    @pytest.mark.parametrize("deg,expected", [
        (0,   "N"),
        (45,  "NE"),
        (90,  "E"),
        (135, "SE"),
        (180, "S"),
        (225, "SW"),
        (270, "W"),
        (315, "NW"),
        (360, "N"),
    ])
    def test_cardinal_directions(self, svc, deg, expected):
        assert svc._deg_to_cardinal(deg) == expected


# ---------------------------------------------------------------------------
# Retry logic
# ---------------------------------------------------------------------------


class TestRetryLogic:
    def test_retries_on_error(self, svc):
        import requests as req

        call_count = 0

        def flaky_get(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise req.exceptions.Timeout("slow")
            mock = MagicMock()
            mock.raise_for_status.return_value = None
            mock.json.return_value = _make_hourly_response()
            return mock

        with patch.object(svc._session, "get", side_effect=flaky_get):
            with patch("api.services.open_meteo_weather.time.sleep"):
                result = svc._fetch("https://fake.url", {})

        assert result is not None
        assert call_count == 3

    def test_returns_none_after_max_retries(self, svc):
        import requests as req

        with patch.object(
            svc._session, "get", side_effect=req.exceptions.ConnectionError("down")
        ):
            with patch("api.services.open_meteo_weather.time.sleep"):
                result = svc._fetch("https://fake.url", {})

        assert result is None
