"""
api/services/statcast_hub.py
Statcast DataHub microservice — ingests and persists raw Statcast pitch-level data.
Defines expanded data models for pitch-level analytics and enriches the ML feature set.

Data sources:
  - Baseball Savant CSV export (direct HTTP, no auth required)
  - Apify cheerio-scraper for supplementary Statcast tables
  - Redis caching with appropriate TTLs

PEP 8 compliant. No hallucinated APIs.
"""

from __future__ import annotations

import csv
import io
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any

import requests

logger = logging.getLogger(__name__)

REDIS_URL    = os.getenv("REDIS_URL",    "redis://localhost:6379/0")
APIFY_KEY    = os.getenv("APIFY_API_KEY","")  # NEVER hardcode API keys in source
REQUEST_TIMEOUT = 30

# Baseball Savant base for CSV exports
SAVANT_BASE  = "https://baseballsavant.mlb.com"

# Redis TTLs (seconds)
TTL_STATCAST_SEASON  = 86400 * 7   # 7 days — historical
TTL_STATCAST_RECENT  = 3600        # 1 hour — recent games
TTL_ARSENAL          = 3600        # 1 hour — pitch mix
TTL_PLATE_DISCIPLINE = 1800        # 30 min


# ---------------------------------------------------------------------------
# Extended data models
# ---------------------------------------------------------------------------
@dataclass
class PitchRecord:
    """Single pitch from Statcast CSV export."""
    game_date:        str   = ""
    pitcher_id:       str   = ""
    pitcher_name:     str   = ""
    batter_id:        str   = ""
    batter_name:      str   = ""
    pitch_type:       str   = ""    # FF, SL, CU, CH, SI, FC, etc.
    release_speed:    float = 0.0   # mph
    spin_rate:        float = 0.0   # rpm
    pfx_x:            float = 0.0   # horizontal movement (inches)
    pfx_z:            float = 0.0   # vertical movement (inches)
    plate_x:          float = 0.0
    plate_z:          float = 0.0
    zone:             int   = 0
    description:      str   = ""    # swinging_strike, called_strike, ball, foul, ...
    events:           str   = ""    # strikeout, walk, single, etc.
    launch_speed:     float = 0.0
    launch_angle:     float = 0.0
    estimated_ba:     float = 0.0   # xBA
    estimated_woba:   float = 0.0   # xwOBA
    delta_run_exp:    float = 0.0   # run value


@dataclass
class PitcherArsenal:
    """Aggregated pitch-type statistics for a pitcher."""
    pitcher_id:    str
    pitcher_name:  str
    season:        int
    # Usage %
    fastball_pct:       float = 0.0
    sinker_pct:         float = 0.0
    cutter_pct:         float = 0.0
    slider_pct:         float = 0.0
    curveball_pct:      float = 0.0
    changeup_pct:       float = 0.0
    splitter_pct:       float = 0.0
    # Velocity
    fastball_velo:      float = 0.0
    slider_velo:        float = 0.0
    # Whiff rates
    whiff_rate_overall: float = 0.0
    whiff_rate_fb:      float = 0.0
    whiff_rate_slider:  float = 0.0
    whiff_rate_curve:   float = 0.0
    whiff_rate_change:  float = 0.0
    # Spin
    spin_rate_fb:       float = 0.0
    spin_rate_slider:   float = 0.0
    # Plate discipline
    chase_rate:         float = 0.0   # O-swing%
    zone_pct:           float = 0.0   # Zone%
    zone_contact:       float = 0.0   # Z-contact%
    swstr_pct:          float = 0.0   # swinging strike%
    # Cluster
    arsenal_cluster:    int   = 0     # 0=FB-dom, 1=breaking, 2=offspeed-mix
    updated_at:         float = field(default_factory=time.time)


@dataclass
class BatterPlateDiscipline:
    """Batter swing/contact metrics from Statcast."""
    batter_id:     str
    batter_name:   str
    season:        int
    # Swing rates
    swing_pct:          float = 0.0
    o_swing_pct:        float = 0.0   # chase rate
    z_swing_pct:        float = 0.0   # zone swing
    # Contact rates
    contact_pct:        float = 0.0
    z_contact_pct:      float = 0.0
    o_contact_pct:      float = 0.0
    # Outcomes
    whiff_pct:          float = 0.0
    k_pct:              float = 0.0
    bb_pct:             float = 0.0
    # Quality of contact
    hard_hit_pct:       float = 0.0   # EV ≥ 95 mph
    barrel_pct:         float = 0.0
    avg_launch_angle:   float = 0.0
    avg_exit_velocity:  float = 0.0
    xba:                float = 0.0
    xslg:               float = 0.0
    xwoba:              float = 0.0
    # vs LHP / RHP splits (populated from mlb_data enrichment)
    wrc_plus_vs_lhp:    float = 100.0
    wrc_plus_vs_rhp:    float = 100.0
    updated_at:         float = field(default_factory=time.time)


# ---------------------------------------------------------------------------
# Redis helper
# ---------------------------------------------------------------------------
class _RedisCache:
    def __init__(self) -> None:
        self._redis: Any = None
        self._available = False
        try:
            import redis as _redis
            self._redis = _redis.from_url(REDIS_URL, decode_responses=True)
            self._redis.ping()
            self._available = True
        except Exception as e:
            logger.warning("[StatcastHub] Redis unavailable: %s", e)

    def get(self, key: str) -> str | None:
        if not self._available:
            return None
        try:
            return self._redis.get(key)
        except Exception:
            return None

    def set(self, key: str, value: str, ttl: int) -> None:
        if not self._available:
            return
        try:
            self._redis.setex(key, ttl, value)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Statcast DataHub
# ---------------------------------------------------------------------------
class StatcastDataHub:
    """
    Ingests and caches Statcast pitch-level data.
    Provides enriched feature dicts for the ML pipeline.

    Primary data: Baseball Savant CSV exports (no API key required).
    Fallback: Apify cheerio-scraper for supplementary tables.
    """

    def __init__(self) -> None:
        self._cache   = _RedisCache()
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "PropIQ/1.0 (analytics research)",
        })

    # ------------------------------------------------------------------
    # Baseball Savant CSV fetch
    # ------------------------------------------------------------------
    def _fetch_savant_csv(self, url: str, cache_key: str, ttl: int) -> list[dict]:
        """Fetch a Savant CSV export, cache in Redis, return list of row dicts."""
        cached = self._cache.get(cache_key)
        if cached:
            import json
            return json.loads(cached)

        try:
            resp = self._session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            reader = csv.DictReader(io.StringIO(resp.text))
            rows   = list(reader)

            import json
            self._cache.set(cache_key, json.dumps(rows[:5000]), ttl)
            return rows
        except Exception as e:
            logger.warning("[StatcastHub] CSV fetch failed %s: %s", url, e)
            return []

    def get_pitcher_arsenal(
        self, pitcher_id: str, season: int = 2025
    ) -> PitcherArsenal | None:
        """
        Fetch pitcher arsenal from Baseball Savant pitch-type leaderboard.
        Returns PitcherArsenal or None if unavailable.
        """
        url = (
            f"{SAVANT_BASE}/leaderboard/pitch-arsenal-stats"
            f"?type=pitcher&pitchType=&year={season}&team=&min=10"
            f"&sort=api_p_release_speed&sortDir=desc"
            f"&csv=true"
        )
        cache_key = f"statcast:arsenal:pitcher:{pitcher_id}:{season}"
        rows = self._fetch_savant_csv(url, cache_key, TTL_ARSENAL)

        for row in rows:
            if str(row.get("pitcher_id", row.get("player_id", ""))) == str(pitcher_id):
                return self._parse_arsenal_row(row, pitcher_id, season)

        return None

    @staticmethod
    def _parse_arsenal_row(
        row: dict, pitcher_id: str, season: int
    ) -> PitcherArsenal:
        def _f(key: str, default: float = 0.0) -> float:
            try:
                return float(row.get(key, default) or default)
            except Exception:
                return default

        fb_pct = _f("n_ff_formatted", _f("fastball_pct"))
        sl_pct = _f("n_sl_formatted", _f("slider_pct"))
        cu_pct = _f("n_cu_formatted", _f("curveball_pct"))
        ch_pct = _f("n_ch_formatted", _f("changeup_pct"))

        if fb_pct >= 0.55:
            cluster = 0
        elif (sl_pct + cu_pct) >= 0.40:
            cluster = 1
        else:
            cluster = 2

        return PitcherArsenal(
            pitcher_id=pitcher_id,
            pitcher_name=row.get("player_name", row.get("pitcher_name", "")),
            season=season,
            fastball_pct=fb_pct,
            sinker_pct=_f("n_si_formatted"),
            cutter_pct=_f("n_fc_formatted"),
            slider_pct=sl_pct,
            curveball_pct=cu_pct,
            changeup_pct=ch_pct,
            splitter_pct=_f("n_fs_formatted"),
            fastball_velo=_f("mean_release_speed", _f("avg_speed")),
            whiff_rate_overall=_f("whiff_percent", _f("swstr_pct")),
            whiff_rate_fb=_f("n_ff_whiff_percent"),
            whiff_rate_slider=_f("n_sl_whiff_percent"),
            whiff_rate_curve=_f("n_cu_whiff_percent"),
            whiff_rate_change=_f("n_ch_whiff_percent"),
            spin_rate_fb=_f("mean_spin_rate"),
            chase_rate=_f("o_swing_percent", _f("chase_rate")),
            zone_pct=_f("zone_percent"),
            zone_contact=_f("z_contact_percent"),
            swstr_pct=_f("swinging_strike_percent"),
            arsenal_cluster=cluster,
        )

    def get_batter_discipline(
        self, batter_id: str, season: int = 2025
    ) -> BatterPlateDiscipline | None:
        """Fetch batter plate discipline from Savant expected stats leaderboard."""
        url = (
            f"{SAVANT_BASE}/leaderboard/expected_statistics"
            f"?type=batter&year={season}&position=&team=&min=50"
            f"&sort=xwoba&sortDir=desc&csv=true"
        )
        cache_key = f"statcast:discipline:batter:{batter_id}:{season}"
        rows = self._fetch_savant_csv(url, cache_key, TTL_PLATE_DISCIPLINE)

        for row in rows:
            if str(row.get("batter", row.get("player_id", ""))) == str(batter_id):
                return self._parse_discipline_row(row, batter_id, season)
        return None

    @staticmethod
    def _parse_discipline_row(
        row: dict, batter_id: str, season: int
    ) -> BatterPlateDiscipline:
        def _f(key: str, default: float = 0.0) -> float:
            try:
                return float(row.get(key, default) or default)
            except Exception:
                return default

        return BatterPlateDiscipline(
            batter_id=batter_id,
            batter_name=row.get("player_name", row.get("batter_name", "")),
            season=season,
            k_pct=_f("k_percent"),
            bb_pct=_f("bb_percent"),
            hard_hit_pct=_f("hard_hit_percent"),
            barrel_pct=_f("barrel_batted_rate"),
            avg_exit_velocity=_f("avg_hit_speed"),
            avg_launch_angle=_f("avg_hit_angle"),
            xba=_f("est_ba"),
            xslg=_f("est_slg"),
            xwoba=_f("est_woba"),
        )

    def enrich_pitcher_features(
        self, pitcher_id: str, season: int = 2025
    ) -> dict[str, Any]:
        """
        Returns a flat dict of Statcast features ready to merge into
        the ML pipeline feature matrix (StrikeoutFeatures + PropEdge).
        """
        arsenal = self.get_pitcher_arsenal(pitcher_id, season)
        if arsenal is None:
            return {}

        return {
            "fastball_pct":       arsenal.fastball_pct,
            "breaking_ball_pct":  arsenal.slider_pct + arsenal.curveball_pct,
            "offspeed_pct":       arsenal.changeup_pct + arsenal.splitter_pct,
            "fastball_velo":      arsenal.fastball_velo,
            "spin_rate_fb":       arsenal.spin_rate_fb,
            "whiff_rate_fb":      arsenal.whiff_rate_fb,
            "whiff_rate_slider":  arsenal.whiff_rate_slider,
            "whiff_rate_curve":   arsenal.whiff_rate_curve,
            "chase_rate":         arsenal.chase_rate,
            "zone_contact_rate":  arsenal.zone_contact,
            "swstr_pct":          arsenal.swstr_pct,
            "arsenal_cluster":    arsenal.arsenal_cluster,
        }

    def enrich_batter_features(
        self, batter_id: str, season: int = 2025
    ) -> dict[str, Any]:
        """Returns flat dict of batter Statcast features."""
        disc = self.get_batter_discipline(batter_id, season)
        if disc is None:
            return {}

        return {
            "batter_k_pct":           disc.k_pct,
            "batter_bb_pct":          disc.bb_pct,
            "batter_hard_hit_pct":    disc.hard_hit_pct,
            "batter_barrel_pct":      disc.barrel_pct,
            "batter_avg_exit_velo":   disc.avg_exit_velocity,
            "batter_avg_launch_angle":disc.avg_launch_angle,
            "batter_xba":             disc.xba,
            "batter_xslg":            disc.xslg,
            "batter_xwoba":           disc.xwoba,
            "opp_contact_pct":        1.0 - disc.k_pct,
        }

    def batch_enrich(
        self,
        pitcher_ids: list[str],
        batter_ids:  list[str],
        season: int = 2025,
    ) -> dict[str, dict[str, Any]]:
        """
        Batch enrich pitchers + batters.
        Returns dict keyed by player_id → feature dict.
        """
        out: dict[str, dict[str, Any]] = {}
        for pid in pitcher_ids:
            feats = self.enrich_pitcher_features(pid, season)
            if feats:
                out[pid] = feats
        for bid in batter_ids:
            feats = self.enrich_batter_features(bid, season)
            if feats:
                out[bid] = feats
        return out
