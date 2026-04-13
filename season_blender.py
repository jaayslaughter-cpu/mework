"""
season_blender.py
=================
PropIQ — Progressive season blending for early-season statistical reliability.

PROBLEM
-------
At game 13 of 162, 2026 season stats are too small a sample to trust alone:
  - ERA after 13 starts: high variance (~±2 ERA)
  - wOBA after 56 PA:    noisy (~±0.050)
  - K% after 56 PA:      already decent (~±0.04)

SOLUTION
--------
Stat-specific weighting based on FanGraphs stability research.
Each stat has a PA/BF threshold at which it stabilizes (becomes reliable).
Before that threshold, blend with 2025 data:

    blend(stat) = w * value_2026 + (1 - w) * value_2025
    where w = min(1.0, sample_size / stability_threshold)

This transitions smoothly from 100% 2025 at game 1 → 100% 2026 at season midpoint.

STABILITY THRESHOLDS (from FanGraphs research)
-----------------------------------------------
Pitchers (BF):       K% ~150   BB% ~250   xFIP/ERA ~750
Batters  (PA):       K% ~60    BB% ~120   wOBA ~300   ISO ~400

USAGE
-----
    from season_blender import SeasonBlender
    blender = SeasonBlender()
    
    # Blend a pitcher's 2026 and 2025 stat dicts
    blended = blender.blend_pitcher(stats_2026, stats_2025)
    
    # Blend a batter's stats
    blended = blender.blend_batter(stats_2026, stats_2025)
    
    # Get current weights for inspection/logging
    weights = blender.pitcher_weights()
"""

from __future__ import annotations

import datetime
import logging
from zoneinfo import ZoneInfo

logger = logging.getLogger("propiq.season_blender")

# Opening Day 2026
_OPENING_DAY = datetime.date(2026, 3, 27)
_SEASON_GAMES = 162
_SEASON_DAYS  = 183   # ~6 month season

# PA/BF stability thresholds from FanGraphs research
# Lower = stabilizes faster = trust 2026 sooner
_PITCHER_THRESHOLDS = {
    "k_rate":    150,   # K%:  fast stabilizer
    "bb_rate":   250,   # BB%: medium
    "k_bb_pct":  200,   # K-BB: medium
    "csw_pct":   200,   # CSW: medium (contact quality signal)
    "swstr_pct": 200,   # SwStr: medium
    "xfip":      300,   # xFIP: medium-slow (defense-independent)
    "siera":     350,   # SIERA: slow
    "fip":       400,   # FIP: slow
    "era":       750,   # ERA: very slow (BABIP/sequencing noise)
    "whip":      500,   # WHIP: slow
    "hr_fb_pct": 500,   # HR/FB: very noisy
    "lob_pct":   800,   # LOB%: pure luck, don't trust 2026 until very late
    "babip":     800,   # BABIP: pure luck
}

_BATTER_THRESHOLDS = {
    "k_pct":      60,   # K%: fastest stabilizer in baseball
    "bb_pct":    120,   # BB%: fast
    "k_bb_pct":  100,   # K-BB: fast
    "o_swing":   150,   # O-Swing: medium
    "z_contact": 150,   # Z-Contact: medium
    "csw_pct":   200,   # CSW: medium
    "woba":      300,   # wOBA: slow
    "slg":       350,   # SLG: slow
    "iso":       400,   # ISO: very slow
    "hr_fb_pct": 500,   # HR/FB: very slow
    "babip":     800,   # BABIP: luck
}


class SeasonBlender:
    """
    Computes stat-by-stat blend weights based on days into the 2026 season.
    Weights update daily — call once per DataHub cycle.
    """

    def __init__(
        self,
        opening_day: datetime.date | None = None,
        pa_per_game:  float = 4.30,   # FG 2025: R/G (was 4.38)
        bf_per_start: float = 23.0,   # ~23 BF per start for average starter
        starts_per_game: float = 0.2, # 1 start per 5 days
    ):
        self._opening   = opening_day or _OPENING_DAY
        self._pa_rate   = pa_per_game
        self._bf_start  = bf_per_start
        self._sp_rate   = starts_per_game

    def _days_played(self) -> int:
        # Use Pacific Time — Railway runs UTC; during PDT (Apr-Oct) UTC flips
        # to the next calendar day at 5 PM PT, corrupting blend weights.
        return max(0, (datetime.datetime.now(ZoneInfo("America/Los_Angeles")).date() - self._opening).days)

    def _games_played(self) -> float:
        """Estimated team games played."""
        d = self._days_played()
        return max(1.0, d * _SEASON_GAMES / _SEASON_DAYS)

    def _batter_pa(self) -> float:
        return self._games_played() * self._pa_rate

    def _starter_bf(self) -> float:
        return self._games_played() * self._sp_rate * self._bf_start

    def _weight(self, sample: float, threshold: int) -> float:
        """
        Blend weight for 2026 data (0 = all 2025, 1 = all 2026).
        Linear from 0 → 1 as sample grows to threshold.
        """
        return min(1.0, sample / max(1, threshold))

    def pitcher_weights(self) -> dict[str, float]:
        """Return current 2026 blend weights for all pitcher stats."""
        bf = self._starter_bf()
        return {
            stat: round(self._weight(bf, thresh), 3)
            for stat, thresh in _PITCHER_THRESHOLDS.items()
        }

    def batter_weights(self) -> dict[str, float]:
        """Return current 2026 blend weights for all batter stats."""
        pa = self._batter_pa()
        return {
            stat: round(self._weight(pa, thresh), 3)
            for stat, thresh in _BATTER_THRESHOLDS.items()
        }

    def blend_pitcher(
        self,
        stats_2026: dict[str, float],
        stats_2025: dict[str, float],
    ) -> dict[str, float]:
        """
        Blend 2026 and 2025 pitcher stats using stability-weighted formula.
        Any stat missing from 2026 uses 2025 value directly.
        Stats not in threshold table use 50/50 blend.
        """
        bf = self._starter_bf()
        blended: dict[str, float] = {}
        all_keys = set(stats_2026) | set(stats_2025)

        for key in all_keys:
            v_2026 = stats_2026.get(key)
            v_2025 = stats_2025.get(key)
            if v_2026 is None and v_2025 is None:
                continue
            if v_2026 is None:
                blended[key] = v_2025
                continue
            if v_2025 is None:
                blended[key] = v_2026
                continue
            thresh = _PITCHER_THRESHOLDS.get(key, 300)
            w = self._weight(bf, thresh)
            blended[key] = round(w * v_2026 + (1 - w) * v_2025, 6)

        return blended

    def blend_batter(
        self,
        stats_2026: dict[str, float],
        stats_2025: dict[str, float],
    ) -> dict[str, float]:
        """
        Blend 2026 and 2025 batter stats using stability-weighted formula.
        """
        pa = self._batter_pa()
        blended: dict[str, float] = {}
        all_keys = set(stats_2026) | set(stats_2025)

        for key in all_keys:
            v_2026 = stats_2026.get(key)
            v_2025 = stats_2025.get(key)
            if v_2026 is None and v_2025 is None:
                continue
            if v_2026 is None:
                blended[key] = v_2025
                continue
            if v_2025 is None:
                blended[key] = v_2026
                continue
            thresh = _BATTER_THRESHOLDS.get(key, 200)
            w = self._weight(pa, thresh)
            blended[key] = round(w * v_2026 + (1 - w) * v_2025, 6)

        return blended

    def log_weights(self) -> None:
        """Log current blend weights for monitoring."""
        games = self._games_played()
        pa    = self._batter_pa()
        bf    = self._starter_bf()
        pw    = self.pitcher_weights()
        bw    = self.batter_weights()
        logger.info(
            "[Blend] Game %d | Batter PA≈%.0f | Starter BF≈%.0f",
            int(games), pa, bf
        )
        logger.info(
            "[Blend] Pitcher: K%%=%.0f%% xFIP=%.0f%% ERA=%.0f%% "
            "| Batter: K%%=%.0f%% wOBA=%.0f%% ISO=%.0f%%",
            pw.get("k_rate", 0) * 100,
            pw.get("xfip",   0) * 100,
            pw.get("era",    0) * 100,
            bw.get("k_pct",  0) * 100,
            bw.get("woba",   0) * 100,
            bw.get("iso",    0) * 100,
        )


# Module-level singleton
_blender: SeasonBlender | None = None

def get_blender() -> SeasonBlender:
    global _blender
    if _blender is None:
        _blender = SeasonBlender()
    return _blender
