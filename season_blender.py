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
Research-backed blend weighting based on how much to trust 2026 in early April.
Each stat has a PA/BF threshold calibrated so that at ~56 PA (13 games) the
2026 weight matches research targets (e.g. K% ~30%, BABIP ≤15%).

    blend(stat) = w * value_2026 + (1 - w) * value_2025
    where w = min(1.0, sample_size / blend_threshold)

This transitions smoothly from 100% 2025 at game 1 → 100% 2026 at season midpoint.

STABILITY THRESHOLDS (research-calibrated for blend weights, not raw reliability)
----------------------------------------------------------------------------------
Batters (PA):   K%/BB% ~190   xwOBA/Barrel% ~160   wOBA ~300   BABIP ~380   ISO ~400
Pitchers (BF):  K% ~230       xFIP ~315             ERA ~600    LOB%/BABIP ~800

Early-April (13 games) blend weight targets:
  Batter K%=29%  xwOBA=35%  wOBA=19%  BABIP=15%  ISO=14%
  Pitcher K%=30%  xFIP=22%  ERA=12%   LOB%=9%

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

logger = logging.getLogger("propiq.season_blender")

# Opening Day 2026
_OPENING_DAY = datetime.date(2026, 3, 27)
_SEASON_GAMES = 162
_SEASON_DAYS  = 183   # ~6 month season

# Blend thresholds (PA/BF) calibrated to research-backed early-season weights.
# These are NOT FanGraphs reliability thresholds — they're tuned so that
# at ~56 PA / ~69 BF (13 games, April) the 2026 weight matches research targets.
_PITCHER_THRESHOLDS = {
    # Rate stats (faster stabilizers for pitchers)
    "k_rate":    230,   # K%: ~30% 2026 in April
    "bb_rate":   260,   # BB%: slightly slower than K%
    "k_bb_pct":  240,   # K-BB
    "csw_pct":   260,   # CSW: medium
    "swstr_pct": 260,   # SwStr: medium
    # Defense-independent ERA estimators (medium-slow)
    "xfip":      315,   # xFIP: ~22% 2026 in April
    "siera":     360,   # SIERA: slower
    "fip":       380,   # FIP: slower (contains HR luck)
    # Outcome-heavy (slow)
    "era":       600,   # ERA: ~12% 2026 in April (~85% 2025 anchor)
    "whip":      500,   # WHIP: slow
    "hr_fb_pct": 550,   # HR/FB: luck-driven
    # Pure luck stats — essentially never trust April 2026
    "lob_pct":   800,   # LOB%: pure sequencing luck
    "babip":     800,   # BABIP: pure luck
}

_BATTER_THRESHOLDS = {
    # Statcast-style (fast stabilizers — lean into 2026 a bit sooner)
    "xwoba":      160,   # xwOBA: Statcast, trust 2026 at ~35% in April
    "xba":        160,   # xBA:   Statcast, same
    "barrel_pct": 175,   # Barrel%: reliable within 160-200 PA
    "hard_hit_pct": 175, # Hard-hit%: same
    "exit_velo":  175,   # Exit velo: same
    # Rate stats (medium-fast stabilizers)
    "k_pct":      190,   # K%: fast, but keep 2026 ≤30% in April
    "bb_pct":     190,   # BB%: similar to K%
    "k_bb_pct":   190,   # K-BB combined
    "o_swing":    220,   # O-Swing: medium
    "z_contact":  220,   # Z-Contact: medium
    "csw_pct":    250,   # CSW: medium
    # Outcome stats (slow stabilizers)
    "woba":       300,   # wOBA: slow (300 PA to stabilize)
    "slg":        280,   # SLG: slower than rate stats
    "iso":        400,   # ISO/power: very slow
    "hr_fb_pct":  500,   # HR/FB: very slow — keep 2025-heavy
    # Luck-driven (essentially don't trust 2026 in April at all)
    "babip":      380,   # BABIP: luck-driven, stay ≥85% 2025 in April
    # Platoon splits — require much bigger per-split samples
    "woba_vs_hand":   350,  # vs-LHP or vs-RHP wOBA
    "k_pct_vs_hand":  300,  # vs-hand K%
    "bb_pct_vs_hand": 350,  # vs-hand BB%
    "iso_vs_hand":    450,  # vs-hand ISO
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
        from zoneinfo import ZoneInfo
        today_pt = datetime.datetime.now(ZoneInfo("America/Los_Angeles")).date()
        return max(0, (today_pt - self._opening).days)

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
            "| Batter: K%%=%.0f%% xwOBA=%.0f%% wOBA=%.0f%% ISO=%.0f%%",
            pw.get("k_rate", 0) * 100,
            pw.get("xfip",   0) * 100,
            pw.get("era",    0) * 100,
            bw.get("k_pct",  0) * 100,
            bw.get("xwoba",  0) * 100,
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
