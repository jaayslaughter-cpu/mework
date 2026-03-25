"""
statcast_feature_layer.py — PropIQ Analytics: Statcast Feature Enhancement Layer
==================================================================================

Enriches PropIQ props with Statcast-derived metrics sourced directly from
Baseball Savant CSV endpoints — no pybaseball dependency required.

Data Sources (free, no API key)
--------------------------------
  Batter expected stats:
    https://baseballsavant.mlb.com/leaderboard/expected_statistics
    → xwOBA, xBA, xSLG, barrel%, HH%, EV, xISO, sweet-spot%
    → MLBAM player_id in every row (no name-matching needed)

  Pitcher Statcast leaderboard:
    https://baseballsavant.mlb.com/statcast_leaderboard (player_type=pitcher)
    → whiff%, EV allowed, barrel%, HH% allowed

Features Added per Batter
--------------------------
  sc_xwoba          - xwOBA (expected wOBA — strips luck from contact)
  sc_xba            - xBA (expected batting average)
  sc_xslg           - xSLG (expected slugging)
  sc_barrel_rate    - barrel% (hard contact at the right angle)
  sc_hard_hit_rate  - hard-hit% (exit velo ≥ 95 mph)
  sc_avg_launch_speed - avg exit velocity
  sc_xiso           - xISO (expected isolated power)

Features Added per Pitcher
---------------------------
  sc_whiff_rate     - whiff% (strongest K-rate predictor)
  sc_hard_hit_rate  - HH% allowed (contact quality allowed)
  sc_barrel_rate    - barrel% allowed
  sc_avg_launch_speed - avg exit velo allowed

Caching
-------
  Each CSV is fetched once per calendar day and cached to disk.
  Falls back gracefully to empty features on any network error.

Author: PropIQ Analytics Engine (Phase 37)
"""

from __future__ import annotations

import io
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger("PropIQ.StatcastLayer")

# ── Configuration ──────────────────────────────────────────────────────────────
CACHE_DIR = Path(os.getenv("PROPIQ_STATCAST_CACHE", "/tmp/propiq_statcast"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

_SAVANT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://baseballsavant.mlb.com/",
}

# Field names exposed to downstream agents (backward-compatible names preserved)
BATTER_STATCAST_COLS = [
    "sc_xwoba",
    "sc_xba",
    "sc_xslg",
    "sc_barrel_rate",
    "sc_hard_hit_rate",    # backward compat (was sc_hard_hit_rate in Phase 27)
    "sc_avg_launch_speed",
    "sc_xiso",
]

PITCHER_STATCAST_COLS = [
    "sc_whiff_rate",       # backward compat (was sc_whiff_rate in Phase 27)
    "sc_hard_hit_rate",
    "sc_barrel_rate",
    "sc_avg_launch_speed",
]


# ===========================================================================
# 1. SAVANT FETCHER — direct CSV download with daily disk cache
# ===========================================================================

class SavantFetcher:
    """
    Downloads Baseball Savant leaderboard CSVs for the current season.

    Two endpoints:
      - expected_statistics (batter/pitcher) → xwOBA, xBA, xSLG, barrel%, HH%
      - statcast_leaderboard (pitcher)        → whiff%, velocity metrics

    Each response is cached to /tmp/propiq_statcast as a parquet file
    so the same calendar day re-uses the disk copy.
    """

    def __init__(self, cache_dir: Path = CACHE_DIR):
        self.cache_dir = cache_dir

    # ------------------------------------------------------------------
    def _today_year(self) -> int:
        return datetime.now().year

    def _cache_path(self, name: str) -> Path:
        today = datetime.today().strftime("%Y-%m-%d")
        return self.cache_dir / f"{name}_{today}.parquet"

    def _csv_to_df(self, url: str, name: str):
        """Download CSV, cache as parquet, return DataFrame or None."""
        try:
            import pandas as pd
            import requests

            cache = self._cache_path(name)
            if cache.exists():
                logger.info("[Savant] Cache hit → %s", cache)
                return pd.read_parquet(cache)

            logger.info("[Savant] Fetching %s ...", url)
            time.sleep(0.5)  # polite rate limit
            resp = requests.get(url, headers=_SAVANT_HEADERS, timeout=20)
            if resp.status_code != 200:
                logger.warning("[Savant] HTTP %d for %s", resp.status_code, name)
                return None

            df = pd.read_csv(io.StringIO(resp.text))
            if df.empty:
                logger.warning("[Savant] Empty CSV for %s", name)
                return None

            df.to_parquet(cache, index=False)
            logger.info("[Savant] %s → %d rows cached", name, len(df))
            return df

        except ImportError:
            logger.warning("[Savant] pandas/requests not available")
            return None
        except Exception as exc:
            logger.warning("[Savant] %s fetch failed: %s", name, exc)
            return None

    # ------------------------------------------------------------------
    def fetch_batter_expected_stats(self) -> dict[int, dict[str, float]]:
        """
        Returns {mlbam_id: {sc_xwoba, sc_xba, sc_xslg, sc_barrel_rate,
                             sc_hard_hit_rate, sc_avg_launch_speed, sc_xiso}}
        """
        year = self._today_year()
        url = (
            f"https://baseballsavant.mlb.com/leaderboard/expected_statistics"
            f"?type=batter&year={year}&position=&team=&min=q&csv=true"
        )
        df = self._csv_to_df(url, f"savant_batter_expected_{year}")
        if df is None or df.empty:
            return {}

        result: dict[int, dict[str, float]] = {}
        for _, row in df.iterrows():
            pid = row.get("player_id") or row.get("batter_id")
            if not pid or str(pid).strip() == "":
                continue
            try:
                pid = int(float(pid))
            except (ValueError, TypeError):
                continue

            result[pid] = {
                "sc_xwoba":           _safe_float(row, "xwoba"),
                "sc_xba":             _safe_float(row, "xba"),
                "sc_xslg":            _safe_float(row, "xslg"),
                "sc_barrel_rate":     _safe_float(row, "barrel_batted_rate") / 100.0
                                      if _safe_float(row, "barrel_batted_rate") > 1.0
                                      else _safe_float(row, "barrel_batted_rate"),
                "sc_hard_hit_rate":   _safe_float(row, "hard_hit_percent") / 100.0
                                      if _safe_float(row, "hard_hit_percent") > 1.0
                                      else _safe_float(row, "hard_hit_percent"),
                "sc_avg_launch_speed": _safe_float(row, "exit_velocity_avg"),
                "sc_xiso":            _safe_float(row, "xiso"),
            }

        logger.info("[Savant] Batter expected stats: %d players", len(result))
        return result

    def fetch_pitcher_stats(self) -> dict[int, dict[str, float]]:
        """
        Returns {mlbam_id: {sc_whiff_rate, sc_hard_hit_rate,
                             sc_barrel_rate, sc_avg_launch_speed}}

        Uses statcast_leaderboard for whiff% and expected_statistics for
        contact quality metrics.
        """
        year = self._today_year()

        # Primary: expected stats (contact quality allowed)
        url_exp = (
            f"https://baseballsavant.mlb.com/leaderboard/expected_statistics"
            f"?type=pitcher&year={year}&position=&team=&min=q&csv=true"
        )
        df_exp = self._csv_to_df(url_exp, f"savant_pitcher_expected_{year}")

        # Secondary: statcast leaderboard (whiff%)
        url_ldr = (
            f"https://baseballsavant.mlb.com/statcast_leaderboard"
            f"?year={year}&abs=&player_type=pitcher&min_pitches=q&csv=true"
        )
        df_ldr = self._csv_to_df(url_ldr, f"savant_pitcher_leaderboard_{year}")

        result: dict[int, dict[str, float]] = {}

        if df_exp is not None and not df_exp.empty:
            for _, row in df_exp.iterrows():
                pid = row.get("player_id") or row.get("pitcher_id")
                if not pid:
                    continue
                try:
                    pid = int(float(pid))
                except (ValueError, TypeError):
                    continue
                result[pid] = {
                    "sc_whiff_rate":       0.0,   # filled below from leaderboard
                    "sc_hard_hit_rate":    _safe_float(row, "hard_hit_percent") / 100.0
                                           if _safe_float(row, "hard_hit_percent") > 1.0
                                           else _safe_float(row, "hard_hit_percent"),
                    "sc_barrel_rate":      _safe_float(row, "barrel_batted_rate") / 100.0
                                           if _safe_float(row, "barrel_batted_rate") > 1.0
                                           else _safe_float(row, "barrel_batted_rate"),
                    "sc_avg_launch_speed": _safe_float(row, "exit_velocity_avg"),
                }

        # Overlay whiff% from leaderboard
        if df_ldr is not None and not df_ldr.empty:
            for _, row in df_ldr.iterrows():
                pid = row.get("player_id") or row.get("pitcher_id")
                if not pid:
                    continue
                try:
                    pid = int(float(pid))
                except (ValueError, TypeError):
                    continue
                whiff_raw = _safe_float(row, "whiff_percent")
                whiff = whiff_raw / 100.0 if whiff_raw > 1.0 else whiff_raw
                if pid in result:
                    result[pid]["sc_whiff_rate"] = whiff
                else:
                    result[pid] = {
                        "sc_whiff_rate": whiff,
                        "sc_hard_hit_rate": 0.0,
                        "sc_barrel_rate": 0.0,
                        "sc_avg_launch_speed": 0.0,
                    }

        logger.info("[Savant] Pitcher Statcast stats: %d players", len(result))
        return result


def _safe_float(row, key: str, default: float = 0.0) -> float:
    """Safely extract a float from a pandas row dict."""
    val = row.get(key)
    if val is None or str(val).strip() in ("", "null", "nan", "None"):
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


# ===========================================================================
# 2. STATCAST FEATURE LAYER — main interface (backward-compatible)
# ===========================================================================

class StatcastFeatureLayer:
    """
    Provides Statcast feature snapshots for the 11 AM dispatcher.

    Replaces the pybaseball-based implementation with direct Baseball Savant
    CSV downloads. Same external interface as Phase 24-27 implementation.
    """

    def __init__(self, fetcher: Optional[SavantFetcher] = None):
        self._fetcher = fetcher or SavantFetcher()
        self._batter_cache: Optional[dict[int, dict[str, float]]] = None
        self._pitcher_cache: Optional[dict[int, dict[str, float]]] = None

    def get_inference_snapshot(self, player_type: str) -> dict[int, dict[str, float]]:
        """
        Return current-season Statcast feature snapshot per player.

        Returns {mlbam_id: {feature_col: value, ...}}
        Empty dict if data unavailable (graceful fallback).
        """
        if player_type == "pitcher":
            if self._pitcher_cache is None:
                self._pitcher_cache = self._fetcher.fetch_pitcher_stats()
            return self._pitcher_cache
        else:
            if self._batter_cache is None:
                self._batter_cache = self._fetcher.fetch_batter_expected_stats()
            return self._batter_cache

    def clear_cache(self) -> None:
        self._batter_cache = None
        self._pitcher_cache = None


# ===========================================================================
# 3. CONVENIENCE FUNCTION — daily dispatcher hook (backward-compatible)
# ===========================================================================

def enrich_props_with_statcast(
    props: list,
    player_type: str,
    layer: Optional[StatcastFeatureLayer] = None,
) -> list:
    """
    Enrich a list of prop dicts with Statcast features.

    Each prop dict must have 'mlbam_id' (int).
    Adds sc_xwoba, sc_xba, sc_xslg, sc_barrel_rate, sc_hard_hit_rate,
    sc_avg_launch_speed (batters) or sc_whiff_rate, sc_hard_hit_rate (pitchers).

    Props without a matching mlbam_id are returned unmodified.
    Falls back gracefully if Baseball Savant is unreachable.
    """
    if not props:
        return props

    if layer is None:
        layer = StatcastFeatureLayer()

    snapshot = layer.get_inference_snapshot(player_type)
    if not snapshot:
        logger.info("[Savant] No snapshot — props unchanged")
        return props

    enriched = []
    matched = 0
    for prop in props:
        pid = int(prop.get("mlbam_id") or prop.get("player_id") or 0)
        if pid and pid in snapshot:
            enriched.append({**prop, **snapshot[pid]})
            matched += 1
        else:
            enriched.append(prop)

    logger.info(
        "[Savant] %s: %d/%d props enriched with Statcast features",
        player_type, matched, len(props),
    )
    return enriched


# ===========================================================================
# 4. STANDALONE TEST
# ===========================================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    )
    print("\n" + "=" * 65)
    print("  PropIQ StatcastFeatureLayer — CSV smoke test")
    print("=" * 65)

    layer = StatcastFeatureLayer()

    print("\n[1] Fetching batter expected stats from Baseball Savant...")
    batter_snap = layer.get_inference_snapshot("batter")
    print(f"    Batters with Statcast features : {len(batter_snap):,}")

    if batter_snap:
        sample_id = next(iter(batter_snap))
        print(f"\n[2] Sample batter ({sample_id}) features:")
        for k, v in batter_snap[sample_id].items():
            print(f"    {k:<28} : {v:.4f}")

    print("\n[3] Fetching pitcher stats from Baseball Savant...")
    pitcher_snap = layer.get_inference_snapshot("pitcher")
    print(f"    Pitchers with Statcast features: {len(pitcher_snap):,}")

    if pitcher_snap:
        sample_id = next(iter(pitcher_snap))
        print(f"\n[4] Sample pitcher ({sample_id}) features:")
        for k, v in pitcher_snap[sample_id].items():
            print(f"    {k:<28} : {v:.4f}")

    print("\n✅ StatcastFeatureLayer CSV test complete.")
    print("=" * 65 + "\n")
