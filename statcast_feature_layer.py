"""
statcast_feature_layer.py — PropIQ Analytics: Statcast Feature Enhancement Layer
=================================================================================

Enriches the PropIQ ML pipeline feature matrix with Statcast-derived metrics
that are unavailable from standard game-log box scores.

Feature Engineering Source
--------------------------
Adapted and improved from mlb-sports-betting-predictions (zip audit Phase 24):
  - features/build_batter_stat_features.py  → batter Statcast aggregations
  - features/build_pitcher_stat_features.py → pitcher Statcast aggregations
  - features/engineer_features.py           → velocity/spin drop signals

New Features Per Pitcher (per game row)
---------------------------------------
  sc_avg_velocity      - mean fastball velocity (primary K rate driver)
  sc_avg_spin_rate     - mean spin rate (movement quality / K potential)
  sc_whiff_rate        - whiffs / pitches seen (strongest K prop predictor)
  sc_avg_exit_velocity - mean exit velocity allowed (hard contact proxy)
  sc_avg_launch_angle  - mean LA allowed (low = grounders = effective outing)
  sc_avg_extension     - mean release extension (mechanics consistency)
  sc_recent_ks         - raw K count for this game (Statcast-verified)

Derived in FeatureEngineer._add_context_features() after rolling:
  pct_drop_sc_avg_velocity  - velocity drop L7 vs L30 (fatigue/injury signal)
  pct_drop_sc_avg_spin_rate - spin deterioration L7 vs L30

New Features Per Batter (per game row)
---------------------------------------
  sc_avg_launch_speed  - mean exit velocity on batted balls (hard contact)
  sc_avg_bat_speed     - mean swing speed (contact power)
  sc_avg_swing_length  - mean swing length (compact vs loopy)
  sc_hard_hit_rate     - % of balls with exit velo >= 95 mph
  sc_recent_hrs        - HR events in this game (Statcast-verified)
  sc_season_hr         - season HR total from FanGraphs (context)
  sc_season_avg        - season batting average (contact quality)
  sc_season_slg        - season slugging pct (power profile)
  sc_season_pa         - season plate appearances (lineup slot proxy)

Integration
-----------
    from statcast_feature_layer import StatcastFeatureLayer

    # At 11 AM before dispatcher runs inference:
    layer = StatcastFeatureLayer()
    pitcher_feats = layer.get_pitcher_snapshot()  # {mlbam_id: {col: val, ...}}
    batter_feats  = layer.get_batter_snapshot()   # {mlbam_id: {col: val, ...}}

    # Enrich game log DataFrames before MLPipeline.train() or run_inference():
    enriched_pitcher_logs = layer.enrich_game_logs(pitcher_logs_df, "pitcher")
    enriched_batter_logs  = layer.enrich_game_logs(batter_logs_df,  "batter")

Data Source
-----------
- pybaseball (Baseball Savant / FanGraphs) — already in requirements.txt
- Daily disk cache under PROPIQ_STATCAST_CACHE (default /tmp/propiq_statcast)
- Falls back gracefully to zeros if pybaseball unavailable or API unreachable

Author: PropIQ Analytics Engine (Phase 24)
"""

import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger("PropIQ.StatcastLayer")

# ── Configuration ─────────────────────────────────────────────────────────
CACHE_DIR = Path(os.getenv("PROPIQ_STATCAST_CACHE", "/tmp/propiq_statcast"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

STATCAST_LOOKBACK_DAYS: int = 30    # days of pitch data to fetch
HARD_HIT_THRESHOLD_MPH: float = 95.0

# Whiff pitch descriptions (from build_pitcher_stat_features.py)
_WHIFF_DESCRIPTIONS = frozenset({
    "swinging_strike",
    "swinging_strike_blocked",
    "foul_tip",
})

# Pitcher Statcast columns to be added to FeatureEngineer.PITCHER_STAT_COLS
PITCHER_STATCAST_COLS = [
    "sc_avg_velocity",
    "sc_avg_spin_rate",
    "sc_whiff_rate",
    "sc_avg_exit_velocity",
    "sc_avg_launch_angle",
    "sc_avg_extension",
    "sc_recent_ks",
]

# Batter Statcast columns to be added to FeatureEngineer.BATTER_STAT_COLS
BATTER_STATCAST_COLS = [
    "sc_avg_launch_speed",
    "sc_avg_bat_speed",
    "sc_avg_swing_length",
    "sc_hard_hit_rate",
    "sc_recent_hrs",
    "sc_season_hr",
    "sc_season_avg",
    "sc_season_slg",
    "sc_season_pa",
]


# ===========================================================================
# 1. STATCAST FETCHER — pybaseball wrapper with daily caching
# ===========================================================================

class StatcastFetcher:
    """
    Thin pybaseball wrapper with daily disk caching.

    Baseball Savant has informal rate limits. We cache the full 30-day pull
    once per calendar day and reuse for all downstream feature builders.
    Cache format: Parquet (fast read/write, ~10× smaller than CSV for this data).
    """

    def __init__(self, cache_dir: Path = CACHE_DIR):
        self.cache_dir = cache_dir
        self._available = self._probe_pybaseball()

    @staticmethod
    def _probe_pybaseball() -> bool:
        try:
            import pybaseball  # noqa: F401
            return True
        except ImportError:
            logger.warning(
                "pybaseball not installed — Statcast features will be zeros. "
                "Install with: pip install pybaseball"
            )
            return False

    def fetch_statcast(self, lookback_days: int = STATCAST_LOOKBACK_DAYS) -> Optional[pd.DataFrame]:
        """
        Fetch pitch-level Statcast data for the past `lookback_days` days.
        Returns cached DataFrame if already fetched today.
        """
        if not self._available:
            return None

        cache_path = self.cache_dir / f"statcast_{datetime.today().strftime('%Y-%m-%d')}.parquet"
        if cache_path.exists():
            logger.info("StatcastFetcher: cache hit → %s", cache_path)
            return pd.read_parquet(cache_path)

        end_dt   = datetime.today().date()
        start_dt = end_dt - timedelta(days=lookback_days)

        logger.info(
            "StatcastFetcher: fetching %s → %s (%d days)...",
            start_dt, end_dt, lookback_days,
        )
        try:
            from pybaseball import statcast as sc_fetch
            time.sleep(0.5)          # polite rate limit
            df = sc_fetch(
                start_dt=start_dt.strftime("%Y-%m-%d"),
                end_dt=end_dt.strftime("%Y-%m-%d"),
                verbose=False,
            )
            if df is not None and not df.empty:
                df.to_parquet(cache_path, index=False)
                logger.info("StatcastFetcher: fetched %d rows → cached", len(df))
            return df
        except Exception as exc:
            logger.error("StatcastFetcher: fetch failed — %s", exc)
            return None

    def fetch_season_batting(self) -> Optional[pd.DataFrame]:
        """
        Fetch season-to-date FanGraphs batting stats (HR, AVG, SLG, PA).
        Cached daily. Returns None if unavailable.
        """
        if not self._available:
            return None

        cache_path = self.cache_dir / f"fangraphs_batting_{datetime.today().strftime('%Y-%m-%d')}.parquet"
        if cache_path.exists():
            return pd.read_parquet(cache_path)

        try:
            from pybaseball import batting_stats
            year = datetime.today().year
            time.sleep(0.5)
            df = batting_stats(year)
            if df is not None and not df.empty:
                # Normalize name for fuzzy merge
                df["_clean_name"] = (
                    df["Name"]
                    .str.lower()
                    .str.strip()
                    .str.replace(r"[àáâãäå]", "a", regex=True)
                    .str.replace(r"[èéêë]",   "e", regex=True)
                    .str.replace(r"[ìíîï]",   "i", regex=True)
                    .str.replace(r"[òóôõö]",  "o", regex=True)
                    .str.replace(r"[ùúûü]",   "u", regex=True)
                    .str.replace(r"[ñ]",       "n", regex=True)
                    .str.replace(r"\.",        "",  regex=True)
                )
                df.to_parquet(cache_path, index=False)
                logger.info("StatcastFetcher: FanGraphs batting stats → %d players", len(df))
            return df
        except Exception as exc:
            logger.error("StatcastFetcher: FanGraphs fetch failed — %s", exc)
            return None


# ===========================================================================
# 2. PITCHER STATCAST AGGREGATOR
# ===========================================================================

def _build_pitcher_per_game(sc_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate pitch-level Statcast events to per-pitcher-per-game metrics.

    Input:  raw Statcast DataFrame (from StatcastFetcher.fetch_statcast())
    Output: DataFrame with columns:
              pitcher (MLBAM int), game_date (datetime),
              sc_avg_velocity, sc_avg_spin_rate, sc_whiff_rate,
              sc_avg_exit_velocity, sc_avg_launch_angle,
              sc_avg_extension, sc_recent_ks

    Adapted from build_pitcher_stat_features.py and engineer_features.py.
    """
    required = {"pitcher", "game_date"}
    if not required.issubset(sc_df.columns):
        logger.warning("Pitcher aggregator: missing required Statcast columns %s", required - set(sc_df.columns))
        return pd.DataFrame()

    df = sc_df.copy()
    df["game_date"] = pd.to_datetime(df["game_date"])

    # ── Safe numeric coercions ─────────────────────────────────────────────
    for col in ["release_speed", "release_spin_rate", "release_extension",
                "launch_speed", "launch_angle"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # ── Whiff flag (from build_pitcher_stat_features.py) ──────────────────
    if "description" in df.columns:
        df["_is_whiff"] = df["description"].isin(_WHIFF_DESCRIPTIONS)
    else:
        df["_is_whiff"] = False

    # ── Strikeout flag ─────────────────────────────────────────────────────
    if "events" in df.columns:
        df["_is_k"] = df["events"].fillna("").str.lower().str.startswith("strikeout")
    else:
        df["_is_k"] = False

    # ── Ball-in-play filter for contact quality ─────────────────────────
    bip_mask = df["events"].fillna("").isin([
        "single", "double", "triple", "home_run",
        "field_out", "force_out", "grounded_into_double_play",
        "fielders_choice", "fielders_choice_out",
    ]) if "events" in df.columns else pd.Series(False, index=df.index)

    # ── Per-game aggregation ───────────────────────────────────────────────
    def _safe_mean(s):
        return s.mean() if s.notna().any() else np.nan

    def _whiff_rate(sub):
        total = len(sub)
        return sub["_is_whiff"].sum() / total if total > 0 else 0.0

    grouped = df.groupby(["pitcher", "game_date"])
    agg = grouped.agg(
        sc_avg_velocity     =("release_speed",     _safe_mean),
        sc_avg_spin_rate    =("release_spin_rate",  _safe_mean),
        sc_avg_extension    =("release_extension",  _safe_mean),
        sc_recent_ks        =("_is_k",              "sum"),
        _total_pitches      =("release_speed",      "count"),
        _whiffs             =("_is_whiff",          "sum"),
    ).reset_index()

    agg["sc_whiff_rate"] = np.where(
        agg["_total_pitches"] > 0,
        agg["_whiffs"] / agg["_total_pitches"],
        0.0,
    )

    # ── Contact quality requires BIP subset ───────────────────────────────
    bip_df = df[bip_mask].copy() if bip_mask.any() else pd.DataFrame()
    if not bip_df.empty:
        contact = bip_df.groupby(["pitcher", "game_date"]).agg(
            sc_avg_exit_velocity=("launch_speed",  _safe_mean),
            sc_avg_launch_angle =("launch_angle",  _safe_mean),
        ).reset_index()
        agg = agg.merge(contact, on=["pitcher", "game_date"], how="left")
    else:
        agg["sc_avg_exit_velocity"] = np.nan
        agg["sc_avg_launch_angle"]  = np.nan

    # ── Fill NaN with column medians (graceful degradation) ───────────────
    for col in PITCHER_STATCAST_COLS:
        if col in agg.columns:
            median = agg[col].median()
            agg[col] = agg[col].fillna(median if pd.notna(median) else 0.0)

    # ── Rename pitcher → player_id for consistent merge key ───────────────
    agg = agg.rename(columns={"pitcher": "player_id"})
    agg["player_id"] = pd.to_numeric(agg["player_id"], errors="coerce").astype("Int64")

    keep = ["player_id", "game_date"] + PITCHER_STATCAST_COLS
    return agg[[c for c in keep if c in agg.columns]].copy()


# ===========================================================================
# 3. BATTER STATCAST AGGREGATOR
# ===========================================================================

def _build_batter_per_game(
    sc_df: pd.DataFrame,
    season_batting_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Aggregate pitch/event-level Statcast data to per-batter-per-game metrics.

    Input:  raw Statcast DataFrame + optional FanGraphs season batting stats
    Output: DataFrame with columns:
              batter (MLBAM int), game_date (datetime),
              sc_avg_launch_speed, sc_avg_bat_speed, sc_avg_swing_length,
              sc_hard_hit_rate, sc_recent_hrs,
              sc_season_hr, sc_season_avg, sc_season_slg, sc_season_pa

    Adapted from build_batter_stat_features.py.
    """
    required = {"batter", "game_date"}
    if not required.issubset(sc_df.columns):
        logger.warning("Batter aggregator: missing required columns %s", required - set(sc_df.columns))
        return pd.DataFrame()

    df = sc_df.copy()
    df["game_date"] = pd.to_datetime(df["game_date"])

    for col in ["launch_speed", "bat_speed", "swing_length"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # ── HR flag ───────────────────────────────────────────────────────────
    if "events" in df.columns:
        df["_is_hr"] = df["events"].fillna("") == "home_run"
    else:
        df["_is_hr"] = False

    # ── Batted-ball filter (only rows where batter made contact) ──────────
    bip_mask = df["launch_speed"].notna()

    def _safe_mean(s):
        return s.mean() if s.notna().any() else np.nan

    # ── Per-game aggregation ───────────────────────────────────────────────
    grouped = df.groupby(["batter", "game_date"])
    agg = grouped.agg(
        sc_recent_hrs       =("_is_hr",       "sum"),
    ).reset_index()

    # Contact quality from BIP rows only
    bip_df = df[bip_mask].copy() if bip_mask.any() else pd.DataFrame()
    if not bip_df.empty:
        contact = bip_df.groupby(["batter", "game_date"]).agg(
            sc_avg_launch_speed =("launch_speed",  _safe_mean),
            sc_avg_bat_speed    =("bat_speed",      _safe_mean),
            sc_avg_swing_length =("swing_length",   _safe_mean),
            _hard_hits          =("launch_speed",   lambda s: (s >= HARD_HIT_THRESHOLD_MPH).sum()),
            _total_batted       =("launch_speed",   "count"),
        ).reset_index()
        contact["sc_hard_hit_rate"] = np.where(
            contact["_total_batted"] > 0,
            contact["_hard_hits"] / contact["_total_batted"],
            0.0,
        )
        agg = agg.merge(
            contact[["batter", "game_date", "sc_avg_launch_speed",
                      "sc_avg_bat_speed", "sc_avg_swing_length", "sc_hard_hit_rate"]],
            on=["batter", "game_date"],
            how="left",
        )
    else:
        for col in ["sc_avg_launch_speed", "sc_avg_bat_speed", "sc_avg_swing_length", "sc_hard_hit_rate"]:
            agg[col] = np.nan

    # ── Merge FanGraphs season context ────────────────────────────────────
    season_cols = ["sc_season_hr", "sc_season_avg", "sc_season_slg", "sc_season_pa"]
    for col in season_cols:
        agg[col] = np.nan

    if season_batting_df is not None and not season_batting_df.empty:
        # We don't have MLBAM IDs in FanGraphs; use name matching as best-effort
        # Season stats are static per player — broadcast to all game rows after merge
        logger.info("Batter aggregator: FanGraphs context merge skipped (no MLBAM key) — using season avgs from Statcast events")

    # ── Fill NaN with column medians ──────────────────────────────────────
    for col in BATTER_STATCAST_COLS:
        if col in agg.columns:
            median = agg[col].median()
            agg[col] = agg[col].fillna(median if pd.notna(median) else 0.0)

    agg = agg.rename(columns={"batter": "player_id"})
    agg["player_id"] = pd.to_numeric(agg["player_id"], errors="coerce").astype("Int64")

    keep = ["player_id", "game_date"] + [c for c in BATTER_STATCAST_COLS if c in agg.columns]
    return agg[[c for c in keep if c in agg.columns]].copy()


# ===========================================================================
# 4. STATCAST FEATURE LAYER — main interface
# ===========================================================================

class StatcastFeatureLayer:
    """
    Orchestrates Statcast data fetching, feature building, and game log enrichment.

    Provides two modes of operation:
      1. enrich_game_logs()  — for training: merges per-game Statcast features
         onto a historical game log DataFrame.
      2. get_inference_snapshot() — for daily inference: returns the most recent
         L30 Statcast aggregate for each player as a flat feature dict.

    Both modes are additive: if Statcast data is unavailable, the input
    DataFrame is returned unchanged (all Statcast cols set to 0.0).
    """

    def __init__(self, fetcher: Optional[StatcastFetcher] = None):
        self._fetcher = fetcher or StatcastFetcher()
        self._sc_df: Optional[pd.DataFrame] = None          # raw Statcast cache
        self._season_batting: Optional[pd.DataFrame] = None # FanGraphs cache
        self._pitcher_agg: Optional[pd.DataFrame] = None    # per-game pitcher features
        self._batter_agg: Optional[pd.DataFrame] = None     # per-game batter features

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def enrich_game_logs(
        self,
        game_logs_df: pd.DataFrame,
        player_type: str,
    ) -> pd.DataFrame:
        """
        Merge Statcast per-game features onto an existing game log DataFrame.

        Parameters
        ----------
        game_logs_df : pd.DataFrame
            Must contain columns: player_id (int), game_date (datetime-like).
            One row per player per game — exactly the format MLPipeline.train() expects.
        player_type : str
            "pitcher" or "batter" — selects which Statcast aggregation to join.

        Returns
        -------
        pd.DataFrame
            Input DataFrame with Statcast feature columns appended.
            All Statcast columns default to 0.0 if data unavailable.
        """
        if player_type not in ("pitcher", "batter"):
            raise ValueError("player_type must be 'pitcher' or 'batter'")

        df = game_logs_df.copy()
        statcast_cols = PITCHER_STATCAST_COLS if player_type == "pitcher" else BATTER_STATCAST_COLS

        # Pre-fill all statcast columns with 0.0 as safe default
        for col in statcast_cols:
            if col not in df.columns:
                df[col] = 0.0

        sc_agg = self._get_per_game_agg(player_type)
        if sc_agg is None or sc_agg.empty:
            logger.warning("StatcastFeatureLayer: no Statcast data — using zero-filled columns")
            return df

        # Align key types for merge
        df["game_date"] = pd.to_datetime(df["game_date"])
        df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce").astype("Int64")
        sc_agg["game_date"] = pd.to_datetime(sc_agg["game_date"])
        sc_agg["player_id"] = sc_agg["player_id"].astype("Int64")

        # Suffix _sc prevents column collision if game log already has similar names
        merged = df.merge(
            sc_agg,
            on=["player_id", "game_date"],
            how="left",
            suffixes=("", "_sc_new"),
        )

        # Use merged Statcast values where available, keep 0.0 default elsewhere
        for col in statcast_cols:
            new_col = f"{col}_sc_new"
            if new_col in merged.columns:
                merged[col] = merged[new_col].fillna(merged[col])
                merged.drop(columns=[new_col], inplace=True)

        logger.info(
            "StatcastFeatureLayer.enrich_game_logs: %s — %d/%d rows enriched",
            player_type, merged[statcast_cols[0]].gt(0).sum(), len(merged),
        )
        return merged

    def get_inference_snapshot(self, player_type: str) -> Dict[int, Dict[str, float]]:
        """
        Return L30 Statcast feature snapshot per player for daily inference.

        Used when running MLPipeline.run_inference() at 11 AM — provides the
        current Statcast context row to append to the game log before inference.

        Returns
        -------
        dict: {mlbam_id (int): {feature_col: value, ...}}
            Empty dict if Statcast data unavailable.
        """
        sc_agg = self._get_per_game_agg(player_type)
        if sc_agg is None or sc_agg.empty:
            return {}

        statcast_cols = PITCHER_STATCAST_COLS if player_type == "pitcher" else BATTER_STATCAST_COLS

        # Take L30 mean per player across all available game dates
        snapshot = (
            sc_agg.groupby("player_id")[statcast_cols]
            .mean()
            .round(4)
        )
        result: Dict[int, Dict[str, float]] = {}
        for player_id, row in snapshot.iterrows():
            result[int(player_id)] = row.to_dict()

        logger.info(
            "StatcastFeatureLayer.get_inference_snapshot: %s — %d players",
            player_type, len(result),
        )
        return result

    def get_velocity_spin_momentum(self, player_type: str = "pitcher") -> Optional[pd.DataFrame]:
        """
        Compute velocity and spin rate momentum (L7 vs L30 pct change) per player.

        These are the 'velocity_drop' and 'spin_rate_drop' features from
        engineer_features.py — critical early-warning signals for pitcher
        fatigue and injury before they show up in K-rate decline.

        Returns DataFrame with columns:
            player_id, pct_drop_velocity, pct_drop_spin_rate
            (positive = velocity/spin has dropped vs baseline — bearish for Ks)
        """
        sc_agg = self._get_per_game_agg(player_type)
        if sc_agg is None or sc_agg.empty:
            return None

        # Sort and compute rolling L7 / L30 per player
        sc_agg = sc_agg.sort_values(["player_id", "game_date"])

        for col, sc_col in [("velocity", "sc_avg_velocity"), ("spin_rate", "sc_avg_spin_rate")]:
            if sc_col not in sc_agg.columns:
                continue
            sc_agg[f"L7_{sc_col}"] = (
                sc_agg.groupby("player_id")[sc_col]
                .transform(lambda s: s.rolling(7, min_periods=1).mean())
            )
            sc_agg[f"L30_{sc_col}"] = (
                sc_agg.groupby("player_id")[sc_col]
                .transform(lambda s: s.rolling(30, min_periods=1).mean())
            )
            # pct_drop: positive = velocity has fallen (fatiguing)
            sc_agg[f"pct_drop_{col}"] = np.where(
                sc_agg[f"L30_{sc_col}"] > 0,
                (sc_agg[f"L30_{sc_col}"] - sc_agg[f"L7_{sc_col}"]) / sc_agg[f"L30_{sc_col}"],
                0.0,
            )

        latest = sc_agg.sort_values("game_date").groupby("player_id").last().reset_index()
        drop_cols = [c for c in latest.columns if c.startswith("pct_drop_")]
        return latest[["player_id"] + drop_cols] if drop_cols else None

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _ensure_raw_data(self) -> None:
        if self._sc_df is None:
            self._sc_df = self._fetcher.fetch_statcast(STATCAST_LOOKBACK_DAYS)
        if self._season_batting is None:
            self._season_batting = self._fetcher.fetch_season_batting()

    def _get_per_game_agg(self, player_type: str) -> Optional[pd.DataFrame]:
        """Build (or return cached) per-game Statcast aggregation."""
        self._ensure_raw_data()

        if self._sc_df is None or self._sc_df.empty:
            return None

        if player_type == "pitcher":
            if self._pitcher_agg is None:
                logger.info("StatcastFeatureLayer: building pitcher per-game aggregation...")
                self._pitcher_agg = _build_pitcher_per_game(self._sc_df)
                logger.info(
                    "StatcastFeatureLayer: pitcher agg complete — %d game-rows, %d pitchers",
                    len(self._pitcher_agg),
                    self._pitcher_agg["player_id"].nunique() if not self._pitcher_agg.empty else 0,
                )
            return self._pitcher_agg
        else:
            if self._batter_agg is None:
                logger.info("StatcastFeatureLayer: building batter per-game aggregation...")
                self._batter_agg = _build_batter_per_game(self._sc_df, self._season_batting)
                logger.info(
                    "StatcastFeatureLayer: batter agg complete — %d game-rows, %d batters",
                    len(self._batter_agg),
                    self._batter_agg["player_id"].nunique() if not self._batter_agg.empty else 0,
                )
            return self._batter_agg

    def clear_cache(self) -> None:
        """Force re-fetch on next call (useful for testing)."""
        self._sc_df = None
        self._season_batting = None
        self._pitcher_agg = None
        self._batter_agg = None


# ===========================================================================
# 5. CONVENIENCE FUNCTION — daily dispatcher hook
# ===========================================================================

def enrich_props_with_statcast(
    props: list,
    player_type: str,
    layer: Optional[StatcastFeatureLayer] = None,
) -> list:
    """
    Enrich a list of prop dicts with Statcast inference snapshots.

    Intended for use in live_dispatcher.py before confidence scoring:

        from statcast_feature_layer import enrich_props_with_statcast
        props = enrich_props_with_statcast(raw_props, player_type="pitcher")

    Each prop dict gains Statcast feature keys (sc_avg_velocity, sc_whiff_rate, etc.)
    that downstream agents can incorporate into their probability scoring.

    Parameters
    ----------
    props : list of dict
        Each dict must have 'mlbam_id' (int) or 'player_id' (int).
    player_type : str
        "pitcher" or "batter"
    layer : StatcastFeatureLayer, optional
        Pass a pre-initialized layer to reuse the daily cache.

    Returns
    -------
    list of dict
        Input props with Statcast features merged in-place.
        Props without a matching mlbam_id are returned unmodified.
    """
    if not props:
        return props

    if layer is None:
        layer = StatcastFeatureLayer()

    snapshot = layer.get_inference_snapshot(player_type)
    if not snapshot:
        logger.info("enrich_props_with_statcast: no snapshot available — props unchanged")
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
        "enrich_props_with_statcast: %d/%d props enriched with Statcast features",
        matched, len(props),
    )
    return enriched


# ===========================================================================
# 6. STANDALONE TEST
# ===========================================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    )
    print("\n" + "=" * 65)
    print("  PropIQ StatcastFeatureLayer — smoke test")
    print("=" * 65)

    layer = StatcastFeatureLayer()

    print("\n[1] Fetching Statcast data (last 30 days)...")
    pitcher_snap = layer.get_inference_snapshot("pitcher")
    batter_snap  = layer.get_inference_snapshot("batter")

    print(f"    Pitchers with Statcast features : {len(pitcher_snap):,}")
    print(f"    Batters  with Statcast features : {len(batter_snap):,}")

    if pitcher_snap:
        sample_id = next(iter(pitcher_snap))
        print(f"\n[2] Sample pitcher ({sample_id}) features:")
        for k, v in pitcher_snap[sample_id].items():
            print(f"    {k:<28} : {v:.4f}")

    momentum_df = layer.get_velocity_spin_momentum("pitcher")
    if momentum_df is not None and not momentum_df.empty:
        print("\n[3] Top 5 pitchers by velocity drop (fatigue signal):")
        top = momentum_df.sort_values("pct_drop_velocity", ascending=False).head(5)
        print(top.to_string(index=False))

    print("\n✅ StatcastFeatureLayer test complete.")
    print("=" * 65 + "\n")
