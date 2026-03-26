"""
marcel_layer.py
===============
Marcel 3-Year Projection System for PropIQ Analytics Engine.

Derived from baseball-sims (thomasosbot/baseball-sims) WITHOUT BHQ subscription.
Original algorithm: Tom Tango's "Marcel the Monkey Forecasting System".
Reference implementation: src/features/marcel.py in thomasosbot/baseball-sims.

Algorithm:
  1. Collect up to 3 prior seasons of player stats from FanGraphs JSON API
  2. Apply year weights: 5 × most-recent + 4 × prior year + 3 × two years back
  3. Regress to league mean: player_weight = weighted_PA / (weighted_PA + regression_PA)
  4. Apply age adjustment: +0.6%/yr improvement under 29, -0.3%/yr decline over 29
  5. Produce projected rates per player for use as confidence modifiers in Layer 1

PropIQ integration (Layer 8a, fires after FanGraphs Layer 6):
  Batter K%   → K Under prop:  if projected K% >> league avg → small K Under boost
  Batter HR/PA → HR/TB Over:   if projected HR rate >> league avg → boost
  Batter wOBA → hits/H+R+RBI: if wOBA >> league avg → hits Over boost
  Pitcher K%  → K Over prop:   if projected K% >> league avg → boost
  Pitcher BB% → ER Under:      if projected BB% << league avg → ER Under boost
  Pitcher HR/9 → ER Under:     if projected HR/9 << league avg → ER Under boost

Max adjustment: ±0.018 per prop — subtle refinement layered on top of Layers 1-7.
Never overrides or replaces; always additive.

"""
Data source:
  FanGraphs JSON API — https://www.fangraphs.com/api/leaders/major-league/data
  Public endpoint, no API key required.
  Fetches 3 prior seasons (e.g. 2023+2024+2025 for 2026 projections).

Cache:
  /tmp/marcel_{year}_{iso_year}w{iso_week}.json — refreshed weekly.
  Season-level projections that don't change day to day.

Dependencies:
  requests  (already in project requirements)

Usage:
    layer = MarcelLayer(projection_year=2026)
    layer.prefetch()
    batter_proj  = layer.get_batter("Aaron Judge")
    pitcher_proj = layer.get_pitcher("Spencer Strider")
    adj = marcel_adjustment("strikeouts", "Over", "pitcher", pitcher_proj)
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone

import requests

logger = logging.getLogger("propiq.marcel")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# 2024–2025 MLB baseline rates used as regression anchors
_LEAGUE_AVG: dict = {
    # Batter rates
    "batter_k_pct":  0.228,   # 22.8% K rate
    "batter_bb_pct": 0.083,   # 8.3%  BB rate
    "batter_hr_pa":  0.033,   # 3.3%  HR per PA (~1 HR per 30 PA)
    "batter_woba":   0.315,   # .315  wOBA
    "batter_iso":    0.165,   # .165  ISO
    # Pitcher rates (rates *allowed*)
    "pitcher_k_pct":  0.228,  # 22.8% K%
    "pitcher_bb_pct": 0.083,  # 8.3%  BB%
    "pitcher_hr9":    1.30,   # 1.30  HR/9
}


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def _get_cache_path(year: int):
    """Weekly cache file — refreshed on Monday of each new week."""
    today = datetime.now(timezone.utc)
    iso = today.isocalendar()
    return tempfile.TemporaryFile(mode='w+')


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _parse_pct(val) -> float:
    """
    Parse FanGraphs percentage field.
    Handles both string format ("22.0 %") and decimal float (0.22 or 22.0).
    Returns a decimal fraction (0.22, not 22).
    """
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        v = float(val)
        return v / 100.0 if v > 1.0 else v
    s = str(val).strip().rstrip("%").strip()
    try:
        v = float(s)
        return v / 100.0 if v > 1.0 else v
    except ValueError:
        return 0.0


def _parse_float(val, default: float = 0.0) -> float:
    """Safe float parse from any type."""
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


# ---------------------------------------------------------------------------
# FanGraphs data fetcher
# ---------------------------------------------------------------------------

def _fetch_fg_data(stats: str, season_start: int, season_end: int) -> list[dict]:
    """
    Fetch multi-year leaderboard from FanGraphs JSON API.

    stats        : "bat" for batters, "pit" for pitchers
    season_start : earliest season (inclusive), e.g. 2023
    season_end   : most recent season (inclusive), e.g. 2025
    ind=1        : return individual season rows (not combined career total)
    type=8       : advanced stats panel (wRC+, wOBA, ISO, K%, BB%, etc.)

    Returns raw list of row dicts.
    """
    params = {
        "age":     "",
        "pos":     "all",
        "stats":   stats,
        "lg":      "all",
        "qual":    "0",       # all players regardless of PA minimum
        "season":  str(season_end),
        "season1": str(season_start),
        "ind":     "1",       # individual seasons
        "type":    "8",       # advanced stats
    }
    try:
        resp = requests.get(
            _FG_BASE_URL, params=params, headers=_HEADERS, timeout=_TIMEOUT
        )
        if resp.status_code != 200:
            logger.warning(
                "[Marcel] FanGraphs HTTP %d (stats=%s, %d-%d)",
                resp.status_code, stats, season_start, season_end,
            )
            return []
        data = resp.json()
        rows = data if isinstance(data, list) else data.get("data", [])
        logger.info(
            "[Marcel] FanGraphs %s: %d rows (%d-%d)",
            stats, len(rows), season_start, season_end,
        )
        return rows
    except Exception as exc:
        logger.warning(
            "[Marcel] FanGraphs fetch failed (stats=%s): %s", stats, exc
        )
        return []


# ---------------------------------------------------------------------------
# Age adjustment (from Marcel spec via baseball-sims)
# ---------------------------------------------------------------------------

def _age_multiplier(age: int | None) -> float:
    """
    Marcel age multiplier for batter performance rates.
    Pitchers invert this (age hurts rates allowed differently — caller handles).
    """
    if age is None:
        return 1.0
    if age < _AGE_PEAK:
        return 1.0 + _AGE_YOUNG_RATE * (_AGE_PEAK - age)
    elif age > _AGE_PEAK:
        return 1.0 - _AGE_OLD_RATE * (age - _AGE_PEAK)
    return 1.0


# ---------------------------------------------------------------------------
# Marcel rate computation (core formula)
# ---------------------------------------------------------------------------

def _marcel_rate(
    data_by_year: dict[int, tuple[float, float]],  # {year: (stat_value, pa_weight)}
    league_avg:     float,
    regression_pa:  float,
    age_mult:       float,
) -> float:
    """
    Compute Marcel projected rate for a single statistic.

    Steps:
      1. Weight 3 most-recent seasons: 5/4/3 (most recent first)
      2. Regress to league mean: player gets more credit with more PA
      3. Apply age adjustment multiplier

    Returns the projected rate (e.g. 0.245 for K%).
    """
    years = sorted(data_by_year.keys(), reverse=True)[:3]  # most recent first

    weighted_sum = 0.0
    weighted_pa  = 0.0
    for i, yr in enumerate(years):
        stat_val, pa = data_by_year[yr]
        w = _MARCEL_WEIGHTS[i]
        weighted_sum += stat_val * pa * w
        weighted_pa  += pa * w

    if weighted_pa == 0:
        return league_avg

    raw_rate = weighted_sum / weighted_pa

    # Bayesian regression toward league mean
    player_weight = weighted_pa / (weighted_pa + regression_pa)
    regressed     = raw_rate * player_weight + league_avg * (1.0 - player_weight)

    return max(0.0, regressed * age_mult)


# ---------------------------------------------------------------------------
# Projection builders
# ---------------------------------------------------------------------------

def _build_batter_projections(
    rows: list[dict], projection_year: int
) -> dict[str, dict]:
    """
    Build Marcel batter projections from multi-year FanGraphs rows.

    Returns {player_name_lower: {k_pct, bb_pct, hr_pa, woba, iso, weighted_pa, age}}
    """
    by_player: dict[str, list[dict]] = {}
    for row in rows:
        name = str(
            row.get("PlayerName") or row.get("Name") or ""
        ).strip()
        if not name:
            continue
        by_player.setdefault(name.lower(), []).append(row)

    projections: dict[str, dict] = {}

    for name_lower, player_rows in by_player.items():
        k_data:    dict[int, tuple[float, float]] = {}
        bb_data:   dict[int, tuple[float, float]] = {}
        hr_data:   dict[int, tuple[float, float]] = {}
        woba_data: dict[int, tuple[float, float]] = {}
        iso_data:  dict[int, tuple[float, float]] = {}

        latest_age:  int | None = None
        latest_year: int = 0

        for row in player_rows:
            season = int(row.get("Season") or 0)
            if not season:
                continue
            pa = _parse_float(row.get("PA") or row.get("TPA"), 0.0)
            if pa < 10:
                continue  # too few PA to be meaningful

            k_pct  = _parse_pct(row.get("K%"))
            bb_pct = _parse_pct(row.get("BB%"))
            hr     = _parse_float(row.get("HR"), 0.0)
            hr_pa  = hr / pa if pa > 0 else 0.0
            woba   = _parse_float(row.get("wOBA"), 0.0)
            iso    = _parse_float(row.get("ISO"), 0.0)
            age    = _parse_float(row.get("Age"), 0.0)

            k_data[season]    = (k_pct,  pa)
            bb_data[season]   = (bb_pct, pa)
            hr_data[season]   = (hr_pa,  pa)
            woba_data[season] = (woba,   pa)
            iso_data[season]  = (iso,    pa)

            if season > latest_year and age > 0:
                latest_year = season
                latest_age  = int(age)

        if not k_data:
            continue

        # Project age to current year
        proj_age  = (
            latest_age + (projection_year - latest_year)
            if latest_age and latest_year else None
        )
        age_mult  = _age_multiplier(proj_age)

        # Confidence-weighted PA (for potential downstream use)
        years = sorted(k_data.keys(), reverse=True)[:3]
        num_weights = len(years)
        weighted_pa = (
            sum(k_data[yr][1] * _MARCEL_WEIGHTS[i] for i, yr in enumerate(years))
            / sum(_MARCEL_WEIGHTS[:num_weights])
        )

        projections[name_lower] = {
            "k_pct":       round(_marcel_rate(k_data,    _LEAGUE_AVG["batter_k_pct"],  _BATTER_REGRESSION_PA, 1.0),      4),
            "bb_pct":      round(_marcel_rate(bb_data,   _LEAGUE_AVG["batter_bb_pct"], _BATTER_REGRESSION_PA, 1.0),      4),
            "hr_pa":       round(_marcel_rate(hr_data,   _LEAGUE_AVG["batter_hr_pa"],  _BATTER_REGRESSION_PA, age_mult), 4),
            "woba":        round(_marcel_rate(woba_data, _LEAGUE_AVG["batter_woba"],   _BATTER_REGRESSION_PA, age_mult), 4),
            "iso":         round(_marcel_rate(iso_data,  _LEAGUE_AVG["batter_iso"],    _BATTER_REGRESSION_PA, age_mult), 4),
            "weighted_pa": round(weighted_pa, 0),
            "age":         proj_age,
        }

    logger.info("[Marcel] Built %d batter projections.", len(projections))
    return projections


def _build_pitcher_projections(
    rows: list[dict], projection_year: int
) -> dict[str, dict]:
    """
    Build Marcel pitcher projections from multi-year FanGraphs rows.

    Returns {player_name_lower: {k_pct, bb_pct, hr9, weighted_bf, age}}

    Note on age adjustment for pitchers (from baseball-sims architecture.md):
      Pitchers project *rates allowed*, so age works in the opposite direction.
      A young pitcher improving = lower rates allowed (good).
      _age_mult is inverted for pitcher projection (older = higher rates allowed).
    """
    by_player: dict[str, list[dict]] = {}
    for row in rows:
        name = str(
            row.get("PlayerName") or row.get("Name") or ""
        ).strip()
        if not name:
            continue
        by_player.setdefault(name.lower(), []).append(row)

    projections: dict[str, dict] = {}

    for name_lower, player_rows in by_player.items():
        k_data:   dict[int, tuple[float, float]] = {}
        bb_data:  dict[int, tuple[float, float]] = {}
        hr9_data: dict[int, tuple[float, float]] = {}

        latest_age: int | None = None
        latest_year: int = 0

        for row in player_rows:
            season = int(row.get("Season") or 0)
            if not season:
                continue
            ip = _parse_float(row.get("IP"), 0.0)
            if ip < 5:
                continue

            k_pct  = _parse_pct(row.get("K%"))
            bb_pct = _parse_pct(row.get("BB%"))
            hr9    = _parse_float(row.get("HR/9") or row.get("HR9"), 0.0)
            age    = _parse_float(row.get("Age"), 0.0)

            # Use IP * 4.3 as BF proxy (batters faced ≈ IP × 4.3)
            bf_proxy = ip * 4.3

            k_data[season]   = (k_pct,  bf_proxy)
            bb_data[season]  = (bb_pct, bf_proxy)
            hr9_data[season] = (hr9,    bf_proxy)

            if season > latest_year and age > 0:
                latest_year = season
                latest_age  = int(age)

        if not k_data:
            continue

        proj_age = (
            latest_age + (projection_year - latest_year)
            if latest_age and latest_year else None
        )

        # Pitcher age multiplier is *inverted* vs batter:
        # K% (strikeout ability) declines with age → use forward age_mult directly
        # BB% and HR/9 (control and flyball) use inverted mult for rates *allowed*
        age_mult_base     = _age_multiplier(proj_age)
        age_mult_inverted = 1.0 / age_mult_base if age_mult_base > 0 else 1.0

        years = sorted(k_data.keys(), reverse=True)[:3]
        num_weights = len(years)
        weighted_bf = (
            sum(k_data[yr][1] * _MARCEL_WEIGHTS[i] for i, yr in enumerate(years))
            / sum(_MARCEL_WEIGHTS[:num_weights])
        )

        projections[name_lower] = {
            "k_pct":       round(_marcel_rate(k_data,   _LEAGUE_AVG["pitcher_k_pct"],  _PITCHER_REGRESSION_BF, age_mult_base),     4),
            "bb_pct":      round(_marcel_rate(bb_data,  _LEAGUE_AVG["pitcher_bb_pct"], _PITCHER_REGRESSION_BF, age_mult_inverted),  4),
            "hr9":         round(_marcel_rate(hr9_data, _LEAGUE_AVG["pitcher_hr9"],    _PITCHER_REGRESSION_BF, age_mult_inverted),  4),
            "weighted_bf": round(weighted_bf, 0),
            "age":         proj_age,
        }

    logger.info("[Marcel] Built %d pitcher projections.", len(projections))
    return projections


# ---------------------------------------------------------------------------
# MarcelLayer class
# ---------------------------------------------------------------------------

class MarcelLayer:
    """
    Marcel 3-year projection system for PropIQ Analytics.

    Loads and caches projected rates for all MLB batters and pitchers.
    Used as a pre-season confidence signal on top of Layers 1-7.

    The weekly cache means Marcel only hits FanGraphs twice per week
    (once for batters, once for pitchers) regardless of how many
    dispatches run that week.

    Usage:
        layer = MarcelLayer(projection_year=2026)
        layer.prefetch()
        batter  = layer.get_batter("Aaron Judge")
        pitcher = layer.get_pitcher("Spencer Strider")
    """

    def __init__(self, projection_year: int | None = None) -> None:
        self._year       = projection_year or datetime.now(timezone.utc).year
        self._cache_path = _get_cache_path(self._year)
        self._batters:   dict[str, dict] = {}
        self._pitchers:  dict[str, dict] = {}
        self._loaded:    bool = False

    # ── cache I/O ──────────────────────────────────────────────────────────

    def _load_cache(self) -> bool:
        if os.path.exists(self._cache_path):
            try:
                with open(self._cache_path) as f:
                    data = json.load(f)
                self._batters  = data.get("batters",  {})
                self._pitchers = data.get("pitchers", {})
                self._loaded   = True
                logger.info(
                    "[Marcel] Cache loaded: %d batters, %d pitchers (%s)",
                    len(self._batters), len(self._pitchers),
                    os.path.basename(self._cache_path),
                )
                return True
            except Exception as exc:
                logger.warning("[Marcel] Cache load failed: %s", exc)
        return False
    def prefetch(self) -> None:
        """
        Load Marcel projections. Reads from weekly cache if available;
        otherwise fetches 3 years of FanGraphs data and computes projections.

        FanGraphs data: prior 3 seasons relative to projection year.
        (e.g. for 2026 projections: 2023 + 2024 + 2025 data)
        """
        if not self._loaded and self._load_cache():
            return  # valid weekly cache exists

        season_end   = self._year - 1    # most recent complete season
        season_start = season_end - 2    # 3 years back

        logger.info(
            "[Marcel] Fetching FanGraphs %d-%d for %d projections...",
            season_start, season_end, self._year,
        )

        batter_rows  = _fetch_fg_data("bat", season_start, season_end)
        time.sleep(_REQUEST_DELAY)
        pitcher_rows = _fetch_fg_data("pit", season_start, season_end)

        if not batter_rows and not pitcher_rows:
            logger.warning(
                "[Marcel] No FanGraphs data retrieved — Marcel layer disabled."
            )
            return

        self._batters  = _build_batter_projections(batter_rows,  self._year)
        self._pitchers = _build_pitcher_projections(pitcher_rows, self._year)
        self._loaded   = True
        self._save_cache()

    def get_batter(self, name: str) -> dict:
        """
        Return Marcel projection for a batter by display name.
        Returns {} if player not found (graceful — adjustment returns 0.0).
        """
        if not self._loaded:
            self._load_cache()
        return self._batters.get(name.strip().lower(), {})

    def get_pitcher(self, name: str) -> dict:
        """
        Return Marcel projection for a pitcher by display name.
        Returns {} if player not found.
        """
        if not self._loaded:
            self._load_cache()
        return self._pitchers.get(name.strip().lower(), {})


# ---------------------------------------------------------------------------
# Probability adjustment function
# ---------------------------------------------------------------------------

def marcel_adjustment(
    prop_type:   str,
    side:        str,
    player_type: str,   # "pitcher" | "batter"
    marcel_data: dict,
) -> float:
    """
    Compute probability adjustment from Marcel projected rates.

    Compares the player's Marcel projection to league average.
    Large positive/negative deviation from mean generates a nudge.

    Adjustments are intentionally small (max ±0.018) — Marcel is a
    pre-season projection layer that adds historical context to the
    already-running 7 real-time layers.  It should never dominate
    a signal that comes from today's matchup context.

    Prop mappings:
      pitcher  + strikeouts → K% deviation
      pitcher  + earned_runs Under → BB% + HR/9 advantage
      batter   + home_runs → HR/PA deviation
      batter   + total_bases → ISO deviation
      batter   + hits / hits_runs_rbis → wOBA deviation
      batter   + strikeouts → batter K% deviation (K Over / Under)
      batter   + runs → wOBA proxy for OBP

    Returns a float delta in range roughly [-0.018, +0.018].
    """
    if not marcel_data:
        return 0.0

    adj = 0.0

    if player_type == "pitcher":
        k_pct  = marcel_data.get("k_pct",  0.0)
        bb_pct = marcel_data.get("bb_pct", 0.0)
        hr9    = marcel_data.get("hr9",    0.0)

        if prop_type == "strikeouts":
            # k_delta: positive = pitcher strikes out more than average
            k_delta = k_pct - _LEAGUE_AVG["pitcher_k_pct"]
            if side == "Over":
                adj = min(0.018, max(-0.012, k_delta * 0.35))
            else:  # Under
                adj = min(0.012, max(-0.018, -k_delta * 0.25))

        elif prop_type == "earned_runs" and side == "Under":
            # Fewer walks + fewer HR = fewer baserunners = fewer earned runs
            bb_adv  = _LEAGUE_AVG["pitcher_bb_pct"] - bb_pct  # pos = fewer walks (good)
            hr9_adv = _LEAGUE_AVG["pitcher_hr9"]    - hr9      # pos = fewer HR (good)
            adj = min(0.015, max(0.0, bb_adv * 0.10 + hr9_adv * 0.025))

    elif player_type == "batter":
        k_pct  = marcel_data.get("k_pct",  0.0)
        bb_pct = marcel_data.get("bb_pct", 0.0)
        hr_pa  = marcel_data.get("hr_pa",  0.0)
        woba   = marcel_data.get("woba",   0.0)
        iso    = marcel_data.get("iso",    0.0)

        if prop_type == "home_runs":
            hr_delta = hr_pa - _LEAGUE_AVG["batter_hr_pa"]  # pos = power hitter
            if side == "Over":
                adj = min(0.018, max(-0.010, hr_delta * 3.50))
            else:
                adj = min(0.010, max(-0.018, -hr_delta * 2.50))

        elif prop_type == "total_bases":
            iso_delta = iso - _LEAGUE_AVG["batter_iso"]  # pos = extra-base hitter
            if side == "Over":
                adj = min(0.015, max(-0.010, iso_delta * 0.25))
            else:
                adj = min(0.010, max(-0.015, -iso_delta * 0.18))

        elif prop_type in ("hits", "hits_runs_rbis"):
            woba_delta = woba - _LEAGUE_AVG["batter_woba"]  # pos = high-contact
            if side == "Over":
                adj = min(0.015, max(-0.010, woba_delta * 0.12))
            else:
                adj = min(0.010, max(-0.015, -woba_delta * 0.09))

        elif prop_type == "strikeouts":
            # Batter K prop — high projected K% = more likely to strike out
            k_delta = k_pct - _LEAGUE_AVG["batter_k_pct"]  # pos = high-K batter
            if side == "Over":
                adj = min(0.012, max(-0.010, k_delta * 0.20))
            else:
                adj = min(0.010, max(-0.012, -k_delta * 0.15))

        elif prop_type == "runs":
            # wOBA as OBP proxy — high wOBA batters score more runs
            woba_delta = woba - _LEAGUE_AVG["batter_woba"]
            if side == "Over":
                adj = min(0.010, max(-0.007, woba_delta * 0.09))

        elif prop_type == "rbis":
            # ISO proxy for RBI ability (extra-base hits drive in more runs)
            iso_delta = iso - _LEAGUE_AVG["batter_iso"]
            if side == "Over":
                adj = min(0.010, max(-0.007, iso_delta * 0.15))

    return round(adj, 4)
