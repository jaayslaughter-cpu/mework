"""
matchup_engine.py — Pitch-type matchup scoring for PropIQ.
===========================================================
Adapted from kekoa-santana/mlb_bayesian_projections (MIT license).
Core math: log-odds additive method for K (whiff), BB (chase) matchups.

Achieved Brier scores in source repo:
  Pitcher K : 0.188  (vs PropIQ current ~0.248)
  Batter K  : 0.147

Integration
-----------
Called from prop_enrichment_layer.enrich_props() after pa_model step.
Stamps the following fields on each prop dict:
  _matchup_k_lift       : float — logit lift for K props (±0.30 cap)
  _matchup_bb_lift      : float — logit lift for BB/walks_allowed props
  _matchup_reliability  : float — avg data reliability 0.0–1.0 (lower = more shrinkage)

Entry point:
  from matchup_engine import get_matchup_lift
  result = get_matchup_lift(prop)
  # result = {"k_lift_logit": 0.12, "bb_lift_logit": -0.05, "avg_reliability": 0.67}

Bullpen entry point:
  from matchup_engine import get_bullpen_matchup_lift
  result = get_bullpen_matchup_lift(team_abbrev, batter_ids)
  # result = {"bullpen_k_lift": 0.08, "bullpen_bb_lift": -0.03}

Data Sources
------------
Primary   : Baseball Savant pitch-arsenal-stats CSV (all pitchers, daily cache)
            https://baseballsavant.mlb.com/leaderboard/pitch-arsenal-stats
            ?type=pitcher&min=1&year={year}&csv=true
Secondary : Baseball Savant batter-vs-pitch-type CSV (all batters, daily cache)
            https://baseballsavant.mlb.com/leaderboard/pitch-arsenal-stats
            ?type=batter&min=1&year={year}&csv=true
Fallback  : _opp_avg_k_pct from lineup_chase_layer (already on prop dict)
            Returns 0.0 lifts on any failure — NEVER raises.

Cache
-----
  /tmp/propiq_statcast/arsenal_pitcher_{date}.parquet — disk, daily
  /tmp/propiq_statcast/arsenal_batter_{date}.parquet  — disk, daily
  Redis key  matchup:{pitcher_id}:{date} — 6h TTL, per-pitcher result
"""

from __future__ import annotations

import io
import json
import logging
import math
import os
import time
from datetime import date
from pathlib import Path
from typing import Any

logger = logging.getLogger("propiq.matchup_engine")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Logit clip bounds — prevents log(0)
_CLIP_LO = 0.005
_CLIP_HI = 0.995

# Maximum logit lift magnitude applied to any prop — ≈±7pp at 50% baseline
_MAX_LIFT_LOGIT = 0.30

# Minimum pitches for a pitch type to count in arsenal
_MIN_PITCHES = 20

# Reliability sample-size threshold (50 swings = full weight)
_RELIABILITY_N = 50

# Cache TTLs
_REDIS_TTL_SECONDS = 21600   # 6 hours
_DISK_CACHE_DIR = Path(os.getenv("PROPIQ_STATCAST_CACHE", "/tmp/propiq_statcast"))
_DISK_CACHE_DIR.mkdir(parents=True, exist_ok=True)

_SAVANT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    "Referer": "https://baseballsavant.mlb.com/",
}

# 2025 Statcast league-average baselines by pitch type
# Source: Baseball Savant 2025 pitch arsenal leaderboard
LEAGUE_AVG_BY_PITCH_TYPE: dict[str, dict[str, float]] = {
    "FF": {"whiff_rate": 0.213, "chase_rate": 0.295, "barrel_rate": 0.075},
    "SI": {"whiff_rate": 0.165, "chase_rate": 0.280, "barrel_rate": 0.065},
    "FC": {"whiff_rate": 0.245, "chase_rate": 0.320, "barrel_rate": 0.055},
    "SL": {"whiff_rate": 0.335, "chase_rate": 0.350, "barrel_rate": 0.045},
    "ST": {"whiff_rate": 0.373, "chase_rate": 0.360, "barrel_rate": 0.030},
    "CU": {"whiff_rate": 0.282, "chase_rate": 0.332, "barrel_rate": 0.030},
    "KC": {"whiff_rate": 0.290, "chase_rate": 0.340, "barrel_rate": 0.025},
    "SV": {"whiff_rate": 0.305, "chase_rate": 0.345, "barrel_rate": 0.040},
    "CH": {"whiff_rate": 0.338, "chase_rate": 0.380, "barrel_rate": 0.035},
    "FS": {"whiff_rate": 0.360, "chase_rate": 0.400, "barrel_rate": 0.030},
    "FO": {"whiff_rate": 0.295, "chase_rate": 0.370, "barrel_rate": 0.030},
    "CS": {"whiff_rate": 0.260, "chase_rate": 0.310, "barrel_rate": 0.025},
}

LEAGUE_AVG_OVERALL: dict[str, float] = {
    "whiff_rate": 0.248,
    "chase_rate": 0.307,
    "barrel_rate": 0.055,
    "k_rate": 0.223,
    "bb_rate": 0.087,
}

# Pitch family groupings — used as fallback when specific pitch type has thin data
PITCH_TO_FAMILY: dict[str, str] = {
    "FF": "fastball", "SI": "fastball", "FC": "fastball",
    "SL": "breaking", "ST": "breaking", "CU": "breaking",
    "KC": "breaking", "SV": "breaking", "CS": "breaking",
    "CH": "offspeed", "FS": "offspeed", "FO": "offspeed",
}

# Prop types that use K matchup lift
_K_PROP_TYPES = {"strikeouts", "pitcher_strikeouts", "hitter_strikeouts"}

# Prop types that use BB matchup lift
_BB_PROP_TYPES = {"walks_allowed"}


# ---------------------------------------------------------------------------
# Math helpers (pure Python — no numpy required)
# ---------------------------------------------------------------------------

def _logit(p: float) -> float:
    """Log-odds transform with clipping to avoid infinities."""
    p = max(_CLIP_LO, min(_CLIP_HI, float(p)))
    return math.log(p / (1.0 - p))


def _inv_logit(x: float) -> float:
    """Inverse logit (sigmoid)."""
    return 1.0 / (1.0 + math.exp(-float(x)))


def _safe_float(val: Any, default: float = 0.0) -> float:
    if val is None:
        return default
    try:
        v = float(val)
        return v if math.isfinite(v) else default
    except (ValueError, TypeError):
        return default


# ---------------------------------------------------------------------------
# Redis cache helper
# ---------------------------------------------------------------------------

def _redis_get(key: str) -> dict | None:
    try:
        import redis as _redis
        _url = os.environ.get("REDIS_URL", "")
        if not _url:
            return None
        r = _redis.from_url(_url, socket_timeout=2)
        raw = r.get(key)
        if raw:
            return json.loads(raw)
    except Exception:
        pass
    return None


def _redis_set(key: str, value: dict, ttl: int = _REDIS_TTL_SECONDS) -> None:
    try:
        import redis as _redis
        _url = os.environ.get("REDIS_URL", "")
        if not _url:
            return
        r = _redis.from_url(_url, socket_timeout=2)
        r.set(key, json.dumps(value), ex=ttl)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Baseball Savant pitch-arsenal CSV fetcher
# ---------------------------------------------------------------------------

# Module-level daily-attempt guard — prevents re-hitting failed endpoints in same day
_FETCH_ATTEMPTED: dict[str, str] = {}


def _fetch_savant_csv(url: str, cache_name: str):
    """
    Download a Baseball Savant CSV, cache as parquet, return DataFrame or None.
    Daily-attempt guard prevents re-fetching on failure.
    """
    today_str = date.today().isoformat()
    cache_file = _DISK_CACHE_DIR / f"{cache_name}_{today_str}.parquet"

    try:
        import pandas as pd

        # L1: disk cache hit
        if cache_file.exists():
            try:
                return pd.read_parquet(cache_file)
            except Exception:
                cache_file.unlink(missing_ok=True)

        # Daily guard: skip if already tried and got nothing
        if _FETCH_ATTEMPTED.get(cache_name) == today_str:
            logger.debug("[MatchupEngine] %s already attempted today — skipping", cache_name)
            return None

        _FETCH_ATTEMPTED[cache_name] = today_str
        logger.info("[MatchupEngine] Fetching %s ...", cache_name)
        time.sleep(0.3)  # polite rate limit

        import requests, urllib.parse as _ul_me
        resp = requests.get(url, headers=_SAVANT_HEADERS, timeout=20)
        if resp.status_code in (403, 429):
            # ScraperAPI retry — same pattern used in statcast_feature_layer + steamer_layer
            _sa_key_me = os.getenv("SCRAPERAPI_KEY", "")
            if _sa_key_me:
                _proxy_me = (
                    f"http://proxy-server.scraperapi.com/?api_key={_sa_key_me}"
                    f"&url={_ul_me.quote(url, safe='')}"
                )
                try:
                    resp = requests.get(_proxy_me, headers=_SAVANT_HEADERS, timeout=45)
                    logger.info(
                        "[MatchupEngine/ScraperAPI] proxy status=%d for %s",
                        resp.status_code, cache_name,
                    )
                except Exception as _sp_exc:
                    logger.warning("[MatchupEngine/ScraperAPI] Proxy failed: %s", _sp_exc)
                    return None
            else:
                logger.warning(
                    "[MatchupEngine] HTTP %d for %s (set SCRAPERAPI_KEY for retry)",
                    resp.status_code, cache_name,
                )
                return None
        if resp.status_code != 200:
            logger.warning("[MatchupEngine] HTTP %d for %s", resp.status_code, cache_name)
            return None

        df = pd.read_csv(io.StringIO(resp.text), low_memory=False)
        if df.empty:
            logger.warning("[MatchupEngine] Empty CSV for %s", cache_name)
            return None

        df.to_parquet(cache_file, index=False)
        _FETCH_ATTEMPTED.pop(cache_name, None)  # clear guard on success
        logger.info("[MatchupEngine] %s → %d rows cached", cache_name, len(df))
        return df

    except ImportError:
        logger.debug("[MatchupEngine] pandas/requests not available")
        return None
    except Exception as exc:
        logger.warning("[MatchupEngine] %s fetch failed: %s", cache_name, exc)
        return None


# Module-level in-memory cache of parsed arsenal DataFrames (avoids re-parsing on every prop)
_PITCHER_ARSENAL_DF = None
_BATTER_VULN_DF = None
_ARSENAL_DATE: str | None = None


def _get_pitcher_arsenal_df():
    """Return full pitcher arsenal DataFrame (all pitchers, current season), cached daily."""
    global _PITCHER_ARSENAL_DF, _ARSENAL_DATE
    today_str = date.today().isoformat()

    if _PITCHER_ARSENAL_DF is not None and _ARSENAL_DATE == today_str:
        return _PITCHER_ARSENAL_DF

    year = date.today().year
    url = (
        f"https://baseballsavant.mlb.com/leaderboard/pitch-arsenal-stats"
        f"?type=pitcher&min=1&year={year}&csv=true"
    )
    df = _fetch_savant_csv(url, f"arsenal_pitcher_{year}")

    if df is None:
        # Try prior year fallback
        url_prior = (
            f"https://baseballsavant.mlb.com/leaderboard/pitch-arsenal-stats"
            f"?type=pitcher&min=1&year={year - 1}&csv=true"
        )
        df = _fetch_savant_csv(url_prior, f"arsenal_pitcher_{year - 1}")

    if df is not None:
        df = _normalize_arsenal_df(df, id_col="pitcher")
        _PITCHER_ARSENAL_DF = df
        _ARSENAL_DATE = today_str
        logger.info("[MatchupEngine] Pitcher arsenal: %d rows loaded", len(df))
    else:
        _PITCHER_ARSENAL_DF = None

    return _PITCHER_ARSENAL_DF


def _get_batter_vuln_df():
    """Return full batter vulnerability DataFrame (all batters, current season), cached daily."""
    global _BATTER_VULN_DF
    today_str = date.today().isoformat()

    # Simple same-day check using ARSENAL_DATE
    if _BATTER_VULN_DF is not None and _ARSENAL_DATE == today_str:
        return _BATTER_VULN_DF

    year = date.today().year
    url = (
        f"https://baseballsavant.mlb.com/leaderboard/pitch-arsenal-stats"
        f"?type=batter&min=1&year={year}&csv=true"
    )
    df = _fetch_savant_csv(url, f"arsenal_batter_{year}")

    if df is not None:
        df = _normalize_arsenal_df(df, id_col="batter")
        _BATTER_VULN_DF = df
        logger.info("[MatchupEngine] Batter vulnerability: %d rows loaded", len(df))
    else:
        _BATTER_VULN_DF = None

    return _BATTER_VULN_DF


def _normalize_arsenal_df(df, id_col: str = "pitcher"):
    """
    Normalize Savant pitch arsenal CSV to standard column names.
    Handles variations in column naming across Savant exports.
    Returns DataFrame with: player_id, pitch_type, pitches, usage_pct,
                             whiff_rate, chase_rate, barrel_rate, pitch_family
    """
    import pandas as pd

    col_map: dict[str, str] = {}

    # Player ID column
    for candidate in [f"{id_col}_id", "player_id", "mlb_id", "batter_id"]:
        if candidate in df.columns:
            col_map[candidate] = "player_id"
            break

    # Pitch type
    for candidate in ["pitch_type", "pitch_name", "pitch"]:
        if candidate in df.columns:
            col_map[candidate] = "pitch_type"
            break

    # Pitch count
    for candidate in ["pitches", "n_pitches", "pitch_count", "pa_count"]:
        if candidate in df.columns:
            col_map[candidate] = "pitches"
            break

    # Usage percentage
    for candidate in ["percent", "usage_percent", "pitch_percent", "pitch_pct"]:
        if candidate in df.columns:
            col_map[candidate] = "usage_raw"
            break

    # Whiff rate
    for candidate in ["whiff_percent", "whiff_rate", "whiff_pct", "whiff"]:
        if candidate in df.columns:
            col_map[candidate] = "whiff_raw"
            break

    # Chase / O-swing rate (may not exist in all exports)
    for candidate in ["o_swing_percent", "chase_percent", "chase_rate", "o_swing"]:
        if candidate in df.columns:
            col_map[candidate] = "chase_raw"
            break

    # Barrel rate
    for candidate in ["barrel_batted_rate", "barrel_percent", "barrel", "barrel_rate"]:
        if candidate in df.columns:
            col_map[candidate] = "barrel_raw"
            break

    df = df.rename(columns=col_map).copy()

    # Ensure required columns exist
    if "player_id" not in df.columns or "pitch_type" not in df.columns:
        logger.warning("[MatchupEngine] Arsenal CSV missing player_id or pitch_type columns — skipping")
        return pd.DataFrame(columns=["player_id", "pitch_type", "pitches", "usage_pct",
                                      "whiff_rate", "chase_rate", "barrel_rate", "pitch_family"])

    # Convert player_id to int
    df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce")
    df = df.dropna(subset=["player_id"]).copy()
    df["player_id"] = df["player_id"].astype(int)

    # Pitch count (default 0)
    df["pitches"] = pd.to_numeric(df.get("pitches", 0), errors="coerce").fillna(0).astype(int)

    # Usage — normalize to 0–1 fraction
    if "usage_raw" in df.columns:
        df["usage_pct"] = pd.to_numeric(df["usage_raw"], errors="coerce").fillna(0.0)
        # Convert from percentage if values > 1
        if df["usage_pct"].max() > 1.0:
            df["usage_pct"] /= 100.0
    else:
        # Derive from pitches within player group
        total = df.groupby("player_id")["pitches"].transform("sum")
        df["usage_pct"] = df["pitches"] / total.replace(0, 1)

    # Whiff rate — normalize to 0–1 fraction
    if "whiff_raw" in df.columns:
        df["whiff_rate"] = pd.to_numeric(df["whiff_raw"], errors="coerce").fillna(0.0)
        if df["whiff_rate"].max() > 1.0:
            df["whiff_rate"] /= 100.0
    else:
        df["whiff_rate"] = LEAGUE_AVG_OVERALL["whiff_rate"]

    # Chase rate (optional)
    if "chase_raw" in df.columns:
        df["chase_rate"] = pd.to_numeric(df["chase_raw"], errors="coerce").fillna(0.0)
        if df["chase_rate"].max() > 1.0:
            df["chase_rate"] /= 100.0
    else:
        df["chase_rate"] = 0.0  # will fall back to league average per pitch type

    # Barrel rate (optional)
    if "barrel_raw" in df.columns:
        df["barrel_rate"] = pd.to_numeric(df["barrel_raw"], errors="coerce").fillna(0.0)
        if df["barrel_rate"].max() > 1.0:
            df["barrel_rate"] /= 100.0
    else:
        df["barrel_rate"] = 0.0

    # Pitch family
    df["pitch_family"] = df["pitch_type"].map(lambda x: PITCH_TO_FAMILY.get(str(x).upper(), "other"))

    # Filter to minimum pitch count
    df = df[df["pitches"] >= _MIN_PITCHES].copy()

    return df[["player_id", "pitch_type", "pitches", "usage_pct",
               "whiff_rate", "chase_rate", "barrel_rate", "pitch_family"]].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Per-player arsenal lookup
# ---------------------------------------------------------------------------

def _get_pitcher_rows(pitcher_id: int):
    """Return pitcher-specific rows from the full arsenal DataFrame, or None."""
    df = _get_pitcher_arsenal_df()
    if df is None or df.empty:
        return None
    rows = df[df["player_id"] == pitcher_id]
    return rows if len(rows) > 0 else None


def _get_batter_rows(batter_id: int):
    """Return batter-specific rows from the full vulnerability DataFrame, or None."""
    df = _get_batter_vuln_df()
    if df is None or df.empty:
        return None
    rows = df[df["player_id"] == batter_id]
    return rows if len(rows) > 0 else None


# ---------------------------------------------------------------------------
# Core log-odds matchup scoring (adapted from kekoa-santana)
# ---------------------------------------------------------------------------

def _get_hitter_whiff_with_fallback(
    batter_rows,      # DataFrame rows for this batter, or None
    pitch_type: str,
    league_whiff: float,
) -> tuple[float, float]:
    """
    Look up batter's whiff rate for a pitch type with 3-level fallback.
    Returns (whiff_rate, reliability) where reliability ∈ [0, 1].

    Level 1: direct pitch-type match
    Level 2: pitch-family weighted average
    Level 3: league baseline (reliability = 0.0)
    """
    if batter_rows is None or len(batter_rows) == 0:
        return league_whiff, 0.0

    # Level 1: direct match
    direct = batter_rows[batter_rows["pitch_type"] == pitch_type]
    if len(direct) > 0:
        row = direct.iloc[0]
        swings = row.get("pitches", 0)
        raw_whiff = _safe_float(row.get("whiff_rate"), 0.0)
        if swings > 0 and raw_whiff > 0:
            reliability = min(float(swings), _RELIABILITY_N) / _RELIABILITY_N
            blended = reliability * raw_whiff + (1.0 - reliability) * league_whiff
            return float(blended), reliability

    # Level 2: pitch family fallback
    target_family = PITCH_TO_FAMILY.get(pitch_type.upper())
    if target_family is not None:
        family_rows = batter_rows[batter_rows["pitch_family"] == target_family]
        if len(family_rows) > 0:
            valid = family_rows[
                (family_rows["whiff_rate"] > 0) & (family_rows["pitches"] > 0)
            ]
            if len(valid) > 0:
                total_p = valid["pitches"].sum()
                weighted_whiff = (valid["whiff_rate"] * valid["pitches"]).sum() / total_p
                reliability = min(float(total_p), _RELIABILITY_N) / _RELIABILITY_N * 0.5
                blended = reliability * weighted_whiff + (1.0 - reliability) * league_whiff
                return float(blended), reliability

    # Level 3: league baseline
    return league_whiff, 0.0


def _score_k_matchup_single(pitcher_rows, batter_rows) -> dict[str, float]:
    """
    Score K matchup for one pitcher vs one batter using log-odds additive method.
    Returns {"k_logit_lift": float, "avg_reliability": float, "n_pitch_types": int}
    """
    if pitcher_rows is None or len(pitcher_rows) == 0:
        return {"k_logit_lift": 0.0, "avg_reliability": 0.0, "n_pitch_types": 0}

    # Normalize usage within this pitcher's arsenal
    total_usage = pitcher_rows["usage_pct"].sum()
    if total_usage <= 0:
        return {"k_logit_lift": 0.0, "avg_reliability": 0.0, "n_pitch_types": 0}

    matchup_whiff = 0.0
    baseline_whiff = 0.0
    reliabilities: list[float] = []

    for _, row in pitcher_rows.iterrows():
        pt = str(row["pitch_type"]).upper()
        usage = _safe_float(row["usage_pct"]) / total_usage
        pitcher_whiff = _safe_float(row["whiff_rate"])

        # League baseline for this pitch type
        league_whiff = LEAGUE_AVG_BY_PITCH_TYPE.get(pt, {}).get(
            "whiff_rate", LEAGUE_AVG_OVERALL["whiff_rate"]
        )

        if pitcher_whiff <= 0:
            pitcher_whiff = league_whiff

        # Hitter whiff with fallback chain
        hitter_whiff, reliability = _get_hitter_whiff_with_fallback(
            batter_rows, pt, league_whiff
        )
        reliabilities.append(reliability)

        # Log-odds additive: league + pitcher_delta + hitter_delta
        league_logit = _logit(league_whiff)
        pitcher_delta = _logit(pitcher_whiff) - league_logit
        hitter_delta = _logit(hitter_whiff) - league_logit
        matchup_logit = league_logit + pitcher_delta + hitter_delta
        matchup_whiff_pt = _inv_logit(matchup_logit)

        matchup_whiff += usage * matchup_whiff_pt
        baseline_whiff += usage * pitcher_whiff

    # Lift = how much the matchup adjusts vs the pitcher's pure baseline
    if baseline_whiff <= 0 or matchup_whiff <= 0:
        return {"k_logit_lift": 0.0, "avg_reliability": 0.0, "n_pitch_types": len(pitcher_rows)}

    k_logit_lift = _logit(matchup_whiff) - _logit(baseline_whiff)
    avg_reliability = sum(reliabilities) / len(reliabilities) if reliabilities else 0.0

    # Shrink lift by reliability — low-sample matchups regressed toward 0
    # At reliability 0.0 → lift × 0.0; at reliability 1.0 → lift × 1.0
    k_logit_lift *= avg_reliability

    # Cap
    k_logit_lift = max(-_MAX_LIFT_LOGIT, min(_MAX_LIFT_LOGIT, k_logit_lift))

    return {
        "k_logit_lift": float(k_logit_lift),
        "avg_reliability": float(avg_reliability),
        "n_pitch_types": int(len(pitcher_rows)),
    }


def _score_bb_matchup_single(pitcher_rows, batter_rows) -> dict[str, float]:
    """
    Score BB matchup for one pitcher vs one batter.
    Chase rate inverted: low chase → more walks → positive BB lift.
    Returns {"bb_logit_lift": float, "avg_reliability": float}
    """
    if pitcher_rows is None or len(pitcher_rows) == 0:
        return {"bb_logit_lift": 0.0, "avg_reliability": 0.0}

    total_usage = pitcher_rows["usage_pct"].sum()
    if total_usage <= 0:
        return {"bb_logit_lift": 0.0, "avg_reliability": 0.0}

    matchup_chase = 0.0
    baseline_chase = 0.0
    reliabilities: list[float] = []

    for _, row in pitcher_rows.iterrows():
        pt = str(row["pitch_type"]).upper()
        usage = _safe_float(row["usage_pct"]) / total_usage
        pitcher_chase = _safe_float(row["chase_rate"])

        league_chase = LEAGUE_AVG_BY_PITCH_TYPE.get(pt, {}).get(
            "chase_rate", LEAGUE_AVG_OVERALL["chase_rate"]
        )

        if pitcher_chase <= 0:
            pitcher_chase = league_chase

        # Batter chase fallback — use whiff_rate as chase proxy if chase_rate not in batter data
        hitter_chase = league_chase
        reliability = 0.0
        if batter_rows is not None and len(batter_rows) > 0:
            # Try to use batter's chase_rate; fall back to whiff_rate as proxy
            direct = batter_rows[batter_rows["pitch_type"] == pt]
            if len(direct) > 0:
                _chase = _safe_float(direct.iloc[0].get("chase_rate", 0.0))
                _whiff = _safe_float(direct.iloc[0].get("whiff_rate", 0.0))
                _count = int(direct.iloc[0].get("pitches", 0))
                # If chase_rate is available and nonzero, use it; else use whiff as proxy
                if _chase > 0:
                    reliability = min(float(_count), _RELIABILITY_N) / _RELIABILITY_N
                    hitter_chase = reliability * _chase + (1.0 - reliability) * league_chase
                elif _whiff > 0:
                    # whiff × 0.85 ≈ chase rate correlation (empirical)
                    _chase_proxy = _whiff * 0.85
                    reliability = min(float(_count), _RELIABILITY_N) / _RELIABILITY_N * 0.5
                    hitter_chase = reliability * _chase_proxy + (1.0 - reliability) * league_chase
        reliabilities.append(reliability)

        league_logit = _logit(league_chase)
        pitcher_delta = _logit(pitcher_chase) - league_logit
        hitter_delta = _logit(hitter_chase) - league_logit
        matchup_logit = league_logit + pitcher_delta + hitter_delta
        matchup_chase_pt = _inv_logit(matchup_logit)

        matchup_chase += usage * matchup_chase_pt
        baseline_chase += usage * pitcher_chase

    if baseline_chase <= 0 or matchup_chase <= 0:
        return {"bb_logit_lift": 0.0, "avg_reliability": 0.0}

    # Chase lift INVERTED: lower chase = more walks = positive BB lift
    chase_lift = _logit(matchup_chase) - _logit(baseline_chase)
    bb_logit_lift = -chase_lift  # invert

    avg_reliability = sum(reliabilities) / len(reliabilities) if reliabilities else 0.0
    bb_logit_lift *= avg_reliability
    bb_logit_lift = max(-_MAX_LIFT_LOGIT, min(_MAX_LIFT_LOGIT, bb_logit_lift))

    return {
        "bb_logit_lift": float(bb_logit_lift),
        "avg_reliability": float(avg_reliability),
    }


# ---------------------------------------------------------------------------
# Lineup-level aggregation
# ---------------------------------------------------------------------------

def _aggregate_lineup_lift(
    pitcher_id: int,
    batter_ids: list[int],
    stat: str,  # "k" or "bb"
) -> dict[str, float]:
    """
    Aggregate matchup lift across an opposing lineup.
    Each batter weighted equally (equal PA assumption).
    Returns lift keys + avg_reliability.
    """
    pitcher_rows = _get_pitcher_rows(pitcher_id)

    if pitcher_rows is None:
        # No arsenal data — return zero lift
        lift_key = "k_logit_lift" if stat == "k" else "bb_logit_lift"
        return {lift_key: 0.0, "avg_reliability": 0.0, "n_matched": 0}

    if not batter_ids:
        # No lineup — use pitcher-only signal (vs league-average batter)
        if stat == "k":
            result = _score_k_matchup_single(pitcher_rows, None)
            return {"k_logit_lift": result["k_logit_lift"], "avg_reliability": 0.2, "n_matched": 0}
        else:
            result = _score_bb_matchup_single(pitcher_rows, None)
            return {"bb_logit_lift": result["bb_logit_lift"], "avg_reliability": 0.2, "n_matched": 0}

    lifts: list[float] = []
    reliabilities: list[float] = []
    n_matched = 0

    for bid in batter_ids[:9]:  # cap at 9 batters
        batter_rows = _get_batter_rows(bid)
        if batter_rows is not None:
            n_matched += 1

        if stat == "k":
            result = _score_k_matchup_single(pitcher_rows, batter_rows)
            lifts.append(result["k_logit_lift"])
            reliabilities.append(result["avg_reliability"])
        else:
            result = _score_bb_matchup_single(pitcher_rows, batter_rows)
            lifts.append(result["bb_logit_lift"])
            reliabilities.append(result["avg_reliability"])

    if not lifts:
        lift_key = "k_logit_lift" if stat == "k" else "bb_logit_lift"
        return {lift_key: 0.0, "avg_reliability": 0.0, "n_matched": 0}

    avg_lift = sum(lifts) / len(lifts)
    avg_rel = sum(reliabilities) / len(reliabilities)
    lift_key = "k_logit_lift" if stat == "k" else "bb_logit_lift"

    return {lift_key: avg_lift, "avg_reliability": avg_rel, "n_matched": n_matched}


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def get_matchup_lift(prop: dict) -> dict[str, float]:
    """
    Compute pitch-type matchup lifts for a prop dict.

    Uses: prop["mlbam_id"] or prop["player_id"] as the pitcher MLBAM ID
          prop["_opp_lineup_ids"] (list of batter MLBAM IDs) if available
          prop["_opp_avg_k_pct"] as a fallback batter K signal

    Returns:
        {
            "k_lift_logit":     float — logit lift for K props (±0.30 cap)
            "bb_lift_logit":    float — logit lift for BB/walks_allowed props
            "avg_reliability":  float — data completeness 0.0–1.0
            "n_pitch_types":    int   — number of pitch types in arsenal
            "n_batter_matched": int   — number of lineup batters with vuln data
        }
    Always returns a valid dict. Never raises.
    """
    _EMPTY = {
        "k_lift_logit": 0.0, "bb_lift_logit": 0.0,
        "avg_reliability": 0.0, "n_pitch_types": 0, "n_batter_matched": 0,
    }

    try:
        prop_type = prop.get("prop_type", "")
        pitcher_id = int(prop.get("mlbam_id") or prop.get("player_id") or 0)

        if not pitcher_id:
            return _EMPTY

        # Only compute for relevant prop types
        if prop_type not in _K_PROP_TYPES and prop_type not in _BB_PROP_TYPES:
            return _EMPTY

        today_str = date.today().isoformat()
        cache_key = f"matchup:{pitcher_id}:{today_str}"

        # Redis cache hit
        cached = _redis_get(cache_key)
        if cached:
            return cached

        # Get opposing lineup batter IDs (stamped by lineup_chase_layer or context)
        batter_ids: list[int] = []
        _raw_ids = prop.get("_opp_lineup_ids") or []
        for bid in _raw_ids:
            try:
                batter_ids.append(int(bid))
            except (ValueError, TypeError):
                pass

        # If no explicit batter IDs, try to parse from context lineups
        if not batter_ids:
            opp_team = prop.get("opposing_team", "")
            _lineups = prop.get("_context_lineups", [])
            for entry in _lineups:
                if str(entry.get("team", "")).lower() == str(opp_team).lower():
                    pid = entry.get("player_id")
                    if pid:
                        try:
                            batter_ids.append(int(pid))
                        except (ValueError, TypeError):
                            pass

        # Compute K lift
        k_result = _aggregate_lineup_lift(pitcher_id, batter_ids, "k")
        k_lift = k_result.get("k_logit_lift", 0.0)
        avg_rel = k_result.get("avg_reliability", 0.0)
        n_matched = k_result.get("n_matched", 0)

        # Compute BB lift
        bb_result = _aggregate_lineup_lift(pitcher_id, batter_ids, "bb")
        bb_lift = bb_result.get("bb_logit_lift", 0.0)

        # Pitcher arsenal info
        pitcher_rows = _get_pitcher_rows(pitcher_id)
        n_pitch_types = len(pitcher_rows) if pitcher_rows is not None else 0

        result = {
            "k_lift_logit": round(k_lift, 4),
            "bb_lift_logit": round(bb_lift, 4),
            "avg_reliability": round(avg_rel, 3),
            "n_pitch_types": n_pitch_types,
            "n_batter_matched": n_matched,
        }

        # Cache in Redis
        _redis_set(cache_key, result)

        if n_pitch_types > 0 or n_matched > 0:
            logger.debug(
                "[MatchupEngine] pitcher=%d  k_lift=%.3f  bb_lift=%.3f  "
                "reliability=%.2f  arsenal=%d  batters_matched=%d",
                pitcher_id, k_lift, bb_lift, avg_rel, n_pitch_types, n_matched,
            )

        return result

    except Exception as exc:
        logger.debug("[MatchupEngine] get_matchup_lift failed: %s", exc)
        return _EMPTY


# ---------------------------------------------------------------------------
# Bullpen matchup lift (for _BullpenAgent)
# ---------------------------------------------------------------------------

def get_bullpen_matchup_lift(
    reliever_ids: list[int],
    batter_ids: list[int],
    bf_shares: list[float] | None = None,
) -> dict[str, float]:
    """
    Compute BF-share-weighted bullpen matchup lifts for a team's relievers.

    Parameters
    ----------
    reliever_ids  : list of pitcher MLBAM IDs for relievers
    batter_ids    : list of opposing batter MLBAM IDs
    bf_shares     : BF share per reliever (defaults to equal weighting)

    Returns
    -------
    {"bullpen_k_lift": float, "bullpen_bb_lift": float, "n_relievers": int}
    Always returns a valid dict. Never raises.
    """
    _EMPTY_BP = {"bullpen_k_lift": 0.0, "bullpen_bb_lift": 0.0, "n_relievers": 0}

    try:
        if not reliever_ids:
            return _EMPTY_BP

        if bf_shares is None or len(bf_shares) != len(reliever_ids):
            # Equal weighting
            n = len(reliever_ids)
            bf_shares = [1.0 / n] * n

        k_total = 0.0
        bb_total = 0.0
        weight_sum = 0.0

        for pitcher_id, share in zip(reliever_ids, bf_shares):
            share = float(share)
            if share <= 0:
                continue

            k_res = _aggregate_lineup_lift(pitcher_id, batter_ids, "k")
            bb_res = _aggregate_lineup_lift(pitcher_id, batter_ids, "bb")

            k_total += k_res.get("k_logit_lift", 0.0) * share
            bb_total += bb_res.get("bb_logit_lift", 0.0) * share
            weight_sum += share

        if weight_sum > 0 and abs(weight_sum - 1.0) > 0.01:
            k_total /= weight_sum
            bb_total /= weight_sum

        return {
            "bullpen_k_lift": round(k_total, 4),
            "bullpen_bb_lift": round(bb_total, 4),
            "n_relievers": len(reliever_ids),
        }

    except Exception as exc:
        logger.debug("[MatchupEngine] get_bullpen_matchup_lift failed: %s", exc)
        return _EMPTY_BP


# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------

def get_arsenal_status() -> dict:
    """
    Return the current status of loaded arsenal data.
    Used by bug_checker.py 10 AM health embed.
    """
    pitcher_df = _get_pitcher_arsenal_df()
    batter_df = _get_batter_vuln_df()

    return {
        "pitcher_arsenal_loaded": pitcher_df is not None,
        "pitcher_count": len(pitcher_df["player_id"].unique()) if pitcher_df is not None else 0,
        "batter_vuln_loaded": batter_df is not None,
        "batter_count": len(batter_df["player_id"].unique()) if batter_df is not None else 0,
        "cache_date": _ARSENAL_DATE or "not loaded",
    }
