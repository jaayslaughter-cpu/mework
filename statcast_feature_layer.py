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
# 3b. SHADOW ZONE WHIFF RATE (2026 ABS Meta-Stat)
# ===========================================================================
# The "Shadow Zone" is the region just off the corner of the strike zone
# (Statcast zones 11-14: plate edges ±0.7–1.1 ft, height 1.5–3.5 ft).
# Whiff rate in this zone is the strongest K-rate predictor in the ABS era
# because automated ball-strike doesn't call borderline pitches — pitchers
# must generate genuine swings-and-misses rather than relying on framing.
#
# Source: Baseball Savant /statcast_search/csv with hfZ=Shadow|| filter
# No pybaseball dependency — direct CSV endpoint used.

_SHADOW_CACHE_TTL_HOURS = 24

def fetch_shadow_zone_whiff(pitcher_mlbam_id: int, season: int | None = None) -> float | None:
    """
    Fetch Shadow Zone whiff rate for a pitcher from Baseball Savant pitch-level
    search endpoint.  Returns float (0.0–1.0) or None on error.

    Cached per pitcher per day to avoid repeated hits.
    """
    import datetime, json, hashlib

    season = season or datetime.date.today().year
    cache_key = f"shadow_{pitcher_mlbam_id}_{season}"
    cache_file = CACHE_DIR / f"{cache_key}.json"

    # Check cache
    if cache_file.exists():
        age_h = (time.time() - cache_file.stat().st_mtime) / 3600
        if age_h < _SHADOW_CACHE_TTL_HOURS:
            try:
                data = json.loads(cache_file.read_text())
                return data.get("shadow_whiff_rate")
            except Exception:
                pass

    # Baseball Savant statcast_search CSV — shadow zone (hfZ=Shadow||)
    # Zones 11–14 are the edge zones just outside the defined strike zone
    url = (
        "https://baseballsavant.mlb.com/statcast_search/csv"
        f"?hfPT=&hfZ=Shadow%7C%7C&player_type=pitcher"
        f"&pitchers_lookup%5B%5D={pitcher_mlbam_id}"
        f"&game_date_gt={season}-01-01&game_date_lt={season}-12-31"
        "&hfGT=R%7C&hfC=&hfSea={season}%7C&hfSit=&position=&hfOuts="
        "&opponent=&pitcher_throws=&batter_stands=&hfSA="
        "&player_lookup%5B%5D=&team=&home_road=&game_date_gt="
        "&game_date_lt=&hfFlag=&hfBBL=&metric_1=&hfInn=&min_pitches=0"
        "&min_results=0&group_by=name&sort_col=pitches&player_event_sort=h_launch_speed"
        "&sort_order=desc&min_pas=0&type=details"
    )

    try:
        import requests as _req
        resp = _req.get(url, headers=_SAVANT_HEADERS, timeout=20)
        resp.raise_for_status()
        import io
        df = __import__("pandas").read_csv(io.StringIO(resp.text), low_memory=False)

        if df.empty or "description" not in df.columns:
            return None

        # Shadow whiff = swinging_strike / all swing events in shadow zone
        swing_events = {"swinging_strike", "swinging_strike_blocked", "foul",
                        "foul_tip", "hit_into_play", "hit_into_play_no_out",
                        "hit_into_play_score"}
        total_swings  = df[df["description"].isin(swing_events)]
        whiffs        = df[df["description"].isin({"swinging_strike",
                                                    "swinging_strike_blocked"})]

        if len(total_swings) < 10:   # too small a sample
            return None

        rate = round(len(whiffs) / len(total_swings), 4)

        # Cache result
        cache_file.write_text(json.dumps({
            "pitcher_id":         pitcher_mlbam_id,
            "season":             season,
            "shadow_whiff_rate":  rate,
            "shadow_swings":      len(total_swings),
            "shadow_whiffs":      len(whiffs),
        }))
        return rate

    except Exception as exc:
        logger.debug("[Savant] Shadow zone fetch failed for %s: %s", pitcher_mlbam_id, exc)
        return None



# ===========================================================================
# 5. ZONE INTEGRITY ANALYZER  (Phase 80)
# ===========================================================================
# Statcast attack zone coordinate definitions
# HEART  : |plate_x| < 0.6,  1.8 < plate_z < 3.2   (true meatball)
# SHADOW : |plate_x| < 1.1,  1.2 < plate_z < 3.8   (edge of zone)
# CHASE  : |plate_x| < 1.5,  0.8 < plate_z < 4.2   (just outside)
# WASTE  : everything else

_ZONE_CACHE_DIR = pathlib.Path("/tmp/zone_integrity_cache")


def _classify_zone(plate_x: float, plate_z: float) -> str:
    ax = abs(plate_x)
    if ax < 0.6 and 1.8 < plate_z < 3.2:
        return "HEART"
    if ax < 1.1 and 1.2 < plate_z < 3.8:
        return "SHADOW"
    if ax < 1.5 and 0.8 < plate_z < 4.2:
        return "CHASE"
    return "WASTE"


def analyze_zone_integrity(
    pitcher_mlbam_id: int,
    season: int | None = None,
) -> dict:
    """
    Pull Statcast pitch-level data for *pitcher_mlbam_id* and compute
    per-zone whiff rates (HEART / SHADOW / CHASE / WASTE).

    Returns
    -------
    {
        "zone_rates": {
            "HEART":  {"whiff_rate": float, "whiffs": int, "swings": int},
            "SHADOW": {...},
            "CHASE":  {...},
            "WASTE":  {...},
        },
        "integrity_multiplier": float,   # 0.85 = fraud, 1.10 = elite shadow
        "verdict": str,                  # "FRAUD" | "ELITE_SHADOW" | "NEUTRAL"
        "heart_whiff":  float,
        "shadow_whiff": float,
    }

    Verdict logic (ABS era):
        FRAUD        : heart_whiff > shadow_whiff  → multiplier 0.85
        ELITE_SHADOW : shadow_whiff ≥ 0.35         → multiplier 1.10
        NEUTRAL                                    → multiplier 1.00

    Returns empty dict {} if data is unavailable (safe default).
    """
    import datetime as _dt  # noqa: PLC0415

    if season is None:
        season = _dt.date.today().year

    _ZONE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = _ZONE_CACHE_DIR / f"{pitcher_mlbam_id}_{season}.json"

    # ── Cache hit (daily) ─────────────────────────────────────────────────
    if cache_file.exists():
        try:
            age = _dt.datetime.now().timestamp() - cache_file.stat().st_mtime
            if age < 86400:
                return json.loads(cache_file.read_text())
        except Exception:
            pass

    # ── Fetch from Baseball Savant pitch-level CSV ───────────────────────
    # Same endpoint as shadow whiff; full pitch log with plate_x / plate_z
    start_dt = f"{season}-03-01"
    end_dt   = _dt.date.today().isoformat()
    url = (
        "https://baseballsavant.mlb.com/statcast_search/csv"
        f"?hfSea={season}%7C&player_type=pitcher"
        f"&pitchers_lookup%5B%5D={pitcher_mlbam_id}"
        f"&game_date_gt={start_dt}&game_date_lt={end_dt}"
        "&min_pitches=0&group_by=name&sort_col=pitches&player_event_sort=api_p_release_speed"
        "&sort_order=desc&min_abs=0&type=details&"
    )

    try:
        import io
        import urllib.request

        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8", errors="replace")

        if not raw.strip() or "," not in raw:
            return {}

        import csv as _csv
        reader = _csv.DictReader(io.StringIO(raw))

        # Tally swings and whiffs by zone
        zone_swings: dict[str, int] = {"HEART": 0, "SHADOW": 0, "CHASE": 0, "WASTE": 0}
        zone_whiffs: dict[str, int] = {"HEART": 0, "SHADOW": 0, "CHASE": 0, "WASTE": 0}

        SWING_EVENTS = {
            "swinging_strike", "swinging_strike_blocked",
            "foul", "foul_tip", "hit_into_play",
            "hit_into_play_no_out", "hit_into_play_score",
        }
        WHIFF_EVENTS = {"swinging_strike", "swinging_strike_blocked"}

        for row in reader:
            try:
                px = float(row.get("plate_x") or "")
                pz = float(row.get("plate_z") or "")
            except (ValueError, TypeError):
                continue
            desc = (row.get("description") or "").strip()
            if desc not in SWING_EVENTS:
                continue
            zone = _classify_zone(px, pz)
            zone_swings[zone] += 1
            if desc in WHIFF_EVENTS:
                zone_whiffs[zone] += 1

        # Build per-zone rates
        zone_rates: dict[str, dict] = {}
        for z in ("HEART", "SHADOW", "CHASE", "WASTE"):
            sw = zone_swings[z]
            wh = zone_whiffs[z]
            rate = round(wh / sw, 4) if sw >= 5 else None
            zone_rates[z] = {
                "whiff_rate": rate,
                "whiffs":     wh,
                "swings":     sw,
            }

        heart_r  = zone_rates["HEART"]["whiff_rate"]  or 0.0
        shadow_r = zone_rates["SHADOW"]["whiff_rate"] or 0.0

        # Verdict
        if shadow_r >= 0.35:
            verdict = "ELITE_SHADOW"
            mult    = 1.10
        elif heart_r > shadow_r and zone_rates["HEART"]["swings"] >= 10:
            verdict = "FRAUD"
            mult    = 0.85
        else:
            verdict = "NEUTRAL"
            mult    = 1.00

        result = {
            "zone_rates":             zone_rates,
            "integrity_multiplier":   mult,
            "verdict":                verdict,
            "heart_whiff":            round(heart_r,  4),
            "shadow_whiff":           round(shadow_r, 4),
        }

        cache_file.write_text(json.dumps(result))
        logger.info(
            "[ZoneIntegrity] pitcher=%s  heart=%.3f  shadow=%.3f  verdict=%s  mult=%.2f",
            pitcher_mlbam_id, heart_r, shadow_r, verdict, mult,
        )
        return result

    except Exception as exc:
        logger.debug("[ZoneIntegrity] Failed for pitcher %s: %s", pitcher_mlbam_id, exc)
        return {}


def enrich_pitchers_shadow_whiff(pitcher_props: list[dict]) -> list[dict]:
    """
    Add sc_shadow_whiff_rate to each pitcher prop dict.
    Only called when pitcher has a strikeout prop — high-value signal.
    Returns props list (original items extended in-place, unchanged if fetch fails).

    Threshold interpretation (ABS era):
        ≥ 0.32  → elite shadow whiff (REAL_BREAKOUT signal)
        0.26–0.31 → above average
        ≤ 0.20  → below average (fade K props)
    """
    enriched = []
    for prop in pitcher_props:
        pid = int(prop.get("mlbam_id") or prop.get("player_id") or 0)
        if pid and prop.get("prop_type") in ("strikeouts", "pitcher_strikeouts", "K"):
            rate = fetch_shadow_zone_whiff(pid)
            if rate is not None:
                prop = {**prop, "sc_shadow_whiff_rate": rate}
        enriched.append(prop)
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
