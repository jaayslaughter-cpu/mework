"""
savant_refresh.py
=================
Nightly refresh of Baseball Savant leaderboard data.

Replaces the 10 bundled CSVs in data/statcast/ with live season-to-date data
pulled directly from baseballsavant.mlb.com.  After writing, resets
statcast_static_layer._loaded so the next agent cycle picks up fresh data
without a service restart.

Sources
-------
Six endpoints via pybaseball wrappers (same URLs, cleaner column handling):
  statcast_pitcher_arsenal_stats   → pitch-arsenal-stats-pitchers.csv
  statcast_pitcher_expected_stats  → expected-stats-pitchers.csv
  statcast_batter_expected_stats   → expected_stats.csv
  statcast_batter_exitvelo_barrels → exit_velocity.csv
  statcast_sprint_speed            → sprint_speed.csv
  statcast_batter_percentile_ranks → percentile_rankings.csv

Four endpoints fetched directly (not in pybaseball):
  /leaderboard/bat-tracking        → bat-tracking.csv
  /leaderboard/swing-take          → swing-take.csv
  /leaderboard/batted-ball         → batted-ball.csv
  /leaderboard/baserunning         → baserunning_run_value.csv

Scheduler slot: job_savant_refresh — daily 4:00 AM PT in orchestrator.py.

Public API
----------
refresh(year=None, min_pa=25)   → {"updated": N, "skipped": M, "errors": [...]}
force_reload()                  → resets statcast_static_layer for in-process reload
"""
from __future__ import annotations

import io
import logging
import os
import time
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

_DATA_DIR = Path(__file__).parent / "data" / "statcast"

_SAVANT_BASE = "https://baseballsavant.mlb.com"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/csv,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://baseballsavant.mlb.com/",
}

# Delay between requests to avoid rate limiting
_REQUEST_DELAY_S = 3.0

# Minimum rows to consider a fetch successful (guards against empty/error responses)
_MIN_ROWS = 20


# ── Fetch helpers ─────────────────────────────────────────────────────────────

def _fetch_url(url: str, timeout: int = 60) -> pd.DataFrame | None:
    """GET url, parse CSV response, return DataFrame or None on failure."""
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=timeout)
        if resp.status_code != 200:
            logger.warning("[SavantRefresh] %s → HTTP %d", url[:80], resp.status_code)
            return None
        text = resp.text.strip()
        if not text or text.startswith("<"):
            logger.warning("[SavantRefresh] %s → HTML/empty response", url[:80])
            return None
        df = pd.read_csv(io.StringIO(text))
        # Strip BOM and whitespace from column names
        df.columns = [c.strip().strip('"').strip() for c in df.columns]
        return df
    except Exception as exc:
        logger.warning("[SavantRefresh] %s → %s", url[:80], exc)
        return None


def _fetch_pybaseball(fn_name: str, year: int, min_pa: int) -> pd.DataFrame | None:
    """Call a pybaseball statcast function by name, return DataFrame or None."""
    try:
        import pybaseball  # noqa: PLC0415
        pybaseball.cache.enable()
        fn = getattr(pybaseball, fn_name)

        # Function signatures vary — try common patterns
        try:
            df = fn(year, minPA=min_pa)
        except TypeError:
            try:
                df = fn(year, min_pa)
            except TypeError:
                df = fn(year)

        if df is None or df.empty:
            return None
        df.columns = [c.strip().strip('"').strip() for c in df.columns]
        return df
    except Exception as exc:
        logger.warning("[SavantRefresh] pybaseball.%s(%d) → %s", fn_name, year, exc)
        return None


# ── Column normalisation helpers ───────────────────────────────────────────────

def _rename(df: pd.DataFrame, col_map: dict[str, str]) -> pd.DataFrame:
    """Rename columns that exist, ignore those that don't."""
    existing = {k: v for k, v in col_map.items() if k in df.columns}
    return df.rename(columns=existing)


def _require(df: pd.DataFrame, cols: list[str], source: str) -> bool:
    """Return True if all required columns exist after rename."""
    missing = [c for c in cols if c not in df.columns]
    if missing:
        logger.warning("[SavantRefresh] %s missing cols: %s", source, missing)
        return False
    return True


# ── Individual fetch functions ─────────────────────────────────────────────────

def _fetch_pitcher_arsenal(year: int, min_pa: int) -> pd.DataFrame | None:
    """pitch-arsenal-stats-pitchers.csv"""
    # pybaseball first
    df = _fetch_pybaseball("statcast_pitcher_arsenal_stats", year, min_pa)
    if df is None:
        url = (f"{_SAVANT_BASE}/leaderboard/pitch-arsenal-stats"
               f"?type=pitcher&pitchType=&year={year}&team=&min={min_pa}&csv=true")
        df = _fetch_url(url)
    if df is None or len(df) < _MIN_ROWS:
        return None

    # Normalise id column: pybaseball uses 'player_id', direct uses 'player_id' too
    # Ensure required columns exist
    required = ["player_id", "pitch_type", "pitch_usage", "k_percent", "whiff_percent"]
    if not _require(df, required, "pitcher_arsenal"):
        return None

    # Fill optional columns with 0 if missing
    for col in ["run_value_per_100", "put_away", "hard_hit_percent"]:
        if col not in df.columns:
            df[col] = 0.0

    return df


def _fetch_pitcher_expected(year: int, min_pa: int) -> pd.DataFrame | None:
    """expected-stats-pitchers.csv"""
    df = _fetch_pybaseball("statcast_pitcher_expected_stats", year, min_pa)
    if df is None:
        url = (f"{_SAVANT_BASE}/leaderboard/expected_statistics"
               f"?type=pitcher&year={year}&position=&team=&min={min_pa}&csv=true")
        df = _fetch_url(url)
    if df is None or len(df) < _MIN_ROWS:
        return None
    if not _require(df, ["player_id", "xera"], "pitcher_expected"):
        return None
    return df


def _fetch_batter_expected(year: int, min_pa: int) -> pd.DataFrame | None:
    """expected_stats.csv"""
    df = _fetch_pybaseball("statcast_batter_expected_stats", year, min_pa)
    if df is None:
        url = (f"{_SAVANT_BASE}/leaderboard/expected_statistics"
               f"?type=batter&year={year}&position=&team=&min={min_pa}&csv=true")
        df = _fetch_url(url)
    if df is None or len(df) < _MIN_ROWS:
        return None
    # Column name varies: est_woba or est_woba_minus_woba_diff present
    if "player_id" not in df.columns:
        return None
    # Ensure est_ba, est_woba, est_slg present
    for col in ["est_ba", "est_woba", "est_slg"]:
        if col not in df.columns:
            df[col] = None
    return df


def _fetch_exit_velocity(year: int, min_pa: int) -> pd.DataFrame | None:
    """exit_velocity.csv"""
    df = _fetch_pybaseball("statcast_batter_exitvelo_barrels", year, min_pa)
    if df is None:
        url = (f"{_SAVANT_BASE}/leaderboard/statcast"
               f"?type=batter&year={year}&position=&team=&min={min_pa}&csv=true")
        df = _fetch_url(url)
    if df is None or len(df) < _MIN_ROWS:
        return None
    if "player_id" not in df.columns:
        return None
    for col in ["avg_hit_speed", "ev50", "brl_percent", "max_hit_speed"]:
        if col not in df.columns:
            df[col] = None
    return df


def _fetch_sprint_speed(year: int, min_pa: int) -> pd.DataFrame | None:
    """sprint_speed.csv"""
    df = _fetch_pybaseball("statcast_sprint_speed", year, min_pa)
    if df is None:
        url = (f"{_SAVANT_BASE}/leaderboard/sprint_speed"
               f"?year={year}&position=&team=&min={min_pa}&csv=true")
        df = _fetch_url(url)
    if df is None or len(df) < _MIN_ROWS:
        return None
    if "player_id" not in df.columns:
        return None
    for col in ["sprint_speed", "bolts", "hp_to_1b"]:
        if col not in df.columns:
            df[col] = None
    return df


def _fetch_percentile_ranks(year: int, _min_pa: int) -> pd.DataFrame | None:
    """percentile_rankings.csv"""
    df = _fetch_pybaseball("statcast_batter_percentile_ranks", year, 1)
    if df is None:
        url = (f"{_SAVANT_BASE}/leaderboard/percentile-rankings"
               f"?type=batter&year={year}&position=&team=&csv=true")
        df = _fetch_url(url)
    if df is None or len(df) < _MIN_ROWS:
        return None
    if "player_id" not in df.columns:
        return None
    for col in ["xwoba", "k_percent", "whiff_percent", "chase_percent",
                "exit_velocity", "sprint_speed"]:
        if col not in df.columns:
            df[col] = None
    return df


def _fetch_bat_tracking(year: int, min_pa: int) -> pd.DataFrame | None:
    """bat-tracking.csv — not in pybaseball, direct URL only."""
    url = (f"{_SAVANT_BASE}/leaderboard/bat-tracking"
           f"?type=batter&year={year}&min={min_pa}&csv=true")
    df = _fetch_url(url)
    if df is None or len(df) < _MIN_ROWS:
        return None

    # Savant bat-tracking uses 'player_id' or 'batter' — normalise to 'id'
    # (statcast_static_layer expects 'id' for bat-tracking.csv)
    if "player_id" in df.columns and "id" not in df.columns:
        df = df.rename(columns={"player_id": "id"})
    if "id" not in df.columns:
        logger.warning("[SavantRefresh] bat-tracking has no player_id/id column; cols=%s",
                       list(df.columns)[:10])
        return None

    for col in ["whiff_per_swing", "avg_bat_speed", "hard_swing_rate",
                "blast_per_swing", "swing_length"]:
        if col not in df.columns:
            df[col] = None
    return df


def _fetch_swing_take(year: int, min_pa: int) -> pd.DataFrame | None:
    """swing-take.csv — not in pybaseball."""
    url = (f"{_SAVANT_BASE}/leaderboard/swing-take"
           f"?year={year}&position=&min={min_pa}&csv=true")
    df = _fetch_url(url)
    if df is None or len(df) < _MIN_ROWS:
        return None
    if "player_id" not in df.columns:
        return None
    for col in ["runs_chase", "runs_heart", "runs_waste", "runs_all"]:
        if col not in df.columns:
            df[col] = None
    return df


def _fetch_batted_ball(year: int, min_pa: int) -> pd.DataFrame | None:
    """batted-ball.csv — not in pybaseball.  statcast_static_layer uses 'id' column."""
    url = (f"{_SAVANT_BASE}/leaderboard/batted-ball"
           f"?type=batter&year={year}&min={min_pa}&csv=true")
    df = _fetch_url(url)
    if df is None or len(df) < _MIN_ROWS:
        return None

    if "player_id" in df.columns and "id" not in df.columns:
        df = df.rename(columns={"player_id": "id"})
    if "id" not in df.columns:
        return None

    for col in ["gb_rate", "fb_rate", "ld_rate", "pull_rate"]:
        if col not in df.columns:
            df[col] = None
    return df


def _fetch_baserunning(year: int, _min_pa: int) -> pd.DataFrame | None:
    """baserunning_run_value.csv — not in pybaseball."""
    url = (f"{_SAVANT_BASE}/leaderboard/baserunning"
           f"?type=batter&year={year}&csv=true")
    df = _fetch_url(url)
    if df is None or len(df) < _MIN_ROWS:
        return None
    if "player_id" not in df.columns:
        return None
    if "runner_runs_tot" not in df.columns:
        df[col] = None
    return df


# ── Write CSV ──────────────────────────────────────────────────────────────────

def _write_csv(df: pd.DataFrame, filename: str) -> bool:
    """Write DataFrame to data/statcast/<filename>, keeping original as .bak."""
    path = _DATA_DIR / filename
    backup = _DATA_DIR / (filename + ".bak")

    try:
        # Atomic-ish: write to .tmp then rename
        tmp = _DATA_DIR / (filename + ".tmp")
        df.to_csv(tmp, index=False, encoding="utf-8")

        # Keep a backup of the previous file
        if path.exists():
            path.replace(backup)

        tmp.rename(path)
        logger.info("[SavantRefresh] ✅ Wrote %s: %d rows", filename, len(df))
        return True
    except Exception as exc:
        logger.error("[SavantRefresh] Failed to write %s: %s", filename, exc)
        return False


# ── Force reload of statcast_static_layer ─────────────────────────────────────

def force_reload() -> None:
    """
    Reset statcast_static_layer's in-process loaded flag.
    The next call to any get_* function will re-read all CSVs from disk.
    """
    try:
        import statcast_static_layer as _ssl  # noqa: PLC0415
        with _ssl._load_lock:
            _ssl._loaded = False
            # Clear all in-memory stores
            _ssl._pitcher_k_rate.clear()
            _ssl._pitcher_whiff.clear()
            _ssl._pitcher_xera.clear()
            _ssl._pitcher_arsenal.clear()
            _ssl._batter_tracking.clear()
            _ssl._batter_ev.clear()
            _ssl._batter_xstats.clear()
            _ssl._batter_discipline.clear()
            _ssl._batter_batted.clear()
            _ssl._batter_percentiles.clear()
            _ssl._batter_fg_proj.clear()
            _ssl._sprint_speed_data.clear()
            _ssl._baserunning_data.clear()
        logger.info("[SavantRefresh] statcast_static_layer reset — will reload from disk")
    except Exception as exc:
        logger.warning("[SavantRefresh] Could not reset statcast_static_layer: %s", exc)


# ── Main refresh ───────────────────────────────────────────────────────────────

# Each entry: (filename, fetch_function, description)
_REFRESHES: list[tuple[str, Any, str]] = [
    ("pitch-arsenal-stats-pitchers.csv", _fetch_pitcher_arsenal,  "pitcher arsenal"),
    ("expected-stats-pitchers.csv",      _fetch_pitcher_expected, "pitcher xERA"),
    ("expected_stats.csv",               _fetch_batter_expected,  "batter xStats"),
    ("exit_velocity.csv",                _fetch_exit_velocity,    "batter EV/barrels"),
    ("sprint_speed.csv",                 _fetch_sprint_speed,     "sprint speed"),
    ("percentile_rankings.csv",          _fetch_percentile_ranks, "batter percentiles"),
    ("bat-tracking.csv",                 _fetch_bat_tracking,     "bat tracking"),
    ("swing-take.csv",                   _fetch_swing_take,       "swing-take"),
    ("batted-ball.csv",                  _fetch_batted_ball,      "batted ball"),
    ("baserunning_run_value.csv",        _fetch_baserunning,      "baserunning"),
]


def refresh(year: int | None = None, min_pa: int = 25) -> dict:
    """
    Refresh all Savant CSVs for the given season.

    Args:
        year:   Season year (defaults to current year).
        min_pa: Minimum plate appearances to filter players.

    Returns:
        {"updated": N, "skipped": M, "errors": [list of filenames that failed],
         "year": year, "date": "YYYY-MM-DD"}
    """
    if year is None:
        year = date.today().year

    _DATA_DIR.mkdir(parents=True, exist_ok=True)

    updated  = 0
    skipped  = 0
    errors: list[str] = []

    logger.info(
        "[SavantRefresh] Starting %d-season refresh (min_pa=%d, %d endpoints)",
        year, min_pa, len(_REFRESHES),
    )

    for filename, fetch_fn, desc in _REFRESHES:
        logger.info("[SavantRefresh] Fetching %s…", desc)
        try:
            df = fetch_fn(year, min_pa)
            time.sleep(_REQUEST_DELAY_S)   # rate-limit courtesy delay

            if df is None or len(df) < _MIN_ROWS:
                logger.warning(
                    "[SavantRefresh] ⚠️  %s returned %s rows — keeping existing file",
                    filename, len(df) if df is not None else 0,
                )
                skipped += 1
                errors.append(filename)
                continue

            if _write_csv(df, filename):
                updated += 1
            else:
                skipped += 1
                errors.append(filename)

        except Exception as exc:
            logger.error("[SavantRefresh] ❌ %s failed: %s", filename, exc)
            skipped += 1
            errors.append(filename)
            time.sleep(_REQUEST_DELAY_S)

    # Trigger in-process reload so agents pick up fresh data immediately
    if updated > 0:
        force_reload()

    result = {
        "year":    year,
        "date":    date.today().isoformat(),
        "updated": updated,
        "skipped": skipped,
        "errors":  errors,
    }
    logger.info(
        "[SavantRefresh] Done: %d updated, %d skipped%s",
        updated, skipped,
        f" (errors: {errors})" if errors else "",
    )
    return result


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys, json
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    year   = int(sys.argv[1]) if len(sys.argv) > 1 else None
    min_pa = int(sys.argv[2]) if len(sys.argv) > 2 else 25

    result = refresh(year=year, min_pa=min_pa)
    print(f"\nSavantRefresh result:\n{json.dumps(result, indent=2)}")
