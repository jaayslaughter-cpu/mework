"""
rolling_window_layer.py
========================
PropIQ — Per-player rolling 10-15 game stats from MLB Stats API.

Captures hot/cold streaks that seasonal averages miss.
No calibration dependency — purely data enrichment.

Stamped fields (batters):
  rolling_hits_per_game, rolling_tb_per_game, rolling_rbi_per_game,
  rolling_runs_per_game, rolling_hrbi_per_game,
  rolling_k_pct, rolling_bb_pct,
  rolling_hits_std, rolling_tb_std,
  rolling_hits_trend, rolling_tb_trend,  (HOT / FLAT / COLD)
  rolling_n, _rolling_adj

Stamped fields (pitchers):
  rolling_k_per_start, rolling_er_per_start,
  rolling_era, rolling_whip, rolling_ip_per_start,
  rolling_k_std,
  rolling_k_trend, rolling_era_trend,   (HOT / FLAT / COLD)
  rolling_n, _rolling_adj

INTEGRATION
-----------
In prop_enrichment_layer.py, inside the enrich_props() loop
(after FanGraphs stats are stamped), add:

    try:
        from rolling_window_layer import enrich_prop_with_rolling as _rw
        prop = _rw(prop, season=season)
    except Exception as _rw_err:
        logger.debug("[Enrichment] Rolling window skipped: %s", _rw_err)
"""

from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger("propiq.rolling_window")

# ── Constants ────────────────────────────────────────────────────────────────
_ROLLING_WINDOW = 15        # games to look back (default)
_TREND_WINDOW   = 5         # games for short vs prior comparison
_MIN_GAMES      = 5         # minimum games before stats are usable

# ── Session-level cache — one fetch per player per cycle ────────────────────
_ROLLING_CACHE: dict[str, dict] = {}

_PITCHER_PROP_TYPES = {
    "strikeouts", "pitcher_strikeouts", "pitching_outs",
    "earned_runs", "hits_allowed", "fantasy_pitcher",
}

# ── MLB Stats API helpers ────────────────────────────────────────────────────

def _mlbapi_game_log(player_id: int,
                     group: str = "hitting",
                     season: int | None = None) -> list[dict]:
    """Fetch per-game stat splits from statsapi.mlb.com.
    Returns list of {"date": str, "stat": dict}, sorted oldest-first.
    """
    import requests
    import datetime
    if season is None:
        season = datetime.date.today().year
    try:
        resp = requests.get(
            f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats",
            params={
                "stats":     "gameLog",
                "group":     group,
                "season":    str(season),
                "gameType":  "R",     # Regular season only
            },
            timeout=10,
        )
        if resp.status_code != 200:
            return []
        games: list[dict] = []
        for sg in resp.json().get("stats", []):
            for split in sg.get("splits", []):
                stat = split.get("stat", {})
                date = split.get("date", "")
                games.append({"date": date, "stat": stat})
        games.sort(key=lambda x: x.get("date", ""))
        return games
    except Exception as exc:
        logger.debug("[Rolling] Game log fetch failed for player %s: %s", player_id, exc)
        return []


# ── Stat computation helpers ─────────────────────────────────────────────────

def _avg(lst: list) -> float:
    return sum(lst) / len(lst) if lst else 0.0


def _std(lst: list) -> float:
    if len(lst) < 2:
        return 0.0
    m = _avg(lst)
    return (sum((x - m) ** 2 for x in lst) / (len(lst) - 1)) ** 0.5


def _safe_float(d: dict, key: str) -> float:
    try:
        return float(d.get(key) or 0)
    except Exception:
        return 0.0


def _trend(lst: list) -> str:
    """HOT if last TREND_WINDOW avg is >15% above prior window; COLD if >15% below."""
    if len(lst) < _TREND_WINDOW * 2:
        return "FLAT"
    recent_avg = _avg(lst[-_TREND_WINDOW:])
    prior_avg  = _avg(lst[-_TREND_WINDOW * 2: -_TREND_WINDOW])
    if prior_avg <= 0:
        return "FLAT"
    pct = (recent_avg - prior_avg) / prior_avg
    if pct > 0.15:
        return "HOT"
    if pct < -0.15:
        return "COLD"
    return "FLAT"


# ── Batter rolling ───────────────────────────────────────────────────────────

def _compute_batter_rolling(games: list[dict],
                             window: int = _ROLLING_WINDOW) -> dict:
    recent = games[-window:] if len(games) >= window else games
    if len(recent) < _MIN_GAMES:
        return {}

    sf = _safe_float
    hits_list  = [sf(g["stat"], "hits")              for g in recent]
    tb_list    = [sf(g["stat"], "totalBases")         for g in recent]
    rbi_list   = [sf(g["stat"], "rbi")                for g in recent]
    runs_list  = [sf(g["stat"], "runs")               for g in recent]
    so_list    = [sf(g["stat"], "strikeOuts")          for g in recent]
    bb_list    = [sf(g["stat"], "baseOnBalls")         for g in recent]
    pa_list    = [max(sf(g["stat"], "plateAppearances") or sf(g["stat"], "atBats"), 1)
                  for g in recent]

    hrbi_list = [h + r + rbi for h, r, rbi in zip(hits_list, runs_list, rbi_list)]

    return {
        "rolling_hits_per_game":  round(_avg(hits_list),  3),
        "rolling_tb_per_game":    round(_avg(tb_list),    3),
        "rolling_rbi_per_game":   round(_avg(rbi_list),   3),
        "rolling_runs_per_game":  round(_avg(runs_list),  3),
        "rolling_hrbi_per_game":  round(_avg(hrbi_list),  3),
        "rolling_k_pct":          round(_avg([s / p for s, p in zip(so_list, pa_list)]), 4),
        "rolling_bb_pct":         round(_avg([b / p for b, p in zip(bb_list, pa_list)]), 4),
        "rolling_hits_std":       round(_std(hits_list),  3),
        "rolling_tb_std":         round(_std(tb_list),    3),
        "rolling_hits_trend":     _trend(hits_list),
        "rolling_tb_trend":       _trend(tb_list),
        "rolling_n":              len(recent),
        "_rolling_source":        "mlbapi_gamelog",
    }


# ── Pitcher rolling ──────────────────────────────────────────────────────────

def _compute_pitcher_rolling(games: list[dict],
                              window: int = _ROLLING_WINDOW) -> dict:
    recent = games[-window:] if len(games) >= window else games
    if len(recent) < _MIN_GAMES:
        return {}

    sf = _safe_float
    so_list  = [sf(g["stat"], "strikeOuts")      for g in recent]
    er_list  = [sf(g["stat"], "earnedRuns")       for g in recent]
    ip_list  = [max(sf(g["stat"], "inningsPitched"), 0.1) for g in recent]
    h_list   = [sf(g["stat"], "hits")             for g in recent]
    bb_list  = [sf(g["stat"], "baseOnBalls")      for g in recent]

    total_er  = sum(er_list)
    total_ip  = max(sum(ip_list), 0.1)
    total_h   = sum(h_list)
    total_bb  = sum(bb_list)

    return {
        "rolling_k_per_start":   round(_avg(so_list),  2),
        "rolling_er_per_start":  round(_avg(er_list),  2),
        "rolling_era":           round(total_er / total_ip * 9, 2),
        "rolling_whip":          round((total_h + total_bb) / total_ip, 3),
        "rolling_ip_per_start":  round(_avg(ip_list),  2),
        "rolling_k_std":         round(_std(so_list),  2),
        "rolling_k_trend":       _trend(so_list),
        "rolling_era_trend":     _trend(er_list),   # HOT = more ER recently (worse for pitcher)
        "rolling_n":             len(recent),
        "_rolling_source":       "mlbapi_gamelog",
    }


# ── Public API ───────────────────────────────────────────────────────────────

def get_batter_rolling(player_id: int, season: int | None = None) -> dict:
    """Rolling window stats for a batter. Cached per session."""
    key = f"batter_{player_id}_{season}"
    if key not in _ROLLING_CACHE:
        games = _mlbapi_game_log(player_id, group="hitting", season=season)
        _ROLLING_CACHE[key] = _compute_batter_rolling(games)
    return _ROLLING_CACHE[key]


def get_pitcher_rolling(player_id: int, season: int | None = None) -> dict:
    """Rolling window stats for a pitcher. Cached per session."""
    key = f"pitcher_{player_id}_{season}"
    if key not in _ROLLING_CACHE:
        games = _mlbapi_game_log(player_id, group="pitching", season=season)
        _ROLLING_CACHE[key] = _compute_pitcher_rolling(games)
    return _ROLLING_CACHE[key]


def enrich_prop_with_rolling(prop: dict, season: int | None = None) -> dict:
    """
    Stamp rolling window stats onto a prop dict in-place.

    Adds:
      - Per-stat rolling averages (rolling_hits_per_game, etc.)
      - Trend signal (HOT / FLAT / COLD)
      - _rolling_adj: probability nudge in percentage-point units
           HOT  → +3pp
           COLD → -3pp
           FLAT →  0pp
      - _rolling_n: number of games in the window

    Gracefully no-ops if player_id is missing or API is unreachable.
    """
    import datetime
    if season is None:
        season = datetime.date.today().year

    player_id = prop.get("player_id") or prop.get("mlbam_id")
    if not player_id:
        return prop

    prop_type  = str(prop.get("prop_type", "")).lower()
    is_pitcher = prop_type in _PITCHER_PROP_TYPES

    try:
        if is_pitcher:
            rolling = get_pitcher_rolling(int(player_id), season)
            if not rolling:
                return prop
            # Stamp all non-private keys onto the prop
            for k, v in rolling.items():
                if not k.startswith("_"):
                    prop[k] = v
            prop["_rolling_n"] = rolling.get("rolling_n", 0)
            # Compute nudge
            if prop_type == "strikeouts":
                trend = rolling.get("rolling_k_trend", "FLAT")
                adj   = 0.03 if trend == "HOT" else (-0.03 if trend == "COLD" else 0.0)
            elif prop_type == "earned_runs":
                trend = rolling.get("rolling_era_trend", "FLAT")
                # HOT era_trend means more ER recently → Over is more likely
                adj   = 0.025 if trend == "HOT" else (-0.025 if trend == "COLD" else 0.0)
            elif prop_type in ("pitching_outs", "hits_allowed"):
                trend = rolling.get("rolling_k_trend", "FLAT")   # K trend proxies performance
                adj   = 0.02 if trend == "HOT" else (-0.02 if trend == "COLD" else 0.0)
            else:
                adj = 0.0
            prop["_rolling_adj"] = round(adj, 4)
            logger.debug(
                "[Rolling] %s (P) | K/start=%.1f ERA=%.2f | K-trend=%s | adj=%+.3f | n=%d",
                prop.get("player", "?"),
                rolling.get("rolling_k_per_start", 0),
                rolling.get("rolling_era",         4.08),
                rolling.get("rolling_k_trend",     "FLAT"),
                adj,
                rolling.get("rolling_n", 0),
            )
        else:
            rolling = get_batter_rolling(int(player_id), season)
            if not rolling:
                return prop
            for k, v in rolling.items():
                if not k.startswith("_"):
                    prop[k] = v
            prop["_rolling_n"] = rolling.get("rolling_n", 0)
            # Compute nudge
            if prop_type in ("hits", "hits_runs_rbis", "fantasy_hitter"):
                trend = rolling.get("rolling_hits_trend", "FLAT")
                adj   = 0.03 if trend == "HOT" else (-0.03 if trend == "COLD" else 0.0)
            elif prop_type == "total_bases":
                trend = rolling.get("rolling_tb_trend", "FLAT")
                adj   = 0.03 if trend == "HOT" else (-0.03 if trend == "COLD" else 0.0)
            elif prop_type in ("rbis", "rbi", "runs"):
                trend = rolling.get("rolling_hits_trend", "FLAT")   # hits trend proxies run production
                adj   = 0.02 if trend == "HOT" else (-0.02 if trend == "COLD" else 0.0)
            else:
                adj = 0.0
            prop["_rolling_adj"] = round(adj, 4)
            logger.debug(
                "[Rolling] %s (B) | H/G=%.2f TB/G=%.2f | H-trend=%s | adj=%+.3f | n=%d",
                prop.get("player", "?"),
                rolling.get("rolling_hits_per_game", 0),
                rolling.get("rolling_tb_per_game",   0),
                rolling.get("rolling_hits_trend",    "FLAT"),
                adj,
                rolling.get("rolling_n", 0),
            )
    except Exception as exc:
        logger.debug("[Rolling] Enrich failed for %s: %s", prop.get("player", "?"), exc)

    return prop
