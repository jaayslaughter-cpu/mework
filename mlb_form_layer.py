"""
mlb_form_layer.py
=================
Hot/cold streak enrichment layer using the free MLB Stats API game logs.

Compares each player's last-7-game rolling average against their full prior
season average to produce a probability adjustment (positive = hot, negative
= cold) that is applied in live_dispatcher._evaluate_props().

No API key required — statsapi.mlb.com is the official MLB data service.

Adjustment tiers
----------------
    ratio ≥ 1.20  →  +0.035  (very hot  — 20 %+ above season avg)
    ratio ≥ 1.10  →  +0.020  (hot       — 10–20 % above)
    ratio ≥ 0.90  →   0.000  (neutral   — within ±10 %)
    ratio ≥ 0.80  →  -0.020  (cold      — 10–20 % below)
    ratio <  0.80 →  -0.035  (very cold — 20 %+ below)

Usage (in live_dispatcher.py)
------------------------------
    from mlb_form_layer import form_layer as _form_layer

    # Before the evaluate loop — pre-fetch once for all players
    _form_layer.prefetch_form_data({raw["player_name"] for raw in raw_props})

    # Inside the evaluate loop — per-leg adjustment
    adj  = _form_layer.get_form_adjustment(player_name, prop_type)
    prob = min(0.80, max(0.40, prob + adj))
"""

from __future__ import annotations

import difflib
import logging
import time
from datetime import datetime

import requests

logger = logging.getLogger("propiq.form")


# ── Postgres helpers (reads from live_batting_logs / live_pitching_logs) ──────
def _get_pg():
    """Return a psycopg2 connection or None."""
    import os
    try:
        import psycopg2
        db_url = os.environ.get("DATABASE_URL", "")
        if not db_url:
            return None
        return psycopg2.connect(db_url)
    except Exception:
        return None

# Mapping: API stat key → (table_col_expr_for_batting, table_col_expr_for_pitching)
# None means not available in that table.
_DB_STAT_MAP = {
    "hits":        ("(h_1b + h_2b + h_3b + home_runs)", None),
    "rbi":         ("b_rbi",                            None),
    "runs":        ("b_runs",                           None),
    "totalBases":  ("(h_1b + 2*h_2b + 3*h_3b + 4*home_runs)", None),
    "strikeOuts":  ("b_k",                              "strikeouts"),
    "earnedRuns":  (None,                               "earnedruns"),
}


# ── Trend significance gates (from sequencebaseball/cogs/trends.py) ───────────
try:
    from fix_trend_significance import gate_form_adjustment, is_significant_trend
    _TREND_GATES_AVAILABLE = True
except ImportError:
    _TREND_GATES_AVAILABLE = False
    def gate_form_adjustment(metric, delta, n, adj_value, player_type="pitcher"):
        return adj_value  # no-op fallback — all adjustments pass through
    def is_significant_trend(metric, delta, n, player_type="pitcher"):
        return abs(delta) > 0  # no-op fallback — everything is "significant"

_MLBAPI_BASE = "https://statsapi.mlb.com/api/v1"
_HEADERS = {"User-Agent": "Mozilla/5.0 (PropIQ/1.0)"}

# ---------------------------------------------------------------------------
# Adjustment tier thresholds
# ---------------------------------------------------------------------------
# List of (min_ratio, adjustment) — first match wins.
_FORM_TIERS: list[tuple[float, float]] = [
    (1.20, +0.035),
    (1.10, +0.020),
    (0.90,  0.000),
    (0.80, -0.020),
    (0.00, -0.035),
]

# How many recent games constitute "recent form"
_HITTER_WINDOW  = 7
_PITCHER_WINDOW = 5   # starters appear less frequently

# Minimum games required in the window — fewer than this → neutral (0.0)
_MIN_GAME_THRESHOLD = 3

# ---------------------------------------------------------------------------
# Prop type → MLB Stats API stat keys + player group
# ---------------------------------------------------------------------------
# "keys" are camelCase MLB Stats API field names from the game log stat dict.
_PROP_STAT_MAP: dict[str, dict] = {
    "hits":            {"group": "hitting",  "keys": ["hits"]},
    "rbis":            {"group": "hitting",  "keys": ["rbi"]},
    "runs":            {"group": "hitting",  "keys": ["runs"]},
    "total_bases":     {"group": "hitting",  "keys": ["totalBases"]},
    "hits_runs_rbis":  {"group": "hitting",  "keys": ["hits", "runs", "rbi"]},
    "fantasy_hitter":  {"group": "hitting",  "keys": ["hits", "totalBases", "rbi", "runs"]},
    "strikeouts":      {"group": "pitching", "keys": ["strikeOuts"]},
    "earned_runs":     {"group": "pitching", "keys": ["earnedRuns"]},
    "fantasy_pitcher": {"group": "pitching", "keys": ["strikeOuts"]},
}


def _ratio_to_adjustment(ratio: float) -> float:
    """Convert rolling/season ratio to a probability adjustment float."""
    for threshold, adjustment in _FORM_TIERS:
        if ratio >= threshold:
            return adjustment
    return _FORM_TIERS[-1][1]  # fallback


class MLBFormLayer:
    """
    Fetches and caches player hot/cold form data from the MLB Stats API.

    Designed for single-session use (one dispatcher run per day).
    All API results are cached in memory — no disk I/O.
    All errors are caught and degraded gracefully to 0.0 adjustments.
    """

    def __init__(self) -> None:
        # name_lower → player_id
        self._roster: dict[str, int] = {}
        self._roster_loaded = False
        # player_id → {prop_type: float adjustment}
        self._form_cache: dict[int, dict[str, float]] = {}

    # ------------------------------------------------------------------
    # Roster / name resolution
    # ------------------------------------------------------------------

    def _load_roster(self) -> None:
        """Fetch all active MLB players for the current season and build name→ID map."""
        if self._roster_loaded:
            return
        season = str(datetime.now().year)
        try:
            resp = requests.get(
                f"{_MLBAPI_BASE}/sports/1/players",
                params={"season": season, "gameType": "R"},
                headers=_HEADERS,
                timeout=20,
            )
            if resp.status_code != 200:
                logger.warning("[Form] Roster HTTP %d — will retry next call", resp.status_code)
                return
            for p in resp.json().get("people", []):
                pid  = p.get("id")
                name = (p.get("fullName") or "").strip()
                if pid and name:
                    self._roster[name.lower()] = pid
            logger.info("[Form] Roster loaded — %d players", len(self._roster))
            self._roster_loaded = True  # only mark done on genuine success
        except Exception as exc:
            logger.warning("[Form] Roster load error: %s — will retry next call", exc)

    def _resolve_player_id(self, player_name: str) -> int | None:
        """Return MLB Stats API player ID for a player name (exact then fuzzy)."""
        self._load_roster()
        key = player_name.lower().strip()

        # 1. Exact match
        if key in self._roster:
            return self._roster[key]

        # 2. Fuzzy match (≥ 85 % similarity)
        matches = difflib.get_close_matches(key, self._roster.keys(), n=1, cutoff=0.85)
        if matches:
            logger.debug("[Form] Fuzzy '%s' → '%s'", player_name, matches[0])
            return self._roster[matches[0]]

        return None

    # ------------------------------------------------------------------
    # API fetchers
    # ------------------------------------------------------------------


    # ------------------------------------------------------------------
    # DB-primary fetchers — read from nightly game_logs_refresh tables
    # ------------------------------------------------------------------

    @staticmethod
    def _fetch_game_log_from_db(
        player_id: int, group: str, window: int
    ) -> list[dict] | None:
        """
        Query live_batting_logs / live_pitching_logs for recent game rows.
        Returns list of synthetic "split" dicts (same shape _compute_form expects)
        or None if the table is empty / player not found.
        """
        conn = _get_pg()
        if conn is None:
            return None
        table  = "live_batting_logs"  if group == "hitting" else "live_pitching_logs"
        try:
            with conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT game_date,
                           h_1b, h_2b, h_3b, home_runs,
                           b_rbi, b_runs, b_k,
                           COALESCE(strikeouts, 0)   AS strikeouts,
                           COALESCE(earnedruns, 0)   AS earnedruns,
                           COALESCE(outs, 0)         AS outs_pitched
                    FROM   {table}
                    WHERE  mlbam_id = %s
                    ORDER  BY game_date DESC
                    LIMIT  %s
                    """,
                    (player_id, window),
                ) if table == "live_batting_logs" else cur.execute(
                    f"""
                    SELECT game_date,
                           0 AS h_1b, 0 AS h_2b, 0 AS h_3b, 0 AS home_runs,
                           0 AS b_rbi, 0 AS b_runs, 0 AS b_k,
                           COALESCE(strikeouts, 0)  AS strikeouts,
                           COALESCE(earnedruns, 0)  AS earnedruns,
                           COALESCE(outs, 0)        AS outs_pitched
                    FROM   {table}
                    WHERE  mlbam_id = %s
                    ORDER  BY game_date DESC
                    LIMIT  %s
                    """,
                    (player_id, window),
                )
                rows = cur.fetchall()
        except Exception as exc:
            logger.debug("[Form][DB] game log query failed: %s", exc)
            return None
        finally:
            conn.close()

        if not rows:
            return None

        def _row_to_stat(r) -> dict:
            hits = (r[1] or 0) + (r[2] or 0) + (r[3] or 0) + (r[4] or 0)
            tb   = (r[1] or 0) + 2*(r[2] or 0) + 3*(r[3] or 0) + 4*(r[4] or 0)
            return {
                "hits":       hits,
                "rbi":        r[5] or 0,
                "runs":       r[6] or 0,
                "totalBases": tb,
                "strikeOuts": r[7] if group == "hitting" else r[8],
                "earnedRuns": r[9] or 0,
            }

        # Return synthetic split dicts
        return [{"stat": _row_to_stat(r)} for r in rows]

    @staticmethod
    def _fetch_season_per_game_from_db(
        player_id: int, group: str
    ) -> dict[str, float] | None:
        """
        Compute season-average-per-game from DB for baseline comparison.
        Returns {api_stat_key: per_game_avg} or None if insufficient data.
        """
        conn = _get_pg()
        if conn is None:
            return None
        table = "live_batting_logs" if group == "hitting" else "live_pitching_logs"
        try:
            with conn, conn.cursor() as cur:
                if table == "live_batting_logs":
                    cur.execute(
                        """
                        SELECT COUNT(*),
                               SUM(h_1b+h_2b+h_3b+home_runs),
                               SUM(b_rbi), SUM(b_runs),
                               SUM(h_1b + 2*h_2b + 3*h_3b + 4*home_runs),
                               SUM(b_k)
                        FROM live_batting_logs
                        WHERE mlbam_id = %s AND game_date >= '2026-03-01'
                        """,
                        (player_id,),
                    )
                else:
                    cur.execute(
                        """
                        SELECT COUNT(*),
                               0, 0, 0, 0,
                               SUM(strikeouts), SUM(earnedruns)
                        FROM live_pitching_logs
                        WHERE mlbam_id = %s AND game_date >= '2026-03-01'
                        """,
                        (player_id,),
                    )
                row = cur.fetchone()
        except Exception as exc:
            logger.debug("[Form][DB] season avg query failed: %s", exc)
            return None
        finally:
            conn.close()

        if not row or not row[0] or row[0] < 3:
            return None

        gp = row[0]
        if group == "hitting":
            return {
                "hits":       (row[1] or 0) / gp,
                "rbi":        (row[2] or 0) / gp,
                "runs":       (row[3] or 0) / gp,
                "totalBases": (row[4] or 0) / gp,
                "strikeOuts": (row[5] or 0) / gp,
            }
        else:
            return {
                "strikeOuts": (row[5] or 0) / gp,
                "earnedRuns": (row[6] or 0) / gp,
            }

    @staticmethod
    def _fetch_game_log(
        player_id: int, group: str, window: int
    ) -> list[dict]:
        """
        Fetch the last `window` game log splits for a player.

        Tries the current calendar year first; falls back to the prior year
        if fewer than _MIN_GAME_THRESHOLD games are found (handles early season).
        """
        current_year = datetime.now().year
        for season in (current_year, current_year - 1):
            try:
                resp = requests.get(
                    f"{_MLBAPI_BASE}/people/{player_id}/stats",
                    params={
                        "stats":  "gameLog",
                        "group":  group,
                        "season": str(season),
                        "limit":  window,
                    },
                    headers=_HEADERS,
                    timeout=10,
                )
                if resp.status_code != 200:
                    continue
                splits: list[dict] = []
                for stat_group in resp.json().get("stats", []):
                    splits.extend(stat_group.get("splits", []))
                recent = splits[-window:]
                if len(recent) >= _MIN_GAME_THRESHOLD:
                    return recent
            except Exception as exc:
                logger.debug("[Form] Game log p%d/%s/%d: %s", player_id, group, season, exc)
        return []

    @staticmethod
    def _fetch_season_per_game(
        player_id: int, group: str
    ) -> dict[str, float]:
        """
        Fetch season totals for a player and return per-game averages.

        Uses prior year as baseline (more complete data than current season).
        Returns {api_stat_key: per_game_float}.
        """
        baseline_season = str(datetime.now().year - 1)
        try:
            resp = requests.get(
                f"{_MLBAPI_BASE}/people/{player_id}/stats",
                params={
                    "stats":  "season",
                    "group":  group,
                    "season": baseline_season,
                },
                headers=_HEADERS,
                timeout=10,
            )
            if resp.status_code != 200:
                return {}
            for stat_group in resp.json().get("stats", []):
                splits = stat_group.get("splits", [])
                if not splits:
                    continue
                s  = splits[0].get("stat", {})
                gp = max(int(s.get("gamesPlayed",  1) or 1), 1)
                gs = max(int(s.get("gamesStarted", 0) or 0), 0)
                # Pitchers: per start (if any); otherwise per game
                denom = gs if (group == "pitching" and gs > 0) else gp
                return {k: float(v) / denom for k, v in s.items()
                        if isinstance(v, (int, float))}
        except Exception as exc:
            logger.debug("[Form] Season stats p%d/%s: %s", player_id, group, exc)
        return {}

    # ------------------------------------------------------------------
    # Form computation
    # ------------------------------------------------------------------

    def _compute_form(self, player_id: int) -> dict[str, float]:
        """
        Compute form probability adjustments for all relevant prop types.

        Returns {prop_type: float_adjustment}.
        """
        adjustments: dict[str, float] = {}

        # Determine which stat groups are needed
        groups_needed: set[str] = {cfg["group"] for cfg in _PROP_STAT_MAP.values()}

        for group in groups_needed:
            window        = _PITCHER_WINDOW if group == "pitching" else _HITTER_WINDOW

            # DB-first: read from nightly game_logs_refresh tables (fast, no API quota)
            recent_splits = self._fetch_game_log_from_db(player_id, group, window)
            season_pg     = self._fetch_season_per_game_from_db(player_id, group)

            # API fallback if DB tables are empty / player not yet seeded
            if recent_splits is None:
                logger.debug("[Form] DB miss for pid=%d group=%s — falling back to live API", player_id, group)
                recent_splits = self._fetch_game_log(player_id, group, window)
            if season_pg is None:
                season_pg = self._fetch_season_per_game(player_id, group)

            if not recent_splits or not season_pg:
                continue

            for prop_type, cfg in _PROP_STAT_MAP.items():
                if cfg["group"] != group:
                    continue

                keys = cfg["keys"]

                # Rolling average across recent games
                game_totals = [
                    sum(float(split.get("stat", {}).get(k, 0) or 0) for k in keys)
                    for split in recent_splits
                ]
                rolling_avg = sum(game_totals) / max(len(game_totals), 1)

                # Season per-game baseline for the same composite stat
                season_avg = sum(float(season_pg.get(k, 0) or 0) for k in keys)
                if season_avg < 0.01:
                    # Near-zero baseline (e.g., SB for slow players) — skip
                    continue

                ratio = rolling_avg / season_avg
                adj   = _ratio_to_adjustment(ratio)
                adjustments[prop_type] = adj

                logger.debug(
                    "[Form] pid=%d  %-18s  rolling=%.2f  season=%.2f  "
                    "ratio=%.2f  adj=%+.3f",
                    player_id, prop_type, rolling_avg, season_avg, ratio, adj,
                )

        return adjustments

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def prefetch_form_data(self, player_names: set[str]) -> None:
        """
        Pre-fetch and cache form adjustments for all players in the prop pool.

        Call once at the start of _evaluate_props before the evaluate loop.
        Rate-limited to be polite to the MLB Stats API.
        """
        self._load_roster()
        to_fetch: list[tuple[str, int]] = []
        for name in player_names:
            pid = self._resolve_player_id(name)
            if pid and pid not in self._form_cache:
                to_fetch.append((name, pid))

        logger.info("[Form] Pre-fetching %d players (pool size %d)",
                    len(to_fetch), len(player_names))

        for i, (name, pid) in enumerate(to_fetch):
            try:
                self._form_cache[pid] = self._compute_form(pid)
            except Exception as exc:
                logger.debug("[Form] Skipped %s (%d): %s", name, pid, exc)
                self._form_cache[pid] = {}
            # Polite rate-limiting — pause every 5 players
            if i > 0 and i % 5 == 0:
                time.sleep(0.3)

        logger.info("[Form] Pre-fetch complete — %d players cached", len(self._form_cache))

    def get_form_adjustment(self, player_name: str, prop_type: str) -> float:
        """
        Return the probability adjustment for a player + prop_type pair.

        Returns 0.0 if:
          - player cannot be resolved to an MLB Stats API ID
          - no game log data was available (early season, injured)
          - prop_type not in _PROP_STAT_MAP

        Always safe to call — never raises.
        """
        try:
            pid = self._resolve_player_id(player_name)
            if not pid:
                return 0.0
            return self._form_cache.get(pid, {}).get(prop_type, 0.0)
        except Exception:
            return 0.0


# ---------------------------------------------------------------------------
# Module-level singleton — import this in live_dispatcher.py
# ---------------------------------------------------------------------------
form_layer = MLBFormLayer()
