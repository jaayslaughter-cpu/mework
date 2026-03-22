"""
platform_selector.py
====================
Live platform comparison engine — PrizePicks vs Underdog Fantasy.

For every player + prop, fetches the current line from both platforms,
calculates the implied win probability (based on line favorability vs
the player's projected stat) and returns the platform with the higher
edge for that specific leg.

Fantasy-point legs for both platforms are supported using the official
scoring tables provided by PrizePicks and Underdog Fantasy.

Public API
----------
  selector = PlatformSelector()
  result   = selector.compare(player_name, prop_type, projected_stat)
  # result → {platform, line, side, implied_prob, fantasy_pts, notes}
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

import requests

logger = logging.getLogger("propiq.platform_selector")

# ---------------------------------------------------------------------------
# Scoring tables (official, as provided)
# ---------------------------------------------------------------------------

# Underdog Fantasy — hitter
UD_HITTER_SCORING: dict[str, float] = {
    "home_run":    10.0,
    "triple":       8.0,
    "double":       6.0,
    "stolen_base":  4.0,
    "single":       3.0,
    "walk":         3.0,
    "hit_by_pitch": 3.0,
    "rbi":          2.0,
    "run":          2.0,
}

# Underdog Fantasy — pitcher
UD_PITCHER_SCORING: dict[str, float] = {
    "inning_pitched": 3.0,   # per IP
    "strikeout":      3.0,
    "win":            5.0,
    "quality_start":  5.0,
    "earned_run":    -3.0,
}

# PrizePicks — hitter (Fantasy Score)
PP_HITTER_SCORING: dict[str, float] = {
    "home_run":      10.0,
    "triple":         8.0,
    "double":         5.0,
    "single":         3.0,
    "run":            2.0,
    "rbi":            2.0,
    "walk":           2.0,
    "hit_by_pitch":   2.0,
    "stolen_base":    5.0,
    "caught_stealing": -2.0,
}

# PrizePicks — pitcher (Fantasy Score)
# IP scored as +2.5 per out recorded (7.5 per full inning)
PP_PITCHER_SCORING: dict[str, float] = {
    "out_recorded":   2.5,   # per out (IP * 3 outs)
    "strikeout":      2.0,
    "win":            4.0,
    "earned_run":    -3.0,
    "hit_allowed":   -0.6,
    "walk_allowed":  -0.6,
    "hit_batsman":   -0.6,
}

# ---------------------------------------------------------------------------
# Prop-type name maps
# stat_key -> (prizepicks_stat_type, underdog_stat_type)
# ---------------------------------------------------------------------------
PROP_NAME_MAP: dict[str, tuple[str, str]] = {
    "strikeouts":      ("Strikeouts",          "strikeouts"),
    "hits":            ("Hits",                "hits"),
    "home_runs":       ("Home Runs",           "home_runs"),
    "rbis":            ("RBIs",                "rbis"),
    "runs":            ("Runs",                "runs"),
    "total_bases":     ("Total Bases",         "total_bases"),
    "stolen_bases":    ("Stolen Bases",        "stolen_bases"),
    "walks":           ("Walks",               "walks"),
    "hits_runs_rbis":  ("Hits+Runs+RBIs",      None),           # PP only combo
    "fantasy_hitter":  ("Hitter Fantasy Score","fantasy_points_hitter"),
    "fantasy_pitcher": ("Pitcher Fantasy Score","fantasy_points_pitcher"),
}

# Props that only exist on PrizePicks (H+R+RBI combo)
PP_ONLY_PROPS = {"hits_runs_rbis"}

# Props that PrizePicks has but Underdog separates into individual stats
COMBO_STATS = {"hits_runs_rbis"}


@dataclass
class PlatformLine:
    """A single line entry from one platform."""
    platform: str          # "prizepicks" | "underdog"
    stat_type: str         # raw stat type label from API
    line: float
    player_name: str
    player_id: str = ""
    entry_type: str = ""   # "FLEX" | "STANDARD" (Underdog only)


@dataclass
class SelectionResult:
    """Result of platform comparison for one prop leg."""
    player_name: str
    prop_type: str
    side: str              # "Over" | "Under"
    platform: str          # "PrizePicks" | "Underdog"
    line: float
    implied_prob: float    # 0.0–1.0 based on line favorability
    alt_platform: str | None = None
    alt_line: float | None = None
    entry_type: str = ""   # Underdog entry type
    fantasy_pts_edge: float = 0.0   # expected FP - line (for FP props)
    notes: str = ""


class PlatformSelector:
    """
    Fetches live lines from PrizePicks and Underdog Fantasy,
    compares them for each player+prop, and returns the platform
    offering the highest win probability for that leg.
    """

    # Simple in-memory cache (TTL = 5 min) so we don't hammer APIs
    _pp_cache: dict[str, Any] = {}
    _ud_cache: dict[str, Any] = {}
    _cache_ts: float = 0.0
    _CACHE_TTL: float = 300.0

    _PP_URL  = "https://api.prizepicks.com/projections"
    _UD_URL  = "https://api.underdogfantasy.com/v1/over_under_lines"

    _HEADERS = {"User-Agent": "Mozilla/5.0 (PropIQ Analytics/1.0)"}

    # ── fetch helpers ─────────────────────────────────────────────────────────

    def _refresh_cache(self) -> None:
        """Refresh both platform caches if TTL has expired."""
        now = time.monotonic()
        if now - self._cache_ts < self._CACHE_TTL:
            return
        self._pp_cache  = self._fetch_prizepicks()
        self._ud_cache  = self._fetch_underdog()
        self._cache_ts  = now

    # Baseball-specific stat types (PrizePicks v3 API has no league attr on projections)
    _PP_MLB_STATS = {
        "hits", "home runs", "strikeouts", "rbis", "rbi", "runs",
        "total bases", "stolen bases", "hits+runs+rbis", "hits + runs + rbis",
        "hitter fantasy score", "pitcher fantasy score",
        "earned runs", "walks", "doubles", "triples",
    }

    def _fetch_prizepicks(self) -> dict[str, list[PlatformLine]]:
        """
        Fetch PrizePicks MLB projections.
        Returns {player_name_lower: [PlatformLine]}.
        Filters by baseball-specific stat types (no reliable league attr in v3 API).
        """
        result: dict[str, list[PlatformLine]] = {}
        try:
            resp = requests.get(
                self._PP_URL,
                params={"per_page": 250, "single_stat": True},
                headers=self._HEADERS,
                timeout=15,
            )
            if resp.status_code != 200:
                logger.warning("[PP] HTTP %d", resp.status_code)
                return result

            data = resp.json()
            player_map: dict[str, str] = {}
            for item in data.get("included", []):
                if item.get("type") == "new_player":
                    pid   = item["id"]
                    attrs = item.get("attributes", {})
                    name  = attrs.get("display_name") or attrs.get("name", "")
                    if name:
                        player_map[pid] = name

            for proj in data.get("data", []):
                attrs     = proj.get("attributes", {})
                stat_type = str(attrs.get("stat_type") or "")
                if stat_type.lower() not in self._PP_MLB_STATS:
                    continue
                if "inning" in stat_type.lower():
                    continue
                line_val = attrs.get("line_score")
                if line_val is None:
                    continue
                pid = (
                    proj.get("relationships", {})
                        .get("new_player", {})
                        .get("data", {})
                        .get("id", "")
                )
                pname = player_map.get(pid, "")
                if not pname:
                    continue
                key = pname.lower().strip()
                result.setdefault(key, []).append(PlatformLine(
                    platform="prizepicks",
                    stat_type=stat_type,
                    line=float(line_val),
                    player_name=pname,
                    player_id=pid,
                ))
        except Exception as exc:
            logger.warning("[PP] Fetch failed: %s", exc)
        return result

    def _fetch_underdog(self) -> dict[str, list[PlatformLine]]:
        """
        Fetch Underdog Fantasy MLB lines.
        Returns {player_name_lower: [PlatformLine]}.

        Correct join chain (confirmed Phase 18):
          line["over_under"]["appearance_stat"]["stat"]           → stat key
          line["over_under"]["appearance_stat"]["appearance_id"]  → app_id
          appearances_map[app_id]["player_id"]                   → player_id
          players_map[player_id]["sport_id"] == "MLB"            → filter
        """
        result: dict[str, list[PlatformLine]] = {}
        try:
            resp = requests.get(self._UD_URL, headers=self._HEADERS, timeout=20)
            if resp.status_code != 200:
                logger.warning("[UD] HTTP %d", resp.status_code)
                return result

            data = resp.json()

            players_map: dict[str, dict] = {
                p["id"]: p for p in data.get("players", [])
            }
            appearances_map: dict[str, dict] = {
                a["id"]: a for a in data.get("appearances", [])
            }

            seen: set[str] = set()
            for line in data.get("over_under_lines", []):
                if line.get("status") != "active":
                    continue
                stable_id = line.get("stable_id", line.get("id", ""))
                if stable_id in seen:
                    continue

                ou       = line.get("over_under") or {}
                app_stat = ou.get("appearance_stat") or {}
                stat_ud  = app_stat.get("stat", "")
                app_id   = app_stat.get("appearance_id", "")

                if not stat_ud or not app_id:
                    continue
                if "inning" in stat_ud.lower():
                    continue

                appearance = appearances_map.get(app_id, {})
                player_id  = appearance.get("player_id", "")
                player     = players_map.get(player_id, {})

                if player.get("sport_id") != "MLB":
                    continue

                pname = f"{player.get('first_name','')} {player.get('last_name','')}".strip()
                if not pname:
                    continue

                ou_line    = float(line.get("stat_value") or 0)
                opts       = line.get("options", [])
                higher_opt = next((o for o in opts if o.get("choice") == "higher"), {})
                entry_type = "FLEX" if higher_opt.get("payout_multiplier") else "STANDARD"

                seen.add(stable_id)
                key = pname.lower().strip()
                result.setdefault(key, []).append(PlatformLine(
                    platform="underdog",
                    stat_type=stat_ud,
                    line=ou_line,
                    player_name=pname,
                    entry_type=entry_type,
                ))
        except Exception as exc:
            logger.warning("[UD] Fetch failed: %s", exc)
        return result

    # ── line lookup ───────────────────────────────────────────────────────────

    def _find_pp_line(self, player_key: str, prop_type: str) -> PlatformLine | None:
        pp_stat, _ = PROP_NAME_MAP.get(prop_type, (None, None))
        if not pp_stat:
            return None
        for entry in self._pp_cache.get(player_key, []):
            if entry.stat_type.lower() == pp_stat.lower():
                return entry
        return None

    def _find_ud_line(self, player_key: str, prop_type: str) -> PlatformLine | None:
        _, ud_stat = PROP_NAME_MAP.get(prop_type, (None, None))
        if not ud_stat:
            return None
        for entry in self._ud_cache.get(player_key, []):
            if entry.stat_type.lower() == ud_stat.lower():
                return entry
        return None

    # ── probability estimation ────────────────────────────────────────────────

    @staticmethod
    def _implied_prob(line: float, projection: float, side: str) -> float:
        """
        Estimate win probability from line vs projection difference.
        Uses a logistic function centred on the projection:
            diff  = projection - line  (positive = favorable for Over)
            prob  = sigmoid(1.5 * diff)   # 1.5 steepness gives realistic spread
        For Under: flip the diff.
        Clipped to [0.45, 0.80] to stay realistic.
        """
        import math
        diff = projection - line if side == "Over" else line - projection
        # sigmoid
        prob = 1.0 / (1.0 + math.exp(-1.5 * diff))
        return max(0.45, min(0.80, prob))

    # ── fantasy point estimation ──────────────────────────────────────────────

    @staticmethod
    def calc_fantasy_pts(stats: dict[str, float], platform: str,
                         player_type: str) -> float:
        """
        Calculate expected fantasy points from a stat projection dict.

        Args:
            stats:       e.g. {"hits": 1.1, "home_runs": 0.08, "runs": 0.45, ...}
            platform:    "prizepicks" | "underdog"
            player_type: "hitter" | "pitcher"
        """
        if player_type == "hitter":
            if platform == "underdog":
                # Underdog: singles = hits - 2B - 3B - HR
                singles = max(0, stats.get("hits", 0) - stats.get("doubles", 0)
                              - stats.get("triples", 0) - stats.get("home_runs", 0))
                return (
                    stats.get("home_runs", 0)   * UD_HITTER_SCORING["home_run"]
                  + stats.get("triples", 0)     * UD_HITTER_SCORING["triple"]
                  + stats.get("doubles", 0)     * UD_HITTER_SCORING["double"]
                  + stats.get("stolen_bases", 0)* UD_HITTER_SCORING["stolen_base"]
                  + singles                     * UD_HITTER_SCORING["single"]
                  + stats.get("walks", 0)       * UD_HITTER_SCORING["walk"]
                  + stats.get("rbis", 0)        * UD_HITTER_SCORING["rbi"]
                  + stats.get("runs", 0)        * UD_HITTER_SCORING["run"]
                )
            else:  # prizepicks
                singles = max(0, stats.get("hits", 0) - stats.get("doubles", 0)
                              - stats.get("triples", 0) - stats.get("home_runs", 0))
                return (
                    stats.get("home_runs", 0)    * PP_HITTER_SCORING["home_run"]
                  + stats.get("triples", 0)      * PP_HITTER_SCORING["triple"]
                  + stats.get("doubles", 0)      * PP_HITTER_SCORING["double"]
                  + singles                      * PP_HITTER_SCORING["single"]
                  + stats.get("runs", 0)         * PP_HITTER_SCORING["run"]
                  + stats.get("rbis", 0)         * PP_HITTER_SCORING["rbi"]
                  + stats.get("walks", 0)        * PP_HITTER_SCORING["walk"]
                  + stats.get("stolen_bases", 0) * PP_HITTER_SCORING["stolen_base"]
                  - stats.get("caught_stealing", 0) * abs(PP_HITTER_SCORING["caught_stealing"])
                )
        else:  # pitcher
            if platform == "underdog":
                return (
                    stats.get("innings_pitched", 0) * UD_PITCHER_SCORING["inning_pitched"]
                  + stats.get("strikeouts", 0)      * UD_PITCHER_SCORING["strikeout"]
                  + stats.get("wins", 0)             * UD_PITCHER_SCORING["win"]
                  + stats.get("quality_starts", 0)   * UD_PITCHER_SCORING["quality_start"]
                  - stats.get("earned_runs", 0)       * abs(UD_PITCHER_SCORING["earned_run"])
                )
            else:  # prizepicks — 2.5 per out
                outs = stats.get("innings_pitched", 0) * 3
                return (
                    outs                               * PP_PITCHER_SCORING["out_recorded"]
                  + stats.get("strikeouts", 0)         * PP_PITCHER_SCORING["strikeout"]
                  + stats.get("wins", 0)               * PP_PITCHER_SCORING["win"]
                  - stats.get("earned_runs", 0)        * abs(PP_PITCHER_SCORING["earned_run"])
                  - stats.get("hits_allowed", 0)       * abs(PP_PITCHER_SCORING["hit_allowed"])
                  - stats.get("walks_allowed", 0)      * abs(PP_PITCHER_SCORING["walk_allowed"])
                )
        return 0.0

    # ── main comparison method ────────────────────────────────────────────────

    def compare(
        self,
        player_name: str,
        prop_type: str,
        projected_stat: float,
        side: str = "Over",
        player_stats: dict[str, float] | None = None,
        player_type: str = "hitter",
    ) -> SelectionResult | None:
        """
        Compare PP vs Underdog for one player+prop and return the better platform.

        Args:
            player_name:    Display name (e.g., "Aaron Judge")
            prop_type:      Key from PROP_NAME_MAP (e.g., "strikeouts", "hits")
            projected_stat: Model/baseline projection for this stat (per game)
            side:           "Over" | "Under"
            player_stats:   Full stat projection dict (used for fantasy_pts legs)
            player_type:    "hitter" | "pitcher"

        Returns:
            SelectionResult with the better platform and its line, or None if
            no line found on either platform.
        """
        self._refresh_cache()

        pkey = player_name.lower().strip()

        pp_line = self._find_pp_line(pkey, prop_type)
        ud_line = self._find_ud_line(pkey, prop_type)

        if not pp_line and not ud_line:
            logger.debug("[Selector] No lines found for %s %s", player_name, prop_type)
            return None

        # Fantasy points edge calculation
        fp_edge = 0.0
        if prop_type in ("fantasy_hitter", "fantasy_pitcher") and player_stats:
            for plat in ("prizepicks", "underdog"):
                exp_fp = self.calc_fantasy_pts(player_stats, plat, player_type)
                pl     = pp_line if plat == "prizepicks" else ud_line
                if pl and exp_fp > 0:
                    fp_edge = max(fp_edge, exp_fp - pl.line)

        # Both platforms available — pick better line
        if pp_line and ud_line:
            pp_prob = self._implied_prob(pp_line.line, projected_stat, side)
            ud_prob = self._implied_prob(ud_line.line, projected_stat, side)

            if ud_prob >= pp_prob:
                return SelectionResult(
                    player_name=player_name,
                    prop_type=prop_type,
                    side=side,
                    platform="Underdog",
                    line=ud_line.line,
                    implied_prob=ud_prob,
                    alt_platform="PrizePicks",
                    alt_line=pp_line.line,
                    entry_type=ud_line.entry_type,
                    fantasy_pts_edge=fp_edge,
                    notes=f"UD {ud_line.line} vs PP {pp_line.line} — UD wins (+{(ud_prob-pp_prob)*100:.1f}% edge)",
                )
            else:
                return SelectionResult(
                    player_name=player_name,
                    prop_type=prop_type,
                    side=side,
                    platform="PrizePicks",
                    line=pp_line.line,
                    implied_prob=pp_prob,
                    alt_platform="Underdog",
                    alt_line=ud_line.line,
                    entry_type="",
                    fantasy_pts_edge=fp_edge,
                    notes=f"PP {pp_line.line} vs UD {ud_line.line} — PP wins (+{(pp_prob-ud_prob)*100:.1f}% edge)",
                )

        # Only one platform available
        if pp_line:
            prob = self._implied_prob(pp_line.line, projected_stat, side)
            return SelectionResult(
                player_name=player_name, prop_type=prop_type, side=side,
                platform="PrizePicks", line=pp_line.line, implied_prob=prob,
                fantasy_pts_edge=fp_edge, notes="PrizePicks only",
            )
        # ud_line only
        prob = self._implied_prob(ud_line.line, projected_stat, side)  # type: ignore[union-attr]
        return SelectionResult(
            player_name=player_name, prop_type=prop_type, side=side,
            platform="Underdog", line=ud_line.line, implied_prob=prob,
            entry_type=ud_line.entry_type, fantasy_pts_edge=fp_edge,
            notes="Underdog only",
        )


# Module-level singleton
platform_selector = PlatformSelector()
