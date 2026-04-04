"""
prop_enrichment_layer.py
========================
PropIQ — Single-pass prop enrichment for AgentTasklet.

WHY THIS EXISTS
---------------
_extract_underdog_props() returns bare dicts:
  {player, prop_type, line, over_american, under_american, team, venue, platform}

_build_feature_vector() expects 20 signals on each prop:
  k_rate, bb_rate, era, whip, shadow_whiff_rate, _zone_integrity_mult,
  _lineup_chase_adj, _opp_o_swing_avg, _wind_speed, _temp_f, ...

Without enrichment every prop hits defaults → XGBoost gets the same
neutral vector for every bet → model is effectively disabled.

WHAT THIS DOES (in one pass, before agents evaluate)
-----------------------------------------------------
1.  Builds player→team, player→opposing_team, player→mlbam_id maps
    from DataHub context (lineups, projected_starters) — free, no API
2.  Attaches FanGraphs pitcher stats (k_pct, bb_pct, xfip, csw_pct)
    for pitcher props; batter stats (wrc_plus, o_swing, k_pct) for
    batter props — already loaded by fangraphs_layer daily cache
3.  Attaches Bayesian probability nudge from bayesian_layer
4.  Attaches CV consistency nudge from cv_consistency_layer
5.  Attaches MLB form (hot/cold) adjustment from mlb_form_layer
6.  Attaches lineup chase difficulty score from lineup_chase_layer
7.  Attaches weather (wind_speed, temp_f) from hub context
8.  Attaches dome/altitude flags from dome_adjustment + altitude_adjustment
9.  Fixes game_prediction_layer import (wrong name used in tasklets)

INTEGRATION
-----------
Drop this file in the repo root. In tasklets.py, replace:

    props = _get_props(hub)

with:

    from prop_enrichment_layer import enrich_props
    props = enrich_props(_get_props(hub), hub)

That's the only change needed.
"""

from __future__ import annotations

import datetime
import logging
import re
from typing import Any
from zoneinfo import ZoneInfo

logger = logging.getLogger("propiq.enrichment")

# ---------------------------------------------------------------------------
# Banned prop types — must NEVER be enriched or evaluated (Phase 112+118)
# ---------------------------------------------------------------------------

_BANNED_PROP_TYPES = {
    "home_runs", "stolen_bases", "walks", "walks_allowed",
    "home_run", "stolen_base", "walk", "bb",
}

# ---------------------------------------------------------------------------
# Pitcher prop types — used to decide whether to look up pitcher vs batter
# ---------------------------------------------------------------------------

_PITCHER_PROP_TYPES = {
    "strikeouts", "pitcher_strikeouts", "pitching_outs",
    "earned_runs", "hits_allowed",
    "hitter_strikeouts",   # batter K — still lookup pitcher's K stats
}

_BATTER_PROP_TYPES = {
    # home_runs, stolen_bases, doubles, singles, walks removed — banned prop types
    "hits", "total_bases", "rbis", "rbi", "runs",
    "hits_runs_rbis", "fantasy_hitter",
}


def _norm(name: str) -> str:
    """Lowercase, strip, collapse whitespace."""
    return re.sub(r"\s+", " ", (name or "").strip().lower())


# ---------------------------------------------------------------------------
# Module-level MLBFormLayer singleton — bug 40 fix: was re-instantiated
# per prop per cycle (one new object every 15 seconds per prop)
# ---------------------------------------------------------------------------

_FORM_LAYER: Any = None


def _get_form_layer() -> Any:
    global _FORM_LAYER
    if _FORM_LAYER is None:
        try:
            from mlb_form_layer import MLBFormLayer  # noqa: PLC0415
            _FORM_LAYER = MLBFormLayer()
        except Exception:
            pass
    return _FORM_LAYER


# ---------------------------------------------------------------------------
# Step 1 — Build lookup maps from DataHub hub (free, no API calls)
# ---------------------------------------------------------------------------

def _build_lookup_maps(hub: dict) -> tuple[dict, dict, dict]:
    """
    Returns:
        player_to_team        {player_name_lower: team_name}
        player_to_opponent    {player_name_lower: opposing_team}
        player_to_mlbam       {player_name_lower: mlbam_id}
    """
    p2team:  dict[str, str] = {}
    p2opp:   dict[str, str] = {}
    p2mlbam: dict[str, int] = {}

    ctx = hub.get("context", {})

    # From confirmed lineups (most accurate)
    for entry in ctx.get("lineups", []):
        name = _norm(entry.get("full_name", ""))
        if not name:
            continue
        team = entry.get("team", "")
        pid  = entry.get("player_id")
        if team:
            p2team[name] = team
        if pid:
            p2mlbam[name] = int(pid)

    # From projected starters (adds opposing_team)
    for s in ctx.get("projected_starters", []):
        name = _norm(s.get("full_name", ""))
        if not name:
            continue
        team = s.get("team", "")
        opp  = s.get("opponent", "")
        pid  = s.get("player_id")
        if team:
            p2team[name] = team
        if opp:
            p2opp[name] = opp
        if pid:
            p2mlbam[name] = int(pid)

    return p2team, p2opp, p2mlbam


# ---------------------------------------------------------------------------
# Step 2 — FanGraphs pitcher/batter stats
# ---------------------------------------------------------------------------

def _get_fg_pitcher(name: str) -> dict:
    try:
        from fangraphs_layer import get_pitcher  # noqa: PLC0415
        stats = get_pitcher(name) or {}
        return {
            "k_rate":       stats.get("k_pct",     stats.get("k_rate",   0.224)),
            "bb_rate":      stats.get("bb_pct",    stats.get("bb_rate",  0.085)),
            "era":          stats.get("xfip",      stats.get("fip",      4.20)),
            "whip":         stats.get("whip",      1.28),
            "csw_pct":      stats.get("csw_pct",   0.275),
            "swstr_pct":    stats.get("swstr_pct", 0.110),
            "xfip":         stats.get("xfip",      4.20),
            "siera":        stats.get("siera",     4.20),
            "k_bb_pct":     stats.get("k_bb_pct",  0.139),
        }
    except Exception:
        return {}


def _get_fg_batter(name: str) -> dict:
    try:
        from fangraphs_layer import get_batter  # noqa: PLC0415
        stats = get_batter(name) or {}
        return {
            "wrc_plus":   stats.get("wrc_plus",  100.0),
            "woba":       stats.get("woba",       0.310),
            "iso":        stats.get("iso",        0.155),
            "babip":      stats.get("babip",      0.300),
            "o_swing":    stats.get("o_swing",    0.310),
            "z_contact":  stats.get("z_contact",  0.850),
            "hr_fb_pct":  stats.get("hr_fb_pct",  0.105),
            "k_pct":      stats.get("k_pct",      0.224),
            "bb_pct":     stats.get("bb_pct",     0.085),
        }
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Step 3 — Bayesian nudge
# Bug 41 fix: was using k_rate (pitcher strikeout %) as player_rate proxy
# for ALL prop types including batter hits/RBIs/runs — now uses woba for
# batter props and k_rate for pitcher props.
# ---------------------------------------------------------------------------

def _get_bayesian_nudge(prop: dict, existing_prob: float) -> float:
    try:
        from bayesian_layer import bayesian_adjustment  # noqa: PLC0415
        prop_type  = prop.get("prop_type", "")
        side       = prop.get("side", "OVER")
        player     = prop.get("player", "")
        line       = float(prop.get("line", 1.5) or 1.5)
        # Bug 41 fix: use appropriate rate for prop type
        if prop_type in _PITCHER_PROP_TYPES:
            player_rate = float(prop.get("k_rate", prop.get("k_pct", 0.224)) or 0.224)
            player_pa   = 27
        else:
            # Batter props — use wOBA as base rate proxy (range 0.0–0.5+)
            player_rate = float(prop.get("woba", 0.310) or 0.310)
            player_pa   = 4
        return bayesian_adjustment(
            prop_type=prop_type,
            side=side,
            player_name=player,
            player_rate=player_rate,
            player_pa=player_pa,
            line=line,
            existing_prob=existing_prob / 100.0,
        )
    except Exception:
        return 0.0


# ---------------------------------------------------------------------------
# Step 4 — CV consistency nudge
# ---------------------------------------------------------------------------

def _get_cv_nudge(player_id: int | None, prop_type: str, season: int) -> float:
    if not player_id:
        return 0.0
    try:
        from cv_consistency_layer import get_player_cv_nudge  # noqa: PLC0415
        return float(get_player_cv_nudge(player_id, prop_type, season) or 0.0)
    except Exception:
        return 0.0


# ---------------------------------------------------------------------------
# Step 5 — MLB form (hot/cold streak) adjustment
# Bug 40 fix: MLBFormLayer() now pulled from module-level singleton
# ---------------------------------------------------------------------------

def _get_form_adj(player_name: str, prop_type: str, hub: dict) -> float:
    try:
        layer = _get_form_layer()
        if layer is None:
            return 0.0
        ctx   = hub.get("context", {})
        lineups = ctx.get("lineups", [])
        # Find player_id from lineups
        pid = None
        for entry in lineups:
            if _norm(entry.get("full_name", "")) == _norm(player_name):
                pid = entry.get("player_id")
                break
        if not pid:
            return 0.0
        return float(layer.get_adjustment(int(pid), prop_type) or 0.0)
    except Exception:
        return 0.0


# ---------------------------------------------------------------------------
# Step 6 — Lineup chase score (for pitcher K props)
# ---------------------------------------------------------------------------

def _get_chase_score(opposing_team: str, hub: dict) -> dict:
    default = {"k_prob_adjustment": 0.0, "lineup_difficulty": "NEUTRAL",
               "avg_chase_rate": 0.310, "_opp_o_swing_avg": 0.310}
    if not opposing_team:
        return default
    try:
        from lineup_chase_layer import get_lineup_chase_score  # noqa: PLC0415
        lineups = hub.get("context", {}).get("lineups", [])
        result  = get_lineup_chase_score(opposing_team, lineups)
        result["_opp_o_swing_avg"] = result.get("avg_chase_rate", 0.310)
        return result
    except Exception:
        return default


# ---------------------------------------------------------------------------
# Step 7 — Weather from hub context
# ---------------------------------------------------------------------------

def _get_weather(team: str, hub: dict) -> dict:
    default = {"_wind_speed": 8.0, "_temp_f": 72.0, "_wind_direction": ""}
    weather_list = hub.get("context", {}).get("weather", [])
    if not weather_list:
        return default
    team_lower = _norm(team)
    for w in weather_list:
        if not isinstance(w, dict):
            continue
        if team_lower in _norm(w.get("team", "")) or team_lower in _norm(w.get("stadium", "")):
            return {
                "_wind_speed":     float(w.get("wind_speed_mph", 8.0) or 8.0),
                "_temp_f":         float(w.get("temp_f", 72.0) or 72.0),
                "_wind_direction": str(w.get("wind_direction", "") or ""),
            }
    return default


# ---------------------------------------------------------------------------
# Step 8 — Dome + altitude flags
# ---------------------------------------------------------------------------

def _get_park_context(venue: str, team: str) -> dict:
    result = {"is_dome": False, "altitude_ft": 0, "humidor": False}
    if not venue and not team:
        return result

    try:
        from dome_adjustment import is_dome_game  # noqa: PLC0415
        result["is_dome"] = bool(is_dome_game(venue or team))
    except Exception:
        pass

    try:
        from altitude_adjustment import (  # noqa: PLC0415
            get_altitude_ft, get_humidor_status, get_venue_for_team,
        )
        v = venue or get_venue_for_team(team)
        result["altitude_ft"] = int(get_altitude_ft(v) or 0)
        result["humidor"]     = bool(get_humidor_status(v))
    except Exception:
        pass

    return result


# ---------------------------------------------------------------------------
# Step 9 — Game prediction context
# ---------------------------------------------------------------------------

def _get_game_context(team: str, hub: dict) -> dict:
    default = {"game_over_prob": 0.50, "game_home_win_prob": 0.50}
    preds = hub.get("physics", {}).get("game_predictions", [])
    if not preds or not team:
        return default
    team_lower = _norm(team)
    for pred in preds:
        if (team_lower in _norm(pred.get("home_team", "")) or
                team_lower in _norm(pred.get("away_team", ""))):
            return {
                "game_over_prob":     pred.get("over_prob",      0.50),
                "game_home_win_prob": pred.get("home_win_prob",  0.50),
                "game_exp_total":     pred.get("exp_total",      8.80),
                "game_confidence":    pred.get("confidence",     "LOW"),
            }
    return default


# ---------------------------------------------------------------------------
# Main public function — call this once per cycle before agents
# ---------------------------------------------------------------------------

def enrich_props(props: list[dict], hub: dict, season: int | None = None) -> list[dict]:
    """
    Enrich a list of raw Underdog/PrizePicks prop dicts with all analytics
    signals needed by _build_feature_vector() and agent evaluate() methods.

    Runs in a single pass: one FanGraphs lookup per unique player,
    cached in-function. All layers are imported lazily with graceful
    fallback on failure.

    Args:
        props:  Raw prop list from _get_props(hub).
        hub:    Current DataHub snapshot from read_hub().
        season: MLB season year (defaults to current PT year).

    Returns:
        Same list, each prop enriched with additional fields in-place.
        Banned prop types are skipped (returned unchanged).
    """
    # Bug 38 fix: was datetime.date.today().year (UTC) — use PT
    if season is None:
        season = datetime.datetime.now(ZoneInfo("America/Los_Angeles")).year

    if not props:
        return props

    # ── Build lookup maps once for all props ──────────────────────────────────
    p2team, p2opp, p2mlbam = _build_lookup_maps(hub)

    # ── Per-player FanGraphs cache (avoid re-fetching same player) ────────────
    _fg_pitcher_cache: dict[str, dict] = {}
    _fg_batter_cache:  dict[str, dict] = {}

    # ── Per-team chase score cache ────────────────────────────────────────────
    _chase_cache:   dict[str, dict] = {}
    _weather_cache: dict[str, dict] = {}
    _park_cache:    dict[str, dict] = {}

    enriched_count = 0
    skipped_banned = 0
    fg_hits        = 0

    for prop in props:
        player    = prop.get("player", "")
        prop_type = prop.get("prop_type", "")
        pn        = _norm(player)

        # Bug 39 fix: skip banned prop types entirely — never enrich or evaluate
        if prop_type in _BANNED_PROP_TYPES:
            skipped_banned += 1
            continue

        line = float(prop.get("line", 1.5) or 1.5)

        # ── Fix missing team / opponent from DataHub ──────────────────────────
        if not prop.get("team") and pn in p2team:
            prop["team"] = p2team[pn]
        if not prop.get("opposing_team") and pn in p2opp:
            prop["opposing_team"] = p2opp[pn]
        if not prop.get("player_id") and pn in p2mlbam:
            prop["player_id"] = p2mlbam[pn]
            prop["mlbam_id"]  = p2mlbam[pn]

        team     = prop.get("team", "")
        opp_team = prop.get("opposing_team", "")

        # ── Fix missing venue from team name ──────────────────────────────────
        if not prop.get("venue") and team:
            try:
                from altitude_adjustment import get_venue_for_team  # noqa: PLC0415
                prop["venue"] = get_venue_for_team(team)
            except Exception:
                pass
        venue = prop.get("venue", "")

        # ── Attach DataHub lineups for downstream use ─────────────────────────
        prop["_context_lineups"] = hub.get("context", {}).get("lineups", [])

        # ── FanGraphs stats ───────────────────────────────────────────────────
        is_pitcher_prop = prop_type in _PITCHER_PROP_TYPES
        is_batter_prop  = prop_type in _BATTER_PROP_TYPES

        if is_pitcher_prop:
            if pn not in _fg_pitcher_cache:
                _fg_pitcher_cache[pn] = _get_fg_pitcher(player)
            fg = _fg_pitcher_cache[pn]
            if fg:
                fg_hits += 1
                prop.update({
                    "k_rate":        fg.get("k_rate",    0.224),
                    "bb_rate":       fg.get("bb_rate",   0.085),
                    "era":           fg.get("era",       4.20),
                    "whip":          fg.get("whip",      1.28),
                    "csw_pct":       fg.get("csw_pct",   0.275),
                    "swstr_pct":     fg.get("swstr_pct", 0.110),
                    "xfip":          fg.get("xfip",      4.20),
                    "k_bb_pct":      fg.get("k_bb_pct",  0.139),
                })

        if is_batter_prop:
            if pn not in _fg_batter_cache:
                _fg_batter_cache[pn] = _get_fg_batter(player)
            fg = _fg_batter_cache[pn]
            if fg:
                fg_hits += 1
                prop.update({
                    "wrc_plus":    fg.get("wrc_plus",  100.0),
                    "woba":        fg.get("woba",       0.310),
                    "iso":         fg.get("iso",        0.155),
                    "o_swing":     fg.get("o_swing",    0.310),
                    "z_contact":   fg.get("z_contact",  0.850),
                    "hr_fb_pct":   fg.get("hr_fb_pct",  0.105),
                    "k_pct":       fg.get("k_pct",      0.224),
                    "bb_pct":      fg.get("bb_pct",     0.085),
                })

        # ── Weather ───────────────────────────────────────────────────────────
        if team not in _weather_cache:
            _weather_cache[team] = _get_weather(team, hub)
        prop.update(_weather_cache[team])

        # ── Park context (dome + altitude) ────────────────────────────────────
        park_key = venue or team
        if park_key not in _park_cache:
            _park_cache[park_key] = _get_park_context(venue, team)
        prop.update(_park_cache[park_key])

        # ── Game prediction context ───────────────────────────────────────────
        prop.update(_get_game_context(team, hub))

        # ── Lineup chase (pitcher props only) ─────────────────────────────────
        if is_pitcher_prop and opp_team:
            if opp_team not in _chase_cache:
                _chase_cache[opp_team] = _get_chase_score(opp_team, hub)
            chase = _chase_cache[opp_team]
            prop["_lineup_chase_adj"] = float(chase.get("k_prob_adjustment", 0.0))
            prop["_opp_o_swing_avg"]  = float(chase.get("avg_chase_rate",    0.310))
            prop["_lineup_difficulty"] = chase.get("lineup_difficulty", "NEUTRAL")

        # ── CV consistency nudge ──────────────────────────────────────────────
        pid = prop.get("player_id") or prop.get("mlbam_id")
        prop["_cv_nudge"] = _get_cv_nudge(pid, prop_type, season)

        # ── MLB form adjustment (uses module-level singleton — bug 40 fix) ────
        prop["_form_adj"] = _get_form_adj(player, prop_type, hub)

        # ── Bayesian nudge (uses prop-appropriate rate — bug 41 fix) ─────────
        base_prob = float(prop.get("implied_prob", 52.4))
        prop["_bayesian_nudge"] = _get_bayesian_nudge(prop, base_prob)

        enriched_count += 1

    logger.info(
        "[Enrichment] %d props enriched | %d banned skipped | FanGraphs hits: %d/%d | "
        "chase scores: %d teams | weather: %d stadiums",
        enriched_count, skipped_banned, fg_hits, len(props),
        len(_chase_cache), len(_weather_cache),
    )
    return props
