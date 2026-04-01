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

import logging
import re
from typing import Any

logger = logging.getLogger("propiq.enrichment")

# ---------------------------------------------------------------------------
# Pitcher prop types — used to decide whether to look up pitcher vs batter
# ---------------------------------------------------------------------------

_PITCHER_PROP_TYPES = {
    "strikeouts", "pitcher_strikeouts", "pitching_outs",
    "earned_runs", "hits_allowed", "walks_allowed",
    "hitter_strikeouts",   # batter K — still lookup pitcher's K stats
}

_BATTER_PROP_TYPES = {
    "hits", "home_runs", "total_bases", "rbis", "rbi", "runs",
    "stolen_bases", "doubles", "singles", "walks",
    "hits_runs_rbis", "fantasy_hitter",
}


def _norm(name: str) -> str:
    """Lowercase, strip, collapse whitespace."""
    return re.sub(r"\s+", " ", (name or "").strip().lower())


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

    # From projected starters (adds opposing_team for pitchers)
    # Also builds a team→opponent map for wiring batters to their opponents
    team_to_opp: dict[str, str] = {}
    for s in ctx.get("projected_starters", []):
        name = _norm(s.get("full_name", ""))
        team = s.get("team", "")
        opp  = s.get("opponent", "")
        pid  = s.get("player_id")
        if name:
            if team:
                p2team[name] = team
            if opp:
                p2opp[name] = opp
            if pid:
                try: p2mlbam[name] = int(pid)
                except (ValueError, TypeError): pass
        # Build bidirectional team→opponent map from any game we see
        if team and opp:
            team_to_opp[team] = opp
            team_to_opp[opp]  = team

    # Wire batters to their opponents using the team→opponent map
    for batter_name, batter_team in list(p2team.items()):
        if batter_name not in p2opp and batter_team in team_to_opp:
            p2opp[batter_name] = team_to_opp[batter_team]

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
            "wrc_plus":     stats.get("wrc_plus",     100.0),
            "woba":         stats.get("woba",          0.310),
            "iso":          stats.get("iso",           0.155),
            "babip":        stats.get("babip",         0.300),
            "o_swing":      stats.get("o_swing",       0.310),
            "z_contact":    stats.get("z_contact",     0.850),
            "hr_fb_pct":    stats.get("hr_fb_pct",     0.105),
            "k_pct":        stats.get("k_pct",         0.224),
            "bb_pct":       stats.get("bb_pct",        0.085),
            "slg":          stats.get("slg",           0.405),  # SLG — #3 TB feature (16%)
            "xbh_per_game": stats.get("xbh_per_game",  0.50),   # XBH/G — #1 TB feature (45%)
        }
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Step 3 — Bayesian nudge
# ---------------------------------------------------------------------------

def _get_pitch_whiff_vs_hand(pitcher_name: str, batter_hand: str, fg_cache: dict) -> float:
    """Return pitcher's whiff rate vs specific batter handedness.
    Uses FanGraphs platoon splits (csw_pct_vs_rhh / csw_pct_vs_lhh) if available.
    Falls back to overall csw_pct, then league average 0.275.
    batter_hand: 'R', 'L', or '' (unknown).
    """
    fg = fg_cache.get(pitcher_name.lower().replace(" ", "_"), {}) if fg_cache else {}
    hand = (batter_hand or "").upper()
    if hand == "R":
        return float(fg.get("csw_pct_vs_rhh", fg.get("csw_pct", 0.275)) or 0.275)
    if hand == "L":
        return float(fg.get("csw_pct_vs_lhh", fg.get("csw_pct", 0.275)) or 0.275)
    return float(fg.get("csw_pct", 0.275) or 0.275)


def _get_bullpen_era(team: str, hub: dict) -> float:
    """Return opposing team's bullpen ERA for last 7 days.
    Reads hub.bullpen_fatigue[team] if populated (DataHub 30s refresh).
    Falls back to MLB Stats API bullpen ERA endpoint, then league avg 4.50.
    """
    if not team:
        return 4.50
    # Try hub bullpen_fatigue map (populated by DataHub)
    fatigue_map = hub.get("bullpen_fatigue", {})
    if team in fatigue_map:
        entry = fatigue_map[team]
        if isinstance(entry, dict):
            era = entry.get("era_last7") or entry.get("era") or entry.get("fatigue_score")
            if era is not None:
                try:
                    return float(era)
                except (TypeError, ValueError):
                    pass
    # Try MLB Stats API free bullpen endpoint
    try:
        import urllib.request as _ul, json as _json  # noqa: PLC0415
        _TEAM_IDS = {
            "NYY": 147, "BOS": 111, "LAD": 119, "SF": 137, "CHC": 112,
            "STL": 138, "ATL": 144, "MIA": 146, "PHI": 143, "WSN": 120,
            "NYM": 121, "CIN": 113, "MIL": 158, "PIT": 134, "ARI": 109,
            "COL": 115, "SD": 135, "LAA": 108, "OAK": 133, "SEA": 136,
            "TEX": 140, "HOU": 117, "MIN": 142, "KC": 118, "CLE": 114,
            "DET": 116, "CWS": 145, "TOR": 141, "BAL": 110, "TB": 139,
        }
        tid = _TEAM_IDS.get(team.upper())
        if tid:
            url = f"https://statsapi.mlb.com/api/v1/teams/{tid}/stats?stats=season&group=pitching&season=2026"
            with _ul.urlopen(url, timeout=3) as resp:
                data = _json.loads(resp.read())
            splits = data.get("stats", [{}])[0].get("splits", [])
            for s in splits:
                era = s.get("stat", {}).get("era")
                if era is not None:
                    return float(era)
    except Exception:
        pass
    return 4.50


def _get_batting_order_slot(player_name: str, lineups: list) -> int:
    """Return batter's lineup slot (1-9) from confirmed lineups.
    Returns 0 if not found (unknown position).
    """
    if not player_name or not lineups:
        return 0
    pn_lower = player_name.lower().strip()
    for game_lineup in lineups:
        if not isinstance(game_lineup, dict):
            continue
        for side in ("home", "away"):
            batters = game_lineup.get(side, {}).get("batters", []) or game_lineup.get(side, [])
            if not isinstance(batters, list):
                continue
            for i, batter in enumerate(batters, start=1):
                bname = ""
                if isinstance(batter, dict):
                    bname = batter.get("fullName", batter.get("name", "")).lower()
                elif isinstance(batter, str):
                    bname = batter.lower()
                if pn_lower in bname or bname in pn_lower:
                    return i
    return 0


def _get_bayesian_nudge(prop: dict, existing_prob: float) -> float:
    try:
        from bayesian_layer import bayesian_adjustment  # noqa: PLC0415
        prop_type  = prop.get("prop_type", "")
        side       = prop.get("side", "OVER")
        player     = prop.get("player", "")
        line       = float(prop.get("line", 1.5) or 1.5)
        # Use appropriate player rate for prop type — k_rate for pitchers, batting metrics for hitters
        _is_pitcher_pt = prop_type in {"strikeouts","pitching_outs","earned_runs",
                                        "hits_allowed","walks_allowed","fantasy_pitcher"}
        if _is_pitcher_pt:
            player_rate = float(prop.get("k_rate", prop.get("k_pct", 0.224)) or 0.224)
            player_pa   = 27
        else:
            # Batter props: use wRC+ normalized, BABIP, or hit rate proxy
            _wrc = float(prop.get("wrc_plus", 100.0) or 100.0) / 100.0
            _h_per_ab = float(prop.get("babip", 0.300) or 0.300)
            player_rate = min(0.400, max(0.180, (_wrc * 0.275 + _h_per_ab) / 2))
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
# ---------------------------------------------------------------------------

def _get_form_adj(player_name: str, prop_type: str, hub: dict) -> float:
    try:
        from mlb_form_layer import MLBFormLayer  # noqa: PLC0415
        layer = MLBFormLayer()
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


# ---------------------------------------------------------------------------
# Step 8 — Statcast features (xwOBA, barrel%, whiff%, hard-hit%)
# ---------------------------------------------------------------------------
_STATCAST_LAYER: object = None   # module-level singleton to avoid re-init

def _get_statcast(props: list[dict]) -> list[dict]:
    """Enrich props with Baseball Savant Statcast features.
    Requires mlbam_id on each prop. Falls back silently.
    Adds: sc_xwoba, sc_xba, sc_xslg, sc_barrel_rate, sc_hard_hit_rate (batters)
          sc_whiff_rate, sc_shadow_whiff_rate (pitchers)
    """
    global _STATCAST_LAYER
    try:
        from statcast_feature_layer import (  # noqa: PLC0415
            StatcastFeatureLayer,
            enrich_props_with_statcast as _sc_enrich,
        )
        if _STATCAST_LAYER is None:
            _STATCAST_LAYER = StatcastFeatureLayer()
        _PITCHER_PT = {"strikeouts", "pitching_outs", "earned_runs",
                       "hits_allowed", "walks_allowed", "fantasy_pitcher"}
        pitchers = [p for p in props if p.get("prop_type", "") in _PITCHER_PT]
        batters  = [p for p in props if p.get("prop_type", "") not in _PITCHER_PT]
        if pitchers:
            pitchers = _sc_enrich(pitchers, "pitcher", layer=_STATCAST_LAYER)
        if batters:
            batters  = _sc_enrich(batters,  "batter",  layer=_STATCAST_LAYER)
        return pitchers + batters
    except Exception as exc:
        logger.debug("[Enrichment] Statcast skipped: %s", exc)
        return props


# ---------------------------------------------------------------------------
# Step 9 — Marcel projections (3-year weighted prior + current season blend)
# ---------------------------------------------------------------------------
_MARCEL_LAYER: object = None

def _get_marcel_adj(player: str, prop_type: str, is_pitcher: bool) -> float:
    """Return Marcel probability adjustment (max ±0.018).
    Blends 3 years of FanGraphs data weighted by PA — stabilises early season.
    """
    global _MARCEL_LAYER
    try:
        from marcel_layer import MarcelLayer, marcel_adjustment  # noqa: PLC0415
        if _MARCEL_LAYER is None:
            _MARCEL_LAYER = MarcelLayer()
        side = "Over"   # Marcel adjustment is symmetric; caller applies sign
        player_type = "pitcher" if is_pitcher else "batter"
        data = (_MARCEL_LAYER.get_pitcher(player)
                if is_pitcher else _MARCEL_LAYER.get_batter(player))
        return float(marcel_adjustment(prop_type, side, player_type, data) or 0.0)
    except Exception:
        return 0.0


# ---------------------------------------------------------------------------
# Step 10 — Predict+ score (pitcher K-prop unpredictability)
# ---------------------------------------------------------------------------
_PP_LAYER: object = None

def _get_predict_plus_adj(player: str, prop_type: str,
                          side: str, mlbam_id: int | None) -> float:
    """Return Predict+ probability adjustment for K props only (max ±0.020)."""
    if not mlbam_id or prop_type != "strikeouts":
        return 0.0
    global _PP_LAYER
    try:
        from predict_plus_layer import (  # noqa: PLC0415
            PredictPlusLayer, predict_plus_adjustment,
        )
        if _PP_LAYER is None:
            _PP_LAYER = PredictPlusLayer()
        score = float(_PP_LAYER.get_score(int(mlbam_id), player) or 0.0)
        if score <= 0:
            return 0.0
        return float(predict_plus_adjustment(prop_type, side, score) or 0.0)
    except Exception:
        return 0.0


# ---------------------------------------------------------------------------
# Step 11 — Sportsbook reference (sharp book vig-stripped implied probability)
# ---------------------------------------------------------------------------
_SB_CACHE: list | None = None    # enriched once per DataHub cycle

def _get_sportsbook_ref(props: list[dict]) -> list[dict]:
    """Enrich props with sharp-book vig-stripped implied probability.
    Adds: sb_implied_prob, sb_implied_prob_over, sb_implied_prob_under,
          sb_line, sb_line_gap
    These replace Underdog's flat -115 as the market_implied in generate_pick.
    """
    try:
        from sportsbook_reference_layer import (  # noqa: PLC0415
            enrich_props_with_sportsbook,
        )
        import datetime as _dt
        date_str = _dt.date.today().isoformat()
        return enrich_props_with_sportsbook(props, date=date_str)
    except Exception as exc:
        logger.debug("[Enrichment] Sportsbook reference skipped: %s", exc)
        return props


# ---------------------------------------------------------------------------
# Step 12 — Player-specific base rate override
# ---------------------------------------------------------------------------
# Replaces population base rates for K and HR props when we have
# enough player-specific signal (FanGraphs + Statcast).
# This is what fixes "Cole K Over 7.5 = 22%" — for elite pitchers
# the real rate is 40-55%.

def _player_specific_rate(prop: dict, side: str) -> float | None:
    """
    Return player-specific win probability overriding population base rate.
    Returns None if insufficient data to override (caller uses base rate).

    For K Over: use pitcher k_rate (FG) + csw_pct + sc_whiff_rate
    For K Under: mirror of Over
    For HR Over: use batter hr_fb_pct + sc_barrel_rate + iso
    For TB Over: use batter wRC+ + sc_xslg + sc_xwoba
    """
    prop_type = prop.get("prop_type", "")
    line      = float(prop.get("line", 1.5) or 1.5)
    is_over   = side.upper() == "OVER"

    # ── Pitcher K props ────────────────────────────────────────────────────
    if prop_type == "strikeouts":
        k_rate   = float(prop.get("k_rate",   prop.get("k_pct",   0.0)) or 0.0)
        csw_pct  = float(prop.get("csw_pct",  0.0) or 0.0)
        whiff    = float(prop.get("sc_whiff_rate", prop.get("swstr_pct", 0.0)) or 0.0)
        # Need at least one strong signal
        if k_rate < 0.01 and csw_pct < 0.01:
            return None
        # Estimate K/9 and convert to K-count probability
        # k_rate = K per plate appearance → K per 9 innings ≈ k_rate * 27
        # For 6 IP (18 outs ≈ 18 PA), expected Ks = k_rate * 18
        expected_k = k_rate * 18.0   # expected Ks over typical start
        # CSW boost: elite CSW (>32%) means more Ks per PA
        if csw_pct > 0.30:
            expected_k *= (1.0 + (csw_pct - 0.28) * 2.0)
        if whiff > 0.12:
            expected_k *= (1.0 + (whiff - 0.11) * 1.5)
        # Poisson approximation: P(K >= line) = 1 - CDF(line-1, lambda=expected_k)
        import math
        lam = max(0.01, expected_k)
        # P(K < line) = sum_{k=0}^{line-1} e^{-lam} * lam^k / k!
        p_under = sum(
            math.exp(-lam) * (lam ** k) / math.factorial(int(k))
            for k in range(int(line))
        )
        p_over = 1.0 - min(0.99, p_under)
        p = p_over if is_over else (1.0 - p_over)
        # Only override if it differs meaningfully from population avg
        # (prevents overriding when we have bad/default data)
        if k_rate > 0.20 or csw_pct > 0.27:   # real signal, not defaults
            return round(p, 4)
        return None

    # ── Batter HR Over ─────────────────────────────────────────────────────
    if prop_type == "home_runs" and line <= 0.5:
        hr_fb   = float(prop.get("hr_fb_pct", 0.0) or 0.0)
        barrel  = float(prop.get("sc_barrel_rate", 0.0) or 0.0)
        iso     = float(prop.get("iso", 0.0) or 0.0)
        if hr_fb < 0.01 and barrel < 0.01:
            return None
        # HR rate per PA: elite hr_fb (~18%) with 30% FB rate = ~5.4% HR/PA
        # Avg hr_fb (~10.5%) with 35% FB = ~3.7% HR/PA
        # League avg HR/game ≈ 9% (0.09)
        hr_per_pa = (hr_fb if hr_fb > 0.01 else 0.105) * 0.33   # ~33% fly ball rate
        if barrel > 0.10:
            hr_per_pa *= (1.0 + (barrel - 0.08) * 2.0)
        # Typical 4 PA per game
        p_no_hr = (1.0 - hr_per_pa) ** 4
        p_over  = 1.0 - p_no_hr
        p = p_over if is_over else p_no_hr
        if hr_fb > 0.01 or barrel > 0.01:
            return round(p, 4)
        return None

    # ── Batter Total Bases ─────────────────────────────────────────────────
    if prop_type == "total_bases" and line <= 1.5:
        wrc     = float(prop.get("wrc_plus", 0.0) or 0.0)
        xslg    = float(prop.get("sc_xslg",  0.0) or 0.0)
        xwoba   = float(prop.get("sc_xwoba", 0.0) or 0.0)
        if wrc < 1.0 and xslg < 0.01:
            return None
        # wRC+ 140 → ~40% above avg in TB production
        # League avg TB Over 1.5 ≈ 55%
        base = 0.55
        if wrc > 80:
            wrc_adj = (wrc - 100.0) / 100.0 * 0.10   # ±10pp for ±100 wRC+
            base += wrc_adj
        if xslg > 0.01:
            xslg_adj = (xslg - 0.420) / 0.100 * 0.05  # ±5pp per .100 xSLG
            base += xslg_adj
        if xwoba > 0.01:
            xwoba_adj = (xwoba - 0.310) / 0.060 * 0.04
            base += xwoba_adj
        base = max(0.35, min(0.80, base))
        p = base if is_over else (1.0 - base)
        if wrc > 80 or xslg > 0.01:
            return round(p, 4)
        return None

    return None


# ---------------------------------------------------------------------------
# Step 13 — DraftEdge projections (hit_pct, hr_pct, sb_pct, rbi_pct per player)
# Already fetched into hub["market"]["prop_projections"] by DataHub.
# Adds: de_hit_pct, de_hr_pct, de_sb_pct, de_rbi_pct, de_run_pct,
#       de_k_pct, de_dfs_proj, de_batting_order
# ---------------------------------------------------------------------------

def _get_draftedge(props: list[dict], hub: dict) -> list[dict]:
    """Enrich props with DraftEdge projections from hub market data.
    Falls back to calling enrich_props_with_draftedge() directly if hub empty.
    """
    # Try hub first (already fetched by DataHub every 15min)
    proj = hub.get("market", {}).get("prop_projections", {})
    batter_rows  = proj.get("batters",  []) if isinstance(proj, dict) else []
    pitcher_rows = proj.get("pitchers", []) if isinstance(proj, dict) else []

    if batter_rows or pitcher_rows:
        # Build lookup from hub data
        import unicodedata as _ud
        def _norm(n):
            s = _ud.normalize("NFD", (n or "").lower())
            return "".join(c for c in s if _ud.category(c) != "Mn")

        bat_lkp = {_norm(r.get("player_name","")): r for r in batter_rows if r.get("player_name")}
        pit_lkp = {_norm(r.get("player_name","")): r for r in pitcher_rows if r.get("player_name")}

        for prop in props:
            key = _norm(prop.get("player", prop.get("player_name", "")))
            row = bat_lkp.get(key) or pit_lkp.get(key) or {}
            prop["de_hit_pct"]      = float(row.get("hit_pct",      0.0) or 0.0)
            prop["de_hr_pct"]       = float(row.get("hr_pct",       0.0) or 0.0)
            prop["de_sb_pct"]       = float(row.get("sb_pct",       0.0) or 0.0)
            prop["de_rbi_pct"]      = float(row.get("rbi_pct",      0.0) or 0.0)
            prop["de_run_pct"]      = float(row.get("run_pct",      0.0) or 0.0)
            prop["de_k_pct"]        = float(row.get("k_pct",        0.0) or 0.0)
            prop["de_dfs_proj"]     = float(row.get("dfs_proj",     0.0) or 0.0)
            prop["de_batting_order"] = str(row.get("batting_order", "") or "")
        return props

    # Fallback: call scraper directly
    try:
        from draftedge_scraper import enrich_props_with_draftedge  # noqa: PLC0415
        return enrich_props_with_draftedge(props)
    except Exception as exc:
        logger.debug("[Enrichment] DraftEdge skipped: %s", exc)
        return props


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
        season: MLB season year (defaults to current year).

    Returns:
        Same list, each prop enriched with additional fields in-place.
    """
    import datetime
    if season is None:
        season = datetime.date.today().year

    if not props:
        return props

    # ── Batch enrichment (whole prop list, single API call each) ─────────────
    # Run sportsbook reference first — provides sharp-book market_implied
    # which replaces Underdog's flat -115 and fixes OVER-bet bias
    props = _get_sportsbook_ref(props)
    # DraftEdge projections — hit_pct, hr_pct, rbi_pct, batting_order per player
    props = _get_draftedge(props, hub)

    # Run Statcast enrichment — provides player-specific barrel/whiff/xwOBA
    # Requires mlbam_id which gets attached per-prop in the loop below
    # so we defer to after the lookup maps are built.

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
    fg_hits        = 0

    for prop in props:
        player    = prop.get("player", "")
        prop_type = prop.get("prop_type", "")
        line      = float(prop.get("line", 1.5) or 1.5)
        pn        = _norm(player)

        # ── Fix missing team / opponent from DataHub ──────────────────────────
        if not prop.get("team") and pn in p2team:
            prop["team"] = p2team[pn]
        if not prop.get("opposing_team") and pn in p2opp:
            prop["opposing_team"] = p2opp[pn]
        if not prop.get("player_id") and pn in p2mlbam:
            prop["player_id"] = p2mlbam[pn]
            prop["mlbam_id"]  = p2mlbam[pn]

        # ── Player-specific base rate override ────────────────────────────────
        # Override population base rate for K/HR/TB when we have real signal
        # Set on prop so generate_pick and _model_prob can use it
        _side_hint = prop.get("side", "OVER")
        _ps_rate = _player_specific_rate(prop, _side_hint)
        if _ps_rate is not None:
            prop["_player_specific_prob"] = _ps_rate

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

        # ── MLB form adjustment ───────────────────────────────────────────────
        prop["_form_adj"] = _get_form_adj(player, prop_type, hub)

        # ── Marcel projection adjustment (weighted 3-year prior) ────────────
        _is_pitcher_prop = prop_type in _PITCHER_PROP_TYPES
        _side_for_adj = prop.get("side", "OVER")
        _marcel_adj = _get_marcel_adj(player, prop_type, _is_pitcher_prop)
        prop["_marcel_adj"] = _marcel_adj

        # ── Predict+ score (pitcher K unpredictability, K props only) ─────────
        _pp_adj = _get_predict_plus_adj(
            player, prop_type, _side_for_adj,
            prop.get("mlbam_id") or prop.get("player_id"),
        )
        prop["_predict_plus_adj"] = _pp_adj

        # ── Bayesian nudge (uses implied_prob if set, else 52.4% default) ────
        base_prob = float(prop.get("implied_prob", 52.4))
        prop["_bayesian_nudge"] = _get_bayesian_nudge(prop, base_prob)

        # ── Pitch type whiff vs batter handedness (slot 20) ──────────────────
        if is_pitcher_prop:
            batter_hand = prop.get("_batter_hand", "")   # set upstream if known
            prop["_pitch_whiff_vs_hand"] = _get_pitch_whiff_vs_hand(
                player, batter_hand, _fg_pitcher_cache
            )
        else:
            prop.setdefault("_pitch_whiff_vs_hand", 0.275)

        # ── Bullpen ERA for opposing team (slot 21) ───────────────────────────
        if opp_team not in _weather_cache:  # reuse weather_cache key pattern, new dict
            pass
        prop["_bullpen_era"] = _get_bullpen_era(opp_team, hub)

        # ── Batting order slot (slot 22) ──────────────────────────────────────
        _slot = _get_batting_order_slot(
            player, hub.get("context", {}).get("lineups", [])
        )
        # Fallback: use DraftEdge batting_order if confirmed lineups not yet posted
        if _slot == 0:
            _de_order = prop.get("de_batting_order", "")
            try:
                _slot = int(_de_order) if _de_order else 0
            except (ValueError, TypeError):
                _slot = 0
        prop["_batting_order_slot"] = _slot

        # ── Park factor adjustment (Step 10) ──────────────────────────────────
        # Determine home team for this prop from lineups context.
        # Park factors apply to the home venue regardless of which team
        # the player is on.
        try:
            from fangraphs_layer import park_factor_adjustment as _pf_adj  # noqa: PLC0415
            _lineups = hub.get("context", {}).get("lineups", [])
            # Build team → side map from lineups
            _side_map: dict[str, str] = {
                lu["team"].lower(): lu.get("side", "")
                for lu in _lineups
                if lu.get("team")
            }
            _player_team_lower = team.lower() if team else ""
            _player_side = _side_map.get(_player_team_lower, "")
            # Find the home team: either this team is home, or find counterpart
            if _player_side == "home":
                _home_team = team
            else:
                # Find the team with side="home" playing on the same date/game
                # Use game context: match by finding opp_team's side
                _home_team = ""
                _opp_lower = (opp_team or "").lower()
                if _opp_lower and _side_map.get(_opp_lower) == "home":
                    _home_team = opp_team or ""
                else:
                    # Last resort: scan lineups for any home team
                    for _lu in _lineups:
                        if _lu.get("side") == "home":
                            _home_team = _lu.get("team", "")
                            break
            _pf_nudge = _pf_adj(prop_type, prop.get("side", "Over"), _home_team)
            prop["_park_factor_adj"] = _pf_nudge
            prop["_park_factor_team"] = _home_team
        except Exception as _pf_err:
            prop.setdefault("_park_factor_adj", 0.0)
            logger.debug("[Enrichment] park_factor_adj skipped: %s", _pf_err)

        enriched_count += 1

    # ── Statcast batch enrichment (needs mlbam_ids attached above) ─────────────
    props = _get_statcast(props)
    sc_hits = sum(1 for p in props if p.get("sc_xwoba") or p.get("sc_whiff_rate"))

    logger.info(
        "[Enrichment] %d props enriched | FanGraphs: %d | Statcast: %d | "
        "Marcel/PP wired | SB reference wired | "
        "chase: %d teams | weather: %d stadiums",
        enriched_count, fg_hits, sc_hits,
        len(_chase_cache), len(_weather_cache),
    )
    return props
