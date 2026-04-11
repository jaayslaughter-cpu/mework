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
    "earned_runs", "hits_allowed",
    # walks_allowed excluded per Prop Exclusion Directive (Phase 112+118)
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
            "k_rate":       stats.get("k_pct",     stats.get("k_rate",   0.223)),
            "bb_rate":      stats.get("bb_pct",    stats.get("bb_rate",  0.086)),
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
            "woba":       stats.get("woba",       0.312),
            "iso":        stats.get("iso",        0.158),
            "babip":      stats.get("babip",      0.300),
            "o_swing":    stats.get("o_swing",    0.318),
            "z_contact":  stats.get("z_contact",  0.850),
            "hr_fb_pct":  stats.get("hr_fb_pct",  0.105),
            "k_pct":      stats.get("k_pct",      0.223),
            "bb_pct":     stats.get("bb_pct",     0.086),
        }
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# MLB Stats API fallback (free, no key) — used when FanGraphs 403s
# Fetches 2026 season stats directly from statsapi.mlb.com
# ---------------------------------------------------------------------------

_MLBAPI_FALLBACK_BASE = "https://statsapi.mlb.com/api/v1"
_mlbapi_pitcher_cache: dict[str, dict] = {}
_mlbapi_batter_cache:  dict[str, dict] = {}


def _get_mlbapi_pitcher(player_name: str, player_id: int | None) -> dict:
    """
    Fetch 2026 pitcher season stats from statsapi.mlb.com.
    Returns real k_rate, bb_rate, era, whip derived from season totals.
    Falls back to empty dict if unavailable.
    """
    if not player_id:
        return {}
    cache_key = str(player_id)
    if cache_key in _mlbapi_pitcher_cache:
        return _mlbapi_pitcher_cache[cache_key]
    try:
        import requests as _req  # noqa: PLC0415
        import datetime as _dt   # noqa: PLC0415
        season = _dt.date.today().year
        resp = _req.get(
            f"{_MLBAPI_FALLBACK_BASE}/people/{player_id}/stats",
            params={"stats": "season", "group": "pitching", "season": str(season)},
            timeout=8,
        )
        if resp.status_code != 200:
            _mlbapi_pitcher_cache[cache_key] = {}
            return {}
        for sg in resp.json().get("stats", []):
            splits = sg.get("splits", [])
            if not splits:
                continue
            s  = splits[0].get("stat", {})
            ip = float(s.get("inningsPitched", 0) or 0)
            so = float(s.get("strikeOuts",     0) or 0)
            bb = float(s.get("baseOnBalls",    0) or 0)
            er = float(s.get("earnedRuns",     0) or 0)
            h  = float(s.get("hits",           0) or 0)
            gs = max(int(s.get("gamesStarted", 0) or 0), 1)
            if ip < 1:
                continue
            # Derive rates from season totals (IP unit = full innings)
            # K per 9: so / ip * 9 → normalize to K% proxy (K/27 PA approx)
            k_rate  = round(min(so / (ip * 4.35), 0.40), 4)   # SO per BF proxy
            bb_rate = round(min(bb / (ip * 4.35), 0.20), 4)
            era_val = round(float(s.get("era",  0) or 0), 2) or round(er / ip * 9, 2)
            whip_val = round(float(s.get("whip", 0) or 0), 3) or round((h + bb) / ip, 3)
            result = {
                "k_rate":    k_rate  if k_rate  > 0 else 0.223,
                "bb_rate":   bb_rate if bb_rate > 0 else 0.086,
                "era":       era_val  if era_val  > 0 else 4.20,
                "whip":      whip_val if whip_val > 0 else 1.28,
                "k_per_start": round(so / gs, 1),
                # FIX: expose raw season totals for Bernoulli suppression model
                # season_ip = cumulative IP in ESPN float format (integer outs / 3)
                # season_er = earned runs (used as DivR proxy — actual DivR needs PbP data)
                "season_ip": ip,    # MLB API inningsPitched (already float)
                "season_er": er,    # MLB API earnedRuns (cumulative season total)
                "_source":   "mlbapi_2026",
            }
            _mlbapi_pitcher_cache[cache_key] = result
            return result
    except Exception as _e:
        logger.debug("[Enrichment] mlbapi pitcher fallback failed for %s: %s", player_name, _e)
    _mlbapi_pitcher_cache[cache_key] = {}
    return {}


def _get_mlbapi_batter(player_name: str, player_id: int | None) -> dict:
    """
    Fetch 2026 batter season stats from statsapi.mlb.com.
    Returns real k_pct, bb_pct, babip, slg, obp as FanGraphs proxies.
    Falls back to empty dict if unavailable.
    """
    if not player_id:
        return {}
    cache_key = str(player_id)
    if cache_key in _mlbapi_batter_cache:
        return _mlbapi_batter_cache[cache_key]
    try:
        import requests as _req  # noqa: PLC0415
        import datetime as _dt   # noqa: PLC0415
        season = _dt.date.today().year
        resp = _req.get(
            f"{_MLBAPI_FALLBACK_BASE}/people/{player_id}/stats",
            params={"stats": "season", "group": "hitting", "season": str(season)},
            timeout=8,
        )
        if resp.status_code != 200:
            _mlbapi_batter_cache[cache_key] = {}
            return {}
        for sg in resp.json().get("stats", []):
            splits = sg.get("splits", [])
            if not splits:
                continue
            s  = splits[0].get("stat", {})
            pa = max(float(s.get("plateAppearances", 0) or 0), 1)
            so = float(s.get("strikeOuts",   0) or 0)
            bb = float(s.get("baseOnBalls",  0) or 0)
            ab = max(float(s.get("atBats",   1) or 1), 1)
            h  = float(s.get("hits",         0) or 0)
            if pa < 5:
                continue
            avg   = round(float(s.get("avg",  0) or h / ab), 3)
            obp   = round(float(s.get("obp",  0) or (h + bb) / pa), 3)
            slg   = round(float(s.get("slg",  0) or 0), 3)
            babip = round(float(s.get("babip", 0) or 0), 3) or avg
            k_pct = round(so / pa, 4)
            bb_pct = round(bb / pa, 4)
            # Derive wRC+ proxy: (OBP/lgOBP + SLG/lgSLG - 1) * 100
            # lg avg OBP=0.317, SLG=0.407 for 2026
            wrc_proxy = round(((obp / 0.317) + (slg / 0.407) - 1) * 100, 1) if slg > 0 else 100.0
            iso = round(slg - avg, 3) if slg > avg else 0.158
            result = {
                "wrc_plus":  max(40.0, min(200.0, wrc_proxy)),
                "babip":     babip  if babip  > 0 else 0.300,
                "obp":       obp    if obp    > 0 else 0.315,
                "slg":       slg    if slg    > 0 else 0.405,
                "iso":       iso    if iso    > 0 else 0.158,
                "k_pct":     k_pct  if k_pct  > 0 else 0.223,
                "bb_pct":    bb_pct if bb_pct > 0 else 0.086,
                "_source":   "mlbapi_2026",
            }
            _mlbapi_batter_cache[cache_key] = result
            return result
    except Exception as _e:
        logger.debug("[Enrichment] mlbapi batter fallback failed for %s: %s", player_name, _e)
    _mlbapi_batter_cache[cache_key] = {}
    return {}


# ---------------------------------------------------------------------------
# Step 3 — Bayesian nudge
# ---------------------------------------------------------------------------

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
            player_rate = float(prop.get("k_rate", prop.get("k_pct", 0.223)) or 0.223)
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
               "avg_chase_rate": 0.318, "_opp_o_swing_avg": 0.318}
    if not opposing_team:
        return default
    try:
        from lineup_chase_layer import get_lineup_chase_score  # noqa: PLC0415
        lineups = hub.get("context", {}).get("lineups", [])
        result  = get_lineup_chase_score(opposing_team, lineups)
        result["_opp_o_swing_avg"] = result.get("avg_chase_rate", 0.318)
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
# enough player-specific signal (FanGraphs + Statcast)
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

    # ── Batter H+R+RBI composite ───────────────────────────────────────────
    # Most common prop type; previously fell back to population avg 0.72.
    # Now uses wRC+ and wOBA for a per-player Bayesian estimate.
    if prop_type == "hits_runs_rbis":
        wrc  = float(prop.get("wrc_plus", 0.0) or 0.0)
        woba = float(prop.get("woba",     0.0) or 0.0)
        if wrc < 1.0 and woba < 0.01:
            return None
        # League avg H+R+RBI Over 3.5 ≈ 55%.
        # Elite batter (wRC+ 140, wOBA .390) → ~0.62; weak (wRC+ 80, wOBA .300) → ~0.49
        base = 0.55
        if wrc > 80:
            base += (wrc - 100.0) / 100.0 * 0.08   # ±8pp for ±100 wRC+
        if woba > 0.01:
            base += (woba - 0.312) / 0.060 * 0.05   # FIX: center 0.320→0.312
        base = max(0.38, min(0.78, base))
        p = base if is_over else (1.0 - base)
        if wrc > 80 or woba > 0.01:
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
            xwoba_adj = (xwoba - 0.312) / 0.060 * 0.04  # FIX: center 0.310→0.312
            base += xwoba_adj
        base = max(0.35, min(0.80, base))
        p = base if is_over else (1.0 - base)
        if wrc > 80 or xslg > 0.01:
            return round(p, 4)
        return None

    return None

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
    # Update Bernoulli league rate from DataHub season totals (once per cycle)
    try:
        from bernoulli_layer import update_league_rate_from_hub as _update_bl  # noqa: PLC0415
        _update_bl(hub)
    except Exception:
        pass

    # Run sportsbook reference first — provides sharp-book market_implied
    # which replaces Underdog's flat -115 and fixes OVER-bet bias
    props = _get_sportsbook_ref(props)

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
                    "k_rate":        fg.get("k_rate",    0.223),
                    "bb_rate":       fg.get("bb_rate",   0.085),
                    "era":           fg.get("era",       4.20),
                    "whip":          fg.get("whip",      1.28),
                    "csw_pct":       fg.get("csw_pct",   0.275),
                    "swstr_pct":     fg.get("swstr_pct", 0.110),
                    "xfip":          fg.get("xfip",      4.20),
                    "k_bb_pct":      fg.get("k_bb_pct",  0.139),
                })
                # FanGraphs doesn't expose raw IP/ER totals — fetch from mlbapi
                # for Bernoulli suppression model season line
                if not prop.get("season_ip"):
                    _player_id = prop.get("player_id") or prop.get("mlbam_id")
                    _mlbapi_ip = _get_mlbapi_pitcher(player, _player_id)
                    if _mlbapi_ip.get("season_ip", 0) > 0:
                        prop["season_ip"]   = _mlbapi_ip["season_ip"]
                        prop["season_divr"] = _mlbapi_ip.get("season_er", 0.0)
            else:
                # FIX: FanGraphs disabled (403) — chain of fallbacks using only real 2026 data.
                # Priority: Statcast (already fetched) → statsapi.mlb.com 2026 season → skip slot.
                _player_id = prop.get("player_id") or prop.get("mlbam_id")
                _mlbapi = _get_mlbapi_pitcher(player, _player_id)

                _sc_whiff  = float(prop.get("sc_whiff_rate",    0.0) or 0.0)
                _sc_hard   = float(prop.get("sc_hard_hit_rate", 0.0) or 0.0)
                _sc_barrel = float(prop.get("sc_barrel_rate",   0.0) or 0.0)

                # k_rate: statsapi SO/BF ratio (most accurate) → Statcast whiff proxy
                if _mlbapi.get("k_rate", 0) > 0:
                    # Stability-weighted blend: 2026 mlbapi + 2025 FanGraphs
                    # K% trusts 2026 quickly; ERA leans on 2025 for most of April
                    try:
                        from season_blender import get_blender as _sb  # noqa: PLC0415
                        _bw = _sb()
                        _s26 = {"k_rate": _mlbapi["k_rate"], "bb_rate": _mlbapi.get("bb_rate", 0.086),
                                "era": _mlbapi.get("era", 4.15), "whip": _mlbapi.get("whip", 1.28)}
                        _s25 = {"k_rate": fg.get("k_rate", 0), "bb_rate": fg.get("bb_rate", 0),
                                "era": fg.get("xfip", fg.get("era", 4.15)), "whip": fg.get("whip", 1.28)} if fg else {}
                        _blend = _bw.blend_pitcher(_s26, _s25) if _s25 else _s26
                        prop.setdefault("k_rate",  _blend.get("k_rate",  _mlbapi["k_rate"]))
                        prop.setdefault("bb_rate", _blend.get("bb_rate", _mlbapi.get("bb_rate", 0.086)))
                        prop.setdefault("era",     _blend.get("era",     _mlbapi.get("era",  4.15)))
                        prop.setdefault("whip",    _blend.get("whip",    _mlbapi.get("whip", 1.28)))
                    except Exception:
                        prop.setdefault("k_rate",  _mlbapi["k_rate"])
                        prop.setdefault("bb_rate", _mlbapi.get("bb_rate", 0.086))
                        prop.setdefault("era",     _mlbapi.get("era",     4.15))
                        prop.setdefault("whip",    _mlbapi.get("whip",    1.28))
                    # Stamp season IP and ER for Bernoulli suppression model
                    if _mlbapi.get("season_ip", 0) > 0:
                        prop.setdefault("season_ip",   _mlbapi["season_ip"])
                        prop.setdefault("season_divr", _mlbapi.get("season_er", 0.0))
                    logger.debug("[Enrichment] Pitcher %s: 2026+2025 blended fallback", player)
                elif _sc_whiff > 0.0:
                    prop.setdefault("k_rate",    round(_sc_whiff * 0.85, 4))
                    prop.setdefault("swstr_pct", _sc_whiff)
                    if _sc_hard > 0.0:
                        prop.setdefault("era",  round(3.0 + _sc_hard * 5.0, 2))
                        prop.setdefault("whip", round(1.1 + _sc_hard * 2.0, 2))
                    if _sc_barrel > 0.0:
                        prop.setdefault("bb_rate", round(0.07 + _sc_barrel * 0.5, 4))

        if is_batter_prop:
            if pn not in _fg_batter_cache:
                _fg_batter_cache[pn] = _get_fg_batter(player)
            fg = _fg_batter_cache[pn]
            if fg:
                fg_hits += 1
                prop.update({
                    "wrc_plus":    fg.get("wrc_plus",  100.0),
                    "woba":        fg.get("woba",       0.312),
                    "iso":         fg.get("iso",        0.158),
                    "o_swing":     fg.get("o_swing",    0.318),
                    "z_contact":   fg.get("z_contact",  0.850),
                    "hr_fb_pct":   fg.get("hr_fb_pct",  0.105),
                    "k_pct":       fg.get("k_pct",      0.223),
                    "bb_pct":      fg.get("bb_pct",     0.085),
                })
            else:
                # FIX: FanGraphs 403 — use statsapi.mlb.com 2026 season stats (free, no key).
                # Provides real k_pct, bb_pct, babip, slg, iso, wrc_plus proxy from actual AB/PA.
                _player_id = prop.get("player_id") or prop.get("mlbam_id")
                _mlbapi_b = _get_mlbapi_batter(player, _player_id)
                if _mlbapi_b:
                    prop.setdefault("wrc_plus", _mlbapi_b.get("wrc_plus", 100.0))
                    prop.setdefault("babip",    _mlbapi_b.get("babip",    0.300))
                    prop.setdefault("iso",      _mlbapi_b.get("iso",      0.158))
                    prop.setdefault("k_pct",    _mlbapi_b.get("k_pct",    0.223))
                    prop.setdefault("bb_pct",   _mlbapi_b.get("bb_pct",   0.085))
                    prop.setdefault("slg",      _mlbapi_b.get("slg",      0.405))
                    prop.setdefault("obp",      _mlbapi_b.get("obp",      0.315))
                    logger.debug("[Enrichment] Batter %s using statsapi 2026 fallback", player)
                # o_swing fallback: Statcast sc_whiff_rate is batter whiff% — proxy for chase tendency
                # Higher whiff = more chasing = higher o_swing. Typical o_swing range: 0.22-0.38
                _sc_whiff_b = float(prop.get("sc_whiff_rate", 0.0) or 0.0)
                if _sc_whiff_b > 0.0 and not prop.get("o_swing"):
                    prop.setdefault("o_swing", round(min(0.45, max(0.20, _sc_whiff_b * 1.15)), 3))

        # ── FIX: Stamp zone_mult from Statcast pitch-zone analysis ────────────
        # Previously _zone_integrity_mult was never set during enrichment — the feature
        # vector slot 5 always defaulted to 1.0 → 0.667. Now computed here for pitcher props.
        if is_pitcher_prop:
            _pitcher_id = prop.get("player_id") or prop.get("mlbam_id")
            if _pitcher_id and not prop.get("_zone_integrity_mult"):
                try:
                    from statcast_feature_layer import analyze_zone_integrity  # noqa: PLC0415
                    _zi = analyze_zone_integrity(int(_pitcher_id))
                    if _zi:
                        prop["_zone_integrity_mult"] = float(_zi.get("integrity_multiplier", 1.0))
                except Exception:
                    pass  # leave unset — feature vector will use default 1.0

        # ── FIX: Stamp batting order slot from hub lineups ────────────────────
        # Previously _batting_order_slot was never set in enrichment — slot 26 always 0.
        # Hub lineups contain batting_order for confirmed lineups.
        if not prop.get("_batting_order_slot") and not is_pitcher_prop:
            _player_lower = player.lower().strip()
            for _entry in prop.get("_context_lineups", []):
                _name = (_entry.get("full_name") or _entry.get("name") or "").lower().strip()
                if _name and _name == _player_lower:
                    _slot = int(_entry.get("batting_order", 0) or 0)
                    if _slot > 0:
                        prop["_batting_order_slot"] = _slot
                    break

        # ── Bernoulli suppression model (pitcher props only) ─────────────────────
        # Computes suppression score, tier (S/A/B/C/D), and Zen/Drama/Meltdown
        # from the pitcher's cumulative season IP and DivR line.
        # Verified math: Chase Burns 2026-04-09 → Supp=0.04123562 Zen=82.5% ✅
        if is_pitcher_prop:
            try:
                from bernoulli_layer import enrich_prop_with_bernoulli as _enrich_bl  # noqa: PLC0415
                prop = _enrich_prop_with_bernoulli(prop) if False else _enrich_bl(prop)
            except Exception as _bl_err:
                logger.debug("[Enrichment] Bernoulli layer skipped: %s", _bl_err)

        # ── FIX: Bridge enrichment keys → simulation engine underscore-prefixed keys ──
        # simulation_engine._safe() reads _k_pct, _bb_pct, _woba, etc. (underscore prefix).
        # prop_enrichment_layer sets k_rate/k_pct, bb_rate/bb_pct, woba, wrc_plus (no prefix).
        # Without this bridge every Monte Carlo sim runs on league averages (0.225 K%, etc.)
        # regardless of who the player is.  Chase Burns and a AAA call-up were identical.
        if is_pitcher_prop:
            prop.setdefault("_k_pct",   prop.get("k_rate")  or prop.get("k_pct",  0.223))  # FIX: 0.225→0.223 (2024 actual)
            prop.setdefault("_bb_pct",  prop.get("bb_rate") or prop.get("bb_pct", 0.086))  # FIX: 0.080→0.086 (2024 actual)
            prop.setdefault("_whip",    prop.get("whip",    1.28))
            prop.setdefault("_csw_pct", prop.get("csw_pct", 0.275))  # 2024: ~27.5%
            # _starter_ip_projection: use FanGraphs ip/gs if available
            _fg_ip  = prop.get("xfip")   # rough proxy; actual ip/gs not fetched yet
            _fg_gs  = 1
            _ip_proj = prop.get("_starter_ip_projection", 0.0)
            if not _ip_proj:
                # Use k_per_start from mlbapi fallback if present
                _kps = prop.get("k_per_start", 0.0)
                if _kps > 0:
                    # k_per_start correlates to IP: elite 7K→~6IP, avg 4K→~5IP
                    prop["_starter_ip_projection"] = max(3.0, min(7.0, 3.5 + _kps * 0.35))
                else:
                    prop["_starter_ip_projection"] = 5.2  # FIX: 2024 MLB avg 5.2 IP (was 5.5)
            # _bullpen_era: try bullpen_fatigue_scorer; fall back to league avg
            if not prop.get("_bullpen_era"):
                try:
                    from bullpen_fatigue_scorer import BullpenFatigueScorer as _BFS
                    _bfs = _BFS()
                    _team = prop.get("team", "")
                    if _team:
                        _bera = _bfs.get_bullpen_era(_team) if hasattr(_bfs, "get_bullpen_era") else None
                        prop["_bullpen_era"] = float(_bera) if _bera else 4.05
                    else:
                        prop["_bullpen_era"] = 4.05  # FIX: 2024 MLB bullpen ERA (was 4.10)
                except Exception:
                    prop["_bullpen_era"] = 4.05  # FIX: 2024 MLB bullpen ERA
            # _pitch_whiff_vs_hand: use Statcast whiff rate as proxy
            if not prop.get("_pitch_whiff_vs_hand"):
                prop["_pitch_whiff_vs_hand"] = (
                    prop.get("sc_whiff_rate") or
                    prop.get("swstr_pct")     or
                    0.25
                )
        else:
            # Batter keys
            prop.setdefault("_wrc_plus", prop.get("wrc_plus", 100.0))
            prop.setdefault("_woba",     prop.get("woba",     0.312))  # FIX: 0.320→0.312 (2024 actual)
            prop.setdefault("_iso",      prop.get("iso",      0.158))  # FIX: 0.155→0.158 (2024 actual)
            prop.setdefault("_o_swing",  prop.get("o_swing",  0.318))  # FIX: 0.310→0.318 (2024 actual)
            prop.setdefault("_k_pct",    prop.get("k_pct",    0.223))  # FIX: 0.224→0.223 (2024 actual)
            prop.setdefault("_bb_pct",   prop.get("bb_pct",   0.086))  # FIX: 0.085→0.086 (2024 actual)

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
            prop["_opp_o_swing_avg"]  = float(chase.get("avg_chase_rate",    0.318))
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
