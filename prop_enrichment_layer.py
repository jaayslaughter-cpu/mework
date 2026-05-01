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
    "walks_allowed",       # ABS 2026: reinstated — BB rate up 18% structurally
    "hitter_strikeouts",   # batter K — still lookup pitcher's K stats
}

_BATTER_PROP_TYPES = {
    "hits", "home_runs", "total_bases", "rbis", "rbi", "runs",
    "stolen_bases", "doubles", "singles",
    "hits_runs_rbis", "fantasy_hitter",
}


def _norm(name: str) -> str:
    """Lowercase, strip, collapse whitespace."""
    return re.sub(r"\s+", " ", (name or "").strip().lower())


# ---------------------------------------------------------------------------
# Step 1 — Build lookup maps from DataHub hub (free, no API calls)
# ---------------------------------------------------------------------------

def _build_lookup_maps(hub: dict) -> tuple[dict, dict, dict, dict]:
    """
    Returns:
        player_to_team        {player_name_lower: team_name}
        player_to_opponent    {player_name_lower: opposing_team}
        player_to_mlbam       {player_name_lower: mlbam_id}
        player_to_pitcher_hand {player_name_lower: "L" or "R"}  ← NEW
    """
    p2team:  dict[str, str] = {}
    p2opp:   dict[str, str] = {}
    p2mlbam: dict[str, int] = {}
    p2hand:  dict[str, str] = {}   # batter_name → opposing pitcher hand

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

    # From projected starters (adds opposing_team for pitchers + pitcher hand)
    team_to_opp:  dict[str, str] = {}
    team_to_hand: dict[str, str] = {}  # batting_team → opposing pitcher throws

    for s in ctx.get("projected_starters", []):
        name = _norm(s.get("full_name", ""))
        team = s.get("team", "")
        opp  = s.get("opponent", "")
        pid  = s.get("player_id")
        hand = str(s.get("pitcher_hand", "") or s.get("throws", "") or "").upper().strip()

        if name:
            if team:
                p2team[name] = team
            if opp:
                p2opp[name] = opp
            if pid:
                try: p2mlbam[name] = int(pid)
                except (ValueError, TypeError): pass
        if team and opp:
            team_to_opp[team] = opp
            team_to_opp[opp]  = team
        # pitcher's hand faces the opposing team's batters
        if opp and hand in ("L", "R"):
            team_to_hand[opp] = hand   # batters on opp team face this hand

    # Wire batters to their opponents and the opposing pitcher's hand
    for batter_name, batter_team in list(p2team.items()):
        if batter_name not in p2opp and batter_team in team_to_opp:
            p2opp[batter_name] = team_to_opp[batter_team]
        if batter_team in team_to_hand:
            p2hand[batter_name] = team_to_hand[batter_team]

    return p2team, p2opp, p2mlbam, p2hand


# ---------------------------------------------------------------------------
# Step 2 — FanGraphs pitcher/batter stats
# ---------------------------------------------------------------------------

def _get_fg_pitcher(name: str) -> dict:
    try:
        from fangraphs_layer import get_pitcher  # noqa: PLC0415
        stats = get_pitcher(name) or {}
        return {
            "k_rate":       stats.get("k_pct",     stats.get("k_rate",   0.223)),
            "bb_rate":      stats.get("bb_pct",    stats.get("bb_rate",  0.087)),
            "era":          stats.get("xfip",      stats.get("fip",      4.06)),
            "whip":         stats.get("whip",      1.28),
            "csw_pct":      stats.get("csw_pct",   0.275),
            "swstr_pct":    stats.get("swstr_pct", 0.110),
            "xfip":         stats.get("xfip",      4.06),
            "siera":        stats.get("siera",     4.06),
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
            "woba":       stats.get("woba",       0.308),
            "iso":        stats.get("iso",        0.156),
            "babip":      stats.get("babip",      0.288),
            "o_swing":    stats.get("o_swing",    0.316),
            "z_contact":  stats.get("z_contact",  0.850),
            "hr_fb_pct":  stats.get("hr_fb_pct",  0.105),
            "k_pct":      stats.get("k_pct",      0.223),
            "bb_pct":     stats.get("bb_pct",     0.087),
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
            k_rate  = round(min(so / (ip * 4.35), 0.40), 4)   # SO per BF proxy
            bb_rate = round(min(bb / (ip * 4.35), 0.20), 4)
            era_val = round(float(s.get("era",  0) or 0), 2) or round(er / ip * 9, 2)
            whip_val = round(float(s.get("whip", 0) or 0), 3) or round((h + bb) / ip, 3)
            result = {
                "k_rate":    k_rate  if k_rate  > 0 else 0.223,
                "bb_rate":   bb_rate if bb_rate > 0 else 0.087,
                "era":       era_val  if era_val  > 0 else 4.06,
                "whip":      whip_val if whip_val > 0 else 1.28,
                "k_per_start": round(so / gs, 1),
                # FIX: expose raw season totals for Bernoulli suppression model
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
            wrc_proxy = round(((obp / 0.317) + (slg / 0.407) - 1) * 100, 1) if slg > 0 else 100.0
            iso = round(slg - avg, 3) if slg > avg else 0.156
            result = {
                "wrc_plus":  max(40.0, min(200.0, wrc_proxy)),
                "babip":     babip  if babip  > 0 else 0.288,
                "obp":       obp    if obp    > 0 else 0.315,
                "slg":       slg    if slg    > 0 else 0.410,
                "iso":       iso    if iso    > 0 else 0.156,
                "k_pct":     k_pct  if k_pct  > 0 else 0.223,
                "bb_pct":    bb_pct if bb_pct > 0 else 0.087,
                "_source":   "mlbapi_2026",
            }
            _mlbapi_batter_cache[cache_key] = result
            return result
    except Exception as _e:
        logger.debug("[Enrichment] mlbapi batter fallback failed for %s: %s", player_name, _e)
    _mlbapi_batter_cache[cache_key] = {}
    return {}



_mlbapi_split_cache: dict[str, dict] = {}

def _get_mlbapi_batter_splits(player_id: int | None, pitcher_hand: str) -> dict:
    """
    Fetch batter vs-hand splits from MLB Stats API.
    Returns woba_vs_hand, babip_vs_hand, iso_vs_hand, k_pct_vs_hand,
    bb_pct_vs_hand, slg_vs_hand when available.

    pitcher_hand: "L" or "R"
    Falls back to empty dict — caller uses season stats.
    """
    if not player_id or not pitcher_hand:
        return {}
    hand = pitcher_hand.upper().strip()
    if hand not in ("L", "R"):
        return {}

    cache_key = f"{player_id}_{hand}"
    if cache_key in _mlbapi_split_cache:
        return _mlbapi_split_cache[cache_key]

    # MLB Stats API sitCodes: vl = vs LHP, vr = vs RHP
    site_code = "vl" if hand == "L" else "vr"
    try:
        import requests as _req
        import datetime as _dt
        season = _dt.date.today().year
        resp = _req.get(
            f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats",
            params={
                "stats": "statSplits",
                "group": "hitting",
                "season": str(season),
                "sitCodes": site_code,
            },
            timeout=8,
        )
        if resp.status_code != 200:
            _mlbapi_split_cache[cache_key] = {}
            return {}

        for sg in resp.json().get("stats", []):
            for split in sg.get("splits", []):
                s  = split.get("stat", {})
                pa = max(float(s.get("plateAppearances", 0) or 0), 1)
                if pa < 10:   # too small a sample — skip
                    continue
                ab  = max(float(s.get("atBats",      1) or 1), 1)
                h   = float(s.get("hits",            0) or 0)
                so  = float(s.get("strikeOuts",      0) or 0)
                bb  = float(s.get("baseOnBalls",     0) or 0)
                avg = round(h / ab, 3)
                obp = round(float(s.get("obp", 0) or (h + bb) / pa), 3)
                slg = round(float(s.get("slg", 0) or 0), 3)
                babip = round(float(s.get("babip", 0) or avg), 3)
                iso  = round(slg - avg, 3) if slg > avg else 0.156
                woba = round(float(s.get("obp", 0) or 0) * 0.90 + slg * 0.10, 3)  # simplified wOBA proxy
                k_pct  = round(so / pa, 4)
                bb_pct = round(bb / pa, 4)
                result = {
                    "woba_vs_hand":   woba  if woba  > 0 else None,
                    "babip_vs_hand":  babip if babip > 0 else None,
                    "iso_vs_hand":    iso   if iso   > 0 else None,
                    "k_pct_vs_hand":  k_pct  if k_pct  > 0 else None,
                    "bb_pct_vs_hand": bb_pct if bb_pct > 0 else None,
                    "slg_vs_hand":    slg   if slg   > 0 else None,
                    "_split_pa":      int(pa),
                    "_pitcher_hand":  hand,
                }
                _mlbapi_split_cache[cache_key] = result
                return result

    except Exception as _e:
        logger.debug("[Enrichment] batter splits failed for %s vs %s: %s", player_id, hand, _e)

    _mlbapi_split_cache[cache_key] = {}
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
            _h_per_ab = float(prop.get("babip", 0.288) or 0.288)
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

_cv_cache: dict = {}  # Module-level CV nudge cache (reset per-cycle)

def _get_cv_nudge(player_id: int | None, prop_type: str, season: int) -> float:
    if not player_id:
        return 0.0
    try:
        from cv_consistency_layer import get_player_cv_nudge  # noqa: PLC0415
        return float(get_player_cv_nudge(player_id, prop_type, season, _cv_cache) or 0.0)
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
               "avg_chase_rate": 0.316, "_opp_o_swing_avg": 0.316}
    if not opposing_team:
        return default
    try:
        from lineup_chase_layer import get_lineup_chase_score  # noqa: PLC0415
        lineups = hub.get("context", {}).get("lineups", [])
        result  = get_lineup_chase_score(opposing_team, lineups)
        result["_opp_o_swing_avg"] = result.get("avg_chase_rate", 0.316)
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
    result = {"is_dome": False, "altitude_ft": 0, "humidor": False, "park_factor_batting": 1.0}
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

    # Park factors — general batting factor stored now, prop-specific applied per-prop
    try:
        from park_factors import get_park_factor, get_park_info  # noqa: PLC0415
        result["park_factor_batting"] = get_park_factor(venue or "", "batting", team)
        result["_park_info"] = get_park_info(venue or "", team)
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
        if k_rate < 0.01 and csw_pct < 0.01:
            return None

        # ── pa_model odds-ratio matchup blender ──────────────────────────────
        # Replaces flat k_rate * 18 with a proper pitcher × batter × league
        # odds-ratio estimate that accounts for the specific opposing lineup's
        # K tendency. Lineup-level avg_k_pct is set by lineup_chase_layer
        # on _opp_avg_k_pct; fallback to league avg 0.227.
        import math as _math  # noqa: PLC0415
        try:
            from pa_model import odds_ratio_blend, LEAGUE_RATES  # noqa: PLC0415
            _lg_k       = LEAGUE_RATES["K"]                 # 0.223
            _opp_k_pct  = float(prop.get("_opp_avg_k_pct", _lg_k) or _lg_k)

            # Build minimal pitcher profile for the K outcome
            _pitcher_profile = {"K": min(0.40, k_rate if k_rate > 0.01 else _lg_k)}

            # Build minimal batter profile from opposing lineup K tendency
            _batter_profile  = {"K": min(0.40, _opp_k_pct)}

            # Odds-ratio blended K probability per PA
            _blended_k_pa = odds_ratio_blend(
                _batter_profile["K"],
                _pitcher_profile["K"],
                _lg_k,
            )

            # CSW% and whiff adjustments on top of the blended rate
            if csw_pct > 0.30:
                _blended_k_pa *= (1.0 + (csw_pct - 0.28) * 2.0)
            if whiff > 0.12:
                _blended_k_pa *= (1.0 + (whiff - 0.11) * 1.5)
            _blended_k_pa = min(0.45, _blended_k_pa)

            # Expected Ks over a typical start (22 BF = 2025 MLB avg ~5.3 IP × 4.1 BF/inn)
            _bf    = float(prop.get("_batters_faced_avg", 22.0) or 22.0)
            lam    = max(0.01, _blended_k_pa * _bf)

        except Exception:
            # Fallback to original flat estimate if pa_model unavailable
            lam = max(0.01, k_rate * 18.0)
            if csw_pct > 0.30:
                lam *= (1.0 + (csw_pct - 0.28) * 2.0)
            if whiff > 0.12:
                lam *= (1.0 + (whiff - 0.11) * 1.5)
            _math = __import__("math")

        # Poisson P(K >= line)
        p_under = sum(
            _math.exp(-lam) * (lam ** k) / _math.factorial(int(k))
            for k in range(int(line))
        )
        p_over = 1.0 - min(0.99, p_under)
        p = p_over if is_over else (1.0 - p_over)
        if k_rate > 0.20 or csw_pct > 0.27:
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

    # ── Batter hitter_strikeouts ──────────────────────────────────────────
    if prop_type == "hitter_strikeouts":
        import math as _math  # noqa: PLC0415
        # Try FG projected K% first (most reliable for batter K props)
        _batter_name = prop.get("player_name", "")
        _batter_fg_k = None
        if _batter_name:
            try:
                from statcast_static_layer import get_batter_fg_proj as _sc_fg  # noqa: PLC0415
                _fg = _sc_fg(_batter_name)
                if _fg and _fg.get("k_pct", 0) > 0:
                    _batter_fg_k = _fg["k_pct"]  # already decimal, e.g. 0.227
            except Exception:
                pass

        # Fallback: use prop-stamped batter_k_pct or league avg 0.222
        _k_pa = _batter_fg_k or float(prop.get("_batter_k_pct", 0.0) or 0.0) or 0.222

        # Typical 4 PA per game for a batter
        _pa   = float(prop.get("_batter_pa_avg", 4.0) or 4.0)
        _lam  = max(0.01, _k_pa * _pa)

        _p_under = sum(
            _math.exp(-_lam) * (_lam ** k) / _math.factorial(int(k))
            for k in range(int(line))
        )
        _p_over = 1.0 - min(0.99, _p_under)
        p = _p_over if is_over else (1.0 - _p_over)
        # ABS challenge edge: K flips saved by batter (positive = Under K edge)
        try:
            from statcast_static_layer import get_batter_abs_k_edge as _abs_edge  # noqa: PLC0415
            _abs_adj = _abs_edge(prop.get("player_name", "") or prop.get("player", "") or "")
            # Negative adj if batter flips Ks to balls (reduces K probability)
            p = max(0.05, min(0.95, p - _abs_adj * 0.5))
        except Exception:
            pass
        # Only return if we have a real K rate (not just the 22.2% fallback)
        if _batter_fg_k or float(prop.get("_batter_k_pct", 0.0) or 0.0) > 0:
            return round(p, 4)
        return None

    # ── Batter H+R+RBI composite ───────────────────────────────────────────
    # Now uses wRC+ and wOBA for a per-player Bayesian estimate.
    if prop_type == "hits_runs_rbis":
        wrc  = float(prop.get("wrc_plus", 0.0) or 0.0)
        woba = float(prop.get("woba",     0.0) or 0.0)
        if wrc < 1.0 and woba < 0.01:
            return None
        # League avg H+R+RBI Over 3.5 ≈ 55%.
        base = 0.55
        if wrc > 80:
            base += (wrc - 100.0) / 100.0 * 0.08   # ±8pp for ±100 wRC+
        if woba > 0.01:
            base += (woba - 0.308) / 0.060 * 0.05   # FG 2025: center 0.308
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
        base = 0.55
        if wrc > 80:
            wrc_adj = (wrc - 100.0) / 100.0 * 0.10   # ±10pp for ±100 wRC+
            base += wrc_adj
        if xslg > 0.01:
            xslg_adj = (xslg - 0.420) / 0.100 * 0.05  # ±5pp per .100 xSLG
            base += xslg_adj
        if xwoba > 0.01:
            xwoba_adj = (xwoba - 0.308) / 0.060 * 0.04  # FG 2025: center 0.308
            base += xwoba_adj
        base = max(0.35, min(0.80, base))
        p = base if is_over else (1.0 - base)
        if wrc > 80 or xslg > 0.01:
            return round(p, 4)
        return None

    return None

def _compute_arsenal_k_sig(prop: dict) -> float:
    """
    Arsenal K-Signature: reliability-weighted whiff/command score for pitcher K props.
    Range 0.0–1.0.  > 0.35 = elite K upside;  < 0.15 = fade K props.

    Formula derived from baseball_simulator_v2 WeightedRBFSimilarity:
      - sc_whiff_rate:        weight 0.45  (fastest-stabilizing, ABS era king)
      - sc_shadow_whiff_rate: weight 0.30  (edge-of-zone — real break-out signal)
      - csw_pct (normalized): weight 0.25  (command + contact quality)

    When shadow_whiff not available: whiff 0.65, csw_norm 0.35.
    """
    whiff  = float(prop.get("sc_whiff_rate")        or prop.get("swstr_pct") or 0.0)
    shadow = float(prop.get("sc_shadow_whiff_rate")  or 0.0)
    csw    = float(prop.get("csw_pct")               or 0.275)

    # Normalize CSW: 0.275 is league average → deviation ×2.5 maps to ~same scale as whiff
    csw_norm = (csw - 0.275) * 2.5 + 0.275

    if shadow > 0.0:
        sig = whiff * 0.45 + shadow * 0.30 + csw_norm * 0.25
    else:
        sig = whiff * 0.65 + csw_norm * 0.35

    return round(max(0.0, min(1.0, sig)), 4)


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
    try:
        from bernoulli_layer import update_league_rate_from_hub as _update_bl  # noqa: PLC0415
        _update_bl(hub)
    except Exception:
        pass

    # Run sportsbook reference first — provides sharp-book market_implied
    props = _get_sportsbook_ref(props)

    # Run Statcast enrichment — provides player-specific barrel/whiff/xwOBA
    # so we defer to after the lookup maps are built.
    p2team, p2opp, p2mlbam, p2hand = _build_lookup_maps(hub)

    # ── Pre-attach mlbam_ids so statcast can batch-fetch ──────────────────────
    for _p in props:
        _pn = _norm(_p.get("player", ""))
        if not _p.get("mlbam_id") and _pn in p2mlbam:
            _p["mlbam_id"] = p2mlbam[_pn]

    # ── Statcast batch enrichment (needs mlbam_ids) ───────────────────────────
    props = _get_statcast(props)

    # ── Per-player FanGraphs cache (avoid re-fetching same player) ────────────
    _fg_pitcher_cache: dict[str, dict] = {}
    _fg_batter_cache:  dict[str, dict] = {}

    # ── Per-team chase score cache ────────────────────────────────────────────
    _chase_cache:   dict[str, dict] = {}
    _weather_cache: dict[str, dict] = {}
    _park_cache:    dict[str, dict] = {}

    # ── Load injury layer — stamps flags before any agent sees the prop ─────────
    try:
        from injury_layer import load_from_hub as _inj_load, get_injury_status as _inj_status, get_confidence_penalty as _inj_penalty  # noqa: PLC0415
        _inj_load(hub)
        _injury_available = True
    except Exception:
        _inj_status   = lambda name: None        # noqa: E731
        _inj_penalty  = lambda name: 0.0         # noqa: E731
        _injury_available = False

    enriched_count = 0
    fg_hits        = 0

    for prop in props:
        player    = prop.get("player", "")
        prop_type = prop.get("prop_type", "")
        line      = float(prop.get("line", 1.5) or 1.5)
        pn        = _norm(player)

        # ── ABS (Automated Ball-Strike) adjustments — 2026 structural shift ────
        try:
            from abs_layer import get_abs_context as _abs_ctx  # noqa: PLC0415
            # Get umpire name from hub umpires list (matched to this game's team)
            _ump_name = ""
            for _u in (hub.get("context") or {}).get("umpires", []):
                _u_home = str(_u.get("home_team", "")).lower()
                _u_away = str(_u.get("away_team", "")).lower()
                _t_lower = (prop.get("team") or "").lower()
                if _t_lower and (_t_lower in _u_home or _t_lower in _u_away):
                    _ump_name = _u.get("name", "")
                    break
            # season_day: days since Opening Day 2026-03-26
            from datetime import date as _date
            _szn_day = (_date.today() - _date(2026, 3, 26)).days + 1
            _abs = _abs_ctx(prop_type, _ump_name, _szn_day)
            prop["_abs_prop_adj"]         = _abs["abs_prop_adj"]
            prop["_abs_umpire_k_adj"]     = _abs["abs_umpire_k_adj"]
            prop["_abs_total_adj"]        = _abs["abs_total_adj"]
            prop["_abs_zone_reliability"] = _abs["abs_zone_reliability"]
            prop["_abs_era_baseline"]     = _abs["abs_era_baseline"]
            # Apply structural ABS adjustment to model_prob
            if _abs["abs_total_adj"] != 0.0:
                _raw_abs = float(prop.get("model_prob", 50.0))
                prop["model_prob"] = max(5.0, min(95.0, _raw_abs + _abs["abs_total_adj"]))
        except Exception:
            prop["_abs_prop_adj"]     = 0.0
            prop["_abs_total_adj"]    = 0.0

        # ── Injury flag — stamped before any feature enrichment ───────────────
        _inj_rec = _inj_status(player)
        if _inj_rec:
            prop["injury_status"]  = _inj_rec["status"]
            prop["injury_is_il"]   = _inj_rec["is_il"]
            prop["injury_is_dtd"]  = _inj_rec["is_dtd"]
            prop["injury_penalty"] = _inj_penalty(player)
            if _inj_rec["is_il"]:
                # Player is on IL — this prop should not exist.
                # Log and skip: it will be filtered in run_agent_tasklet.
                logger.info(
                    "[Enrichment] SKIP — %s is on %s (%s)",
                    player, _inj_rec["status"], _inj_rec.get("detail", "")
                )
                prop["_skip_injury"] = True
        else:
            prop["injury_status"]  = None
            prop["injury_is_il"]   = False
            prop["injury_is_dtd"]  = False
            prop["injury_penalty"] = 0.0
            prop["_skip_injury"]   = False

        # ── Fix missing team / opponent from DataHub ──────────────────────────
        if not prop.get("team") and pn in p2team:
            prop["team"] = p2team[pn]
        if not prop.get("opposing_team") and pn in p2opp:
            prop["opposing_team"] = p2opp[pn]
        if not prop.get("player_id") and pn in p2mlbam:
            prop["player_id"] = p2mlbam[pn]
            prop["mlbam_id"]  = p2mlbam[pn]

        # ── Platoon split (batter vs opposing pitcher hand) ─────────────────────
        pitcher_hand = p2hand.get(pn, "")
        if pitcher_hand:
            prop["_pitcher_hand"] = pitcher_hand

        # ── Player-specific base rate override ────────────────────────────────
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
                    "bb_rate":       fg.get("bb_rate",   0.087),
                    "era":           fg.get("era",       4.06),
                    "whip":          fg.get("whip",      1.28),
                    "csw_pct":       fg.get("csw_pct",   0.275),
                    "swstr_pct":     fg.get("swstr_pct", 0.110),
                    "xfip":          fg.get("xfip",      4.06),
                    "k_bb_pct":      fg.get("k_bb_pct",  0.139),
                })
                # ── Pitcher type cluster — derived from FanGraphs rates ──────
                # Classifies pitcher archetype using already-stamped k_rate,
                # csw_pct, and bb_rate. Used by F5Agent and XGBoost feature slot.
                # Power:   elite K% + elite CSW% → K-over bias +3pp
                # Command: low K% + low BB%      → K-under bias, safer ER unders
                # Neutral: everything else
                _k   = float(prop.get("k_rate",   0.223))
                _csw = float(prop.get("csw_pct",  0.275))
                _bb  = float(prop.get("bb_rate",  0.087))
                if _k >= 0.270 and _csw >= 0.300:
                    prop["_pitcher_type"] = "power"      # elite swing-and-miss
                elif _k <= 0.185 and _bb <= 0.070:
                    prop["_pitcher_type"] = "command"    # contact/location pitcher
                else:
                    prop["_pitcher_type"] = "neutral"
                # FanGraphs doesn't expose raw IP/ER totals — fetch from mlbapi
                if not prop.get("season_ip"):
                    _player_id = prop.get("player_id") or prop.get("mlbam_id")
                    _mlbapi_ip = _get_mlbapi_pitcher(player, _player_id)
                    if _mlbapi_ip.get("season_ip", 0) > 0:
                        prop["season_ip"]   = _mlbapi_ip["season_ip"]
                        prop["season_divr"] = _mlbapi_ip.get("season_er", 0.0)
            else:
                # FIX: FanGraphs disabled (403) — chain of fallbacks using only real 2026 data.
                _player_id = prop.get("player_id") or prop.get("mlbam_id")
                _mlbapi = _get_mlbapi_pitcher(player, _player_id)

                # Tier 2.0: Statcast 2026 arsenal K% — real per-pitch data, Railway-safe
                if not _mlbapi.get("k_rate") and _player_id:
                    try:
                        from statcast_static_layer import (  # noqa: PLC0415
                            get_pitcher_k_rate as _sc_kr,
                            get_pitcher_whiff_rate as _sc_wr,
                            get_pitcher_xera as _sc_xera,
                        )
                        _sc_k = _sc_kr(int(_player_id))
                        if _sc_k:
                            _mlbapi["k_rate"]    = _sc_k
                            _mlbapi["swstr_pct"] = _sc_wr(int(_player_id)) or 0.110
                            _mlbapi["xfip"]      = _sc_xera(int(_player_id)) or 4.06
                            logger.debug(
                                "[Enrichment] Statcast arsenal k_rate=%.3f for %s",
                                _sc_k, player,
                            )
                    except Exception:
                        pass

                # Tier 2.5: Career-weighted stats (2023-2025 blend) — better than league avg
                # Fills gap when current-season sample too small (< 5 IP in 2026)
                if not _mlbapi.get("k_rate") and _player_id:
                    try:
                        from mlb_stats_layer import get_career_pitcher as _gcp  # noqa: PLC0415
                        _career = _gcp(int(_player_id))
                        if _career.get("k_rate"):
                            _mlbapi = _career
                            logger.debug("[Enrichment] Career stats for pitcher %s", player)
                    except Exception:
                        pass

                _sc_whiff  = float(prop.get("sc_whiff_rate",    0.0) or 0.0)
                _sc_hard   = float(prop.get("sc_hard_hit_rate", 0.0) or 0.0)
                _sc_barrel = float(prop.get("sc_barrel_rate",   0.0) or 0.0)

                # k_rate: statsapi SO/BF ratio (most accurate) → Statcast whiff proxy
                if _mlbapi.get("k_rate", 0) > 0:
                    # Stability-weighted blend: 2026 mlbapi + 2025 FanGraphs
                    try:
                        from season_blender import get_blender as _sb  # noqa: PLC0415
                        _bw = _sb()
                        _s26 = {"k_rate": _mlbapi["k_rate"], "bb_rate": _mlbapi.get("bb_rate", 0.087),
                                "era": _mlbapi.get("era", 4.06), "whip": _mlbapi.get("whip", 1.28)}
                        _s25 = {"k_rate": fg.get("k_rate", 0), "bb_rate": fg.get("bb_rate", 0),
                                "era": fg.get("xfip", fg.get("era", 4.06)), "whip": fg.get("whip", 1.28)} if fg else {}
                        _blend = _bw.blend_pitcher(_s26, _s25) if _s25 else _s26
                        prop.setdefault("k_rate",  _blend.get("k_rate",  _mlbapi["k_rate"]))
                        prop.setdefault("bb_rate", _blend.get("bb_rate", _mlbapi.get("bb_rate", 0.087)))
                        prop.setdefault("era",     _blend.get("era",     _mlbapi.get("era",  4.06)))
                        prop.setdefault("whip",    _blend.get("whip",    _mlbapi.get("whip", 1.28)))
                    except Exception:
                        prop.setdefault("k_rate",  _mlbapi["k_rate"])
                        prop.setdefault("bb_rate", _mlbapi.get("bb_rate", 0.087))
                        prop.setdefault("era",     _mlbapi.get("era",     4.06))
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
                # Blend 2026 FanGraphs with 2025 for stability weighting
                try:
                    from season_blender import SeasonBlender as _SB  # noqa: PLC0415
                    _batter_blender = _SB()
                    _mlbapi_b2 = _get_mlbapi_batter(player, prop.get("player_id") or prop.get("mlbam_id"))
                    if _mlbapi_b2:
                        fg = _batter_blender.blend_batter(fg, _mlbapi_b2)
                except Exception:
                    pass
                prop.update({
                    "wrc_plus":    fg.get("wrc_plus",  100.0),
                    "woba":        fg.get("woba",       0.308),
                    "iso":         fg.get("iso",        0.156),
                    "o_swing":     fg.get("o_swing",    0.316),
                    "z_contact":   fg.get("z_contact",  0.850),
                    "hr_fb_pct":   fg.get("hr_fb_pct",  0.105),
                    "k_pct":       fg.get("k_pct",      0.223),
                    "bb_pct":      fg.get("bb_pct",     0.087),
                })
                # ── Platoon splits overlay ──────────────────────────────────────────
                # Override season stats with vs-hand splits when pitcher hand is known
                # and split has enough PA (checked inside _get_mlbapi_batter_splits)
                if pitcher_hand and prop.get("player_id"):
                    _splits = _get_mlbapi_batter_splits(
                        prop.get("player_id") or prop.get("mlbam_id"),
                        pitcher_hand,
                    )
                    if _splits:
                        # Stamp split values — these override season stats in _player_specific_rate
                        prop["woba_vs_hand"]   = _splits.get("woba_vs_hand")   or prop.get("woba")
                        prop["babip_vs_hand"]  = _splits.get("babip_vs_hand")  or prop.get("babip")
                        prop["iso_vs_hand"]    = _splits.get("iso_vs_hand")    or prop.get("iso")
                        prop["k_pct_vs_hand"]  = _splits.get("k_pct_vs_hand")  or prop.get("k_pct")
                        prop["bb_pct_vs_hand"] = _splits.get("bb_pct_vs_hand") or prop.get("bb_pct")
                        prop["_split_pa"]      = _splits.get("_split_pa", 0)
                        prop["_pitcher_hand"]  = pitcher_hand
                        logger.debug(
                            "[Enrichment] %s platoon split vs %sHP: woba=%.3f babip=%.3f iso=%.3f (PA=%d)",
                            player, pitcher_hand,
                            prop["woba_vs_hand"] or 0,
                            prop["babip_vs_hand"] or 0,
                            prop["iso_vs_hand"] or 0,
                            prop["_split_pa"],
                        )
            else:
                # FIX: FanGraphs 403 — use statsapi.mlb.com 2026 season stats (free, no key).
                _player_id = prop.get("player_id") or prop.get("mlbam_id")
                _mlbapi_b = _get_mlbapi_batter(player, _player_id)

                # Tier 2.5: Career-weighted stats (2023-2025 blend) when season sample too small
                if not _mlbapi_b and _player_id:
                    try:
                        from mlb_stats_layer import get_career_batter as _gcb  # noqa: PLC0415
                        _career_b = _gcb(int(_player_id))
                        if _career_b.get("k_pct") or _career_b.get("avg"):
                            _mlbapi_b = _career_b
                            logger.debug("[Enrichment] Career stats for batter %s", player)
                    except Exception:
                        pass

                # Tier 2.0: Statcast batter metrics — whiff susceptibility + xStats
                if _player_id:
                    try:
                        from statcast_static_layer import (  # noqa: PLC0415
                            get_batter_k_susceptibility as _sc_bw,
                            get_batter_xstats as _sc_bx,
                            get_batter_ev_profile as _sc_ev,
                        )
                        _b_whiff = _sc_bw(int(_player_id))
                        _b_xs    = _sc_bx(int(_player_id))
                        _b_ev    = _sc_ev(int(_player_id))
                        if _b_whiff:
                            prop["_batter_whiff_rate"] = _b_whiff
                        if _b_xs.get("xba"):
                            prop.setdefault("xba",   _b_xs["xba"])
                            prop.setdefault("xwoba", _b_xs.get("xwoba", 0.32))
                        if _b_ev.get("ev50"):
                            prop.setdefault("_batter_ev50",    _b_ev["ev50"])
                            prop.setdefault("_batter_brl_pct", _b_ev.get("brl_percent", 0.0))
                    except Exception:
                        pass

                if _mlbapi_b:
                    prop.setdefault("wrc_plus", _mlbapi_b.get("wrc_plus", 100.0))
                    prop.setdefault("babip",    _mlbapi_b.get("babip",    0.288))
                    prop.setdefault("iso",      _mlbapi_b.get("iso",      0.156))
                    prop.setdefault("k_pct",    _mlbapi_b.get("k_pct",    0.223))
                    prop.setdefault("bb_pct",   _mlbapi_b.get("bb_pct",   0.087))
                    prop.setdefault("slg",      _mlbapi_b.get("slg",      0.410))
                    prop.setdefault("obp",      _mlbapi_b.get("obp",      0.315))
                    logger.debug("[Enrichment] Batter %s using statsapi 2026 fallback", player)
                # o_swing fallback: Statcast sc_whiff_rate is batter whiff% — proxy for chase tendency
                _sc_whiff_b = float(prop.get("sc_whiff_rate", 0.0) or 0.0)
                if _sc_whiff_b > 0.0 and not prop.get("o_swing"):
                    prop.setdefault("o_swing", round(min(0.45, max(0.20, _sc_whiff_b * 1.15)), 3))

        # ── FIX: Stamp zone_mult from Statcast pitch-zone analysis ────────────
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
        # Previously _batting_order_slot was never set in enrichment — slot 26 always 0.
        if not prop.get("_batting_order_slot") and not is_pitcher_prop:
            _player_lower = player.lower().strip()
            for _entry in prop.get("_context_lineups", []):
                _name = (_entry.get("full_name") or _entry.get("name") or "").lower().strip()
                if _name and _name == _player_lower:
                    _slot = int(_entry.get("batting_order", 0) or 0)
                    if _slot > 0:
                        prop["_batting_order_slot"] = _slot
                    break

        # ── Bayesian shrinkage — regress player rates toward 2025 priors ─────────
        # Early-season data is thin; shrink toward league/prior to avoid overreacting.
        # Uses season_ip (pitchers) or _split_pa (batters) as sample size proxy.
        try:
            from bayesian_shrinkage import apply_shrinkage_to_prop as _shrink  # noqa: PLC0415
            _n_sample = float(
                prop.get("season_ip", 0) * 3          # pitchers: IP×3 ≈ BF proxy
                or prop.get("_split_pa", 0)             # batters: PA from splits
                or 0
            )
            if _n_sample > 0:
                prop = _shrink(prop, is_pitcher=is_pitcher_prop, n_current=_n_sample)
        except Exception as _shr_err:
            logger.debug("[Enrichment] Bayesian shrinkage skipped: %s", _shr_err)

        # ── Bernoulli suppression model (pitcher props only) ─────────────────────
        # from the pitcher's cumulative season IP and DivR line.
        if is_pitcher_prop:
            try:
                from bernoulli_layer import enrich_prop_with_bernoulli as _enrich_bl  # noqa: PLC0415
                prop = _enrich_prop_with_bernoulli(prop) if False else _enrich_bl(prop)
            except Exception as _bl_err:
                logger.debug("[Enrichment] Bernoulli layer skipped: %s", _bl_err)

        # ── Arsenal K-Signature (pitcher K props only) ──────────────────────────
        if is_pitcher_prop and prop_type == "strikeouts":
            prop["_arsenal_k_sig"] = _compute_arsenal_k_sig(prop)
            # Apply K-sig nudge: elite (>0.35) → +3pp; below avg (<0.15) → -3pp
            _k_sig = prop["_arsenal_k_sig"]
            if _k_sig > 0.01:   # only when we have real signal
                _k_sig_nudge = (_k_sig - 0.25) / 0.10 * 0.03   # ±3pp per 0.10 above/below avg
                _k_sig_nudge = max(-0.04, min(0.04, _k_sig_nudge))
                prop["_arsenal_k_sig_nudge"] = round(_k_sig_nudge, 4)
                logger.debug(
                    "[Enrichment] %s arsenal_k_sig=%.3f → nudge=%.3f",
                    player, _k_sig, _k_sig_nudge,
                )

        # ── Rolling window stats (last 15 games) ─────────────────────────────────
        try:
            from rolling_window_layer import enrich_prop_with_rolling as _enrich_rw  # noqa: PLC0415
            prop = _enrich_rw(prop, season=season)
        except Exception as _rw_err:
            logger.debug("[Enrichment] Rolling window skipped: %s", _rw_err)

        # ── FIX: Bridge enrichment keys → simulation engine underscore-prefixed keys ──
        # prop_enrichment_layer sets k_rate/k_pct, bb_rate/bb_pct, woba, wrc_plus (no prefix).
        # regardless of who the player is.  Chase Burns and a AAA call-up were identical.
        if is_pitcher_prop:
            # Use FG/MLB stats if available; fall back to DraftEdge projection (de_k_pct)
            # then league average. This prevents flat 72.4% when FanGraphs is 403-blocked.
            _de_k   = float(prop.get("de_k_pct",   0.0) or 0.0)
            _de_bb  = float(prop.get("de_bb_pct",  0.0) or 0.0)
            _de_era = float(prop.get("de_era",     0.0) or 0.0)
            prop.setdefault("_k_pct",  prop.get("k_rate") or prop.get("k_pct") or (_de_k if _de_k > 0.05 else 0.223))
            prop.setdefault("_bb_pct",  prop.get("bb_rate") or prop.get("bb_pct") or (_de_bb if _de_bb > 0.02 else 0.087))
            prop.setdefault("_whip",    prop.get("whip") or (_de_era / 3.5 if _de_era > 0 else 1.28))  # rough proxy from ERA
            prop.setdefault("_csw_pct", prop.get("csw_pct", 0.275))  # 2025: ~27.5%
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
            if not prop.get("_bullpen_era"):
                try:
                    from bullpen_fatigue_scorer import BullpenFatigueScorer as _BFS
                    _bfs = _BFS()
                    _team = prop.get("team", "")
                    if _team:
                        _bera = _bfs.get_bullpen_era(_team) if hasattr(_bfs, "get_bullpen_era") else None
                        prop["_bullpen_era"] = float(_bera) if _bera else 4.00
                    else:
                        prop["_bullpen_era"] = 4.00  # FG 2025: bullpen ERA
                except Exception:
                    prop["_bullpen_era"] = 4.00  # FG 2025: bullpen ERA
            if not prop.get("_pitch_whiff_vs_hand"):
                prop["_pitch_whiff_vs_hand"] = (
                    prop.get("sc_whiff_rate") or
                    prop.get("swstr_pct")     or
                    0.25
                )
        else:
            # Batter keys — use platoon splits when available (overrides season stats)
            # woba_vs_hand / k_pct_vs_hand are set by _get_mlbapi_batter_splits
            _woba_src = prop.get("woba_vs_hand")   or prop.get("woba",    0.308)
            _iso_src  = prop.get("iso_vs_hand")    or prop.get("iso",     0.156)
            _k_src    = prop.get("k_pct_vs_hand")  or prop.get("k_pct",   0.223)
            _bb_src   = prop.get("bb_pct_vs_hand") or prop.get("bb_pct",  0.087)
            prop.setdefault("_wrc_plus", prop.get("wrc_plus", 100.0))
            prop.setdefault("_woba",     _woba_src)
            prop.setdefault("_iso",      _iso_src)
            prop.setdefault("_o_swing",  prop.get("o_swing",  0.316))
            prop.setdefault("_k_pct",    _k_src)
            prop.setdefault("_bb_pct",   _bb_src)
        if team not in _weather_cache:
            _weather_cache[team] = _get_weather(team, hub)
        prop.update(_weather_cache[team])

        # ── Park context (dome + altitude) ────────────────────────────────────
        park_key = venue or team
        if park_key not in _park_cache:
            _park_cache[park_key] = _get_park_context(venue, team)
        prop.update(_park_cache[park_key])

        # ── Prop-specific park factor ─────────────────────────────────────────
        try:
            from park_factors import get_park_factor as _gpf  # noqa: PLC0415
            _pf = _gpf(venue, prop_type, team)
            prop["_park_factor"] = round(_pf, 4)
            # Apply park factor to model_prob: boost or suppress toward/away from 50%
            # A 1.15 park factor means expected stat is 15% higher → shifts OVER prob up
            if _pf != 1.0:
                _raw = float(prop.get("model_prob", 50.0))
                # Scale: factor 1.15 → shift +5pp toward OVER; 0.85 → shift -5pp
                _shift = (_pf - 1.0) * 33.0   # ~5pp per 15% park factor
                prop["model_prob"]    = max(5.0, min(95.0, _raw + _shift))
                prop["_park_adj"]     = round(_shift, 2)
        except Exception:
            prop["_park_factor"] = 1.0
            prop["_park_adj"]    = 0.0

        # ── Pitcher days rest / recent workload ───────────────────────────────
        if is_pitcher_prop:
            try:
                from mlb_stats_layer import get_pitcher_workload as _gpw  # noqa: PLC0415
                _wl = _gpw(player)
                if _wl:
                    prop["_days_rest"]        = _wl.get("days_rest", 5)
                    prop["_last_pitch_count"] = _wl.get("last_pitch_count", 85)
                    prop["_recent_era"]       = _wl.get("recent_era", 4.06)
                    prop["_recent_k_rate"]    = _wl.get("recent_k_rate", 0.22)
                    # Fatigue signal: short rest (<4 days) or high pitch count (>100) → adjust
                    _days  = _wl.get("days_rest", 5)
                    _pc    = _wl.get("last_pitch_count", 85)
                    _raw   = float(prop.get("model_prob", 50.0))
                    if _days <= 3:
                        # Short rest: pitcher typically less effective
                        prop["model_prob"]  = max(5.0, _raw - 4.0)
                        prop["_rest_adj"]   = -4.0
                    elif _days >= 7:
                        # Extra rest: generally positive for performance
                        prop["model_prob"]  = min(95.0, _raw + 2.0)
                        prop["_rest_adj"]   = 2.0
                    else:
                        prop["_rest_adj"] = 0.0
                    if _pc >= 105:
                        # High pitch count last outing → likely on pitch limit today
                        _raw2 = float(prop.get("model_prob", 50.0))
                        prop["model_prob"]    = max(5.0, _raw2 - 3.0)
                        prop["_pitch_count_adj"] = -3.0
                    else:
                        prop["_pitch_count_adj"] = 0.0
            except Exception:
                prop["_days_rest"] = 5
                prop["_rest_adj"]  = 0.0

        # ── Game prediction context ───────────────────────────────────────────
        prop.update(_get_game_context(team, hub))

        # ── Lineup chase (pitcher props only) ─────────────────────────────────
        if is_pitcher_prop and opp_team:
            if opp_team not in _chase_cache:
                _chase_cache[opp_team] = _get_chase_score(opp_team, hub)
            chase = _chase_cache[opp_team]
            prop["_lineup_chase_adj"] = float(chase.get("k_prob_adjustment", 0.0))
            # Expose opposing lineup's avg K rate for pa_model odds-ratio blender
            prop["_opp_avg_k_pct"]    = float(chase.get("avg_k_pct", 0.227) or 0.227)
            prop["_opp_o_swing_avg"]  = float(chase.get("avg_chase_rate",    0.316))
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

        # ── Steamer 2026 counting stat projection (Layer 8b) ─────────────────
        # Fills gaps Marcel leaves: runs, rbis, stolen_bases, home_runs
        # Marcel covers rate stats (wOBA, K%, BB%); Steamer adds counting stat priors
        try:
            from steamer_layer import get_steamer_adj as _get_steamer_adj  # noqa: PLC0415
            _steamer_adj = _get_steamer_adj(
                player, prop_type, _side_for_adj, float(prop.get("line", 0.5) or 0.5)
            )
            prop["_steamer_adj"] = _steamer_adj
        except Exception as _se:
            prop["_steamer_adj"] = 0.0
            logger.debug("[Enrichment] Steamer layer skipped for %s: %s", player, _se)

        # ── Bayesian nudge (uses implied_prob if set, else 52.4% default) ────
        base_prob = float(prop.get("implied_prob", 52.4))
        prop["_bayesian_nudge"] = _get_bayesian_nudge(prop, base_prob)

        # ── Reliability weights (used by nudge stack to dampen low-sample signals) ─
        try:
            from reliability_weights import get_feature_weights as _get_fw
            prop["_feature_weights"] = _get_fw(prop)
        except Exception:
            pass

        # ── DTD/QUESTIONABLE confidence penalty ──────────────────────────────
        # IL props are already filtered at the tasklet level (_skip_injury=True).
        # For DTD/OUT players that somehow still have a prop posted, reduce
        # model_prob so agents see the uncertainty and are less likely to pick.
        _inj_pen = prop.get("injury_penalty", 0.0)
        if _inj_pen > 0 and not prop.get("injury_is_il"):
            _raw_prob = float(prop.get("model_prob", 50.0))
            # Penalty is subtracted from probability in percentage points
            # DTD = -25pp (e.g. 62% → 37%), OUT = -90pp (e.g. 62% → -28% → floor 5%)
            _adj_prob = max(5.0, _raw_prob - (_inj_pen * 100))
            prop["model_prob"]     = _adj_prob
            prop["_injury_adj"]    = round(_raw_prob - _adj_prob, 1)
            logger.debug(
                "[Enrichment] %s  %s penalty: prob %.1f → %.1f  (-%s%%)",
                prop.get("player",""), prop.get("injury_status",""),
                _raw_prob, _adj_prob, round(_inj_pen*100),
            )

        enriched_count += 1

    # ── Statcast batch enrichment ── (moved above main loop; mlbam_ids pre-attached)
    # props = _get_statcast(props)  # NOTE: now called before per-prop loop
    sc_hits = sum(1 for p in props if p.get("sc_xwoba") or p.get("sc_whiff_rate"))

    _platoon_hits = sum(1 for p in props if p.get("_pitcher_hand") and p.get("woba_vs_hand"))
    _arsenal_hits = sum(1 for p in props if p.get("_arsenal_k_sig", 0) > 0)
    logger.info(
        "[Enrichment] %d props enriched | FanGraphs: %d | Statcast: %d | "
        "Platoon splits: %d | Arsenal K-sig: %d | "
        "Marcel/PP wired | SB reference wired | "
        "chase: %d teams | weather: %d stadiums",
        enriched_count, fg_hits, sc_hits,
        _platoon_hits, _arsenal_hits,
        len(_chase_cache), len(_weather_cache),
    )
    return props
