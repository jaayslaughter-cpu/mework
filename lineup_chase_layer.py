"""
lineup_chase_layer.py
---------------------
Phase 80 — Opposing Lineup Chase Difficulty Analyzer.

Computes a "Chase Score" for the 9 hitters a pitcher faces today.
A high-chase lineup (O-Swing% > 31%) is a K-target; a disciplined
lineup (O-Swing% < 25%) is a "Discipline Trap" that suppresses K props.

Uses FanGraphs data already loaded by fangraphs_layer.py — no extra
API calls required. MLB Stats API lineup context (from DataHub) provides
the actual confirmed batting order.

Public API
----------
get_lineup_chase_score(team_name, context_lineups) -> dict
    team_name        : opponent team string (matches DataHub lineup 'team' key)
    context_lineups  : list[dict] from DataHub ctx["lineups"] — each dict has
                       {"player_id", "full_name", "team", "batting_pos"}

Returns:
    {
        "avg_chase_rate"   : float   (O-Swing% mean)
        "avg_k_pct"        : float   (K% mean)
        "avg_z_contact"    : float   (Z-Contact% mean)
        "players_found"    : int     (matched in FanGraphs)
        "is_k_target"      : bool    (avg_chase > 0.31)
        "is_discipline_trap": bool   (avg_chase < 0.25)
        "lineup_difficulty": str     ("K_TARGET" | "NEUTRAL" | "DISCIPLINE_TRAP")
        "k_prob_adjustment": float   (probability delta to apply to pitcher K props)
    }

If FanGraphs data is unavailable or lineup is empty, returns neutral defaults.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# League average baselines (2024)
_LEAGUE_O_SWING  = 0.310
_LEAGUE_K_PCT    = 0.230
_LEAGUE_Z_CONTACT = 0.850

# Chase thresholds
_K_TARGET_THRESHOLD    = 0.31   # O-Swing% above this → K-target lineup
_DISCIPLINE_THRESHOLD  = 0.25   # O-Swing% below this → discipline trap

# Max probability adjustment (cap ±4pp from lineup chase alone)
_MAX_ADJ = 0.040


def get_lineup_chase_score(
    team_name: str,
    context_lineups: list[dict],
) -> dict[str, Any]:
    """
    Compute lineup chase difficulty for the opposing team.

    Parameters
    ----------
    team_name      : Name of the opposing batting team (e.g. "Cleveland Guardians")
    context_lineups: DataHub ctx["lineups"] — confirmed batting order today.
    """
    defaults = {
        "avg_chase_rate":    _LEAGUE_O_SWING,
        "avg_k_pct":         _LEAGUE_K_PCT,
        "avg_z_contact":     _LEAGUE_Z_CONTACT,
        "players_found":     0,
        "is_k_target":       False,
        "is_discipline_trap": False,
        "lineup_difficulty": "NEUTRAL",
        "k_prob_adjustment": 0.0,
    }

    if not team_name or not context_lineups:
        return defaults

    # ── Filter lineup to opposing team ───────────────────────────────────────
    team_lower = team_name.strip().lower()
    opposing = [
        p for p in context_lineups
        if (p.get("team") or "").strip().lower() == team_lower
    ]

    # Fallback: partial match (handles "Guardians" vs "Cleveland Guardians")
    if not opposing:
        short = team_lower.split()[-1]  # last word: "Guardians", "Yankees", etc.
        opposing = [
            p for p in context_lineups
            if short in (p.get("team") or "").strip().lower()
        ]

    if not opposing:
        logger.debug("[Chase] No lineup found for team '%s'", team_name)
        return defaults

    # ── Pull FanGraphs plate discipline stats for each batter ────────────────
    _fg_available = True
    try:
        from fangraphs_layer import get_batter  # noqa: PLC0415
    except ImportError:
        logger.warning("[Chase] fangraphs_layer not available — using statsapi fallback")
        _fg_available = False
        get_batter = lambda name: None  # noqa: E731

    o_swings:   list[float] = []
    k_pcts:     list[float] = []
    z_contacts: list[float] = []

    for player in opposing:
        name    = player.get("full_name", "")
        mlbam   = player.get("player_id") or player.get("mlbam_id")
        if not name:
            continue

        fg = get_batter(name) if _fg_available else None
        if fg:
            o_swings.append(fg.get("o_swing",   _LEAGUE_O_SWING))
            k_pcts.append(  fg.get("k_pct",     _LEAGUE_K_PCT))
            z_contacts.append(fg.get("z_contact", _LEAGUE_Z_CONTACT))
        else:
            # FIX: FanGraphs 403 — use statsapi.mlb.com 2026 season k_pct + Statcast sc_whiff
            # statsapi gives real K% from PA/SO counts this season (free, no key)
            _used_fallback = False
            if mlbam:
                try:
                    import requests as _req  # noqa: PLC0415
                    import datetime as _dt   # noqa: PLC0415
                    _season = _dt.date.today().year
                    _r = _req.get(
                        f"https://statsapi.mlb.com/api/v1/people/{mlbam}/stats",
                        params={"stats": "season", "group": "hitting", "season": str(_season)},
                        timeout=6,
                    )
                    if _r.status_code == 200:
                        for _sg in _r.json().get("stats", []):
                            _splits = _sg.get("splits", [])
                            if not _splits:
                                continue
                            _s  = _splits[0].get("stat", {})
                            _pa = max(float(_s.get("plateAppearances", 0) or 0), 1)
                            _so = float(_s.get("strikeOuts", 0) or 0)
                            if _pa >= 5:
                                _k_pct_real = _so / _pa
                                k_pcts.append(_k_pct_real)
                                # o_swing proxy: K% * 1.35 ≈ O-Swing (high K% batters chase more)
                                o_swings.append(min(0.45, _k_pct_real * 1.35))
                                z_contacts.append(_LEAGUE_Z_CONTACT)  # no statsapi equivalent
                                _used_fallback = True
                                break
                except Exception:
                    pass
            # Last resort: Statcast sc_whiff_rate on the prop itself (already fetched)
            if not _used_fallback:
                _sc_w = float(player.get("sc_whiff_rate", 0.0) or 0.0)
                if _sc_w > 0.0:
                    o_swings.append(min(0.45, _sc_w * 1.15))
                    k_pcts.append(min(0.38, _sc_w * 0.90))
                    z_contacts.append(_LEAGUE_Z_CONTACT)

    if not o_swings:
        logger.debug("[Chase] Zero FanGraphs/fallback hits for team '%s'", team_name)
        return defaults

    avg_chase   = sum(o_swings)   / len(o_swings)
    avg_k_pct   = sum(k_pcts)     / len(k_pcts)
    avg_z_cont  = sum(z_contacts) / len(z_contacts)
    found       = len(o_swings)

    # ── Classify lineup difficulty ────────────────────────────────────────────
    if avg_chase > _K_TARGET_THRESHOLD:
        difficulty      = "K_TARGET"
        is_k_target     = True
        is_disc_trap    = False
        # High-chase lineup → boost K-over probability
        # Scale: 31%→neutral, 36%→+2pp, 41%→+4pp (cap at 4pp)
        k_adj = min(_MAX_ADJ, (avg_chase - _K_TARGET_THRESHOLD) / 0.10 * 0.020)
    elif avg_chase < _DISCIPLINE_THRESHOLD:
        difficulty      = "DISCIPLINE_TRAP"
        is_k_target     = False
        is_disc_trap    = True
        # Disciplined lineup → fade pitcher K-over
        # Scale: 25%→neutral, 20%→-2pp, 15%→-4pp (cap at -4pp)
        k_adj = max(-_MAX_ADJ, (avg_chase - _DISCIPLINE_THRESHOLD) / 0.10 * 0.020)
    else:
        difficulty   = "NEUTRAL"
        is_k_target  = False
        is_disc_trap = False
        k_adj        = 0.0

    # Additional z_contact refinement: low z_contact → more misses in zone → Over
    if avg_z_cont < 0.820:
        k_adj += 0.010   # contact issues boost K-over
    elif avg_z_cont > 0.880:
        k_adj -= 0.010   # elite contact suppresses K-over

    k_adj = max(-_MAX_ADJ, min(_MAX_ADJ, k_adj))

    result = {
        "avg_chase_rate":    round(avg_chase,  3),
        "avg_k_pct":         round(avg_k_pct,  3),
        "avg_z_contact":     round(avg_z_cont, 3),
        "players_found":     found,
        "is_k_target":       is_k_target,
        "is_discipline_trap": is_disc_trap,
        "lineup_difficulty": difficulty,
        "k_prob_adjustment": round(k_adj, 4),
    }

    logger.info(
        "[Chase] %s (%d/9 matched) → chase=%.1f%%  k_pct=%.1f%%  difficulty=%s  k_adj=%+.1f%%",
        team_name, found,
        avg_chase * 100, avg_k_pct * 100,
        difficulty, k_adj * 100,
    )
    return result
