"""
defense_layer.py
================
PropIQ — Outfield Outs Above Average (OAA) defense layer.

Fetches outfield OAA from pybaseball.statcast_outs_above_average(year, pos)
for LF / CF / RF and aggregates to per-team outfield totals.

Higher OAA = better outfield defense → harder for hitters to get extra bases.
Lower OAA (negative) = bad outfield → easier for hitters.

Effect on batter props (hits, TB, H+R+RBI, rbis, runs):
  Facing a bad outfield (team OAA ≤ −5): +1.0pp to +1.5pp hitter boost
  Facing a good outfield (team OAA ≥ +5): −1.0pp to −1.5pp hitter penalty
  Max effect: ±1.5pp (0.015) — modest but consistent directional signal.

Cached in Redis 12 hours. Falls back to 0.0 (neutral) on any failure.

Public API
----------
get_team_outfield_oaa(team_abbr) -> float
    Aggregate outfield OAA for a team (LF+CF+RF combined).
    Positive = good defense. Negative = bad defense.

get_player_oaa(player_id) -> float
    Individual player OAA (any position).

stamp_defense_on_prop(prop) -> prop
    Stamp _defense_oaa (probability adjustment) onto a prop dict in-place.
    Only stamps on batter props where outfield defense matters.

prefetch() -> None
    Call at 8:15 AM PT to warm cache before dispatch window.
"""

from __future__ import annotations

import json
import logging
import os
import threading

logger = logging.getLogger("propiq.defense")

# ── Constants ─────────────────────────────────────────────────────────────────
_CACHE_KEY   = "oaa_defense_2026"
_CACHE_TTL   = 43200   # 12 hours
_MIN_OPP     = 5       # minimum opportunities to include a player

# Outfield positions to aggregate for team OAA
_OUTFIELD_POS = ["LF", "CF", "RF"]

# Prop types where outfield defense matters
_DEFENSE_RELEVANT_PROPS = {
    "hits", "total_bases", "hits_runs_rbis",
    "fantasy_hitter", "rbis", "runs",
}

# ── Module-level state ────────────────────────────────────────────────────────
_oaa_by_player: dict[int, float]  = {}   # mlbam_id → OAA
_oaa_by_team:   dict[str, float]  = {}   # team_abbr → aggregate outfield OAA
_loaded      = False
_load_lock   = threading.Lock()


# ── Redis helper ──────────────────────────────────────────────────────────────

def _get_redis():
    try:
        import redis as _r
        url = os.environ.get("REDIS_URL") or os.environ.get("REDIS_PUBLIC_URL")
        if not url:
            return None
        return _r.from_url(url, decode_responses=True, socket_connect_timeout=3)
    except Exception:
        return None


# ── pybaseball fetch ──────────────────────────────────────────────────────────

def _fetch_oaa() -> dict:
    """Fetch OAA for outfielders. Returns {players: {mlbam_id: oaa}, teams: {abbr: agg_oaa}}."""
    all_players: dict[int, float] = {}
    team_agg:    dict[str, float] = {}

    try:
        import pybaseball  # noqa: PLC0415
        pybaseball.cache.enable()

        for pos in _OUTFIELD_POS:
            try:
                df = pybaseball.statcast_outs_above_average(2026, pos=pos, minOpp=_MIN_OPP)
                if df is None or df.empty:
                    continue
                for _, row in df.iterrows():
                    try:
                        # pybaseball OAA df columns vary by year — try multiple names
                        pid = int(
                            row.get("outs_above_average_id")
                            or row.get("player_id")
                            or row.get("mlbam_id")
                            or 0
                        )
                        oaa  = float(row.get("outs_above_average") or row.get("oaa") or 0)
                        team = str(
                            row.get("team_abbrev")
                            or row.get("team_abbreviation")
                            or row.get("team")
                            or ""
                        ).strip().upper()

                        if pid:
                            all_players[pid] = all_players.get(pid, 0.0) + oaa
                        if team:
                            team_agg[team] = team_agg.get(team, 0.0) + oaa
                    except Exception:
                        continue
                logger.info("[Defense] OAA loaded for pos=%s: %d players", pos, len(df))
            except Exception as exc:
                logger.debug("[Defense] OAA fetch pos=%s failed: %s", pos, exc)

    except Exception as exc:
        logger.warning("[Defense] OAA pybaseball fetch failed entirely: %s", exc)
        return {}

    if not all_players and not team_agg:
        return {}

    logger.info(
        "[Defense] OAA loaded: %d players, %d teams (outfield LF+CF+RF aggregate)",
        len(all_players), len(team_agg),
    )
    return {"players": all_players, "teams": team_agg}


# ── Load / cache ──────────────────────────────────────────────────────────────

def _load() -> None:
    global _oaa_by_player, _oaa_by_team, _loaded
    if _loaded:
        return
    with _load_lock:
        if _loaded:
            return

        r = _get_redis()

        # Try Redis cache first
        if r:
            try:
                cached = r.get(_CACHE_KEY)
                if cached:
                    data = json.loads(cached)
                    _oaa_by_player = {int(k): v for k, v in data.get("players", {}).items()}
                    _oaa_by_team   = data.get("teams", {})
                    _loaded = True
                    logger.info(
                        "[Defense] OAA loaded from Redis: %d players, %d teams",
                        len(_oaa_by_player), len(_oaa_by_team),
                    )
                    return
            except Exception as exc:
                logger.debug("[Defense] Redis read failed: %s", exc)

        # Fetch fresh
        logger.info("[Defense] Fetching OAA from pybaseball (cold start)…")
        data = _fetch_oaa()
        if data:
            _oaa_by_player = {int(k): v for k, v in data.get("players", {}).items()}
            _oaa_by_team   = data.get("teams", {})

            if r:
                try:
                    r.setex(
                        _CACHE_KEY,
                        _CACHE_TTL,
                        json.dumps({
                            "players": {str(k): v for k, v in _oaa_by_player.items()},
                            "teams":   _oaa_by_team,
                        }),
                    )
                    logger.info("[Defense] Cached OAA in Redis (12h TTL)")
                except Exception as exc:
                    logger.debug("[Defense] Redis write failed: %s", exc)
        else:
            logger.warning("[Defense] OAA data unavailable — returning neutral 0.0 adjustments")

        _loaded = True


def prefetch() -> None:
    """Force-refresh the OAA cache.  Call at 8:15 AM PT."""
    global _loaded, _oaa_by_player, _oaa_by_team
    _loaded       = False
    _oaa_by_player = {}
    _oaa_by_team   = {}
    _load()


# ── Public API ────────────────────────────────────────────────────────────────

def get_team_outfield_oaa(team_abbr: str) -> float:
    """Return aggregate outfield OAA for a team (LF+CF+RF combined).
    Positive = good defense. Negative = bad defense. Returns 0.0 if unknown.
    """
    _load()
    return _oaa_by_team.get(str(team_abbr).strip().upper(), 0.0)


def get_player_oaa(player_id: int) -> float:
    """Return individual player OAA across all outfield positions. 0.0 if unknown."""
    _load()
    return _oaa_by_player.get(int(player_id), 0.0)


def stamp_defense_on_prop(prop: dict) -> dict:
    """
    Stamp _defense_oaa (probability adjustment in ratio units) onto a prop dict.

    Only stamps on batter props where outfield defense matters:
    hits, total_bases, hits_runs_rbis, fantasy_hitter, rbis, runs.

    Effect:
        team_oaa = +10 (elite outfield, great defense) → _defense_oaa = -0.015 (−1.5pp)
        team_oaa = -10 (bad outfield, leaky defense)   → _defense_oaa = +0.015 (+1.5pp)
        Scaled: 0.0015 per OAA unit, capped at ±0.015 (±1.5pp)

    The prop dict must have `opponent_team`, `opp_team`, `away_team`, or `opposing_team`
    set to the opposing team's abbreviation (e.g. "NYY", "LAD").

    Returns prop dict unchanged if data unavailable.
    """
    _load()

    prop_type = str(prop.get("prop_type", "")).lower()
    if prop_type not in _DEFENSE_RELEVANT_PROPS:
        return prop

    # Try multiple key names for the opposing team
    opp_team = (
        prop.get("opponent_team")
        or prop.get("opposing_team")
        or prop.get("opp_team")
        or prop.get("away_team")
        or ""
    )
    if not opp_team:
        return prop

    opp_str  = str(opp_team).strip().upper()
    team_oaa = get_team_outfield_oaa(opp_str)

    # OAA of +10 → good outfield → penalty for hitters → negative adjustment
    # OAA of −10 → bad outfield  → boost for hitters  → positive adjustment
    # Scale: 0.0015 per OAA unit, capped at ±0.015 (±1.5pp)
    adj = max(-0.015, min(0.015, -team_oaa * 0.0015))
    prop["_defense_oaa"] = round(adj, 5)

    if abs(adj) >= 0.005:
        logger.debug(
            "[Defense] %s %s vs %s outfield OAA=%.1f → adj=%+.4f",
            prop.get("player", "?"), prop_type, opp_str, team_oaa, adj,
        )
    return prop
