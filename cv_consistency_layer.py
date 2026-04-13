"""
cv_consistency_layer.py — Layer 9: CV-Based Consistency Gate

Calculates the coefficient of variation (CV = std / mean) for each player's
relevant stat over their last 10 games. Volatile players (high CV) receive a
probability nudge downward before agent claiming begins.

CV thresholds:
  < 0.50  → Very consistent → +0.01 boost
  0.50–0.80 → Normal        → no adjustment
  0.81–1.10 → Volatile      → −0.02 nudge
  > 1.10  → Very volatile   → −0.04 nudge

Fires after Layer 8 (Marcel + Predict+), before agent claiming phase.
Uses MLB Stats API game logs — free, no key required.
Daily cache per player to avoid redundant calls.
"""

import os
import json
import time
import logging
import statistics
import requests
from datetime import date, datetime
from typing import Optional

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────

MLBSTATS_BASE = "https://statsapi.mlb.com/api/v1"
GAME_LOG_COUNT = 10          # L10 games for CV calculation
CACHE_DIR = "/tmp"
try:
    import pytz as _cv_pytz
    _cv_pt = _cv_pytz.timezone("America/Los_Angeles")
    _cv_today = datetime.now(_cv_pt).strftime("%Y-%m-%d")
except ImportError:
    from zoneinfo import ZoneInfo as _cv_zi
    _cv_today = datetime.now(_cv_zi("America/Los_Angeles")).strftime("%Y-%m-%d")
CACHE_FILE = f"{CACHE_DIR}/cv_cache_{_cv_today}.json"

# CV tier boundaries → nudge values
CV_TIERS = [
    (0.50, +0.01),   # CV < 0.50 → consistent bonus
    (0.80,  0.00),   # CV 0.50–0.80 → neutral
    (1.10, -0.02),   # CV 0.81–1.10 → volatile penalty
    (float("inf"), -0.04),  # CV > 1.10 → very volatile penalty
]

# Map prop_type → (stat_group, stat_key, custom_fn)
# custom_fn: optional function to compute value from a game log entry (for derived stats)
PROP_STAT_MAP = {
    # Hitting props
    "hits":          ("hitting", "hits",        None),
    "total_bases":   ("hitting", None,           "calc_total_bases"),
    "rbis":          ("hitting", "rbi",          None),
    "runs":          ("hitting", "runs",         None),
    "singles":       ("hitting", "hits",         None),  # approximate
    "doubles":       ("hitting", "doubles",      None),
    "strikeouts":    ("hitting", "strikeOuts",   None),  # batter Ks
    # Pitching props
    "pitcher_strikeouts": ("pitching", "strikeOuts", None),
    "pitcher_hits":       ("pitching", "hits",        None),
    "pitcher_er":         ("pitching", "earnedRuns",  None),
    "pitcher_outs":       ("pitching", "outs",        None),
}


# ─────────────────────────────────────────────
# Cache helpers
# ─────────────────────────────────────────────

def _load_cache() -> dict:
    """Load today's CV cache from disk."""
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _save_cache(cache: dict) -> None:
    """Persist CV cache to disk."""
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f)
    except Exception as exc:
        logger.warning(f"CV cache write failed: {exc}")


# ─────────────────────────────────────────────
# Stat computation helpers
# ─────────────────────────────────────────────

def _calc_total_bases(game: dict) -> float:
    """Derive total bases from a game log hitting entry."""
    singles = game.get("hits", 0) - game.get("doubles", 0) - \
              game.get("triples", 0) - game.get("homeRuns", 0)
    singles = max(singles, 0)
    return (
        singles * 1
        + game.get("doubles", 0) * 2
        + game.get("triples", 0) * 3
        + game.get("homeRuns", 0) * 4
    )


# ─────────────────────────────────────────────
# MLB Stats API game log fetch
# ─────────────────────────────────────────────

def _fetch_game_log(player_id: int, stat_group: str, season: int) -> list:
    """
    Fetch game-by-game log for a player from MLB Stats API.
    Returns list of stat dicts (most recent first), up to GAME_LOG_COUNT entries.
    """
    url = (
        f"{MLBSTATS_BASE}/people/{player_id}/stats"
        f"?stats=gameLog&group={stat_group}&season={season}&gameType=R"
    )
    try:
        resp = requests.get(url, timeout=8)
        resp.raise_for_status()
        data = resp.json()
        splits = data.get("stats", [{}])[0].get("splits", [])
        # splits are oldest-first; reverse to get most recent first
        return [s.get("stat", {}) for s in reversed(splits)][:GAME_LOG_COUNT]
    except Exception as exc:
        logger.debug(f"CV game log fetch failed for player {player_id}: {exc}")
        return []


# ─────────────────────────────────────────────
# CV calculation
# ─────────────────────────────────────────────

def _compute_cv(values: list) -> Optional[float]:
    """
    Compute CV = std / mean for a list of numeric values.
    Returns None if insufficient data.
    Returns 2.0 (very volatile sentinel) if mean == 0.
    """
    values = [float(v) for v in values if v is not None]
    if len(values) < 3:
        return None  # not enough data → no nudge
    mean = statistics.mean(values)
    if mean == 0:
        return 2.0  # zero-mean → treat as maximally volatile
    std = statistics.stdev(values) if len(values) > 1 else 0.0
    return std / mean


def _cv_to_nudge(cv: Optional[float]) -> float:
    """Convert a CV value to a probability nudge."""
    if cv is None:
        return 0.0
    for threshold, nudge in CV_TIERS:
        if cv < threshold:
            return nudge
    return -0.04  # fallback


# ─────────────────────────────────────────────
# Per-player CV lookup
# ─────────────────────────────────────────────

def get_player_cv_nudge(
    player_id: int,
    prop_type: str,
    season: int,
    cache: dict,
) -> float:
    """
    Return a probability nudge for a player based on their L10 CV
    for the relevant stat. Uses cache to avoid duplicate API calls.

    Args:
        player_id: MLBAM player ID
        prop_type: prop type string (e.g., "hits", "pitcher_strikeouts")
        season: MLB season year
        cache: shared mutable dict for today's results

    Returns:
        float nudge value (±0.04 max)
    """
    cache_key = f"{player_id}_{prop_type}"
    if cache_key in cache:
        return cache[cache_key]

    if prop_type not in PROP_STAT_MAP:
        cache[cache_key] = 0.0
        return 0.0

    stat_group, stat_key, custom_fn = PROP_STAT_MAP[prop_type]

    # Rate-limit: small jitter between requests
    time.sleep(0.2)

    game_log = _fetch_game_log(player_id, stat_group, season)
    if not game_log:
        cache[cache_key] = 0.0
        return 0.0

    # Extract per-game stat values
    if custom_fn == "calc_total_bases":
        values = [_calc_total_bases(g) for g in game_log]
    elif stat_key:
        values = [g.get(stat_key, 0) for g in game_log]
    else:
        cache[cache_key] = 0.0
        return 0.0

    cv = _compute_cv(values)
    nudge = _cv_to_nudge(cv)

    cache[cache_key] = nudge
    logger.debug(
        f"CV layer | player={player_id} prop={prop_type} "
        f"L10={values} CV={cv:.3f if cv else 'N/A'} nudge={nudge:+.3f}"
    )
    return nudge


# ─────────────────────────────────────────────
# Main layer entry point
# ─────────────────────────────────────────────

def apply_cv_consistency_layer(props: list, season: int) -> list:
    """
    Layer 9: Apply CV-based consistency gate to all props.

    For each prop with a known player_id, fetches L10 game log,
    computes CV for the relevant stat, and nudges implied_prob.

    Args:
        props: list of prop dicts (must have implied_prob, player_id, prop_type)
        season: current MLB season year (e.g., 2026)

    Returns:
        Updated list of prop dicts with cv_nudge and adjusted implied_prob.
    """
    logger.info("Layer 9 — CV Consistency Gate starting...")
    cache = _load_cache()
    updated = 0

    for prop in props:
        try:
            player_id = prop.get("player_id") or prop.get("mlbam_id")
            prop_type = prop.get("prop_type", "").lower()

            if not player_id or not prop_type:
                prop["cv_nudge"] = 0.0
                prop["cv"] = None
                continue

            nudge = get_player_cv_nudge(
                player_id=int(player_id),
                prop_type=prop_type,
                season=season,
                cache=cache,
            )

            # Apply nudge to implied_prob — clamp to [0.01, 0.99]
            original = prop.get("implied_prob", 0.5)
            prop["cv_nudge"] = nudge
            prop["implied_prob"] = max(0.01, min(0.99, original + nudge))

            if nudge != 0.0:
                updated += 1

        except Exception as exc:
            logger.warning(f"CV layer error for prop {prop.get('description', '?')}: {exc}")
            prop["cv_nudge"] = 0.0

    _save_cache(cache)
    logger.info(f"Layer 9 — CV Consistency Gate complete. {updated}/{len(props)} props nudged.")
    return props


# ─────────────────────────────────────────────
# Dispatcher integration snippet
# ─────────────────────────────────────────────
#
# Add to live_dispatcher.py after Layer 8 (Marcel + Predict+) block:
#
#   # ── Layer 9: CV Consistency Gate ──────────────────────────────
#   try:
#       from cv_consistency_layer import apply_cv_consistency_layer
#       props = apply_cv_consistency_layer(props, season=CURRENT_SEASON)
#       logger.info("Layer 9 (CV consistency) applied.")
#   except Exception as e:
#       logger.warning(f"Layer 9 CV skipped (fallback): {e}")
#
# ─────────────────────────────────────────────


if __name__ == "__main__":
    # Quick smoke test
    logging.basicConfig(level=logging.DEBUG)
    test_props = [
        {
            "player_id": 592450,  # Gerrit Cole
            "prop_type": "pitcher_strikeouts",
            "implied_prob": 0.58,
            "description": "Gerrit Cole K Over 7.5",
        },
        {
            "player_id": 660271,  # Juan Soto
            "prop_type": "hits",
            "implied_prob": 0.62,
            "description": "Juan Soto Hits Over 0.5",
        },
    ]
    result = apply_cv_consistency_layer(test_props, season=2026)
    for p in result:
        print(
            f"{p['description']} | "
            f"CV nudge: {p['cv_nudge']:+.3f} | "
            f"Final prob: {p['implied_prob']:.4f}"
        )
