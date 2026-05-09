"""
player_id_resolver.py
=====================
Resolves player names to MLBAM player IDs using two tiers:

  Tier 0: Static map built from bundled Statcast CSVs (1200+ players, instant)
  Tier 1: MLB Stats API people/search (free, Railway-safe, ~200ms per call)

This is the critical fix for the "league average" problem.
Every data tier (statcast_static_layer, mlb_stats_layer, career stats) is
keyed by MLBAM player_id. Underdog props arrive with only a player name.
Without resolving name → MLBAM, every player not in today's confirmed
lineup cache (58 batters + 6 pitchers) gets league-average stats.

Usage (in prop_enrichment_layer.py):
    from player_id_resolver import resolve_player_id
    mlbam_id = resolve_player_id("Kevin Gausman")  # → 592332
    mlbam_id = resolve_player_id("Shohei Ohtani")  # → 660271
    mlbam_id = resolve_player_id("UNKNOWN PLAYER") # → None
"""
from __future__ import annotations

import csv
import logging
import os
import re
import unicodedata
from typing import Optional

logger = logging.getLogger(__name__)

_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "statcast")

# ── Static name→MLBAM map ────────────────────────────────────────────────────
_NAME_TO_ID:  dict[str, int] = {}
_ID_TO_NAME:  dict[int, str] = {}
_STATIC_LOADED = False

# ── Live API cache ────────────────────────────────────────────────────────────
_API_CACHE:   dict[str, Optional[int]] = {}   # norm_name → mlbam_id or None


def _norm(s: str) -> str:
    nfkd = unicodedata.normalize("NFKD", str(s))
    ascii_s = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"[^a-z ]", "", ascii_s.lower()).strip()


def _load_static_map() -> None:
    global _STATIC_LOADED
    if _STATIC_LOADED:
        return

    csv_configs = [
        ("percentile_rankings.csv",          "player_name",          "player_id",  True),
        ("percentile_rankings-pitchers.csv",  "player_name",          "player_id",  True),
        ("expected_stats.csv",               "last_name, first_name", "player_id",  True),
        ("expected-stats-pitchers.csv",      "last_name, first_name", "player_id",  True),
        ("bat-tracking.csv",                 "name",                 "id",          False),
        ("bat-tracking-swing-path.csv",      "name",                 "id",          False),
        ("exit_velocity.csv",               "last_name, first_name", "player_id",  True),
        ("sprint_speed.csv",                "last_name, first_name", "player_id",  True),
        ("pitch-arsenal-stats-pitchers.csv", "last_name, first_name", "player_id",  True),
        ("pitch-arsenal-stats-batters.csv",  "last_name, first_name", "player_id",  True),
        ("swing-take.csv",                  "last_name, first_name", "player_id",  True),
        ("batted-ball.csv",                 "name",                 "id",          False),
        ("spin-direction.csv",              "last_name, first_name", "player_id",  True),
        ("pitcher_arm_angles.csv",          "pitcher_name",         "pitcher",     False),
    ]

    total = 0
    for fname, name_col, id_col, is_last_first in csv_configs:
        path = os.path.join(_DATA_DIR, fname)
        if not os.path.exists(path):
            continue
        try:
            with open(path, encoding="utf-8-sig") as f:
                for row in csv.DictReader(f):
                    raw_id  = (row.get(id_col) or "").strip()
                    raw_name = (row.get(name_col) or "").strip()
                    if not raw_id or not raw_name or not raw_id.isdigit():
                        continue
                    if is_last_first and "," in raw_name:
                        last, first = raw_name.split(",", 1)
                        full = f"{first.strip()} {last.strip()}"
                    else:
                        full = raw_name
                    key = _norm(full)
                    pid = int(raw_id)
                    if key and pid and key not in _NAME_TO_ID:
                        _NAME_TO_ID[key] = pid
                        _ID_TO_NAME[pid] = full
                        total += 1
        except Exception:
            pass

    _STATIC_LOADED = True
    logger.info("[PlayerIDResolver] Static map: %d players loaded from Statcast CSVs", total)



# ── Tier 1.5: Chadwick Bureau registry ───────────────────────────────────────
# ~150k player rows; key_mlbam maps to MLBAM IDs.
# Downloaded once at startup and cached in Redis (48h TTL) as a compact dict.
# URL: https://raw.githubusercontent.com/chadwickbureau/register/master/data/people.csv
_CHADWICK_LOADED = False
_CHADWICK_CACHE: dict[str, int] = {}   # norm_name → mlbam_id
_CHADWICK_REDIS_KEY = "chadwick_name_map_v1"
_CHADWICK_TTL = 172800   # 48h


def _get_redis_for_chadwick():
    try:
        import redis as _r
        url = __import__("os").environ.get("REDIS_URL") or __import__("os").environ.get("REDIS_PUBLIC_URL")
        if not url:
            return None
        return _r.from_url(url, decode_responses=True, socket_connect_timeout=3)
    except Exception:
        return None


def _load_chadwick() -> None:
    """Download Chadwick register CSV and build name→MLBAM map.
    
    Checks Redis first (48h TTL), then fetches the CSV from GitHub raw.
    Silently skips on any network/parse error — system degrades to MLB API.
    """
    global _CHADWICK_LOADED, _CHADWICK_CACHE
    if _CHADWICK_LOADED:
        return

    # Try Redis first
    r = _get_redis_for_chadwick()
    if r:
        try:
            cached = r.get(_CHADWICK_REDIS_KEY)
            if cached:
                import json as _j
                _CHADWICK_CACHE = _j.loads(cached)
                _CHADWICK_LOADED = True
                logger.info(
                    "[PlayerIDResolver] Chadwick: %d players loaded from Redis cache",
                    len(_CHADWICK_CACHE),
                )
                return
        except Exception:
            pass

    # Download from GitHub raw (Chadwick Bureau register)
    try:
        import csv as _csv
        import io
        import requests as _req

        resp = _req.get(
            "https://raw.githubusercontent.com/chadwickbureau/register/master/data/people.csv",
            timeout=30,
            headers={"Accept": "text/csv"},
        )
        if resp.status_code != 200:
            logger.debug("[PlayerIDResolver] Chadwick download failed: HTTP %d", resp.status_code)
            _CHADWICK_LOADED = True   # Mark as attempted to avoid repeated failures
            return

        reader = _csv.DictReader(io.StringIO(resp.text))
        count = 0
        for row in reader:
            mlbam_raw = row.get("key_mlbam", "").strip()
            first     = row.get("name_first", "").strip()
            last      = row.get("name_last",  "").strip()
            if not mlbam_raw or not mlbam_raw.isdigit():
                continue
            if not first and not last:
                continue
            full = f"{first} {last}".strip()
            key  = _norm(full)
            pid  = int(mlbam_raw)
            if key and pid and key not in _CHADWICK_CACHE:
                _CHADWICK_CACHE[key] = pid
                count += 1

        # Store in Redis for next startup
        if r and _CHADWICK_CACHE:
            try:
                import json as _j
                r.setex(_CHADWICK_REDIS_KEY, _CHADWICK_TTL, _j.dumps(_CHADWICK_CACHE))
            except Exception:
                pass

        _CHADWICK_LOADED = True
        logger.info("[PlayerIDResolver] Chadwick: %d players loaded from CSV", count)

    except Exception as exc:
        logger.debug("[PlayerIDResolver] Chadwick load failed: %s", exc)
        _CHADWICK_LOADED = True   # Don't retry on this startup


def _chadwick_lookup(player_name: str) -> "Optional[int]":
    """Look up MLBAM ID from Chadwick registry.  Returns None on miss."""
    _load_chadwick()
    if not _CHADWICK_CACHE:
        return None
    key = _norm(player_name)
    pid = _CHADWICK_CACHE.get(key)
    if pid:
        return pid
    # Last-name fallback: try "<first> <last>" rearrangements
    parts = key.split()
    if len(parts) == 2:
        swapped = f"{parts[1]} {parts[0]}"
        pid = _CHADWICK_CACHE.get(swapped)
    return pid



def _api_lookup(player_name: str) -> Optional[int]:
    """Look up MLBAM ID via MLB Stats API people/search — free, no key."""
    key = _norm(player_name)
    if key in _API_CACHE:
        return _API_CACHE[key]

    try:
        import requests as _req  # noqa: PLC0415
        resp = _req.get(
            "https://statsapi.mlb.com/api/v1/people/search",
            params={"names": player_name, "sportId": 1, "limit": 5},
            timeout=8,
        )
        if resp.status_code != 200:
            _API_CACHE[key] = None
            return None

        people = resp.json().get("people", [])
        # Find best match — exact full name match preferred
        for p in people:
            full_api = _norm(f"{p.get('firstName', '')} {p.get('lastName', '')}")
            if full_api == key:
                pid = int(p["id"])
                _API_CACHE[key] = pid
                _NAME_TO_ID[key] = pid
                logger.debug("[PlayerIDResolver] API resolved %s → %d", player_name, pid)
                return pid

        # Fallback: first result if only one returned
        if len(people) == 1:
            pid = int(people[0]["id"])
            _API_CACHE[key] = pid
            _NAME_TO_ID[key] = pid
            return pid

        _API_CACHE[key] = None
        return None

    except Exception as exc:
        logger.debug("[PlayerIDResolver] API lookup failed for %s: %s", player_name, exc)
        _API_CACHE[key] = None
        return None


def resolve_player_id(
    player_name: str,
    hint_id: Optional[int] = None,
    use_api: bool = True,
) -> Optional[int]:
    """
    Resolve a player name to MLBAM player_id.

    Args:
        player_name: Full player name (e.g. "Kevin Gausman")
        hint_id:     If the caller already has a non-MLBAM id hint, ignored
        use_api:     Whether to fall through to MLB Stats API when CSV misses

    Returns:
        MLBAM player_id (int) or None if unresolvable
    """
    if not player_name:
        return None

    _load_static_map()
    key = _norm(player_name)

    # Tier 0: static CSV map
    pid = _NAME_TO_ID.get(key)
    if pid:
        return pid

    # Tier 1.5: Chadwick Bureau registry (~150k players, Redis-cached 48h)
    # Covers fringe call-ups not in Statcast leaderboards
    chadwick_pid = _chadwick_lookup(player_name)
    if chadwick_pid:
        _NAME_TO_ID[key] = chadwick_pid   # backfill static map
        return chadwick_pid

    # Tier 1: MLB Stats API (live, ~200ms, Railway-safe)
    if use_api:
        return _api_lookup(player_name)

    return None


def warm_static_map() -> int:
    """Pre-load the static map at startup. Returns player count."""
    _load_static_map()
    return len(_NAME_TO_ID)


def warm_chadwick() -> int:
    """Pre-load the Chadwick registry in a background thread at startup.
    
    Called from orchestrator startup so the 8:30 AM dispatch window
    never blocks on a cold Chadwick download.
    Returns number of players loaded.
    """
    _load_chadwick()
    return len(_CHADWICK_CACHE)
