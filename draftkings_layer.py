"""
draftkings_layer.py — DraftKings sharp player prop lines
PR #521

Fetches MLB player prop data directly from the DraftKings public sportsbook API
(sportsbook-nash.draftkings.com) using curl_cffi TLS fingerprint spoofing.
No auth required. Datacenter-accessible (confirmed).

Prop types supported:
  hits_runs_rbis   → subcategory 17843 (milestone)
  strikeouts       → subcategory 17323 (milestone — pitcher Ks thrown)
  hitter_strikeouts → subcategory 17490 (milestone — batter Ks)
  hits             → subcategory 17320 (milestone)
  total_bases      → subcategory 17321 (milestone)
  pitching_outs    → subcategory 17413 (O/U with points field)

NOT available on DK (fall through to other tiers):
  hits_allowed, walks_allowed, earned_runs, home_runs (excluded), etc.

Data format:
  DK uses two formats:
  1. MILESTONE — label="N+", milestoneValue=N, trueOdds=decimal w/ vig
     For UD/PP line of L (half-increment L = N - 0.5):
       P(over L) = P(≥N) ≈ 1/trueOdds[N]
     Single-side milestone markets have minimal vig; no counter-side to de-vig against.

  2. O/U — label="Over"/"Under", outcomeType, points=line, trueOdds=decimal w/ vig
     De-vig via: P_true = P_raw / (P_raw_over + P_raw_under)

Redis cache:
  Key: dk_props_{date}_{sub_id}
  TTL: 2 hours
  Populated during 8:15 AM prefetch (job_predict_plus) and on first lookup.
"""

from __future__ import annotations

import logging
import time
import unicodedata
from typing import Optional
import re

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Subcategory ID map  — DK internal IDs for MLB player prop categories
# ---------------------------------------------------------------------------
_DK_SUBCATS: dict[str, int] = {
    "hits_runs_rbis":    17843,   # milestone
    "strikeouts":        17323,   # milestone (pitcher Ks thrown)
    "hitter_strikeouts": 17490,   # milestone (batter Ks) — sparse
    "hits":              17320,   # milestone
    "total_bases":       17321,   # milestone
    "pitching_outs":     17413,   # O/U with points field
}

# Subcategory IDs that use the MILESTONE format (N+ labels)
_MILESTONE_SUBS = {17843, 17323, 17490, 17320, 17321}

# Subcategory IDs that use standard O/U format (points field carries the line)
_OU_SUBS = {17413}

# DK API base
_DK_BASE = (
    "https://sportsbook-nash.draftkings.com/sites/US-VA-SB/api/sportscontent/"
    "controldata/league/leagueSubcategory/v1/markets"
)
_DK_LEAGUE = "84240"   # MLB
_DK_CACHE_TTL = 7200   # 2 hours

# In-memory fallback cache when Redis is unavailable
_mem_cache: dict[str, tuple[float, object]] = {}


# ---------------------------------------------------------------------------
# Name normalization
# ---------------------------------------------------------------------------

def _norm_name(name: str) -> str:
    """Lowercase, strip accents, remove suffixes like (WAS), collapse whitespace."""
    name = re.sub(r'\(.*?\)', '', name)                    # remove (TEAM) suffix
    name = unicodedata.normalize('NFD', name)
    name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
    name = name.lower().strip()
    name = re.sub(r'\s+', ' ', name)
    # Remove Jr./Sr./III suffixes
    name = re.sub(r'\s+(jr\.?|sr\.?|ii+|iv|v)$', '', name)
    return name


def _name_match(dk_name: str, target_name: str) -> bool:
    """Fuzzy match: exact norm, or last-name match, or last+first-initial."""
    dn = _norm_name(dk_name)
    tn = _norm_name(target_name)
    if dn == tn:
        return True
    # Last-name fallback
    dn_last = dn.split()[-1] if dn else ''
    tn_last = tn.split()[-1] if tn else ''
    if dn_last and tn_last and dn_last == tn_last:
        # Also check first initial to avoid false positives on common last names
        dn_parts = dn.split()
        tn_parts = tn.split()
        if len(dn_parts) >= 1 and len(tn_parts) >= 1:
            if dn_parts[0][:1] == tn_parts[0][:1]:
                return True
    return False


# ---------------------------------------------------------------------------
# HTTP fetch with curl_cffi
# ---------------------------------------------------------------------------

def _build_url(sub_id: int) -> str:
    sub = str(sub_id)
    evt_filter = (
        f"$filter=leagueId eq '{_DK_LEAGUE}' AND "
        f"clientMetadata/Subcategories/any(s: s/Id eq '{sub}')"
    )
    mkt_filter = (
        f"$filter=clientMetadata/subCategoryId eq '{sub}' AND "
        f"tags/all(t: t ne 'SportcastBetBuilder')"
    )
    import urllib.parse
    return (
        f"{_DK_BASE}?isBatchable=false"
        f"&templateVars={_DK_LEAGUE}%2C{sub}"
        f"&eventsQuery={urllib.parse.quote(evt_filter)}"
        f"&marketsQuery={urllib.parse.quote(mkt_filter)}"
        f"&include=Events&entity=events"
    )


def _fetch_sub_raw(sub_id: int, timeout: int = 15) -> Optional[dict]:
    """Fetch one DK subcategory via curl_cffi. Returns parsed JSON or None."""
    try:
        from curl_cffi import requests as cffi_requests
    except ImportError:
        logger.warning("[DK] curl_cffi not installed — skipping DK layer")
        return None

    url = _build_url(sub_id)
    headers = {
        "Accept": "application/json",
        "Referer": "https://sportsbook.draftkings.com/",
        "Origin": "https://sportsbook.draftkings.com",
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/110.0.0.0 Safari/537.36"
        ),
    }
    try:
        resp = cffi_requests.get(url, headers=headers, impersonate="chrome110", timeout=timeout)
        if resp.status_code == 200:
            return resp.json()
        logger.warning("[DK] sub %s HTTP %s", sub_id, resp.status_code)
        return None
    except Exception as exc:
        logger.warning("[DK] fetch sub %s error: %s", sub_id, exc)
        return None


# ---------------------------------------------------------------------------
# Parse raw API response into unified prop records
# ---------------------------------------------------------------------------

def _parse_raw(data: dict, sub_id: int, prop_type: str) -> list[dict]:
    """
    Parse DK API response into list of:
      {player_name, prop_type, line, over_prob, under_prob, source}

    For milestone format (N+):
      over_prob at line L = N - 0.5  →  1 / trueOdds[milestone N]
      under_prob = 1 - over_prob  (single-side milestone; minimal vig)

    For O/U format:
      line = selection.points
      De-vigged via additive method: P_true = P_raw / total_implied
    """
    if not data:
        return []

    markets = data.get('markets', [])
    selections = data.get('selections', [])

    # Build market → selections map
    sel_map: dict[str, list[dict]] = {}
    for s in selections:
        mid = str(s.get('marketId', ''))
        sel_map.setdefault(mid, []).append(s)

    results = []
    is_milestone = sub_id in _MILESTONE_SUBS

    for mkt in markets:
        mid = str(mkt.get('id', ''))
        mkt_sels = sel_map.get(mid, [])
        if not mkt_sels:
            continue

        # Extract player name from first selection's participants
        player_name = _extract_player_name(mkt, mkt_sels)
        if not player_name:
            continue

        if is_milestone:
            # Build milestone → trueOdds map
            milestone_odds: dict[int, float] = {}
            for s in mkt_sels:
                mv = s.get('milestoneValue')
                to = s.get('trueOdds')
                if mv is not None and to and to > 1.0:
                    milestone_odds[int(mv)] = float(to)

            # Convert to O/U props at half-increment lines
            for n, odds in milestone_odds.items():
                line = n - 0.5  # over N-0.5 ↔ P(≥N)
                raw_over = 1.0 / odds
                # Cap probability between 0.01 and 0.99
                over_prob = min(0.99, max(0.01, raw_over))
                under_prob = 1.0 - over_prob
                results.append({
                    "player_name": player_name,
                    "prop_type": prop_type,
                    "line": line,
                    "over_prob": over_prob,
                    "under_prob": under_prob,
                    "source": "draftkings",
                })

        else:
            # Standard O/U format — group by line (points field)
            ou_by_line: dict[float, dict] = {}
            for s in mkt_sels:
                pts = s.get('points')
                if pts is None:
                    continue
                pts = float(pts)
                label = (s.get('outcomeType') or s.get('label') or '').lower()
                to = s.get('trueOdds')
                if not to or to <= 1.0:
                    continue
                ou_by_line.setdefault(pts, {})[label] = float(to)

            for line, sides in ou_by_line.items():
                over_raw = sides.get('over')
                under_raw = sides.get('under')
                if not over_raw or not under_raw:
                    continue
                # Additive de-vig
                p_over_raw = 1.0 / over_raw
                p_under_raw = 1.0 / under_raw
                total = p_over_raw + p_under_raw
                over_prob = min(0.99, max(0.01, p_over_raw / total))
                under_prob = 1.0 - over_prob
                results.append({
                    "player_name": player_name,
                    "prop_type": prop_type,
                    "line": line,
                    "over_prob": over_prob,
                    "under_prob": under_prob,
                    "source": "draftkings",
                })

    return results


def _extract_player_name(mkt: dict, sels: list[dict]) -> Optional[str]:
    """Extract player name from DK market/selection. Selections' participants are most reliable."""
    for s in sels:
        for p in s.get('participants', []):
            if p.get('type') == 'Player':
                name = p.get('name') or p.get('seoIdentifier') or ''
                if name:
                    # Strip team suffix like " (WAS)"
                    name = re.sub(r'\s*\(.*?\)\s*$', '', name).strip()
                    return name
    # Fallback: parse from market name (e.g. "Jacob deGrom Strikeouts Thrown")
    mkt_name = mkt.get('name', '')
    for suffix in [
        ' Strikeouts Thrown', ' Hits + Runs + RBIs', ' Outs O/U',
        ' Hits', ' Total Bases', ' RBIs', ' Home Runs', ' Strikeouts',
    ]:
        if mkt_name.endswith(suffix):
            return mkt_name[: -len(suffix)].strip()
    return None


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def _cache_key(date_str: str, sub_id: int) -> str:
    return f"dk_props_{date_str}_{sub_id}"


def _get_cached(redis_client, key: str) -> Optional[list]:
    """Try Redis then in-memory cache."""
    import json
    if redis_client:
        try:
            raw = redis_client.get(key)
            if raw:
                return json.loads(raw)
        except Exception:
            pass
    # In-memory fallback
    entry = _mem_cache.get(key)
    if entry:
        expires_at, data = entry
        if time.time() < expires_at:
            return data
    return None


def _set_cached(redis_client, key: str, data: list, ttl: int = _DK_CACHE_TTL) -> None:
    import json
    serialized = json.dumps(data)
    if redis_client:
        try:
            redis_client.setex(key, ttl, serialized)
            return
        except Exception:
            pass
    _mem_cache[key] = (time.time() + ttl, data)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def prefetch_dk_props(date_str: str, redis_client=None) -> dict[str, int]:
    """
    Fetch all supported DK prop categories and cache them.
    Called during 8:15 AM prefetch job.

    Returns dict of {prop_type: record_count} for logging.
    """
    summary: dict[str, int] = {}
    for prop_type, sub_id in _DK_SUBCATS.items():
        key = _cache_key(date_str, sub_id)
        if _get_cached(redis_client, key) is not None:
            logger.info("[DK] sub %s (%s) already cached", sub_id, prop_type)
            continue
        raw = _fetch_sub_raw(sub_id)
        if raw is None:
            summary[prop_type] = -1
            continue
        records = _parse_raw(raw, sub_id, prop_type)
        _set_cached(redis_client, key, records)
        summary[prop_type] = len(records)
        logger.info("[DK] cached %d records for %s (sub %s)", len(records), prop_type, sub_id)
    return summary


def get_dk_prob(
    player_name: str,
    prop_type: str,
    line: float,
    side: str,           # "over" or "under"
    date_str: str,
    redis_client=None,
) -> Optional[float]:
    """
    Return de-vigged DraftKings implied probability for (player, prop_type, line, side).
    Returns None if no matching DK line found.

    side: "higher"/"over" → over_prob; "lower"/"under" → under_prob
    """
    sub_id = _DK_SUBCATS.get(prop_type)
    if sub_id is None:
        return None  # prop type not supported by DK layer

    key = _cache_key(date_str, sub_id)
    records = _get_cached(redis_client, key)

    # Lazy fetch if not cached
    if records is None:
        raw = _fetch_sub_raw(sub_id)
        if raw is None:
            return None
        records = _parse_raw(raw, sub_id, prop_type)
        _set_cached(redis_client, key, records)

    side_key = "over_prob" if side.lower() in ("over", "higher", "h") else "under_prob"
    target_line = float(line)

    # Find matching record: player name match + line within 0.25 tolerance
    best: Optional[dict] = None
    for rec in records:
        if not _name_match(rec['player_name'], player_name):
            continue
        if abs(rec['line'] - target_line) <= 0.26:
            best = rec
            break

    if best is None:
        logger.debug("[DK] no match for %s %s %.1f %s", player_name, prop_type, line, side)
        return None

    prob = best.get(side_key)
    logger.debug(
        "[DK] sharp_prob=%.3f for %s %s %.1f %s (DK line=%.1f)",
        prob, player_name, prop_type, line, side, best['line']
    )
    return prob


def get_dk_all_props(
    prop_type: str,
    date_str: str,
    redis_client=None,
) -> list[dict]:
    """
    Return all cached DK records for a given prop_type.
    Useful for bulk enrichment (e.g. building the DataHub).
    """
    sub_id = _DK_SUBCATS.get(prop_type)
    if sub_id is None:
        return []
    key = _cache_key(date_str, sub_id)
    records = _get_cached(redis_client, key)
    if records is None:
        raw = _fetch_sub_raw(sub_id)
        if raw is None:
            return []
        records = _parse_raw(raw, sub_id, prop_type)
        _set_cached(redis_client, key, records)
    return records
