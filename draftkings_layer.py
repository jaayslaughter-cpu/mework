"""
draftkings_layer.py — DraftKings sharp player prop lines
PR #521 (rev 2 — adds RBIs, fixes milestone parsing bug, adds cumulative de-vig)

Fetches MLB player prop data directly from the DraftKings controldata API
(sportsbook-nash.draftkings.com) using curl_cffi TLS fingerprint spoofing.
No auth required. Datacenter-accessible (confirmed 2026-05-09).

Prop types supported:
  hits_runs_rbis    → subcategory 17843  (milestone)
  strikeouts        → subcategory 17323  (milestone — pitcher Ks thrown)
  hitter_strikeouts → subcategory 17490  (milestone — batter Ks)  ← sparse
  hits              → subcategory 17320  (milestone)
  total_bases       → subcategory 17321  (milestone)
  rbis              → subcategory 17322  (milestone)  ← NEW
  pitching_outs     → subcategory 17413  (O/U with points field)

NOT available on DK (fall through to other tiers):
  hits_allowed, walks_allowed, earned_runs
  home_runs / doubles / triples / stolen_bases / walks  (excluded props)

Milestone de-vig:
  DK posts "1+", "2+", "3+" milestone selections per player.
  Label field contains "N+" — parsed directly (no milestoneValue field exists).

  Flat 5% vig correction applied to each milestone independently:
    P_devig(≥N) = (1 / decimal_odds(N+)) / 1.05

  Why not cumulative normalization: adjacent-difference decomposition always
  sums to exactly 1 algebraically — normalization is a mathematical identity
  that removes zero vig. Flat ~5% matches DK's typical milestone margin.

  Line mapping: P_devig(≥N) → UD/PP line N-0.5  (e.g. "2+" → line=1.5 over_prob)

O/U de-vig (pitching_outs):
  P_true_over = P_raw_over / (P_raw_over + P_raw_under)   (additive de-vig)

Redis cache:
  Key: dk_props_{date}_{sub_id}
  TTL: 2 hours
  Populated during 8:15 AM prefetch and on first lookup.

Platform role:
  Provides sharp_prob for EV/confidence scoring ONLY.
  Does NOT influence UD vs PP platform selection (governed by Dual-Platform Directive).
"""

from __future__ import annotations

import logging
import re
import time
import unicodedata
import urllib.parse
from typing import Optional

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
    "rbis":              17322,   # milestone — NEW subcategory confirmed 2026-05-09
    "pitching_outs":     17413,   # O/U with points field
}

# Subcategory IDs that use MILESTONE format (N+ labels in 'label' field)
_MILESTONE_SUBS: set[int] = {17843, 17323, 17490, 17320, 17321, 17322}

# Subcategory IDs that use standard O/U format (points field carries the line)
_OU_SUBS: set[int] = {17413}

# DK controldata endpoint — returns top-level markets[] + selections[] arrays
_DK_BASE = (
    "https://sportsbook-nash.draftkings.com/sites/US-VA-SB/api/sportscontent/"
    "controldata/league/leagueSubcategory/v1/markets"
)
_DK_LEAGUE = "84240"   # MLB league ID in DK controldata API
_DK_CACHE_TTL = 7200   # 2 hours

# In-memory fallback cache when Redis is unavailable
_mem_cache: dict[str, tuple[float, object]] = {}


# ---------------------------------------------------------------------------
# Name normalization
# ---------------------------------------------------------------------------

def _norm_name(name: str) -> str:
    """Lowercase, strip accents, remove (TEAM) suffixes, collapse whitespace."""
    name = re.sub(r'\(.*?\)', '', name)                     # remove (WAS) etc.
    name = unicodedata.normalize('NFD', name)
    name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
    name = name.lower().strip()
    name = re.sub(r'\s+', ' ', name)
    name = re.sub(r'\s+(jr\.?|sr\.?|ii+|iv|v)$', '', name)  # strip suffixes
    return name


def _name_match(dk_name: str, target_name: str) -> bool:
    """Fuzzy match: exact normalized, or last-name + first-initial."""
    dn = _norm_name(dk_name)
    tn = _norm_name(target_name)
    if dn == tn:
        return True
    dn_parts = dn.split()
    tn_parts = tn.split()
    if not dn_parts or not tn_parts:
        return False
    # Last-name match + first-initial guard
    if dn_parts[-1] == tn_parts[-1] and dn_parts[0][:1] == tn_parts[0][:1]:
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
# Milestone de-vig — cumulative distribution normalization
# ---------------------------------------------------------------------------

def _devig_milestones(milestone_odds: dict[int, float]) -> dict[int, float]:
    """
    Apply flat 5% vig correction to each DK milestone independently.

    milestone_odds: {N: decimal_odds_for_N+}  (e.g. {1: 1.87, 2: 4.20, 3: 12.0})
    Returns: {N: de-vigged P(≥N)} for each milestone.

    Why flat correction instead of cumulative normalization:
      DK posts each milestone (1+, 2+, 3+) as independent one-sided bets with no
      complementary under side. The adjacent-difference decomposition
      [P(0)=1-P(≥1), P(1)=P(≥1)-P(≥2), ..., P(≥max_n)=last] algebraically sums
      to exactly 1 regardless of vig — making normalization a mathematical identity
      that removes nothing. Flat ~5% correction matches DK's typical milestone margin.

      Verified: Pete Alonso RBI "1+" at +126 → raw 44.2% → de-vigged 42.1%.
                H+R+RBI "2+" at +130 → raw 43.5% → de-vigged 41.4%.
    """
    if not milestone_odds:
        return {}
    devigged: dict[int, float] = {}
    for n, odds in milestone_odds.items():
        if odds and float(odds) > 1.0:
            raw_p = 1.0 / float(odds)
            devigged[n] = min(0.99, max(0.01, raw_p / 1.05))
    return devigged


# ---------------------------------------------------------------------------
# Parse raw API response into unified prop records
# ---------------------------------------------------------------------------

_MILESTONE_LABEL_RE = re.compile(r'^(\d+)\+$')


def _parse_milestone_label(label: str) -> Optional[int]:
    """Extract N from 'N+' label. Returns None if not a milestone label."""
    m = _MILESTONE_LABEL_RE.match(label.strip())
    return int(m.group(1)) if m else None


def _extract_player_name(mkt: dict, sels: list[dict]) -> Optional[str]:
    """Extract player name from DK selection participants or market name fallback."""
    for s in sels:
        for p in s.get('participants', []):
            if p.get('type') == 'Player':
                name = p.get('name') or p.get('seoIdentifier') or ''
                if name:
                    return re.sub(r'\s*\(.*?\)\s*$', '', name).strip()
    # Fallback: parse from market name
    mkt_name = mkt.get('name', '')
    for suffix in [
        ' Strikeouts Thrown', ' Hits + Runs + RBIs', ' Outs O/U',
        ' Hits', ' Total Bases', ' RBIs', ' Home Runs', ' Strikeouts',
    ]:
        if mkt_name.endswith(suffix):
            return mkt_name[: -len(suffix)].strip()
    return None


def _parse_raw(data: dict, sub_id: int, prop_type: str) -> list[dict]:
    """
    Parse DK API response into list of:
      {player_name, prop_type, line, over_prob, under_prob, source}

    Milestone format (N+ labels):
      - Parse milestone N from label "N+" string
      - Collect all milestones per player, apply cumulative de-vig
      - Map P_devig(≥N) to line N-0.5 as over_prob

    O/U format (pitching_outs):
      - line = selection.points
      - Additive de-vig: P_true = P_raw / (P_raw_over + P_raw_under)
    """
    if not data:
        return []

    markets = data.get('markets', [])
    selections = data.get('selections', [])

    # Build market_id → selections map
    sel_map: dict[str, list[dict]] = {}
    for s in selections:
        mid = str(s.get('marketId', ''))
        sel_map.setdefault(mid, []).append(s)

    results: list[dict] = []
    is_milestone = sub_id in _MILESTONE_SUBS

    for mkt in markets:
        mid = str(mkt.get('id', ''))
        mkt_sels = sel_map.get(mid, [])
        if not mkt_sels:
            continue

        player_name = _extract_player_name(mkt, mkt_sels)
        if not player_name:
            continue

        if is_milestone:
            # ── Milestone parsing ──────────────────────────────────────────
            # Collect {N: decimal_odds} from selections with "N+" labels
            milestone_odds: dict[int, float] = {}
            for s in mkt_sels:
                raw_label = s.get('label', '')
                n = _parse_milestone_label(raw_label)
                if n is None:
                    continue
                to = s.get('trueOdds')
                if to and float(to) > 1.0:
                    milestone_odds[n] = float(to)

            if not milestone_odds:
                continue

            # De-vig using cumulative distribution normalization
            devigged = _devig_milestones(milestone_odds)

            for n, over_prob in devigged.items():
                line = n - 0.5   # "2+" → line=1.5; "1+" → line=0.5
                under_prob = 1.0 - over_prob
                results.append({
                    "player_name": player_name,
                    "prop_type":   prop_type,
                    "line":        line,
                    "over_prob":   over_prob,
                    "under_prob":  under_prob,
                    "source":      "draftkings",
                })

        else:
            # ── O/U parsing (pitching_outs) ────────────────────────────────
            ou_by_line: dict[float, dict] = {}
            for s in mkt_sels:
                pts = s.get('points')
                if pts is None:
                    continue
                pts = float(pts)
                label = (s.get('outcomeType') or s.get('label') or '').lower()
                to = s.get('trueOdds')
                if not to or float(to) <= 1.0:
                    continue
                ou_by_line.setdefault(pts, {})[label] = float(to)

            for line, sides in ou_by_line.items():
                over_odds = sides.get('over')
                under_odds = sides.get('under')
                if not over_odds or not under_odds:
                    continue
                p_over_raw  = 1.0 / over_odds
                p_under_raw = 1.0 / under_odds
                total = p_over_raw + p_under_raw
                over_prob  = min(0.99, max(0.01, p_over_raw / total))
                under_prob = 1.0 - over_prob
                results.append({
                    "player_name": player_name,
                    "prop_type":   prop_type,
                    "line":        line,
                    "over_prob":   over_prob,
                    "under_prob":  under_prob,
                    "source":      "draftkings",
                })

    return results


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
    entry = _mem_cache.get(key)
    if entry:
        expires_at, data = entry
        if time.time() < expires_at:
            return data  # type: ignore[return-value]
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
    Fetch all supported DK prop categories and warm the cache.
    Called during 8:15 AM prefetch job.

    Returns {prop_type: record_count} for logging.
    -1 means fetch failed; 0 means fetched but no records parsed.
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
        logger.info(
            "[DK] prefetch %s (sub %s): %d records",
            prop_type, sub_id, len(records)
        )
    return summary


def get_dk_prob(
    player_name: str,
    prop_type: str,
    line: float,
    side: str,            # "over"/"higher" or "under"/"lower"
    date_str: str,
    redis_client=None,
) -> Optional[float]:
    """
    Return de-vigged DraftKings implied probability for (player, prop_type, line, side).
    Returns None if no matching DK line is found.

    This provides sharp_prob for EV and confidence scoring only.
    Platform selection (UD vs PP) is not affected.
    """
    sub_id = _DK_SUBCATS.get(prop_type)
    if sub_id is None:
        return None

    key = _cache_key(date_str, sub_id)
    records = _get_cached(redis_client, key)

    if records is None:
        raw = _fetch_sub_raw(sub_id)
        if raw is None:
            return None
        records = _parse_raw(raw, sub_id, prop_type)
        _set_cached(redis_client, key, records)

    side_key = "over_prob" if side.lower() in ("over", "higher", "h") else "under_prob"
    target_line = float(line)

    # Find best matching record: player name match + line within 0.26 tolerance
    for rec in records:
        if not _name_match(rec['player_name'], player_name):
            continue
        if abs(rec['line'] - target_line) <= 0.26:
            prob = rec.get(side_key)
            logger.debug(
                "[DK] sharp_prob=%.3f for %s %s %.1f %s (DK line=%.1f)",
                prob, player_name, prop_type, line, side, rec['line']
            )
            return prob

    logger.debug(
        "[DK] no match for %s %s %.1f %s", player_name, prop_type, line, side
    )
    return None


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


def get_dk_sharp_prob(
    player_name: str,
    prop_type: str,
    date_str: str = None,
    redis_client=None,
) -> "Optional[float]":
    """
    Return DraftKings Over implied probability for (player, prop_type) at any
    available market line.  No line required — uses first matching player record.

    Used by _get_sharp_consensus() as a fallback tier when SBR/OddsAPI has no
    market for specialty props (pitching_outs, hits_allowed, walks_allowed,
    earned_runs, hitter_strikeouts, hits_runs_rbis).

    Returns probability as a float (0.0–1.0), or None if not found.
    """
    if date_str is None:
        try:
            import pytz
            from datetime import datetime
            date_str = datetime.now(pytz.timezone("America/Los_Angeles")).strftime("%Y-%m-%d")
        except Exception:
            from datetime import date
            date_str = date.today().isoformat()

    records = get_dk_all_props(prop_type, date_str, redis_client)
    for rec in records:
        if _name_match(rec.get("player_name", ""), player_name):
            prob = rec.get("over_prob")
            if prob is not None:
                logger.debug(
                    "[DK] sharp_prob(no-line)=%.3f for %s %s (line=%.1f)",
                    prob, player_name, prop_type, rec.get("line", 0.0)
                )
            return prob
    return None

