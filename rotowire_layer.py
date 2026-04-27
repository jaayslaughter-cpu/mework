"""
rotowire_layer.py  —  PR #441
Scrapes RotoWire MLB player props page for multi-book consensus lines.

Prop types extracted:
  strikeouts   → 32 pitchers, up to 9 books (median-filtered — BetRivers/CircaSports
                 post F5/segment lines which skew low; outliers beyond 2.0 of median dropped)
  earned_runs  → 33 pitchers, 5 books (all agree — no filtering needed)

Output dict keyed to sportsbook_reference_layer market names:
  "pitcher_strikeouts" → {player_name: {"line": float, "books": int, "raw": {book: line}}}
  "pitcher_er"         → {player_name: {"line": float, "books": int, "raw": {book: line}}}

Redis cache: 2h TTL under key "rw_props:{YYYY-MM-DD}"
Falls back to {} on any scrape/parse error.
"""

from __future__ import annotations

import json
import logging
import os
import re
import statistics
from datetime import date, datetime
from typing import Any

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
_URL = "https://www.rotowire.com/betting/mlb/player-props.php"
_CACHE_TTL = 7200  # 2 hours
_CACHE_KEY_PREFIX = "rw_props"

# Books that reliably post full-game lines (F5/segment books excluded by median filter)
_TRUSTED_BOOKS = {"draftkings", "fanduel", "mgm", "caesars", "fanatics", "hardrock", "thescore"}

# Prop type in RotoWire JS → internal pipeline name
_PROP_MAP: dict[str, str] = {
    "strikeouts": "pitcher_strikeouts",
    "er":         "pitcher_er",
}

# Max deviation from median to be considered a full-game line (filters F5 variants)
_OUTLIER_THRESHOLD = 2.0

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}


# ---------------------------------------------------------------------------
# Redis helpers (optional — graceful no-op when Redis unavailable)
# ---------------------------------------------------------------------------
def _get_redis():
    """Return a Redis client or None if unavailable."""
    try:
        import redis as _redis
        url = os.getenv("REDIS_URL", "redis://localhost:6379")
        client = _redis.Redis.from_url(url, decode_responses=True, socket_timeout=2)
        client.ping()
        return client
    except Exception:
        return None


def _cache_get(key: str) -> Any | None:
    r = _get_redis()
    if not r:
        return None
    try:
        raw = r.get(key)
        return json.loads(raw) if raw else None
    except Exception:
        return None


def _cache_set(key: str, value: Any, ttl: int) -> None:
    r = _get_redis()
    if not r:
        return
    try:
        r.setex(key, ttl, json.dumps(value))
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Core scrape + parse
# ---------------------------------------------------------------------------
def _extract_prop_array(text: str, prop_type: str) -> list[dict]:
    """
    Extract the JSON data array for a given prop type from RotoWire page HTML.
    Each prop section contains a JS block like:
        const prop = "strikeouts";
        ...
        data: [{...players...}]
    """
    # Find the script block for this prop type
    # Locate 'const prop = "strikeouts";' then find the next JSON array
    pattern = rf'const prop = "{re.escape(prop_type)}";'
    idx = text.find(f'const prop = "{prop_type}";')
    if idx < 0:
        return []

    # From that point, find the next JSON array start
    search_text = text[idx:idx + 500_000]
    array_match = re.search(r'\[\{"gameID"', search_text)
    if not array_match:
        return []

    array_start = array_match.start()
    chunk = search_text[array_start:]

    # Extract balanced JSON array
    depth, end = 0, 0
    for j, c in enumerate(chunk):
        if c == "[":
            depth += 1
        elif c == "]":
            depth -= 1
            if depth == 0:
                end = j + 1
                break

    if not end:
        return []

    try:
        return json.loads(chunk[:end])
    except json.JSONDecodeError as exc:
        logger.debug("RotoWire JSON parse error for %s: %s", prop_type, exc)
        return []


def _median_filter_lines(raw: dict[str, float]) -> dict[str, float]:
    """
    Drop books whose line deviates by more than _OUTLIER_THRESHOLD from the median.
    Guards against BetRivers/CircaSports posting F5/segment lines.
    """
    if len(raw) < 2:
        return raw
    vals = list(raw.values())
    med = statistics.median(vals)
    return {book: line for book, line in raw.items() if abs(line - med) <= _OUTLIER_THRESHOLD}


def _parse_player_lines(players: list[dict], prop_type: str, filter_outliers: bool) -> dict[str, dict]:
    """
    Build per-player line dict from raw RotoWire player records.

    Returns:
        {player_name: {"line": float, "books": int, "raw": {book: line}}}
    """
    result: dict[str, dict] = {}

    for p in players:
        name: str = p.get("name", "")
        if not name:
            continue

        # Collect lines across books
        raw: dict[str, float] = {}
        for key, val in p.items():
            if not val:
                continue
            # Keys look like "draftkings_strikeouts" / "fanduel_er"
            if not key.endswith(f"_{prop_type}"):
                continue
            book = key[: -(len(prop_type) + 1)]
            # Skip Under/Over odds (we only need lines for now)
            if "Under" in book or "Over" in book:
                continue
            try:
                raw[book] = float(val)
            except (ValueError, TypeError):
                continue

        if not raw:
            continue

        # Prefer trusted books; fall back to all if none qualify
        trusted = {b: v for b, v in raw.items() if b in _TRUSTED_BOOKS}
        working = trusted if trusted else raw

        if filter_outliers:
            working = _median_filter_lines(working)

        if not working:
            continue

        consensus = statistics.median(list(working.values()))
        result[name] = {
            "line": consensus,
            "books": len(working),
            "raw": working,
        }

    return result


def _scrape() -> dict[str, dict[str, dict]]:
    """
    Fetch the RotoWire props page and return:
        {"pitcher_strikeouts": {...}, "pitcher_er": {...}}
    """
    try:
        resp = requests.get(_URL, headers=_HEADERS, timeout=20)
        resp.raise_for_status()
    except Exception as exc:
        logger.warning("RotoWire fetch failed: %s", exc)
        return {}

    text = resp.text
    output: dict[str, dict[str, dict]] = {}

    for rw_key, pipeline_key in _PROP_MAP.items():
        players = _extract_prop_array(text, rw_key)
        if not players:
            logger.debug("RotoWire: no players for prop=%s", rw_key)
            continue

        filter_outliers = rw_key == "strikeouts"  # ER lines are already consistent
        parsed = _parse_player_lines(players, rw_key, filter_outliers)
        output[pipeline_key] = parsed
        logger.info(
            "RotoWire: prop=%s players=%d (median-filter=%s)",
            rw_key,
            len(parsed),
            filter_outliers,
        )

    return output


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def fetch_rotowire_props(force: bool = False) -> dict[str, dict[str, dict]]:
    """
    Return multi-book consensus lines from RotoWire, Redis-cached for 2h.

    Returns:
        {
          "pitcher_strikeouts": {
              "Kyle Harrison": {"line": 8.5, "books": 6, "raw": {"draftkings": 8.5, ...}},
              ...
          },
          "pitcher_er": {
              "Michael King": {"line": 3.5, "books": 5, "raw": {...}},
              ...
          }
        }
    """
    today = date.today().isoformat()
    cache_key = f"{_CACHE_KEY_PREFIX}:{today}"

    if not force:
        cached = _cache_get(cache_key)
        if cached:
            logger.debug("RotoWire cache hit: %s", cache_key)
            return cached

    data = _scrape()
    if data:
        _cache_set(cache_key, data, _CACHE_TTL)

    return data


def get_rotowire_line(player_name: str, pipeline_prop: str) -> float | None:
    """
    Convenience: return consensus line for a specific player + prop, or None.

    Args:
        player_name:   e.g. "Kyle Harrison"
        pipeline_prop: e.g. "pitcher_strikeouts" or "pitcher_er"
    """
    props = fetch_rotowire_props()
    prop_data = props.get(pipeline_prop, {})
    entry = prop_data.get(player_name)
    if entry:
        return entry["line"]

    # Fuzzy fallback: last-name match
    last = player_name.split()[-1].lower() if player_name else ""
    if last:
        for name, entry in prop_data.items():
            if name.split()[-1].lower() == last:
                return entry["line"]

    return None


# ---------------------------------------------------------------------------
# Smoke test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    data = fetch_rotowire_props(force=True)

    for market, players in data.items():
        print(f"\n=== {market} ({len(players)} players) ===")
        for name, info in list(players.items())[:8]:
            print(
                f"  {name:<25} line={info['line']:<5} books={info['books']} "
                f"raw={info['raw']}"
            )
