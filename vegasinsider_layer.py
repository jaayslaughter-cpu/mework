"""
vegasinsider_layer.py  —  PR #440
===================================
Scrapes VegasInsider player-props pages (server-rendered HTML, no JS renderer needed)
for multi-book Over odds on MLB strikeouts.

Data pipeline role
------------------
  Tier 2 in sportsbook_reference_layer for prop_type == "strikeouts" only:
    OddsAPI  →  VegasInsider (this file)  →  Covers  →  Pinnacle direct  →  DraftEdge
                                                        → Action Network → TheRundown

What we extract per pitcher
---------------------------
  - Consensus line: mode line across Bet365 / BetMGM / DraftKings / Caesars / FanDuel
  - vi_over_prob: average de-vigged Over probability at consensus line
  - book_count: number of sportsbooks that posted the consensus line
  - raw per-book lines + odds (stored in "books" dict for diagnostics)

De-vig method
-------------
  We only receive the Over side from each book (no Under posted).
  We apply a 4.5% flat vig correction (median market hold for this market):
      fair_prob = raw_over_prob - VIG_HALF
  This is intentionally conservative — these feed as one signal alongside
  OddsAPI and other layers, not as a sole source.

Caching
-------
  Redis key: vi_strikeouts  (15-minute TTL)
  Falls back gracefully if Redis unavailable or page returns unexpected structure.

Usage
-----
  from vegasinsider_layer import VegasInsiderLayer
  vi = VegasInsiderLayer()
  data = vi.get_strikeouts_consensus()
  # → {"Chris Sale": {"consensus_line": 6.5, "vi_over_prob": 0.58, "book_count": 4, "books": {...}}, ...}

  # Convenience wrapper (returns None if player/prop not found):
  prob = vi.get_sb_implied_prob("Chris Sale", "strikeouts", line=6.5)
"""

from __future__ import annotations

import logging
import os
import re
import time
import json
from collections import Counter

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_URL = "https://www.vegasinsider.com/mlb/odds/player-props/strikeouts/"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# Real sportsbooks only — skip PrizePicks (col 2), Sleeper (col 9), Underdog (col 10)
# Column indices are 0-based relative to the <td> list in each row (col 0 = player name)
_BOOK_COL_MAP: dict[str, int] = {
    "Bet365":   1,
    "BetMGM":   3,
    "DraftKings": 4,
    "Caesars":  5,
    "FanDuel":  6,
    "HardRock": 7,
    "Fanatics": 8,
}

# Market hold correction: subtract from raw Over implied prob to get fair prob.
# Typical MLB strikeout market hold is 8–10%, split ~4.5pp per side.
_VIG_HALF = 0.045

# Minimum sportsbooks at consensus line before we trust the signal
_MIN_BOOKS = 2

# Redis cache TTL seconds
_CACHE_TTL = 900  # 15 minutes
_REDIS_KEY = "vi_strikeouts"

# HTTP request timeout
_TIMEOUT = 12


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def _parse_cell(text: str) -> tuple[float, int] | None:
    """
    Parse a cell like 'o7.5-105+' or 'o6.5+120' into (line, american_odds).
    The trailing '+' is a VegasInsider "best odds" marker — stripped before parsing.
    Returns None if the cell is empty or doesn't match expected pattern.
    """
    text = text.strip().rstrip("+")
    if not text or not text.startswith("o"):
        return None
    m = re.match(r"o(\d+(?:\.\d+)?)([+-]\d+)$", text)
    if not m:
        return None
    try:
        return float(m.group(1)), int(m.group(2))
    except ValueError:
        return None


def _american_to_raw_prob(odds: int) -> float:
    """Convert American odds to raw (vig-inclusive) implied probability (0–1)."""
    if odds > 0:
        return 100.0 / (odds + 100.0)
    return abs(odds) / (abs(odds) + 100.0)


def _fair_prob(american_odds: int) -> float:
    """Raw implied prob minus half-vig correction → approximate fair probability."""
    return max(0.01, min(0.99, _american_to_raw_prob(american_odds) - _VIG_HALF))


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

def _scrape_vi_raw() -> dict[str, dict]:
    """
    Fetch VegasInsider strikeouts page and parse all rows.

    Returns
    -------
    dict keyed by player_name (str) →
        {book_name: {"line": float, "odds": int, "fair_prob": float}, ...}

    Players with multiple rows (different games) are merged: the entry
    with more books is preferred, ties go to the later row (which is usually
    the primary game for the day).
    """
    try:
        r = requests.get(_URL, headers=_HEADERS, timeout=_TIMEOUT)
        r.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("VegasInsider fetch failed: %s", exc)
        return {}

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table")
    if not table:
        logger.warning("VegasInsider: no <table> found in page")
        return {}

    rows = table.find_all("tr")
    if not rows:
        return {}

    # Parse header row to confirm column order
    header_cells = [td.get_text(strip=True) for td in rows[0].find_all(["th", "td"])]
    logger.debug("VegasInsider header: %s", header_cells)

    results: dict[str, dict] = {}

    for row in rows[1:]:
        cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
        if not cells:
            continue

        player_name = cells[0].strip()
        if not player_name or player_name in ("›", "Time"):
            continue

        books: dict[str, dict] = {}
        for book, col_idx in _BOOK_COL_MAP.items():
            if col_idx >= len(cells):
                continue
            parsed = _parse_cell(cells[col_idx])
            if parsed is None:
                continue
            line, odds = parsed
            books[book] = {
                "line": line,
                "odds": odds,
                "fair_prob": _fair_prob(odds),
            }

        if not books:
            continue

        # Merge duplicate player rows: prefer row with more books
        if player_name in results:
            existing_books = results[player_name]
            if len(books) >= len(existing_books):
                results[player_name] = books
        else:
            results[player_name] = books

    logger.info("VegasInsider: parsed %d pitchers", len(results))
    return results


def _build_consensus(raw: dict[str, dict]) -> dict[str, dict]:
    """
    For each player, derive:
      - consensus_line: mode line across all books
      - vi_over_prob: average fair_prob across books at the consensus line
      - book_count: number of books at consensus line
      - books: raw per-book data (diagnostics)
    """
    consensus: dict[str, dict] = {}

    for player, books in raw.items():
        if not books:
            continue

        # Find the mode line
        line_counts: Counter = Counter(b["line"] for b in books.values())
        consensus_line, _ = line_counts.most_common(1)[0]

        # Average fair_prob from books that posted the consensus line
        matching = [b for b in books.values() if b["line"] == consensus_line]
        if len(matching) < _MIN_BOOKS:
            # Only one book at this line — not reliable enough
            continue

        avg_prob = sum(b["fair_prob"] for b in matching) / len(matching)

        consensus[player] = {
            "consensus_line": consensus_line,
            "vi_over_prob": round(avg_prob, 4),
            "book_count": len(matching),
            "books": books,
        }

    return consensus


# ---------------------------------------------------------------------------
# Redis cache helpers
# ---------------------------------------------------------------------------

def _get_redis():
    """Return a Redis client or None if unavailable."""
    try:
        import redis as _redis
        url = os.getenv("REDIS_URL", "redis://localhost:6379")
        client = _redis.from_url(url, decode_responses=True, socket_timeout=2)
        client.ping()
        return client
    except Exception:
        return None


def _cache_get(key: str):
    r = _get_redis()
    if r is None:
        return None
    try:
        raw = r.get(key)
        return json.loads(raw) if raw else None
    except Exception:
        return None


def _cache_set(key: str, value, ttl: int = _CACHE_TTL):
    r = _get_redis()
    if r is None:
        return
    try:
        r.setex(key, ttl, json.dumps(value))
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

class VegasInsiderLayer:
    """
    Thin wrapper around the VegasInsider scraper with Redis caching.
    All public methods are safe to call even if the site is unreachable —
    they return {} / None gracefully.
    """

    def get_strikeouts_consensus(self) -> dict[str, dict]:
        """
        Return consensus strikeout data for all available pitchers.

        Returns dict of:
            {
              "Chris Sale": {
                "consensus_line": 6.5,
                "vi_over_prob": 0.582,
                "book_count": 5,
                "books": {"Bet365": {...}, "DraftKings": {...}, ...}
              }, ...
            }
        """
        cached = _cache_get(_REDIS_KEY)
        if cached is not None:
            logger.debug("VegasInsider: cache hit (%d players)", len(cached))
            return cached

        t0 = time.time()
        raw = _scrape_vi_raw()
        data = _build_consensus(raw)
        elapsed = time.time() - t0

        logger.info(
            "VegasInsider scrape complete: %d pitchers with consensus in %.1fs",
            len(data), elapsed,
        )

        if data:
            _cache_set(_REDIS_KEY, data)

        return data

    def get_sb_implied_prob(
        self,
        player_name: str,
        prop_type: str,
        line: float,
    ) -> float | None:
        """
        Returns the VegasInsider consensus Over implied probability for a pitcher's
        strikeouts line, or None if:
          - prop_type != "strikeouts"
          - player not found
          - consensus line doesn't match the requested line
          - fewer than MIN_BOOKS agree on the line

        Parameters
        ----------
        player_name : str   Display name (e.g. "Chris Sale")
        prop_type   : str   Must be "strikeouts" to get a result
        line        : float The UD/PP line to match (e.g. 6.5)

        Returns
        -------
        float in [0, 1] or None
        """
        if prop_type != "strikeouts":
            return None

        data = self.get_strikeouts_consensus()
        if not data:
            return None

        # Exact name match
        entry = data.get(player_name)

        # Fuzzy fallback: last name match (handles "Chris Sale" vs "Sale, C." style differences)
        if entry is None:
            last_name = player_name.strip().split()[-1].lower()
            for name, val in data.items():
                if name.strip().split()[-1].lower() == last_name:
                    entry = val
                    break

        if entry is None:
            return None

        # Only return if consensus line matches the requested line exactly
        if abs(entry["consensus_line"] - line) > 0.01:
            logger.debug(
                "VegasInsider: %s consensus line %.1f != requested %.1f — skipping",
                player_name, entry["consensus_line"], line,
            )
            return None

        return entry["vi_over_prob"]

    def get_all_for_enrichment(self) -> dict[tuple, dict]:
        """
        Returns a flat dict keyed by (player_name_lower, "strikeouts") for bulk
        enrichment lookups.  Used in prop_enrichment_layer.

        Return format:
            {("chris sale", "strikeouts"): {"vi_over_prob": 0.58, "consensus_line": 6.5, "book_count": 5}}
        """
        data = self.get_strikeouts_consensus()
        out: dict[tuple, dict] = {}
        for name, entry in data.items():
            key = (name.lower(), "strikeouts")
            out[key] = {
                "vi_over_prob": entry["vi_over_prob"],
                "consensus_line": entry["consensus_line"],
                "book_count": entry["book_count"],
            }
        return out


# ---------------------------------------------------------------------------
# CLI smoke-test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    vi = VegasInsiderLayer()
    data = vi.get_strikeouts_consensus()
    print(f"\nVegasInsider strikeouts — {len(data)} pitchers\n")
    for player, entry in sorted(data.items(), key=lambda x: -x[1]["vi_over_prob"]):
        books_str = ", ".join(
            f"{b}:{v['line']}({v['odds']:+d})" for b, v in entry["books"].items()
        )
        print(
            f"  {player:<30s}  line={entry['consensus_line']:.1f}  "
            f"vi_prob={entry['vi_over_prob']:.3f}  n={entry['book_count']}  [{books_str}]"
        )
