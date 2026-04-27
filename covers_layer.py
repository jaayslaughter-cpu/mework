"""
covers_layer.py — PR #439
Covers.com player props + THE BAT X projections as Tier 3 sportsbook reference fallback.

Provides:
  - sb_implied_prob from DraftKings/FanDuel/BetMGM/Caesars book odds
  - THE BAT X projection as a supplemental model signal
  - Coverage: strikeouts, hits, hits+runs+rbi, earned_runs, hitter_strikeouts

Timing: Pre-game only. Returns empty dict after game starts (Covers clears data).
Cache: Postgres layer_cache, 4-hour TTL (same as BVI).
"""

from __future__ import annotations

import logging
import math
import os
import re
import time
from typing import Optional

import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

_BASE = "https://www.covers.com"
_PROPS_PAGE = f"{_BASE}/sport/baseball/mlb/player-props"
_MATCHUP_URL = f"{_BASE}/sport/player-props/matchup/mlb"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html, application/xhtml+xml, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": _PROPS_PAGE,
}

# Covers propEvent → our prop_type
_PROP_MAP: dict[str, str] = {
    "strikeouts":   "strikeouts",
    "hits":         "hits",
    "hrbi":         "hits+runs+rbi",   # Covers uses "hrbi" for H+R+RBI
    "earned_runs":  "earned_runs",
    "hitter_strikeouts": "hitter_strikeouts",
}

# Reverse: our prop_type → Covers propEvent
_REVERSE_MAP: dict[str, str] = {v: k for k, v in _PROP_MAP.items()}

# Books Covers shows — ordered by sharpness weight
_BOOK_PRIORITY = ["draftkings", "fanduel", "betmgm", "caesars", "fanatics", "bet365"]

# Postgres cache TTL: 4 hours
_CACHE_TTL_SECONDS = 4 * 3600
_CACHE_KEY_PREFIX = "covers_ref"

# Session (connection pooling)
_session: Optional[requests.Session] = None


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update(_HEADERS)
    return _session


# ── Name normalizer ────────────────────────────────────────────────────────────

def _norm(name: str) -> str:
    """Lowercase, strip punctuation, normalize whitespace."""
    name = name.lower().strip()
    name = re.sub(r"[''`\-\.]", "", name)
    name = re.sub(r"\s+", " ", name)
    return name


# ── American odds → vig-stripped implied prob ──────────────────────────────────

def _american_to_impl(odds: int) -> float:
    """Convert American odds integer to implied probability (raw, WITH vig)."""
    if odds >= 0:
        return 100 / (odds + 100)
    else:
        return abs(odds) / (abs(odds) + 100)


def _no_vig_pair(over_odds: int, under_odds: int) -> tuple[float, float]:
    """
    Shin (1993) power method de-vig — same as tasklets.py _no_vig().
    Returns (p_over, p_under) without vig.
    """
    try:
        r_over  = _american_to_impl(over_odds)
        r_under = _american_to_impl(under_odds)
        overround = r_over + r_under
        if overround <= 0:
            return 0.5, 0.5
        # Simple normalization (acceptable for small vig spreads)
        p_over  = r_over  / overround
        p_under = r_under / overround
        return round(p_over, 6), round(p_under, 6)
    except Exception:
        return 0.5, 0.5


# ── Game ID scraping ───────────────────────────────────────────────────────────

def _fetch_game_ids() -> str:
    """
    Scrape the Covers MLB player-props page to get today's game IDs.
    Returns comma-separated game ID string (e.g. "368784,367891").
    Returns empty string on failure.
    """
    try:
        sess = _get_session()
        r = sess.get(_PROPS_PAGE, timeout=12)
        if r.status_code != 200:
            log.warning("[Covers] Props page returned %d", r.status_code)
            return ""

        soup = BeautifulSoup(r.text, "html.parser")

        # Primary: data-game-id attribute on market buttons container
        el = soup.select_one("[data-game-id]")
        if el:
            gids = el.get("data-game-id", "").strip()
            if gids:
                log.debug("[Covers] Game IDs from data-game-id attr: %s", gids)
                return gids

        # Fallback: parse inline script for `var gameId = "..."`
        for script in soup.find_all("script", src=False):
            m = re.search(r'var gameId\s*=\s*"([^"]+)"', script.get_text())
            if m:
                gids = m.group(1).strip()
                log.debug("[Covers] Game IDs from inline script: %s", gids)
                return gids

        log.warning("[Covers] Could not find game IDs on props page")
        return ""

    except Exception as exc:
        log.warning("[Covers] Game ID scrape failed: %s", exc)
        return ""


# ── Per-market data fetch ──────────────────────────────────────────────────────

def _parse_odds(cell_text: str) -> Optional[int]:
    """
    Parse an American odds string like '+135', '-110', 'N/A', '' → int or None.
    """
    txt = cell_text.strip().replace(",", "")
    if not txt or txt in ("N/A", "—", "-", "–"):
        return None
    m = re.match(r'^([+\-]?\d+)$', txt)
    return int(m.group(1)) if m else None


def _fetch_market(game_ids: str, prop_event: str) -> list[dict]:
    """
    Fetch one market from Covers and parse the HTML table.
    Returns list of row dicts with keys:
        player_norm, prop_type, line, batx_proj, ev_pct,
        over_odds_{book}, under_odds_{book}
    """
    if not game_ids:
        return []

    url = f"{_MATCHUP_URL}/{game_ids}"
    params = {
        "propEvent": prop_event,
        "countryCode": "US",
        "stateProv": "VA",
        "isLeagueVersion": "True",
        "experiment": "false",
    }

    try:
        sess = _get_session()
        r = sess.get(url, params=params, timeout=12)
        if r.status_code != 200:
            log.debug("[Covers] Market %s returned %d", prop_event, r.status_code)
            return []

        html = r.text

        # Detect "no props yet" placeholder
        if "crunching the numbers" in html.lower() or "player-props-no-props-message" in html:
            log.debug("[Covers] Market %s: no props available yet (pre-game too early or game live)", prop_event)
            return []

        soup = BeautifulSoup(html, "html.parser")

        # Find the props table — Covers uses a standard <table> with thead/tbody
        table = soup.find("table")
        if not table:
            log.debug("[Covers] Market %s: no table found in response", prop_event)
            return []

        # Parse header to map column names → indices
        header_row = table.find("thead")
        if not header_row:
            return []

        headers_raw = [th.get_text(strip=True).lower() for th in header_row.find_all(["th", "td"])]
        log.debug("[Covers] Market %s headers: %s", prop_event, headers_raw)

        # Column index lookups (flexible — column order may shift)
        def col_idx(keywords: list[str]) -> Optional[int]:
            for kw in keywords:
                for i, h in enumerate(headers_raw):
                    if kw in h:
                        return i
            return None

        idx_player  = col_idx(["player", "name"])
        idx_line    = col_idx(["line", "total"])
        idx_batx    = col_idx(["bat x", "batx", "projection", "proj"])
        idx_ev      = col_idx(["ev", "edge", "value"])

        # Book columns: look for known book names in headers
        book_cols: dict[str, int] = {}
        for book in _BOOK_PRIORITY:
            for i, h in enumerate(headers_raw):
                if book.replace("mgm", "mgm").replace("draftkings", "dk") in h or book[:4] in h:
                    if i not in book_cols.values():
                        book_cols[book] = i
                        break

        prop_type = _PROP_MAP.get(prop_event, prop_event)
        rows = []

        tbody = table.find("tbody")
        if not tbody:
            return []

        for tr in tbody.find_all("tr"):
            cells = tr.find_all(["td", "th"])
            if len(cells) < 3:
                continue

            def cell_text(idx: Optional[int]) -> str:
                if idx is None or idx >= len(cells):
                    return ""
                return cells[idx].get_text(strip=True)

            player_raw = cell_text(idx_player)
            if not player_raw:
                continue

            player_norm = _norm(player_raw)

            # Parse line
            line_str = cell_text(idx_line)
            try:
                line = float(re.sub(r"[^0-9.\-]", "", line_str))
            except (ValueError, TypeError):
                line = None

            # Parse THE BAT X projection
            batx_str = cell_text(idx_batx)
            try:
                batx_proj = float(re.sub(r"[^0-9.\-]", "", batx_str))
            except (ValueError, TypeError):
                batx_proj = None

            # Parse EV%
            ev_str = cell_text(idx_ev)
            try:
                ev_pct = float(re.sub(r"[^0-9.\-]", "", ev_str))
            except (ValueError, TypeError):
                ev_pct = None

            # Parse book odds — each book typically spans 2 columns (over/under)
            book_odds: dict[str, tuple[Optional[int], Optional[int]]] = {}
            for book, col_i in book_cols.items():
                over_raw  = cell_text(col_i)
                under_raw = cell_text(col_i + 1) if col_i + 1 < len(cells) else ""
                over_odds  = _parse_odds(over_raw)
                under_odds = _parse_odds(under_raw)
                if over_odds is not None and under_odds is not None:
                    book_odds[book] = (over_odds, under_odds)

            # Compute weighted implied prob — best book (sharpest) wins
            sb_implied_prob: Optional[float] = None
            sb_line: Optional[float] = line
            best_book: Optional[str] = None

            for book in _BOOK_PRIORITY:
                if book in book_odds:
                    over_o, under_o = book_odds[book]
                    p_over, _ = _no_vig_pair(over_o, under_o)
                    sb_implied_prob = p_over
                    best_book = book
                    break

            if sb_implied_prob is None or line is None:
                continue

            rows.append({
                "player_norm":    player_norm,
                "prop_type":      prop_type,
                "line":           line,
                "batx_proj":      batx_proj,
                "ev_pct":         ev_pct,
                "sb_implied_prob": sb_implied_prob,
                "sb_line":        sb_line,
                "bookmaker":      best_book or "covers",
                "book_odds":      book_odds,  # full odds for all books
            })

        log.info("[Covers] Market %s: %d rows parsed", prop_event, len(rows))
        return rows

    except Exception as exc:
        log.warning("[Covers] Market %s fetch failed: %s", prop_event, exc)
        return []


# ── Postgres cache ─────────────────────────────────────────────────────────────

def _cache_key(date_int: int) -> str:
    return f"{_CACHE_KEY_PREFIX}:{date_int}"


def _read_cache(date_int: int) -> Optional[dict]:
    """Read covers reference from layer_cache table (Postgres). Returns None on miss."""
    try:
        import json
        import psycopg2

        db_url = os.environ.get("DATABASE_URL", "")
        if not db_url:
            return None

        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT value FROM layer_cache
                    WHERE key = %s
                      AND created_at > NOW() - INTERVAL '%s seconds'
                    ORDER BY created_at DESC LIMIT 1
                    """,
                    (_cache_key(date_int), _CACHE_TTL_SECONDS),
                )
                row = cur.fetchone()
                if row:
                    return json.loads(row[0])
        return None
    except Exception as exc:
        log.debug("[Covers] Cache read failed: %s", exc)
        return None


def _write_cache(date_int: int, data: dict) -> None:
    """Write covers reference to layer_cache table (Postgres)."""
    try:
        import json
        import psycopg2

        db_url = os.environ.get("DATABASE_URL", "")
        if not db_url:
            return

        payload = json.dumps(data)
        with psycopg2.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO layer_cache (key, value, created_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, created_at = NOW()
                    """,
                    (_cache_key(date_int), payload),
                )
            conn.commit()
    except Exception as exc:
        log.debug("[Covers] Cache write failed: %s", exc)


# ── Public interface ───────────────────────────────────────────────────────────

def fetch_covers_reference(date_int: int | None = None) -> dict:
    """
    Fetch Covers.com player prop data for today.

    Returns:
        dict keyed by (player_norm: str, prop_type: str) →
        {
            "sb_implied_prob": float,   # vig-stripped over probability
            "sb_line":         float,
            "bookmaker":       str,
            "batx_proj":       float | None,  # THE BAT X projection
            "ev_pct":          float | None,  # Covers EV%
        }

    Returns empty dict when no data is available (games live, too early, etc.).
    """
    from datetime import datetime
    from zoneinfo import ZoneInfo

    if date_int is None:
        date_int = int(datetime.now(ZoneInfo("America/Los_Angeles")).strftime("%Y%m%d"))

    # Check Postgres cache first
    cached = _read_cache(date_int)
    if cached:
        log.info("[Covers] Cache hit: %d entries", len(cached))
        return cached

    log.info("[Covers] Fetching fresh data from Covers.com...")

    # Step 1: Get today's game IDs
    game_ids = _fetch_game_ids()
    if not game_ids:
        log.warning("[Covers] No game IDs found — skipping")
        return {}

    # Step 2: Fetch each prop market
    ref: dict[str, dict] = {}

    for prop_event, prop_type in _PROP_MAP.items():
        rows = _fetch_market(game_ids, prop_event)
        for row in rows:
            key = f"{row['player_norm']}|{row['prop_type']}"
            ref[key] = {
                "sb_implied_prob": row["sb_implied_prob"],
                "sb_line":         row["sb_line"],
                "bookmaker":       row["bookmaker"],
                "batx_proj":       row["batx_proj"],
                "ev_pct":          row["ev_pct"],
            }
        time.sleep(0.4)  # polite rate limit

    total = len(ref)
    log.info("[Covers] Built reference: %d player/prop entries across %d markets", total, len(_PROP_MAP))

    if total > 0:
        _write_cache(date_int, ref)

    return ref


def enrich_props_with_covers(props: list, date_int: int | None = None) -> list:
    """
    Stamp covers_batx_proj and optionally upgrade sb_implied_prob for any prop
    where sb_implied_prob is still at the 0.5 default (i.e. OddsAPI and Pinnacle
    both missed this player).

    Only fills sb_implied_prob when it's 0.5 (default/unfilled) to avoid
    overwriting sharper Pinnacle/OddsAPI data.
    """
    ref = fetch_covers_reference(date_int)
    if not ref:
        return props

    filled = 0
    batx_filled = 0

    for prop in props:
        player_norm = _norm(prop.get("player_name", ""))
        prop_type   = prop.get("prop_type", "").lower()

        key = f"{player_norm}|{prop_type}"
        entry = ref.get(key)
        if not entry:
            continue

        # Stamp THE BAT X projection regardless
        if entry.get("batx_proj") is not None:
            prop["covers_batx_proj"] = entry["batx_proj"]
            batx_filled += 1

        # Only fill sb_implied_prob if it's still default (0.5) or missing
        current_impl = float(prop.get("sb_implied_prob", 0.5) or 0.5)
        if abs(current_impl - 0.5) < 0.001:
            prop["sb_implied_prob"] = entry["sb_implied_prob"]
            prop["sb_line"]         = entry.get("sb_line", prop.get("line"))
            prop["bookmaker"]       = entry.get("bookmaker", "covers")
            filled += 1

    log.info(
        "[Covers] enrich_props_with_covers: %d sb_implied_prob filled, %d batx_proj stamped (of %d props)",
        filled, batx_filled, len(props),
    )
    return props
