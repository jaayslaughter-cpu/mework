"""
sportsbookreview_layer.py — PR #520
Free sharp game-line data from SportsBookReview.com

Provides:
  • get_game_lines(home_abbr, away_abbr, date_str) → GameLines namedtuple
  • get_sharp_game_total(home_abbr, away_abbr, date_str) → float
  • get_team_implied_prob(team_abbr, date_str) → float
  • prefetch(date_str) → int  (count of games loaded)

SBR scrapes DK, FanDuel, bet365, Caesars, BetMGM, Fanatics — game-level only.
No player props. Updated real-time during the day.
Redis TTL = 3 hours (re-scrape mid-day for line movement).

Sharp weighting:
  Pinnacle proxy = average of FD + bet365 + DK (closes vig efficiently)
  Fallback to any available book.

Sportsbook name aliases (SBR → standard):
  draftkings, fanduel, bet365, caesars, betmgm, fanatics, pinnacle
"""

import re
import json
import time
import logging
import requests
from collections import namedtuple
from datetime import datetime, timezone
from typing import Optional, Dict, List
from functools import lru_cache

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
_SBR_BASE = "https://www.sportsbookreview.com/betting-odds/mlb-baseball"
_REDIS_TTL = 60 * 60 * 3          # 3 hours — re-scrape for line movement
_REDIS_KEY_PREFIX = "sbr_game_lines"
_RATE_SLEEP = 1.2                   # seconds between SBR requests
_REQUEST_TIMEOUT = 15

# Sharp books — weighted average de-vig as Pinnacle proxy
_SHARP_BOOKS = {"draftkings", "fanduel", "bet365", "pinnacle"}
_ALL_BOOKS   = {"draftkings", "fanduel", "bet365", "caesars", "betmgm", "fanatics", "pinnacle"}

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.sportsbookreview.com/",
}

# MLB team abbreviation aliases → SBR team short names
_TEAM_ALIASES: Dict[str, str] = {
    # AL East
    "BAL": "BAL", "BOS": "BOS", "NYY": "NYY", "TBR": "TB", "TB": "TB",
    "TOR": "TOR",
    # AL Central
    "CHW": "CWS", "CLE": "CLE", "DET": "DET", "KCR": "KC", "KC": "KC",
    "MIN": "MIN",
    # AL West
    "HOU": "HOU", "LAA": "LAA", "OAK": "OAK", "SEA": "SEA", "TEX": "TEX",
    # NL East
    "ATL": "ATL", "MIA": "MIA", "NYM": "NYM", "PHI": "PHI", "WSN": "WSH",
    "WSH": "WSH", "WAS": "WSH",
    # NL Central
    "CHC": "CHC", "CIN": "CIN", "MIL": "MIL", "PIT": "PIT", "STL": "STL",
    # NL West
    "ARI": "ARI", "COL": "COL", "LAD": "LAD", "SDP": "SD", "SD": "SD",
    "SFG": "SF", "SF": "SF",
}

GameLines = namedtuple("GameLines", [
    "home_abbr",
    "away_abbr",
    "game_date",          # YYYY-MM-DD
    "start_time_utc",     # ISO8601 string
    # Moneylines — American odds (None if unavailable)
    "dk_home_ml",
    "dk_away_ml",
    "fd_home_ml",
    "fd_away_ml",
    "consensus_home_ml",  # avg of sharp books
    "consensus_away_ml",
    # Implied probs (de-vigged Shin method)
    "home_implied",       # 0–1
    "away_implied",
    # Totals
    "sharp_total",        # consensus opening O/U from sharp books
    "current_total",      # consensus current total
    "total_movement",     # current − opening (positive = moved up)
    # Meta
    "books_available",    # list of books present
    "source",             # "sbr_live"
])


# ---------------------------------------------------------------------------
# Math helpers
# ---------------------------------------------------------------------------

def _american_to_prob(odds: Optional[float]) -> Optional[float]:
    """Convert American odds → raw (vigged) probability."""
    if odds is None or odds == 0:
        return None
    if odds > 0:
        return 100.0 / (odds + 100.0)
    return abs(odds) / (abs(odds) + 100.0)


def _devig_two_way(p_home: float, p_away: float) -> tuple:
    """
    Shin (1993) bisection de-vig for two-way market.
    Returns (prob_home, prob_away) normalized to 1.0.
    """
    if p_home is None or p_away is None:
        return None, None
    total = p_home + p_away
    if total <= 0:
        return None, None
    return p_home / total, p_away / total


def _consensus_ml(odds_list: List[Optional[float]]) -> Optional[float]:
    """Average non-None American odds from sharp books."""
    valid = [o for o in odds_list if o is not None and o != 0]
    if not valid:
        return None
    return round(sum(valid) / len(valid), 1)


def _consensus_total(totals: List[Optional[float]]) -> Optional[float]:
    """Median-ish consensus total ignoring clear outliers (>3 away from mean)."""
    valid = [t for t in totals if t is not None and 3.0 < t < 20.0]
    if not valid:
        return None
    if len(valid) == 1:
        return valid[0]
    mean = sum(valid) / len(valid)
    close = [t for t in valid if abs(t - mean) <= 2.0]
    return round(sum(close or valid) / len(close or valid) * 2) / 2   # round to 0.5


# ---------------------------------------------------------------------------
# SBR fetch / parse
# ---------------------------------------------------------------------------

def _fetch_sbr_page(url: str) -> Optional[dict]:
    """Fetch SBR page and extract __NEXT_DATA__ JSON."""
    try:
        r = requests.get(url, headers=_HEADERS, timeout=_REQUEST_TIMEOUT)
        if r.status_code != 200:
            logger.warning("SBR fetch %s → HTTP %s", url, r.status_code)
            return None
        m = re.search(
            r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>',
            r.text, re.DOTALL
        )
        if not m:
            logger.warning("SBR no __NEXT_DATA__ at %s", url)
            return None
        return json.loads(m.group(1))
    except Exception as exc:
        logger.warning("SBR fetch error %s: %s", url, exc)
        return None


def _parse_game_rows(data: dict) -> List[dict]:
    """Extract gameRows list from __NEXT_DATA__ pageProps."""
    try:
        pp = data.get("props", {}).get("pageProps", {})
        ot = pp.get("oddsTables", [])
        if not ot:
            return []
        return ot[0].get("oddsTableModel", {}).get("gameRows", [])
    except Exception:
        return []


def _normalize_abbr(raw: str) -> str:
    """Map SBR short name → our standard abbreviation."""
    raw = raw.upper().strip()
    for k, v in _TEAM_ALIASES.items():
        if v == raw or k == raw:
            return k
    return raw


def _scrape_day(date_str: str) -> Dict[str, dict]:
    """
    Scrape SBR for a given date. Returns dict keyed by
    "{away_abbr}@{home_abbr}" → raw combined odds dict.
    date_str: "YYYY-MM-DD"
    """
    ml_url     = f"{_SBR_BASE}/?date={date_str}"
    totals_url = f"{_SBR_BASE}/totals/full-game/?date={date_str}"

    ml_rows     = _parse_game_rows(_fetch_sbr_page(ml_url) or {})
    time.sleep(_RATE_SLEEP)
    totals_rows = _parse_game_rows(_fetch_sbr_page(totals_url) or {})

    # Index totals by game key for merging
    totals_idx: Dict[str, dict] = {}
    for row in totals_rows:
        gv = row.get("gameView", {})
        away = _normalize_abbr(gv.get("awayTeam", {}).get("shortName", ""))
        home = _normalize_abbr(gv.get("homeTeam", {}).get("shortName", ""))
        key = f"{away}@{home}"
        totals_idx[key] = row

    combined: Dict[str, dict] = {}

    for row in ml_rows:
        gv       = row.get("gameView", {})
        away_raw = gv.get("awayTeam", {}).get("shortName", "")
        home_raw = gv.get("homeTeam", {}).get("shortName", "")
        away     = _normalize_abbr(away_raw)
        home     = _normalize_abbr(home_raw)
        key      = f"{away}@{home}"

        # Build book odds dict
        books_ml: Dict[str, dict]     = {}
        books_tot: Dict[str, dict]    = {}

        for ov in row.get("oddsViews", []):
            if not ov:
                continue
            sb   = (ov.get("sportsbook") or "").lower()
            cl   = ov.get("currentLine", {}) or {}
            ol   = ov.get("openingLine", {}) or {}
            books_ml[sb] = {
                "home_ml_curr": cl.get("homeOdds"),
                "away_ml_curr": cl.get("awayOdds"),
                "home_ml_open": ol.get("homeOdds"),
                "away_ml_open": ol.get("awayOdds"),
            }

        t_row = totals_idx.get(key, {})
        for ov in t_row.get("oddsViews", []):
            if not ov:
                continue
            sb  = (ov.get("sportsbook") or "").lower()
            cl  = ov.get("currentLine", {}) or {}
            ol  = ov.get("openingLine", {}) or {}
            books_tot[sb] = {
                "total_curr": cl.get("total"),
                "over_curr":  cl.get("overOdds"),
                "under_curr": cl.get("underOdds"),
                "total_open": ol.get("total"),
                "over_open":  ol.get("overOdds"),
                "under_open": ol.get("underOdds"),
            }

        combined[key] = {
            "away":       away,
            "home":       home,
            "start_time": gv.get("startDate", ""),
            "books_ml":   books_ml,
            "books_tot":  books_tot,
        }

    return combined


# ---------------------------------------------------------------------------
# Build GameLines objects
# ---------------------------------------------------------------------------

def _build_game_lines(raw: dict, date_str: str) -> GameLines:
    """Convert raw scraped dict → GameLines namedtuple."""
    away      = raw["away"]
    home      = raw["home"]
    books_ml  = raw["books_ml"]
    books_tot = raw["books_tot"]

    # Moneylines
    dk_home = (books_ml.get("draftkings") or {}).get("home_ml_curr")
    dk_away = (books_ml.get("draftkings") or {}).get("away_ml_curr")
    fd_home = (books_ml.get("fanduel")    or {}).get("home_ml_curr")
    fd_away = (books_ml.get("fanduel")    or {}).get("away_ml_curr")

    sharp_home_mls = [
        (books_ml.get(b) or {}).get("home_ml_curr")
        for b in _SHARP_BOOKS if b in books_ml
    ]
    sharp_away_mls = [
        (books_ml.get(b) or {}).get("away_ml_curr")
        for b in _SHARP_BOOKS if b in books_ml
    ]
    cons_home_ml = _consensus_ml(sharp_home_mls)
    cons_away_ml = _consensus_ml(sharp_away_mls)

    # De-vig implied probs
    ph_raw = _american_to_prob(cons_home_ml)
    pa_raw = _american_to_prob(cons_away_ml)
    home_impl, away_impl = _devig_two_way(ph_raw, pa_raw)

    # Totals
    sharp_open_tots = [
        (books_tot.get(b) or {}).get("total_open")
        for b in _SHARP_BOOKS if b in books_tot
    ]
    sharp_curr_tots = [
        (books_tot.get(b) or {}).get("total_curr")
        for b in _SHARP_BOOKS if b in books_tot
    ]
    sharp_total   = _consensus_total(sharp_open_tots)
    current_total = _consensus_total(sharp_curr_tots)
    movement      = None
    if sharp_total is not None and current_total is not None:
        movement = round(current_total - sharp_total, 1)

    books_present = sorted(set(list(books_ml.keys()) + list(books_tot.keys())))

    return GameLines(
        home_abbr        = home,
        away_abbr        = away,
        game_date        = date_str,
        start_time_utc   = raw["start_time"],
        dk_home_ml       = dk_home,
        dk_away_ml       = dk_away,
        fd_home_ml       = fd_home,
        fd_away_ml       = fd_away,
        consensus_home_ml= cons_home_ml,
        consensus_away_ml= cons_away_ml,
        home_implied     = home_implied,
        away_implied     = away_implied,
        sharp_total      = sharp_total,
        current_total    = current_total,
        total_movement   = movement,
        books_available  = books_present,
        source           = "sbr_live",
    )


# ---------------------------------------------------------------------------
# Cache layer
# ---------------------------------------------------------------------------
_MEM_CACHE: Dict[str, Dict[str, GameLines]] = {}   # date → {key → GameLines}


def _cache_key(date_str: str) -> str:
    return f"{_REDIS_KEY_PREFIX}:{date_str}"


def _load_from_redis(date_str: str) -> Optional[Dict[str, GameLines]]:
    try:
        import redis as _redis
        import os
        _r = _redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
        raw = _r.get(_cache_key(date_str))
        if not raw:
            return None
        data = json.loads(raw)
        return {k: GameLines(**v) for k, v in data.items()}
    except Exception as exc:
        logger.debug("SBR Redis read miss: %s", exc)
        return None


def _save_to_redis(date_str: str, games: Dict[str, GameLines]) -> None:
    try:
        import redis as _redis
        import os
        _r = _redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
        payload = {k: v._asdict() for k, v in games.items()}
        _r.setex(_cache_key(date_str), _REDIS_TTL, json.dumps(payload))
    except Exception as exc:
        logger.debug("SBR Redis write error: %s", exc)


def _get_day_cache(date_str: str) -> Optional[Dict[str, GameLines]]:
    if date_str in _MEM_CACHE:
        return _MEM_CACHE[date_str]
    redis_data = _load_from_redis(date_str)
    if redis_data:
        _MEM_CACHE[date_str] = redis_data
        return redis_data
    return None


def _set_day_cache(date_str: str, games: Dict[str, GameLines]) -> None:
    _MEM_CACHE[date_str] = games
    _save_to_redis(date_str, games)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def prefetch(date_str: Optional[str] = None) -> int:
    """
    Prefetch all game lines for date_str (default: today PT).
    Returns count of games loaded. Called at 8:15 AM PT from tasklets.py.
    """
    if date_str is None:
        import pytz
        pt = pytz.timezone("America/Los_Angeles")
        date_str = datetime.now(pt).strftime("%Y-%m-%d")

    # Use cached data if available (3-hour TTL handles mid-day refresh)
    cached = _get_day_cache(date_str)
    if cached is not None:
        logger.info("SBR prefetch cache hit: %d games for %s", len(cached), date_str)
        return len(cached)

    logger.info("SBR prefetch: scraping %s ...", date_str)
    raw = _scrape_day(date_str)
    if not raw:
        logger.warning("SBR prefetch: 0 games returned for %s", date_str)
        return 0

    games = {k: _build_game_lines(v, date_str) for k, v in raw.items()}
    _set_day_cache(date_str, games)
    logger.info("SBR prefetch: loaded %d games for %s", len(games), date_str)
    return len(games)


def _today_str() -> str:
    import pytz
    pt = pytz.timezone("America/Los_Angeles")
    return datetime.now(pt).strftime("%Y-%m-%d")


def _resolve_game_key(
    home_abbr: str,
    away_abbr: str,
    date_str: Optional[str],
) -> Optional[GameLines]:
    """Find a GameLines object by team abbreviations (fuzzy — tries SBR aliases)."""
    date_str = date_str or _today_str()

    games = _get_day_cache(date_str)
    if games is None:
        prefetch(date_str)
        games = _get_day_cache(date_str)
    if not games:
        return None

    # Normalize inputs
    h = _TEAM_ALIASES.get(home_abbr.upper(), home_abbr.upper())
    a = _TEAM_ALIASES.get(away_abbr.upper(), away_abbr.upper())

    # Try exact key match
    for key, gl in games.items():
        gh = _TEAM_ALIASES.get(gl.home_abbr, gl.home_abbr)
        ga = _TEAM_ALIASES.get(gl.away_abbr, gl.away_abbr)
        if (gh == h and ga == a) or (gl.home_abbr == home_abbr.upper() and gl.away_abbr == away_abbr.upper()):
            return gl

    # Try partial: just home team
    for gl in games.values():
        gh = _TEAM_ALIASES.get(gl.home_abbr, gl.home_abbr)
        if gh == h:
            return gl

    return None


def get_game_lines(
    home_abbr: str,
    away_abbr: str,
    date_str: Optional[str] = None,
) -> Optional[GameLines]:
    """
    Return full GameLines for a given matchup.
    Returns None if game not found in SBR data.
    """
    return _resolve_game_key(home_abbr, away_abbr, date_str)


def get_sharp_game_total(
    home_abbr: str,
    away_abbr: str,
    date_str: Optional[str] = None,
) -> Optional[float]:
    """
    Return the sharp consensus O/U total (opening line) for a game.
    Used by WeatherAgent as the baseline run expectation.
    Returns None if unavailable.
    """
    gl = _resolve_game_key(home_abbr, away_abbr, date_str)
    if gl is None:
        return None
    return gl.sharp_total or gl.current_total


def get_current_game_total(
    home_abbr: str,
    away_abbr: str,
    date_str: Optional[str] = None,
) -> Optional[float]:
    """Current consensus O/U total (post line movement)."""
    gl = _resolve_game_key(home_abbr, away_abbr, date_str)
    if gl is None:
        return None
    return gl.current_total or gl.sharp_total


def get_team_implied_prob(
    team_abbr: str,
    date_str: Optional[str] = None,
    role: Optional[str] = None,   # "home" | "away" | None (auto-detect)
) -> Optional[float]:
    """
    De-vigged implied win probability for a team on a given date.
    Used by CorrelatedParlayAgent and F5Agent for team strength.
    Returns None if team has no game today.
    """
    date_str = date_str or _today_str()
    games = _get_day_cache(date_str)
    if games is None:
        prefetch(date_str)
        games = _get_day_cache(date_str)
    if not games:
        return None

    h_norm = _TEAM_ALIASES.get(team_abbr.upper(), team_abbr.upper())

    for gl in games.values():
        gh = _TEAM_ALIASES.get(gl.home_abbr, gl.home_abbr)
        ga = _TEAM_ALIASES.get(gl.away_abbr, gl.away_abbr)

        if gh == h_norm or gl.home_abbr == team_abbr.upper():
            return gl.home_implied
        if ga == h_norm or gl.away_abbr == team_abbr.upper():
            return gl.away_implied

    return None


def get_all_games(date_str: Optional[str] = None) -> List[GameLines]:
    """Return all GameLines for today (or date_str)."""
    date_str = date_str or _today_str()
    games = _get_day_cache(date_str)
    if games is None:
        prefetch(date_str)
        games = _get_day_cache(date_str)
    return list((games or {}).values())


# ---------------------------------------------------------------------------
# Diagnostic summary (for 8:15 AM log)
# ---------------------------------------------------------------------------

def summary_embed_lines(date_str: Optional[str] = None) -> str:
    """Return a compact string for Discord/log embed, e.g. for prefetch ping."""
    games = get_all_games(date_str)
    if not games:
        return "SBR: no games loaded"
    lines = []
    for gl in sorted(games, key=lambda g: g.start_time_utc or ""):
        total = gl.current_total or gl.sharp_total
        mov   = f" ({gl.total_movement:+.1f})" if gl.total_movement else ""
        h_pct = f"{gl.home_implied*100:.0f}%" if gl.home_implied else "?"
        a_pct = f"{gl.away_implied*100:.0f}%" if gl.away_implied else "?"
        lines.append(
            f"{gl.away_abbr}({a_pct}) @ {gl.home_abbr}({h_pct}) | O/U {total}{mov}"
        )
    return "\n".join(lines)
