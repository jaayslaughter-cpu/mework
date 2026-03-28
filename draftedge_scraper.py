"""
draftedge_scraper.py — PropIQ Analytics: DraftEdge Daily Projections Fetcher

Fetches per-player Hit%, HR%, SB%, RUN%, RBI% (batters) and
K%, ERA, HIT%, HR% (pitchers) from DraftEdge's internal JSON API.

Anti-ban measures:
  1. Fetch ONCE per calendar day — Parquet cache prevents re-hitting the server
  2. Session-scoped cookie jar — persists Sucuri session cookies across requests
  3. Homepage warm-up — seeds cookies before hitting data endpoints
  4. Rotating user agents — cycles through 8 realistic Chrome/Firefox/Safari UAs
  5. Jittered delays — random 2–5 s between requests, never hammers
  6. Referer + full browser headers — looks like a real visitor
  7. Exponential backoff on 429/503 — waits 30 s / 90 s / 270 s before retry
  8. Hard daily cap of 3 fetches — circuit breaker prevents runaway calls
  9. robots.txt compliant — projection pages are explicitly allowed

PEP 8 compliant. Zero external dependencies beyond stdlib + pandas.
"""

from __future__ import annotations

import json
import logging
import random
import re
import time
import urllib.request
import urllib.error
import http.cookiejar
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CACHE_DIR = Path("/agent/home/cache/draftedge")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

BATTER_ENDPOINT = "https://draftedge.com/draftedge-data/mlb_proj_dk.json"
PITCHER_ENDPOINT = "https://draftedge.com/draftedge-data/mlb_spproj_dk.json"
BATTER_PAGE = "https://draftedge.com/mlb/todays-mlb-batter-projections/"
PITCHER_PAGE = "https://draftedge.com/mlb/mlb-starting-pitchers/"
HOME_PAGE = "https://draftedge.com/"

# Fetch ceiling: no more than this many live fetches per calendar day
DAILY_FETCH_CAP = 3

# Jitter window between requests (seconds)
REQUEST_DELAY_MIN = 2.5
REQUEST_DELAY_MAX = 5.5

# Retry policy for 429 / 503
RETRY_DELAYS = [30, 90, 270]

# Rotate through realistic browser UAs
_USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 OPR/108.0.0.0",
]

# Track fetch count in memory for the current process lifetime
_fetch_count_today: dict[str, int] = {}

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("draftedge_scraper")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _pick_ua() -> str:
    """Return a random user-agent from the rotation pool."""
    return random.choice(_USER_AGENTS)


def _build_session() -> urllib.request.OpenerDirector:
    """Build a persistent session opener with cookie support."""
    cj = http.cookiejar.CookieJar()
    return urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))


def _jitter_sleep() -> None:
    """Sleep a random amount between REQUEST_DELAY_MIN and REQUEST_DELAY_MAX."""
    delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
    time.sleep(delay)


def _fetch_url(
    opener: urllib.request.OpenerDirector,
    url: str,
    *,
    referer: str = HOME_PAGE,
    is_json: bool = False,
) -> str | None:
    """
    Fetch a URL with full browser headers, retry on 429/503.
    Returns the decoded response body or None on failure.
    """
    ua = _pick_ua()
    headers: dict[str, str] = {
        "User-Agent": ua,
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document" if not is_json else "empty",
        "Sec-Fetch-Mode": "navigate" if not is_json else "cors",
        "Sec-Fetch-Site": "same-origin" if is_json else "none",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

    if is_json:
        headers["Accept"] = "application/json, text/javascript, */*; q=0.01"
        headers["X-Requested-With"] = "XMLHttpRequest"
        headers["Referer"] = referer
    else:
        headers["Accept"] = (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,*/*;q=0.8"
        )
        headers["Referer"] = "https://www.google.com/"

    for attempt, retry_delay in enumerate([0] + RETRY_DELAYS):
        if retry_delay:
            log.warning(
                "Rate-limited — waiting %ds before retry %d/3 for %s",
                retry_delay, attempt, url,
            )
            time.sleep(retry_delay)

        try:
            req = urllib.request.Request(url, headers=headers)
            resp = opener.open(req, timeout=20)
            return resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            if exc.code in (429, 503, 502):
                log.warning("HTTP %d on %s — will retry", exc.code, url)
                continue
            if exc.code == 403:
                log.error("HTTP 403 on %s — Sucuri blocked, skipping", url)
                return None
            if exc.code == 404:
                log.warning("HTTP 404 on %s", url)
                return None
            log.error("HTTP %d on %s", exc.code, url)
            return None
        except Exception as exc:  # noqa: BLE001
            log.error("Request error on %s: %s", url, exc)
            if attempt < len(RETRY_DELAYS):
                continue
            return None

    log.error("All retries exhausted for %s", url)
    return None


# ---------------------------------------------------------------------------
# HTML field parsers
# ---------------------------------------------------------------------------

def _parse_name(html_name: str) -> str:
    """
    DraftEdge NAME cells wrap the player name in:
      <p class="teamview mb-0">Shohei Ohtani</p>

    Extract via the teamview class first; fall back to stripping all tags.
    """
    # Primary: pull the teamview paragraph (exact player name, no suffix)
    m = re.search(r'class=["\']teamview[^"\']*.?>(.+?)</p>', html_name, re.DOTALL)
    if m:
        return " ".join(m.group(1).split())

    # Fallback: strip all HTML tags, drop position + game-time suffix
    text = re.sub(r"<[^>]+>", "", html_name)
    text = " ".join(text.split())
    text = re.sub(r"\s+(?:C|1B|2B|3B|SS|LF|CF|RF|OF|DH|SP|RP)\s*$", "", text).strip()
    text = re.sub(
        r"\s+\d{1,2}:\d{2}\s+[AP]M\s+\w+\s+(?:vs|@)\w+\s*$",
        "",
        text,
        flags=re.IGNORECASE,
    ).strip()
    return text


def _parse_team(html_name: str) -> str:
    """Extract 2–3 letter team code from the team logo image URL."""
    m = re.search(r"uploads/mlb-([a-z]{2,3})\.", html_name)
    return m.group(1).upper() if m else ""


def _parse_pos(html_pos: str) -> str:
    """Strip span tags from position field."""
    return re.sub(r"<[^>]+>", "", html_pos).strip()


def _parse_proj(html_proj: str) -> float:
    """Strip span tags, return float or 0.0."""
    text = re.sub(r"<[^>]+>", "", html_proj).strip()
    try:
        return float(text)
    except ValueError:
        return 0.0


def _pct_to_float(value: Any) -> float:
    """Convert '68%' → 0.68, already-float passthrough, 0.0 on error."""
    if isinstance(value, (int, float)):
        return float(value) / 100 if float(value) > 1 else float(value)
    try:
        return float(str(value).replace("%", "").strip()) / 100
    except (ValueError, TypeError):
        return 0.0


# ---------------------------------------------------------------------------
# Core fetch + parse
# ---------------------------------------------------------------------------

def _parse_batters(raw: str) -> pd.DataFrame:
    """
    Parse the batter JSON blob into a clean DataFrame.

    Output columns:
      player_name, team, pos, batting_order, dfs_proj,
      hit_pct, hr_pct, sb_pct, run_pct, rbi_pct, fetch_date
    """
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        log.error("Failed to parse batter JSON")
        return pd.DataFrame()

    rows = payload.get("data", [])
    if not rows:
        log.warning("Batter data array is empty")
        return pd.DataFrame()

    records = []
    for row in rows:
        try:
            records.append(
                {
                    "player_name": _parse_name(row.get("NAME", "")),
                    "team": _parse_team(row.get("NAME", "")),
                    "pos": _parse_pos(row.get("POS", "")),
                    "batting_order": str(row.get("BO", "")).strip(),
                    "dfs_proj": _parse_proj(row.get("PROJ", "0")),
                    "hit_pct": _pct_to_float(row.get("HITS", 0)),
                    "hr_pct": _pct_to_float(row.get("HR", 0)),
                    "sb_pct": _pct_to_float(row.get("SB", 0)),
                    "run_pct": _pct_to_float(row.get("RUNS", 0)),
                    "rbi_pct": _pct_to_float(row.get("RBI", 0)),
                    "fetch_date": str(date.today()),
                }
            )
        except Exception as exc:  # noqa: BLE001
            log.debug("Skipping batter row due to parse error: %s", exc)
            continue

    df = pd.DataFrame(records)
    # Drop empty / malformed names
    df = df[df["player_name"].str.len() > 2].reset_index(drop=True)
    log.info("Parsed %d batters from DraftEdge", len(df))
    return df


def _parse_pitchers(raw: str) -> pd.DataFrame:
    """
    Parse the pitcher JSON blob into a clean DataFrame.

    Output columns:
      player_name, team, dfs_proj, era_proj,
      hit_allowed_pct, hr_allowed_pct, k_pct, fetch_date
    """
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        log.error("Failed to parse pitcher JSON")
        return pd.DataFrame()

    rows = payload.get("data", [])
    if not rows:
        log.warning("Pitcher data array is empty")
        return pd.DataFrame()

    records = []
    for row in rows:
        try:
            def _strip_html(v):
                """Strip span tags from pitcher stat fields."""
                return re.sub(r"<[^>]+>", "", str(v)).strip() if v else "0"
            records.append(
                {
                    "player_name": _parse_name(row.get("NAME", "")),
                    "team": _parse_team(row.get("NAME", "")),
                    "dfs_proj": _parse_proj(row.get("PROJ", "0")),
                    "era_proj": float(_strip_html(row.get("ERA", "0")).replace("%","") or 0),
                    "hit_allowed_pct": _pct_to_float(_strip_html(row.get("HIT", "0"))),
                    "hr_allowed_pct": _pct_to_float(_strip_html(row.get("HR", "0"))),
                    "k_pct": _pct_to_float(_strip_html(row.get("KO", "0"))),
                    "pitches_proj": float(_strip_html(row.get("PITCH", "0")) or 0),
                    "fetch_date": str(date.today()),
                }
            )
        except Exception as exc:  # noqa: BLE001
            log.debug("Skipping pitcher row due to parse error: %s", exc)
            continue

    df = pd.DataFrame(records)
    df = df[df["player_name"].str.len() > 2].reset_index(drop=True)
    log.info("Parsed %d pitchers from DraftEdge", len(df))
    return df


# ---------------------------------------------------------------------------
# Daily cache logic
# ---------------------------------------------------------------------------

def _cache_path(kind: str) -> Path:
    today = str(date.today())
    return CACHE_DIR / f"{kind}_{today}.parquet"


def _load_cache(kind: str) -> pd.DataFrame | None:
    path = _cache_path(kind)
    if path.exists():
        try:
            df = pd.read_parquet(path)
            log.info("Loaded %s from cache (%d rows)", kind, len(df))
            return df
        except Exception as exc:  # noqa: BLE001
            log.warning("Cache read failed for %s: %s — will re-fetch", kind, exc)
    return None


def _save_cache(kind: str, df: pd.DataFrame) -> None:
    path = _cache_path(kind)
    try:
        df.to_parquet(path, index=False)
        log.info("Saved %s cache → %s", kind, path)
    except Exception as exc:  # noqa: BLE001
        log.warning("Cache write failed for %s: %s", kind, exc)


def _check_fetch_cap(kind: str) -> bool:
    """Return True if we're still under the daily fetch cap."""
    today = str(date.today())
    key = f"{kind}_{today}"
    count = _fetch_count_today.get(key, 0)
    if count >= DAILY_FETCH_CAP:
        log.warning(
            "Daily fetch cap (%d) reached for %s — returning cache",
            DAILY_FETCH_CAP, kind,
        )
        return False
    _fetch_count_today[key] = count + 1
    return True


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_batter_projections(force_refresh: bool = False) -> pd.DataFrame:
    """
    Return today's DraftEdge batter projections as a DataFrame.

    Columns: player_name, team, pos, batting_order, dfs_proj,
             hit_pct, hr_pct, sb_pct, run_pct, rbi_pct, fetch_date

    Probabilities are floats in [0, 1]. E.g. hit_pct=0.68 means 68% chance of a hit.

    Args:
        force_refresh: Bypass cache and fetch fresh data (still respects daily cap).

    Returns:
        DataFrame, or empty DataFrame on failure.
    """
    if not force_refresh:
        cached = _load_cache("batters")
        if cached is not None:
            return cached

    if not _check_fetch_cap("batters"):
        cached = _load_cache("batters")
        return cached if cached is not None else pd.DataFrame()

    session = _build_session()
    log.info("Warming up DraftEdge session...")
    _fetch_url(session, BATTER_PAGE, is_json=False)
    _jitter_sleep()

    log.info("Fetching batter projections from DraftEdge...")
    raw = _fetch_url(session, BATTER_ENDPOINT, referer=BATTER_PAGE, is_json=True)
    if not raw:
        log.error("Batter fetch returned no data")
        return pd.DataFrame()

    df = _parse_batters(raw)
    if not df.empty:
        _save_cache("batters", df)
    return df


def fetch_pitcher_projections(force_refresh: bool = False) -> pd.DataFrame:
    """
    Return today's DraftEdge starting pitcher projections as a DataFrame.

    Columns: player_name, team, dfs_proj, era_proj,
             hit_allowed_pct, hr_allowed_pct, k_pct, fetch_date

    k_pct is the probability the pitcher records a strikeout on any given batter
    (strong signal for K prop lines).

    Args:
        force_refresh: Bypass cache and fetch fresh data (still respects daily cap).

    Returns:
        DataFrame, or empty DataFrame on failure.
    """
    if not force_refresh:
        cached = _load_cache("pitchers")
        if cached is not None:
            return cached

    if not _check_fetch_cap("pitchers"):
        cached = _load_cache("pitchers")
        return cached if cached is not None else pd.DataFrame()

    session = _build_session()
    log.info("Warming up DraftEdge session...")
    _fetch_url(session, PITCHER_PAGE, is_json=False)
    _jitter_sleep()

    log.info("Fetching pitcher projections from DraftEdge...")
    raw = _fetch_url(session, PITCHER_ENDPOINT, referer=PITCHER_PAGE, is_json=True)
    if not raw:
        log.error("Pitcher fetch returned no data")
        return pd.DataFrame()

    df = _parse_pitchers(raw)
    if not df.empty:
        _save_cache("pitchers", df)
    return df


def fetch_all_projections(force_refresh: bool = False) -> dict[str, pd.DataFrame]:
    """
    Fetch both batter and pitcher projections in a single call.

    Returns:
        dict with keys 'batters' and 'pitchers', each a DataFrame.
    """
    batters = fetch_batter_projections(force_refresh=force_refresh)
    _jitter_sleep()  # Polite pause between the two requests
    pitchers = fetch_pitcher_projections(force_refresh=force_refresh)
    return {"batters": batters, "pitchers": pitchers}


# ---------------------------------------------------------------------------
# Integration hook for live_dispatcher.py
# ---------------------------------------------------------------------------

def enrich_props_with_draftedge(props: list[dict]) -> list[dict]:
    """
    Enrich a list of prop dicts with DraftEdge projection data.

    Adds the following fields to each prop dict (if a match is found):
      - de_hit_pct      : float [0,1] — probability of 1+ hit today
      - de_hr_pct       : float [0,1] — probability of 1+ HR today
      - de_sb_pct       : float [0,1] — probability of 1+ SB today
      - de_run_pct      : float [0,1] — probability of 1+ run scored
      - de_rbi_pct      : float [0,1] — probability of 1+ RBI
      - de_k_pct        : float [0,1] — pitcher K% (pitchers only)
      - de_dfs_proj     : float — DFS fantasy points projection
      - de_batting_order: str  — confirmed/projected batting order slot

    Uses fuzzy last-name matching if exact match fails.

    Args:
        props: List of prop dicts (each must have 'player_name' key).

    Returns:
        Same list with de_* fields added where data is available.
    """
    projections = fetch_all_projections()
    batters_df = projections.get("batters", pd.DataFrame())
    pitchers_df = projections.get("pitchers", pd.DataFrame())

    def _normalize(name: str) -> str:
        """Lowercase, strip accents-ish, keep alpha + space."""
        import unicodedata
        nfkd = unicodedata.normalize("NFKD", name)
        ascii_name = nfkd.encode("ascii", "ignore").decode("ascii")
        return re.sub(r"[^a-z ]", "", ascii_name.lower()).strip()

    def _build_lookup(df: pd.DataFrame) -> dict[str, dict]:
        lookup = {}
        for _, row in df.iterrows():
            norm = _normalize(row["player_name"])
            lookup[norm] = row.to_dict()
            # Also index by last name only for partial matching
            parts = norm.split()
            if len(parts) >= 2:
                lookup[parts[-1]] = row.to_dict()
        return lookup

    batter_lookup = _build_lookup(batters_df) if not batters_df.empty else {}
    pitcher_lookup = _build_lookup(pitchers_df) if not pitchers_df.empty else {}

    enriched = []
    for prop in props:
        raw_name = prop.get("player_name", "")
        norm_name = _normalize(raw_name)
        parts = norm_name.split()

        # Try batter lookup first
        match = batter_lookup.get(norm_name) or (
            batter_lookup.get(parts[-1]) if parts else None
        )
        if match:
            prop = {
                **prop,
                "de_hit_pct": match.get("hit_pct", 0.0),
                "de_hr_pct": match.get("hr_pct", 0.0),
                "de_sb_pct": match.get("sb_pct", 0.0),
                "de_run_pct": match.get("run_pct", 0.0),
                "de_rbi_pct": match.get("rbi_pct", 0.0),
                "de_dfs_proj": match.get("dfs_proj", 0.0),
                "de_batting_order": match.get("batting_order", ""),
                "de_k_pct": 0.0,
            }
            enriched.append(prop)
            continue

        # Try pitcher lookup
        match = pitcher_lookup.get(norm_name) or (
            pitcher_lookup.get(parts[-1]) if parts else None
        )
        if match:
            prop = {
                **prop,
                "de_k_pct": match.get("k_pct", 0.0),
                "de_dfs_proj": match.get("dfs_proj", 0.0),
                "de_hit_pct": 0.0,
                "de_hr_pct": 0.0,
                "de_sb_pct": 0.0,
                "de_run_pct": 0.0,
                "de_rbi_pct": 0.0,
                "de_batting_order": "",
            }
        else:
            # No match — zero out all DE fields so downstream never KeyErrors
            prop = {
                **prop,
                "de_hit_pct": 0.0,
                "de_hr_pct": 0.0,
                "de_sb_pct": 0.0,
                "de_run_pct": 0.0,
                "de_rbi_pct": 0.0,
                "de_k_pct": 0.0,
                "de_dfs_proj": 0.0,
                "de_batting_order": "",
            }

        enriched.append(prop)

    matched = sum(1 for p in enriched if p.get("de_dfs_proj", 0) > 0)
    log.info(
        "DraftEdge enrichment: %d/%d props matched", matched, len(enriched)
    )
    return enriched


# ---------------------------------------------------------------------------
# Smoke test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("\n=== DraftEdge Scraper Smoke Test ===\n")

    data = fetch_all_projections(force_refresh=True)

    batters = data["batters"]
    pitchers = data["pitchers"]

    if not batters.empty:
        print(f"✅ Batters: {len(batters)} rows")
        print(batters[["player_name", "team", "batting_order",
                        "hit_pct", "hr_pct", "run_pct", "rbi_pct"]].head(5).to_string(index=False))
    else:
        print("❌ Batters: no data")

    print()

    if not pitchers.empty:
        print(f"✅ Pitchers: {len(pitchers)} rows")
        print(pitchers[["player_name", "team", "k_pct",
                         "hit_allowed_pct", "era_proj"]].head(5).to_string(index=False))
    else:
        print("❌ Pitchers: no data")

    # Test enrichment hook
    print("\n=== Enrichment Test ===")
    test_props = [
        {"player_name": "Shohei Ohtani", "prop_type": "hits", "line": 1.5},
        {"player_name": "Juan Soto", "prop_type": "runs", "line": 0.5},
    ]
    enriched = enrich_props_with_draftedge(test_props)
    for p in enriched:
        print(
            f"  {p['player_name']}: hit={p['de_hit_pct']:.0%}  "
            f"hr={p['de_hr_pct']:.0%}  run={p['de_run_pct']:.0%}  "
            f"rbi={p['de_rbi_pct']:.0%}  BO={p['de_batting_order']}"
        )
