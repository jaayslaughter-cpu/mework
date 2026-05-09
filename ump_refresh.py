"""
ump_refresh.py
==============
Refreshes umpire_rates.py static tables from live sources.

Sources (tried in order):
  1. swishanalytics.com/mlb/mlb-umpire-factors  — K%, BB%, RPG, K Boost, BB Boost, R Boost
  2. umpscorecards.com/api/umpires               — run_impact, accuracy (already in umpire_rates.py)

Scraped data is written into umpire_rates._UMPIRE_TABLE and _STATIC_RUN_IMPACT
in-process (no file rewrite needed — the module dicts are mutable globals).
Also persists to Redis so the refresh survives restarts within the same day.

Scheduler slot: called by job_ump_refresh (weekly Monday 3:00 AM PT) in orchestrator.py.

Public API
----------
refresh()                 → dict with counts and status
get_swish_ump_table()     → raw {name: {k_pct, bb_pct, rpg, k_boost, bb_boost}} from swish
"""
from __future__ import annotations

import json
import logging
import os
import unicodedata
from datetime import date
from typing import Any

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_SWISH_URL = "https://swishanalytics.com/mlb/mlb-umpire-factors"
_SWISH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Redis cache key — persists the swish table across restarts within same day
_REDIS_KEY    = "ump_refresh_swish"
_REDIS_TTL    = 86400  # 24h

# League average denominators used to compute k_mod / bb_mod
_LEAGUE_K_9   = 8.8
_LEAGUE_BB_9  = 3.1

# Swishanalytics reports K% and BB% as fractions (0–1) and RPG as runs/game.
# We convert K% → K/9 using the approximation K/9 ≈ K% × 27  (9 innings × 3 outs/inn)
# This matches the scale of _UMPIRE_TABLE entries (range 7.8–9.8) reasonably well.
_K_PCT_TO_K9  = 27.0
_BB_PCT_TO_BB9 = 27.0


# ── Helpers ────────────────────────────────────────────────────────────────────

def _norm(name: str) -> str:
    """Lowercase, strip diacritics, collapse whitespace."""
    n = unicodedata.normalize("NFD", name.lower().strip())
    return " ".join("".join(c for c in n if unicodedata.category(c) != "Mn").split())


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(str(v).replace("%", "").replace("x", "").strip())
    except (TypeError, ValueError):
        return default


def _get_redis():
    try:
        import redis as _r
        url = os.environ.get("REDIS_URL") or os.environ.get("REDIS_PUBLIC_URL")
        if not url:
            return None
        return _r.from_url(url, decode_responses=True, socket_connect_timeout=3)
    except Exception:
        return None


# ── Swishanalytics scraper ─────────────────────────────────────────────────────

def get_swish_ump_table() -> dict[str, dict]:
    """
    Scrape swishanalytics.com/mlb/mlb-umpire-factors.

    Returns {name_lower: {k_pct, bb_pct, rpg, ba, obp, slg, k_boost, bb_boost, r_boost}}
    with raw values as floats.  Returns {} on any failure.

    Table columns (0-indexed from stat_miner.py reference):
      0: Umpire name
      1: Games
      2: ERA
      3: K%
      4: BB%
      5: RPG
      6: BA
      7: OBP
      8: SLG
      9: K Boost
      10: BB Boost
      11: R Boost
      12: BA Boost
      13: OBP Boost
      14: SLG Boost
    """
    try:
        resp = requests.get(_SWISH_URL, headers=_SWISH_HEADERS, timeout=20)
        if resp.status_code != 200:
            logger.warning("[UmpRefresh] Swish returned HTTP %d", resp.status_code)
            return {}

        soup = BeautifulSoup(resp.text, "lxml")
        table = soup.find("table", {"id": "ump-table"})
        if table is None:
            # Try any table on the page
            table = soup.find("table")
        if table is None:
            logger.warning("[UmpRefresh] No table found on swishanalytics page")
            return {}

        tbody = table.find("tbody")
        if tbody is None:
            logger.warning("[UmpRefresh] Table has no tbody")
            return {}

        rows = tbody.find_all("tr")
        result: dict[str, dict] = {}

        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 12:
                continue
            try:
                name = cells[0].get_text(strip=True)
                if not name:
                    continue
                result[_norm(name)] = {
                    "name":     name,
                    "games":    _safe_float(cells[1].get_text(strip=True)),
                    "era":      _safe_float(cells[2].get_text(strip=True)),
                    "k_pct":    _safe_float(cells[3].get_text(strip=True)),
                    "bb_pct":   _safe_float(cells[4].get_text(strip=True)),
                    "rpg":      _safe_float(cells[5].get_text(strip=True)),
                    "ba":       _safe_float(cells[6].get_text(strip=True)),
                    "obp":      _safe_float(cells[7].get_text(strip=True)),
                    "slg":      _safe_float(cells[8].get_text(strip=True)),
                    "k_boost":  _safe_float(cells[9].get_text(strip=True)),
                    "bb_boost": _safe_float(cells[10].get_text(strip=True)),
                    "r_boost":  _safe_float(cells[11].get_text(strip=True)),
                }
            except (IndexError, AttributeError):
                continue

        logger.info("[UmpRefresh] Swish: scraped %d umpires", len(result))
        return result

    except Exception as exc:
        logger.warning("[UmpRefresh] Swish scrape failed: %s", exc)
        return {}


# ── Redis persistence ──────────────────────────────────────────────────────────

def _load_from_redis() -> dict[str, dict]:
    r = _get_redis()
    if not r:
        return {}
    try:
        raw = r.get(_REDIS_KEY)
        if raw:
            data = json.loads(raw)
            logger.debug("[UmpRefresh] Loaded %d umps from Redis cache", len(data))
            return data
    except Exception as exc:
        logger.debug("[UmpRefresh] Redis load failed: %s", exc)
    return {}


def _save_to_redis(table: dict[str, dict]) -> None:
    r = _get_redis()
    if not r:
        return
    try:
        r.setex(_REDIS_KEY, _REDIS_TTL, json.dumps(table))
        logger.debug("[UmpRefresh] Saved %d umps to Redis", len(table))
    except Exception as exc:
        logger.debug("[UmpRefresh] Redis save failed: %s", exc)


# ── Main refresh logic ─────────────────────────────────────────────────────────

def refresh() -> dict:
    """
    Scrape swishanalytics and update umpire_rates in-process.

    Updates:
      umpire_rates._UMPIRE_TABLE       — (k_rate/9, bb_rate/9) per ump name
      umpire_rates._STATIC_RUN_IMPACT  — run_impact proxy per ump name
      Redis cache                       — for cross-restart persistence

    Returns:
      {"scraped": N, "updated": M, "source": "swish"|"redis"|"none",
       "date": "YYYY-MM-DD"}
    """
    import umpire_rates as _ur  # late import so module is always fresh

    # 1. Try live scrape
    swish = get_swish_ump_table()

    if not swish:
        # 2. Fall back to Redis cache (yesterday's scrape still useful)
        swish = _load_from_redis()
        source = "redis" if swish else "none"
    else:
        _save_to_redis(swish)
        source = "swish"

    if not swish:
        logger.warning("[UmpRefresh] No ump data available from swish or redis — skipping update")
        return {"scraped": 0, "updated": 0, "source": "none", "date": date.today().isoformat()}

    updated = 0
    for name_lower, stats in swish.items():
        # Convert K% → approximate K/9, BB% → approximate BB/9
        # swish K% is e.g. 0.213 (21.3%) — multiply by 27 to get per-9-innings rate
        k_pct  = stats.get("k_pct",  0.0)
        bb_pct = stats.get("bb_pct", 0.0)

        # Handle if swish returns values as percentages (21.3) vs fractions (0.213)
        if k_pct > 1.0:
            k_pct  = k_pct  / 100.0
        if bb_pct > 1.0:
            bb_pct = bb_pct / 100.0

        k9  = round(k_pct  * _K_PCT_TO_K9,  2)
        bb9 = round(bb_pct * _BB_PCT_TO_BB9, 2)

        # Sanity clamp — league avg K/9 ~ 8.8, range ~7.0–11.5
        k9  = max(6.0, min(12.0, k9))  if k9  > 0 else _LEAGUE_K_9
        bb9 = max(2.0, min(5.0,  bb9)) if bb9 > 0 else _LEAGUE_BB_9

        # Update _UMPIRE_TABLE (mutates the module-level dict)
        _ur._UMPIRE_TABLE[name_lower] = (k9, bb9)

        # run_impact proxy: use R Boost.  R Boost > 1.0 = hitter-friendly.
        # We map: boost 1.0 → impact 0.0 (neutral), >1.0 → positive, <1.0 → negative
        r_boost = stats.get("r_boost", 1.0)
        if r_boost > 0:
            run_impact = round((r_boost - 1.0) * 0.5, 3)  # scale: boost 1.1 → +0.05 runs
        else:
            run_impact = 0.0
        run_impact = max(-0.8, min(0.8, run_impact))
        _ur._STATIC_RUN_IMPACT[name_lower] = run_impact

        updated += 1

    # Invalidate the live cache so next call to get_umpire_rates() re-reads fresh data
    _ur._LIVE_CACHE = {}
    _ur._LIVE_CACHE_DATE = ""

    logger.info(
        "[UmpRefresh] Updated %d umpires in umpire_rates tables (source=%s)",
        updated, source,
    )
    return {
        "scraped": len(swish),
        "updated": updated,
        "source":  source,
        "date":    date.today().isoformat(),
    }


# ── CLI entry point ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    result = refresh()
    print(f"UmpRefresh: {result}")

    # Print sample of updated table
    try:
        import umpire_rates as _ur
        sample = sorted(_ur._UMPIRE_TABLE.items())[:5]
        print("Sample _UMPIRE_TABLE entries:")
        for name, (k9, bb9) in sample:
            ri = _ur._STATIC_RUN_IMPACT.get(name, 0.0)
            print(f"  {name:<30} K/9={k9:.1f}  BB/9={bb9:.1f}  run_impact={ri:+.3f}")
    except Exception as e:
        print(f"Could not print sample: {e}")
