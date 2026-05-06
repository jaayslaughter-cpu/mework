"""
odds_api_net_layer.py
=====================
Sportsbook reference data from odds-api.net — Tier 1.5 in the fallback chain.

Fires AFTER OddsAPI (the-odds-api.com) quota exhaustion and before PropOdds.
Provides real bet365 + betr MLB player prop lines.

API key: set ODDS_API_NET_KEY in Railway env vars.
Get a free key at https://odds-api.net — no quota shown for free tier.
Auth: X-API-Key header.

Coverage (MLB):
  bet365 : player strikeouts, player hits, player hits allowed,
            player total bases, player hits runs and rbis, player rbi, player runs
  betr   : player hits, player hits allowed, player home runs, player rbi, player stolen bases

Market → prop_type mapping mirrors sportsbook_reference_layer.py conventions.

Output format:
  {(player_name, market_key, side): {"sharp_prob": float, "source": "odds_api_net", ...}}

Redis cache: odds_api_net_{date_int} — 12-hour TTL.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime, date
from zoneinfo import ZoneInfo

import requests

log = logging.getLogger("propiq.odds_api_net_layer")

_TZ      = ZoneInfo("America/Los_Angeles")
_BASE    = "https://api.odds-api.net/v1"
_TIMEOUT = 20

# ── Prop market → internal prop_type ──────────────────────────────────────────
_MARKET_TO_PROP: dict[str, str] = {
    "player strikeouts":          "pitcher_strikeouts",
    "player strikeouts thrown":   "pitcher_strikeouts",
    "player pitcher strikeouts":  "pitcher_strikeouts",
    "player hits":                "hits",
    "player hits allowed":        "hits_allowed",
    "player hits allowed pitched":"hits_allowed",
    "player total bases":         "total_bases",
    "player total bases (hits)":  "total_bases",
    "player hits runs and rbis":  "hits_runs_rbis",
    "player hits + runs + rbis":  "hits_runs_rbis",
    "player rbi":                 "rbis",
    "player rbis":                "rbis",
    "player runs":                "runs",
    "player runs scored":         "runs",
    "player home runs":           "home_runs",
    "player stolen bases":        "stolen_bases",
    "player walks":               "walks",
    "player walks (batter)":      "walks",
    "player pitching walks":      "walks_allowed",
    "player walks allowed":       "walks_allowed",
    "player earned runs":         "earned_runs",
    "player earned runs allowed": "earned_runs",
    "player outs":                "pitching_outs",
    "player pitching outs":       "pitching_outs",
    "player hitter strikeouts":   "hitter_strikeouts",
    "player batter strikeouts":   "hitter_strikeouts",
}

# Props blocked by Prop Exclusion Directive (never evaluated)
_EXCLUDED: frozenset[str] = frozenset({
    "stolen_bases", "home_runs", "walks", "doubles", "triples", "singles",
})

# Target bookmakers — bet365 + betr have best MLB player prop coverage
_TARGET_BOOKS = {"bet365", "betr", "fanduel", "draftkings", "betmgm", "caesars"}


# ── Utilities ─────────────────────────────────────────────────────────────────

def _norm_name(s: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace."""
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", "", s.lower())).strip()


def _to_prob(price: int | float) -> float | None:
    """Convert American or decimal odds to implied probability."""
    try:
        p = float(price)
    except (TypeError, ValueError):
        return None
    if p == 0:
        return None
    if abs(p) >= 100:
        # American odds
        if p > 0:
            return 100.0 / (p + 100.0)
        else:
            return abs(p) / (abs(p) + 100.0)
    else:
        # Decimal odds (already a multiplier ≥ 1.0 usually, but sanity check)
        if p < 1.01:
            return None
        return 1.0 / p


def _headers() -> dict:
    key = os.getenv("ODDS_API_NET_KEY", "")
    return {"X-API-Key": key, "Accept": "application/json"}


def _get(path: str, params: dict | None = None) -> dict | list | None:
    """Single GET with basic error handling."""
    key = os.getenv("ODDS_API_NET_KEY", "")
    if not key:
        return None
    url = f"{_BASE}/{path.lstrip('/')}"
    try:
        r = requests.get(url, headers=_headers(), params=params or {}, timeout=_TIMEOUT)
        if r.status_code == 401:
            log.warning("[OddsApiNet] 401 Unauthorized — verify ODDS_API_NET_KEY at odds-api.net")
            return None
        if r.status_code == 403:
            log.warning("[OddsApiNet] 403 Forbidden — key lacks MLB player prop plan")
            return None
        if r.status_code == 429:
            retry = int(r.headers.get("Retry-After", "30"))
            log.warning("[OddsApiNet] Rate limited — sleeping %ds", retry)
            time.sleep(min(retry, 30))
            return None
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        log.debug("[OddsApiNet] GET %s failed: %s", path, exc)
        return None


# ── League / Event discovery ───────────────────────────────────────────────────

def _get_mlb_league_id() -> str | None:
    """Return odds-api.net league ID for MLB."""
    data = _get("/leagues", {"sport": "baseball"})
    if not isinstance(data, (dict, list)):
        return None
    items = data if isinstance(data, list) else data.get("data", [])
    for item in items:
        name = str(item.get("name") or item.get("title") or "").lower()
        key = str(item.get("key") or item.get("id") or "").lower()
        if "mlb" in name or "mlb" in key or "major league baseball" in name:
            return str(item.get("id") or item.get("key") or "")
    return None


def _get_todays_mlb_events(league_id: str) -> list[dict]:
    """Return today's MLB event dicts."""
    today_pt = datetime.now(_TZ)
    from_ts = today_pt.strftime("%Y-%m-%d")
    to_ts   = today_pt.strftime("%Y-%m-%d")
    data = _get("/events", {
        "league": league_id,
        "from":   from_ts,
        "to":     to_ts,
        "limit":  30,
    })
    if not isinstance(data, (dict, list)):
        return []
    return data if isinstance(data, list) else data.get("data", [])


# ── Odds fetching ──────────────────────────────────────────────────────────────

def _fetch_event_props(event_id: str, bookmakers: list[str]) -> dict:
    """
    Fetch player prop odds snapshot for one event.
    Returns {(player_name_norm, prop_type, side): {sharp_prob, line, bookmaker, source}}.
    """
    result: dict = {}
    books_param = ",".join(bookmakers)
    data = _get(f"/events/{event_id}/odds/snapshot", {
        "bookmakers": books_param,
        "limit": 5000,
    })
    if not isinstance(data, (dict, list)):
        return result

    odds_list = data if isinstance(data, list) else data.get("data", [])
    if not isinstance(odds_list, list):
        # Some schemas wrap in {"odds": [...]}
        odds_list = data.get("odds", []) if isinstance(data, dict) else []

    for entry in odds_list:
        if not isinstance(entry, dict):
            continue

        # Market name → prop_type
        market_raw = str(entry.get("market") or entry.get("market_key") or entry.get("name") or "").lower()
        prop_type = _MARKET_TO_PROP.get(market_raw)
        if prop_type is None:
            # Try partial match
            for mkt, pt in _MARKET_TO_PROP.items():
                if mkt in market_raw or market_raw in mkt:
                    prop_type = pt
                    break
        if not prop_type or prop_type in _EXCLUDED:
            continue

        # Player name
        player_raw = (
            entry.get("participant") or entry.get("player") or
            entry.get("player_name") or entry.get("selection_name") or ""
        )
        player_norm = _norm_name(str(player_raw))
        if not player_norm or len(player_norm) < 3:
            continue

        # Line
        try:
            line = float(entry.get("point") or entry.get("handicap") or entry.get("line") or 0)
        except (TypeError, ValueError):
            line = 0.0

        # Over / Under prices → Higher / Lower convention
        over_price  = entry.get("over_price")  or entry.get("price_over")  or entry.get("back_price")
        under_price = entry.get("under_price") or entry.get("price_under") or entry.get("lay_price")

        bookmaker = str(entry.get("bookmaker") or entry.get("book") or "odds_api_net").lower()

        for side_label, raw_price in [("higher", over_price), ("lower", under_price)]:
            prob = _to_prob(raw_price)
            if prob is None or prob <= 0.02 or prob >= 0.98:
                continue
            key = (player_norm, prop_type, side_label)
            # Keep the sharpest bookmaker (lowest vig = prob closest to 0.5)
            existing = result.get(key)
            if existing is None or abs(prob - 0.5) < abs(existing["sharp_prob"] - 0.5):
                result[key] = {
                    "sharp_prob": round(prob, 4),
                    "line":       line,
                    "bookmaker":  bookmaker,
                    "source":     "odds_api_net",
                }

    return result


# ── Redis helpers ──────────────────────────────────────────────────────────────

def _redis_get(key: str) -> str | None:
    try:
        import redis as _redis
        r = _redis.from_url(os.getenv("REDIS_URL", ""))
        v = r.get(key)
        return v.decode() if v else None
    except Exception:
        return None


def _redis_setex(key: str, ttl: int, value: str) -> None:
    try:
        import redis as _redis
        r = _redis.from_url(os.getenv("REDIS_URL", ""))
        r.setex(key, ttl, value)
    except Exception:
        pass


# ── Public interface ───────────────────────────────────────────────────────────

def fetch_mlb_props(date_int: int | None = None) -> dict:
    """
    Fetch today's MLB player prop lines from odds-api.net.

    Returns {(player_name_norm, prop_type, side): {"sharp_prob": float, ...}}
    or {} if ODDS_API_NET_KEY not set / key invalid / no data.

    Results cached in Redis for 12 hours.
    """
    key = os.getenv("ODDS_API_NET_KEY", "")
    if not key:
        log.debug("[OddsApiNet] ODDS_API_NET_KEY not set — skipped")
        return {}

    today_pt = datetime.now(_TZ)
    _date_int = date_int or int(today_pt.strftime("%Y%m%d"))
    cache_key = f"odds_api_net_{_date_int}"

    # Redis cache hit
    cached = _redis_get(cache_key)
    if cached:
        try:
            raw = json.loads(cached)
            result = {tuple(json.loads(k)): v for k, v in raw.items()}
            log.info("[OddsApiNet] Redis cache hit: %d entries", len(result))
            return result
        except Exception:
            pass

    # Discover MLB league
    league_id = _get_mlb_league_id()
    if not league_id:
        log.warning("[OddsApiNet] Could not discover MLB league ID")
        return {}

    # Get today's games
    events = _get_todays_mlb_events(league_id)
    if not events:
        log.warning("[OddsApiNet] No MLB events found for %d", _date_int)
        return {}

    log.info("[OddsApiNet] Found %d MLB events — fetching props", len(events))

    # Fetch props for each event
    all_props: dict = {}
    books = list(_TARGET_BOOKS)
    for event in events:
        event_id = str(event.get("id") or event.get("event_id") or "")
        if not event_id:
            continue
        props = _fetch_event_props(event_id, books)
        all_props.update(props)
        time.sleep(0.1)  # polite pacing

    if not all_props:
        log.warning("[OddsApiNet] No player props returned from %d events", len(events))
        return {}

    log.info("[OddsApiNet] %d player prop entries fetched from odds-api.net", len(all_props))

    # Cache in Redis 12h
    try:
        serialisable = {json.dumps(list(k)): v for k, v in all_props.items()}
        _redis_setex(cache_key, 43200, json.dumps(serialisable))
    except Exception as exc:
        log.debug("[OddsApiNet] Redis save failed: %s", exc)

    return all_props


def get_sharp_prob(
    player_name: str,
    prop_type: str,
    side: str,
    date_int: int | None = None,
) -> float | None:
    """
    Look up a single player/prop/side sharp prob.
    Returns implied probability (0–1) or None.
    """
    props = fetch_mlb_props(date_int)
    if not props:
        return None
    norm = _norm_name(player_name)
    # Exact match
    key = (norm, prop_type, side.lower())
    hit = props.get(key)
    if hit:
        return hit["sharp_prob"]
    # Fuzzy: last name match
    last = norm.split()[-1] if norm.split() else norm
    for (pn, pt, sd), v in props.items():
        if pt == prop_type and sd == side.lower() and pn.endswith(last):
            return v["sharp_prob"]
    return None
