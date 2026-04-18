"""
injury_layer.py
===============
Fetches current MLB injury and availability data for PropIQ.

Sources (in priority order):
  1. ESPN site API  — site.api.espn.com/apis/site/v2/sports/baseball/mlb/news
                      same domain proven working on Railway. Returns injury
                      news items with player names and status keywords.
  2. Action Network PRO — api.actionnetwork.com/web/v1/injuries
                          Uses existing JWT (ACTION_NETWORK_JWT env var).
                          Returns structured status per player.

Output schema per player (stored in hub["injuries"]):
    {
        "player_name":  "Spencer Strider",
        "status":       "IL-15",          # IL-10, IL-15, IL-60, DTD, OUT, QUESTIONABLE
        "is_il":        True,             # True if on any IL
        "is_dtd":       False,            # True if day-to-day
        "is_out":       False,            # True if OUT / QUESTIONABLE
        "detail":       "right elbow inflammation",
        "source":       "espn",
    }

Usage in agents / enrichment:
    from injury_layer import get_injury_status, is_player_available

    status = get_injury_status("Spencer Strider")   # → dict or None
    ok     = is_player_available("Spencer Strider") # → False if IL/OUT

DataHub wires this in automatically — agents do not call it directly.
The hub["injuries"] list is used by prop_enrichment_layer to stamp
injury flags onto props before agent evaluation.
"""
from __future__ import annotations

import logging
import os
import re
import unicodedata
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_TIMEOUT = 10
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

# ESPN news endpoint — same base domain as scoreboard (proven Railway-accessible)
_ESPN_NEWS_URL = "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/news"

# Action Network PRO injury endpoint
_AN_INJURY_URL  = "https://api.actionnetwork.com/web/v1/injuries"
_AN_SPORT_PARAM = {"sport": "baseball"}

# Status keywords to classify from ESPN injury text
_IL_60_KEYWORDS   = ["60-day il", "60 day il", "60-day", "60 day"]
_IL_15_KEYWORDS   = ["15-day il", "15 day il", "15-day", "15 day"]
_IL_10_KEYWORDS   = ["10-day il", "10 day il", "10-day", "10 day"]
_DTD_KEYWORDS     = ["day-to-day", "day to day", "dtd", "questionable"]
_OUT_KEYWORDS     = ["out indefinitely", "out for season", "season-ending",
                     "surgery", "placed on the", "transferred to the 60"]
_SKIP_KEYWORDS    = ["activated", "reinstated", "returned", "recalled",
                     "selected", "optioned", "designated for assignment",
                     "traded", "signed", "released"]   # healthy transactions

# Confidence penalties applied by prop_enrichment_layer
PENALTY_IL     = 1.00   # full skip — on IL means prop shouldn't exist, but
                         # if it somehow shows up (early posting), block it
PENALTY_OUT    = 0.90   # 90% confidence penalty (near-skip)
PENALTY_DTD    = 0.25   # 25% confidence penalty
PENALTY_QUEST  = 0.15   # 15% confidence penalty


# ---------------------------------------------------------------------------
# Name normalisation
# ---------------------------------------------------------------------------

def _norm(name: str) -> str:
    n = unicodedata.normalize("NFD", name.lower().strip())
    n = "".join(c for c in n if unicodedata.category(c) != "Mn")
    return " ".join(n.split())


# ---------------------------------------------------------------------------
# ESPN injury news parser
# ---------------------------------------------------------------------------

def _parse_espn_news(items: list[dict]) -> list[dict]:
    """
    Parse ESPN news items into structured injury records.

    ESPN returns general MLB news — we filter to injury-relevant items
    by checking headline + description for IL/DTD/OUT keywords.
    Items about activations/reinstatements are explicitly excluded.
    """
    injuries: list[dict] = []

    for item in items:
        headline    = (item.get("headline")    or "").lower()
        description = (item.get("description") or "").lower()
        text        = f"{headline} {description}"

        # Skip healthy transactions (activations, returns)
        if any(kw in text for kw in _SKIP_KEYWORDS):
            continue

        # Classify status
        status   = None
        is_il    = False
        is_dtd   = False
        is_out   = False

        if any(kw in text for kw in _IL_60_KEYWORDS):
            status, is_il = "IL-60", True
        elif any(kw in text for kw in _IL_15_KEYWORDS):
            status, is_il = "IL-15", True
        elif any(kw in text for kw in _IL_10_KEYWORDS):
            status, is_il = "IL-10", True
        elif any(kw in text for kw in _OUT_KEYWORDS):
            status, is_out = "OUT", True
        elif any(kw in text for kw in _DTD_KEYWORDS):
            status, is_dtd = "DTD", True

        if not status:
            continue   # not an injury item

        # Extract player name from the 'athletes' field if present,
        # otherwise fall back to extracting from headline
        player_name = ""
        for athlete in (item.get("athletes") or []):
            fn = (athlete.get("athlete") or {}).get("fullName", "")
            if fn:
                player_name = fn
                break

        if not player_name:
            # Headline format is often "Player Name placed on IL" — grab first
            # two capitalised words as a rough name extraction
            caps = re.findall(r'[A-Z][a-z]+(?:\s[A-Z][a-z]+)+', item.get("headline", ""))
            if caps:
                player_name = caps[0]

        if not player_name:
            continue

        # Detail: first sentence of description
        detail = (item.get("description") or "").split(".")[0].strip()

        injuries.append({
            "player_name": player_name,
            "status":      status,
            "is_il":       is_il,
            "is_dtd":      is_dtd,
            "is_out":      is_out,
            "detail":      detail[:120],
            "source":      "espn",
        })

    return injuries


def _fetch_espn_injuries() -> list[dict]:
    """Fetch MLB injury news from ESPN site API."""
    try:
        resp = requests.get(
            _ESPN_NEWS_URL,
            params={"limit": "50"},
            headers=_HEADERS,
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        data  = resp.json()
        items = data.get("articles") or data.get("items") or []
        parsed = _parse_espn_news(items)
        logger.info("[Injuries] ESPN: %d injury items from %d news articles",
                    len(parsed), len(items))
        return parsed
    except Exception as exc:
        logger.warning("[Injuries] ESPN fetch failed: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Action Network PRO injury fetch
# ---------------------------------------------------------------------------

def _fetch_an_injuries() -> list[dict]:
    """
    Fetch injuries from Action Network PRO API.
    Uses ACTION_NETWORK_JWT env var (same as action_network_layer.py).
    """
    token = os.getenv("ACTION_NETWORK_JWT", "")
    if not token:
        logger.debug("[Injuries] ACTION_NETWORK_JWT not set — skipping AN injuries")
        return []

    try:
        resp = requests.get(
            _AN_INJURY_URL,
            params=_AN_SPORT_PARAM,
            headers={
                **_HEADERS,
                "Authorization": f"Bearer {token}",
            },
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        data   = resp.json()
        # AN returns list of injury objects or {"injuries": [...]}
        items  = data if isinstance(data, list) else data.get("injuries", [])
        result = []
        for item in items:
            player = (item.get("player") or {})
            name   = (player.get("full_name") or player.get("name") or "").strip()
            if not name:
                continue
            raw_status = (item.get("status") or "").upper()
            # Map AN status strings to our schema
            if "IL" in raw_status or "INJURED" in raw_status:
                status, is_il, is_dtd, is_out = "IL-15", True, False, False
                if "60" in raw_status:
                    status = "IL-60"
                elif "10" in raw_status:
                    status = "IL-10"
            elif "DAY" in raw_status or "DTD" in raw_status:
                status, is_il, is_dtd, is_out = "DTD", False, True, False
            elif "OUT" in raw_status or "QUEST" in raw_status:
                status, is_il, is_dtd, is_out = "OUT", False, False, True
            else:
                continue  # healthy / unknown

            result.append({
                "player_name": name,
                "status":      status,
                "is_il":       is_il,
                "is_dtd":      is_dtd,
                "is_out":      is_out,
                "detail":      (item.get("injury_type") or item.get("comment") or "")[:120],
                "source":      "action_network",
            })

        logger.info("[Injuries] Action Network: %d injury records", len(result))
        return result
    except Exception as exc:
        logger.warning("[Injuries] Action Network fetch failed: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Main fetch — merges both sources, deduplicates, AN takes priority
# ---------------------------------------------------------------------------

def fetch_injuries() -> list[dict]:
    """
    Fetch and merge injury data from ESPN + Action Network PRO.

    Deduplication: if a player appears in both sources, Action Network
    takes priority (more structured data). ESPN fills in players AN missed.

    Returns list of injury dicts ready to store in hub["injuries"].
    """
    # Primary: Action Network (structured, explicit status fields)
    an_injuries = _fetch_an_injuries()

    # Secondary: ESPN news (catches injuries AN may lag on)
    espn_injuries = _fetch_espn_injuries()

    # Merge: AN takes priority, ESPN fills gaps
    seen: dict[str, dict] = {}
    for rec in an_injuries:
        seen[_norm(rec["player_name"])] = rec
    for rec in espn_injuries:
        key = _norm(rec["player_name"])
        if key not in seen:
            seen[key] = rec

    result = list(seen.values())
    logger.info(
        "[Injuries] Combined: %d injured/limited players (%d IL, %d DTD/OUT)",
        len(result),
        sum(1 for r in result if r["is_il"]),
        sum(1 for r in result if r["is_dtd"] or r["is_out"]),
    )
    return result


# ---------------------------------------------------------------------------
# Public interface — used by prop_enrichment_layer and agents
# ---------------------------------------------------------------------------

# Module-level cache — refreshed each DataHub cycle via hub
_INJURY_MAP: dict[str, dict] = {}   # norm_name → injury record


def load_from_hub(hub: dict) -> None:
    """Called by prop_enrichment_layer at start of each eval cycle."""
    global _INJURY_MAP
    _INJURY_MAP = {
        _norm(rec["player_name"]): rec
        for rec in (hub.get("injuries") or [])
        if rec.get("player_name")
    }


def get_injury_status(player_name: str) -> Optional[dict]:
    """
    Return injury record for player, or None if healthy / unknown.

    Example:
        status = get_injury_status("Spencer Strider")
        # → {"player_name": "Spencer Strider", "status": "IL-15",
        #     "is_il": True, "is_dtd": False, "is_out": False,
        #     "detail": "right elbow inflammation", "source": "espn"}
    """
    return _INJURY_MAP.get(_norm(player_name))


def is_player_available(player_name: str) -> bool:
    """
    Return False if player is on IL or listed as OUT.
    Return True if healthy, DTD (penalised but not blocked), or unknown.
    """
    rec = get_injury_status(player_name)
    if rec is None:
        return True   # not in injury list → assumed healthy
    return not (rec["is_il"] or rec["is_out"])


def get_confidence_penalty(player_name: str) -> float:
    """
    Return confidence penalty multiplier for player (0.0–1.0).
    1.0 = no penalty, 0.0 = full block.

    Used by prop_enrichment_layer to scale model_prob_pct.
    """
    rec = get_injury_status(player_name)
    if rec is None:
        return 0.0          # no penalty
    if rec["is_il"]:
        return PENALTY_IL   # should be blocked entirely
    if rec["is_out"]:
        return PENALTY_OUT
    if rec["is_dtd"]:
        return PENALTY_DTD
    return PENALTY_QUEST    # QUESTIONABLE catch-all
