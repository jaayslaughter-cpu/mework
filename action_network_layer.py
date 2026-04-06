"""
action_network_layer.py
=======================
Fetches MLB public betting data from Action Network.

The scoreboard public-betting endpoint is fully public (no auth required).
Returns game-level ticket% / money% per team for today's MLB slate.

Usage
-----
    from action_network_layer import fetch_mlb_game_sentiment

    sentiment = fetch_mlb_game_sentiment()
    # -> {"new york yankees": {over_ticket_pct, over_money_pct, rlm_signal, ...}, ...}
"""

import logging
import requests
from datetime import datetime
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_PT = ZoneInfo("America/Los_Angeles")

# Books included: Bovada(15), Caesars(30), BetMGM(79), FanDuel(2988),
#                  Bet365(75), PointsBet(123), WynnBet(71), DraftKings(68), BetRivers(69)
_BOOK_IDS = "15,30,79,2988,75,123,71,68,69"
_PRIMARY_BOOK = "68"  # DraftKings — most liquid, most reliable ticket data
_FALLBACK_BOOKS = ["15", "69", "79", "2988"]

_BASE_URL = (
    "https://api.actionnetwork.com/web/v2/scoreboard/publicbetting/mlb"
    "?bookIds={books}&date={date}&periods=event"
)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Referer": "https://www.actionnetwork.com/mlb/public-betting",
    "Origin": "https://www.actionnetwork.com",
}

# In-process daily cache so hub refresh cycles don't hammer the endpoint
_CACHE: dict = {}
_CACHE_DATE: str = ""


# ── helpers ──────────────────────────────────────────────────────────────────

def _today_pt() -> str:
    return datetime.now(_PT).strftime("%Y%m%d")


def _best_book_data(markets: dict) -> dict:
    """Return the first non-empty event-level market block for any known book."""
    for bid in [_PRIMARY_BOOK] + _FALLBACK_BOOKS:
        bd = markets.get(bid, {}).get("event", {})
        if bd:
            return bd
    return {}


def _parse_total(book_data: dict) -> dict:
    """
    Pull ticket% / money% for Over and Under from the total market.
    Returns dict with keys: over_ticket_pct, over_money_pct,
                             under_ticket_pct, under_money_pct, game_total.
    """
    out = {
        "over_ticket_pct": 50,
        "over_money_pct":  50,
        "under_ticket_pct": 50,
        "under_money_pct":  50,
        "game_total": None,
    }
    for entry in book_data.get("total", []):
        if not isinstance(entry, dict):
            continue
        side   = entry.get("side", "")
        pct_t  = entry.get("bet_info", {}).get("tickets", {}).get("percent", 0)
        pct_m  = entry.get("bet_info", {}).get("money",   {}).get("percent", 0)
        # Skip stale/zero rows
        if pct_t == 0 and pct_m == 0:
            continue
        if side == "over":
            out["over_ticket_pct"] = pct_t
            out["over_money_pct"]  = pct_m
            out["game_total"]      = entry.get("value")
        elif side == "under":
            out["under_ticket_pct"] = pct_t
            out["under_money_pct"]  = pct_m
            if out["game_total"] is None:
                out["game_total"] = entry.get("value")
    return out


# ── public API ───────────────────────────────────────────────────────────────

def fetch_mlb_game_sentiment(date_str: str | None = None) -> dict:
    """
    Return a dict keyed by team full_name.lower() with public-betting context:

        {
            "new york yankees": {
                "over_ticket_pct":  72,    # % of bets placed on Over
                "over_money_pct":   55,    # % of dollar volume on Over
                "under_ticket_pct": 28,
                "under_money_pct":  45,
                "rlm_signal":       True,  # |ticket% - money%| >= 15pp
                "rlm_direction":    "under",  # side sharp money is on
                "game_total":       8.5,
                "opposing_team":    "boston red sox",
            },
            ...
        }

    RLM (Reverse Line Movement) logic:
        - over_ticket_pct >> over_money_pct  → sharp money on UNDER
        - over_money_pct >> over_ticket_pct  → sharp money on OVER (steam)
        - threshold: 15 percentage points

    Notes
    -----
    - Endpoint is publicly accessible — no API key or cookie required.
    - Results are cached in-process per PT calendar day.
    - Both home and away teams get an entry (same sentiment, different opposing_team).
    """
    global _CACHE, _CACHE_DATE

    date_str = date_str or _today_pt()

    if _CACHE_DATE == date_str and _CACHE:
        return _CACHE

    url = _BASE_URL.format(books=_BOOK_IDS, date=date_str)

    try:
        resp = requests.get(url, headers=_HEADERS, timeout=12)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning(f"[ActionNetwork] fetch failed: {exc}")
        return {}

    result: dict = {}
    games = data.get("games", [])

    for game in games:
        teams = game.get("teams", [])
        if len(teams) < 2:
            continue

        # Map team id → full_name so home/away labels are correct
        home_id = game.get("home_team_id")
        away_id = game.get("away_team_id")
        team_map = {t["id"]: t["full_name"].lower() for t in teams}

        home = team_map.get(home_id, teams[0]["full_name"].lower())
        away = team_map.get(away_id, teams[1]["full_name"].lower())

        markets  = game.get("markets", {})
        book_data = _best_book_data(markets)

        if not book_data:
            # No market data for this game yet — skip quietly
            continue

        totals = _parse_total(book_data)

        over_t = totals["over_ticket_pct"]
        over_m = totals["over_money_pct"]

        divergence = over_t - over_m          # positive → public on Over, sharp on Under
        rlm_signal = abs(divergence) >= 15
        if rlm_signal:
            rlm_direction = "under" if divergence > 0 else "over"
        else:
            rlm_direction = None

        sentiment = {
            "over_ticket_pct":  over_t,
            "over_money_pct":   over_m,
            "under_ticket_pct": totals["under_ticket_pct"],
            "under_money_pct":  totals["under_money_pct"],
            "rlm_signal":       rlm_signal,
            "rlm_direction":    rlm_direction,
            "game_total":       totals["game_total"],
        }

        result[home] = {**sentiment, "opposing_team": away}
        result[away] = {**sentiment, "opposing_team": home}

    _CACHE      = result
    _CACHE_DATE = date_str

    rlm_count = sum(1 for v in result.values() if v["rlm_signal"])
    logger.info(
        f"[ActionNetwork] {len(games)} games loaded | "
        f"{len(result)} teams | {rlm_count // 2} games with RLM signal"
    )
    return result
