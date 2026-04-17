"""
sportsbook_reference_layer.py
=============================
Fetches MLB player prop lines from The Odds API once per calendar day.

Gate architecture (checked in order, fastest first):
  1. In-memory dict            — zero I/O, sub-microsecond
  2. /tmp/sb_ref_{date}.json   — file cache, survives within Railway session
  3. Postgres sportsbook_props_cache table — survives Railway restarts
  4. Live Odds API fetch (events → per-event prop odds) — once per day only

Public interface:
  build_sportsbook_reference(date_int=None) -> dict
    Returns {
      (player_norm, market_key, "Over"|"Under"): {
        "sb_implied_prob": float,   # vig-stripped, 0–1 range
        "line": float,
        "bookmaker": str,
        "over_odds": int | None,
        "under_odds": int | None,
      }
    }
    Returns {} gracefully if no data available — never raises.

Called from:
  - orchestrator.job_streak()  at 8:00 AM PT  →  first fetch of the day
  - run_data_hub_tasklet()     warm section    →  free cache hit every 15s
  - per-prop enrichment stamp  in run_agent_tasklet()  →  free memory hit

DIRECTIVE: No Odds API calls inside the 15-second DataHub loop.
  The in-memory gate (_fetch_date == date_int) guarantees the API is
  called exactly once per calendar day regardless of invocation frequency.
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime
from typing import Optional

import pytz
import requests

log = logging.getLogger(__name__)

_PT = pytz.timezone("America/Los_Angeles")

# ── Odds API config ────────────────────────────────────────────────────────────
_ODDS_KEY: str = os.getenv("ODDS_API_KEY_2") or os.getenv("ODDS_API_KEY_3") or ""
if not _ODDS_KEY:
    log.error(
        "[SBRef] ODDS_API_KEY_2 / ODDS_API_KEY_3 not set — sportsbook reference layer "
        "will return empty dicts. Set these in Railway environment variables. "
        "NEVER hardcode API keys in source code."
    )
_BASE_URL = "https://api.the-odds-api.com/v4"
_BOOKMAKERS = "pinnacle,draftkings,fanduel,betmgm"
_PRIORITY: dict[str, int] = {"pinnacle": 0, "draftkings": 1, "fanduel": 2, "betmgm": 3}

# Prop markets to fetch.  Excluded per PropIQ directive:
#   stolen_bases, home_runs, walks, walks_allowed, doubles, triples, singles
_MARKETS: list[str] = [
    "pitcher_strikeouts",
    "pitcher_hits_allowed",
    "pitcher_earned_runs",
    "pitcher_outs",
    "batter_hits",
    "batter_total_bases",
    "batter_rbis",
    "batter_runs_scored",
    "batter_strikeouts",   # hitter_strikeouts in PropIQ
]
_MARKETS_STR = ",".join(_MARKETS)

# PropIQ internal stat_type  →  Odds API market key
STAT_TO_MARKET: dict[str, str] = {
    "strikeouts":          "pitcher_strikeouts",
    "pitcher_strikeouts":  "pitcher_strikeouts",
    "hits_allowed":        "pitcher_hits_allowed",
    "earned_runs":         "pitcher_earned_runs",
    "pitching_outs":       "pitcher_outs",
    "hitter_strikeouts":   "batter_strikeouts",
    "hits":                "batter_hits",
    "total_bases":         "batter_total_bases",
    "rbis":                "batter_rbis",
    "rbi":                 "batter_rbis",
    "runs":                "batter_runs_scored",
    "runs_scored":         "batter_runs_scored",
}

# ── In-memory gate ─────────────────────────────────────────────────────────────
_mem_ref: dict = {}
_fetch_date: int = 0       # YYYYMMDD int; 0 = not yet fetched this session


# ── Utility helpers ────────────────────────────────────────────────────────────

def _today_int() -> int:
    return int(datetime.now(_PT).strftime("%Y%m%d"))


def _normalize(name: str) -> str:
    """Normalise player name for key matching."""
    return (
        name.lower().strip()
        .replace(".", "")
        .replace("'", "")
        .replace("-", " ")
        .replace("  ", " ")
    )


def _american_to_implied(odds: int) -> float:
    """American odds → implied probability (0–1)."""
    if odds >= 0:
        return 100.0 / (odds + 100.0)
    return abs(odds) / (abs(odds) + 100.0)


def _vig_strip(over_odds: int, under_odds: int) -> tuple[float, float]:
    """Return vig-stripped (over_implied, under_implied) as 0–1 floats."""
    po = _american_to_implied(over_odds)
    pu = _american_to_implied(under_odds)
    total = po + pu
    if total <= 0:
        return 0.5, 0.5
    return round(po / total, 6), round(pu / total, 6)


def _tmp_path(date_int: int) -> str:
    return f"/tmp/sb_ref_{date_int}.json"


# ── Postgres helpers ───────────────────────────────────────────────────────────

def _pg_conn():
    import psycopg2
    return psycopg2.connect(os.getenv("DATABASE_URL", ""))


def _ensure_table() -> None:
    try:
        with _pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS sportsbook_props_cache (
                        id          SERIAL PRIMARY KEY,
                        fetch_date  INTEGER  NOT NULL,
                        player_name TEXT     NOT NULL,
                        market_key  TEXT     NOT NULL,
                        side        TEXT     NOT NULL,
                        sb_implied  FLOAT    NOT NULL,
                        line        FLOAT    NOT NULL,
                        bookmaker   TEXT     NOT NULL,
                        over_odds   INTEGER,
                        under_odds  INTEGER,
                        created_at  TIMESTAMP DEFAULT NOW(),
                        UNIQUE(fetch_date, player_name, market_key, side, bookmaker)
                    )
                """)
            conn.commit()
    except Exception as exc:
        log.warning("[SBRef] _ensure_table failed: %s", exc)


def _pg_load(date_int: int) -> dict:
    ref: dict = {}
    try:
        with _pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT player_name, market_key, side, sb_implied, line, "
                    "bookmaker, over_odds, under_odds "
                    "FROM sportsbook_props_cache WHERE fetch_date = %s",
                    (date_int,),
                )
                for row in cur.fetchall():
                    pn, mk, side, si, line, bk, oo, uo = row
                    k = (pn, mk, side)
                    # Keep the sharpest / highest-priority bookmaker entry
                    existing = ref.get(k)
                    if existing is None or _PRIORITY.get(bk, 99) < _PRIORITY.get(existing["bookmaker"], 99):
                        ref[k] = {
                            "sb_implied_prob": float(si),
                            "line": float(line),
                            "bookmaker": bk,
                            "over_odds": oo,
                            "under_odds": uo,
                        }
    except Exception as exc:
        log.warning("[SBRef] PG load failed: %s", exc)
    return ref


def _pg_save(date_int: int, rows: list[dict]) -> None:
    if not rows:
        return
    try:
        with _pg_conn() as conn:
            with conn.cursor() as cur:
                for r in rows:
                    cur.execute(
                        """
                        INSERT INTO sportsbook_props_cache
                            (fetch_date, player_name, market_key, side,
                             sb_implied, line, bookmaker, over_odds, under_odds)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (fetch_date, player_name, market_key, side, bookmaker)
                        DO NOTHING
                        """,
                        (
                            date_int,
                            r["player_name"], r["market_key"], r["side"],
                            r["sb_implied_prob"], r["line"], r["bookmaker"],
                            r.get("over_odds"), r.get("under_odds"),
                        ),
                    )
            conn.commit()
        log.info("[SBRef] Saved %d rows to Postgres for %d", len(rows), date_int)
    except Exception as exc:
        log.warning("[SBRef] PG save failed: %s", exc)


# ── File cache helpers ─────────────────────────────────────────────────────────

def _file_load(date_int: int) -> dict:
    path = _tmp_path(date_int)
    if not os.path.exists(path):
        return {}
    try:
        with open(path) as f:
            raw: dict = json.load(f)
        # JSON cannot store tuple keys — we serialised them as JSON arrays
        return {tuple(k): v for k, v in raw.items()}
    except Exception:
        return {}


def _file_save(date_int: int, ref: dict) -> None:
    try:
        path = _tmp_path(date_int)
        serialisable = {json.dumps(list(k)): v for k, v in ref.items()}
        with open(path, "w") as f:
            json.dump(serialisable, f)
    except Exception as exc:
        log.debug("[SBRef] File save failed: %s", exc)


# ── Live Odds API fetch ────────────────────────────────────────────────────────

def _fetch_events() -> list[dict]:
    """Step 1: retrieve today's MLB event IDs."""
    if not _ODDS_KEY:
        return []
    try:
        resp = requests.get(
            f"{_BASE_URL}/sports/baseball_mlb/events",
            params={"apiKey": _ODDS_KEY},
            timeout=20,
        )
        resp.raise_for_status()
        events: list[dict] = resp.json()
        log.info("[SBRef] %d MLB events from Odds API", len(events))
        return events
    except Exception as exc:
        log.error("[SBRef] Event fetch failed: %s", exc)
        return []


def _fetch_event_props(event_id: str) -> list[dict]:
    """Step 2: fetch vig-stripped prop odds for one event.
    Returns a flat list of row dicts ready for _pg_save / reference dict.
    """
    try:
        resp = requests.get(
            f"{_BASE_URL}/sports/baseball_mlb/events/{event_id}/odds",
            params={
                "apiKey": _ODDS_KEY,
                "regions": "us",
                "markets": _MARKETS_STR,
                "bookmakers": _BOOKMAKERS,
                "oddsFormat": "american",
            },
            timeout=20,
        )
        if resp.status_code == 422:
            # Prop markets not yet posted for this event — normal early in the day
            return []
        resp.raise_for_status()
        data: dict = resp.json()
    except Exception as exc:
        log.debug("[SBRef] Props fetch failed for %s: %s", event_id, exc)
        return []

    # Parse outcomes into per-(player, market, bookmaker) rows, then vig-strip
    rows_by_key: dict[tuple, dict] = {}
    for bookmaker in data.get("bookmakers", []):
        bk = bookmaker.get("key", "")
        for market in bookmaker.get("markets", []):
            mk = market.get("key", "")
            if mk not in _MARKETS:
                continue

            by_player: dict[str, dict] = {}
            for outcome in market.get("outcomes", []):
                pname = _normalize(outcome.get("name", ""))
                desc = (outcome.get("description") or "").title()   # "Over" / "Under"
                point = outcome.get("point")
                price = outcome.get("price")
                if not pname or point is None or desc not in ("Over", "Under"):
                    continue
                if pname not in by_player:
                    by_player[pname] = {"line": float(point), "over_odds": None, "under_odds": None}
                if desc == "Over":
                    by_player[pname]["over_odds"] = int(price) if price is not None else None
                    by_player[pname]["line"] = float(point)
                else:
                    by_player[pname]["under_odds"] = int(price) if price is not None else None

            for pname, pd in by_player.items():
                over_o = pd.get("over_odds")
                under_o = pd.get("under_odds")
                if over_o is None or under_o is None:
                    continue  # Need both sides for vig-strip
                ovi, uvi = _vig_strip(over_o, under_o)
                line = pd["line"]
                for side, si in (("Over", ovi), ("Under", uvi)):
                    rk = (pname, mk, bk, side)
                    existing = rows_by_key.get(rk)
                    if existing is None or _PRIORITY.get(bk, 99) < _PRIORITY.get(existing["bookmaker"], 99):
                        rows_by_key[rk] = {
                            "player_name": pname,
                            "market_key":  mk,
                            "side":        side,
                            "sb_implied_prob": si,
                            "line":        line,
                            "bookmaker":   bk,
                            "over_odds":   over_o,
                            "under_odds":  under_o,
                        }

    return list(rows_by_key.values())


def _fetch_live(date_int: int) -> dict:
    """Full live fetch: events → per-event props.  Returns reference dict."""
    events = _fetch_events()
    if not events:
        return {}

    all_rows: list[dict] = []
    for event in events[:20]:   # Cap at 20 events — full MLB slate
        eid = event.get("id")
        if not eid:
            continue
        rows = _fetch_event_props(eid)
        all_rows.extend(rows)
        time.sleep(0.1)         # Courtesy pause — well within Odds API rate limits

    log.info("[SBRef] Fetched %d prop lines from Odds API for %d", len(all_rows), date_int)
    if not all_rows:
        return {}

    # Build reference dict — best bookmaker wins per (player, market, side) key
    ref: dict = {}
    for r in all_rows:
        k = (r["player_name"], r["market_key"], r["side"])
        existing = ref.get(k)
        if existing is None or _PRIORITY.get(r["bookmaker"], 99) < _PRIORITY.get(existing["bookmaker"], 99):
            ref[k] = {
                "sb_implied_prob": r["sb_implied_prob"],
                "line":            r["line"],
                "bookmaker":       r["bookmaker"],
                "over_odds":       r["over_odds"],
                "under_odds":      r["under_odds"],
            }
    return ref


# ── Public interface ───────────────────────────────────────────────────────────

def build_sportsbook_reference(date_int: int | None = None) -> dict:
    """
    Return today's sportsbook prop reference dict.

    Gate order: memory → file → Postgres → live Odds API.
    Safe to call on every DataHub cycle (every 15s) — no I/O after first fetch.

    Returns dict keyed by (player_norm, market_key, "Over"|"Under") with:
        {sb_implied_prob, line, bookmaker, over_odds, under_odds}
    Returns {} gracefully if no data available.
    """
    global _mem_ref, _fetch_date

    if date_int is None:
        date_int = _today_int()

    # ── 1. Memory (fastest path — zero I/O) ──────────────────────────────────
    if _fetch_date == date_int and _mem_ref:
        return _mem_ref

    _ensure_table()

    # ── 2. File cache ─────────────────────────────────────────────────────────
    ref = _file_load(date_int)
    if ref:
        _mem_ref = ref
        _fetch_date = date_int
        log.info("[SBRef] Loaded %d entries from file cache for %d", len(ref), date_int)
        return ref

    # ── 3. Postgres cache ─────────────────────────────────────────────────────
    ref = _pg_load(date_int)
    if ref:
        _mem_ref = ref
        _fetch_date = date_int
        _file_save(date_int, ref)       # Populate file cache for this session
        log.info("[SBRef] Loaded %d entries from Postgres for %d", len(ref), date_int)
        return ref

    # ── 4. Live fetch (once per day) ──────────────────────────────────────────
    if not _ODDS_KEY:
        log.warning("[SBRef] No Odds API key configured — sportsbook reference unavailable")
        _mem_ref = {}
        _fetch_date = date_int
        return {}

    ref = _fetch_live(date_int)
    _mem_ref = ref
    _fetch_date = date_int

    if ref:
        _file_save(date_int, ref)
        flat: list[dict] = []
        for (pn, mk, side), v in ref.items():
            flat.append({"player_name": pn, "market_key": mk, "side": side, **v})
        _pg_save(date_int, flat)
        log.info("[SBRef] Built and cached %d entries for %d", len(ref), date_int)
    else:
        log.warning("[SBRef] No prop data returned from Odds API for %d", date_int)

    return _mem_ref
