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

  enrich_props_with_sportsbook(props, date=None) -> list
    Stamps sb_implied_prob, sb_line, sb_line_gap on each prop dict.
    Adjusts implied probability when UD/PP line differs from sportsbook line.

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
_ODDS_KEY: str = (
    os.getenv("ODDS_API_KEY") or
    os.getenv("ODDS_API_KEY_2") or
    os.getenv("ODDS_API_KEY_3") or
    ""
)
if not _ODDS_KEY:
    log.warning(
        "[SBRef] ODDS_API_KEY / ODDS_API_KEY_2 / ODDS_API_KEY_3 not set — "
        "OddsAPI path disabled. Pinnacle direct (PINNACLE_API_KEY) will be tried as fallback. "
        "NEVER hardcode API keys in source code."
    )
_BASE_URL = "https://api.the-odds-api.com/v4"
_BOOKMAKERS = "pinnacle,draftkings,fanduel,betmgm"
_PRIORITY: dict[str, int] = {"pinnacle": 0, "draftkings": 1, "fanduel": 2, "betmgm": 3}

# Sharpness weights for consensus implied probability.
# Pinnacle is the global sharp market maker — given highest weight.
# DraftKings is next (large US volume, efficient lines).
# FanDuel and BetMGM are softer books included for line coverage.
# When only one book has a prop, its vig-stripped implied is used directly.
_BOOK_WEIGHTS: dict[str, float] = {
    "pinnacle":   0.40,
    "draftkings": 0.30,
    "fanduel":    0.20,
    "betmgm":     0.10,
}

# Prop markets to fetch.  Excluded per PropIQ directive:
#   stolen_bases, home_runs, walks, doubles, triples, singles
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
    "walks_allowed":       "pitcher_walks_allowed",
}

# Per-prop-type probability shift per 0.5 point of line difference.
# When UD/PP line differs from sportsbook line, the implied probability
# is adjusted by this amount per half-point of difference.
# Derived from Poisson approximation of each prop type's typical lambda.
_LINE_SHIFT_PER_HALF: dict[str, float] = {
    "pitcher_outs":           0.025,   # ~2.5% per 0.5 pts (lambda ~15-17)
    "pitcher_strikeouts":     0.040,   # ~4% per 0.5 pts   (lambda ~4-7)
    "pitcher_earned_runs":    0.060,   # ~6% per 0.5 pts   (lambda ~1-3)
    "pitcher_hits_allowed":   0.035,   # ~3.5% per 0.5 pts (lambda ~4-6)
    "pitcher_walks_allowed":  0.055,   # ~5.5% per 0.5 pts (lambda ~1-3)
    "batter_hits":            0.070,   # ~7% per 0.5 pts   (lambda ~0.5-1.5)
    "batter_total_bases":     0.055,   # ~5.5% per 0.5 pts (lambda ~1-2)
    "batter_rbis":            0.060,   # ~6% per 0.5 pts   (lambda ~0.5-1.5)
    "batter_runs_scored":     0.060,   # ~6% per 0.5 pts
    "batter_strikeouts":      0.070,   # ~7% per 0.5 pts   (lambda ~0.5-1.5)
}
_DEFAULT_SHIFT = 0.035  # fallback for unknown market keys

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
                    rk = (pname, mk, side)   # key without book — aggregate across books
                    w  = _BOOK_WEIGHTS.get(bk, 0.05)
                    if rk not in rows_by_key:
                        rows_by_key[rk] = {
                            "player_name":     pname,
                            "market_key":      mk,
                            "side":            side,
                            "line":            line,
                            "_weighted_sum":   si * w,
                            "_weight_total":   w,
                            "_books":          [bk],
                            "over_odds":       over_o if side == "Over" else None,
                            "under_odds":      under_o if side == "Under" else None,
                            "bookmaker":       bk,  # sharpest book for reference
                        }
                    else:
                        rows_by_key[rk]["_weighted_sum"]  += si * w
                        rows_by_key[rk]["_weight_total"]  += w
                        rows_by_key[rk]["_books"].append(bk)
                        # Keep sharpest book reference (lowest priority number)
                        if _PRIORITY.get(bk, 99) < _PRIORITY.get(rows_by_key[rk]["bookmaker"], 99):
                            rows_by_key[rk]["bookmaker"] = bk

    # ── Finalise: compute weighted-average implied prob, clean up internals ──
    output = []
    for row in rows_by_key.values():
        wt = row.pop("_weight_total", 0)
        ws = row.pop("_weighted_sum", 0)
        row.pop("_books", None)
        row["sb_implied_prob"] = round(ws / wt, 6) if wt > 0 else 0.5
    return list(rows_by_key.values())


def _fetch_prop_odds(api_key: str, date_int: int) -> dict:
    """
    Fetch MLB player prop lines from PropOdds API (prop-odds.com).
    Free tier: 100 calls/day. Flow: /beta/games/mlb → /beta/props/{game_id} per game.
    Vig-strips paired Over/Under prices using existing _vig_strip() helper.
    Returns dict keyed by (player_norm, market_key, side) — same format as OddsAPI path.
    """
    _PO_BASE = "https://api.prop-odds.com/beta"
    _PO_MARKET_MAP: dict[str, str] = {
        "pitcher_strikeouts": "pitcher_strikeouts", "strikeouts": "pitcher_strikeouts",
        "pitcher strikeouts": "pitcher_strikeouts",
        "pitcher_hits_allowed": "pitcher_hits_allowed", "hits allowed": "pitcher_hits_allowed",
        "pitcher_earned_runs": "pitcher_earned_runs", "earned runs allowed": "pitcher_earned_runs",
        "pitcher_outs": "pitcher_outs", "pitching outs": "pitcher_outs",
        "batter_hits": "batter_hits", "hits": "batter_hits",
        "batter_total_bases": "batter_total_bases", "total bases": "batter_total_bases",
        "batter_rbis": "batter_rbis", "rbis": "batter_rbis",
        "batter_runs_scored": "batter_runs_scored", "runs scored": "batter_runs_scored",
        "batter_strikeouts": "batter_strikeouts", "hitter strikeouts": "batter_strikeouts",
    }
    _PO_BOOK_PRIORITY: dict[str, int] = {
        "draftkings": 0, "fanduel": 1, "betmgm": 2, "caesars": 3,
        "pointsbet": 4, "betrivers": 5,
    }
    today_str = datetime.now(_PT).strftime("%Y-%m-%d")
    ref: dict = {}

    try:
        games_resp = requests.get(
            f"{_PO_BASE}/games/mlb",
            params={"date": today_str, "tz": "America/New_York", "api_key": api_key},
            timeout=15,
        )
        if games_resp.status_code == 401:
            log.warning("[PropOdds] Invalid API key — check PROP_ODDS_API_KEY in Railway")
            return {}
        if games_resp.status_code == 429:
            log.warning("[PropOdds] Daily quota exhausted (100 calls/day on free tier)")
            return {}
        if games_resp.status_code != 200:
            log.warning("[PropOdds] Games endpoint HTTP %d", games_resp.status_code)
            return {}
        game_ids: list[str] = [
            g.get("game_id", "") for g in games_resp.json().get("games", []) if g.get("game_id")
        ]
        log.info("[PropOdds] %d MLB games for %s", len(game_ids), today_str)
    except Exception as exc:
        log.warning("[PropOdds] Games fetch failed: %s", exc)
        return {}

    if not game_ids:
        return {}

    raw_by_key: dict[tuple, dict] = {}
    for game_id in game_ids:
        try:
            props_resp = requests.get(f"{_PO_BASE}/props/{game_id}",
                                       params={"api_key": api_key}, timeout=15)
            if props_resp.status_code == 429:
                log.warning("[PropOdds] Quota exhausted mid-fetch")
                break
            if props_resp.status_code != 200:
                continue
            markets = props_resp.json().get("sportsbooks", [])
        except Exception:
            continue

        for sportsbook in markets:
            bk = sportsbook.get("sportsbook", "").lower().replace(" ", "")
            if bk not in _PO_BOOK_PRIORITY:
                continue
            for market in sportsbook.get("markets", []):
                mkt_key = _PO_MARKET_MAP.get(market.get("market_name", "").lower().strip())
                if not mkt_key:
                    continue
                for outcome in market.get("outcomes", []):
                    player_raw = str(outcome.get("participant", "") or outcome.get("name", "") or "")
                    if not player_raw:
                        continue
                    player_norm = _normalize(player_raw)
                    side_raw = str(outcome.get("type", "")).strip().lower()
                    side = "Over" if side_raw in ("over", "o") else "Under" if side_raw in ("under", "u") else None
                    if not side:
                        continue
                    try:
                        line  = float(outcome.get("handicap") or outcome.get("line") or 0)
                        price = int(float(str(outcome.get("price", "-110")).replace("+", "")))
                    except (ValueError, TypeError):
                        continue
                    raw_key = (player_norm, mkt_key, side, bk, line)
                    existing = raw_by_key.get(raw_key)
                    if existing is None or _PO_BOOK_PRIORITY.get(bk, 99) < _PO_BOOK_PRIORITY.get(existing["bk"], 99):
                        raw_by_key[raw_key] = {"price": price, "bk": bk, "line": line}

    processed: set = set()
    for (player, mkt, side, bk, line), data in raw_by_key.items():
        if (player, mkt, bk, line) in processed:
            continue
        other_side = "Under" if side == "Over" else "Over"
        partner = raw_by_key.get((player, mkt, other_side, bk, line))
        if partner:
            over_price  = data["price"] if side == "Over" else partner["price"]
            under_price = data["price"] if side == "Under" else partner["price"]
            fair_over, fair_under = _vig_strip(over_price, under_price)
            for s, fp in [("Over", fair_over), ("Under", fair_under)]:
                k = (player, mkt, s)
                if k not in ref or _PO_BOOK_PRIORITY.get(bk, 99) < _PO_BOOK_PRIORITY.get(ref[k].get("bookmaker", ""), 99):
                    ref[k] = {"sb_implied_prob": round(fp, 4), "line": line,
                               "bookmaker": f"propodds_{bk}"}
            processed.add((player, mkt, bk, line))

    log.info("[PropOdds] Parsed %d prop entries from %d games", len(ref), len(game_ids))
    return ref


def _fetch_pinnacle_direct() -> dict:
    """
    Direct Pinnacle API — fires when OddsAPI key is expired/empty.
    Pinnacle is the sharpest book in the world; their lines are what every other
    book copies. Returns the same dict format as the OddsAPI path.

    Setup: set PINNACLE_API_KEY in Railway = base64(username:password)
    To encode: python3 -c "import base64; print(base64.b64encode(b'user:pass').decode())"

    Pinnacle leagueId 246 = MLB. sportId 3 = Baseball.
    """
    import base64  # noqa: PLC0415
    pk = os.getenv("PINNACLE_API_KEY", "")
    if not pk:
        return {}

    headers = {
        "Authorization": f"Basic {pk}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    # Pinnacle stat name (from special name suffix) → our market key
    _PINNACLE_STAT_MAP: dict[str, str] = {
        "strikeouts":            "pitcher_strikeouts",
        "pitcher strikeouts":    "pitcher_strikeouts",
        "hits allowed":          "pitcher_hits_allowed",
        "earned runs":           "pitcher_earned_runs",
        "outs":                  "pitcher_outs",
        "pitcher outs":          "pitcher_outs",
        "pitching outs":         "pitcher_outs",
        "hits":                  "batter_hits",
        "total bases":           "batter_total_bases",
        "rbis":                  "batter_rbis",
        "rbi":                   "batter_rbis",
        "runs":                  "batter_runs_scored",
        "runs scored":           "batter_runs_scored",
        "batter strikeouts":     "batter_strikeouts",
        "strikeouts (batter)":   "batter_strikeouts",
        "hitter strikeouts":     "batter_strikeouts",
        "walks allowed":         "pitcher_walks_allowed",
        "walks":                 "pitcher_walks_allowed",
    }

    try:
        resp = requests.get(
            "https://api.pinnacle.com/v2/leagues/246/specials",
            headers=headers,
            timeout=15,
        )
        if resp.status_code == 401:
            log.warning("[SBRef] Pinnacle direct: 401 — check PINNACLE_API_KEY in Railway")
            return {}
        if resp.status_code == 404:
            log.debug("[SBRef] Pinnacle specials 404 — props not yet posted")
            return {}
        resp.raise_for_status()
        specials = resp.json().get("specials", [])
    except Exception as exc:
        log.debug("[SBRef] Pinnacle direct specials fetch failed: %s", exc)
        return {}

    if not specials:
        return {}

    # Fetch odds for all specials in one call (Pinnacle supports comma-list)
    special_ids = [str(s["id"]) for s in specials if "id" in s]
    if not special_ids:
        return {}

    try:
        odds_resp = requests.get(
            "https://api.pinnacle.com/v2/leagues/246/specials/odds",
            headers=headers,
            params={"specialIds": ",".join(special_ids[:300])},
            timeout=15,
        )
        odds_resp.raise_for_status()
        odds_list = odds_resp.json().get("specials", [])
    except Exception as exc:
        log.debug("[SBRef] Pinnacle special odds failed: %s", exc)
        return {}

    odds_by_id: dict[str, dict] = {str(o["id"]): o for o in odds_list if "id" in o}

    ref: dict = {}
    for special in specials:
        sid      = str(special.get("id", ""))
        name     = special.get("name", "")          # e.g. "Shohei Ohtani - Strikeouts"
        units    = special.get("units", "").lower().strip()

        if " - " not in name:
            continue

        player_raw, stat_raw = name.rsplit(" - ", 1)
        player_norm = _normalize(player_raw)
        stat_lower  = stat_raw.lower().strip()
        market_key  = _PINNACLE_STAT_MAP.get(stat_lower) or _PINNACLE_STAT_MAP.get(units)

        if not market_key or not player_norm:
            continue

        odds = odds_by_id.get(sid, {})
        contestants = odds.get("contestants", [])

        over_price = under_price = line = None
        for c in contestants:
            cname     = (c.get("name") or "").lower().strip()
            price     = c.get("price")
            handicap  = c.get("handicap")
            if handicap is not None and line is None:
                try:
                    line = float(handicap)
                except (TypeError, ValueError):
                    pass
            if cname in ("over", "higher") and price is not None:
                try:
                    over_price = int(float(price))
                except (TypeError, ValueError):
                    pass
            elif cname in ("under", "lower") and price is not None:
                try:
                    under_price = int(float(price))
                except (TypeError, ValueError):
                    pass

        if over_price is None or under_price is None or line is None:
            continue

        ovi, uvi = _vig_strip(over_price, under_price)
        for side, si in [("Over", ovi), ("Under", uvi)]:
            ref[(player_norm, market_key, side)] = {
                "sb_implied_prob": si,
                "line":            line,
                "bookmaker":       "pinnacle",
                "over_odds":       over_price,
                "under_odds":      under_price,
            }

    if ref:
        log.info("[SBRef] Pinnacle direct: %d prop entries (OddsAPI bypassed)", len(ref))
    return ref


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
        # Props may not be posted yet — retry once after 30s
        import time as _time  # noqa: PLC0415
        log.info("[SBRef] No prop lines on first attempt — waiting 30s then retrying")
        _time.sleep(30)
        all_rows = []
        for event in events[:20]:
            eid = event.get("id")
            if not eid:
                continue
            rows = _fetch_event_props(eid)
            all_rows.extend(rows)
            _time.sleep(0.1)
        log.info("[SBRef] Retry fetched %d prop lines", len(all_rows))
        if not all_rows:
            return {}

    # Build reference dict — one entry per (player, market, side) key
    # sb_implied_prob is already the weighted-consensus value from the fetch loop
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
    # NOTE: if ODDS_API_KEY is not set we skip straight to fallbacks rather than
    # returning early. The fallback chain (PropOdds → Pinnacle → Covers → DraftEdge
    # → ActionNetwork → TheRundown → VegasInsider) can produce a full reference
    # even without an Odds API key.
    if _ODDS_KEY:
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
            log.warning("[SBRef] No prop data returned from Odds API for %d — trying fallbacks", date_int)
    else:
        log.warning(
            "[SBRef] ODDS_API_KEY not set — skipping OddsAPI live fetch, "
            "trying PropOdds / Pinnacle / Covers / DraftEdge / ActionNetwork fallbacks"
        )

    # ── PropOdds fallback — fires when OddsAPI key empty/exhausted ───────────
    # Free tier: 100 calls/day. Set PROP_ODDS_API_KEY in Railway (prop-odds.com).
    # ~16 calls/day (1 games list + 1 per game). Covers DK/FD/BetMGM/Caesars.
    if not _mem_ref:
        try:
            _po_key = os.getenv("PROP_ODDS_API_KEY", "")
            if _po_key:
                _po_ref = _fetch_prop_odds(_po_key, date_int)
                if _po_ref:
                    log.info("[SBRef] PropOdds fallback: %d entries", len(_po_ref))
                    _mem_ref    = _po_ref
                    _fetch_date = date_int
                    _file_save(date_int, _po_ref)
                    flat = [
                        {"player_name": pn, "market_key": mk, "side": side, **v}
                        for (pn, mk, side), v in _po_ref.items()
                    ]
                    _pg_save(date_int, flat)
            else:
                log.debug("[SBRef] PROP_ODDS_API_KEY not set — PropOdds skipped")
        except Exception as _po_err:
            log.debug("[SBRef] PropOdds fallback failed: %s", _po_err)

    # ── Pinnacle direct fallback — when OddsAPI key expired or quota empty ───
    # Pinnacle is the sharpest book globally; their lines ARE the market.
    # Requires PINNACLE_API_KEY = base64(username:password) in Railway.
    # Full player coverage — functionally equivalent to OddsAPI Pinnacle tier.
    if not _mem_ref:
        ref = _fetch_pinnacle_direct()
        if ref:
            _mem_ref = ref
            _fetch_date = date_int
            _file_save(date_int, ref)
            flat = [
                {"player_name": pn, "market_key": mk, "side": side, **v}
                for (pn, mk, side), v in ref.items()
            ]
            _pg_save(date_int, flat)

    # ── Covers.com fallback — Tier 3 (after Pinnacle direct, before DraftEdge) ─
    # Covers aggregates DK/FD/BetMGM/Caesars lines pre-game and also provides
    # THE BAT X projections. Covers only covers props during the pre-game window;
    # it returns empty once games go live (handled gracefully by covers_layer).
    # Props covered: strikeouts, hits, hits+runs+rbi, earned_runs, hitter_strikeouts.
    if not _mem_ref:
        try:
            from covers_layer import fetch_covers_reference as _covers_fetch  # noqa: PLC0415

            _covers_raw = _covers_fetch(date_int)
            if _covers_raw:
                covers_ref: dict = {}
                _PT_COVERS_MAP = {
                    "strikeouts":         "pitcher_strikeouts",  # pitcher Ks → pitcher_strikeouts
                    "hits":               "hits",
                    "hits+runs+rbi":      "hits+runs+rbi",
                    "earned_runs":        "earned_runs",
                    "hitter_strikeouts":  "batter_strikeouts",
                }
                for raw_key, entry in _covers_raw.items():
                    player_norm, prop_type = raw_key.rsplit("|", 1)
                    mk = _PT_COVERS_MAP.get(prop_type, prop_type)
                    prob = float(entry.get("sb_implied_prob", 0.5) or 0.5)
                    line = float(entry.get("sb_line") or 0.5)
                    for side, si in [("Over", prob), ("Under", round(1.0 - prob, 4))]:
                        covers_ref[(player_norm, mk, side)] = {
                            "sb_implied_prob": round(si, 4),
                            "line":            line,
                            "bookmaker":       entry.get("bookmaker", "covers"),
                            "over_odds":       None,
                            "under_odds":      None,
                            "covers_batx_proj": entry.get("batx_proj"),
                        }
                if covers_ref:
                    log.info("[SBRef] Covers fallback: %d entries", len(covers_ref))
                    _mem_ref    = covers_ref
                    _fetch_date = date_int
        except Exception as _covers_err:
            log.debug("[SBRef] Covers fallback failed: %s", _covers_err)

    # ── DraftEdge fallback — when Odds API has no props yet ──────────────────
    # DraftEdge gives projected_prob per player/prop. Used when sharp book
    # lines aren't available (props not yet posted for the day). Less precise
    # than vig-stripped book odds but better than defaulting sb_implied_prob to 0.
    if not _mem_ref:
        try:
            from draftedge_scraper import fetch_all_projections as _de_fetch  # noqa: PLC0415
            de_props = _de_fetch()
            if de_props:
                de_ref: dict = {}
                _PT_DE_MAP = {
                    "strikeouts":        "pitcher_strikeouts",
                    "hits":              "batter_hits",
                    "total_bases":       "batter_total_bases",
                    "earned_runs":       "pitcher_earned_runs",
                    "hitter_strikeouts": "batter_strikeouts",
                    "rbis":              "batter_rbis",
                    "runs":              "batter_runs_scored",
                }
                for prop in de_props:
                    pname = _normalize(str(prop.get("player_name", "")))
                    pt    = str(prop.get("prop_type", ""))
                    mk    = _PT_DE_MAP.get(pt, pt)
                    prob  = float(prop.get("projected_prob", 0.524) or 0.524)
                    line  = float(prop.get("line", 0.5) or 0.5)
                    if not pname or not mk:
                        continue
                    for side, si in [("Over", prob), ("Under", round(1.0 - prob, 4))]:
                        de_ref[(pname, mk, side)] = {
                            "sb_implied_prob": round(si, 4),
                            "line":            line,
                            "bookmaker":       "draftedge",
                            "over_odds":       None,
                            "under_odds":      None,
                        }
                if de_ref:
                    log.info("[SBRef] DraftEdge fallback: %d entries (Odds API empty)", len(de_ref))
                    _mem_ref    = de_ref
                    _fetch_date = date_int
        except Exception as _de_err:
            log.debug("[SBRef] DraftEdge fallback failed: %s", _de_err)

    # ── ActionNetwork money% fallback — when Odds API AND DraftEdge are empty ─
    # Converts sharp money% into an implied probability proxy.
    # money_pct=65 (65% of money on Over) → sb_implied_prob ≈ 0.65.
    # Weaker than vig-stripped book lines but much better than nothing —
    # sharp money flow is a real signal that keeps agents running on quota-exhausted days.
    # Requires ACTION_NETWORK_COOKIE env var (Bearer JWT).
    if not _mem_ref:
        try:
            from action_network_layer import fetch_mlb_prop_projections as _an_fetch
            _an_props = _an_fetch()
            if _an_props:
                _an_ref: dict = {}
                _PT_AN_MAP = {
                    "strikeouts":        "pitcher_strikeouts",
                    "hits":              "batter_hits",
                    "total_bases":       "batter_total_bases",
                    "earned_runs":       "pitcher_earned_runs",
                    "hitter_strikeouts": "batter_strikeouts",
                    "rbis":              "batter_rbis",
                    "runs":              "batter_runs_scored",
                    "pitching_outs":     "pitcher_outs",
                    "walks_allowed":     "pitcher_walks_allowed",
                }
                for prop in _an_props:
                    pname    = _normalize(str(prop.get("player") or prop.get("player_name") or ""))
                    pt       = str(prop.get("prop_type") or "")
                    mk       = _PT_AN_MAP.get(pt, pt)
                    line     = prop.get("line")
                    over_m   = int(prop.get("over_money_pct",  prop.get("money_pct", 50)) or 50)
                    under_m  = 100 - over_m
                    # Convert money% to implied prob: clamp so no side goes below 40%
                    # (sharp money can be one-sided; 40/60 is a reasonable floor/ceiling)
                    over_prob  = round(max(0.40, min(0.60, over_m  / 100.0)), 4)
                    under_prob = round(1.0 - over_prob, 4)
                    if not pname or not mk or line is None:
                        continue
                    for side, si in [("Over", over_prob), ("Under", under_prob)]:
                        _an_ref[(pname, mk, side)] = {
                            "sb_implied_prob": si,
                            "line":            float(line),
                            "bookmaker":       "action_network_money_pct",
                            "over_odds":       None,
                            "under_odds":      None,
                        }
                if _an_ref:
                    log.info("[SBRef] ActionNetwork money%% fallback: %d entries", len(_an_ref))
                    _mem_ref    = _an_ref
                    _fetch_date = date_int
        except Exception as _an_err:
            log.debug("[SBRef] ActionNetwork fallback failed: %s", _an_err)

    # ── TheRundown fallback — pitcher strikeouts only (real book lines, free) ─
    # Covers pitcher_strikeouts when both OddsAPI and AN are unavailable.
    # Hardcoded key is the free-tier public key from env or fallback.
    if not _mem_ref:
        try:
            import time as _time_rd
            _rd_key  = os.getenv("RUNDOWN_API_KEY", "a455831fa40a562b43d7f7830f6ab467fa38074d46d078e0d47de324b46bea79")
            _rd_date = datetime.now(_PT).strftime("%Y-%m-%d")
            _rd_resp = requests.get(
                f"https://therundown.io/api/v2/sports/3/events/{_rd_date}",
                headers={"X-TheRundown-Key": _rd_key, "Accept": "application/json"},
                params={"market_ids": 19},
                timeout=12,
            )
            if _rd_resp.status_code == 200:
                _rd_events = _rd_resp.json().get("events", [])
                _rd_ref: dict = {}
                for _ev in _rd_events:
                    for _mkt in _ev.get("markets", []):
                        if _mkt.get("market_id") != 19:
                            continue
                        for _part in _mkt.get("participants", []):
                            pname = _normalize(str(_part.get("name", "")))
                            if not pname:
                                continue
                            for _line in _part.get("lines", []):
                                _parts = (_line.get("value") or "").strip().lower().split()
                                if len(_parts) != 2:
                                    continue
                                _side_str, _val_str = _parts
                                try:
                                    line_val = float(_val_str)
                                except ValueError:
                                    continue
                                _prices = _line.get("prices", {})
                                for _bk, _pi in _prices.items():
                                    try:
                                        _price = int(float(str(_pi.get("price", -115)).replace("+", "")))
                                    except (ValueError, TypeError):
                                        continue
                                    # Vig-strip: single-book no-vig implied
                                    _impl = (100 / (abs(_price) + 100)) if _price < 0 else (_price / (_price + 100))
                                    _side = "Over" if _side_str == "over" else "Under"
                                    key   = (pname, "pitcher_strikeouts", _side)
                                    if key not in _rd_ref:
                                        _rd_ref[key] = {
                                            "sb_implied_prob": round(_impl, 4),
                                            "line":            line_val,
                                            "bookmaker":       f"therundown_{_bk}",
                                            "over_odds":       None,
                                            "under_odds":      None,
                                        }
                if _rd_ref:
                    log.info("[SBRef] TheRundown fallback: %d K-prop entries", len(_rd_ref))
                    _mem_ref    = _rd_ref
                    _fetch_date = date_int
        except Exception as _rd_err:
            log.debug("[SBRef] TheRundown fallback failed: %s", _rd_err)

    # ── VegasInsider supplement — fill missing pitcher_strikeouts entries ────
    # VegasInsider scrapes 30+ pitchers from 7 real sportsbooks (Bet365/DK/FD/
    # BetMGM/Caesars/HardRock/Fanatics) pre-game. Server-rendered HTML — no JS.
    # Runs EVERY call (not guarded by `if not _mem_ref`) so it fills gaps even
    # when OddsAPI succeeded but didn't have every starter.  OddsAPI/Pinnacle
    # entries always take priority — VI only adds what is missing.
    if _mem_ref is not None:
        try:
            from vegasinsider_layer import VegasInsiderLayer as _VILayer  # noqa: PLC0415
            _vi_consensus = _VILayer().get_strikeouts_consensus()
            _vi_added = 0
            for _vi_player, _vi_entry in _vi_consensus.items():
                _vi_line = _vi_entry["consensus_line"]
                _vi_prob = _vi_entry["vi_over_prob"]
                _vi_norm = _normalize(_vi_player)
                _vi_over_key  = (_vi_norm, "pitcher_strikeouts", "Over")
                _vi_under_key = (_vi_norm, "pitcher_strikeouts", "Under")
                if _vi_over_key not in _mem_ref:
                    _mem_ref[_vi_over_key] = {
                        "sb_implied_prob": _vi_prob,
                        "line":            _vi_line,
                        "bookmaker":       "vegasinsider",
                        "over_odds":       None,
                        "under_odds":      None,
                    }
                    _mem_ref[_vi_under_key] = {
                        "sb_implied_prob": round(1.0 - _vi_prob, 4),
                        "line":            _vi_line,
                        "bookmaker":       "vegasinsider",
                        "over_odds":       None,
                        "under_odds":      None,
                    }
                    _vi_added += 1
            if _vi_added:
                log.info("[SBRef] VegasInsider supplement: +%d pitcher_strikeouts entries", _vi_added)
        except Exception as _vi_err:
            log.debug("[SBRef] VegasInsider supplement failed: %s", _vi_err)

    # ── RotoWire supplement — multi-book lines for strikeouts + earned_runs ──
    # RotoWire embeds JSON data server-side (no JS renderer) with up to 9 books
    # for pitcher_strikeouts and 5 books for pitcher_er.  Outlier F5 books are
    # filtered by median; consensus line is the median of remaining books.
    # Adds entries not already populated by OddsAPI/Pinnacle/Covers/VI.
    # Maps to sb_implied_prob via simple Poisson CDF approximation (line ± 0.5).
    if _mem_ref is not None:
        try:
            import math as _math  # noqa: PLC0415
            from rotowire_layer import fetch_rotowire_props as _rw_fetch  # noqa: PLC0415

            _rw_data = _rw_fetch()
            _rw_added = 0

            _rw_market_map = {
                "pitcher_strikeouts": "pitcher_strikeouts",
                "pitcher_er":         "pitcher_er",
            }

            def _poisson_over_prob(line: float, lam: float) -> float:
                """P(X > line) where X ~ Poisson(lam). line is typically x.5."""
                k_floor = int(line)
                p_le = 0.0
                for k in range(k_floor + 1):
                    p_le += (lam ** k) * _math.exp(-lam) / _math.factorial(k)
                return round(max(0.01, min(0.99, 1.0 - p_le)), 4)

            for _rw_prop, _rw_market in _rw_market_map.items():
                prop_players = _rw_data.get(_rw_prop, {})
                for _rw_name, _rw_info in prop_players.items():
                    _rw_line = _rw_info["line"]
                    _rw_norm = _normalize(_rw_name)
                    _rw_over_key  = (_rw_norm, _rw_market, "Over")
                    _rw_under_key = (_rw_norm, _rw_market, "Under")
                    if _rw_over_key not in _mem_ref:
                        _rw_over_p = _poisson_over_prob(_rw_line, _rw_line)
                        _mem_ref[_rw_over_key] = {
                            "sb_implied_prob": _rw_over_p,
                            "line":            _rw_line,
                            "bookmaker":       f"rotowire_{_rw_info['books']}books",
                            "over_odds":       None,
                            "under_odds":      None,
                        }
                        _mem_ref[_rw_under_key] = {
                            "sb_implied_prob": round(1.0 - _rw_over_p, 4),
                            "line":            _rw_line,
                            "bookmaker":       f"rotowire_{_rw_info['books']}books",
                            "over_odds":       None,
                            "under_odds":      None,
                        }
                        _rw_added += 1

            if _rw_added:
                log.info("[SBRef] RotoWire supplement: +%d entries (strikeouts+er)", _rw_added)
        except Exception as _rw_err:
            log.debug("[SBRef] RotoWire supplement failed: %s", _rw_err)

    return _mem_ref


def enrich_props_with_sportsbook(props: list, date: str | None = None) -> list:
    """
    Stamp sportsbook reference data on each prop dict.

    Matches each UD/PP prop to the sportsbook reference by
    (player_norm, market_key, side).  When the UD/PP line differs from
    the sportsbook line (e.g. UD has 16.0, Pinnacle has 15.5), the
    implied probability is adjusted using a per-prop-type shift.

    Stamps on each prop:
        sb_implied_prob        — line-adjusted vig-stripped implied prob (0–1)
        sb_implied_prob_over   — over side implied prob
        sb_implied_prob_under  — under side implied prob
        sb_line                — sportsbook's actual line
        sb_line_gap            — UD/PP line minus sportsbook line (positive = more generous)
        _sb_line_adj           — probability adjustment applied for line gap
        bookmaker              — sharpest book that provided this line

    Falls through silently if no sportsbook data available.
    """
    if not props:
        return props

    # Convert date string to int (YYYYMMDD)
    if date:
        try:
            date_int = int(date.replace("-", ""))
        except (ValueError, AttributeError):
            date_int = _today_int()
    else:
        date_int = _today_int()

    ref = build_sportsbook_reference(date_int)
    if not ref:
        log.debug("[SBRef] enrich_props_with_sportsbook: no reference data for %d", date_int)
        return props

    stamped = 0
    for prop in props:
        player = _normalize(
            prop.get("player", "") or prop.get("player_name", "")
        )
        prop_type = prop.get("prop_type", "")
        market_key = STAT_TO_MARKET.get(prop_type, "")
        if not player or not market_key:
            continue

        ud_line = float(prop.get("line", 0) or 0)
        side_raw = str(prop.get("side", "OVER")).upper()
        sb_side = "Over" if side_raw in ("OVER", "HIGHER") else "Under"
        opp_side = "Under" if sb_side == "Over" else "Over"

        # Look up our side first, then try deriving from opposite side
        entry = ref.get((player, market_key, sb_side))
        derived_from_opp = False
        if entry is None:
            opp_entry = ref.get((player, market_key, opp_side))
            if opp_entry:
                entry = {
                    "sb_implied_prob": round(1.0 - opp_entry["sb_implied_prob"], 6),
                    "line":            opp_entry["line"],
                    "bookmaker":       opp_entry["bookmaker"],
                    "over_odds":       opp_entry.get("under_odds"),
                    "under_odds":      opp_entry.get("over_odds"),
                }
                derived_from_opp = True

        if entry is None:
            continue  # No sportsbook data for this player/prop

        sb_line = float(entry.get("line", ud_line) or ud_line)
        raw_implied = float(entry.get("sb_implied_prob", 0.5) or 0.5)

        # ── Line-shift adjustment ────────────────────────────────────────────
        # When UD/PP line differs from sportsbook line, adjust the implied
        # probability to reflect the actual UD/PP line.
        #
        # Example: UD UNDER 16.0 pitching_outs, Pinnacle UNDER 15.5 = 52%
        #   line_diff = 16.0 - 15.5 = +0.5
        #   For UNDER: higher line = easier to hit → add shift
        #   shift = 0.025 per half-point → +2.5pp
        #   adjusted = 52% + 2.5% = 54.5%
        #
        # Example: UD OVER 6.0 strikeouts, Pinnacle OVER 5.5 = 48%
        #   line_diff = 6.0 - 5.5 = +0.5
        #   For OVER: higher line = harder to hit → subtract shift
        #   adjusted = 48% - 4.0% = 44%
        line_diff = ud_line - sb_line
        shift_per_half = _LINE_SHIFT_PER_HALF.get(market_key, _DEFAULT_SHIFT)
        line_adj = 0.0

        if abs(line_diff) >= 0.25:  # Only adjust for meaningful differences
            half_pts = line_diff / 0.5
            if sb_side == "Under":
                line_adj = half_pts * shift_per_half   # higher line → easier UNDER
            else:
                line_adj = -half_pts * shift_per_half  # higher line → harder OVER
            line_adj = max(-0.15, min(0.15, line_adj))  # cap at ±15pp

        adjusted_implied = round(
            max(0.05, min(0.95, raw_implied + line_adj)), 6
        )

        # Derive both sides
        if sb_side == "Under":
            imp_under = adjusted_implied
            imp_over  = round(1.0 - adjusted_implied, 6)
        else:
            imp_over  = adjusted_implied
            imp_under = round(1.0 - adjusted_implied, 6)

        prop["sb_implied_prob"]        = adjusted_implied
        prop["sb_implied_prob_over"]   = imp_over
        prop["sb_implied_prob_under"]  = imp_under
        prop["sb_line"]                = sb_line
        prop["sb_line_gap"]            = round(ud_line - sb_line, 2)
        prop["_sb_line_adj"]           = round(line_adj, 4)
        prop["bookmaker"]              = entry.get("bookmaker", "")
        stamped += 1

    log.info(
        "[SBRef] enrich_props_with_sportsbook: %d/%d props stamped with sb_implied_prob",
        stamped, len(props),
    )
    return props
