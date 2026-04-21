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
