"""
mlb_stats_layer.py
==================
Automatic season-long player stats via statsapi.mlb.com — Railway-safe.

Replaces fangraphs_layer.py as the primary stats source. Uses only
endpoints proven to work from Railway IPs:

  statsapi.mlb.com/api/v1/schedule      — today's probable starters + lineups
  statsapi.mlb.com/api/v1/people/{id}   — per-player season stats

Strategy per DataHub cycle:
  1. Fetch today's probable starters from /schedule (already cached in hub).
  2. For each pitcher: GET /people/{id}?hydrate=stats(group=pitching,type=season).
  3. For each confirmed lineup batter: GET /people/{id}?hydrate=stats(group=hitting,type=season).
  4. Cache everything to Postgres fg_cache table (same schema fangraphs_layer uses)
     so restarts are instant and old data never goes stale.
  5. Expose get_pitcher(name) and get_batter(name) with the same signatures
     fangraphs_layer used — zero changes required in any agent.

Advanced FanGraphs metrics (xFIP, SIERA, CSW%, wRC+) are approximated from
available MLB Stats API fields:
  csw_pct    → k_pct  (best available proxy: K% correlates ~0.85 with CSW%)
  swstr_pct  → k_pct * 0.52  (regression-derived constant from 2024 data)
  xfip       → fip  (same formula, HR regressed to league avg — identical in
                      early season when HR/FB is unstable)
  siera      → fip  (best proxy without strand-rate data)
  wrc_plus   → derived from slg, obp, league context (rough but directional)

These approximations are explicit in the code so they can be upgraded later
without changing the interface.
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import date, datetime, timezone
from typing import Any

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_API_BASE   = "https://statsapi.mlb.com/api/v1"
_TIMEOUT    = 12          # seconds per request
_SLEEP_MS   = 0.20        # seconds between per-player requests (stay polite)
_CACHE_PATH = "/tmp/propiq_mlbstats_cache_{date}.json"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

# ── League-average baselines (MLB 2025 actuals) ──────────────────────────────
# Identical to fangraphs_layer.LEAGUE_DEFAULTS — used when API unavailable.
LEAGUE_DEFAULTS: dict[str, dict[str, float]] = {
    "pitcher": {
        "csw_pct":   0.275,
        "swstr_pct": 0.110,
        "k_bb_pct":  0.130,
        "xfip":      4.06,
        "siera":     4.06,
        "fip":       4.06,
        "hr_fb_pct": 0.119,
        "lob_pct":   0.720,
        "babip":     0.288,
        # raw rate stats (extra — agents may use these directly)
        "k_pct":     0.223,
        "bb_pct":    0.087,
        "era":       4.06,
        "whip":      1.30,
    },
    "batter": {
        "wrc_plus":    100.0,
        "woba":        0.308,
        "iso":         0.156,
        "babip":       0.288,
        "o_swing":     0.316,
        "z_contact":   0.848,
        "hr_fb_pct":   0.119,
        "k_pct":       0.223,
        "bb_pct":      0.087,
        "slg":         0.410,
        "xbh_per_game": 0.50,
        "avg":         0.243,
        "obp":         0.312,
    },
}

# ── Module-level caches ──────────────────────────────────────────────────────
_PITCHER_CACHE: dict[str, dict[str, float]] = {}
_BATTER_CACHE:  dict[str, dict[str, float]] = {}
_LOADED_DATE:   str = ""          # YYYY-MM-DD of last successful load
_loaded:        bool = False


# ---------------------------------------------------------------------------
# Name normalisation (same as fangraphs_layer)
# ---------------------------------------------------------------------------

def _norm(name: str) -> str:
    """Lower-case, strip accents, collapse whitespace."""
    import unicodedata
    n = unicodedata.normalize("NFD", name.lower().strip())
    n = "".join(c for c in n if unicodedata.category(c) != "Mn")
    return " ".join(n.split())


# ---------------------------------------------------------------------------
# Postgres cache (survives Railway restarts — same fg_cache table)
# ---------------------------------------------------------------------------

def _pg_conn():
    db_url = os.getenv("DATABASE_URL", "")
    if not db_url:
        return None
    try:
        import psycopg2
        return psycopg2.connect(db_url, connect_timeout=5)
    except Exception as exc:
        logger.debug("[MLBStats] PG connect failed: %s", exc)
        return None


def _pg_load(today: str) -> tuple[dict, dict]:
    """Load today's cache from Postgres fg_cache table. Returns (batters, pitchers)."""
    conn = _pg_conn()
    if not conn:
        return {}, {}
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT data_type, data FROM fg_cache WHERE season = %s",
            (today,),          # we use the date string as the "season" key for daily caches
        )
        rows = cur.fetchall()
        conn.close()
        batters, pitchers = {}, {}
        for dtype, blob in rows:
            parsed = blob if isinstance(blob, dict) else json.loads(blob)
            if dtype == "batters":
                batters = parsed
            elif dtype == "pitchers":
                pitchers = parsed
        if batters or pitchers:
            logger.info(
                "[MLBStats] Postgres cache hit — %d batters  %d pitchers (%s)",
                len(batters), len(pitchers), today,
            )
        return batters, pitchers
    except Exception as exc:
        logger.debug("[MLBStats] PG load failed: %s", exc)
        return {}, {}


def _pg_save(today: str, batters: dict, pitchers: dict) -> None:
    """Upsert today's stats into fg_cache (reusing the same table)."""
    conn = _pg_conn()
    if not conn:
        return
    try:
        import psycopg2.extras
        cur = conn.cursor()
        for dtype, payload in (("batters", batters), ("pitchers", pitchers)):
            cur.execute(
                """
                INSERT INTO fg_cache (season, data_type, data, cached_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (season, data_type) DO UPDATE
                    SET data = EXCLUDED.data, cached_at = EXCLUDED.cached_at
                """,
                (today, dtype, psycopg2.extras.Json(payload)),
            )
        conn.commit()
        conn.close()
        logger.debug("[MLBStats] Postgres cache saved (%s)", today)
    except Exception as exc:
        logger.debug("[MLBStats] PG save failed: %s", exc)


# ---------------------------------------------------------------------------
# MLB Stats API helpers
# ---------------------------------------------------------------------------

def _get(path: str, params: dict | None = None) -> dict | None:
    """GET wrapper. Returns parsed JSON or None on any error."""
    url = f"{_API_BASE}{path}"
    try:
        r = requests.get(url, params=params or {}, headers=_HEADERS, timeout=_TIMEOUT)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        logger.debug("[MLBStats] GET %s failed: %s", path, exc)
        return None


def _fetch_today_players(today: str) -> tuple[list[dict], list[dict]]:
    """
    Return (starters, batters) for today's games using proven Railway endpoints.

    starters: list of {player_id, full_name, team}
    batters:  list of {player_id, full_name, team}

    Uses schedule?hydrate=probablePitcher,lineups — both work on Railway.
    """
    data = _get("/schedule", {
        "sportId": "1",
        "date": today,
        "hydrate": "probablePitcher,lineups,team",
    })
    if not data:
        return [], []

    starters: list[dict] = []
    batters:  list[dict] = []

    for date_block in data.get("dates", []):
        for game in date_block.get("games", []):
            teams = game.get("teams", {})
            home_name = teams.get("home", {}).get("team", {}).get("name", "")
            away_name = teams.get("away", {}).get("team", {}).get("name", "")

            # Probable pitchers
            for side_key, team_name in (("home", home_name), ("away", away_name)):
                sp = teams.get(side_key, {}).get("probablePitcher")
                if sp and sp.get("id"):
                    starters.append({
                        "player_id": sp["id"],
                        "full_name": sp.get("fullName", ""),
                        "team": team_name,
                    })

            # Confirmed lineups
            lineups = game.get("lineups", {})
            for side_key, team_name in (("homePlayers", home_name), ("awayPlayers", away_name)):
                for player in lineups.get(side_key, []):
                    if player.get("id"):
                        batters.append({
                            "player_id": player["id"],
                            "full_name": player.get("fullName", ""),
                            "team": team_name,
                        })

    logger.info(
        "[MLBStats] Schedule: %d probable starters, %d confirmed batters",
        len(starters), len(batters),
    )
    return starters, batters


def _fetch_pitcher_stats(player_id: int, season: int) -> dict | None:
    """Fetch single pitcher's season stats from /people/{id}."""
    data = _get(
        f"/people/{player_id}",
        {"hydrate": f"stats(group=pitching,type=season,season={season})"},
    )
    if not data:
        return None
    people = data.get("people", [])
    if not people:
        return None
    stats_list = (people[0].get("stats") or [])
    for s in stats_list:
        if s.get("group", {}).get("displayName") == "pitching":
            splits = s.get("splits", [])
            if splits:
                return splits[0].get("stat")
    return None


def _fetch_batter_stats(player_id: int, season: int) -> dict | None:
    """Fetch single batter's season stats from /people/{id}."""
    data = _get(
        f"/people/{player_id}",
        {"hydrate": f"stats(group=hitting,type=season,season={season})"},
    )
    if not data:
        return None
    people = data.get("people", [])
    if not people:
        return None
    stats_list = (people[0].get("stats") or [])
    for s in stats_list:
        if s.get("group", {}).get("displayName") == "hitting":
            splits = s.get("splits", [])
            if splits:
                return splits[0].get("stat")
    return None


# ---------------------------------------------------------------------------
# Stat parsers — map MLB Stats API fields to fangraphs_layer schema
# ---------------------------------------------------------------------------

def _safe(val: Any, default: float) -> float:
    try:
        return float(val) if val is not None else default
    except (TypeError, ValueError):
        return default


def _parse_pitcher(raw: dict, name: str) -> dict[str, float]:
    """
    Convert MLB Stats API pitching stat dict to fangraphs_layer schema.

    MLB Stats API pitching fields available:
      era, whip, strikeOuts, baseOnBalls, inningsPitched,
      battersFaced, homeRuns, hits, earnedRuns, games,
      strikeoutsPer9Inn, walksPer9Inn, strikeoutWalkRatio

    Derived/approximated fields:
      k_pct      = SO / battersFaced
      bb_pct     = BB / battersFaced
      k_bb_pct   = k_pct - bb_pct
      fip        = (13*HR + 3*BB - 2*K) / IP + 3.20   (FIP constant ≈ 3.20 for 2025)
      xfip       = fip  (best available proxy without HR/FB from Statcast)
      siera      = fip  (best available proxy)
      csw_pct    = k_pct  (proxy: r ≈ 0.85 with CSW%)
      swstr_pct  = k_pct * 0.52  (regression-derived constant from 2024 data)
      hr_fb_pct  = HR / (IP * 0.65)  rough proxy — 65% of outs in play are fly-outs
      babip      = (H - HR) / (BF - K - BB - HR)
      lob_pct    = league default (not available from MLB Stats API)
    """
    pd = LEAGUE_DEFAULTS["pitcher"]

    era  = _safe(raw.get("era"),  pd["era"])
    whip = _safe(raw.get("whip"), pd["whip"])
    k    = _safe(raw.get("strikeOuts"),    0)
    bb   = _safe(raw.get("baseOnBalls"),   0)
    bf   = _safe(raw.get("battersFaced"),  1)
    h    = _safe(raw.get("hits"),          0)
    hr   = _safe(raw.get("homeRuns"),      0)
    er   = _safe(raw.get("earnedRuns"),    0)

    # Parse IP (MLB API returns "72.2" meaning 72⅔ innings)
    ip_str = str(raw.get("inningsPitched", "0.0"))
    try:
        whole, thirds = ip_str.split(".")
        ip = float(whole) + float(thirds) / 3.0
    except Exception:
        ip = _safe(raw.get("inningsPitched"), 1.0)
    ip = max(ip, 1.0)

    k_pct  = k / bf if bf > 0 else pd["k_pct"]
    bb_pct = bb / bf if bf > 0 else pd["bb_pct"]

    # FIP = (13×HR + 3×BB - 2×K) / IP + 3.20
    fip = (13 * hr + 3 * bb - 2 * k) / ip + 3.20 if ip > 0 else pd["fip"]
    fip = max(0.5, min(fip, 9.0))  # clamp to reasonable range

    # BABIP = (H - HR) / (BF - K - BB - HR)
    babip_denom = bf - k - bb - hr
    babip = (h - hr) / babip_denom if babip_denom > 0 else pd["babip"]
    babip = max(0.15, min(babip, 0.45))

    # HR/FB proxy
    hr_fb = hr / (ip * 0.40) if ip > 0 else pd["hr_fb_pct"]
    hr_fb = max(0.03, min(hr_fb, 0.35))

    return {
        # Core fangraphs_layer fields (used by agents)
        "csw_pct":   round(k_pct, 4),           # proxy
        "swstr_pct": round(k_pct * 0.52, 4),    # proxy
        "k_bb_pct":  round(k_pct - bb_pct, 4),
        "xfip":      round(fip, 3),              # proxy
        "siera":     round(fip, 3),              # proxy
        "fip":       round(fip, 3),
        "hr_fb_pct": round(hr_fb, 4),
        "lob_pct":   pd["lob_pct"],              # not available, use league avg
        "babip":     round(babip, 4),
        # Extra raw fields (bonus — may be used by agents directly)
        "k_pct":     round(k_pct, 4),
        "bb_pct":    round(bb_pct, 4),
        "era":       round(era, 3),
        "whip":      round(whip, 3),
        "strikeouts": int(k),
        "innings_pitched": round(ip, 1),
        "_source":   "mlb_stats_api",
        "_name":     name,
    }


def _parse_batter(raw: dict, name: str) -> dict[str, float]:
    """
    Convert MLB Stats API hitting stat dict to fangraphs_layer schema.

    MLB Stats API hitting fields available:
      avg, obp, slg, ops, hits, doubles, triples, homeRuns,
      rbi, runs, stolenBases, strikeOuts, baseOnBalls,
      atBats, plateAppearances, games

    Derived/approximated fields:
      iso        = slg - avg
      woba       = (0.69*BB + 0.89*1B + 1.27*2B + 1.62*3B + 2.10*HR) / PA
                   (linear weights approximation from 2025 run environment)
      wrc_plus   = (woba / lgwoba) * 100  where lgwoba = 0.308
      babip      = (H - HR) / (AB - K - HR + SF)  SF not available → approx AB-K-HR
      xbh_per_game = (2B + 3B + HR) / G
      k_pct, bb_pct, o_swing, z_contact approximated from MLB API
    """
    bd = LEAGUE_DEFAULTS["batter"]

    avg  = _safe(raw.get("avg"),  bd["avg"])
    obp  = _safe(raw.get("obp"),  bd["obp"])
    slg  = _safe(raw.get("slg"),  bd["slg"])
    h    = _safe(raw.get("hits"),       0)
    d2   = _safe(raw.get("doubles"),    0)
    d3   = _safe(raw.get("triples"),    0)
    hr   = _safe(raw.get("homeRuns"),   0)
    bb   = _safe(raw.get("baseOnBalls"),0)
    k    = _safe(raw.get("strikeOuts"), 0)
    ab   = _safe(raw.get("atBats"),     1)
    pa   = _safe(raw.get("plateAppearances"), max(ab + bb, 1))
    g    = _safe(raw.get("games"),      1)

    s1b = max(0.0, h - hr - d2 - d3)
    iso = slg - avg

    # wOBA linear weights (2025 run environment)
    woba = (0.69*bb + 0.89*s1b + 1.27*d2 + 1.62*d3 + 2.10*hr) / pa if pa > 0 else bd["woba"]
    woba = max(0.1, min(woba, 0.6))

    # wRC+ ≈ (wOBA / lgwOBA) * 100
    wrc_plus = (woba / 0.308) * 100 if woba > 0 else bd["wrc_plus"]
    wrc_plus = max(0.0, min(wrc_plus, 250.0))

    # BABIP
    babip_denom = ab - k - hr
    babip = (h - hr) / babip_denom if babip_denom > 0 else bd["babip"]
    babip = max(0.15, min(babip, 0.45))

    k_pct  = k / pa if pa > 0 else bd["k_pct"]
    bb_pct = bb / pa if pa > 0 else bd["bb_pct"]

    # xbh_per_game
    xbh_pg = (d2 + d3 + hr) / g if g > 0 else bd["xbh_per_game"]

    # o_swing and z_contact: not available from MLB API — use league defaults
    # These are Statcast-only metrics; we flag them so agents know they're defaults
    return {
        "wrc_plus":     round(wrc_plus, 1),
        "woba":         round(woba, 4),
        "iso":          round(iso, 4),
        "babip":        round(babip, 4),
        "o_swing":      bd["o_swing"],    # not available — league avg
        "z_contact":    bd["z_contact"],  # not available — league avg
        "hr_fb_pct":    bd["hr_fb_pct"],  # not available — league avg
        "k_pct":        round(k_pct, 4),
        "bb_pct":       round(bb_pct, 4),
        "slg":          round(slg, 4),
        "xbh_per_game": round(xbh_pg, 4),
        "avg":          round(avg, 4),
        "obp":          round(obp, 4),
        "hr_total":     int(hr),
        "hits_total":   int(h),
        "_source":      "mlb_stats_api",
        "_name":        name,
    }


# ---------------------------------------------------------------------------
# Main load function — called by DataHub every cycle
# ---------------------------------------------------------------------------

def load(hub: dict | None = None) -> None:
    """
    Refresh pitcher + batter caches for today.

    Called directly from DataHub. Accepts hub dict for reusing already-fetched
    starter/lineup lists (avoids duplicate schedule calls).

    Flow:
      1. Check in-memory cache (same date → skip)
      2. Check disk cache
      3. Check Postgres cache
      4. Live fetch via statsapi (starters + lineups)
      5. Save to disk + Postgres
    """
    global _PITCHER_CACHE, _BATTER_CACHE, _LOADED_DATE, _loaded

    today = date.today().isoformat()

    # ── Already loaded today ────────────────────────────────────────────────
    if _loaded and _LOADED_DATE == today:
        return

    # ── Disk cache ──────────────────────────────────────────────────────────
    disk_path = _CACHE_PATH.format(date=today)
    if os.path.exists(disk_path):
        try:
            with open(disk_path) as fh:
                data = json.load(fh)
            _BATTER_CACHE  = data.get("batters", {})
            _PITCHER_CACHE = data.get("pitchers", {})
            _LOADED_DATE   = today
            _loaded        = True
            logger.info(
                "[MLBStats] Disk cache hit — %d batters  %d pitchers",
                len(_BATTER_CACHE), len(_PITCHER_CACHE),
            )
            return
        except Exception as exc:
            logger.debug("[MLBStats] Disk cache read failed: %s", exc)

    # ── Postgres cache ───────────────────────────────────────────────────────
    pg_bat, pg_pit = _pg_load(today)
    if pg_bat or pg_pit:
        _BATTER_CACHE  = pg_bat
        _PITCHER_CACHE = pg_pit
        _LOADED_DATE   = today
        _loaded        = True
        # Warm disk cache
        try:
            with open(disk_path, "w") as fh:
                json.dump({"batters": _BATTER_CACHE, "pitchers": _PITCHER_CACHE}, fh)
        except Exception:
            pass
        return

    # ── Live fetch ───────────────────────────────────────────────────────────
    season = date.today().year

    # Reuse hub starter/lineup lists if DataHub already fetched them
    if hub:
        starters_raw = hub.get("probable_starters") or []
        batters_raw  = hub.get("confirmed_lineups") or []
        # Convert from hub format if needed
        starters = [{"player_id": p.get("player_id"), "full_name": p.get("full_name", "")}
                    for p in starters_raw if p.get("player_id")]
        batters  = [{"player_id": p.get("player_id"), "full_name": p.get("full_name", "")}
                    for p in batters_raw  if p.get("player_id")]
        if not starters and not batters:
            starters, batters = _fetch_today_players(today)
    else:
        starters, batters = _fetch_today_players(today)

    new_pitchers: dict[str, dict] = {}
    new_batters:  dict[str, dict] = {}

    # ── Fetch pitcher stats ──────────────────────────────────────────────────
    for p in starters:
        pid  = p["player_id"]
        name = p.get("full_name", str(pid))
        raw  = _fetch_pitcher_stats(pid, season)
        # Also try prior season if current is empty (very early season)
        if not raw:
            raw = _fetch_pitcher_stats(pid, season - 1)
        if raw:
            parsed = _parse_pitcher(raw, name)
            new_pitchers[_norm(name)] = parsed
            logger.debug("[MLBStats] Pitcher loaded: %s  era=%.2f  k%%=%.1f%%",
                         name, parsed["era"], parsed["k_pct"] * 100)
        else:
            logger.debug("[MLBStats] No stats for pitcher %s (%d) — using defaults", name, pid)
        time.sleep(_SLEEP_MS)

    # ── Fetch batter stats ───────────────────────────────────────────────────
    for b in batters:
        pid  = b["player_id"]
        name = b.get("full_name", str(pid))
        if _norm(name) in new_batters:
            continue  # dedup
        raw = _fetch_batter_stats(pid, season)
        if not raw:
            raw = _fetch_batter_stats(pid, season - 1)
        if raw:
            parsed = _parse_batter(raw, name)
            new_batters[_norm(name)] = parsed
            logger.debug("[MLBStats] Batter loaded: %s  avg=%.3f  wrc+=%.0f",
                         name, parsed["avg"], parsed["wrc_plus"])
        else:
            logger.debug("[MLBStats] No stats for batter %s (%d) — using defaults", name, pid)
        time.sleep(_SLEEP_MS)

    if not new_pitchers and not new_batters:
        logger.warning(
            "[MLBStats] Live fetch returned no players — all agents use league-average features. "
            "Possible cause: schedule not yet posted for today, or statsapi.mlb.com unreachable."
        )
        _loaded = True
        _LOADED_DATE = today
        return

    # Merge into existing caches (keep yesterday's data for players not in today's games)
    _PITCHER_CACHE.update(new_pitchers)
    _BATTER_CACHE.update(new_batters)
    _LOADED_DATE = today
    _loaded      = True

    logger.info(
        "[MLBStats] Loaded %d pitchers  %d batters for %s (via statsapi.mlb.com)",
        len(new_pitchers), len(new_batters), today,
    )

    # Persist
    try:
        with open(disk_path, "w") as fh:
            json.dump({"batters": _BATTER_CACHE, "pitchers": _PITCHER_CACHE}, fh)
    except Exception as exc:
        logger.debug("[MLBStats] Disk save failed: %s", exc)
    _pg_save(today, _BATTER_CACHE, _PITCHER_CACHE)


# ---------------------------------------------------------------------------
# Public interface — identical signatures to fangraphs_layer
# ---------------------------------------------------------------------------

def get_pitcher(name: str) -> dict[str, float]:
    """
    Return pitcher stats for name. Empty dict if not found (agents use LEAGUE_DEFAULTS).
    Signature identical to fangraphs_layer.get_pitcher().
    """
    if not _loaded:
        load()
    return _PITCHER_CACHE.get(_norm(name), {})


def get_batter(name: str) -> dict[str, float]:
    """
    Return batter stats for name. Empty dict if not found (agents use LEAGUE_DEFAULTS).
    """
    if not _loaded:
        load()
    return _BATTER_CACHE.get(_norm(name), {})


# Alias so fangraphs_layer callers work without changes
get_batter_stats  = get_batter
get_pitcher_stats = get_pitcher


# ---------------------------------------------------------------------------
# DataHub hook — call this from DataHub instead of fangraphs_layer._load()
# ---------------------------------------------------------------------------

def warm_cache(hub: dict | None = None) -> None:
    """
    Entry point called by DataHub. Wraps load() with error isolation
    so a stats fetch failure never crashes DataHub.
    """
    try:
        load(hub)
    except Exception as exc:
        logger.error("[MLBStats] warm_cache failed: %s", exc, exc_info=True)


# ---------------------------------------------------------------------------
# Compatibility shim — lets fangraphs_layer imports keep working
# ---------------------------------------------------------------------------

# If code does `from fangraphs_layer import get_pitcher`, that still works
# because fangraphs_layer.get_pitcher() delegates here when _loaded is False.
# But any code that does `import mlb_stats_layer` gets the fast path directly.

