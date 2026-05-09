"""
pitch_whiff_refresh.py
======================
Nightly live whiff% refresh for pitcher arsenal and batter vulnerability.

Architecture (tnestico mlb_scraper pattern):
  1. Fetch yesterday's game IDs from MLB Stats API schedule
  2. Parallel-fetch /api/v1.1/game/{id}/feed/live for all games (ThreadPoolExecutor)
  3. Parse every pitch event → extract pitch_type, is_swing, is_whiff, pitcher_id, batter_id
  4. Aggregate to season-to-date totals (merging with prior day's Postgres rows)
  5. Upsert into pitch_whiff_live (pitcher view) and batter_pitch_whiff_live (batter view)
  6. Invalidate Redis cache so batter_pitch_arsenal_layer picks up fresh data

Scheduler slot: job_pitch_whiff (daily 3:30 AM PT) in orchestrator.py.

Public API
----------
refresh(target_date=None)   → {"games_fetched": N, "pitches_parsed": M, ...}
get_pitcher_whiff(pitcher_id, pitch_type, season=None) → float (whiff%, 0.0–1.0)
get_batter_vulnerability(batter_id, pitcher_id) → float (logit delta, –0.25 to +0.25)

The get_batter_vulnerability() function is a drop-in replacement for
batter_pitch_arsenal_layer.get_batter_pitch_vulnerability() and returns the same
logit-space adjustment — callers don't need to change.
"""
from __future__ import annotations

import json
import logging
import math
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import Any

import requests

logger = logging.getLogger(__name__)

# ── MLB API constants ──────────────────────────────────────────────────────────
_MLB_SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"
_MLB_FEED_URL     = "https://statsapi.mlb.com/api/v1.1/game/{game_id}/feed/live"
_MLB_HEADERS      = {"Accept": "application/json"}

# Pitch codes that count as a swing (from tnestico api_scraper.py)
_SWING_CODES  = frozenset("X F S D E T W L M Q Z R O J".split())
# Pitch codes that count as a whiff (swing-and-miss)
_WHIFF_CODES  = frozenset("S T W M Q O".split())

# Minimum pitches thrown to include a row (avoids tiny-sample noise)
_MIN_PITCHES = 30     # pitcher/pitch-type threshold
_MIN_SWINGS  = 15     # batter/pitch-type threshold

# League averages for logit delta computation
_LG_WHIFF = 0.245
_LG_K_PCT = 0.235

# Redis cache key — invalidated after each refresh
_BATTER_ARSENAL_KEY = "batter_pitch_arsenal_2026"  # same key as batter_pitch_arsenal_layer
_PITCHER_WHIFF_KEY  = "pitcher_whiff_live_2026"

# ── DB connection ──────────────────────────────────────────────────────────────

def _pg_conn():
    import psycopg2
    url = os.getenv("DATABASE_URL")
    if url:
        return psycopg2.connect(url)
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", 5432)),
        dbname=os.getenv("PGDATABASE", "propiq"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", ""),
    )


def _get_redis():
    try:
        import redis as _r
        url = os.environ.get("REDIS_URL") or os.environ.get("REDIS_PUBLIC_URL")
        if not url:
            return None
        return _r.from_url(url, decode_responses=True, socket_connect_timeout=3)
    except Exception:
        return None


# ── Schedule fetch ─────────────────────────────────────────────────────────────

def _get_game_ids(target_date: date) -> list[int]:
    """Return list of game PKs for target_date (regular season games only)."""
    try:
        resp = requests.get(
            _MLB_SCHEDULE_URL,
            params={"sportId": 1, "date": target_date.isoformat(), "gameType": "R"},
            headers=_MLB_HEADERS,
            timeout=15,
        )
        resp.raise_for_status()
        games = []
        for date_block in resp.json().get("dates", []):
            for game in date_block.get("games", []):
                status = game.get("status", {}).get("codedGameState", "")
                # Only Final (F) or Complete (O, I) games have live feed data
                if status in ("F", "O", "I"):
                    games.append(int(game["gamePk"]))
        return games
    except Exception as exc:
        logger.warning("[WhiffRefresh] Schedule fetch failed for %s: %s", target_date, exc)
        return []


# ── Live feed parser ──────────────────────────────────────────────────────────

def _parse_feed(game_json: dict) -> tuple[
    dict[tuple[int, str], dict],   # pitcher_stats: (pitcher_id, pitch_type) → counts
    dict[tuple[int, str], dict],   # batter_stats:  (batter_id, pitch_type) → counts
]:
    """
    Parse a single game's live feed JSON.
    Returns two accumulators keyed by (player_id, pitch_type).

    Each value dict has keys: pitches, swings, whiffs, strikeouts, pa, woba_total.
    Mirrors tnestico's get_data_df() logic but aggregates directly instead of building a DataFrame.
    """
    pitcher_stats: dict[tuple[int, str], dict] = {}
    batter_stats:  dict[tuple[int, str], dict] = {}

    def _pa_acc(d: dict, key: tuple) -> dict:
        if key not in d:
            d[key] = {"pitches": 0, "swings": 0, "whiffs": 0, "strikeouts": 0,
                      "pa": 0, "woba_num": 0.0, "woba_denom": 0}
        return d[key]

    players_lookup = game_json.get("gameData", {}).get("players", {})
    pitcher_of_record: dict[str, dict] = {}   # "home"/"away" → {id, hand}

    try:
        all_plays = game_json["liveData"]["plays"]["allPlays"]
    except KeyError:
        return pitcher_stats, batter_stats

    for ab in all_plays:
        try:
            matchup  = ab.get("matchup", {})
            is_top   = ab["about"]["isTopInning"]
            pitch_side = "home" if is_top else "away"
            bat_side   = "away" if is_top else "home"

            # Resolve pitcher — track substitutions (mirrors tnestico)
            if pitch_side in pitcher_of_record:
                cur_pitcher_id = pitcher_of_record[pitch_side]["id"]
            else:
                cur_pitcher_id = matchup.get("pitcher", {}).get("id")

            cur_batter_id = matchup.get("batter", {}).get("id")

            # Scan for mid-AB pitcher substitutions
            for evt in ab.get("playEvents", []):
                evt_type = evt.get("details", {}).get("eventType", "")
                if evt_type == "pitching_substitution" and "player" in evt:
                    new_id = evt["player"]["id"]
                    cur_pitcher_id = new_id
                    break

            result     = ab.get("result", {})
            is_k       = result.get("eventType") == "strikeout"
            ab_index   = len(ab.get("playEvents", []))

            for n, evt in enumerate(ab.get("playEvents", [])):
                if not (evt.get("isPitch") or "call" in evt.get("details", {})):
                    continue

                # Handle mid-AB substitutions (update current pitcher)
                evt_type = evt.get("details", {}).get("eventType", "")
                if evt_type == "pitching_substitution" and "player" in evt:
                    cur_pitcher_id = evt["player"]["id"]
                elif evt_type == "offensive_substitution" and "player" in evt:
                    new_id = evt["player"]["id"]
                    if new_id == matchup.get("batter", {}).get("id"):
                        cur_batter_id = new_id

                code       = evt.get("details", {}).get("code", "")
                is_swing   = code in _SWING_CODES
                is_whiff   = code in _WHIFF_CODES

                # Pitch type (use Statcast code: FF, SL, CH, CU, SI, KC, FS, etc.)
                pitch_type = (
                    evt.get("details", {}).get("type", {}).get("code", "")
                    or evt.get("pitchData", {}).get("pitchType", {}).get("code", "")
                    or ""
                ).strip().upper()
                if not pitch_type or not cur_pitcher_id:
                    continue

                # ── Pitcher accumulator ───────────────────────────────────────
                pk = (int(cur_pitcher_id), pitch_type)
                pa = _pa_acc(pitcher_stats, pk)
                pa["pitches"] += 1
                if is_swing:
                    pa["swings"] += 1
                if is_whiff:
                    pa["whiffs"] += 1

                # Count PA and strikeout on last pitch of AB
                if n == len(ab["playEvents"]) - 1:
                    pa["pa"] += 1
                    if is_k:
                        pa["strikeouts"] += 1

                # ── Batter accumulator ────────────────────────────────────────
                if cur_batter_id:
                    bk = (int(cur_batter_id), pitch_type)
                    ba = _pa_acc(batter_stats, bk)
                    ba["pitches"] += 1
                    if is_swing:
                        ba["swings"] += 1
                    if is_whiff:
                        ba["whiffs"] += 1

                    if n == len(ab["playEvents"]) - 1:
                        ba["pa"] += 1
                        if is_k:
                            ba["strikeouts"] += 1

            # Update pitcher tracker at end of AB
            if cur_pitcher_id:
                pitcher_of_record[pitch_side] = {"id": cur_pitcher_id}

        except (KeyError, TypeError, ValueError):
            continue

    return pitcher_stats, batter_stats


# ── Parallel fetch ────────────────────────────────────────────────────────────

def _fetch_game_feeds(game_ids: list[int], max_workers: int = 8) -> list[dict]:
    """Fetch multiple game feeds in parallel.  Returns list of feed JSON dicts."""
    feeds = []

    def _fetch_one(gid: int) -> dict | None:
        try:
            resp = requests.get(
                _MLB_FEED_URL.format(game_id=gid),
                headers=_MLB_HEADERS,
                timeout=30,
            )
            if resp.status_code == 200:
                return resp.json()
            logger.debug("[WhiffRefresh] Game %d returned HTTP %d", gid, resp.status_code)
            return None
        except Exception as exc:
            logger.debug("[WhiffRefresh] Game %d fetch error: %s", gid, exc)
            return None

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_fetch_one, gid): gid for gid in game_ids}
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                feeds.append(result)

    return feeds


# ── Season-to-date accumulation from Postgres ─────────────────────────────────

def _load_existing_pitcher(conn, season: int) -> dict[tuple[int, str], dict]:
    """Load existing season-to-date pitcher rows from pitch_whiff_live."""
    existing: dict[tuple[int, str], dict] = {}
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pitcher_id, pitch_type, pitches, swings, whiffs, strikeouts, pa
                FROM pitch_whiff_live WHERE season = %s
                """,
                (season,),
            )
            for row in cur.fetchall():
                key = (row[0], row[1])
                existing[key] = {
                    "pitches": row[2], "swings": row[3], "whiffs": row[4],
                    "strikeouts": row[5], "pa": row[6],
                }
    except Exception as exc:
        logger.debug("[WhiffRefresh] Load existing pitcher rows failed: %s", exc)
    return existing


def _load_existing_batter(conn, season: int) -> dict[tuple[int, str], dict]:
    """Load existing season-to-date batter rows from batter_pitch_whiff_live."""
    existing: dict[tuple[int, str], dict] = {}
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT batter_id, pitch_type, pa, swings, whiffs, strikeouts
                FROM batter_pitch_whiff_live WHERE season = %s
                """,
                (season,),
            )
            for row in cur.fetchall():
                key = (row[0], row[1])
                existing[key] = {
                    "pa": row[2], "swings": row[3], "whiffs": row[4],
                    "strikeouts": row[5], "pitches": 0,
                }
    except Exception as exc:
        logger.debug("[WhiffRefresh] Load existing batter rows failed: %s", exc)
    return existing


# ── Postgres upsert ────────────────────────────────────────────────────────────

def _upsert_pitcher_rows(conn, rows: list[dict], season: int) -> int:
    """Upsert pitcher whiff rows. Returns count of rows written."""
    if not rows:
        return 0
    try:
        with conn.cursor() as cur:
            for row in rows:
                pitches   = row["pitches"]
                swings    = row["swings"]
                whiffs    = row["whiffs"]
                pa        = row["pa"]
                strk      = row["strikeouts"]
                whiff_pct = round(whiffs / swings, 4) if swings > 0 else 0.0
                k_pct     = round(strk   / pa,     4) if pa    > 0 else 0.0
                put_away  = round(whiffs / pa,     4) if pa    > 0 else 0.0

                cur.execute(
                    """
                    INSERT INTO pitch_whiff_live
                        (pitcher_id, pitch_type, season, pitches, swings, whiffs,
                         strikeouts, pa, whiff_pct, k_pct, put_away_pct, refreshed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (pitcher_id, pitch_type, season) DO UPDATE SET
                        pitches      = EXCLUDED.pitches,
                        swings       = EXCLUDED.swings,
                        whiffs       = EXCLUDED.whiffs,
                        strikeouts   = EXCLUDED.strikeouts,
                        pa           = EXCLUDED.pa,
                        whiff_pct    = EXCLUDED.whiff_pct,
                        k_pct        = EXCLUDED.k_pct,
                        put_away_pct = EXCLUDED.put_away_pct,
                        refreshed_at = NOW()
                    """,
                    (row["pitcher_id"], row["pitch_type"], season,
                     pitches, swings, whiffs, strk, pa, whiff_pct, k_pct, put_away),
                )
        conn.commit()
        return len(rows)
    except Exception as exc:
        conn.rollback()
        logger.warning("[WhiffRefresh] Pitcher upsert failed: %s", exc)
        return 0


def _upsert_batter_rows(conn, rows: list[dict], season: int) -> int:
    """Upsert batter whiff rows. Returns count of rows written."""
    if not rows:
        return 0
    try:
        with conn.cursor() as cur:
            for row in rows:
                pa       = row["pa"]
                swings   = row["swings"]
                whiffs   = row["whiffs"]
                strk     = row["strikeouts"]
                whiff_pct = round(whiffs / swings, 4) if swings > 0 else 0.0
                k_pct    = round(strk   / pa,     4) if pa    > 0 else 0.0

                cur.execute(
                    """
                    INSERT INTO batter_pitch_whiff_live
                        (batter_id, pitch_type, season, pa, swings, whiffs,
                         strikeouts, whiff_pct, k_pct, refreshed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (batter_id, pitch_type, season) DO UPDATE SET
                        pa           = EXCLUDED.pa,
                        swings       = EXCLUDED.swings,
                        whiffs       = EXCLUDED.whiffs,
                        strikeouts   = EXCLUDED.strikeouts,
                        whiff_pct    = EXCLUDED.whiff_pct,
                        k_pct        = EXCLUDED.k_pct,
                        refreshed_at = NOW()
                    """,
                    (row["batter_id"], row["pitch_type"], season,
                     pa, swings, whiffs, strk, whiff_pct, k_pct),
                )
        conn.commit()
        return len(rows)
    except Exception as exc:
        conn.rollback()
        logger.warning("[WhiffRefresh] Batter upsert failed: %s", exc)
        return 0


# ── Redis cache invalidation ───────────────────────────────────────────────────

def _invalidate_redis_caches() -> None:
    """Delete batter_pitch_arsenal Redis key so layer reloads from Postgres next call."""
    r = _get_redis()
    if not r:
        return
    try:
        r.delete(_BATTER_ARSENAL_KEY)
        r.delete(_PITCHER_WHIFF_KEY)
        logger.debug("[WhiffRefresh] Redis caches invalidated")
    except Exception as exc:
        logger.debug("[WhiffRefresh] Redis invalidation error: %s", exc)


def _write_pitcher_redis(pitcher_rows: list[dict], season: int) -> None:
    """Write pitcher whiff data to Redis for same-day reads without DB hit."""
    r = _get_redis()
    if not r:
        return
    try:
        payload = {
            f"{row['pitcher_id']}:{row['pitch_type']}": {
                "whiff_pct": row.get("whiff_pct", 0.0),
                "k_pct":     row.get("k_pct", 0.0),
                "pitches":   row.get("pitches", 0),
            }
            for row in pitcher_rows
        }
        r.setex(_PITCHER_WHIFF_KEY, 86400, json.dumps(payload))
    except Exception as exc:
        logger.debug("[WhiffRefresh] Redis pitcher write failed: %s", exc)


# ── Public API — read functions ───────────────────────────────────────────────

def get_pitcher_whiff(pitcher_id: int, pitch_type: str, season: int | None = None) -> float:
    """
    Return pitcher's whiff% vs given pitch type this season.
    Reads from Postgres pitch_whiff_live (Redis-cached after refresh).
    Returns league-average (0.245) if not found.
    """
    if season is None:
        season = date.today().year

    # Try Redis first (fast path after refresh)
    r = _get_redis()
    if r:
        try:
            raw = r.get(_PITCHER_WHIFF_KEY)
            if raw:
                cache = json.loads(raw)
                key   = f"{pitcher_id}:{pitch_type.upper()}"
                if key in cache:
                    return float(cache[key].get("whiff_pct", _LG_WHIFF))
        except Exception:
            pass

    # Postgres fallback
    try:
        conn = _pg_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT whiff_pct FROM pitch_whiff_live "
                "WHERE pitcher_id=%s AND pitch_type=%s AND season=%s",
                (pitcher_id, pitch_type.upper(), season),
            )
            row = cur.fetchone()
            if row:
                return float(row[0])
    except Exception as exc:
        logger.debug("[WhiffRefresh] get_pitcher_whiff DB read failed: %s", exc)
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return _LG_WHIFF


def get_batter_vulnerability(batter_id: int, pitcher_id: int, season: int | None = None) -> float:
    """
    Logit-space K probability adjustment for batter vs this pitcher's arsenal.
    Drop-in replacement for batter_pitch_arsenal_layer.get_batter_pitch_vulnerability().

    Algorithm:
      1. Get pitcher's arsenal (pitch_type → usage%) from statcast_static_layer
      2. For each pitch type, look up batter's whiff% from batter_pitch_whiff_live
      3. Weighted-average batter whiff% across pitcher arsenal
      4. Logit delta vs league avg, scaled by 0.65 dampener
    """
    if season is None:
        season = date.today().year

    # Get pitcher arsenal from statcast static layer (CSV-based, always available)
    arsenal: dict[str, dict] = {}
    try:
        from statcast_static_layer import get_pitcher_arsenal  # noqa: PLC0415
        arsenal = get_pitcher_arsenal(int(pitcher_id)) or {}
    except Exception:
        pass

    if not arsenal:
        return 0.0

    # Read batter whiff data from DB
    batter_whiff: dict[str, float] = {}
    try:
        conn = _pg_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pitch_type, whiff_pct FROM batter_pitch_whiff_live "
                "WHERE batter_id=%s AND season=%s AND swings >= %s",
                (batter_id, season, _MIN_SWINGS),
            )
            for row in cur.fetchall():
                batter_whiff[row[0].strip().upper()] = float(row[1])
        conn.close()
    except Exception as exc:
        logger.debug("[WhiffRefresh] Batter DB read failed: %s", exc)

    # Fall back to batter_pitch_arsenal_layer CSV data if DB empty
    if not batter_whiff:
        try:
            from batter_pitch_arsenal_layer import get_batter_pitch_vulnerability as _csv_fn  # noqa: PLC0415
            return _csv_fn(batter_id, pitcher_id)
        except Exception:
            return 0.0

    # Weighted-average batter whiff% across pitcher arsenal
    total_usage    = 0.0
    weighted_whiff = 0.0

    for pitch_type, metrics in arsenal.items():
        raw_usage = metrics.get("usage", 0.0) or 0.0
        usage = raw_usage / 100.0 if raw_usage > 1.0 else raw_usage
        if usage <= 0.001:
            continue

        bw = batter_whiff.get(pitch_type.upper(), _LG_WHIFF)
        if bw <= 0:
            bw = _LG_WHIFF

        weighted_whiff += usage * bw
        total_usage    += usage

    if total_usage <= 0.001:
        return 0.0

    avg_whiff = weighted_whiff / total_usage

    def _logit(p: float) -> float:
        p = max(0.01, min(0.99, p))
        return math.log(p / (1.0 - p))

    delta  = (_logit(avg_whiff) - _logit(_LG_WHIFF)) * 0.65
    result = round(max(-0.25, min(0.25, delta)), 4)

    logger.debug(
        "[WhiffRefresh] batter=%d pitcher=%d avg_whiff=%.3f delta=%.4f",
        batter_id, pitcher_id, avg_whiff, result,
    )
    return result


# ── Main refresh entry point ───────────────────────────────────────────────────

def refresh(target_date: date | None = None) -> dict:
    """
    Run nightly whiff refresh for target_date (defaults to yesterday).

    Steps:
      1. Get finished game IDs for target_date
      2. Parallel-fetch live feeds
      3. Parse all pitches
      4. Load existing season-to-date totals from Postgres
      5. Merge and upsert updated totals
      6. Invalidate Redis cache

    Returns summary dict.
    """
    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    season = target_date.year
    logger.info("[WhiffRefresh] Starting refresh for %s (season %d)", target_date, season)

    # 1. Game IDs
    game_ids = _get_game_ids(target_date)
    if not game_ids:
        logger.info("[WhiffRefresh] No finished games for %s", target_date)
        return {"games_fetched": 0, "pitches_parsed": 0, "pitcher_rows": 0, "batter_rows": 0}

    logger.info("[WhiffRefresh] Fetching %d game feeds for %s", len(game_ids), target_date)

    # 2. Fetch feeds in parallel
    feeds = _fetch_game_feeds(game_ids)
    logger.info("[WhiffRefresh] Got %d/%d feeds", len(feeds), len(game_ids))

    # 3. Parse all pitches into accumulators
    pitcher_today: dict[tuple[int, str], dict] = {}
    batter_today:  dict[tuple[int, str], dict] = {}
    total_pitches  = 0

    for feed in feeds:
        p_stats, b_stats = _parse_feed(feed)
        for key, counts in p_stats.items():
            if key not in pitcher_today:
                pitcher_today[key] = {"pitches": 0, "swings": 0, "whiffs": 0,
                                      "strikeouts": 0, "pa": 0}
            for k in ("pitches", "swings", "whiffs", "strikeouts", "pa"):
                pitcher_today[key][k] += counts.get(k, 0)
            total_pitches += counts.get("pitches", 0)
        for key, counts in b_stats.items():
            if key not in batter_today:
                batter_today[key] = {"pitches": 0, "swings": 0, "whiffs": 0,
                                     "strikeouts": 0, "pa": 0}
            for k in ("pitches", "swings", "whiffs", "strikeouts", "pa"):
                batter_today[key][k] += counts.get(k, 0)

    logger.info("[WhiffRefresh] Parsed %d pitches; %d pitcher-type pairs, %d batter-type pairs",
                total_pitches, len(pitcher_today), len(batter_today))

    # 4 & 5. Load existing season totals, merge, upsert
    pitcher_rows_written = 0
    batter_rows_written  = 0

    try:
        conn = _pg_conn()

        # Pitcher merge
        existing_p = _load_existing_pitcher(conn, season)
        merged_p: list[dict] = []
        all_p_keys = set(pitcher_today.keys()) | set(existing_p.keys())
        for key in all_p_keys:
            pid, pt = key
            today   = pitcher_today.get(key, {})
            prior   = existing_p.get(key, {})
            pitches = today.get("pitches", 0) + prior.get("pitches", 0)
            swings  = today.get("swings",  0) + prior.get("swings",  0)
            whiffs  = today.get("whiffs",  0) + prior.get("whiffs",  0)
            strk    = today.get("strikeouts", 0) + prior.get("strikeouts", 0)
            pa      = today.get("pa",      0) + prior.get("pa",      0)
            if pitches < _MIN_PITCHES:
                continue
            wh_pct = round(whiffs / swings, 4) if swings > 0 else 0.0
            k_pct  = round(strk   / pa,     4) if pa    > 0 else 0.0
            merged_p.append({
                "pitcher_id": pid, "pitch_type": pt,
                "pitches": pitches, "swings": swings, "whiffs": whiffs,
                "strikeouts": strk, "pa": pa, "whiff_pct": wh_pct, "k_pct": k_pct,
            })

        pitcher_rows_written = _upsert_pitcher_rows(conn, merged_p, season)

        # Batter merge
        existing_b = _load_existing_batter(conn, season)
        merged_b: list[dict] = []
        all_b_keys = set(batter_today.keys()) | set(existing_b.keys())
        for key in all_b_keys:
            bid, pt = key
            today  = batter_today.get(key, {})
            prior  = existing_b.get(key, {})
            pa     = today.get("pa",      0) + prior.get("pa",      0)
            swings = today.get("swings",  0) + prior.get("swings",  0)
            whiffs = today.get("whiffs",  0) + prior.get("whiffs",  0)
            strk   = today.get("strikeouts", 0) + prior.get("strikeouts", 0)
            if swings < _MIN_SWINGS:
                continue
            wh_pct = round(whiffs / swings, 4) if swings > 0 else 0.0
            k_pct  = round(strk   / pa,     4) if pa    > 0 else 0.0
            merged_b.append({
                "batter_id": bid, "pitch_type": pt,
                "pa": pa, "swings": swings, "whiffs": whiffs,
                "strikeouts": strk, "whiff_pct": wh_pct, "k_pct": k_pct,
            })

        batter_rows_written = _upsert_batter_rows(conn, merged_b, season)
        conn.close()

        # Write pitcher summary to Redis for fast in-process reads
        _write_pitcher_redis(merged_p, season)

    except Exception as exc:
        logger.error("[WhiffRefresh] DB operations failed: %s", exc)

    # 6. Invalidate batter arsenal Redis cache so layer picks up new data
    _invalidate_redis_caches()

    result = {
        "date":           target_date.isoformat(),
        "games_fetched":  len(feeds),
        "pitches_parsed": total_pitches,
        "pitcher_rows":   pitcher_rows_written,
        "batter_rows":    batter_rows_written,
    }
    logger.info("[WhiffRefresh] Complete: %s", result)
    return result


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    from datetime import datetime

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    target = None
    if len(sys.argv) > 1:
        try:
            target = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
        except ValueError:
            print(f"Usage: python pitch_whiff_refresh.py [YYYY-MM-DD]")
            sys.exit(1)

    result = refresh(target)
    print(f"\nPitchWhiffRefresh result: {json.dumps(result, indent=2)}")
