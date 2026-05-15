"""
game_logs_refresh.py
====================
Daily refresh of MLB batting and pitching game logs for 2026.

Inspired by tnestico/mlb_scraper — uses the official MLB Stats API
(same source) to fetch boxscore data for every completed game.

Architecture:
  1. Fetch completed game IDs from MLB Stats API schedule for target date range
  2. Parallel-fetch /api/v1/game/{id}/boxscore for each game (ThreadPoolExecutor)
  3. Parse batter rows (mlbam_id, player, date, starter, home_runs, h_1b, h_2b,
     h_3b, b_ab, b_pa, b_runs, b_rbi, b_k) and pitcher rows
     (mlbam_id, player, date, starter, outs, strikeouts, earnedruns, walks, hits)
  4. Upsert into Postgres tables live_batting_logs / live_pitching_logs
  5. Rebuild CSV files data/stats/2026/mlb_batting_logs.csv and
     data/stats/2026/mlb_pitching_logs.csv from full Postgres history

Scheduler slot: job_game_logs_refresh (daily 4:15 AM PT) in orchestrator.py.

Public API
----------
refresh(target_date=None, lookback_days=7)
    → {"games_fetched": N, "batting_rows": M, "pitching_rows": P, "errors": E}
"""
from __future__ import annotations

import csv
import io
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import requests

logger = logging.getLogger(__name__)

# ── MLB Stats API ──────────────────────────────────────────────────────────────
_MLB_SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"
_MLB_BOXSCORE_URL = "https://statsapi.mlb.com/api/v1/game/{game_id}/boxscore"
_MLB_HEADERS      = {"Accept": "application/json", "User-Agent": "PropIQ/1.0"}
_TIMEOUT          = 12
_MAX_WORKERS      = 8

# ── CSV output paths (relative to repo root) ──────────────────────────────────
_BATTING_CSV  = Path("data/stats/2026/mlb_batting_logs.csv")
_PITCHING_CSV = Path("data/stats/2026/mlb_pitching_logs.csv")

_BATTING_COLS  = ["mlbam_id", "player", "date", "starter",
                  "home_runs", "h_1b", "h_2b", "h_3b",
                  "b_ab", "b_pa", "b_runs", "b_rbi", "b_k"]
_PITCHING_COLS = ["mlbam_id", "player", "date", "starter",
                  "outs", "strikeouts", "earnedruns", "walks", "hits"]


# ── Postgres helpers ───────────────────────────────────────────────────────────
def _get_pg():
    """Return a psycopg2 connection or None."""
    try:
        import psycopg2
        url = os.environ.get("DATABASE_URL", "")
        if not url:
            return None
        return psycopg2.connect(url)
    except Exception as exc:
        logger.warning("[GameLogs] PG connect failed: %s", exc)
        return None


def _ensure_tables(conn) -> None:
    sql = """
    CREATE TABLE IF NOT EXISTS live_batting_logs (
        mlbam_id   INT          NOT NULL,
        player     TEXT         NOT NULL,
        game_date  DATE         NOT NULL,
        starter    BOOLEAN      DEFAULT FALSE,
        home_runs  INT          DEFAULT 0,
        h_1b       INT          DEFAULT 0,
        h_2b       INT          DEFAULT 0,
        h_3b       INT          DEFAULT 0,
        b_ab       INT          DEFAULT 0,
        b_pa       INT          DEFAULT 0,
        b_runs     INT          DEFAULT 0,
        b_rbi      INT          DEFAULT 0,
        b_k        INT          DEFAULT 0,
        PRIMARY KEY (mlbam_id, game_date)
    );
    CREATE TABLE IF NOT EXISTS live_pitching_logs (
        mlbam_id    INT          NOT NULL,
        player      TEXT         NOT NULL,
        game_date   DATE         NOT NULL,
        starter     BOOLEAN      DEFAULT FALSE,
        outs        INT          DEFAULT 0,
        strikeouts  INT          DEFAULT 0,
        earnedruns  INT          DEFAULT 0,
        walks       INT          DEFAULT 0,
        hits        INT          DEFAULT 0,
        PRIMARY KEY (mlbam_id, game_date)
    );
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def _upsert_batting(conn, rows: list[dict]) -> int:
    if not rows:
        return 0
    sql = """
    INSERT INTO live_batting_logs
        (mlbam_id, player, game_date, starter,
         home_runs, h_1b, h_2b, h_3b, b_ab, b_pa, b_runs, b_rbi, b_k)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (mlbam_id, game_date) DO UPDATE SET
        player=EXCLUDED.player, starter=EXCLUDED.starter,
        home_runs=EXCLUDED.home_runs, h_1b=EXCLUDED.h_1b,
        h_2b=EXCLUDED.h_2b, h_3b=EXCLUDED.h_3b,
        b_ab=EXCLUDED.b_ab, b_pa=EXCLUDED.b_pa,
        b_runs=EXCLUDED.b_runs, b_rbi=EXCLUDED.b_rbi, b_k=EXCLUDED.b_k
    """
    with conn.cursor() as cur:
        for r in rows:
            cur.execute(sql, (
                r["mlbam_id"], r["player"], r["date"], r["starter"],
                r["home_runs"], r["h_1b"], r["h_2b"], r["h_3b"],
                r["b_ab"], r["b_pa"], r["b_runs"], r["b_rbi"], r["b_k"],
            ))
    conn.commit()
    return len(rows)


def _upsert_pitching(conn, rows: list[dict]) -> int:
    if not rows:
        return 0
    sql = """
    INSERT INTO live_pitching_logs
        (mlbam_id, player, game_date, starter,
         outs, strikeouts, earnedruns, walks, hits)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (mlbam_id, game_date) DO UPDATE SET
        player=EXCLUDED.player, starter=EXCLUDED.starter,
        outs=EXCLUDED.outs, strikeouts=EXCLUDED.strikeouts,
        earnedruns=EXCLUDED.earnedruns, walks=EXCLUDED.walks,
        hits=EXCLUDED.hits
    """
    with conn.cursor() as cur:
        for r in rows:
            cur.execute(sql, (
                r["mlbam_id"], r["player"], r["date"], r["starter"],
                r["outs"], r["strikeouts"], r["earnedruns"],
                r["walks"], r["hits"],
            ))
    conn.commit()
    return len(rows)


# ── MLB API fetchers ───────────────────────────────────────────────────────────
def _fetch_game_ids(start: date, end: date) -> list[tuple[int, str]]:
    """Return list of (game_id, game_date_str) for completed MLB games."""
    params = {
        "sportId": 1,
        "season": start.year,
        "gameType": "R",
        "startDate": start.isoformat(),
        "endDate": end.isoformat(),
        "fields": "dates,date,games,gamePk,status,abstractGameState",
    }
    try:
        resp = requests.get(_MLB_SCHEDULE_URL, params=params,
                            headers=_MLB_HEADERS, timeout=_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.error("[GameLogs] Schedule fetch failed: %s", exc)
        return []

    results = []
    for day in data.get("dates", []):
        game_date = day.get("date", "")
        for game in day.get("games", []):
            state = game.get("status", {}).get("abstractGameState", "")
            if state == "Final":
                results.append((game["gamePk"], game_date))
    return results


def _parse_boxscore(game_id: int, game_date: str) -> tuple[list[dict], list[dict]]:
    """Fetch boxscore for one game; return (batting_rows, pitching_rows)."""
    url = _MLB_BOXSCORE_URL.format(game_id=game_id)
    try:
        resp = requests.get(url, headers=_MLB_HEADERS, timeout=_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("[GameLogs] Boxscore %d failed: %s", game_id, exc)
        return [], []

    batting_rows: list[dict]  = []
    pitching_rows: list[dict] = []

    for side in ("away", "home"):
        team_data = data.get("teams", {}).get(side, {})
        players   = team_data.get("players", {})
        batters   = set(team_data.get("batters", []))
        pitchers  = set(team_data.get("pitchers", []))
        # Starter detection
        batting_order  = team_data.get("battingOrder", [])
        pitching_order = team_data.get("pitchers", [])

        starter_batter_id  = batting_order[0]  if batting_order  else None
        starter_pitcher_id = pitching_order[0] if pitching_order else None

        for pid_str, pdata in players.items():
            pid     = pdata.get("person", {}).get("id")
            name    = pdata.get("person", {}).get("fullName", "")
            stats   = pdata.get("stats", {})

            # ── Batting ───────────────────────────────────────────────────────
            if pid in batters:
                b = stats.get("batting", {}).get("summary", None)
                raw = stats.get("batting", {})
                batting_rows.append({
                    "mlbam_id":  pid,
                    "player":    name,
                    "date":      game_date,
                    "starter":   pid == starter_batter_id,
                    "home_runs": int(raw.get("homeRuns", 0) or 0),
                    "h_1b":      int(raw.get("singles",  0) or 0),
                    "h_2b":      int(raw.get("doubles",  0) or 0),
                    "h_3b":      int(raw.get("triples",  0) or 0),
                    "b_ab":      int(raw.get("atBats",   0) or 0),
                    "b_pa":      int(raw.get("plateAppearances", 0) or 0),
                    "b_runs":    int(raw.get("runs",     0) or 0),
                    "b_rbi":     int(raw.get("rbi",      0) or 0),
                    "b_k":       int(raw.get("strikeOuts", 0) or 0),
                })

            # ── Pitching ──────────────────────────────────────────────────────
            if pid in pitchers:
                raw = stats.get("pitching", {})
                # outs pitched: MLB stores as "inningsPitched" string like "5.2"
                ip_str = raw.get("inningsPitched", "0.0") or "0.0"
                try:
                    innings, thirds = ip_str.split(".")
                    outs = int(innings) * 3 + int(thirds)
                except (ValueError, TypeError):
                    outs = 0
                pitching_rows.append({
                    "mlbam_id":    pid,
                    "player":      name,
                    "date":        game_date,
                    "starter":     pid == starter_pitcher_id,
                    "outs":        outs,
                    "strikeouts":  int(raw.get("strikeOuts",   0) or 0),
                    "earnedruns":  int(raw.get("earnedRuns",   0) or 0),
                    "walks":       int(raw.get("baseOnBalls",  0) or 0),
                    "hits":        int(raw.get("hits",         0) or 0),
                })

    return batting_rows, pitching_rows


# ── CSV rebuild from Postgres ──────────────────────────────────────────────────
def _rebuild_csvs(conn) -> None:
    """Re-export full 2026 Postgres tables to CSV files."""
    for table, path, cols in [
        ("live_batting_logs",  _BATTING_CSV,  _BATTING_COLS),
        ("live_pitching_logs", _PITCHING_CSV, _PITCHING_COLS),
    ]:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            # Map DB col game_date → date for CSV compatibility
            db_cols = [c if c != "date" else "game_date AS date" for c in cols]
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT {', '.join(db_cols)} FROM {table} "
                    f"WHERE EXTRACT(year FROM game_date) = 2026 "
                    f"ORDER BY game_date, mlbam_id"
                )
                rows = cur.fetchall()
            with path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(cols)
                writer.writerows(rows)
            logger.info("[GameLogs] %s rebuilt — %d rows", path.name, len(rows))
        except Exception as exc:
            logger.warning("[GameLogs] CSV rebuild failed for %s: %s", path.name, exc)


# ── Seed CSVs into Postgres on first run ──────────────────────────────────────
def _seed_from_csv(conn) -> None:
    """If Postgres tables are empty, seed from existing CSVs."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM live_batting_logs")
        count = cur.fetchone()[0]
    if count > 0:
        return

    logger.info("[GameLogs] Seeding Postgres from existing CSVs...")
    for table, path, cols, pk_col in [
        ("live_batting_logs",  _BATTING_CSV,  _BATTING_COLS,  "game_date"),
        ("live_pitching_logs", _PITCHING_CSV, _PITCHING_COLS, "game_date"),
    ]:
        if not path.exists():
            continue
        rows_inserted = 0
        with path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            with conn.cursor() as cur:
                for row in reader:
                    placeholders = ", ".join(["%s"] * len(cols))
                    col_names    = ", ".join(
                        [c if c != "date" else "game_date" for c in cols]
                    )
                    values = [row.get(c, row.get("date" if c == "date" else c, None))
                              for c in cols]
                    cur.execute(
                        f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) "
                        f"ON CONFLICT DO NOTHING",
                        values,
                    )
                    rows_inserted += 1
        conn.commit()
        logger.info("[GameLogs] Seeded %d rows into %s", rows_inserted, table)


# ── Main entry point ──────────────────────────────────────────────────────────
def refresh(target_date: date | None = None, lookback_days: int = 7) -> dict:
    """
    Fetch boxscores for the last `lookback_days` days (or a specific date),
    upsert into Postgres, rebuild CSVs.
    """
    end   = target_date or (date.today() - timedelta(days=1))
    start = end - timedelta(days=lookback_days - 1)

    logger.info("[GameLogs] Refreshing %s → %s (%d days)",
                start, end, lookback_days)

    # ── Fetch game IDs ─────────────────────────────────────────────────────────
    game_ids = _fetch_game_ids(start, end)
    logger.info("[GameLogs] %d completed games found", len(game_ids))

    if not game_ids:
        return {"games_fetched": 0, "batting_rows": 0,
                "pitching_rows": 0, "errors": 0}

    # ── Parallel boxscore fetch ────────────────────────────────────────────────
    all_batting:  list[dict] = []
    all_pitching: list[dict] = []
    errors = 0

    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
        futures = {
            pool.submit(_parse_boxscore, gid, gdate): (gid, gdate)
            for gid, gdate in game_ids
        }
        for fut in as_completed(futures):
            try:
                bat, pit = fut.result()
                all_batting.extend(bat)
                all_pitching.extend(pit)
            except Exception as exc:
                gid, _ = futures[fut]
                logger.warning("[GameLogs] Parse error game %d: %s", gid, exc)
                errors += 1

    logger.info("[GameLogs] Parsed %d batting rows, %d pitching rows",
                len(all_batting), len(all_pitching))

    # ── Upsert into Postgres + rebuild CSVs ───────────────────────────────────
    conn = _get_pg()
    if conn:
        try:
            _ensure_tables(conn)
            _seed_from_csv(conn)
            _upsert_batting(conn, all_batting)
            _upsert_pitching(conn, all_pitching)
            _rebuild_csvs(conn)
        finally:
            conn.close()
    else:
        logger.warning("[GameLogs] No Postgres — CSV rebuild skipped")

    return {
        "games_fetched": len(game_ids),
        "batting_rows":  len(all_batting),
        "pitching_rows": len(all_pitching),
        "errors":        errors,
    }


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 7
    result = refresh(lookback_days=days)
    print(result)
