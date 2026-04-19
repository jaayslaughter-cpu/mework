"""
historical_seed_slow.py
=======================
Resume-safe, disk-friendly historical seed for XGBoost training data.

Changes vs historical_seed.py:
  - discord_sent = TRUE on every row (so XGBoost can read them)
  - Progress tracked in `seed_progress` Postgres table — fully resume-safe
  - Commits every BATCH_SIZE rows (default 100) instead of per-player
  - 1.5s sleep between players (vs 0.15s) to avoid Postgres temp pressure
  - PLAYERS_PER_RUN cap (default 150) — designed to be called repeatedly

Usage:
    python historical_seed_slow.py              # process next 150 players
    python historical_seed_slow.py --players 50 # process next 50 players
    python historical_seed_slow.py --status     # show progress, don't seed
    python historical_seed_slow.py --reset      # clear progress, start over
"""
from __future__ import annotations

import argparse
import logging
import os
import time
import urllib3
import warnings

import requests
import psycopg2
import psycopg2.extras

warnings.filterwarnings("ignore")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

MLBAPI   = "https://statsapi.mlb.com/api/v1"
SEASONS  = [2022, 2023, 2024, 2025]
BATCH_SIZE      = 100   # rows per commit
PLAYERS_PER_RUN = 150   # stop after this many players per invocation
SLEEP_BETWEEN   = 1.5   # seconds between players

PITCHER_LINES = {
    "strikeouts":    5.5,
    "earned_runs":   2.5,
    "pitching_outs": 14.5,
}
BATTER_LINES = {
    "hits":        0.5,
    "total_bases": 1.5,
}
MIN_PA = 1
MIN_BF = 3


# ─── DB ──────────────────────────────────────────────────────────────────────

def _get_conn():
    url = os.environ.get("DATABASE_URL", "").strip().rstrip()
    if not url:
        raise SystemExit("ERROR: DATABASE_URL not set.")
    return psycopg2.connect(url, connect_timeout=15)


def _ensure_progress_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS seed_progress (
                season       INTEGER NOT NULL,
                player_type  VARCHAR(10) NOT NULL,   -- 'pitcher' or 'batter'
                player_id    INTEGER NOT NULL,
                player_name  VARCHAR(120),
                done         BOOLEAN DEFAULT FALSE,
                inserted     INTEGER DEFAULT 0,
                processed_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (season, player_type, player_id)
            )
        """)
    conn.commit()


def _already_done(conn, season: int, player_type: str, player_id: int) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT done FROM seed_progress WHERE season=%s AND player_type=%s AND player_id=%s",
            (season, player_type, player_id)
        )
        row = cur.fetchone()
    return bool(row and row[0])


def _mark_done(conn, season: int, player_type: str, player_id: int, name: str, inserted: int):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO seed_progress (season, player_type, player_id, player_name, done, inserted)
            VALUES (%s, %s, %s, %s, TRUE, %s)
            ON CONFLICT (season, player_type, player_id)
            DO UPDATE SET done=TRUE, inserted=%s, processed_at=NOW()
        """, (season, player_type, player_id, name, inserted, inserted))
    conn.commit()


def _progress_summary(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT season, player_type,
                   COUNT(*) FILTER (WHERE done) AS done_count,
                   COUNT(*) total,
                   SUM(inserted) FILTER (WHERE done) AS rows_ins
            FROM seed_progress
            GROUP BY season, player_type
            ORDER BY season, player_type
        """)
        rows = cur.fetchall()
    return rows


# ─── Fetch helpers ────────────────────────────────────────────────────────────

def _get(url: str, params: dict = None) -> dict:
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, timeout=20, verify=False)
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            if attempt == 2:
                raise
            time.sleep(1.5 * (attempt + 1))
    return {}


def get_pitcher_ids(season: int) -> list[tuple[int, str]]:
    data = _get(f"{MLBAPI}/stats", {
        "stats": "season", "group": "pitching", "season": season,
        "gameType": "R", "sportId": 1, "limit": 1000, "playerPool": "All",
    })
    results = []
    for split in data.get("stats", [{}])[0].get("splits", []):
        p = split.get("player", {})
        st = split.get("stat", {})
        if int(st.get("gamesStarted", 0) or 0) >= 5:
            results.append((int(p["id"]), p["fullName"]))
    return results


def get_batter_ids(season: int) -> list[tuple[int, str]]:
    data = _get(f"{MLBAPI}/stats", {
        "stats": "season", "group": "hitting", "season": season,
        "gameType": "R", "sportId": 1, "limit": 1500, "playerPool": "All",
    })
    results = []
    for split in data.get("stats", [{}])[0].get("splits", []):
        p = split.get("player", {})
        st = split.get("stat", {})
        if int(st.get("plateAppearances", 0) or 0) >= 50:
            results.append((int(p["id"]), p["fullName"]))
    return results


def get_pitcher_game_log(player_id: int, season: int) -> list[dict]:
    data = _get(f"{MLBAPI}/people/{player_id}", {
        "hydrate": f"stats(group=pitching,type=gameLog,season={season},gameType=R)"
    })
    people = data.get("people", [])
    if not people:
        return []
    stats = people[0].get("stats", [])
    return stats[0].get("splits", []) if stats else []


def get_batter_game_log(player_id: int, season: int) -> list[dict]:
    data = _get(f"{MLBAPI}/people/{player_id}", {
        "hydrate": f"stats(group=hitting,type=gameLog,season={season},gameType=R)"
    })
    people = data.get("people", [])
    if not people:
        return []
    stats = people[0].get("stats", [])
    return stats[0].get("splits", []) if stats else []


# ─── Row builders ─────────────────────────────────────────────────────────────

def build_pitcher_rows(name: str, splits: list[dict]) -> list[dict]:
    rows = []
    for s in splits:
        st = s.get("stat", {})
        date_str = s.get("date", "")
        if not date_str:
            continue
        if int(st.get("battersFaced", 0) or 0) < MIN_BF:
            continue
        ks   = int(st.get("strikeOuts",  0) or 0)
        er   = int(st.get("earnedRuns",  0) or 0)
        outs = int(st.get("outs",        0) or 0)
        for prop_type, line, actual in [
            ("strikeouts",    PITCHER_LINES["strikeouts"],    ks),
            ("earned_runs",   PITCHER_LINES["earned_runs"],   er),
            ("pitching_outs", PITCHER_LINES["pitching_outs"], outs),
        ]:
            for side in ("Over", "Under"):
                outcome = 1 if (actual > line if side == "Over" else actual < line) else 0
                rows.append({
                    "player_name": name, "prop_type": prop_type, "line": line,
                    "side": side, "agent_name": "HistoricalSeed",
                    "status": "WIN" if outcome == 1 else "LOSS",
                    "actual_outcome": outcome, "actual_result": float(actual),
                    "profit_loss": 1.0 if outcome == 1 else -1.0,
                    "model_prob": 55.0, "ev_pct": 3.0,
                    "bet_date": date_str, "platform": "historical",
                })
    return rows


def build_batter_rows(name: str, splits: list[dict]) -> list[dict]:
    rows = []
    for s in splits:
        st = s.get("stat", {})
        date_str = s.get("date", "")
        if not date_str:
            continue
        if int(st.get("plateAppearances", 0) or 0) < MIN_PA:
            continue
        hits = int(st.get("hits",       0) or 0)
        tb   = int(st.get("totalBases", 0) or 0)
        for prop_type, line, actual in [
            ("hits",        BATTER_LINES["hits"],        hits),
            ("total_bases", BATTER_LINES["total_bases"], tb),
        ]:
            for side in ("Over", "Under"):
                outcome = 1 if (actual > line if side == "Over" else actual < line) else 0
                rows.append({
                    "player_name": name, "prop_type": prop_type, "line": line,
                    "side": side, "agent_name": "HistoricalSeed",
                    "status": "WIN" if outcome == 1 else "LOSS",
                    "actual_outcome": outcome, "actual_result": float(actual),
                    "profit_loss": 1.0 if outcome == 1 else -1.0,
                    "model_prob": 55.0, "ev_pct": 3.0,
                    "bet_date": date_str, "platform": "historical",
                })
    return rows


# ─── Insert with discord_sent=TRUE ───────────────────────────────────────────

INSERT_SQL = """
INSERT INTO bet_ledger (
    player_name, prop_type, line, side,
    agent_name, status, actual_outcome, actual_result,
    profit_loss, model_prob, ev_pct, bet_date, platform, discord_sent
) VALUES (
    %(player_name)s, %(prop_type)s, %(line)s, %(side)s,
    %(agent_name)s, %(status)s, %(actual_outcome)s, %(actual_result)s,
    %(profit_loss)s, %(model_prob)s, %(ev_pct)s, %(bet_date)s, %(platform)s, TRUE
)
ON CONFLICT DO NOTHING
"""


def insert_rows_batched(conn, rows: list[dict]) -> int:
    """Insert rows in batches of BATCH_SIZE. Returns count inserted."""
    inserted = 0
    for i in range(0, len(rows), BATCH_SIZE):
        chunk = rows[i:i + BATCH_SIZE]
        with conn.cursor() as cur:
            for row in chunk:
                cur.execute(INSERT_SQL, row)
                if cur.rowcount:
                    inserted += 1
        conn.commit()
    return inserted


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--players", type=int, default=PLAYERS_PER_RUN,
                        help="Max players to process this run (default 150)")
    parser.add_argument("--status", action="store_true",
                        help="Print progress summary and exit")
    parser.add_argument("--reset", action="store_true",
                        help="Delete seed_progress rows and HistoricalSeed bet_ledger rows")
    parser.add_argument("--sleep", type=float, default=SLEEP_BETWEEN,
                        help="Seconds between players (default 1.5)")
    args = parser.parse_args()

    conn = _get_conn()
    log.info("Connected to Postgres ✓")
    _ensure_progress_table(conn)

    # ── Status mode ──
    if args.status:
        rows = _progress_summary(conn)
        if not rows:
            log.info("No seed progress yet.")
        else:
            log.info("%-6s %-8s %6s %6s %10s", "Season", "Type", "Done", "Total", "Rows")
            for season, ptype, done, total, ins in rows:
                log.info("%-6s %-8s %6d %6d %10d", season, ptype, done or 0, total or 0, ins or 0)
        conn.close()
        return

    # ── Reset mode ──
    if args.reset:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM seed_progress")
            cur.execute("DELETE FROM bet_ledger WHERE agent_name = 'HistoricalSeed'")
        conn.commit()
        log.info("Reset complete — seed_progress cleared, HistoricalSeed rows deleted.")
        log.info("Run VACUUM ANALYZE bet_ledger in Railway Postgres console to reclaim disk.")
        conn.close()
        return

    players_processed = 0
    total_inserted    = 0

    for season in SEASONS:
        if players_processed >= args.players:
            break

        # ── Pitchers ──
        try:
            pitchers = get_pitcher_ids(season)
        except Exception as exc:
            log.warning("Could not fetch pitcher list for %d: %s", season, exc)
            pitchers = []

        for pid, name in pitchers:
            if players_processed >= args.players:
                log.info("Reached player cap (%d). Stopping — resume next run.", args.players)
                break
            if _already_done(conn, season, "pitcher", pid):
                continue

            try:
                splits = get_pitcher_game_log(pid, season)
                rows   = build_pitcher_rows(name, splits)
                ins    = insert_rows_batched(conn, rows) if rows else 0
                _mark_done(conn, season, "pitcher", pid, name, ins)
                total_inserted    += ins
                players_processed += 1
                if players_processed % 10 == 0:
                    log.info("[%d/%d] %s (%d) → +%d rows | total so far: %d",
                             players_processed, args.players, name, pid, ins, total_inserted)
            except Exception as exc:
                log.debug("Pitcher %s [%d] skipped: %s", name, pid, exc)

            time.sleep(args.sleep)

        if players_processed >= args.players:
            break

        # ── Batters ──
        try:
            batters = get_batter_ids(season)
        except Exception as exc:
            log.warning("Could not fetch batter list for %d: %s", season, exc)
            batters = []

        for pid, name in batters:
            if players_processed >= args.players:
                log.info("Reached player cap (%d). Stopping — resume next run.", args.players)
                break
            if _already_done(conn, season, "batter", pid):
                continue

            try:
                splits = get_batter_game_log(pid, season)
                rows   = build_batter_rows(name, splits)
                ins    = insert_rows_batched(conn, rows) if rows else 0
                _mark_done(conn, season, "batter", pid, name, ins)
                total_inserted    += ins
                players_processed += 1
                if players_processed % 25 == 0:
                    log.info("[%d/%d] %s (%d) → +%d rows | total so far: %d",
                             players_processed, args.players, name, pid, ins, total_inserted)
            except Exception as exc:
                log.debug("Batter %s [%d] skipped: %s", name, pid, exc)

            time.sleep(args.sleep)

    conn.close()
    log.info("Run complete — players: %d | rows inserted: %d", players_processed, total_inserted)
    log.info("Run again to continue. Use --status to check overall progress.")


if __name__ == "__main__":
    main()
