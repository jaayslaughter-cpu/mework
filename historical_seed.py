"""
historical_seed.py
==================
Seeds bet_ledger with historical MLB game logs (2022-2024) for XGBoost training.
Fetches pitcher K/ER/outs and batter hit/TB game logs from MLB Stats API.

Usage (Windows cmd):
    set DATABASE_URL=postgresql://postgres:PASSWORD@host:port/railway
    python historical_seed.py

Usage (dry run — no DB writes, just prints counts):
    python historical_seed.py --dry-run

Usage (single season):
    python historical_seed.py --seasons 2023
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import time
import urllib3
import warnings

import requests
import psycopg2

warnings.filterwarnings("ignore")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

MLBAPI = "https://statsapi.mlb.com/api/v1"
SEASONS = [2022, 2023, 2024]

# ── Prop lines: median DFS line for each stat (used as the historical "line")
PITCHER_LINES = {
    "strikeouts":    5.5,
    "earned_runs":   2.5,
    "pitching_outs": 14.5,
    "walks_allowed": 1.5,
}
BATTER_LINES = {
    "hits":              0.5,
    "total_bases":       1.5,
    "hitter_strikeouts": 0.5,
    "hits_runs_rbis":    2.5,
}

# ── Minimum plate appearances / batters faced to include a game
MIN_PA  = 1    # batters: at least 1 PA
MIN_BF  = 3    # pitchers: at least 3 batters faced


def _get_conn():
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        raise SystemExit("ERROR: DATABASE_URL not set. Export it before running.")
    # Strip any accidental trailing space (Windows && trick side effect)
    url = url.rstrip()
    return psycopg2.connect(url, connect_timeout=15)


def _get(url: str, params: dict = None) -> dict:
    """GET with retries and SSL verify=False for Windows cert issues."""
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


# ────────────────────────────────────────────────────────────
# Fetch helpers
# ────────────────────────────────────────────────────────────

def get_pitcher_ids(season: int) -> list[tuple[int, str]]:
    """Return [(mlbam_id, full_name)] for all pitchers with ≥5 starts."""
    data = _get(f"{MLBAPI}/stats", {
        "stats":       "season",
        "group":       "pitching",
        "season":      season,
        "gameType":    "R",
        "sportId":     1,
        "limit":       1000,
        "playerPool":  "All",
    })
    results = []
    for split in data.get("stats", [{}])[0].get("splits", []):
        p   = split.get("player", {})
        st  = split.get("stat", {})
        gs  = int(st.get("gamesStarted", 0) or 0)
        if gs >= 5:
            results.append((int(p["id"]), p["fullName"]))
    return results


def get_batter_ids(season: int) -> list[tuple[int, str]]:
    """Return [(mlbam_id, full_name)] for all batters with ≥50 PA."""
    data = _get(f"{MLBAPI}/stats", {
        "stats":      "season",
        "group":      "hitting",
        "season":     season,
        "gameType":   "R",
        "sportId":    1,
        "limit":      1500,
        "playerPool": "All",
    })
    results = []
    for split in data.get("stats", [{}])[0].get("splits", []):
        p  = split.get("player", {})
        st = split.get("stat", {})
        pa = int(st.get("plateAppearances", 0) or 0)
        if pa >= 50:
            results.append((int(p["id"]), p["fullName"]))
    return results


def get_pitcher_game_log(player_id: int, season: int) -> list[dict]:
    """Fetch per-game pitching stats for one pitcher."""
    data = _get(f"{MLBAPI}/people/{player_id}", {
        "hydrate": f"stats(group=pitching,type=gameLog,season={season},gameType=R)"
    })
    people = data.get("people", [])
    if not people:
        return []
    stats = people[0].get("stats", [])
    if not stats:
        return []
    return stats[0].get("splits", [])


def get_batter_game_log(player_id: int, season: int) -> list[dict]:
    """Fetch per-game batting stats for one batter."""
    data = _get(f"{MLBAPI}/people/{player_id}", {
        "hydrate": f"stats(group=hitting,type=gameLog,season={season},gameType=R)"
    })
    people = data.get("people", [])
    if not people:
        return []
    stats = people[0].get("stats", [])
    if not stats:
        return []
    return stats[0].get("splits", [])


# ────────────────────────────────────────────────────────────
# Row builders
# ────────────────────────────────────────────────────────────

def build_pitcher_rows(name: str, splits: list[dict]) -> list[dict]:
    rows = []
    for s in splits:
        st       = s.get("stat", {})
        date_str = s.get("date", "")
        if not date_str:
            continue

        bf = int(st.get("battersFaced", 0) or 0)
        if bf < MIN_BF:
            continue

        ks   = int(st.get("strikeOuts",    0) or 0)
        er   = int(st.get("earnedRuns",    0) or 0)
        outs = int(st.get("outs",          0) or 0)
        bb   = int(st.get("baseOnBalls",   0) or 0)  # walks allowed

        for prop_type, line, actual in [
            ("strikeouts",    PITCHER_LINES["strikeouts"],    ks),
            ("earned_runs",   PITCHER_LINES["earned_runs"],   er),
            ("pitching_outs", PITCHER_LINES["pitching_outs"], outs),
            ("walks_allowed", PITCHER_LINES["walks_allowed"], bb),
        ]:
            for side, threshold in [("Over", line), ("Under", line)]:
                if side == "Over":
                    outcome = 1 if actual > threshold else 0
                else:
                    outcome = 1 if actual < threshold else 0

                rows.append({
                    "player_name":    name,
                    "prop_type":      prop_type,
                    "line":           threshold,
                    "side":           side,
                    "agent_name":     "HistoricalSeed",
                    "status":         "WIN" if outcome == 1 else "LOSS",
                    "actual_outcome": outcome,
                    "actual_result":  float(actual),
                    "profit_loss":    1.0 if outcome == 1 else -1.0,
                    "model_prob":     55.0,
                    "ev_pct":         3.0,
                    "bet_date":       date_str,
                    "platform":       "historical",
                    "discord_sent":   True,
                })
    return rows


def build_batter_rows(name: str, splits: list[dict]) -> list[dict]:
    rows = []
    for s in splits:
        st       = s.get("stat", {})
        date_str = s.get("date", "")
        if not date_str:
            continue

        pa = int(st.get("plateAppearances", 0) or 0)
        if pa < MIN_PA:
            continue

        hits  = int(st.get("hits",       0) or 0)
        tb    = int(st.get("totalBases", 0) or 0)
        bk    = int(st.get("strikeOuts", 0) or 0)   # batter Ks
        runs  = int(st.get("runs",       0) or 0)
        rbi   = int(st.get("rbi",        0) or 0)
        hrbi  = hits + runs + rbi                    # hits_runs_rbis composite

        for prop_type, line, actual in [
            ("hits",              BATTER_LINES["hits"],              hits),
            ("total_bases",       BATTER_LINES["total_bases"],       tb),
            ("hitter_strikeouts", BATTER_LINES["hitter_strikeouts"], bk),
            ("hits_runs_rbis",    BATTER_LINES["hits_runs_rbis"],    hrbi),
        ]:
            for side, threshold in [("Over", line), ("Under", line)]:
                if side == "Over":
                    outcome = 1 if actual > threshold else 0
                else:
                    outcome = 1 if actual < threshold else 0

                rows.append({
                    "player_name":    name,
                    "prop_type":      prop_type,
                    "line":           threshold,
                    "side":           side,
                    "agent_name":     "HistoricalSeed",
                    "status":         "WIN" if outcome == 1 else "LOSS",
                    "actual_outcome": outcome,
                    "actual_result":  float(actual),
                    "profit_loss":    1.0 if outcome == 1 else -1.0,
                    "model_prob":     55.0,
                    "ev_pct":         3.0,
                    "bet_date":       date_str,
                    "platform":       "historical",
                    "discord_sent":   True,
                })
    return rows


# ────────────────────────────────────────────────────────────
# DB insert
# ────────────────────────────────────────────────────────────

INSERT_SQL = """
INSERT INTO bet_ledger (
    player_name, prop_type, line, side,
    agent_name, status, actual_outcome, actual_result,
    profit_loss, model_prob, ev_pct, bet_date, platform,
    discord_sent
) VALUES (
    %(player_name)s, %(prop_type)s, %(line)s, %(side)s,
    %(agent_name)s, %(status)s, %(actual_outcome)s, %(actual_result)s,
    %(profit_loss)s, %(model_prob)s, %(ev_pct)s, %(bet_date)s, %(platform)s,
    %(discord_sent)s
)
ON CONFLICT DO NOTHING
"""


def insert_rows(conn, rows: list[dict]) -> tuple[int, int]:
    """Insert rows. Returns (inserted, skipped)."""
    ins = skp = 0
    with conn.cursor() as cur:
        for row in rows:
            try:
                cur.execute(INSERT_SQL, row)
                if cur.rowcount:
                    ins += 1
                else:
                    skp += 1
            except Exception as exc:
                conn.rollback()
                log.warning("Insert failed %s %s %s: %s",
                            row["player_name"], row["prop_type"], row["bet_date"], exc)
                skp += 1
                continue
    conn.commit()
    return ins, skp


# ────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch data but don't write to DB")
    parser.add_argument("--seasons", nargs="+", type=int, default=SEASONS,
                        help="Seasons to seed (default: 2022 2023 2024)")
    parser.add_argument("--pitchers-only", action="store_true")
    parser.add_argument("--batters-only",  action="store_true")
    args = parser.parse_args()

    conn = None if args.dry_run else _get_conn()
    if conn:
        log.info("Connected to Railway Postgres ✓")

    grand_ins = grand_skp = 0

    for season in args.seasons:
        log.info("═══ Season %d ═══", season)
        season_ins = season_skp = 0

        # ── PITCHERS ──────────────────────────────────────────────────────────
        if not args.batters_only:
            log.info("Fetching pitcher list for %d...", season)
            try:
                pitchers = get_pitcher_ids(season)
            except Exception as exc:
                log.warning("  Could not fetch pitcher list: %s", exc)
                pitchers = []
            log.info("  %d qualifying pitchers found", len(pitchers))

            for i, (pid, name) in enumerate(pitchers, 1):
                try:
                    splits = get_pitcher_game_log(pid, season)
                    rows   = build_pitcher_rows(name, splits)
                    if args.dry_run:
                        season_ins += len(rows)
                    elif rows:
                        ins, skp = insert_rows(conn, rows)
                        season_ins += ins
                        season_skp += skp
                except Exception as exc:
                    log.debug("  %s [%d]: %s", name, pid, exc)
                    continue

                if i % 25 == 0 or i == len(pitchers):
                    log.info("  Pitchers %d/%d | rows inserted: %d skipped: %d",
                             i, len(pitchers), season_ins, season_skp)
                time.sleep(0.15)   # polite rate limit

        # ── BATTERS ───────────────────────────────────────────────────────────
        if not args.pitchers_only:
            log.info("Fetching batter list for %d...", season)
            try:
                batters = get_batter_ids(season)
            except Exception as exc:
                log.warning("  Could not fetch batter list: %s", exc)
                batters = []
            log.info("  %d qualifying batters found", len(batters))

            bat_ins = bat_skp = 0
            for i, (pid, name) in enumerate(batters, 1):
                try:
                    splits = get_batter_game_log(pid, season)
                    rows   = build_batter_rows(name, splits)
                    if args.dry_run:
                        bat_ins += len(rows)
                    elif rows:
                        ins, skp = insert_rows(conn, rows)
                        bat_ins += ins
                        bat_skp += skp
                except Exception as exc:
                    log.debug("  %s [%d]: %s", name, pid, exc)
                    continue

                if i % 50 == 0 or i == len(batters):
                    log.info("  Batters %d/%d | rows inserted: %d skipped: %d",
                             i, len(batters), bat_ins, bat_skp)
                time.sleep(0.10)

            season_ins += bat_ins
            season_skp += bat_skp

        log.info("Season %d complete — inserted: %d  skipped/dup: %d",
                 season, season_ins, season_skp)
        grand_ins += season_ins
        grand_skp += season_skp

    if conn:
        conn.close()

    log.info("")
    log.info("══════════════════════════════════════")
    log.info("TOTAL inserted : %d", grand_ins)
    log.info("TOTAL skipped  : %d  (duplicates or already present)", grand_skp)
    log.info("══════════════════════════════════════")
    if args.dry_run:
        log.info("DRY RUN — nothing written to DB")
    else:
        log.info("Done. XGBoost retraining will use these rows on Sunday 2:30 AM.")


if __name__ == "__main__":
    main()
