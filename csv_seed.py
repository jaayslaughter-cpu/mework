"""
csv_seed.py
===========
Seeds bet_ledger with REAL 2026 MLB game logs from fantasy_baseball CSV exports.

WHY THIS REPLACES historical_seed.py:
  The old seeder used fixed median lines for every player (Bailey Ober got the same
  5.5 K line as Spencer Strider). This is label noise — XGBoost learns garbage.
  
  This seeder computes per-player rolling median lines from actual 2026 game logs,
  then classifies each appearance as OVER or UNDER that real line.
  Result: accurate labels that reflect true DFS prop pricing.

DATA SOURCE:
  fantasy_baseball-main/data/stats/2026/mlb_pitching_logs.csv
  fantasy_baseball-main/data/stats/2026/mlb_batting_logs.csv
  
  These CSVs are scraped from Baseball Reference and updated through today.
  Each row = one player appearance in one game, with full counting stats.

USAGE:
  export DATABASE_URL="postgresql://..."
  python3 csv_seed.py --dry-run          # preview row counts
  python3 csv_seed.py --write            # commit to DB
  python3 csv_seed.py --write --clear    # wipe historical seed rows first

LINES COMPUTED:
  Per player, we compute median(stat) across all appearances, then round to
  the nearest valid DFS line (e.g. 4.7 K median → 4.5 K line).
  Players with < MIN_APPEARANCES are excluded (insufficient sample).
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date

import pandas as pd
import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
MIN_APPEARANCES   = 3       # minimum games to generate a per-player line
NEUTRAL_FEATURES  = json.dumps([0.5] * 27)

# Valid DFS line values for each prop type (nearest match used)
_VALID_LINES: dict[str, list[float]] = {
    "strikeouts":    [1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5],
    "pitching_outs": [8.5, 10.5, 12.5, 13.5, 14.5, 15.5, 16.5, 17.5, 18.5, 19.5, 20.5],
    "earned_runs":   [0.5, 1.5, 2.5, 3.5, 4.5],
    "walks_allowed": [0.5, 1.5, 2.5, 3.5],
    "hits_allowed":  [2.5, 3.5, 4.5, 5.5, 6.5, 7.5],
    "hits":          [0.5, 1.5, 2.5],
    "total_bases":   [0.5, 1.5, 2.5, 3.5, 4.5],
    "hits_runs_rbis":[0.5, 1.5, 2.5, 3.5, 4.5],
    "runs":          [0.5, 1.5],
    "hitter_strikeouts": [0.5, 1.5],
    "home_runs":     [0.5],
}

def _snap_to_valid_line(value: float, prop_type: str) -> float:
    """Snap a computed median to the nearest valid DFS line for this prop type."""
    valid = _VALID_LINES.get(prop_type, [0.5, 1.5, 2.5, 3.5, 4.5, 5.5])
    return min(valid, key=lambda v: abs(v - value))


def _get_conn():
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        raise SystemExit("ERROR: DATABASE_URL not set.")
    return psycopg2.connect(url, connect_timeout=15)


def _find_csv(name: str) -> str:
    """Search for the CSV in common locations."""
    candidates = [
        f"data/stats/2026/{name}",
        f"../fantasy_baseball-main/data/stats/2026/{name}",
        os.path.expanduser(f"~/repos2/fantasy-baseball/fantasy_baseball-main/data/stats/2026/{name}"),
    ]
    for path in candidates:
        if os.path.exists(path):
            return path
    raise FileNotFoundError(
        f"Cannot find {name}. Place it at data/stats/2026/{name} "
        "or pass --pitch-csv / --bat-csv flags."
    )


# ── Pitcher prep ──────────────────────────────────────────────────────────────

def load_pitcher_rows(csv_path: str) -> list[dict]:
    """
    Load mlb_pitching_logs.csv and generate bet_ledger seed rows.
    
    For each pitcher with >= MIN_APPEARANCES starts:
      - Compute per-player median for each prop type
      - Snap to nearest valid DFS line
      - Classify each start as OVER(1) or UNDER(0) that line
    """
    df = pd.read_csv(csv_path)
    df = df[df['starter'] == True].copy()
    df['date'] = pd.to_datetime(df['date']).dt.date

    # Derived stat columns
    df['outs'] = pd.to_numeric(df['outs'], errors='coerce').fillna(0)
    df['strikeouts'] = pd.to_numeric(df['strikeouts'], errors='coerce').fillna(0)
    df['earnedruns'] = pd.to_numeric(df['earnedruns'], errors='coerce').fillna(0)
    df['walks'] = pd.to_numeric(df['walks'], errors='coerce').fillna(0)
    df['hits'] = pd.to_numeric(df['hits'], errors='coerce').fillna(0)

    PROP_MAP = {
        "strikeouts":    "strikeouts",
        "pitching_outs": "outs",
        "earned_runs":   "earnedruns",
        "walks_allowed": "walks",
        "hits_allowed":  "hits",
    }

    rows = []
    skipped = 0

    for mlbam_id, grp in df.groupby('mlbam_id'):
        if len(grp) < MIN_APPEARANCES:
            skipped += 1
            continue

        player_name = grp['player'].iloc[0]
        appearances = grp.sort_values('date')

        for prop_type, col in PROP_MAP.items():
            # Per-player median → snap to nearest valid DFS line
            median_val = float(appearances[col].median())
            line = _snap_to_valid_line(median_val, prop_type)

            for _, row in appearances.iterrows():
                actual_val = float(row[col])
                for side in ("OVER", "UNDER"):
                    if side == "OVER":
                        outcome = 1 if actual_val > line else 0
                    else:
                        outcome = 1 if actual_val < line else 0

                    rows.append({
                        "player_name":   player_name,
                        "prop_type":     prop_type,
                        "line":          line,
                        "side":          side,
                        "agent_name":    "HistoricalCSVSeed",
                        "bet_date":      str(row['date']),
                        "status":        "WIN" if outcome == 1 else "LOSS",
                        "actual_outcome": outcome,
                        "actual_result": actual_val,
                        "platform":      "historical",
                        "discord_sent":  False,
                        "features_json": NEUTRAL_FEATURES,
                        "lookahead_safe": True,
                        "mlbam_id":      int(mlbam_id),
                    })

    log.info("[Pitcher] %d pitchers → %d rows (%d skipped < %d apps)",
             df['mlbam_id'].nunique(), len(rows), skipped, MIN_APPEARANCES)
    return rows


# ── Batter prep ───────────────────────────────────────────────────────────────

def load_batter_rows(csv_path: str) -> list[dict]:
    """
    Load mlb_batting_logs.csv and generate bet_ledger seed rows.
    """
    df = pd.read_csv(csv_path)
    df = df[df['starter'] == True].copy()
    df['date'] = pd.to_datetime(df['date']).dt.date

    for col in ['home_runs','h_1b','h_2b','h_3b','b_ab','b_pa','b_runs','b_rbi','b_k']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Derived
    df['hits']            = df['h_1b'] + df['h_2b'] + df['h_3b'] + df['home_runs']
    df['total_bases']     = df['h_1b'] + df['h_2b']*2 + df['h_3b']*3 + df['home_runs']*4
    df['hits_runs_rbis']  = df['hits'] + df['b_runs'] + df['b_rbi']
    df['runs']            = df['b_runs']
    df['hitter_strikeouts'] = df['b_k']

    PROP_MAP = {
        "hits":              "hits",
        "total_bases":       "total_bases",
        "hits_runs_rbis":    "hits_runs_rbis",
        "runs":              "runs",
        "hitter_strikeouts": "hitter_strikeouts",
        "home_runs":         "home_runs",
    }

    rows = []
    skipped = 0

    for mlbam_id, grp in df.groupby('mlbam_id'):
        if len(grp) < MIN_APPEARANCES:
            skipped += 1
            continue

        player_name = grp['player'].iloc[0]
        appearances = grp.sort_values('date')

        for prop_type, col in PROP_MAP.items():
            median_val = float(appearances[col].median())
            line = _snap_to_valid_line(median_val, prop_type)

            for _, row in appearances.iterrows():
                actual_val = float(row[col])
                for side in ("OVER", "UNDER"):
                    if side == "OVER":
                        outcome = 1 if actual_val > line else 0
                    else:
                        outcome = 1 if actual_val < line else 0

                    rows.append({
                        "player_name":   player_name,
                        "prop_type":     prop_type,
                        "line":          line,
                        "side":          side,
                        "agent_name":    "HistoricalCSVSeed",
                        "bet_date":      str(row['date']),
                        "status":        "WIN" if outcome == 1 else "LOSS",
                        "actual_outcome": outcome,
                        "actual_result": actual_val,
                        "platform":      "historical",
                        "discord_sent":  False,
                        "features_json": NEUTRAL_FEATURES,
                        "lookahead_safe": True,
                        "mlbam_id":      int(mlbam_id),
                    })

    log.info("[Batter] %d batters → %d rows (%d skipped < %d apps)",
             df['mlbam_id'].nunique(), len(rows), skipped, MIN_APPEARANCES)
    return rows


# ── DB write ──────────────────────────────────────────────────────────────────

INSERT_SQL = """
INSERT INTO bet_ledger (
    player_name, prop_type, line, side,
    agent_name, bet_date, status, actual_outcome, actual_result,
    platform, discord_sent, features_json, lookahead_safe
) VALUES (
    %(player_name)s, %(prop_type)s, %(line)s, %(side)s,
    %(agent_name)s, %(bet_date)s, %(status)s, %(actual_outcome)s, %(actual_result)s,
    %(platform)s, %(discord_sent)s, %(features_json)s, %(lookahead_safe)s
)
ON CONFLICT (player_name, prop_type, line, side, agent_name, bet_date) DO NOTHING
"""

CLEAR_SQL = "DELETE FROM bet_ledger WHERE agent_name = 'HistoricalCSVSeed'"


def write_rows(rows: list[dict], dry_run: bool, clear_first: bool) -> None:
    if dry_run:
        log.info("[DRY RUN] Would write %d rows", len(rows))
        # Show a sample
        by_type = {}
        for r in rows:
            key = (r['prop_type'], r['side'])
            by_type[key] = by_type.get(key, 0) + 1
        for k, v in sorted(by_type.items()):
            log.info("  %-35s → %d rows", str(k), v)
        return

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            if clear_first:
                cur.execute(CLEAR_SQL)
                log.info("[DB] Cleared previous HistoricalCSVSeed rows (%d deleted)", cur.rowcount)

            inserted = 0
            skipped  = 0
            BATCH = 1000
            for i in range(0, len(rows), BATCH):
                batch = rows[i:i+BATCH]
                for row in batch:
                    try:
                        cur.execute(INSERT_SQL, row)
                        if cur.rowcount:
                            inserted += 1
                        else:
                            skipped += 1
                    except Exception as e:
                        log.warning("Row insert failed: %s — %s", row.get('player_name'), e)
                        conn.rollback()
                conn.commit()
                if (i // BATCH) % 10 == 0:
                    log.info("  Progress: %d / %d", min(i+BATCH, len(rows)), len(rows))

        log.info("[DB] Done. Inserted=%d, Skipped(duplicate)=%d", inserted, skipped)
    finally:
        conn.close()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Seed bet_ledger from 2026 CSV game logs")
    ap.add_argument("--dry-run", action="store_true", help="Preview only, no DB writes")
    ap.add_argument("--write",   action="store_true", help="Commit to database")
    ap.add_argument("--clear",   action="store_true", help="Delete existing CSV seed rows first")
    ap.add_argument("--pitch-csv", default=None, help="Path to mlb_pitching_logs.csv")
    ap.add_argument("--bat-csv",   default=None, help="Path to mlb_batting_logs.csv")
    args = ap.parse_args()

    if not args.dry_run and not args.write:
        ap.print_help()
        sys.exit(1)

    pitch_csv = args.pitch_csv or _find_csv("mlb_pitching_logs.csv")
    bat_csv   = args.bat_csv   or _find_csv("mlb_batting_logs.csv")

    log.info("Loading pitching logs from: %s", pitch_csv)
    log.info("Loading batting logs from:  %s", bat_csv)

    pitcher_rows = load_pitcher_rows(pitch_csv)
    batter_rows  = load_batter_rows(bat_csv)
    all_rows     = pitcher_rows + batter_rows

    log.info("Total rows to seed: %d (%d pitcher, %d batter)",
             len(all_rows), len(pitcher_rows), len(batter_rows))

    write_rows(all_rows, dry_run=args.dry_run, clear_first=args.clear)


if __name__ == "__main__":
    main()
