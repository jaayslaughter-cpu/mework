"""
wire_walkforward_backtest.py
=============================
Connects propiq_walkforward_backtest.py to live PropIQ data.

THE PROBLEM
-----------
propiq_walkforward_backtest.py needs:
  backtest_results/prop_lines_input.csv — historical prop lines with outcomes

This file doesn't exist automatically. It must be generated from the
bet_ledger Postgres table which already contains every prop PropIQ has
evaluated, with actual outcomes for settled bets.

THIS SCRIPT
-----------
1. Exports prop_lines_input.csv from the bet_ledger DB
2. Validates the export has enough data to run meaningful backtests
3. Runs the walk-forward backtest and prints a summary
4. Wires a monthly backtest cron into Railway (or prints the cron spec)

THREE WAYS TO GET prop_lines_input.csv
---------------------------------------
A. From Postgres (production — recommended):
   python wire_walkforward_backtest.py --export-from-db

B. From a CSV export of bet_ledger you already have:
   python wire_walkforward_backtest.py --from-csv path/to/bet_ledger.csv

C. Test with synthetic data (verifies the backtest engine works):
   python wire_walkforward_backtest.py --synthetic

THEN RUN THE BACKTEST
---------------------
   python propiq_walkforward_backtest.py
   python propiq_walkforward_backtest.py --prop-type strikeouts --folds 3

OUTPUTS
-------
   backtest_results/walkforward_bets.csv     — every bet evaluated
   backtest_results/walkforward_summary.json — fold ROI, Brier, p-values
   backtest_results/calibration_table.csv   — predicted vs actual buckets
"""

from __future__ import annotations

import csv
import json
import logging
import os
import random
import sys
from datetime import date, timedelta
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [WF-BACKTEST] %(message)s")
log = logging.getLogger(__name__)

OUTPUT_DIR  = Path("backtest_results")
INPUT_CSV   = OUTPUT_DIR / "prop_lines_input.csv"
OUTPUT_DIR.mkdir(exist_ok=True)

# Required columns for propiq_walkforward_backtest.py
REQUIRED_COLS = [
    "game_date",
    "pitcher_name",
    "prop_type",
    "line",
    "side",
    "model_prob",
    "market_implied",
    "over_american",
    "actual_outcome",
    "result",          # "win" | "loss" | "push"
    "pnl",
    "ev",
]

# Optional but enriching columns
OPTIONAL_COLS = [
    "player_name",
    "team",
    "venue",
    "k_rate",
    "bb_rate",
    "era",
    "whip",
    "swstr_pct",
    "opp_k_pct",
    "wind_speed",
    "temp_f",
    "platform",
    "agent_name",
    "sim_edge_reasons",
]


# ══════════════════════════════════════════════════════════════════════════════
# EXPORT FROM POSTGRES
# ══════════════════════════════════════════════════════════════════════════════

BET_LEDGER_QUERY = """
SELECT
    TO_CHAR(game_date, 'YYYY-MM-DD')          AS game_date,
    COALESCE(pitcher_name, player_name, '')    AS pitcher_name,
    player_name,
    prop_type,
    CAST(line AS FLOAT)                        AS line,
    LOWER(COALESCE(side, direction, 'over'))   AS side,
    CAST(COALESCE(model_prob, 0.5) AS FLOAT)   AS model_prob,
    CAST(COALESCE(market_implied, 0.5) AS FLOAT) AS market_implied,
    CAST(COALESCE(over_american, -110) AS INT) AS over_american,
    CAST(COALESCE(actual_outcome, 0) AS FLOAT) AS actual_outcome,
    LOWER(COALESCE(result, ''))                AS result,
    CAST(COALESCE(pnl, 0) AS FLOAT)            AS pnl,
    CAST(COALESCE(ev, 0) AS FLOAT)             AS ev,
    team,
    venue,
    CAST(COALESCE(k_rate, 0) AS FLOAT)         AS k_rate,
    CAST(COALESCE(bb_rate, 0) AS FLOAT)        AS bb_rate,
    CAST(COALESCE(era, 0) AS FLOAT)            AS era,
    CAST(COALESCE(whip, 0) AS FLOAT)           AS whip,
    platform,
    COALESCE(agent_name, '')                   AS agent_name,
    sim_edge_reasons::TEXT                     AS sim_edge_reasons
FROM bet_ledger
WHERE
    actual_outcome IS NOT NULL          -- only settled bets
    AND result IN ('win', 'loss', 'push')
    AND game_date >= CURRENT_DATE - INTERVAL '365 days'
ORDER BY game_date ASC, player_name ASC;
"""


def export_from_db(database_url: str | None = None) -> bool:
    """Export settled bet_ledger rows to prop_lines_input.csv."""
    db_url = database_url or os.environ.get("DATABASE_URL", "")
    if not db_url:
        log.error("DATABASE_URL not set. Export with --from-csv or set DATABASE_URL.")
        return False

    try:
        import psycopg2
    except ImportError:
        log.error("psycopg2 not installed. Run: pip install psycopg2-binary")
        return False

    log.info("Connecting to Postgres...")
    try:
        conn = psycopg2.connect(db_url, connect_timeout=10)
    except Exception as e:
        log.error("Connection failed: %s", e)
        return False

    try:
        with conn.cursor() as cur:
            cur.execute(BET_LEDGER_QUERY)
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
    finally:
        conn.close()

    if not rows:
        log.warning("No settled bets found in bet_ledger. Run --synthetic to test the engine.")
        return False

    log.info("Exported %d settled bet records.", len(rows))

    with open(INPUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=colnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(zip(colnames, row)))

    log.info("Saved to %s", INPUT_CSV)
    return True


def export_from_csv(source_csv: str) -> bool:
    """Remap an existing bet_ledger CSV export to the required schema."""
    src = Path(source_csv)
    if not src.exists():
        log.error("Source CSV not found: %s", source_csv)
        return False

    import csv as csv_mod
    with open(src, newline="") as f:
        reader = csv_mod.DictReader(f)
        rows = list(reader)
        all_cols = set(reader.fieldnames or [])

    log.info("Source: %d rows, %d columns", len(rows), len(all_cols))

    # Column name remapping (handle common variations)
    REMAP = {
        "date":             "game_date",
        "player":           "player_name",
        "pitcher":          "pitcher_name",
        "stat_type":        "prop_type",
        "prop_line":        "line",
        "direction":        "side",
        "predicted_prob":   "model_prob",
        "market_prob":      "market_implied",
        "american_odds":    "over_american",
        "outcome":          "actual_outcome",
        "win_loss":         "result",
        "profit_loss":      "pnl",
        "expected_value":   "ev",
    }

    remapped_rows = []
    for row in rows:
        new_row = {}
        for k, v in row.items():
            new_row[REMAP.get(k, k)] = v
        remapped_rows.append(new_row)

    # Check required columns
    if remapped_rows:
        missing = [c for c in REQUIRED_COLS if c not in remapped_rows[0]]
        if missing:
            log.warning("Missing required columns: %s", missing)
            log.warning("The backtest will use defaults for missing fields.")

    # Filter to settled bets only
    settled = [
        r for r in remapped_rows
        if r.get("result", "").lower() in ("win", "loss", "push")
        and r.get("actual_outcome") not in (None, "", "None")
    ]
    log.info("Settled bets: %d / %d", len(settled), len(remapped_rows))

    if len(settled) < 50:
        log.warning("Only %d settled bets — backtest may not be statistically meaningful.", len(settled))

    all_cols_out = list({k for row in settled for k in row.keys()})
    with open(INPUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_cols_out, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(settled)

    log.info("Saved %d rows to %s", len(settled), INPUT_CSV)
    return True


def generate_synthetic(n_bets: int = 500, seed: int = 42) -> bool:
    """
    Generate synthetic prop_lines_input.csv for testing the backtest engine.

    Uses a slightly-positive-EV model (win rate ~53-55%) to simulate
    realistic PropIQ output. Useful for verifying the engine works before
    real data is available.
    """
    random.seed(seed)
    log.info("Generating %d synthetic bet records...", n_bets)

    prop_types = ["strikeouts"] * 6 + ["hits"] * 4
    pitcher_pool = [
        "Spencer Strider", "Gerrit Cole", "Zack Wheeler", "Dylan Cease",
        "Blake Snell", "Shane Bieber", "Corbin Burnes", "Kevin Gausman",
        "Logan Webb", "Max Fried", "Sandy Alcantara", "Tarik Skubal",
    ]

    rows = []
    start_date = date(2025, 4, 1)

    for i in range(n_bets):
        prop_type   = random.choice(prop_types)
        game_date   = start_date + timedelta(days=random.randint(0, 200))
        pitcher     = random.choice(pitcher_pool)
        line        = random.choice([3.5, 4.5, 5.5, 6.5]) if prop_type == "strikeouts" else random.choice([0.5, 1.5])
        side        = "over"

        # Model has slight edge: model_prob slightly > market_implied on winning bets
        true_win_rate = 0.535 + random.gauss(0, 0.05)
        true_win_rate = max(0.40, min(0.70, true_win_rate))

        model_prob     = true_win_rate + random.gauss(0, 0.03)
        model_prob     = max(0.52, min(0.75, model_prob))
        market_implied = model_prob - random.uniform(0.02, 0.08)
        market_implied = max(0.45, min(0.65, market_implied))

        ev = (model_prob - market_implied)
        actual_win = random.random() < true_win_rate
        result = "win" if actual_win else "loss"
        pnl    = round(0.909 if actual_win else -1.0, 4)  # -110 standard
        actual_outcome = line + 1.0 if (actual_win and side == "over") else line - 0.5

        rows.append({
            "game_date":       game_date.isoformat(),
            "pitcher_name":    pitcher,
            "player_name":     f"Batter {i % 20 + 1}" if prop_type != "strikeouts" else pitcher,
            "prop_type":       prop_type,
            "line":            line,
            "side":            side,
            "model_prob":      round(model_prob, 4),
            "market_implied":  round(market_implied, 4),
            "over_american":   -110,
            "actual_outcome":  round(actual_outcome, 1),
            "result":          result,
            "pnl":             pnl,
            "ev":              round(ev, 4),
            "team":            random.choice(["NYY", "LAD", "HOU", "ATL", "PHI"]),
            "venue":           random.choice(["Yankee Stadium", "Dodger Stadium", "Minute Maid Park"]),
            "k_rate":          round(random.uniform(0.20, 0.35), 3),
            "bb_rate":         round(random.uniform(0.06, 0.12), 3),
            "era":             round(random.uniform(2.5, 5.0), 2),
            "whip":            round(random.uniform(0.9, 1.4), 2),
            "platform":        random.choice(["prizepicks", "underdog"]),
            "agent_name":      random.choice(["EVHunter", "StreakAgent"]),
            "sim_edge_reasons": json.dumps(["xgb_model", f"prop_{prop_type}"]),
        })

    with open(INPUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    log.info("Synthetic data saved to %s", INPUT_CSV)
    log.info("Note: synthetic data shows ~53-55%% win rate — verify engine output matches.")
    return True


def validate_input_csv() -> dict:
    """Check prop_lines_input.csv has enough data to run a meaningful backtest."""
    if not INPUT_CSV.exists():
        return {"valid": False, "reason": f"{INPUT_CSV} not found"}

    import csv as csv_mod
    with open(INPUT_CSV, newline="") as f:
        reader = csv_mod.DictReader(f)
        rows = list(reader)

    if not rows:
        return {"valid": False, "reason": "empty file"}

    cols = set(rows[0].keys())
    missing = [c for c in REQUIRED_COLS if c not in cols]

    prop_types = set(r.get("prop_type", "") for r in rows)
    date_range = sorted(set(r.get("game_date", "") for r in rows))

    return {
        "valid":       len(missing) == 0 and len(rows) >= 50,
        "n_rows":      len(rows),
        "missing_cols": missing,
        "prop_types":  sorted(prop_types),
        "date_range":  (date_range[0], date_range[-1]) if date_range else ("?", "?"),
        "n_wins":      sum(1 for r in rows if r.get("result") == "win"),
        "n_losses":    sum(1 for r in rows if r.get("result") == "loss"),
        "win_rate":    round(sum(1 for r in rows if r.get("result") == "win") / len(rows), 3) if rows else 0,
    }


def run_backtest(extra_args: list[str] | None = None) -> None:
    """Run the walk-forward backtest using the current prop_lines_input.csv."""
    import subprocess
    cmd = [sys.executable, "propiq_walkforward_backtest.py"] + (extra_args or [])
    log.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=False)
    if result.returncode != 0:
        log.error("Backtest exited with code %d", result.returncode)
    else:
        log.info("Backtest complete. Check backtest_results/ for output.")


def print_cron_spec() -> None:
    """Print Railway cron spec for monthly backtest runs."""
    print("""
RAILWAY CRON — Monthly walk-forward backtest
=============================================
Add to railway.toml or via Railway dashboard:

  [[cron]]
  name    = "monthly-walkforward-backtest"
  command = "python wire_walkforward_backtest.py --export-from-db && python propiq_walkforward_backtest.py --folds 3 --permutations 1000"
  schedule = "0 4 1 * *"   # 4:00 AM on the 1st of each month

This exports fresh data from bet_ledger, then runs the full backtest.
Results appear in backtest_results/walkforward_summary.json.

You can also run on demand:
  python wire_walkforward_backtest.py --export-from-db
  python propiq_walkforward_backtest.py --prop-type strikeouts
""")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Wire walk-forward backtest to PropIQ data")
    parser.add_argument("--export-from-db", action="store_true",
                        help="Export prop_lines_input.csv from Postgres bet_ledger")
    parser.add_argument("--from-csv",       metavar="PATH",
                        help="Remap an existing bet_ledger CSV export")
    parser.add_argument("--synthetic",      action="store_true",
                        help="Generate synthetic test data (500 bets)")
    parser.add_argument("--validate",       action="store_true",
                        help="Validate existing prop_lines_input.csv")
    parser.add_argument("--run",            action="store_true",
                        help="Run the backtest after exporting data")
    parser.add_argument("--cron",           action="store_true",
                        help="Print Railway cron specification")
    parser.add_argument("--db-url",         metavar="URL",
                        help="Postgres connection URL (overrides DATABASE_URL env var)")
    args = parser.parse_args()

    if args.cron:
        print_cron_spec()
        sys.exit(0)

    ok = True
    if args.export_from_db:
        ok = export_from_db(args.db_url)
    elif args.from_csv:
        ok = export_from_csv(args.from_csv)
    elif args.synthetic:
        ok = generate_synthetic()

    if args.validate or ok:
        info = validate_input_csv()
        print(f"\nprop_lines_input.csv validation:")
        for k, v in info.items():
            print(f"  {k}: {v}")
        if not info.get("valid"):
            print("\n  Run with --export-from-db, --from-csv, or --synthetic to create input data.")

    if args.run and ok and validate_input_csv().get("valid"):
        run_backtest()
    elif args.run:
        log.error("Cannot run backtest — input data not valid. Fix validation errors first.")

    if len(sys.argv) == 1:
        parser.print_help()
        print()
        print_cron_spec()
