"""
scripts/validate_props.py — PropIQ Calibration Validation Report
=================================================================
Offline Brier/ECE/CRPS/ROI calibration report against graded bet_ledger rows.

Usage:
    uv run --with psycopg2-binary,scipy python3 scripts/validate_props.py

Output:
    - Per-prop-type Brier score and ECE (calibration error)
    - Per-agent win rate and ROI
    - Overall calibration bucket table
    - Flags any agent with Brier > 0.25 (worse than coin flip)
    - Flags any prop type with ECE > 0.08 (needs recalibration)

Database:
    Reads from bet_ledger WHERE actual_outcome IS NOT NULL AND discord_sent = TRUE
    Uses DATABASE_URL environment variable (or Railway internal URL).
"""

from __future__ import annotations

import json
import math
import os
import sys
from collections import defaultdict
from datetime import datetime

# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------

_DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:baPrVnFBbEDgEPtiusCMjHHjyPZMyBbD@gondola.proxy.rlwy.net:12150/railway"
)


def _connect():
    try:
        import psycopg2
        return psycopg2.connect(_DB_URL)
    except ImportError:
        print("ERROR: psycopg2 not installed. Run: pip install psycopg2-binary")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Cannot connect to database: {e}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

def brier_score(y_true: list[float], y_prob: list[float]) -> float:
    """Mean squared error between predicted probability and actual outcome."""
    if not y_true:
        return float("nan")
    return sum((p - y) ** 2 for p, y in zip(y_prob, y_true)) / len(y_true)


def log_loss(y_true: list[float], y_prob: list[float]) -> float:
    """Log loss (cross-entropy)."""
    if not y_true:
        return float("nan")
    eps = 1e-7
    return -sum(
        y * math.log(max(eps, p)) + (1 - y) * math.log(max(eps, 1 - p))
        for p, y in zip(y_prob, y_true)
    ) / len(y_true)


def ece(y_true: list[float], y_prob: list[float], n_bins: int = 10) -> float:
    """
    Expected Calibration Error (ECE) — measures miscalibration.
    Lower is better. 0.05 = well-calibrated, >0.10 = needs recalibration.
    """
    if not y_true:
        return float("nan")

    bins = defaultdict(lambda: {"count": 0, "correct": 0, "prob_sum": 0.0})
    for prob, actual in zip(y_prob, y_true):
        b = min(int(prob * n_bins), n_bins - 1)
        bins[b]["count"] += 1
        bins[b]["correct"] += actual
        bins[b]["prob_sum"] += prob

    n = len(y_true)
    ece_val = 0.0
    for b_data in bins.values():
        cnt = b_data["count"]
        if cnt == 0:
            continue
        avg_prob = b_data["prob_sum"] / cnt
        avg_acc = b_data["correct"] / cnt
        ece_val += (cnt / n) * abs(avg_prob - avg_acc)

    return ece_val


def calibration_buckets(y_true: list[float], y_prob: list[float], n_bins: int = 10) -> list[dict]:
    """Return per-bucket calibration stats for table output."""
    bins: dict[int, dict] = defaultdict(lambda: {"count": 0, "wins": 0, "prob_sum": 0.0})
    for prob, actual in zip(y_prob, y_true):
        b = min(int(prob * n_bins), n_bins - 1)
        bins[b]["count"] += 1
        bins[b]["wins"] += actual
        bins[b]["prob_sum"] += prob

    rows = []
    for b_idx in range(n_bins):
        data = bins.get(b_idx)
        if not data or data["count"] == 0:
            continue
        avg_prob = data["prob_sum"] / data["count"]
        act_rate = data["wins"] / data["count"]
        rows.append({
            "bucket": f"{b_idx * 10}–{(b_idx + 1) * 10}%",
            "n": data["count"],
            "predicted": round(avg_prob, 3),
            "actual": round(act_rate, 3),
            "gap": round(act_rate - avg_prob, 3),
        })
    return rows


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_graded_rows(conn) -> list[dict]:
    """Load all graded rows from bet_ledger."""
    query = """
        SELECT
            player_name,
            prop_type,
            side,
            line,
            agent_name,
            model_prob,
            ev_pct,
            actual_outcome,
            payout,
            units_wagered,
            entry_type,
            bet_date,
            parlay_id,
            features_json
        FROM bet_ledger
        WHERE actual_outcome IS NOT NULL
          AND discord_sent = TRUE
          AND prop_type != 'fantasy_score'
        ORDER BY bet_date DESC
    """
    with conn.cursor() as cur:
        cur.execute(query)
        cols = [d[0] for d in cur.description]
        rows = []
        for r in cur.fetchall():
            row = dict(zip(cols, r))
            # Normalize outcome to binary
            outcome_str = str(row.get("actual_outcome", "")).upper()
            row["_win"] = 1.0 if outcome_str == "WIN" else 0.0
            row["_is_win"] = outcome_str == "WIN"
            row["_is_loss"] = outcome_str == "LOSS"
            # Normalize model_prob to 0–1
            mp = float(row.get("model_prob") or 50.0)
            row["_prob"] = mp / 100.0 if mp > 1.0 else mp
            rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Report sections
# ---------------------------------------------------------------------------

def print_header(title: str) -> None:
    print(f"\n{'='*65}")
    print(f"  {title}")
    print(f"{'='*65}")


def print_separator() -> None:
    print("-" * 65)


def report_overall(rows: list[dict]) -> None:
    print_header("OVERALL CALIBRATION")
    if not rows:
        print("  No graded rows found.")
        return

    y_true = [r["_win"] for r in rows]
    y_prob = [r["_prob"] for r in rows]
    wins = sum(1 for r in rows if r["_is_win"])
    losses = sum(1 for r in rows if r["_is_loss"])
    total = len(rows)

    print(f"  Total graded legs  : {total:,}")
    print(f"  Wins               : {wins:,}  ({wins/total*100:.1f}%)")
    print(f"  Losses             : {losses:,}  ({losses/total*100:.1f}%)")
    print(f"  Brier Score        : {brier_score(y_true, y_prob):.4f}  (random=0.25, perfect=0.00)")
    print(f"  Log Loss           : {log_loss(y_true, y_prob):.4f}")
    print(f"  ECE                : {ece(y_true, y_prob):.4f}  (well-calibrated < 0.05)")

    # ROI
    total_wagered = sum(float(r.get("units_wagered") or 5.0) for r in rows)
    total_payout = sum(float(r.get("payout") or 0.0) for r in rows)
    net = total_payout - total_wagered
    roi = (net / total_wagered * 100) if total_wagered > 0 else 0.0
    print(f"  Net P&L            : ${net:,.2f}")
    print(f"  ROI                : {roi:+.1f}%")

    # Calibration buckets
    print_separator()
    print(f"  {'Bucket':<12} {'N':>6} {'Predicted':>10} {'Actual':>8} {'Gap':>8}")
    print_separator()
    for b in calibration_buckets(y_true, y_prob):
        gap_str = f"{b['gap']:+.3f}"
        flag = "⚠️" if abs(b["gap"]) > 0.08 else ""
        print(f"  {b['bucket']:<12} {b['n']:>6} {b['predicted']:>10.3f} {b['actual']:>8.3f} {gap_str:>8} {flag}")


def report_by_prop_type(rows: list[dict]) -> None:
    print_header("BY PROP TYPE")
    by_type: dict[str, list] = defaultdict(list)
    for r in rows:
        by_type[r.get("prop_type", "unknown")].append(r)

    print(f"  {'Prop Type':<22} {'N':>5} {'Win%':>7} {'Brier':>7} {'ECE':>7} {'ROI':>8}")
    print_separator()

    for pt, pt_rows in sorted(by_type.items(), key=lambda x: -len(x[1])):
        if len(pt_rows) < 5:
            continue
        y_t = [r["_win"] for r in pt_rows]
        y_p = [r["_prob"] for r in pt_rows]
        win_pct = sum(y_t) / len(y_t) * 100
        bs = brier_score(y_t, y_p)
        ec = ece(y_t, y_p)
        wagered = sum(float(r.get("units_wagered") or 5.0) for r in pt_rows)
        payout = sum(float(r.get("payout") or 0.0) for r in pt_rows)
        roi = (payout - wagered) / wagered * 100 if wagered > 0 else 0.0
        flag = " ⚠️" if bs > 0.25 or ec > 0.08 else ""
        print(
            f"  {pt:<22} {len(pt_rows):>5} {win_pct:>6.1f}% {bs:>7.4f} {ec:>7.4f} {roi:>7.1f}%{flag}"
        )


def report_by_agent(rows: list[dict]) -> None:
    print_header("BY AGENT")
    by_agent: dict[str, list] = defaultdict(list)
    for r in rows:
        by_agent[r.get("agent_name", "unknown")].append(r)

    print(f"  {'Agent':<25} {'N':>5} {'Win%':>7} {'Brier':>7} {'ROI':>8}")
    print_separator()

    for agent, ag_rows in sorted(by_agent.items(), key=lambda x: -len(x[1])):
        if len(ag_rows) < 3:
            continue
        y_t = [r["_win"] for r in ag_rows]
        y_p = [r["_prob"] for r in ag_rows]
        win_pct = sum(y_t) / len(y_t) * 100
        bs = brier_score(y_t, y_p)
        wagered = sum(float(r.get("units_wagered") or 5.0) for r in ag_rows)
        payout = sum(float(r.get("payout") or 0.0) for r in ag_rows)
        roi = (payout - wagered) / wagered * 100 if wagered > 0 else 0.0
        flag = " ⚠️" if bs > 0.25 else ""
        print(
            f"  {agent:<25} {len(ag_rows):>5} {win_pct:>6.1f}% {bs:>7.4f} {roi:>7.1f}%{flag}"
        )


def report_confidence_tiers(rows: list[dict]) -> None:
    """Show actual win rates vs predicted probability by confidence tier."""
    print_header("CONFIDENCE TIER ACCURACY  (does 62%+ actually win 62%+?)")

    tiers = [
        ("57–60%", 0.57, 0.60),
        ("60–62%", 0.60, 0.62),
        ("62–65%", 0.62, 0.65),
        ("65–70%", 0.65, 0.70),
        ("70%+",   0.70, 1.00),
    ]

    print(f"  {'Tier':<12} {'N':>5} {'Predicted':>10} {'Actual':>8} {'Gap':>8}")
    print_separator()

    for label, lo, hi in tiers:
        tier_rows = [r for r in rows if lo <= r["_prob"] < hi]
        if not tier_rows:
            continue
        avg_pred = sum(r["_prob"] for r in tier_rows) / len(tier_rows)
        avg_act = sum(r["_win"] for r in tier_rows) / len(tier_rows)
        gap = avg_act - avg_pred
        flag = " ⚠️" if abs(gap) > 0.08 else ""
        print(f"  {label:<12} {len(tier_rows):>5} {avg_pred:>10.3f} {avg_act:>8.3f} {gap:>+8.3f}{flag}")


def report_parlay_accuracy(rows: list[dict]) -> None:
    """Group by parlay_id and show slip-level win rate."""
    print_header("PARLAY SLIP ACCURACY")

    by_parlay: dict[str, list] = defaultdict(list)
    solo_rows = []

    for r in rows:
        pid = r.get("parlay_id")
        if pid:
            by_parlay[pid].append(r)
        else:
            solo_rows.append(r)

    if not by_parlay:
        print("  No parlay data found (all solo legs).")
        return

    slips_2leg = [v for v in by_parlay.values() if len(v) == 2]
    slips_3leg = [v for v in by_parlay.values() if len(v) == 3]
    slips_other = [v for v in by_parlay.values() if len(v) not in (2, 3)]

    def slip_win_rate(slips):
        if not slips:
            return 0.0, 0
        wins = sum(1 for legs in slips if all(r["_is_win"] for r in legs))
        return wins / len(slips), len(slips)

    for label, slips in [("2-leg", slips_2leg), ("3-leg", slips_3leg), ("Other", slips_other)]:
        rate, n = slip_win_rate(slips)
        if n > 0:
            print(f"  {label} slip win rate: {rate*100:.1f}%  ({n} slips)")


def report_model_source(rows: list[dict]) -> None:
    """Show how often XGBoost vs base_rate vs pa_model is the primary signal."""
    print_header("MODEL SOURCE BREAKDOWN")
    sources: dict[str, int] = defaultdict(int)
    for r in rows:
        fj = r.get("features_json")
        if fj and not isinstance(fj, list):
            try:
                fj = json.loads(fj) if isinstance(fj, str) else fj
            except Exception:
                fj = None
        source = "unknown"
        if fj is None:
            source = "no_features"
        elif isinstance(fj, dict):
            source = "placeholder_dict"
        elif isinstance(fj, list) and all(x == 0.5 for x in fj):
            source = "neutral_seed"
        else:
            source = "real_features"
        sources[source] += 1

    for s, n in sorted(sources.items(), key=lambda x: -x[1]):
        print(f"  {s:<25} : {n:,} rows")

    if sources.get("real_features", 0) < 100:
        print("\n  ⚠️  Less than 100 rows with real features — model still training on seed data.")
        print("     Accuracy will improve significantly after 200+ real dispatched+graded rows.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print(f"\nPropIQ Calibration Report — {datetime.now().strftime('%Y-%m-%d %H:%M PT')}")
    print("Connecting to database...")

    conn = _connect()
    print("Loading graded rows...")
    rows = load_graded_rows(conn)
    conn.close()

    if not rows:
        print("\n⚠️  No graded rows found in bet_ledger.")
        print("   Make sure discord_sent=TRUE and actual_outcome is populated.")
        return

    print(f"Loaded {len(rows):,} graded legs.\n")

    report_overall(rows)
    report_confidence_tiers(rows)
    report_by_prop_type(rows)
    report_by_agent(rows)
    report_parlay_accuracy(rows)
    report_model_source(rows)

    print(f"\n{'='*65}")
    print("  Interpretation Guide")
    print(f"{'='*65}")
    print("  Brier < 0.25 = better than random    Brier < 0.20 = unlock gate changes")
    print("  ECE < 0.05   = well-calibrated        ECE > 0.10   = needs recalibration")
    print("  Win% should match predicted prob within ±5pp in each tier")
    print("  200+ real graded legs needed before gates should be changed")
    print()


if __name__ == "__main__":
    main()
