"""
seed_monitor.py — PropIQ Historical Seed Progress Monitor
Run this anytime to see where the slow seed stands and what XGBoost has learned.

Usage:
    python seed_monitor.py

Output:
    - seed_progress table: how many players done per season/type
    - bet_ledger: how many historical rows XGBoost can train on
    - XGBoost readiness: estimated model quality based on row count
    - Next batch ETA
"""

import psycopg2
from datetime import date, datetime

DATABASE_URL = "postgresql://postgres:baPrVnFBbEDgEPtiusCMjHHjyPZMyBbD@gondola.proxy.rlwy.net:12150/railway"

def bar(pct, width=30):
    filled = int(width * pct / 100)
    return "█" * filled + "░" * (width - filled)

conn = psycopg2.connect(DATABASE_URL, connect_timeout=15)
conn.autocommit = True   # Each statement is its own transaction — one error never cascades
cur  = conn.cursor()
print()
print("━" * 60)
print("  PROPIQ HISTORICAL SEED MONITOR")
print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S PT')}")
print("━" * 60)

# ── 1. seed_progress table ────────────────────────────────────────
print("\n📋  SEED PROGRESS (by season + type)\n")
try:
    cur.execute("""
        SELECT season, player_type,
               COUNT(*) FILTER (WHERE done = TRUE)  AS done,
               COUNT(*)                              AS total,
               COALESCE(SUM(inserted) FILTER (WHERE done = TRUE), 0) AS rows_inserted
        FROM seed_progress
        GROUP BY season, player_type
        ORDER BY season, player_type
    """)
    rows = cur.fetchall()
    if not rows:
        print("  No seed_progress rows yet — first batch hasn't run.")
    else:
        total_done = total_players = total_rows = 0
        for season, ptype, done, total, rows_ins in rows:
            pct = done / total * 100 if total > 0 else 0
            print(f"  {season} {ptype:<8}  {done:>4}/{total:<4}  {bar(pct, 20)} {pct:5.1f}%  ~{rows_ins:,} rows")
            total_done    += done
            total_players += total
            total_rows    += rows_ins
        overall_pct = total_done / total_players * 100 if total_players > 0 else 0
        print(f"\n  {'TOTAL':<14}  {total_done:>4}/{total_players:<4}  {bar(overall_pct, 20)} {overall_pct:5.1f}%  ~{total_rows:,} rows")
        remaining = total_players - total_done
        if remaining == 0:
            eta_str = "Complete ✅"
        elif remaining <= 300:
            eta_str = "~today"
        else:
            days_left = remaining / 300
            eta_str = f"~{days_left:.1f} days"
        print(f"\n  Players remaining: {remaining:,}  |  ETA: {eta_str}")
except Exception as ex:
    print(f"  seed_progress not available yet: {ex}")

# ── 2. bet_ledger XGBoost-visible rows ───────────────────────────
print("\n\n📊  BET_LEDGER — XGBOOST TRAINING DATA\n")
try:
    cur.execute("""
        SELECT
            prop_type,
            COUNT(*) AS total_rows,
            COUNT(*) FILTER (WHERE actual_outcome IS NOT NULL) AS graded,
            COUNT(*) FILTER (WHERE features_json IS NOT NULL) AS has_features,
            MIN(bet_date) AS earliest,
            MAX(bet_date) AS latest
        FROM bet_ledger
        WHERE discord_sent = TRUE
          AND agent_name   = 'HistoricalSeed'
        GROUP BY prop_type
        ORDER BY total_rows DESC
    """)
    seed_rows = cur.fetchall()
except Exception as ex:
    seed_rows = []
    print(f"  seed rows query failed: {ex}")

try:
    cur.execute("""
        SELECT
            prop_type,
            COUNT(*) AS total_rows,
            COUNT(*) FILTER (WHERE actual_outcome IS NOT NULL) AS graded,
            COUNT(*) FILTER (WHERE features_json IS NOT NULL) AS has_features
        FROM bet_ledger
        WHERE discord_sent = TRUE
          AND agent_name  != 'HistoricalSeed'
        GROUP BY prop_type
        ORDER BY total_rows DESC
    """)
    live_rows = cur.fetchall()
except Exception as ex:
    live_rows = []
    print(f"  live rows query failed: {ex}")

print("  Historical seed rows (discord_sent=TRUE, XGBoost can train):")
if not seed_rows:
    print("  No historical seed rows yet.")
else:
    for prop_type, total, graded, has_feat, earliest, latest in seed_rows:
        print(f"    {prop_type:<22} {total:>7,} rows  ({earliest} → {latest})")

print()
print("  Live pick rows (graded real picks):")
if not live_rows:
    print("  No live graded rows yet.")
else:
    for prop_type, total, graded, has_feat in live_rows:
        print(f"    {prop_type:<22} {total:>7,} rows  ({graded} graded, {has_feat} with features)")

# ── 3. Total XGBoost-visible rows ────────────────────────────────
try:
    cur.execute("""
        SELECT COUNT(*) FROM bet_ledger
        WHERE discord_sent = TRUE AND actual_outcome IS NOT NULL
    """)
    xgb_total = cur.fetchone()[0]
except Exception as ex:
    xgb_total = 0
    print(f"  total rows query failed: {ex}")

print(f"\n  Total XGBoost-trainable rows: {xgb_total:,}")

# ── 4. XGBoost readiness assessment ─────────────────────────────
print("\n\n🤖  XGBOOST READINESS\n")
thresholds = [
    (500,    "🔴  Cold start — flat probabilities expected"),
    (5000,   "🟡  Warming up — prop-type base rates learned"),
    (20000,  "🟠  Developing — some player differentiation"),
    (50000,  "🟢  Functional — meaningful probability spread"),
    (100000, "✅  Strong — player archetypes distinguishable"),
    (300000, "🏆  Full — historical context complete"),
]
status = thresholds[0][1]
for threshold, label in thresholds:
    if xgb_total >= threshold:
        status = label

print(f"  {status}")
print(f"  Current rows: {xgb_total:,}")

# Progress to next threshold
for i, (threshold, label) in enumerate(thresholds):
    if xgb_total < threshold:
        prev = thresholds[i-1][0] if i > 0 else 0
        pct  = (xgb_total - prev) / (threshold - prev) * 100
        print(f"  Next milestone: {threshold:,} rows  {bar(pct, 20)} {pct:.0f}%")
        break

# ── 5. Last XGBoost retrain ──────────────────────────────────────
# Real schema: id, model_json, feature_names, trained_at, n_rows, notes
print("\n\n🔁  LAST XGBOOST RETRAIN\n")
try:
    cur.execute("""
        SELECT trained_at, n_rows, notes
        FROM xgb_model_store
        ORDER BY trained_at DESC
        LIMIT 5
    """)
    retrains = cur.fetchall()
    if not retrains:
        print("  No retrains recorded yet — tonight's 2:30 AM will be the first.")
        print("  (xgb_model_store empty — model has only run on Railway filesystem so far)")
    else:
        for trained_at, n_rows, notes in retrains:
            notes_str = f"  [{notes}]" if notes else ""
            print(f"  {str(trained_at)[:16]}  n={n_rows or 0:>7,} rows{notes_str}")
except Exception as ex:
    print(f"  xgb_model_store not available: {ex}")

# ── 6. Brier score trend ─────────────────────────────────────────
# Real schema: id, agent_name, brier_score, n_samples, graded_at
print("\n\n📈  BRIER SCORE TREND (lower = better, <0.23 = target)\n")
try:
    cur.execute("""
        SELECT graded_at, brier_score, n_samples, agent_name
        FROM brier_ledger
        ORDER BY graded_at DESC
        LIMIT 10
    """)
    briers = cur.fetchall()
    if not briers:
        print("  No Brier scores recorded yet (need 30+ graded rows).")
    else:
        for graded_at, score, n, agent in briers:
            bar_str = bar(min(score * 400, 100), 20)  # 0.25 = full bar
            flag = "✅" if score < 0.23 else "🟡" if score < 0.25 else "❌"
            print(f"  {str(graded_at)[:16]}  {score:.4f}  {bar_str}  n={n or 0}  {flag}  [{agent}]")
        print("\n  Target: < 0.23  |  Random = 0.25  |  Launch threshold: < 0.23 sustained")
except Exception as ex:
    print(f"  brier_ledger not available: {ex}")

# ── 7. Daily seed batch log ──────────────────────────────────────
print("\n\n🌱  RECENT SEED ACTIVITY\n")
try:
    cur.execute("""
        SELECT DATE(processed_at) AS day,
               COUNT(*) AS players_processed,
               SUM(inserted) AS rows_added
        FROM seed_progress
        WHERE done = TRUE
        GROUP BY DATE(processed_at)
        ORDER BY day DESC
        LIMIT 7
    """)
    activity = cur.fetchall()
    if not activity:
        print("  No activity yet.")
    else:
        for day, players, rows in activity:
            print(f"  {day}  {players:>4} players  ~{rows or 0:,} rows added")
except Exception as ex:
    print(f"  Activity log unavailable: {ex}")

cur.close()
conn.close()
print()
print("━" * 60)
print("  Run this anytime: python seed_monitor.py")
print("  Seed batches: 7 AM + 7 PM PT daily through May 1")
print("  XGBoost retrains: nightly at 2:30 AM PT")
print("━" * 60)
print()
