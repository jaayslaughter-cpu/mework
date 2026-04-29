"""
scripts/verify_seed.py
======================
Verifies the historical seed is properly loaded and XGBoost can train on it.

Usage:
    python scripts/verify_seed.py

Checks:
  1. Total rows by agent/platform
  2. Rows with valid features_json (XGBoost-ready)
  3. Win rates by prop type (should be ~50% for neutral lines)
  4. Whether retraining would pass the 200-row minimum
  5. Model file existence and age
"""
from __future__ import annotations
import json
import os
import sys
from datetime import datetime

import psycopg2

DB_URL = os.getenv("DATABASE_URL", "")
if not DB_URL:
    print("ERROR: DATABASE_URL not set")
    sys.exit(1)

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

print("\n" + "="*60)
print("HISTORICAL SEED VERIFICATION")
print("="*60)

# 1. Row counts by agent
print("\n── Row counts by agent ──")
cur.execute("""
    SELECT agent_name, platform, COUNT(*) as n,
           SUM(CASE WHEN actual_outcome IS NOT NULL THEN 1 ELSE 0 END) as graded,
           SUM(CASE WHEN features_json IS NOT NULL THEN 1 ELSE 0 END) as has_features,
           SUM(CASE WHEN features_json IS NOT NULL
                     AND features_json NOT LIKE '{\"backfilled\"%'
                     AND features_json != 'null'
                     THEN 1 ELSE 0 END) as xgb_ready
    FROM bet_ledger
    GROUP BY agent_name, platform
    ORDER BY n DESC
    LIMIT 20
""")
print(f"{'Agent':<25} {'Platform':<12} {'Total':>8} {'Graded':>8} {'HasFeat':>8} {'XGBRdy':>8}")
print("-"*75)
for row in cur.fetchall():
    print(f"{row[0]:<25} {row[1]:<12} {row[2]:>8,} {row[3]:>8,} {row[4]:>8,} {row[5]:>8,}")

# 2. XGBoost training data readiness
print("\n── XGBoost training readiness ──")
cur.execute("""
    SELECT
        COUNT(*) as total_eligible,
        COUNT(CASE WHEN features_json IS NOT NULL
                    AND features_json NOT LIKE '{\"backfilled\"%' THEN 1 END) as real_features,
        COUNT(CASE WHEN features_json IS NULL THEN 1 END) as null_features,
        COUNT(CASE WHEN features_json LIKE '{\"backfilled\"%' THEN 1 END) as placeholder_dicts
    FROM bet_ledger
    WHERE actual_outcome IS NOT NULL
      AND discord_sent = TRUE
      AND (lookahead_safe IS NULL OR lookahead_safe = TRUE)
""")
row = cur.fetchone()
print(f"  Total eligible rows:      {row[0]:>8,}")
print(f"  With real features_json:  {row[1]:>8,}  ← XGBoost trains on these")
print(f"  With NULL features_json:  {row[2]:>8,}  ← Uses neutral [0.5]*27 defaults")
print(f"  With placeholder dicts:   {row[3]:>8,}  ← Also uses neutral defaults (V40 should fix these)")
print(f"  {'✅ READY' if (row[1] + row[2]) >= 200 else '❌ INSUFFICIENT (<200)'} for XGBoost training")

# 3. Win rates by prop type
print("\n── Win rates by prop type (should be ~45-55% for balanced model) ──")
cur.execute("""
    SELECT prop_type,
           COUNT(*) as n,
           ROUND(AVG(actual_outcome::float) * 100, 1) as win_rate_pct
    FROM bet_ledger
    WHERE actual_outcome IS NOT NULL AND discord_sent = TRUE
    GROUP BY prop_type
    ORDER BY n DESC
    LIMIT 15
""")
print(f"{'Prop Type':<25} {'N':>8} {'Win%':>8} {'Status'}")
print("-"*55)
for row in cur.fetchall():
    ptype, n, wr = row
    if wr is None: continue
    flag = "⚠️  SKEWED" if wr < 40 or wr > 60 else "✅"
    print(f"{ptype:<25} {n:>8,} {wr:>7.1f}% {flag}")

# 4. Model file check
print("\n── Model file status ──")
model_path = os.getenv("XGB_MODEL_PATH", "/app/api/models/prop_model_v1.json")
cur.execute("SELECT trained_at, n_rows, notes FROM xgb_model_store ORDER BY trained_at DESC LIMIT 1")
db_model = cur.fetchone()
if db_model:
    age = (datetime.utcnow() - db_model[0].replace(tzinfo=None)).days if db_model[0] else "?"
    print(f"  DB model: trained {age}d ago | {db_model[1]} rows | {db_model[2]}")
else:
    print("  ❌ No model in xgb_model_store — XGBoost has never trained successfully")

# 5. Recent dispatch activity
print("\n── Recent live dispatch picks (last 14 days) ──")
cur.execute("""
    SELECT agent_name, prop_type, COUNT(*) as n,
           ROUND(AVG(actual_outcome::float)*100,1) as win_rate
    FROM bet_ledger
    WHERE discord_sent = TRUE
      AND agent_name != 'HistoricalSeed'
      AND bet_date >= CURRENT_DATE - 14
      AND actual_outcome IS NOT NULL
    GROUP BY agent_name, prop_type
    ORDER BY n DESC
    LIMIT 15
""")
rows = cur.fetchall()
if rows:
    print(f"{'Agent':<25} {'PropType':<20} {'N':>5} {'Win%':>7}")
    print("-"*60)
    for row in rows:
        print(f"{row[0]:<25} {row[1]:<20} {row[2]:>5} {row[3]:>6.1f}%")
else:
    print("  No graded live picks yet")

cur.close()
conn.close()

print("\n" + "="*60)
print("Run 'python historical_seed.py' to populate if counts are low.")
print("Run 'python historical_seed.py --dry-run' to preview row counts.")
print("="*60 + "\n")
