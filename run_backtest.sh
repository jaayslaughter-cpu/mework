#!/usr/bin/env bash
# run_backtest.sh
# ---------------
# PropIQ 10-Season Historical Backtest Orchestrator
#
# Usage:
#   ./run_backtest.sh              # Full 2016-2025 run
#   ./run_backtest.sh --dry-run   # First 14 days per season (quick smoke test)
#
# Outputs written to ./backtest_results/
#   summary.json      - Machine-readable aggregate stats
#   report.csv        - Full per-bet ledger
#   agent_pnl.csv     - Per-agent ROI / Sharpe / drawdown
#   season_pnl.csv    - Per-season win rate and ROI
#
# Requirements:
#   Python 3.11+, pip packages in requirements.txt
#   SPORTSDATA_API_KEY set in environment or .env file

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── Load .env if present ──────────────────────────────────────────────────────
if [[ -f .env ]]; then
  echo "Loading .env..."
  set -o allexport
  # shellcheck disable=SC1091
  source .env
  set +o allexport
fi

# ── Validate API key ──────────────────────────────────────────────────────────
: "${SPORTSDATA_API_KEY:=c2abf26f55714d228c7c311290f956d7}"
export SPORTSDATA_API_KEY

# ── Parse optional --dry-run flag ─────────────────────────────────────────────
DRY_RUN_FLAG=""
for arg in "$@"; do
  if [[ "$arg" == "--dry-run" ]]; then
    DRY_RUN_FLAG="--dry-run"
    echo ">>> DRY RUN MODE: first 14 days per season only <<<"
  fi
done

# ── Install dependencies ──────────────────────────────────────────────────────
echo ""
echo "=== Installing Python dependencies ==="
pip install -q -r requirements.txt

# ── Run backtest ──────────────────────────────────────────────────────────────
echo ""
echo "=== Launching PropIQ 10-Season Backtest (2016-2025) ==="
echo "    This will take ~15-30 minutes for the full run."
echo "    Progress is logged to stdout."
echo ""

START=$(date +%s)

python backtest_historical.py \
  --seasons 2016-2025 \
  --output-dir backtest_results \
  $DRY_RUN_FLAG

END=$(date +%s)
ELAPSED=$(( END - START ))

echo ""
echo "=== Backtest complete in ${ELAPSED}s ==="
echo ""

# ── Print file sizes ──────────────────────────────────────────────────────────
echo "Output files:"
ls -lh backtest_results/ 2>/dev/null || echo "(no output directory found)"

# ── Quick summary from JSON ───────────────────────────────────────────────────
if command -v python3 &>/dev/null && [[ -f backtest_results/summary.json ]]; then
  echo ""
  echo "=== Quick Stats from summary.json ==="
  python3 - <<'PYEOF'
import json, pathlib
s = json.loads(pathlib.Path("backtest_results/summary.json").read_text())
print(f"  Total bets       : {s['total_bets']:,}")
print(f"  Overall win rate : {s['overall_win_rate']:.1%}")
print(f"  Total units P&L  : {s['total_units_pnl']:+.2f}")
print(f"  Overall ROI      : {s['overall_roi_pct']:+.2f}%")
print()
print(f"  Top 5 agents by ROI:")
agents = s.get("by_agent", {})
ranked = sorted(agents.items(), key=lambda x: x[1].get("roi_pct", 0), reverse=True)
for agent, stats in ranked[:5]:
    print(f"    {agent:<18}  ROI={stats.get('roi_pct',0):+.2f}%  "
          f"WR={stats.get('win_rate',0):.1%}  "
          f"Sharpe={stats.get('sharpe',0):.2f}  "
          f"MaxDD={stats.get('max_drawdown',0):.2f}u")
PYEOF
fi

echo ""
echo "Full results: backtest_results/"
