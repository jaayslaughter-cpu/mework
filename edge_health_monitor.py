"""
edge_health_monitor.py — PropIQ Edge Health & Model Drift Detection
====================================================================
Monitors per-agent "edge health" on a rolling basis:
  - 30-day CLV (closing line value) — are we beating closing lines?
  - 30-day ROI — are we making money after juice?
  - 30-day win rate vs backtest expectation — statistical drift detection
  - Z-score alert when agent statistically underperforms its backtest baseline

Discord alert format:
  🏥 Edge Health Report — 30-Day Window
    EVHunter   | CLV: +0.021 | ROI: +8.4% | W-L: 23-14 | Z: +1.2 ✅
    FadeAgent  | CLV: -0.041 | ROI: -18% | W-L: 8-19  | Z: -2.8 🚨

Called by nightly_recap.py after settlement.

Usage:
    python3 edge_health_monitor.py           # Post report to Discord
    python3 edge_health_monitor.py --quiet   # Return metrics only
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
from collections import defaultdict
from datetime import date, timedelta

import psycopg2
import requests
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s [EDGE-HEALTH] %(message)s")
logger = logging.getLogger(__name__)

_DB_URL = os.environ.get("DATABASE_URL", "")
_DISCORD = os.environ.get("DISCORD_WEBHOOK_URL", "")
_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "agent_config.yaml")

# Backtest baseline win rates per agent (from Phase 26 backtest results)
# These are the expected win rates from historical validation.
# Update these after each off-season re-tuning.
BACKTEST_BASELINES: dict[str, float] = {
    "EVHunter": 0.587,
    "UnderMachine": 0.571,
    "F5Agent": 0.563,
    "MLEdgeAgent": 0.601,
    "UmpireAgent": 0.558,
    "FadeAgent": 0.544,
    "LineValueAgent": 0.574,
    "BullpenAgent": 0.556,
    "WeatherAgent": 0.561,
    "SteamAgent": 0.578,
    "ArsenalAgent": 0.562,
    "PlatoonAgent": 0.547,
    "CatcherAgent": 0.559,
    "LineupAgent": 0.551,
    "GetawayAgent": 0.553,
    "ArbitrageAgent": 0.620,
    "VultureStack": 0.571,
    "OmegaStack": 0.652,
    "StreakAgent": 0.803,
}

# Z-score thresholds
Z_ALERT_THRESHOLD = -2.0     # Flag if significantly underperforming
Z_CONCERN_THRESHOLD = -1.5   # Warn if trending negative


def _load_config() -> dict:
    try:
        with open(_CONFIG_PATH) as f:
            return yaml.safe_load(f)
    except Exception:
        return {}


def _get_conn():
    return psycopg2.connect(_DB_URL)


# ---------------------------------------------------------------------------
# Fetch rolling data
# ---------------------------------------------------------------------------
def fetch_rolling_bets(days: int = 30) -> list[dict]:
    since = (date.today() - timedelta(days=days)).isoformat()
    try:
        with _get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT agent_name, status, stake, payout, confidence, legs_json, created_at
                FROM propiq_season_record
                WHERE status IN ('WIN', 'LOSS', 'PUSH')
                  AND settled_at IS NOT NULL
                  AND created_at::date >= %s
                ORDER BY created_at
            """, (since,))
            cols = ["agent_name", "status", "stake", "payout", "confidence", "legs_json", "created_at"]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as exc:
        logger.error("DB fetch failed: %s", exc)
        return []


def fetch_clv_data(days: int = 30) -> list[dict]:
    """Get closing line value data from line_snapshots."""
    since = (date.today() - timedelta(days=days)).isoformat()
    try:
        with _get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT player_name, prop_type, game_date,
                       MIN(line) AS opening_line,
                       MAX(CASE WHEN is_closing_line THEN line END) AS closing_line
                FROM line_snapshots
                WHERE game_date >= %s
                GROUP BY player_name, prop_type, game_date
                HAVING COUNT(CASE WHEN is_closing_line THEN 1 END) > 0
            """, (since,))
            cols = ["player_name", "prop_type", "game_date", "opening_line", "closing_line"]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as exc:
        logger.warning("CLV fetch failed: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Compute per-agent metrics
# ---------------------------------------------------------------------------
def compute_agent_metrics(bets: list[dict]) -> dict[str, dict]:
    data: dict[str, dict] = defaultdict(lambda: {
        "wins": 0, "losses": 0, "pushes": 0,
        "stake_total": 0.0, "payout_total": 0.0,
    })
    for bet in bets:
        agent = bet["agent_name"]
        status = bet["status"]
        stake = float(bet.get("stake") or 0)
        payout = float(bet.get("payout") or 0)
        if status == "WIN":
            data[agent]["wins"] += 1
            data[agent]["stake_total"] += stake
            data[agent]["payout_total"] += payout
        elif status == "LOSS":
            data[agent]["losses"] += 1
            data[agent]["stake_total"] += stake
        elif status == "PUSH":
            data[agent]["pushes"] += 1
            data[agent]["stake_total"] += stake
            data[agent]["payout_total"] += stake  # refund

    results = {}
    for agent, d in data.items():
        total = d["wins"] + d["losses"]
        if total == 0:
            continue
        win_rate = d["wins"] / total
        roi = (d["payout_total"] - d["stake_total"]) / d["stake_total"] if d["stake_total"] > 0 else 0.0

        # Z-score vs backtest baseline
        baseline = BACKTEST_BASELINES.get(agent, 0.55)
        se = math.sqrt(baseline * (1 - baseline) / total) if total > 0 else 1.0
        z_score = (win_rate - baseline) / se if se > 0 else 0.0

        results[agent] = {
            "wins": d["wins"],
            "losses": d["losses"],
            "pushes": d["pushes"],
            "win_rate": round(win_rate, 4),
            "roi_30d": round(roi, 4),
            "baseline_win_rate": baseline,
            "z_score": round(z_score, 3),
            "stake_total": round(d["stake_total"], 2),
            "payout_total": round(d["payout_total"], 2),
            "profit_loss": round(d["payout_total"] - d["stake_total"], 2),
        }
    return results


def compute_agent_clv(bets: list[dict], clv_data: list[dict]) -> dict[str, float]:
    """
    Simple CLV: for each settled bet, check if our opening line
    was better than the closing line. Positive CLV = we beat the market.
    """
    # Build closing line lookup: (player_name, prop_type, date) -> clv
    clv_lookup: dict[tuple, float] = {}
    for c in clv_data:
        if c["opening_line"] and c["closing_line"]:
            clv = float(c["closing_line"]) - float(c["opening_line"])
            key = (c["player_name"], c["prop_type"], str(c["game_date"]))
            clv_lookup[key] = clv

    agent_clv: dict[str, list[float]] = defaultdict(list)
    for bet in bets:
        try:
            legs = json.loads(bet["legs_json"]) if isinstance(bet["legs_json"], str) else bet["legs_json"]
            for leg in (legs or []):
                player = leg.get("player_name", leg.get("player", ""))
                prop = leg.get("prop_type", leg.get("stat", ""))
                game_date = str(bet["created_at"])[:10] if bet["created_at"] else ""
                key = (player, prop, game_date)
                if key in clv_lookup:
                    agent_clv[bet["agent_name"]].append(clv_lookup[key])
        except Exception:
            continue

    return {
        agent: round(sum(vals) / len(vals), 4)
        for agent, vals in agent_clv.items() if vals
    }


# ---------------------------------------------------------------------------
# Persist metrics to agent_metrics
# ---------------------------------------------------------------------------
def store_edge_metrics(metrics: dict[str, dict], clv: dict[str, float], config_version: str, days: int) -> None:
    try:
        with _get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS agent_metrics (
                    id SERIAL PRIMARY KEY,
                    agent_name TEXT NOT NULL,
                    metric_date DATE NOT NULL DEFAULT CURRENT_DATE,
                    window_days INT NOT NULL,
                    config_version TEXT,
                    brier_score FLOAT,
                    ece FLOAT,
                    actual_win_rate FLOAT,
                    n_bets INT,
                    n_legs INT,
                    reliability_curve JSONB,
                    roi_30d FLOAT,
                    clv_30d FLOAT,
                    z_score FLOAT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            for agent, m in metrics.items():
                cur.execute("""
                    INSERT INTO agent_metrics
                        (agent_name, window_days, config_version, actual_win_rate,
                         n_bets, roi_30d, clv_30d, z_score)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    agent, days, config_version,
                    m["win_rate"], m["wins"] + m["losses"],
                    m["roi_30d"], clv.get(agent, 0.0), m["z_score"],
                ))
            conn.commit()
        logger.info("Stored edge metrics for %d agents", len(metrics))
    except Exception as exc:
        logger.error("Failed to store edge metrics: %s", exc)


# ---------------------------------------------------------------------------
# Discord report
# ---------------------------------------------------------------------------
    f"{'Agent':<18} {'CLV':>7} {'ROI':>8} {'W-L':>8} {'Z-Score':>9} {'Status':>7}",
    "-" * 64,
]
flagged = []
combined: dict[str, dict] = {}
for agent, m in sorted(metrics.items(), key=lambda x: x[1]["z_score"]):
    agent_clv = clv.get(agent, 0.0)
    z = m["z_score"]
    if z <= Z_ALERT_THRESHOLD:
        status = "🚨 ALERT"
        flagged.append((agent, m, agent_clv))
    elif z <= Z_CONCERN_THRESHOLD:
        status = "⚠️  WARN"
    elif z >= 1.5:
        status = "🔥 HOT "
    else:
        status = "✅ OK  "
    wl = f"{m['wins']}-{m['losses']}"
    lines.append(
        f"{agent:<18} {agent_clv:>+7.3f} {m['roi_30d']:>+7.1%} {wl:>8} {z:>+9.2f} {status}"
    )
    combined[agent] = {**m, "clv_30d": agent_clv}

lines.append("""

# ---------------------------------------------------------------------------
# Main
""")
# ---------------------------------------------------------------------------
def run(days: int = 30, quiet: bool = False) -> dict[str, dict]:
    cfg = _load_config()
    config_version = cfg.get("version", "unknown")

    logger.info("Running edge health monitor — %d-day window", days)
    bets = fetch_rolling_bets(days=days)
    clv_data = fetch_clv_data(days=days)

    if not bets:
        logger.warning("No settled bets found in %d-day window", days)
        return {}

    metrics = compute_agent_metrics(bets)
    clv = compute_agent_clv(bets, clv_data)
    store_edge_metrics(metrics, clv, config_version, days)

    if not quiet:
        combined = _post_edge_report(metrics, clv, days)
    else:
        combined = {agent: {**m, "clv_30d": clv.get(agent, 0.0)} for agent, m in metrics.items()}

    return combined


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PropIQ Edge Health Monitor")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()
    run(days=args.days, quiet=args.quiet)
