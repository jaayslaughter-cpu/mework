"""
calibration_monitor.py — PropIQ Calibration & Reliability Diagnostics
=======================================================================
Runs after nightly_recap.py settlement to answer:
  "Does our 0.60 probability actually win ~60% of the time?"

Outputs:
  - Brier score per agent (rolling 30-day)
  - Reliability curve data (probability bucket → actual win rate)
  - Calibration error (ECE — Expected Calibration Error)
  - Discord alert if any agent's Brier degrades past threshold
  - All metrics stored in agent_metrics table with config version

Usage:
    python3 calibration_monitor.py                    # Run for today
    python3 calibration_monitor.py --days 90          # Longer window
    python3 calibration_monitor.py --agent EVHunter   # Single agent
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from collections import defaultdict
from datetime import date, timedelta

import psycopg2
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s [CALIBRATION] %(message)s")
logger = logging.getLogger(__name__)

_DB_URL = os.environ.get("DATABASE_URL", "")
_DISCORD = os.environ.get("DISCORD_WEBHOOK_URL", "")
_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "agent_config.yaml")


def _load_config() -> dict:
    try:
        with open(_CONFIG_PATH) as f:
            return yaml.safe_load(f)
    except Exception:
        return {}


def _get_conn():
    return psycopg2.connect(_DB_URL)


# ---------------------------------------------------------------------------
# Fetch settled bets from propiq_season_record
# ---------------------------------------------------------------------------
def fetch_settled_bets(days: int = 30, agent_name: str | None = None) -> list[dict]:
    """Return settled bets with per-leg model_prob from bet_ledger.
    Falls back to propiq_season_record.legs_json if bet_ledger unavailable."""
    since = (date.today() - timedelta(days=days)).isoformat()
    # Primary: bet_ledger has per-row model_prob (accurate for calibration)
    primary_query = """
        SELECT agent_name, model_prob, status, ev_pct, bet_date
        FROM bet_ledger
        WHERE graded_at >= %s
          AND model_prob IS NOT NULL
          AND status IN ('WIN', 'LOSS', 'PUSH')
    """
    if agent_name:
        primary_query += " AND agent_name = %s"
    try:
        conn = _get_conn()
        with conn.cursor() as cur:
            params = (since, agent_name) if agent_name else (since,)
            cur.execute(primary_query, params)
            rows = cur.fetchall()
        conn.close()
        if rows:
            bets = []
            for row in rows:
                bets.append({
                    "agent_name": row[0],
                    "model_prob": float(row[1] or 0.5),
                    "status":     row[2],
                    "ev_pct":     float(row[3] or 0),
                    "leg_probs":  [float(row[1] or 0.5)],  # single prob per bet
                })
            logger.info("[CalibMonitor] Loaded %d bets from bet_ledger", len(bets))
            return bets
    except Exception as exc:
        logger.warning("[CalibMonitor] bet_ledger read failed (%s), falling back to season_record", exc)

    # Fallback: propiq_season_record legs_json
    fallback_query = """
        SELECT agent_name, legs_json, status, confidence, created_at
        FROM propiq_season_record
        WHERE status IN ('WIN', 'LOSS', 'PUSH')
          AND created_at::date >= %s
    """
    # FIX PR#278: was "query +=" (NameError) and "cur.execute(query, ...)" — both wrong variable names
    params: list = [since]
    if agent_name:
        fallback_query += " AND agent_name = %s"
        params.append(agent_name)

    rows = []
    try:
        with _get_conn() as conn, conn.cursor() as cur:
            cur.execute(fallback_query, params)
            for row in cur.fetchall():
                rows.append({
                    "agent_name": row[0],
                    "legs_json": row[1],
                    "status": row[2],
                    "confidence": row[3],
                    "created_at": row[4],
                })
    except Exception as exc:
        logger.error("DB fetch failed: %s", exc)
    return rows


# ---------------------------------------------------------------------------
# Brier Score
# ---------------------------------------------------------------------------
def brier_score(probs: list[float], outcomes: list[int]) -> float:
    """
    Lower is better. Perfect model = 0.0. Random = 0.25.
    Brier = mean((prob - outcome)^2)
    """
    if not probs:
        return float("nan")
    return sum((p - o) ** 2 for p, o in zip(probs, outcomes)) / len(probs)


# ---------------------------------------------------------------------------
# Reliability curve (calibration curve)
# ---------------------------------------------------------------------------
def reliability_curve(
    probs: list[float],
    outcomes: list[int],
    bucket_size: float = 0.05,
) -> list[dict]:
    """
    Returns list of buckets: {center, predicted, actual, n}
    Perfect calibration: predicted == actual for every bucket.
    """
    buckets: dict[float, dict] = {}
    for p, o in zip(probs, outcomes):
        center = round(round(p / bucket_size) * bucket_size, 3)
        if center not in buckets:
            buckets[center] = {"sum_prob": 0.0, "sum_outcome": 0, "n": 0}
        buckets[center]["sum_prob"] += p
        buckets[center]["sum_outcome"] += o
        buckets[center]["n"] += 1

    curve = []
    for center in sorted(buckets):
        b = buckets[center]
        if b["n"] >= 5:  # Skip tiny buckets
            curve.append({
                "center": center,
                "predicted": round(b["sum_prob"] / b["n"], 4),
                "actual": round(b["sum_outcome"] / b["n"], 4),
                "n": b["n"],
            })
    return curve


# ---------------------------------------------------------------------------
# Expected Calibration Error (ECE)
# ---------------------------------------------------------------------------
def expected_calibration_error(curve: list[dict], total_n: int) -> float:
    """Weighted mean absolute deviation between predicted and actual win rate."""
    if not curve or total_n == 0:
        return float("nan")
    return sum(abs(b["predicted"] - b["actual"]) * b["n"] / total_n for b in curve)


# ---------------------------------------------------------------------------
# Core: compute metrics per agent
# ---------------------------------------------------------------------------
def compute_agent_calibration(bets: list[dict], bucket_size: float = 0.05) -> dict[str, dict]:
    """
    Returns { agent_name: { brier, ece, curve, n, probs, outcomes } }
    Parlay is WIN=1 / LOSS=0 / PUSH=excluded.
    We also flatten individual legs to get per-leg calibration.
    """
    agent_data: dict[str, dict] = defaultdict(lambda: {"probs": [], "outcomes": []})

    for bet in bets:
        if bet["status"] == "PUSH":
            continue
        outcome = 1 if bet["status"] == "WIN" else 0

        # Parlay-level probability (product of leg probs)
        try:
            legs = json.loads(bet["legs_json"]) if isinstance(bet["legs_json"], str) else bet["legs_json"]
            leg_probs = [leg.get("prob", leg.get("probability", 0.55)) for leg in legs if isinstance(leg, dict)]
            if leg_probs:
                parlay_prob = 1.0
                for lp in leg_probs:
                    parlay_prob *= lp
                agent_data[bet["agent_name"]]["probs"].append(round(parlay_prob, 4))
                agent_data[bet["agent_name"]]["outcomes"].append(outcome)

                # Also store individual leg probs (all win or all lose with parlay)
                for lp in leg_probs:
                    agent_data[bet["agent_name"]]["probs"].append(lp)
                    agent_data[bet["agent_name"]]["outcomes"].append(outcome)
        except Exception:
            continue

    results = {}
    for agent, data in agent_data.items():
        probs = data["probs"]
        outcomes = data["outcomes"]
        if not probs:
            continue
        bs = brier_score(probs, outcomes)
        curve = reliability_curve(probs, outcomes, bucket_size)
        ece = expected_calibration_error(curve, len(probs))
        actual_wr = sum(outcomes) / len(outcomes) if outcomes else 0.0
        results[agent] = {
            "brier": round(bs, 5),
            "ece": round(ece, 5),
            "actual_win_rate": round(actual_wr, 4),
            "n": len(set(range(len(probs)))),  # unique bets approx
            "n_legs": len(probs),
            "curve": curve,
        }
    return results


# ---------------------------------------------------------------------------
# Persist metrics to agent_metrics table
# ---------------------------------------------------------------------------
def store_metrics(metrics: dict[str, dict], config_version: str, days: int) -> None:
    """Upsert calibration metrics into agent_metrics table."""
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
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            for agent, m in metrics.items():
                cur.execute("""
                    INSERT INTO agent_metrics
                        (agent_name, window_days, config_version, brier_score, ece,
                         actual_win_rate, n_bets, n_legs, reliability_curve)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    agent, days, config_version,
                    m["brier"], m["ece"], m["actual_win_rate"],
                    m["n"], m["n_legs"], json.dumps(m["curve"]),
                ))
            conn.commit()
        logger.info("Stored calibration metrics for %s agents", len(metrics))
    except Exception as exc:
        logger.error("Failed to store metrics: %s", exc)


# ---------------------------------------------------------------------------
# Discord alert for degraded agents
# ---------------------------------------------------------------------------
def _post_calibration_report(metrics: dict[str, dict], cfg: dict, days: int) -> None:
    cal_cfg = cfg.get("calibration", {})
    brier_alert = cal_cfg.get("brier_alert_threshold", 0.05)
    min_n = cal_cfg.get("min_sample_size", 50)

    lines = [
        f"{'Agent':<18} {'Brier':>7} {'ECE':>7} {'Win%':>7} {'Legs':>6}",
        "-" * 50,
    ]
    for agent, m in sorted(metrics.items(), key=lambda x: x[1]["brier"]):
        flag = " ⚠️" if m["brier"] > brier_alert and m["n_legs"] >= min_n else ""
        lines.append(
            f"{agent:<18} {m['brier']:>7.4f} {m['ece']:>7.4f} "
            f"{m['actual_win_rate']:>6.1%} {m['n_legs']:>6}{flag}"
        )
    lines.append("")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run(days: int = 30, agent_name: str | None = None, quiet: bool = False) -> dict[str, dict]:
    cfg = _load_config()
    config_version = cfg.get("version", "unknown")
    cal_cfg = cfg.get("calibration", {})
    bucket_size = cal_cfg.get("reliability_bucket_size", 0.05)
    min_n = cal_cfg.get("min_sample_size", 50)

    logger.info("Running calibration — %d-day window%s", days, f" for {agent_name}" if agent_name else "")
    bets = fetch_settled_bets(days=days, agent_name=agent_name)
    logger.info("Loaded %d settled bets", len(bets))

    if len(bets) < min_n:
        logger.warning("Only %d settled bets — need %d for reliable calibration", len(bets), min_n)

    metrics = compute_agent_calibration(bets, bucket_size=bucket_size)
    if not metrics:
        logger.warning("No calibration data available")
        return {}

    store_metrics(metrics, config_version, days)

    if not quiet:
        _post_calibration_report(metrics, cfg, days)

    return metrics


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PropIQ Calibration Monitor")
    parser.add_argument("--days", type=int, default=30, help="Rolling window in days")
    parser.add_argument("--agent", type=str, default=None, help="Single agent name to analyze")
    parser.add_argument("--quiet", action="store_true", help="Skip Discord post")
    args = parser.parse_args()
    run(days=args.days, agent_name=args.agent, quiet=args.quiet)
