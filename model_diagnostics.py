"""
model_diagnostics.py
====================
Risk-adjusted performance diagnostics for PropIQ agents.

Computes per-confidence-tier metrics every Monday at 6 AM PT
(after the weekly calibration run) and posts to Discord.

Metrics (per tier and overall):
  - ROI%
  - Sharpe ratio  (annualized, using daily P/L as returns stream)
  - Max drawdown  (largest peak-to-trough equity decline)
  - Calmar ratio  (annualized ROI / max drawdown — risk/reward summary)
  - Win rate, avg EV, avg model_prob

Tiers (by confidence column in bet_ledger):
  HIGH   confidence ≥ 8
  MEDIUM confidence 6–7
  LOW    confidence ≤ 5

All numbers read from bet_ledger grouped by parlay_id (settled rows only).
Output sent to Discord as an embed and written to agent_diagnostics table.

Public API
----------
run_weekly_diagnostics() → dict   (called from orchestrator Monday job)
compute_drawdown(equity_curve) → float
compute_sharpe(daily_returns) → float
"""
from __future__ import annotations

import logging
import math
import os
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_PT = ZoneInfo("America/Los_Angeles")

# Confidence tiers
_TIERS = {
    "HIGH":   (8, 10),   # confidence 8-10
    "MEDIUM": (6, 7),    # confidence 6-7
    "LOW":    (1, 5),    # confidence 1-5
}


def _pg_conn():
    import psycopg2
    url = os.getenv("DATABASE_URL")
    if url:
        return psycopg2.connect(url)
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", 5432)),
        dbname=os.getenv("PGDATABASE", "railway"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", ""),
    )


def compute_drawdown(equity_curve: list[float]) -> float:
    """Compute max peak-to-trough drawdown from an equity curve.

    Returns drawdown as a positive fraction (e.g. 0.15 = 15% drawdown).
    Returns 0.0 if curve is empty or monotonically increasing.
    """
    if not equity_curve:
        return 0.0
    peak = equity_curve[0]
    max_dd = 0.0
    for val in equity_curve:
        if val > peak:
            peak = val
        dd = (peak - val) / peak if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd
    return round(max_dd, 4)


def compute_sharpe(daily_returns: list[float], risk_free: float = 0.0) -> float:
    """Annualised Sharpe ratio from a list of daily return fractions.

    Uses 252 trading days for annualisation.
    Returns 0.0 if fewer than 5 data points or zero std.
    """
    if len(daily_returns) < 5:
        return 0.0
    n = len(daily_returns)
    mean = sum(daily_returns) / n
    excess = [r - risk_free for r in daily_returns]
    variance = sum((r - mean) ** 2 for r in daily_returns) / max(n - 1, 1)
    std = math.sqrt(variance)
    if std < 1e-9:
        return 0.0
    daily_sharpe = (mean - risk_free) / std
    return round(daily_sharpe * math.sqrt(252), 3)




def _brier_by_prop_type(conn, lookback_days: int = 30) -> dict[str, dict]:
    """Return Brier score, win-rate and sample count broken down by prop_type.

    Only includes rows where actual_outcome IS NOT NULL and discord_sent = TRUE.
    """
    result: dict[str, dict] = {}
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    prop_type,
                    COUNT(*)                                             AS n,
                    ROUND(AVG((model_prob - CASE WHEN actual_outcome = 'WIN' THEN 1.0 ELSE 0.0 END)^2)::numeric, 4) AS brier,
                    ROUND(AVG(CASE WHEN actual_outcome = 'WIN' THEN 1.0 ELSE 0.0 END)::numeric, 3)                   AS win_rate
                FROM bet_ledger
                WHERE discord_sent = TRUE
                  AND actual_outcome IS NOT NULL
                  AND bet_date >= CURRENT_DATE - INTERVAL '%s days'
                GROUP BY prop_type
                ORDER BY brier DESC NULLS LAST
                """,
                (lookback_days,),
            )
            for row in cur.fetchall():
                prop_type, n, brier, win_rate = row
                result[prop_type or "unknown"] = {
                    "n":        int(n),
                    "brier":    float(brier or 0),
                    "win_rate": float(win_rate or 0),
                }
    except Exception as exc:
        logger.warning("[ModelDiag] Brier-by-prop query failed: %s", exc)
    return result

def _fetch_settled_parlays(conn, lookback_days: int = 90) -> list[dict]:
    """Fetch settled parlay records from bet_ledger.

    Groups legs by parlay_id. A parlay WINS only if ALL legs win.
    Returns list of dicts: {date, parlay_id, status, stake, payout,
                            confidence, model_prob, ev_pct, agent_name}.
    """
    rows = []
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    bl.parlay_id,
                    bl.bet_date,
                    bl.agent_name,
                    ROUND(AVG(bl.model_prob) * 10, 1) AS avg_conf,
                    AVG(bl.model_prob)   AS avg_prob,
                    AVG(bl.ev_pct)       AS avg_ev,
                    SUM(bl.stake)        AS total_stake,
                    SUM(bl.payout)       AS total_payout,
                    BOOL_AND(bl.status = 'WIN')   AS all_win,
                    BOOL_OR(bl.status = 'LOSS')   AS any_loss,
                    BOOL_AND(bl.status IN ('WIN','LOSS','PUSH')) AS all_settled
                FROM bet_ledger bl
                WHERE bl.discord_sent = TRUE
                  AND bl.bet_date >= CURRENT_DATE - INTERVAL '%s days'
                  AND bl.parlay_id IS NOT NULL
                GROUP BY bl.parlay_id, bl.bet_date, bl.agent_name
                HAVING BOOL_AND(bl.status IN ('WIN','LOSS','PUSH'))
                ORDER BY bl.bet_date
                """,
                (lookback_days,),
            )
            for r in cur.fetchall():
                parlay_id, bet_date, agent, conf, prob, ev, stake, payout, all_win, any_loss, _ = r
                status = "WIN" if all_win else ("LOSS" if any_loss else "PUSH")
                rows.append({
                    "parlay_id": parlay_id,
                    "date":      bet_date,
                    "agent":     agent or "unknown",
                    "confidence": float(conf or 0),
                    "model_prob": float(prob or 0),
                    "ev_pct":    float(ev or 0),
                    "stake":     float(stake or 10),
                    "payout":    float(payout or 0),
                    "status":    status,
                })
    except Exception as exc:
        logger.warning("[ModelDiag] Parlay fetch failed: %s", exc)
    return rows


def _compute_tier_metrics(parlays: list[dict], conf_min: int, conf_max: int) -> dict:
    """Compute metrics for parlays within a confidence tier."""
    subset = [p for p in parlays if conf_min <= p["confidence"] <= conf_max]
    if not subset:
        return {"n": 0}

    wins   = [p for p in subset if p["status"] == "WIN"]
    losses = [p for p in subset if p["status"] == "LOSS"]
    pushes = [p for p in subset if p["status"] == "PUSH"]

    total_staked = sum(p["stake"]  for p in subset if p["status"] != "PUSH")
    total_payout = sum(p["payout"] for p in wins)

    roi = (total_payout - total_staked) / total_staked * 100 if total_staked > 0 else 0.0

    # Build daily equity curve (by date)
    by_date: dict = {}
    for p in subset:
        d = str(p["date"])
        if d not in by_date:
            by_date[d] = 0.0
        if p["status"] == "WIN":
            by_date[d] += p["payout"] - p["stake"]
        elif p["status"] == "LOSS":
            by_date[d] -= p["stake"]

    sorted_dates = sorted(by_date.keys())
    daily_pnl    = [by_date[d] for d in sorted_dates]

    # Equity curve (cumulative, start at 0)
    equity = []
    running = 0.0
    for pnl in daily_pnl:
        running += pnl
        equity.append(running)

    max_dd = compute_drawdown([100 + e for e in equity])  # base 100
    sharpe = compute_sharpe([p / 10.0 for p in daily_pnl])  # fractional returns vs $10 stake
    calmar = abs(roi / (max_dd * 100)) if max_dd > 0 else float("inf")

    return {
        "n":            len(subset),
        "wins":         len(wins),
        "losses":       len(losses),
        "pushes":       len(pushes),
        "win_rate":     round(len(wins) / max(len(wins) + len(losses), 1) * 100, 1),
        "roi_pct":      round(roi, 2),
        "sharpe":       sharpe,
        "max_drawdown": round(max_dd * 100, 2),
        "calmar":       round(min(calmar, 999), 2),
        "avg_ev":       round(sum(p["ev_pct"] for p in subset) / len(subset), 2),
        "avg_prob":     round(sum(p["model_prob"] for p in subset) / len(subset), 1),
    }


def _write_to_diagnostics(conn, metrics: dict, run_date: "date") -> None:
    """Persist diagnostic snapshot to agent_diagnostics table."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO agent_diagnostics
                    (agent_name, metric_date, metric_json, created_at)
                VALUES (%s, %s, %s::jsonb, NOW())
                ON CONFLICT (agent_name, metric_date) DO UPDATE
                    SET metric_json = EXCLUDED.metric_json,
                        created_at  = NOW()
                """,
                ("model_diagnostics_weekly", run_date, __import__("json").dumps(metrics)),
            )
        conn.commit()
    except Exception as exc:
        logger.debug("[ModelDiag] Write to agent_diagnostics failed: %s", exc)
        conn.rollback()


def _discord_embed(metrics: dict, brier_by_prop: dict | None = None) -> None:
    """Post risk-adjusted metrics to Discord."""
    try:
        from DiscordAlertService import discord_alert  # noqa: PLC0415
    except Exception:
        logger.debug("[ModelDiag] DiscordAlertService not available")
        return

    lines = ["**📊 Weekly Risk-Adjusted Diagnostics**\n"]
    for tier_name, tier_data in metrics.get("tiers", {}).items():
        n = tier_data.get("n", 0)
        if n == 0:
            continue
        lines.append(
            f"**{tier_name}** (n={n})  "
            f"ROI: {tier_data.get('roi_pct', 0):+.1f}%  "
            f"Sharpe: {tier_data.get('sharpe', 0):.2f}  "
            f"MaxDD: {tier_data.get('max_drawdown', 0):.1f}%  "
            f"Calmar: {tier_data.get('calmar', 0):.2f}  "
            f"WR: {tier_data.get('win_rate', 0):.0f}%"
        )
    overall = metrics.get("overall", {})
    if overall.get("n", 0):
        lines.append(
            f"\n**OVERALL** (n={overall['n']})  "
            f"ROI: {overall.get('roi_pct', 0):+.1f}%  "
            f"Sharpe: {overall.get('sharpe', 0):.2f}  "
            f"MaxDD: {overall.get('max_drawdown', 0):.1f}%  "
            f"Calmar: {overall.get('calmar', 0):.2f}"
        )

    # Brier breakdown by prop type
    if brier_by_prop:
        _bp_sorted = sorted(brier_by_prop.items(), key=lambda x: -x[1]["brier"])[:8]
        if _bp_sorted:
            lines.append("\n**📉 Brier by Prop Type (30d)**")
            for _pt, _bv in _bp_sorted:
                _flag = "❌" if _bv["brier"] >= 0.25 else ("⚠️" if _bv["brier"] >= 0.22 else "✅")
                lines.append(f"{_flag} `{_pt}`: Brier {_bv['brier']:.4f} WR {_bv['win_rate']:.1%} n={_bv['n']}")

    msg = "\n".join(lines)
    try:
        discord_alert.send_embed(
            title="Weekly Model Diagnostics",
            description=msg,
            color=0x3498DB,
        )
    except Exception as exc:
        logger.debug("[ModelDiag] Discord send failed: %s", exc)


def run_weekly_diagnostics(lookback_days: int = 90) -> dict:
    """Compute and post weekly risk-adjusted diagnostics.

    Called from orchestrator Monday 6 AM PT job, after calibration.

    Returns:
        dict with keys: tiers (HIGH/MEDIUM/LOW), overall, run_at.
    """
    logger.info("[ModelDiag] Running weekly diagnostics (lookback=%d days)...", lookback_days)

    try:
        conn = _pg_conn()
    except Exception as exc:
        logger.warning("[ModelDiag] DB connect failed: %s", exc)
        return {}

    try:
        brier_by_prop = _brier_by_prop_type(conn)
        parlays = _fetch_settled_parlays(conn, lookback_days)
        if not parlays:
            logger.info("[ModelDiag] No settled parlays in last %d days — skipping", lookback_days)
            return {}

        metrics: dict = {"tiers": {}, "overall": {}}
        for tier_name, (cmin, cmax) in _TIERS.items():
            metrics["tiers"][tier_name] = _compute_tier_metrics(parlays, cmin, cmax)

        metrics["overall"] = _compute_tier_metrics(parlays, 0, 100)
        metrics["run_at"]  = datetime.now(_PT).isoformat()
        metrics["n_total"] = len(parlays)

        # Persist
        from datetime import date as _date
        _write_to_diagnostics(conn, metrics, datetime.now(_PT).date())

        # Discord embed
        _discord_embed(metrics, brier_by_prop=brier_by_prop)

        logger.info(
            "[ModelDiag] Done: %d parlays analysed — "
            "Overall ROI=%.1f%% Sharpe=%.2f MaxDD=%.1f%%",
            len(parlays),
            metrics["overall"].get("roi_pct", 0),
            metrics["overall"].get("sharpe", 0),
            metrics["overall"].get("max_drawdown", 0),
        )
        return metrics

    finally:
        try:
            conn.close()
        except Exception:
            pass
