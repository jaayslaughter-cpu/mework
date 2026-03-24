"""
backtest/propIQ_backtrader.py
PropIQ Backtesting Engine.
Evaluates model prediction accuracy and agent ROI over a historical date range.
"""
import os
import logging
import argparse
import psycopg2
import pandas as pd

logger = logging.getLogger(__name__)

DB_CONN = {
    "dbname": os.environ.get("POSTGRES_DB", "propiq"),
    "user": os.environ.get("POSTGRES_USER", "propiq_admin"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": 5432,
}


def _query_df(sql: str, params=None) -> pd.DataFrame:
    try:
        conn = psycopg2.connect(**DB_CONN)
        df = pd.read_sql(sql, conn, params=params)
        conn.close()
        return df
    except Exception as e:
        logger.error("[Backtest] DB error: %s", e)
        return pd.DataFrame()


def run_agent_army_backtest(start_date: str, end_date: str) -> dict:
    """
    Evaluates all agent bets over the date range.
    Compares model_projected_over vs actual outcome.
    """
    logger.info("[Backtest] Running backtest %s → %s", start_date, end_date)

    df = _query_df("""
        SELECT bl.agent, bl.bet_date, bl.joint_probability, bl.estimated_odds,
               bl.result, bl.profit_loss
        FROM bets_log bl
        WHERE bl.bet_date BETWEEN %s AND %s
        ORDER BY bl.bet_date ASC;
    """, (start_date, end_date))

    if df.empty:
        logger.warning("[Backtest] No bets found in date range.")
        return {}

    backtest_results = {}
    for agent_name in df["agent"].unique():
        agent_df = df[df["agent"] == agent_name].copy()
        total = len(agent_df)
        wins = len(agent_df[agent_df["result"] == "win"] )
        total_pl = agent_df["profit_loss"].sum() if "profit_loss" in agent_df.columns else 0.0

        backtest_results[agent_name] = {
            "total_bets": total,
            "wins": wins,
            "win_rate": round(wins / total * 100, 2) if total > 0 else 0,
            "total_profit_loss": round(float(total_pl), 2),
            "roi": round(float(total_pl) / total * 100, 2) if total > 0 else 0,
        }
        logger.info("[Backtest] %s: %s/%s wins, ROI=%s%%", agent_name, wins, total, backtest_results[agent_name]['roi'])

    return backtest_results


def run_model_calibration(start_date: str, end_date: str) -> dict:
    """Compares model_projected_over against actual results from live_projections."""
    df = _query_df("""
        SELECT lp.prop_category,
               lp.model_projected_over,
               lp.edge_percentage,
               lp.actual_result
        FROM live_projections lp
        WHERE lp.game_date BETWEEN %s AND %s
          AND lp.actual_result IS NOT NULL;
    """, (start_date, end_date))

    if df.empty:
        return {}

    df["correct"] = (
        ((df["model_projected_over"] >= 50) & (df["actual_result"] == "over")) |
        ((df["model_projected_over"] < 50) & (df["actual_result"] == "under"))
    ).astype(int)

    accuracy = df["correct"].mean() * 100
    playable = df[df["edge_percentage"] >= 3.0]
    playable_acc = playable["correct"].mean() * 100 if len(playable) > 0 else 0.0

    return {
        "total_projections": len(df),
        "overall_accuracy": round(accuracy, 2),
        "playable_props": len(playable),
        "playable_accuracy": round(playable_acc, 2),
        "by_category": df.groupby("prop_category")["correct"].mean().multiply(100).round(2).to_dict(),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PropIQ Backtester")
    parser.add_argument("--start_date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end_date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    print("\n=== AGENT ARMY BACKTEST ===")
    results = run_agent_army_backtest(args.start_date, args.end_date)
    for agent, stats in results.items():
        print(f"\n{agent}:")
        for k, v in stats.items():
            print(f"  {k}: {v}")

    print("\n=== MODEL CALIBRATION ===")
    calibration = run_model_calibration(args.start_date, args.end_date)
    for k, v in calibration.items():
        print(f"  {k}: {v}")
