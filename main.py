"""
main.py
PropIQ Daily Pipeline — runs every morning to generate today's Agent Army tickets.
"""
import logging
import os
from datetime import datetime, timedelta

from etl.odds_pipeline import run_odds_etl
from etl.weather_ump import update_weather_ump
from agents.agent_2leg import generate_ticket as agent2_ticket
from agents.agent_3leg import generate_ticket as agent3_ticket
from agents.agent_best import generate_ticket as agentbest_ticket
from agents.agent_5leg import generate_ticket as agent5_ticket
from backtest.propIQ_backtrader import run_agent_army_backtest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def run_daily_pipeline():
    today = datetime.today().strftime("%Y-%m-%d")
    yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    logger.info("=== PropIQ Daily Pipeline — %s ===", today)

    # 1. ETL: Fetch odds and weather/ump data
    logger.info("[Pipeline] Step 1: Running ETL...")
    markets_count = run_odds_etl(today)
    update_weather_ump(today)
    logger.info("[Pipeline] ETL complete: %s markets loaded", markets_count)

    # 2. Generate projections via ML Engine API
    logger.info("[Pipeline] Step 2: Fetching live projections from ML Engine...")
    projections = _fetch_projections()
    logger.info("[Pipeline] %s projections fetched", len(projections))

    # 3. Run Agent Army
    logger.info("[Pipeline] Step 3: Generating Agent Army tickets...")
    tickets = {}
    tickets["Agent_2Leg"] = agent2_ticket(today, projections)
    tickets["Agent_3Leg"] = agent3_ticket(today, projections)
    tickets["Agent_Best"] = agentbest_ticket(today, projections)
    tickets["Agent_5Leg"] = agent5_ticket(today, projections)

    for name, ticket in tickets.items():
        if ticket:
            legs = ticket.get("legs", [])
            logger.info("[Pipeline] %s: %s-leg ticket, joint_prob=%s%%", name, len(legs), ticket.get("joint_probability", "N/A"))
        else:
            logger.info("[Pipeline] %s: No qualifying ticket today", name)

    # 4. Backtest yesterday's bets
    logger.info("[Pipeline] Step 4: Backtesting %s...", yesterday)
    backtest_results = run_agent_army_backtest(yesterday, yesterday)
    for agent, stats in backtest_results.items():
        logger.info("[Pipeline] Backtest %s: %s", agent, stats)

    logger.info("[Pipeline] Daily pipeline complete!")
    return {"date": today, "markets": markets_count, "tickets": tickets, "backtest": backtest_results}


def _fetch_projections() -> list:
    """Fetch today's projections from the FastAPI ML Engine."""
    import requests
    engine_url = os.environ.get("ENGINE_URL", "http://localhost:8000")
    try:
        r = requests.get(f"{engine_url}/api/mlb/projections/today", timeout=30)
        if r.status_code == 200:
            return r.json().get("projections", [])
    except Exception as e:
        logger.warning("[Pipeline] Could not fetch projections: %s", e)
    return []


if __name__ == "__main__":
    run_daily_pipeline()
