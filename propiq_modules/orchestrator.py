"""
PropIQ Orchestrator — 7-Agent Army + Bet Analyzer
Runs all 7 Spring Batch-style tasklets on their own schedules.
On startup: seeds spring training baselines (all records 0-0).

Tasklet Schedule:
  DataHubTasklet    → every 15s
  BetAnalyzerTasklet→ every  5s  ← NEW
  AgentTasklet      → every 30s
  LeaderboardTasklet→ every 60s
  BacktestTasklet   → daily 12:01 AM
  GradingTasklet    → daily  1:05 AM
  XGBoostTasklet    → Sunday 2:00 AM (+ Spring Training seeder re-run)
"""

import os
import time
import logging
import threading
import schedule
import uvicorn
from concurrent.futures import ThreadPoolExecutor

from tasklets.data_hub_tasklet      import DataHubTasklet
from tasklets.agent_tasklet         import AgentTasklet
from tasklets.leaderboard_tasklet   import LeaderboardTasklet
from tasklets.backtest_tasklet      import BacktestTasklet
from tasklets.grading_tasklet       import GradingTasklet
from tasklets.xgboost_tasklet       import XGBoostTasklet
from tasklets.bet_analyzer_tasklet  import BetAnalyzerTasklet
from spring_training_seeder         import SpringTrainingSeeder
from api.bet_analyzer_controller    import app as analyzer_api

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

executor = ThreadPoolExecutor(max_workers=12)

# ── tasklet instances ──────────────────────────────────────────────────────
data_hub      = DataHubTasklet()
agent         = AgentTasklet()
leaderboard   = LeaderboardTasklet()
backtest      = BacktestTasklet()
grading       = GradingTasklet()
xgboost       = XGBoostTasklet()
bet_analyzer  = BetAnalyzerTasklet()
st_seeder     = SpringTrainingSeeder()


# ── safe wrappers ──────────────────────────────────────────────────────────
def run(tasklet_name: str, fn):
    try:
        logger.info("▶ %s", tasklet_name)
        fn()
        logger.info("✅ %s complete", tasklet_name)
    except Exception as exc:
        logger.error("❌ %s error: %s", tasklet_name, exc, exc_info=True)


# ── schedule definitions ───────────────────────────────────────────────────
def setup_schedules():
    # High-frequency (use threads to avoid blocking)
    schedule.every(5).seconds.do(
        lambda: executor.submit(run, "BetAnalyzerTasklet", bet_analyzer.execute)
    )
    schedule.every(15).seconds.do(
        lambda: executor.submit(run, "DataHubTasklet", data_hub.execute)
    )
    schedule.every(30).seconds.do(
        lambda: executor.submit(run, "AgentTasklet", agent.execute)
    )
    schedule.every(60).seconds.do(
        lambda: executor.submit(run, "LeaderboardTasklet", leaderboard.execute)
    )

    # Daily jobs
    schedule.every().day.at("00:01").do(
        lambda: executor.submit(run, "BacktestTasklet", backtest.execute)
    )
    schedule.every().day.at("01:05").do(
        lambda: executor.submit(run, "GradingTasklet", grading.execute)
    )

    # Weekly jobs (Sunday)
    schedule.every().sunday.at("02:00").do(
        lambda: executor.submit(run, "XGBoostTasklet", xgboost.execute)
    )
    schedule.every().sunday.at("02:05").do(
        lambda: executor.submit(run, "SpringTrainingSeeder.seed_all", st_seeder.seed_all)
    )


# ── REST API server (separate thread) ─────────────────────────────────────
def start_api_server():
    """Run FastAPI on port 8081 (nginx proxies /analyze → 8081)."""
    port = int(os.getenv("API_PORT", "8081"))
    logger.info("Starting Bet Analyzer API on port %d", port)
    uvicorn.run(analyzer_api, host="127.0.0.1", port=port, log_level="warning")


# ── main ───────────────────────────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("🚀 PropIQ Agent Army starting up")
    logger.info("=" * 60)

    # 1. Seed spring training baselines (all records 0-0)
    logger.info("🌱 Seeding spring training baselines...")
    try:
        meta = st_seeder.seed_all()
        logger.info("Spring Training mode=%s  days_to_opening=%d",
                    meta["mode"], meta["days_remaining"])
    except Exception as e:
        logger.warning("ST seeder error (non-fatal): %s", e)

    # 2. Warm up data hub immediately
    logger.info("📡 Initial DataHub warm-up...")
    try:
        data_hub.execute()
    except Exception as e:
        logger.warning("DataHub warm-up error: %s", e)

    # 3. Start API server in background thread
    api_thread = threading.Thread(target=start_api_server, daemon=True)
    api_thread.start()

    # 4. Set up schedules
    setup_schedules()
    logger.info("✅ All 7 tasklets scheduled")

    # 5. Run schedule loop
    logger.info("⚡ Running. DataHub↻15s | BetAnalyzer↻5s | Agents↻30s | Leaderboard↻60s")
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
