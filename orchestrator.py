"""
PropIQ Agent Army — Main Orchestrator
=======================================
Runs 6 tasklets on their defined schedules:
  - DataHubTasklet:      every 15s
  - AgentTasklet:        every 30s
  - LeaderboardTasklet:  every 60s
  - BacktestTasklet:     daily  12:01AM
  - GradingTasklet:      daily  1:05AM
  - XGBoostTasklet:      weekly Sunday 2:00AM

Also exposes a FastAPI dashboard at localhost:8080.
"""
from __future__ import annotations
import asyncio
import logging
import sys
import time
from contextlib import asynccontextmanager
from datetime import datetime, date

import uvicorn
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from tasklets import (
    run_data_hub_tasklet, read_hub,
    run_agent_tasklet, get_agents,
    run_leaderboard_tasklet, read_leaderboard,
    run_backtest_tasklet,
    run_grading_tasklet,
    run_xgboost_tasklet,
)
from DiscordAlertService import discord_alert

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("propiq_army.log", mode="a"),
    ]
)
logger = logging.getLogger("propiq.orchestrator")

# ── Scheduler ────────────────────────────────────────────────────────────────
scheduler = AsyncIOScheduler(timezone="America/Los_Angeles")

_last_hub_run: str | None = None
_last_agent_run: str | None = None
_last_leaderboard_run: str | None = None


async def _safe_run(name: str, fn, *args, **kwargs):
    """Run a tasklet with error logging."""
    try:
        logger.info("[orchestrator] Running %s...", name)
        start = time.time()
        result = fn(*args, **kwargs)
        elapsed = time.time() - start
        logger.info("[orchestrator] %s done in %.2fs", name, elapsed)
        return result
    except Exception as e:
        logger.error("[orchestrator] %s FAILED: %s", name, e, exc_info=True)
        return None


async def job_data_hub():
    global _last_hub_run
    await _safe_run("DataHubTasklet", run_data_hub_tasklet)
    _last_hub_run = datetime.utcnow().isoformat()


async def job_agents():
    global _last_agent_run
    await _safe_run("AgentTasklet", run_agent_tasklet)
    _last_agent_run = datetime.utcnow().isoformat()


async def job_leaderboard():
    global _last_leaderboard_run
    await _safe_run("LeaderboardTasklet", run_leaderboard_tasklet)
    _last_leaderboard_run = datetime.utcnow().isoformat()


async def job_backtest():
    await _safe_run("BacktestTasklet", run_backtest_tasklet)


async def job_grading():
    await _safe_run("GradingTasklet", run_grading_tasklet)


async def job_xgboost():
    await _safe_run("XGBoostTasklet", run_xgboost_tasklet)


# ── FastAPI App ───────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(_app: FastAPI):
    logger.info("PropIQ Agent Army starting up...")

    # Register tasklet jobs
    scheduler.add_job(job_data_hub, IntervalTrigger(seconds=15), id="data_hub")
    scheduler.add_job(job_agents, IntervalTrigger(seconds=30), id="agents")
    scheduler.add_job(job_leaderboard, IntervalTrigger(seconds=60), id="leaderboard")
    scheduler.add_job(job_backtest, CronTrigger(hour=0, minute=1), id="backtest")
    scheduler.add_job(job_grading, CronTrigger(hour=1, minute=5), id="grading")
    scheduler.add_job(job_xgboost, CronTrigger(day_of_week="sun", hour=2), id="xgboost")

    scheduler.start()

    # Discord startup ping
    try:
        discord_alert.send_startup_ping()
    except Exception as _disc_err:
        logger.warning("Discord startup ping failed: %s", _disc_err)

    # Kick off initial data pull
    asyncio.create_task(job_data_hub())

    logger.info("All 6 tasklets scheduled and running.")
    yield

    scheduler.shutdown()
    logger.info("PropIQ Agent Army shut down.")


app = FastAPI(
    title="PropIQ Agent Army",
    description="7 competing MLB betting agents with auto-capital allocation",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    return {
        "service": "PropIQ Agent Army",
        "version": "2.0.0",
        "date": date.today().isoformat(),
        "status": "running",
        "endpoints": ["/props", "/insights", "/leaderboard", "/backtest/latest", "/health",
                      "/propiq/dispatch", "/propiq/settle", "/propiq/status", "/propiq/record"],
    }


@app.get("/props")
async def get_props():
    """Live player props."""
    hub = read_hub()
    props = hub.get("player_props", [])
    formatted = []
    for p in props[:60]:
        over_odds = p.get("over_odds")
        under_odds = p.get("under_odds")
        formatted.append({
            "player": p.get("player_name", ""),
            "prop_type": p.get("prop_type", ""),
            "line": p.get("line", 0),
            "book": p.get("bookmaker", ""),
            "over": f"+{over_odds}" if over_odds and int(over_odds) > 0 else str(over_odds or "-"),
            "under": f"+{under_odds}" if under_odds and int(under_odds) > 0 else str(under_odds or "-"),
        })
    return JSONResponse({"props": formatted, "count": len(formatted), "timestamp": hub.get("timestamp")})


@app.get("/insights")
async def get_insights():
    """Agent rankings + active bet queue."""
    lb = read_leaderboard()
    hub = read_hub()
    agents = get_agents()
    agent_status = {}
    for name, agent in agents.items():
        stats = agent.stats
        pending = len(agent.db.get_pending_bets(name))
        agent_status[name] = {**stats, "pending_bets": pending}
    return JSONResponse({
        "leaderboard": lb.get("leaderboard", []),
        "agent_status": agent_status,
        "games_today": len(hub.get("games_today", [])),
        "timestamp": lb.get("timestamp"),
    })


@app.get("/leaderboard")
async def get_leaderboard():
    return JSONResponse(read_leaderboard())


@app.get("/leaderboard/live")
async def get_leaderboard_live():
    result = run_leaderboard_tasklet()
    return JSONResponse(result)


@app.get("/backtest/latest")
async def get_backtest():
    from pathlib import Path
    import json, glob
    data_dir = Path(__file__).parent / "data"
    files = sorted(glob.glob(str(data_dir / "backtest_*.json")), reverse=True)
    if not files:
        return JSONResponse({"status": "no_data", "message": "No backtest data."})
    with open(files[0]) as f:
        return JSONResponse(json.load(f))


@app.post("/backtest/run")
async def trigger_backtest(start_date: str = None, end_date: str = None):
    asyncio.create_task(_safe_run("BacktestTasklet", run_backtest_tasklet, start_date, end_date))
    return JSONResponse({"status": "started", "message": "Backtest running in background"})


@app.post("/grade")
async def trigger_grading(game_date: str = None):
    result = run_grading_tasklet(game_date=game_date)
    return JSONResponse(result)


@app.post("/xgboost/retrain")
async def trigger_xgboost():
    asyncio.create_task(_safe_run("XGBoostTasklet", run_xgboost_tasklet))
    return JSONResponse({"status": "started", "message": "XGBoost retraining in background"})


@app.get("/health")
async def health():
    hub = read_hub()
    lb = read_leaderboard()
    return JSONResponse({
        "status": "healthy",
        "hub_timestamp": hub.get("timestamp"),
        "hub_props": len(hub.get("player_props", [])),
        "hub_games": len(hub.get("games_today", [])),
        "leaderboard_agents": len(lb.get("leaderboard", [])),
        "last_hub_run": _last_hub_run,
        "last_agent_run": _last_agent_run,
        "last_leaderboard_run": _last_leaderboard_run,
        "scheduler_running": scheduler.running,
    })


# ── PropIQ HTTP endpoints (consumed by Spring Boot PropIQHttpClient) ──────────

@app.post("/propiq/dispatch")
async def trigger_dispatch():
    """Trigger the live dispatcher — called by Spring Boot HTTP client.

    Runs live_dispatcher.py as a background subprocess.
    No RabbitMQ required — pure HTTP.
    """
    async def _run():
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "/app/live_dispatcher.py",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            logger.error("[dispatch] live_dispatcher.py failed: %s", stderr.decode()[-500:])
        else:
            logger.info("[dispatch] live_dispatcher.py completed successfully")

    asyncio.create_task(_run())
    return JSONResponse({"status": "started", "message": "Live dispatcher triggered in background"})


@app.post("/propiq/settle")
async def trigger_settle():
    """Trigger nightly settlement engine — called by Spring Boot HTTP client.

    Runs nightly_recap.py as a background subprocess.
    No RabbitMQ required — pure HTTP.
    """
    async def _run():
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "/app/nightly_recap.py",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            logger.error("[settle] nightly_recap.py failed: %s", stderr.decode()[-500:])
        else:
            logger.info("[settle] nightly_recap.py completed successfully")

    asyncio.create_task(_run())
    return JSONResponse({"status": "started", "message": "Settlement engine triggered in background"})


@app.get("/propiq/status")
async def get_propiq_status():
    """Full system status — polled by Spring Boot health checks."""
    hub = read_hub()
    lb = read_leaderboard()
    return JSONResponse({
        "service": "PropIQ Agent Army",
        "version": "2.0.0",
        "status": "healthy",
        "scheduler_running": scheduler.running,
        "hub_props": len(hub.get("player_props", [])),
        "hub_games": len(hub.get("games_today", [])),
        "leaderboard_agents": len(lb.get("leaderboard", [])),
        "last_hub_run": _last_hub_run,
        "last_agent_run": _last_agent_run,
        "last_leaderboard_run": _last_leaderboard_run,
    })


@app.get("/propiq/record")
async def get_season_record():
    """Season W/L record from Postgres — queried by Spring Boot."""
    import os
    import psycopg2  # noqa: PLC0415

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        return JSONResponse({"error": "DATABASE_URL not set"}, status_code=503)
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE status = 'WIN')     AS wins,
                COUNT(*) FILTER (WHERE status = 'LOSS')    AS losses,
                COUNT(*) FILTER (WHERE status = 'PUSH')    AS pushes,
                COUNT(*) FILTER (WHERE status = 'PENDING') AS pending,
                COALESCE(SUM(payout) FILTER (WHERE status = 'WIN'), 0) AS total_payout,
                COALESCE(SUM(stake),  0)                   AS total_staked
            FROM propiq_season_record
            """
        )
        row = cur.fetchone()
        cur.close()
        conn.close()
        wins, losses, pushes, pending, total_payout, total_staked = row
        roi = (
            (float(total_payout) - float(total_staked)) / float(total_staked) * 100
            if total_staked and float(total_staked) > 0
            else 0.0
        )
        return JSONResponse({
            "wins": wins,
            "losses": losses,
            "pushes": pushes,
            "pending": pending,
            "total_staked": float(total_staked),
            "total_payout": float(total_payout),
            "roi_pct": round(roi, 2),
        })
    except Exception as exc:  # noqa: BLE001
        logger.error("[record] Postgres query failed: %s", exc)
        return JSONResponse({"error": str(exc)}, status_code=500)


if __name__ == "__main__":
    uvicorn.run("orchestrator:app", host="0.0.0.0", port=8080, reload=False)
