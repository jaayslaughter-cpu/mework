"""
PropIQ Agent Army — Main Orchestrator
=======================================
Runs 8 tasklets on their defined schedules:
  - DataHubTasklet:      every 15s
  - AgentTasklet:        every 30s
  - LeaderboardTasklet:  every 60s
  - BacktestTasklet:     daily  12:01AM PT
  - GradingTasklet:      daily   1:05AM PT
  - XGBoostTasklet:      weekly Sunday 2:00AM PT
  - LiveDispatch:        daily   8:00AM PT (11:00AM ET) → Discord parlays
  - NightlyRecap:        daily  11:00PM PT ( 2:00AM ET) → Discord settlement

Also exposes a FastAPI dashboard on $PORT.
"""
from __future__ import annotations
import asyncio
import logging
import os
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

# ── Gap-fix: line_stream + monthly_leaderboard (Phase 48 – Gap Closure) ──────
try:
    from line_stream import main as _run_line_stream
    _LINE_STREAM_AVAILABLE = True
except ImportError:
    _LINE_STREAM_AVAILABLE = False
    def _run_line_stream():
        raise NotImplementedError("line_stream module not available")

try:
    from monthly_leaderboard import run_monthly_leaderboard as _run_monthly_leaderboard
    _LEADERBOARD_AVAILABLE = True
except ImportError:
    _LEADERBOARD_AVAILABLE = False
    def _run_monthly_leaderboard():
        raise NotImplementedError("monthly_leaderboard module not available")

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

# Dispatch guard — prevents double/triple firing (Problem 7 & 15)
_dispatch_running: bool = False


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


async def _run_subprocess(name: str, script_path: str) -> None:
    """Run a Python script as a subprocess with full logging."""
    logger.info("[orchestrator] Launching %s (%s)...", name, script_path)
    try:
        proc = await asyncio.create_subprocess_exec(
            sys.executable, script_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            logger.error(
                "[orchestrator] %s FAILED (exit %d): %s",
                name, proc.returncode, stderr.decode()[-500:]
            )
        else:
            logger.info("[orchestrator] %s completed successfully", name)
    except Exception as exc:
        logger.error("[orchestrator] %s subprocess error: %s", name, exc, exc_info=True)


async def job_data_hub():
    """Run DataHub in a thread so it never blocks the event loop."""
    global _last_hub_run
    loop = asyncio.get_event_loop()
    try:
        logger.info("[orchestrator] Running DataHubTasklet...")
        start = time.time()
        await loop.run_in_executor(None, run_data_hub_tasklet)
        elapsed = time.time() - start
        logger.info("[orchestrator] DataHubTasklet done in %.2fs", elapsed)
        _last_hub_run = datetime.utcnow().isoformat()
    except Exception as exc:
        logger.error("[orchestrator] DataHubTasklet FAILED: %s", exc, exc_info=True)


async def job_agents():
    """Run AgentTasklet in a thread so it runs independently of DataHub."""
    global _last_agent_run
    loop = asyncio.get_event_loop()
    try:
        logger.info("[orchestrator] Running AgentTasklet...")
        start = time.time()
        await loop.run_in_executor(None, run_agent_tasklet)
        elapsed = time.time() - start
        logger.info("[orchestrator] AgentTasklet done in %.2fs", elapsed)
        _last_agent_run = datetime.utcnow().isoformat()
    except Exception as exc:
        logger.error("[orchestrator] AgentTasklet FAILED: %s", exc, exc_info=True)


async def job_leaderboard():
    """Run LeaderboardTasklet in a thread."""
    global _last_leaderboard_run
    loop = asyncio.get_event_loop()
    try:
        await loop.run_in_executor(None, run_leaderboard_tasklet)
        _last_leaderboard_run = datetime.utcnow().isoformat()
    except Exception as exc:
        logger.error("[orchestrator] LeaderboardTasklet FAILED: %s", exc, exc_info=True)


async def job_backtest():
    await _safe_run("BacktestTasklet", run_backtest_tasklet)


async def job_grading():
    await _safe_run("GradingTasklet", run_grading_tasklet)


async def job_xgboost():
    await _safe_run("XGBoostTasklet", run_xgboost_tasklet)


async def job_line_stream():
    """Runs every 30 min 10 AM–10 PM PT — steam detection + CLV + in-game tracking."""
    if _LINE_STREAM_AVAILABLE:
        await _safe_run("LineStream", _run_line_stream)
    else:
        logger.warning("[orchestrator] line_stream not available — skipping")


async def job_monthly_leaderboard():
    """Fires 9 AM PT on the 1st of each month — Discord agent performance report."""
    if _LEADERBOARD_AVAILABLE:
        await _safe_run("MonthlyLeaderboard", _run_monthly_leaderboard)
    else:
        logger.warning("[orchestrator] monthly_leaderboard not available — skipping")



async def _startup_dispatch_if_ready() -> None:
    """
    On startup, poll until DataHub is populated, then fire dispatch once.
    Ensures plays go out even when Railway redeploys after 8 AM PT.
    Reads in-memory cache directly — does not depend on Redis being up.
    Max wait: 3 minutes (6 x 30s attempts).
    """
    await asyncio.sleep(30)  # give DataHub one full cycle to populate
    for attempt in range(1, 7):
        # Read hub directly from in-memory fallback (_mem_get) — avoids
        # Redis dependency that caused "waiting" loops in prior deployments
        try:
            from tasklets import _mem_get  # noqa: PLC0415
            hub = _mem_get("mlb_hub") or {}
        except Exception:
            hub = read_hub()

        # DataHub is ready when game_states is populated (ESPN always works)
        game_states = hub.get("game_states", {})
        has_context = bool(hub.get("context"))
        has_games = bool(game_states)
        has_props = bool(hub.get("dfs", {}).get("underdog"))

        if has_games or has_context or has_props:
            logger.info(
                "[orchestrator] DataHub ready — firing startup dispatch (attempt %d) "
                "games=%d context=%s props=%s",
                attempt, len(game_states), has_context, has_props,
            )
            await job_dispatch()
            return

        logger.info(
            "[orchestrator] Startup dispatch waiting for DataHub... attempt %d/6 "
            "(games=%d context=%s)",
            attempt, len(game_states), has_context,
        )
        await asyncio.sleep(30)
    logger.warning("[orchestrator] Startup dispatch skipped — DataHub not ready after 3 min")


async def job_dispatch():
    """8:00 AM PT (11:00 AM ET) daily — build parlays and post to Discord.
    Guard prevents double/triple firing from APScheduler + startup dispatch racing.
    """
    global _dispatch_running
    if _dispatch_running:
        logger.warning("[orchestrator] Dispatch already running — skipping duplicate trigger")
        return
    _dispatch_running = True
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "live_dispatcher.py")
    try:
        await _run_subprocess("LiveDispatch", script)
    finally:
        _dispatch_running = False


async def job_settle():
    """11:00 PM PT (2:00 AM ET) daily — settle bets and post recap to Discord."""
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nightly_recap.py")
    asyncio.create_task(_run_subprocess("NightlyRecap", script))


# ── FastAPI App ───────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(_app: FastAPI):
    logger.info("PropIQ Agent Army starting up...")

    # ── Tasklet interval jobs ─────────────────────────────────────────────────
    scheduler.add_job(job_data_hub,   IntervalTrigger(seconds=15), id="data_hub")
    scheduler.add_job(job_agents,     IntervalTrigger(seconds=30), id="agents")
    scheduler.add_job(job_leaderboard, IntervalTrigger(seconds=60), id="leaderboard")

    # ── Nightly maintenance jobs ──────────────────────────────────────────────
    scheduler.add_job(job_backtest, CronTrigger(hour=0,  minute=1),  id="backtest")
    scheduler.add_job(job_grading,  CronTrigger(hour=1,  minute=5),  id="grading")
    scheduler.add_job(job_xgboost,  CronTrigger(day_of_week="sun", hour=2), id="xgboost")

    # ── Line stream every 30 min 10 AM–10 PM PT ───────────────────────────────
    scheduler.add_job(
        job_line_stream,
        CronTrigger(hour="10-22", minute="0,30", timezone="America/Los_Angeles"),
        id="line_stream",
    )

    # ── Monthly leaderboard — 1st of month 9 AM PT ───────────────────────────
    scheduler.add_job(
        job_monthly_leaderboard,
        CronTrigger(day=1, hour=9, timezone="America/Los_Angeles"),
        id="monthly_leaderboard",
    )

    # ── Daily parlay dispatch — 8:00 AM PT (11:00 AM ET) ─────────────────────
    scheduler.add_job(
        job_dispatch,
        CronTrigger(hour=8, minute=0, timezone="America/Los_Angeles"),
        id="live_dispatch",
    )

    # ── Nightly settlement — 11:00 PM PT (2:00 AM ET) ────────────────────────
    scheduler.add_job(
        job_settle,
        CronTrigger(hour=23, minute=0, timezone="America/Los_Angeles"),
        id="nightly_recap",
    )

    scheduler.start()

    # Discord startup ping
    try:
        discord_alert.send_startup_ping()
    except Exception as _disc_err:
        logger.warning("Discord startup ping failed: %s", _disc_err)

    # Kick off initial data pull
    asyncio.create_task(job_data_hub())
    # Fire dispatch once DataHub is ready (handles post-8AM redeploys)
    asyncio.create_task(_startup_dispatch_if_ready())

    logger.info(
        "All jobs scheduled: dispatch@8AM PT, settle@11PM PT, "
        "line_stream@30min, leaderboard@monthly, "
        "backtest@12:01AM, grading@1:05AM, xgboost@Sun2AM"
    )
    yield

    scheduler.shutdown()
    logger.info("PropIQ Agent Army shut down.")


app = FastAPI(
    title="PropIQ Agent Army",
    description="17-agent MLB DFS betting system with auto-schedule",
    version="2.1.0",
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
        "version": "2.1.0",
        "date": date.today().isoformat(),
        "status": "running",
        "endpoints": [
            "/props", "/insights", "/leaderboard", "/backtest/latest",
            "/health", "/propiq/dispatch", "/propiq/settle",
            "/propiq/status", "/propiq/record",
        ],
    }


@app.get("/props")
async def get_props():
    """Live player props from Underdog + PrizePicks."""
    hub = read_hub()
    ud_props = hub.get("dfs", {}).get("underdog", [])
    pp_props = hub.get("dfs", {}).get("prizepicks", [])
    all_raw  = ud_props + pp_props
    formatted = []
    for p in all_raw[:100]:
        over_odds  = p.get("over_american",  p.get("over_odds",  -115))
        under_odds = p.get("under_american", p.get("under_odds", -115))
        formatted.append({
            "player":    p.get("player", p.get("player_name", "")),
            "prop_type": p.get("stat_type", p.get("prop_type", "")),
            "line":      p.get("line", p.get("stat_value", 0)),
            "platform":  p.get("platform", "underdog"),
            "over":  f"+{over_odds}"  if isinstance(over_odds,  int) and over_odds  > 0 else str(over_odds  or "-"),
            "under": f"+{under_odds}" if isinstance(under_odds, int) and under_odds > 0 else str(under_odds or "-"),
        })
    return JSONResponse({
        "props": formatted,
        "count": len(formatted),
        "underdog": len(ud_props),
        "prizepicks": len(pp_props),
        "timestamp": datetime.utcnow().isoformat(),
    })

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
    return JSONResponse(run_leaderboard_tasklet())


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
    """Trigger backtest in background. Optional start_date/end_date params (YYYY-MM-DD)."""
    asyncio.create_task(_safe_run("BacktestTasklet", run_backtest_tasklet, start_date, end_date))
    return JSONResponse({"status": "started", "message": "Backtest running in background"})


@app.post("/grade")
async def trigger_grading_endpoint(game_date: str = None):
    # run_grading_tasklet is a sync void function — run in background thread
    asyncio.create_task(_safe_run("GradingTasklet", run_grading_tasklet))
    return JSONResponse({"status": "started", "message": "Grading running in background"})


@app.post("/xgboost/retrain")
async def trigger_xgboost():
    asyncio.create_task(_safe_run("XGBoostTasklet", run_xgboost_tasklet))
    return JSONResponse({"status": "started", "message": "XGBoost retraining in background"})


@app.get("/health")
async def health():
    # Return immediately — do not call read_hub() or read_leaderboard()
    # as both attempt Redis connections which timeout in ~10s when Redis is unavailable.
    # Railway health check must get a fast 200 or the deployment is killed.
    return JSONResponse({
        "status": "healthy",
        "scheduler_running": scheduler.running,
        "last_hub_run": _last_hub_run,
        "last_agent_run": _last_agent_run,
        "last_leaderboard_run": _last_leaderboard_run,
    })


# ── PropIQ HTTP endpoints (also callable via Tasklet HTTP triggers) ────────────

@app.post("/propiq/dispatch")
async def trigger_dispatch():
    """Manual or Tasklet-triggered parlay dispatch."""
    await job_dispatch()
    return JSONResponse({"status": "started", "message": "Live dispatcher triggered in background"})


@app.post("/propiq/settle")
async def trigger_settle():
    """Manual or Tasklet-triggered nightly settlement."""
    await job_settle()
    return JSONResponse({"status": "started", "message": "Settlement engine triggered in background"})


@app.post("/trigger/dispatch")
async def trigger_dispatch_alt():
    """Alias for /propiq/dispatch — matches Tasklet schedule trigger path."""
    await job_dispatch()
    return JSONResponse({"status": "started", "message": "Live dispatcher triggered"})


@app.post("/trigger/settle")
async def trigger_settle_alt():
    """Alias for /propiq/settle — matches Tasklet schedule trigger path."""
    await job_settle()
    return JSONResponse({"status": "started", "message": "Settlement engine triggered"})


@app.post("/trigger/leaderboard")
async def trigger_leaderboard():
    """Trigger monthly leaderboard — called by Tasklet schedule on 1st of month."""
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "monthly_leaderboard.py")
    asyncio.create_task(_run_subprocess("MonthlyLeaderboard", script))
    return JSONResponse({"status": "started", "message": "Monthly leaderboard triggered in background"})


@app.get("/propiq/status")
async def get_propiq_status():
    """Full system status — polled by Spring Boot health checks."""
    hub = read_hub()
    lb = read_leaderboard()
    return JSONResponse({
        "service": "PropIQ Agent Army",
        "version": "2.1.0",
        "status": "healthy",
        "scheduler_running": scheduler.running,
        "hub_ud_props": len(hub.get("dfs", {}).get("underdog", [])),
        "hub_starters": len(hub.get("context", {}).get("projected_starters", [])),
        "leaderboard_agents": len(lb.get("leaderboard", [])),
        "last_hub_run": _last_hub_run,
        "last_agent_run": _last_agent_run,
        "last_leaderboard_run": _last_leaderboard_run,
    })


@app.get("/propiq/record")
async def get_season_record():
    """Season W/L record from Postgres — queried by Spring Boot."""
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
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run("orchestrator:app", host="0.0.0.0", port=port, reload=False)
