"""
PropIQ Agent Army — Main Orchestrator
=======================================
Runs 8 tasklets on their defined schedules:
  - DataHubTasklet:      every 15s
  - AgentTasklet:        every 30s
  - LeaderboardTasklet:  every 60s
  - BacktestTasklet:     daily  12:01AM PT
  - GradingTasklet:      daily  2:00AM PT (after all West Coast games finish)
  - XGBoostTasklet:      daily 2:30AM PT
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
from zoneinfo import ZoneInfo

import uvicorn
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from bug_checker import run_bug_checker
from apscheduler.triggers.interval import IntervalTrigger
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse

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

# ── Railway-compatible JSON log formatter ─────────────────────────────────────
import json as _json_log
class _RailwayFormatter(logging.Formatter):
    _LEVEL_MAP = {
        "DEBUG": "debug", "INFO": "info",
        "WARNING": "warning", "ERROR": "error", "CRITICAL": "critical",
    }
    def format(self, record: logging.LogRecord) -> str:
        return _json_log.dumps({
            "level":   self._LEVEL_MAP.get(record.levelname, "info"),
            "message": self.formatMessage(record),
            "logger":  record.name,
            "time":    self.formatTime(record),
        }, ensure_ascii=False)

_stdout_handler = logging.StreamHandler(sys.stdout)
_stdout_handler.setFormatter(_RailwayFormatter())
_file_handler = logging.FileHandler("propiq_army.log", mode="a")
_file_handler.setFormatter(
    logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
)
logging.basicConfig(level=logging.INFO, handlers=[_stdout_handler, _file_handler])
logger = logging.getLogger("propiq.orchestrator")

# ── Scheduler ────────────────────────────────────────────────────────────────
scheduler = AsyncIOScheduler(
    timezone="America/Los_Angeles",
    job_defaults={
        "coalesce": True,
        "misfire_grace_time": 30,
        "max_instances": 1,
    },
)

_last_hub_run: str | None = None
_last_agent_run: str | None = None
_last_leaderboard_run: str | None = None


# ── Cross-process dispatch dedup ──────────────────────────────────────────────

def _record_dispatch_ran_today() -> None:
    import psycopg2
    pt_today = datetime.now(ZoneInfo("America/Los_Angeles")).date()
    today_str = pt_today.isoformat()
    try:
        from tasklets import _redis as _tredis
        _r = _tredis()
        _r.set(f"dispatch_ran:{today_str}", "1", ex=28 * 3600)
    except Exception:
        pass
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        return
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dispatch_date_log (
                dispatch_date DATE PRIMARY KEY
            )
        """)
        cur.execute(
            "INSERT INTO dispatch_date_log (dispatch_date) VALUES (%s) ON CONFLICT DO NOTHING",
            (pt_today,)
        )
        conn.commit()
        cur.close()
        conn.close()
        logger.info("[orchestrator] Dispatch date recorded: %s", pt_today)
    except Exception as exc:
        logger.warning("[orchestrator] _record_dispatch_ran_today failed: %s", exc)


def _dispatch_already_ran_today() -> bool:
    import psycopg2
    pt_today = datetime.now(ZoneInfo("America/Los_Angeles")).date()
    today_str = pt_today.isoformat()
    try:
        from tasklets import _redis as _tredis
        _r = _tredis()
        if _r.exists(f"dispatch_ran:{today_str}"):
            logger.debug("[orchestrator] Dispatch already ran today (Redis) — skipping.")
            return True
    except Exception:
        pass
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        return False
    try:
        conn = psycopg2.connect(db_url, connect_timeout=5)
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM dispatch_date_log WHERE dispatch_date = %s LIMIT 1",
            (pt_today,)
        )
        found = cur.fetchone() is not None
        cur.close()
        conn.close()
        return found
    except Exception as exc:
        logger.warning("[orchestrator] _dispatch_already_ran_today DB check failed: %s — failing CLOSED (assuming ran)", exc)
        return True


def _startup_ping_if_needed() -> None:
    import psycopg2
    db_url = os.environ.get("DATABASE_URL")
    pt_today = datetime.now(ZoneInfo("America/Los_Angeles")).date()
    if not db_url:
        try:
            discord_alert.send_startup_ping()
        except Exception as _e:
            logger.warning("Discord startup ping failed: %s", _e)
        return
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS startup_ping_log (
                ping_date DATE PRIMARY KEY
            )
        """)
        cur.execute(
            "SELECT 1 FROM startup_ping_log WHERE ping_date = %s",
            (pt_today,)
        )
        already_sent = cur.fetchone() is not None
        if not already_sent:
            discord_alert.send_startup_ping()
            cur.execute(
                "INSERT INTO startup_ping_log (ping_date) VALUES (%s) ON CONFLICT DO NOTHING",
                (pt_today,)
            )
            conn.commit()
            logger.info("[orchestrator] Startup ping sent for %s", pt_today)
        else:
            logger.info(
                "[orchestrator] Startup ping suppressed — already sent today (%s)", pt_today
            )
        cur.close()
        conn.close()
    except Exception as exc:
        logger.warning(
            "[orchestrator] startup_ping_log check failed: %s — sending ping anyway", exc
        )
        try:
            discord_alert.send_startup_ping()
        except Exception as _e2:
            logger.warning("Discord startup ping failed: %s", _e2)


async def _safe_run(name: str, fn, *args, **kwargs):
    loop = asyncio.get_event_loop()
    import functools
    try:
        logger.info("[orchestrator] Running %s...", name)
        start = time.time()
        result = await loop.run_in_executor(None, functools.partial(fn, *args, **kwargs))
        elapsed = time.time() - start
        logger.info("[orchestrator] %s done in %.2fs", name, elapsed)
        return result
    except Exception as e:
        logger.error("[orchestrator] %s FAILED: %s", name, e, exc_info=True)
        return None


async def _run_subprocess(name: str, script_path: str) -> None:
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
    global _last_hub_run
    loop = asyncio.get_event_loop()
    try:
        logger.info("[orchestrator] Running DataHubTasklet...")
        start = time.time()
        await loop.run_in_executor(None, run_data_hub_tasklet)
        elapsed = time.time() - start
        logger.info("[orchestrator] DataHubTasklet done in %.2fs", elapsed)
        _last_hub_run = datetime.now(ZoneInfo("America/Los_Angeles")).isoformat()
    except Exception as exc:
        logger.error("[orchestrator] DataHubTasklet FAILED: %s", exc, exc_info=True)


async def job_agents():
    global _last_agent_run
    loop = asyncio.get_event_loop()

    if _dispatch_already_ran_today():
        logger.info("[orchestrator] Dispatch already ran today — skipping this cycle.")
        return

    _pt_ck = datetime.now(ZoneInfo("America/Los_Angeles"))

    _open_pt  = _pt_ck.replace(hour=8, minute=30, second=0, microsecond=0)
    if _pt_ck < _open_pt:
        logger.debug(
            "[orchestrator] Pre-window at %02d:%02d PT — opens 8:30 AM. Skipping.",
            _pt_ck.hour, _pt_ck.minute,
        )
        return

    _hub_snap  = read_hub()
    _game_times = (_hub_snap.get("context") or {}).get("game_times", {})
    _earliest_pt_str = None
    for _e in _game_times.values():
        _gtp = _e.get("game_time_pt", "")
        if not _gtp:
            continue
        if _e.get("abstract_state", "") in ("Live", "InProgress", "Final", "Completed"):
            continue
        if _earliest_pt_str is None or _gtp < _earliest_pt_str:
            _earliest_pt_str = _gtp

    if _earliest_pt_str:
        _fh, _fm   = int(_earliest_pt_str[:2]), int(_earliest_pt_str[3:])
        _cut_total  = _fh * 60 + _fm - 30
        _cutoff_pt  = _pt_ck.replace(
            hour=_cut_total // 60, minute=_cut_total % 60,
            second=0, microsecond=0,
        )
    else:
        _cutoff_pt = _pt_ck.replace(hour=12, minute=30, second=0, microsecond=0)

    if _pt_ck >= _cutoff_pt:
        logger.debug(
            "[orchestrator] Post-window at %02d:%02d PT — cutoff %02d:%02d PT "
            "(first pitch %s PT). Skipping.",
            _pt_ck.hour, _pt_ck.minute,
            _cutoff_pt.hour, _cutoff_pt.minute,
            _earliest_pt_str or "unknown",
        )
        return

    try:
        logger.info("[orchestrator] Running AgentTasklet...")
        start = time.time()
        _record_dispatch_ran_today()
        result = await loop.run_in_executor(None, run_agent_tasklet)
        elapsed = time.time() - start
        logger.info("[orchestrator] AgentTasklet done in %.2fs", elapsed)
        _last_agent_run = datetime.now(ZoneInfo("America/Los_Angeles")).isoformat()
    except Exception as exc:
        logger.error("[orchestrator] AgentTasklet FAILED: %s", exc, exc_info=True)


async def job_leaderboard():
    global _last_leaderboard_run
    loop = asyncio.get_event_loop()
    try:
        await loop.run_in_executor(None, run_leaderboard_tasklet)
        _last_leaderboard_run = datetime.now(ZoneInfo("America/Los_Angeles")).isoformat()
    except Exception as exc:
        logger.error("[orchestrator] LeaderboardTasklet FAILED: %s", exc, exc_info=True)


async def job_backtest():
    await _safe_run("BacktestTasklet", run_backtest_tasklet)


async def job_grading():
    await _safe_run("GradingTasklet", run_grading_tasklet)


async def job_xgboost():
    await _safe_run("XGBoostTasklet", run_xgboost_tasklet)


async def job_line_stream():
    if _LINE_STREAM_AVAILABLE:
        await _safe_run("LineStream", _run_line_stream)
    else:
        logger.warning("[orchestrator] line_stream not available — skipping")


async def job_monthly_leaderboard():
    if _LEADERBOARD_AVAILABLE:
        await _safe_run("MonthlyLeaderboard", _run_monthly_leaderboard)
    else:
        logger.warning("[orchestrator] monthly_leaderboard not available — skipping")


async def job_settle():
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nightly_recap.py")
    asyncio.create_task(_run_subprocess("NightlyRecap", script))


async def job_bug_checker():
    await _safe_run("BugChecker", run_bug_checker)

async def job_log_watcher():
    try:
        from log_watcher import main as _log_watcher_main
        await asyncio.get_event_loop().run_in_executor(None, _log_watcher_main)
        logger.info("[LogWatcher] Daily summary dispatched.")
    except Exception as exc:
        logger.warning("[LogWatcher] Failed: %s", exc)

async def job_streak():
    try:
        from streak_agent import run_streak_pick
        result = await asyncio.get_event_loop().run_in_executor(None, run_streak_pick)
        if result:
            logger.info("[StreakAgent] Pick posted — streak_id=%s picks=%d",
                        result.get("streak_id"), len(result.get("picks", [])))
        else:
            logger.warning("[StreakAgent] run_streak_pick returned None — "
                           "no qualifying pick today (conf/prob gate, no props, or DB error).")
    except Exception as exc:
        logger.error("[StreakAgent] FAILED: %s", exc, exc_info=True)


async def job_predict_plus_prefetch():
    try:
        from predict_plus_layer import PredictPlusLayer
        hub_snap  = read_hub()
        props     = hub_snap.get("player_props", [])

        _PITCHER_PROP_TYPES = frozenset({
            "strikeouts", "pitching_outs", "hits_allowed",
            "earned_runs", "walks_allowed",
        })
        seen: set[int] = set()
        pitcher_ids: list[tuple[int, str]] = []
        for p in props:
            if p.get("prop_type") not in _PITCHER_PROP_TYPES:
                continue
            mid = int(p.get("mlbam_id") or p.get("pitcher_mlbam_id") or 0)
            if mid <= 0 or mid in seen:
                continue
            seen.add(mid)
            pitcher_ids.append((mid, str(p.get("player_name", "unknown"))))

        if not pitcher_ids:
            logger.info("[PredictPlus] No pitcher props in hub — prefetch skipped.")
            return

        logger.info(
            "[PredictPlus] Prefetching scores for %d unique pitchers...", len(pitcher_ids)
        )
        layer = PredictPlusLayer()
        loop  = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: layer.prefetch(pitcher_ids))
        logger.info("[PredictPlus] Prefetch complete — %d pitchers cached.", len(pitcher_ids))

    except Exception as exc:
        logger.warning("[PredictPlus] Prefetch failed (non-fatal): %s", exc)

    try:
        from batter_pitch_arsenal_layer import prefetch as _bpv_prefetch
        _bpv_prefetch()
        logger.info("[PredictPlus] Batter pitch-type vulnerability cache warmed.")
    except Exception as exc:
        logger.debug("[PredictPlus] BPV prefetch failed (non-fatal): %s", exc)

    try:
        from defense_layer import prefetch as _def_prefetch
        _def_prefetch()
        logger.info("[PredictPlus] Defense OAA cache warmed.")
    except Exception as exc:
        logger.debug("[PredictPlus] Defense OAA prefetch failed (non-fatal): %s", exc)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    logger.info("PropIQ Agent Army starting up...")

    try:
        import glob as _glob
        import psycopg2 as _pg

        _db_url = os.getenv("DATABASE_URL", "")
        if _db_url:
            with _pg.connect(_db_url) as _mc:
                with _mc.cursor() as _cur:
                    _cur.execute("""
                        CREATE TABLE IF NOT EXISTS migration_history (
                            filename   TEXT PRIMARY KEY,
                            applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                        )
                    """)
                    _mc.commit()

                    _mig_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "migrations")
                    _sql_files = sorted(_glob.glob(os.path.join(_mig_dir, "V*.sql")))

                    _applied = 0
                    for _sql_path in _sql_files:
                        _fname = os.path.basename(_sql_path)
                        _cur.execute("SELECT 1 FROM migration_history WHERE filename = %s", (_fname,))
                        if _cur.fetchone():
                            continue
                        try:
                            with open(_sql_path) as _f:
                                _sql = _f.read()
                            _cur.execute(_sql)
                            _cur.execute(
                                "INSERT INTO migration_history (filename) VALUES (%s) ON CONFLICT DO NOTHING",
                                (_fname,),
                            )
                            _mc.commit()
                            logger.info("[Migrations] Applied: %s", _fname)
                            _applied += 1
                        except Exception as _mig_exc:
                            _mc.rollback()
                            logger.error("[Migrations] FAILED %s: %s", _fname, _mig_exc)

                    if _applied == 0:
                        logger.info("[Migrations] All migrations already applied.")
                    else:
                        logger.info("[Migrations] %d migration(s) applied on startup.", _applied)
    except Exception as _mig_outer:
        logger.error("[Migrations] Migration runner failed: %s", _mig_outer)

    scheduler.add_job(job_data_hub,   IntervalTrigger(seconds=15), id="data_hub")
    scheduler.add_job(job_agents,     IntervalTrigger(seconds=30), id="agents")
    scheduler.add_job(job_leaderboard, IntervalTrigger(seconds=60), id="leaderboard")

    scheduler.add_job(job_backtest, CronTrigger(hour=0,  minute=1,  timezone="America/Los_Angeles"), id="backtest")
    scheduler.add_job(job_grading,  CronTrigger(hour=2,  minute=0,  timezone="America/Los_Angeles"), id="grading")
    scheduler.add_job(job_xgboost,  CronTrigger(hour=2, minute=30, timezone="America/Los_Angeles"), id="xgboost")

    scheduler.add_job(
        job_line_stream,
        CronTrigger(hour="10-22", minute="0,30", timezone="America/Los_Angeles"),
        id="line_stream",
    )

    def job_calibrate_model():
        try:
            from calibrate_model import generate_calibration_map_from_db
            result = generate_calibration_map_from_db()
            logger.info("[Scheduler] Calibration map: %s",
                        f"{len(result)} buckets updated" if result else "insufficient data (<100 graded rows)")
        except Exception as exc:
            logger.warning("[Scheduler] Calibration map rebuild failed: %s", exc)

    scheduler.add_job(
        job_calibrate_model,
        CronTrigger(day_of_week="mon", hour=6, minute=0, timezone="America/Los_Angeles"),
        id="job_calibrate_model",
        name="Weekly calibration map rebuild",
        replace_existing=True,
    )

    scheduler.add_job(
        job_monthly_leaderboard,
        CronTrigger(day=1, hour=9, timezone="America/Los_Angeles"),
        id="monthly_leaderboard",
    )

    scheduler.add_job(
        job_bug_checker,
        CronTrigger(hour=10, minute=0, timezone="America/Los_Angeles"),
        id="bug_checker",
    )

    scheduler.add_job(
        job_predict_plus_prefetch,
        CronTrigger(hour=8, minute=15, timezone="America/Los_Angeles"),
        id="predict_plus_prefetch",
    )

    scheduler.add_job(
        job_streak,
        CronTrigger(hour=8, minute=45, timezone="America/Los_Angeles"),
        id="streak",
    )

    scheduler.add_job(
        job_log_watcher,
        CronTrigger(hour=9, minute=15, timezone="America/Los_Angeles"),
        id="log_watcher",
    )

    scheduler.add_job(
        job_settle,
        CronTrigger(hour=23, minute=0, timezone="America/Los_Angeles"),
        id="nightly_recap",
    )

    scheduler.start()

    _startup_ping_if_needed()

    asyncio.create_task(job_data_hub())

    logger.info(
        "All jobs scheduled: AgentTasklet@30s (canonical dispatch), settle@11PM PT, "
        "predict_plus_prefetch@8:15AM, streak@8:45AM, log_watcher@9:15AM, "
        "line_stream@30min, leaderboard@monthly, "
        "backtest@12:01AM, grading@2:00AM, xgboost@2:30AM (daily)"
    )
    yield

    scheduler.shutdown()
    logger.info("PropIQ Agent Army shut down.")


app = FastAPI(
    title="PropIQ Agent Army",
    description="17-agent MLB DFS betting system with auto-schedule",
    version="2.2.0",
    lifespan=lifespan,
)

_cors_env = os.getenv("FRONTEND_URL", "")
_allowed_origins: list[str] = (
    [o.strip() for o in _cors_env.split(",") if o.strip()]
    if _cors_env
    else ["http://localhost:3000", "http://localhost:3002"]
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    return {
        "service": "PropIQ Agent Army",
        "version": "2.2.0",
        "date": datetime.now(ZoneInfo("America/Los_Angeles")).date().isoformat(),
        "status": "running",
        "endpoints": [
            "/props", "/insights", "/leaderboard", "/backtest/latest",
            "/health", "/propiq/dispatch", "/propiq/settle",
            "/propiq/status", "/propiq/record",
        ],
    }


@app.get("/props")
async def get_props():
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
    lb = read_leaderboard()
    hub = read_hub()
    agents = get_agents()
    return JSONResponse({
        "leaderboard": lb,
        "agents": agents,
        "games_today": len(hub.get("games_today", [])),
        "timestamp": lb.get("timestamp"),
    })


@app.get("/leaderboard")
async def get_leaderboard():
    return JSONResponse(read_leaderboard())


@app.get("/leaderboard/live")
async def get_leaderboard_live():
    run_leaderboard_tasklet()
    return JSONResponse({"leaderboard": read_leaderboard()})


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
    asyncio.create_task(_safe_run("BacktestTasklet", run_backtest_tasklet))
    return JSONResponse({"status": "started", "message": "Backtest running in background"})


@app.post("/grade")
async def trigger_grading_endpoint(game_date: str = None):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, run_grading_tasklet)
    return JSONResponse({"status": "ok", "message": "Grading complete — check Discord for recap"})


@app.post("/xgboost/retrain")
async def trigger_xgboost():
    asyncio.create_task(_safe_run("XGBoostTasklet", run_xgboost_tasklet))
    return JSONResponse({"status": "started", "message": "XGBoost retraining in background"})


@app.get("/health")
async def health():
    return JSONResponse({
        "status": "healthy",
        "scheduler_running": scheduler.running,
        "last_hub_run": _last_hub_run,
        "last_agent_run": _last_agent_run,
        "last_leaderboard_run": _last_leaderboard_run,
    })


# ── PropIQ HTTP endpoints ─────────────────────────────────────────────────────

@app.post("/propiq/dispatch")
async def trigger_dispatch():
    return JSONResponse({"status": "disabled", "message": "job_dispatch removed. AgentTasklet (every 30s) is the canonical parlay sender."})


@app.post("/propiq/settle")
async def trigger_settle():
    await job_settle()
    return JSONResponse({"status": "started", "message": "Settlement engine triggered in background"})


@app.post("/trigger/dispatch")
async def trigger_dispatch_alt():
    return JSONResponse({"status": "disabled", "message": "job_dispatch removed. AgentTasklet (every 30s) is the canonical parlay sender."})


@app.post("/trigger/settle")
async def trigger_settle_alt():
    await job_settle()
    return JSONResponse({"status": "started", "message": "Settlement engine triggered"})


@app.post("/trigger/leaderboard")
async def trigger_leaderboard():
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "monthly_leaderboard.py")
    asyncio.create_task(_run_subprocess("MonthlyLeaderboard", script))
    return JSONResponse({"status": "started", "message": "Monthly leaderboard triggered in background"})


@app.get("/propiq/status")
async def get_propiq_status():
    hub = read_hub()
    lb = read_leaderboard()
    return JSONResponse({
        "service": "PropIQ Agent Army",
        "version": "2.2.0",
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
    import psycopg2

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
                COALESCE(SUM(stake) FILTER (WHERE status != 'PENDING'),  0)  AS total_staked
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
    except Exception as exc:
        logger.error("[record] Postgres query failed: %s", exc)
        return JSONResponse({"error": str(exc)}, status_code=500)


# ── Admin: one-shot seed trigger ──────────────────────────────────────────────
# Opens a browser-accessible endpoint so csv_seed.py can be run without
# needing Railway shell access. Protected by a static token.
# Usage: GET https://mework-production-864d.up.railway.app/admin/run-seed?token=propiq-seed-2026

_SEED_TOKEN = "propiq-seed-2026"

@app.get("/admin/run-seed", response_class=PlainTextResponse)
async def admin_run_seed(token: str = Query(default="")):
    """Trigger csv_seed.py --write --clear and stream output as plain text."""
    if token != _SEED_TOKEN:
        return PlainTextResponse("Unauthorized — pass ?token=propiq-seed-2026", status_code=401)

    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "csv_seed.py")
    if not os.path.exists(script):
        return PlainTextResponse("ERROR: csv_seed.py not found", status_code=500)

    logger.info("[admin] /admin/run-seed triggered — running csv_seed.py --write --clear")
    try:
        proc = await asyncio.create_subprocess_exec(
            sys.executable, script, "--write", "--clear",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=600)
        output = stdout.decode(errors="replace")
        status = "SUCCESS" if proc.returncode == 0 else f"FAILED (exit {proc.returncode})"
        logger.info("[admin] csv_seed.py finished: %s", status)
        return PlainTextResponse(f"=== csv_seed.py {status} ===\n\n{output}")
    except asyncio.TimeoutError:
        return PlainTextResponse("ERROR: Seed timed out after 10 minutes", status_code=500)
    except Exception as exc:
        logger.error("[admin] run-seed failed: %s", exc)
        return PlainTextResponse(f"ERROR: {exc}", status_code=500)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run("orchestrator:app", host="0.0.0.0", port=port, reload=False)
