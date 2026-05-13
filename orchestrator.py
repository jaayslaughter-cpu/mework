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

# ── Railway-compatible JSON log formatter ─────────────────────────────────────
# Railway reads structured JSON from stdout and maps the "level" field to its
# severity filter. Plain-text output causes Railway to tag every line as "error"
# regardless of actual Python log level, breaking severity-based filtering.
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
        "coalesce": True,          # if a job is missed N times, fire it once not N times
        "misfire_grace_time": 30,  # skip a job run if the scheduler is more than 30s late
        "max_instances": 1,        # never run the same job concurrently
    },
)

_last_hub_run: str | None = None
_last_agent_run: str | None = None
_last_leaderboard_run: str | None = None


# ── Cross-process dispatch dedup ──────────────────────────────────────────────
# Uses Postgres so a Railway redeploy (new process) still sees today's dispatch.


def _record_dispatch_ran_today() -> None:
    """Insert today's PT date into dispatch_date_log (no-op if already there).
    Cross-process guard: survives Railway restarts. If today is already present,
    job_agents() post-window check will skip re-dispatch.
    """
    import psycopg2  # noqa: PLC0415
    pt_today = datetime.now(ZoneInfo("America/Los_Angeles")).date()
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


def _startup_ping_if_needed() -> None:
    """Send the Discord startup ping at most once per PT calendar day.
    Uses startup_ping_log table as a cross-process guard — survives Railway
    redeploys so merging multiple PRs on the same day sends only one ping.
    Falls back to always-send if Postgres is unavailable.
    """
    import psycopg2  # noqa: PLC0415
    db_url = os.environ.get("DATABASE_URL")
    pt_today = datetime.now(ZoneInfo("America/Los_Angeles")).date()
    if not db_url:
        # No DB — send unconditionally (edge case: DB env var not set)
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
    """Run a synchronous tasklet in a thread so it never blocks the event loop."""
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
        _last_hub_run = datetime.now(ZoneInfo("America/Los_Angeles")).isoformat()
    except Exception as exc:
        logger.error("[orchestrator] DataHubTasklet FAILED: %s", exc, exc_info=True)


async def job_agents():
    """Run AgentTasklet in a thread so it runs independently of DataHub."""
    global _last_agent_run
    loop = asyncio.get_event_loop()

    _pt_ck = datetime.now(ZoneInfo("America/Los_Angeles"))

    # ── Dynamic dispatch window ───────────────────────────────────────────────
    # Open : 9:00 AM PT (props are posted, no games live yet)
    # Open : 8:30 AM PT
    # Close: 30 min before the earliest scheduled first pitch of the day
    # Fallback ceiling: 12:30 PM PT if game time data isn't in the hub yet
    _open_pt  = _pt_ck.replace(hour=8, minute=30, second=0, microsecond=0)
    if _pt_ck < _open_pt:
        logger.debug(
            "[orchestrator] Pre-window at %02d:%02d PT — opens 8:30 AM. Skipping.",
            _pt_ck.hour, _pt_ck.minute,
        )
        return

    # Compute cutoff from hub game_times (game_time_pt = "HH:MM" PT string)
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
        result = await loop.run_in_executor(None, run_agent_tasklet)
        elapsed = time.time() - start
        logger.info("[orchestrator] AgentTasklet done in %.2fs", elapsed)
        _last_agent_run = datetime.now(ZoneInfo("America/Los_Angeles")).isoformat()
        # Only record dispatch when picks were actually sent (run_agent_tasklet returns True)
        # Avoids "Dispatch date recorded" log spam every 30s during non-dispatch hours
        if result is True:
            _record_dispatch_ran_today()
    except Exception as exc:
        logger.error("[orchestrator] AgentTasklet FAILED: %s", exc, exc_info=True)
        _last_agent_run = f"ERROR at {datetime.now(ZoneInfo('America/Los_Angeles')).strftime('%H:%M PT')}: {type(exc).__name__}: {exc}"


async def job_leaderboard():
    """Run LeaderboardTasklet in a thread."""
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


async def job_settle():
    """11:00 PM PT (2:00 AM ET) daily — settle bets and post recap to Discord."""
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nightly_recap.py")
    asyncio.create_task(_run_subprocess("NightlyRecap", script))


# ── FastAPI App ───────────────────────────────────────────────────────────────


async def job_bug_checker():
    await _safe_run("BugChecker", run_bug_checker)

async def job_log_watcher():
    """10:10 AM PT daily — hits Railway log API, emails/SMSs dispatch summary."""
    try:
        from log_watcher import main as _log_watcher_main  # noqa: PLC0415
        await asyncio.get_event_loop().run_in_executor(None, _log_watcher_main)
        logger.info("[LogWatcher] Daily summary dispatched.")
    except Exception as exc:
        logger.warning("[LogWatcher] Failed: %s", exc)

async def job_streak():
    """Streak pick — runs at 8:45 AM PT, within the 8:30 AM dispatch window."""
    try:
        from streak_agent import run_streak_pick  # noqa: PLC0415
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
    """9:55 AM PT daily — pre-compute Predict+ scores for today's starting pitchers.

    PredictPlusLayer.prefetch() fetches prior-season Savant pitch data per pitcher,
    fits a LogisticRegression full/baseline model pair, and normalises the resulting
    surprise ratio into a Predict+ score (mean=100, SD=10).  The weekly on-disk cache
    means Railway restarts within the same ISO week are free (< 1 ms).

    Runs 25 minutes before the dispatch window opens so _get_predict_plus_adj() in
    prop enrichment always finds a warm cache.  Falls back gracefully if scikit-learn
    is unavailable or the hub has no pitcher props yet.
    """
    try:
        from predict_plus_layer import PredictPlusLayer  # noqa: PLC0415
        hub_snap  = read_hub()
        props     = hub_snap.get("player_props", [])

        # Collect unique starting pitchers that have mlbam_id stamped by enrichment.
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

    # ── SBR sharp game lines (PR #520) — warm cache for inject_team_total ──
    # Runs alongside Predict+ at 8:15 AM so hub context["games"] is warm
    # before the 8:30 AM dispatch window opens.
    try:
        from sportsbookreview_layer import prefetch as _sbr_prefetch  # noqa: PLC0415
        _sbr_count = await asyncio.get_event_loop().run_in_executor(None, _sbr_prefetch)
        logger.info("[SBR] Prefetch complete — %d games loaded.", _sbr_count)
    except Exception as _sbr_exc:
        logger.warning("[SBR] Prefetch failed (non-fatal): %s", _sbr_exc)

    # ── DraftKings player props (PR #521) — Tier 0 sharp lines ──────────────
    # Fetches all 6 supported MLB prop categories from DK's public nash API
    # (curl_cffi TLS spoof; confirmed 200 from datacenter IPs).
    # Warms Redis cache so enrich_props_with_sportsbook() Tier 0 lookup is instant.
    try:
        import redis as _redis_mod  # noqa: PLC0415
        import os as _dk_os          # noqa: PLC0415
        from draftkings_layer import prefetch_dk_props as _dk_prefetch  # noqa: PLC0415
        from datetime import datetime
        import pytz
        _dk_date = datetime.now(pytz.timezone("America/Los_Angeles")).strftime("%Y%m%d")
        _dk_redis = None
        _redis_url = _dk_os.getenv("REDIS_URL") or _dk_os.getenv("REDIS_PUBLIC_URL")
        if _redis_url:
            _dk_redis = _redis_mod.from_url(_redis_url, decode_responses=True)
        _dk_summary = await asyncio.get_event_loop().run_in_executor(
            None, lambda: _dk_prefetch(_dk_date, _dk_redis)
        )
        total_dk = sum(v for v in _dk_summary.values() if v >= 0)
        logger.info("[DK] Prefetch complete — %d prop lines across %d categories: %s",
                    total_dk, len(_dk_summary), _dk_summary)
    except Exception as _dk_exc:
        logger.warning("[DK] Prefetch failed (non-fatal): %s", _dk_exc)



async def job_bp2vec_retrain():
    """Monthly (batter|pitcher)2vec retrain — 3:00 AM PT on the 1st.
    Trains on 4 seasons of Statcast PA data; saves models/bp2vec_*.pkl.
    No-op if bp2vec_train.py is not present (graceful degradation).
    """
    try:
        import importlib, sys as _sys  # noqa: PLC0415
        spec = importlib.util.find_spec("bp2vec_train")
        if spec is None:
            logger.warning("[bp2vec] bp2vec_train.py not found — skipping retrain")
            return
        bp2vec_train = importlib.import_module("bp2vec_train")
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, bp2vec_train.train, [2022, 2023, 2024, 2025])
        logger.info("[bp2vec] Monthly retrain complete.")
    except Exception as exc:
        logger.error("[bp2vec] Retrain failed: %s", exc, exc_info=True)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    logger.info("PropIQ Agent Army starting up...")

    # ── Run pending SQL migrations ────────────────────────────────────────────
    # No Flyway process is attached to this Railway deployment — migrations in
    # the migrations/ folder were never being applied. This runner applies any
    # .sql file that hasn't been recorded in migration_history yet.
    # Safe to run on every startup: all SQL uses IF NOT EXISTS / CREATE OR REPLACE.
    try:
        import glob as _glob  # noqa: PLC0415
        import psycopg2 as _pg  # noqa: PLC0415

        _db_url = os.getenv("DATABASE_URL", "")
        if _db_url:
            with _pg.connect(_db_url) as _mc:
                with _mc.cursor() as _cur:
                    # Create migration history table if it doesn't exist
                    _cur.execute("""
                        CREATE TABLE IF NOT EXISTS migration_history (
                            filename   TEXT PRIMARY KEY,
                            applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                        )
                    """)
                    _mc.commit()

                    # Find all migration files in order
                    _mig_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "migrations")
                    _sql_files = sorted(_glob.glob(os.path.join(_mig_dir, "V*.sql")))

                    # Ensure status column exists (idempotent)
                    try:
                        _cur.execute("ALTER TABLE migration_history ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'ok'")
                        _mc.commit()
                    except Exception:
                        _mc.rollback()

                    _applied = 0
                    for _sql_path in _sql_files:
                        _fname = os.path.basename(_sql_path)
                        _cur.execute("SELECT status FROM migration_history WHERE filename = %s", (_fname,))
                        _mig_row = _cur.fetchone()
                        if _mig_row:
                            if _mig_row[0] == "failed":
                                logger.warning(
                                    "[Migrations] Skipping %s — previously failed. "
                                    "Fix SQL then: DELETE FROM migration_history WHERE filename='%s'",
                                    _fname, _fname,
                                )
                            continue  # already applied or permanently skipped
                        try:
                            with open(_sql_path) as _f:
                                _sql = _f.read()
                            _cur.execute(_sql)
                            _cur.execute(
                                "INSERT INTO migration_history (filename, status) VALUES (%s, 'ok') ON CONFLICT (filename) DO UPDATE SET status='ok'",
                                (_fname,),
                            )
                            _mc.commit()
                            logger.info("[Migrations] Applied: %s", _fname)
                            _applied += 1
                        except Exception as _mig_exc:
                            _mc.rollback()
                            logger.error("[Migrations] FAILED %s: %s", _fname, _mig_exc)
                            # Circuit breaker: mark failed so we don't retry on every restart
                            try:
                                _cur.execute(
                                    "INSERT INTO migration_history (filename, status) VALUES (%s, 'failed') "
                                    "ON CONFLICT (filename) DO UPDATE SET status='failed'",
                                    (_fname,),
                                )
                                _mc.commit()
                            except Exception:
                                _mc.rollback()
                            # Discord notify once
                            try:
                                from DiscordAlertService import discord_alert as _da  # noqa: PLC0415
                                _da._post({"embeds": [{"title": "🚨 DB Migration Failed", "description": f"**{_fname}** failed and will not retry automatically.\n```{str(_mig_exc)[:300]}```\nTo retry: `DELETE FROM migration_history WHERE filename=\'{_fname}\'` then redeploy.", "color": 0xFF0000}]})
                            except Exception:
                                pass

                    if _applied == 0:
                        logger.info("[Migrations] All migrations already applied.")
                    else:
                        logger.info("[Migrations] %d migration(s) applied on startup.", _applied)
    except Exception as _mig_outer:
        logger.error("[Migrations] Migration runner failed: %s", _mig_outer)
        # Never block startup on a migration failure

    # ── Tasklet interval jobs ─────────────────────────────────────────────────
    scheduler.add_job(job_data_hub,   IntervalTrigger(seconds=15), id="data_hub")
    scheduler.add_job(job_agents,     IntervalTrigger(seconds=30), id="agents")
    scheduler.add_job(job_leaderboard, IntervalTrigger(seconds=60), id="leaderboard")

    # ── Nightly maintenance jobs ──────────────────────────────────────────────
    scheduler.add_job(job_backtest, CronTrigger(hour=0,  minute=1,  timezone="America/Los_Angeles"), id="backtest")
    scheduler.add_job(job_grading,  CronTrigger(hour=2,  minute=0,  timezone="America/Los_Angeles"), id="grading")
    scheduler.add_job(job_xgboost,  CronTrigger(hour=2, minute=30, timezone="America/Los_Angeles"), id="xgboost")  # daily retrain now that seed data available

    # ── Line stream every 30 min 10 AM–10 PM PT ───────────────────────────────
    scheduler.add_job(
        job_line_stream,
        CronTrigger(hour="10-22", minute="0,30", timezone="America/Los_Angeles"),
        id="line_stream",
    )

    # ── Weekly calibration map rebuild (every Monday 6:00 AM PT) ─────────────
    def job_calibrate_model():
        try:
            from calibrate_model import generate_calibration_map_from_db  # noqa: PLC0415
            result = generate_calibration_map_from_db()
            logger.info("[Scheduler] Calibration map: %s",
                        f"{len(result)} buckets updated" if result else "insufficient data (<100 graded rows)")
        except Exception as exc:
            logger.warning("[Scheduler] Calibration map rebuild failed: %s", exc)

        # ── Risk-adjusted diagnostics (Sharpe, max drawdown, Calmar) ─────────
        # Runs immediately after calibration on Monday morning.
        try:
            from model_diagnostics import run_weekly_diagnostics  # noqa: PLC0415
            run_weekly_diagnostics(lookback_days=90)
        except Exception as _diag_exc:
            logger.warning("[Scheduler] Weekly diagnostics failed (non-fatal): %s", _diag_exc)

    scheduler.add_job(
        job_calibrate_model,
        CronTrigger(day_of_week="mon", hour=6, minute=0, timezone="America/Los_Angeles"),
        id="job_calibrate_model",
        name="Weekly calibration map rebuild",
        replace_existing=True,
    )

    # ── Monthly leaderboard — 1st of month 9 AM PT ───────────────────────────
    scheduler.add_job(
        job_monthly_leaderboard,
        CronTrigger(day=1, hour=9, timezone="America/Los_Angeles"),
        id="monthly_leaderboard",
    )

    # ── Daily health check — 10:00 AM PT ─────────────────────────────────────
    scheduler.add_job(
        job_bug_checker,
        CronTrigger(hour=10, minute=0, timezone="America/Los_Angeles"),
        id="bug_checker",
    )

    # ── Predict+ prefetch — 8:15 AM PT (15 min before dispatch window opens at 8:30) ──
    # Pre-computes pitcher unpredictability scores so _get_predict_plus_adj()
    # in prop enrichment always finds a warm weekly cache.
    scheduler.add_job(
        job_predict_plus_prefetch,
        CronTrigger(hour=8, minute=15, timezone="America/Los_Angeles"),
        id="predict_plus_prefetch",
    )

    # ── Streak pick — 8:45 AM PT (within dispatch window, well before first pitch) ──
    scheduler.add_job(
        job_streak,
        CronTrigger(hour=8, minute=45, timezone="America/Los_Angeles"),
        id="streak",
    )

    # ── Log watcher summary — 9:15 AM PT (after streak, within dispatch window) ──
    scheduler.add_job(
        job_log_watcher,
        CronTrigger(hour=9, minute=15, timezone="America/Los_Angeles"),
        id="log_watcher",
    )

    # ── Nightly settlement — 11:00 PM PT ─────────────────────────────────────
    scheduler.add_job(
        job_settle,
        CronTrigger(hour=23, minute=0, timezone="America/Los_Angeles"),
        id="nightly_recap",
    )

    # ── Nightly Savant CSV refresh — 4:00 AM PT ───────────────────────────────
    # Fetches live season-to-date data from baseballsavant.mlb.com for all 10
    # statcast CSVs (pitcher arsenal, xERA, batter EV, expected stats, sprint speed,
    # percentiles, bat tracking, swing-take, batted ball, baserunning).
    # Overwrites data/statcast/ CSVs then resets statcast_static_layer in-process.
    # Runs AFTER grading (2 AM) and XGBoost retrain (2:30 AM), BEFORE dispatch (8:30 AM).
    def job_savant_refresh():
        try:
            from savant_refresh import refresh as _sv_refresh  # noqa: PLC0415
            result = _sv_refresh()
            logger.info(
                "[Scheduler] SavantRefresh: %d updated, %d skipped%s",
                result.get("updated", 0),
                result.get("skipped", 0),
                f" errors={result['errors']}" if result.get("errors") else "",
            )
        except Exception as exc:
            logger.warning("[Scheduler] SavantRefresh failed: %s", exc)

    scheduler.add_job(
        job_savant_refresh,
        CronTrigger(hour=4, minute=0, timezone="America/Los_Angeles"),
        id="savant_refresh",
        name="Nightly Savant CSV refresh",
        replace_existing=True,
    )

    # ── Nightly pitch whiff refresh — 3:30 AM PT ──────────────────────────────
    # Fetches yesterday's live game feeds from MLB Stats API, parses all pitches,
    # aggregates to season-to-date whiff%/K% by pitcher+pitch_type and batter+pitch_type.
    # Upserts into pitch_whiff_live and batter_pitch_whiff_live tables.
    # Invalidates batter_pitch_arsenal Redis cache so next cycle reads live data.
    def job_pitch_whiff():
        try:
            from pitch_whiff_refresh import refresh as _pw_refresh  # noqa: PLC0415
            result = _pw_refresh()
            logger.info(
                "[Scheduler] PitchWhiffRefresh: %d games, %d pitches, "
                "%d pitcher rows, %d batter rows",
                result.get("games_fetched", 0),
                result.get("pitches_parsed", 0),
                result.get("pitcher_rows", 0),
                result.get("batter_rows", 0),
            )
        except Exception as exc:
            logger.warning("[Scheduler] PitchWhiffRefresh failed: %s", exc)

        # ── WPA drama scores (feeds BVI layer + CorrelatedParlayAgent) ────────
        try:
            from wpa_drama_layer import prefetch_yesterday_drama as _wpa_fetch  # noqa: PLC0415
            drama = _wpa_fetch()
            logger.info("[Scheduler] WPADrama: %d teams loaded", len(drama))
        except Exception as exc:
            logger.warning("[Scheduler] WPADrama prefetch failed: %s", exc)

    scheduler.add_job(
        job_pitch_whiff,
        CronTrigger(hour=3, minute=30, timezone="America/Los_Angeles"),
        id="pitch_whiff_refresh",
        name="Nightly pitch whiff refresh",
        replace_existing=True,
    )

    # ── Weekly umpire table refresh — Monday 3:00 AM PT ───────────────────────
    # Scrapes swishanalytics.com/mlb/mlb-umpire-factors for live K%, BB%, RPG, boosts.
    # Updates umpire_rates._UMPIRE_TABLE and _STATIC_RUN_IMPACT in-process.
    # Falls back to Redis-cached prior scrape if site returns 403.
    def job_ump_refresh():
        try:
            from ump_refresh import refresh as _ur_refresh  # noqa: PLC0415
            result = _ur_refresh()
            logger.info(
                "[Scheduler] UmpRefresh: %d scraped, %d updated (source=%s)",
                result.get("scraped", 0),
                result.get("updated", 0),
                result.get("source", "?"),
            )
        except Exception as exc:
            logger.warning("[Scheduler] UmpRefresh failed: %s", exc)

    scheduler.add_job(
        job_ump_refresh,
        CronTrigger(day_of_week="mon", hour=3, minute=0, timezone="America/Los_Angeles"),
        id="ump_refresh",
        name="Weekly umpire table refresh",
        replace_existing=True,
    )


    # (batter|pitcher)2vec monthly retrain — 3:00 AM PT 1st of month
    scheduler.add_job(
        job_bp2vec_retrain,
        CronTrigger(day=1, hour=3, minute=0, timezone="America/Los_Angeles"),
        id="bp2vec_retrain", replace_existing=True,
    )
    scheduler.start()

    # Discord startup ping — guarded: at most once per PT calendar day
    _startup_ping_if_needed()

    # Kick off initial data pull
    asyncio.create_task(job_data_hub())

    # Startup catch-up: fire immediately if we restart inside the dispatch window
    asyncio.create_task(_startup_dispatch_catchup())

    logger.info(
        "All jobs scheduled: AgentTasklet@30s (canonical dispatch), settle@11PM PT, "
        "predict_plus_prefetch@8:15AM, streak@8:45AM, log_watcher@9:15AM, "
        "line_stream@30min, leaderboard@monthly, "
        "backtest@12:01AM, grading@2:00AM, xgboost@2:30AM (daily)"
    )
    yield

    scheduler.shutdown()
    logger.info("PropIQ Agent Army shut down.")


async def _startup_dispatch_catchup() -> None:
    """
    Option-A startup catch-up: if the service restarts while the dispatch
    window is open (8:30 AM to cutoff PT), fire job_agents immediately after
    a short hub-warm delay rather than waiting up to 30 s for the interval tick.

    This prevents missed dispatches caused by deployments that finish a few
    minutes after the window opened — today's root cause (service restarted at
    8:49 AM, cutoff was 8:45 AM, picks silently skipped).
    """
    import asyncio as _asyncio
    # Give DataHub 12 s to populate before we read game times
    await _asyncio.sleep(12)

    _pt_now = datetime.now(ZoneInfo("America/Los_Angeles"))
    _open_pt = _pt_now.replace(hour=8, minute=30, second=0, microsecond=0)

    if _pt_now < _open_pt:
        logger.debug("[orchestrator] startup_catchup: pre-window (%02d:%02d PT) — no catchup needed.",
                     _pt_now.hour, _pt_now.minute)
        return

    # Compute cutoff the same way job_agents does
    _hub_snap   = read_hub()
    _game_times = (_hub_snap.get("context") or {}).get("game_times", {})
    _earliest   = None
    for _e in _game_times.values():
        _gtp = _e.get("game_time_pt", "")
        if not _gtp:
            continue
        if _e.get("abstract_state", "") in ("Live", "InProgress", "Final", "Completed"):
            continue
        if _earliest is None or _gtp < _earliest:
            _earliest = _gtp

    if _earliest:
        _fh, _fm   = int(_earliest[:2]), int(_earliest[3:])
        _cut_total = _fh * 60 + _fm - 30
        _cutoff_h, _cutoff_m = _cut_total // 60, _cut_total % 60
    else:
        _cutoff_h, _cutoff_m = 12, 30  # fallback ceiling

    _cutoff_pt = _pt_now.replace(hour=_cutoff_h, minute=_cutoff_m, second=0, microsecond=0)

    if _pt_now >= _cutoff_pt:
        logger.info("[orchestrator] startup_catchup: already past cutoff (%02d:%02d PT) — skipping.",
                    _cutoff_h, _cutoff_m)
        return

    logger.info(
        "[orchestrator] startup_catchup: service restarted inside dispatch window "
        "(%02d:%02d PT, cutoff %02d:%02d PT) — firing job_agents immediately.",
        _pt_now.hour, _pt_now.minute, _cutoff_h, _cutoff_m,
    )
    await job_agents()




app = FastAPI(
    title="PropIQ Agent Army",
    description="17-agent MLB DFS betting system with auto-schedule",
    version="2.2.0",
    lifespan=lifespan,
)

# SECURITY: Restrict CORS to known origins. Add your Railway/Vercel frontend URL
# as the FRONTEND_URL environment variable (e.g. https://mework.up.railway.app).
# Multiple origins can be comma-separated: "https://a.com,https://b.com"
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
    hub = read_hub() or {}
    if not isinstance(hub, dict):
        hub = {}
    agents = get_agents()
    return JSONResponse({
        "leaderboard": lb,
        "agents": agents,
        "games_today": len(hub.get("game_states", {})),
        "timestamp": None,
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


# ── PropIQ HTTP endpoints ──────────────────────────────────────────────────────

@app.post("/propiq/dispatch")
async def trigger_dispatch():
    """live_dispatcher.py removed — AgentTasklet is the canonical dispatch system.
    Parlays are sent continuously by AgentTasklet (every 30s) with full dedup."""
    return JSONResponse({"status": "disabled", "message": "job_dispatch removed. AgentTasklet (every 30s) is the canonical parlay sender."})


@app.post("/propiq/settle")
async def trigger_settle():
    """Manual or Tasklet-triggered nightly settlement."""
    await job_settle()
    return JSONResponse({"status": "started", "message": "Settlement engine triggered in background"})


@app.post("/trigger/dispatch")
async def trigger_dispatch_alt():
    """Alias for /propiq/dispatch — both removed. AgentTasklet is canonical."""
    return JSONResponse({"status": "disabled", "message": "job_dispatch removed. AgentTasklet (every 30s) is the canonical parlay sender."})


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
    """Full system status."""
    try:
        hub = read_hub() or {}
        if not isinstance(hub, dict):
            hub = {}
        lb = read_leaderboard()
        lb_list = lb if isinstance(lb, list) else lb.get("leaderboard", []) if isinstance(lb, dict) else []
        # Compute hub prop count from actual dfs subkey
        _dfs = hub.get("dfs", {}) or {}
        _ud_count = len(_dfs.get("underdog", []))
        _pp_count = len(_dfs.get("prizepicks", []))
        return JSONResponse({
            "service": "PropIQ Agent Army",
            "version": "2.2.0",
            "status": "healthy",
            "scheduler_running": scheduler.running,
            "hub_props_ud": _ud_count,
            "hub_props_pp": _pp_count,
            "hub_props_total": _ud_count + _pp_count,
            "hub_games": len(hub.get("game_states", {})),
            "leaderboard_agents": len(lb_list),
            "last_hub_run": _last_hub_run,
            "last_agent_run": _last_agent_run,
            "last_leaderboard_run": _last_leaderboard_run,
        })
    except Exception as _st_exc:
        return JSONResponse({
            "service": "PropIQ Agent Army",
            "status": "error",
            "error": str(_st_exc),
            "last_hub_run": _last_hub_run,
            "last_agent_run": _last_agent_run,
        })


@app.get("/propiq/record")
async def get_season_record():
    """Season W/L record from Postgres."""
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
    except Exception as exc:  # noqa: BLE001
        logger.error("[record] Postgres query failed: %s", exc)
        return JSONResponse({"error": str(exc)}, status_code=500)


@app.get("/admin/run-seed")
async def admin_run_seed(token: str = "", clear: bool = False):
    """Streaming endpoint to run csv_seed.py and break the model lock.
    Pass ?clear=true to wipe and re-insert seed rows (needed after discord_sent fix).

    Uses pg_try_advisory_lock(12345) so concurrent calls don't deadlock
    on the DELETE FROM bet_ledger step.
    """
    from fastapi.responses import StreamingResponse  # noqa: PLC0415
    import subprocess  # noqa: PLC0415

    SEED_TOKEN = os.environ.get("SEED_TOKEN", "propiq-seed-2026")
    if token != SEED_TOKEN:
        return JSONResponse({"error": "Unauthorized"}, status_code=403)

    # Advisory lock — prevent two concurrent seed runs from deadlocking
    # on DELETE FROM bet_ledger WHERE agent_name='HistoricalCSVSeed'
    _SEED_LOCK_ID = 20260001
    try:
        import psycopg2 as _pg2
        _lock_conn = _pg2.connect(os.environ["DATABASE_URL"])
        _lock_conn.autocommit = True
        with _lock_conn.cursor() as _lc:
            _lc.execute("SELECT pg_try_advisory_lock(%s)", (_SEED_LOCK_ID,))
            _got_lock = _lc.fetchone()[0]
        if not _got_lock:
            _lock_conn.close()
            return JSONResponse(
                {"error": "Seed already running — try again in a few minutes"},
                status_code=409,
            )
    except Exception as _lock_exc:
        logger.warning("[admin/run-seed] Advisory lock check failed: %s", _lock_exc)
        _lock_conn = None
        _got_lock = True  # proceed anyway — better than blocking forever

    cmd = ["python3", "csv_seed.py", "--write"]
    if clear:
        cmd.append("--clear")

    def _stream():
        yield "=== csv_seed.py starting ===\n"
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                cwd=os.path.dirname(os.path.abspath(__file__)),
            )
            for line in iter(proc.stdout.readline, ""):
                yield line
            proc.wait()
            if proc.returncode == 0:
                yield "\n=== csv_seed.py SUCCESS ===\n"
            else:
                yield f"\n=== csv_seed.py FAILED (exit {proc.returncode}) ===\n"
        except Exception as exc:  # noqa: BLE001
            yield f"\n=== ERROR: {exc} ===\n"
        finally:
            # Release advisory lock so the next /admin/run-seed call can proceed
            if _lock_conn and _got_lock:
                try:
                    with _lock_conn.cursor() as _rlc:
                        _rlc.execute("SELECT pg_advisory_unlock(%s)", (_SEED_LOCK_ID,))
                    _lock_conn.close()
                except Exception:
                    pass

    return StreamingResponse(_stream(), media_type="text/plain")


@app.get("/admin/bp2vec-train")
@app.post("/admin/bp2vec-train")
async def admin_bp2vec_train():
    """Trigger a (batter|pitcher)2vec background retrain immediately.
    Models are saved to models/bp2vec_batter.pkl + bp2vec_pitcher.pkl.
    Once saved, apply_bp2vec_adjustment() activates automatically.
    """
    asyncio.create_task(job_bp2vec_retrain())
    return JSONResponse({
        "status": "started",
        "message": "bp2vec retrain running in background (~15-25 min). "
                   "Models activate automatically once saved.",
    })






@app.get("/admin/scan-logs")
async def admin_scan_logs(hours: int = 6):
    """
    On-demand Railway log scan for silent failures.
    Scans the last N hours of propiq_army.log for known failure patterns.
    Posts results to Discord if DISCORD_WEBHOOK_URL is set.

    Usage: GET /admin/scan-logs?hours=12
    """
    try:
        from railway_log_scanner import scan_logs, _check_pipeline_health  # noqa: PLC0415
        findings = scan_logs(hours=hours)
        fails  = [(n, s, d, c) for n, s, d, c in findings if s == "fail"]
        warns  = [(n, s, d, c) for n, s, d, c in findings if s == "warn"]

        # Post to Discord if findings
        if findings and os.getenv("DISCORD_WEBHOOK_URL"):
            from railway_log_scanner import post_silent_failure_report  # noqa: PLC0415
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, post_silent_failure_report)

        _, ph_status, ph_detail = _check_pipeline_health()

        return JSONResponse({
            "status":         "ok",
            "hours_scanned":  hours,
            "failures":       len(fails),
            "warnings":       len(warns),
            "pipeline_health": ph_status,
            "pipeline_detail": ph_detail,
            "findings": [
                {"name": n, "severity": s, "hits": c, "description": d[:200]}
                for n, s, d, c in findings
            ],
        })
    except Exception as exc:
        return JSONResponse({"status": "error", "error": str(exc)}, status_code=500)


@app.get("/admin/force-dispatch")
async def admin_force_dispatch():
    """Diagnostic: run run_agent_tasklet() RIGHT NOW, bypass window check.
    Returns the result or error details so crashes can be diagnosed."""
    import traceback as _tb  # noqa: PLC0415
    loop = asyncio.get_event_loop()
    try:
        # force=True bypasses the internal window gate in run_agent_tasklet
        import functools as _ft  # noqa: PLC0415
        _fn = _ft.partial(run_agent_tasklet, force=True) if "force" in __import__("inspect").signature(run_agent_tasklet).parameters else run_agent_tasklet
        result = await loop.run_in_executor(None, _fn)
        global _last_agent_run
        _last_agent_run = datetime.now(ZoneInfo("America/Los_Angeles")).isoformat()
        if result is True:
            _record_dispatch_ran_today()
        return JSONResponse({"status": "ok", "result": str(result), "last_agent_run": _last_agent_run})
    except Exception as exc:
        return JSONResponse({
            "status": "error",
            "exception": type(exc).__name__,
            "message": str(exc),
            "traceback": _tb.format_exc(),
        }, status_code=500)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run("orchestrator:app", host="0.0.0.0", port=port, reload=False)
