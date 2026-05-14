"""
railway_log_scanner.py
=======================
Scans Railway logs for silent failures and degraded states.

v2 — Time-aware absent pattern checks:
  - no_dispatch_attempt: only flags during/after dispatch window (8:30 AM–2 PM PT)
  - no_xgb_k_log: only flags if K-props were actually active today
  - no_datahub_refresh: uses multiple log patterns, not just one exact string

Plugs into bug_checker.py as two check functions:
    from railway_log_scanner import _check_railway_silent_failures, _check_pipeline_health
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import NamedTuple
from zoneinfo import ZoneInfo

logger = logging.getLogger("propiq.log_scanner")

LOG_FILE       = Path("propiq_army.log")
LOOKBACK_HOURS = 6
MAX_LOG_LINES  = 50000

PT = ZoneInfo("America/Los_Angeles")

# ── Dispatch window (PT) ──────────────────────────────────────────────────────
DISPATCH_OPEN_HOUR  = 8    # 8:30 AM PT
DISPATCH_OPEN_MIN   = 30
DISPATCH_CLOSE_HOUR = 14   # 2:00 PM PT — after this, no dispatch expected


def _now_pt() -> datetime:
    return datetime.now(PT)


def _is_within_dispatch_window() -> bool:
    """True if current PT time is between 8:30 AM and 2:00 PM."""
    now = _now_pt()
    open_  = now.replace(hour=DISPATCH_OPEN_HOUR,  minute=DISPATCH_OPEN_MIN, second=0)
    close_ = now.replace(hour=DISPATCH_CLOSE_HOUR, minute=0,                 second=0)
    return open_ <= now <= close_


def _dispatch_already_ran_today(full_text: str) -> bool:
    """Return True if any dispatch activity appears anywhere in today's logs."""
    return bool(re.search(r"AgentTasklet|run_agent_tasklet|Dispatching.*slip|slip.*dispatched",
                          full_text, re.IGNORECASE))


def _dispatch_ran_today_db() -> bool | None:
    """
    Query bet_ledger for any discord_sent=TRUE row with today's PT date.
    Returns True  → dispatch confirmed via DB
    Returns False → DB confirms no dispatch today
    Returns None  → DB unavailable, fall back to log check
    """
    try:
        import psycopg2  # type: ignore
        db_url = os.environ.get("DATABASE_URL", "")
        if not db_url:
            return None
        today_pt = _now_pt().strftime("%Y-%m-%d")
        conn = psycopg2.connect(db_url, connect_timeout=5)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM bet_ledger WHERE bet_date = %s AND discord_sent = TRUE LIMIT 1",
                    (today_pt,),
                )
                return cur.fetchone() is not None
        finally:
            conn.close()
    except Exception as e:
        logger.debug("[log_scanner] DB dispatch check failed (falling back to log): %s", e)
        return None


def _service_just_started(full_text: str, grace_minutes: int = 15) -> bool:
    """
    Return True if the service started within the last `grace_minutes`.
    Detects startup log lines written by orchestrator at boot.
    """
    startup_patterns = [
        r"PropIQ.*starting",
        r"APScheduler.*started",
        r"Orchestrator.*boot",
        r"startup.*ping",
        r"service.*start",
        r"\[startup\]",
    ]
    now = _now_pt()
    # Look for any startup pattern in recent text — if logs are short, service is fresh
    lines = full_text.strip().splitlines()
    # Heuristic: if we have fewer than 200 log lines total, service restarted recently
    if len(lines) < 200:
        return True
    # Also check for explicit startup markers
    for p in startup_patterns:
        if re.search(p, full_text, re.IGNORECASE):
            return True
    return False


def _k_props_active_today(full_text: str) -> bool:
    """
    Return True if any K-prop (strikeouts) was in today's active parlays.
    If only HRB/hits/total_bases props were dispatched today, no XGBoost K
    log lines are expected — the model ran correctly, just no K props.
    """
    return bool(re.search(
        r"strikeout|Strikeouts|pitching_outs|K.*Over|K.*Under|Ks.*Over|Ks.*Under",
        full_text, re.IGNORECASE,
    ))


# ── Pattern definitions ────────────────────────────────────────────────────────

class LogPattern(NamedTuple):
    name:        str
    pattern:     str
    severity:    str
    description: str
    min_hits:    int = 1
    inverse:     bool = False


FAILURE_PATTERNS: list[LogPattern] = [

    # ── Pipeline / enrichment ─────────────────────────────────────────────────
    LogPattern(
        name="enrich_props_stub",
        pattern=r"_enrich_props.*stub|enrich_props.*return props",
        severity="fail",
        description="enrich_props() stub fired — props returned with ZERO enrichment. "
                    "All model signals bypassed.",
    ),
    LogPattern(
        name="simulate_prop_fallback",
        pattern=r"simulation_engine.*ImportError|_SIM_ENGINE_AVAILABLE.*False",
        severity="warn",
        description="simulation_engine.py failed to import — K/hit props using Poisson-only.",
    ),
    LogPattern(
        name="bp2vec_nameerror",
        pattern=r"bp2vec.*NameError|_bp2vec_adj.*NameError",
        severity="fail",
        description="bp2vec NameError — matchup adjustment silently returning 0.0 on every prop.",
    ),
    LogPattern(
        name="bernoulli_markdown_missing",
        pattern=r"bernoulli.*FileNotFoundError|load_bernoulli.*error",
        severity="warn",
        description="Bernoulli drama markdown file missing — drama penalty 0.0 for all pitchers.",
    ),
    LogPattern(
        name="xgb_feature_mismatch",
        pattern=r"XGBoost.*feature.*mismatch|xgb.*column.*not.*found|xgb_k.*failed",
        severity="fail",
        description="XGBoost feature mismatch — model receiving wrong column names.",
    ),
    LogPattern(
        name="adjustment_dampener_fallback",
        pattern=r"dampen_adjustments.*failed|dampener.*fallback.*naive",
        severity="warn",
        description="adjustment_dampener failed — correlated signals stacking uncapped.",
    ),
    LogPattern(
        name="market_validator_skip",
        pattern=r"market_validator.*failed|_stamp_market_validation.*error",
        severity="warn",
        description="Market validator errored — extreme model/market divergence uncapped.",
    ),
    LogPattern(
        name="adaptive_cal_failed",
        pattern=r"Adaptive calibration failed|AdaptiveCalibrator.*error",
        severity="warn",
        description="Adaptive calibration failed — lambda_bias using hardcoded BBE defaults.",
    ),

    # ── Data pipeline ─────────────────────────────────────────────────────────
    LogPattern(
        name="steamer_low_coverage",
        pattern=r"Steamer 2026 projections loaded: [0-9]{1,2} players",
        severity="fail",
        description="Steamer loaded <100 players — DraftEdge fallback active. "
                    "Most props running on league-average priors.",
    ),
    LogPattern(
        name="fangraphs_blocked",
        pattern=r"FanGraphs.*403|fangraphs.*blocked|fg.*rate.limit",
        severity="warn",
        description="FanGraphs API returning 403 — fg_kpct/fg_bbpct/fg_era using cached values.",
    ),
    LogPattern(
        name="lineups_empty",
        pattern=r"lineups.*0 confirmed|confirmed players.*0|no lineup",
        severity="fail",
        description="MLB lineups returned 0 confirmed players — batter matchup features at defaults.",
    ),
    LogPattern(
        name="umpires_degraded",
        pattern=r"umpires.*[0-4] home plate|home plate umpires.*[0-4] ",
        severity="warn",
        description="Fewer than 5 umpires loaded — umpire K adjustment partially blind.",
    ),
    LogPattern(
        name="zero_starters",
        pattern=r"Probable starters: 0|probable.*starters.*0 pitcher",
        severity="fail",
        description="Zero probable starters — pitcher identity unknown for all K props.",
    ),
    LogPattern(
        name="pp_zero_props",
        pattern=r"\[PP\] 0 props fetched",
        severity="warn",
        description="PrizePicks returned 0 props — PP slate not posted or API down.",
    ),
    LogPattern(
        name="ud_zero_props",
        pattern=r"\[UD\] 0 props fetched",
        severity="warn",
        description="Underdog returned 0 props — UD API down or slate not posted.",
    ),
    LogPattern(
        name="both_platforms_zero",
        pattern=r"\[DB\] Stored 0 snapshots",
        severity="fail",
        description="ZERO snapshots stored — both PP and UD returned empty. No picks today.",
    ),

    # ── Dispatch / agent loop ─────────────────────────────────────────────────
    LogPattern(
        name="cutoff_skip_flood",
        pattern=r"Past cutoff.*skipping cycle",
        severity="warn",
        description="AgentTasklet repeatedly skipping due to cutoff window being too tight.",
        min_hits=4,
    ),
    LogPattern(
        name="zero_slips_generated",
        pattern=r"0 slips generated|slips.*generated.*0|no qualifying props",
        severity="warn",
        description="Agent loop produced 0 slips — all props below MIN_CONFIDENCE or MIN_PROB.",
    ),
    LogPattern(
        name="discord_webhook_fail",
        pattern=r"discord.*webhook.*error|discord.*failed|webhook.*4\d\d",
        severity="fail",
        description="Discord webhook failing — picks generated but not being delivered.",
    ),

    # ── Database errors ───────────────────────────────────────────────────────
    LogPattern(
        name="migration_retry_loop",
        pattern=r"cannot change name of view column|V5\d.*ERROR.*migration",
        severity="fail",
        description="Database migration retrying — consuming connections and generating WAL pressure.",
        min_hits=3,
    ),
    LogPattern(
        name="stale_column_query",
        pattern=r'column.*"confidence".*does not exist|column.*"run_date".*does not exist|'
                r'column.*"last_updated".*does not exist',
        severity="warn",
        description="Stale admin queries hitting columns that don't exist in schema.",
    ),
    LogPattern(
        name="checkpoint_too_frequent",
        pattern=r"checkpoints are occurring too frequently",
        severity="warn",
        description="Postgres WAL pressure — migration retry loop or max_wal_size too small.",
    ),
    LogPattern(
        name="db_connection_reset",
        pattern=r"unexpected EOF on client connection|Connection reset by peer.*transaction",
        severity="warn",
        description="DB connections dropping mid-transaction — possible pool exhaustion.",
        min_hits=3,
    ),
]


# ── Time-aware absent pattern checker ─────────────────────────────────────────
#
# Instead of static ABSENT_PATTERNS checked blindly, we use functions that
# incorporate time-of-day and today's prop context.

def _check_datahub_absent(full_text: str) -> tuple[str, str, str, int] | None:
    """
    Flag if DataHub refresh log is absent.
    Uses multiple patterns — any one match is sufficient to confirm DataHub is running.
    DataHub runs 24/7 so absence at any hour is a real failure.
    """
    patterns = [
        r"Hub refreshed\. Groups:",
        r"DataHub.*refresh",
        r"hub.*cycle.*complete",
        r"\[DataHub\].*✅",
        r"\[DataHub\].*Cycle",
        r"Groups:.*physics.*context",
        r"Stored \d+ snapshots",      # line_stream stores snapshots each hub cycle
    ]
    for p in patterns:
        if re.search(p, full_text, re.IGNORECASE):
            return None  # found — no issue

    # Grace window: if service just (re)started, DataHub hasn't cycled yet — skip
    if _service_just_started(full_text):
        logger.debug("[log_scanner] DataHub check skipped — service freshly started")
        return None

    return (
        "no_datahub_refresh",
        "fail",
        "No DataHub refresh log found in last 6 hours — DataHub may have stopped. "
        "Check Railway deploy logs and orchestrator health.",
        0,
    )


def _check_xgb_k_absent(full_text: str) -> tuple[str, str, str, int] | None:
    """
    Flag if XGBoost K scoring is absent — but ONLY if K-props were active today.
    If tonight's parlays are all HRB/hits/total_bases, no K log lines is correct.
    """
    if not _k_props_active_today(full_text):
        return None  # no K props today — absence of K logs is expected

    patterns = [
        r"\[xgb_k\]",
        r"XGBWire.*K-prop",
        r"xgb.*line=",
        r"K-prop.*formula.*xgb",
    ]
    for p in patterns:
        if re.search(p, full_text, re.IGNORECASE):
            return None  # found

    return (
        "no_xgb_k_log",
        "warn",
        "K-props are active today but no XGBoost K scoring log lines found. "
        "K blend may not be running. Check models/xgb_k_4_5.pkl exists.",
        0,
    )


def _check_dispatch_absent(full_text: str) -> tuple[str, str, str, int] | None:
    """
    Flag if no dispatch activity found — but ONLY during/after the dispatch window.
    Before 8:30 AM PT: silence is expected.
    After 2:00 PM PT: dispatch already happened, silence is expected.
    8:30 AM – 2:00 PM PT: silence means dispatch may have failed.
    """
    now = _now_pt()

    # Before dispatch window opens — don't flag
    open_time = now.replace(hour=DISPATCH_OPEN_HOUR, minute=DISPATCH_OPEN_MIN, second=0)
    if now < open_time:
        return None

    # After 2 PM — dispatch is done, don't flag absence in current window
    # PRIMARY check: query bet_ledger DB — survives Railway redeploys
    close_time = now.replace(hour=DISPATCH_CLOSE_HOUR, minute=0, second=0)
    if now > close_time:
        db_result = _dispatch_ran_today_db()
        if db_result is True:
            return None  # DB confirms dispatch ran today — all good
        if db_result is False:
            # DB confirms NO dispatch today — real problem
            return (
                "no_dispatch_today",
                "warn",
                f"bet_ledger has no discord_sent rows for today (PT). "
                "Dispatch was blocked by health gate or errored silently.",
                0,
            )
        # db_result is None (DB unavailable) — fall back to log scan
        if _dispatch_already_ran_today(full_text):
            return None
        return (
            "no_dispatch_today",
            "warn",
            f"No dispatch activity found in today's logs (checked after {DISPATCH_CLOSE_HOUR}:00 PT). "
            "Dispatch may have been blocked by health gate or errored silently.",
            0,
        )

    # Within dispatch window (8:30 AM – 2:00 PM PT) — check for activity
    db_result = _dispatch_ran_today_db()
    if db_result is True:
        return None
    if _dispatch_already_ran_today(full_text):
        return None

    minutes_past_open = int((now - open_time).total_seconds() / 60)
    if minutes_past_open < 20:
        return None  # give it 20 min grace after window opens

    return (
        "no_dispatch_attempt",
        "warn",
        f"No agent tasklet activity found {minutes_past_open} min into dispatch window. "
        "Scheduler may have stopped or dispatch was blocked.",
        0,
    )


# ── Scanner ────────────────────────────────────────────────────────────────────

def _read_recent_logs(hours: int = LOOKBACK_HOURS) -> list[str]:
    """Read recent log lines from propiq_army.log + Railway Logs API."""
    lines = []

    if LOG_FILE.exists():
        try:
            all_lines = LOG_FILE.read_text(errors="ignore").splitlines()
            lines.extend(all_lines[-MAX_LOG_LINES:])
        except Exception as e:
            logger.debug("Log file read failed: %s", e)

    railway_token   = os.getenv("RAILWAY_API_TOKEN", "")
    railway_service = os.getenv("RAILWAY_SERVICE_ID", "")

    if railway_token and railway_service:
        try:
            import requests
            since = datetime.now(timezone.utc) - timedelta(hours=hours)
            resp = requests.post(
                "https://backboard.railway.app/graphql/v2",
                headers={
                    "Authorization": f"Bearer {railway_token}",
                    "Content-Type":  "application/json",
                },
                json={"query": f"""
                    query {{
                        deploymentLogs(
                            deploymentId: "{railway_service}"
                            filter: {{ since: "{since.isoformat()}" }}
                            limit: 5000
                        ) {{
                            nodes {{ message timestamp severity }}
                        }}
                    }}
                """},
                timeout=15,
            )
            if resp.status_code == 200:
                data  = resp.json()
                nodes = (data.get("data", {})
                             .get("deploymentLogs", {})
                             .get("nodes", []))
                lines.extend(n.get("message", "") for n in nodes)
        except Exception as e:
            logger.debug("Railway Logs API unavailable: %s", e)

    return lines


def _filter_recent(lines: list[str], hours: int = LOOKBACK_HOURS) -> list[str]:
    """Filter to lines from today/yesterday based on timestamp patterns."""
    today     = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    filtered  = [l for l in lines if today in l or yesterday in l]
    return filtered if filtered else lines


def scan_logs(hours: int = LOOKBACK_HOURS) -> list[tuple[str, str, str, int]]:
    """
    Scan recent logs for known silent failure patterns.
    Returns list of (name, severity, description, hit_count).
    """
    lines     = _read_recent_logs(hours)
    recent    = _filter_recent(lines, hours)
    full_text = "\n".join(recent)
    findings  = []

    # Static failure patterns (always checked)
    for pat in FAILURE_PATTERNS:
        try:
            matches   = re.findall(pat.pattern, full_text, re.IGNORECASE)
            hit_count = len(matches)
            if hit_count >= pat.min_hits:
                findings.append((pat.name, pat.severity, pat.description, hit_count))
        except re.error:
            pass

    # Time-aware absent pattern checks
    for check_fn in [
        _check_datahub_absent,
        _check_xgb_k_absent,
        _check_dispatch_absent,
    ]:
        try:
            result = check_fn(full_text)
            if result is not None:
                findings.append(result)
        except Exception as e:
            logger.debug("Absent check %s failed: %s", check_fn.__name__, e)

    return findings


# ── Bug checker integration ────────────────────────────────────────────────────

def _check_railway_silent_failures() -> tuple[str, str, str]:
    """Bug checker check: scan logs for silent failure patterns."""
    try:
        findings = scan_logs(hours=LOOKBACK_HOURS)
        if not findings:
            return ("Silent Failures", "ok",
                    f"No silent failure patterns in last {LOOKBACK_HOURS}h logs")

        fails = [(n, d, c) for n, s, d, c in findings if s == "fail"]
        warns = [(n, d, c) for n, s, d, c in findings if s == "warn"]

        if fails:
            top = fails[0]
            return ("Silent Failures", "fail",
                    f"{len(fails)} FAILURE(s) + {len(warns)} warning(s). "
                    f"Top: [{top[0]}] {top[1][:120]}")
        if warns:
            top = warns[0]
            return ("Silent Failures", "warn",
                    f"{len(warns)} warning(s). Top: [{top[0]}] {top[1][:120]}")

        return ("Silent Failures", "ok", f"No issues in last {LOOKBACK_HOURS}h")

    except Exception as exc:
        return "Silent Failures", "warn", f"Scanner error: {exc}"


def _check_pipeline_health() -> tuple[str, str, str]:
    """Bug checker check: verify core pipeline fired correctly in last 6h."""
    try:
        lines     = _read_recent_logs(hours=LOOKBACK_HOURS)
        full_text = "\n".join(_filter_recent(lines, hours=LOOKBACK_HOURS))
        now       = _now_pt()

        checks = {
            "DataHub refreshing": bool(re.search(
                r"Hub refreshed|DataHub.*Cycle|Stored \d+ snapshots|Groups:.*physics",
                full_text, re.IGNORECASE)),
            "Props loaded": bool(re.search(
                r"Stored \d+ snapshots", full_text)),
            "Steamer 500+ players": bool(re.search(
                r"Steamer 2026 projections loaded: [5-9]\d{2}|[1-9]\d{3}", full_text)),
            "Umpires loaded": bool(re.search(
                r"umpires loaded|home plate umpires", full_text, re.I)),
            "Lineups loaded": bool(re.search(
                r"\d+ confirmed players", full_text)),
        }

        # Only check dispatch if we're within or past the dispatch window
        open_time = now.replace(hour=DISPATCH_OPEN_HOUR, minute=DISPATCH_OPEN_MIN, second=0)
        if now >= open_time:
            checks["Agent dispatched"] = bool(re.search(
                r"AgentTasklet|slip.*dispatched|Dispatching.*slip",
                full_text, re.IGNORECASE))

        failed = [k for k, v in checks.items() if not v]
        passed = [k for k, v in checks.items() if v]

        if len(failed) >= 3:
            return ("Pipeline Health", "fail",
                    f"Pipeline degraded — {len(failed)} checks missing: {', '.join(failed)}")
        if failed:
            return ("Pipeline Health", "warn",
                    f"{len(failed)} check(s) not confirmed: {', '.join(failed)}")

        return ("Pipeline Health", "ok",
                f"All {len(passed)} pipeline checks confirmed in last {LOOKBACK_HOURS}h")

    except Exception as exc:
        return "Pipeline Health", "warn", f"Pipeline check error: {exc}"


# ── Standalone Discord report ──────────────────────────────────────────────────

def post_silent_failure_report() -> None:
    """Post full silent failure report to Discord."""
    webhook = os.getenv("DISCORD_WEBHOOK_URL", "")
    findings = scan_logs(hours=LOOKBACK_HOURS)

    fails = [(n, s, d, c) for n, s, d, c in findings if s == "fail"]
    warns = [(n, s, d, c) for n, s, d, c in findings if s == "warn"]

    if not findings:
        msg = f"✅ No silent failures detected in last {LOOKBACK_HOURS}h logs."
        logger.info("[LogScanner] %s", msg)
        if webhook:
            _post(webhook, "Log Scan Clean", msg, 0x00FF00)
        return

    fields = []
    for name, sev, desc, count in (fails + warns)[:15]:
        emoji   = "🔴" if sev == "fail" else "🟡"
        hit_str = f" ({count}x)" if count > 1 else (" (absent)" if count == 0 else "")
        fields.append({
            "name":   f"{emoji} {name}{hit_str}",
            "value":  desc[:200],
            "inline": False,
        })

    color   = 0xFF0000 if fails else 0xFF8C00
    title   = f"🚨 PropIQ Silent Failure Report — {len(fails)} fail, {len(warns)} warn"
    summary = (
        f"Scanned last **{LOOKBACK_HOURS}h** of logs. "
        f"**{len(fails)} failures** and **{len(warns)} warnings**."
    )

    logger.warning("[LogScanner] %s", title)
    if webhook:
        _post(webhook, title, summary, color, fields)


def _post(webhook: str, title: str, description: str,
          color: int, fields: list | None = None) -> None:
    try:
        import requests
        payload = {"embeds": [{"title": title, "description": description,
                               "color": color, "fields": fields or []}]}
        requests.post(webhook, json=payload, timeout=10)
    except Exception as e:
        logger.warning("[LogScanner] Discord post failed: %s", e)


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    hours = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else LOOKBACK_HOURS
    now   = _now_pt()
    print(f"\nPropIQ Log Scanner — {now.strftime('%I:%M %p PT')}")
    print(f"Dispatch window: {'OPEN' if _is_within_dispatch_window() else 'CLOSED'}")
    print(f"K-props expected: checking logs...")
    print(f"Scanning last {hours}h...\n")

    findings = scan_logs(hours=hours)

    if not findings:
        print("✅ No silent failure patterns detected.")
    else:
        fails = [(n, s, d, c) for n, s, d, c in findings if s == "fail"]
        warns = [(n, s, d, c) for n, s, d, c in findings if s == "warn"]

        if fails:
            print(f"🔴 FAILURES ({len(fails)}):")
            for name, _, desc, count in fails:
                hit = f" [{count}x]" if count > 1 else " [absent]" if count == 0 else ""
                print(f"  ❌ {name}{hit}")
                print(f"     {desc[:120]}")

        if warns:
            print(f"\n🟡 WARNINGS ({len(warns)}):")
            for name, _, desc, count in warns:
                hit = f" [{count}x]" if count > 1 else " [absent]" if count == 0 else ""
                print(f"  ⚠️  {name}{hit}")
                print(f"     {desc[:120]}")

    if "--post" in sys.argv:
        post_silent_failure_report()
        print("\nPosted to Discord.")

    print()
    _, ph_status, ph_detail = _check_pipeline_health()
    emoji = {"ok": "✅", "warn": "⚠️", "fail": "❌"}[ph_status]
    print(f"{emoji} Pipeline: {ph_detail}")
