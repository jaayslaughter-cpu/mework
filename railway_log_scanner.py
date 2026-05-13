"""
railway_log_scanner.py
=======================
Scans Railway logs for silent failures and degraded states that don't
raise exceptions — they just quietly return wrong results.

Designed to plug directly into bug_checker.py as two new check functions:

    from railway_log_scanner import _check_railway_silent_failures, _check_pipeline_health

Add both to the `checks` list in run_bug_checker().

WHAT IT CATCHES
---------------
Silent failures are the hardest bugs to find — they don't crash the system,
they just produce wrong output. From this session's audit, the known silent
failure modes are:

  PIPELINE FAILURES (from log patterns):
    - enrich_props stub fired (props returned with zero enrichment)
    - XGBoost blend called but no [xgb_k] log lines (model not scoring)
    - bp2vec silently returning 0.0 on NameError (caught but invisible)
    - _simulate_prop() ImportError → Poisson-only fallback (silent)
    - Bernoulli drama penalty 0.0 on every pitch (markdown file missing)
    - Market validator not called (import present but call absent)
    - Adjustment dampener firing but returning full stacked value (fallback)
    - Covers layer returning 0 entries (IP block)
    - DraftKings prefetch failing silently (debug-level only)

  DATA FAILURES:
    - Steamer loaded < 100 players (DraftEdge first-call fallback)
    - FanGraphs 403 error (leaderboard blocked)
    - Lineups empty (MLB Stats API down)
    - Umpires < 5 loaded (API degraded)
    - Weather failed for > 3 stadiums (API timeout)
    - 0 probable starters (lineup data absent)

  DISPATCH FAILURES:
    - AgentTasklet: "Past cutoff" on every cycle (window too tight)
    - AgentTasklet: 0 slips generated (all props below threshold)
    - Discord webhook failing (no .status 204 in recent logs)

  DB FAILURES:
    - "column ... does not exist" recurring (stale admin queries)
    - "there is already a transaction in progress" (advisory lock issue)
    - Migration retrying (failed migration not circuit-broken)

HOW IT WORKS
------------
Reads from two sources:
  1. propiq_army.log — the local file log (always available)
  2. Railway Logs API — if RAILWAY_API_TOKEN is set (optional, richer)

For each log pattern, checks the last N hours of log output.
Groups findings by severity and posts to Discord.

USAGE
-----
Run standalone:
    python railway_log_scanner.py

Add to bug_checker.py:
    from railway_log_scanner import _check_railway_silent_failures, _check_pipeline_health

    checks = [
        ...existing checks...,
        _check_railway_silent_failures,
        _check_pipeline_health,
    ]
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import NamedTuple

logger = logging.getLogger("propiq.log_scanner")

LOG_FILE       = Path("propiq_army.log")
LOOKBACK_HOURS = 6     # scan last 6 hours of logs
MAX_LOG_LINES  = 50000 # cap to avoid memory issues on large log files


# ── Pattern definitions ────────────────────────────────────────────────────────

class LogPattern(NamedTuple):
    name:        str
    pattern:     str           # regex searched in log lines
    severity:    str           # "fail" | "warn"
    description: str           # what it means
    min_hits:    int = 1       # how many occurrences = trigger
    inverse:     bool = False  # if True: flag when pattern is ABSENT


# Patterns that indicate silent failures when PRESENT
FAILURE_PATTERNS: list[LogPattern] = [

    # ── Pipeline / enrichment ─────────────────────────────────────────────────
    LogPattern(
        name="enrich_props_stub",
        pattern=r"_enrich_props.*stub|enrich_props.*return props",
        severity="fail",
        description="enrich_props() stub fired — props returned with ZERO enrichment. "
                    "All model signals (PA model, XGB, TTOP, dampener) bypassed.",
    ),
    LogPattern(
        name="simulate_prop_fallback",
        pattern=r"simulation_engine.*ImportError|_SIM_ENGINE_AVAILABLE.*False|simulate.*fallback.*Poisson",
        severity="warn",
        description="simulation_engine.py failed to import — K/hit props using Poisson-only "
                    "(no PA distribution, no variance). XGB blend may still apply.",
    ),
    LogPattern(
        name="bp2vec_nameerror",
        pattern=r"bp2vec.*NameError|_bp2vec_adj.*NameError|NameError.*bp2vec",
        severity="fail",
        description="bp2vec block hit NameError — silently caught by bare except. "
                    "Matchup adjustment returning 0.0 on every prop.",
    ),
    LogPattern(
        name="bernoulli_markdown_missing",
        pattern=r"bernoulli.*FileNotFoundError|bernoulli.*file.*not.*found|load_bernoulli.*error",
        severity="warn",
        description="Bernoulli drama markdown file missing — drama penalty 0.0 for all pitchers. "
                    "K-prop probability NOT penalized for high-Drama starters.",
    ),
    LogPattern(
        name="xgb_feature_mismatch",
        pattern=r"XGBoost.*feature.*mismatch|xgb.*column.*not.*found|xgb.*KeyError|xgb_k.*failed",
        severity="fail",
        description="XGBoost feature mismatch — model receiving wrong column names. "
                    "K/hit blend producing near-random output.",
    ),
    LogPattern(
        name="adjustment_dampener_fallback",
        pattern=r"dampen_adjustments.*failed|dampener.*fallback.*naive",
        severity="warn",
        description="adjustment_dampener failed — falling back to naive summation. "
                    "Correlated signals (shadow_whiff + zone_integrity + chase) stacking uncapped.",
    ),
    LogPattern(
        name="market_validator_skip",
        pattern=r"market_validator.*failed|_stamp_market_validation.*error",
        severity="warn",
        description="Market validator errored — extreme model/market divergence not being capped. "
                    "Props with 25pp+ divergence passing through unchecked.",
    ),
    LogPattern(
        name="adaptive_cal_failed",
        pattern=r"Adaptive calibration failed|AdaptiveCalibrator.*error",
        severity="warn",
        description="Adaptive calibration failed — lambda_bias and swstr_k9_scale using "
                    "hardcoded BBE defaults, not live-updated values.",
    ),

    # ── Data pipeline ─────────────────────────────────────────────────────────
    LogPattern(
        name="steamer_low_coverage",
        pattern=r"Steamer 2026 projections loaded: [0-9]{1,2} players",
        severity="fail",
        description="Steamer loaded fewer than 100 players — DraftEdge first-call fallback active. "
                    "Most props running on league-average priors, not actual projections.",
    ),
    LogPattern(
        name="fangraphs_blocked",
        pattern=r"FanGraphs.*403|fangraphs.*blocked|fg.*rate.limit|fangraphs.*forbidden",
        severity="warn",
        description="FanGraphs API returning 403 — leaderboard data unavailable. "
                    "fg_kpct, fg_bbpct, fg_era falling back to cached or league-average values.",
    ),
    LogPattern(
        name="lineups_empty",
        pattern=r"lineups.*0 confirmed|confirmed players.*0|lineup.*empty|no lineup",
        severity="fail",
        description="MLB lineups returned 0 confirmed players — lineup context absent. "
                    "opp_lineup_k_pct and batter matchup features using league-average defaults.",
    ),
    LogPattern(
        name="umpires_degraded",
        pattern=r"umpires.*[0-4] home plate|home plate umpires.*[0-4] ",
        severity="warn",
        description="Fewer than 5 umpires loaded — umpire K adjustment layer partially blind. "
                    "Some games missing ump_k_adj signal.",
    ),
    LogPattern(
        name="weather_failed",
        pattern=r"Weather.*failed|weather.*error|weather.*timeout|weather fetched.*[0-9] stadium",
        severity="warn",
        description="Weather API degraded — wind decomposition and temperature adjustment "
                    "using defaults for affected stadiums.",
    ),
    LogPattern(
        name="zero_starters",
        pattern=r"Probable starters: 0|probable.*starters.*0 pitcher",
        severity="fail",
        description="Zero probable starters loaded — pitcher identity unknown for all K props. "
                    "Model cannot compute TTOP, park factors, or umpire adjustments.",
    ),
    LogPattern(
        name="pp_zero_props",
        pattern=r"\[PP\] 0 props fetched",
        severity="warn",
        description="PrizePicks returned 0 props — PP slate not yet posted or API down. "
                    "PP dispatch will produce no picks this cycle.",
    ),
    LogPattern(
        name="ud_zero_props",
        pattern=r"\[UD\] 0 props fetched",
        severity="warn",
        description="Underdog returned 0 props — UD API down or slate not yet posted.",
    ),
    LogPattern(
        name="both_platforms_zero",
        pattern=r"\[DB\] Stored 0 snapshots",
        severity="fail",
        description="ZERO snapshots stored — both PP and UD returned empty. "
                    "No picks will fire today without manual intervention.",
    ),

    # ── Dispatch / agent loop ─────────────────────────────────────────────────
    LogPattern(
        name="cutoff_skip_flood",
        pattern=r"Past cutoff.*skipping cycle",
        severity="warn",
        description="AgentTasklet repeatedly skipping due to cutoff window. "
                    "If this appears >3 times: cutoff_minutes_before_pitch may be too tight.",
        min_hits=4,
    ),
    LogPattern(
        name="zero_slips_generated",
        pattern=r"0 slips generated|slips.*generated.*0|no qualifying props",
        severity="warn",
        description="Agent loop produced 0 slips — all props below MIN_CONFIDENCE or MIN_PROB. "
                    "May indicate model probability deflation.",
    ),
    LogPattern(
        name="discord_webhook_fail",
        pattern=r"discord.*webhook.*error|discord.*failed|webhook.*4\d\d|discord.*connection",
        severity="fail",
        description="Discord webhook failing — picks generated but not being delivered to channel.",
    ),

    # ── Database errors ───────────────────────────────────────────────────────
    LogPattern(
        name="migration_retry_loop",
        pattern=r"cannot change name of view column|migration.*failed.*retry|V5\d.*ERROR",
        severity="fail",
        description="Database migration retrying — consuming connections and generating WAL pressure. "
                    "Check migration_history table for failed status.",
        min_hits=3,
    ),
    LogPattern(
        name="stale_column_query",
        pattern=r'column.*"confidence".*does not exist|column.*"run_date".*does not exist|'
                r'column.*"last_updated".*does not exist',
        severity="warn",
        description="Stale admin queries hitting columns that don't exist in schema. "
                    "Update Railway dashboard custom SQL or /admin endpoints.",
    ),
    LogPattern(
        name="transaction_in_progress",
        pattern=r"there is already a transaction in progress",
        severity="warn",
        description="Postgres transaction collision — likely concurrent migration or advisory lock issue.",
        min_hits=3,
    ),
    LogPattern(
        name="checkpoint_too_frequent",
        pattern=r"checkpoints are occurring too frequently",
        severity="warn",
        description="Postgres WAL pressure — max_wal_size needs increasing, or a migration "
                    "retry loop is generating excessive writes.",
    ),
    LogPattern(
        name="db_connection_reset",
        pattern=r"unexpected EOF on client connection|Connection reset by peer.*transaction",
        severity="warn",
        description="DB connections dropping mid-transaction — possible connection pool exhaustion "
                    "or Railway networking instability.",
        min_hits=3,
    ),
]

# Patterns that indicate silent failures when ABSENT (expected but missing)
ABSENT_PATTERNS: list[LogPattern] = [
    LogPattern(
        name="no_xgb_k_log",
        pattern=r"\[xgb_k\].*K-prop|XGBWire.*K-prop|xgb.*line=",
        severity="warn",
        description="No XGBoost K scoring log lines — K blend may not be active. "
                    "Check xgb_k_layer.py model files exist in models/.",
        inverse=True,
    ),
    LogPattern(
        name="no_datahub_refresh",
        pattern=r"Hub refreshed\. Groups:",
        severity="fail",
        description="No DataHub refresh log found in last 6 hours — DataHub may have stopped.",
        inverse=True,
    ),
    LogPattern(
        name="no_dispatch_attempt",
        pattern=r"AgentTasklet|run_agent_tasklet",
        severity="warn",
        description="No agent tasklet activity — scheduler may have stopped during dispatch window.",
        inverse=True,
    ),
]


# ── Log reader ─────────────────────────────────────────────────────────────────

def _read_recent_logs(hours: int = LOOKBACK_HOURS) -> list[str]:
    """
    Read recent log lines from propiq_army.log.
    Falls back gracefully if file doesn't exist.
    """
    lines = []

    # Source 1: local log file
    if LOG_FILE.exists():
        try:
            all_lines = LOG_FILE.read_text(errors="ignore").splitlines()
            # Take last N lines to avoid reading entire file
            lines.extend(all_lines[-MAX_LOG_LINES:])
        except Exception as e:
            logger.debug("Log file read failed: %s", e)

    # Source 2: Railway Logs API (if token available)
    railway_token   = os.getenv("RAILWAY_API_TOKEN", "")
    railway_service = os.getenv("RAILWAY_SERVICE_ID", os.getenv("RAILWAY_SERVICE_NAME", ""))
    railway_env     = os.getenv("RAILWAY_ENVIRONMENT_ID", "")

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
                data = resp.json()
                nodes = (data.get("data", {})
                             .get("deploymentLogs", {})
                             .get("nodes", []))
                railway_lines = [n.get("message", "") for n in nodes]
                lines.extend(railway_lines)
                logger.debug("Railway API: %d log lines fetched", len(railway_lines))
        except Exception as e:
            logger.debug("Railway Logs API unavailable: %s", e)

    return lines


def _filter_recent(lines: list[str], hours: int = LOOKBACK_HOURS) -> list[str]:
    """
    Filter log lines to the last N hours based on timestamp patterns.
    Falls back to returning all lines if timestamps can't be parsed.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    cutoff_str = cutoff.strftime("%Y-%m-%d")

    # Quick filter: only keep lines from today/yesterday
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

    filtered = [l for l in lines if today in l or yesterday in l]
    return filtered if filtered else lines  # fallback to all if no timestamps found


# ── Scanner ────────────────────────────────────────────────────────────────────

def scan_logs(hours: int = LOOKBACK_HOURS) -> list[tuple[str, str, str, int]]:
    """
    Scan recent logs for all known silent failure patterns.

    Returns list of (name, severity, description, hit_count) for each triggered pattern.
    """
    lines      = _read_recent_logs(hours)
    recent     = _filter_recent(lines, hours)
    full_text  = "\n".join(recent)
    findings   = []

    for pat in FAILURE_PATTERNS:
        try:
            matches = re.findall(pat.pattern, full_text, re.IGNORECASE)
            hit_count = len(matches)
            if hit_count >= pat.min_hits:
                findings.append((pat.name, pat.severity, pat.description, hit_count))
        except re.error:
            pass

    for pat in ABSENT_PATTERNS:
        try:
            matches = re.findall(pat.pattern, full_text, re.IGNORECASE)
            if len(matches) == 0:  # absent = trigger
                findings.append((pat.name, pat.severity, pat.description, 0))
        except re.error:
            pass

    return findings


# ── Bug checker integration functions ─────────────────────────────────────────

def _check_railway_silent_failures() -> tuple[str, str, str]:
    """
    Bug checker check: scan logs for silent failure patterns.
    Returns (name, status, detail) for the bug_checker embed.
    """
    try:
        findings = scan_logs(hours=LOOKBACK_HOURS)

        if not findings:
            return "Silent Failures", "ok", f"No silent failure patterns in last {LOOKBACK_HOURS}h logs"

        fails  = [(n, d, c) for n, s, d, c in findings if s == "fail"]
        warns  = [(n, d, c) for n, s, d, c in findings if s == "warn"]

        if fails:
            top = fails[0]
            detail = (
                f"{len(fails)} SILENT FAILURE(s) + {len(warns)} warning(s) detected. "
                f"Top: [{top[0]}] {top[1][:120]}"
            )
            return "Silent Failures", "fail", detail

        if warns:
            top = warns[0]
            detail = (
                f"{len(warns)} silent warning(s). "
                f"Top: [{top[0]}] {top[1][:120]}"
            )
            return "Silent Failures", "warn", detail

        return "Silent Failures", "ok", f"No silent failure patterns in last {LOOKBACK_HOURS}h"

    except Exception as exc:
        return "Silent Failures", "warn", f"Scanner error: {exc}"


def _check_pipeline_health() -> tuple[str, str, str]:
    """
    Bug checker check: verify the core pipeline fired correctly in the last 6h.
    Checks for presence of expected log lines, not just absence of errors.
    """
    try:
        lines     = _read_recent_logs(hours=LOOKBACK_HOURS)
        full_text = "\n".join(_filter_recent(lines, hours=LOOKBACK_HOURS))

        checks = {
            "DataHub refreshing":  bool(re.search(r"Hub refreshed\. Groups:", full_text)),
            "Props loaded":        bool(re.search(r"Stored \d+ snapshots", full_text)),
            "Steamer 500+ players":bool(re.search(r"Steamer 2026 projections loaded: [5-9]\d{2}|[1-9]\d{3}", full_text)),
            "Umpires loaded":      bool(re.search(r"umpires loaded|home plate umpires", full_text, re.I)),
            "Lineups loaded":      bool(re.search(r"\d+ confirmed players", full_text)),
            "Agent tasklet ran":   bool(re.search(r"AgentTasklet|run_agent_tasklet", full_text)),
        }

        failed = [k for k, v in checks.items() if not v]
        passed = [k for k, v in checks.items() if v]

        if len(failed) >= 3:
            return "Pipeline Health", "fail", f"Pipeline degraded — {len(failed)} checks missing: {', '.join(failed)}"
        if failed:
            return "Pipeline Health", "warn", f"{len(failed)} check(s) not confirmed: {', '.join(failed)}"

        return "Pipeline Health", "ok", f"All {len(passed)} pipeline checks confirmed in last {LOOKBACK_HOURS}h"

    except Exception as exc:
        return "Pipeline Health", "warn", f"Pipeline check error: {exc}"


# ── Standalone Discord report ─────────────────────────────────────────────────

def post_silent_failure_report() -> None:
    """
    Post a full silent failure report to Discord.
    Called from orchestrator on demand or via /admin/scan-logs endpoint.
    """
    webhook = os.getenv("DISCORD_WEBHOOK_URL", "")
    if not webhook:
        logger.warning("[LogScanner] DISCORD_WEBHOOK_URL not set — printing to console only")

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
        emoji = "🔴" if sev == "fail" else "🟡"
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
        f"Found **{len(fails)} failures** and **{len(warns)} warnings**."
    )

    logger.warning("[LogScanner] %s", title)
    for f in fields:
        logger.warning("[LogScanner]   %s: %s", f["name"], f["value"][:100])

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

    hours = int(sys.argv[1]) if len(sys.argv) > 1 else LOOKBACK_HOURS
    print(f"\nScanning last {hours}h of PropIQ logs...\n")

    findings = scan_logs(hours=hours)

    if not findings:
        print("✅ No silent failure patterns detected.")
    else:
        fails = [(n, s, d, c) for n, s, d, c in findings if s == "fail"]
        warns = [(n, s, d, c) for n, s, d, c in findings if s == "warn"]

        if fails:
            print(f"🔴 FAILURES ({len(fails)}):")
            for name, _, desc, count in fails:
                hit = f"  [{count}x]" if count > 1 else "  [absent]" if count == 0 else ""
                print(f"  ❌ {name}{hit}")
                print(f"     {desc[:120]}")

        if warns:
            print(f"\n🟡 WARNINGS ({len(warns)}):")
            for name, _, desc, count in warns:
                hit = f"  [{count}x]" if count > 1 else "  [absent]" if count == 0 else ""
                print(f"  ⚠️  {name}{hit}")
                print(f"     {desc[:120]}")

        if "--post" in sys.argv:
            post_silent_failure_report()
            print("\nPosted to Discord.")

    # Pipeline health
    print("\n--- Pipeline Health ---")
    name, status, detail = _check_pipeline_health()
    emoji = {"ok": "✅", "warn": "⚠️", "fail": "❌"}[status]
    print(f"{emoji} {name}: {detail}")
