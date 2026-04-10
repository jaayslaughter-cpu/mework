"""
bug_checker.py — PropIQ daily health check
Runs at 10:00 AM PT. Posts a color-coded Discord embed.
Green = all clear. Orange = warnings. Red = failures.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Any

import pytz

logger = logging.getLogger(__name__)

# ── Banned prop types — must NEVER appear in sent bets ───────────────────────
BANNED_PROPS = {"stolen_bases", "home_runs", "walks", "walks_allowed", "doubles", "triples"}

# ── Discord ───────────────────────────────────────────────────────────────────
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_URL", "")

_PT = pytz.timezone("America/Los_Angeles")


def _pt_today() -> str:
    return datetime.now(_PT).strftime("%Y-%m-%d")


def _pt_now_str() -> str:
    return datetime.now(_PT).strftime("%Y-%m-%d %H:%M PT")


# ── Checks ────────────────────────────────────────────────────────────────────

def _check_postgres() -> tuple[str, str, str]:
    """Returns (name, status, detail). status: ok | warn | fail"""
    try:
        import psycopg2  # type: ignore
        url = os.getenv("POSTGRES_URL", os.getenv("DATABASE_URL", ""))
        if not url:
            return "Postgres", "fail", "DATABASE_URL not set"
        conn = psycopg2.connect(url)
        cur = conn.cursor()
        # Check required tables exist
        required = [
            "bet_ledger", "ud_streak_state", "propiq_season_record",
            "startup_ping_log", "agent_leaderboard",
        ]
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema='public'"
        )
        existing = {r[0] for r in cur.fetchall()}
        missing = [t for t in required if t not in existing]
        # Check bet_ledger columns
        col_issues = []
        if "bet_ledger" in existing:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name='bet_ledger'"
            )
            cols = {r[0] for r in cur.fetchall()}
            for c in ("mlbam_id", "entry_type", "units_wagered", "discord_sent"):
                if c not in cols:
                    col_issues.append(c)
        conn.close()
        if missing or col_issues:
            detail = ""
            if missing:
                detail += f"Missing tables: {', '.join(missing)}. "
            if col_issues:
                detail += f"bet_ledger missing columns: {', '.join(col_issues)}."
            return "Postgres", "warn", detail.strip()
        return "Postgres", "ok", f"Connected. {len(existing)} tables. Required schema OK."
    except Exception as exc:
        return "Postgres", "fail", str(exc)


def _check_redis() -> tuple[str, str, str]:
    try:
        import redis  # type: ignore
        url = os.getenv("REDIS_URL", os.getenv("REDIS_PRIVATE_URL", ""))
        if not url:
            return "Redis", "warn", "REDIS_URL not set — DataHub will fall back to local"
        r = redis.from_url(url, socket_timeout=3)
        r.ping()
        return "Redis", "ok", "PING OK"
    except Exception as exc:
        return "Redis", "fail", str(exc)


def _check_datahub() -> tuple[str, str, str]:
    try:
        import redis  # type: ignore
        import json
        url = os.getenv("REDIS_URL", os.getenv("REDIS_PRIVATE_URL", ""))
        if not url:
            return "DataHub", "warn", "Redis unavailable — cannot check hub"
        r = redis.from_url(url, socket_timeout=3)
        raw = r.get("hub:dfs")
        if not raw:
            return "DataHub", "fail", "hub:dfs key missing — DataHub has not populated"
        hub = json.loads(raw)
        groups = {
            "physics": bool(hub.get("physics")),
            "context": bool(hub.get("context")),
            "market":  bool(hub.get("market")),
            "dfs":     bool(hub.get("dfs")),
        }
        false_groups = [k for k, v in groups.items() if not v]
        if false_groups:
            return "DataHub", "warn", f"Groups not populated: {', '.join(false_groups)}"
        return "DataHub", "ok", "All 4 groups populated: physics context market dfs"
    except Exception as exc:
        return "DataHub", "fail", str(exc)


def _check_dispatch_fired() -> tuple[str, str, str]:
    """Check bet_ledger for a record from today (PT)."""
    try:
        import psycopg2  # type: ignore
        url = os.getenv("POSTGRES_URL", os.getenv("DATABASE_URL", ""))
        if not url:
            return "Dispatch", "warn", "Cannot verify — no DB URL"
        today = _pt_today()
        conn = psycopg2.connect(url)
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM bet_ledger WHERE DATE(created_at AT TIME ZONE 'America/Los_Angeles') = %s",
            (today,),
        )
        count = cur.fetchone()[0]
        conn.close()
        if count == 0:
            return "Dispatch", "warn", f"No bets recorded for {today} — dispatch may not have fired or all fell below gate"
        return "Dispatch", "ok", f"{count} bet record(s) saved for {today}"
    except Exception as exc:
        return "Dispatch", "warn", f"Could not query bet_ledger: {exc}"


def _check_banned_props() -> tuple[str, str, str]:
    """Check bet_ledger for any leaked banned prop types sent today."""
    try:
        import psycopg2  # type: ignore
        url = os.getenv("POSTGRES_URL", os.getenv("DATABASE_URL", ""))
        if not url:
            return "Banned Props", "warn", "Cannot verify — no DB URL"
        today = _pt_today()
        conn = psycopg2.connect(url)
        cur = conn.cursor()
        # Check legs stored in bet_ledger — prop_type column
        cur.execute(
            "SELECT DISTINCT prop_type FROM bet_ledger "
            "WHERE DATE(created_at AT TIME ZONE 'America/Los_Angeles') = %s",
            (today,),
        )
        rows = cur.fetchall()
        conn.close()
        leaked = [r[0] for r in rows if r[0] and r[0] in BANNED_PROPS]
        if leaked:
            return "Banned Props", "fail", f"LEAKED today: {', '.join(leaked)}"
        return "Banned Props", "ok", "No banned prop types in today's bets"
    except Exception as exc:
        return "Banned Props", "warn", f"Could not verify: {exc}"


def _check_draftedge() -> tuple[str, str, str]:
    """Check if DraftEdge data is available (non-empty) in DataHub."""
    try:
        import redis  # type: ignore
        import json
        url = os.getenv("REDIS_URL", os.getenv("REDIS_PRIVATE_URL", ""))
        if not url:
            return "DraftEdge", "warn", "Redis unavailable"
        r = redis.from_url(url, socket_timeout=3)
        raw = r.get("hub:dfs")
        if not raw:
            return "DraftEdge", "warn", "hub:dfs missing"
        hub = json.loads(raw)
        de = hub.get("dfs", {}).get("draftedge", {})
        count = len(de) if isinstance(de, dict) else 0
        if count == 0:
            return "DraftEdge", "warn", "DraftEdge returned 0 players — flat base rates expected"
        return "DraftEdge", "ok", f"{count} players with DraftEdge data"
    except Exception as exc:
        return "DraftEdge", "warn", str(exc)


def _check_odds_api_quota() -> tuple[str, str, str]:
    """Read Odds API quota from Redis (cached by _odds_api_get after each DataHub cycle).
    Does NOT make a live API call — zero quota cost."""
    try:
        import redis  # type: ignore
        url = os.getenv("REDIS_URL", os.getenv("REDIS_PRIVATE_URL", ""))
        if not url:
            return "Odds API", "warn", "REDIS_URL not set — cannot read quota"
        r = redis.from_url(url, socket_timeout=3)
        raw = r.get("odds_api_quota_remaining")
        if raw is None:
            return "Odds API", "warn", "Quota not yet cached — will appear after first DataHub cycle"
        remaining = int(raw)
        if remaining < 50:
            return "Odds API", "fail", f"Critically low: {remaining} requests remaining"
        if remaining < 200:
            return "Odds API", "warn", f"Low: {remaining} requests remaining"
        return "Odds API", "ok", f"{remaining} requests remaining"
    except Exception as exc:
        return "Odds API", "warn", f"Could not read quota from Redis: {exc}"


def _check_sbref_cache() -> tuple[str, str, str]:
    """Check sportsbook reference disk cache (/tmp/sb_ref_YYYY-MM-DD.json).
    sportsbook_reference_layer._CACHE_DIR = '/tmp' — this matches that path."""
    try:
        today = datetime.date.today().strftime("%Y-%m-%d")
        cache_path = f"/tmp/sb_ref_{today}.json"
        if not os.path.exists(cache_path):
            return "SB Reference", "warn", f"Cache file not found for {today} — fetch may not have run"
        size_kb = os.path.getsize(cache_path) // 1024
        with open(cache_path) as f:
            data = json.load(f)
        count = len(data)
        if count == 0:
            return "SB Reference", "warn", "Cache exists but is empty — Odds API returned no prop markets"
        return "SB Reference", "ok", f"{count} entries cached ({size_kb}KB)"
    except Exception as exc:
        return "SB Reference", "warn", f"Cache check failed: {exc}"


def _check_streak_state() -> tuple[str, str, str]:
    """Check ud_streak_state and recent restart count."""
    try:
        import psycopg2  # type: ignore
        url = os.getenv("POSTGRES_URL", os.getenv("DATABASE_URL", ""))
        if not url:
            return "Streak State", "warn", "Cannot check — no DB URL"
        conn = psycopg2.connect(url)
        cur = conn.cursor()
        # Active streaks
        cur.execute("SELECT COUNT(*), MAX(current_count) FROM ud_streak_state WHERE current_count > 0")
        row = cur.fetchone()
        active_count = row[0] or 0
        max_count = row[1] or 0
        # Restarts in last 7 days (resets = count went to 0)
        seven_days_ago = (datetime.now(_PT) - timedelta(days=7)).strftime("%Y-%m-%d")
        cur.execute(
            "SELECT COUNT(*) FROM ud_streak_state "
            "WHERE current_count = 0 AND last_updated >= %s",
            (seven_days_ago,),
        )
        resets = cur.fetchone()[0] or 0
        conn.close()
        detail = f"{active_count} active streak(s), max count={max_count}, {resets} reset(s) last 7d"
        status = "warn" if resets > 10 else "ok"
        return "Streak State", status, detail
    except Exception as exc:
        return "Streak State", "warn", str(exc)


def _check_action_network_cookie() -> tuple[str, str, str]:
    """Check that ACTION_NETWORK_COOKIE is set — required for SharpFadeAgent PRO path."""
    token = os.getenv("ACTION_NETWORK_COOKIE", "").strip()
    if not token:
        return (
            "Action Network Cookie",
            "fail",
            "ACTION_NETWORK_COOKIE not set — SharpFadeAgent on Path 2 fallback only. "
            "Set at Railway SERVICE level (not project level).",
        )
    if len(token) < 20:
        return (
            "Action Network Cookie",
            "warn",
            f"ACTION_NETWORK_COOKIE looks too short ({len(token)} chars) — may be truncated.",
        )
    return "Action Network Cookie", "ok", f"Token present ({len(token)} chars)"


def _check_pythonunbuffered() -> tuple[str, str, str]:
    """Check PYTHONUNBUFFERED is set — required for Railway log streaming."""
    val = os.getenv("PYTHONUNBUFFERED", "").strip()
    if val != "1":
        return (
            "PYTHONUNBUFFERED",
            "warn",
            "PYTHONUNBUFFERED not set to '1' — Railway may show no logs. "
            "Add ENV PYTHONUNBUFFERED=1 to Dockerfile or set as Railway variable.",
        )
    return "PYTHONUNBUFFERED", "ok", "Set correctly"


# ── Discord embed sender ───────────────────────────────────────────────────────

def _post_discord_embed(results: list[tuple[str, str, str]]) -> None:
    import json
    import urllib.request

    webhook = DISCORD_WEBHOOK
    if not webhook:
        logger.warning("[BugChecker] DISCORD_WEBHOOK_URL not set — skipping Discord post")
        return

    failures = [r for r in results if r[1] == "fail"]
    warnings = [r for r in results if r[1] == "warn"]

    if failures:
        color = 0xED4245   # red
        title = "🔴 PropIQ Health Check — ACTION REQUIRED"
    elif warnings:
        color = 0xFEE75C   # yellow/orange
        title = "🟡 PropIQ Health Check — Warnings"
    else:
        color = 0x57F287   # green
        title = "🟢 PropIQ Health Check — All Clear"

    STATUS_EMOJI = {"ok": "✅", "warn": "⚠️", "fail": "❌"}
    fields = []
    for name, status, detail in results:
        fields.append({
            "name": f"{STATUS_EMOJI[status]} {name}",
            "value": detail or "—",
            "inline": False,
        })

    embed = {
        "title": title,
        "color": color,
        "fields": fields,
        "footer": {"text": f"PropIQ · {_pt_now_str()}"},
    }
    payload = json.dumps({"embeds": [embed]}).encode()
    req = urllib.request.Request(
        webhook,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            logger.info("[BugChecker] Discord embed posted — status %s", resp.status)
    except Exception as exc:
        logger.error("[BugChecker] Failed to post Discord embed: %s", exc)


# ── Entry point ───────────────────────────────────────────────────────────────

def run_bug_checker() -> None:
    logger.info("[BugChecker] Starting daily health check at %s", _pt_now_str())

    checks = [
        _check_postgres,
        _check_redis,
        _check_datahub,
        _check_dispatch_fired,
        _check_banned_props,
        _check_draftedge,
        _check_odds_api_quota,
        _check_sbref_cache,
        _check_streak_state,
        _check_action_network_cookie,
        _check_pythonunbuffered,
    ]

    results: list[tuple[str, str, str]] = []
    for check_fn in checks:
        try:
            result = check_fn()
            results.append(result)
            emoji = {"ok": "✅", "warn": "⚠️", "fail": "❌"}[result[1]]
            logger.info("[BugChecker] %s %s — %s", emoji, result[0], result[2])
        except Exception as exc:
            logger.error("[BugChecker] Check %s raised: %s", check_fn.__name__, exc)
            results.append((check_fn.__name__, "fail", str(exc)))

    _post_discord_embed(results)
    logger.info("[BugChecker] Done. OK=%d WARN=%d FAIL=%d",
                sum(1 for r in results if r[1] == "ok"),
                sum(1 for r in results if r[1] == "warn"),
                sum(1 for r in results if r[1] == "fail"))
