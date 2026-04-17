"""
nightly_recap.py
================
Runs at 11:00 PM PT every night.

1. Fetches actual MLB player stats from ESPN for yesterday's games
2. Settles all PENDING parlays from that date (WIN / LOSS / PUSH)
3. Posts a summary recap embed to Discord
4. Updates the propiq_season_record table with final results

Run directly: python3 nightly_recap.py [YYYY-MM-DD]
If no date given, defaults to yesterday (America/Los_Angeles, DST-aware).
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Optional

from espn_scraper import get_all_player_stats, get_game_states
from settlement_engine import settle_parlay
from season_record import (
    get_pending_parlays,
    get_all_pending_parlays,
    settle_parlay_record,
    get_overall_season_stats,
)
from clv_tracker import get_daily_clv_summary

# Phase 94: CLV feedback engine — adaptive thresholds + bet_ledger population
try:
    from clv_feedback_engine import rebuild_thresholds as _rebuild_thresholds, build_discord_summary as _build_edge_summary
    _CLV_FEEDBACK_AVAILABLE = True
except ImportError:
    _CLV_FEEDBACK_AVAILABLE = False
    def _rebuild_thresholds(): return {}
    def _build_edge_summary(): return ""

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
_WEBHOOK_FALLBACK = (
    "https://discordapp.com/api/webhooks/1484795164961800374/"
    "jYxCVWeN8F1TFIs9SFjQtr0lZASPitLRnGBwjD3Oo2CknXOqVZB2gmmLqqQ1eH-_2liM"
)
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_URL", _WEBHOOK_FALLBACK)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger("nightly_recap")

# Emoji map for agent names
_AGENT_EMOJI: dict[str, str] = {
    "EVHunter":              "🎯",
    "UnderMachine":          "🔽",
    "F5Agent":               "5️⃣",
    "MLEdgeAgent":           "🧠",
    "UmpireAgent":           "⚖️",
    "FadeAgent":             "👻",
    "LineValueAgent":        "📐",
    "BullpenAgent":          "🔥",
    "WeatherAgent":          "🌬️",
    "UnderDogAgent":         "🐕",
    "StackSmithAgent":       "🏗️",
    "ChalkBusterAgent":      "💥",
    "SharpFadeAgent":        "📡",
    "CorrelatedParlayAgent": "🔗",
    "PropCycleAgent":        "🔄",
    "LineupChaseAgent":      "🎣",
    "LineDriftAgent":        "📈",
    "StreakAgent":            "⚡",
}

# FIX: canonical list of active agents — filter out phantom legacy picks
_ACTIVE_AGENTS = set(_AGENT_EMOJI.keys())

_OUTCOME_EMOJI = {"WIN": "\u2705", "LOSS": "\u274c", "PUSH": "\u23e9"}


# ---------------------------------------------------------------------------
# DST-aware "yesterday in PT" helper
# ---------------------------------------------------------------------------

def _yesterday_pt() -> str:
    """Return yesterday's date as YYYY-MM-DD in America/Los_Angeles, DST-aware."""
    try:
        import pytz  # noqa: PLC0415
        pt_tz = pytz.timezone("America/Los_Angeles")
        now_pt = datetime.now(pt_tz)
        yesterday_pt = now_pt - timedelta(days=1)
        return yesterday_pt.strftime("%Y-%m-%d")
    except ImportError:
        pass
    from zoneinfo import ZoneInfo as _ZI  # noqa: PLC0415
    now_pt_approx = datetime.now(_ZI("America/Los_Angeles"))
    yesterday_approx = now_pt_approx - timedelta(days=1)
    return yesterday_approx.strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# PR #333 FIX 1: settlement_date_log dedup guard
# Prevents double-send when APScheduler misfires on Railway restart at ~11 PM.
# Same pattern as dispatch_date_log in orchestrator.py.
# ---------------------------------------------------------------------------

def _settlement_already_ran(settle_date: str) -> bool:
    """
    Returns True if nightly settlement for settle_date already completed.
    Uses Postgres `settlement_date_log` table — survives Railway restarts.
    """
    import psycopg2  # noqa: PLC0415
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        return False
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS settlement_date_log (
                settle_date DATE PRIMARY KEY,
                ran_at      TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        conn.commit()
        cur.execute(
            "SELECT 1 FROM settlement_date_log WHERE settle_date = %s",
            (settle_date,)
        )
        already = cur.fetchone() is not None
        cur.close()
        conn.close()
        return already
    except Exception as exc:
        logger.warning("[Recap] settlement_date_log check failed: %s", exc)
        return False


def _record_settlement_ran(settle_date: str) -> None:
    """Mark settle_date as done in settlement_date_log."""
    import psycopg2  # noqa: PLC0415
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        return
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO settlement_date_log (settle_date) VALUES (%s) ON CONFLICT DO NOTHING",
            (settle_date,)
        )
        conn.commit()
        cur.close()
        conn.close()
        logger.info("[Recap] Settlement date recorded: %s", settle_date)
    except Exception as exc:
        logger.warning("[Recap] _record_settlement_ran failed: %s", exc)


# ---------------------------------------------------------------------------
# Discord helpers
# ---------------------------------------------------------------------------

def _send_discord_embed(payload: dict) -> bool:
    """POST a Discord embed payload to the webhook."""
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        DISCORD_WEBHOOK,
        data=data,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "PropIQ/1.0",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status in (200, 204)
    except Exception as exc:
        logger.error("Discord send failed: %s", exc)
        return False


def _build_recap_embed(
    date_str: str,
    results: list[dict],
    season_stats: dict,
    clv_summary: dict | None = None,
) -> dict:
    """Build the nightly recap Discord embed."""
    wins   = sum(1 for r in results if r["outcome"] == "WIN")
    losses = sum(1 for r in results if r["outcome"] == "LOSS")
    pushes = sum(1 for r in results if r["outcome"] == "PUSH")
    units  = sum(r["units_profit"] for r in results)
    total  = len(results)

    day_record = f"{wins}W-{losses}L-{pushes}P"
    day_units  = f"{'+' if units >= 0 else ''}{units:.1f}u"

    if total == 0:
        color = 0x95A5A6
    elif wins > losses:
        color = 0x2ECC71
    elif losses > wins:
        color = 0xE74C3C
    else:
        color = 0xF39C12

    fields = []
    for r in results[:23]:
        emoji = _AGENT_EMOJI.get(r["agent_name"], "\U0001f916")
        outcome_emoji = _OUTCOME_EMOJI.get(r["outcome"], "\u2753")
        profit = r["units_profit"]
        profit_str = f"{'+' if profit >= 0 else ''}{profit:.1f}u"

        leg_lines = []
        for leg in r.get("legs", [])[:4]:
            act = leg.get("actual", -1)
            act_str = f" (actual: {act:.0f})" if act >= 0 else ""
            leg_lines.append(
                f"\u2022 {leg['player_name']} {leg['side']} {leg['line']} {leg['prop_type']}{act_str}"
            )

        fields.append({
            "name":   f"{outcome_emoji} {emoji} {r['agent_name']} \u2014 {profit_str}",
            "value":  "\n".join(leg_lines) or "No leg details available",
            "inline": False,
        })

    if clv_summary and clv_summary.get("available"):
        beat_pct = clv_summary["beat_pct"]
        avg_clv = clv_summary["avg_clv_pts"]
        clv_icon = "\U0001f4c8" if beat_pct >= 55 else ("\u27a1\ufe0f" if beat_pct >= 45 else "\U0001f4c9")
        fields.append({
            "name": f"{clv_icon} Closing Line Value",
            "value": (
                f"Beat close on **{clv_summary['beat_close']}/{clv_summary['total_legs']} legs "
                f"({beat_pct:.0f}%)** \u00b7 "
                f"Avg CLV: **{'+' if avg_clv >= 0 else ''}{avg_clv:.2f}**"
            ),
            "inline": False,
        })

    if _CLV_FEEDBACK_AVAILABLE:
        edge_summary = _build_edge_summary()
        if edge_summary and edge_summary != "No edge threshold data yet.":
            fields.append({
                "name": "\U0001f3af Edge Threshold Health",
                "value": edge_summary[:1024],
                "inline": False,
            })

    _sw = season_stats.get("wins",         0)
    _sl = season_stats.get("losses",       0)
    _sp = season_stats.get("pushes",       0)
    season_record = f"{_sw}W-{_sl}L-{_sp}P"
    season_units  = round(
        season_stats.get("total_payout", 0.0) - season_stats.get("total_staked", 0.0), 1
    )
    season_roi    = season_stats.get("roi_pct",  0.0)
    pending_count = season_stats.get("pending",  0)

    embed = {
        "embeds": [{
            "title": f"\U0001f4ca PropIQ Nightly Recap \u2014 {date_str}",
            "description": (
                f"**Today:** {day_record} \u00b7 {day_units} \u00b7 {total} parlays settled\n"
                f"{'⏳ Parlays pending — games still in progress.' if total == 0 and pending_count > 0 else ('No parlays sent today.' if total == 0 else '')}"
            ),
            "color": color,
            "fields": fields,
            "footer": {
                "text": (
                    f"Season: {season_record} \u00b7 "
                    f"{'+' if season_units >= 0 else ''}{season_units:.1f}u \u00b7 "
                    f"ROI: {'+' if season_roi >= 0 else ''}{season_roi:.1f}% \u00b7 "
                    f"{pending_count} pending"
                )
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }]
    }
    return embed


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(settle_date: Optional[str] = None) -> None:
    """
    settle_date: 'YYYY-MM-DD' — defaults to yesterday PT (DST-aware).
    """
    if settle_date is None:
        settle_date = _yesterday_pt()

    logger.info("=== PropIQ Nightly Settlement: %s ===", settle_date)

    # PR #333 FIX 1: dedup guard — one settlement per date, survives Railway restarts
    if _settlement_already_ran(settle_date):
        logger.info(
            "[Recap] Settlement for %s already ran — skipping duplicate. "
            "(APScheduler misfire on redeploy suppressed.)",
            settle_date,
        )
        return

    # 1. Fetch all PENDING parlays across all dates (rollover-aware)
    pending = get_all_pending_parlays()
    logger.info("[Recap] Found %d PENDING parlays in propiq_season_record for settlement.", len(pending))
    _phantom_count = len([p for p in pending if p.get("agent_name") not in _ACTIVE_AGENTS])
    if _phantom_count:
        logger.warning(
            "[Recap] Filtering %d parlays from legacy agents not in current roster: %s",
            _phantom_count,
            list({p["agent_name"] for p in pending if p.get("agent_name") not in _ACTIVE_AGENTS}),
        )
    pending = [p for p in pending if p.get("agent_name") in _ACTIVE_AGENTS]
    if not pending:
        logger.info("No PENDING parlays for %s — nothing to settle", settle_date)
        season_stats = get_overall_season_stats()
        clv_summary = get_daily_clv_summary(settle_date)
        embed = _build_recap_embed(settle_date, [], season_stats, clv_summary)
        _send_discord_embed(embed)
        # PR #333: still record so restarts don't send this zero-recap twice
        _record_settlement_ran(settle_date)
        return

    logger.info("Found %d PENDING parlays for %s", len(pending), settle_date)

    # 2. Fetch ESPN stats
    espn_date = settle_date.replace("-", "")
    player_stats = get_all_player_stats(espn_date)
    if not player_stats:
        logger.warning("ESPN returned no stats for %s — aborting settlement", settle_date)
        return

    # 3. Settle each parlay
    settled_results = []
    for parlay_row in pending:
        parlay_date = parlay_row.get("date", settle_date)
        espn_parlay_date = parlay_date.replace("-", "") if isinstance(parlay_date, str) else settle_date.replace("-", "")
        parlay_games = get_game_states(espn_parlay_date)
        all_final = all(g["status"] == "FINAL" for g in parlay_games) if parlay_games else True
        today_et = datetime.now(ZoneInfo("America/Los_Angeles")).date()
        parlay_dt = datetime.strptime(parlay_date, "%Y-%m-%d").date() if isinstance(parlay_date, str) else today_et
        days_old = (today_et - parlay_dt).days
        if not all_final and days_old < 2:
            logger.info(
                "[Rollover] Parlay %s from %s skipped — games not yet FINAL (age=%d day(s))",
                parlay_row.get("id"), parlay_date, days_old
            )
            continue
        if not all_final and days_old >= 2:
            logger.warning(
                "[Rollover] Parlay %s from %s force-pushed after %d days without FINAL stats",
                parlay_row.get("id"), parlay_date, days_old
            )

        parlay_id  = parlay_row["id"]
        agent_name = parlay_row["agent_name"]
        stake      = parlay_row["stake"]
        legs       = parlay_row["legs"]

        result = settle_parlay(
            parlay_id=parlay_id,
            agent_name=agent_name,
            date=settle_date,
            stake=stake,
            legs_data=legs,
            player_stats=player_stats,
        )

        settle_parlay_record(
            parlay_id=parlay_id,
            status=result.outcome,
            units_profit=result.units_profit,
        )

        logger.info(
            "[%s] %s → %s (%+.1fu)",
            agent_name, parlay_id, result.outcome, result.units_profit,
        )

        leg_summaries = [
            {
                "player_name": lr.player_name,
                "prop_type":   lr.prop_type,
                "side":        lr.side,
                "line":        lr.line,
                "actual":      lr.actual,
                "outcome":     lr.outcome,
            }
            for lr in result.legs
        ]

        # Phase 94: Populate bet_ledger for each settled leg
        if _CLV_FEEDBACK_AVAILABLE:
            try:
                import os, psycopg2
                _db_url = os.environ.get("DATABASE_URL", "")
                if _db_url:
                    _conn = psycopg2.connect(_db_url, sslmode="require")
                    _cur  = _conn.cursor()
                    for _lr in result.legs:
                        _actual_outcome = 1 if _lr.outcome == "WIN" else (0 if _lr.outcome == "LOSS" else None)
                        _cur.execute(
                            """
                            INSERT INTO bet_ledger
                                (bet_date, agent_name, player_name, prop_type, side, line,
                                 actual_outcome, profit_loss, status, created_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                            ON CONFLICT (player_name, prop_type, line, side, agent_name, bet_date)
                            DO UPDATE SET
                                actual_outcome = EXCLUDED.actual_outcome,
                                status         = EXCLUDED.status,
                                profit_loss    = EXCLUDED.profit_loss
                            WHERE bet_ledger.actual_outcome IS NULL
                            """,
                            (
                                settle_date,
                                agent_name,
                                _lr.player_name,
                                _lr.prop_type,
                                _lr.side,
                                _lr.line,
                                _actual_outcome,
                                result.units_profit / max(len(result.legs), 1),
                                _lr.outcome,
                            ),
                        )
                    _conn.commit()
                    _conn.close()
            except Exception as _ledger_err:
                logger.warning("[Phase94] bet_ledger insert error: %s", _ledger_err)

        if agent_name not in _ACTIVE_AGENTS:
            logger.info("[Recap] Skipping ghost agent %s — not in current roster.", agent_name)
            continue

        settled_results.append({
            "parlay_id":    parlay_id,
            "agent_name":   agent_name,
            "outcome":      result.outcome,
            "units_profit": result.units_profit,
            "legs":         leg_summaries,
        })

        time.sleep(0.1)

    # 4. Fetch updated season stats
    season_stats = get_overall_season_stats()

    # Phase 94: Rebuild adaptive edge thresholds
    if _CLV_FEEDBACK_AVAILABLE:
        try:
            logger.info("[Phase94] Rebuilding edge thresholds from bet_ledger...")
            updated = _rebuild_thresholds()
            logger.info("[Phase94] Rebuilt %d edge threshold overrides.", len(updated))
        except Exception as _thresh_err:
            logger.warning("[Phase94] rebuild_thresholds error: %s", _thresh_err)

    # 5. Fetch CLV summary
    clv_summary = get_daily_clv_summary(settle_date)

    # 6. Post Discord recap
    embed = _build_recap_embed(settle_date, settled_results, season_stats, clv_summary)
    ok = _send_discord_embed(embed)
    if ok:
        logger.info("Recap sent to Discord for %s", settle_date)
        # PR #333: mark settled AFTER successful Discord send — dedup from here forward
        _record_settlement_ran(settle_date)
    else:
        logger.error("Failed to send recap to Discord for %s", settle_date)

    wins   = sum(1 for r in settled_results if r["outcome"] == "WIN")
    losses = sum(1 for r in settled_results if r["outcome"] == "LOSS")
    pushes = sum(1 for r in settled_results if r["outcome"] == "PUSH")
    units  = sum(r["units_profit"] for r in settled_results)
    logger.info(
        "=== Settlement complete: %dW-%dL-%dP  %+.1fu ===",
        wins, losses, pushes, units,
    )

    # ── StreakAgent settlement ────────────────────────────────────────────────
    try:
        from streak_agent import settle_streak_picks
        logger.info("[StreakAgent] Running streak settlement for %s", settle_date)
        settle_streak_picks(settle_date)
    except ImportError:
        logger.debug("[StreakAgent] streak_agent.py not found — skipping settlement.")
    except Exception as _streak_settle_err:
        logger.warning("[StreakAgent] Settlement error: %s", _streak_settle_err)

    # ── Calibration + Edge Health ─────────────────────────────────────────────
    try:
        from calibration_monitor import run as run_calibration
        logger.info("[Phase35] Running calibration monitor (30-day window)...")
        run_calibration(days=30, quiet=False)
    except ImportError:
        logger.debug("[Phase35] calibration_monitor.py not found — skipping.")
    except Exception as _cal_err:
        logger.warning("[Phase35] Calibration monitor error: %s", _cal_err)

    try:
        from edge_health_monitor import run as run_edge_health
        from risk_manager import RiskManager
        logger.info("[Phase35] Running edge health monitor...")
        edge_metrics = run_edge_health(days=30, quiet=False)
        if edge_metrics:
            rm = RiskManager()
            rm.check_and_apply_cool_downs(edge_metrics)
            logger.info("[Phase35] Cool-down check complete for %d agents", len(edge_metrics))
    except ImportError:
        logger.debug("[Phase35] edge_health_monitor.py not found — skipping.")
    except Exception as _eh_err:
        logger.warning("[Phase35] Edge health monitor error: %s", _eh_err)


if __name__ == "__main__":
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    run(date_arg)
