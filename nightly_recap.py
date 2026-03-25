"""
nightly_recap.py
================
Runs at midnight ET every night.

1. Fetches actual MLB player stats from ESPN for yesterday's games
2. Settles all PENDING parlays from that date (WIN / LOSS / PUSH)
3. Posts a summary recap embed to Discord
4. Updates the propiq_season_record table with final results

Run directly: python3 nightly_recap.py [YYYY-MM-DD]
If no date given, defaults to yesterday (UTC-7).
"""

from __future__ import annotations

import json
import logging
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Optional

from espn_scraper import get_all_player_stats
from settlement_engine import settle_parlay
from season_record import (
    get_pending_parlays,
    settle_parlay_record,
    get_overall_season_stats,
)
from clv_tracker import get_daily_clv_summary

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DISCORD_WEBHOOK = (
    "https://discordapp.com/api/webhooks/1484795164961800374/"
    "jYxCVWeN8F1TFIs9SFjQtr0lZASPitLRnGBwjD3Oo2CknXOqVZB2gmmLqqQ1eH-_2liM"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger("nightly_recap")

# Emoji map for agent names
_AGENT_EMOJI: dict[str, str] = {
    "EVHunter":      "🎯",
    "UnderMachine":  "🔽",
    "F5Agent":       "5️⃣",
    "MLEdgeAgent":   "🧠",
    "UmpireAgent":   "⚖️",
    "FadeAgent":     "👻",
    "LineValueAgent": "📊",
    "BullpenAgent":  "🔥",
    "WeatherAgent":  "🌬️",
    "SteamAgent":    "♨️",
    "ArsenalAgent":  "⚔️",
    "PlatoonAgent":  "🤝",
    "CatcherAgent":  "🧤",
    "LineupAgent":   "📋",
    "GetawayAgent":  "✈️",
    "ArbitrageAgent": "💰",
    "VultureStack":  "🦅",
    "OmegaStack":    "🔱",
}

_OUTCOME_EMOJI = {"WIN": "✅", "LOSS": "❌", "PUSH": "⏩"}


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

    # Color: green=WIN majority, red=LOSS majority, grey=all push / no bets
    if total == 0:
        color = 0x95A5A6  # grey
    elif wins > losses:
        color = 0x2ECC71  # green
    elif losses > wins:
        color = 0xE74C3C  # red
    else:
        color = 0xF39C12  # yellow

    # Build per-parlay fields (max 24 fields, Discord limit)
    fields = []
    for r in results[:23]:
        emoji = _AGENT_EMOJI.get(r["agent_name"], "🤖")
        outcome_emoji = _OUTCOME_EMOJI.get(r["outcome"], "❓")
        profit = r["units_profit"]
        profit_str = f"{'+' if profit >= 0 else ''}{profit:.1f}u"

        # Leg summary (max 3 lines)
        leg_lines = []
        for leg in r.get("legs", [])[:4]:
            act = leg.get("actual", -1)
            act_str = f" (actual: {act:.0f})" if act >= 0 else ""
            leg_lines.append(
                f"• {leg['player_name']} {leg['side']} {leg['line']} {leg['prop_type']}{act_str}"
            )

        fields.append({
            "name":   f"{outcome_emoji} {emoji} {r['agent_name']} — {profit_str}",
            "value":  "\n".join(leg_lines) or "No leg details available",
            "inline": False,
        })

    # Optional CLV summary field
    if clv_summary and clv_summary.get("available"):
        beat_pct = clv_summary["beat_pct"]
        avg_clv = clv_summary["avg_clv_pts"]
        clv_icon = "📈" if beat_pct >= 55 else ("➡️" if beat_pct >= 45 else "📉")
        fields.append({
            "name": f"{clv_icon} Closing Line Value",
            "value": (
                f"Beat close on **{clv_summary['beat_close']}/{clv_summary['total_legs']} legs "
                f"({beat_pct:.0f}%)** · "
                f"Avg CLV: **{'+' if avg_clv >= 0 else ''}{avg_clv:.2f}**"
            ),
            "inline": False,
        })

    # Season stats footer
    season_record = season_stats.get("record", "0W-0L-0P")
    season_units  = season_stats.get("units_profit", 0.0)
    season_roi    = season_stats.get("roi_pct", 0.0)
    pending_count = season_stats.get("pending", 0)

    embed = {
        "embeds": [{
            "title": f"📊 PropIQ Nightly Recap — {date_str}",
            "description": (
                f"**Today:** {day_record} · {day_units} · {total} parlays settled\n"
                f"{'No parlays sent today.' if total == 0 else ''}"
            ),
            "color": color,
            "fields": fields,
            "footer": {
                "text": (
                    f"Season: {season_record} · "
                    f"{'+' if season_units >= 0 else ''}{season_units:.1f}u · "
                    f"ROI: {'+' if season_roi >= 0 else ''}{season_roi:.1f}% · "
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
    settle_date: 'YYYY-MM-DD' — defaults to yesterday ET.
    """
    if settle_date is None:
        et_offset  = timedelta(hours=7)  # UTC-7 (PDT)
        yesterday  = datetime.now(timezone.utc) - et_offset - timedelta(days=1)
        settle_date = yesterday.strftime("%Y-%m-%d")

    logger.info("=== PropIQ Nightly Settlement: %s ===", settle_date)

    # 1. Fetch all PENDING parlays for this date
    pending = get_pending_parlays(settle_date)
    if not pending:
        logger.info("No PENDING parlays for %s — nothing to settle", settle_date)
        # Still post a recap showing no action today
        season_stats = get_overall_season_stats()
        clv_summary = get_daily_clv_summary(settle_date)
        embed = _build_recap_embed(settle_date, [], season_stats, clv_summary)
        _send_discord_embed(embed)
        return

    logger.info("Found %d PENDING parlays for %s", len(pending), settle_date)

    # 2. Fetch ESPN stats (date in YYYYMMDD format)
    espn_date = settle_date.replace("-", "")
    player_stats = get_all_player_stats(espn_date)
    if not player_stats:
        logger.warning("ESPN returned no stats for %s — aborting settlement", settle_date)
        return

    # 3. Settle each parlay
    settled_results = []
    for parlay_row in pending:
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

        # Update DB
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

        settled_results.append({
            "parlay_id":    parlay_id,
            "agent_name":   agent_name,
            "outcome":      result.outcome,
            "units_profit": result.units_profit,
            "legs":         leg_summaries,
        })

        time.sleep(0.1)  # small pause between DB writes

    # 4. Fetch updated season stats
    season_stats = get_overall_season_stats()

    # 5. Fetch CLV summary (available if line_stream ran today)
    clv_summary = get_daily_clv_summary(settle_date)

    # 6. Post Discord recap
    embed = _build_recap_embed(settle_date, settled_results, season_stats, clv_summary)
    ok = _send_discord_embed(embed)
    if ok:
        logger.info("Recap sent to Discord for %s", settle_date)
    else:
        logger.error("Failed to send recap to Discord for %s", settle_date)

    # Summary
    wins   = sum(1 for r in settled_results if r["outcome"] == "WIN")
    losses = sum(1 for r in settled_results if r["outcome"] == "LOSS")
    pushes = sum(1 for r in settled_results if r["outcome"] == "PUSH")
    units  = sum(r["units_profit"] for r in settled_results)
    logger.info(
        "=== Settlement complete: %dW-%dL-%dP  %+.1fu ===",
        wins, losses, pushes, units,
    )

    # ── StreakAgent settlement (19th agent) ────────────────────────────────
    # Grade today's Streaks pick via ESPN box scores, update streak state,
    # and post a settlement embed to Discord.
    try:
        from streak_agent import settle_streak_picks
        logger.info("[StreakAgent] Running streak settlement for %s", settle_date)
        settle_streak_picks(settle_date)
    except ImportError:
        logger.debug("[StreakAgent] streak_agent.py not found — skipping settlement.")
    except Exception as _streak_settle_err:
        logger.warning("[StreakAgent] Settlement error: %s", _streak_settle_err)


if __name__ == "__main__":
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    run(date_arg)
