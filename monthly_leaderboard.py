"""
monthly_leaderboard.py — Phase 43
Generates and posts the monthly agent performance leaderboard to Discord.

Triggered on the 1st of each month via Tasklet HTTP trigger.
Pulls propiq_season_record for the previous calendar month,
groups by agent, calculates W/L/P, ROI, profit/loss.
Cross-references agent_unit_sizing for current tier/unit info.
Posts a rich Discord embed with rankings + tier badges.
"""

import os
import json
import requests
import psycopg2
from datetime import datetime, timezone, date
from calendar import month_name

DATABASE_URL = os.environ.get("DATABASE_URL")
DISCORD_WEBHOOK = os.environ.get(
    "DISCORD_WEBHOOK",
    "https://discordapp.com/api/webhooks/1484795164961800374/jYxCVWeN8F1TFIs9SFjQtr0lZASPitLRnGBwjD3Oo2CknXOqVZB2gmmLqqQ1eH-_2liM",
)

TIER_EMOJI = {1: "🌱", 2: "🌿", 3: "⭐", 4: "🔥", 5: "👑"}
TIER_DOLLARS = {1: 5.0, 2: 8.0, 3: 12.0, 4: 16.0, 5: 20.0}

# PrizePicks 2-leg flex payout multiplier table
# We use a simple flat unit P&L: win = +unit, loss = -unit (straight picks)
# For parlays: actual payout depends on legs — approximated as +unit per leg won
PRIZEPICKS_PAYOUT_MULTIPLIER = 0.85  # ~85 cents per dollar on average after juice


def _get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def get_previous_month_range() -> tuple:
    """Return (first_day, last_day) strings for previous calendar month."""
    today = date.today()
    first_this_month = today.replace(day=1)
    last_prev = first_this_month.replace(day=1).__class__(
        first_this_month.year if first_this_month.month > 1 else first_this_month.year - 1,
        first_this_month.month - 1 if first_this_month.month > 1 else 12,
        1,
    )
    # Last day of previous month = day before first of this month
    import calendar
    last_day = calendar.monthrange(last_prev.year, last_prev.month)[1]
    first_day = last_prev.strftime("%Y-%m-%d")
    last_day_str = last_prev.replace(day=last_day).strftime("%Y-%m-%d")
    return first_day, last_day_str, last_prev.month, last_prev.year


def fetch_monthly_results(first_day: str, last_day: str) -> list:
    """Pull all settled parlays from propiq_season_record for the month."""
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT agent_name, status, stake, payout, bet_date
            FROM propiq_season_record
            WHERE bet_date >= %s AND bet_date <= %s
              AND status IN ('W', 'L', 'P')
            ORDER BY bet_date ASC
            """,
            (first_day, last_day),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [
            {
                "agent_name": row[0],
                "status": row[1],
                "stake": float(row[2]) if row[2] else 5.0,
                "payout": float(row[3]) if row[3] else 0.0,
                "bet_date": row[4],
            }
            for row in rows
        ]
    except Exception as e:
        print(f"[Leaderboard] fetch_monthly_results error: {e}")
        return []


def fetch_unit_sizing() -> dict:
    """Return {agent_name: {tier, unit_dollars}} for all agents."""
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT agent_name, tier, unit_dollars FROM agent_unit_sizing"
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {row[0]: {"tier": row[1], "unit_dollars": float(row[2])} for row in rows}
    except Exception as e:
        print(f"[Leaderboard] fetch_unit_sizing error: {e}")
        return {}


def compute_agent_stats(results: list, unit_sizing: dict) -> list:
    """
    Aggregate results per agent.
    Returns list of dicts sorted by ROI desc.
    """
    from collections import defaultdict

    stats = defaultdict(lambda: {"wins": 0, "losses": 0, "pushes": 0, "profit": 0.0, "staked": 0.0})

    for r in results:
        name = r["agent_name"] or "Unknown"
        stake = r["stake"]
        stats[name]["staked"] += stake

        if r["status"] == "W":
            stats[name]["wins"] += 1
            stats[name]["profit"] += r["payout"] - stake
        elif r["status"] == "L":
            stats[name]["losses"] += 1
            stats[name]["profit"] -= stake
        elif r["status"] == "P":
            stats[name]["pushes"] += 1
            # Push: stake returned, no profit/loss

    agent_list = []
    for name, s in stats.items():
        total_bets = s["wins"] + s["losses"] + s["pushes"]
        win_rate = (s["wins"] / (s["wins"] + s["losses"])) if (s["wins"] + s["losses"]) > 0 else 0.0
        roi = (s["profit"] / s["staked"]) * 100 if s["staked"] > 0 else 0.0
        unit_info = unit_sizing.get(name, {"tier": 1, "unit_dollars": 5.0})
        agent_list.append({
            "agent_name": name,
            "wins": s["wins"],
            "losses": s["losses"],
            "pushes": s["pushes"],
            "total_bets": total_bets,
            "win_rate": win_rate,
            "roi": roi,
            "profit": s["profit"],
            "staked": s["staked"],
            "tier": unit_info["tier"],
            "unit_dollars": unit_info["unit_dollars"],
        })

    # Sort by ROI descending (profit/loss as tiebreaker)
    agent_list.sort(key=lambda x: (x["roi"], x["profit"]), reverse=True)
    return agent_list


def format_discord_message(agent_stats: list, month_num: int, year: int) -> str:
    """
    Build the Discord leaderboard message.
    """
    month_label = f"{month_name[month_num]} {year}"
    total_bets = sum(a["total_bets"] for a in agent_stats)
    total_wins = sum(a["wins"] for a in agent_stats)
    total_losses = sum(a["losses"] for a in agent_stats)
    total_profit = sum(a["profit"] for a in agent_stats)
    overall_roi = (total_profit / sum(a["staked"] for a in agent_stats) * 100) if sum(a["staked"] for a in agent_stats) > 0 else 0.0

    lines = [
        f"# 📊 PropIQ Monthly Leaderboard — {month_label}",
        f"",
        f"**{total_bets} parlays graded** | {total_wins}W-{total_losses}L | Overall ROI: **{overall_roi:+.1f}%** | Net: **${total_profit:+.2f}**",
        f"",
        f"## 🏆 Agent Rankings",
        f"```",
        f"{'Rank':<5} {'Agent':<18} {'Record':<12} {'Win%':<8} {'ROI':<10} {'Net P/L':<12} {'Tier':<6} {'Unit'}",
        f"{'─'*5} {'─'*18} {'─'*12} {'─'*8} {'─'*10} {'─'*12} {'─'*6} {'─'*6}",
    ]

    for i, a in enumerate(agent_stats, 1):
        record = f"{a['wins']}W-{a['losses']}L"
        if a["pushes"] > 0:
            record += f"-{a['pushes']}P"
        win_pct = f"{a['win_rate']:.1%}"
        roi_str = f"{a['roi']:+.1f}%"
        net_str = f"${a['profit']:+.2f}"
        tier_str = f"T{a['tier']}"
        unit_str = f"${a['unit_dollars']:.0f}"

        # Medal for top 3
        medal = ""
        if i == 1:
            medal = "🥇"
        elif i == 2:
            medal = "🥈"
        elif i == 3:
            medal = "🥉"
        elif i >= len(agent_stats) - 2:
            medal = "⚠️ "

        lines.append(
            f"{medal:<5} {a['agent_name']:<18} {record:<12} {win_pct:<8} {roi_str:<10} {net_str:<12} {tier_str:<6} {unit_str}"
        )

    lines.append("```")
    lines.append("")

    # Top 3 shoutout
    if agent_stats:
        top = agent_stats[0]
        lines.append(
            f"**🏆 MVP: {top['agent_name']}** — {top['wins']}W-{top['losses']}L at {top['win_rate']:.1%} | ROI {top['roi']:+.1f}% | Now at {TIER_EMOJI.get(top['tier'], '🌱')} Tier {top['tier']} (${top['unit_dollars']:.0f}/unit)"
        )

    # Bottom 3 warning
    if len(agent_stats) > 3:
        bottom = agent_stats[-1]
        lines.append(
            f"**⚠️ Needs Work: {bottom['agent_name']}** — {bottom['wins']}W-{bottom['losses']}L | ROI {bottom['roi']:+.1f}% | At {TIER_EMOJI.get(bottom['tier'], '🌱')} Tier {bottom['tier']} (${bottom['unit_dollars']:.0f}/unit)"
        )

    lines.append("")
    lines.append(f"*Next leaderboard drops 1st of {month_name[month_num % 12 + 1] if month_num < 12 else 'January'} — keep stacking* 🎯")

    return "\n".join(lines)


def post_to_discord(message: str) -> bool:
    """Post message to Discord webhook."""
    try:
        resp = requests.post(
            DISCORD_WEBHOOK,
            json={"content": message},
            timeout=10,
        )
        if resp.status_code in (200, 204):
            print("[Leaderboard] Discord post successful.")
            return True
        else:
            print(f"[Leaderboard] Discord post failed: {resp.status_code} {resp.text}")
            return False
    except Exception as e:
        print(f"[Leaderboard] Discord post error: {e}")
        return False


def run_monthly_leaderboard():
    """Main entry point — called by FastAPI /trigger/leaderboard endpoint."""
    print("[Leaderboard] Starting monthly leaderboard generation...")

    first_day, last_day, month_num, year = get_previous_month_range()
    print(f"[Leaderboard] Period: {first_day} → {last_day} ({month_name[month_num]} {year})")

    results = fetch_monthly_results(first_day, last_day)
    print(f"[Leaderboard] Fetched {len(results)} settled parlays.")

    if not results:
        msg = f"📊 **PropIQ Monthly Leaderboard — {month_name[month_num]} {year}**\n\nNo settled parlays found for this month. Season might not have started yet — stay tuned! ⚾"
        post_to_discord(msg)
        return {"status": "no_data", "month": f"{month_name[month_num]} {year}"}

    unit_sizing = fetch_unit_sizing()
    agent_stats = compute_agent_stats(results, unit_sizing)

    message = format_discord_message(agent_stats, month_num, year)
    success = post_to_discord(message)

    return {
        "status": "ok" if success else "discord_error",
        "month": f"{month_name[month_num]} {year}",
        "agents_ranked": len(agent_stats),
        "total_parlays": len(results),
    }


if __name__ == "__main__":
    result = run_monthly_leaderboard()
    print(json.dumps(result, indent=2))
