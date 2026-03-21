"""
Leaderboard Tasklet — Runs every 60 seconds
---------------------------------------------
Tracks agent ROI, win rates, and auto-allocates capital.
Top 3 agents get 2x capital. Bottom 2 get 0.5x.
"""
from __future__ import annotations
import json
import logging
from datetime import datetime
from pathlib import Path

from ..agents.base_agent import get_db

logger = logging.getLogger("propiq.tasklet.leaderboard")

LEADERBOARD_PATH = Path(__file__).parent.parent / "data" / "leaderboard.json"
LEADERBOARD_PATH.parent.mkdir(parents=True, exist_ok=True)

BASE_CAPITAL = 100.0
TOP_N_BOOST = 3     # Top 3 get 2x capital
BOTTOM_N_CUT = 2    # Bottom 2 get 0.5x capital
BOOST_MULT = 2.0
CUT_MULT = 0.5

AGENT_NAMES = ["ev_hunter", "under_machine", "three_leg", "parlay", "live", "arb", "grading"]
AGENT_DISPLAY = {
    "ev_hunter": "+EV Hunter",
    "under_machine": "Under Machine",
    "three_leg": "3-Leg Correlated",
    "parlay": "Parlay",
    "live": "Live",
    "arb": "Arbitrage",
    "grading": "Grading",
}
AGENT_STRATEGIES = {
    "ev_hunter": "EV > 5% | 1-3 legs",
    "under_machine": "ERA < 3.50 duels | 58% WR",
    "three_leg": "Same-game correlated | 8-12% edge",
    "parlay": "Game outcomes | 2-3% ROI",
    "live": ">5% line movement | 5-8% edge",
    "arb": "Cross-book >1% guaranteed",
    "grading": "Boxscore settlement",
}


def _allocate_capital(stats_list: list[dict]) -> dict[str, float]:
    """Return capital multiplier per agent based on ROI ranking."""
    # Filter grading agent (doesn't trade)
    ranked = [s for s in stats_list if s["agent_name"] != "grading"]
    ranked.sort(key=lambda x: x.get("roi_pct", 0), reverse=True)

    allocations = {}
    for i, s in enumerate(ranked):
        name = s["agent_name"]
        if i < TOP_N_BOOST:
            mult = BOOST_MULT
        elif i >= len(ranked) - BOTTOM_N_CUT:
            mult = CUT_MULT
        else:
            mult = 1.0
        allocations[name] = round(BASE_CAPITAL * mult, 2)

    allocations["grading"] = 0.0  # Grading doesn't get capital
    return allocations


def run_leaderboard_tasklet() -> dict:
    db = get_db()
    stats_list = db.get_all_stats()

    # Ensure all agents have a row
    existing_names = {s["agent_name"] for s in stats_list}
    for name in AGENT_NAMES:
        if name not in existing_names:
            db.update_agent_stats(name)
    stats_list = db.get_all_stats()

    # Auto-capital allocation
    capital_map = _allocate_capital(stats_list)

    # Write capital back to DB
    import sqlite3
    conn = sqlite3.connect(db.db_path)
    with conn:
        for name, capital in capital_map.items():
            conn.execute(
                "UPDATE agent_stats SET current_capital=? WHERE agent_name=?",
                (capital, name)
            )
    conn.close()

    # Build leaderboard output
    leaderboard = []
    for s in stats_list:
        name = s["agent_name"]
        wins = s.get("wins", 0)
        losses = s.get("losses", 0)
        total = s.get("total_bets", 0)
        roi = s.get("roi_pct", 0.0)
        win_rate = s.get("win_rate_pct", 0.0)
        profit = s.get("total_profit_units", 0.0)
        capital = capital_map.get(name, BASE_CAPITAL)
        prev_capital = s.get("current_capital", BASE_CAPITAL)
        capital_change = capital - BASE_CAPITAL

        # Rank badge
        rank_in_list = next(
            (i + 1 for i, ss in enumerate(
                sorted([x for x in stats_list if x["agent_name"] != "grading"],
                       key=lambda x: x.get("roi_pct", 0), reverse=True)
            ) if ss["agent_name"] == name), None
        )

        leaderboard.append({
            "agent_name": name,
            "display_name": AGENT_DISPLAY.get(name, name),
            "strategy": AGENT_STRATEGIES.get(name, ""),
            "rank": rank_in_list,
            "total_bets": total,
            "wins": wins,
            "losses": losses,
            "pushes": s.get("pushes", 0),
            "win_rate_pct": round(win_rate, 2),
            "roi_pct": round(roi, 2),
            "total_profit_units": round(profit, 2),
            "current_capital": capital,
            "capital_change": round(capital_change, 2),
            "capital_mult": capital / BASE_CAPITAL,
            "status": "🔥 2x Capital" if capital == BASE_CAPITAL * BOOST_MULT
                      else ("⚠️ 0.5x Capital" if capital == BASE_CAPITAL * CUT_MULT
                            else "Active"),
        })

    leaderboard.sort(key=lambda x: (x.get("roi_pct", 0)), reverse=True)

    output = {
        "timestamp": datetime.utcnow().isoformat(),
        "total_agents": len(leaderboard),
        "total_capital_deployed": sum(e["current_capital"] for e in leaderboard),
        "leaderboard": leaderboard,
    }

    # Write to file
    with open(LEADERBOARD_PATH, "w") as f:
        json.dump(output, f, indent=2)

    logger.info(
        f"[leaderboard] Updated {len(leaderboard)} agents — "
        f"Top: {leaderboard[0]['display_name'] if leaderboard else 'N/A'} "
        f"({leaderboard[0]['roi_pct'] if leaderboard else 0:.1f}% ROI)"
    )
    return output


def read_leaderboard() -> dict:
    try:
        with open(LEADERBOARD_PATH) as f:
            return json.load(f)
    except Exception:
        return {"leaderboard": [], "timestamp": None}
