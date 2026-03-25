"""
replay_tool.py — PropIQ Decision Replay
=========================================
Given a date (and optionally a player/agent), reconstructs exactly what
every agent saw: input probabilities, feature values, and final decision.

This is the debugging tool. When you get a bad beat and want to know why
BullpenAgent picked Yordan Alvarez Under 1.5 hits on a Saturday road game,
this shows you every number that went into that decision.

Usage:
    python3 replay_tool.py --date 2026-03-25
    python3 replay_tool.py --date 2026-03-25 --agent FadeAgent
    python3 replay_tool.py --date 2026-03-25 --player "Aaron Judge"
    python3 replay_tool.py --date 2026-03-25 --included-only
    python3 replay_tool.py --date 2026-03-25 --rejected-only

Output:
    Pretty-printed table of every leg evaluated with all feature values.
    Also shows the final parlays posted that day from propiq_season_record.

API usage (from api_server.py):
    GET /replay?date=2026-03-25&agent=EVHunter
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import date

import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s [REPLAY] %(message)s")
logger = logging.getLogger(__name__)

_DB_URL = os.environ.get("DATABASE_URL", "")


def _get_conn():
    return psycopg2.connect(_DB_URL)


# ---------------------------------------------------------------------------
# Fetch decision log for a date
# ---------------------------------------------------------------------------
def fetch_decisions(
    log_date: str,
    agent_name: str | None = None,
    player_name: str | None = None,
    decision_filter: str | None = None,  # "INCLUDED" or "REJECTED"
) -> list[dict]:
    query = """
        SELECT agent_name, player_name, prop_type, direction, line, platform,
               prob_base, prob_draftedge, prob_statcast, prob_sbd, prob_form,
               prob_fangraphs, prob_final, edge_pct, decision, reject_reason,
               features, config_version, created_at
        FROM decision_log
        WHERE log_date = %s
    """
    params: list = [log_date]

    if agent_name:
        query += " AND agent_name = %s"
        params.append(agent_name)
    if player_name:
        query += " AND player_name ILIKE %s"
        params.append(f"%{player_name}%")
    if decision_filter:
        query += " AND decision = %s"
        params.append(decision_filter)

    query += " ORDER BY agent_name, prob_final DESC"

    try:
        with _get_conn() as conn, conn.cursor() as cur:
            cur.execute(query, params)
            cols = [
                "agent_name", "player_name", "prop_type", "direction", "line", "platform",
                "prob_base", "prob_draftedge", "prob_statcast", "prob_sbd", "prob_form",
                "prob_fangraphs", "prob_final", "edge_pct", "decision", "reject_reason",
                "features", "config_version", "created_at",
            ]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as exc:
        logger.error("Decision log fetch failed: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Fetch parlays posted that day
# ---------------------------------------------------------------------------
def fetch_posted_parlays(log_date: str) -> list[dict]:
    try:
        with _get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT agent_name, platform, confidence, stake, payout,
                       status, legs_json, config_version
                FROM propiq_season_record
                WHERE created_at::date = %s
                ORDER BY confidence DESC
            """, (log_date,))
            cols = ["agent_name", "platform", "confidence", "stake", "payout",
                    "status", "legs_json", "config_version"]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as exc:
        logger.error("Parlay fetch failed: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Pretty print
# ---------------------------------------------------------------------------
def render_decisions(decisions: list[dict]) -> str:
    if not decisions:
        return "  No decisions found."

    lines = []
    current_agent = None
    for d in decisions:
        if d["agent_name"] != current_agent:
            current_agent = d["agent_name"]
            lines.append(f"\n  ── {current_agent} (config: {d['config_version']}) ──")
            lines.append(
                f"  {'Player':<22} {'Prop':<18} {'Dir':>4} {'Line':>5} "
                f"{'Base':>5} {'DE':>5} {'SC':>5} {'SBD':>5} {'Form':>5} {'FG':>5} "
                f"{'Final':>6} {'Edge':>5}  {'Decision'}"
            )
            lines.append("  " + "-" * 115)

        reason = f" [{d['reject_reason']}]" if d.get("reject_reason") else ""
        lines.append(
            f"  {(d['player_name'] or ''):<22} {(d['prop_type'] or ''):<18} "
            f"{(d['direction'] or ''):>4} {(d['line'] or 0):>5.1f} "
            f"{(d['prob_base'] or 0):>5.3f} {(d['prob_draftedge'] or 0):>5.3f} "
            f"{(d['prob_statcast'] or 0):>5.3f} {(d['prob_sbd'] or 0):>5.3f} "
            f"{(d['prob_form'] or 0):>5.3f} {(d['prob_fangraphs'] or 0):>5.3f} "
            f"{(d['prob_final'] or 0):>6.3f} {(d['edge_pct'] or 0):>4.1%}  "
            f"{'✅' if d['decision'] == 'INCLUDED' else '❌'}{reason}"
        )
    return "\n".join(lines)


def render_parlays(parlays: list[dict]) -> str:
    if not parlays:
        return "  No parlays posted that day."
    lines = []
    for p in parlays:
        status_icon = {"WIN": "✅", "LOSS": "❌", "PUSH": "➖", "PENDING": "⏳"}.get(p["status"], "?")
        lines.append(
            f"\n  {status_icon} {p['agent_name']:<18} | {p['platform']:<10} | "
            f"Conf: {p['confidence']}/10 | ${p['stake']} → ${p['payout']} | {p['status']}"
        )
        try:
            legs = json.loads(p["legs_json"]) if isinstance(p["legs_json"], str) else p["legs_json"]
            for i, leg in enumerate(legs or [], 1):
                player = leg.get("player_name", leg.get("player", "?"))
                prop = leg.get("prop_type", leg.get("stat", "?"))
                direction = leg.get("direction", "?")
                prob = leg.get("prob", leg.get("probability", 0))
                lines.append(f"       Leg {i}: {player} {prop} {direction} (prob: {prob:.3f})")
        except Exception:
            lines.append(f"       [Could not parse legs]")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run(
    log_date: str,
    agent_name: str | None = None,
    player_name: str | None = None,
    decision_filter: str | None = None,
    show_parlays: bool = True,
) -> dict:
    logger.info("Replaying decisions for %s", log_date)
    decisions = fetch_decisions(log_date, agent_name, player_name, decision_filter)
    parlays = fetch_posted_parlays(log_date) if show_parlays else []

    n_included = sum(1 for d in decisions if d["decision"] == "INCLUDED")
    n_rejected = sum(1 for d in decisions if d["decision"] == "REJECTED")
    agents_seen = set(d["agent_name"] for d in decisions)

    print(f"\n{'='*60}")
    print(f"  PropIQ Decision Replay — {log_date}")
    print(f"  {len(agents_seen)} agents | {n_included} legs included | {n_rejected} legs rejected")
    print(f"{'='*60}")
    print("\n📋 LEG DECISIONS:")
    print(render_decisions(decisions))
    if show_parlays:
        print(f"\n\n🎯 PARLAYS POSTED ({len(parlays)}):")
        print(render_parlays(parlays))
    print()

    return {
        "decisions": decisions,
        "parlays": parlays,
        "summary": {
            "date": log_date,
            "included": n_included,
            "rejected": n_rejected,
            "agents": list(agents_seen),
        },
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PropIQ Decision Replay Tool")
    parser.add_argument("--date", default=date.today().isoformat(), help="Date to replay (YYYY-MM-DD)")
    parser.add_argument("--agent", default=None, help="Filter by agent name")
    parser.add_argument("--player", default=None, help="Filter by player name (partial match)")
    parser.add_argument("--included-only", action="store_true")
    parser.add_argument("--rejected-only", action="store_true")
    parser.add_argument("--no-parlays", action="store_true")
    args = parser.parse_args()

    decision_filter = None
    if args.included_only:
        decision_filter = "INCLUDED"
    elif args.rejected_only:
        decision_filter = "REJECTED"

    run(
        log_date=args.date,
        agent_name=args.agent,
        player_name=args.player,
        decision_filter=decision_filter,
        show_parlays=not args.no_parlays,
    )
