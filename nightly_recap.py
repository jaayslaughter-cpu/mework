"""
nightly_recap.py
================
Runs at 11:00 PM PT every night.

1. Reads bet_ledger WHERE bet_date=yesterday AND discord_sent=TRUE AND status OPEN/NULL
2. Groups legs by parlay_id — each slip is graded as one unit (all-or-nothing)
3. Settles each leg against ESPN boxscores
4. Updates bet_ledger (status, profit_loss, actual_result, actual_outcome, graded_at)
5. Posts a per-slip recap embed to Discord via DiscordAlertService
6. Records in settlement_date_log to prevent duplicate sends

All timestamps: America/Los_Angeles (DST-aware).
Run directly: python3 nightly_recap.py [YYYY-MM-DD]
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

logger = logging.getLogger("nightly_recap")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)

_PT = ZoneInfo("America/Los_Angeles")

# DFS parlay multipliers
_UD_MULTS = {2: 3.0, 3: 6.0, 4: 10.0, 5: 20.0}
_PP_MULTS = {2: 3.0, 3: 5.0, 4: 10.0, 5: 20.0}

# Active agents — filter phantom legacy picks
_ACTIVE_AGENTS = {
    "EVHunter", "UnderMachine", "UmpireAgent", "F5Agent", "FadeAgent",
    "LineValueAgent", "BullpenAgent", "WeatherAgent", "MLEdgeAgent",
    "UnderDogAgent", "StackSmithAgent", "ChalkBusterAgent", "SharpFadeAgent",
    "CorrelatedParlayAgent", "PropCycleAgent", "LineupChaseAgent", "LineDriftAgent",
    "SteamAgent", "StreakAgent",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _yesterday_pt() -> str:
    """Return yesterday's date as YYYY-MM-DD in America/Los_Angeles."""
    now_pt = datetime.now(_PT)
    return (now_pt - timedelta(days=1)).strftime("%Y-%m-%d")


def _pg_conn():
    import psycopg2  # noqa: PLC0415
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(db_url, sslmode="require")


# ---------------------------------------------------------------------------
# Dedup guard
# ---------------------------------------------------------------------------

def _settlement_already_ran(settle_date: str) -> bool:
    try:
        conn = _pg_conn()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS settlement_date_log (
                settle_date DATE PRIMARY KEY,
                ran_at      TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        # Heal missing column (PR #410 migration)
        cur.execute("""
            ALTER TABLE settlement_date_log
            ADD COLUMN IF NOT EXISTS settlement_date DATE
        """)
        conn.commit()
        cur.execute(
            "SELECT 1 FROM settlement_date_log WHERE settle_date = %s",
            (settle_date,),
        )
        already = cur.fetchone() is not None
        cur.close(); conn.close()
        return already
    except Exception as exc:
        logger.warning("[Recap] settlement_date_log check failed: %s", exc)
        return False


def _record_settlement_ran(settle_date: str) -> None:
    try:
        conn = _pg_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO settlement_date_log (settle_date) VALUES (%s) ON CONFLICT DO NOTHING",
            (settle_date,),
        )
        conn.commit()
        cur.close(); conn.close()
        logger.info("[Recap] Settlement date recorded: %s", settle_date)
    except Exception as exc:
        logger.warning("[Recap] _record_settlement_ran failed: %s", exc)


# ---------------------------------------------------------------------------
# Season record sync
# ---------------------------------------------------------------------------

def _sync_season_record(parlay_results: list[dict], settle_date: str) -> None:
    """Upsert each settled slip into propiq_season_record for downstream reporting."""
    try:
        from season_record import settle_parlay_record  # noqa: PLC0415
        for pr in parlay_results:
            if pr.get("_season_id"):
                settle_parlay_record(pr["_season_id"], pr["status"], pr["profit_loss"])
    except Exception as exc:
        logger.debug("[Recap] season_record sync failed (non-fatal): %s", exc)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(settle_date: str | None = None) -> None:
    if settle_date is None:
        settle_date = _yesterday_pt()

    logger.info("=== PropIQ Nightly Settlement: %s (PT) ===", settle_date)

    if _settlement_already_ran(settle_date):
        logger.info("[Recap] Settlement for %s already ran — skipping.", settle_date)
        return

    # ── 1. ESPN stats ─────────────────────────────────────────────────────
    espn_date = settle_date.replace("-", "")
    try:
        from espn_scraper import get_all_player_stats  # noqa: PLC0415
        player_stats = get_all_player_stats(espn_date)
    except Exception as exc:
        logger.warning("[Recap] ESPN fetch failed: %s", exc)
        player_stats = {}

    if not player_stats:
        logger.warning("[Recap] No ESPN boxscores for %s — aborting settlement.", settle_date)
        return

    # ── 2. Read bet_ledger — open rows for settle_date ────────────────────
    try:
        conn = _pg_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, parlay_id, agent_name, player_name, prop_type,
                   side, line, entry_type,
                   COALESCE(units_wagered, ABS(kelly_units), 5.0) AS stake,
                   COALESCE(platform, 'underdog') AS platform
            FROM   bet_ledger
            WHERE  bet_date = %s
              AND  discord_sent = TRUE
              AND  (status IS NULL OR status = 'OPEN')
            ORDER  BY agent_name, parlay_id NULLS LAST, id
        """, (settle_date,))
        rows = cur.fetchall()
        conn.close()
    except Exception as exc:
        logger.error("[Recap] bet_ledger read failed: %s", exc)
        return

    logger.info("[Recap] Found %d open legs for %s.", len(rows), settle_date)

    # ── 3. Group by parlay_id ─────────────────────────────────────────────
    slip_groups: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        bid, pid, agent, player, prop_type, side, line, entry_type, stake, platform = row
        if agent not in _ACTIVE_AGENTS:
            logger.debug("[Recap] Skipping phantom agent %s", agent)
            continue
        key = str(pid) if pid else f"solo_{bid}"
        slip_groups[key].append({
            "id":         bid,
            "player":     player,
            "prop_type":  prop_type,
            "side":       side,
            "line":       float(line or 0),
            "entry_type": entry_type or "FlexPlay",
            "stake":      float(stake or 5.0),
            "agent":      agent,
            "platform":   platform,
        })

    if not slip_groups:
        logger.info("[Recap] No open bets from active agents for %s.", settle_date)
        # Still send empty recap so Discord shows 0-0-0
        _send_empty_recap(settle_date)
        _record_settlement_ran(settle_date)
        return

    # ── 4. Settle each slip ───────────────────────────────────────────────
    parlay_results: list[dict] = []
    try:
        conn = _pg_conn()
        cur = conn.cursor()

        for pid_key, legs in slip_groups.items():
            agent      = legs[0]["agent"]
            stake      = legs[0]["stake"]
            entry_type = legs[0]["entry_type"]
            platform   = legs[0]["platform"]
            _is_pp     = "prize" in platform.lower()
            n          = len(legs)

            # Settle each leg
            for leg in legs:
                actual = _lookup_actual(player_stats, leg["player"], leg["prop_type"])
                leg["actual"] = actual
                if actual is None:
                    leg["status"] = "PUSH"      # no data = void/push
                elif leg["side"].lower() in ("higher", "over"):
                    if actual > leg["line"]:     leg["status"] = "WIN"
                    elif actual == leg["line"]:  leg["status"] = "PUSH"
                    else:                        leg["status"] = "LOSS"
                else:
                    if actual < leg["line"]:     leg["status"] = "WIN"
                    elif actual == leg["line"]:  leg["status"] = "PUSH"
                    else:                        leg["status"] = "LOSS"

            # Slip-level outcome (all-or-nothing)
            any_loss = any(l["status"] == "LOSS" for l in legs)
            all_win  = all(l["status"] == "WIN"  for l in legs)
            all_push = all(l["status"] == "PUSH" for l in legs)

            if all_push:
                slip_status = "PUSH";  slip_pl = 0.0
            elif any_loss:
                slip_status = "LOSS";  slip_pl = -stake
            elif all_win:
                slip_status = "WIN"
                mult        = (_PP_MULTS if _is_pp else _UD_MULTS).get(n, 3.0)
                slip_pl     = round(stake * mult - stake, 4)
            else:
                # Mixed WIN/PUSH, no LOSS — conservative push
                slip_status = "PUSH";  slip_pl = 0.0

            # UPDATE bet_ledger — set status + actual on each leg
            per_leg_pl = round(slip_pl / max(n, 1), 4)
            for leg in legs:
                ao = 1 if leg["status"] == "WIN" else (0 if leg["status"] == "LOSS" else None)
                cur.execute("""
                    UPDATE bet_ledger
                    SET    status         = %s,
                           profit_loss    = %s,
                           actual_result  = %s,
                           actual_outcome = %s,
                           graded_at      = NOW() AT TIME ZONE 'America/Los_Angeles'
                    WHERE  id = %s AND (status IS NULL OR status = 'OPEN')
                """, (
                    slip_status,
                    per_leg_pl,
                    leg.get("actual"),
                    ao,
                    leg["id"],
                ))

            logger.info("[%s] slip %s → %s (%+.2fu)", agent, pid_key, slip_status, slip_pl)

            parlay_results.append({
                "parlay_id":   pid_key,
                "agent":       agent,
                "legs":        legs,
                "leg_count":   n,
                "status":      slip_status,
                "profit_loss": slip_pl,
                "stake":       stake,
                "entry_type":  entry_type,
            })

        conn.commit()
        conn.close()
    except Exception as exc:
        logger.error("[Recap] Settlement error: %s", exc)
        return

    # ── 5. Season record sync ─────────────────────────────────────────────
    try:
        from season_record import get_overall_season_stats  # noqa: PLC0415
        season_stats = get_overall_season_stats()
    except Exception:
        season_stats = {}

    # ── 6. Discord recap ──────────────────────────────────────────────────
    total_profit = sum(p["profit_loss"] for p in parlay_results)
    try:
        from DiscordAlertService import DiscordAlertService  # noqa: PLC0415
        _da = DiscordAlertService()
        _da.send_daily_recap(parlay_results, total_profit, settle_date)
        logger.info("[Recap] Discord recap sent — %s  %+.2fu  %d slips",
                    settle_date, total_profit, len(parlay_results))
    except Exception as exc:
        logger.error("[Recap] Discord send failed: %s", exc)
        # Non-fatal — still record so we don't loop forever
        return

    _record_settlement_ran(settle_date)

    wins   = sum(1 for p in parlay_results if p["status"] == "WIN")
    losses = sum(1 for p in parlay_results if p["status"] == "LOSS")
    pushes = sum(1 for p in parlay_results if p["status"] == "PUSH")
    logger.info("=== Settlement complete: %dW-%dL-%dP  %+.2fu ===",
                wins, losses, pushes, total_profit)

    # ── 7. Post-settlement hooks ──────────────────────────────────────────
    _run_post_hooks(settle_date)


def _send_empty_recap(settle_date: str) -> None:
    """Send a minimal no-bets recap so Discord always has a nightly message."""
    try:
        from DiscordAlertService import DiscordAlertService  # noqa: PLC0415
        from season_record import get_overall_season_stats  # noqa: PLC0415
        DiscordAlertService().send_daily_recap([], 0.0, settle_date)
    except Exception as exc:
        logger.warning("[Recap] Empty recap send failed: %s", exc)


def _run_post_hooks(settle_date: str) -> None:
    """Non-critical post-settlement jobs."""
    try:
        from streak_agent import settle_streak_picks  # noqa: PLC0415
        settle_streak_picks(settle_date)
        logger.info("[Recap] Streak picks settled.")
    except Exception as exc:
        logger.debug("[Recap] Streak settlement skipped: %s", exc)

    try:
        from calibration_monitor import run as _cal_run  # noqa: PLC0415
        _cal_run(days=30, quiet=True)
    except Exception as exc:
        logger.debug("[Recap] Calibration monitor skipped: %s", exc)

    try:
        from edge_health_monitor import run as _edge_run  # noqa: PLC0415
        from risk_manager import RiskManager  # noqa: PLC0415
        metrics = _edge_run(days=30, quiet=True)
        if metrics:
            RiskManager().check_and_apply_cool_downs(metrics)
    except Exception as exc:
        logger.debug("[Recap] Edge health monitor skipped: %s", exc)


# ---------------------------------------------------------------------------
# Stat lookup (ESPN box score)
# ---------------------------------------------------------------------------

_PROP_TO_ESPN: dict[str, list[str]] = {
    "strikeouts":       ["strikeouts"],
    "pitching_outs":    ["pitching_outs"],
    "walks_allowed":    ["base_on_balls", "walks_allowed", "bb_allowed"],
    "earned_runs":      ["earned_runs"],
    "hits":             ["hits"],
    "total_bases":      ["total_bases"],
    "hitter_strikeouts":["strikeouts"],
    "hits_runs_rbis":   ["hits_runs_rbis"],
    "fantasy_score":    ["fantasy_score"],
}


def _lookup_actual(player_stats: dict, player_name: str, prop_type: str) -> float | None:
    """Look up the actual stat for a player from the ESPN stat dict."""
    import unicodedata  # noqa: PLC0415

    def _norm(s: str) -> str:
        nfd = unicodedata.normalize("NFD", s)
        return "".join(c for c in nfd if unicodedata.category(c) != "Mn").lower().strip()

    p_norm = _norm(player_name)
    espn_data: dict | None = None
    for key, val in player_stats.items():
        if _norm(key) == p_norm or _norm(val.get("full_name", "")) == p_norm:
            espn_data = val
            break

    if espn_data is None:
        logger.debug("[Recap] No ESPN data for %s", player_name)
        return None

    for stat_key in _PROP_TO_ESPN.get(prop_type, [prop_type]):
        v = espn_data.get(stat_key)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                pass

    logger.debug("[Recap] Stat key not found: %s / %s", player_name, prop_type)
    return None


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    run(date_arg)
