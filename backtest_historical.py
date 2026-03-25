"""
backtest_historical.py
======================
10-Season MLB Historical Backtest Engine (2016 – 2025)
======================================================

Pipeline
--------
1. Fetch all player-game logs from SportsData.io for every season.
2. Build a rolling feature window (L7 / L14 / L30 per prop type) that
   mirrors what the ML pipeline would have seen on game day.
3. Simulate a "book line" as the L14 rolling median (±½ unit rounding).
4. Apply the same no-vig EV gate used by all 15 production agents.
5. Grade each bet against the actual outcome.
6. Aggregate: ROI, units, win-rate, max-drawdown, Sharpe ratio —
   broken out by agent, season, prop type, and handedness split.

All results are written to:
  - backtest_results/summary.json     (machine-readable)
  - backtest_results/report.csv       (per-bet ledger)
  - backtest_results/agent_pnl.csv    (per-agent season breakdown)
  - backtest_results/season_pnl.csv   (per-season aggregate)

Usage
-----
    python backtest_historical.py [--seasons 2016-2025] [--dry-run]

Environment variables required
--------------------------------
    SPORTSDATA_API_KEY   (c2abf26f55714d228c7c311290f956d7)

PEP 8 compliant. No external API keys beyond SportsData.io.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import os
import statistics
import sys
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import requests

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("backtest")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SEASONS = list(range(2016, 2026))          # 2016 – 2025 inclusive
SPORTSDATA_KEY = os.getenv(
    "SPORTSDATA_API_KEY", "c2abf26f55714d228c7c311290f956d7"
)
SPORTSDATA_BASE = "https://api.sportsdata.io/v3/mlb/stats/json"

# Prop types we simulate (maps SportsData field → prop label)
PROP_MAP: dict[str, str] = {
    "Hits": "hits",
    "HomeRuns": "home_runs",
    "RBIs": "rbis",
    "Runs": "runs",
    "StrikeoutsAsBatter": "batter_k",
    "TotalBases": "total_bases",
    "StolenBases": "stolen_bases",
    "PitchingStrikeouts": "pitcher_k",
    "EarnedRunsAllowed": "era_line",
    "InningsPitchedDecimal": "innings",
    "Walks": "walks",
    "Singles": "singles",
    "Doubles": "doubles",
    "PitchingHitsAllowed": "hits_allowed",
}

# Rolling window sizes
L7 = 7
L14 = 14
L30 = 30

# EV gate (mirrors production odds_math.py)
EV_GATE = 0.03            # 3 %
HALF_KELLY_CAP = 0.10     # 10 % of bankroll per bet

# Simulated implied probability for the "book" (typical DFS -115 both sides)
BOOK_ODDS = -115          # American odds

# ---------------------------------------------------------------------------
# Utility: American odds → decimal probability
# ---------------------------------------------------------------------------

def american_to_prob(american: int) -> float:
    """Convert American odds to implied probability (including vig)."""
    if american > 0:
        return 100 / (american + 100)
    return abs(american) / (abs(american) + 100)


def strip_vig(prob_over_raw: float, prob_under_raw: float) -> tuple[float, float]:
    """Remove overround from a two-sided market."""
    total = prob_over_raw + prob_under_raw
    return prob_over_raw / total, prob_under_raw / total


def calculate_ev(model_prob: float, true_prob: float) -> float:
    """(model_prob / true_prob) − 1  →  expected value percentage."""
    if true_prob <= 0:
        return -1.0
    return (model_prob / true_prob) - 1.0


def kelly_fraction(ev: float, true_prob: float) -> float:
    """Full Kelly, capped at HALF_KELLY_CAP."""
    # Win odds implied by the true prob at -110 payout
    b = 100 / 110  # decimal profit per unit at -110
    f = (b * true_prob - (1 - true_prob)) / b
    return min(max(f, 0.0), HALF_KELLY_CAP)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class PlayerGameLog:
    """Single game record for one player."""
    player_id: int
    player_name: str
    team: str
    game_date: str        # YYYY-MM-DD
    season: int
    stats: dict[str, float] = field(default_factory=dict)
    position: str = ""
    batter_hand: str = ""   # L / R / S
    pitcher_hand: str = ""  # L / R


@dataclass
class SimulatedBet:
    """One graded bet."""
    season: int
    game_date: str
    player_id: int
    player_name: str
    prop_type: str
    line: float
    direction: str          # "over" | "under"
    model_prob: float
    true_prob: float
    ev_pct: float
    kelly: float
    units_wagered: float
    actual_value: float
    hit: bool               # did the bet win?
    units_pnl: float        # profit/loss in units
    agent: str


# ---------------------------------------------------------------------------
# Rolling feature window
# ---------------------------------------------------------------------------

class RollingWindow:
    """Maintains per-player rolling stat windows."""

    def __init__(self) -> None:
        # {player_id: {prop_type: deque[float]}}
        self._data: dict[int, dict[str, deque]] = defaultdict(
            lambda: defaultdict(lambda: deque(maxlen=L30))
        )

    def push(self, player_id: int, prop_type: str, value: float) -> None:
        self._data[player_id][prop_type].append(value)

    def rolling_median(self, player_id: int, prop_type: str, n: int) -> float | None:
        buf = list(self._data[player_id][prop_type])
        if len(buf) < 3:  # minimum sample before we simulate a line
            return None
        window = buf[-n:]
        return statistics.median(window)

    def rolling_mean(self, player_id: int, prop_type: str, n: int) -> float | None:
        buf = list(self._data[player_id][prop_type])
        if len(buf) < 3:
            return None
        window = buf[-n:]
        return statistics.mean(window)

    def sample_size(self, player_id: int, prop_type: str) -> int:
        return len(self._data[player_id][prop_type])


# ---------------------------------------------------------------------------
# Prop line simulator
# ---------------------------------------------------------------------------

def simulate_line(
    rolling: RollingWindow, player_id: int, prop_type: str
) -> float | None:
    """
    Simulate the book line as L14 rolling median rounded to nearest 0.5.
    Falls back to L7 if < 14 games available.
    """
    med = rolling.rolling_median(player_id, prop_type, L14)
    if med is None:
        med = rolling.rolling_median(player_id, prop_type, L7)
    if med is None:
        return None
    # Round to nearest 0.5 (standard DFS line granularity)
    return round(med * 2) / 2


def simulate_model_probability(
    rolling: RollingWindow,
    player_id: int,
    prop_type: str,
    line: float,
    direction: str,
) -> float:
    """
    Generate a model probability using historical hit rate over/under the
    simulated line across the L30 window.

    This mirrors the XGBoost calibrated output: a true fraction of game
    outcomes that exceeded / fell below the line.
    """
    buf = list(rolling._data[player_id][prop_type])
    if len(buf) < 3:
        return 0.5  # no signal
    relevant = buf[-L30:]
    if direction == "over":
        hits = sum(1 for v in relevant if v > line)
    else:
        hits = sum(1 for v in relevant if v < line)
    # Laplace smoothing to avoid 0/1 extremes
    return (hits + 1) / (len(relevant) + 2)


# ---------------------------------------------------------------------------
# Agent filter logic (mirrors production execution_agents.py)
# ---------------------------------------------------------------------------

AGENTS = [
    "EVHunter",
    "UnderMachine",
    "F5Agent",
    "MLEdgeAgent",
    "UmpireAgent",
    "FadeAgent",
    "LineValueAgent",
    "BullpenAgent",
    "WeatherAgent",
    "SteamAgent",
    "ArsenalAgent",
    "PlatoonAgent",
    "CatcherAgent",
    "LineupAgent",
    "GetawayAgent",
]

# Minimum probability threshold per agent (mirrors prod)
AGENT_MIN_PROB: dict[str, float] = {
    "EVHunter": 0.53,
    "UnderMachine": 0.53,
    "F5Agent": 0.53,
    "MLEdgeAgent": 0.55,
    "UmpireAgent": 0.54,
    "FadeAgent": 0.52,
    "LineValueAgent": 0.53,
    "BullpenAgent": 0.54,
    "WeatherAgent": 0.54,
    "SteamAgent": 0.56,
    "ArsenalAgent": 0.54,
    "PlatoonAgent": 0.52,
    "CatcherAgent": 0.54,
    "LineupAgent": 0.52,
    "GetawayAgent": 0.52,
}

# Prop type affinity per agent
AGENT_PROP_FILTER: dict[str, set[str]] = {
    "EVHunter": set(PROP_MAP.values()),           # all props
    "UnderMachine": set(PROP_MAP.values()),        # unders only (direction filter)
    "F5Agent": {"pitcher_k", "hits_allowed", "walks", "era_line", "innings"},
    "MLEdgeAgent": set(PROP_MAP.values()),
    "UmpireAgent": {"pitcher_k", "batter_k", "walks", "era_line"},
    "FadeAgent": set(PROP_MAP.values()),
    "LineValueAgent": set(PROP_MAP.values()),
    "BullpenAgent": {"era_line", "hits_allowed", "walks", "innings"},
    "WeatherAgent": {"home_runs", "total_bases", "hits"},
    "SteamAgent": set(PROP_MAP.values()),
    "ArsenalAgent": {"pitcher_k", "total_bases"},
    "PlatoonAgent": {"hits", "home_runs", "rbis", "total_bases"},
    "CatcherAgent": {"pitcher_k", "stolen_bases"},
    "LineupAgent": {"hits", "runs", "rbis", "total_bases"},
    "GetawayAgent": {"hits", "runs", "rbis", "total_bases", "singles"},
}

# Direction preference (None = both)
AGENT_DIRECTION: dict[str, str | None] = {
    "UnderMachine": "under",
    "GetawayAgent": "under",
}


def passes_agent_filter(
    agent: str,
    prop_type: str,
    direction: str,
    model_prob: float,
    ev_pct: float,
) -> bool:
    """Return True if this bet passes the agent's logic gates."""
    if ev_pct < EV_GATE:
        return False
    if model_prob < AGENT_MIN_PROB.get(agent, 0.53):
        return False
    if prop_type not in AGENT_PROP_FILTER.get(agent, set()):
        return False
    preferred_dir = AGENT_DIRECTION.get(agent)
    if preferred_dir and direction != preferred_dir:
        return False
    return True


# ---------------------------------------------------------------------------
# SportsData.io fetcher
# ---------------------------------------------------------------------------

class SportsDataClient:
    """Thin wrapper around the SportsData.io MLB stats v3 endpoints."""

    _SEASON_CACHE: dict[int, list[dict]] = {}

    def __init__(self, api_key: str) -> None:
        self._key = api_key
        self._session = requests.Session()
        self._session.headers.update({"Ocp-Apim-Subscription-Key": api_key})
        self._rate_limit_sleep = 0.35  # ~3 req/s to stay under free tier

    def _get(self, path: str) -> Any:
        url = f"{SPORTSDATA_BASE}/{path}"
        for attempt in range(4):
            try:
                resp = self._session.get(url, timeout=20)
                if resp.status_code == 429:
                    wait = int(resp.headers.get("Retry-After", 10))
                    logger.warning("Rate limited — sleeping %ds", wait)
                    time.sleep(wait)
                    continue
                if resp.status_code == 200:
                    return resp.json()
                logger.warning("HTTP %d for %s", resp.status_code, url)
                return []
            except requests.RequestException as exc:
                logger.error("Request error (attempt %d): %s", attempt + 1, exc)
                time.sleep(2 ** attempt)
        return []

    def get_player_season_stats(self, season: int) -> list[dict]:
        """All players' cumulative stats for a season."""
        if season in self._SEASON_CACHE:
            return self._SEASON_CACHE[season]
        data = self._get(f"PlayerSeasonStats/{season}")
        result = data if isinstance(data, list) else []
        self._SEASON_CACHE[season] = result
        return result

    def get_game_stats_by_date(self, game_date: str) -> list[dict]:
        """All player-game records for a specific date (YYYY-MM-DD)."""
        time.sleep(self._rate_limit_sleep)
        return self._get(f"PlayerGameStatsByDate/{game_date}") or []

    def get_schedule(self, season: int) -> list[dict]:
        """Full schedule for a season (used to enumerate game dates)."""
        time.sleep(self._rate_limit_sleep)
        return self._get(f"Games/{season}") or []


# ---------------------------------------------------------------------------
# Date range helpers
# ---------------------------------------------------------------------------

def season_date_range(season: int) -> list[str]:
    """Return all calendar dates that fall within an MLB regular season."""
    # MLB regular seasons roughly: late March – late September
    start = date(season, 3, 28)
    end = date(season, 9, 30)
    dates = []
    cur = start
    while cur <= end:
        dates.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return dates


# ---------------------------------------------------------------------------
# Main backtest loop
# ---------------------------------------------------------------------------

def run_backtest(
    seasons: list[int],
    dry_run: bool = False,
    output_dir: str = "backtest_results",
) -> dict[str, Any]:
    """
    Full 10-season backtest.

    Parameters
    ----------
    seasons  : list of season years to include
    dry_run  : if True, only process first 14 days of each season
    output_dir: directory to write CSVs + JSON to

    Returns
    -------
    summary dict
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    client = SportsDataClient(SPORTSDATA_KEY)
    rolling = RollingWindow()

    all_bets: list[SimulatedBet] = []

    for season in seasons:
        logger.info("=== Season %d ===", season)
        dates = season_date_range(season)
        if dry_run:
            dates = dates[:14]

        for game_date in dates:
            raw_games = client.get_game_stats_by_date(game_date)
            if not raw_games:
                continue

            for record in raw_games:
                player_id = record.get("PlayerID") or record.get("FantasyPlayerKey")
                if not player_id:
                    continue
                player_id = int(player_id)
                player_name = record.get("Name", "Unknown")
                team = record.get("Team", "")
                position = record.get("Position", "")

                # Push this game's stats into the rolling windows FIRST
                # (we peek at them BEFORE pushing for look-ahead-bias prevention)
                pre_push_windows: dict[str, float | None] = {}
                for sd_field, prop_label in PROP_MAP.items():
                    raw_val = record.get(sd_field)
                    if raw_val is None:
                        continue
                    try:
                        actual = float(raw_val)
                    except (TypeError, ValueError):
                        continue
                    # Capture line BEFORE this game is in the window
                    line = simulate_line(rolling, player_id, prop_label)
                    pre_push_windows[prop_label] = (actual, line)

                # Now generate bets using the pre-game windows
                for prop_label, payload in pre_push_windows.items():
                    if payload is None:
                        continue
                    actual, line = payload
                    if line is None:
                        continue

                    for direction in ("over", "under"):
                        model_prob = simulate_model_probability(
                            rolling, player_id, prop_label, line, direction
                        )
                        # Simulate a symmetrical book at BOOK_ODDS both sides
                        raw_prob = american_to_prob(BOOK_ODDS)
                        true_prob_over, true_prob_under = strip_vig(
                            raw_prob, raw_prob
                        )
                        true_prob = (
                            true_prob_over if direction == "over" else true_prob_under
                        )
                        ev = calculate_ev(model_prob, true_prob)
                        if ev < EV_GATE:
                            continue

                        k = kelly_fraction(ev, true_prob)
                        if k <= 0:
                            continue

                        # Determine which agents would have taken this bet
                        for agent in AGENTS:
                            if not passes_agent_filter(
                                agent, prop_label, direction, model_prob, ev
                            ):
                                continue

                            # Grade the bet
                            if direction == "over":
                                hit = actual > line
                            else:
                                hit = actual < line
                            # P&L at -110 (standard DFS payout)
                            units = k
                            pnl = units * (100 / 110) if hit else -units

                            bet = SimulatedBet(
                                season=season,
                                game_date=game_date,
                                player_id=player_id,
                                player_name=player_name,
                                prop_type=prop_label,
                                line=line,
                                direction=direction,
                                model_prob=round(model_prob, 4),
                                true_prob=round(true_prob, 4),
                                ev_pct=round(ev, 4),
                                kelly=round(k, 4),
                                units_wagered=round(units, 4),
                                actual_value=actual,
                                hit=hit,
                                units_pnl=round(pnl, 4),
                                agent=agent,
                            )
                            all_bets.append(bet)

                # Push this game's stats into the rolling window AFTER generating bets
                for sd_field, prop_label in PROP_MAP.items():
                    raw_val = record.get(sd_field)
                    if raw_val is None:
                        continue
                    try:
                        actual = float(raw_val)
                    except (TypeError, ValueError):
                        continue
                    rolling.push(player_id, prop_label, actual)

        logger.info(
            "Season %d complete — %d bets accumulated", season, len(all_bets)
        )

    # ------------------------------------------------------------------
    # Aggregate results
    # ------------------------------------------------------------------
    logger.info("Aggregating %d total bets...", len(all_bets))
    summary = _aggregate(all_bets)

    # ------------------------------------------------------------------
    # Write outputs
    # ------------------------------------------------------------------
    _write_per_bet_ledger(all_bets, output_dir)
    _write_agent_pnl(summary["by_agent"], output_dir)
    _write_season_pnl(summary["by_season"], output_dir)

    with open(f"{output_dir}/summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)

    logger.info("Results written to %s/", output_dir)
    return summary


# ---------------------------------------------------------------------------
# Aggregation helpers
# ---------------------------------------------------------------------------

def _aggregate(bets: list[SimulatedBet]) -> dict[str, Any]:
    """Build summary dicts keyed by agent, season, and prop type."""

    def empty_stats() -> dict:
        return {
            "total_bets": 0,
            "wins": 0,
            "losses": 0,
            "pushes": 0,
            "units_wagered": 0.0,
            "units_pnl": 0.0,
            "roi_pct": 0.0,
            "win_rate": 0.0,
            "max_drawdown": 0.0,
            "sharpe": 0.0,
            "avg_ev": 0.0,
            "avg_kelly": 0.0,
        }

    by_agent: dict[str, dict] = defaultdict(empty_stats)
    by_season: dict[int, dict] = defaultdict(empty_stats)
    by_prop: dict[str, dict] = defaultdict(empty_stats)
    # Agent × Season cross-tab
    by_agent_season: dict[str, dict[int, dict]] = defaultdict(
        lambda: defaultdict(empty_stats)
    )

    # Track daily PnL per agent for drawdown + Sharpe
    agent_daily_pnl: dict[str, dict[str, float]] = defaultdict(
        lambda: defaultdict(float)
    )

    for bet in bets:
        for bucket, key in [
            (by_agent, bet.agent),
            (by_season, bet.season),
            (by_prop, bet.prop_type),
        ]:
            s = bucket[key]
            s["total_bets"] += 1
            s["units_wagered"] += bet.units_wagered
            s["units_pnl"] += bet.units_pnl
            s["avg_ev"] += bet.ev_pct
            s["avg_kelly"] += bet.kelly
            if bet.hit:
                s["wins"] += 1
            else:
                s["losses"] += 1

        by_agent_season[bet.agent][bet.season]["total_bets"] += 1
        by_agent_season[bet.agent][bet.season]["units_pnl"] += bet.units_pnl
        by_agent_season[bet.agent][bet.season]["wins"] += (1 if bet.hit else 0)
        by_agent_season[bet.agent][bet.season]["units_wagered"] += bet.units_wagered

        agent_daily_pnl[bet.agent][bet.game_date] += bet.units_pnl

    # Finalise rates + risk metrics
    for d in (by_agent, by_season, by_prop):
        for key, s in d.items():
            n = s["total_bets"]
            if n == 0:
                continue
            s["win_rate"] = round(s["wins"] / n, 4)
            s["roi_pct"] = round(
                s["units_pnl"] / s["units_wagered"] * 100
                if s["units_wagered"] > 0
                else 0,
                2,
            )
            s["avg_ev"] = round(s["avg_ev"] / n, 4)
            s["avg_kelly"] = round(s["avg_kelly"] / n, 4)

    # Drawdown + Sharpe per agent
    for agent, daily in agent_daily_pnl.items():
        pnl_series = [daily[d] for d in sorted(daily)]
        by_agent[agent]["max_drawdown"] = round(_max_drawdown(pnl_series), 4)
        by_agent[agent]["sharpe"] = round(_sharpe(pnl_series), 4)

    # Overall totals
    total_bets = len(bets)
    total_wins = sum(1 for b in bets if b.hit)
    total_wagered = sum(b.units_wagered for b in bets)
    total_pnl = sum(b.units_pnl for b in bets)

    return {
        "total_bets": total_bets,
        "total_wins": total_wins,
        "total_losses": total_bets - total_wins,
        "overall_win_rate": round(total_wins / total_bets, 4) if total_bets else 0,
        "total_units_wagered": round(total_wagered, 2),
        "total_units_pnl": round(total_pnl, 2),
        "overall_roi_pct": round(
            total_pnl / total_wagered * 100 if total_wagered > 0 else 0, 2
        ),
        "by_agent": {k: dict(v) for k, v in by_agent.items()},
        "by_season": {str(k): dict(v) for k, v in by_season.items()},
        "by_prop": {k: dict(v) for k, v in by_prop.items()},
        "by_agent_season": {
            agent: {str(s): dict(v) for s, v in seasons.items()}
            for agent, seasons in by_agent_season.items()
        },
    }


def _max_drawdown(pnl_series: list[float]) -> float:
    """Peak-to-trough maximum drawdown on cumulative P&L curve."""
    if not pnl_series:
        return 0.0
    cum = 0.0
    peak = 0.0
    max_dd = 0.0
    for pnl in pnl_series:
        cum += pnl
        if cum > peak:
            peak = cum
        dd = peak - cum
        if dd > max_dd:
            max_dd = dd
    return max_dd


def _sharpe(pnl_series: list[float], risk_free: float = 0.0) -> float:
    """Annualised Sharpe ratio on daily P&L (252 trading / game days)."""
    if len(pnl_series) < 2:
        return 0.0
    mean = statistics.mean(pnl_series) - risk_free
    std = statistics.stdev(pnl_series)
    if std == 0:
        return 0.0
    return (mean / std) * math.sqrt(252)


# ---------------------------------------------------------------------------
# CSV / JSON writers
# ---------------------------------------------------------------------------

def _write_per_bet_ledger(bets: list[SimulatedBet], output_dir: str) -> None:
    path = f"{output_dir}/report.csv"
    fields = [
        "season", "game_date", "player_id", "player_name", "prop_type",
        "line", "direction", "model_prob", "true_prob", "ev_pct", "kelly",
        "units_wagered", "actual_value", "hit", "units_pnl", "agent",
    ]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for bet in bets:
            writer.writerow(
                {
                    "season": bet.season,
                    "game_date": bet.game_date,
                    "player_id": bet.player_id,
                    "player_name": bet.player_name,
                    "prop_type": bet.prop_type,
                    "line": bet.line,
                    "direction": bet.direction,
                    "model_prob": bet.model_prob,
                    "true_prob": bet.true_prob,
                    "ev_pct": bet.ev_pct,
                    "kelly": bet.kelly,
                    "units_wagered": bet.units_wagered,
                    "actual_value": bet.actual_value,
                    "hit": bet.hit,
                    "units_pnl": bet.units_pnl,
                    "agent": bet.agent,
                }
            )
    logger.info("Per-bet ledger → %s", path)


def _write_agent_pnl(by_agent: dict, output_dir: str) -> None:
    path = f"{output_dir}/agent_pnl.csv"
    fields = [
        "agent", "total_bets", "wins", "losses", "win_rate",
        "units_wagered", "units_pnl", "roi_pct",
        "max_drawdown", "sharpe", "avg_ev", "avg_kelly",
    ]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for agent, stats in sorted(by_agent.items()):
            row = {"agent": agent, **stats}
            writer.writerow({k: row.get(k, "") for k in fields})
    logger.info("Agent P&L → %s", path)


def _write_season_pnl(by_season: dict, output_dir: str) -> None:
    path = f"{output_dir}/season_pnl.csv"
    fields = [
        "season", "total_bets", "wins", "losses", "win_rate",
        "units_wagered", "units_pnl", "roi_pct", "avg_ev",
    ]
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for season, stats in sorted(by_season.items()):
            row = {"season": season, **stats}
            writer.writerow({k: row.get(k, "") for k in fields})
    logger.info("Season P&L → %s", path)


# ---------------------------------------------------------------------------
# Print summary to stdout
# ---------------------------------------------------------------------------

def print_summary(summary: dict) -> None:
    sep = "=" * 70
    print(f"\n{sep}")
    print("  PropIQ 10-Season Historical Backtest Results (2016 – 2025)")
    print(sep)
    print(f"  Total bets       : {summary['total_bets']:,}")
    print(f"  Overall win rate : {summary['overall_win_rate']:.1%}")
    print(f"  Total units P&L  : {summary['total_units_pnl']:+.2f}")
    print(f"  Overall ROI      : {summary['overall_roi_pct']:+.2f}%")
    print(f"\n{'─' * 70}")
    print(f"  {'Agent':<18} {'Bets':>7} {'WR':>7} {'ROI':>8} {'Sharpe':>8} {'MaxDD':>8}")
    print(f"{'─' * 70}")
    for agent in AGENTS:
        s = summary["by_agent"].get(agent, {})
        if not s:
            continue
        print(
            f"  {agent:<18} {s['total_bets']:>7,} "
            f"{s['win_rate']:>7.1%} "
            f"{s['roi_pct']:>+8.2f}% "
            f"{s['sharpe']:>8.2f} "
            f"{s['max_drawdown']:>8.2f}"
        )
    print(f"\n{'─' * 70}")
    print(f"  {'Season':<10} {'Bets':>7} {'WR':>7} {'ROI':>8}")
    print(f"{'─' * 70}")
    for season, s in sorted(summary["by_season"].items()):
        if not s:
            continue
        print(
            f"  {season:<10} {s['total_bets']:>7,} "
            f"{s['win_rate']:>7.1%} {s['roi_pct']:>+8.2f}%"
        )
    print(sep)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="PropIQ 10-Season MLB Historical Backtest"
    )
    parser.add_argument(
        "--seasons",
        default="2016-2025",
        help="Season range in format YYYY-YYYY (default: 2016-2025)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Process only the first 14 days of each season (fast test)",
    )
    parser.add_argument(
        "--output-dir",
        default="backtest_results",
        help="Directory for output CSVs and JSON",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    start_year, end_year = (int(y) for y in args.seasons.split("-"))
    seasons_to_run = list(range(start_year, end_year + 1))

    logger.info(
        "Starting backtest: seasons=%s  dry_run=%s", seasons_to_run, args.dry_run
    )

    summary = run_backtest(
        seasons=seasons_to_run,
        dry_run=args.dry_run,
        output_dir=args.output_dir,
    )
    print_summary(summary)
    sys.exit(0)
