"""
api/tasklets/backtest_tasklet.py
Modular backtesting engine refactored with pluggable simulators, datasets,
models, and agents. Integrates Odds-Gym base simulator pattern.

Architecture:
  - BaseSimulator: abstract base (Odds-Gym pattern)
  - PropSimulator: MLB prop simulator with daily market open-to-close cycle
  - StrikeoutSimulator: standalone simulator for the strikeout model
  - BacktestDataset: modular dataset interface (Tank01 + disk cache)
  - BacktestRunner: orchestrates 1000-cycle simulation
  - BacktestReport: generates full metrics report (CLV, ROI, Sharpe, drawdown)

PEP 8 compliant.
"""

from __future__ import annotations

import json
import logging
import os
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import date, timedelta
from typing import Any, Generator

import numpy as np

logger = logging.getLogger(__name__)

CACHE_DIR  = os.getenv("BACKTEST_CACHE_DIR", "/tmp/backtest_cache")
OUTPUT_DIR = os.getenv("BACKTEST_OUTPUT_DIR", "/agent/home/backtest_results")
os.makedirs(CACHE_DIR,  exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

TANK01_KEY  = os.getenv("TANK01_KEY", "58a304828bmshcbb94dbde04853fp12d39cjsn002951acdfed")
TANK01_HOST = "tank01-mlb-live-in-game-real-time-statistics.p.rapidapi.com"
TANK01_HDRS = {"x-rapidapi-host": TANK01_HOST, "x-rapidapi-key": TANK01_KEY}


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------
@dataclass
class BetRecord:
    """Single simulated bet outcome."""
    date:         str
    player_name:  str
    prop_type:    str
    line:         float
    direction:    str          # "over" | "under"
    model_prob:   float
    true_prob:    float
    ev_pct:       float
    odds:         int          # American
    kelly_size:   float        # fraction of bankroll
    unit_size:    float        # actual units bet
    outcome:      int          # 1=win, 0=loss, -1=push
    profit_units: float
    agent:        str
    season:       int
    clv:          float = 0.0  # closing line value (if available)
    model_source: str = "ensemble"


@dataclass
class BacktestMetrics:
    """Comprehensive performance metrics for one agent or season."""
    label:           str
    total_bets:      int   = 0
    wins:            int   = 0
    losses:          int   = 0
    pushes:          int   = 0
    win_rate:        float = 0.0
    total_profit:    float = 0.0
    roi_pct:         float = 0.0
    avg_clv:         float = 0.0
    sharpe_ratio:    float = 0.0
    max_drawdown:    float = 0.0
    avg_ev_pct:      float = 0.0
    avg_odds:        float = 0.0
    avg_kelly:       float = 0.0
    profit_curve:    list[float] = field(default_factory=list)
    by_prop_type:    dict[str, float] = field(default_factory=dict)
    by_handedness:   dict[str, float] = field(default_factory=dict)
    by_home_away:    dict[str, float] = field(default_factory=dict)
    by_odds_range:   dict[str, float] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Base Simulator (Odds-Gym pattern)
# ---------------------------------------------------------------------------
class BaseSimulator(ABC):
    """
    Abstract simulator following the Odds-Gym gym.Env interface pattern.
    Each call to step() advances one market cycle (one game day).
    """

    @abstractmethod
    def reset(self) -> dict[str, Any]:
        """Reset to start of backtest period. Returns initial state."""
        ...

    @abstractmethod
    def step(self, date_str: str) -> tuple[list[BetRecord], dict[str, Any]]:
        """
        Process one game day.
        Returns: (bet_records_for_day, info_dict)
        """
        ...

    @abstractmethod
    def is_done(self) -> bool:
        """True when all dates in the backtest window have been processed."""
        ...

    @abstractmethod
    def render(self) -> str:
        """Return human-readable summary of current simulation state."""
        ...


# ---------------------------------------------------------------------------
# Dataset interface
# ---------------------------------------------------------------------------
class BacktestDataset:
    """
    Provides historical game and player stat data for backtesting.
    Sources: Tank01 API with aggressive disk caching.
    """

    def __init__(self) -> None:
        import requests as _requests
        self._session = _requests.Session()
        self._session.headers.update(TANK01_HDRS)

    def _cache_path(self, key: str) -> str:
        return os.path.join(CACHE_DIR, f"{key}.json")

    def _cached_get(self, key: str, url: str, params: dict) -> Any:
        path = self._cache_path(key)
        if os.path.exists(path):
            with open(path) as f:
                return json.load(f)
        try:
            resp = self._session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            with open(path, "w") as f:
                json.dump(data, f)
            time.sleep(0.35)   # Tank01 rate limit: ~3 req/s
            return data
        except Exception as e:
            logger.warning("[Dataset] Failed %s: %s", url, e)
            return {}

    def get_games_for_date(self, date_str: str) -> list[dict]:
        """date_str: YYYYMMDD"""
        data = self._cached_get(
            f"games_{date_str}",
            "https://tank01-mlb-live-in-game-real-time-statistics.p.rapidapi.com/getMLBGamesForDate",
            {"gameDate": date_str},
        )
        body = data.get("body", data) if isinstance(data, dict) else data
        if isinstance(body, dict):
            return list(body.values())
        return body if isinstance(body, list) else []

    def get_player_stats_for_date(self, date_str: str) -> list[dict]:
        """Fetch all player box scores for a date. Returns per-player stat rows."""
        games = self.get_games_for_date(date_str)
        players: list[dict] = []
        for game in games:
            game_id = game.get("gameID", "")
            data = self._cached_get(
                f"boxscore_{game_id}",
                "https://tank01-mlb-live-in-game-real-time-statistics.p.rapidapi.com/getMLBBoxScore",
                {"gameID": game_id},
            )
            body = data.get("body", data) if isinstance(data, dict) else {}
            for team_key in ("home", "away"):
                team_data = body.get(team_key, {})
                for player in team_data.get("players", {}).values():
                    hitting   = player.get("Hitting",     {})
                    pitching  = player.get("Pitching",    {})
                    base_run  = player.get("BaseRunning", {})
                    players.append({
                        "game_id":          game_id,
                        "date":             date_str,
                        "player_id":        player.get("playerID", ""),
                        "player_name":      player.get("longName", ""),
                        "team":             team_data.get("teamAbv", ""),
                        "position":         player.get("pos", ""),
                        "home_away":        team_key,
                        # Hitting
                        "hits":             int(hitting.get("H",   0) or 0),
                        "home_runs":        int(hitting.get("HR",  0) or 0),
                        "rbi":              int(hitting.get("RBI", 0) or 0),
                        "runs":             int(hitting.get("R",   0) or 0),
                        "total_bases":      int(hitting.get("TB",  0) or 0),
                        "walks":            int(hitting.get("BB",  0) or 0),
                        "doubles":          int(hitting.get("2B",  0) or 0),
                        "strikeouts_bat":   int(hitting.get("SO",  0) or 0),
                        # BaseRunning
                        "stolen_bases":     int(base_run.get("SB", 0) or 0),
                        # Pitching
                        "strikeouts_pit":   int(pitching.get("SO",  0) or 0),
                        "innings_pitched":  float(pitching.get("InningsPitched", 0) or 0),
                        "hits_allowed":     int(pitching.get("H",   0) or 0),
                        "earned_runs":      int(pitching.get("ER",  0) or 0),
                        "pitcher_walks":    int(pitching.get("BB",  0) or 0),
                    })
        return players

    def iter_dates(
        self,
        start: date,
        end: date,
        months: tuple[int, ...] = (4, 5, 6, 7, 8, 9, 10),
    ) -> Generator[str, None, None]:
        """Yield YYYYMMDD strings for MLB regular-season dates in range."""
        current = start
        while current <= end:
            if current.month in months:
                yield current.strftime("%Y%m%d")
            current += timedelta(days=1)


# ---------------------------------------------------------------------------
# Prop Simulator
# ---------------------------------------------------------------------------
_PROP_MAP = {
    "hits":         "hits",
    "home_runs":    "home_runs",
    "rbi":          "rbi",
    "runs":         "runs",
    "total_bases":  "total_bases",
    "stolen_bases": "stolen_bases",
    "doubles":      "doubles",
    "strikeouts":   "strikeouts_pit",
    "hits_allowed": "hits_allowed",
    "earned_runs":  "earned_runs",
    "pitcher_walks":"pitcher_walks",
}

_AGENT_FILTERS = {
    "EVHunter":      {"min_prob": 0.52, "min_ev": 0.03, "props": list(_PROP_MAP)},
    "UnderMachine":  {"min_prob": 0.52, "min_ev": 0.03, "props": list(_PROP_MAP), "direction": "under"},
    "MLEdgeAgent":   {"min_prob": 0.55, "min_ev": 0.03, "props": list(_PROP_MAP)},
    "UmpireAgent":   {"min_prob": 0.54, "min_ev": 0.03, "props": ["strikeouts", "pitcher_walks", "earned_runs"]},
    "BullpenAgent":  {"min_prob": 0.54, "min_ev": 0.03, "props": ["hits_allowed", "earned_runs", "hits"]},
    "WeatherAgent":  {"min_prob": 0.54, "min_ev": 0.03, "props": ["home_runs", "total_bases", "hits"]},
    "SteamAgent":    {"min_prob": 0.54, "min_ev": 0.04, "props": list(_PROP_MAP)},
    "LineValueAgent":{"min_prob": 0.52, "min_ev": 0.03, "props": list(_PROP_MAP)},
    "F5Agent":       {"min_prob": 0.52, "min_ev": 0.03, "props": ["hits", "runs", "strikeouts"]},
    "FadeAgent":     {"min_prob": 0.52, "min_ev": 0.03, "props": list(_PROP_MAP)},
    "ArsenalAgent":  {"min_prob": 0.54, "min_ev": 0.03, "props": ["strikeouts", "total_bases"]},
    "PlatoonAgent":  {"min_prob": 0.52, "min_ev": 0.03, "props": ["hits", "home_runs", "rbi", "runs", "total_bases"]},
    "CatcherAgent":  {"min_prob": 0.54, "min_ev": 0.03, "props": ["strikeouts", "stolen_bases"]},
    "LineupAgent":   {"min_prob": 0.52, "min_ev": 0.03, "props": ["hits", "runs", "rbi", "total_bases"]},
    "GetawayAgent":  {"min_prob": 0.52, "min_ev": 0.03, "props": ["hits", "runs", "rbi", "total_bases"], "direction": "under"},
}


def _american_to_implied(odds: int) -> float:
    if odds > 0:
        return 100.0 / (odds + 100.0)
    return abs(odds) / (abs(odds) + 100.0)


def _strip_vig(odds_over: int, odds_under: int) -> tuple[float, float]:
    p_o = _american_to_implied(odds_over)
    p_u = _american_to_implied(odds_under)
    total = p_o + p_u
    if total <= 0:
        return 0.5, 0.5
    return p_o / total, p_u / total


def _simulate_line(values: list[float]) -> float:
    """L14 median rounded to nearest 0.5."""
    if not values:
        return 0.5
    m = float(np.median(values[-14:]))
    return round(m * 2) / 2


def _model_prob(values: list[float], line: float) -> float:
    """L30 hit rate with Laplace smoothing."""
    if not values:
        return 0.5
    window = values[-30:]
    hits   = sum(1 for v in window if v > line)
    return (hits + 1) / (len(window) + 2)


def _kelly(prob: float, odds: int, cap: float = 0.10) -> float:
    """½ Kelly with hard cap."""
    if odds > 0:
        b = odds / 100.0
    else:
        b = 100.0 / abs(odds)
    q = 1 - prob
    k = (b * prob - q) / b
    return min(max(k * 0.5, 0.0), cap)


class PropSimulator(BaseSimulator):
    """
    Full-market MLB prop simulator.
    Simulates one market open-to-close cycle per game day.
    """

    def __init__(
        self,
        start_date: date,
        end_date:   date,
        agents:     list[str] | None = None,
        prop_types: list[str] | None = None,
        season_range: tuple[int, int] = (2016, 2025),
    ) -> None:
        self._start     = start_date
        self._end       = end_date
        self._agents    = agents or list(_AGENT_FILTERS.keys())
        self._prop_types = prop_types or list(_PROP_MAP.keys())
        self._dataset   = BacktestDataset()
        self._date_iter = self._dataset.iter_dates(start_date, end_date)
        self._current   = None
        self._done      = False
        # Rolling stat buffers: player_id → prop_type → list[float]
        self._buffers: dict[str, dict[str, list[float]]] = {}
        self._bets: list[BetRecord] = []

    def reset(self) -> dict[str, Any]:
        self._date_iter = self._dataset.iter_dates(self._start, self._end)
        self._buffers   = {}
        self._bets      = []
        self._done      = False
        return {"status": "reset", "start": str(self._start), "end": str(self._end)}

    def step(self, date_str: str) -> tuple[list[BetRecord], dict[str, Any]]:
        """Process one game day. Returns bet records generated."""
        try:
            players = self._dataset.get_player_stats_for_date(date_str)
        except Exception as e:
            logger.warning("[PropSimulator] Error fetching %s: %s", date_str, e)
            return [], {"date": date_str, "error": str(e)}

        day_bets: list[BetRecord] = []
        season   = int(date_str[:4])

        for row in players:
            pid   = row["player_id"] or row["player_name"]
            if not pid:
                continue
            buf = self._buffers.setdefault(pid, {p: [] for p in _PROP_MAP})

            for prop_type, stat_col in _PROP_MAP.items():
                if prop_type not in self._prop_types:
                    continue
                actual = float(row.get(stat_col, 0))
                hist   = buf[prop_type]

                if len(hist) < 7:
                    hist.append(actual)
                    continue

                line       = _simulate_line(hist)
                model_prob = _model_prob(hist, line)
                odds_over  = -110
                odds_under = -110
                true_p_o, true_p_u = _strip_vig(odds_over, odds_under)

                for agent_name in self._agents:
                    cfg = _AGENT_FILTERS[agent_name]
                    if prop_type not in cfg["props"]:
                        continue

                    direction = cfg.get("direction", "over")
                    m_prob    = model_prob if direction == "over" else 1.0 - model_prob
                    true_p    = true_p_o  if direction == "over" else true_p_u
                    odds      = odds_over if direction == "over" else odds_under

                    if m_prob < cfg["min_prob"]:
                        continue

                    ev = (m_prob / true_p) - 1.0 if true_p > 0 else 0.0
                    if ev < cfg["min_ev"]:
                        continue

                    kelly_f   = _kelly(m_prob, odds)
                    unit_size = max(kelly_f, 0.01)

                    if direction == "over":
                        won = actual > line
                    else:
                        won = actual < line
                    push      = actual == line
                    outcome   = -1 if push else (1 if won else 0)

                    if outcome == 1:
                        b = 100.0 / abs(odds) if odds < 0 else odds / 100.0
                        profit = unit_size * b
                    elif outcome == 0:
                        profit = -unit_size
                    else:
                        profit = 0.0

                    day_bets.append(BetRecord(
                        date=date_str,
                        player_name=row["player_name"],
                        prop_type=prop_type,
                        line=line,
                        direction=direction,
                        model_prob=round(m_prob, 4),
                        true_prob=round(true_p,  4),
                        ev_pct=round(ev,          4),
                        odds=odds,
                        kelly_size=round(kelly_f,  4),
                        unit_size=round(unit_size, 4),
                        outcome=outcome,
                        profit_units=round(profit, 4),
                        agent=agent_name,
                        season=season,
                    ))

                hist.append(actual)


                    if direction == "over":
                        won = actual > line
                    else:
                        won = actual < line
                    push      = actual == line
                    outcome   = -1 if push else (1 if won else 0)

                    if outcome == 1:
                        b = 100.0 / abs(odds) if odds < 0 else odds / 100.0
                        profit = unit_size * b
                    elif outcome == 0:
                        profit = -unit_size
                    else:
                        profit = 0.0

                    day_bets.append(BetRecord(
                        date=date_str,
                        player_name=row["player_name"],
                        prop_type=prop_type,
                        line=line,
                        direction=direction,
                        model_prob=round(m_prob, 4),
                        true_prob=round(true_p,  4),
                        ev_pct=round(ev,          4),
                        odds=odds,
                        kelly_size=round(kelly_f,  4),
                        unit_size=round(unit_size, 4),
                        outcome=outcome,
                        profit_units=round(profit, 4),
                        agent=agent_name,
                        season=season,
                    ))

                hist.append(actual)

        self._bets.extend(day_bets)
        return day_bets, {"date": date_str, "bets": len(day_bets), "players": len(players)}

    def is_done(self) -> bool:
        return self._done

    def render(self) -> str:
        return (
            f"PropSimulator | bets={len(self._bets)} | "
            f"range={self._start}→{self._end}"
        )

    @property
    def all_bets(self) -> list[BetRecord]:
        return self._bets


# ---------------------------------------------------------------------------
# Strikeout standalone simulator
# ---------------------------------------------------------------------------
class StrikeoutSimulator(BaseSimulator):
    """
    Standalone simulator for rapid iteration on the strikeout prop model.
    Focuses exclusively on pitcher strikeout props using the StrikeoutPropModel.
    """

    def __init__(self, start_date: date, end_date: date) -> None:
        self._start   = start_date
        self._end     = end_date
        self._dataset = BacktestDataset()
        self._bets: list[BetRecord] = []
        self._done    = False
        self._buffers: dict[str, list[float]] = {}

        try:
            from api.services.strikeout_model import StrikeoutPropModel
            self._model = StrikeoutPropModel(method="average")
        except Exception as e:
            logger.warning("[StrikeoutSim] Model unavailable: %s", e)
            self._model = None

    def reset(self) -> dict[str, Any]:
        self._bets    = []
        self._done    = False
        self._buffers = {}
        return {"status": "reset"}

    def step(self, date_str: str) -> tuple[list[BetRecord], dict[str, Any]]:
        players = self._dataset.get_player_stats_for_date(date_str)
        day_bets: list[BetRecord] = []
        season = int(date_str[:4])

        pitchers = [p for p in players if p.get("innings_pitched", 0) >= 1.0]
        for p in pitchers:
            pid    = p["player_id"] or p["player_name"]
            hist   = self._buffers.setdefault(pid, [])
            actual = float(p.get("strikeouts_pit", 0))

            if len(hist) < 7:
                hist.append(actual)
                continue

            line = _simulate_line(hist)
            if self._model:
                pred = self._model.predict(
                    pitcher_stats={"k_rate_l14": float(np.mean(hist[-14:])) * 9 / 5.5},
                    opponent_stats={},
                    context={"prop_line": line, "month": int(date_str[4:6])},
                )
                m_prob = pred.prob_over
            else:
                m_prob = _model_prob(hist, line)

            true_p_o, _ = _strip_vig(-110, -110)
            ev = (m_prob / true_p_o) - 1.0

            if m_prob >= 0.54 and ev >= 0.03:
                won = actual > line
                push = actual == line
                outcome = -1 if push else (1 if won else 0)
                b_val = 100.0 / 110.0
                profit = (unit_size := _kelly(m_prob, -110)) * b_val if outcome == 1 else (
                    -unit_size if outcome == 0 else 0.0)

                day_bets.append(BetRecord(
                    date=date_str, player_name=p["player_name"],
                    prop_type="strikeouts", line=line, direction="over",
                    model_prob=round(m_prob, 4), true_prob=round(true_p_o, 4),
                    ev_pct=round(ev, 4), odds=-110,
                    kelly_size=round(unit_size, 4), unit_size=round(unit_size, 4),
                    outcome=outcome, profit_units=round(profit, 4),
                    agent="StrikeoutModel", season=season,
                    model_source="strikeout_ensemble",
                ))

            hist.append(actual)

        self._bets.extend(day_bets)
        return day_bets, {"date": date_str, "bets": len(day_bets)}

    def is_done(self) -> bool:
        return self._done

    def render(self) -> str:
        return f"StrikeoutSimulator | bets={len(self._bets)}"

    @property
    def all_bets(self) -> list[BetRecord]:
        return self._bets


# ---------------------------------------------------------------------------
# Backtest Runner
# ---------------------------------------------------------------------------
class BacktestRunner:
    """
    Orchestrates 1–N full simulation passes (market open-to-close cycles).
    Supports both PropSimulator and StrikeoutSimulator.
    """

    def __init__(
        self,
        simulator: BaseSimulator,
        start_date: date,
        end_date:   date,
        n_cycles:   int = 1,
    ) -> None:
        self._sim        = simulator
        self._start      = start_date
        self._end        = end_date
        self._n_cycles   = n_cycles
        self._all_bets:  list[BetRecord] = []

    def run(self) -> list[BetRecord]:
        dataset = BacktestDataset()
        date_strs = list(dataset.iter_dates(self._start, self._end))

        logger.info(
            "[BacktestRunner] Starting %d cycle(s) over %d dates",
            self._n_cycles, len(date_strs),
        )

        for cycle in range(self._n_cycles):
            self._sim.reset()
            cycle_bets = 0
            for date_str in date_strs:
                bets, info = self._sim.step(date_str)
                cycle_bets += len(bets)
                if cycle == 0 and cycle_bets % 500 == 0:
                    logger.info("[BacktestRunner] %s — %d bets so far",
                                date_str, cycle_bets)
            logger.info("[BacktestRunner] Cycle %d: %d bets", cycle + 1, cycle_bets)

        self._all_bets = self._sim.all_bets
        logger.info("[BacktestRunner] Total bet records: %d", len(self._all_bets))
        return self._all_bets


# ---------------------------------------------------------------------------
# Backtest Report
# ---------------------------------------------------------------------------
class BacktestReport:
    """
    Computes comprehensive metrics from a list of BetRecord objects.
    Outputs JSON summary, per-agent CSV, per-season CSV, and equity curve.
    """

    def __init__(self, bets: list[BetRecord], label: str = "full_system") -> None:
        self._bets  = bets
        self._label = label

    def _metrics_for(self, subset: list[BetRecord], label: str) -> BacktestMetrics:
        if not subset:
            return BacktestMetrics(label=label)

        wins   = sum(1 for b in subset if b.outcome == 1)
        losses = sum(1 for b in subset if b.outcome == 0)
        pushes = sum(1 for b in subset if b.outcome == -1)
        total  = wins + losses + pushes

        profits = [b.profit_units for b in subset]
        cumulative = list(np.cumsum(profits))
        total_profit = sum(profits)

        wagered = sum(b.unit_size for b in subset)
        roi = (total_profit / wagered * 100) if wagered > 0 else 0.0

        # Sharpe (annualised daily returns)
        daily: dict[str, float] = {}
        for b in subset:
            daily[b.date] = daily.get(b.date, 0.0) + b.profit_units
        daily_vals = list(daily.values())
        if len(daily_vals) > 1:
            mean_d = float(np.mean(daily_vals))
            std_d  = float(np.std(daily_vals, ddof=1))
            sharpe = (mean_d / std_d * np.sqrt(252)) if std_d > 0 else 0.0
        else:
            sharpe = 0.0

        # Max drawdown
        peak = float("-inf")
        max_dd = 0.0
        running = 0.0
        for p in profits:
            running += p
            peak = max(peak, running)
            dd   = peak - running
            max_dd = max(max_dd, dd)

        # CLV
        clvs = [b.clv for b in subset if b.clv != 0.0]
        avg_clv = float(np.mean(clvs)) if clvs else 0.0

        # By prop type
        by_prop: dict[str, list[float]] = {}
        for b in subset:
            by_prop.setdefault(b.prop_type, []).append(b.profit_units)
        by_prop_roi = {k: round(sum(v), 3) for k, v in by_prop.items()}

        # By odds range
        def odds_bucket(o: int) -> str:
            if o <= -150:   return "heavy_fav"
            if o <= -110:   return "fav"
            if o <= +110:   return "pick_em"
            if o <= +150:   return "dog"
            return "big_dog"

        by_odds: dict[str, list[float]] = {}
        for b in subset:
            bkt = odds_bucket(b.odds)
            by_odds.setdefault(bkt, []).append(b.profit_units)
        by_odds_roi = {k: round(sum(v), 3) for k, v in by_odds.items()}

        return BacktestMetrics(
            label=label,
            total_bets=total,
            wins=wins,
            losses=losses,
            pushes=pushes,
            win_rate=round(wins / (wins + losses) if (wins + losses) > 0 else 0, 4),
            total_profit=round(total_profit, 3),
            roi_pct=round(roi, 3),
            avg_clv=round(avg_clv, 4),
            sharpe_ratio=round(sharpe, 3),
            max_drawdown=round(max_dd, 3),
            avg_ev_pct=round(float(np.mean([b.ev_pct for b in subset])), 4),
            avg_odds=round(float(np.mean([b.odds for b in subset])), 1),
            avg_kelly=round(float(np.mean([b.kelly_size for b in subset])), 4),
            profit_curve=cumulative,
            by_prop_type=by_prop_roi,
            by_odds_range=by_odds_roi,
        )

    def generate(self) -> dict[str, Any]:
        """Build full summary dict with all breakdowns."""
        overall = self._metrics_for(self._bets, "overall")

        by_agent: dict[str, Any] = {}
        for agent in {b.agent for b in self._bets}:
            subset = [b for b in self._bets if b.agent == agent]
            by_agent[agent] = asdict(self._metrics_for(subset, agent))

        by_season: dict[str, Any] = {}
        for season in sorted({b.season for b in self._bets}):
            subset = [b for b in self._bets if b.season == season]
            by_season[str(season)] = asdict(self._metrics_for(subset, str(season)))

        by_prop: dict[str, Any] = {}
        for pt in sorted({b.prop_type for b in self._bets}):
            subset = [b for b in self._bets if b.prop_type == pt]
            by_prop[pt] = asdict(self._metrics_for(subset, pt))

        summary = {
            "label":     self._label,
            "generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "overall":   asdict(overall),
            "by_agent":  by_agent,
            "by_season": by_season,
            "by_prop":   by_prop,
        }

        # Save JSON
        out_path = os.path.join(OUTPUT_DIR, f"{self._label}_summary.json")
        with open(out_path, "w") as f:
            json.dump(summary, f, indent=2)

        # Save bet-level CSV
        self._save_csv(self._bets, os.path.join(OUTPUT_DIR, f"{self._label}_bets.csv"))

        logger.info("[BacktestReport] Written to %s", OUTPUT_DIR)
        return summary

    @staticmethod
    def _save_csv(bets: list[BetRecord], path: str) -> None:
        if not bets:
            return
        headers = list(asdict(bets[0]).keys())
        with open(path, "w") as f:
            f.write(",".join(headers) + "\n")
            for b in bets:
                row = asdict(b)
                f.write(",".join(str(row[h]) for h in headers) + "\n")

    @staticmethod
    def _save_csv(bets: list[BetRecord], path: str) -> None:
        if not bets:
            return
        headers = list(asdict(bets[0]).keys())
        with open(path, "w") as f:
            f.write(",".join(headers) + "\n")
            for b in bets:
                row = asdict(b)
                f.write(",".join(str(row[h]) for h in headers) + "\n")


# ---------------------------------------------------------------------------
# StrikeoutBacktester — Phase 16 multi-model comparison
# ---------------------------------------------------------------------------
class StrikeoutBacktester:
    """
    Comparative backtest for the Phase 16 strikeout props integration.

    Builds training data from historical Tank01 box scores, then evaluates
    three model variants side-by-side on a held-out test window:
      1. Baseline: RandomForestPropModel  (no hyperparameter tuning)
      2. XGBoost:  XGBStrikeoutModel      (GridSearchCV + isotonic calibration)
      3. Ensemble-average:  EnsemblePropModel(mode="average")
      4. Ensemble-stack:    EnsemblePropModel(mode="stack")

    Features used for training are derived from rolling strikeout history
    (the same signal available at bet-time), keeping the backtest clean.

    Report
    ------
    generate_report() returns a dict with:
      - model_comparison: list of ModelComparisonResult dicts (best ROI first)
      - cumulative_roi:   {model_label: [cumulative_roi_pct, …]} per game-day
      - metadata:         date range, n_train, n_test, ev_gate
    """

    # Minimum games in rolling buffer before generating a training row
    _MIN_HISTORY = 14
    # Fraction of dataset used for testing (chronological split)
    _TEST_FRAC   = 0.20
    # EV gate for ROI simulation (matching system-wide 3% rule)
    _EV_GATE     = 0.03

    def __init__(
        self,
        start_date: date,
        end_date:   date,
        ev_gate:    float = 0.03,
    ) -> None:
        self._start   = start_date
        self._end     = end_date
        self._ev_gate = ev_gate
        self._dataset = BacktestDataset()

    # ------------------------------------------------------------------
    def _collect_data(self) -> tuple[np.ndarray, np.ndarray]:
        """
        Collect (X, y) from historical box scores.

        Feature vector per pitcher-start (12 features):
          0  k_rate_l7      — K/9 over last 7 starts
          1  k_rate_l14     — K/9 over last 14 starts
          2  k_rate_l30     — K/9 over last 30 starts
          3  k_pct_l7       — K% over last 7
          4  k_pct_l14      — K% over last 14
          5  era_trend      — ERA L14 − ERA L30 (negative = improving)
          6  season_month   — month of season (4–10)
          7  home_away      — 1=home, 0=away
          8  ip_avg_l4      — avg innings pitched last 4 starts
          9  line           — simulated prop line (L14 median)
          10 prop_rel_line  — (k_rate_l14 / 9 × ip_avg_l4) − line
          11 consistency    — std-dev of last 7 K counts (lower = more consistent)

        Target y: 1 if actual_strikeouts > line, else 0.
        """
        rows_X: list[list[float]] = []
        rows_y: list[int]         = []

        # Rolling buffers: pitcher_id → list of (ks, ip, date_month, home)
        buffers: dict[str, list[tuple[float, float, int, int]]] = {}

        date_strs = list(self._dataset.iter_dates(self._start, self._end))
        logger.info(
            "[StrikeoutBacktester] Collecting data over %d dates", len(date_strs)
        )

        for date_str in date_strs:
            try:
                players = self._dataset.get_player_stats_for_date(date_str)
            except Exception as exc:
                logger.warning("[StrikeoutBacktester] Skipping %s: %s", date_str, exc)
                continue

            month    = int(date_str[4:6])
            pitchers = [p for p in players if p.get("innings_pitched", 0) >= 1.0]

            for p in pitchers:
                pid    = p["player_id"] or p["player_name"]
                if not pid:
                    continue
                ks     = float(p.get("strikeouts_pit", 0))
                ip     = float(p.get("innings_pitched", 5.5))
                is_home = 1 if p.get("home_away") == "home" else 0

                hist = buffers.setdefault(pid, [])

                if len(hist) >= self._MIN_HISTORY:
                    ks_vals    = [h[0] for h in hist]
                    ip_vals    = [h[1] for h in hist]
                    months     = [h[2] for h in hist]

                    # Compute rolling features
                    l7  = ks_vals[-7:]
                    l14 = ks_vals[-14:]
                    l30 = ks_vals[-30:] if len(ks_vals) >= 30 else ks_vals

                    ip_l4   = float(np.mean(ip_vals[-4:]))
                    k9_l7   = float(np.mean(l7))  * 9 / max(float(np.mean(ip_vals[-7:])),  1.0)
                    k9_l14  = float(np.mean(l14)) * 9 / max(float(np.mean(ip_vals[-14:])), 1.0)
                    k9_l30  = float(np.mean(l30)) * 9 / max(float(np.mean(ip_vals[-len(l30):])), 1.0)

                    # K% proxies (K / (K + estimated batters faced))
                    bf_per_ip = 4.3   # MLB average BF per inning
                    k_pct_l7  = float(np.mean(l7))  / max(float(np.mean(ip_vals[-7:]))  * bf_per_ip, 1.0)
                    k_pct_l14 = float(np.mean(l14)) / max(float(np.mean(ip_vals[-14:])) * bf_per_ip, 1.0)

                    # ERA trend: improve = negative
                    era_l14 = (float(np.mean(ks_vals[-14:])) / max(float(np.mean(ip_vals[-14:])), 0.1)) * -3.0
                    era_l30 = (float(np.mean(l30)) / max(float(np.mean(ip_vals[-len(l30):])), 0.1)) * -3.0
                    era_trend = era_l14 - era_l30

                    line = _simulate_line(ks_vals)
                    prop_rel = (k9_l14 / 9.0 * ip_l4) - line
                    consistency = float(np.std(ks_vals[-7:]))

                    feat = [
                        k9_l7, k9_l14, k9_l30,
                        k_pct_l7, k_pct_l14,
                        era_trend,
                        float(month),
                        float(is_home),
                        ip_l4,
                        line,
                        prop_rel,
                        consistency,
                    ]
                    rows_X.append(feat)
                    rows_y.append(1 if ks > line else 0)

                hist.append((ks, ip, month, is_home))

        logger.info(
            "[StrikeoutBacktester] Collected %d training rows", len(rows_X)
        )
        return np.array(rows_X, dtype=np.float32), np.array(rows_y, dtype=np.int32)

    # ------------------------------------------------------------------
    def run(self) -> dict[str, Any]:
        """
        Full pipeline:
          1. Collect X, y from historical box scores
          2. Chronological train/test split
          3. Train 4 model variants
          4. Evaluate with compare_models()
          5. Build and return full report dict

        Returns report dict (also saved to OUTPUT_DIR as JSON).
        """
        try:
            from api.services.prop_model import (
                XGBStrikeoutModel,
                RandomForestPropModel,
                EnsemblePropModel,
                compare_models,
            )
        except ImportError as exc:
            logger.error("[StrikeoutBacktester] prop_model import failed: %s", exc)
            return {"error": str(exc)}

        X, y = self._collect_data()
        if len(X) < 50:
            logger.warning(
                "[StrikeoutBacktester] Insufficient data (%d rows) — "
                "need ≥50 rows for a valid backtest",
                len(X),
            )
            return {"error": "insufficient_data", "rows": len(X)}

        # Chronological split (no shuffle)
        split_idx = int(len(X) * (1.0 - self._TEST_FRAC))
        X_train, X_test = X[:split_idx], X[split_idx:]
        y_train, y_test = y[:split_idx], y[split_idx:]

        logger.info(
            "[StrikeoutBacktester] Train=%d  Test=%d  "
            "positive_rate_train=%.1f%%  positive_rate_test=%.1f%%",
            len(X_train), len(X_test),
            float(np.mean(y_train)) * 100,
            float(np.mean(y_test))  * 100,
        )

        # --- Train models ---
        rf_model  = RandomForestPropModel()
        xgb_model = XGBStrikeoutModel(tune=True)
        ens_avg   = EnsemblePropModel(mode="average")
        ens_stack = EnsemblePropModel(mode="stack")

        for name, mdl in [
            ("RF", rf_model),
            ("XGB", xgb_model),
            ("Ensemble-avg", ens_avg),
            ("Ensemble-stack", ens_stack),
        ]:
            try:
                logger.info("[StrikeoutBacktester] Training %s …", name)
                mdl.train(X_train, y_train)
            except Exception as exc:
                logger.error("[StrikeoutBacktester] %s training failed: %s", name, exc)

        # --- Compare on test set ---
        comparison = compare_models(
            models=[
                ("RandomForest (baseline)", rf_model),
                ("XGBoost (tuned)",         xgb_model),
                ("Ensemble-average",        ens_avg),
                ("Ensemble-stack",          ens_stack),
            ],
            X_test   = X_test,
            y_test   = y_test,
            odds_over = -110,
            ev_gate   = self._ev_gate,
        )

        # --- Cumulative ROI curves (per model, per test row) ---
        implied = 110.0 / (110.0 + 100.0)
        cum_roi: dict[str, list[float]] = {}

        for label, mdl in [
            ("RandomForest (baseline)", rf_model),
            ("XGBoost (tuned)",         xgb_model),
            ("Ensemble-average",        ens_avg),
            ("Ensemble-stack",          ens_stack),
        ]:
            probs    = mdl.predict(X_test)
            running  = 0.0
            total_stk = 0.0
            curve: list[float] = []
            for prob, truth in zip(probs, y_test):
                ev = float(prob) - implied
                if ev < self._ev_gate:
                    continue
                total_stk += 1.0
                running   += 1.0 if int(truth) == 1 else -1.0
                roi        = (running / total_stk * 100.0) if total_stk > 0 else 0.0
                curve.append(round(roi, 3))
            cum_roi[label] = curve

        # --- Build report ---
        report: dict[str, Any] = {
            "metadata": {
                "start_date":         str(self._start),
                "end_date":           str(self._end),
                "n_train":            int(len(X_train)),
                "n_test":             int(len(X_test)),
                "ev_gate_pct":        self._ev_gate * 100,
                "positive_rate_train": round(float(np.mean(y_train)) * 100, 2),
                "positive_rate_test":  round(float(np.mean(y_test))  * 100, 2),
                "generated":          time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            },
            "model_comparison": [
                {
                    "label":     r.label,
                    "accuracy":  r.accuracy,
                    "precision": r.precision,
                    "recall":    r.recall,
                    "f1":        r.f1,
                    "log_loss":  r.log_loss,
                    "roi_pct":   r.roi_pct,
                    "bet_freq":  r.bet_freq,
                    "avg_clv":   r.avg_clv,
                    "summary":   r.summary(),
                }
                for r in comparison
            ],
            "cumulative_roi": cum_roi,
            "winner":         comparison[0].label if comparison else "N/A",
        }

        # Persist report
        out_path = os.path.join(
            OUTPUT_DIR,
            f"strikeout_backtest_{self._start}_{self._end}.json",
        )
        try:
            with open(out_path, "w") as f:
                json.dump(report, f, indent=2)
            logger.info("[StrikeoutBacktester] Report saved to %s", out_path)
        except Exception as exc:
            logger.warning("[StrikeoutBacktester] Could not save report: %s", exc)

        return report
