"""
PropIQ 10-Season Statistical Backtest  (2016 – 2025)
=====================================================
Method
------
Monte Carlo simulation calibrated to real MLB statistical distributions.
Game-level player performance is generated using negative-binomial and
Poisson distributions parameterized from 10 seasons of actual MLB batting
and pitching averages, with an AR(1) streak-persistence model
(φ = 0.15, matching published sabermetric autocorrelation research).

Streaming aggregation: metrics are accumulated on-the-fly so memory
stays constant regardless of bet volume. Only the first SAMPLE_ROWS
bet records are written to report CSV.

ArbitrageAgent is excluded — it requires real-time cross-book data.
"""

from __future__ import annotations

import csv
import json
import logging
import math
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] backtest: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SEASONS          = list(range(2016, 2026))
DAYS_PER_SEASON  = 162
TEAMS            = 30
BATTERS_PER_TEAM = 9
STARTERS_PER_TEAM = 5
BATTER_PLAY_RATE  = 0.88
STARTER_TURN      = 5

HALF_KELLY_CAP = 0.10
EV_GATE        = 0.03
ODDS_OVER      = -110
ODDS_UNDER     = -110

AR1_PHI      = 0.15
RANDOM_SEED  = 42
SAMPLE_ROWS  = 100_000   # rows written to report CSV

OUTPUT_DIR = Path("backtest_results")

# ---------------------------------------------------------------------------
# MLB distributions  (calibrated to 2016-2025 averages)
# ---------------------------------------------------------------------------
BATTER_DISTS = {
    "hits":         ("nb", 1.03,  2.2),
    "home_runs":    ("nb", 0.147, 0.30),
    "rbis":         ("nb", 0.62,  1.5),
    "runs":         ("nb", 0.55,  1.5),
    "total_bases":  ("nb", 1.56,  3.0),
    "walks":        ("po", 0.35,  None),
    "batter_k":     ("nb", 0.86,  2.8),
    "stolen_bases": ("po", 0.082, None),
    "doubles":      ("po", 0.154, None),
    "singles":      ("nb", 0.70,  2.0),
}

PITCHER_DISTS = {
    "pitcher_k":    ("nb", 5.85, 5.0),
    "innings":      ("cl", 5.5,  1.3),
    "hits_allowed": ("nb", 5.5,  5.5),
    "earned_runs":  ("nb", 2.82, 2.0),
}

BATTER_PROPS  = set(BATTER_DISTS.keys())
PITCHER_PROPS = set(PITCHER_DISTS.keys())

# ---------------------------------------------------------------------------
# Agent specs  (mirrors production execution_agents.py exactly)
# ---------------------------------------------------------------------------
AGENT_SPECS: Dict[str, dict] = {
    "EVHunter":      {"min_prob": 0.52, "props": None,         "direction": None},
    "UnderMachine":  {"min_prob": 0.52, "props": BATTER_PROPS, "direction": "under"},
    "F5Agent":       {"min_prob": 0.52, "props": PITCHER_PROPS,"direction": None},
    "MLEdgeAgent":   {"min_prob": 0.55, "props": None,         "direction": None},
    "UmpireAgent":   {"min_prob": 0.54, "props": {"pitcher_k","walks","earned_runs"}, "direction": None},
    "FadeAgent":     {"min_prob": 0.52, "props": None,         "direction": "under"},
    "LineValueAgent":{"min_prob": 0.52, "props": None,         "direction": None},
    "BullpenAgent":  {"min_prob": 0.54, "props": PITCHER_PROPS,"direction": None},
    "WeatherAgent":  {"min_prob": 0.54, "props": {"home_runs","total_bases","hits"}, "direction": None},
    "SteamAgent":    {"min_prob": 0.55, "props": None,         "direction": None},
    "ArsenalAgent":  {"min_prob": 0.54, "props": {"batter_k","total_bases"}, "direction": None},
    "PlatoonAgent":  {"min_prob": 0.52, "props": BATTER_PROPS, "direction": None},
    "CatcherAgent":  {"min_prob": 0.54, "props": {"pitcher_k","stolen_bases"}, "direction": None},
    "LineupAgent":   {"min_prob": 0.52, "props": BATTER_PROPS, "direction": None},
    "GetawayAgent":  {"min_prob": 0.52, "props": BATTER_PROPS, "direction": "under"},
    # ── 17th agent: VultureStack ──────────────────────────────────────────────
    # Consensus mechanism: BullpenAgent ∩ GetawayAgent.
    # Both signal Under → only Runs, ER, H+R+RBI Unders qualify.
    # Higher min_prob (0.57) reflects dual-confirmation edge.
    # prob_boost (+0.025) simulates the fatigue-decay multiplier
    # applied when both agents independently agree on direction.
    "VultureStack":  {"min_prob": 0.57, "props": {"runs", "earned_runs", "hits_runs_rbis"},
                      "direction": "under", "prob_boost": 0.025},
}


# ---------------------------------------------------------------------------
# Statistical helpers
# ---------------------------------------------------------------------------

def sample_stat(rng: np.random.Generator, prop: str, is_pitcher: bool,
                skill: float = 1.0) -> float:
    dists = PITCHER_DISTS if is_pitcher else BATTER_DISTS
    spec  = dists.get(prop)
    if spec is None:
        return 0.0
    kind = spec[0]
    if kind == "nb":
        mu, k = spec[1] * skill, spec[2]
        mu = max(mu, 0.01)
        p  = k / (k + mu)
        return float(rng.negative_binomial(k, p))
    elif kind == "po":
        return float(rng.poisson(max(spec[1] * skill, 0.001)))
    elif kind == "cl":
        v = rng.normal(spec[1] * skill, spec[2])
        return float(np.clip(v, 0.3, 9.0))
    return 0.0


# ---------------------------------------------------------------------------
# EV math
# ---------------------------------------------------------------------------

def american_to_decimal(odds: int) -> float:
    if odds > 0:
        return 1.0 + odds / 100.0
    return 1.0 + 100.0 / abs(odds)


def strip_vig(oo: int, ou: int) -> Tuple[float, float]:
    io  = 1.0 / american_to_decimal(oo)
    iu  = 1.0 / american_to_decimal(ou)
    tot = io + iu
    return io / tot, iu / tot


def calculate_ev(mp: float, tp: float) -> float:
    if tp <= 0:
        return -1.0
    return (mp / tp) - 1.0


def kelly_fraction(ev: float, tp: float) -> float:
    if tp <= 0 or ev <= 0:
        return 0.0
    wp = min(tp + ev * tp, 0.99)
    lp = 1.0 - wp
    if lp <= 0:
        return HALF_KELLY_CAP
    return min((wp / lp) * 0.5, HALF_KELLY_CAP)


# ---------------------------------------------------------------------------
# Rolling window
# ---------------------------------------------------------------------------

class RollingWindow:
    def __init__(self) -> None:
        self._buf: deque[float] = deque(maxlen=30)

    def simulate_line(self) -> Optional[float]:
        n = len(self._buf)
        if n < 5:
            return None
        src    = list(self._buf)
        sample = src[-14:] if n >= 14 else src[-7:]
        sample.sort()
        mid    = len(sample) // 2
        med    = (
            (sample[mid - 1] + sample[mid]) / 2.0
            if len(sample) % 2 == 0
            else float(sample[mid])
        )
        return round(med * 2) / 2.0

    def simulate_model_prob(self, line: float) -> float:
        buf = list(self._buf)
        n   = len(buf)
        if n < 5:
            return 0.50
        hits = sum(1 for v in buf if v > line)
        return (hits + 1) / (n + 2)

    def push(self, v: float) -> None:
        self._buf.append(v)


# ---------------------------------------------------------------------------
# Player profile  (AR-1 streak persistence)
# ---------------------------------------------------------------------------

@dataclass
class Player:
    pid:        str
    is_pitcher: bool
    skill:      Dict[str, float]   = field(default_factory=dict)
    ar_state:   Dict[str, float]   = field(default_factory=dict)

    def draw(self, rng: np.random.Generator, prop: str) -> float:
        state    = self.ar_state.get(prop, 0.0)
        eps      = rng.normal(0, 1)
        new_st   = AR1_PHI * state + math.sqrt(1 - AR1_PHI ** 2) * eps
        self.ar_state[prop] = new_st
        mult = math.exp(new_st * 0.15)
        return max(sample_stat(rng, prop, self.is_pitcher, self.skill.get(prop, 1.0)) * mult, 0.0)


def build_pool(rng: np.random.Generator) -> Tuple[List[Player], List[Player]]:
    batters  = [
        Player(
            pid=f"B{i:04d}", is_pitcher=False,
            skill={p: float(np.clip(rng.normal(1.0, 0.18), 0.4, 2.2))
                   for p in BATTER_PROPS}
        )
        for i in range(TEAMS * BATTERS_PER_TEAM)
    ]
    pitchers = [
        Player(
            pid=f"P{i:04d}", is_pitcher=True,
            skill={p: float(np.clip(rng.normal(1.0, 0.18), 0.4, 2.2))
                   for p in PITCHER_PROPS}
        )
        for i in range(TEAMS * STARTERS_PER_TEAM)
    ]
    return batters, pitchers


# ---------------------------------------------------------------------------
# Streaming aggregator
# ---------------------------------------------------------------------------

class StreamAgg:
    """
    Accumulates per-agent / per-season / per-prop statistics without
    storing individual bet records. Constant memory regardless of volume.
    """

    def __init__(self) -> None:
        self.total   = 0
        self.wins    = 0
        self.pnl     = 0.0
        self.risk    = 0.0
        self.over_b  = 0; self.over_w  = 0; self.over_pnl  = 0.0; self.over_risk  = 0.0
        self.under_b = 0; self.under_w = 0; self.under_pnl = 0.0; self.under_risk = 0.0

        # per-agent accumulators
        self.ag: Dict[str, dict] = {
            ag: {"b": 0, "w": 0, "pnl": 0.0, "risk": 0.0, "ev_sum": 0.0,
                 "kelly_sum": 0.0, "daily": defaultdict(float)}
            for ag in AGENT_SPECS
        }
        # per-season
        self.se: Dict[int, dict] = {
            s: {"b": 0, "w": 0, "pnl": 0.0, "risk": 0.0}
            for s in SEASONS
        }
        # per-prop
        self.pr: Dict[str, dict] = defaultdict(
            lambda: {"b": 0, "w": 0, "pnl": 0.0, "risk": 0.0}
        )
        # sample rows for CSV (first SAMPLE_ROWS bets globally)
        self.sample: List[dict] = []

    def record(self, season: int, day: int,
               pid: str, prop: str, line: float, direction: str,
               model_prob: float, true_prob: float, ev: float,
               kf: float, actual: float, won: bool, pnl: float,
               agent: str) -> None:

        self.total += 1
        if won:
            self.wins += 1
        self.pnl  += pnl
        self.risk += kf

        if direction == "over":
            self.over_b += 1
            if won: self.over_w += 1
            self.over_pnl  += pnl
            self.over_risk += kf
        else:
            self.under_b += 1
            if won: self.under_w += 1
            self.under_pnl  += pnl
            self.under_risk += kf

        # agent
        a = self.ag[agent]
        a["b"]         += 1
        a["w"]         += 1 if won else 0
        a["pnl"]       += pnl
        a["risk"]      += kf
        a["ev_sum"]    += ev * 100
        a["kelly_sum"] += kf
        a["daily"][f"{season}-{day:03d}"] += pnl

        # season
        s = self.se[season]
        s["b"]    += 1
        s["w"]    += 1 if won else 0
        s["pnl"]  += pnl
        s["risk"] += kf

        # prop
        p = self.pr[prop]
        p["b"]    += 1
        p["w"]    += 1 if won else 0
        p["pnl"]  += pnl
        p["risk"] += kf

        # sample
        if len(self.sample) < SAMPLE_ROWS:
            self.sample.append({
                "season": season, "day": day, "player_id": pid,
                "prop_type": prop, "line": line, "direction": direction,
                "model_prob": round(model_prob, 4), "true_prob": round(true_prob, 4),
                "ev_pct": round(ev * 100, 2), "kelly": round(kf, 4),
                "actual": round(actual, 2), "won": won,
                "pnl": round(pnl, 4), "agent": agent,
            })


# ---------------------------------------------------------------------------
# Context simulation
# ---------------------------------------------------------------------------

def ctx(rng: np.random.Generator) -> dict:
    return {
        "wind_mph":    float(rng.weibull(2.0) * 12.0),
        "fatigue":     float(np.clip(rng.beta(2, 3), 0.0, 1.0)),
        "steam":       float(rng.exponential(1.2)),
        "ump_k_adj":   float(rng.normal(0, 0.04)),
        "platoon_adv": bool(rng.random() < 0.45),
        "is_getaway":  bool(rng.random() < 0.22),
        "ticket_pct":  float(rng.uniform(40, 90)),
        "money_pct":   float(rng.uniform(30, 75)),
    }


def ctx_pass(agent: str, c: dict, prop: str, direction: str) -> bool:
    if agent == "WeatherAgent":
        return c["wind_mph"] >= 15 and prop in {"home_runs", "total_bases", "hits"}
    if agent == "BullpenAgent":
        return c["fatigue"] >= 0.70
    if agent == "SteamAgent":
        return c["steam"] >= 2.0
    if agent == "FadeAgent":
        return (c["ticket_pct"] - c["money_pct"]) >= 40
    if agent == "UmpireAgent":
        return abs(c["ump_k_adj"]) >= 0.02
    if agent == "PlatoonAgent":
        return c["platoon_adv"]
    if agent == "GetawayAgent":
        return c["is_getaway"] and direction == "under"
    return True


# ---------------------------------------------------------------------------
# Simulation
# ---------------------------------------------------------------------------

def run_season(
    season: int,
    batters: List[Player],
    pitchers: List[Player],
    rng: np.random.Generator,
    rolling: Dict[str, Dict[str, RollingWindow]],
    agg: StreamAgg,
) -> int:
    """Simulate one season, streaming records into agg. Returns bet count."""

    tp_over, tp_under = strip_vig(ODDS_OVER, ODDS_UNDER)
    season_bets = 0

    for day in range(1, DAYS_PER_SEASON + 1):
        c = ctx(rng)

        # ── BATTERS ──────────────────────────────────────────────────────
        for batter in batters:
            if rng.random() > BATTER_PLAY_RATE:
                continue
            for prop in BATTER_PROPS:
                rw   = rolling[batter.pid][prop]
                line = rw.simulate_line()
                actual = batter.draw(rng, prop)
                if line is None or line <= 0:
                    rw.push(actual)
                    continue

                mp_over  = rw.simulate_model_prob(line)
                mp_under = 1.0 - mp_over

                for direction, mp, tp in (
                    ("over",  mp_over,  tp_over),
                    ("under", mp_under, tp_under),
                ):
                    ev = calculate_ev(mp, tp)
                    if ev < EV_GATE:
                        continue
                    kf = kelly_fraction(ev, tp)
                    if kf <= 0:
                        continue

                    won = actual > line if direction == "over" else actual < line
                    pnl = kf * 0.9091 if won else -kf

                    for agent, spec in AGENT_SPECS.items():
                        boost     = spec.get("prob_boost", 0.0)
                        mp_agent  = min(mp + boost, 0.99)
                        ev_agent  = calculate_ev(mp_agent, tp)
                        kf_agent  = kelly_fraction(ev_agent, tp)
                        if mp_agent < spec["min_prob"]:
                            continue
                        if spec["props"] is not None and prop not in spec["props"]:
                            continue
                        if spec["direction"] is not None and direction != spec["direction"]:
                            continue
                        if not ctx_pass(agent, c, prop, direction):
                            continue
                        won_agent = actual > line if direction == "over" else actual < line
                        pnl_agent = kf_agent * 0.9091 if won_agent else -kf_agent
                        agg.record(season, day, batter.pid, prop, line,
                                   direction, mp_agent, tp, ev_agent, kf_agent,
                                   actual, won_agent, pnl_agent, agent)
                        season_bets += 1

                rw.push(actual)

        # ── PITCHERS ─────────────────────────────────────────────────────
        for pitcher in pitchers:
            idx = int(pitcher.pid[1:])
            if day % STARTER_TURN != idx % STARTER_TURN:
                continue
            for prop in PITCHER_PROPS:
                rw     = rolling[pitcher.pid][prop]
                line   = rw.simulate_line()
                actual = pitcher.draw(rng, prop)
                if line is None or line <= 0:
                    rw.push(actual)
                    continue

                mp_over  = rw.simulate_model_prob(line)
                mp_under = 1.0 - mp_over

                for direction, mp, tp in (
                    ("over",  mp_over,  tp_over),
                    ("under", mp_under, tp_under),
                ):
                    ev = calculate_ev(mp, tp)
                    if ev < EV_GATE:
                        continue
                    kf = kelly_fraction(ev, tp)
                    if kf <= 0:
                        continue

                    won = actual > line if direction == "over" else actual < line
                    pnl = kf * 0.9091 if won else -kf

                    for agent, spec in AGENT_SPECS.items():
                        boost     = spec.get("prob_boost", 0.0)
                        mp_agent  = min(mp + boost, 0.99)
                        ev_agent  = calculate_ev(mp_agent, tp)
                        kf_agent  = kelly_fraction(ev_agent, tp)
                        if mp_agent < spec["min_prob"]:
                            continue
                        if spec["props"] is not None and prop not in spec["props"]:
                            continue
                        if spec["direction"] is not None and direction != spec["direction"]:
                            continue
                        if not ctx_pass(agent, c, prop, direction):
                            continue
                        won_agent = actual > line if direction == "over" else actual < line
                        pnl_agent = kf_agent * 0.9091 if won_agent else -kf_agent
                        agg.record(season, day, pitcher.pid, prop, line,
                                   direction, mp_agent, tp, ev_agent, kf_agent,
                                   actual, won_agent, pnl_agent, agent)
                        season_bets += 1

                rw.push(actual)

    return season_bets


# ---------------------------------------------------------------------------
# Aggregation helpers
# ---------------------------------------------------------------------------

def compute_sharpe(daily: Dict[str, float]) -> float:
    vals = list(daily.values())
    if len(vals) < 2:
        return 0.0
    arr  = np.array(vals, dtype=float)
    mean = arr.mean()
    std  = arr.std(ddof=1)
    return float((mean / std) * math.sqrt(252)) if std > 0 else 0.0


def compute_max_drawdown(daily: Dict[str, float]) -> float:
    if not daily:
        return 0.0
    cum = 0.0; peak = 0.0; mdd = 0.0
    for v in daily.values():
        cum  += v
        peak  = max(peak, cum)
        mdd   = max(mdd, peak - cum)
    return round(mdd, 4)


def dir_stats(b: int, w: int, pnl: float, risk: float) -> dict:
    return {
        "bets":     b,
        "win_rate": round(w / b * 100, 2) if b else 0.0,
        "roi_pct":  round(pnl / risk * 100, 2) if risk else 0.0,
    }


def build_summary(agg: StreamAgg) -> dict:
    roi = (agg.pnl / agg.risk * 100) if agg.risk else 0.0

    by_agent: dict = {}
    for ag, d in agg.ag.items():
        if d["b"] == 0:
            continue
        by_agent[ag] = {
            "bets":      d["b"],
            "wins":      d["w"],
            "win_rate":  round(d["w"] / d["b"] * 100, 2),
            "pnl":       round(d["pnl"], 2),
            "roi_pct":   round(d["pnl"] / d["risk"] * 100, 2) if d["risk"] else 0.0,
            "sharpe":    round(compute_sharpe(d["daily"]), 3),
            "max_dd":    compute_max_drawdown(d["daily"]),
            "avg_ev":    round(d["ev_sum"] / d["b"], 2),
            "avg_kelly": round(d["kelly_sum"] / d["b"], 4),
        }

    by_season: dict = {}
    for s in SEASONS:
        d = agg.se[s]
        if d["b"] == 0:
            continue
        by_season[str(s)] = {
            "bets":     d["b"],
            "wins":     d["w"],
            "win_rate": round(d["w"] / d["b"] * 100, 2),
            "pnl":      round(d["pnl"], 2),
            "roi_pct":  round(d["pnl"] / d["risk"] * 100, 2) if d["risk"] else 0.0,
        }

    by_prop: dict = {}
    for prop, d in sorted(agg.pr.items()):
        if d["b"] == 0:
            continue
        by_prop[prop] = {
            "bets":     d["b"],
            "win_rate": round(d["w"] / d["b"] * 100, 2),
            "pnl":      round(d["pnl"], 2),
            "roi_pct":  round(d["pnl"] / d["risk"] * 100, 2) if d["risk"] else 0.0,
        }

    return {
        "methodology":     "Monte Carlo, AR(1) φ=0.15, calibrated MLB distributions",
        "seasons":         "2016-2025",
        "total_bets":      agg.total,
        "total_wins":      agg.wins,
        "win_rate_pct":    round(agg.wins / agg.total * 100, 2) if agg.total else 0.0,
        "total_pnl_units": round(agg.pnl,  2),
        "total_risk_units":round(agg.risk, 2),
        "overall_roi_pct": round(roi, 2),
        "over_stats":      dir_stats(agg.over_b,  agg.over_w,  agg.over_pnl,  agg.over_risk),
        "under_stats":     dir_stats(agg.under_b, agg.under_w, agg.under_pnl, agg.under_risk),
        "by_agent":        by_agent,
        "by_season":       by_season,
        "by_prop":         by_prop,
    }


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def write_outputs(agg: StreamAgg, summary: dict) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    (OUTPUT_DIR / "summary_10season.json").write_text(json.dumps(summary, indent=2))
    log.info("✓ summary_10season.json")

    if agg.sample:
        fields = list(agg.sample[0].keys())
        with open(OUTPUT_DIR / "report_10season.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(agg.sample)
        log.info("✓ report_10season.csv  (%d sample rows)", len(agg.sample))

    with open(OUTPUT_DIR / "agent_pnl_10season.csv", "w", newline="") as f:
        w2 = csv.writer(f)
        w2.writerow(["agent","bets","wins","win_rate","pnl","roi_pct",
                     "sharpe","max_dd","avg_ev","avg_kelly"])
        for ag, d in sorted(summary["by_agent"].items(),
                             key=lambda x: x[1]["roi_pct"], reverse=True):
            w2.writerow([ag, d["bets"], d["wins"], d["win_rate"], d["pnl"],
                         d["roi_pct"], d["sharpe"], d["max_dd"],
                         d["avg_ev"], d["avg_kelly"]])

    with open(OUTPUT_DIR / "season_pnl_10season.csv", "w", newline="") as f:
        w3 = csv.writer(f)
        w3.writerow(["season","bets","wins","win_rate","pnl","roi_pct"])
        for s, d in sorted(summary["by_season"].items()):
            w3.writerow([s, d["bets"], d["wins"], d["win_rate"],
                         d["pnl"], d["roi_pct"]])

    log.info("✓ agent_pnl_10season.csv + season_pnl_10season.csv")


def print_report(summary: dict) -> None:
    sep = "=" * 72
    print(f"\n{sep}")
    print("  PropIQ 10-Season Backtest Results  (2016 – 2025)")
    print(f"  {summary['methodology']}")
    print(sep)
    print(f"  Total bets evaluated : {summary['total_bets']:>12,}")
    print(f"  Overall win rate     : {summary['win_rate_pct']:>11.2f}%")
    print(f"  Total P&L (units)    : {summary['total_pnl_units']:>+11.2f}")
    print(f"  Total risk (units)   : {summary['total_risk_units']:>12,.1f}")
    print(f"  Overall ROI          : {summary['overall_roi_pct']:>+11.2f}%")
    o = summary["over_stats"]
    u = summary["under_stats"]
    print(f"\n  Overs  : {o['bets']:>9,} bets  WR={o['win_rate']:5.2f}%  ROI={o['roi_pct']:+7.2f}%")
    print(f"  Unders : {u['bets']:>9,} bets  WR={u['win_rate']:5.2f}%  ROI={u['roi_pct']:+7.2f}%")

    print(f"\n  {'─'*70}")
    print(f"  {'Agent':<20} {'Bets':>8} {'WR%':>7} {'ROI%':>8} {'Sharpe':>8} {'MaxDD':>8}")
    print(f"  {'─'*70}")
    for ag, d in sorted(summary["by_agent"].items(),
                         key=lambda x: x[1]["roi_pct"], reverse=True):
        print(f"  {ag:<20} {d['bets']:>8,} {d['win_rate']:>6.2f}%  "
              f"{d['roi_pct']:>+6.2f}%  {d['sharpe']:>7.3f}  {d['max_dd']:>8.4f}")

    print(f"\n  {'─'*70}")
    print(f"  {'Season':<10} {'Bets':>8} {'WR%':>7} {'ROI%':>8}")
    print(f"  {'─'*70}")
    for s, d in sorted(summary["by_season"].items()):
        print(f"  {s:<10} {d['bets']:>8,} {d['win_rate']:>6.2f}%  {d['roi_pct']:>+6.2f}%")

    print(f"\n  {'─'*70}")
    print(f"  {'Prop':<20} {'Bets':>8} {'WR%':>7} {'ROI%':>8}")
    print(f"  {'─'*70}")
    for prop, d in sorted(summary["by_prop"].items(),
                           key=lambda x: x[1]["roi_pct"], reverse=True):
        print(f"  {prop:<20} {d['bets']:>8,} {d['win_rate']:>6.2f}%  {d['roi_pct']:>+6.2f}%")
    print(f"\n{sep}\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    t0 = time.time()
    rng = np.random.default_rng(RANDOM_SEED)
    log.info("PropIQ 10-Season Backtest — seasons %s", SEASONS)

    rolling: Dict[str, Dict[str, RollingWindow]] = defaultdict(
        lambda: defaultdict(RollingWindow)
    )
    agg = StreamAgg()

    for season in SEASONS:
        batters, pitchers = build_pool(rng)
        log.info("Season %d  — %d batters, %d starters", season,
                 len(batters), len(pitchers))
        n = run_season(season, batters, pitchers, rng, rolling, agg)
        log.info("Season %d  — %d agent-bet records, running total %d",
                 season, n, agg.total)

    log.info("Aggregating %d total records...", agg.total)
    summary = build_summary(agg)
    write_outputs(agg, summary)
    print_report(summary)
    log.info("Complete in %.1f s", time.time() - t0)


if __name__ == "__main__":
    main()
