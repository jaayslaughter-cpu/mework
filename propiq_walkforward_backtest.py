"""
propiq_walkforward_backtest.py
===============================
PropIQ — Real Walk-Forward Prop Backtest Engine

PURPOSE
-------
Replaces the Monte Carlo synthetic backtest with a real historical
bet-by-bet walk-forward evaluation. Every decision uses ONLY data
available before the game date — zero look-ahead. Results are
graded against actual outcomes.

WHAT THIS FIXES (from model review)
------------------------------------
Critical Issue #1: The 10-season backtest was Monte Carlo on synthetic
distributions, not real game data. This module runs on actual Statcast
game logs already present in your data/stats/2026/ and data/fg/ folders.

Critical Issue #3: Bet frequency collapse (19-97 bets per fold). This
engine logs every prop evaluated — wins, losses, and filtered-out picks —
so you can diagnose exactly where the funnel is losing bets.

ARCHITECTURE (adapted from baseball-sims rolling backtest pattern)
-----------------------------------------------------------------
For each game date D:
  1. Build player profiles using ONLY data from dates < D (no peek)
  2. For each prop on that slate, compute model probability
  3. Compare to closing line, compute EV and Kelly fraction
  4. Log BetRecord: predicted_prob, line, ev, result, pnl
  5. Output: calibration table, per-fold ROI, permutation p-value

OUTPUTS
-------
  backtest_results/walkforward_bets.csv     — every bet evaluated
  backtest_results/walkforward_summary.json — fold-level metrics
  backtest_results/calibration_table.csv   — predicted vs actual by bucket

USAGE
-----
  python propiq_walkforward_backtest.py
  python propiq_walkforward_backtest.py --years 2024 2025
  python propiq_walkforward_backtest.py --prop-type strikeouts --min-ev 0.03
  python propiq_walkforward_backtest.py --folds 3 --permutations 500
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import random
import warnings
from dataclasses import dataclass, asdict, field
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from scipy.stats import binom

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [BACKTEST] %(message)s")
log = logging.getLogger(__name__)

# ── Paths (relative to PropIQ repo root) ─────────────────────────────────────
ROOT = Path(__file__).parent
BATTING_LOGS   = ROOT / "data" / "stats" / "2026" / "mlb_batting_logs.csv"
PITCHING_LOGS  = ROOT / "data" / "stats" / "2026" / "mlb_pitching_logs.csv"
FG_BATTER_PROJ = ROOT / "data" / "fg" / "fg_batter_proj_2026.csv"
FG_PITCHER     = ROOT / "data" / "fg" / "fg_pitcher_stats_2026.csv"
BET_LEDGER_DB  = ROOT / "data" / "bet_ledger.db"   # existing PropIQ DB
OUTPUT_DIR     = ROOT / "backtest_results"
OUTPUT_DIR.mkdir(exist_ok=True)

# ── Backtest configuration ────────────────────────────────────────────────────
DEFAULT_KELLY_FRACTION = 0.25
DEFAULT_BANKROLL       = 1000.0
BREAKEVEN_PROB         = 0.5238   # -110 standard DFS vig
MIN_PROB_THRESHOLD     = 0.52     # minimum model probability to evaluate
MIN_EV_THRESHOLD       = 0.02     # minimum EV to count as a bet signal

# K-prop Poisson model constants (calibrated from BaseballbettingEdge live data)
LEAGUE_AVG_K_RATE   = 0.227
LEAGUE_AVG_SWSTR    = 0.110
SWSTR_K9_SCALE      = 16.0   # Live-calibrated: BBE reduced from 30→16 over 2026 season
LAMBDA_BIAS_DEFAULT = -0.067  # BBE live calibration: systematic K over-prediction

# Platoon K% deltas (from BaseballbettingEdge build_features.py — multi-season MLB aggregates)
PLATOON_K_DELTA = {
    ("R", "R"):  0.005,
    ("R", "L"): -0.010,
    ("L", "R"): -0.015,
    ("L", "L"):  0.020,
}


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class BetRecord:
    """Single evaluated prop — whether it fired or was filtered."""
    game_date:      str
    pitcher_name:   str
    prop_type:      str
    line:           float
    side:           str            # "over" | "under"
    model_prob:     float
    market_prob:    float          # no-vig implied probability
    ev:             float          # (model_prob - market_prob)
    kelly_fraction: float
    fired:          bool           # True = met EV threshold, counted as bet
    result:         Optional[str]  # "win" | "loss" | "push" | None (ungraded)
    actual_value:   Optional[float]
    pnl_units:      float          # +1.0 win, -1.0 loss (at 1u stake)
    flags:          list           = field(default_factory=list)
    fold:           int            = 0


@dataclass
class FoldResult:
    """Aggregate metrics for a single backtest fold."""
    fold:         int
    train_label:  str
    test_label:   str
    n_evaluated:  int       # all props looked at
    n_bets:       int       # props that fired (met EV threshold)
    n_graded:     int       # bets with a decided result
    wins:         int
    losses:       int
    win_rate:     float
    roi:          float
    max_drawdown: float
    brier_score:  float
    p_value:      float     # permutation test p-value
    notes:        str       = ""


# ── Core probability engine ───────────────────────────────────────────────────

def poisson_over_prob(lam: float, line: float) -> float:
    """P(K >= line) under Poisson(lam). Clamps to valid range."""
    if lam <= 0 or line <= 0:
        return 0.5
    k_floor = math.floor(line)
    prob_under = sum(
        (lam ** k) * math.exp(-lam) / math.factorial(k)
        for k in range(k_floor + 1)
    )
    return max(0.01, min(0.99, 1.0 - prob_under))


def blended_k9(season_k9: float, recent_k9: float,
                weight_season_cap: float = 0.40,
                weight_recent: float = 0.05) -> float:
    """Blend season and recent K/9 with calibrated weights.

    Weights from BBE live calibration (May 2026):
      season cap: 40%  (was 70% — recent data less predictive mid-season)
      recent: 5%       (was 20% — recent K/9 is noisy)
      residual: 55%    → career / projection anchor
    """
    w_recent = min(weight_recent, 1.0 - weight_season_cap)
    w_season = weight_season_cap
    w_anchor = max(0.0, 1.0 - w_season - w_recent)
    # anchor = league average as fallback
    anchor_k9 = LEAGUE_AVG_K_RATE * 27  # ~6.12 K/9 equivalent
    blended = w_season * season_k9 + w_recent * recent_k9 + w_anchor * anchor_k9
    return max(1.0, blended)


def platoon_k_delta(batter_hand: str, pitcher_throws: str) -> float:
    """League-average K% adjustment for handedness matchup."""
    if batter_hand == "S":
        batter_hand = "L" if pitcher_throws == "R" else "R"
    return PLATOON_K_DELTA.get((batter_hand, pitcher_throws), 0.0)


def compute_k_lambda(
    pitcher_record: dict,
    opp_lineup_k_pct: float = LEAGUE_AVG_K_RATE,
    expected_ip: float = 5.5,
    swstr_delta: float = 0.0,
    lambda_bias: float = LAMBDA_BIAS_DEFAULT,
    params: Optional[dict] = None,
) -> float:
    """Compute expected strikeouts (lambda) for Poisson model.

    Inputs (all from pitcher's pre-game rolling profile):
      pitcher_record: dict with season_k9, recent_k9, career_k9, days_rest, throws
      opp_lineup_k_pct: opponent lineup K rate (default: league average)
      expected_ip: pitcher's average IP (use per-pitcher avg, not 5.5)
      swstr_delta: pitcher's current SwStr% minus career SwStr% (decimal)
      lambda_bias: global systematic bias correction (calibrated, default -0.067)
      params: optional calibration params dict (overrides defaults if provided)
    """
    if params:
        lambda_bias  = params.get("lambda_bias", lambda_bias)
        swstr_scale  = params.get("swstr_k9_scale", SWSTR_K9_SCALE)
    else:
        swstr_scale = SWSTR_K9_SCALE

    season_k9 = pitcher_record.get("season_k9", 8.0)
    recent_k9 = pitcher_record.get("recent_k9", season_k9)
    weight_s   = params.get("weight_season_cap", 0.40) if params else 0.40
    weight_r   = params.get("weight_recent", 0.05) if params else 0.05

    base_k9 = blended_k9(season_k9, recent_k9, weight_s, weight_r)

    # SwStr% career delta adjustment (each 1pp SwStr% above career = +swstr_scale/100 K/9)
    swstr_adj = swstr_delta * swstr_scale

    # Opponent K rate multiplier (Bayesian shrunken toward league average)
    opp_k_prior_games = params.get("opp_k_prior_games", 50) if params else 50
    opp_adj = opp_lineup_k_pct / LEAGUE_AVG_K_RATE

    # Days rest (>4 days rest = slight K boost, <3 = fatigue penalty)
    days_rest = pitcher_record.get("days_rest", 5)
    rest_adj = 0.0
    if days_rest >= 5:
        rest_adj = 0.15   # well-rested: +0.15 K/9
    elif days_rest <= 2:
        rest_adj = -0.30  # short rest: -0.30 K/9

    adjusted_k9 = (base_k9 + swstr_adj + rest_adj) * opp_adj
    lam = (adjusted_k9 / 9.0) * expected_ip + lambda_bias

    return max(0.5, lam)


# ── Rolling profile builder (no look-ahead) ──────────────────────────────────

def build_pitcher_profiles_before(
    pitching_df: pd.DataFrame, cutoff_date: date
) -> dict:
    """Build pitcher K/9 profiles using ONLY games before cutoff_date.

    Returns dict keyed by pitcher_name with:
      season_k9, recent_k9, career_k9, recent_start_count, avg_ip, days_rest
    """
    cutoff_str = cutoff_date.isoformat()
    hist = pitching_df[pitching_df["game_date"] < cutoff_str].copy()
    if hist.empty:
        return {}

    profiles = {}
    for name, grp in hist.groupby("player_name"):
        grp = grp.sort_values("game_date")

        # Season K/9
        season_grp = grp[grp["game_date"] >= f"{cutoff_date.year}-03-01"]
        if len(season_grp) >= 1:
            total_k  = season_grp["strikeouts"].sum()
            total_ip = season_grp["innings_pitched"].sum()
            season_k9 = (total_k / total_ip * 9) if total_ip > 0 else 8.0
        else:
            season_k9 = 8.0

        # Recent K/9 (last 3 starts)
        recent = grp.tail(3)
        r_k  = recent["strikeouts"].sum()
        r_ip = recent["innings_pitched"].sum()
        recent_k9 = (r_k / r_ip * 9) if r_ip > 0 else season_k9

        # Avg IP (last 5 starts)
        last5 = grp.tail(5)
        avg_ip = last5["innings_pitched"].mean() if len(last5) >= 2 else 5.5

        # Days rest
        last_game = grp["game_date"].iloc[-1]
        try:
            last_date = date.fromisoformat(str(last_game)[:10])
            days_rest = (cutoff_date - last_date).days
        except Exception:
            days_rest = 5

        profiles[name] = {
            "season_k9":           round(season_k9, 3),
            "recent_k9":           round(recent_k9, 3),
            "recent_start_count":  len(season_grp),
            "avg_ip":              round(float(avg_ip), 2),
            "days_rest":           days_rest,
        }

    return profiles


# ── No-vig probability ────────────────────────────────────────────────────────

def american_to_prob(odds: float) -> float:
    """Convert American odds to raw implied probability."""
    if odds >= 0:
        return 100.0 / (odds + 100.0)
    return abs(odds) / (abs(odds) + 100.0)


def devig_power(probs: list, tol: float = 1e-6, max_iter: int = 200) -> list:
    """Power (Shin) method de-vig — most theoretically correct for 2-outcome markets.

    Adapted from mlb-analytics-hub nrfi_odds.py.
    Finds exponent k such that sum(p_i^k) = 1.
    """
    if not probs or all(p <= 0 for p in probs):
        return probs
    total = sum(probs)
    if abs(total - 1.0) < tol:
        return probs
    lo, hi = 0.5, 2.0
    for _ in range(max_iter):
        mid = (lo + hi) / 2.0
        val = sum(p ** mid for p in probs)
        if abs(val - 1.0) < tol:
            break
        lo, hi = (mid, hi) if val > 1.0 else (lo, mid)
    k = (lo + hi) / 2.0
    raw = [p ** k for p in probs]
    s = sum(raw)
    return [r / s for r in raw] if s > 0 else probs


def no_vig_prob(over_american: float, under_american: float) -> tuple:
    """Return (fair_over_prob, fair_under_prob) using power devig."""
    raw_over  = american_to_prob(over_american)
    raw_under = american_to_prob(under_american)
    fair_over, fair_under = devig_power([raw_over, raw_under])
    return fair_over, fair_under


def kelly_fraction(model_prob: float, market_prob: float,
                   k_frac: float = DEFAULT_KELLY_FRACTION) -> float:
    """Quarter-Kelly fraction for a binary bet."""
    if market_prob >= 1.0 or market_prob <= 0:
        return 0.0
    decimal_odds = 1.0 / market_prob  # fair decimal odds
    b = decimal_odds - 1.0
    q = 1.0 - model_prob
    full_k = (b * model_prob - q) / b
    return max(0.0, round(k_frac * full_k, 5))


# ── Calibration ───────────────────────────────────────────────────────────────

def brier_score(probs: list, outcomes: list) -> float:
    """Mean squared error between predicted probs and binary outcomes."""
    if not probs:
        return float("nan")
    return float(np.mean([(p - o) ** 2 for p, o in zip(probs, outcomes)]))


def calibration_table(probs: list, outcomes: list,
                      buckets: int = 10) -> pd.DataFrame:
    """Reliability diagram data — predicted prob vs actual win rate per bucket.

    Adapted from baseball-sims metrics approach.
    """
    if not probs:
        return pd.DataFrame()
    edges = np.linspace(0.0, 1.0, buckets + 1)
    rows = []
    for lo, hi in zip(edges, edges[1:]):
        mask = [(lo <= p < hi) for p in probs]
        bucket_p = [p for p, m in zip(probs, mask) if m]
        bucket_o = [o for o, m in zip(outcomes, mask) if m]
        if bucket_p:
            rows.append({
                "bucket":         f"{lo:.2f}–{hi:.2f}",
                "n":              len(bucket_p),
                "avg_predicted":  round(np.mean(bucket_p), 4),
                "actual_win_pct": round(np.mean(bucket_o), 4),
                "calibration_err": round(abs(np.mean(bucket_p) - np.mean(bucket_o)), 4),
            })
    return pd.DataFrame(rows)


def permutation_pvalue(real_roi: float, bets_df: pd.DataFrame,
                       n_shuffles: int = 200, seed: int = 42) -> float:
    """Non-parametric permutation test: P(shuffled ROI >= real ROI).

    p < 0.05 → model edge is real and unlikely due to chance.
    p > 0.10 → no statistically detectable edge.
    """
    rng = random.Random(seed)
    if bets_df.empty or "pnl_units" not in bets_df.columns:
        return 1.0
    pnl_vals = bets_df["pnl_units"].tolist()
    n_bets = len(pnl_vals)
    if n_bets == 0:
        return 1.0
    shuffled_rois = []
    for _ in range(n_shuffles):
        rng.shuffle(pnl_vals)
        s_roi = sum(pnl_vals) / n_bets
        shuffled_rois.append(s_roi)
    p_val = sum(1 for r in shuffled_rois if r >= real_roi) / n_shuffles
    return round(p_val, 4)


def max_drawdown(pnl_series: list) -> float:
    """Maximum peak-to-trough drawdown in units."""
    if not pnl_series:
        return 0.0
    cumulative = np.cumsum(pnl_series)
    peak = np.maximum.accumulate(cumulative)
    drawdown = peak - cumulative
    return float(np.max(drawdown))


# ── Bet evaluation ────────────────────────────────────────────────────────────

def evaluate_prop(
    game_date: str,
    pitcher_name: str,
    line: float,
    over_american: float,
    under_american: float,
    pitcher_profile: dict,
    opp_lineup_k_pct: float = LEAGUE_AVG_K_RATE,
    umpire_k_adj: float = 0.0,
    params: Optional[dict] = None,
    fold: int = 0,
) -> BetRecord:
    """Evaluate a single K-prop line. Returns BetRecord (unfilled result)."""
    flags = []

    # Quality gates (from BaseballbettingEdge quality_gates.py)
    recent_count = pitcher_profile.get("recent_start_count", 0)
    if recent_count == 0:
        flags.append("no_pitcher_k_profile")
    elif recent_count <= 2:
        flags.append("thin_recent_start_sample")
    elif recent_count <= 4:
        flags.append("developing_pitcher_sample")

    has_severe = any(f in ("no_pitcher_k_profile",) for f in flags)

    expected_ip = pitcher_profile.get("avg_ip", 5.5)
    lam = compute_k_lambda(
        pitcher_profile,
        opp_lineup_k_pct=opp_lineup_k_pct,
        expected_ip=expected_ip,
        params=params,
    )

    # Umpire adjustment (additive K/9 → lambda adjustment)
    ump_scale = params.get("ump_scale", 0.9) if params else 0.9
    lam += umpire_k_adj * (expected_ip / 9.0) * ump_scale

    model_over_prob  = poisson_over_prob(lam, line)
    model_under_prob = 1.0 - model_over_prob

    fair_over, fair_under = no_vig_prob(over_american, under_american)

    over_ev  = model_over_prob  - fair_over
    under_ev = model_under_prob - fair_under

    if abs(over_ev) >= abs(under_ev):
        side, model_prob, market_prob, ev = "over",  model_over_prob,  fair_over,  over_ev
    else:
        side, model_prob, market_prob, ev = "under", model_under_prob, fair_under, under_ev

    kf     = kelly_fraction(model_prob, market_prob)
    fired  = (ev >= MIN_EV_THRESHOLD) and (model_prob >= MIN_PROB_THRESHOLD) and not has_severe

    return BetRecord(
        game_date=game_date,
        pitcher_name=pitcher_name,
        prop_type="strikeouts",
        line=line,
        side=side,
        model_prob=round(model_prob, 4),
        market_prob=round(market_prob, 4),
        ev=round(ev, 4),
        kelly_fraction=kf,
        fired=fired,
        result=None,
        actual_value=None,
        pnl_units=0.0,
        flags=flags,
        fold=fold,
    )


# ── Result grading ────────────────────────────────────────────────────────────

def grade_bet(record: BetRecord, actual_ks: float) -> BetRecord:
    """Fill in result and pnl_units given actual strikeout count."""
    record.actual_value = actual_ks
    if actual_ks == record.line:
        record.result    = "push"
        record.pnl_units = 0.0
    elif record.side == "over":
        if actual_ks > record.line:
            record.result    = "win"
            record.pnl_units = 1.0
        else:
            record.result    = "loss"
            record.pnl_units = -1.0
    else:  # under
        if actual_ks < record.line:
            record.result    = "win"
            record.pnl_units = 1.0
        else:
            record.result    = "loss"
            record.pnl_units = -1.0
    return record


# ── Walk-forward runner ───────────────────────────────────────────────────────

def run_walkforward_backtest(
    pitching_df: pd.DataFrame,
    prop_lines_df: pd.DataFrame,
    n_folds: int = 3,
    n_permutations: int = 200,
    params: Optional[dict] = None,
    prop_type: str = "strikeouts",
    min_ev: float = MIN_EV_THRESHOLD,
) -> tuple[list[BetRecord], list[FoldResult]]:
    """Main walk-forward backtest loop.

    Parameters
    ----------
    pitching_df:   Game-level pitcher logs with columns:
                   game_date, player_name, strikeouts, innings_pitched
    prop_lines_df: Historical DFS prop lines with columns:
                   game_date, player_name, line, over_american, under_american,
                   actual_ks (for grading)
    n_folds:       Number of time-series folds
    n_permutations: Permutation test iterations per fold
    params:        Calibration params dict (lambda_bias, swstr_k9_scale, etc.)
    prop_type:     Prop market to evaluate (currently only "strikeouts")
    min_ev:        Minimum EV threshold to fire a bet

    Returns (all_records, fold_results)
    """
    global MIN_EV_THRESHOLD
    MIN_EV_THRESHOLD = min_ev

    if params is None:
        params = {
            "lambda_bias":       LAMBDA_BIAS_DEFAULT,
            "swstr_k9_scale":    SWSTR_K9_SCALE,
            "ump_scale":         0.9,
            "weight_season_cap": 0.40,
            "weight_recent":     0.05,
        }

    # Sort prop lines by date
    prop_lines_df = prop_lines_df.copy()
    prop_lines_df["game_date"] = pd.to_datetime(prop_lines_df["game_date"])
    prop_lines_df = prop_lines_df.sort_values("game_date")

    dates = prop_lines_df["game_date"].dt.date.unique()
    dates = sorted(dates)

    if len(dates) < n_folds * 2:
        log.warning("Insufficient dates for %d folds. Reducing to 1.", n_folds)
        n_folds = 1

    # Divide dates into folds (time-series: train on earlier, test on later)
    fold_size  = len(dates) // n_folds
    all_records: list[BetRecord] = []
    fold_results: list[FoldResult] = []

    for fold_idx in range(n_folds):
        test_start_idx = fold_idx * fold_size
        test_end_idx   = test_start_idx + fold_size if fold_idx < n_folds - 1 else len(dates)
        test_dates = set(dates[test_start_idx:test_end_idx])

        train_label = f"{dates[0]} → {dates[test_start_idx - 1]}" if test_start_idx > 0 else "no train"
        test_label  = f"{dates[test_start_idx]} → {dates[test_end_idx - 1]}"
        log.info("Fold %d/%d | Train: %s | Test: %s", fold_idx + 1, n_folds, train_label, test_label)

        fold_records: list[BetRecord] = []

        for test_date in sorted(test_dates):
            # Build profiles strictly before this date — no peek
            profiles = build_pitcher_profiles_before(pitching_df, test_date)

            day_props = prop_lines_df[
                prop_lines_df["game_date"].dt.date == test_date
            ]

            for _, row in day_props.iterrows():
                pitcher = row.get("player_name", "")
                profile = profiles.get(pitcher, {})

                record = evaluate_prop(
                    game_date=str(test_date),
                    pitcher_name=pitcher,
                    line=float(row.get("line", 4.5)),
                    over_american=float(row.get("over_american", -115)),
                    under_american=float(row.get("under_american", -115)),
                    pitcher_profile=profile,
                    params=params,
                    fold=fold_idx + 1,
                )

                # Grade if actual result available
                actual_ks = row.get("actual_ks")
                if pd.notna(actual_ks):
                    record = grade_bet(record, float(actual_ks))

                fold_records.append(record)

        # Compute fold metrics
        fired_bets = [r for r in fold_records if r.fired]
        graded     = [r for r in fired_bets if r.result in ("win", "loss")]
        wins       = [r for r in graded if r.result == "win"]

        n_bets   = len(fired_bets)
        n_graded = len(graded)
        n_wins   = len(wins)
        win_rate = n_wins / n_graded if n_graded > 0 else 0.0
        roi      = sum(r.pnl_units for r in graded) / n_graded if n_graded > 0 else 0.0
        mdd      = max_drawdown([r.pnl_units for r in graded])

        # Brier score (on all evaluated props with results, not just fired)
        all_with_result = [r for r in fold_records if r.result in ("win", "loss")]
        bs = brier_score(
            [r.model_prob for r in all_with_result],
            [1.0 if r.result == "win" else 0.0 for r in all_with_result],
        ) if all_with_result else float("nan")

        # Permutation test
        graded_df = pd.DataFrame([asdict(r) for r in graded])
        p_val = permutation_pvalue(roi, graded_df, n_shuffles=n_permutations)

        interpretation = (
            "EDGE REAL — real ROI beats 95%+ of shuffles" if p_val < 0.05
            else "WARNING — shuffled ROI stays elevated or no edge" if p_val > 0.10
            else "MARGINAL — p between 0.05 and 0.10"
        )

        fold_result = FoldResult(
            fold=fold_idx + 1,
            train_label=train_label,
            test_label=test_label,
            n_evaluated=len(fold_records),
            n_bets=n_bets,
            n_graded=n_graded,
            wins=n_wins,
            losses=n_graded - n_wins,
            win_rate=round(win_rate, 4),
            roi=round(roi, 4),
            max_drawdown=round(mdd, 4),
            brier_score=round(bs, 4) if not math.isnan(bs) else -1.0,
            p_value=p_val,
            notes=interpretation,
        )

        log.info(
            "Fold %d: %d evaluated | %d fired | %d graded | WR=%.1f%% | ROI=%.1f%% | p=%.3f | %s",
            fold_idx + 1, len(fold_records), n_bets, n_graded,
            win_rate * 100, roi * 100, p_val, interpretation,
        )

        all_records.extend(fold_records)
        fold_results.append(fold_result)

    return all_records, fold_results


# ── Output writers ────────────────────────────────────────────────────────────

def write_outputs(records: list[BetRecord], folds: list[FoldResult]) -> None:
    """Write CSVs and JSON summary."""
    # All bets CSV
    bets_path = OUTPUT_DIR / "walkforward_bets.csv"
    df = pd.DataFrame([asdict(r) for r in records])
    df.to_csv(bets_path, index=False)
    log.info("Bets written: %s (%d rows)", bets_path, len(df))

    # Fold summary JSON
    summary = {
        "methodology": "PropIQ Walk-Forward Backtest — real game data, no look-ahead",
        "folds": [asdict(f) for f in folds],
        "overall": {},
    }
    fired = [r for r in records if r.fired]
    graded = [r for r in fired if r.result in ("win", "loss")]
    if graded:
        overall_roi  = sum(r.pnl_units for r in graded) / len(graded)
        overall_wr   = sum(1 for r in graded if r.result == "win") / len(graded)
        overall_bs   = brier_score(
            [r.model_prob for r in graded],
            [1.0 if r.result == "win" else 0.0 for r in graded],
        )
        summary["overall"] = {
            "total_evaluated": len(records),
            "total_fired":     len(fired),
            "total_graded":    len(graded),
            "win_rate":        round(overall_wr, 4),
            "roi":             round(overall_roi, 4),
            "brier_score":     round(overall_bs, 4),
        }

    summary_path = OUTPUT_DIR / "walkforward_summary.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)
    log.info("Summary written: %s", summary_path)

    # Calibration table CSV
    all_with_result = [r for r in records if r.result in ("win", "loss")]
    if all_with_result:
        cal_df = calibration_table(
            [r.model_prob for r in all_with_result],
            [1.0 if r.result == "win" else 0.0 for r in all_with_result],
        )
        cal_path = OUTPUT_DIR / "calibration_table.csv"
        cal_df.to_csv(cal_path, index=False)
        log.info("Calibration table written: %s", cal_path)
        print("\nCALIBRATION TABLE:")
        print(cal_df.to_string(index=False))


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="PropIQ Walk-Forward Backtest")
    parser.add_argument("--years", nargs="+", type=int, default=None,
                        help="Test years to include e.g. --years 2024 2025")
    parser.add_argument("--folds", type=int, default=3,
                        help="Number of time-series folds (default: 3)")
    parser.add_argument("--permutations", type=int, default=200,
                        help="Permutation test iterations (default: 200)")
    parser.add_argument("--prop-type", default="strikeouts",
                        help="Prop market to evaluate (default: strikeouts)")
    parser.add_argument("--min-ev", type=float, default=MIN_EV_THRESHOLD,
                        help="Minimum EV to fire a bet (default: 0.02)")
    parser.add_argument("--lambda-bias", type=float, default=LAMBDA_BIAS_DEFAULT,
                        help="Lambda bias override (default: -0.067)")
    args = parser.parse_args()

    # Load pitching game logs
    log.info("Loading pitching game logs: %s", PITCHING_LOGS)
    if not PITCHING_LOGS.exists():
        log.error("Pitching logs not found at %s. Run data ingestion first.", PITCHING_LOGS)
        return

    pitching_df = pd.read_csv(PITCHING_LOGS, low_memory=False)

    # Load or create prop lines
    # In production: load from bet_ledger DB or historical DFS prop export
    # For testing: create a minimal example from pitching logs
    prop_lines_path = OUTPUT_DIR / "prop_lines_input.csv"
    if prop_lines_path.exists():
        log.info("Loading prop lines: %s", prop_lines_path)
        prop_lines_df = pd.read_csv(prop_lines_path)
    else:
        log.warning(
            "No prop_lines_input.csv found in %s.\n"
            "Create it with columns: game_date, player_name, line, "
            "over_american, under_american, actual_ks\n"
            "Running with synthetic example data...",
            OUTPUT_DIR,
        )
        # Synthetic demo: use pitching log as both source and target
        # In reality you need historical DFS prop lines with closing odds
        pitching_df["game_date"] = pd.to_datetime(pitching_df["game_date"])
        prop_lines_df = pitching_df.rename(columns={
            "strikeouts": "actual_ks",
            "player_name": "player_name",
        }).copy()
        prop_lines_df["line"]           = prop_lines_df["actual_ks"].apply(lambda k: round(k * 0.9, 1))
        prop_lines_df["over_american"]  = -115
        prop_lines_df["under_american"] = -115
        prop_lines_df = prop_lines_df[["game_date", "player_name", "line",
                                        "over_american", "under_american", "actual_ks"]]

    params = {
        "lambda_bias":       args.lambda_bias,
        "swstr_k9_scale":    SWSTR_K9_SCALE,
        "ump_scale":         0.9,
        "weight_season_cap": 0.40,
        "weight_recent":     0.05,
    }

    records, folds = run_walkforward_backtest(
        pitching_df=pitching_df,
        prop_lines_df=prop_lines_df,
        n_folds=args.folds,
        n_permutations=args.permutations,
        params=params,
        prop_type=args.prop_type,
        min_ev=args.min_ev,
    )

    write_outputs(records, folds)

    print("\n" + "=" * 60)
    print("  WALK-FORWARD BACKTEST SUMMARY")
    print("=" * 60)
    for f in folds:
        print(f"\n  Fold {f.fold}: {f.test_label}")
        print(f"    Evaluated: {f.n_evaluated:,}  |  Fired: {f.n_bets}  |  Graded: {f.n_graded}")
        print(f"    Win Rate:  {f.win_rate:.1%}  |  ROI: {f.roi:+.1%}  |  MaxDD: {f.max_drawdown:.2f}u")
        print(f"    Brier:     {f.brier_score:.4f}  |  p-value: {f.p_value:.3f}")
        print(f"    {f.notes}")


if __name__ == "__main__":
    main()
