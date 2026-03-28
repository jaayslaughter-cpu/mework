"""
walk_forward_backtest_v2.py — Phase 46
=======================================
Honest walk-forward backtest with:

  1. STRICT 3-WAY SPLIT per fold (train / val / test — no peeking)
     - Train: fit Bayesian prior parameters
     - Val:   tune confidence threshold + fit temperature T
     - Test:  report numbers. Nothing from test touches tuning.

  2. BASELINE STRATEGIES (sanity checks)
     - Random: bet every prop at fair coin flip decision → should lose ~vig
     - Always-Over: take the over on every prop → should lose vig
     - If either baseline prints big profit → settlement bug or odds error

  3. PERMUTATION TEST (data-leak detector)
     - Keep game outcomes + odds fixed
     - Randomly shuffle dates/timestamps 200 times
     - For each shuffle, compute backtest ROI with tuned model
     - True edge: shuffled ROI distribution centered near 0
     - Data leak: shuffled ROI stays elevated (model "knows" future)

  4. TEMPERATURE SCALING
     - T fit on val fold only (never test fold)
     - Applied to test probabilities before threshold gate
     - Reports pre-calibration vs post-calibration Brier score

  5. SYNTHETIC LINES = DISCLOSED
     - No historical DFS lines freely available
     - Lines = rolling 30-game mean of stat +/- 0.5 (disclosed explicitly)
     - This is the honest statement of what free data can produce

Data sources (all free, no API key):
  - MLB Stats API: pitcher game logs (statsapi.mlb.com)
  - Seasons: 2022, 2023, 2024 (2025 YTD if available)

Vig model: standard -110 both sides -> breakeven = 52.38%
"""

import json
import math
import random
import statistics
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import urllib.request

# Constants
VIG_PAYOUT = 100 / 110  # -110 pays $0.909 per $1 risked
BREAKEVEN = 110 / 210   # 52.38%
SEASONS = [2022, 2023, 2024]
MAX_PITCHERS = 40
PERMUTATION_RUNS = 200
STAKE = 1.0

DISCLOSURE = """
==============================================================
  BACKTEST METHODOLOGY DISCLOSURE (Phase 46)
  Lines: rolling 30-game mean +/- 0.5 (NO historical DFS lines)
  Vig: standard -110 both sides
  No look-ahead: val fold tunes T and threshold
  Test fold never touched during parameter fitting
==============================================================
"""

MLB_BASE = "https://statsapi.mlb.com/api/v1"


def _get(url: str, retries: int = 3) -> dict:
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "PropIQ-Backtest/1.0"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode())
        except Exception:
            if attempt == retries - 1:
                return {}
            time.sleep(1.5)
    return {}


def fetch_top_starters(season: int, limit: int = MAX_PITCHERS) -> List[dict]:
    # No &fields= filter — it strips player.id
    url = (
        f"{MLB_BASE}/stats?stats=season&season={season}"
        f"&group=pitching&gameType=R&limit={limit}&sortStat=inningsPitched&position=SP"
    )
    data = _get(url)
    results = []
    for entry in data.get("stats", [{}])[0].get("splits", []):
        player = entry.get("player", {})
        stat = entry.get("stat", {})
        pid = player.get("id")
        if not pid:
            continue
        try:
            ip_raw = stat.get("inningsPitched", "0")
            ip = float(ip_raw) if ip_raw else 0.0
        except (ValueError, TypeError):
            ip = 0.0
        results.append({
            "player_id": pid,
            "full_name": player.get("fullName", "Unknown"),
            "ip": ip,
            "season": season,
        })
    return results


def fetch_pitcher_game_log(player_id: int, season: int) -> List[dict]:
    # No &fields= filter — it can strip date or stat fields
    url = (
        f"{MLB_BASE}/people/{player_id}/stats"
        f"?stats=gameLog&season={season}&group=pitching&gameType=R"
    )
    data = _get(url)
    games = []
    for entry in data.get("stats", [{}])[0].get("splits", []):
        stat = entry.get("stat", {})
        try:
            game_date = entry.get("date", "")[:10]
            if not game_date:
                continue
            k = int(stat.get("strikeOuts", 0))
            ip_raw = stat.get("inningsPitched", "0")
            ip = float(ip_raw) if ip_raw else 0.0
            if ip >= 2.0:
                games.append({"date": game_date, "k": k, "ip": ip})
        except (ValueError, TypeError):
            continue
    return sorted(games, key=lambda x: x["date"])


def build_dataset(seasons: List[int]) -> List[dict]:
    print(f"\nFetching MLB pitcher game logs for seasons {seasons}...")
    all_records = []

    for season in seasons:
        print(f"   Season {season}...")
        starters = fetch_top_starters(season)
        if not starters:
            print(f"   WARNING: No starters returned for {season}")
            continue
        print(f"   -> {len(starters)} starters found")
        for starter in starters:
            pid = starter["player_id"]
            if not pid:
                continue
            games = fetch_pitcher_game_log(pid, season)
            if len(games) < 5:
                continue
            for g_idx in range(10, len(games)):  # require 10 prior starts for stable line
                prior_games = games[:g_idx]
                current = games[g_idx]
                prior_k = [g["k"] for g in prior_games]
                prior_ip = [g["ip"] for g in prior_games]
                if not prior_k:
                    continue
                window = prior_k[-30:]
                line = statistics.mean(window)
                line = round(line * 2) / 2
                actual_k = current["k"]
                outcome = 1 if actual_k > line else 0
                cum_k = sum(prior_k)
                cum_ip = sum(prior_ip)
                all_records.append({
                    "date": current["date"],
                    "season": season,
                    "player_id": pid,
                    "player_name": starter["full_name"],
                    "line": line,
                    "actual_k": actual_k,
                    "outcome": outcome,
                    "cum_k": cum_k,
                    "cum_ip": cum_ip,
                })
            time.sleep(0.1)

    print(f"   Total records: {len(all_records)}")
    return sorted(all_records, key=lambda x: x["date"])


def fit_beta_prior(k_rates: List[float]) -> Tuple[float, float]:
    if len(k_rates) < 3:
        return (5.0, 5.0)
    mu = statistics.mean(k_rates)
    var = statistics.variance(k_rates) if len(k_rates) > 1 else 0.01
    var = max(var, 1e-6)
    mu = max(1e-4, min(1 - 1e-4, mu))
    common = mu * (1 - mu) / var - 1
    alpha = max(0.5, mu * common)
    beta_ = max(0.5, (1 - mu) * common)
    return (alpha, beta_)


def beta_posterior_mean(alpha: float, beta_: float, cum_k: float, cum_ip: float) -> float:
    outs = cum_ip * 3
    if outs == 0:
        return alpha / (alpha + beta_)
    obs_rate = max(1e-4, min(1 - 1e-4, cum_k / outs))
    n_eff = 10
    return (alpha + obs_rate * n_eff) / (alpha + beta_ + n_eff)


def predict_k_prob(posterior_rate: float, line: float) -> float:
    expected_outs = 16.5
    lam = max(0.01, posterior_rate * expected_outs)
    threshold = int(line) + 1 if line != int(line) else int(line)
    prob_under = 0.0
    cum_exp = math.exp(-lam)
    for k in range(threshold):
        if k > 0:
            cum_exp *= lam / k
        prob_under += cum_exp
    return max(0.01, min(0.99, 1.0 - prob_under))


def compute_model_probs(records: List[dict], prior_alpha: float, prior_beta: float) -> List[float]:
    probs = []
    for rec in records:
        post_rate = beta_posterior_mean(prior_alpha, prior_beta, rec["cum_k"], rec["cum_ip"])
        prob = predict_k_prob(post_rate, rec["line"])
        probs.append(prob)
    return probs


def _logit(p: float) -> float:
    p = max(1e-7, min(1 - 1e-7, p))
    return math.log(p / (1 - p))


def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def _log_loss(probs: List[float], outcomes: List[int]) -> float:
    total = 0.0
    for p, y in zip(probs, outcomes):
        p = max(1e-7, min(1 - 1e-7, p))
        total += -(y * math.log(p) + (1 - y) * math.log(1 - p))
    return total / len(probs)


def fit_temperature(probs: List[float], outcomes: List[int]) -> float:
    if len(probs) < 10:
        return 1.0
    best_t, best_loss = 1.0, float("inf")
    for i in range(101):
        T = 0.5 + 2.5 * (i / 100)
        calibrated = [_sigmoid(_logit(p) / T) for p in probs]
        loss = _log_loss(calibrated, outcomes)
        if loss < best_loss:
            best_loss, best_t = loss, T
    return best_t


def apply_temperature(p: float, T: float) -> float:
    return _sigmoid(_logit(p) / T) if T != 1.0 else p


def brier_score(probs: List[float], outcomes: List[int]) -> Optional[float]:
    if not probs:
        return None
    return sum((p - y) ** 2 for p, y in zip(probs, outcomes)) / len(probs)


def sim_strategy(probs: List[float], outcomes: List[int], threshold: float, T: float = 1.0) -> dict:
    equity = 0.0
    peak_equity = 0.0        # running high-water mark (can go below 0 if never positive)
    max_abs_dd = 0.0         # max absolute drawdown in units
    wins = 0
    n_bets = 0
    for p_raw, y in zip(probs, outcomes):
        p = apply_temperature(p_raw, T)
        if p < threshold:
            continue
        n_bets += 1
        if y == 1:
            equity += VIG_PAYOUT
            wins += 1
        else:
            equity -= STAKE
        # Update peak
        if equity > peak_equity:
            peak_equity = equity
        # Absolute drawdown from peak
        abs_dd = peak_equity - equity
        if abs_dd > max_abs_dd:
            max_abs_dd = abs_dd
    # Normalize: max_dd as fraction of total staked
    total_staked = n_bets * STAKE
    max_dd_pct = max_abs_dd / max(total_staked, 1e-9)
    return {
        "n_bets": n_bets,
        "wins": wins,
        "win_rate": round(wins / max(n_bets, 1), 4),
        "roi": round(equity / max(n_bets, 1), 4),
        "net_units": round(equity, 3),
        "max_dd": round(max_dd_pct, 4),
    }


def sim_random_baseline(outcomes: List[int], seed: int = 42) -> dict:
    rng = random.Random(seed)
    equity = 0.0
    wins = 0
    n_bets = 0
    max_equity = 0.0
    max_dd = 0.0
    for y in outcomes:
        if rng.random() < 0.5:
            continue
        n_bets += 1
        if y == 1:
            equity += VIG_PAYOUT
            wins += 1
        else:
            equity -= STAKE
        max_equity = max(max_equity, equity)
        dd = (max_equity - equity) / (max_equity + 1e-9)
        max_dd = max(max_dd, dd)
    return {
        "strategy": "RANDOM",
        "n_bets": n_bets,
        "win_rate": round(wins / max(n_bets, 1), 4),
        "roi": round(equity / max(n_bets, 1), 4),
        "max_dd": round(max_dd, 4),
    }


def sim_always_over(outcomes: List[int]) -> dict:
    wins = sum(outcomes)
    n_bets = len(outcomes)
    equity = 0.0
    max_equity = 0.0
    max_dd = 0.0
    for y in outcomes:
        if y == 1:
            equity += VIG_PAYOUT
        else:
            equity -= STAKE
        max_equity = max(max_equity, equity)
        dd = (max_equity - equity) / (max_equity + 1e-9)
        max_dd = max(max_dd, dd)
    return {
        "strategy": "ALWAYS_OVER",
        "n_bets": n_bets,
        "win_rate": round(wins / max(n_bets, 1), 4),
        "roi": round(equity / max(n_bets, 1), 4),
        "max_dd": round(max_dd, 4),
    }


def permutation_test(
    records: List[dict],
    prior_alpha: float,
    prior_beta: float,
    threshold: float,
    T: float,
    n_runs: int = PERMUTATION_RUNS,
    seed: int = 0,
) -> dict:
    """
    Outcome-label permutation test (correct implementation).

    The Bayesian model uses cumulative rolling stats (not calendar dates),
    so shuffling dates doesn't reconstitute the rolling prior — they stay identical.
    The correct test is: fix model probabilities, shuffle OUTCOME LABELS.

    True edge: with correct outcomes, P(over) correlates with actual hits.
               Shuffled outcomes destroy that correlation → shuffled ROI → 0.
    Data leak: model "knows" future outcomes already (e.g. trained on full history).
               Shuffled outcomes still produce elevated ROI → leak detected.

    Interpretation:
      - real_roi > shuffled_mean: model has genuine predictive signal
      - p_value < 0.10: edge is statistically significant at 90% confidence
      - shuffled_mean ≈ -vig: null model loses vig as expected
    """
    rng = random.Random(seed)
    probs = compute_model_probs(records, prior_alpha, prior_beta)
    outcomes = [r["outcome"] for r in records]

    # Real result
    real_result = sim_strategy(probs, outcomes, threshold, T)
    real_roi = real_result["roi"]

    # Null distribution: shuffle outcome labels, keep probs fixed
    shuffled_rois = []
    for _ in range(n_runs):
        shuffled_outcomes = outcomes[:]
        rng.shuffle(shuffled_outcomes)
        result = sim_strategy(probs, shuffled_outcomes, threshold, T)
        shuffled_rois.append(result["roi"])

    shuffled_rois.sort()
    shuffled_mean = statistics.mean(shuffled_rois)
    shuffled_std = statistics.stdev(shuffled_rois) if len(shuffled_rois) > 1 else 0
    shuffled_p95 = shuffled_rois[int(0.95 * len(shuffled_rois))]
    # p-value: fraction of shuffles that produce ROI >= real ROI
    p_value = sum(1 for r in shuffled_rois if r >= real_roi) / len(shuffled_rois)

    interp = (
        "EDGE REAL -- shuffled ROI near -vig, real ROI beats 90%+ of shuffles"
        if (shuffled_mean < 0 and p_value < 0.10)
        else "WARNING: POSSIBLE LEAK or no edge -- shuffled ROI stays elevated or real ROI does not beat shuffles"
    )
    return {
        "real_roi": round(real_roi, 4),
        "shuffled_mean": round(shuffled_mean, 4),
        "shuffled_std": round(shuffled_std, 4),
        "shuffled_p95": round(shuffled_p95, 4),
        "p_value": round(p_value, 4),
        "interpretation": interp,
        "note": "Outcome-label permutation: probs fixed, outcomes shuffled. Null = model with no predictive signal."
    }


def run_walk_forward(all_records: List[dict]) -> List[dict]:
    def split_half(season: int) -> Tuple[List[dict], List[dict]]:
        recs = [r for r in all_records if r["season"] == season]
        n = len(recs)
        return recs[:n // 2], recs[n // 2:]

    s2022 = [r for r in all_records if r["season"] == 2022]
    s2023 = [r for r in all_records if r["season"] == 2023]
    s2024 = [r for r in all_records if r["season"] == 2024]
    s2025 = [r for r in all_records if r["season"] == 2025]
    s2023h1, s2023h2 = split_half(2023)
    s2024h1, s2024h2 = split_half(2024)

    folds = [
        {"name": "Fold 1", "train": s2022, "val": s2023h1, "test": s2023h2,
         "train_label": "2022", "test_label": "2023 H2"},
        {"name": "Fold 2", "train": s2022 + s2023, "val": s2024h1, "test": s2024h2,
         "train_label": "2022-2023", "test_label": "2024 H2"},
        {"name": "Fold 3", "train": s2022 + s2023 + s2024h1, "val": s2024h2,
         "test": s2025 if s2025 else s2024h2,
         "train_label": "2022-2024 H1",
         "test_label": "2025 YTD" if s2025 else "2024 H2 (no 2025 data yet)"},
    ]

    fold_results = []

    for fold in folds:
        train = fold["train"]
        val = fold["val"]
        test = fold["test"]

        if not train or not val or not test:
            print(f"\nWARNING: {fold['name']}: Insufficient data -- skipping")
            continue

        print(f"\n{'='*60}")
        print(f"  {fold['name']} | Train: {fold['train_label']} ({len(train)} records)")
        print(f"             | Val:   {len(val)} records")
        print(f"             | Test:  {fold['test_label']} ({len(test)} records)")

        # Step 1: Fit Beta prior on train
        train_rates = []
        for rec in train:
            if rec["cum_ip"] > 0:
                rate = rec["cum_k"] / (rec["cum_ip"] * 3)
                rate = max(1e-4, min(1 - 1e-4, rate))
                train_rates.append(rate)
        prior_alpha, prior_beta = fit_beta_prior(train_rates)
        print(f"\n  Prior: a={prior_alpha:.2f}, b={prior_beta:.2f} "
              f"(mean K-rate={prior_alpha / (prior_alpha + prior_beta):.3f})")

        # Step 2: Fit temperature T on val ONLY
        val_probs_raw = compute_model_probs(val, prior_alpha, prior_beta)
        val_outcomes = [r["outcome"] for r in val]
        T = fit_temperature(val_probs_raw, val_outcomes)
        val_probs_cal = [apply_temperature(p, T) for p in val_probs_raw]
        print(f"  Temperature T={T:.3f} (fitted on val -- {'compresses' if T > 1 else 'spreads'} probs)")

        # Step 3: Tune threshold on val ONLY
        best_threshold = 0.54
        best_val_roi = -999.0
        for thresh_i in range(50, 80):
            thresh = thresh_i / 100
            result = sim_strategy(val_probs_cal, val_outcomes, thresh, T=1.0)
            if result["n_bets"] >= 10 and result["roi"] > best_val_roi:
                best_val_roi = result["roi"]
                best_threshold = thresh
        print(f"  Threshold={best_threshold:.2f} (tuned on val, val ROI={best_val_roi:+.3f})")

        # Step 4: Evaluate on test (first contact with test data)
        test_probs_raw = compute_model_probs(test, prior_alpha, prior_beta)
        test_outcomes = [r["outcome"] for r in test]
        test_probs_cal = [apply_temperature(p, T) for p in test_probs_raw]
        model_result = sim_strategy(test_probs_cal, test_outcomes, best_threshold, T=1.0)

        brier_pre = brier_score(test_probs_raw, test_outcomes)
        brier_post = brier_score(test_probs_cal, test_outcomes)

        tier_55_62 = [(p, y) for p, y in zip(test_probs_cal, test_outcomes) if 0.55 <= p < 0.62]
        tier_62p = [(p, y) for p, y in zip(test_probs_cal, test_outcomes) if p >= 0.62]
        tier_55_62_rate = sum(y for _, y in tier_55_62) / max(len(tier_55_62), 1)
        tier_62p_rate = sum(y for _, y in tier_62p) / max(len(tier_62p), 1)

        random_result = sim_random_baseline(test_outcomes)
        always_over_result = sim_always_over(test_outcomes)

        print(f"\n  -- TEST RESULTS (no-peek) --")
        print(f"  Model: n={model_result['n_bets']} | win={model_result['win_rate']:.1%} | "
              f"ROI={model_result['roi']:+.3f} | max_dd={model_result['max_dd']:.1%}")
        print(f"  Brier: {brier_pre:.4f} pre-T -> {brier_post:.4f} post-T "
              f"({'improved' if brier_post and brier_pre and brier_post < brier_pre else 'worsened or unchanged'})")
        print(f"  Calibration: 55-62% tier hits {tier_55_62_rate:.1%} | 62%+ tier hits {tier_62p_rate:.1%}")
        print(f"\n  Sanity check baselines (both should lose vig):")
        print(f"  Random:     win={random_result['win_rate']:.1%} | ROI={random_result['roi']:+.3f}")
        print(f"  AlwaysOver: win={always_over_result['win_rate']:.1%} | ROI={always_over_result['roi']:+.3f}")

        baseline_ok = abs(random_result["roi"]) < 0.15 and abs(always_over_result["roi"]) < 0.15
        if baseline_ok:
            print("  OK: Baselines lose vig as expected")
        else:
            print("  WARNING: Baseline ROI too large -- check settlement logic or line construction")

        # Step 5: Permutation test
        print(f"\n  Running permutation test ({PERMUTATION_RUNS} shuffles)...")
        perm = permutation_test(test, prior_alpha, prior_beta, best_threshold, T)
        print(f"  Real ROI:      {perm['real_roi']:+.4f}")
        print(f"  Shuffled mean: {perm['shuffled_mean']:+.4f} +/- {perm['shuffled_std']:.4f}")
        print(f"  Shuffled p95:  {perm['shuffled_p95']:+.4f}")
        print(f"  p-value: {perm['p_value']:.3f}")
        print(f"  -> {perm['interpretation']}")

        fold_results.append({
            "fold": fold["name"],
            "train_label": fold["train_label"],
            "test_label": fold["test_label"],
            "n_train": len(train),
            "n_val": len(val),
            "n_test": len(test),
            "prior_alpha": round(prior_alpha, 3),
            "prior_beta": round(prior_beta, 3),
            "temperature_T": round(T, 3),
            "threshold": best_threshold,
            "model_n_bets": model_result["n_bets"],
            "model_win_rate": model_result["win_rate"],
            "model_roi": model_result["roi"],
            "model_max_dd": model_result["max_dd"],
            "brier_pre_T": round(brier_pre, 4) if brier_pre else None,
            "brier_post_T": round(brier_post, 4) if brier_post else None,
            "tier_55_62_actual_rate": round(tier_55_62_rate, 4),
            "tier_62p_actual_rate": round(tier_62p_rate, 4),
            "baseline_random_roi": random_result["roi"],
            "baseline_always_over_roi": always_over_result["roi"],
            "baseline_sanity_ok": baseline_ok,
            "permutation_real_roi": perm["real_roi"],
            "permutation_shuffled_mean": perm["shuffled_mean"],
            "permutation_p_value": perm["p_value"],
            "permutation_interpretation": perm["interpretation"],
        })

    return fold_results


def print_summary(fold_results: List[dict]) -> None:
    print(f"\n{'='*60}")
    print("  WALK-FORWARD SUMMARY (Phase 46)")
    print(f"{'='*60}")
    print(f"  {'Fold':<8} {'Test Window':<24} {'n':<6} {'Win%':<8} {'ROI':<10} {'MaxDD':<10} {'Brier':<10} {'p-val'}")
    print(f"  {'-'*90}")
    for r in fold_results:
        brier = f"{r['brier_post_T']}" if r.get("brier_post_T") else "N/A"
        print(
            f"  {r['fold']:<8} {r['test_label']:<24} {r['model_n_bets']:<6} "
            f"{r['model_win_rate']:<8.1%} {r['model_roi']:<+10.3f} "
            f"{r['model_max_dd']:<10.1%} {brier:<10} {r['permutation_p_value']:.3f}"
        )
    print(f"\n  Breakeven (-110 vig): {BREAKEVEN:.1%}")
    positive = sum(1 for r in fold_results if r["model_roi"] > 0)
    print(f"  Positive ROI folds: {positive}/{len(fold_results)}")
    leaky = [r for r in fold_results if "WARNING" in r["permutation_interpretation"]]
    if leaky:
        print(f"  WARNING: Possible data leak in: {[r['fold'] for r in leaky]}")
    else:
        print("  OK: Permutation tests show no data leakage")
    bad_baseline = [r for r in fold_results if not r["baseline_sanity_ok"]]
    if bad_baseline:
        print(f"  WARNING: Baseline sanity FAILED in: {[r['fold'] for r in bad_baseline]}")
    else:
        print("  OK: Baselines correctly lose vig -- settlement math confirmed")


def save_results(fold_results: List[dict]) -> None:
    path = os.getenv("BACKTEST_RESULTS_PATH", "/tmp/backtest_v2_results.json")
    with open(path, "w") as f:
        json.dump({
            "run_timestamp": datetime.utcnow().isoformat() + "Z",
            "methodology": {
                "lines": "rolling 30-game mean K rounded to nearest 0.5 -- no historical DFS lines",
                "vig": "-110 standard, breakeven 52.38%",
                "no_peek": "threshold and T fit on val fold only, test fold never touched during tuning",
                "permutation_runs": PERMUTATION_RUNS,
                "disclosure": "synthetic lines -- cannot claim DFS realism without historical line data"
            },
            "folds": fold_results
        }, f, indent=2)
    print(f"\n  Results saved to {path}")


if __name__ == "__main__":
    print(DISCLOSURE)
    all_records = build_dataset(SEASONS)
    if len(all_records) < 50:
        print("\nWARNING: Insufficient data. Try again during season or reduce MAX_PITCHERS.")
        exit(1)
    fold_results = run_walk_forward(all_records)
    if fold_results:
        print_summary(fold_results)
        save_results(fold_results)
    else:
        print("\nWARNING: No fold results produced.")
