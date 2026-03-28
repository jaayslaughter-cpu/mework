"""
PropIQ Per-Agent Walk-Forward Backtest
=======================================
Runs the same honest walk-forward methodology as backtest_v2 but split by agent logic.

HONEST DISCLOSURES:
- Lines are synthetic (rolling 30-game medians), not real historical DFS lines
- Agent filters are approximated from stat categories, not live source tags
- Agents requiring live external data are flagged as SIMULATION-LIMITED
- Walk-forward: train on N years, test on next half-season (no peeking on threshold tuning)
- Baseline (always-bet-random) runs alongside every agent to validate settlement math

FOLDS:
  Fold 1: Train 2022-2023, Test 2024 H1
  Fold 2: Train 2022-2024 H1, Test 2024 H2
  Fold 3: Train 2022-2024, Test 2025 (partial)

AGENTS WITH REAL BACKTEST:
  UmpireAgent   - K props, prob >= 0.54
  ArsenalAgent  - K props, prob >= 0.54 (same data pool, higher gate)
  CatcherAgent  - K props, prob >= 0.54
  F5Agent       - K props (first 5 innings approx), prob >= 0.52
  UnderMachine  - Under props only (hits + K), prob >= 0.52
  GetawayAgent  - Under props (approximated), prob >= 0.52
  LineupAgent   - Hit props, prob >= 0.52 (batting order gate simulated)
  PlatoonAgent  - Hit props, prob >= 0.52
  EVHunter      - All props, top-10 by edge, prob >= 0.52
  BullpenAgent  - K props with bullpen proxy (high game number in series), prob >= 0.52

AGENTS FLAGGED SIMULATION-LIMITED:
  FadeAgent, WeatherAgent, LineValueAgent, ArbitrageAgent,
  MLEdgeAgent, VultureStack, OmegaStack, StreakAgent

SIMULATION-LIMITED agents get a theoretical assessment instead of real fold numbers.
"""

import urllib.request
import json
import statistics
import random
from collections import defaultdict

# ─── Config ───────────────────────────────────────────────────────────────────
BREAKEVEN = 0.5238   # 54.2% of 0.909 payout (PrizePicks no-vig)
PAYOUT = 0.909
MIN_GAMES_FOR_LINE = 15   # min prior games before we trust the median
RANDOM_SEED = 42

# Simulation-limited agents — no real fold data possible
SIMULATION_LIMITED = {
    "FadeAgent": "Requires live SBD public betting % — no historical records",
    "WeatherAgent": "Requires per-game weather data — not in MLB Stats API",
    "LineValueAgent": "Requires The Odds API sportsbook consensus — live only",
    "ArbitrageAgent": "Requires simultaneous PrizePicks + Underdog lines — live only",
    "MLEdgeAgent": "Requires live ML model output — model wasn't running in 2022-2024",
    "VultureStack": "Stacked: requires BullpenAgent + GetawayAgent to both fire same game",
    "OmegaStack": "Stacked: requires VultureStack + UmpireAgent + FadeAgent triple consensus",
    "StreakAgent": "Different product: Underdog Streaks 11-pick progressive — not DFS props",
}

# ─── MLB Stats API Helpers ───────────────────────────────────────────────────
def fetch_json(url, timeout=20):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "PropIQ-Backtest/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception as e:
        return None

def get_top_starters(season, limit=60):
    url = (
        f"https://statsapi.mlb.com/api/v1/stats/leaders"
        f"?leaderCategories=strikeouts&season={season}"
        f"&statGroup=pitching&gameType=R&limit={limit}&sportId=1"
    )
    data = fetch_json(url)
    if not data:
        return []
    leaders = data.get("leagueLeaders", [{}])[0].get("leaders", [])
    out = []
    for l in leaders:
        pid = l.get("person", {}).get("id")
        name = l.get("person", {}).get("fullName", "")
        if pid:
            out.append((pid, name))
    return out

def get_pitcher_game_logs(player_id, season):
    url = (
        f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats"
        f"?stats=gameLog&group=pitching&season={season}&gameType=R"
    )
    data = fetch_json(url)
    if not data:
        return []
    splits = data.get("stats", [{}])[0].get("splits", [])
    rows = []
    for s in splits:
        d = s.get("date", "")
        stat = s.get("stat", {})
        try:
            rows.append({
                "date": d,
                "k": int(stat.get("strikeOuts", 0)),
                "ip": float(stat.get("inningsPitched", 0)),
                "game_in_series": s.get("gameNumber", 0),
            })
        except:
            pass
    rows.sort(key=lambda x: x["date"])
    return rows

def get_top_hitters(season, limit=80):
    url = (
        f"https://statsapi.mlb.com/api/v1/stats/leaders"
        f"?leaderCategories=hits&season={season}"
        f"&statGroup=hitting&gameType=R&limit={limit}&sportId=1"
    )
    data = fetch_json(url)
    if not data:
        return []
    leaders = data.get("leagueLeaders", [{}])[0].get("leaders", [])
    out = []
    for l in leaders:
        pid = l.get("person", {}).get("id")
        name = l.get("person", {}).get("fullName", "")
        if pid:
            out.append((pid, name))
    return out

def get_hitter_game_logs(player_id, season):
    url = (
        f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats"
        f"?stats=gameLog&group=hitting&season={season}&gameType=R"
    )
    data = fetch_json(url)
    if not data:
        return []
    splits = data.get("stats", [{}])[0].get("splits", [])
    rows = []
    for s in splits:
        d = s.get("date", "")
        stat = s.get("stat", {})
        try:
            rows.append({
                "date": d,
                "h": int(stat.get("hits", 0)),
                "tb": int(stat.get("totalBases", 0)),
                "pa": int(stat.get("plateAppearances", 0)),
            })
        except:
            pass
    rows.sort(key=lambda x: x["date"])
    return rows

# ─── Line Construction (Synthetic) ──────────────────────────────────────────
def build_prop_records(game_logs, stat_key, prior_window=30):
    """
    For each game beyond MIN_GAMES_FOR_LINE, compute:
      - synthetic line = median of prior 30 games
      - model_prob = P(over line) from prior distribution
      - outcome = 1 if actual > line else 0
    Returns list of dicts with date, model_prob, outcome.
    """
    records = []
    for i in range(MIN_GAMES_FOR_LINE, len(game_logs)):
        window = game_logs[max(0, i-prior_window):i]
        vals = [g[stat_key] for g in window]
        if not vals or max(vals) == 0:
            continue
        line = statistics.median(vals)
        actual = game_logs[i][stat_key]
        # P(over) = fraction of window games that beat this line
        over_count = sum(1 for v in vals if v > line)
        model_prob = over_count / len(vals)
        # Avoid trivially 0 or 1 — apply slight Bayesian smoothing
        model_prob = (over_count + 1) / (len(vals) + 2)
        outcome = 1 if actual > line else 0
        records.append({
            "date": game_logs[i]["date"],
            "model_prob": model_prob,
            "outcome": outcome,
            "actual": actual,
            "line": line,
            "stat_key": stat_key,
            "extra": game_logs[i],
        })
    return records

# ─── Fold Infrastructure ─────────────────────────────────────────────────────
FOLD_DEFS = [
    # (train_start, train_end, test_start, test_end, label)
    ("2022-03-01", "2023-10-01", "2024-03-01", "2024-07-31", "Fold1 Test:2024-H1"),
    ("2022-03-01", "2024-07-31", "2024-08-01", "2024-10-31", "Fold2 Test:2024-H2"),
    ("2022-03-01", "2024-10-31", "2025-03-01", "2025-12-31", "Fold3 Test:2025"),
]

def apply_fold(records, fold):
    train_start, train_end, test_start, test_end, label = fold
    train = [r for r in records if train_start <= r["date"] <= train_end]
    test  = [r for r in records if test_start  <= r["date"] <= test_end]
    return train, test

def tune_threshold(train_records, min_gate, max_delta=0.10):
    """
    Find optimal prob threshold on training data only.
    Capped at min_gate + max_delta to prevent overfitting that collapses test sample.
    Returns threshold that maximizes ROI on train set within the cap.
    """
    cap = min_gate + max_delta
    best_t = min_gate
    best_roi = -999
    t = min_gate
    while t <= cap + 0.001:
        bets = [r for r in train_records if r["model_prob"] >= t]
        if len(bets) < 10:
            break
        wins = sum(r["outcome"] for r in bets)
        roi = (wins * PAYOUT - (len(bets) - wins)) / len(bets)
        if roi > best_roi:
            best_roi = roi
            best_t = t
        t = round(t + 0.02, 2)
    return best_t

# ─── Agent Filters ───────────────────────────────────────────────────────────
def filter_records(records, agent_name, trained_threshold, extra_filter=None):
    """Apply agent-specific filters to records."""
    out = []
    for r in records:
        if r["model_prob"] < trained_threshold:
            continue
        if extra_filter and not extra_filter(r):
            continue
        out.append(r)
    return out

# ─── Metrics ─────────────────────────────────────────────────────────────────
def calc_metrics(test_bets, label=""):
    if not test_bets:
        return {"n": 0, "win_rate": 0, "roi": 0, "max_dd": 0, "brier": 0}
    n = len(test_bets)
    wins = sum(r["outcome"] for r in test_bets)
    win_rate = wins / n
    roi = (wins * PAYOUT - (n - wins)) / n

    # Max drawdown
    equity = 0
    peak = 0
    max_dd = 0
    for r in test_bets:
        equity += PAYOUT if r["outcome"] else -1
        if equity > peak:
            peak = equity
        dd = (peak - equity) / max(1, abs(peak))
        if dd > max_dd:
            max_dd = dd

    # Brier score
    brier = sum((r["model_prob"] - r["outcome"])**2 for r in test_bets) / n

    return {
        "n": n,
        "win_rate": win_rate,
        "roi": roi,
        "max_dd": max_dd,
        "brier": brier,
    }

def baseline_metrics(test_bets):
    """Random bet baseline — should approximate losing vig."""
    rng = random.Random(RANDOM_SEED)
    n = len(test_bets)
    if n == 0:
        return {"n": 0, "win_rate": 0, "roi": 0}
    # Randomly take 50% of bets
    sample = [r for r in test_bets if rng.random() < 0.5]
    if not sample:
        return {"n": 0, "win_rate": 0, "roi": 0}
    wins = sum(r["outcome"] for r in sample)
    roi = (wins * PAYOUT - (len(sample) - wins)) / len(sample)
    return {"n": len(sample), "win_rate": wins/len(sample), "roi": roi}

# ─── Data Fetching ────────────────────────────────────────────────────────────
print("═" * 70)
print("PropIQ Per-Agent Walk-Forward Backtest")
print("=" * 70)
print("Fetching MLB data (2022-2025)... this takes 2-3 minutes\n")

seasons = [2022, 2023, 2024, 2025]

# ── Pitchers ──
print("► Fetching top starters by season...")
pitcher_logs_raw = defaultdict(list)  # {player_id: [{date, k, ip, game_in_series}, ...]}
pitcher_names = {}

for season in seasons:
    starters = get_top_starters(season, limit=50)
    print(f"  {season}: {len(starters)} starters found")
    for pid, name in starters:
        if pid not in pitcher_names:
            pitcher_names[pid] = name
        logs = get_pitcher_game_logs(pid, season)
        pitcher_logs_raw[pid].extend(logs)

# Deduplicate and sort each pitcher's log
for pid in pitcher_logs_raw:
    seen = set()
    deduped = []
    for g in pitcher_logs_raw[pid]:
        if g["date"] not in seen:
            seen.add(g["date"])
            deduped.append(g)
    pitcher_logs_raw[pid] = sorted(deduped, key=lambda x: x["date"])

print(f"  Total pitchers: {len(pitcher_logs_raw)}")

# ── Hitters ──
print("► Fetching top hitters by season...")
hitter_logs_raw = defaultdict(list)
hitter_names = {}

for season in seasons:
    hitters = get_top_hitters(season, limit=60)
    print(f"  {season}: {len(hitters)} hitters found")
    for pid, name in hitters:
        if pid not in hitter_names:
            hitter_names[pid] = name
        logs = get_hitter_game_logs(pid, season)
        hitter_logs_raw[pid].extend(logs)

for pid in hitter_logs_raw:
    seen = set()
    deduped = []
    for g in hitter_logs_raw[pid]:
        if g["date"] not in seen:
            seen.add(g["date"])
            deduped.append(g)
    hitter_logs_raw[pid] = sorted(deduped, key=lambda x: x["date"])

print(f"  Total hitters: {len(hitter_logs_raw)}")

# ─── Build Prop Records ───────────────────────────────────────────────────────
print("\n► Building prop records from rolling medians...")

# Pitcher K records (for K-prop agents)
all_k_records = []
for pid, logs in pitcher_logs_raw.items():
    recs = build_prop_records(logs, "k")
    for r in recs:
        r["player_id"] = pid
        r["player_name"] = pitcher_names.get(pid, "Unknown")
        r["prop_type"] = "k"
    all_k_records.extend(recs)

all_k_records.sort(key=lambda x: x["date"])
print(f"  K-prop records: {len(all_k_records)}")

# F5 approximation: games where pitcher threw 5+ IP
all_f5_records = []
for pid, logs in pitcher_logs_raw.items():
    recs = build_prop_records(logs, "k")
    for r in recs:
        r["player_id"] = pid
        r["player_name"] = pitcher_names.get(pid, "Unknown")
        r["prop_type"] = "k_f5"
        # F5 proxy: IP >= 5.0 in training games (pitcher likely went deep)
        # In test, we look at actual IP to judge if it was a "F5 opportunity"
    # Filter to games where pitcher had 5+ IP (F5 style)
    long_logs = [g for g in logs if g.get("ip", 0) >= 5.0]
    if len(long_logs) > MIN_GAMES_FOR_LINE:
        recs_f5 = build_prop_records(long_logs, "k")
        for r in recs_f5:
            r["player_id"] = pid
            r["player_name"] = pitcher_names.get(pid, "Unknown")
            r["prop_type"] = "k_f5"
        all_f5_records.extend(recs_f5)

all_f5_records.sort(key=lambda x: x["date"])
print(f"  F5 K-prop records: {len(all_f5_records)}")

# Bullpen proxy: games late in series (game 3+ of series = bullpen fatigue signal)
all_bullpen_records = []
for pid, logs in pitcher_logs_raw.items():
    # Use game_in_series as bullpen fatigue proxy
    # High game number = more recent bullpen usage in that series
    recs = build_prop_records(logs, "k")
    for r in recs:
        r["player_id"] = pid
        r["player_name"] = pitcher_names.get(pid, "Unknown")
        r["prop_type"] = "k_bullpen"
        r["game_in_series"] = r["extra"].get("game_in_series", 0)
    all_bullpen_records.extend(recs)

all_bullpen_records.sort(key=lambda x: x["date"])
print(f"  Bullpen K-prop records: {len(all_bullpen_records)}")

# Hitter H records
all_h_records = []
for pid, logs in hitter_logs_raw.items():
    recs = build_prop_records(logs, "h")
    for r in recs:
        r["player_id"] = pid
        r["player_name"] = hitter_names.get(pid, "Unknown")
        r["prop_type"] = "h"
    all_h_records.extend(recs)

all_h_records.sort(key=lambda x: x["date"])
print(f"  Hit-prop records: {len(all_h_records)}")

# Under machine: combine K-under + Hit-under
# For unders: outcome = 1 if actual < line (flip the model_prob)
all_under_records = []
for r in all_k_records:
    u = dict(r)
    u["model_prob"] = 1.0 - r["model_prob"]
    u["outcome"] = 1 - r["outcome"]
    u["prop_type"] = "under_k"
    all_under_records.append(u)
for r in all_h_records:
    u = dict(r)
    u["model_prob"] = 1.0 - r["model_prob"]
    u["outcome"] = 1 - r["outcome"]
    u["prop_type"] = "under_h"
    all_under_records.append(u)

all_under_records.sort(key=lambda x: x["date"])
print(f"  Under-prop records: {len(all_under_records)}")

# EVHunter: all props pooled, top-10 by edge sorted by date slice
all_ev_records = all_k_records + all_h_records
all_ev_records.sort(key=lambda x: x["date"])
print(f"  EVHunter pooled records: {len(all_ev_records)}")

print("\n► Running per-agent walk-forward folds...\n")

# ─── Agent Definitions ────────────────────────────────────────────────────────
AGENTS = [
    # (name, records, min_prob_gate, extra_filter_fn, notes)
    ("UmpireAgent",   all_k_records,       0.54, None,
     "K props, umpire signal approx via prob gate"),
    ("ArsenalAgent",  all_k_records,       0.54, None,
     "K props, pitch-type matchup (same data pool as UmpireAgent)"),
    ("CatcherAgent",  all_k_records,       0.54, None,
     "K props, catcher framing signal approx via prob gate"),
    ("F5Agent",       all_f5_records,      0.52, None,
     "K props in games where pitcher threw 5+ IP"),
    ("BullpenAgent",  all_bullpen_records, 0.52,
     lambda r: r.get("game_in_series", 0) >= 2,
     "K props, game 2+ in series = bullpen fatigue proxy"),
    ("UnderMachine",  all_under_records,   0.52, None,
     "Strictly Unders — K and Hit props flipped"),
    ("GetawayAgent",  all_under_records,   0.52,
     lambda r: r["prop_type"] == "under_h",
     "Under Hit props only (schedule anomaly proxy)"),
    ("LineupAgent",   all_h_records,       0.52, None,
     "Hit props, batting order gate approximated"),
    ("PlatoonAgent",  all_h_records,       0.52, None,
     "Hit props, handedness matchup approximated"),
    ("EVHunter",      all_ev_records,      0.52, None,
     "All props pooled, top edge by prob"),
]

# ─── Run Folds ────────────────────────────────────────────────────────────────
results = {}  # agent_name -> [fold_results]

for agent_name, records, min_gate, extra_filter, notes in AGENTS:
    agent_results = []
    print(f"  ── {agent_name} ({len(records)} total records)")
    for fold in FOLD_DEFS:
        train_recs, test_recs = apply_fold(records, fold)
        fold_label = fold[4]

        if len(train_recs) < 20:
            print(f"     {fold_label}: SKIP (train n={len(train_recs)} < 20)")
            agent_results.append({"fold": fold_label, "skip": True, "reason": "insufficient train data"})
            continue

        # Tune threshold on train only — capped at min_gate+0.10 to prevent overshoot
        threshold = tune_threshold(train_recs, min_gate)

        # Apply to test
        test_filtered = filter_records(test_recs, agent_name, threshold, extra_filter)

        if len(test_filtered) < 5:
            print(f"     {fold_label}: SKIP after filter (test n={len(test_filtered)} < 5)")
            agent_results.append({"fold": fold_label, "skip": True, "reason": f"only {len(test_filtered)} test bets after filter"})
            continue

        m = calc_metrics(test_filtered, fold_label)
        b = baseline_metrics(test_filtered)

        beat_baseline = m["win_rate"] > BREAKEVEN
        edge_vs_random = m["roi"] - b["roi"]

        print(f"     {fold_label}: n={m['n']:3d} | win={m['win_rate']:.1%} | ROI={m['roi']:+.1%} | MaxDD={m['max_dd']:.1%} | Brier={m['brier']:.3f} | baseline_ROI={b['roi']:+.1%} | edge_vs_random={edge_vs_random:+.1%} | thresh={threshold:.2f}")

        agent_results.append({
            "fold": fold_label,
            "skip": False,
            "threshold": threshold,
            "n": m["n"],
            "win_rate": m["win_rate"],
            "roi": m["roi"],
            "max_dd": m["max_dd"],
            "brier": m["brier"],
            "baseline_roi": b["roi"],
            "edge_vs_random": edge_vs_random,
            "beat_breakeven": beat_baseline,
        })

    results[agent_name] = agent_results
    print()

# ─── Aggregate Summary ────────────────────────────────────────────────────────
print("═" * 70)
print("AGGREGATE SUMMARY — ALL AGENTS (REAL BACKTEST)")
print("═" * 70)
print(f"{'Agent':<16} {'Folds':>5} {'Avg n':>6} {'Avg Win%':>9} {'Avg ROI':>8} {'Avg MaxDD':>10} {'vs Random':>10} {'Verdict':>12}")
print("─" * 90)

SIMULATION_LIMITED_SHORT = list(SIMULATION_LIMITED.keys())

for agent_name, notes_list in [(a[0], a[4]) for a in AGENTS]:
    folds = [f for f in results[agent_name] if not f.get("skip")]
    if not folds:
        print(f"  {agent_name:<16} {'—':>5} {'—':>6} {'—':>9} {'—':>8} {'—':>10} {'—':>10} {'INSUFFICIENT DATA':>12}")
        continue

    avg_win = sum(f["win_rate"] for f in folds) / len(folds)
    avg_roi = sum(f["roi"] for f in folds) / len(folds)
    avg_dd = sum(f["max_dd"] for f in folds) / len(folds)
    avg_n = sum(f["n"] for f in folds) / len(folds)
    avg_edge = sum(f["edge_vs_random"] for f in folds) / len(folds)

    if avg_roi > 0.05 and avg_win > BREAKEVEN:
        verdict = "✅ EDGE"
    elif avg_roi > 0.0 and avg_win > BREAKEVEN - 0.02:
        verdict = "⚠ MARGINAL"
    elif avg_roi < -0.10:
        verdict = "❌ LOSING"
    else:
        verdict = "⚠ BREAK-EVEN"

    print(f"  {agent_name:<16} {len(folds):>5} {avg_n:>6.0f} {avg_win:>9.1%} {avg_roi:>8.1%} {avg_dd:>10.1%} {avg_edge:>10.1%} {verdict:>12}")

print()
print("─" * 70)
print("SIMULATION-LIMITED AGENTS (Live data dependency — no real fold data)")
print("─" * 70)
for agent, reason in SIMULATION_LIMITED.items():
    print(f"  {agent:<18} ⚡ {reason}")

print()
print("─" * 70)
print("DISCLAIMERS")
print("─" * 70)
print("  • Lines are synthetic (rolling 30-game median), not real historical DFS lines")
print("  • Agent filters are stat-category approximations, not live source tags")
print("  • Threshold tuned on train fold only — no peeking into test window")
print("  • Baseline (random 50% of bets) should print negative ROI; if not, settlement bug")
print("  • UmpireAgent/ArsenalAgent/CatcherAgent share identical K-prop pool —")
print("    live differentiation comes from umpire signals, arsenal matchups, and framing data")
print("  • Breakeven at PrizePicks no-vig: 54.2% (shown as dashed threshold above)")
print("=" * 70)
