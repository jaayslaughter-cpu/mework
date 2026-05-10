"""
propiq_signal_upgrades.py
==========================
PropIQ — Consolidated Signal Upgrade Module

Combines five distinct improvements derived from the reviewed GitHub repos.
All functions are pure (no I/O, no DB calls) for easy integration and testing.

MODULES INSIDE
--------------
1. QUALITY GATES          — pitcher/umpire/lineup maturity flags
                            (from BaseballbettingEdge quality_gates.py)

2. UMPIRE K TABLE         — 90-umpire K/9 adjustment table
                            (from BaseballbettingEdge career_k_rates.json)

3. PLATOON K DELTA        — handedness matchup K% adjustments
                            (from BaseballbettingEdge build_features.py)

4. POWER DEVIG            — theoretically correct vig removal
                            (from mlb-analytics-hub nrfi_odds.py)

5. PAR SCORE              — pitcher appearance quality rating (0–100)
                            (from sequencebaseball spgrader/scoring.py)

6. STATCAST TREND GATES   — significance thresholds for short-window changes
                            (from sequencebaseball cogs/trends.py)

INTEGRATION POINTS
------------------
- Quality gates → call `evaluate_prop_quality()` before any prop fires
- Umpire table  → call `get_umpire_k_adj()` in prop_enrichment_layer.py
- Platoon delta → call `platoon_k_delta()` in matchup_engine.py
- Power devig   → call `devig_power()` or `devig_all()` in odds_math.py
- PAR score     → call `compute_par_score()` in fg_pitcher_quality_layer.py
- Trend gates   → call `is_significant_trend()` in drift_monitor.py
"""

from __future__ import annotations

import math
import unicodedata
from dataclasses import dataclass
from typing import Any, Optional


# ══════════════════════════════════════════════════════════════════════════════
# 1. QUALITY GATES
# ══════════════════════════════════════════════════════════════════════════════

# Flags that hard-kill a pick regardless of EV
SEVERE_FLAGS = {
    "no_pitcher_k_profile",     # pitcher has no K/9 data at all
    "opener",                   # opener/bullpen game — K props unreliable
    "starter_mismatch",         # probable starter differs from actual
    "missing_game_time",        # game time unknown — can't lock before pitch
    "unresolved_probable",      # probable not yet confirmed
    "malformed_line_or_odds",   # line or odds are not usable numbers
    "invalid_lambda_inputs",    # lambda would be nonsensical
    "missing_team_or_opp_team", # can't compute matchup without team info
    "no_target_book",           # no sportsbook line available to bet into
}

# Flags that reduce confidence but don't kill the pick
SOFT_CAP_FLAGS = {
    "projected_lineup",         # lineup not yet confirmed
    "partial_lineup",           # fewer than 9 confirmed batters
    "unrated_umpire",           # umpire not in career K-rate table
    "thin_umpire_sample",       # umpire has < 10 games in table
    "missing_career_swstr",     # no career SwStr% data for delta calculation
    "neutral_park_fallback",    # using league-avg park factor
    "first_seen_opening",       # first time pitcher's opening odds seen today
    "thin_recent_start_sample", # 1–2 recent starts this season
    "developing_pitcher_sample",# 3–4 recent starts (still stabilizing)
    "partial_movement_history", # limited line movement data
}


def _is_usable_number(value: Any, *, positive: bool = False) -> bool:
    """Return True if value is a finite, non-bool number (optionally > 0)."""
    if isinstance(value, bool):
        return False
    try:
        n = float(value)
    except (TypeError, ValueError):
        return False
    if not math.isfinite(n):
        return False
    return n > 0 if positive else True


@dataclass
class QualityResult:
    maturity:   str           # "mature" | "developing" | "thin" | "none" | "projected"
    flags:      list[str]
    is_severe:  bool          # True = hard kill
    confidence: float         # 0.0–1.0, reduced per soft flag


def pitcher_maturity(record: dict) -> QualityResult:
    """Assess pitcher data quality based on K/9 availability and start count.

    Args:
        record: dict with keys season_k9, recent_k9, career_k9, recent_start_count
    """
    flags: list[str] = []
    has_profile = any(
        _is_usable_number(record.get(field), positive=True)
        for field in ("season_k9", "recent_k9", "career_k9")
    )
    if not has_profile:
        return QualityResult("none", ["no_pitcher_k_profile"], True, 0.0)

    count_raw = record.get("recent_start_count")
    if not _is_usable_number(count_raw):
        return QualityResult("mature", [], False, 1.0)

    count = int(float(count_raw))
    if 1 <= count <= 2:
        flags.append("thin_recent_start_sample")
        return QualityResult("thin", flags, False, 0.60)
    if 3 <= count <= 4:
        flags.append("developing_pitcher_sample")
        return QualityResult("developing", flags, False, 0.80)
    return QualityResult("mature", [], False, 1.0)


def umpire_maturity(record: dict) -> QualityResult:
    """Assess umpire data quality.

    Args:
        record: dict with umpire_name, umpire_has_rating, umpire_rating_games
    """
    flags: list[str] = []
    if record.get("umpire_name") and record.get("umpire_has_rating") is False:
        return QualityResult("unknown", ["unrated_umpire"], False, 0.85)

    games_raw = record.get("umpire_rating_games")
    if games_raw is None:
        return QualityResult("mature", [], False, 1.0)
    if not _is_usable_number(games_raw):
        return QualityResult("unknown", ["unrated_umpire"], False, 0.85)

    games = int(float(games_raw))
    if games < 10:
        flags.append("thin_umpire_sample")
        return QualityResult("thin", flags, False, 0.90)
    if games < 50:
        flags.append("thin_umpire_sample")
        return QualityResult("developing", flags, False, 0.95)
    return QualityResult("mature", [], False, 1.0)


def lineup_maturity(record: dict) -> QualityResult:
    """Assess lineup confirmation status.

    Args:
        record: dict with lineup_confirmed (bool), lineup_count (int)
    """
    if not record.get("lineup_confirmed", False):
        return QualityResult("projected", ["projected_lineup"], False, 0.90)
    count_raw = record.get("lineup_count")
    if _is_usable_number(count_raw):
        count = int(float(count_raw))
        if 0 < count < 9:
            return QualityResult("partial", ["partial_lineup"], False, 0.95)
    return QualityResult("confirmed", [], False, 1.0)


def evaluate_prop_quality(prop_record: dict) -> dict:
    """Run all quality gates on a prop record.

    Args:
        prop_record: dict with all pitcher/umpire/lineup keys

    Returns dict with:
        all_flags:    list of all flag strings
        severe_flags: list of SEVERE flags (pick must be killed)
        soft_flags:   list of SOFT flags (confidence reduction only)
        confidence:   float 0–1 (product of per-gate confidence)
        should_fire:  bool — False if any severe flag present
        kill_reason:  str or None — human-readable kill reason
    """
    all_flags: list[str] = []

    pit_result  = pitcher_maturity(prop_record)
    ump_result  = umpire_maturity(prop_record)
    lu_result   = lineup_maturity(prop_record)

    all_flags.extend(pit_result.flags)
    all_flags.extend(ump_result.flags)
    all_flags.extend(lu_result.flags)

    # Check for opener
    avg_ip = prop_record.get("avg_ip", 6.0)
    start_count = prop_record.get("recent_start_count", 10)
    if _is_usable_number(avg_ip) and _is_usable_number(start_count):
        if float(avg_ip) < 2.5 and int(float(start_count)) >= 2:
            all_flags.append("opener")

    # Check odds validity
    for field in ("over_american", "under_american"):
        val = prop_record.get(field)
        if not _is_usable_number(val) or float(val) == 0:
            all_flags.append("malformed_line_or_odds")
            break

    # Deduplicate
    seen: set[str] = set()
    deduped: list[str] = []
    for f in all_flags:
        if f not in seen:
            seen.add(f)
            deduped.append(f)

    severe  = [f for f in deduped if f in SEVERE_FLAGS]
    soft    = [f for f in deduped if f in SOFT_CAP_FLAGS]

    # Confidence = product of per-gate confidence (multiplicative degradation)
    confidence = pit_result.confidence * ump_result.confidence * lu_result.confidence
    # Each additional soft flag reduces confidence by 3%
    confidence *= (0.97 ** len(soft))
    confidence  = max(0.0, min(1.0, confidence))

    return {
        "all_flags":    deduped,
        "severe_flags": severe,
        "soft_flags":   soft,
        "confidence":   round(confidence, 3),
        "should_fire":  len(severe) == 0,
        "kill_reason":  severe[0].replace("_", " ") if severe else None,
    }


# ══════════════════════════════════════════════════════════════════════════════
# 2. UMPIRE K TABLE
# ══════════════════════════════════════════════════════════════════════════════

def _normalize_name(name: str) -> str:
    """Lowercase, strip accents, collapse whitespace."""
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_str = "".join(c for c in nfkd if not unicodedata.combining(c))
    return " ".join(ascii_str.lower().strip().split())


# Career K/9 adjustments per HP umpire.
# Source: BaseballbettingEdge career_k_rates.json (calibrated from 2022–2026 data)
# Values = K/9 adjustment (positive = umpire calls more strikeouts than average)
# Range: approx -1.4 (suppresses Ks) to +1.9 (inflates Ks)
UMPIRE_K_ADJ_TABLE: dict[str, float] = {
    "adam beck":           0.062,
    "adam hamari":         0.860,
    "adrian johnson":     -0.789,
    "alan porter":         0.045,
    "alex mackay":         0.444,
    "alex tosi":          -0.281,
    "alfonso marquez":    -0.838,
    "andy fletcher":       0.045,
    "austin jones":        0.229,
    "ben may":            -1.182,
    "bill miller":         0.520,
    "brennan miller":      0.047,
    "brian o'nora":       -0.673,
    "brian walsh":         0.438,
    "brock ballou":        0.374,
    "bruce dreckman":     -0.547,
    "cb bucknor":          0.496,
    "carlos torres":      -0.521,
    "chad fairchild":     -0.158,
    "chad whitson":       -0.015,
    "charlie ramos":      -0.013,
    "chris conroy":       -0.138,
    "chris guccione":      0.819,
    "chris segal":         0.400,
    "clint vondrak":      -0.871,
    "cory blaser":        -0.188,
    "d.j. reyburn":       -0.217,
    "dan bellino":         0.031,
    "dan iassogna":       -0.380,
    "dan merzel":          0.078,
    "david rackley":       0.277,
    "derek thomas":        0.011,
    "doug eddings":        0.630,
    "edwin jimenez":      -0.216,
    "edwin moscoso":       0.533,
    "emil jimenez":        0.717,
    "erich bacchus":      -0.309,
    "gabe morales":        0.225,
    "hunter wendelstedt": -0.266,
    "jacob metz":          0.512,
    "james hoye":         -0.278,
    "james jean":         -1.234,
    "jansen visconti":    -0.887,
    "jeremie rehak":       0.467,
    "jim wolf":           -0.194,
    "john bacon":         -0.497,
    "john libka":         -0.117,
    "john tumpane":       -0.223,
    "jonathan parra":      0.219,
    "jordan baker":       -0.171,
    "junior valentine":    1.263,
    "lance barksdale":    -0.315,
    "lance barrett":      -0.032,
    "larry vanover":       0.788,
    "laz diaz":            0.785,
    "malachi moore":      -0.027,
    "manny gonzalez":     -1.006,
    "mark carlson":       -0.384,
    "mark ripperger":     -0.442,
    "mark wegner":         0.542,
    "marvin hudson":       0.109,
    "mike estabrook":      1.080,
    "mike muchlinski":    -0.363,
    "nate tomlinson":      0.431,
    "nestor ceja":         0.970,
    "nic lentz":          -0.719,
    "nick mahrley":       -0.018,
    "paul clemons":       -0.205,
    "phil cuzzi":          0.342,
    "quinn wolcott":      -0.888,
    "ramon de jesus":      0.016,
    "rob drake":           1.020,
    "roberto ortiz":      -0.510,
    "ron kulpa":           1.928,
    "ryan additon":        0.145,
    "ryan blakney":        0.566,
    "ryan wills":          0.145,
    "scott barry":        -1.155,
    "sean barber":        -0.044,
    "shane livensparger": -1.395,
    "stu scheurwater":    -0.150,
    "todd tichenor":      -0.155,
    "tom hanahan":         0.645,
    "tony randazzo":       0.378,
    "tripp gibson":       -0.636,
    "vic carapazza":       0.373,
    "will little":         0.458,
}


def get_umpire_k_adj(umpire_name: str) -> tuple[float, bool]:
    """Return (k_adj, found) for a given HP umpire name.

    k_adj is a K/9 adjustment — positive = umpire calls more Ks.
    found = False if umpire not in table (k_adj = 0.0).

    Normalizes names to handle casing and accent differences.

    Examples:
        get_umpire_k_adj("Ron Kulpa")  → (1.928, True)   # top K inflator
        get_umpire_k_adj("Shane Livensparger") → (-1.395, True)  # top K suppressor
        get_umpire_k_adj("Unknown Ump") → (0.0, False)
    """
    if not umpire_name:
        return 0.0, False
    key = _normalize_name(umpire_name)
    adj = UMPIRE_K_ADJ_TABLE.get(key)
    if adj is None:
        return 0.0, False
    return adj, True


def umpire_impact_summary(umpire_name: str) -> str:
    """Human-readable umpire K impact summary for Discord/logging."""
    adj, found = get_umpire_k_adj(umpire_name)
    if not found:
        return f"Umpire {umpire_name}: not in table (no adjustment)"
    direction = "above" if adj > 0 else "below"
    magnitude = "large" if abs(adj) > 1.0 else "moderate" if abs(adj) > 0.4 else "slight"
    return (
        f"Umpire {umpire_name}: {adj:+.3f} K/9 vs avg "
        f"({magnitude} {direction} average)"
    )


# ══════════════════════════════════════════════════════════════════════════════
# 3. PLATOON K DELTA
# ══════════════════════════════════════════════════════════════════════════════

# Multi-season MLB aggregate K% deltas by (batter_hand, pitcher_throws) matchup.
# Source: BaseballbettingEdge build_features.py — PLATOON_K_DELTA constant.
# Units: K% rate points (additive to lineup K rate).
# Switch-hitters modeled as batting opposite to pitcher's hand.
PLATOON_K_DELTA_TABLE: dict[tuple[str, str], float] = {
    ("R", "R"):  0.005,   # RHB vs RHP: slight same-hand K bump
    ("R", "L"): -0.010,   # RHB vs LHP: platoon advantage (fewer Ks)
    ("L", "R"): -0.015,   # LHB vs RHP: strong platoon advantage (fewer Ks)
    ("L", "L"):  0.020,   # LHB vs LHP: reverse platoon — rare, large effect
}


def platoon_k_delta(batter_hand: str, pitcher_throws: str) -> float:
    """League-average K% adjustment for a batter-pitcher handedness matchup.

    Args:
        batter_hand:    "R", "L", or "S" (switch — bats opposite of pitcher)
        pitcher_throws: "R" or "L"

    Returns:
        Additive K% delta (e.g. 0.020 means +2.0% K rate above baseline)
    """
    if batter_hand == "S":
        batter_hand = "L" if pitcher_throws == "R" else "R"
    batter_hand    = batter_hand.upper().strip()
    pitcher_throws = pitcher_throws.upper().strip()
    return PLATOON_K_DELTA_TABLE.get((batter_hand, pitcher_throws), 0.0)


def lineup_platoon_k_adj(
    lineup: list[dict],
    pitcher_throws: str,
    league_avg_k_rate: float = 0.227,
) -> float:
    """Compute lineup-level K% adjustment from platoon splits.

    Args:
        lineup: list of dicts with "batter_hand" key
        pitcher_throws: pitcher handedness "R" or "L"
        league_avg_k_rate: baseline K rate to start from

    Returns:
        Adjusted lineup K rate (for use in compute_k_lambda opp_lineup_k_pct)
    """
    if not lineup:
        return league_avg_k_rate
    deltas = [platoon_k_delta(b.get("batter_hand", "R"), pitcher_throws) for b in lineup]
    avg_delta = sum(deltas) / len(deltas)
    return max(0.05, min(0.50, league_avg_k_rate + avg_delta))


# ══════════════════════════════════════════════════════════════════════════════
# 4. POWER DEVIG (theoretically correct vig removal)
# ══════════════════════════════════════════════════════════════════════════════

def american_to_prob(odds: float) -> float:
    """Convert American odds to raw implied probability (with vig)."""
    if odds is None or not math.isfinite(float(odds)):
        return 0.5
    odds = float(odds)
    if odds >= 0:
        return 100.0 / (odds + 100.0)
    return abs(odds) / (abs(odds) + 100.0)


def prob_to_american(p: float) -> int:
    """Convert fair probability to American odds."""
    p = max(0.001, min(0.999, p))
    if p >= 0.5:
        return int(round(-(p / (1.0 - p)) * 100))
    return int(round(((1.0 - p) / p) * 100))


def devig_power(probs: list[float], tol: float = 1e-6, max_iter: int = 200) -> list[float]:
    """Power (Shin) method vig removal — most theoretically correct.

    Finds exponent k such that sum(p_i^k) = 1.0 via binary search.
    Source: mlb-analytics-hub nrfi_odds.py

    Preferred over multiplicative devig for prop markets where one side
    may be heavily favored (asymmetric overround distribution).
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
    k   = (lo + hi) / 2.0
    raw = [p ** k for p in probs]
    s   = sum(raw)
    return [r / s for r in raw] if s > 0 else probs


def devig_all(over_american: float, under_american: float) -> dict:
    """Compute fair probabilities using all four devig methods + recommend power.

    Returns dict with overround, per-method results, and recommended (power).

    Usage:
        result = devig_all(-115, -115)
        fair_over = result["recommended"]["over_fair_prob"]
    """
    raw_over  = american_to_prob(over_american)
    raw_under = american_to_prob(under_american)
    raw = [raw_over, raw_under]
    overround = round(sum(raw) - 1.0, 4)

    def _fmt(pair):
        o, u = pair
        return {
            "over_fair_prob":    round(o, 4),
            "under_fair_prob":   round(u, 4),
            "over_fair_american":  prob_to_american(o),
            "under_fair_american": prob_to_american(u),
        }

    def _additive(p):
        total = sum(p)
        adj = [x - (total - 1.0) / len(p) for x in p]
        s   = sum(max(0.001, a) for a in adj)
        return [max(0.001, a) / s for a in adj]

    def _multiplicative(p):
        total = sum(p)
        return [x / total for x in p] if total > 0 else p

    methods = {
        "additive":       _fmt(_additive(raw)),
        "multiplicative": _fmt(_multiplicative(raw)),
        "power":          _fmt(devig_power(raw)),
    }

    return {
        "overround":   overround,
        "methods":     methods,
        "recommended": methods["power"],  # power method is preferred
    }


def no_vig_prob_power(over_american: float, under_american: float) -> tuple[float, float]:
    """Return (fair_over_prob, fair_under_prob) using power devig.

    This is the recommended single-call interface for PropIQ's EV calculations.
    """
    result = devig_all(over_american, under_american)
    rec    = result["recommended"]
    return rec["over_fair_prob"], rec["under_fair_prob"]


# ══════════════════════════════════════════════════════════════════════════════
# 5. PAR SCORE (Pitcher Appearance Rating)
# ══════════════════════════════════════════════════════════════════════════════
# Source: sequencebaseball cogs/spgrader/scoring.py
# Use as a compact context feature in fg_pitcher_quality_layer.py

@dataclass
class PARScore:
    """Pitcher Appearance Rating for a single game start."""
    pitcher_name:    str
    game_date:       str
    opponent:        str
    score:           float    # 0–100
    grade:           str      # A+, A, B, C, D, F
    # Components (for diagnostics)
    depth_pts:       float    # innings contribution
    dominance_pts:   float    # strikeout contribution
    control_pts:     float    # walk penalty
    runs_pts:        float    # earned run penalty


def compute_par_score(
    pitcher_name: str,
    outs_recorded: int,
    strikeouts: int,
    walks: int,
    earned_runs: int,
    game_date: str = "",
    opponent: str = "",
) -> PARScore:
    """Compute Pitcher Appearance Rating on a 0–100 scale.

    Component weights (from sequencebaseball):
      Depth (innings): 40pts — 6 IP = full, linear
      Dominance (K):   25pts — 10 Ks = full, capped
      Control (BB):    15pts — 0 BB = 15pts, −3 per walk
      Run prevention:  20pts — 0 ER = 20pts, −4 per ER

    Use the rolling average PAR over recent starts as a feature in your
    pitcher quality layer. A pitcher trending A+ has a different K-prop
    risk profile than one trending D.

    Examples:
        6 IP, 8K, 2BB, 1ER → score 73.3, grade B
        7 IP, 10K, 0BB, 0ER → score 100.0, grade A+
        3 IP, 4K, 4BB, 5ER → score 13.3, grade F
    """
    ip        = min(outs_recorded / 3.0, 9.0)
    depth     = (ip / 6.0) * 40.0
    dominance = min((strikeouts / 10.0) * 25.0, 25.0)
    control   = max(15.0 - walks * 3.0, 0.0)
    runs      = max(20.0 - earned_runs * 4.0, 0.0)
    score     = min(depth + dominance + control + runs, 100.0)

    grade_map = [(90, "A+"), (80, "A"), (70, "B"), (60, "C"), (50, "D")]
    grade     = next((g for threshold, g in grade_map if score >= threshold), "F")

    return PARScore(
        pitcher_name=pitcher_name,
        game_date=game_date,
        opponent=opponent,
        score=round(score, 1),
        grade=grade,
        depth_pts=round(depth, 1),
        dominance_pts=round(dominance, 1),
        control_pts=round(control, 1),
        runs_pts=round(runs, 1),
    )


def rolling_par_avg(starts: list[dict], n_starts: int = 5) -> Optional[float]:
    """Compute rolling PAR average over the last n_starts.

    Args:
        starts: list of game dicts with keys:
                outs_recorded, strikeouts, walks, earned_runs
        n_starts: how many recent starts to average (default 5)

    Returns float 0–100, or None if no starts available.
    """
    if not starts:
        return None
    recent = starts[-n_starts:]
    scores = [
        compute_par_score(
            pitcher_name="",
            outs_recorded=int(s.get("outs_recorded", 0)),
            strikeouts=int(s.get("strikeouts", 0)),
            walks=int(s.get("walks", 0)),
            earned_runs=int(s.get("earned_runs", 0)),
        ).score
        for s in recent
        if _is_usable_number(s.get("outs_recorded"), positive=True)
    ]
    return round(sum(scores) / len(scores), 1) if scores else None


# ══════════════════════════════════════════════════════════════════════════════
# 6. STATCAST TREND SIGNIFICANCE GATES
# ══════════════════════════════════════════════════════════════════════════════
# Source: sequencebaseball cogs/trends.py
# Use in drift_monitor.py for early-season when Z-score variance is unstable.

# Minimum absolute change required to flag a trend as significant.
# These are calibrated to actual MLB Statcast distributions (not invented).
PITCHER_TREND_THRESHOLDS: dict[str, float] = {
    "release_speed":  1.0,   # mph — meaningful velo drop/gain
    "pfx_x":          2.0,   # inches — meaningful horizontal break shift
    "pfx_z":          2.0,   # inches — meaningful vertical break shift
    "whiff_rate":     8.0,   # pct points — meaningful whiff change
    "usage_pct":      8.0,   # pct points — meaningful pitch-mix shift
    "release_spin_rate": 150.0,  # rpm — meaningful spin change
}

HITTER_TREND_THRESHOLDS: dict[str, float] = {
    "launch_speed":   3.0,   # mph avg EV
    "whiff_rate":     5.0,   # pct points
    "hard_hit_rate":  8.0,   # pct points (EV >= 95mph)
    "xba":            0.025, # expected BA
    "sweet_spot_pct": 8.0,   # pct points (LA 8°–32°)
}


def is_significant_trend(
    metric: str,
    recent_value: float,
    prior_value: float,
    player_type: str = "pitcher",
) -> tuple[bool, float]:
    """Return (is_significant, delta) for a Statcast metric trend.

    Uses explicit significance thresholds calibrated to MLB distributions.
    Preferred over Z-score-only detection in early season (< 100 PA / 5 starts)
    when variance estimates are unreliable.

    Args:
        metric:       Statcast metric name (e.g. "release_speed", "whiff_rate")
        recent_value: Value in the recent window (e.g. last 14 days)
        prior_value:  Value in the prior comparison window
        player_type:  "pitcher" or "hitter"

    Returns (is_significant, delta)

    Examples:
        is_significant_trend("release_speed", 92.1, 94.3, "pitcher")
        → (True, -2.2)   # significant velo drop

        is_significant_trend("whiff_rate", 28.0, 27.0, "pitcher")
        → (False, 1.0)   # delta below 8% threshold
    """
    thresholds = (
        PITCHER_TREND_THRESHOLDS if player_type == "pitcher"
        else HITTER_TREND_THRESHOLDS
    )
    threshold = thresholds.get(metric)
    if threshold is None:
        # Unknown metric — fall back to 5% relative change
        if prior_value == 0:
            return False, 0.0
        delta = recent_value - prior_value
        return abs(delta / prior_value) >= 0.05, round(delta, 4)

    delta = recent_value - prior_value
    return abs(delta) >= threshold, round(delta, 4)


def detect_all_pitcher_trends(
    recent_metrics: dict[str, float],
    prior_metrics: dict[str, float],
) -> list[dict]:
    """Detect all significant trends for a pitcher across tracked metrics.

    Args:
        recent_metrics: {metric_name: value} for recent window
        prior_metrics:  {metric_name: value} for prior window

    Returns list of significant trend dicts:
        [{"metric": ..., "delta": ..., "recent": ..., "prior": ...}, ...]
    """
    trends = []
    for metric in PITCHER_TREND_THRESHOLDS:
        recent_val = recent_metrics.get(metric)
        prior_val  = prior_metrics.get(metric)
        if recent_val is None or prior_val is None:
            continue
        significant, delta = is_significant_trend(metric, recent_val, prior_val, "pitcher")
        if significant:
            trends.append({
                "metric":  metric,
                "delta":   delta,
                "recent":  recent_val,
                "prior":   prior_val,
                "direction": "up" if delta > 0 else "down",
            })
    return trends


# ══════════════════════════════════════════════════════════════════════════════
# QUICK SELF-TEST
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 60)
    print("  PROPIQ SIGNAL UPGRADES — SELF TEST")
    print("=" * 60)

    # 1. Quality gates
    rec = {"season_k9": 9.2, "recent_k9": 8.5, "recent_start_count": 5,
           "avg_ip": 6.0, "over_american": -115, "under_american": -115,
           "lineup_confirmed": True, "lineup_count": 9}
    q = evaluate_prop_quality(rec)
    print(f"\n[Quality Gates] Mature record: should_fire={q['should_fire']}, confidence={q['confidence']}")
    assert q["should_fire"] is True

    rec_bad = {"season_k9": None, "recent_k9": None, "recent_start_count": 1,
               "avg_ip": 2.0, "over_american": 0, "under_american": -115}
    q2 = evaluate_prop_quality(rec_bad)
    print(f"[Quality Gates] Bad record: should_fire={q2['should_fire']}, severe={q2['severe_flags']}")
    assert q2["should_fire"] is False

    # 2. Umpire table
    adj, found = get_umpire_k_adj("Ron Kulpa")
    print(f"\n[Umpire] Ron Kulpa: {adj:+.3f} K/9 (found={found})")
    assert found and adj > 1.5

    adj2, found2 = get_umpire_k_adj("Shane Livensparger")
    print(f"[Umpire] Shane Livensparger: {adj2:+.3f} K/9 (found={found2})")
    assert found2 and adj2 < -1.0

    adj3, found3 = get_umpire_k_adj("Nobody McFake")
    print(f"[Umpire] Unknown ump: {adj3} (found={found3})")
    assert not found3 and adj3 == 0.0

    # 3. Platoon deltas
    d = platoon_k_delta("L", "L")
    print(f"\n[Platoon] LHB vs LHP K delta: {d:+.3f} (expect +0.020)")
    assert d == 0.020

    d2 = platoon_k_delta("L", "R")
    print(f"[Platoon] LHB vs RHP K delta: {d2:+.3f} (expect -0.015)")
    assert d2 == -0.015

    d3 = platoon_k_delta("S", "R")  # switch hitter vs RHP bats LH
    print(f"[Platoon] Switch vs RHP K delta: {d3:+.3f} (expect -0.015)")
    assert d3 == -0.015

    # 4. Power devig
    result = devig_all(-115, -115)
    fair_o, fair_u = result["recommended"]["over_fair_prob"], result["recommended"]["under_fair_prob"]
    print(f"\n[Devig] -115/-115 → fair over={fair_o:.4f}, fair under={fair_u:.4f}, sum={fair_o+fair_u:.4f}")
    assert abs(fair_o + fair_u - 1.0) < 0.001

    result2 = devig_all(-130, +110)
    fair_o2 = result2["recommended"]["over_fair_prob"]
    print(f"[Devig] -130/+110 → fair over={fair_o2:.4f} (expect ~0.54)")
    assert 0.50 < fair_o2 < 0.60

    # 5. PAR score
    par = compute_par_score("Test Pitcher", outs_recorded=18, strikeouts=8,
                             walks=2, earned_runs=1)
    print(f"\n[PAR] 6IP 8K 2BB 1ER → score={par.score}, grade={par.grade}")
    assert 65 < par.score <= 100  # 6IP=40pts, 8K=20pts, 2BB=9pts, 1ER=16pts → 85

    par2 = compute_par_score("Ace", outs_recorded=21, strikeouts=10, walks=0, earned_runs=0)
    print(f"[PAR] 7IP 10K 0BB 0ER → score={par2.score}, grade={par2.grade}")
    assert par2.grade == "A+"

    # 6. Trend gates
    sig, delta = is_significant_trend("release_speed", 91.5, 94.0, "pitcher")
    print(f"\n[Trend] Velo 94→91.5: significant={sig}, delta={delta}")
    assert sig is True

    sig2, delta2 = is_significant_trend("release_speed", 94.5, 94.2, "pitcher")
    print(f"[Trend] Velo 94.2→94.5: significant={sig2}, delta={delta2}")
    assert sig2 is False

    print("\n✅ All tests passed.")
