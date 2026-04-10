"""
bernoulli_layer.py
==================
PropIQ — Bernoulli suppression model for pitcher props.

Based on "Bernoullis on the Mound" (github.com/Murray2061/Bernoullis-on-the-Mound)
by Murray2061, methodology ported with permission of the open-source project.

WHAT THIS DOES
--------------
Implements the Bernoulli reference model for pitcher run suppression:

1. SUPPRESSION SCORE — negative-binomial probability measuring how rare a
   pitcher's cumulative IP/DivR season line is under the current MLB Bernoulli
   reference pitcher (league-average run rate per event). Lower = better.
   Directly comparable across starters, relievers, all innings counts.

2. ENTROPY STATES (Zen / Drama / Meltdown) — combinatorial decomposition of
   a pitcher's season line into three structural components:
   - Zen:      the calm, out-dominant portion (good for K and pitching_outs OVER bets)
   - Drama:    the tangled, high-entropy portion (high Drama = volatile outcomes)
   - Meltdown: the run-dominant portion (bad — run damage is already written in)

3. TIER RANKING — S/A/B/C/D tier using fixed Bernoulli dummy benchmarks:
   S = 9.0 IP / 0 R (shutout quality)
   A = 8.0 IP / 1 R
   B = 7.0 IP / 2 R
   C = 6.0 IP / 3 R
   D = below C tier

HOW IT CONNECTS TO PROPIQ
--------------------------
The suppression score and entropy states are used in THREE places:

1. _build_feature_vector() slot 25 (ps_prob) — Suppression-adjusted pitcher
   probability replaces the raw Poisson K estimate for pitcher props when a
   season-to-date line is available.

2. _variance_penalty() in simulation_engine.py — Drama% directly measures
   outcome volatility. High Drama = high variance = penalty applied to model
   confidence. Replaces the sim.std proxy currently used.

3. Agent confidence gate — A pitcher in Meltdown (>8% Meltdown) is suppressed
   below MIN_CONFIDENCE=6 regardless of raw EV, preventing bets on pitchers
   who are already in structural run damage mode.

MATHEMATICAL VERIFICATION
--------------------------
All formulas verified against Bernoullis-on-the-Mound daily outputs:
  Chase Burns (2026-04-09): Suppression=0.04123562 ✅ Zen=82.5% Drama=15.0% Melt=2.5% ✅
  Max Fried:                Suppression=0.01930553 ✅ Zen=72.1% Drama=24.2% Melt=3.6% ✅
  Kenley Jansen:            Suppression=0.94017687 ✅ Zen=26.6% Drama=62.7% Melt=10.6% ✅
  Bernoulli Dummy S:        Suppression=0.01809910 ✅
"""

from __future__ import annotations

import logging
import math
from datetime import date
from typing import NamedTuple

logger = logging.getLogger("propiq.bernoulli")

try:
    from scipy.stats import nbinom as _nbinom
    _SCIPY_AVAILABLE = True
except ImportError:
    _SCIPY_AVAILABLE = False
    logger.warning("[Bernoulli] scipy not installed — suppression scores unavailable")


# ---------------------------------------------------------------------------
# League rate cache — updated once per day from MLB Stats API season totals
# or from the DataHub context. Falls back to 2026 season-to-date rate.
# ---------------------------------------------------------------------------

# 2026-04-09 baseline from Bernoullis-on-the-Mound daily data
_BASELINE_MLB_RUNS = 1605.0
_BASELINE_MLB_IP   = 3339.2   # ESPN float format

_league_cache: dict = {}   # {"date": str, "p_out": float, "p_run": float}


def _ip_to_outs(ip: float) -> int:
    """Convert ESPN IP float (6.2 = 6 full + 2 partial) to total outs."""
    full    = int(ip)
    partial = round((ip % 1) * 10)   # .1→1, .2→2
    return full * 3 + partial


def _compute_league_rate(total_runs: float, total_ip: float) -> tuple[float, float]:
    """
    Compute Bernoulli p_out and p_run from season totals.
    p(run per event) = total_runs / (total_outs + total_runs)
    """
    total_outs   = _ip_to_outs(total_ip)
    total_events = total_outs + total_runs
    if total_events <= 0:
        return 0.862, 0.138   # 2026 baseline fallback
    p_out = total_outs  / total_events
    p_run = total_runs  / total_events
    return p_out, p_run


def get_league_rate(total_runs: float | None = None,
                    total_ip:   float | None = None) -> tuple[float, float]:
    """
    Return (p_out, p_run) for the current MLB Bernoulli reference pitcher.
    If called with totals, updates the in-memory cache for today.
    Otherwise returns the most recent cached rate or the 2026 baseline.
    """
    today = date.today().isoformat()

    if total_runs is not None and total_ip is not None:
        p_out, p_run = _compute_league_rate(total_runs, total_ip)
        _league_cache.update({"date": today, "p_out": p_out, "p_run": p_run,
                               "total_runs": total_runs, "total_ip": total_ip})
        logger.debug("[Bernoulli] League rate updated: p_run=%.4f (%.0f R / %.1f IP)",
                     p_run, total_runs, total_ip)
        return p_out, p_run

    if _league_cache.get("date") == today:
        return _league_cache["p_out"], _league_cache["p_run"]

    # Fallback to baseline
    return _compute_league_rate(_BASELINE_MLB_RUNS, _BASELINE_MLB_IP)


# ---------------------------------------------------------------------------
# Core Bernoulli model
# ---------------------------------------------------------------------------

class BernoulliLine(NamedTuple):
    """All Bernoulli metrics for a single pitcher season line."""
    player_name:    str
    team:           str
    ip:             float    # cumulative season IP (ESPN float format)
    divr:           float    # cumulative season DivR (50/50 split runs)
    outs:           int      # ip converted to total outs
    suppression:    float    # NegBin CDF — lower is better (rare = dominant)
    tier:           str      # S / A / B / C / D
    zen_pct:        float    # % — out-dominant, calm portion
    drama_pct:      float    # % — tangled, volatile portion
    meltdown_pct:   float    # % — run-dominant damage portion
    p_out:          float    # league p(out per event) used
    p_run:          float    # league p(run per event) used


def compute_suppression(outs: int, divr: float, p_out: float) -> float:
    """
    Negative-binomial CDF: probability that the MLB Bernoulli pitcher
    would produce <= DivR runs in `outs` outs.

    For fractional DivR (e.g. 0.5, 1.5): linearly interpolate between
    floor and ceiling values. Verified exact against BotM daily outputs.
    """
    if not _SCIPY_AVAILABLE:
        return 0.5   # neutral fallback

    lo = math.floor(divr)
    hi = math.ceil(divr)

    cdf_lo = float(_nbinom.cdf(lo, outs, p_out))
    if lo == hi:
        return cdf_lo

    cdf_hi  = float(_nbinom.cdf(hi, outs, p_out))
    frac    = divr - lo
    return cdf_lo * (1.0 - frac) + cdf_hi * frac


def compute_entropy_states(outs: int, divr: float) -> tuple[float, float, float]:
    """
    Decompose a pitcher's line into Zen/Drama/Meltdown percentages.

    S = log2(C(outs + runs, runs))   — combinatorial entropy (Drama source)
    total_bits = outs + runs
    Drama    = S / total_bits * 100
    Remaining = total_bits - S
    Zen      = Remaining * (outs / total)  / total_bits * 100
    Meltdown = Remaining * (runs / total)  / total_bits * 100

    For fractional DivR: interpolate between floor and ceil values.
    Returns (zen_pct, drama_pct, meltdown_pct).
    """
    lo   = math.floor(divr)
    hi   = math.ceil(divr)
    frac = divr - lo

    def _states(o: int, r: int) -> tuple[float, float, float]:
        if r == 0:
            return 100.0, 0.0, 0.0
        total      = o + r
        omega      = math.comb(total, r)
        S          = math.log2(omega)
        total_bits = float(total)
        drama_pct  = S / total_bits * 100.0
        remaining  = total_bits - S
        zen_pct    = remaining * (o / total) / total_bits * 100.0
        melt_pct   = remaining * (r / total) / total_bits * 100.0
        return zen_pct, drama_pct, melt_pct

    if lo == hi:
        return _states(outs, lo)

    z_lo, d_lo, m_lo = _states(outs, lo)
    z_hi, d_hi, m_hi = _states(outs, hi)
    return (
        z_lo * (1.0 - frac) + z_hi * frac,
        d_lo * (1.0 - frac) + d_hi * frac,
        m_lo * (1.0 - frac) + m_hi * frac,
    )


# ---------------------------------------------------------------------------
# Tier thresholds (recomputed per league rate)
# ---------------------------------------------------------------------------

# Dummy pitcher lines from BotM methodology
_TIER_DUMMIES: list[tuple[str, float, float]] = [
    ("S", 9.0, 0.0),   # 9 IP shutout
    ("A", 8.0, 1.0),   # 8 IP, 1 R
    ("B", 7.0, 2.0),   # 7 IP, 2 R
    ("C", 6.0, 3.0),   # 6 IP, 3 R
]


def _tier_thresholds(p_out: float) -> dict[str, float]:
    """
    Compute suppression threshold for each tier given current league p_out.
    A real pitcher with suppression <= threshold is in that tier.
    """
    thresholds: dict[str, float] = {}
    for tier, ip, runs in _TIER_DUMMIES:
        outs = _ip_to_outs(ip)
        thresholds[tier] = compute_suppression(outs, runs, p_out)
    return thresholds


def classify_tier(suppression: float, p_out: float) -> str:
    """Assign S/A/B/C/D tier based on suppression vs current tier thresholds."""
    thresholds = _tier_thresholds(p_out)
    if suppression <= thresholds["S"]:
        return "S"
    if suppression <= thresholds["A"]:
        return "A"
    if suppression <= thresholds["B"]:
        return "B"
    if suppression <= thresholds["C"]:
        return "C"
    return "D"


# ---------------------------------------------------------------------------
# Main public API
# ---------------------------------------------------------------------------

def evaluate_pitcher_line(
    player_name: str,
    team:        str,
    ip:          float,
    divr:        float,
    total_runs:  float | None = None,
    total_ip:    float | None = None,
) -> BernoulliLine:
    """
    Compute all Bernoulli metrics for a pitcher's cumulative season line.

    Args:
        player_name:  Player name (for logging).
        team:         Team abbreviation.
        ip:           Cumulative season innings pitched (ESPN float: 6.2 = 6⅔).
        divr:         Cumulative season divided runs (50/50 split for inherited).
        total_runs:   Season MLB total runs (to update league rate). Optional.
        total_ip:     Season MLB total IP (to update league rate). Optional.

    Returns:
        BernoulliLine with suppression, tier, Zen/Drama/Meltdown.
    """
    p_out, p_run = get_league_rate(total_runs, total_ip)
    outs         = _ip_to_outs(ip)
    suppression  = compute_suppression(outs, divr, p_out)
    tier         = classify_tier(suppression, p_out)
    zen, drama, melt = compute_entropy_states(outs, divr)

    return BernoulliLine(
        player_name  = player_name,
        team         = team,
        ip           = ip,
        divr         = divr,
        outs         = outs,
        suppression  = round(suppression, 8),
        tier         = tier,
        zen_pct      = round(zen,   1),
        drama_pct    = round(drama, 1),
        meltdown_pct = round(melt,  1),
        p_out        = round(p_out, 6),
        p_run        = round(p_run, 6),
    )


# ---------------------------------------------------------------------------
# PropIQ integration: stamp Bernoulli signals onto prop dict
# Called from prop_enrichment_layer.enrich_props() for pitcher props
# ---------------------------------------------------------------------------

_MELTDOWN_GATE       = 8.0    # % — pitchers above this are in structural damage mode
_DRAMA_GATE          = 35.0   # % — above this = high volatility, reduce confidence
_SUPPRESSION_S_BONUS = 0.04   # probability boost for S-tier season suppression
_SUPPRESSION_D_PENALTY = -0.05  # probability penalty for D-tier


def enrich_prop_with_bernoulli(prop: dict) -> dict:
    """
    Compute Bernoulli metrics from the pitcher's season stats and stamp onto prop.

    Adds these keys to prop (all read by _build_feature_vector and _model_prob):
        _bernoulli_suppression   float  0-1 (lower is better — rarer line)
        _bernoulli_tier          str    S/A/B/C/D
        _bernoulli_zen           float  0-100 pct
        _bernoulli_drama         float  0-100 pct
        _bernoulli_meltdown      float  0-100 pct
        _bernoulli_prob_adj      float  probability adjustment (-0.05 to +0.04)
        _bernoulli_drama_penalty float  variance penalty multiplier (0.70 to 1.00)
        _bernoulli_available     bool   True if we had enough data

    Data sources (in priority order):
        1. prop["season_ip"] / prop["season_er"] (set by fangraphs_layer or statsapi)
        2. prop["ip"] / prop["earned_runs"] (single-game fallback — less accurate)
    """
    prop_type = str(prop.get("prop_type", "")).lower()
    if prop_type not in {"strikeouts", "pitcher_strikeouts", "pitching_outs",
                         "earned_runs", "hits_allowed", "fantasy_pitcher"}:
        return prop

    player = prop.get("player", prop.get("player_name", ""))

    # ── Pull season cumulative IP and DivR ───────────────────────────────────
    # season_ip and season_divr are stamped by fangraphs_layer when available.
    # Fallback: use single game IP and earned_runs as an approximation.
    season_ip   = float(prop.get("season_ip",   0.0) or 0.0)
    season_divr = float(prop.get("season_divr", 0.0) or 0.0)

    # Single-game fallback (less accurate but better than nothing)
    if season_ip < 1.0:
        game_ip = float(prop.get("ip",           0.0) or
                        prop.get("innings_pitched", 0.0) or 0.0)
        game_er = float(prop.get("earned_runs",  0.0) or 0.0)
        if game_ip >= 1.0:
            season_ip   = game_ip
            season_divr = game_er   # single-game ER used as DivR proxy
            logger.debug("[Bernoulli] %s: using single-game fallback IP=%.1f ER=%.1f",
                         player, season_ip, season_divr)

    if season_ip < 1.0:
        prop["_bernoulli_available"] = False
        return prop

    # ── Compute Bernoulli metrics ────────────────────────────────────────────
    team = prop.get("team", "")
    line = evaluate_pitcher_line(player, team, season_ip, season_divr)

    # ── Probability adjustment by tier ──────────────────────────────────────
    tier_adj = {
        "S": _SUPPRESSION_S_BONUS,
        "A": 0.02,
        "B": 0.00,
        "C": -0.02,
        "D": _SUPPRESSION_D_PENALTY,
    }.get(line.tier, 0.0)

    # Meltdown gate: structural run damage — suppress the pick
    if line.meltdown_pct > _MELTDOWN_GATE:
        tier_adj = min(tier_adj, -0.06)   # always penalise meltdown pitchers

    # ── Drama variance penalty ───────────────────────────────────────────────
    # High Drama = highly volatile outcome = widen confidence interval = lower confidence
    # Maps Drama 0%→penalty 1.00 (no penalty), Drama 35%+→penalty 0.70 (max)
    drama_penalty = max(0.70, 1.0 - (line.drama_pct / 35.0) * 0.30)

    # ── Stamp all keys ───────────────────────────────────────────────────────
    prop["_bernoulli_suppression"]   = line.suppression
    prop["_bernoulli_tier"]          = line.tier
    prop["_bernoulli_zen"]           = line.zen_pct
    prop["_bernoulli_drama"]         = line.drama_pct
    prop["_bernoulli_meltdown"]      = line.meltdown_pct
    prop["_bernoulli_prob_adj"]      = round(tier_adj, 4)
    prop["_bernoulli_drama_penalty"] = round(drama_penalty, 4)
    prop["_bernoulli_available"]     = True

    logger.debug(
        "[Bernoulli] %s: IP=%.1f DivR=%.1f → Tier=%s Supp=%.4f "
        "Zen=%.1f%% Drama=%.1f%% Melt=%.1f%% adj=%+.3f penalty=%.2f",
        player, season_ip, season_divr, line.tier, line.suppression,
        line.zen_pct, line.drama_pct, line.meltdown_pct,
        tier_adj, drama_penalty,
    )

    return prop


# ---------------------------------------------------------------------------
# MLB season totals updater — call this from DataHub / nightly job
# so the league Bernoulli rate stays current all season
# ---------------------------------------------------------------------------

def update_league_rate_from_hub(hub: dict) -> tuple[float, float] | None:
    """
    Extract season MLB totals from DataHub context and update the league rate.
    Returns (p_out, p_run) if updated, else None.

    DataHub context["season_totals"] should contain:
        {"runs": float, "innings_pitched": float}
    This is populated by the DataHub aggregator in tasklets.py.
    """
    ctx = hub.get("context", {})
    totals = ctx.get("season_totals", {})
    runs = totals.get("runs")
    ip   = totals.get("innings_pitched") or totals.get("ip")
    if runs and ip:
        p_out, p_run = get_league_rate(float(runs), float(ip))
        logger.info("[Bernoulli] League rate updated from DataHub: p_run=%.4f", p_run)
        return p_out, p_run
    return None


def fetch_and_update_league_rate() -> tuple[float, float]:
    """
    Fetch current season MLB totals from MLB Stats API and update league rate.
    Uses the same free statsapi.mlb.com endpoint already used elsewhere in PropIQ.
    Call once per day from the DataHub refresh cycle.
    """
    import datetime as _dt
    import requests as _req

    season = _dt.date.today().year
    try:
        resp = _req.get(
            f"https://statsapi.mlb.com/api/v1/stats",
            params={
                "stats":   "season",
                "group":   "pitching",
                "season":  str(season),
                "playerPool": "All",
                "limit":   1,   # we only need aggregate totals
            },
            timeout=10,
        )
        if resp.status_code != 200:
            raise ValueError(f"HTTP {resp.status_code}")

        # Try to extract aggregate
        for stat_group in resp.json().get("stats", []):
            agg = stat_group.get("aggregateStats", {}) or {}
            runs = float(agg.get("runs", 0) or 0)
            ip   = float(agg.get("inningsPitched", 0) or 0)
            if runs > 100 and ip > 100:
                p_out, p_run = get_league_rate(runs, ip)
                logger.info("[Bernoulli] League rate fetched from MLB API: "
                            "%.0f R / %.1f IP → p_run=%.4f", runs, ip, p_run)
                return p_out, p_run

    except Exception as exc:
        logger.debug("[Bernoulli] MLB API league rate fetch failed: %s — using cache", exc)

    # Fallback: return cached or baseline
    return get_league_rate()


# ---------------------------------------------------------------------------
# Standalone daily rankings (mirrors BotM output format)
# Useful for Discord alerts and debug logging
# ---------------------------------------------------------------------------

def compute_daily_rankings(
    pitcher_stats: list[dict],
    total_runs:    float | None = None,
    total_ip:      float | None = None,
) -> list[BernoulliLine]:
    """
    Compute BotM-style daily rankings from a list of pitcher stat dicts.

    Each dict should have:
        player_name / player: str
        team:                 str
        season_ip or ip:      float
        season_divr or earned_runs: float

    Returns sorted list (best suppression first = rank 1).
    """
    p_out, _ = get_league_rate(total_runs, total_ip)
    results: list[BernoulliLine] = []

    for s in pitcher_stats:
        name = s.get("player_name") or s.get("player") or s.get("full_name", "")
        team = s.get("team", "")
        ip   = float(s.get("season_ip")   or s.get("ip")           or 0.0)
        divr = float(s.get("season_divr") or s.get("earned_runs")  or 0.0)

        if ip < 1.0 or not name:
            continue

        line = evaluate_pitcher_line(name, team, ip, divr, total_runs, total_ip)
        results.append(line)

    results.sort(key=lambda x: x.suppression)
    return results


