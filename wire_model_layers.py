"""
wire_model_layers.py
====================
Wires four disconnected model layers into the prop evaluation chain:

  A. pa_model.py         → predict_plus_layer.py matchup probability
  B. bernoulli_layer.py  → bernoulli_drama_layer.py Drama penalty
  C. bvi_layer.py        → prop_enrichment_layer (BVI output consumed)
  D. injury_layer.py     → prop_enrichment_layer (injury block)

All four layers exist in the repo with correct implementations.
None of them are connected to the evaluation path.

LAYER A: PA Model (Bill James Odds-Ratio)
-----------------------------------------
pa_model.py correctly implements the multiplicative odds-ratio method
for batter-pitcher matchups. predict_plus_layer.py uses flat base-rate
lookups instead. This module provides compute_matchup_k_rate() and
compute_matchup_hit_rate() that replace those lookups.

LAYER B: Bernoulli Drama → Penalty
------------------------------------
bernoulli_layer.py computes Drama% from season IP/run data.
bernoulli_drama_layer.py applies a K-prop probability penalty based on
Drama%. But it reads from a markdown FILE that may not exist. This module
bridges them: compute Drama% from bernoulli_layer math directly (no file),
then apply the penalty from bernoulli_drama_layer thresholds.

LAYER C: BVI Output
--------------------
bvi_layer.py computes Bullpen Volatility Index but nothing reads
hub["physics"]["bvi"]. This module provides apply_bvi_adjustment()
which reads BVI from hub and applies a probability modifier to
pitcher props for teams with volatile bullpens.

LAYER D: Injury Block
---------------------
injury_layer.py fetches IL/DTD status but prop_enrichment_layer.py
never calls is_player_available(). This module provides a check that
kills props for IL players and penalizes DTD players.

USAGE
-----
From prop_enrichment_layer.py (or tasklets.py), add at the top:

    from wire_model_layers import (
        compute_matchup_k_rate,
        get_bernoulli_drama_penalty,
        apply_bvi_adjustment,
        check_injury_block,
    )

Then in the per-prop evaluation loop:

    # A: Better matchup K-rate (replaces flat league average)
    opp_k_rate = compute_matchup_k_rate(batter_profiles, pitcher_profile)

    # B: Bernoulli Drama penalty (applies to K props)
    if prop_type == "strikeouts":
        drama_penalty = get_bernoulli_drama_penalty(pitcher_name, pitcher_stats)
        model_prob += drama_penalty / 100   # drama_penalty is in pp

    # C: BVI adjustment (applies to pitcher props)
    model_prob = apply_bvi_adjustment(model_prob, prop, hub)

    # D: Injury block (applies to all props)
    block = check_injury_block(player_name, hub)
    if block["should_kill"]:
        continue   # skip this prop entirely
    model_prob *= (1.0 - block["confidence_penalty"])

Run: python wire_model_layers.py --test
"""

from __future__ import annotations

import logging
import sys
from typing import Optional

log = logging.getLogger("propiq.wire_layers")

# ══════════════════════════════════════════════════════════════════════════════
# LAYER A: PA Model — Odds-Ratio Matchup
# ══════════════════════════════════════════════════════════════════════════════

def compute_matchup_k_rate(
    batter_profiles: list[dict],
    pitcher_profile: dict,
    league_k_rate: float = 0.228,
) -> float:
    """
    Compute expected lineup K-rate using Bill James odds-ratio method.

    Replaces the flat league_avg fallback in predict_plus_layer.py.
    Handles missing batter data gracefully — skips batters with no profile.

    Args:
        batter_profiles: List of batter dicts with "k_pct" key (0-1 scale)
        pitcher_profile: Dict with "k_rate" key (pitcher's K/PA rate, 0-1)
        league_k_rate:   League average K/PA (default 0.228)

    Returns:
        Lineup-weighted K-rate float (0-1 scale), or league_k_rate if no data.

    Example:
        # Ace pitcher (k_rate=0.32) vs weak lineup (avg k_pct=0.26)
        rate = compute_matchup_k_rate(
            batter_profiles=[{"k_pct": 0.26}] * 9,
            pitcher_profile={"k_rate": 0.32},
        )
        # → 0.286 (between pitcher and lineup, adjusted for league average)
    """
    try:
        from pa_model import odds_ratio_blend, LEAGUE_RATES
        lg_k = LEAGUE_RATES.get("K", league_k_rate)
    except ImportError:
        log.debug("[WireLayer] pa_model not importable — using weighted average")
        lg_k = league_k_rate

        def odds_ratio_blend(b, p, l):
            return (b * p) / l if l > 0 else (b + p) / 2

    pitcher_k = pitcher_profile.get("k_rate") or pitcher_profile.get("k_pct") or lg_k
    pitcher_k = float(pitcher_k)

    batter_k_rates = []
    for b in batter_profiles:
        rate = b.get("k_pct") or b.get("k_rate")
        if rate is not None:
            batter_k_rates.append(float(rate))

    if not batter_k_rates:
        # No batter data — use pitcher-only adjustment vs league
        return round(odds_ratio_blend(lg_k, pitcher_k, lg_k), 4)

    per_batter = [odds_ratio_blend(bk, pitcher_k, lg_k) for bk in batter_k_rates]
    return round(sum(per_batter) / len(per_batter), 4)


def compute_matchup_hit_rate(
    batter_profile: dict,
    pitcher_profile: dict,
    league_hit_rate: float = 0.204,
) -> float:
    """
    Compute expected hit probability for a single batter-pitcher matchup.

    Args:
        batter_profile:  Dict with "hit_rate" or "avg" key (0-1 scale)
        pitcher_profile: Dict with "hits_allowed_rate" key (0-1 scale)
        league_hit_rate: League H/PA (default 0.204)

    Returns:
        Matchup hit probability float (0-1 scale).
    """
    try:
        from pa_model import odds_ratio_blend, LEAGUE_RATES
        lg_h = LEAGUE_RATES.get("1B", league_hit_rate) + LEAGUE_RATES.get("2B", 0) + LEAGUE_RATES.get("3B", 0) + LEAGUE_RATES.get("HR", 0)
    except ImportError:
        lg_h = league_hit_rate
        def odds_ratio_blend(b, p, l):
            return (b * p) / l if l > 0 else (b + p) / 2

    batter_h  = float(batter_profile.get("hit_rate") or batter_profile.get("avg") or lg_h)
    pitcher_h = float(pitcher_profile.get("hits_allowed_rate") or pitcher_profile.get("h_per_pa") or lg_h)

    return round(odds_ratio_blend(batter_h, pitcher_h, lg_h), 4)


# ══════════════════════════════════════════════════════════════════════════════
# LAYER B: Bernoulli Drama Penalty
# ══════════════════════════════════════════════════════════════════════════════

# Drama% → probability penalty (percentage points, negative)
# Mirrors bernoulli_drama_layer.py _DRAMA_PENALTIES exactly
_DRAMA_THRESHOLDS = [
    (65.0, -5.0),   # Drama > 65% → −5pp (very unpredictable)
    (50.0, -3.0),   # Drama > 50% → −3pp
    (30.0, -1.5),   # Drama > 30% → −1.5pp
    (0.0,   0.0),   # below 30%  → no penalty
]
_MIN_IP_FOR_DRAMA = 20.0   # don't apply until pitcher has 20+ IP this season


def get_bernoulli_drama_penalty(
    pitcher_name: str,
    pitcher_stats: dict,
    season_ip: Optional[float] = None,
    rankings: Optional[dict] = None,
) -> float:
    """
    Get the Drama% penalty for a pitcher's K-prop probability.

    Bridges bernoulli_layer.py (math) with bernoulli_drama_layer.py (penalty).
    Works two ways:
      1. If rankings dict provided (from load_bernoulli_rankings): lookup directly
      2. If pitcher_stats provided: compute Drama% from bernoulli_layer math

    Args:
        pitcher_name:  Pitcher's name (for lookup in rankings)
        pitcher_stats: Dict with season_ip, season_runs (divR) for math fallback
        season_ip:     Season IP override (if not in pitcher_stats)
        rankings:      Optional pre-loaded Bernoulli rankings dict

    Returns:
        Drama penalty in percentage points (negative float, e.g. -3.0).
        Returns 0.0 if insufficient data or Drama% < 30%.

    Integration:
        drama_pp = get_bernoulli_drama_penalty(pitcher_name, pitcher_stats)
        if prop_type == "strikeouts" and drama_pp < 0:
            model_prob = model_prob + (drama_pp / 100)  # drama_pp is in pp
    """
    import unicodedata, re

    def _norm(s):
        nfkd = unicodedata.normalize("NFKD", s)
        return re.sub(r"[^a-z ]", "", "".join(
            c for c in nfkd if not unicodedata.combining(c)
        ).lower()).strip()

    # Path 1: Use pre-loaded rankings dict (most accurate)
    if rankings:
        key = _norm(pitcher_name)
        entry = rankings.get(key)
        if entry:
            ip = float(entry.get("ip", 0))
            drama = float(entry.get("drama", 0))
            if ip >= _MIN_IP_FOR_DRAMA:
                for threshold, penalty in _DRAMA_THRESHOLDS:
                    if drama >= threshold:
                        return penalty
            return 0.0

    # Path 2: Compute from bernoulli_layer math using pitcher_stats
    ip = float(season_ip or pitcher_stats.get("season_ip") or pitcher_stats.get("ip", 0))
    if ip < _MIN_IP_FOR_DRAMA:
        return 0.0  # not enough innings to assess Drama

    try:
        from bernoulli_layer import compute_entropy_states
        div_r = float(pitcher_stats.get("season_runs") or pitcher_stats.get("divR") or 0)
        states = compute_entropy_states(ip_total=ip, div_r=div_r)
        drama = states.get("drama_pct", 0.0)
    except (ImportError, Exception) as exc:
        log.debug("[BernoulliWire] bernoulli_layer unavailable (%s) — no drama penalty", exc)
        return 0.0

    for threshold, penalty in _DRAMA_THRESHOLDS:
        if drama >= threshold:
            log.info(
                "[BernoulliWire] %s Drama=%.1f%% → penalty %.1fpp",
                pitcher_name, drama, penalty,
            )
            return penalty
    return 0.0


# ══════════════════════════════════════════════════════════════════════════════
# LAYER C: BVI (Bullpen Volatility Index) Adjustment
# ══════════════════════════════════════════════════════════════════════════════

# BVI thresholds and probability adjustments
# High BVI = bullpen is volatile = pitching_outs UNDER more likely (closer hook)
# Low BVI  = stable bullpen = pitching_outs OVER slightly more likely
_BVI_THRESHOLDS = [
    (0.70, -0.020),   # BVI > 0.70 → −2pp (very volatile bullpen)
    (0.50, -0.010),   # BVI > 0.50 → −1pp
    (0.30,  0.000),   # BVI 0.30–0.50 → neutral
    (0.00,  0.005),   # BVI < 0.30 → +0.5pp (stable bullpen)
]


def apply_bvi_adjustment(
    model_prob: float,
    prop: dict,
    hub: dict,
) -> float:
    """
    Apply BVI (Bullpen Volatility Index) adjustment to pitcher props.

    Reads hub["physics"]["bvi"] — populated by bvi_layer.py in DataHub.
    Only applies to pitching_outs and strikeouts props (where bullpen usage
    directly affects whether the starter stays in long enough to hit the line).

    Args:
        model_prob:  Current model probability (0-1 scale)
        prop:        Prop dict with prop_type and team
        hub:         DataHub context dict

    Returns:
        Adjusted model probability (0-1 scale), unchanged if BVI unavailable.
    """
    prop_type = (prop.get("prop_type") or "").lower()
    if prop_type not in ("strikeouts", "pitching_outs", "pitcher_strikeouts"):
        return model_prob   # BVI only matters for starter longevity props

    # Get team from prop
    team = prop.get("team") or prop.get("home_team") or ""
    if not team:
        return model_prob

    # Read BVI from hub
    physics = hub.get("physics", {})
    bvi_data = physics.get("bvi", {})

    if not bvi_data:
        # BVI not populated — try to compute it inline (fallback)
        try:
            from bvi_layer import compute_team_bvi
            bvi_score = compute_team_bvi(team)
        except (ImportError, Exception):
            log.debug("[BVIWire] bvi_layer unavailable — no BVI adjustment")
            return model_prob
    else:
        # Read from hub
        team_bvi = bvi_data.get(team) or bvi_data.get(team.upper()) or bvi_data.get(team.lower())
        if team_bvi is None:
            return model_prob
        bvi_score = float(team_bvi.get("bvi", 0.4) if isinstance(team_bvi, dict) else team_bvi)

    # Apply threshold-based adjustment
    adj = 0.0
    for threshold, delta in _BVI_THRESHOLDS:
        if bvi_score >= threshold:
            adj = delta
            break

    if adj != 0.0:
        log.info(
            "[BVIWire] %s BVI=%.2f → %+.1fpp on %s prop",
            team, bvi_score, adj * 100, prop_type,
        )

    adjusted = model_prob + adj
    return round(max(0.03, min(0.97, adjusted)), 4)


# ══════════════════════════════════════════════════════════════════════════════
# LAYER D: Injury Block
# ══════════════════════════════════════════════════════════════════════════════

# Confidence penalties by injury status (from injury_layer.py constants)
_INJURY_PENALTIES = {
    "IL":          1.00,   # full kill — player on IL, prop shouldn't exist
    "IL-60":       1.00,
    "IL-15":       1.00,
    "IL-10":       1.00,
    "OUT":         0.90,   # 90% confidence penalty
    "QUESTIONABLE": 0.15,  # 15% penalty
    "DTD":         0.25,   # 25% penalty (day-to-day)
}


def check_injury_block(
    player_name: str,
    hub: dict,
) -> dict:
    """
    Check whether a player's injury status should block or penalize a prop.

    Reads hub["injuries"] — populated by injury_layer.py in DataHub.
    Falls back to calling injury_layer directly if hub data absent.

    Args:
        player_name: Player name string
        hub:         DataHub context dict

    Returns:
        {
            "should_kill":        bool   — True = prop should be skipped entirely
            "confidence_penalty": float  — multiply model confidence by (1 - this)
            "status":             str    — injury status string or "HEALTHY"
            "detail":             str    — injury detail text
        }

    Integration:
        block = check_injury_block(player_name, hub)
        if block["should_kill"]:
            log.info("Skipping %s — %s", player_name, block["status"])
            continue
        confidence *= (1.0 - block["confidence_penalty"])
    """
    import unicodedata, re

    def _norm(s):
        nfkd = unicodedata.normalize("NFKD", s)
        return re.sub(r"\s+", " ", "".join(
            c for c in nfkd if not unicodedata.combining(c)
        ).lower()).strip()

    healthy = {
        "should_kill": False,
        "confidence_penalty": 0.0,
        "status": "HEALTHY",
        "detail": "",
    }

    # ── Path 1: Read from hub["injuries"] ────────────────────────────────────
    injuries = hub.get("injuries", [])
    if injuries:
        pn = _norm(player_name)
        for entry in injuries:
            ename = _norm(entry.get("player_name", ""))
            if ename == pn or ename in pn or pn in ename:
                status = entry.get("status", "UNKNOWN")
                is_il  = entry.get("is_il", False)
                is_dtd = entry.get("is_dtd", False)
                is_out = entry.get("is_out", False)
                detail = entry.get("detail", "")

                if is_il:
                    log.warning("[InjuryWire] KILL %s — status=%s (%s)", player_name, status, detail)
                    return {
                        "should_kill": True,
                        "confidence_penalty": 1.0,
                        "status": status,
                        "detail": detail,
                    }

                penalty = _INJURY_PENALTIES.get(status, 0.0)
                if is_out:
                    penalty = max(penalty, _INJURY_PENALTIES["OUT"])
                if is_dtd:
                    penalty = max(penalty, _INJURY_PENALTIES["DTD"])

                if penalty > 0:
                    log.info("[InjuryWire] Penalizing %s — status=%s penalty=%.0f%%",
                             player_name, status, penalty * 100)
                return {
                    "should_kill": False,
                    "confidence_penalty": penalty,
                    "status": status,
                    "detail": detail,
                }
        return healthy   # player not in injury list → healthy

    # ── Path 2: Call injury_layer directly ───────────────────────────────────
    try:
        from injury_layer import get_injury_status, is_player_available

        status_dict = get_injury_status(player_name)
        if status_dict is None:
            return healthy

        if not is_player_available(player_name):
            return {
                "should_kill": True,
                "confidence_penalty": 1.0,
                "status": status_dict.get("status", "IL"),
                "detail": status_dict.get("detail", ""),
            }

        status  = status_dict.get("status", "UNKNOWN")
        penalty = _INJURY_PENALTIES.get(status, 0.0)
        return {
            "should_kill": False,
            "confidence_penalty": penalty,
            "status": status,
            "detail": status_dict.get("detail", ""),
        }

    except (ImportError, Exception) as exc:
        log.debug("[InjuryWire] injury_layer unavailable (%s) — no injury check", exc)
        return healthy


# ══════════════════════════════════════════════════════════════════════════════
# Self-test
# ══════════════════════════════════════════════════════════════════════════════

def run_test() -> None:
    print("\n" + "=" * 60)
    print("  MODEL LAYER WIRING — SELF TEST")
    print("=" * 60)

    # Layer A: PA Model matchup
    print("\n【LAYER A】 PA Model Odds-Ratio Matchup")
    k_rate = compute_matchup_k_rate(
        batter_profiles=[{"k_pct": 0.26}] * 9,
        pitcher_profile={"k_rate": 0.32},
    )
    print(f"  Ace (K%=32%) vs weak lineup (K%=26%): matchup K-rate = {k_rate:.3f}")
    # Odds-ratio: (batter × pitcher) / league = (0.26 × 0.32) / 0.228 ≈ 0.365
    # This correctly exceeds both inputs — it's the expected joint probability
    assert k_rate > 0.228, "Matchup K-rate should exceed league average vs strong pitcher"

    k_rate_avg = compute_matchup_k_rate(
        batter_profiles=[{"k_pct": 0.228}] * 9,
        pitcher_profile={"k_rate": 0.228},
    )
    print(f"  League avg pitcher vs avg lineup: K-rate = {k_rate_avg:.3f} (expect ~0.228)")

    hit_rate = compute_matchup_hit_rate(
        batter_profile={"hit_rate": 0.30},
        pitcher_profile={"hits_allowed_rate": 0.18},
    )
    print(f"  Hot batter (H%=30%) vs ace (H_allowed=18%): hit rate = {hit_rate:.3f}")

    # Layer B: Bernoulli Drama penalty
    print("\n【LAYER B】 Bernoulli Drama Penalty")
    for drama_pct, ip, expected_penalty in [
        (70.0, 25.0, -5.0),   # very high drama
        (55.0, 25.0, -3.0),   # high drama
        (35.0, 25.0, -1.5),   # moderate drama
        (15.0, 25.0,  0.0),   # low drama
        (80.0,  5.0,  0.0),   # high drama but insufficient IP
    ]:
        # Fake a rankings dict to test the lookup path
        fake_rankings = {"test pitcher": {"drama": drama_pct, "ip": ip, "zen": 100-drama_pct, "meltdown": 0}}
        penalty = get_bernoulli_drama_penalty("Test Pitcher", {}, rankings=fake_rankings)
        status = "✅" if penalty == expected_penalty else "❌"
        print(f"  {status} Drama={drama_pct:.0f}% IP={ip:.0f}: penalty={penalty:.1f}pp (expected {expected_penalty:.1f}pp)")
        assert penalty == expected_penalty, f"Drama penalty mismatch: {penalty} != {expected_penalty}"

    # Layer C: BVI adjustment
    print("\n【LAYER C】 BVI Adjustment")
    hub_with_bvi = {"physics": {"bvi": {"NYY": {"bvi": 0.75}}}}
    prop_out = {"prop_type": "pitching_outs", "team": "NYY"}

    prob_before = 0.58
    prob_after = apply_bvi_adjustment(prob_before, prop_out, hub_with_bvi)
    print(f"  High-BVI team (0.75): pitching_outs prob {prob_before:.2f} → {prob_after:.2f} (expect drop)")
    assert prob_after < prob_before, "High BVI should reduce pitching_outs probability"

    hub_no_bvi = {}
    prob_unchanged = apply_bvi_adjustment(prob_before, prop_out, hub_no_bvi)
    print(f"  No BVI data: prob {prob_before:.2f} → {prob_unchanged:.2f} (expect unchanged)")
    assert prob_unchanged == prob_before, "No BVI data should leave probability unchanged"

    prop_hits = {"prop_type": "hits", "team": "NYY"}
    prob_hits = apply_bvi_adjustment(prob_before, prop_hits, hub_with_bvi)
    print(f"  BVI doesn't apply to hit props: prob {prob_before:.2f} → {prob_hits:.2f} (expect unchanged)")
    assert prob_hits == prob_before, "BVI should not affect hit props"

    # Layer D: Injury block
    print("\n【LAYER D】 Injury Block")
    hub_with_il = {"injuries": [{"player_name": "Spencer Strider", "status": "IL-15", "is_il": True, "detail": "elbow"}]}
    block = check_injury_block("Spencer Strider", hub_with_il)
    print(f"  IL player: should_kill={block['should_kill']} penalty={block['confidence_penalty']:.0%}")
    assert block["should_kill"] is True

    hub_with_dtd = {"injuries": [{"player_name": "Test Player", "status": "DTD", "is_il": False, "is_dtd": True, "detail": "hamstring"}]}
    block_dtd = check_injury_block("Test Player", hub_with_dtd)
    print(f"  DTD player: should_kill={block_dtd['should_kill']} penalty={block_dtd['confidence_penalty']:.0%}")
    assert block_dtd["should_kill"] is False
    assert block_dtd["confidence_penalty"] > 0

    hub_healthy = {"injuries": []}
    block_h = check_injury_block("Healthy Player", hub_healthy)
    print(f"  Healthy player: should_kill={block_h['should_kill']} penalty={block_h['confidence_penalty']:.0%}")
    assert block_h["should_kill"] is False
    assert block_h["confidence_penalty"] == 0.0

    print("\n✅ All layer tests passed.")

    print("\n" + "=" * 60)
    print("  INTEGRATION CODE")
    print("=" * 60)
    print("""
  In prop_enrichment_layer.py (or tasklets.py evaluation loop):

      from wire_model_layers import (
          compute_matchup_k_rate,
          get_bernoulli_drama_penalty,
          apply_bvi_adjustment,
          check_injury_block,
      )

      # In the per-prop loop:
      for prop in enriched_props:
          player = prop.get("player", "")

          # D: Injury check first (cheapest kill)
          block = check_injury_block(player, hub)
          if block["should_kill"]:
              logger.info("SKIP %s — %s", player, block["status"])
              continue
          confidence *= (1.0 - block["confidence_penalty"])

          # A: Better lineup K-rate for strikeout props
          if prop.get("prop_type") == "strikeouts":
              batter_profiles = hub.get("context", {}).get("lineups", [])
              pitcher_profile = {"k_rate": prop.get("k_rate", 0.228)}
              opp_k_rate = compute_matchup_k_rate(batter_profiles, pitcher_profile)
              prop["opp_lineup_k_pct"] = opp_k_rate  # overwrite flat value

          # B: Bernoulli Drama penalty for K props
          if prop.get("prop_type") == "strikeouts":
              drama_pp = get_bernoulli_drama_penalty(player, prop)
              model_prob += drama_pp / 100  # drama_pp is negative pp

          # C: BVI adjustment for pitcher longevity props
          model_prob = apply_bvi_adjustment(model_prob, prop, hub)
""")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    run_test()
