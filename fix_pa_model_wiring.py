"""
fix_pa_model_wiring.py
=======================
Wires pa_model.py into prop_enrichment_layer.py.

THE SITUATION
-------------
pa_model.py implements the Bill James odds-ratio matchup model and
has two relevant public functions:

    prop_matchup_prob(prop_type, batter_profile, pitcher_profile, line, side)
        → float: P(Over) for any prop type

    build_batter_profile(mlb_stats) → dict
    build_pitcher_profile(mlb_stats) → dict

pa_model.py's own docstring says it is meant to replace the flat
base-rate lookups in predict_plus_layer.py and nsfi_layer.py.
It has never been called anywhere in the codebase.

THE TWO INTEGRATION POINTS
--------------------------

POINT 1 — opp_k_pct in prop_enrichment_layer.py
   Currently the opposing lineup K-rate defaults to a flat 0.227.
   pa_model computes the correct batter×pitcher joint K-rate per PA.
   This replaces the flat number with a matchup-adjusted rate.

POINT 2 — hit prop probability in prop_enrichment_layer.py
   For hits / total_bases props, pa_model.prop_matchup_prob() returns
   a matchup-aware P(≥1 hit) that accounts for batter xBA, pitcher
   hits-allowed rate, and park factors — instead of treating every
   batter-pitcher pairing as league average.

WHERE TO INSERT IN prop_enrichment_layer.py
--------------------------------------------
Find the function that enriches each prop. It's the one that sets
fields like "_opp_avg_k_pct", "_bayesian_nudge", "_form_adj", etc.
It loops over props and attaches signal fields.

POINT 1 — find where opp_k_pct / _opp_avg_k_pct is set.
It will look like:
    prop["_opp_avg_k_pct"] = 0.227   # or some lookup

Replace with the pa_model call below (see PA_MODEL_OPP_K_SNIPPET).

POINT 2 — find where hit-prop base probability is set.
It will look like something that sets "_hit_matchup_prob" or
just leaves hit props at a base rate.

Insert the PA_MODEL_HIT_SNIPPET after any existing hit-rate computation.

HOW TO APPLY
------------
Option A — automatic (run from PropIQ repo root):
    python fix_pa_model_wiring.py

Option B — manual (paste snippets from PA_MODEL_OPP_K_SNIPPET and
    PA_MODEL_HIT_SNIPPET into prop_enrichment_layer.py at the
    two points described above).

VERIFICATION
------------
python fix_pa_model_wiring.py --verify
After deployment: check logs for "[pa_model]" entries.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [PA-MODEL] %(message)s")
log = logging.getLogger(__name__)

ENRICHMENT = Path("prop_enrichment_layer.py")

# ── Snippet 1: opp_k_pct via pa_model ────────────────────────────────────────
# Replace wherever opp_k_pct is set to a flat number.
# Paste this function into prop_enrichment_layer.py (top-level, after imports).

PA_MODEL_OPP_K_SNIPPET = '''\
# ── pa_model: odds-ratio lineup K-rate ──────────────────────────────────────
def _pa_model_opp_k_rate(
    batter_profiles: list[dict],
    pitcher_profile: dict,
) -> float:
    """
    Compute opposing lineup K-rate using the Bill James odds-ratio method.

    Replaces the flat 0.227 league-average default in prop_enrichment_layer.
    Each batter's K-rate and the pitcher's K-rate are combined per-PA:
        P(K) = batter_K_rate × pitcher_K_rate / league_K_rate

    Then averaged across the lineup.

    Args:
        batter_profiles: list of batter dicts with "K" key (per-PA K rate 0-1)
                         Accepts either build_batter_profile() output or
                         fangraphs_layer dicts with "k_pct" key.
        pitcher_profile: dict with "K" key (pitcher K-rate allowed per BF)
                         Accepts build_pitcher_profile() output or
                         fangraphs_layer dict with "k_pct" key.

    Returns:
        float: lineup-average K-rate (0-1 scale), or 0.227 if no data.
    """
    try:
        from pa_model import odds_ratio_blend, LEAGUE_RATES  # noqa: PLC0415
    except ImportError:
        return 0.227

    lg_k = LEAGUE_RATES["K"]  # 0.228

    # Resolve pitcher K-rate (handle both pa_model and fangraphs_layer formats)
    pit_k = float(
        pitcher_profile.get("K")
        or pitcher_profile.get("k_pct")
        or pitcher_profile.get("k_rate")
        or lg_k
    )
    # Scale from pct to fraction if needed
    if pit_k > 1.0:
        pit_k /= 100.0
    pit_k = max(0.05, min(0.55, pit_k))

    batter_k_rates = []
    for b in batter_profiles:
        bk = float(
            b.get("K")
            or b.get("k_pct")
            or b.get("k_rate")
            or lg_k
        )
        if bk > 1.0:
            bk /= 100.0
        batter_k_rates.append(max(0.05, min(0.55, bk)))

    if not batter_k_rates:
        return round(odds_ratio_blend(lg_k, pit_k, lg_k), 4)

    matchup_rates = [
        odds_ratio_blend(bk, pit_k, lg_k)
        for bk in batter_k_rates
    ]
    return round(sum(matchup_rates) / len(matchup_rates), 4)
'''

# ── Usage in enrichment loop (Point 1) ───────────────────────────────────────
# Find the line that sets opp_k_rate or _opp_avg_k_pct to a flat value and
# replace it with:

POINT1_USAGE = """\
        # pa_model: odds-ratio lineup K-rate (replaces flat 0.227 default)
        _batter_profiles = hub.get("context", {}).get("lineups", [])
        _pitcher_k_profile = {
            "K": float(prop.get("k_rate") or prop.get("fg_kpct", 0) / 100 or 0.228),
        }
        prop["_opp_avg_k_pct"] = _pa_model_opp_k_rate(
            batter_profiles=_batter_profiles,
            pitcher_profile=_pitcher_k_profile,
        )
        prop["opp_k_rate"] = prop["_opp_avg_k_pct"]
"""

# ── Snippet 2: hit-prop matchup probability ───────────────────────────────────

PA_MODEL_HIT_SNIPPET = '''\
# ── pa_model: hit-prop matchup probability ───────────────────────────────────
def _pa_model_hit_prob(
    prop: dict,
    pitcher_profile: dict,
    line: float = 0.5,
    side: str = "Over",
) -> float | None:
    """
    Compute P(batter ≥ line hits) using the Bill James odds-ratio PA model.

    Replaces the flat base-rate lookup for hit and total_bases props.
    Returns None if pa_model unavailable (caller uses existing probability).

    Args:
        prop:            Enriched batter prop dict. Reads these keys:
                           sv_xba, sv_xwoba, fg_kpct, fg_bbpct (Statcast/FG)
                           Or falls back to pa_model LEAGUE_RATES.
        pitcher_profile: Enriched pitcher dict (same keys, pitcher rates).
        line:            Prop line (e.g. 0.5 for hits, 1.5 for total_bases)
        side:            "Over" or "Under"

    Returns:
        float [0.01, 0.99] or None if pa_model import fails.
    """
    try:
        from pa_model import (  # noqa: PLC0415
            prop_matchup_prob,
            build_batter_profile,
            build_pitcher_profile,
            LEAGUE_RATES,
        )
    except ImportError:
        return None

    def _safe(d: dict, *keys, default: float = 0.0) -> float:
        for k in keys:
            v = d.get(k)
            if v is not None:
                try:
                    f = float(v)
                    return f / 100.0 if f > 1.0 else f
                except (TypeError, ValueError):
                    pass
        return default

    # Build batter profile from Statcast/FanGraphs keys on the prop dict
    lg = LEAGUE_RATES
    batter_profile = {
        "K":   _safe(prop, "fg_kpct",  "k_pct",   default=lg["K"]),
        "BB":  _safe(prop, "fg_bbpct", "bb_pct",  default=lg["BB"]),
        "HBP": lg["HBP"],
        "HR":  _safe(prop, "sv_brl_pct", "hr_rate", default=lg["HR"]),
        "3B":  lg["3B"],
        "2B":  _safe(prop, "fg_2b_rate", default=lg["2B"]),
        "1B":  _safe(prop, "sv_xba",  "hit_rate", default=lg["1B"]),
        "OUT": max(0.01, 1.0 - _safe(prop, "fg_kpct", "k_pct", default=lg["K"])
                          - _safe(prop, "fg_bbpct", "bb_pct", default=lg["BB"])
                          - _safe(prop, "sv_xba", default=lg["1B"] + lg["2B"] + lg["HR"])),
    }

    # Build pitcher profile from pitcher_profile dict
    pit_k  = _safe(pitcher_profile, "fg_kpct",  "k_rate",  default=lg["K"])
    pit_bb = _safe(pitcher_profile, "fg_bbpct", "bb_rate", default=lg["BB"])
    pit_hr = _safe(pitcher_profile, "hr_per_bf", default=lg["HR"])
    pit_h  = _safe(pitcher_profile, "whip",      default=1.30) / 3.0 * (1 - pit_bb)
    pitcher_profile_built = {
        "K":   min(0.40, pit_k),
        "BB":  min(0.20, pit_bb),
        "HBP": lg["HBP"],
        "HR":  min(0.08, pit_hr),
        "3B":  lg["3B"],
        "2B":  lg["2B"],
        "1B":  max(0.01, pit_h - pit_hr - lg["2B"] - lg["3B"]),
        "OUT": max(0.01, 1.0 - pit_k - pit_bb - lg["HBP"] - pit_hr - pit_h),
    }

    # Park factor for hits (from prop if attached by dome/park enrichment)
    park_k = float(prop.get("_park_k_factor", 1.0) or 1.0)
    park_factors = {"1B": float(prop.get("_park_hit_factor", 1.0) or 1.0),
                    "k": park_k}

    prop_type = prop.get("prop_type", "hits")
    result = prop_matchup_prob(
        prop_type=prop_type,
        batter_profile=batter_profile,
        pitcher_profile=pitcher_profile_built,
        line=float(line),
        side=side,
        park_factors=park_factors,
    )
    return result
'''

# ── Usage in enrichment loop (Point 2) ───────────────────────────────────────
POINT2_USAGE = """\
        # pa_model: hit-prop matchup probability (replaces flat base rate)
        if prop_type in ("hits", "total_bases", "hits_runs_rbis"):
            _pit_profile_for_pa = {
                "fg_kpct":  prop.get("opp_k_rate", 0.228),
                "fg_bbpct": prop.get("opp_bb_rate", 0.083),
                "whip":     prop.get("opp_whip", 1.30),
                "hr_per_bf": prop.get("opp_hr_rate", 0.033),
            }
            _side = prop.get("side", "Over")
            _line = float(prop.get("line", 0.5))
            _pa_hit_p = _pa_model_hit_prob(prop, _pit_profile_for_pa, _line, _side)
            if _pa_hit_p is not None:
                prop["_pa_model_hit_prob"] = _pa_hit_p
                # This value is available to the XGBoost hit blend and
                # to _compute_prop_prob as a secondary signal.
                # It does NOT override raw_p directly — agents decide weighting.
"""

# ── Patch functions ───────────────────────────────────────────────────────────

# Anchor for Point 1: find where flat opp_k_pct is set
_OPP_K_ANCHORS = [
    'prop.get("opp_k_rate",\n                                         prop.get("_opp_team_k_pct", 0.227))',
    '"_opp_avg_k_pct"',
    'opp_k_rate", 0.227',
    'opp_k_rate", 0.228',
]


def _find_anchor(content: str, anchors: list[str]) -> str | None:
    for a in anchors:
        if a in content:
            return a
    return None


def apply_point1(content: str) -> tuple[str, bool]:
    """Add _pa_model_opp_k_rate function to prop_enrichment_layer.py."""
    if "_pa_model_opp_k_rate" in content:
        log.info("Point 1 (_pa_model_opp_k_rate) already present — skipping.")
        return content, True

    # Insert the function definition before the first def in the module
    # Find a safe insertion point after imports
    insert_after = "logger = logging.getLogger"
    idx = content.find(insert_after)
    if idx == -1:
        insert_after = "from typing import"
        idx = content.find(insert_after)
    if idx == -1:
        log.warning("Point 1: could not find insertion anchor — add manually.")
        return content, False

    # Find end of that line
    eol = content.find("\n", idx)
    new_content = content[:eol + 1] + "\n\n" + PA_MODEL_OPP_K_SNIPPET + content[eol + 1:]
    log.info("Point 1: _pa_model_opp_k_rate function inserted.")
    return new_content, True


def apply_point2(content: str) -> tuple[str, bool]:
    """Add _pa_model_hit_prob function to prop_enrichment_layer.py."""
    if "_pa_model_hit_prob" in content:
        log.info("Point 2 (_pa_model_hit_prob) already present — skipping.")
        return content, True

    # Insert after _pa_model_opp_k_rate if present, else after logger line
    anchor = "_pa_model_opp_k_rate"
    idx = content.find(anchor)
    if idx == -1:
        insert_after = "logger = logging.getLogger"
        idx = content.find(insert_after)

    if idx == -1:
        log.warning("Point 2: could not find insertion anchor — add manually.")
        return content, False

    # Find a blank line after the anchor to place the new function
    eol = content.find("\n\n", idx)
    if eol == -1:
        eol = content.find("\n", idx)
    new_content = content[:eol + 1] + "\n" + PA_MODEL_HIT_SNIPPET + content[eol + 1:]
    log.info("Point 2: _pa_model_hit_prob function inserted.")
    return new_content, True


def apply() -> None:
    if not ENRICHMENT.exists():
        log.error("prop_enrichment_layer.py not found — run from PropIQ repo root.")
        return

    content = ENRICHMENT.read_text(encoding="utf-8")
    original = content

    content, ok1 = apply_point1(content)
    content, ok2 = apply_point2(content)

    if content != original:
        ENRICHMENT.write_text(content, encoding="utf-8")
        log.info("prop_enrichment_layer.py updated.")
    else:
        log.info("No changes made — already applied or anchors not found.")

    log.info("")
    log.info("NEXT: Add usage calls inside the per-prop enrichment loop.")
    log.info("See POINT1_USAGE and POINT2_USAGE in this file for the exact code.")
    log.info("Search prop_enrichment_layer.py for '_opp_avg_k_pct' to find Point 1.")
    log.info("Search for where hit-prop base rate is set for Point 2.")


def verify() -> None:
    if not ENRICHMENT.exists():
        print("prop_enrichment_layer.py not found.")
        return
    content = ENRICHMENT.read_text(encoding="utf-8")
    for name, term in [
        ("_pa_model_opp_k_rate function", "_pa_model_opp_k_rate"),
        ("_pa_model_hit_prob function",   "_pa_model_hit_prob"),
        ("pa_model import in either",     "from pa_model import"),
    ]:
        count = content.count(term)
        status = "✅" if count > 0 else "❌"
        print(f"  {status} {name}: {count} occurrence(s)")

    print("\nManual usage snippets to add inside the enrichment loop:")
    print("\n--- POINT 1 (opp K-rate) ---")
    print(POINT1_USAGE)
    print("\n--- POINT 2 (hit-prop probability) ---")
    print(POINT2_USAGE)


if __name__ == "__main__":
    if "--verify" in sys.argv or "--show" in sys.argv:
        verify()
    else:
        apply()
        verify()
