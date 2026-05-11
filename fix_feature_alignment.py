"""
fix_feature_alignment.py
=========================
Audits and fixes the feature name mismatch between the hub training
pipeline (xgb_training_pipeline.py) and PropIQ's scorer (xgb_k_layer.py).

THE PROBLEM
-----------
The models in models/*.pkl were trained by mlb-analytics-hub using these
exact column names:

  K_FEATURES (training):
    'sv_xera', 'sv_era', 'sv_k_pct', 'sv_bb_pct', 'sv_whiff_pct',
    'l3_ks', 'l5_ks', 'l10_ks', 'l3_ip', 'l5_ip', 'days_rest',
    'opp_lineup_k_pct_proxy', 'opp_lineup_xwoba_proxy'

  HITS_FEATURES (training):
    'sv_xba', 'sv_xwoba', 'sv_xslg', 'sv_ev', 'sv_brl_pct', 'sv_hh_pct',
    'sv_ss_pct', 'sv_la', 'sv_k_pct', 'sv_bb_pct',
    'opp_xera', 'opp_k_pct', 'opp_bb_pct', 'opp_whiff',
    'bats_L', 'throws_R', 'platoon_adv', 'l7_hits', 'l7_hit_rate'

PropIQ's xgb_k_layer.py uses DIFFERENT names for the same columns:

  K_FEATURES (PropIQ — WRONG):
    'sv_xera', 'fg_era', 'fg_kpct', 'fg_bbpct', 'sv_swstr_pct',  ← fg_era≠sv_era, fg_kpct≠sv_k_pct
    'l5_ks', 'l5_k_rate', 'l10_ks',                               ← l3_ks missing, l5_k_rate not in training
    'opp_k_pct', 'opp_xwoba'                                       ← l3_ip/l5_ip/days_rest MISSING

  HITS_FEATURES (PropIQ — WRONG):
    'sv_swstr_pct' instead of 'sv_ss_pct'                         ← SwStr% ≠ SwStr% (same stat, different key)
    Missing: 'opp_whiff'                                           ← completely absent

RESULT: Every model call silently fills mismatched columns with zeros
or defaults, degrading accuracy. The model sees 0.0 for days_rest,
l3_ks, l3_ip, l5_ip on every K prop prediction. For hits, opp_whiff
is always 0.0.

THE FIX
-------
This script:
1. Audits xgb_k_layer.py to show all mismatches
2. Patches _build_k_features() to use training-exact column names
3. Patches _build_hit_features() similarly
4. Updates xgb_feature_cols.json with the correct column order

RUN:
    python fix_feature_alignment.py --audit   # show mismatches, no changes
    python fix_feature_alignment.py           # apply patch
    python fix_feature_alignment.py --verify  # confirm after patch
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [FEAT-ALIGN] %(message)s")
log = logging.getLogger(__name__)

XGB_LAYER   = Path("xgb_k_layer.py")
FEAT_JSON   = Path("models/xgb_feature_cols.json")

# ── Ground truth from mlb-analytics-hub xgb_training_pipeline.py ─────────────
# These are the EXACT column names the .pkl models were trained on.
# Do not change these without retraining the models.

TRAINING_K_FEATURES = [
    "sv_xera",                  # Statcast xERA
    "sv_era",                   # FanGraphs ERA (stored as sv_era in training)
    "sv_k_pct",                 # K% (0–100 scale in training data)
    "sv_bb_pct",                # BB% (0–100 scale)
    "sv_whiff_pct",             # SwStr% / whiff% (0–100)
    "l3_ks",                    # L3-start avg Ks — MISSING in current PropIQ build
    "l5_ks",                    # L5-start avg Ks
    "l10_ks",                   # L10-start avg Ks
    "l3_ip",                    # L3-start avg IP — MISSING in current PropIQ build
    "l5_ip",                    # L5-start avg IP — MISSING in current PropIQ build
    "days_rest",                # Days since last start — MISSING in current PropIQ build
    "opp_lineup_k_pct_proxy",   # Opp lineup K% proxy (0–100)
    "opp_lineup_xwoba_proxy",   # Opp lineup xwOBA proxy
]

TRAINING_HITS_FEATURES = [
    "sv_xba",           # Statcast xBA
    "sv_xwoba",         # Statcast xwOBA
    "sv_xslg",          # Statcast xSLG
    "sv_ev",            # Exit velocity
    "sv_brl_pct",       # Barrel %
    "sv_hh_pct",        # Hard-hit %
    "sv_ss_pct",        # SwStr% (NOTE: training uses sv_ss_pct, PropIQ uses sv_swstr_pct — MISMATCH)
    "sv_la",            # Launch angle
    "sv_k_pct",         # Batter K% (NOTE: training uses sv_k_pct, PropIQ uses fg_kpct — MISMATCH)
    "sv_bb_pct",        # Batter BB% (NOTE: training uses sv_bb_pct, PropIQ uses fg_bbpct — MISMATCH)
    "opp_xera",         # Opposing pitcher xERA
    "opp_k_pct",        # Pitcher K%
    "opp_bb_pct",       # Pitcher BB%
    "opp_whiff",        # Pitcher SwStr% — MISSING in current PropIQ build
    "bats_L",           # Binary: batter is left-handed
    "throws_R",         # Binary: pitcher is right-handed
    "platoon_adv",      # Binary: favorable platoon matchup
    "l7_hits",          # L7-game hit total
    "l7_hit_rate",      # L7-game hit rate
]


# ── Mismatch table (PropIQ name → training name → status) ────────────────────

K_MISMATCHES = {
    # PropIQ key           → training key       → note
    "fg_era":              ("sv_era",            "RENAME: fg_era → sv_era"),
    "fg_kpct":             ("sv_k_pct",          "RENAME: fg_kpct → sv_k_pct"),
    "fg_bbpct":            ("sv_bb_pct",         "RENAME: fg_bbpct → sv_bb_pct"),
    "sv_swstr_pct":        ("sv_whiff_pct",      "RENAME: sv_swstr_pct → sv_whiff_pct"),
    "l5_k_rate":           (None,                "REMOVE: not in training features"),
    # MISSING from PropIQ build:
    # l3_ks, l3_ip, l5_ip, days_rest, opp_lineup_k_pct_proxy, opp_lineup_xwoba_proxy
}

HITS_MISMATCHES = {
    "sv_swstr_pct":        ("sv_ss_pct",         "RENAME: sv_swstr_pct → sv_ss_pct"),
    "fg_kpct":             ("sv_k_pct",          "RENAME: fg_kpct → sv_k_pct"),
    "fg_bbpct":            ("sv_bb_pct",         "RENAME: fg_bbpct → sv_bb_pct"),
    # MISSING from PropIQ build:
    # opp_whiff
}


def run_audit() -> None:
    """Print full mismatch report without making changes."""
    print("\n" + "=" * 65)
    print("  XGBoost FEATURE ALIGNMENT AUDIT")
    print("=" * 65)

    print("\n【K MODEL FEATURES】")
    print(f"  Training features ({len(TRAINING_K_FEATURES)}): {TRAINING_K_FEATURES}")

    if XGB_LAYER.exists():
        content = XGB_LAYER.read_text()
        # Extract K_FEATURES list from the file
        k_start = content.find("K_FEATURES = [")
        k_end   = content.find("]", k_start)
        k_block = content[k_start:k_end+1]
        print(f"\n  PropIQ xgb_k_layer.py K_FEATURES block:\n{k_block}")

    print("\n  MISMATCHES:")
    for propiq_name, (training_name, note) in K_MISMATCHES.items():
        print(f"    ❌ {propiq_name:30s} → {note}")

    print("\n  MISSING from PropIQ build (sent as 0.0 to model):")
    propiq_k_names = {"sv_xera","fg_era","fg_kpct","fg_bbpct","sv_swstr_pct",
                      "l5_ks","l5_k_rate","l10_ks","opp_k_pct","opp_xwoba"}
    for feat in TRAINING_K_FEATURES:
        # Check if a propiq key maps to this training feat
        mapped = any(v[0] == feat for v in K_MISMATCHES.values())
        direct = feat in propiq_k_names
        if not mapped and not direct:
            print(f"    ❌ {feat} — no PropIQ key provides this value")

    print("\n【HIT MODEL FEATURES】")
    print(f"  Training features ({len(TRAINING_HITS_FEATURES)}): {TRAINING_HITS_FEATURES}")
    print("\n  MISMATCHES:")
    for propiq_name, (training_name, note) in HITS_MISMATCHES.items():
        print(f"    ❌ {propiq_name:30s} → {note}")
    print("\n  MISSING:")
    print("    ❌ opp_whiff — pitcher SwStr% never populated in hit feature build")

    print("\n  IMPACT:")
    print("    K model: 4 of 13 features are zero or wrong on every prediction")
    print("      days_rest always 0.0  (model learned rest→K patterns, gets no signal)")
    print("      l3_ks always 0.0     (L3 form is different from L5)")
    print("      l3_ip always 0.0     (IP trend missing)")
    print("      l5_ip always 0.0")
    print("    Hit model: opp_whiff always 0.0 (pitcher whiff rate absent)")
    print()


# ── Corrected feature builder code ───────────────────────────────────────────

CORRECTED_K_BUILD = '''def _build_k_features(prop: dict, feat_order: list) -> Optional[np.ndarray]:
    """
    Build the K feature vector — column names match xgb_training_pipeline.py exactly.

    Mapping from PropIQ prop dict keys to training column names:
      fg_era / sv_era_p    → sv_era          (ERA stored as sv_era in training)
      fg_kpct / sv_kpct    → sv_k_pct        (K% in 0-100 scale)
      fg_bbpct             → sv_bb_pct       (BB% in 0-100 scale)
      sv_swstr_pct / csw   → sv_whiff_pct    (SwStr% in 0-100 scale)
      _l3_ks / l3_ks       → l3_ks           (L3-start avg Ks — was missing)
      _l3_ip / l3_ip       → l3_ip           (L3-start avg IP — was missing)
      _l5_ip / l5_ip       → l5_ip           (L5-start avg IP — was missing)
      _days_rest           → days_rest       (days since last start — was missing)
      _opp_avg_k_pct       → opp_lineup_k_pct_proxy
      _opp_avg_xwoba       → opp_lineup_xwoba_proxy
    """
    raw: dict[str, float] = {
        "sv_xera":                  _sf(prop, "sv_xera",           default=4.50),
        "sv_era":                   _sf(prop, "fg_era", "sv_era_p", "era",
                                        default=4.50),
        "sv_k_pct":                 _sf(prop, "fg_kpct", "sv_kpct", "k_pct",
                                        default=22.0),
        "sv_bb_pct":                _sf(prop, "fg_bbpct", "sv_bbpct", "bb_pct",
                                        default=8.0),
        "sv_whiff_pct":             _sf(prop, "sv_swstr_pct", "swstr_pct",
                                        "csw_pct", "sv_whiff_pct", default=24.0),
        "l3_ks":                    _sf(prop, "l3_ks", "_l3_ks",   default=4.5),
        "l5_ks":                    _sf(prop, "l5_ks", "_l5_ks",   default=4.5),
        "l10_ks":                   _sf(prop, "l10_ks", "_l10_ks", default=4.5),
        "l3_ip":                    _sf(prop, "l3_ip", "_l3_ip",   default=5.0),
        "l5_ip":                    _sf(prop, "l5_ip", "_l5_ip",   default=5.0),
        "days_rest":                _sf(prop, "days_rest", "_days_rest",
                                        "rest_days",               default=5.0),
        "opp_lineup_k_pct_proxy":   _sf(prop, "_opp_avg_k_pct", "opp_k_pct",
                                        "opp_lineup_k_pct_proxy",  default=22.0),
        "opp_lineup_xwoba_proxy":   _sf(prop, "_opp_avg_xwoba", "opp_xwoba",
                                        "opp_lineup_xwoba_proxy",  default=0.320),
    }

    # Scale fractions → percent (training data used 0-100 scale for pct cols)
    for pct_key in ("sv_k_pct", "sv_bb_pct", "sv_whiff_pct", "opp_lineup_k_pct_proxy"):
        if 0.0 < raw[pct_key] <= 1.0:
            raw[pct_key] *= 100.0

    cols = feat_order if feat_order else TRAINING_K_FEATURES
    try:
        return np.array([[raw.get(c, 0.0) for c in cols]], dtype=np.float32)
    except Exception:
        logger.debug("[xgb_k] K feature build error", exc_info=True)
        return None
'''

CORRECTED_HIT_BUILD = '''def _build_hit_features(prop: dict, pitcher: dict,
                         feat_order: list) -> Optional[np.ndarray]:
    """
    Build the batter-hit feature vector — column names match xgb_training_pipeline.py.

    Key corrections vs prior version:
      sv_swstr_pct → sv_ss_pct  (training used sv_ss_pct for SwStr%)
      fg_kpct      → sv_k_pct   (training used sv_k_pct, not fg_kpct)
      fg_bbpct     → sv_bb_pct  (training used sv_bb_pct, not fg_bbpct)
      opp_whiff now populated from pitcher dict (was always 0.0 before)
    """
    bat_side = str(prop.get("batter_hand", prop.get("bats", "R")) or "R").upper()[:1]
    pit_hand = str(pitcher.get("_pitcher_hand", pitcher.get("pitcher_hand",
                   pitcher.get("pitchHand", "R"))) or "R").upper()[:1]
    platoon = 1 if (bat_side == "L" and pit_hand == "R") or \\
                   (bat_side == "R" and pit_hand == "L") else 0

    raw: dict[str, float] = {
        # Batter Statcast — use sv_ prefix to match training column names
        "sv_xba":       _sf(prop, "sv_xba",                       default=0.250),
        "sv_xwoba":     _sf(prop, "sv_xwoba",    "fg_woba",        default=0.320),
        "sv_xslg":      _sf(prop, "sv_xslg",     "fg_slg",         default=0.400),
        "sv_ev":        _sf(prop, "sv_ev",                         default=88.0),
        "sv_brl_pct":   _sf(prop, "sv_brl_pct",                   default=4.0),
        "sv_hh_pct":    _sf(prop, "sv_hh_pct",                    default=35.0),
        # sv_ss_pct = SwStr% (training key) — was wrongly keyed as sv_swstr_pct
        "sv_ss_pct":    _sf(prop, "sv_swstr_pct", "sv_ss_pct",
                            "swstr_pct",                           default=10.0),
        "sv_la":        _sf(prop, "sv_la",                         default=12.0),
        # sv_k_pct and sv_bb_pct — training used sv_ prefix, not fg_
        "sv_k_pct":     _sf(prop, "fg_kpct", "sv_k_pct", "k_pct", default=22.0),
        "sv_bb_pct":    _sf(prop, "fg_bbpct", "sv_bb_pct", "bb_pct", default=8.0),
        # Pitcher opposition — keyed from pitcher sub-dict
        "opp_xera":     _sf(pitcher, "sv_xera",  "fg_era",         default=4.50),
        "opp_k_pct":    _sf(pitcher, "fg_kpct", "sv_k_pct",        default=22.0),
        "opp_bb_pct":   _sf(pitcher, "fg_bbpct", "sv_bb_pct",      default=8.0),
        # opp_whiff = pitcher SwStr% — was always 0.0 before (key was missing)
        "opp_whiff":    _sf(pitcher, "sv_swstr_pct", "sv_whiff_pct",
                            "swstr_pct", "opp_whiff",              default=24.0),
        # Platoon flags
        "bats_L":       1.0 if bat_side == "L" else 0.0,
        "throws_R":     1.0 if pit_hand == "R" else 0.0,
        "platoon_adv":  float(platoon),
        # Rolling form
        "l7_hits":      _sf(prop, "l7_hits",    "_l7_hits",        default=1.5),
        "l7_hit_rate":  _sf(prop, "l7_hit_rate", "_l7_hit_rate",   default=0.50),
    }

    # Scale fractions → percent
    for pct_key in ("sv_ss_pct", "sv_brl_pct", "sv_hh_pct",
                    "sv_k_pct", "sv_bb_pct", "opp_k_pct", "opp_bb_pct", "opp_whiff"):
        if 0.0 < raw[pct_key] <= 1.0:
            raw[pct_key] *= 100.0

    cols = feat_order if feat_order else TRAINING_HITS_FEATURES
    try:
        return np.array([[raw.get(c, 0.0) for c in cols]], dtype=np.float32)
    except Exception:
        logger.debug("[xgb_k] hit feature build error", exc_info=True)
        return None
'''

CORRECTED_K_FEATURES_CONST = '''# K model feature names — must match xgb_training_pipeline.py exactly
K_FEATURES = [
    "sv_xera",                  # Statcast xERA
    "sv_era",                   # ERA (FanGraphs, stored as sv_era in training)
    "sv_k_pct",                 # K% (0-100 scale)
    "sv_bb_pct",                # BB% (0-100 scale)
    "sv_whiff_pct",             # SwStr% (0-100 scale)
    "l3_ks",                    # L3-start avg strikeouts
    "l5_ks",                    # L5-start avg strikeouts
    "l10_ks",                   # L10-start avg strikeouts
    "l3_ip",                    # L3-start avg IP
    "l5_ip",                    # L5-start avg IP
    "days_rest",                # Days since last start
    "opp_lineup_k_pct_proxy",   # Opposing lineup K% (0-100)
    "opp_lineup_xwoba_proxy",   # Opposing lineup xwOBA
]

# Hit model feature names — must match xgb_training_pipeline.py exactly
HITS_FEATURES = [
    "sv_xba",       # Statcast xBA
    "sv_xwoba",     # Statcast xwOBA
    "sv_xslg",      # Statcast xSLG
    "sv_ev",        # Exit velocity
    "sv_brl_pct",   # Barrel %
    "sv_hh_pct",    # Hard-hit %
    "sv_ss_pct",    # SwStr% (NOTE: training key is sv_ss_pct, not sv_swstr_pct)
    "sv_la",        # Launch angle
    "sv_k_pct",     # Batter K% (training key is sv_k_pct, not fg_kpct)
    "sv_bb_pct",    # Batter BB% (training key is sv_bb_pct, not fg_bbpct)
    "opp_xera",     # Pitcher xERA
    "opp_k_pct",    # Pitcher K%
    "opp_bb_pct",   # Pitcher BB%
    "opp_whiff",    # Pitcher SwStr% (was missing — always 0.0 before)
    "bats_L",       # 1 = left-handed batter
    "throws_R",     # 1 = right-handed pitcher
    "platoon_adv",  # 1 = favorable platoon matchup
    "l7_hits",      # L7-game hit total
    "l7_hit_rate",  # L7-game hit rate
]
'''


def apply_patch() -> None:
    """Patch xgb_k_layer.py with corrected feature builders."""
    if not XGB_LAYER.exists():
        log.error("xgb_k_layer.py not found — run from PropIQ repo root.")
        return

    content = XGB_LAYER.read_text(encoding="utf-8")

    if "TRAINING_ALIGNED" in content:
        log.info("xgb_k_layer.py already patched — skipping.")
        return

    # Replace K_FEATURES constant block
    k_feat_start = content.find("# Pitcher strikeout features")
    k_feat_end   = content.find("\n\n\ndef _build_k_features", k_feat_start)
    if k_feat_start == -1 or k_feat_end == -1:
        log.warning("Could not find K_FEATURES block — patching manually.")
    else:
        old_k_block = content[k_feat_start:k_feat_end]
        content = content.replace(old_k_block,
            "# TRAINING_ALIGNED — feature names match xgb_training_pipeline.py\n"
            + CORRECTED_K_FEATURES_CONST)
        log.info("K_FEATURES and HITS_FEATURES constants updated.")

    # Replace _build_k_features function
    k_build_start = content.find("def _build_k_features(")
    k_build_end   = content.find("\n\ndef _build_hit_features(", k_build_start)
    if k_build_start != -1 and k_build_end != -1:
        content = content.replace(
            content[k_build_start:k_build_end],
            CORRECTED_K_BUILD.rstrip()
        )
        log.info("_build_k_features() replaced with training-aligned version.")

    # Replace _build_hit_features function
    h_build_start = content.find("def _build_hit_features(")
    h_build_end   = content.find("\n\n\n# ── Public API", h_build_start)
    if h_build_start != -1 and h_build_end != -1:
        content = content.replace(
            content[h_build_start:h_build_end],
            CORRECTED_HIT_BUILD.rstrip()
        )
        log.info("_build_hit_features() replaced with training-aligned version.")

    XGB_LAYER.write_text(content, encoding="utf-8")
    log.info("xgb_k_layer.py patched.")

    # Update xgb_feature_cols.json
    feat_cols = {
        "hits":  TRAINING_HITS_FEATURES,
        "k_3.5": TRAINING_K_FEATURES,
        "k_4.5": TRAINING_K_FEATURES,
        "k_5.5": TRAINING_K_FEATURES,
        "k_6.5": TRAINING_K_FEATURES,
    }
    FEAT_JSON.parent.mkdir(exist_ok=True)
    FEAT_JSON.write_text(json.dumps(feat_cols, indent=2))
    log.info("models/xgb_feature_cols.json updated with training-exact column order.")


def verify() -> None:
    """Confirm the patch applied correctly."""
    if not XGB_LAYER.exists():
        print("xgb_k_layer.py not found.")
        return
    content = XGB_LAYER.read_text()

    checks = [
        ("TRAINING_ALIGNED marker present",    "TRAINING_ALIGNED" in content),
        ("sv_era in K build",                  '"sv_era"' in content),
        ("sv_k_pct in K build",                '"sv_k_pct"' in content),
        ("sv_whiff_pct in K build",            '"sv_whiff_pct"' in content),
        ("l3_ks in K build",                   '"l3_ks"' in content),
        ("l3_ip in K build",                   '"l3_ip"' in content),
        ("days_rest in K build",               '"days_rest"' in content),
        ("opp_lineup_k_pct_proxy in K build",  '"opp_lineup_k_pct_proxy"' in content),
        ("sv_ss_pct in hit build",             '"sv_ss_pct"' in content),
        ("opp_whiff in hit build",             '"opp_whiff"' in content),
        ("fg_kpct removed from K build",       'K_FEATURES' not in content or
                                               "fg_kpct" not in content.split("K_FEATURES")[1][:200]),
    ]

    print("\nFeature alignment verification:")
    for label, ok in checks:
        print(f"  {'✅' if ok else '❌'} {label}")

    # Check xgb_feature_cols.json
    if FEAT_JSON.exists():
        cols = json.loads(FEAT_JSON.read_text())
        print(f"\n  models/xgb_feature_cols.json:")
        for key, feats in cols.items():
            print(f"    {key}: {len(feats)} features")
    else:
        print("  ❌ models/xgb_feature_cols.json not found")


if __name__ == "__main__":
    if "--audit" in sys.argv:
        run_audit()
    elif "--verify" in sys.argv:
        verify()
    else:
        run_audit()
        apply_patch()
        verify()
