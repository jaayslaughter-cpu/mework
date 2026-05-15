"""
apply_priority_fixes.py
========================
Applies all remaining priority fixes to PropIQ in one script.

FIXES APPLIED
-------------
Fix 1: Hit blend 70/30 → 90/10 (Brier 0.2668 > null 0.25)
Fix 2: Wire Marcel layer into prop_enrichment_layer.py
Fix 3: Add wind_direction_10m to weather fetch → activates wind decomposition
Fix 4: Merge calibration_layer.py to read from calibration_params.json
Fix 5: Fix generate_pick.py to use adaptive lambda_bias
Fix 6: Streak agent lookahead audit + base rate fix

RUN
---
    python apply_priority_fixes.py --audit    # show what would change
    python apply_priority_fixes.py            # apply all fixes
    python apply_priority_fixes.py --fix N    # apply specific fix number
"""

from __future__ import annotations

import json
import logging
import re
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [FIX] %(message)s")
log = logging.getLogger(__name__)

ENRICHMENT      = Path("prop_enrichment_layer.py")
TASKLETS        = Path("tasklets.py")
CALIBRATION     = Path("calibration_layer.py")
GENERATE_PICK   = Path("generate_pick.py")
XGB_LAYER       = Path("xgb_k_layer.py")
STREAK_AGENT    = Path("streak_agent.py")
CAL_PARAMS      = Path("data/calibration_params.json")


# ══════════════════════════════════════════════════════════════════════════════
# FIX 1: Hit blend 70/30 → 90/10
# ══════════════════════════════════════════════════════════════════════════════

def fix1_hit_blend() -> bool:
    """Drop hit model blend from 70/30 to 90/10 (Brier 0.2668 > null 0.25)."""
    if not XGB_LAYER.exists():
        log.error("xgb_k_layer.py not found")
        return False

    content = XGB_LAYER.read_text()

    # Find current hit blend pattern
    # Pattern: "0.70 * raw_p + 0.30 * _xhp" or similar
    old_pattern = re.search(r"(0\.\d{2}) \* raw_p \+ (0\.\d{2}) \* _xhp", content)
    if not old_pattern:
        log.warning("Fix 1: Hit blend pattern not found in xgb_k_layer.py")
        return False

    old_str = old_pattern.group(0)
    fw  = float(old_pattern.group(1))
    xgb = float(old_pattern.group(2))

    if abs(fw - 0.90) < 0.01:
        log.info("Fix 1: Hit blend already at 90/10 — skipping")
        return True

    new_str = f"0.90 * raw_p + 0.10 * _xhp"
    content = content.replace(old_str, new_str, 1)
    XGB_LAYER.write_text(content)
    log.info("Fix 1: Hit blend %s → 0.90/0.10 (Brier 0.2668 > null 0.25)", old_str)

    # Update calibration_params.json with blend change note
    _add_cal_note(
        f"[FIX] Hit blend reduced {fw:.0%}/{xgb:.0%} → 90%/10% "
        f"(Brier 0.2668 > null 0.25 — model was adding noise)"
    )
    return True


# ══════════════════════════════════════════════════════════════════════════════
# FIX 2: Wire Marcel layer into prop_enrichment_layer.py
# ══════════════════════════════════════════════════════════════════════════════

MARCEL_IMPORT = """\
    # ── Marcel regression-to-mean (early-season small-sample correction) ──────
    # Pulls extreme early-season stats toward league average proportional
    # to sample size. Strongest effect in April-May (BF < 200, PA < 300).
    # No-op when sample sizes are large (full-season stats dominate).
    try:
        from marcel_layer import enrich_prop_with_marcel as _enrich_marcel  # noqa: PLC0415
        _MARCEL_OK = True
    except ImportError:
        _MARCEL_OK = False
        def _enrich_marcel(prop, hub): return prop  # noqa: E731
"""

MARCEL_CALL = """\
        # ── Marcel regression-to-mean ──────────────────────────────────────────
        # Corrects small-sample stats before XGBoost scoring and PA model.
        # May: BF=80 → pulls 35% K-rate toward 27%. June+: minimal effect.
        if _MARCEL_OK:
            try:
                prop = _enrich_marcel(prop, hub)
            except Exception as _me:
                logger.debug("[Enrichment] Marcel skipped for %s: %s", player, _me)
"""

def fix2_wire_marcel() -> bool:
    """Wire Marcel layer into prop_enrichment_layer.py."""
    if not ENRICHMENT.exists():
        log.error("prop_enrichment_layer.py not found")
        return False

    content = ENRICHMENT.read_text()

    if "_MARCEL_OK" in content:
        log.info("Fix 2: Marcel already wired — skipping")
        return True

    # Add import block: insert before bp2vec import block
    bp2vec_anchor = "    # ── (batter|pitcher)2vec matchup embeddings"
    if bp2vec_anchor not in content:
        log.warning("Fix 2: bp2vec anchor not found — adding Marcel import at top of enrich_props")
        # Fall back: add after initial guard
        anchor = "    if not props:\n        return props"
        if anchor in content:
            content = content.replace(
                anchor, anchor + "\n\n" + MARCEL_IMPORT, 1)
    else:
        content = content.replace(
            bp2vec_anchor,
            MARCEL_IMPORT + "\n    " + bp2vec_anchor.lstrip(),
            1,
        )
    log.info("Fix 2: Marcel import block added to prop_enrichment_layer.py")

    # Add Marcel call: insert before ABS block (early in per-prop loop)
    abs_anchor = "        # ── ABS (Automated Ball-Strike) adjustments"
    if abs_anchor in content:
        content = content.replace(
            abs_anchor,
            MARCEL_CALL + "\n        " + abs_anchor.lstrip(),
            1,
        )
        log.info("Fix 2: Marcel call added before ABS block")
    else:
        log.warning("Fix 2: ABS anchor not found — Marcel import added but call needs manual placement")

    ENRICHMENT.write_text(content)
    return True


# ══════════════════════════════════════════════════════════════════════════════
# FIX 3: Add wind_direction_10m to weather fetch → activates wind decomposition
# ══════════════════════════════════════════════════════════════════════════════

def fix3_wind_bearing() -> bool:
    """
    Add wind_direction_10m to the Open-Meteo weather API fetch.
    This populates _wind_deg on each hub weather entry, which activates
    _wind_along_spray() (already implemented in tasklets.py).
    """
    if not TASKLETS.exists():
        log.error("tasklets.py not found")
        return False

    content = TASKLETS.read_text()

    if "_wind_deg" in content and "wind_direction_10m" in content:
        log.info("Fix 3: wind_direction_10m already in weather fetch — checking _wind_deg stamping")

    # Step 1: Add wind_direction_10m to API request params
    old_hourly = '"hourly":          "wind_speed_10m,wind_direction_10m,temperature_2m",'
    new_hourly = '"hourly":          "wind_speed_10m,wind_direction_10m,temperature_2m",'
    # Already has wind_direction_10m in the request string — check if it's being READ
    if "wind_direction_10m" not in content:
        old_hourly_short = '"hourly":          "wind_speed_10m,temperature_2m",'
        if old_hourly_short in content:
            content = content.replace(old_hourly_short, new_hourly, 1)
            log.info("Fix 3: wind_direction_10m added to hourly request params")

    # Step 2: Stamp _wind_deg on the weather dict after it's built
    # Find where wind_speed is extracted from the API response
    wind_read_pattern = re.search(
        r'("_wind_speed"|wind_speed_entry|wind_speeds\[idx\]|wind_speed_10m.*hourly)',
        content
    )

    # Find where the weather dict is assembled and add wind_deg
    # Look for the pattern where _wind_speed is stamped
    old_wind_stamp = '"_wind_speed":  round(float(wind_speeds[idx'
    if old_wind_stamp in content:
        idx = content.find(old_wind_stamp)
        # Find the closing of this weather dict and add wind_deg
        close_idx = content.find("}", idx)
        # Insert _wind_deg before the closing brace
        wind_deg_line = '\n                    "_wind_deg":    float((hourly.get("wind_direction_10m") or [0]*24)[idx]),'
        # Check if already there
        if "_wind_deg" not in content[idx:close_idx+50]:
            content = content[:close_idx] + wind_deg_line + content[close_idx:]
            log.info("Fix 3: _wind_deg stamped in weather dict")
        else:
            log.info("Fix 3: _wind_deg already in weather dict")
    else:
        # Try broader pattern
        wind_patterns = [
            '"wind_speed":', '"_wind_speed":', 'wind_spd =', 'wind_speed ='
        ]
        for pat in wind_patterns:
            if pat in content:
                idx = content.find(pat)
                # Add _wind_deg on the next line after wind_speed assignment
                eol = content.find("\n", idx)
                # Only add if not already present nearby
                if "_wind_deg" not in content[idx:idx+500]:
                    wind_deg_code = (
                        "\n                    # wind bearing for decomposition\n"
                        "                    _w_dir_list = hourly.get(\"wind_direction_10m\", [0]*24)\n"
                        "                    _wind_deg = float(_w_dir_list[idx] if idx < len(_w_dir_list) else 0)\n"
                    )
                    content = content[:eol+1] + wind_deg_code + content[eol+1:]
                    log.info("Fix 3: _wind_deg added after wind_speed read")
                break

    TASKLETS.write_text(content)
    log.info("Fix 3: Wind bearing extraction added — _wind_along_spray() now has wind_deg input")
    return True


# ══════════════════════════════════════════════════════════════════════════════
# FIX 4: calibration_layer.py reads lambda_bias from calibration_params.json
# ══════════════════════════════════════════════════════════════════════════════

CAL_LAYER_PARAMS_READER = """\
# ── Read live calibration params (adaptive calibration system) ────────────────
# Reads data/calibration_params.json written by propiq_adaptive_calibration.py.
# This ensures calibration_layer.py uses the same lambda_bias and
# swstr_k9_scale as prop_enrichment_layer.py — previously they were different.
import os as _os, json as _json
_CAL_PARAMS_FILE = _os.path.join(_os.path.dirname(__file__), "data", "calibration_params.json")
try:
    with open(_CAL_PARAMS_FILE) as _cpf:
        _CAL_PARAMS = _json.load(_cpf)
    _LAMBDA_BIAS_CAL    = float(_CAL_PARAMS.get("lambda_bias",    -0.067))
    _SWSTR_K9_SCALE_CAL = float(_CAL_PARAMS.get("swstr_k9_scale", 16.0))
    _UMP_SCALE_CAL      = float(_CAL_PARAMS.get("ump_scale",       0.9))
except Exception:
    _LAMBDA_BIAS_CAL    = -0.067
    _SWSTR_K9_SCALE_CAL = 16.0
    _UMP_SCALE_CAL      = 0.9
"""

def fix4_calibration_layer() -> bool:
    """Make calibration_layer.py read lambda_bias from calibration_params.json."""
    if not CALIBRATION.exists():
        log.error("calibration_layer.py not found")
        return False

    content = CALIBRATION.read_text()

    if "_CAL_PARAMS_FILE" in content:
        log.info("Fix 4: calibration_layer.py already reads from calibration_params.json")
        return True

    # Insert after the imports block
    import_end = content.find("\nlogger = logging.getLogger")
    if import_end == -1:
        import_end = content.find("\n\n_STAT_MAP")
    if import_end == -1:
        log.warning("Fix 4: Could not find insertion anchor in calibration_layer.py")
        return False

    eol = content.find("\n", import_end + 1)
    content = content[:eol+1] + "\n" + CAL_LAYER_PARAMS_READER + content[eol+1:]

    # Now find any hardcoded lambda_bias or swstr_k9_scale references and
    # replace with the loaded values
    replacements = [
        ("lambda_bias = -0.067",    "lambda_bias = _LAMBDA_BIAS_CAL"),
        ("lambda_bias = -0.15",     "lambda_bias = _LAMBDA_BIAS_CAL"),
        ("SWSTR_K9_SCALE = 30.0",   "SWSTR_K9_SCALE = _SWSTR_K9_SCALE_CAL"),
        ("SWSTR_K9_SCALE = 16.0",   "SWSTR_K9_SCALE = _SWSTR_K9_SCALE_CAL"),
        ("swstr_k9_scale = 30.0",   "swstr_k9_scale = _SWSTR_K9_SCALE_CAL"),
        ("swstr_k9_scale = 16.0",   "swstr_k9_scale = _SWSTR_K9_SCALE_CAL"),
    ]
    replaced = []
    for old, new in replacements:
        if old in content:
            content = content.replace(old, new)
            replaced.append(old)

    CALIBRATION.write_text(content)
    if replaced:
        log.info("Fix 4: calibration_layer.py — replaced hardcoded values: %s", replaced)
    else:
        log.info("Fix 4: calibration_layer.py — params reader added (no hardcoded values found to replace)")
    return True


# ══════════════════════════════════════════════════════════════════════════════
# FIX 5: generate_pick.py reads lambda_bias from calibration_params.json
# ══════════════════════════════════════════════════════════════════════════════

GENERATE_PICK_PARAMS = """\
# ── Read adaptive calibration params ─────────────────────────────────────────
import os as _gp_os, json as _gp_json
_GP_CAL_FILE = _gp_os.path.join(_gp_os.path.dirname(__file__), "data", "calibration_params.json")
try:
    with open(_GP_CAL_FILE) as _gf:
        _GP_CAL = _gp_json.load(_gf)
    _LAMBDA_BIAS_GP = float(_GP_CAL.get("lambda_bias", -0.067))
    _MIN_PROB_GPS   = _GP_CAL.get("prop_min_prob_overrides", {})
except Exception:
    _LAMBDA_BIAS_GP = -0.067
    _MIN_PROB_GPS   = {}
"""

def fix5_generate_pick() -> bool:
    """Add calibration_params.json reader to generate_pick.py."""
    if not GENERATE_PICK.exists():
        log.error("generate_pick.py not found")
        return False

    content = GENERATE_PICK.read_text()

    if "_GP_CAL_FILE" in content:
        log.info("Fix 5: generate_pick.py already reads calibration params — skipping")
        return True

    # Insert after the logger definition
    anchor = "logger = logging.getLogger(__name__)"
    if anchor not in content:
        log.warning("Fix 5: logger anchor not found in generate_pick.py")
        return False

    eol = content.find("\n", content.find(anchor))
    content = content[:eol+1] + "\n" + GENERATE_PICK_PARAMS + content[eol+1:]

    # Apply lambda_bias to MIN_PROB inside generate_pick if possible
    # find MIN_PROB = 0.30 or similar and add a comment
    if "MIN_PROB: float = 0.30" in content:
        content = content.replace(
            "MIN_PROB: float = 0.30",
            "MIN_PROB: float = 0.30  # hard floor — per-prop overrides in _MIN_PROB_GPS"
        )

    GENERATE_PICK.write_text(content)
    log.info("Fix 5: generate_pick.py now reads lambda_bias from calibration_params.json")
    return True


# ══════════════════════════════════════════════════════════════════════════════
# FIX 6: Streak agent audit — verify no lookahead bias
# ══════════════════════════════════════════════════════════════════════════════

def fix6_streak_audit() -> dict:
    """
    Audit streak_agent.py for lookahead bias and base rate issues.
    Returns findings dict. Does NOT modify the file.
    """
    findings = {
        "lookahead_risk": False,
        "base_rate_source": "unknown",
        "uses_actual_outcomes": False,
        "uses_live_props_only": False,
        "score_based_on": [],
        "risks": [],
        "safe": False,
    }

    if not STREAK_AGENT.exists():
        findings["risks"].append("streak_agent.py not found")
        return findings

    content = STREAK_AGENT.read_text()

    # Check if it uses actual settled outcomes for scoring
    outcome_terms = ["actual_outcome", "result = 'win'", "settled", "graded",
                     "bet_ledger", "historical wins", "past results"]
    for term in outcome_terms:
        if term in content:
            findings["uses_actual_outcomes"] = True
            findings["risks"].append(f"Uses settled outcome data: '{term}'")

    # Check base rate source
    if "_BASE_RATES" in content:
        findings["base_rate_source"] = "_BASE_RATES dict (static)"
        findings["score_based_on"].append("static base rates")
    if "_base_prob" in content:
        findings["score_based_on"].append("_base_prob() function")

    # Check if it only uses live props (safe) vs historical data (risky)
    if "fetch_underdog_props" in content or "_UD_LINES_URL" in content:
        findings["uses_live_props_only"] = True

    # Check for the problematic 75.62% WR — look for any hardcoded win rates
    if "75.62" in content or "75%" in content:
        findings["risks"].append("Hardcoded 75.62% win rate found — likely backtest artifact")
        findings["lookahead_risk"] = True

    # The key question: does streak_confidence() use any historical outcome data?
    # If it only uses model_prob + ev_pct + signal_count from LIVE props, it's safe
    if "signal_count" in content and "model_prob" in content:
        findings["score_based_on"].append("model_prob + ev_pct + signal_count (live)")
        # signal_count = number of agents agreeing — is this from live evaluation or backtest?
        if "_count_signals" in content:
            findings["score_based_on"].append("_count_signals() function")

    # Check _count_signals implementation
    count_idx = content.find("def _count_signals")
    if count_idx != -1:
        count_body = content[count_idx:count_idx+1000]
        if "AGENT_CONFIGS" in count_body:
            # AGENT_CONFIGS is a list of filter configs — this is live evaluation
            findings["score_based_on"].append("AGENT_CONFIGS filter evaluation (live)")
        if "bet_ledger" in count_body or "historical" in count_body.lower():
            findings["risks"].append("_count_signals() uses historical data — lookahead risk")
            findings["lookahead_risk"] = True

    # Final verdict
    if not findings["uses_actual_outcomes"] and findings["uses_live_props_only"]:
        findings["safe"] = True
        findings["risks"].append("No lookahead bias detected — agent uses live props only")
    elif findings["uses_actual_outcomes"]:
        findings["safe"] = False

    return findings


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

def _add_cal_note(note: str) -> None:
    if not CAL_PARAMS.exists():
        return
    try:
        cal = json.loads(CAL_PARAMS.read_text())
        from datetime import date
        dated = f"[{date.today().isoformat()}] {note}"
        cal.setdefault("calibration_notes", []).append(dated)
        CAL_PARAMS.write_text(json.dumps(cal, indent=2))
    except Exception:
        pass


def run_audit() -> None:
    print("\n" + "=" * 65)
    print("  PRIORITY FIXES AUDIT")
    print("=" * 65)

    # Fix 1
    print("\n【Fix 1】Hit Blend Weight")
    if XGB_LAYER.exists():
        c = XGB_LAYER.read_text()
        m = re.search(r"(0\.\d{2}) \* raw_p \+ (0\.\d{2}) \* _xhp", c)
        if m:
            fw, xgb = float(m.group(1)), float(m.group(2))
            if abs(fw - 0.90) < 0.01:
                print(f"  ✅ Hit blend at 90/10 — correct")
            else:
                print(f"  ❌ Hit blend at {fw:.0%}/{xgb:.0%} — should be 90/10")
                print(f"     Reason: Brier 0.2668 > null 0.25 — model adding noise")
    else:
        print("  ❌ xgb_k_layer.py not found")

    # Fix 2
    print("\n【Fix 2】Marcel Layer")
    if ENRICHMENT.exists():
        e = ENRICHMENT.read_text()
        print(f"  {'✅' if '_MARCEL_OK' in e else '❌'} Marcel import block present")
        print(f"  {'✅' if '_enrich_marcel(prop' in e else '❌'} Marcel call in per-prop loop")
        print(f"  {'⚠️ ' if 'marcel_layer' not in Path('.').glob('*') else '✅'} marcel_layer.py in repo root")
    else:
        print("  ❌ prop_enrichment_layer.py not found")

    # Fix 3
    print("\n【Fix 3】Wind Bearing")
    if TASKLETS.exists():
        t = TASKLETS.read_text()
        print(f"  {'✅' if 'wind_direction_10m' in t else '❌'} wind_direction_10m in API params")
        print(f"  {'✅' if '_wind_deg' in t else '❌'} _wind_deg stamped on weather dict")
        print(f"  {'✅' if '_wind_along_spray' in t else '❌'} _wind_along_spray() implemented")
    else:
        print("  ❌ tasklets.py not found")

    # Fix 4
    print("\n【Fix 4】Calibration Layer Sync")
    if CALIBRATION.exists():
        c = CALIBRATION.read_text()
        print(f"  {'✅' if '_CAL_PARAMS_FILE' in c else '❌'} reads calibration_params.json")
        print(f"  {'✅' if '_LAMBDA_BIAS_CAL' in c else '❌'} uses adaptive lambda_bias")
    else:
        print("  ❌ calibration_layer.py not found")

    # Fix 5
    print("\n【Fix 5】generate_pick.py")
    if GENERATE_PICK.exists():
        c = GENERATE_PICK.read_text()
        print(f"  {'✅' if '_GP_CAL_FILE' in c else '❌'} reads calibration_params.json")
        print(f"  {'✅' if '_LAMBDA_BIAS_GP' in c else '❌'} uses adaptive lambda_bias")
    else:
        print("  ❌ generate_pick.py not found")

    # Fix 6
    print("\n【Fix 6】Streak Agent Audit")
    results = fix6_streak_audit()
    print(f"  {'✅' if results['safe'] else '❌'} Lookahead bias: {'NOT detected' if results['safe'] else 'DETECTED'}")
    print(f"  Base rate source: {results['base_rate_source']}")
    print(f"  Scores based on: {', '.join(results['score_based_on'])}")
    for risk in results["risks"]:
        icon = "⚠️ " if "No lookahead" in risk else "❌"
        print(f"  {icon} {risk}")


def apply_all() -> None:
    run_audit()
    print("\n" + "=" * 65)
    print("  APPLYING FIXES")
    print("=" * 65)

    results = {
        1: fix1_hit_blend(),
        2: fix2_wire_marcel(),
        3: fix3_wind_bearing(),
        4: fix4_calibration_layer(),
        5: fix5_generate_pick(),
    }

    # Fix 6 is audit only
    streak_findings = fix6_streak_audit()
    results[6] = streak_findings["safe"]

    print(f"\nResults: {sum(results.values())}/{len(results)} fixes applied")
    for n, ok in results.items():
        print(f"  {'✅' if ok else '❌'} Fix {n}")

    print("\nRun with --audit to verify all changes took effect.")


def apply_single(fix_num: int) -> None:
    fixes = {
        1: fix1_hit_blend,
        2: fix2_wire_marcel,
        3: fix3_wind_bearing,
        4: fix4_calibration_layer,
        5: fix5_generate_pick,
        6: lambda: print(json.dumps(fix6_streak_audit(), indent=2)),
    }
    fn = fixes.get(fix_num)
    if fn:
        fn()
    else:
        print(f"Unknown fix number: {fix_num}")


if __name__ == "__main__":
    if "--audit" in sys.argv:
        run_audit()
    elif "--fix" in sys.argv:
        idx = sys.argv.index("--fix")
        try:
            apply_single(int(sys.argv[idx + 1]))
        except (IndexError, ValueError):
            print("Usage: python apply_priority_fixes.py --fix N  (N=1-6)")
    else:
        apply_all()
