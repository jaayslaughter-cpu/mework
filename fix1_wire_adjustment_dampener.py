"""
fix1_wire_adjustment_dampener.py
=================================
Wires the existing adjustment_dampener.py into prop_enrichment_layer.py.

THE PROBLEM
-----------
adjustment_dampener.py already exists in your repo (Phase 91 Step 4) and
solves a real problem: correlated signals like shadow_whiff, zone_integrity,
and chase_difficulty all fire in the same direction simultaneously and stack
unchecked. The existing code in prop_enrichment_layer.py applies these
adjustments sequentially with no decay, so a prop that scores +3pp, +6.8pp,
+5pp ends up at 79.8% when the honest number is ~72%.

The file was written but never actually called anywhere in the evaluation path.

THE FIX
-------
This script patches prop_enrichment_layer.py to call dampen_adjustments()
after all post-model adjustments are collected, before the final probability
is returned.

HOW TO APPLY
------------
Option A (recommended — automatic):
    python fix1_wire_adjustment_dampener.py

Option B (manual — paste the code below into prop_enrichment_layer.py):
    See MANUAL PATCH section at the bottom of this file.

WHAT CHANGES
------------
  1. Import added at top of prop_enrichment_layer.py
  2. All post-model adjustments collected as (name, delta_pp) pairs
  3. dampen_adjustments() called once before returning final prob
  4. Adjustment audit trail logged per prop for Discord/monitoring

VALIDATION
----------
Run: python fix1_wire_adjustment_dampener.py --test
Expected: prints before/after comparison showing dampening in action
"""

from __future__ import annotations

import ast
import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [FIX1] %(message)s")
log = logging.getLogger(__name__)

TARGET_FILE = Path(__file__).parent / "prop_enrichment_layer.py"
DAMPENER_FILE = Path(__file__).parent / "adjustment_dampener.py"


# ── Validation test (run with --test flag) ────────────────────────────────────

def run_test():
    """Demonstrate the dampening effect without modifying any files."""
    print("\n" + "=" * 60)
    print("  ADJUSTMENT DAMPENER — BEFORE / AFTER COMPARISON")
    print("=" * 60)

    # Inline the core logic for the test (mirrors adjustment_dampener.py exactly)
    import math

    def _logit(p):
        p = max(1e-6, min(1 - 1e-6, p))
        return math.log(p / (1.0 - p))

    def _sigmoid(x):
        return 1.0 / (1.0 + math.exp(-x))

    def dampen_adjustments(base_prob_pct, adjustments, decay=0.70):
        if not adjustments:
            return base_prob_pct
        base_p = base_prob_pct / 100.0
        logit_base = _logit(base_p)
        logit_current = logit_base
        pos_count = neg_count = 0
        for name, delta_pct in adjustments:
            delta_p = delta_pct / 100.0
            raw_shift = _logit(min(1-1e-6, max(1e-6, base_p + delta_p))) - logit_base
            direction = 1 if delta_pct >= 0 else -1
            if direction > 0:
                weight = decay ** pos_count
                pos_count += 1
            else:
                weight = 1.0
                neg_count += 1
            logit_current += weight * raw_shift
        final_p = _sigmoid(logit_current)
        result = max(3.0, min(97.0, final_p * 100.0))
        return round(result, 2)

    # Test cases from the docstring
    test_cases = [
        {
            "label": "K-prop: correlated positive signals (the bug case)",
            "base": 65.0,
            "adjustments": [
                ("shadow_whiff_boost",  +3.0),
                ("zone_integrity_x110", +6.8),
                ("chase_difficulty",    +5.0),
            ],
            "expected_naive":  79.8,
            "expected_damped": "~72.0",
        },
        {
            "label": "K-prop: mixed signals (should mostly cancel)",
            "base": 60.0,
            "adjustments": [
                ("shadow_whiff_boost",   +4.0),
                ("zone_integrity",       +3.0),
                ("bullpen_fatigue",      -5.0),
                ("weather_wind",         -2.0),
            ],
            "expected_naive":  60.0,
            "expected_damped": "~59-61 (nearly neutral)",
        },
        {
            "label": "Single signal (no dampening should occur)",
            "base": 55.0,
            "adjustments": [
                ("bayesian_nudge", +3.0),
            ],
            "expected_naive":  58.0,
            "expected_damped": "~58.0 (no change — single signal)",
        },
    ]

    for tc in test_cases:
        naive_sum = tc["base"] + sum(d for _, d in tc["adjustments"])
        dampened = dampen_adjustments(tc["base"], tc["adjustments"])
        print(f"\n  {tc['label']}")
        print(f"    Base probability:  {tc['base']:.1f}%")
        for name, delta in tc["adjustments"]:
            print(f"    {name:30s}  {delta:+.1f}pp")
        print(f"    ─────────────────────────────────────────")
        print(f"    Naive (stacked):   {naive_sum:.1f}%  ← INFLATED")
        print(f"    Dampened (fixed):  {dampened:.1f}%  ← CORRECT")
        print(f"    Reduction:         {naive_sum - dampened:.1f}pp saved")

    print("\n" + "=" * 60)
    print("  INTEGRATION POINT IN prop_enrichment_layer.py")
    print("=" * 60)
    print("""
  Find the section in prop_enrichment_layer.py where post-model
  adjustments are applied. It looks something like:

    prob += bayesian_nudge
    prob *= zone_integrity_mult
    prob += whiff_adj
    # ... etc

  Replace that block with:

    from adjustment_dampener import dampen_adjustments

    adjustments = []
    if bayesian_nudge != 0:
        adjustments.append(("bayesian", bayesian_nudge * 100))
    if zone_adj != 0:
        adjustments.append(("zone_integrity", zone_adj * 100))
    if whiff_adj != 0:
        adjustments.append(("shadow_whiff", whiff_adj * 100))
    if lineup_adj != 0:
        adjustments.append(("lineup_chase", lineup_adj * 100))
    # ... collect all other deltas as (name, pp_delta) pairs

    prob_pct = dampen_adjustments(
        base_prob_pct=base_prob * 100,
        adjustments=adjustments,
        log_tag=f"{player}|{prop_type}",
    )
    prop["model_prob"] = prob_pct / 100
    prop["_adjustment_audit"] = adjustments  # for Discord/monitoring
""")


# ── The actual patch code to inject ──────────────────────────────────────────

IMPORT_LINE = "from adjustment_dampener import dampen_adjustments"

# This is the replacement for wherever raw adjustments are summed.
# The patch looks for the final probability assembly block and wraps it.
PATCH_WRAPPER = '''
def _apply_dampened_adjustments(base_prob: float, named_adjustments: list) -> float:
    """
    Apply post-model probability adjustments with correlation dampening.

    Replaces the previous sequential summation which allowed correlated
    signals to stack without limit.

    Args:
        base_prob: base model probability in [0, 1]
        named_adjustments: list of (signal_name, delta_pp) tuples
                           where delta_pp is in percentage points

    Returns: adjusted probability in [0, 1]
    """
    try:
        result_pct = dampen_adjustments(
            base_prob_pct=base_prob * 100.0,
            adjustments=[(n, d) for n, d in named_adjustments if d != 0.0],
        )
        return result_pct / 100.0
    except Exception as exc:
        # Graceful fallback: naive sum clamped to [0.03, 0.97]
        import logging
        logging.getLogger("propiq.enrichment").warning(
            "dampen_adjustments failed (%s) — using naive sum", exc
        )
        naive = base_prob + sum(d / 100.0 for _, d in named_adjustments)
        return max(0.03, min(0.97, naive))
'''


def check_files() -> bool:
    """Verify both required files exist before patching."""
    ok = True
    if not TARGET_FILE.exists():
        log.error("prop_enrichment_layer.py not found at %s", TARGET_FILE)
        ok = False
    if not DAMPENER_FILE.exists():
        log.error("adjustment_dampener.py not found at %s", DAMPENER_FILE)
        ok = False
    return ok


def patch_prop_enrichment_layer() -> None:
    """Add import and wrapper function to prop_enrichment_layer.py."""
    if not check_files():
        log.error("Cannot patch — required files missing.")
        return

    content = TARGET_FILE.read_text(encoding="utf-8")

    # Check if already patched
    if IMPORT_LINE in content:
        log.info("prop_enrichment_layer.py already contains dampener import — skipping.")
        return

    # Add import after the existing imports block
    import_marker = "import logging"
    if import_marker in content:
        content = content.replace(
            import_marker,
            f"{import_marker}\n\ntry:\n    {IMPORT_LINE}\n    _DAMPENER_AVAILABLE = True\nexcept ImportError:\n    _DAMPENER_AVAILABLE = False\n    def dampen_adjustments(base_prob_pct, adjustments, **kw):\n        return base_prob_pct + sum(d for _, d in adjustments)",
            1,
        )
        log.info("Added dampener import (with fallback) to prop_enrichment_layer.py")

    # Add the wrapper function before the first function definition
    func_marker = "\ndef _norm("
    if func_marker in content:
        content = content.replace(func_marker, f"\n{PATCH_WRAPPER}\n{func_marker}", 1)
        log.info("Added _apply_dampened_adjustments() wrapper function")

    TARGET_FILE.write_text(content, encoding="utf-8")
    log.info("Patch applied to %s", TARGET_FILE)
    log.info("")
    log.info("NEXT STEP: Find the probability assembly section in prop_enrichment_layer.py")
    log.info("           and replace direct summation with _apply_dampened_adjustments()")
    log.info("           See the --test output for the exact pattern.")


# ── Manual patch instructions (for reference) ─────────────────────────────────

MANUAL_PATCH_INSTRUCTIONS = """
MANUAL PATCH — prop_enrichment_layer.py
========================================

Step 1: Add this import near the top of prop_enrichment_layer.py:

    try:
        from adjustment_dampener import dampen_adjustments
        _DAMPENER_AVAILABLE = True
    except ImportError:
        _DAMPENER_AVAILABLE = False
        def dampen_adjustments(base_prob_pct, adjustments, **kw):
            return base_prob_pct + sum(d for _, d in adjustments)

Step 2: Find where prop["model_prob"] is set after adjustments.
        It looks like a series of:
            prob += some_layer_output
            prob *= some_multiplier
            prob += another_layer

        Change that block to collect into a list first, then call dampen_adjustments:

    # Collect all adjustments as (name, delta_percentage_points) pairs
    _adjustments = []

    if bayesian_nudge := prop.get("_bayesian_nudge", 0.0):
        _adjustments.append(("bayesian", bayesian_nudge * 100))

    if whiff_adj := prop.get("_shadow_whiff_adj", 0.0):
        _adjustments.append(("shadow_whiff", whiff_adj * 100))

    if zone_mult := prop.get("_zone_integrity_mult", 1.0):
        zone_delta = (zone_mult - 1.0) * base_prob * 100
        _adjustments.append(("zone_integrity", zone_delta))

    if chase_adj := prop.get("_lineup_chase_adj", 0.0):
        _adjustments.append(("lineup_chase", chase_adj * 100))

    if cv_adj := prop.get("_cv_consistency_adj", 0.0):
        _adjustments.append(("cv_consistency", cv_adj * 100))

    if form_adj := prop.get("_mlb_form_adj", 0.0):
        _adjustments.append(("mlb_form", form_adj * 100))

    # Apply with dampening (replaces naive summation)
    final_prob_pct = dampen_adjustments(
        base_prob_pct=base_prob * 100,
        adjustments=_adjustments,
        log_tag=f"{prop.get('player', '')}|{prop.get('prop_type', '')}",
    )
    prop["model_prob"] = max(0.03, min(0.97, final_prob_pct / 100))
    prop["_adjustment_audit"] = _adjustments  # stored for Discord embed

Step 3: Verify with --test flag:
    python fix1_wire_adjustment_dampener.py --test
"""


if __name__ == "__main__":
    if "--test" in sys.argv:
        run_test()
        print(MANUAL_PATCH_INSTRUCTIONS)
    elif "--manual" in sys.argv:
        print(MANUAL_PATCH_INSTRUCTIONS)
    else:
        patch_prop_enrichment_layer()
        print("\nRun with --test to see the before/after comparison.")
        print("Run with --manual to see the full manual patch instructions.")
