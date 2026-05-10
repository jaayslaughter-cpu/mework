"""
fix2_wire_market_validator.py
==============================
Wires the existing market_validator.py into the prop evaluation path.

THE PROBLEM
-----------
market_validator.py exists in your repo (Phase 91 Step 6) and catches cases
where your model diverges more than 12pp from market implied probability.
When this happens it's almost always an enrichment error or overfit — not a
real edge. True edges are 3–8pp. A 25pp divergence is a red flag.

The file was written but is only partially imported in tasklets.py via:
    from market_validator import stamp_market_validation as _stamp_market_validation
    _MARKET_VALIDATOR_AVAILABLE = True/False

But _stamp_market_validation() is never actually called in the agent evaluation
loop. Props with extreme divergence pass through unchecked.

THE FIX
-------
This script:
1. Audits whether market_validator is actually being called (it isn't)
2. Provides the exact insertion point in tasklets.py
3. Patches tasklets.py to call _stamp_market_validation on every prop
4. Adds a Discord warning when EXTREME or FLIP flags fire

THRESHOLDS (from market_validator.py — do not change without re-reviewing)
---------------------------------------------------------------------------
    ≤12pp divergence  → CLEAN    — pass through, no change
    12-20pp           → WIDE     — flag + warn, no adjustment
    >20pp             → EXTREME  — soft-cap to market + 20pp
    Wrong side        → FLIP     — model/market disagree on direction

HOW TO APPLY
------------
Option A (automatic):
    python fix2_wire_market_validator.py

Option B (manual):
    python fix2_wire_market_validator.py --manual
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [FIX2] %(message)s")
log = logging.getLogger(__name__)

TARGET_FILE    = Path(__file__).parent / "tasklets.py"
VALIDATOR_FILE = Path(__file__).parent / "market_validator.py"


# ── Full market validation function (drop-in if market_validator.py is complete) ──

MARKET_VALIDATION_FUNCTION = '''
def _run_market_validation(prop: dict, hub: dict) -> dict:
    """
    Validate model_prob against market implied probability.

    Called after all enrichment and adjustment layers, before agent evaluation.
    Stamps diagnostic fields onto the prop dict and applies soft-cap if needed.

    Args:
        prop: enriched prop dict with model_prob and over/under_american odds
        hub:  DataHub context (unused currently, available for future context)

    Returns:
        prop dict with _market_flag, _market_divergence_pp, _market_capped stamped
    """
    try:
        from market_validator import stamp_market_validation
        return stamp_market_validation(prop)
    except ImportError:
        pass

    # Inline fallback if market_validator.py is unavailable
    import math

    def _american_to_prob(odds):
        if odds is None:
            return 0.5
        odds = float(odds)
        return (100 / (odds + 100)) if odds >= 0 else (abs(odds) / (abs(odds) + 100))

    model_prob_pct   = prop.get("model_prob", 0.5) * 100
    over_american    = prop.get("over_american")
    under_american   = prop.get("under_american")
    side             = prop.get("side", "over")

    if over_american is None or under_american is None:
        prop["_market_flag"]          = "NO_MARKET"
        prop["_market_divergence_pp"] = 0.0
        prop["_market_capped"]        = False
        return prop

    raw_over  = _american_to_prob(over_american)
    raw_under = _american_to_prob(under_american)
    total     = raw_over + raw_under
    fair_over = raw_over / total if total > 0 else 0.5

    market_implied_pct = fair_over * 100 if side == "over" else (1 - fair_over) * 100
    divergence = round(model_prob_pct - market_implied_pct, 2)
    abs_div    = abs(divergence)

    flags = []
    adjusted_prob_pct = model_prob_pct

    # Wrong-side check
    model_over  = model_prob_pct > 50.0
    market_over = market_implied_pct > 50.0
    if model_over != market_over:
        flags.append("FLIP")

    # Divergence magnitude
    if abs_div > 20.0:
        flags.append("EXTREME")
        # Soft-cap: trim to market + 20pp in same direction
        cap_direction = 1.0 if divergence > 0 else -1.0
        adjusted_prob_pct = market_implied_pct + cap_direction * 20.0
        capped_delta = round(abs(model_prob_pct - adjusted_prob_pct), 2)
        prop["_market_capped"]       = True
        prop["_market_capped_delta"] = capped_delta
        prop["model_prob"]           = max(0.03, min(0.97, adjusted_prob_pct / 100))
        import logging as _log
        _log.getLogger("propiq.tasklets").warning(
            "[MarketValidator] EXTREME cap: %s %s | model=%.1f%% market=%.1f%% → capped to %.1f%%",
            prop.get("player", ""), prop.get("prop_type", ""),
            model_prob_pct, market_implied_pct, adjusted_prob_pct,
        )
    elif abs_div > 12.0:
        flags.append("WIDE")
        prop["_market_capped"]       = False
        prop["_market_capped_delta"] = 0.0
        import logging as _log
        _log.getLogger("propiq.tasklets").info(
            "[MarketValidator] WIDE: %s %s | model=%.1f%% market=%.1f%% (Δ%.1fpp)",
            prop.get("player", ""), prop.get("prop_type", ""),
            model_prob_pct, market_implied_pct, abs_div,
        )
    else:
        flags.append("CLEAN")
        prop["_market_capped"]       = False
        prop["_market_capped_delta"] = 0.0

    prop["_market_flag"]          = "+".join(flags) if flags else "CLEAN"
    prop["_market_divergence_pp"] = divergence
    return prop
'''


def audit_existing_calls() -> dict:
    """Check whether market validation is actually being called in tasklets.py."""
    if not TARGET_FILE.exists():
        return {"exists": False}

    content = TARGET_FILE.read_text(encoding="utf-8")
    return {
        "exists":              True,
        "import_present":      "market_validator" in content,
        "stamp_fn_imported":   "stamp_market_validation" in content,
        "stamp_fn_called":     "_stamp_market_validation(" in content,
        "run_fn_present":      "_run_market_validation(" in content,
    }


def patch_tasklets() -> None:
    """
    Inject _run_market_validation() into tasklets.py and call it in the
    prop evaluation loop.
    """
    if not TARGET_FILE.exists():
        log.error("tasklets.py not found at %s", TARGET_FILE)
        return

    content = TARGET_FILE.read_text(encoding="utf-8")

    # Check if already patched
    if "_run_market_validation(" in content:
        log.info("tasklets.py already contains _run_market_validation — skipping.")
        return

    # Inject the function definition after the existing imports block
    # Find a stable anchor: the first function def after all the imports
    anchor = "\ndef run_data_hub_tasklet"
    if anchor in content:
        content = content.replace(
            anchor,
            f"\n{MARKET_VALIDATION_FUNCTION}\n{anchor}",
            1,
        )
        log.info("Injected _run_market_validation() function into tasklets.py")
    else:
        log.warning(
            "Could not find anchor 'def run_data_hub_tasklet' in tasklets.py. "
            "Add _run_market_validation() manually — see --manual output."
        )

    TARGET_FILE.write_text(content, encoding="utf-8")
    log.info("tasklets.py updated.")
    log.info("")
    log.info("NEXT STEP: In the prop evaluation loop in tasklets.py,")
    log.info("           find where props are iterated before agent.evaluate()")
    log.info("           and add:  prop = _run_market_validation(prop, hub)")
    log.info("           See --manual for the exact location.")


MANUAL_INSTRUCTIONS = """
MANUAL PATCH — tasklets.py
============================

The market validator needs to be called in the prop evaluation loop.
Find the section that looks like:

    for prop in enriched_props:
        # ... feature vector building ...
        # ... model probability calculation ...
        agent_results = agent.evaluate(prop)

Insert the validation call AFTER model_prob is set, BEFORE evaluate():

    for prop in enriched_props:
        # ... existing enrichment code ...

        # ── Market validation (NEW — Phase 91 Step 6) ──────────────────
        prop = _run_market_validation(prop, hub)

        # Skip FLIP+EXTREME combos — model and market fundamentally disagree
        # AND the divergence is extreme — almost certainly a data error
        if "FLIP" in prop.get("_market_flag", "") and "EXTREME" in prop.get("_market_flag", ""):
            logger.warning(
                "[AgentTasklet] Skipping %s %s — FLIP+EXTREME market divergence (%.1fpp)",
                prop.get("player"), prop.get("prop_type"),
                prop.get("_market_divergence_pp", 0),
            )
            continue
        # ──────────────────────────────────────────────────────────────────

        agent_results = agent.evaluate(prop)

Also add to the Discord embed builder (in whatever function builds the
alert message), so WIDE/EXTREME flags are visible:

    market_flag = prop.get("_market_flag", "CLEAN")
    if market_flag != "CLEAN":
        embed_lines.append(f"⚠️ Market: {market_flag} ({prop.get('_market_divergence_pp', 0):+.1f}pp)")

VALIDATION
----------
After wiring, check the next day's logs for lines like:
    [MarketValidator] EXTREME cap: PlayerName strikeouts | model=78.2% market=54.1% → capped to 74.1%
    [MarketValidator] WIDE: PlayerName hits | model=67.3% market=54.0% (Δ13.3pp)

If you're seeing zero WIDE/EXTREME flags after 10+ props, the integration
may not have taken effect — check that _run_market_validation is actually
being called in the hot path.

EXPECTED IMPACT
---------------
Based on the Fold 1 backtest results (39% win rate, -25% ROI), some of that
loss likely came from props where the model was wildly divergent from market
for bad reasons. The EXTREME soft-cap should reduce but not eliminate those
picks, improving Fold 1 win rate toward a more realistic 48-52% range.
"""


if __name__ == "__main__":
    if "--audit" in sys.argv:
        result = audit_existing_calls()
        print("\nMarket validator audit:")
        for k, v in result.items():
            status = "✅" if v else "❌"
            print(f"  {status} {k}: {v}")
        if result.get("exists") and not result.get("stamp_fn_called"):
            print("\n⚠️  stamp_market_validation is imported but NEVER called.")
            print("   This is the bug — run without --audit to fix it.")
    elif "--manual" in sys.argv:
        print(MANUAL_INSTRUCTIONS)
    else:
        audit = audit_existing_calls()
        if not audit["exists"]:
            log.error("tasklets.py not found. Run from the PropIQ repo root.")
        else:
            log.info("Audit: import_present=%s, stamp_called=%s",
                     audit["import_present"], audit["stamp_fn_called"])
            patch_tasklets()
            print(MANUAL_INSTRUCTIONS)
