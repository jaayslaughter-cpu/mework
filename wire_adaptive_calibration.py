"""
wire_adaptive_calibration.py
=============================
Wires propiq_adaptive_calibration.py into two places:

WIRE A — tasklets.py / run_grading_tasklet()
    After temperature calibration runs nightly, call AdaptiveCalibrator.update()
    with that day's graded bet records. Parameters update automatically.

WIRE B — prop_enrichment_layer.py / enrich_props()
    Load current calibrated params at the start of each enrichment pass
    and apply lambda_bias + swstr_k9_scale to K props.

THE PROBLEM THIS SOLVES
-----------------------
propiq_adaptive_calibration.py exists in the repo with pre-loaded BBE
live calibration values (lambda_bias=-0.067, swstr_k9_scale=16.0).
But neither lambda_bias nor swstr_k9_scale is referenced anywhere in
prop_enrichment_layer.py or tasklets.py — they're dead parameters.

Without this wiring:
  - K props systematically over-predict strikeouts by ~6.7% (lambda_bias=0)
  - SwStr% delta signal is 2x too strong (swstr_k9_scale=30 not 16)
  - Parameters accumulate in data/calibration_params.json but nothing reads them

HOW TO APPLY
------------
    python wire_adaptive_calibration.py          # apply both wires
    python wire_adaptive_calibration.py --verify # confirm
    python wire_adaptive_calibration.py --status # show current param values

AFTER APPLYING
--------------
Run: python propiq_adaptive_calibration.py --status
Expected: shows lambda_bias=-0.067, swstr_k9_scale=16.0
These flow into K-prop Poisson lambda on next evaluation cycle.
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [ADAPTIVE-CAL] %(message)s")
log = logging.getLogger(__name__)

TASKLETS    = Path("tasklets.py")
ENRICHMENT  = Path("prop_enrichment_layer.py")
PARAMS_PATH = Path("data") / "calibration_params.json"


# ══════════════════════════════════════════════════════════════════════════════
# WIRE A: Grading tasklet — update calibrator after each nightly grading cycle
# ══════════════════════════════════════════════════════════════════════════════

GRADING_UPDATE_CODE = """\
    # ── Adaptive calibration update (propiq_adaptive_calibration.py) ─────────
    # Runs after temperature calibration. Reads today's graded bet records from
    # bet_ledger and updates lambda_bias, swstr_k9_scale, ump_scale in
    # data/calibration_params.json. Parameters take effect on the next
    # DataHub cycle (prop_enrichment_layer.py reads them at enrich_props start).
    try:
        from propiq_adaptive_calibration import AdaptiveCalibrator as _AdaptiveCal  # noqa: PLC0415
        _adaptive_cal = _AdaptiveCal()
        _cal_result   = _adaptive_cal.run_daily_update()
        if _cal_result.get("updated"):
            logger.info(
                "[GradingTasklet] Adaptive calibration updated: %s",
                {k: round(v, 4) for k, v in _cal_result.get("new_params", {}).items()},
            )
        else:
            logger.info(
                "[GradingTasklet] Adaptive calibration: n=%d (phase threshold: %d)",
                _cal_result.get("n_graded", 0),
                _cal_result.get("phase_threshold", 30),
            )
    except Exception as _ac_err:
        logger.warning("[GradingTasklet] Adaptive calibration failed (non-fatal): %s", _ac_err)
"""

# Anchor: insert after the temperature calibration block, before the next def
GRADING_ANCHOR = "    except Exception as _tc_err:\n        logger.warning(\"[GradingTasklet] Temperature calibration failed (non-fatal): %s\", _tc_err)"


# ══════════════════════════════════════════════════════════════════════════════
# WIRE B: prop_enrichment_layer — load params at start of enrich_props
# ══════════════════════════════════════════════════════════════════════════════

ENRICHMENT_LOAD_CODE = """\
    # ── Load adaptive calibration params ─────────────────────────────────────
    # Reads data/calibration_params.json (written nightly by adaptive calibration).
    # Provides lambda_bias and swstr_k9_scale to K-prop Poisson computation.
    # Falls back to BBE-calibrated defaults if file not found.
    _lambda_bias    = -0.067   # BBE live default: systematic K over-prediction
    _swstr_k9_scale = 16.0     # BBE live default: reduced from 30→16 in 2026
    _ump_scale      = 0.9      # BBE live default
    try:
        from propiq_adaptive_calibration import AdaptiveCalibrator as _AdaptiveCal  # noqa: PLC0415
        _cal_params      = _AdaptiveCal().load_params()
        _lambda_bias     = _cal_params.get("lambda_bias",     _lambda_bias)
        _swstr_k9_scale  = _cal_params.get("swstr_k9_scale",  _swstr_k9_scale)
        _ump_scale       = _cal_params.get("ump_scale",        _ump_scale)
    except Exception:
        pass   # use BBE defaults above
"""

# Per-prop K adjustment code — applied inside the loop for K props
PER_PROP_K_CODE = """\
        # ── Apply adaptive calibration to K props ──────────────────────────────
        # lambda_bias corrects systematic K over/under-prediction.
        # swstr_k9_scale converts SwStr% delta → K/9 contribution.
        # Both params updated nightly by propiq_adaptive_calibration.py.
        if prop_type in ("strikeouts", "pitcher_strikeouts"):
            # Stamp params on prop so XGBoost build and Poisson lambda can use them
            prop["_lambda_bias"]    = _lambda_bias
            prop["_swstr_k9_scale"] = _swstr_k9_scale
            prop["_ump_scale"]      = _ump_scale
            # Adjust k_rate if already computed
            if prop.get("k_rate"):
                prop["k_rate"] = float(prop["k_rate"]) + _lambda_bias
"""

# Anchors for enrichment layer
ENRICHMENT_LOAD_ANCHOR = "    if not props:\n        return props"
ENRICHMENT_CALL_ANCHOR = "        # ── ABS (Automated Ball-Strike) adjustments"


def wire_grading_tasklet() -> bool:
    if not TASKLETS.exists():
        log.error("tasklets.py not found.")
        return False

    content = TASKLETS.read_text(encoding="utf-8")

    if "_AdaptiveCal" in content:
        log.info("Adaptive calibration already wired into tasklets.py — skipping.")
        return True

    if GRADING_ANCHOR not in content:
        log.warning("Grading anchor not found in tasklets.py.")
        log.warning("Add GRADING_UPDATE_CODE manually after the temperature calibration block.")
        return False

    content = content.replace(
        GRADING_ANCHOR,
        GRADING_ANCHOR + "\n\n" + GRADING_UPDATE_CODE,
        1,
    )
    TASKLETS.write_text(content, encoding="utf-8")
    log.info("Wire A: adaptive calibration update added to run_grading_tasklet().")
    return True


def wire_enrichment_layer() -> bool:
    if not ENRICHMENT.exists():
        log.error("prop_enrichment_layer.py not found.")
        return False

    content = ENRICHMENT.read_text(encoding="utf-8")

    if "_lambda_bias" in content:
        log.info("Adaptive calibration already wired into prop_enrichment_layer.py — skipping.")
        return True

    # Add the param load block after the initial guard
    if ENRICHMENT_LOAD_ANCHOR not in content:
        log.warning("Enrichment load anchor not found. Add load block manually.")
        return False

    content = content.replace(
        ENRICHMENT_LOAD_ANCHOR,
        ENRICHMENT_LOAD_ANCHOR + "\n\n" + ENRICHMENT_LOAD_CODE,
        1,
    )

    # Add the per-prop K application before the ABS block
    if ENRICHMENT_CALL_ANCHOR not in content:
        log.warning("Enrichment call anchor not found. Add per-prop K code manually.")
    else:
        content = content.replace(
            ENRICHMENT_CALL_ANCHOR,
            PER_PROP_K_CODE + "\n        " + ENRICHMENT_CALL_ANCHOR,
            1,
        )

    ENRICHMENT.write_text(content, encoding="utf-8")
    log.info("Wire B: adaptive calibration params load added to enrich_props().")
    return True


def show_status() -> None:
    print("\n=== Adaptive Calibration Status ===")

    if not PARAMS_PATH.exists():
        print(f"  ❌ {PARAMS_PATH} not found — calibrator not yet initialized.")
        print("  Run: python propiq_adaptive_calibration.py --status")
        return

    params = json.loads(PARAMS_PATH.read_text())
    print(f"  File: {PARAMS_PATH}")
    for k, v in params.items():
        if k == "notes":
            print(f"  notes: ({len(v)} entries)")
        elif isinstance(v, float):
            print(f"  {k}: {v:.4f}")
        else:
            print(f"  {k}: {v}")

    # BBE reference values
    bbe = {"lambda_bias": -0.067, "swstr_k9_scale": 16.0, "ump_scale": 0.9}
    print("\n  vs BBE live reference:")
    for k, ref in bbe.items():
        cur = params.get(k, "N/A")
        if isinstance(cur, float):
            delta = cur - ref
            flag = f" ({delta:+.3f} from BBE)" if abs(delta) > 0.001 else " ✅ matches BBE"
            print(f"  {k}: {cur:.4f}{flag}")


def verify() -> None:
    print("\n=== Adaptive Calibration Wiring Verification ===")
    checks = []

    if TASKLETS.exists():
        t = TASKLETS.read_text()
        checks.append(("tasklets.py: _AdaptiveCal call in grading",    "_AdaptiveCal" in t))
        checks.append(("tasklets.py: run_daily_update() called",       "run_daily_update" in t))
    else:
        checks.append(("tasklets.py found", False))

    if ENRICHMENT.exists():
        e = ENRICHMENT.read_text()
        checks.append(("enrichment: _lambda_bias loaded",              "_lambda_bias" in e))
        checks.append(("enrichment: _swstr_k9_scale loaded",          "_swstr_k9_scale" in e))
        checks.append(("enrichment: stamped on K props",              "prop[\"_lambda_bias\"]" in e))
    else:
        checks.append(("prop_enrichment_layer.py found", False))

    checks.append(("propiq_adaptive_calibration.py exists",   Path("propiq_adaptive_calibration.py").exists()))
    checks.append(("data/calibration_params.json exists",     PARAMS_PATH.exists()))

    for label, ok in checks:
        print(f"  {'✅' if ok else '❌'} {label}")

    if not PARAMS_PATH.exists():
        print("\n  Initialize: python propiq_adaptive_calibration.py --status")
    else:
        print("\n  Current params:")
        show_status()


if __name__ == "__main__":
    if "--verify" in sys.argv:
        verify()
    elif "--status" in sys.argv:
        show_status()
    else:
        ok_a = wire_grading_tasklet()
        ok_b = wire_enrichment_layer()
        if ok_a and ok_b:
            log.info("Both wires applied. Run --verify to confirm.")
        verify()
