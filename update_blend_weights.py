"""
update_blend_weights.py
========================
Reads model_metrics.json after training and automatically updates the
XGBoost blend weights in xgb_k_layer.py based on actual Brier scores.

THE PROBLEM
-----------
The blend weights (80/20 for K, 70/30 for hits) were set as fixed constants
based on theory, not measurement. Now that we have real Brier scores:
  - Hit model Brier = 0.2668 (WORSE than null at 0.25) → 70/30 is wrong
  - K model Brier = 0.2458 (barely better than null)  → 80/20 is marginal

BLEND SCHEDULE (based on Brier)
--------------------------------
Brier < 0.23:  Model has real edge  → 70/30 (increase XGB weight)
Brier < 0.25:  Marginal edge        → 80/20 (current default)
Brier >= 0.25: Worse than null      → 90/10 (reduce XGB, limit noise)
Brier >= 0.27: Actively hurting     → 95/5  (minimal contribution only)

USAGE
-----
    python update_blend_weights.py           # preview changes (no writes)
    python update_blend_weights.py --apply   # write changes to xgb_k_layer.py
    python update_blend_weights.py --status  # show current blend weights in code
"""

from __future__ import annotations

import json
import logging
import re
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [BLEND] %(message)s")
log = logging.getLogger(__name__)

METRICS_FILE = Path("models/model_metrics.json")
XGB_LAYER    = Path("xgb_k_layer.py")

NULL_BRIER = 0.25  # null model always predicts 50%


def _get_blend_weight(brier: float | None, model_name: str) -> tuple[float, float, str]:
    """
    Return (formula_weight, xgb_weight, reason) based on Brier score.
    formula_weight + xgb_weight = 1.0
    """
    if brier is None:
        return 0.90, 0.10, "no test data — using conservative 90/10"
    if brier < 0.23:
        return 0.70, 0.30, f"Brier {brier:.4f} well below null — strong edge, 70/30"
    if brier < NULL_BRIER:
        return 0.80, 0.20, f"Brier {brier:.4f} marginal edge over null — 80/20"
    if brier < 0.27:
        return 0.90, 0.10, f"Brier {brier:.4f} ≥ null (0.25) — reducing to 90/10"
    return 0.95, 0.05, f"Brier {brier:.4f} actively hurting — minimal 95/5"


def load_metrics() -> dict:
    if not METRICS_FILE.exists():
        log.error("models/model_metrics.json not found — run xgb_k_training.py first")
        return {}
    return json.loads(METRICS_FILE.read_text())


def compute_recommendations(metrics: dict) -> dict:
    """Compute blend weight recommendations from training metrics."""
    recs = {}

    # K models — all share the same blend weight (averaged across lines)
    k_briers = []
    for line in [3.5, 4.5, 5.5, 6.5]:
        key = f"k_{line}"
        b = metrics.get(key, {}).get("brier")
        if b:
            k_briers.append(b)

    avg_k_brier = sum(k_briers) / len(k_briers) if k_briers else None
    fw_k, xgb_k, reason_k = _get_blend_weight(avg_k_brier, "K")
    recs["k"] = {
        "formula_weight": fw_k,
        "xgb_weight":     xgb_k,
        "avg_brier":      round(avg_k_brier, 4) if avg_k_brier else None,
        "reason":         reason_k,
    }

    # Hit model
    hit_brier = metrics.get("hits", {}).get("brier")
    fw_h, xgb_h, reason_h = _get_blend_weight(hit_brier, "hits")
    recs["hits"] = {
        "formula_weight": fw_h,
        "xgb_weight":     xgb_h,
        "brier":          round(hit_brier, 4) if hit_brier else None,
        "reason":         reason_h,
    }

    return recs


def show_current_weights() -> None:
    """Show what blend weights are currently in xgb_k_layer.py."""
    if not XGB_LAYER.exists():
        print("xgb_k_layer.py not found.")
        return
    content = XGB_LAYER.read_text()
    print("\nCurrent blend weights in xgb_k_layer.py:")

    # Find K blend
    m_k = re.search(r"(\d+\.\d+) \* model_prob \+ (\d+\.\d+) \* _xkp", content)
    if m_k:
        xgb_w = float(m_k.group(2))
        print(f"  K props:   formula={1-xgb_w:.0%} / XGB={xgb_w:.0%}")
    else:
        print("  K props:   pattern not found")

    # Find hit blend
    m_h = re.search(r"(\d+\.\d+) \* model_prob \+ (\d+\.\d+) \* _xhp", content)
    if m_h:
        xgb_w = float(m_h.group(2))
        print(f"  Hit props: formula={1-xgb_w:.0%} / XGB={xgb_w:.0%}")
    else:
        print("  Hit props: pattern not found")


def apply_blend_updates(recs: dict, dry_run: bool = True) -> bool:
    """Patch xgb_k_layer.py with recommended blend weights."""
    if not XGB_LAYER.exists():
        log.error("xgb_k_layer.py not found.")
        return False

    content = XGB_LAYER.read_text()
    original = content
    changed = False

    # Update K blend: pattern "0.XX * model_prob + 0.YY * _xkp"
    k_fw  = recs["k"]["formula_weight"]
    k_xgb = recs["k"]["xgb_weight"]

    k_old = re.search(r"(\d+\.\d+) \* model_prob \+ (\d+\.\d+) \* _xkp", content)
    if k_old:
        old_str = k_old.group(0)
        new_str = f"{k_fw:.2f} * model_prob + {k_xgb:.2f} * _xkp"
        if old_str != new_str:
            content = content.replace(old_str, new_str, 1)
            log.info("K blend:   %s → %s (%s)",
                     old_str, new_str, recs["k"]["reason"])
            changed = True
        else:
            log.info("K blend already at %s — no change needed", old_str)
    else:
        log.warning("K blend pattern not found in xgb_k_layer.py")

    # Update hit blend: pattern "0.XX * model_prob + 0.YY * _xhp"
    h_fw  = recs["hits"]["formula_weight"]
    h_xgb = recs["hits"]["xgb_weight"]

    h_old = re.search(r"(\d+\.\d+) \* model_prob \+ (\d+\.\d+) \* _xhp", content)
    if h_old:
        old_str = h_old.group(0)
        new_str = f"{h_fw:.2f} * model_prob + {h_xgb:.2f} * _xhp"
        if old_str != new_str:
            content = content.replace(old_str, new_str, 1)
            log.info("Hit blend: %s → %s (%s)",
                     old_str, new_str, recs["hits"]["reason"])
            changed = True
        else:
            log.info("Hit blend already at %s — no change needed", old_str)
    else:
        log.warning("Hit blend pattern not found in xgb_k_layer.py")

    if dry_run:
        if changed:
            log.info("DRY RUN — changes NOT written. Run with --apply to write.")
        else:
            log.info("No changes needed.")
        return changed

    if changed:
        XGB_LAYER.write_text(content)
        log.info("xgb_k_layer.py updated with new blend weights.")

        # Update calibration_params.json with blend info
        cal_path = Path("data/calibration_params.json")
        if cal_path.exists():
            try:
                cal = json.loads(cal_path.read_text())
                cal["xgb_blend_weights"] = {
                    "k":   {"formula": k_fw,  "xgb": k_xgb},
                    "hits":{"formula": h_fw,  "xgb": h_xgb},
                }
                cal["calibration_notes"] = cal.get("calibration_notes", [])
                from datetime import date
                cal["calibration_notes"].append(
                    f"[{date.today().isoformat()}] Blend weights updated: "
                    f"K={k_fw:.0%}/{k_xgb:.0%} Hits={h_fw:.0%}/{h_xgb:.0%} "
                    f"based on Brier K={recs['k']['avg_brier']} "
                    f"Hits={recs['hits']['brier']}"
                )
                cal_path.write_text(json.dumps(cal, indent=2))
                log.info("calibration_params.json updated with blend weights.")
            except Exception as e:
                log.warning("Failed to update calibration_params.json: %s", e)
    else:
        log.info("No changes needed.")

    return changed


def main() -> None:
    metrics = load_metrics()
    if not metrics:
        return

    recs = compute_recommendations(metrics)

    print("\n=== XGBoost Blend Weight Recommendations ===")
    print(f"  Null model Brier: {NULL_BRIER} (baseline — worse = model is noise)")
    print()

    for model, rec in recs.items():
        brier_str = f"{rec.get('brier') or rec.get('avg_brier') or 'N/A'}"
        print(f"  {model.upper()}")
        print(f"    Brier:    {brier_str}")
        print(f"    Blend:    {rec['formula_weight']:.0%} formula / {rec['xgb_weight']:.0%} XGB")
        print(f"    Reason:   {rec['reason']}")
        print()

    show_current_weights()

    apply_arg = "--apply" in sys.argv
    if "--status" not in sys.argv:
        print(f"\n{'Applying changes...' if apply_arg else 'DRY RUN — use --apply to write changes'}")
        apply_blend_updates(recs, dry_run=not apply_arg)


if __name__ == "__main__":
    main()
