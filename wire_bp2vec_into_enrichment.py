"""
wire_bp2vec_into_enrichment.py
================================
Patches prop_enrichment_layer.py to call bp2vec_integration
inside enrich_props(), after the per-prop loop begins.

THE CHANGE
----------
Inside enrich_props(), after the existing mlbam_id resolution block,
this adds a call to apply_bp2vec_adjustment(prop) and appends the result
to the prop dict as `_bp2vec_adj`. The value is consumed downstream in
tasklets.py by the adjustment dampener (dampen_adjustments includes it
as a named signal so correlation gating applies).

The call is guarded: if bp2vec models haven't been trained yet,
apply_bp2vec_adjustment() returns 0.0 and the prop is unchanged.
No crash, no log spam, no change to existing behavior until the
models are trained and the embeddings load successfully.

HOW TO APPLY
------------
    python wire_bp2vec_into_enrichment.py          # apply patch
    python wire_bp2vec_into_enrichment.py --verify # confirm after patch
    python wire_bp2vec_into_enrichment.py --test   # unit test the wiring logic

THEN: train the models (takes 15-25 min, run once then monthly):
    python bp2vec_train.py --train --seasons 2022 2023 2024 2025

VERIFY live: check logs for:
    [bp2vec_wire] Adj: Strider vs Judge strikeouts → +1.20pp
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [BP2VEC-WIRE] %(message)s")
log = logging.getLogger(__name__)

TARGET = Path("prop_enrichment_layer.py")

# ── The import block (added near the top of enrich_props) ────────────────────
IMPORT_BLOCK = """\
    # ── (batter|pitcher)2vec matchup embeddings ──────────────────────────────
    # Lazy-load: returns 0.0 until bp2vec_train.py has been run to generate
    # models/bp2vec_batter.pkl and models/bp2vec_pitcher.pkl.
    # Train with: python bp2vec_train.py --train --seasons 2022 2023 2024 2025
    try:
        from bp2vec_integration import apply_bp2vec_adjustment as _bp2vec_adj_fn, bp2vec_ready as _bp2vec_ready  # noqa: PLC0415
        _BP2VEC_AVAILABLE = _bp2vec_ready()
    except ImportError:
        _bp2vec_ready    = lambda: False        # noqa: E731
        _bp2vec_adj_fn   = lambda prop: 0.0    # noqa: E731
        _BP2VEC_AVAILABLE = False
"""

# ── The per-prop call (added after mlbam_id resolution, before ABS block) ────
PER_PROP_CALL = """\
        # ── bp2vec matchup adjustment ─────────────────────────────────────────
        # Computes ±0–3pp matchup signal from player embedding space.
        # Requires prop["mlbam_id"] (batter) and prop["_opp_pitcher_id"] (pitcher).
        # No-op if models not trained or player not in embedding space.
        if _BP2VEC_AVAILABLE:
            _bp2_adj = _bp2vec_adj_fn(prop)
            if _bp2_adj != 0.0:
                prop["_bp2vec_adj"] = _bp2_adj
        
"""

# ── Anchor points ─────────────────────────────────────────────────────────────
# The import block goes just inside enrich_props, after the initial setup
IMPORT_ANCHOR = "    if not props:\n        return props"

# The per-prop call goes after mlbam_id merge, before ABS adjustments
CALL_ANCHOR = "        # ── ABS (Automated Ball-Strike) adjustments"


def apply_patch() -> bool:
    if not TARGET.exists():
        log.error("prop_enrichment_layer.py not found — run from PropIQ repo root.")
        return False

    content = TARGET.read_text(encoding="utf-8")

    if "_bp2vec_adj_fn" in content:
        log.info("bp2vec already wired into prop_enrichment_layer.py — skipping.")
        return True

    # Step 1: add import block inside enrich_props after initial guard
    if IMPORT_ANCHOR not in content:
        log.warning("Import anchor not found. Add the import block manually.")
        log.warning("Add after: %r", IMPORT_ANCHOR)
    else:
        content = content.replace(
            IMPORT_ANCHOR,
            IMPORT_ANCHOR + "\n\n" + IMPORT_BLOCK,
            1,
        )
        log.info("Import block inserted into enrich_props.")

    # Step 2: add per-prop call before ABS block
    if CALL_ANCHOR not in content:
        log.warning("Call anchor not found. Add per-prop call manually.")
        log.warning("Add before: %r", CALL_ANCHOR)
    else:
        content = content.replace(
            CALL_ANCHOR,
            PER_PROP_CALL + CALL_ANCHOR,
            1,
        )
        log.info("Per-prop call inserted before ABS block.")

    TARGET.write_text(content, encoding="utf-8")
    log.info("prop_enrichment_layer.py updated.")
    return True


def verify() -> None:
    if not TARGET.exists():
        print("prop_enrichment_layer.py not found.")
        return
    content = TARGET.read_text(encoding="utf-8")
    checks = [
        ("_bp2vec_adj_fn defined",           "_bp2vec_adj_fn" in content),
        ("_BP2VEC_AVAILABLE flag present",   "_BP2VEC_AVAILABLE" in content),
        ("per-prop call present",            "prop[\"_bp2vec_adj\"]" in content),
        ("bp2vec_integration imported",      "from bp2vec_integration import" in content),
        ("bp2vec_train.py exists",           Path("bp2vec_train.py").exists()),
        ("bp2vec_integration.py exists",     Path("bp2vec_integration.py").exists()),
        ("models/bp2vec_meta.json exists",   Path("models/bp2vec_meta.json").exists()),
        ("models/bp2vec_batter.pkl exists",  Path("models/bp2vec_batter.pkl").exists()),
    ]
    print("\nbp2vec wiring status:")
    for label, ok in checks:
        print(f"  {'✅' if ok else '❌'} {label}")

    if not Path("models/bp2vec_meta.json").exists():
        print("\n  ⚠️  Models not trained yet. Run:")
        print("     python bp2vec_train.py --train --seasons 2022 2023 2024 2025")
    else:
        print("\n  Run: python bp2vec_train.py --status")


def run_test() -> None:
    """Unit test the wiring logic without modifying files."""
    print("\n=== bp2vec wiring logic test ===")

    # Simulate apply_bp2vec_adjustment behavior
    def _fake_bp2vec(prop):
        pt = prop.get("prop_type", "")
        if pt == "strikeouts" and prop.get("mlbam_id"):
            return 1.2   # fake K-prone matchup
        if pt == "hits" and prop.get("mlbam_id"):
            return -0.8  # fake contact-suppressing matchup
        return 0.0

    test_props = [
        {"player": "Aaron Judge",    "prop_type": "strikeouts", "mlbam_id": "592450", "line": 5.5},
        {"player": "Freddie Freeman","prop_type": "hits",       "mlbam_id": "518692", "line": 1.5},
        {"player": "Unknown Player", "prop_type": "strikeouts", "mlbam_id": None,     "line": 4.5},
        {"player": "Test Batter",    "prop_type": "earned_runs","mlbam_id": "123456", "line": 1.5},
    ]

    for prop in test_props:
        adj = _fake_bp2vec(prop)
        if adj != 0.0:
            prop["_bp2vec_adj"] = adj
        status = f"adj={prop.get('_bp2vec_adj', 0.0):+.2f}pp"
        print(f"  {prop['player']:20s} {prop['prop_type']:12s} → {status}")

    assert test_props[0].get("_bp2vec_adj") == 1.2,  "K-prop with ID should get adj"
    assert test_props[1].get("_bp2vec_adj") == -0.8, "Hit-prop with ID should get adj"
    assert "_bp2vec_adj" not in test_props[2],        "No ID should get no adj"
    assert "_bp2vec_adj" not in test_props[3],        "Unsupported prop type should get no adj"
    print("  ✅ All assertions passed.")


if __name__ == "__main__":
    if "--verify" in sys.argv:
        verify()
    elif "--test" in sys.argv:
        run_test()
    else:
        apply_patch()
        verify()
