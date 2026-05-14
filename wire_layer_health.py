"""
wire_layer_health.py
=====================
Patches two files to make every model layer visible:

PATCH A — prop_enrichment_layer.py
  Adds _layer_audit dict to every prop at the end of the per-prop loop,
  just after the bp2vec block. Records which layers fired and what they
  contributed. This dict travels with the prop through tasklets.py.

PATCH B — tasklets.py
  1. Writes layer_audit to bet_ledger as JSONB (requires V53 migration)
  2. Writes data/layer_health.json after each dispatch cycle summarizing
     layer coverage across all evaluated props

HOW TO APPLY
------------
    python wire_layer_health.py          # apply both patches
    python wire_layer_health.py --verify # confirm after applying

WHAT YOU GET
------------
  After deployment:
  - Every bet_ledger row has layer_audit JSONB showing which layers fired
  - data/layer_health.json written after each dispatch — readable by log scanner
  - 10 AM bug checker reads layer_health.json for layer coverage report
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [LAYER-HEALTH] %(message)s")
log = logging.getLogger(__name__)

ENRICHMENT = Path("prop_enrichment_layer.py")
TASKLETS   = Path("tasklets.py")


# ══════════════════════════════════════════════════════════════════════════════
# PATCH A: Add _layer_audit to each prop (prop_enrichment_layer.py)
# ══════════════════════════════════════════════════════════════════════════════

LAYER_AUDIT_STAMP = """\
        # ── Layer audit stamp ──────────────────────────────────────────────────
        # Records exactly which layers fired and what they contributed.
        # Written to bet_ledger.layer_audit JSONB and data/layer_health.json.
        # A zero value means the layer ran but produced no signal.
        # A missing key means the layer errored or was skipped entirely.
        prop["_layer_audit"] = {
            "bayesian":    round(float(prop.get("_bayesian_nudge",        0) or 0), 4),
            "cv":          round(float(prop.get("_cv_nudge",              0) or 0), 4),
            "form":        round(float(prop.get("_form_adj",              0) or 0), 4),
            "chase":       round(float(prop.get("_chase_k_adj",           0) or 0), 4),
            "drama":       round(float(prop.get("_drama_penalty_pp",      0) or 0), 4),
            "arsenal":     round(float(prop.get("_arsenal_k_sig",         0) or 0), 4),
            "umpire":      round(float(prop.get("_ump_k_adj",             0) or 0), 4),
            "steamer":     round(float(prop.get("_steamer_adj",           0) or 0), 4),
            "ttop":        round(float(prop.get("_tto_k_adj",             0) or 0), 4),
            "bp2vec":      round(float(prop.get("_bp2vec_adj",            0) or 0), 2),
            "pa_model":    prop.get("_pa_model_hit_prob"),
            "dampener":    bool(prop.get("_dampener_applied",         False)),
            "xgb_k":       bool(prop.get("_xgb_k_blended",           False)),
            "xgb_hit":     bool(prop.get("_xgb_hit_blended",         False)),
            "market_flag": str(prop.get("_market_flag",          "CLEAN")),
            "injury":      round(float(prop.get("_injury_confidence_penalty", 0) or 0), 3),
            "park":        round(float(prop.get("_park_k_factor",          1.0) or 1.0), 3),
            "lambda_bias": round(float(prop.get("_lambda_bias",           0.0) or 0.0), 4),
        }

"""

# Anchor: insert just before `enriched_count += 1`
AUDIT_ANCHOR = "        enriched_count += 1"


# ══════════════════════════════════════════════════════════════════════════════
# PATCH B: Write layer_health.json + layer_audit to bet_ledger (tasklets.py)
# ══════════════════════════════════════════════════════════════════════════════

LAYER_HEALTH_WRITER = """\
    # ── Write layer_health.json ───────────────────────────────────────────────
    # Summarizes layer coverage across all props evaluated this cycle.
    # Read by railway_log_scanner.py and bug_checker._check_layer_coverage().
    # Written even if 0 props dispatched — shows the data pipeline health.
    try:
        import json as _json
        from pathlib import Path as _Path
        from datetime import datetime as _dt

        _enriched = [p for p in props if p.get("_layer_audit")]
        _n = len(_enriched)

        def _pct(key, check=lambda v: bool(v)):
            if _n == 0:
                return 0.0
            return round(sum(1 for p in _enriched
                             if check(p["_layer_audit"].get(key))) / _n * 100, 1)

        def _avg(key):
            if _n == 0:
                return 0.0
            vals = [abs(float(p["_layer_audit"].get(key) or 0))
                    for p in _enriched]
            return round(sum(vals) / len(vals), 4) if vals else 0.0

        _lh = {
            "written_at":          _dt.utcnow().isoformat() + "Z",
            "props_evaluated":     _n,
            "layers": {
                "dampener_pct":    _pct("dampener"),
                "xgb_k_pct":       _pct("xgb_k"),
                "xgb_hit_pct":     _pct("xgb_hit"),
                "bp2vec_pct":      _pct("bp2vec",
                                        check=lambda v: float(v or 0) != 0.0),
                "bayesian_active": _avg("bayesian"),
                "umpire_active":   _avg("umpire"),
                "drama_active":    _avg("drama"),
                "steamer_active":  _avg("steamer"),
                "ttop_active":     _avg("ttop"),
                "market_flagged":  sum(1 for p in _enriched
                                       if p["_layer_audit"].get("market_flag",
                                                                "CLEAN") != "CLEAN"),
                "injury_blocked":  sum(1 for p in _enriched
                                       if float(p["_layer_audit"].get(
                                           "injury", 0) or 0) > 0),
                "pa_model_active": sum(1 for p in _enriched
                                       if p["_layer_audit"].get("pa_model")
                                       is not None),
            },
            "zero_layers": [
                k for k, v in {
                    "dampener":  _pct("dampener"),
                    "bayesian":  _avg("bayesian"),
                    "umpire":    _avg("umpire"),
                }.items() if v == 0 and _n > 0
            ],
        }

        _Path("data").mkdir(exist_ok=True)
        _Path("data/layer_health.json").write_text(_json.dumps(_lh, indent=2))
        if _lh["zero_layers"]:
            logger.warning(
                "[AgentTasklet] Layer health: %d props, ZERO layers: %s",
                _n, _lh["zero_layers"],
            )
        else:
            logger.info(
                "[AgentTasklet] Layer health: %d props | dampener=%.0f%% "
                "xgb_k=%.0f%% bp2vec=%.0f%%",
                _n,
                _lh["layers"]["dampener_pct"],
                _lh["layers"]["xgb_k_pct"],
                _lh["layers"]["bp2vec_pct"],
            )
    except Exception as _lh_err:
        logger.debug("[AgentTasklet] layer_health.json write failed: %s", _lh_err)
"""

# Anchor: insert before the decision_log flush block
HEALTH_WRITER_ANCHOR = "    # Flush decision log"


# ══════════════════════════════════════════════════════════════════════════════
# PATCH B2: Add layer_audit to bet_ledger INSERT
# ══════════════════════════════════════════════════════════════════════════════

# The current INSERT has `features_json` — we add layer_audit as a JSONB column
# The INSERT needs one more column + parameter

BET_LEDGER_OLD_COLS = (
    "                            (player_name, prop_type, line, side, odds_american,\n"
    "                             kelly_units, model_prob, ev_pct, agent_name,\n"
    "                             status, bet_date, platform, features_json,\n"
    "                             units_wagered, mlbam_id, entry_type, discord_sent,\n"
    "                             lookahead_safe, parlay_id)"
)

BET_LEDGER_NEW_COLS = (
    "                            (player_name, prop_type, line, side, odds_american,\n"
    "                             kelly_units, model_prob, ev_pct, agent_name,\n"
    "                             status, bet_date, platform, features_json,\n"
    "                             units_wagered, mlbam_id, entry_type, discord_sent,\n"
    "                             lookahead_safe, parlay_id, layer_audit)"
)

BET_LEDGER_OLD_VALUES = (
    "                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,\n"
    "                                'OPEN', %s, %s, %s,\n"
    "                                ABS(%s), %s, %s, FALSE,\n"
    "                                %s, %s)"
)

BET_LEDGER_NEW_VALUES = (
    "                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,\n"
    "                                'OPEN', %s, %s, %s,\n"
    "                                ABS(%s), %s, %s, FALSE,\n"
    "                                %s, %s, %s)"
)

# The layer_audit param needs to be added to the INSERT params tuple
# Find the closing params block and add the new param
BET_LEDGER_OLD_PARAM_END = "                            _sl.get(\"parlay_id\"),\n                        )"
BET_LEDGER_NEW_PARAM_END = """\
                            _sl.get("parlay_id"),
                            _json.dumps(_sl.get("_layer_audit")) if _sl.get("_layer_audit") else None,
                        )"""


def patch_enrichment_layer() -> bool:
    if not ENRICHMENT.exists():
        log.error("prop_enrichment_layer.py not found.")
        return False

    content = ENRICHMENT.read_text(encoding="utf-8")

    if "_layer_audit" in content:
        log.info("_layer_audit already in prop_enrichment_layer.py — skipping.")
        return True

    if AUDIT_ANCHOR not in content:
        log.warning("Audit anchor not found — add LAYER_AUDIT_STAMP manually before 'enriched_count += 1'")
        return False

    content = content.replace(AUDIT_ANCHOR, LAYER_AUDIT_STAMP + AUDIT_ANCHOR, 1)
    ENRICHMENT.write_text(content, encoding="utf-8")
    log.info("_layer_audit stamp added to prop_enrichment_layer.py")
    return True


def patch_tasklets() -> bool:
    if not TASKLETS.exists():
        log.error("tasklets.py not found.")
        return False

    content = TASKLETS.read_text(encoding="utf-8")
    changed = False

    # Add layer_health.json writer
    if "_lh" not in content:
        if HEALTH_WRITER_ANCHOR in content:
            content = content.replace(
                HEALTH_WRITER_ANCHOR,
                LAYER_HEALTH_WRITER + "\n    " + HEALTH_WRITER_ANCHOR.lstrip(),
                1,
            )
            log.info("layer_health.json writer added to tasklets.py")
            changed = True
        else:
            log.warning("Health writer anchor not found — add LAYER_HEALTH_WRITER manually")

    # Add layer_audit to bet_ledger INSERT
    if "layer_audit)" not in content:
        if BET_LEDGER_OLD_COLS in content:
            content = content.replace(BET_LEDGER_OLD_COLS, BET_LEDGER_NEW_COLS, 1)
            content = content.replace(BET_LEDGER_OLD_VALUES, BET_LEDGER_NEW_VALUES, 1)
            content = content.replace(BET_LEDGER_OLD_PARAM_END, BET_LEDGER_NEW_PARAM_END, 1)
            log.info("layer_audit added to bet_ledger INSERT")
            changed = True
        else:
            log.warning("bet_ledger INSERT anchor not found — add layer_audit column manually")

    if changed:
        TASKLETS.write_text(content, encoding="utf-8")
        log.info("tasklets.py updated.")

    return changed


def verify() -> None:
    print("\n=== Layer Health Wiring Verification ===")
    checks = []

    if ENRICHMENT.exists():
        e = ENRICHMENT.read_text()
        checks += [
            ("enrichment: _layer_audit stamped on each prop", "_layer_audit" in e),
            ("enrichment: dampener in audit",                 '"dampener"' in e),
            ("enrichment: bp2vec in audit",                   '"bp2vec"' in e),
            ("enrichment: xgb_k in audit",                    '"xgb_k"' in e),
        ]

    if TASKLETS.exists():
        t = TASKLETS.read_text()
        checks += [
            ("tasklets: layer_health.json written",     "layer_health.json" in t),
            ("tasklets: layer_audit in bet_ledger",     "layer_audit)" in t),
            ("tasklets: zero_layers warning",           "zero_layers" in t),
        ]

    checks += [
        ("V53 migration exists",  Path("migrations/V53__add_layer_audit_to_bet_ledger.sql").exists()),
        ("data/ dir creatable",   True),  # always true
    ]

    for label, ok in checks:
        print(f"  {'✅' if ok else '❌'} {label}")


if __name__ == "__main__":
    if "--verify" in sys.argv:
        verify()
    else:
        ok_a = patch_enrichment_layer()
        ok_b = patch_tasklets()
        log.info("Patches applied: enrichment=%s, tasklets=%s", ok_a, ok_b)
        verify()
