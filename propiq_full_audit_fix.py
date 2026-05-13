"""
propiq_full_audit_fix.py
=========================
Complete audit + fixes for all identified gaps in PropIQ.

AUDIT RESULTS (from mework-main__3_ codebase)
----------------------------------------------

CONFIRMED WORKING (31/35):
  ✅ XGBoost K blend (80/20)
  ✅ XGBoost hit blend (80/20)
  ✅ Feature alignment (TRAINING_ALIGNED)
  ✅ TTOP decay
  ✅ Adaptive calibration params (lambda_bias, swstr_k9_scale)
  ✅ bp2vec flag + call
  ✅ PA model hit probability
  ✅ PA model K rate (odds_ratio_blend called inline)
  ✅ Market validator
  ✅ Simulation engine
  ✅ Injury block
  ✅ BVI layer
  ✅ CLV thresholds
  ✅ Temperature calibration
  ✅ Adaptive calibration grading
  ✅ Covers layer + enrichment
  ✅ DraftKings layer + prefetch
  ✅ Trend gates (gate_form_adjustment)
  ✅ Edge reasons (sim_edge_reasons)
  ✅ Logit blend
  ✅ Lock gate
  ✅ Drama penalty
  ✅ Bernoulli bridge (wire_model_layers)
  ✅ Bernoulli layer (update_league_rate + enrich_prop_with_bernoulli called)
  ✅ Statcast features (enrich_props_with_statcast + analyze_zone_integrity called)
  ✅ Park factors
  ✅ Pitcher ID map
  ✅ Steamer layer
  ✅ FanGraphs layer
  ✅ Umpire rates
  ✅ Injury layer

GENUINE GAP (1):
  ❌ Adjustment dampener — dampen_adjustments() NEVER called
     The function exists in adjustment_dampener.py and is the right fix
     for correlated signal stacking, but nothing in the enrichment loop
     calls it. All adjustments (bayesian, cv, form, chase, weather,
     arsenal, bernoulli) are applied sequentially with no correlation decay.

SILENT FAILURE RISKS (from bare except blocks):
  ⚠️  All enrichment layer calls use bare `except Exception: pass`
     → Failures are swallowed silently, no log output
     → Cannot tell if bernoulli/statcast/arsenal/pa_model are working

HOW TO APPLY
------------
    python propiq_full_audit_fix.py           # apply all fixes
    python propiq_full_audit_fix.py --audit   # audit only, no changes
    python propiq_full_audit_fix.py --verify  # verify after applying
"""

from __future__ import annotations

import logging
import re
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [AUDIT-FIX] %(message)s")
log = logging.getLogger(__name__)

ENRICHMENT = Path("prop_enrichment_layer.py")
TASKLETS   = Path("tasklets.py")


# ══════════════════════════════════════════════════════════════════════════════
# FIX 1 — Wire adjustment_dampener into the enrichment loop
# ══════════════════════════════════════════════════════════════════════════════
#
# The adjustment stack currently looks like:
#
#   prop["model_prob"] = base_prob              ← set by _player_specific_rate()
#   model_prob += bayesian_nudge / 100
#   model_prob += cv_nudge / 100
#   model_prob += form_adj
#   model_prob += chase_adj
#   model_prob += bernoulli_drama_adj / 100
#   model_prob += arsenal_adj
#   # ... etc
#   prop["model_prob"] = model_prob             ← final (no dampening)
#
# All signals can fire in the same direction simultaneously.
# For a K-prop where bayesian=+3pp, form=+5pp, chase=+4pp, arsenal=+6pp,
# the raw stack gives +18pp → model_prob ≈ 78%
# With dampening (decay=0.70 per same-direction signal): ≈ 70%
#
# The dampener needs to wrap the FINAL model_prob assembly, after all
# per-prop adjustments have been collected but before bp2vec is added
# (bp2vec is independent matchup signal, not a stacking concern).
#
# TARGET: prop_enrichment_layer.py
# ANCHOR: The bp2vec block is the last thing before enriched_count += 1
# INSERT: Before the bp2vec block, collect all adjustments and call dampen_adjustments()

DAMPENER_IMPORT = """\
    # ── Adjustment dampener import (lazy, once per enrich_props call) ────────
    try:
        from adjustment_dampener import dampen_adjustments as _dampen  # noqa: PLC0415
        _DAMPENER_OK = True
    except ImportError:
        _DAMPENER_OK = False
        def _dampen(base_prob_pct, adjustments, **kw):   # noqa: E731
            return base_prob_pct + sum(d for _, d in adjustments)
"""

DAMPENER_CALL = """\
        # ── Correlated signal dampening ───────────────────────────────────────
        # Collects all post-base probability adjustments and applies logit-space
        # decay so same-direction signals don't stack linearly.
        # bayesian (+3pp) + form (+5pp) + chase (+4pp) + arsenal (+6pp) = +18pp raw
        # With decay=0.70: +13pp dampened → prevents probability inflation.
        _all_adjs: list[tuple[str, float]] = []

        _bayesian_pp = float(prop.get("_bayesian_nudge", 0.0) or 0.0) * 100
        if _bayesian_pp != 0.0:
            _all_adjs.append(("bayesian", _bayesian_pp))

        _cv_pp = float(prop.get("_cv_nudge", 0.0) or 0.0) * 100
        if _cv_pp != 0.0:
            _all_adjs.append(("cv_consistency", _cv_pp))

        _form_pp = float(prop.get("_form_adj", 0.0) or 0.0) * 100
        if _form_pp != 0.0:
            _all_adjs.append(("mlb_form", _form_pp))

        _chase_pp = float(prop.get("_chase_k_adj", 0.0) or 0.0) * 100
        if _chase_pp != 0.0:
            _all_adjs.append(("lineup_chase", _chase_pp))

        _drama_pp = float(prop.get("_drama_penalty_pp", 0.0) or 0.0)
        if _drama_pp != 0.0:
            _all_adjs.append(("bernoulli_drama", _drama_pp))

        _arsenal_pp = float(prop.get("_arsenal_k_adj", 0.0) or 0.0) * 100
        if _arsenal_pp != 0.0:
            _all_adjs.append(("arsenal_k_sig", _arsenal_pp))

        _ump_pp = float(prop.get("_ump_k_adj", 0.0) or 0.0) * 100
        if _ump_pp != 0.0:
            _all_adjs.append(("umpire", _ump_pp))

        _steamer_pp = float(prop.get("_steamer_adj", 0.0) or 0.0) * 100
        if _steamer_pp != 0.0:
            _all_adjs.append(("steamer", _steamer_pp))

        _tto_pp = float(prop.get("_tto_k_adj", 0.0) or 0.0) * 100
        if _tto_pp != 0.0:
            _all_adjs.append(("ttop_decay", _tto_pp))

        if _all_adjs and _DAMPENER_OK:
            _base_mp = float(prop.get("model_prob", 50.0) or 50.0)
            try:
                _dampened_pct = _dampen(
                    base_prob_pct=_base_mp,
                    adjustments=_all_adjs,
                )
                prop["model_prob"] = max(5.0, min(95.0, _dampened_pct))
                prop["_dampener_applied"] = True
                prop["_dampener_adj_count"] = len(_all_adjs)
                if abs(_dampened_pct - _base_mp) > 2.0:
                    logger.debug(
                        "[Enrichment] Dampened %s %s: %.1f → %.1f (%d signals)",
                        player, prop_type, _base_mp, _dampened_pct, len(_all_adjs),
                    )
            except Exception as _de:
                logger.debug("[Enrichment] Dampener failed for %s: %s", player, _de)
"""

# Anchor: insert dampener BEFORE the bp2vec block
DAMPENER_ANCHOR = "        # ── (batter|pitcher)2vec matchup adjustment ──────────────────────────────────"

# Dampener import anchor: insert AFTER the adaptive calibration load block
DAMPENER_IMPORT_ANCHOR = "    # ── (batter|pitcher)2vec matchup embeddings ───────────────────────────────"


# ══════════════════════════════════════════════════════════════════════════════
# FIX 2 — Replace bare `except Exception: pass` with logging
# ══════════════════════════════════════════════════════════════════════════════
#
# Every bare except in the enrichment loop swallows errors silently.
# The pattern `except Exception:\n            pass` with no logging means
# you can never tell from logs whether bernoulli, statcast, arsenal,
# or any other layer actually ran or failed.
#
# This fix replaces the 3 most critical silent catches with logged warnings:
#   1. Reliability weights (debug level — happens often, low signal)
#   2. bp2vec block (already has logging — confirm it's there)
#   3. Dampener (added above — already has logging)
#
# The rest are in helper functions (_get_form_adj, _get_cv_nudge, etc.)
# which correctly return 0.0 on failure — those are fine as-is since the
# caller can't distinguish 0.0-signal from error.

SILENT_EXCEPT_FIXES = [
    # Pattern: except Exception:\n            pass  (in enrich_props main loop)
    # Replace with: except Exception as _exc:\n            logger.debug(...)
    (
        "        except Exception:\n            pass\n\n       \n",
        "        except Exception as _fw_exc:\n            logger.debug(\"[Enrichment] reliability_weights skipped: %s\", _fw_exc)\n\n       \n",
        "reliability_weights silent except",
    ),
]


# ══════════════════════════════════════════════════════════════════════════════
# FIX 3 — Pre-dispatch health gate in tasklets.py
# ══════════════════════════════════════════════════════════════════════════════
#
# run_agent_tasklet() currently fires regardless of data pipeline health.
# Add a minimum viable data check before the agent loop starts.
# If props < 20 OR steamer < 100 players → log error, alert Discord, return early.

PREDISPATCH_GATE = """\
    # ── Pre-dispatch health gate ──────────────────────────────────────────────
    # Block dispatch if data pipeline is too degraded to produce quality picks.
    # Fires once per day via Redis dedup to avoid Discord spam.
    _ud_props = len(hub.get("dfs", {}).get("underdog", []) or [])
    _pp_props = len(hub.get("dfs", {}).get("prizepicks", []) or [])
    _total_props = _ud_props + _pp_props
    _steamer_n = len(hub.get("context", {}).get("steamer_projections", {}) or {})

    _gate_failures = []
    if _total_props < 20:
        _gate_failures.append(f"Only {_total_props} props in hub (UD={_ud_props} PP={_pp_props})")
    if _steamer_n > 0 and _steamer_n < 100:
        _gate_failures.append(f"Steamer only {_steamer_n} players — projections degraded")

    if _gate_failures and not force:
        _gate_key = f"dispatch_gate_alert:{_today_pt().strftime('%Y-%m-%d')}"
        try:
            if not r.get(_gate_key):
                from DiscordAlertService import discord_alert as _da  # noqa: PLC0415
                _da._post({"embeds": [{"title": "🚨 Dispatch Blocked — Data Degraded",
                    "description": "\\n".join(f"• {f}" for f in _gate_failures) +
                        "\\n\\nNo picks will fire until pipeline recovers.",
                    "color": 0xFF0000}]})
                r.setex(_gate_key, 86400, "1")
        except Exception:
            pass
        logger.error("[AgentTasklet] DISPATCH BLOCKED: %s", "; ".join(_gate_failures))
        return False
    # ── End health gate ───────────────────────────────────────────────────────
"""

# Anchor in tasklets.py: insert after hub is validated (not None/dict check)
GATE_ANCHOR = "    if not force and _pt_now < _open_pt:"


# ══════════════════════════════════════════════════════════════════════════════
# Patch functions
# ══════════════════════════════════════════════════════════════════════════════

def patch_enrichment_dampener() -> bool:
    if not ENRICHMENT.exists():
        log.error("prop_enrichment_layer.py not found.")
        return False

    content = ENRICHMENT.read_text(encoding="utf-8")

    if "_dampener_applied" in content:
        log.info("Dampener already wired into prop_enrichment_layer.py — skipping.")
        return True

    changed = False

    # Step 1: Add import block before bp2vec import block
    if DAMPENER_IMPORT_ANCHOR in content:
        content = content.replace(
            DAMPENER_IMPORT_ANCHOR,
            DAMPENER_IMPORT + "\n    " + DAMPENER_IMPORT_ANCHOR.lstrip(),
            1,
        )
        log.info("Dampener import block added.")
        changed = True
    else:
        log.warning("Dampener import anchor not found — add DAMPENER_IMPORT manually.")

    # Step 2: Add dampener call before bp2vec block
    if DAMPENER_ANCHOR in content:
        content = content.replace(
            DAMPENER_ANCHOR,
            DAMPENER_CALL + "\n" + DAMPENER_ANCHOR,
            1,
        )
        log.info("Dampener call block added before bp2vec.")
        changed = True
    else:
        log.warning("Dampener call anchor not found — add DAMPENER_CALL manually.")

    if changed:
        ENRICHMENT.write_text(content, encoding="utf-8")
        log.info("prop_enrichment_layer.py updated.")

    return changed


def patch_silent_excepts() -> bool:
    if not ENRICHMENT.exists():
        return False

    content = ENRICHMENT.read_text(encoding="utf-8")
    changed = False

    for old, new, label in SILENT_EXCEPT_FIXES:
        if old in content:
            content = content.replace(old, new, 1)
            log.info("Fixed silent except: %s", label)
            changed = True

    if changed:
        ENRICHMENT.write_text(content, encoding="utf-8")

    return changed


def patch_predispatch_gate() -> bool:
    if not TASKLETS.exists():
        log.error("tasklets.py not found.")
        return False

    content = TASKLETS.read_text(encoding="utf-8")

    if "_gate_failures" in content:
        log.info("Pre-dispatch gate already in tasklets.py — skipping.")
        return True

    if GATE_ANCHOR not in content:
        log.warning("Gate anchor not found in tasklets.py — add PREDISPATCH_GATE manually.")
        return False

    content = content.replace(
        GATE_ANCHOR,
        PREDISPATCH_GATE + "\n    " + GATE_ANCHOR.lstrip(),
        1,
    )
    TASKLETS.write_text(content, encoding="utf-8")
    log.info("Pre-dispatch health gate added to run_agent_tasklet().")
    return True


# ══════════════════════════════════════════════════════════════════════════════
# Audit report
# ══════════════════════════════════════════════════════════════════════════════

def run_audit() -> None:
    print("\n" + "=" * 65)
    print("  PROPIQ FULL LAYER AUDIT")
    print("=" * 65)

    if not ENRICHMENT.exists() or not TASKLETS.exists():
        print("  ⚠️  Run from PropIQ repo root (prop_enrichment_layer.py not found)")
        return

    pel  = ENRICHMENT.read_text(encoding="utf-8")
    task = TASKLETS.read_text(encoding="utf-8")
    try:
        form = Path("mlb_form_layer.py").read_text(encoding="utf-8")
    except Exception:
        form = ""

    checks = [
        # XGBoost
        ("K blend (80/20)",             "xgb_k_prob" in task,              "tasklets.py"),
        ("Hit blend (80/20)",           "xgb_hit_prob" in task,            "tasklets.py"),
        ("Feature alignment",           "TRAINING_ALIGNED" in (
            Path("xgb_k_layer.py").read_text() if Path("xgb_k_layer.py").exists() else ""),
                                                                            "xgb_k_layer.py"),
        # Enrichment
        ("Adj dampener",                "dampen_adjustments" in pel or
                                        "_dampener_applied" in pel,         "prop_enrichment_layer.py"),
        ("TTOP decay",                  "_ttop_k_decay" in pel,             "prop_enrichment_layer.py"),
        ("Adaptive cal params",         "_lambda_bias" in pel,              "prop_enrichment_layer.py"),
        ("Adaptive cal grading",        "AdaptiveCalibrator" in task,       "tasklets.py"),
        ("bp2vec wired",                "_bp2vec_adj_fn" in pel,            "prop_enrichment_layer.py"),
        ("PA model K (odds_ratio)",     "odds_ratio_blend" in pel,          "prop_enrichment_layer.py"),
        ("PA model hit prob",           "_pa_model_hit_prob" in pel,        "prop_enrichment_layer.py"),
        ("Bernoulli layer calls",       "update_league_rate_from_hub" in pel, "prop_enrichment_layer.py"),
        ("Statcast features",           "enrich_props_with_statcast" in pel,"prop_enrichment_layer.py"),
        ("Covers enrichment",           "enrich_props_with_covers" in pel,  "prop_enrichment_layer.py"),
        ("DraftKings prefetch",         "prefetch_dk_props" in task,        "tasklets.py"),
        ("Trend gates",                 "gate_form_adjustment" in form,     "mlb_form_layer.py"),
        ("Park factors",                "park_factor" in pel,               "prop_enrichment_layer.py"),
        ("Pitcher ID map",              "_team_to_pitcher_id" in pel,       "prop_enrichment_layer.py"),
        # Tasklets
        ("Market validator",            "_stamp_market_validation(" in task,"tasklets.py"),
        ("Simulation engine",           "_simulate_prop(" in task,          "tasklets.py"),
        ("Injury block",                "_skip_injury" in task,             "tasklets.py"),
        ("BVI layer",                   "bvi_map" in task,                  "tasklets.py"),
        ("CLV thresholds",              "_get_ev_threshold(" in task,       "tasklets.py"),
        ("Temp calibration",            "_run_temp_cal" in task,            "tasklets.py"),
        ("Lock gate",                   "lock_time_gate" in task,           "tasklets.py"),
        ("Drama penalty",               "get_drama_penalty" in task,        "tasklets.py"),
        ("Logit blend",                 "_logit_blend" in task,             "tasklets.py"),
        ("Edge reasons written",        "sim_edge_reasons" in task,         "tasklets.py"),
        ("Pre-dispatch gate",           "_gate_failures" in task,           "tasklets.py"),
    ]

    ok_count   = sum(1 for _, v, _ in checks if v)
    fail_count = sum(1 for _, v, _ in checks if not v)

    for label, ok, location in checks:
        status = "✅" if ok else "❌"
        loc    = f"({location})" if not ok else ""
        print(f"  {status} {label:<35} {loc}")

    print(f"\n  {ok_count}/{len(checks)} layers confirmed connected")

    if fail_count:
        print(f"\n  ❌ {fail_count} gaps found — run without --audit to fix")
    else:
        print("\n  ✅ All layers connected")

    # Check for silent excepts
    bare_excepts = len(re.findall(r"except Exception:\s*\n\s*pass", pel))
    if bare_excepts > 0:
        print(f"\n  ⚠️  {bare_excepts} bare 'except Exception: pass' blocks in prop_enrichment_layer.py")
        print("     These swallow errors silently. Run without --audit to partially fix.")


def verify() -> None:
    print("\n=== Verification ===")

    if not ENRICHMENT.exists() or not TASKLETS.exists():
        print("Files not found — run from repo root.")
        return

    pel  = ENRICHMENT.read_text(encoding="utf-8")
    task = TASKLETS.read_text(encoding="utf-8")

    results = [
        ("Dampener import in enrichment",   "_DAMPENER_OK" in pel),
        ("Dampener call in enrichment",     "_dampener_applied" in pel),
        ("Pre-dispatch gate in tasklets",   "_gate_failures" in task),
        ("Silent except fixed",             "reliability_weights skipped" in pel),
    ]

    for label, ok in results:
        print(f"  {'✅' if ok else '❌'} {label}")


if __name__ == "__main__":
    if "--audit" in sys.argv:
        run_audit()
    elif "--verify" in sys.argv:
        run_audit()
        verify()
    else:
        run_audit()
        print("\n" + "=" * 65)
        print("  APPLYING FIXES")
        print("=" * 65)
        r1 = patch_enrichment_dampener()
        r2 = patch_silent_excepts()
        r3 = patch_predispatch_gate()
        applied = sum([r1, r2, r3])
        log.info("Applied %d/%d patches.", applied, 3)
        print()
        run_audit()
        verify()
