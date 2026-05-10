"""
run_all_missed_fixes.py
========================
Master runner for all five missed-fix scripts.

Run this from the PropIQ repo root for a complete audit + fix cycle.

USAGE
-----
    python run_all_missed_fixes.py --audit      # audit only, no changes
    python run_all_missed_fixes.py --fix        # apply all fixes
    python run_all_missed_fixes.py --fix-safe   # apply fixes with backup
    python run_all_missed_fixes.py --report     # print full report

FIXES IN THIS BUNDLE
---------------------
Fix 1: Wire adjustment_dampener into prop_enrichment_layer
        (correlated signal stacking — existing file, never called)

Fix 2: Wire market_validator into tasklets evaluation loop
        (extreme divergence goes unchecked — existing file, never called)

Fix 3: Audit and fix EVHunter / LineValueAgent duplication
        (identical backtest results — likely double-counting)

Fix 4: Season blender dynamic refresh + sim_edge_reasons population
        (blend ratio frozen at startup; CLV adaptive thresholds silently broken)
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [FIXES] %(message)s")
log = logging.getLogger(__name__)


def run_audit() -> dict:
    """Run all audits and return findings."""
    from fix1_wire_adjustment_dampener import check_files as check_dampener
    from fix2_wire_market_validator import audit_existing_calls
    from fix3_agent_duplication_audit import audit_backtest_json, audit_agent_class
    from fix4_season_blender_and_clv_audit import audit_season_blender, audit_clv_feedback

    results = {}

    print("\n" + "═" * 65)
    print("  PROPIQ MISSED-FIX AUDIT REPORT")
    print("═" * 65)

    # Fix 1
    print("\n【FIX 1】 Adjustment Dampener Wiring")
    dampener_ok = check_files_exist(
        Path("adjustment_dampener.py"),
        Path("prop_enrichment_layer.py"),
    )
    enrichment_content = Path("prop_enrichment_layer.py").read_text(errors="ignore") if Path("prop_enrichment_layer.py").exists() else ""
    dampener_called = "dampen_adjustments" in enrichment_content
    results["fix1"] = {"files_exist": dampener_ok, "already_wired": dampener_called}
    status = "✅ Already wired" if dampener_called else ("❌ NOT WIRED — adjustment_dampener.py exists but is never called" if dampener_ok else "⚠️  Files missing")
    print(f"  Status: {status}")
    if not dampener_called and dampener_ok:
        print("  Impact: Correlated signals stack without dampening.")
        print("          K-props can reach 79% when honest answer is ~72%.")

    # Fix 2
    print("\n【FIX 2】 Market Validator Wiring")
    mv_audit = audit_existing_calls()
    results["fix2"] = mv_audit
    imported = mv_audit.get("import_present", False)
    called   = mv_audit.get("stamp_fn_called", False)
    if called:
        print("  Status: ✅ Already called in evaluation path")
    elif imported:
        print("  Status: ❌ IMPORTED but never called")
        print("          Props with 25pp model/market divergence pass through unchecked.")
    else:
        print("  Status: ⚠️  market_validator not imported in tasklets.py")

    # Fix 3
    print("\n【FIX 3】 EVHunter / LineValueAgent Duplication")
    json_findings = audit_backtest_json()
    class_audit   = audit_agent_class()
    results["fix3"] = {"json_findings": json_findings, "class_audit": class_audit}
    any_dup = any(f.get("is_duplicate") for f in json_findings.values())
    if any_dup:
        print("  Status: ❌ CONFIRMED DUPLICATE — EVHunter and LineValueAgent")
        print("          produce identical backtest numbers. Every ROI is inflated.")
        for fname, f in json_findings.items():
            if f.get("is_duplicate"):
                print(f"          Affected file: {fname}")
    elif json_findings:
        print("  Status: ✅ No exact duplication found in backtest JSONs")
    else:
        print("  Status: ⚠️  No backtest JSONs with both agents found to compare")
    print(f"  Agent class verdict: {class_audit.get('verdict', 'unknown')}")

    # Fix 4
    print("\n【FIX 4A】 Season Blender Dynamic Refresh")
    blender_audit = audit_season_blender()
    results["fix4a"] = blender_audit
    if blender_audit.get("module_level_instance"):
        print("  Status: ❌ SINGLETON — blend ratio is frozen at startup time")
        print("          Blend weight does not advance daily as intended.")
    elif blender_audit.get("verdict") == "OK — appears to be instantiated per-call":
        print("  Status: ✅ Appears to recalculate per cycle")
    else:
        print(f"  Status: ⚠️  {blender_audit.get('verdict', 'unknown')}")

    print("\n【FIX 4B】 CLV Feedback Engine sim_edge_reasons")
    clv_audit = audit_clv_feedback()
    results["fix4b"] = clv_audit
    if clv_audit["db_connected"]:
        pct = clv_audit["pct_populated"]
        if pct < 5.0:
            print(f"  Status: ❌ BROKEN — only {pct:.0f}% of bets have edge reason tags")
            print("          Adaptive EV thresholds are silently doing nothing.")
        elif pct < 50.0:
            print(f"  Status: ⚠️  PARTIAL — {pct:.0f}% populated (target: >80%)")
        else:
            print(f"  Status: ✅ {pct:.0f}% of recent bets have sim_edge_reasons")
    else:
        print(f"  Status: ⚠️  No DB connection — {clv_audit['verdict']}")
        missing = clv_audit.get("code_analysis", {}).get("files_missing_reasons", [])
        if missing:
            print(f"          {len(missing)} bet-write locations missing edge reason writes:")
            for f in missing[:3]:
                print(f"            {f['file']}")

    # Summary
    print("\n" + "═" * 65)
    print("  SUMMARY")
    print("═" * 65)
    issues = []
    if not results["fix1"].get("already_wired") and results["fix1"].get("files_exist"):
        issues.append("Fix 1: adjustment_dampener not wired")
    if results["fix2"].get("import_present") and not results["fix2"].get("stamp_fn_called"):
        issues.append("Fix 2: market_validator imported but not called")
    if any_dup:
        issues.append("Fix 3: EVHunter/LineValueAgent duplication inflating ROI")
    if results["fix4a"].get("module_level_instance"):
        issues.append("Fix 4A: season blender frozen at startup")
    if results["fix4b"].get("db_connected") and results["fix4b"].get("pct_populated", 100) < 50:
        issues.append("Fix 4B: sim_edge_reasons not being written to bet_ledger")

    if issues:
        print(f"\n  {len(issues)} issue(s) found:")
        for issue in issues:
            print(f"    ❌ {issue}")
        print("\n  Run: python run_all_missed_fixes.py --fix")
    else:
        print("\n  ✅ No issues found — or issues require manual inspection.")

    return results


def check_files_exist(*paths: Path) -> bool:
    return all(p.exists() for p in paths)


def apply_all_fixes(backup: bool = True) -> None:
    """Apply all fixes in order."""
    log.info("Applying all missed fixes...")

    if backup:
        import shutil, datetime
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = Path(f"propiq_backup_{ts}")
        backup_dir.mkdir()
        for fname in ["tasklets.py", "prop_enrichment_layer.py"]:
            if Path(fname).exists():
                shutil.copy(fname, backup_dir / fname)
                log.info("Backed up %s → %s/", fname, backup_dir)

    # Fix 1
    log.info("Applying Fix 1: adjustment dampener wiring...")
    try:
        from fix1_wire_adjustment_dampener import patch_prop_enrichment_layer
        patch_prop_enrichment_layer()
    except Exception as e:
        log.error("Fix 1 failed: %s", e)

    # Fix 2
    log.info("Applying Fix 2: market validator wiring...")
    try:
        from fix2_wire_market_validator import patch_tasklets
        patch_tasklets()
    except Exception as e:
        log.error("Fix 2 failed: %s", e)

    # Fix 3
    log.info("Applying Fix 3: agent duplication fix...")
    try:
        from fix3_agent_duplication_audit import fix_backtest_aggregation, audit_backtest_json
        json_findings = audit_backtest_json()
        if any(f.get("is_duplicate") for f in json_findings.values()):
            fix_backtest_aggregation()
        else:
            log.info("Fix 3: no confirmed duplication — skipping automatic patch")
    except Exception as e:
        log.error("Fix 3 failed: %s", e)

    # Fix 4A
    log.info("Applying Fix 4A: season blender refresh...")
    try:
        from fix4_season_blender_and_clv_audit import audit_season_blender, fix_season_blender
        blender_audit = audit_season_blender()
        if blender_audit.get("module_level_instance"):
            fix_season_blender()
        else:
            log.info("Fix 4A: no singleton pattern found — skipping")
    except Exception as e:
        log.error("Fix 4A failed: %s", e)

    # Fix 4B — CLV (print instructions, too risky to auto-patch DB writes)
    log.info("Fix 4B: CLV feedback — printing manual instructions...")
    try:
        from fix4_season_blender_and_clv_audit import audit_clv_feedback, print_clv_fix_instructions
        clv_audit = audit_clv_feedback()
        if clv_audit.get("pct_populated", 100) < 50:
            print_clv_fix_instructions()
        else:
            log.info("Fix 4B: sim_edge_reasons appears to be adequately populated")
    except Exception as e:
        log.error("Fix 4B audit failed: %s", e)

    log.info("All fixes applied. Run --audit to verify.")


if __name__ == "__main__":
    if "--fix" in sys.argv:
        backup = "--fix-safe" in sys.argv or "--backup" in sys.argv
        apply_all_fixes(backup=backup)
    elif "--audit" in sys.argv or len(sys.argv) == 1:
        run_audit()
    elif "--report" in sys.argv:
        run_audit()
    else:
        print("Usage:")
        print("  python run_all_missed_fixes.py --audit       # audit only")
        print("  python run_all_missed_fixes.py --fix         # apply fixes")
        print("  python run_all_missed_fixes.py --fix-safe    # fix with backup")
