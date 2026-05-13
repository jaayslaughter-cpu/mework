"""
patch_bug_checker.py
=====================
Patches bug_checker.py to add two new checks from railway_log_scanner.py:
  - _check_railway_silent_failures  (scans logs for silent failure patterns)
  - _check_pipeline_health          (verifies core pipeline fired correctly)

Also adds /admin/scan-logs endpoint to orchestrator.py for on-demand scans.

RUN:
    python patch_bug_checker.py           # apply patches
    python patch_bug_checker.py --verify  # confirm applied
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [PATCH] %(message)s")
log = logging.getLogger(__name__)

BUG_CHECKER  = Path("bug_checker.py")
ORCHESTRATOR = Path("orchestrator.py")


# ── Patch 1: bug_checker.py ───────────────────────────────────────────────────

BUG_CHECKER_IMPORT = """\
# ── Railway log scanner (silent failure detection) ────────────────────────────
try:
    from railway_log_scanner import (
        _check_railway_silent_failures,
        _check_pipeline_health,
    )
except ImportError:
    def _check_railway_silent_failures():
        return "Silent Failures", "warn", "railway_log_scanner.py not found — copy to repo root"
    def _check_pipeline_health():
        return "Pipeline Health", "warn", "railway_log_scanner.py not found — copy to repo root"
"""

# Anchor: insert after the existing imports block, before the BANNED_PROPS line
BUG_CHECKER_IMPORT_ANCHOR = "# ── Banned prop types"

# The two new checks go at the TOP of the checks list (run first)
BUG_CHECKER_CHECKS_OLD = "    checks = [\n        _check_postgres,"
BUG_CHECKER_CHECKS_NEW = """\
    checks = [
        _check_railway_silent_failures,   # NEW: silent failure log scan
        _check_pipeline_health,            # NEW: pipeline activity confirmation
        _check_postgres,"""


# ── Patch 2: orchestrator.py — /admin/scan-logs endpoint ──────────────────────

SCAN_LOGS_ENDPOINT = '''

@app.get("/admin/scan-logs")
async def admin_scan_logs(hours: int = 6):
    """
    On-demand Railway log scan for silent failures.
    Scans the last N hours of propiq_army.log for known failure patterns.
    Posts results to Discord if DISCORD_WEBHOOK_URL is set.

    Usage: GET /admin/scan-logs?hours=12
    """
    try:
        from railway_log_scanner import scan_logs, _check_pipeline_health  # noqa: PLC0415
        findings = scan_logs(hours=hours)
        fails  = [(n, s, d, c) for n, s, d, c in findings if s == "fail"]
        warns  = [(n, s, d, c) for n, s, d, c in findings if s == "warn"]

        # Post to Discord if findings
        if findings and os.getenv("DISCORD_WEBHOOK_URL"):
            from railway_log_scanner import post_silent_failure_report  # noqa: PLC0415
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, post_silent_failure_report)

        _, ph_status, ph_detail = _check_pipeline_health()

        return JSONResponse({
            "status":         "ok",
            "hours_scanned":  hours,
            "failures":       len(fails),
            "warnings":       len(warns),
            "pipeline_health": ph_status,
            "pipeline_detail": ph_detail,
            "findings": [
                {"name": n, "severity": s, "hits": c, "description": d[:200]}
                for n, s, d, c in findings
            ],
        })
    except Exception as exc:
        return JSONResponse({"status": "error", "error": str(exc)}, status_code=500)
'''

# Anchor: insert before the last app route or at the end of the route section
SCAN_LOGS_ANCHOR = '@app.get("/admin/force-dispatch")'


def patch_bug_checker() -> bool:
    if not BUG_CHECKER.exists():
        log.error("bug_checker.py not found — run from PropIQ repo root.")
        return False

    content = BUG_CHECKER.read_text(encoding="utf-8")

    if "_check_railway_silent_failures" in content:
        log.info("bug_checker.py already patched — skipping.")
        return True

    # Add import block
    if BUG_CHECKER_IMPORT_ANCHOR in content:
        content = content.replace(
            BUG_CHECKER_IMPORT_ANCHOR,
            BUG_CHECKER_IMPORT + "\n" + BUG_CHECKER_IMPORT_ANCHOR,
            1,
        )
        log.info("Added railway_log_scanner imports to bug_checker.py")
    else:
        log.warning("Import anchor not found — add import block manually.")

    # Add checks to top of checks list
    if BUG_CHECKER_CHECKS_OLD in content:
        content = content.replace(BUG_CHECKER_CHECKS_OLD, BUG_CHECKER_CHECKS_NEW, 1)
        log.info("Added _check_railway_silent_failures and _check_pipeline_health to checks list.")
    else:
        log.warning("Checks list anchor not found — add checks manually.")

    BUG_CHECKER.write_text(content, encoding="utf-8")
    log.info("bug_checker.py updated.")
    return True


def patch_orchestrator() -> bool:
    if not ORCHESTRATOR.exists():
        log.warning("orchestrator.py not found — skipping /admin/scan-logs endpoint.")
        return False

    content = ORCHESTRATOR.read_text(encoding="utf-8")

    if "/admin/scan-logs" in content:
        log.info("orchestrator.py already has /admin/scan-logs — skipping.")
        return True

    if SCAN_LOGS_ANCHOR in content:
        content = content.replace(
            SCAN_LOGS_ANCHOR,
            SCAN_LOGS_ENDPOINT + "\n\n" + SCAN_LOGS_ANCHOR,
            1,
        )
        ORCHESTRATOR.write_text(content, encoding="utf-8")
        log.info("Added /admin/scan-logs endpoint to orchestrator.py")
        return True
    else:
        log.warning("/admin/force-dispatch anchor not found in orchestrator.py")
        log.warning("Add SCAN_LOGS_ENDPOINT manually before the force-dispatch route.")
        return False


def verify() -> None:
    print("\n=== Patch Verification ===")

    checks = []
    if BUG_CHECKER.exists():
        bc = BUG_CHECKER.read_text()
        checks += [
            ("bug_checker: silent failure import",   "from railway_log_scanner import" in bc),
            ("bug_checker: import fallback present",  "_check_railway_silent_failures" in bc),
            ("bug_checker: checks list updated",      "_check_pipeline_health" in bc),
            ("bug_checker: scanner runs FIRST",
             bc.find("_check_railway_silent_failures") < bc.find("_check_postgres")),
        ]

    if ORCHESTRATOR.exists():
        oc = ORCHESTRATOR.read_text()
        checks += [
            ("orchestrator: /admin/scan-logs endpoint", "/admin/scan-logs" in oc),
        ]

    checks.append(("railway_log_scanner.py exists", Path("railway_log_scanner.py").exists()))

    for label, ok in checks:
        print(f"  {'✅' if ok else '❌'} {label}")

    if all(v for _, v in checks):
        print("\n  All patches applied. Drop railway_log_scanner.py in repo root and deploy.")
    else:
        print("\n  Some patches missing. Run without --verify to apply.")


if __name__ == "__main__":
    if "--verify" in sys.argv:
        verify()
    else:
        patch_bug_checker()
        patch_orchestrator()
        verify()
