"""
fix4_season_blender_and_clv_audit.py
=====================================
Two related fixes in one script:

FIX 4A — Season Blender: Force dynamic recalculation per DataHub cycle
FIX 4B — CLV Feedback: Audit and fix sim_edge_reasons population

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

FIX 4A: SEASON BLENDER
=======================

THE PROBLEM
-----------
The logs show: "Season blend: 2026=56.2% | 2025=43.8% (day 45 of season)"
This same value repeats across every hub refresh cycle for hours. SeasonBlender
correctly calculates day-of-season weights, BUT if the blender instance is
created once at module import time and cached, the blend ratio freezes at
startup and never updates — even across day boundaries.

This means on day 46, the model still uses day 45's blend ratio. Over a full
season, this compounds: by September you're still using the May blend weight.

THE FIX
-------
Ensure SeasonBlender._days_played() is called fresh on each DataHub cycle,
not cached. The fix is a single property change: _days_played() already uses
datetime.now() correctly, but the instance that calls it must not be a
module-level singleton.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

FIX 4B: CLV FEEDBACK ENGINE — sim_edge_reasons audit
=====================================================

THE PROBLEM
-----------
clv_feedback_engine.py has a sophisticated adaptive EV threshold system:
  - Proven edge types (win_rate ≥ 0.60, CLV ≥ 0) → lower EV threshold to 2%
  - Noisy edge types (win_rate < 0.48 OR CLV < -2) → raise EV threshold to 5%

This system works by reading `sim_edge_reasons TEXT[]` from bet_ledger rows.
BUT: if those tags are not being written when bets are placed, the entire
adaptive system silently does nothing. Every call to get_threshold() returns
the global default (3%) regardless of historical performance.

THE FIX
-------
This script:
1. Connects to your Postgres DB and samples recent bet_ledger rows
2. Checks whether sim_edge_reasons is populated or always NULL/empty
3. If empty: traces where bets are written to find the missing write
4. Provides the exact code to add to the bet-writing path

HOW TO APPLY
------------
    python fix4_season_blender_and_clv_audit.py --audit-only
    python fix4_season_blender_and_clv_audit.py --fix-blender
    python fix4_season_blender_and_clv_audit.py --audit-clv
    python fix4_season_blender_and_clv_audit.py --fix-all
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [FIX4] %(message)s")
log = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).parent


# ══════════════════════════════════════════════════════════════════════════════
# FIX 4A — Season Blender
# ══════════════════════════════════════════════════════════════════════════════

def audit_season_blender() -> dict:
    """Check whether SeasonBlender is being instantiated fresh each cycle."""
    findings = {
        "blender_file_exists":     False,
        "singleton_pattern_found": False,
        "module_level_instance":   False,
        "tasklets_usage":          [],
        "verdict":                 "UNKNOWN",
    }

    blender_file = REPO_ROOT / "season_blender.py"
    findings["blender_file_exists"] = blender_file.exists()

    if not blender_file.exists():
        findings["verdict"] = "FILE_MISSING"
        return findings

    # Check tasklets.py usage
    tasklets = REPO_ROOT / "tasklets.py"
    if tasklets.exists():
        content = tasklets.read_text(encoding="utf-8", errors="ignore")
        if "SeasonBlender" in content or "season_blender" in content:
            # Find all lines referencing blender
            lines = [
                (i + 1, line.strip())
                for i, line in enumerate(content.splitlines())
                if "season_blender" in line.lower() or "SeasonBlender" in line
            ]
            findings["tasklets_usage"] = lines

            # Check if it's instantiated at module level (outside a function)
            import re
            module_level = re.search(
                r"^_?blender\s*=\s*SeasonBlender",
                content,
                re.MULTILINE,
            )
            if module_level:
                findings["module_level_instance"] = True
                findings["singleton_pattern_found"] = True
                findings["verdict"] = "SINGLETON — blend ratio is frozen at startup"
            else:
                findings["verdict"] = "OK — appears to be instantiated per-call"
        else:
            findings["verdict"] = "NOT_USED — SeasonBlender not referenced in tasklets.py"

    return findings


BLENDER_WRAPPER = '''
# ── Season blend helper — instantiated fresh each cycle, never cached ────────
def _get_season_blend_weights() -> dict:
    """
    Return current season blend weights, recalculated from today's date.

    IMPORTANT: Do NOT cache this at module level. SeasonBlender._days_played()
    uses datetime.now() and must be called fresh each DataHub cycle so the
    blend ratio advances daily instead of freezing at startup.
    """
    try:
        from season_blender import SeasonBlender
        blender = SeasonBlender()   # fresh instance — intentional, not a bug
        return {
            "pitcher_weights": blender.pitcher_weights(),
            "batter_weights":  blender.batter_weights(),
            "days_played":     blender._days_played(),
            "blend_label":     blender.blend_label(),
        }
    except Exception as exc:
        import logging
        logging.getLogger("propiq.tasklets").warning(
            "SeasonBlender unavailable (%s) — using equal blend", exc
        )
        return {
            "pitcher_weights": {},
            "batter_weights":  {},
            "days_played":     0,
            "blend_label":     "fallback_equal",
        }
'''


def fix_season_blender(tasklets_path: Path | None = None) -> None:
    """
    Patch tasklets.py to use _get_season_blend_weights() instead of a
    cached singleton.
    """
    target = tasklets_path or REPO_ROOT / "tasklets.py"
    if not target.exists():
        log.error("tasklets.py not found at %s", target)
        return

    content = target.read_text(encoding="utf-8")

    # Already patched?
    if "_get_season_blend_weights" in content:
        log.info("tasklets.py already has _get_season_blend_weights — skipping.")
        return

    # Remove any module-level singleton
    import re
    content = re.sub(
        r"^(_?blender\s*=\s*SeasonBlender\(\).*?)\n",
        "# REMOVED module-level SeasonBlender singleton (see _get_season_blend_weights below)\n",
        content,
        flags=re.MULTILINE,
    )

    # Add the fresh-instance wrapper before run_data_hub_tasklet
    anchor = "\ndef run_data_hub_tasklet"
    if anchor in content:
        content = content.replace(anchor, f"\n{BLENDER_WRAPPER}\n{anchor}", 1)
        log.info("Injected _get_season_blend_weights() into tasklets.py")
    else:
        log.warning("Anchor not found — add _get_season_blend_weights() manually")

    # Replace singleton usage with fresh call
    content = re.sub(
        r"\b_?blender\.blend_pitcher\(",
        "_get_season_blend_weights(); blender_fresh = __import__('season_blender').SeasonBlender(); blender_fresh.blend_pitcher(",
        content,
    )

    target.write_text(content, encoding="utf-8")
    log.info("Season blender fix applied.")


# ══════════════════════════════════════════════════════════════════════════════
# FIX 4B — CLV Feedback: sim_edge_reasons audit
# ══════════════════════════════════════════════════════════════════════════════

SIM_EDGE_REASONS_WRITE_PATCH = '''
def _build_edge_reasons(prop: dict) -> list[str]:
    """
    Build the sim_edge_reasons tag list for a prop before writing to bet_ledger.

    These tags drive the adaptive EV threshold system in clv_feedback_engine.py.
    Without them, get_threshold() always returns the global default (0.030)
    regardless of how each edge type has historically performed.

    Tags should describe WHY this bet was taken, not just that it was taken.
    The feedback engine groups bets by tag and computes win_rate + CLV per group.

    Add more tags here as new signal sources are integrated.
    """
    reasons = []

    # Model source
    model_source = prop.get("model_source", "")
    if "xgboost" in model_source.lower():
        reasons.append("xgb_model")
    elif "bayesian" in model_source.lower():
        reasons.append("bayesian_model")
    else:
        reasons.append("base_model")

    # Primary edge signal
    if prop.get("_steam_move"):
        reasons.append("steam_move")
    if prop.get("_clv_edge", 0) > 0.02:
        reasons.append("clv_positive")
    if prop.get("_shadow_whiff_active"):
        reasons.append("shadow_whiff")
    if prop.get("_zone_integrity_active"):
        reasons.append("zone_integrity")
    if prop.get("_bayesian_nudge", 0) > 0.01:
        reasons.append("bayesian_nudge")

    # Prop type
    prop_type = prop.get("prop_type", "")
    if prop_type:
        reasons.append(f"prop_{prop_type}")

    # Direction
    side = prop.get("side", prop.get("direction", ""))
    if side:
        reasons.append(f"side_{side}")

    # Market flag
    market_flag = prop.get("_market_flag", "CLEAN")
    if market_flag != "CLEAN":
        reasons.append(f"market_{market_flag.lower()}")

    # Umpire impact
    ump_adj = prop.get("ump_k_adj", 0.0)
    if abs(ump_adj) > 0.5:
        reasons.append("ump_large_adj")

    return reasons if reasons else ["untagged"]


def write_bet_with_edge_reasons(conn, prop: dict, bet_record: dict) -> None:
    """
    Write a bet to bet_ledger with sim_edge_reasons populated.

    Call this instead of the raw INSERT wherever bets are written.
    This is the missing piece that enables clv_feedback_engine to work.

    Args:
        conn:       psycopg2 connection
        prop:       enriched prop dict (source of edge signals)
        bet_record: dict with all bet_ledger columns already populated
    """
    import json as _json

    edge_reasons = _build_edge_reasons(prop)
    bet_record["sim_edge_reasons"] = _json.dumps(edge_reasons)

    cols   = ", ".join(bet_record.keys())
    values = ", ".join(["%s"] * len(bet_record))
    sql    = f"INSERT INTO bet_ledger ({cols}) VALUES ({values}) ON CONFLICT DO NOTHING"

    with conn.cursor() as cur:
        cur.execute(sql, list(bet_record.values()))
    conn.commit()
'''


def audit_clv_feedback(database_url: str | None = None) -> dict:
    """
    Connect to Postgres and check whether sim_edge_reasons is populated.
    Falls back to code analysis if no DB connection available.
    """
    findings = {
        "db_connected":           False,
        "bet_ledger_exists":      False,
        "sim_edge_reasons_col":   False,
        "rows_sampled":           0,
        "rows_with_reasons":      0,
        "rows_without_reasons":   0,
        "pct_populated":          0.0,
        "verdict":                "UNKNOWN",
        "code_analysis":          {},
    }

    db_url = database_url or os.environ.get("DATABASE_URL", "")

    if db_url:
        try:
            import psycopg2
            conn = psycopg2.connect(db_url, connect_timeout=5)
            findings["db_connected"] = True

            with conn.cursor() as cur:
                # Check table exists
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_name = 'bet_ledger'
                    )
                """)
                findings["bet_ledger_exists"] = cur.fetchone()[0]

                if findings["bet_ledger_exists"]:
                    # Check column exists
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.columns
                            WHERE table_name = 'bet_ledger'
                            AND column_name = 'sim_edge_reasons'
                        )
                    """)
                    findings["sim_edge_reasons_col"] = cur.fetchone()[0]

                    if findings["sim_edge_reasons_col"]:
                        # Sample recent rows
                        cur.execute("""
                            SELECT
                                COUNT(*) as total,
                                COUNT(CASE WHEN sim_edge_reasons IS NOT NULL
                                           AND sim_edge_reasons != '[]'
                                           AND sim_edge_reasons != 'null'
                                      THEN 1 END) as has_reasons
                            FROM bet_ledger
                            WHERE created_at >= NOW() - INTERVAL '30 days'
                        """)
                        row = cur.fetchone()
                        total, has_reasons = row
                        findings["rows_sampled"]       = total or 0
                        findings["rows_with_reasons"]  = has_reasons or 0
                        findings["rows_without_reasons"] = (total or 0) - (has_reasons or 0)
                        findings["pct_populated"] = (
                            (has_reasons / total * 100) if total else 0.0
                        )

                        if findings["pct_populated"] < 5.0:
                            findings["verdict"] = "BROKEN — sim_edge_reasons is almost never populated"
                        elif findings["pct_populated"] < 50.0:
                            findings["verdict"] = "PARTIAL — sim_edge_reasons populated on some bets only"
                        else:
                            findings["verdict"] = "OK — sim_edge_reasons appears to be working"

            conn.close()

        except Exception as exc:
            log.warning("DB connection failed: %s", exc)
            findings["verdict"] = f"DB_ERROR — {exc}"

    if not findings["db_connected"]:
        # Fall back to code analysis
        findings["verdict"] = "NO_DB — running code analysis only"
        ledger_writes = []
        for py_file in REPO_ROOT.rglob("*.py"):
            try:
                text = py_file.read_text(encoding="utf-8", errors="ignore")
                if "bet_ledger" in text and ("INSERT" in text or "execute" in text):
                    has_reasons = "sim_edge_reasons" in text
                    ledger_writes.append({
                        "file":       str(py_file.relative_to(REPO_ROOT)),
                        "has_reasons_write": has_reasons,
                    })
            except Exception:
                pass

        findings["code_analysis"]["files_writing_to_ledger"] = ledger_writes
        missing = [f for f in ledger_writes if not f["has_reasons_write"]]
        findings["code_analysis"]["files_missing_reasons"] = missing
        if missing:
            findings["verdict"] = f"CODE — {len(missing)} bet-write locations missing sim_edge_reasons"

    return findings


def print_clv_fix_instructions() -> None:
    print("""
CLV FEEDBACK FIX — Where to add sim_edge_reasons writes
=========================================================

Find every place that writes to bet_ledger (search for "INSERT INTO bet_ledger"
or the DB write function in tasklets.py / live_dispatcher.py).

Replace the raw INSERT with write_bet_with_edge_reasons():

  BEFORE:
    cur.execute(
        "INSERT INTO bet_ledger (player_name, prop_type, ...) VALUES (%s, %s, ...)",
        (player, prop_type, ...)
    )

  AFTER:
    from fix4_season_blender_and_clv_audit import write_bet_with_edge_reasons
    write_bet_with_edge_reasons(conn, prop=enriched_prop, bet_record={
        "player_name": player,
        "prop_type":   prop_type,
        # ... all other columns ...
    })

The write_bet_with_edge_reasons() function calls _build_edge_reasons(prop)
which reads signal flags already stamped on the prop dict by the enrichment
layer. No new data needed — it uses what's already there.

After fixing, run:
    python fix4_season_blender_and_clv_audit.py --audit-clv
Expected: "pct_populated > 80%" for recent bets

Once sim_edge_reasons is populated, clv_feedback_engine.rebuild_thresholds()
(which runs nightly) will start adjusting EV gates per edge type. You'll see
entries in the edge_thresholds table within 48 hours.
""")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    if "--audit-only" in sys.argv or len(sys.argv) == 1:
        print("\n" + "=" * 60)
        print("  FIX 4A — SEASON BLENDER AUDIT")
        print("=" * 60)
        blender_audit = audit_season_blender()
        for k, v in blender_audit.items():
            print(f"  {k}: {v}")

        print("\n" + "=" * 60)
        print("  FIX 4B — CLV FEEDBACK AUDIT")
        print("=" * 60)
        clv_audit = audit_clv_feedback()
        for k, v in clv_audit.items():
            if k != "code_analysis":
                print(f"  {k}: {v}")
        if clv_audit.get("code_analysis"):
            missing = clv_audit["code_analysis"].get("files_missing_reasons", [])
            if missing:
                print(f"\n  Files writing to bet_ledger WITHOUT sim_edge_reasons:")
                for f in missing:
                    print(f"    ❌ {f['file']}")

    if "--fix-blender" in sys.argv or "--fix-all" in sys.argv:
        print("\nFixing season blender...")
        fix_season_blender()

    if "--audit-clv" in sys.argv or "--fix-all" in sys.argv:
        print("\nAuditing CLV feedback...")
        clv_audit = audit_clv_feedback()
        print(f"  Verdict: {clv_audit['verdict']}")
        print(f"  Rows sampled: {clv_audit['rows_sampled']}")
        print(f"  Populated: {clv_audit['pct_populated']:.1f}%")
        if clv_audit["pct_populated"] < 50.0:
            print_clv_fix_instructions()

    if "--print-patch" in sys.argv:
        print(SIM_EDGE_REASONS_WRITE_PATCH)


# Export for use in other scripts
def get_sim_edge_reasons_patch() -> str:
    return SIM_EDGE_REASONS_WRITE_PATCH


if __name__ == "__main__":
    main()
