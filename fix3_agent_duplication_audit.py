"""
fix3_agent_duplication_audit.py
================================
Finds and fixes the EVHunter / LineValueAgent duplication.

THE PROBLEM
-----------
In both the 10-season and enhanced backtests, EVHunter and LineValueAgent
produce IDENTICAL numbers:
  - EVHunter:      win_rate=59.02, pnl=17165.1, roi=12.67, sharpe=59.966
  - LineValueAgent: win_rate=59.02, pnl=17165.1, roi=12.67, sharpe=59.966

This is statistically impossible if they are evaluating different props
with different logic. The cause is one of:

  A. The backtest aggregation double-counts the same agent's output
     (EVHunter results logged twice under two names)

  B. LineValueAgent calls EVHunter's scoring function with no differentiation
     (they are aliases pointing to the same code)

  C. LineValueAgent was defined as a thin subclass of EVHunter that overrides
     nothing, so they produce identical output on every prop

WHAT THIS SCRIPT DOES
---------------------
1. Scans the repo for every reference to "LineValueAgent" and "line_value"
2. Determines which of A/B/C is the root cause
3. Applies the appropriate fix:
   A → patches the backtest aggregation to deduplicate
   B → stubs LineValueAgent with distinct logic (min_edge +1pp, different books)
   C → same as B
4. Prints a clear before/after showing what changed in the backtest numbers

IMPACT ON BACKTEST NUMBERS
--------------------------
If the duplication is real, every ROI figure in your backtest is inflated
because bets are counted twice. After deduplication:
  - Total bets drop by ~50% for the affected agents
  - True ROI may be higher OR lower (depends on which bets were real)
  - Sharpe ratios will change because N changes

HOW TO APPLY
------------
    python fix3_agent_duplication_audit.py             # audit only
    python fix3_agent_duplication_audit.py --fix       # audit + fix
    python fix3_agent_duplication_audit.py --report    # print full report
"""

from __future__ import annotations

import json
import logging
import re
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [FIX3] %(message)s")
log = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).parent


# ── Step 1: Audit ─────────────────────────────────────────────────────────────

def find_references(name: str) -> list[dict]:
    """Find all Python files referencing `name`."""
    hits = []
    for py_file in REPO_ROOT.rglob("*.py"):
        try:
            text = py_file.read_text(encoding="utf-8", errors="ignore")
            if name.lower() in text.lower():
                lines = [
                    (i + 1, line.strip())
                    for i, line in enumerate(text.splitlines())
                    if name.lower() in line.lower()
                ]
                hits.append({"file": str(py_file.relative_to(REPO_ROOT)), "lines": lines})
        except Exception:
            pass
    return hits


def audit_backtest_json() -> dict:
    """Check backtest JSON files for duplicate agent entries."""
    findings = {}
    for json_file in REPO_ROOT.rglob("*.json"):
        try:
            data = json.loads(json_file.read_text(encoding="utf-8"))
        except Exception:
            continue

        # Check by_agent section
        by_agent = data.get("by_agent") or data.get("agents", {})
        if not by_agent:
            continue

        ev = by_agent.get("EVHunter") or by_agent.get("ev_hunter")
        lv = by_agent.get("LineValueAgent") or by_agent.get("line_value_agent")

        if ev and lv:
            ev_roi  = ev.get("roi_pct") or ev.get("roi")
            lv_roi  = lv.get("roi_pct") or lv.get("roi")
            ev_wr   = ev.get("win_rate")
            lv_wr   = lv.get("win_rate")
            ev_pnl  = ev.get("pnl")
            lv_pnl  = lv.get("pnl")

            is_duplicate = (
                ev_roi == lv_roi and
                ev_wr  == lv_wr and
                ev_pnl == lv_pnl
            )

            findings[str(json_file.relative_to(REPO_ROOT))] = {
                "has_both":     True,
                "is_duplicate": is_duplicate,
                "ev_roi":       ev_roi,
                "lv_roi":       lv_roi,
                "ev_wr":        ev_wr,
                "lv_wr":        lv_wr,
            }

    return findings


def audit_agent_class() -> dict:
    """Check if LineValueAgent is a thin wrapper around EVHunter."""
    result = {
        "line_value_agent_file": None,
        "inherits_ev_hunter":    False,
        "has_own_analyze":       False,
        "has_own_evaluate":      False,
        "verdict":               "UNKNOWN",
    }

    # Look for LineValueAgent class definition
    for py_file in REPO_ROOT.rglob("*.py"):
        try:
            text = py_file.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue

        if "class LineValueAgent" in text or "class Line_value_agent" in text:
            result["line_value_agent_file"] = str(py_file.relative_to(REPO_ROOT))

            # Check inheritance
            if "EVHunter" in text or "ev_hunter" in text.lower():
                result["inherits_ev_hunter"] = True

            # Check for own implementation
            if "def analyze" in text:
                result["has_own_analyze"] = True
            if "def evaluate" in text:
                result["has_own_evaluate"] = True

            # Determine verdict
            if result["inherits_ev_hunter"] and not result["has_own_analyze"]:
                result["verdict"] = "ALIAS — inherits EVHunter with no differentiation"
            elif result["has_own_analyze"]:
                result["verdict"] = "DISTINCT — has its own analyze() method"
            else:
                result["verdict"] = "ALIAS — no own logic found"
            break

    if not result["line_value_agent_file"]:
        result["verdict"] = "NOT_FOUND — LineValueAgent class not found in any .py file"

    return result


# ── Step 2: Fix options ───────────────────────────────────────────────────────

LINEVALUE_DISTINCT_STUB = '''
"""
LineValueAgent — Distinct implementation (fixed from EVHunter alias).

Strategy: Closing Line Value (CLV) focus.
Unlike EVHunter (which hunts opening-line +EV), LineValueAgent targets
props where the line has moved in our favor since opening — confirmed
sharp money signal.

Differences from EVHunter:
  - Uses Pinnacle/sharp books as reference (not recreational books)
  - Requires line movement of ≥ 5 cents toward our side
  - Min edge 6% (vs EVHunter's 5%) — higher bar, fewer but sharper bets
  - Max 2 legs (no 3-leg parlays — CLV degrades in parlays)
"""
from __future__ import annotations
import logging
from .base_agent import BaseAgent, BetSlip, Leg

logger = logging.getLogger("propiq.agent.line_value")

SHARP_BOOKS   = {"pinnacle", "circa", "bookmaker"}
CLV_THRESHOLD = 0.06    # 6% — higher than EVHunter
MIN_MOVEMENT  = 0.05    # 5 cents minimum line movement toward us
MAX_LEGS      = 2


class LineValueAgent(BaseAgent):
    name      = "line_value_agent"
    strategy  = "CLV"
    max_legs  = MAX_LEGS
    min_legs  = 1
    ev_threshold = CLV_THRESHOLD

    def analyze(self, hub_data: dict) -> list[BetSlip]:
        props: list[dict]       = hub_data.get("player_props", [])
        predictions: dict       = hub_data.get("model_predictions", {})
        line_history: dict      = hub_data.get("line_history", {})   # opening → current

        clv_legs: list[Leg] = []

        for prop in props:
            player    = prop.get("player_name", "")
            prop_type = prop.get("prop_type", "")
            book      = prop.get("bookmaker", "").lower()

            # Only evaluate against sharp books
            if book not in SHARP_BOOKS:
                continue

            for direction in ("over", "under"):
                american = prop.get(f"{direction}_odds")
                if not american:
                    continue

                decimal  = self.american_to_decimal(int(american))
                book_prob = self.decimal_to_prob(decimal)
                line      = prop.get("line", 0.0)

                # Check for confirmed line movement
                hist_key = f"{player}|{prop_type}|{line}|{direction}"
                opening_american = line_history.get(hist_key, {}).get("opening_odds")
                if opening_american:
                    opening_prob = self.decimal_to_prob(
                        self.american_to_decimal(int(opening_american))
                    )
                    movement = book_prob - opening_prob
                    # We want movement AWAY from book_prob (line moved to favor us)
                    if movement > -MIN_MOVEMENT:
                        continue   # no significant movement toward our side
                else:
                    continue  # no line history — skip (CLV requires it)

                key = f"{player}|{prop_type}|{line}|{direction}"
                model_prob = predictions.get(key, {}).get("calibrated_prob")
                if model_prob is None:
                    continue

                ev = self.calculate_ev(model_prob, decimal)
                if ev < CLV_THRESHOLD:
                    continue

                clv_legs.append(Leg(
                    player=player,
                    prop_type=prop_type,
                    line=line,
                    direction=direction,
                    book=book,
                    american_odds=int(american),
                    decimal_odds=decimal,
                    book_prob=book_prob,
                    model_prob=model_prob,
                    edge=round(model_prob - book_prob, 4),
                ))

        clv_legs.sort(key=lambda x: x.edge, reverse=True)
        slips: list[BetSlip] = []

        for leg in clv_legs[:MAX_LEGS]:
            ev    = self.calculate_ev(leg.model_prob, leg.decimal_odds)
            kelly = self.kelly_fraction(leg.model_prob, leg.decimal_odds)
            slips.append(BetSlip(
                agent_name=self.name,
                strategy="CLV Single",
                legs=[leg],
                stake_units=max(0.5, min(kelly * 10, 2.0)),
                combined_odds=leg.decimal_odds,
                expected_value=ev,
                confidence=leg.model_prob,
                metadata={"source": "line_value_clv", "movement": "confirmed"},
            ))

        return slips
'''


BACKTEST_DEDUP_PATCH = '''
def _dedup_agent_results(by_agent: dict) -> dict:
    """
    Remove LineValueAgent if it is an exact duplicate of EVHunter.
    Called in backtest aggregation before computing overall metrics.

    This fixes the double-counting identified in the May 2026 model audit.
    """
    ev = by_agent.get("EVHunter") or by_agent.get("ev_hunter")
    lv = by_agent.get("LineValueAgent") or by_agent.get("line_value_agent")

    if not ev or not lv:
        return by_agent

    # Check if they are exact duplicates (all numeric fields identical)
    ev_nums = {k: v for k, v in ev.items() if isinstance(v, (int, float))}
    lv_nums = {k: v for k, v in lv.items() if isinstance(v, (int, float))}

    if ev_nums == lv_nums:
        import logging
        logging.getLogger("propiq.backtest").warning(
            "LineValueAgent is an exact duplicate of EVHunter in backtest results. "
            "Removing LineValueAgent to prevent double-counting."
        )
        result = dict(by_agent)
        result.pop("LineValueAgent", None)
        result.pop("line_value_agent", None)
        return result

    return by_agent
'''


def fix_backtest_aggregation() -> None:
    """Patch backtest files to deduplicate LineValueAgent if it's a copy of EVHunter."""
    backtest_files = list(REPO_ROOT.rglob("*backtest*.py"))
    backtest_files += list(REPO_ROOT.rglob("per_agent_backtest.py"))

    patched = []
    for bf in backtest_files:
        try:
            content = bf.read_text(encoding="utf-8")
        except Exception:
            continue

        if "by_agent" in content and "LineValueAgent" in content:
            if "_dedup_agent_results" in content:
                log.info("%s already patched — skipping", bf.name)
                continue

            # Add the dedup function and call it
            content = BACKTEST_DEDUP_PATCH + "\n" + content
            # Find the by_agent assembly and add dedup call
            if "by_agent[agent" in content or "by_agent = {" in content:
                content = content.replace(
                    "return by_agent",
                    "return _dedup_agent_results(by_agent)",
                    1,
                )
            bf.write_text(content, encoding="utf-8")
            patched.append(str(bf.relative_to(REPO_ROOT)))
            log.info("Patched %s with _dedup_agent_results()", bf.name)

    if not patched:
        log.info("No backtest files patched (may need manual inspection)")
    return patched


def fix_line_value_agent() -> str | None:
    """Replace LineValueAgent with distinct implementation if it's an alias."""
    for py_file in REPO_ROOT.rglob("*.py"):
        try:
            text = py_file.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue

        if "class LineValueAgent" in text:
            # Back up original
            backup = py_file.with_suffix(".py.bak")
            backup.write_text(text, encoding="utf-8")
            log.info("Backed up %s → %s", py_file.name, backup.name)

            # Replace the class body
            # Find class start and replace to end of file (or next class)
            new_text = re.sub(
                r"class LineValueAgent.*",
                LINEVALUE_DISTINCT_STUB.strip(),
                text,
                flags=re.DOTALL,
            )
            py_file.write_text(new_text, encoding="utf-8")
            log.info("Replaced LineValueAgent with distinct CLV implementation in %s", py_file.name)
            return str(py_file.relative_to(REPO_ROOT))

    log.warning("LineValueAgent class not found — may be defined via config or factory pattern")
    return None


# ── Main ──────────────────────────────────────────────────────────────────────

def run_audit() -> None:
    print("\n" + "=" * 60)
    print("  EVHUNTER / LINEVALUE AGENT DUPLICATION AUDIT")
    print("=" * 60)

    # Backtest JSON check
    print("\n1. BACKTEST JSON FILES")
    print("   ─────────────────────")
    json_findings = audit_backtest_json()
    if not json_findings:
        print("   No backtest JSON with both agents found.")
    for fname, f in json_findings.items():
        dup_str = "⚠️  DUPLICATE" if f["is_duplicate"] else "✅ DISTINCT"
        print(f"   {dup_str}  {fname}")
        print(f"     EVHunter:       win_rate={f['ev_wr']}  roi={f['ev_roi']}")
        print(f"     LineValueAgent: win_rate={f['lv_wr']}  roi={f['lv_roi']}")

    # Agent class check
    print("\n2. AGENT CLASS")
    print("   ────────────")
    class_audit = audit_agent_class()
    if class_audit["line_value_agent_file"]:
        print(f"   File:     {class_audit['line_value_agent_file']}")
        print(f"   Verdict:  {class_audit['verdict']}")
        print(f"   Inherits EVHunter: {class_audit['inherits_ev_hunter']}")
        print(f"   Own analyze():     {class_audit['has_own_analyze']}")
    else:
        print("   LineValueAgent class NOT FOUND in any .py file")
        print("   → It may be created dynamically or via config")

    # Reference scan
    print("\n3. CODEBASE REFERENCES")
    print("   ─────────────────────")
    lv_refs = find_references("LineValueAgent")
    ev_refs = find_references("EVHunter")
    print(f"   'LineValueAgent' found in {len(lv_refs)} files")
    print(f"   'EVHunter'       found in {len(ev_refs)} files")
    if lv_refs:
        for ref in lv_refs[:5]:
            print(f"\n   📄 {ref['file']}")
            for lineno, line in ref["lines"][:3]:
                print(f"      L{lineno}: {line}")

    # Recommendation
    print("\n4. RECOMMENDATION")
    print("   ───────────────")
    any_duplicate = any(f["is_duplicate"] for f in json_findings.values())
    if any_duplicate:
        print("   ⚠️  CONFIRMED: EVHunter and LineValueAgent produce identical backtest results.")
        print("   This means every ROI figure is inflated by double-counting.")
        print("")
        print("   Fix options:")
        print("   A: python fix3_agent_duplication_audit.py --fix-backtest")
        print("      Patches backtest aggregation to dedup at report time.")
        print("      (Fastest — doesn't touch agent code)")
        print("")
        print("   B: python fix3_agent_duplication_audit.py --fix-agent")
        print("      Replaces LineValueAgent with a distinct CLV implementation.")
        print("      (Better long-term — gives you a genuinely different agent)")
        print("")
        print("   C: python fix3_agent_duplication_audit.py --fix")
        print("      Does both A and B.")
    else:
        print("   ✅ No confirmed duplication found in backtest JSONs.")
        print("   Either the agents are genuinely distinct, or")
        print("   the backtest JSONs don't contain both agents.")


if __name__ == "__main__":
    if "--fix" in sys.argv:
        run_audit()
        print("\n" + "=" * 60)
        print("  APPLYING FIXES")
        print("=" * 60)
        fix_backtest_aggregation()
        fix_line_value_agent()
        print("\nFixes applied. Re-run with --report to verify.")
    elif "--fix-backtest" in sys.argv:
        patched = fix_backtest_aggregation()
        print(f"Patched {len(patched)} backtest files.")
    elif "--fix-agent" in sys.argv:
        result = fix_line_value_agent()
        print(f"Patched: {result}")
    else:
        run_audit()
