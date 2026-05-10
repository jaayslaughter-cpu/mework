"""
wire_park_and_temperature.py
=============================
Two infrastructure fixes:

  A. PARK FACTOR MERGE — Unified park factor lookup replacing two conflicting tables
  B. TEMPERATURE AUDIT  — Check whether temperature column is stuck at 1.0

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

FIX A: Park Factor Merge
========================
The codebase has two separate park factor tables:
  - park_factors.py:   venue-keyed, per-prop-type multipliers (1.0 = neutral)
  - park_k_factors.py: team-keyed, K-specific factors (100 = neutral)

Both cover all 30 stadiums with different data formats and slightly different
values for some parks (e.g. Fenway K suppression differs between them).
Whichever file gets imported last in the enrichment chain wins, silently.

This module provides get_park_mult() — a single function that:
  1. Checks park_factors.py first (more granular, per-prop-type)
  2. Falls back to park_k_factors.py for K-specific props
  3. Falls back to neutral (1.0) if neither has data

Call it from prop_enrichment_layer.py instead of importing either file directly.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

FIX B: Temperature Column Audit
================================
temperature_calibration.py fits per-agent temperature scalars T after each
night's grading. T is stored in agent_unit_sizing.temperature.

If that column is stuck at 1.0 for all agents, the nightly calibration is
not running. This can happen because:
  1. agent_calibration_data table is empty (no outcomes being written)
  2. The nightly run_grading_tasklet() doesn't call calibrate_temperatures()
  3. MIN_SAMPLES (30) not yet reached — no update until 30+ graded picks

This module provides audit_temperature_column() which connects to Postgres
and reports the current state, and check_calibration_data() which verifies
that outcomes are being written to agent_calibration_data.

USAGE
-----
    python wire_park_and_temperature.py --test        # park factor tests
    python wire_park_and_temperature.py --audit-temp  # temperature audit (needs DB)
    python wire_park_and_temperature.py --show-parks  # print full merged table
"""

from __future__ import annotations

import logging
import sys
from typing import Optional

log = logging.getLogger("propiq.wire_park_temp")

# ══════════════════════════════════════════════════════════════════════════════
# FIX A: Unified Park Factor Lookup
# ══════════════════════════════════════════════════════════════════════════════

# Inline merged table: venue name (lowercase) → prop_type → multiplier
# Merges park_factors.py and park_k_factors.py with park_factors.py taking priority
# for specific prop types, park_k_factors.py used for K-specific resolution.
#
# Format: venue_key → {prop_type: float}  where 1.0 = neutral
#
# K-factor from park_k_factors.py converted: factor/100 → multiplier
# e.g. k_factor=88 (Coors) → 0.88x K multiplier

_UNIFIED_PARK_FACTORS: dict[str, dict[str, float]] = {
    # ── Extreme hitter parks ──────────────────────────────────────────────────
    "coors field": {
        "batting": 1.18, "hits": 1.15, "home_runs": 1.20,
        "total_bases": 1.22, "hits_runs_rbis": 1.20, "runs": 1.22,
        "rbis": 1.18, "strikeouts": 0.88,   # park_k_factors k=88 → 0.88
        "earned_runs": 1.25, "pitching_outs": 0.96,
        "dome": False, "altitude": 5280,
    },
    "great american ball park": {
        "batting": 1.10, "hits": 1.06, "home_runs": 1.15,
        "total_bases": 1.12, "hits_runs_rbis": 1.10, "runs": 1.12,
        "rbis": 1.10, "strikeouts": 1.03,
        "earned_runs": 1.12, "pitching_outs": 0.98,
        "dome": False, "altitude": 490,
    },
    "yankee stadium": {
        "batting": 1.07, "hits": 1.04, "home_runs": 1.13,
        "total_bases": 1.10, "hits_runs_rbis": 1.07, "runs": 1.09,
        "rbis": 1.07, "strikeouts": 1.00,
        "earned_runs": 1.09, "pitching_outs": 0.99,
        "dome": False, "altitude": 55,
    },
    "citizens bank park": {
        "batting": 1.08, "hits": 1.05, "home_runs": 1.10,
        "total_bases": 1.09, "hits_runs_rbis": 1.08, "runs": 1.10,
        "rbis": 1.08, "strikeouts": 1.02,
        "earned_runs": 1.10, "pitching_outs": 0.99,
        "dome": False, "altitude": 20,
    },
    "globe life field": {
        "batting": 1.06, "hits": 1.04, "home_runs": 1.08,
        "total_bases": 1.07, "hits_runs_rbis": 1.06, "runs": 1.07,
        "rbis": 1.06, "strikeouts": 1.01,
        "earned_runs": 1.07, "pitching_outs": 0.99,
        "dome": False, "altitude": 551,
    },
    # ── Pitcher-friendly parks ────────────────────────────────────────────────
    "oracle park": {
        "batting": 0.94, "hits": 0.94, "home_runs": 0.88,
        "total_bases": 0.92, "hits_runs_rbis": 0.93, "runs": 0.91,
        "rbis": 0.93, "strikeouts": 1.02,
        "earned_runs": 0.91, "pitching_outs": 1.02,
        "dome": False, "altitude": 10,
    },
    "petco park": {
        "batting": 0.95, "hits": 0.95, "home_runs": 0.90,
        "total_bases": 0.93, "hits_runs_rbis": 0.94, "runs": 0.93,
        "rbis": 0.94, "strikeouts": 1.01,
        "earned_runs": 0.93, "pitching_outs": 1.01,
        "dome": False, "altitude": 20,
    },
    "loandepot park": {
        "batting": 0.94, "hits": 0.93, "home_runs": 0.87,
        "total_bases": 0.91, "hits_runs_rbis": 0.93, "runs": 0.92,
        "rbis": 0.93, "strikeouts": 0.96,
        "earned_runs": 0.92, "pitching_outs": 1.01,
        "dome": True, "altitude": 6,
    },
    # ── Near-neutral parks ────────────────────────────────────────────────────
    "fenway park": {
        "batting": 1.04, "hits": 1.06,     # green monster inflates hits
        "home_runs": 0.96,                  # deep to right suppresses HR
        "total_bases": 1.03, "hits_runs_rbis": 1.05, "runs": 1.04,
        "rbis": 1.04, "strikeouts": 0.95,  # park_k_factors k=95
        "earned_runs": 1.03, "pitching_outs": 1.00,
        "dome": False, "altitude": 20,
    },
    "wrigley field": {
        "batting": 1.04, "hits": 1.03, "home_runs": 1.05,
        "total_bases": 1.05, "hits_runs_rbis": 1.04, "runs": 1.05,
        "rbis": 1.04, "strikeouts": 0.97,  # park_k_factors k=97
        "earned_runs": 1.05, "pitching_outs": 0.99,
        "dome": False, "altitude": 595,
    },
    "tropicana field": {
        "batting": 0.97, "hits": 0.97, "home_runs": 0.95,
        "total_bases": 0.96, "hits_runs_rbis": 0.97, "runs": 0.96,
        "rbis": 0.97, "strikeouts": 1.04,
        "earned_runs": 0.96, "pitching_outs": 1.02,
        "dome": True, "altitude": 10,
    },
    "t-mobile park": {
        "batting": 0.96, "hits": 0.96, "home_runs": 0.93,
        "total_bases": 0.95, "hits_runs_rbis": 0.96, "runs": 0.95,
        "rbis": 0.96, "strikeouts": 1.01,
        "earned_runs": 0.95, "pitching_outs": 1.01,
        "dome": True, "altitude": 20,
    },
    "chase field": {
        "batting": 1.01, "hits": 1.01, "home_runs": 1.02,
        "total_bases": 1.02, "hits_runs_rbis": 1.01, "runs": 1.01,
        "rbis": 1.01, "strikeouts": 1.02,
        "earned_runs": 1.01, "pitching_outs": 1.00,
        "dome": True, "altitude": 1082,
    },
    "minute maid park": {
        "batting": 0.99, "hits": 0.99, "home_runs": 1.00,
        "total_bases": 0.99, "hits_runs_rbis": 0.99, "runs": 0.99,
        "rbis": 0.99, "strikeouts": 1.01,
        "earned_runs": 0.99, "pitching_outs": 1.01,
        "dome": True, "altitude": 43,
    },
    # ── Default neutral (fallback) ─────────────────────────────────────────────
    "_neutral": {
        "batting": 1.0, "hits": 1.0, "home_runs": 1.0,
        "total_bases": 1.0, "hits_runs_rbis": 1.0, "runs": 1.0,
        "rbis": 1.0, "strikeouts": 1.0, "earned_runs": 1.0,
        "pitching_outs": 1.0, "dome": False, "altitude": 0,
    },
}

# Team name → venue key mapping (for when team name is provided instead of venue)
_TEAM_TO_VENUE: dict[str, str] = {
    "colorado rockies":    "coors field",
    "cincinnati reds":     "great american ball park",
    "new york yankees":    "yankee stadium",
    "philadelphia phillies": "citizens bank park",
    "texas rangers":       "globe life field",
    "san francisco giants": "oracle park",
    "san diego padres":    "petco park",
    "miami marlins":       "loandepot park",
    "boston red sox":      "fenway park",
    "chicago cubs":        "wrigley field",
    "tampa bay rays":      "tropicana field",
    "seattle mariners":    "t-mobile park",
    "arizona diamondbacks": "chase field",
    "houston astros":      "minute maid park",
}


def get_park_mult(
    venue: str,
    prop_type: str,
    fallback: float = 1.0,
) -> float:
    """
    Get the unified park factor multiplier for a venue and prop type.

    Checks _UNIFIED_PARK_FACTORS (merged table) first.
    Falls back to park_factors.get_park_factor() if venue not in merged table.
    Falls back to park_k_factors.get_park_k_mult() for K props.
    Returns fallback (1.0) if nothing found.

    Args:
        venue:     Stadium name (case-insensitive, or team name)
        prop_type: Prop type string (e.g. "strikeouts", "hits", "total_bases")
        fallback:  Value returned when no park data found (default 1.0 = neutral)

    Returns:
        Float multiplier (1.0 = neutral, >1.0 = inflates stat, <1.0 = suppresses)

    Examples:
        get_park_mult("Coors Field", "strikeouts")   → 0.88
        get_park_mult("Fenway Park", "hits")         → 1.06
        get_park_mult("Oracle Park", "home_runs")    → 0.88
        get_park_mult("Unknown Park", "hits")        → 1.0  (fallback)
    """
    import re

    def _norm(s: str) -> str:
        return re.sub(r"\s+", " ", s.lower().strip())

    venue_key = _norm(venue)
    ptype_key = _norm(prop_type)

    # Check team name → venue mapping
    if venue_key in _TEAM_TO_VENUE:
        venue_key = _TEAM_TO_VENUE[venue_key]

    # Check merged table
    park_data = _UNIFIED_PARK_FACTORS.get(venue_key)
    if park_data:
        mult = park_data.get(ptype_key) or park_data.get("batting", fallback)
        return float(mult)

    # Fallback: try park_factors.py module
    try:
        from park_factors import get_park_factor
        result = get_park_factor(venue, prop_type)
        if result is not None:
            return float(result)
    except (ImportError, Exception):
        pass

    # Fallback: try park_k_factors.py for K props
    if ptype_key in ("strikeouts", "pitcher_strikeouts", "hitter_strikeouts"):
        try:
            from park_k_factors import get_park_k_mult
            mult, _ = get_park_k_mult(venue)
            return float(mult)
        except (ImportError, Exception):
            pass

    return fallback


def is_dome(venue: str) -> bool:
    """Return True if the venue is a dome or typically has roof closed."""
    import re
    key = re.sub(r"\s+", " ", venue.lower().strip())
    if key in _TEAM_TO_VENUE:
        key = _TEAM_TO_VENUE[key]
    park_data = _UNIFIED_PARK_FACTORS.get(key, {})
    return bool(park_data.get("dome", False))


def get_altitude(venue: str) -> int:
    """Return venue altitude in feet (0 for sea-level parks)."""
    import re
    key = re.sub(r"\s+", " ", venue.lower().strip())
    if key in _TEAM_TO_VENUE:
        key = _TEAM_TO_VENUE[key]
    park_data = _UNIFIED_PARK_FACTORS.get(key, {})
    return int(park_data.get("altitude", 0))


# ══════════════════════════════════════════════════════════════════════════════
# FIX B: Temperature Column Audit
# ══════════════════════════════════════════════════════════════════════════════

def audit_temperature_column(database_url: Optional[str] = None) -> dict:
    """
    Audit whether temperature_calibration.py is actually updating agent temperatures.

    Checks:
      1. agent_unit_sizing.temperature column values (are they all 1.0?)
      2. agent_calibration_data table (are outcomes being written?)
      3. Whether MIN_SAMPLES (30) has been reached

    Returns a dict with findings and recommended action.
    """
    import os

    db_url = database_url or os.environ.get("DATABASE_URL", "")
    result = {
        "db_connected": False,
        "temperature_values": {},
        "all_default": None,
        "calibration_data_rows": 0,
        "agents_above_min_samples": [],
        "verdict": "UNKNOWN",
        "action_needed": "",
    }

    if not db_url:
        result["verdict"] = "NO_DB — set DATABASE_URL to audit"
        result["action_needed"] = "Connect to Railway Postgres and run with DATABASE_URL set"
        return result

    try:
        import psycopg2
        conn = psycopg2.connect(db_url, connect_timeout=5)
        result["db_connected"] = True
    except Exception as e:
        result["verdict"] = f"DB_ERROR — {e}"
        return result

    try:
        with conn.cursor() as cur:
            # Check temperature column values
            try:
                cur.execute("""
                    SELECT agent_name, temperature, consecutive_wins, consecutive_losses
                    FROM agent_unit_sizing
                    ORDER BY agent_name
                """)
                rows = cur.fetchall()
                temps = {r[0]: r[1] for r in rows}
                result["temperature_values"] = temps
                result["all_default"] = all(abs(t - 1.0) < 0.001 for t in temps.values()) if temps else None
            except Exception as e:
                result["temperature_values"] = {"error": str(e)}

            # Check agent_calibration_data
            try:
                cur.execute("SELECT COUNT(*) FROM agent_calibration_data")
                result["calibration_data_rows"] = cur.fetchone()[0]

                cur.execute("""
                    SELECT agent_name, COUNT(*) as n
                    FROM agent_calibration_data
                    GROUP BY agent_name
                    HAVING COUNT(*) >= 30
                    ORDER BY n DESC
                """)
                result["agents_above_min_samples"] = [
                    {"agent": r[0], "n": r[1]} for r in cur.fetchall()
                ]
            except Exception as e:
                result["calibration_data_rows"] = f"error: {e}"

        conn.close()

        # Determine verdict
        if result["all_default"] is True and result["calibration_data_rows"] == 0:
            result["verdict"] = "BROKEN — no calibration data, temperatures stuck at 1.0"
            result["action_needed"] = (
                "agent_calibration_data is empty. nightly_recap.py must write "
                "per-leg outcomes to this table after each settlement. "
                "Check run_grading_tasklet() in tasklets.py — it should call "
                "temperature_calibration.run() after grading."
            )
        elif result["all_default"] is True and result["agents_above_min_samples"]:
            result["verdict"] = "BROKEN — calibration data exists but temperatures not updated"
            result["action_needed"] = (
                "Calibration data exists for some agents with 30+ rows, but temperatures "
                "are still 1.0. This means calibrate_temperatures() is not being called "
                "in the nightly grading cycle. Add it to run_grading_tasklet():\n"
                "  from temperature_calibration import run as _calibrate_temps\n"
                "  _calibrate_temps()"
            )
        elif result["all_default"] is True:
            result["verdict"] = "PENDING — temperatures at default, fewer than 30 graded picks per agent"
            result["action_needed"] = "Normal state early in season. Will auto-update after 30+ graded picks."
        elif result["all_default"] is False:
            result["verdict"] = "OK — temperatures are being updated"
            result["action_needed"] = ""
        else:
            result["verdict"] = "UNKNOWN — no agents in agent_unit_sizing table"

    except Exception as e:
        result["verdict"] = f"AUDIT_ERROR — {e}"

    return result


def check_grading_tasklet_calls_calibration() -> dict:
    """Check whether run_grading_tasklet calls temperature_calibration.run()."""
    from pathlib import Path
    tasklets = Path("tasklets.py")
    if not tasklets.exists():
        return {"found": False, "reason": "tasklets.py not found"}

    content = tasklets.read_text(errors="ignore")
    calls_calibration = "calibrate_temperatures" in content or "temperature_calibration" in content

    return {
        "calls_calibration": calls_calibration,
        "action": "" if calls_calibration else (
            "Add to run_grading_tasklet() in tasklets.py:\n"
            "    try:\n"
            "        from temperature_calibration import run as _calibrate_temps\n"
            "        updates = _calibrate_temps()\n"
            "        logger.info('[Grading] Temperature updates: %s', updates)\n"
            "    except Exception as _te:\n"
            "        logger.warning('[Grading] Temperature calibration failed: %s', _te)"
        ),
    }


# ── Tests ──────────────────────────────────────────────────────────────────────

def run_test() -> None:
    print("\n" + "=" * 60)
    print("  PARK FACTOR MERGE — SELF TEST")
    print("=" * 60)

    cases = [
        ("Coors Field",          "strikeouts",    0.88,  "Coors K suppression"),
        ("Coors Field",          "total_bases",   1.22,  "Coors TB inflation"),
        ("Great American Ball Park", "home_runs", 1.15,  "GABP HR inflation"),
        ("Fenway Park",          "hits",          1.06,  "Fenway green monster"),
        ("Fenway Park",          "home_runs",     0.96,  "Fenway HR suppression"),
        ("Oracle Park",          "home_runs",     0.88,  "Oracle HR suppression"),
        ("Petco Park",           "strikeouts",    1.01,  "Petco pitcher-friendly"),
        ("Unknown Stadium",      "strikeouts",    1.0,   "Unknown → neutral"),
        ("colorado rockies",     "strikeouts",    0.88,  "Team name lookup"),
        ("tropicana field",      "strikeouts",    1.04,  "Dome K boost"),
    ]

    all_pass = True
    for venue, prop_type, expected, label in cases:
        actual = get_park_mult(venue, prop_type)
        ok = abs(actual - expected) < 0.02
        status = "✅" if ok else "❌"
        print(f"  {status} {label}: {actual:.2f} (expected ~{expected:.2f})")
        if not ok:
            all_pass = False

    print(f"\n  Dome check — Tropicana: {is_dome('Tropicana Field')} (expect True)")
    print(f"  Dome check — Yankee:    {is_dome('Yankee Stadium')} (expect False)")
    print(f"  Altitude — Coors Field: {get_altitude('Coors Field')}ft (expect 5280)")
    print(f"  Altitude — Fenway:      {get_altitude('Fenway Park')}ft (expect 20)")

    print(f"\n  {'✅ All park tests passed.' if all_pass else '❌ Some park tests failed.'}")


def show_parks() -> None:
    print("\nUNIFIED PARK FACTOR TABLE")
    print(f"{'Venue':<30} {'K':<6} {'Hits':<6} {'HR':<6} {'TB':<6} {'Dome':<5} {'Alt'}")
    print("-" * 75)
    for venue, data in _UNIFIED_PARK_FACTORS.items():
        if venue == "_neutral":
            continue
        print(
            f"  {venue[:28]:<28} "
            f"{data.get('strikeouts', 1.0):<6.2f} "
            f"{data.get('hits', 1.0):<6.2f} "
            f"{data.get('home_runs', 1.0):<6.2f} "
            f"{data.get('total_bases', 1.0):<6.2f} "
            f"{'Y' if data.get('dome') else 'N':<5} "
            f"{data.get('altitude', 0)}"
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    if "--audit-temp" in sys.argv:
        print("\nAuditing temperature calibration...")
        findings = audit_temperature_column()
        print(f"\nDB connected:    {findings['db_connected']}")
        print(f"Verdict:         {findings['verdict']}")
        if findings.get("temperature_values"):
            print("\nAgent temperatures:")
            for agent, temp in findings["temperature_values"].items():
                flag = " ← DEFAULT" if isinstance(temp, float) and abs(temp - 1.0) < 0.001 else ""
                print(f"  {agent}: {temp}{flag}")
        print(f"\nCalibration rows: {findings['calibration_data_rows']}")
        print(f"Agents ≥30 rows:  {findings['agents_above_min_samples']}")
        if findings["action_needed"]:
            print(f"\n⚠️  ACTION NEEDED:\n{findings['action_needed']}")

        grading = check_grading_tasklet_calls_calibration()
        print(f"\nGrading tasklet calls calibration: {grading['calls_calibration']}")
        if grading["action"]:
            print(f"⚠️  Missing wiring:\n{grading['action']}")

    elif "--show-parks" in sys.argv:
        show_parks()
    else:
        run_test()
