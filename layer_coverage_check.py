"""
layer_coverage_check.py
========================
Provides two additions to bug_checker.py:

  _check_layer_coverage()   — reads data/layer_health.json and yesterday's
                              bet_ledger to verify all layers fired on real props

  format_layer_embed()      — formats the layer coverage section for the
                              10 AM Discord bug checker embed

Add to bug_checker.py:
    from layer_coverage_check import _check_layer_coverage, format_layer_embed

    # In the checks list (after existing checks):
    checks.append(_check_layer_coverage)

    # In the embed builder, add:
    embed["fields"].append(format_layer_embed())
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger("propiq.layer_coverage")

LAYER_HEALTH_FILE = Path("data/layer_health.json")

# Thresholds — what % of props must a layer fire on to be considered "active"
# Set conservatively — not all layers apply to all prop types
THRESHOLDS = {
    "dampener_pct":  50.0,   # should fire on most multi-signal props
    "bayesian_active": 0.005, # any non-zero bayesian activity
    "umpire_active":   0.001, # any umpire adjustment
}

# These layers are optional — only warn if they're 0% AND their prereqs exist
OPTIONAL_LAYERS = {
    "xgb_k_pct":   "XGB K models (check models/xgb_k_4_5.pkl)",
    "xgb_hit_pct": "XGB hit models (check models/xgb_hits.pkl)",
    "bp2vec_pct":  "bp2vec matchup (run bp2vec_train.py --train)",
}


def _read_layer_health() -> dict | None:
    """Read data/layer_health.json written by the last dispatch cycle."""
    if not LAYER_HEALTH_FILE.exists():
        return None
    try:
        data = json.loads(LAYER_HEALTH_FILE.read_text())
        # Check freshness — should be from today
        written_at = data.get("written_at", "")
        if written_at:
            ts = datetime.fromisoformat(written_at.replace("Z", "+00:00"))
            age_hours = (datetime.now().astimezone() - ts).total_seconds() / 3600
            data["_age_hours"] = round(age_hours, 1)
        return data
    except Exception as e:
        logger.debug("layer_health.json read failed: %s", e)
        return None


def _query_layer_coverage_db() -> dict | None:
    """
    Query bet_ledger for yesterday's layer coverage stats.
    Falls back to None if DB unavailable or layer_audit column missing.
    """
    db_url = os.getenv("DATABASE_URL", "")
    if not db_url:
        return None

    try:
        import psycopg2
        conn = psycopg2.connect(db_url, connect_timeout=5)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) AS total,
                    AVG(CASE WHEN (layer_audit->>'dampener')::bool
                             THEN 1.0 ELSE 0.0 END) * 100  AS dampener_pct,
                    AVG(CASE WHEN (layer_audit->>'xgb_k')::bool
                             THEN 1.0 ELSE 0.0 END) * 100  AS xgb_k_pct,
                    AVG(CASE WHEN ABS((layer_audit->>'bp2vec')::float) > 0
                             THEN 1.0 ELSE 0.0 END) * 100  AS bp2vec_pct,
                    AVG(ABS((layer_audit->>'bayesian')::float))  AS bayesian_avg,
                    AVG(ABS((layer_audit->>'umpire')::float))    AS umpire_avg,
                    COUNT(CASE WHEN layer_audit->>'market_flag' != 'CLEAN'
                               THEN 1 END)                  AS market_flagged,
                    COUNT(CASE WHEN (layer_audit->>'injury')::float > 0
                               THEN 1 END)                  AS injury_blocked
                FROM bet_ledger
                WHERE bet_date = CURRENT_DATE - 1
                  AND layer_audit IS NOT NULL
            """)
            row = cur.fetchone()
            if row and row[0]:
                return {
                    "source":         "db",
                    "props":          row[0],
                    "dampener_pct":   round(float(row[1] or 0), 1),
                    "xgb_k_pct":      round(float(row[2] or 0), 1),
                    "bp2vec_pct":     round(float(row[3] or 0), 1),
                    "bayesian_avg":   round(float(row[4] or 0), 4),
                    "umpire_avg":     round(float(row[5] or 0), 4),
                    "market_flagged": int(row[6] or 0),
                    "injury_blocked": int(row[7] or 0),
                }
        conn.close()
    except Exception as e:
        logger.debug("Layer coverage DB query failed: %s", e)
    return None


def _check_layer_coverage() -> tuple[str, str, str]:
    """
    Bug checker check: verify key layers fired on real props.

    Reads data/layer_health.json (written by last dispatch cycle) and
    optionally queries bet_ledger for yesterday's layer coverage.

    Returns (name, status, detail) for bug_checker embed.
    """
    issues   = []
    warnings = []
    details  = []

    # Try JSON file first (same-day, from last dispatch)
    lh = _read_layer_health()
    if lh:
        n      = lh.get("props_evaluated", 0)
        layers = lh.get("layers", {})
        age    = lh.get("_age_hours", 999)

        if age > 18:
            warnings.append(f"layer_health.json is {age:.0f}h old — dispatch may not have run")

        if n == 0:
            issues.append("0 props evaluated — enrichment may be broken")
        else:
            # Core layers — must fire
            dampener = layers.get("dampener_pct", 0)
            if dampener == 0:
                issues.append(f"dampener 0% ({n} props) — adjustment_dampener not wiring")
            elif dampener < THRESHOLDS["dampener_pct"]:
                warnings.append(f"dampener only {dampener:.0f}% (expect ≥50%)")

            bayesian = layers.get("bayesian_active", 0)
            if bayesian == 0:
                issues.append("bayesian 0 avg adj — bayesian layer returning null")

            umpire = layers.get("umpire_active", 0)
            if umpire == 0:
                warnings.append("umpire 0 avg adj — umpire rates not applying")

            # Optional layers — warn if 0% but don't fail
            for key, label in OPTIONAL_LAYERS.items():
                if layers.get(key, 0) == 0:
                    warnings.append(f"{key.replace('_pct','').replace('_',' ')} 0% — {label}")

            zero_layers = lh.get("zero_layers", [])
            if zero_layers:
                issues.append(f"Zero-activity layers: {', '.join(zero_layers)}")

            details.append(
                f"dampener={dampener:.0f}% "
                f"xgb_k={layers.get('xgb_k_pct',0):.0f}% "
                f"bp2vec={layers.get('bp2vec_pct',0):.0f}% "
                f"({n} props)"
            )

    else:
        # Fall back to DB query
        db = _query_layer_coverage_db()
        if db:
            n = db["props"]
            if n == 0:
                warnings.append("No layer_audit rows in bet_ledger yesterday")
            else:
                if db["dampener_pct"] == 0:
                    issues.append(f"dampener 0% in bet_ledger ({n} props yesterday)")
                if db["bayesian_avg"] == 0:
                    issues.append("bayesian 0 avg in bet_ledger")
                details.append(
                    f"dampener={db['dampener_pct']:.0f}% "
                    f"xgb_k={db['xgb_k_pct']:.0f}% "
                    f"bp2vec={db['bp2vec_pct']:.0f}% "
                    f"({n} props yesterday)"
                )
        else:
            warnings.append(
                "layer_health.json not found and no DB data — "
                "deploy wire_layer_health.py to enable layer tracking"
            )

    if issues:
        return (
            "Layer Coverage",
            "fail",
            f"{len(issues)} layer(s) broken: {'; '.join(issues[:2])}",
        )
    if warnings:
        return (
            "Layer Coverage",
            "warn",
            "; ".join(warnings[:2]) + (f" | {details[0]}" if details else ""),
        )

    return (
        "Layer Coverage",
        "ok",
        details[0] if details else "Layer coverage check passed",
    )


def format_layer_embed() -> dict:
    """
    Format layer coverage as a Discord embed field for the 10 AM bug checker.

    Returns a dict suitable for the embed fields list:
        {"name": "...", "value": "...", "inline": False}

    Example output:
        ⚙️ Layer Coverage (18 props, 3h ago)
        Dampener:  ✅ 94%
        XGB K:     ✅ 67%
        XGB Hit:   ⚠️ 0% — check models/xgb_hits.pkl
        bp2vec:    ⚠️ 0% — run bp2vec_train.py
        Bayesian:  ✅ avg ±0.023
        Umpire:    ✅ avg ±0.011
        Flagged:   2 WIDE | Blocked: 1 injury
    """
    lh = _read_layer_health()
    db = None
    if not lh:
        db = _query_layer_coverage_db()

    data   = lh or db
    source = "layer_health.json" if lh else "bet_ledger"

    if not data:
        return {
            "name":   "⚙️ Layer Coverage",
            "value":  "⚠️ No data — deploy wire_layer_health.py first",
            "inline": False,
        }

    if lh:
        n      = lh.get("props_evaluated", 0)
        layers = lh.get("layers", {})
        age    = lh.get("_age_hours", 0)
        header = f"**{n} props**, {age:.0f}h ago"

        def _fmt(key, threshold=0, label=None, fmt="pct"):
            val = layers.get(key, 0)
            lbl = label or key.replace("_pct", "").replace("_active", "").replace("_", " ")
            if fmt == "pct":
                ok  = "✅" if val >= threshold else ("⚠️" if val > 0 else "❌")
                return f"{ok} {lbl.title()}: {val:.0f}%"
            else:
                ok  = "✅" if val > threshold else ("⚠️" if val >= 0 else "❌")
                return f"{ok} {lbl.title()}: avg ±{val:.4f}"

        lines = [
            header,
            _fmt("dampener_pct",   50,    "Dampener"),
            _fmt("xgb_k_pct",      10,    "XGB K"),
            _fmt("xgb_hit_pct",    0,     "XGB Hit"),
            _fmt("bp2vec_pct",     0,     "bp2vec"),
            _fmt("bayesian_active",0.001, "Bayesian", fmt="avg"),
            _fmt("umpire_active",  0.001, "Umpire",   fmt="avg"),
        ]
        flagged = layers.get("market_flagged", 0)
        blocked = layers.get("injury_blocked", 0)
        pa_hits = layers.get("pa_model_active", 0)
        if flagged or blocked:
            lines.append(f"🚩 Flagged: {flagged} | Blocked: {blocked} inj | PA: {pa_hits}")

        zero = lh.get("zero_layers", [])
        if zero:
            lines.append(f"🔴 Zero layers: {', '.join(zero)}")

    else:
        # DB fallback format
        n = db.get("props", 0)
        header = f"**{n} props** (yesterday, from DB)"
        lines = [
            header,
            f"{'✅' if db['dampener_pct'] > 50 else '❌'} Dampener: {db['dampener_pct']:.0f}%",
            f"{'✅' if db['xgb_k_pct'] > 0 else '⚠️'} XGB K: {db['xgb_k_pct']:.0f}%",
            f"{'✅' if db['bp2vec_pct'] > 0 else '⚠️'} bp2vec: {db['bp2vec_pct']:.0f}%",
            f"{'✅' if db['bayesian_avg'] > 0 else '❌'} Bayesian: avg ±{db['bayesian_avg']:.4f}",
            f"🚩 Flagged: {db['market_flagged']} | Blocked: {db['injury_blocked']} inj",
        ]

    return {
        "name":   "⚙️ Layer Coverage",
        "value":  "\n".join(lines),
        "inline": False,
    }


# ── Patch bug_checker.py ───────────────────────────────────────────────────────

def patch_bug_checker() -> bool:
    bc_path = Path("bug_checker.py")
    if not bc_path.exists():
        logger.error("bug_checker.py not found.")
        return False

    content = bc_path.read_text(encoding="utf-8")

    if "_check_layer_coverage" in content:
        logger.info("bug_checker.py already has _check_layer_coverage.")
        return True

    # Add import
    import_line = "from layer_coverage_check import _check_layer_coverage, format_layer_embed\n"
    import_anchor = "from railway_log_scanner import"
    if import_anchor in content:
        content = content.replace(
            import_anchor,
            import_line + import_anchor,
            1,
        )

    # Add check to checks list (after _check_pipeline_health)
    old_check = "        _check_pipeline_health,            # NEW: pipeline activity confirmation\n"
    new_check = (old_check +
                 "        _check_layer_coverage,             # NEW: per-layer firing verification\n")
    content = content.replace(old_check, new_check, 1)

    bc_path.write_text(content, encoding="utf-8")
    logger.info("bug_checker.py updated with _check_layer_coverage.")
    return True


if __name__ == "__main__":
    import sys
    if "--patch" in sys.argv:
        patch_bug_checker()
    elif "--embed" in sys.argv:
        import json
        print(json.dumps(format_layer_embed(), indent=2))
    else:
        name, status, detail = _check_layer_coverage()
        emoji = {"ok": "✅", "warn": "⚠️", "fail": "❌"}[status]
        print(f"{emoji} {name}: {detail}")
        print()
        embed = format_layer_embed()
        print(f"Discord embed:\n{embed['name']}\n{embed['value']}")
