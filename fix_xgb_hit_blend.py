"""
fix_xgb_hit_blend.py
=====================
Wires xgb_hit_prob() into the hit-prop evaluation path in tasklets.py.

THE SITUATION
-------------
xgb_k_layer.py has two scorers:
  - xgb_k_prob()   → wired ✅  (80/20 blend for strikeout props)
  - xgb_hit_prob() → missing ❌ (hit props run on formula only)

Both functions are in the same file. The K blend is already inside
_BaseAgent's probability computation at the bottom of the prop evaluation.
The hit blend is documented in xgb_k_layer.py's own docstring but was
never added.

WHERE TO INSERT
---------------
In tasklets.py, inside _BaseAgent (or whichever class owns
_compute_prop_prob), find this exact block:

    # ── Per-line XGBoost K model blend (xgb_k_layer) ─────────────────
    if prop_type == "strikeouts":
        try:
            from xgb_k_layer import xgb_k_ready, xgb_k_prob as _xgb_k_prob
            if xgb_k_ready():
                _k_line_val = float(prop.get("line", 4.5) or 4.5)
                _xkp = _xgb_k_prob(prop, line=_k_line_val)
                if _xkp is not None:
                    raw_p = round(0.80 * raw_p + 0.20 * _xkp * 100, 2)
                    raw_p = max(5.0, min(95.0, raw_p))
        except ImportError:
            pass

    raw_prob = round(max(5.0, min(95.0, raw_p)), 2)
    return self._apply_temperature(raw_prob)

ADD THIS BLOCK immediately after the `except ImportError: pass` line
and before `raw_prob = round(...)`:

        # ── XGBoost hit model blend (xgb_k_layer) ────────────────────
        # 70/30 blend — hit model trained on Statcast xBA/xwOBA/EV features.
        # Mirrors the K blend above; same file, same lazy-load mechanism.
        # No-op when xgb_hits.pkl not yet trained (xgb_hit_ready() → False).
        if prop_type in ("hits", "total_bases", "hits_runs_rbis",
                         "fantasy_score", "fantasy_hitter"):
            try:
                from xgb_k_layer import xgb_hit_ready, xgb_hit_prob as _xgb_hit_prob  # noqa: PLC0415
                if xgb_hit_ready():
                    _xhp = _xgb_hit_prob(prop, prop)   # pass prop as pitcher proxy
                    if _xhp is not None:
                        raw_p = round(0.70 * raw_p + 0.30 * _xhp * 100, 2)
                        raw_p = max(5.0, min(95.0, raw_p))
            except ImportError:
                pass

HOW TO APPLY
------------
Option A — automatic (run from PropIQ repo root):
    python fix_xgb_hit_blend.py

Option B — manual (copy the block above into tasklets.py).

VERIFICATION
------------
After applying, search tasklets.py for "xgb_hit_prob" — should appear once.
In live logs, look for:
    [xgb_k] hit feature build ...
to confirm the model is being called.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [HIT-BLEND] %(message)s")
log = logging.getLogger(__name__)

TASKLETS = Path("tasklets.py")

# The anchor — insert immediately after this line
ANCHOR = "            except ImportError:\n                pass\n\n        raw_prob = round(max(5.0, min(95.0, raw_p)), 2)"

# The block to insert between the K blend and raw_prob=
INSERTION = """\n        # ── XGBoost hit model blend (xgb_k_layer) ────────────────────────────
        # 70/30 blend — hit model trained on Statcast xBA/xwOBA/EV features.
        # Mirrors the K blend above; same file, same lazy-load mechanism.
        # No-op when xgb_hits.pkl not yet trained (xgb_hit_ready() → False).
        if prop_type in ("hits", "total_bases", "hits_runs_rbis",
                         "fantasy_score", "fantasy_hitter"):
            try:
                from xgb_k_layer import xgb_hit_ready, xgb_hit_prob as _xgb_hit_prob  # noqa: PLC0415
                if xgb_hit_ready():
                    _xhp = _xgb_hit_prob(prop, prop)   # pass prop as pitcher proxy for opp fields
                    if _xhp is not None:
                        raw_p = round(0.70 * raw_p + 0.30 * _xhp * 100, 2)
                        raw_p = max(5.0, min(95.0, raw_p))
            except ImportError:
                pass

"""

REPLACEMENT = INSERTION + "        raw_prob = round(max(5.0, min(95.0, raw_p)), 2)"


def apply() -> bool:
    if not TASKLETS.exists():
        log.error("tasklets.py not found — run from PropIQ repo root.")
        return False

    content = TASKLETS.read_text(encoding="utf-8")

    if "xgb_hit_prob" in content:
        log.info("xgb_hit_prob already present in tasklets.py — nothing to do.")
        return True

    if ANCHOR not in content:
        log.error(
            "Anchor block not found in tasklets.py.\n"
            "The K-blend block may have been reformatted. Apply manually:\n"
            "Find the line: raw_prob = round(max(5.0, min(95.0, raw_p)), 2)\n"
            "that follows the xgb_k_prob block and insert the hit blend before it."
        )
        return False

    new_content = content.replace(ANCHOR, REPLACEMENT, 1)
    TASKLETS.write_text(new_content, encoding="utf-8")
    log.info("Hit blend inserted into tasklets.py.")
    log.info("Verify: grep -n 'xgb_hit_prob' tasklets.py")
    return True


def verify() -> None:
    if not TASKLETS.exists():
        print("tasklets.py not found.")
        return
    content = TASKLETS.read_text(encoding="utf-8")
    count = content.count("xgb_hit_prob")
    if count == 0:
        print("❌ xgb_hit_prob NOT found in tasklets.py — fix not applied.")
    else:
        print(f"✅ xgb_hit_prob found {count} time(s) in tasklets.py.")
        # Find and show context
        idx = content.find("xgb_hit_prob")
        print("\nContext:")
        print(content[max(0, idx - 100):idx + 300])


if __name__ == "__main__":
    if "--verify" in sys.argv:
        verify()
    else:
        ok = apply()
        if ok:
            verify()
