"""
pr469_apply_patches.py
======================
Automated patch applicator for PR #469.
Reads prop_enrichment_layer.py and tasklets.py from the repo and applies
the matchup_engine wiring patches. Outputs patched files to /tmp/.

Usage:
    Run this after cloning the repo:
    python3 pr469_apply_patches.py --input-dir /path/to/repo --output-dir /tmp/pr469_patched
"""

import re
import sys
import os
import argparse


def apply_enrichment_patch(src: str) -> str:
    """Apply matchup lift patches to prop_enrichment_layer.py"""
    out = src

    # ── PATCH 1: K lift in _player_specific_rate() ─────────────────────────
    # Insert after: lam    = max(0.01, _blended_k_pa * _bf)
    # Use a flexible regex to match with any spacing
    k_patch = """\n            # ── Pitch-type matchup lift (matchup_engine.py) ─────────────────
            # Log-odds arsenal-level K adjustment from kekoa-santana method.
            # Achieves Brier 0.188 on pitcher K (vs current ~0.248).
            # Weight 0.35 prevents over-influence on existing pa_model signal.
            try:
                from matchup_engine import get_matchup_lift as _gml_k  # noqa: PLC0415
                _mu_k = _gml_k(prop)
                _k_logit_lift = float(_mu_k.get("k_lift_logit", 0.0) or 0.0)
                if abs(_k_logit_lift) > 0.02:
                    import math as _math_mu
                    _lo_clip = max(0.005, min(0.995, _blended_k_pa))
                    _logit_blended = _math_mu.log(_lo_clip / (1.0 - _lo_clip))
                    _logit_blended += _k_logit_lift * 0.35
                    _blended_k_pa = 1.0 / (1.0 + _math_mu.exp(-_logit_blended))
                    lam = max(0.01, _blended_k_pa * _bf)
                    prop["_matchup_k_lift"]      = round(_k_logit_lift, 4)
                    prop["_matchup_reliability"] = round(float(_mu_k.get("avg_reliability", 0.0)), 3)
                    prop["_matchup_n_pitches"]   = int(_mu_k.get("n_pitch_types", 0))
            except Exception:
                pass"""

    # Find the lam assignment line in _player_specific_rate
    pattern1 = r'([ \t]*lam\s*=\s*max\(0\.01,\s*_blended_k_pa\s*\*\s*_bf\))'
    if re.search(pattern1, out):
        out = re.sub(pattern1, r'\1' + k_patch, out, count=1)
        print("[PATCH 1] K lift inserted after lam assignment ✓")
    else:
        print("[PATCH 1] WARNING: Could not find lam assignment — skipping K lift patch")

    # ── PATCH 2: BB lift for walks_allowed props ────────────────────────────
    # Insert in main enrich_props() loop after prop_type filtering block
    bb_patch = """
        # ── Pitch-type matchup lift for walks_allowed props ──────────────────
        if prop.get("prop_type") in ("walks_allowed",):
            try:
                from matchup_engine import get_matchup_lift as _gml_bb  # noqa: PLC0415
                _mu_bb = _gml_bb(prop)
                if _mu_bb:
                    prop["_matchup_bb_lift"]     = float(_mu_bb.get("bb_lift_logit", 0.0))
                    if not prop.get("_matchup_reliability"):
                        prop["_matchup_reliability"] = float(_mu_bb.get("avg_reliability", 0.0))
            except Exception:
                pass"""

    # Anchor: find the ABS enrichment call (always present) and insert after it
    pattern2 = r'(prop\["_abs_adjustment"\]\s*=|abs_layer\.enrich_prop\(prop\))'
    if re.search(pattern2, out):
        # Insert after the first match + remainder of that statement line
        out = re.sub(
            pattern2,
            lambda m: m.group(0) + bb_patch,
            out, count=1
        )
        print("[PATCH 2] BB lift block inserted after ABS enrichment ✓")
    else:
        print("[PATCH 2] WARNING: Could not anchor BB lift insertion — skipping")

    return out


def apply_tasklets_patch(src: str) -> str:
    """Apply nudge stack and BullpenAgent patches to tasklets.py"""
    out = src

    # ── PATCH 3: Nudge stack layer 13 ──────────────────────────────────────
    nudge_patch = """
        # ── Layer 13: Pitch-type matchup lift (matchup_engine) ───────────────
        # Arsenal-level log-odds lift for K and BB props.
        # Logit 0.10 ≈ +2.5pp at baseline 50%. Capped at ±4pp.
        _mu_k_lift  = float(prop.get("_matchup_k_lift", 0.0) or 0.0)
        _mu_bb_lift = float(prop.get("_matchup_bb_lift", 0.0) or 0.0)
        if abs(_mu_k_lift) > 0.03 and _pt in {"strikeouts", "hitter_strikeouts"}:
            try:
                import math as _mu_math
                _mu_lo   = max(0.005, min(0.995, prob / 100.0))
                _mu_logit = _mu_math.log(_mu_lo / (1.0 - _mu_lo))
                _mu_new  = 1.0 / (1.0 + _mu_math.exp(-(_mu_logit + _mu_k_lift * 0.35))) * 100.0
                _mu_nudge = max(-4.0, min(4.0, _mu_new - prob))
                if abs(_mu_nudge) > 0.1:
                    nudge_stack.append(("matchup_k", _mu_nudge))
            except Exception:
                pass
        if abs(_mu_bb_lift) > 0.03 and _pt in {"walks_allowed"}:
            try:
                import math as _mu_math2
                _mu_lo2    = max(0.005, min(0.995, prob / 100.0))
                _mu_logit2 = _mu_math2.log(_mu_lo2 / (1.0 - _mu_lo2))
                _mu_new2   = 1.0 / (1.0 + _mu_math2.exp(-(_mu_logit2 + _mu_bb_lift * 0.35))) * 100.0
                _mu_nudge2 = max(-4.0, min(4.0, _mu_new2 - prob))
                if abs(_mu_nudge2) > 0.1:
                    nudge_stack.append(("matchup_bb", _mu_nudge2))
            except Exception:
                pass"""

    # Anchor: find game_env_nudge append (always present in nudge stack)
    pattern3 = r'(nudge_stack\.append\(\("game_env"[^)]+\)\))'
    if re.search(pattern3, out):
        out = re.sub(pattern3, r'\1' + nudge_patch, out, count=1)
        print("[PATCH 3] Nudge layer 13 inserted after game_env ✓")
    else:
        # Fallback: find any nudge_stack.append near the end of the nudge block
        pattern3b = r'(nudge_stack\.append\(\("weather[^)]+\)\))'
        if re.search(pattern3b, out):
            out = re.sub(pattern3b, r'\1' + nudge_patch, out, count=1)
            print("[PATCH 3] Nudge layer 13 inserted after weather nudge ✓")
        else:
            print("[PATCH 3] WARNING: Could not anchor nudge layer 13 — skipping")

    # ── PATCH 4: BullpenAgent ───────────────────────────────────────────────
    bullpen_patch = """
            # ── Pitch-type bullpen matchup lift (matchup_engine) ─────────────
            try:
                from matchup_engine import get_bullpen_matchup_lift as _gbml  # noqa: PLC0415
                _rel_ids  = [int(x) for x in (prop.get("_reliever_ids") or []) if x]
                _bat_ids  = [int(x) for x in (prop.get("_opp_lineup_ids") or []) if x]
                _bf_share = prop.get("_reliever_bf_shares")
                if _rel_ids:
                    _bp_mu = _gbml(_rel_ids, _bat_ids, _bf_share)
                    _bp_k  = float(_bp_mu.get("bullpen_k_lift", 0.0) or 0.0)
                    if abs(_bp_k) > 0.03 and prop_type in {"strikeouts", "hitter_strikeouts"}:
                        _bp_nudge = max(-3.0, min(3.0, _bp_k * 20.0))
                        prob += _bp_nudge
            except Exception:
                pass"""

    # Anchor: BVI block always ends with a BVI comment or hub["bullpen_bvi"] lookup
    pattern4 = r'(hub\["bullpen_bvi"\][^\n]+\n)'
    if re.search(pattern4, out):
        out = re.sub(pattern4, r'\1' + bullpen_patch, out, count=1)
        print("[PATCH 4] BullpenAgent matchup lift inserted after BVI lookup ✓")
    else:
        print("[PATCH 4] WARNING: Could not anchor BullpenAgent patch — skipping")

    return out


def apply_bug_checker_patch(src: str) -> str:
    """Add matchup engine health check to bug_checker.py"""
    check_fn = '''
async def _check_matchup_engine() -> str:
    """Check if pitch-type matchup arsenal data loaded successfully."""
    try:
        from matchup_engine import get_arsenal_status
        status = get_arsenal_status()
        if not status["pitcher_arsenal_loaded"]:
            return "❌ Matchup Engine: pitcher arsenal not loaded (Savant CSV failed)"
        n_p = status["pitcher_count"]
        n_b = status["batter_count"]
        return (
            f"✅ Matchup Engine: {n_p} pitchers, {n_b} batters loaded "
            f"({status['cache_date']})"
        )
    except Exception as exc:
        return f"⚠️ Matchup Engine: {exc}"

'''
    # Insert before the existing _check_fangraphs_data function
    pattern5 = r'(async def _check_fangraphs_data)'
    if re.search(pattern5, src):
        out = re.sub(pattern5, check_fn + r'\1', src, count=1)
        print("[PATCH 5] Matchup engine health check added to bug_checker.py ✓")
        # Also add to the checks list
        out = re.sub(
            r'(_check_fangraphs_data\(\))',
            r'_check_matchup_engine(),\n        \1',
            out, count=1
        )
        return out
    else:
        print("[PATCH 5] WARNING: Could not anchor bug_checker patch — skipping")
        return src


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", default=".", help="Path to repo root")
    parser.add_argument("--output-dir", default="/tmp/pr469_patched", help="Output dir for patched files")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    files = {
        "prop_enrichment_layer.py": apply_enrichment_patch,
        "tasklets.py": apply_tasklets_patch,
        "bug_checker.py": apply_bug_checker_patch,
    }

    success = 0
    for fname, patch_fn in files.items():
        fpath = os.path.join(args.input_dir, fname)
        if not os.path.exists(fpath):
            print(f"[SKIP] {fname} not found at {fpath}")
            continue
        with open(fpath, "r", encoding="utf-8") as f:
            src = f.read()
        patched = patch_fn(src)
        out_path = os.path.join(args.output_dir, fname)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(patched)
        print(f"[DONE] {fname} → {out_path}")
        success += 1

    print(f"\n{success}/{len(files)} files patched. Check {args.output_dir}")


if __name__ == "__main__":
    main()
