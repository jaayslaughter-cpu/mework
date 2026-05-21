"""
tests/test_smoke.py — PropIQ pre-merge smoke test.

Run locally:  python tests/test_smoke.py
In CI:        python tests/test_smoke.py (via .github/workflows/smoke_test.yml)

No Railway/Postgres/Redis deps — pure Python stdlib + json.
Each test targets a specific class of past production failure.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent


def _src(filename: str) -> str:
    return (ROOT / filename).read_text(encoding="utf-8")


def _find_val(src: str, varname: str, key: int | str) -> float:
    """Extract a numeric value from a dict literal in Python source."""
    start = src.find(varname)
    if start == -1:
        raise ValueError(f"{varname!r} not found in source")
    block = src[start: start + 800]
    if isinstance(key, int):
        pattern = rf"(?<![0-9]){key}\s*:\s*([0-9]+\.?[0-9]*)"
    else:
        pattern = rf"['\"]?{re.escape(str(key))}['\"]?\s*:\s*([0-9]+\.?[0-9]*)"
    m = re.search(pattern, block)
    if not m:
        raise ValueError(f"Key {key!r} not found in {varname!r} block")
    return float(m.group(1))


def test_key_files_not_placeholders():
    """PR #585: critical modules committed as 1-line localPath placeholders."""
    for fname in [
        "sportsbook_reference_layer.py", "action_network_layer.py",
        "calibration_layer.py", "tasklets.py", "streak_agent.py",
        "orchestrator.py", "nightly_recap.py", "espn_scraper.py",
    ]:
        path = ROOT / fname
        assert path.exists(), f"MISSING FILE: {fname}"
        size = path.stat().st_size
        assert size > 500, (
            f"{fname} is {size} bytes — likely a localPath placeholder. "
            "Always pass file content inline in PRs."
        )


def test_ud_multipliers_correct_values():
    """PR #589: _UD_MULTIPLIERS[5] was 20.0. Correct: 2=3.5x 3=6x 4=10x 5=10x."""
    src = _src("calibration_layer.py")
    for k, v in {2: 3.5, 3: 6.0, 4: 10.0, 5: 10.0}.items():
        actual = _find_val(src, "_UD_MULTIPLIERS", k)
        assert actual == v, f"calibration_layer._UD_MULTIPLIERS[{k}]={actual}, expected {v}"


def test_pp_multipliers_correct_values():
    """PR #589: _PP_MULTIPLIERS[3] was 6.0. Correct: 2=3x 3=5x 4=10x 5=20x."""
    src = _src("calibration_layer.py")
    for k, v in {2: 3.0, 3: 5.0, 4: 10.0, 5: 20.0}.items():
        actual = _find_val(src, "_PP_MULTIPLIERS", k)
        assert actual == v, f"calibration_layer._PP_MULTIPLIERS[{k}]={actual}, expected {v}"


def test_ud_multipliers_match_propiq_constants():
    """Cross-file: calibration_layer._UD_MULTIPLIERS must match propiq_constants.UD_MULTIPLIERS."""
    cal = _src("calibration_layer.py")
    const = _src("propiq_constants.py")
    for k in (2, 3, 4, 5):
        cv = _find_val(cal, "_UD_MULTIPLIERS", k)
        pv = _find_val(const, "UD_MULTIPLIERS", k)
        assert cv == pv, f"UD_MULTIPLIERS[{k}] drift: calibration_layer={cv} propiq_constants={pv}"


def test_pp_multipliers_match_propiq_constants():
    """Cross-file: calibration_layer._PP_MULTIPLIERS must match propiq_constants.PP_MULTIPLIERS."""
    cal = _src("calibration_layer.py")
    const = _src("propiq_constants.py")
    for k in (2, 3, 4, 5):
        cv = _find_val(cal, "_PP_MULTIPLIERS", k)
        pv = _find_val(const, "PP_MULTIPLIERS", k)
        assert cv == pv, f"PP_MULTIPLIERS[{k}] drift: calibration_layer={cv} propiq_constants={pv}"


def test_bet_ledger_insert_has_18_placeholders():
    """PR #586: INSERT had 18 %s but only 17 values. layer_audit is the 18th."""
    src = _src("tasklets.py")
    inserts = list(re.finditer(r"INSERT\s+INTO\s+bet_ledger", src, re.IGNORECASE))
    assert inserts, "No INSERT INTO bet_ledger found in tasklets.py"
    for m in inserts:
        block = src[m.start(): m.start() + 2000]
        vm = re.search(r"VALUES\s*\(([^;]+?)\)", block, re.DOTALL)
        if vm:
            count = vm.group(1).count("%s")
            assert count == 18, f"bet_ledger INSERT has {count} %s placeholders, expected 18"


def test_xgb_query_uses_correct_model_prob_scale():
    """PR #586: model_prob >= 0.59 always TRUE on 0-100 scale. Must be >= 59."""
    src = _src("tasklets.py")
    assert "model_prob >= 0.59" not in src, "XGB training uses wrong scale: >= 0.59"
    assert "model_prob >= 59" in src, "XGB training missing: model_prob >= 59"


def test_xgb_query_has_discord_sent_filter():
    """Discord Sent Filter Directive: XGB must only train on discord_sent = TRUE."""
    src = _src("tasklets.py")
    assert any(f in src for f in ("discord_sent = TRUE", "discord_sent=TRUE", "discord_sent = true")), (
        "XGB training query missing discord_sent = TRUE filter"
    )


def test_min_prob_overrides_not_below_floor():
    """PR #588: per-prop floors must be >= 0.55 while XGB builds training data."""
    p = ROOT / "data" / "calibration_params.json"
    if not p.exists():
        print("    [SKIP] data/calibration_params.json not present")
        return
    overrides = json.loads(p.read_text()).get("min_prob_overrides", {})
    for prop, floor in overrides.items():
        if prop != "fantasy_score":
            assert float(floor) >= 0.55, f"min_prob_overrides[{prop!r}]={floor} below 0.55"


def test_no_live_dispatcher_import():
    """PR #417: live_dispatcher.py was deleted. line_stream.py must not import it."""
    src = _src("line_stream.py")
    assert "from live_dispatcher" not in src, "line_stream.py imports deleted live_dispatcher"
    assert "import live_dispatcher" not in src, "line_stream.py imports deleted live_dispatcher"


if __name__ == "__main__":
    tests = [
        test_key_files_not_placeholders,
        test_ud_multipliers_correct_values,
        test_pp_multipliers_correct_values,
        test_ud_multipliers_match_propiq_constants,
        test_pp_multipliers_match_propiq_constants,
        test_bet_ledger_insert_has_18_placeholders,
        test_xgb_query_uses_correct_model_prob_scale,
        test_xgb_query_has_discord_sent_filter,
        test_min_prob_overrides_not_below_floor,
        test_no_live_dispatcher_import,
    ]
    passed = failed = 0
    for t in tests:
        try:
            t()
            print(f"  \u2705 {t.__name__}")
            passed += 1
        except Exception as exc:
            print(f"  \u274c {t.__name__}")
            print(f"       {exc}")
            failed += 1
    print(f"\n{'='*55}")
    print(f"  {passed} passed  |  {failed} failed")
    sys.exit(1 if failed else 0)
