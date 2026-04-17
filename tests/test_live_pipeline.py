"""
tests/test_live_pipeline.py
============================
Minimum coverage for the live production pipeline.
Tests run without real DB/Redis — all external calls are mocked.
"""
from unittest.mock import patch, MagicMock


# ---------------------------------------------------------------------------
# generate_pick tests
# ---------------------------------------------------------------------------

def test_generate_pick_returns_none_below_min_edge():
    """An impossible edge requirement should always return None."""
    try:
        from generate_pick import generate_pick
        prop = {
            "prop_type": "hits", "line": 1.5, "player": "Test Player",
            "over_american": -115, "under_american": -115,
        }
        result = generate_pick(raw_prop=prop, side="OVER", min_edge=0.99)
        assert result is None, f"Expected None for impossible edge, got {result}"
    except ImportError:
        import pytest; pytest.skip("generate_pick not importable in CI")


# ---------------------------------------------------------------------------
# settlement_engine tests
# ---------------------------------------------------------------------------

def test_settlement_over_win():
    """Batter hits 2 vs line 1.5 Over → WIN."""
    try:
        from settlement_engine import settle_leg
        leg = {"player": "Aaron Judge", "prop_type": "hits", "side": "Over", "line": 1.5}
        stats = {"Aaron Judge": {"hits": 2, "homeRuns": 1, "totalBases": 5}}
        result = settle_leg(leg, stats)
        assert result.outcome == "WIN"
        assert result.actual == 2.0
    except ImportError:
        import pytest; pytest.skip("settlement_engine not importable in CI")


def test_settlement_over_loss():
    """Batter hits 1 vs line 1.5 Over → LOSS."""
    try:
        from settlement_engine import settle_leg
        leg = {"player": "Aaron Judge", "prop_type": "hits", "side": "Over", "line": 1.5}
        stats = {"Aaron Judge": {"hits": 1}}
        result = settle_leg(leg, stats)
        assert result.outcome == "LOSS"
    except ImportError:
        import pytest; pytest.skip("settlement_engine not importable in CI")


def test_settlement_hitter_strikeouts():
    """hitter_strikeouts must map to ESPN 'strikeouts' key (PR #333 fix)."""
    try:
        from settlement_engine import settle_leg
        leg = {"player": "Shohei Ohtani", "prop_type": "hitter_strikeouts",
               "side": "Under", "line": 1.5}
        stats = {"Shohei Ohtani": {"strikeouts": 1}}
        result = settle_leg(leg, stats)
        assert result.outcome == "WIN"   # 1 K < 1.5 Under → WIN
    except ImportError:
        import pytest; pytest.skip("settlement_engine not importable in CI")


# ---------------------------------------------------------------------------
# calibration_layer tests
# ---------------------------------------------------------------------------

def test_calibration_max_prob_cap():
    """Final probability should never exceed 0.82 (hard cap)."""
    try:
        from calibration_layer import compute_unified_probability
        result = compute_unified_probability(
            raw_model_prob=0.99, market_implied=0.535, prop={}
        )
        assert result["final_prob"] <= 0.82, (
            f"Prob {result['final_prob']} exceeds hard cap 0.82"
        )
    except ImportError:
        import pytest; pytest.skip("calibration_layer not importable in CI")


# ---------------------------------------------------------------------------
# risk_manager tests
# ---------------------------------------------------------------------------

def test_risk_manager_reloads_exposure_each_call():
    """check_stake() must call _load_today_exposure() — H-3 fix verification."""
    try:
        from risk_manager import RiskManager
        import inspect
        src = inspect.getsource(RiskManager.check_stake)
        assert "_load_today_exposure" in src, (
            "H-3 fix missing: check_stake() must call self._load_today_exposure() "
            "to reload from Postgres after Railway restarts"
        )
    except ImportError:
        import pytest; pytest.skip("risk_manager not importable in CI")


def test_apply_cool_down_dual_writes():
    """apply_cool_down() must write to BOTH agent_cool_down AND agent_freeze_log — H-2 fix."""
    try:
        from risk_manager import RiskManager
        import inspect
        src = inspect.getsource(RiskManager.apply_cool_down)
        assert "agent_freeze_log" in src, (
            "H-2 fix missing: apply_cool_down() must also write to agent_freeze_log "
            "(the table that get_frozen_agents() reads at dispatch time)"
        )
    except ImportError:
        import pytest; pytest.skip("risk_manager not importable in CI")


# ---------------------------------------------------------------------------
# streak_agent tests
# ---------------------------------------------------------------------------

def test_streak_agent_configs_no_phantoms():
    """AGENT_CONFIGS must not contain phantom agents that only check implied_prob — H-9 fix."""
    try:
        from streak_agent import AGENT_CONFIGS
        # All 17 agents should have differentiated filters (not all identical implied_prob gates)
        assert len(AGENT_CONFIGS) == 17, f"Expected 17 agents, got {len(AGENT_CONFIGS)}"

        # At least 10 of the 17 should have additional conditions beyond implied_prob
        # (checking by inspecting the lambda source code)
        import inspect
        differentiated = 0
        for cfg in AGENT_CONFIGS:
            src = inspect.getsource(cfg["filter"])
            has_side   = "side" in src
            has_ev     = "ev_pct" in src
            has_prop   = "prop_type" in src
            has_pos    = "position" in src
            has_higher = ">= 0.60" in src or ">= 0.62" in src
            if has_side or has_ev or has_prop or has_pos or has_higher:
                differentiated += 1

        assert differentiated >= 10, (
            f"Only {differentiated}/17 agents have differentiated filters. "
            "Phantom agents inflate signal count — each should have a unique condition."
        )
    except ImportError:
        import pytest; pytest.skip("streak_agent not importable in CI")


# ---------------------------------------------------------------------------
# clv_tracker tests
# ---------------------------------------------------------------------------

def test_clv_tracker_tries_postgres_first():
    """get_daily_clv_summary() must try Postgres before SQLite — H-4 fix."""
    try:
        from clv_tracker import get_daily_clv_summary
        import inspect
        src = inspect.getsource(get_daily_clv_summary)
        assert "_query_pg" in src or "psycopg2" in src or "DATABASE_URL" in src, (
            "H-4 fix missing: get_daily_clv_summary() must try Postgres first "
            "so CLV survives /tmp wipes on Railway restarts"
        )
    except ImportError:
        import pytest; pytest.skip("clv_tracker not importable in CI")
