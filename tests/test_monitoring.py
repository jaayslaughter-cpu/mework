"""
test_monitoring.py — Tests for monitoring.py

Pure-Python tests: no external dependencies (no sklearn, numpy, xgboost).
All external I/O (pika, redis, urllib) is stubbed.
"""

from __future__ import annotations

import sys
import types
import unittest
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# Stubs (same pattern as test_odds_integration.py)
# ---------------------------------------------------------------------------

def _stub_pika() -> None:
    if "pika" not in sys.modules:
        pika_mod = types.ModuleType("pika")
        pika_mod.BlockingConnection = MagicMock()  # type: ignore[attr-defined]
        pika_mod.URLParameters = MagicMock()       # type: ignore[attr-defined]
        sys.modules["pika"] = pika_mod


def _stub_redis_lib() -> None:
    if "redis" not in sys.modules:
        redis_mod = types.ModuleType("redis")
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        redis_mod.from_url = MagicMock(return_value=mock_client)  # type: ignore[attr-defined]
        sys.modules["redis"] = redis_mod

_stub_pika()
_stub_redis_lib()

from api.services.monitoring import (  # noqa: E402
    AlertManager,
    AlertThresholds,
    BetRecord,
    FeatureDriftMonitor,
    HealthChecker,
    MetricTracker,
    ModelPerformanceMonitor,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_bet(
    outcome: int = 1,
    clv: float = 0.02,
    ev_pct: float = 0.05,
    kelly: float = 0.03,
    agent: str = "EVHunter",
    pnl: float = 0.5,
    game_date: str = "2025-06-01",
) -> BetRecord:
    return BetRecord(
        slip_id        = "test_slip_001",
        agent          = agent,
        prop_type      = "strikeouts",
        ml_prob        = 0.62,
        ev_pct         = ev_pct,
        kelly_fraction = kelly,
        outcome        = outcome,
        clv            = clv,
        pnl_units      = pnl,
        game_date      = game_date,
    )


# ---------------------------------------------------------------------------
# Tests: BetRecord
# ---------------------------------------------------------------------------

class TestBetRecord(unittest.TestCase):
    def test_fields_accessible(self) -> None:
        r = _make_bet()
        self.assertEqual(r.agent, "EVHunter")
        self.assertEqual(r.outcome, 1)
        self.assertAlmostEqual(r.ev_pct, 0.05)

    def test_push_outcome_is_minus_one(self) -> None:
        r = _make_bet(outcome=-1, pnl=0.0)
        self.assertEqual(r.outcome, -1)
        self.assertEqual(r.pnl_units, 0.0)


# ---------------------------------------------------------------------------
# Tests: MetricTracker
# ---------------------------------------------------------------------------

class TestMetricTracker(unittest.TestCase):
    def _tracker(self) -> MetricTracker:
        return MetricTracker(redis_client=None)

    def test_empty_summary(self) -> None:
        t = self._tracker()
        summary = t.get_summary()
        self.assertEqual(summary["n_bets"], 0)

    def test_single_win(self) -> None:
        t = self._tracker()
        t.record_bet(_make_bet(outcome=1, pnl=1.0))
        s = t.get_summary()
        self.assertEqual(s["n_bets"], 1)
        self.assertEqual(s["n_wins"], 1)
        self.assertAlmostEqual(s["win_rate"], 1.0)

    def test_single_loss(self) -> None:
        t = self._tracker()
        t.record_bet(_make_bet(outcome=0, pnl=-1.0))
        s = t.get_summary()
        self.assertEqual(s["n_losses"], 1)
        self.assertAlmostEqual(s["win_rate"], 0.0)

    def test_mixed_outcomes(self) -> None:
        t = self._tracker()
        for i in range(6):
            outcome = 1 if i < 4 else 0
            t.record_bet(_make_bet(outcome=outcome, pnl=1.0 if outcome else -1.0))
        s = t.get_summary()
        self.assertEqual(s["n_bets"], 6)
        self.assertAlmostEqual(s["win_rate"], 4 / 6, places=4)

    def test_pnl_accumulates(self) -> None:
        t = self._tracker()
        t.record_bet(_make_bet(pnl=2.0, outcome=1))
        t.record_bet(_make_bet(pnl=-1.0, outcome=0))
        s = t.get_summary()
        self.assertAlmostEqual(s["total_pnl_units"], 1.0)

    def test_agent_breakdown(self) -> None:
        t = self._tracker()
        t.record_bet(_make_bet(agent="EVHunter", outcome=1))
        t.record_bet(_make_bet(agent="SteamAgent", outcome=0))
        s = t.get_summary()
        self.assertIn("EVHunter", s["agent_breakdown"])
        self.assertIn("SteamAgent", s["agent_breakdown"])
        self.assertEqual(s["agent_breakdown"]["EVHunter"]["wins"], 1)
        self.assertEqual(s["agent_breakdown"]["SteamAgent"]["wins"], 0)

    def test_discord_success_rate(self) -> None:
        t = self._tracker()
        t.record_discord_attempt(True)
        t.record_discord_attempt(True)
        t.record_discord_attempt(False)
        t.record_bet(_make_bet())
        s = t.get_summary()
        self.assertAlmostEqual(s["discord_success_rate"], 2 / 3, places=4)

    def test_discord_rate_default_when_no_attempts(self) -> None:
        t = self._tracker()
        t.record_bet(_make_bet())
        s = t.get_summary()
        self.assertAlmostEqual(s["discord_success_rate"], 1.0)

    def test_kelly_avg(self) -> None:
        t = self._tracker()
        t.record_bet(_make_bet(kelly=0.04))
        t.record_bet(_make_bet(kelly=0.06))
        s = t.get_summary()
        self.assertAlmostEqual(s["kelly_avg"], 0.05, places=4)

    def test_clv_avg(self) -> None:
        t = self._tracker()
        t.record_bet(_make_bet(clv=0.02))
        t.record_bet(_make_bet(clv=0.04))
        s = t.get_summary()
        self.assertAlmostEqual(s["clv_avg"], 0.03, places=4)

    def test_sharpe_computed_for_multi_day(self) -> None:
        t = self._tracker()
        for day, pnl in [("2025-06-01", 1.0), ("2025-06-02", -0.5),
                         ("2025-06-03", 0.8), ("2025-06-04", 1.2),
                         ("2025-06-05", -0.3)]:
            t.record_bet(_make_bet(pnl=pnl, game_date=day))
        s = t.get_summary()
        # Sharpe should be computed and not be NaN
        self.assertIsInstance(s["sharpe"], float)
        self.assertFalse(s["sharpe"] != s["sharpe"])  # NaN check


# ---------------------------------------------------------------------------
# Tests: FeatureDriftMonitor
# ---------------------------------------------------------------------------

class TestFeatureDriftMonitor(unittest.TestCase):
    def _monitor_with_ref(self) -> FeatureDriftMonitor:
        """Build a monitor with a known reference distribution."""
        m = FeatureDriftMonitor()
        # Reference: 100 values uniformly in [5, 10] → PSI from similar dist ≈ 0
        ref_vals = [5.0 + (i % 50) * 0.1 for i in range(100)]
        m.update_reference("k_per_9", ref_vals)
        return m

    def test_no_reference_returns_zero(self) -> None:
        m = FeatureDriftMonitor()
        psi = m.compute_psi("unknown_feature", [1.0, 2.0, 3.0])
        self.assertEqual(psi, 0.0)

    def test_identical_distribution_low_psi(self) -> None:
        m = self._monitor_with_ref()
        similar_vals = [5.0 + (i % 50) * 0.1 for i in range(200)]
        psi = m.compute_psi("k_per_9", similar_vals)
        self.assertLess(psi, 0.10, f"Expected low PSI for similar dist, got {psi}")

    def test_completely_different_distribution_high_psi(self) -> None:
        m = self._monitor_with_ref()
        # Completely different range: [20, 25]
        different_vals = [20.0 + i * 0.1 for i in range(100)]
        psi = m.compute_psi("k_per_9", different_vals)
        self.assertGreater(psi, 0.20, f"Expected high PSI for shifted dist, got {psi}")

    def test_check_all_features_returns_dict(self) -> None:
        m = self._monitor_with_ref()
        scores = m.check_all_features({"k_per_9": [5.5, 6.0, 7.0, 5.8]})
        self.assertIn("k_per_9", scores)
        self.assertIsInstance(scores["k_per_9"], float)

    def test_update_reference_populates_stats(self) -> None:
        m = FeatureDriftMonitor()
        m.update_reference("era", [3.0, 4.0, 5.0, 3.5, 4.5])
        self.assertIn("era", m._reference)
        self.assertIn("bins", m._reference["era"])
        self.assertIn("counts", m._reference["era"])


# ---------------------------------------------------------------------------
# Tests: AlertManager
# ---------------------------------------------------------------------------

class TestAlertManager(unittest.TestCase):
    def _manager(self, thresholds: AlertThresholds | None = None) -> AlertManager:
        """AlertManager with no webhook (no-op Discord)."""
        return AlertManager(
            webhook_url = "",
            thresholds  = thresholds or AlertThresholds(),
            cooldown_seconds = 0,   # disable cooldown for testing
        )

    def test_good_metrics_no_alerts(self) -> None:
        mgr = self._manager()
        fired = mgr.check_thresholds({
            "win_rate":             0.56,
            "clv_avg":              0.015,
            "kelly_avg":            0.04,
            "discord_success_rate": 0.98,
            "n_bets":               12,
        })
        self.assertEqual(fired, [])

    def test_low_win_rate_fires_alert(self) -> None:
        mgr = self._manager()
        fired = mgr.check_thresholds({
            "win_rate":             0.49,
            "clv_avg":              0.015,
            "kelly_avg":            0.04,
            "discord_success_rate": 0.98,
            "n_bets":               12,
        })
        self.assertIn("win_rate_low", fired)

    def test_negative_clv_fires_alert(self) -> None:
        mgr = self._manager()
        fired = mgr.check_thresholds({
            "win_rate":             0.56,
            "clv_avg":              -0.01,
            "kelly_avg":            0.04,
            "discord_success_rate": 0.98,
            "n_bets":               12,
        })
        self.assertIn("clv_negative", fired)

    def test_high_kelly_fires_alert(self) -> None:
        mgr = self._manager()
        fired = mgr.check_thresholds({
            "win_rate":             0.56,
            "clv_avg":              0.015,
            "kelly_avg":            0.09,
            "discord_success_rate": 0.98,
            "n_bets":               12,
        })
        self.assertIn("kelly_high", fired)

    def test_low_discord_rate_fires_alert(self) -> None:
        mgr = self._manager()
        fired = mgr.check_thresholds({
            "win_rate":             0.56,
            "clv_avg":              0.015,
            "kelly_avg":            0.04,
            "discord_success_rate": 0.90,
            "n_bets":               12,
        })
        self.assertIn("discord_low", fired)

    def test_low_slip_count_fires_alert(self) -> None:
        mgr = self._manager()
        fired = mgr.check_thresholds({
            "win_rate":             0.56,
            "clv_avg":              0.015,
            "kelly_avg":            0.04,
            "discord_success_rate": 0.98,
            "n_bets":               1,
        })
        self.assertIn("slips_low", fired)

    def test_high_slip_count_fires_alert(self) -> None:
        mgr = self._manager()
        fired = mgr.check_thresholds({
            "win_rate":             0.56,
            "clv_avg":              0.015,
            "kelly_avg":            0.04,
            "discord_success_rate": 0.98,
            "n_bets":               60,
        })
        self.assertIn("slips_high", fired)

    def test_cooldown_prevents_duplicate_alerts(self) -> None:
        mgr = AlertManager(
            webhook_url      = "",
            cooldown_seconds = 3_600,   # 1 hour
        )
        bad_metrics = {
            "win_rate":             0.49,
            "clv_avg":              0.015,
            "kelly_avg":            0.04,
            "discord_success_rate": 0.98,
            "n_bets":               12,
        }
        fired_1 = mgr.check_thresholds(bad_metrics)
        fired_2 = mgr.check_thresholds(bad_metrics)   # within cooldown
        self.assertIn("win_rate_low", fired_1)
        self.assertNotIn("win_rate_low", fired_2)

    def test_psi_warning_fires(self) -> None:
        mgr = self._manager()
        fired = mgr.check_feature_drift({"k_per_9": 0.25})
        self.assertIn("psi_k_per_9", fired)

    def test_psi_ok_does_not_fire(self) -> None:
        mgr = self._manager()
        fired = mgr.check_feature_drift({"k_per_9": 0.10})
        self.assertNotIn("psi_k_per_9", fired)

    def test_psi_critical_fires(self) -> None:
        mgr = self._manager()
        fired = mgr.check_feature_drift({"era": 0.40})
        self.assertIn("psi_era", fired)

    def test_multiple_violations_all_fire(self) -> None:
        mgr = self._manager()
        fired = mgr.check_thresholds({
            "win_rate":             0.49,
            "clv_avg":              -0.01,
            "kelly_avg":            0.09,
            "discord_success_rate": 0.88,
            "n_bets":               1,
        })
        self.assertGreaterEqual(len(fired), 4)


# ---------------------------------------------------------------------------
# Tests: ModelPerformanceMonitor
# ---------------------------------------------------------------------------

class TestModelPerformanceMonitor(unittest.TestCase):
    def _monitor(self) -> tuple[ModelPerformanceMonitor, AlertManager]:
        mgr = AlertManager(webhook_url="", cooldown_seconds=0)
        mon = ModelPerformanceMonitor(mgr, baseline_brier=0.228, baseline_auc=0.634)
        return mon, mgr

    def test_ok_performance_no_alerts(self) -> None:
        mon, _ = self._monitor()
        result = mon.check(current_brier=0.230, current_auc=0.631)
        self.assertEqual(result["status"], "ok")

    def test_brier_drift_triggers_alert(self) -> None:
        mon, _ = self._monitor()
        result = mon.check(current_brier=0.250, current_auc=0.634)  # +0.022 drift
        self.assertEqual(result["status"], "alert")
        self.assertIn("brier_drift", result["alerts_fired"])

    def test_auc_drop_triggers_alert(self) -> None:
        mon, _ = self._monitor()
        result = mon.check(current_brier=0.228, current_auc=0.610)   # -0.024 drop
        self.assertEqual(result["status"], "alert")
        self.assertIn("auc_drop", result["alerts_fired"])

    def test_delta_values_in_result(self) -> None:
        mon, _ = self._monitor()
        result = mon.check(current_brier=0.240, current_auc=0.620)
        self.assertAlmostEqual(result["brier_delta"], 0.012, places=3)
        self.assertAlmostEqual(result["auc_delta"], -0.014, places=3)

    def test_both_metrics_bad_fires_both(self) -> None:
        mon, _ = self._monitor()
        result = mon.check(current_brier=0.260, current_auc=0.600)
        self.assertIn("brier_drift", result["alerts_fired"])
        self.assertIn("auc_drop", result["alerts_fired"])


# ---------------------------------------------------------------------------
# Tests: HealthChecker (dependency checks stubbed)
# ---------------------------------------------------------------------------

class TestHealthChecker(unittest.TestCase):
    def test_run_all_checks_returns_dict(self) -> None:
        checker = HealthChecker(rabbitmq_url="amqp://fake", redis_url="redis://fake")
        # All checks will fail (no real services) — we just check structure
        result = checker.run_all_checks()
        self.assertIn("status", result)
        self.assertIn("checks", result)
        self.assertIn("overall_ok", result)
        self.assertIsInstance(result["checks"], list)

    def test_checks_list_has_expected_tiers(self) -> None:
        checker = HealthChecker(rabbitmq_url="amqp://fake", redis_url="redis://fake")
        result  = checker.run_all_checks()
        tiers   = {c["tier"] for c in result["checks"]}
        self.assertIn("rabbitmq", tiers)
        self.assertIn("redis", tiers)
        self.assertIn("fastapi", tiers)
        self.assertIn("discord", tiers)

    def test_all_failed_shows_degraded(self) -> None:
        checker = HealthChecker(rabbitmq_url="amqp://no-host", redis_url="redis://no-host")
        result  = checker.run_all_checks()
        # All checks fail without real services
        self.assertEqual(result["status"], "degraded")
        self.assertFalse(result["overall_ok"])

    def test_each_check_has_latency(self) -> None:
        checker = HealthChecker()
        result  = checker.run_all_checks()
        for c in result["checks"]:
            self.assertIn("latency_ms", c)
            self.assertIsInstance(c["latency_ms"], float)


if __name__ == "__main__":
    unittest.main()
