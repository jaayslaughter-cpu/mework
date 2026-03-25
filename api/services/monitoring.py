"""
monitoring.py — PropIQ System Health Monitor

Ports key patterns from sportsbookreview-scraper/monitoring.py and extends them
for PropIQ's 5-tier architecture. Provides:
  - HealthChecker: per-tier liveness + dependency checks
  - MetricTracker: rolling performance metrics (EV%, CLV, win rate, Brier drift)
  - AlertManager: threshold-based alerting with Discord webhook integration
  - FeatureDriftMonitor: PSI-based feature drift detection
  - ModelPerformanceMonitor: calibration drift + AUC tracking

Usage:
    from api.services.monitoring import HealthChecker, MetricTracker, AlertManager

    health   = HealthChecker()
    metrics  = MetricTracker(redis_client=redis)
    alerter  = AlertManager(webhook_url=DISCORD_WEBHOOK_URL)

    status = health.run_all_checks()
    alerter.check_thresholds(metrics.get_summary())
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")

_ALERT_COLOR_WARNING  = 0xFFA500  # orange
_ALERT_COLOR_CRITICAL = 0xFF0000  # red
_ALERT_COLOR_OK       = 0x00CC66  # green

# Rolling window sizes
_METRIC_WINDOW_DAYS  = 30
_MAX_DEQUE_SIZE      = 10_000    # per-metric ring buffer

# PSI bucket count for feature drift
_PSI_BINS = 10


# ---------------------------------------------------------------------------
# Data Classes
# ---------------------------------------------------------------------------

@dataclass
class HealthStatus:
    tier: str
    ok: bool
    latency_ms: float
    detail: str = ""
    checked_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


@dataclass
class BetRecord:
    slip_id: str
    agent: str
    prop_type: str
    ml_prob: float
    ev_pct: float
    kelly_fraction: float
    outcome: int               # 1 = win, 0 = loss, -1 = push
    clv: float                 # closing line value
    pnl_units: float           # profit/loss in units
    game_date: str


@dataclass
class AlertThresholds:
    min_win_rate: float          = 0.52
    min_clv_avg: float           = 0.005     # 0.5%
    max_brier_drift: float       = 0.015
    max_auc_drop: float          = 0.02
    max_psi: float               = 0.20
    max_kelly_avg: float         = 0.07
    min_slips_per_day: int       = 2
    max_slips_per_day: int       = 50
    min_discord_success_rate: float = 0.95
    max_queue_depth: int         = 1_000


# ---------------------------------------------------------------------------
# 1. HealthChecker
# ---------------------------------------------------------------------------

class HealthChecker:
    """
    Runs liveness checks for all 5 tiers + dependencies.
    Designed to back the GET /api/ml/health FastAPI endpoint.
    """

    def __init__(
        self,
        rabbitmq_url: str | None = None,
        redis_url: str | None = None,
        fastapi_base_url: str = "http://localhost:8000",
    ) -> None:
        self._rabbitmq_url    = rabbitmq_url    or os.getenv("RABBITMQ_URL", "")
        self._redis_url       = redis_url       or os.getenv("REDIS_URL", "")
        self._fastapi_base    = fastapi_base_url

    # ------------------------------------------------------------------
    def run_all_checks(self) -> dict[str, Any]:
        results: list[HealthStatus] = []
        results.append(self._check_rabbitmq())
        results.append(self._check_redis())
        results.append(self._check_fastapi())
        results.append(self._check_discord())

        all_ok = all(s.ok for s in results)
        return {
            "status": "ok" if all_ok else "degraded",
            "checks": [
                {
                    "tier": s.tier,
                    "ok": s.ok,
                    "latency_ms": round(s.latency_ms, 1),
                    "detail": s.detail,
                    "checked_at": s.checked_at,
                }
                for s in results
            ],
            "overall_ok": all_ok,
        }

    # ------------------------------------------------------------------
    def _check_rabbitmq(self) -> HealthStatus:
        start = time.monotonic()
        try:
            import pika  # type: ignore[import]
            conn = pika.BlockingConnection(
                pika.URLParameters(self._rabbitmq_url)
            )
            conn.close()
            return HealthStatus(
                tier="rabbitmq",
                ok=True,
                latency_ms=(time.monotonic() - start) * 1000,
            )
        except Exception as exc:
            return HealthStatus(
                tier="rabbitmq",
                ok=False,
                latency_ms=(time.monotonic() - start) * 1000,
                detail=str(exc),
            )

    def _check_redis(self) -> HealthStatus:
        start = time.monotonic()
        try:
            import redis as redis_lib  # type: ignore[import]
            r = redis_lib.from_url(self._redis_url, socket_timeout=2)
            r.ping()
            return HealthStatus(
                tier="redis",
                ok=True,
                latency_ms=(time.monotonic() - start) * 1000,
            )
        except Exception as exc:
            return HealthStatus(
                tier="redis",
                ok=False,
                latency_ms=(time.monotonic() - start) * 1000,
                detail=str(exc),
            )

    def _check_fastapi(self) -> HealthStatus:
        start = time.monotonic()
        try:
            import urllib.request
            url = f"{self._fastapi_base}/api/ml/health"
            with urllib.request.urlopen(url, timeout=3) as resp:
                body = json.loads(resp.read())
            ok = body.get("status") == "ok"
            return HealthStatus(
                tier="fastapi",
                ok=ok,
                latency_ms=(time.monotonic() - start) * 1000,
                detail="" if ok else f"unexpected status: {body.get('status')}",
            )
        except Exception as exc:
            return HealthStatus(
                tier="fastapi",
                ok=False,
                latency_ms=(time.monotonic() - start) * 1000,
                detail=str(exc),
            )

    def _check_discord(self) -> HealthStatus:
        """Non-destructive Discord check — sends to /api/webhooks/.../slack (no-op endpoint)."""
        start = time.monotonic()
        if not _DISCORD_WEBHOOK_URL:
            return HealthStatus(
                tier="discord",
                ok=False,
                latency_ms=0.0,
                detail="DISCORD_WEBHOOK_URL not set",
            )
        try:
            import urllib.request
            # Use the /github endpoint which accepts GET and returns 405 (not 404)
            probe_url = _DISCORD_WEBHOOK_URL.rstrip("/") + "/github"
            req = urllib.request.Request(probe_url, method="GET")
            try:
                urllib.request.urlopen(req, timeout=3)
            except Exception as probe_exc:
                # 405 = endpoint exists but wrong method → webhook URL is valid
                detail = str(probe_exc)
                ok = "405" in detail or "Method Not Allowed" in detail
            else:
                ok = True
                detail = ""
            return HealthStatus(
                tier="discord",
                ok=ok,
                latency_ms=(time.monotonic() - start) * 1000,
                detail="" if ok else detail,
            )
        except Exception as exc:
            return HealthStatus(
                tier="discord",
                ok=False,
                latency_ms=(time.monotonic() - start) * 1000,
                detail=str(exc),
            )


# ---------------------------------------------------------------------------
# 2. MetricTracker
# ---------------------------------------------------------------------------

class MetricTracker:
    """
    Rolling performance metric accumulator.
    Stores BetRecords in an in-memory ring buffer (also persists to Redis).
    Thread-safe via GIL (single-process assumption on Railway).
    """

    def __init__(self, redis_client: Any = None, window_days: int = _METRIC_WINDOW_DAYS) -> None:
        self._redis       = redis_client
        self._window_days = window_days
        self._records: deque[BetRecord] = deque(maxlen=_MAX_DEQUE_SIZE)

        # Per-agent counters
        self._agent_wins:   defaultdict[str, int]   = defaultdict(int)
        self._agent_total:  defaultdict[str, int]   = defaultdict(int)
        self._agent_pnl:    defaultdict[str, float] = defaultdict(float)

        # Discord delivery tracking
        self._discord_attempts:  int = 0
        self._discord_successes: int = 0

    # ------------------------------------------------------------------
    def record_bet(self, record: BetRecord) -> None:
        self._records.append(record)
        self._agent_total[record.agent] += 1
        if record.outcome == 1:
            self._agent_wins[record.agent] += 1
        self._agent_pnl[record.agent] += record.pnl_units

        if self._redis:
            try:
                key = f"propiq:bets:{record.game_date}"
                self._redis.rpush(key, json.dumps(record.__dict__))
                self._redis.expire(key, 86_400 * 60)    # 60-day TTL
            except Exception as exc:
                logger.warning("[MetricTracker] Redis write failed: %s", exc)

    def record_discord_attempt(self, success: bool) -> None:
        self._discord_attempts  += 1
        self._discord_successes += int(success)

    # ------------------------------------------------------------------
    def get_summary(self) -> dict[str, Any]:
        records = list(self._records)
        if not records:
            return {"n_bets": 0, "message": "no data yet"}

        import statistics

        wins       = [r for r in records if r.outcome == 1]
        losses     = [r for r in records if r.outcome == 0]
        win_rate   = len(wins) / len(records) if records else 0.0
        total_pnl  = sum(r.pnl_units for r in records)
        clv_vals   = [r.clv for r in records]
        ev_vals    = [r.ev_pct for r in records]

        daily_pnl: defaultdict[str, float] = defaultdict(float)
        for r in records:
            daily_pnl[r.game_date] += r.pnl_units

        pnl_series  = list(daily_pnl.values())
        sharpe      = 0.0
        if len(pnl_series) >= 5:
            mean_d = statistics.mean(pnl_series)
            std_d  = statistics.stdev(pnl_series)
            sharpe = (mean_d / std_d * (252 ** 0.5)) if std_d > 0 else 0.0

        # Max drawdown
        max_dd = 0.0
        peak   = 0.0
        cum    = 0.0
        for p in pnl_series:
            cum  += p
            peak  = max(peak, cum)
            max_dd = min(max_dd, cum - peak)

        discord_rate = (
            self._discord_successes / self._discord_attempts
            if self._discord_attempts > 0 else 1.0
        )

        return {
            "n_bets":                len(records),
            "n_wins":                len(wins),
            "n_losses":              len(losses),
            "win_rate":              round(win_rate, 4),
            "total_pnl_units":       round(total_pnl, 2),
            "roi":                   round(total_pnl / len(records), 4) if records else 0.0,
            "clv_avg":               round(statistics.mean(clv_vals), 4) if clv_vals else 0.0,
            "ev_avg":                round(statistics.mean(ev_vals), 4) if ev_vals else 0.0,
            "sharpe":                round(sharpe, 3),
            "max_drawdown":          round(max_dd, 3),
            "kelly_avg":             round(statistics.mean(r.kelly_fraction for r in records), 4),
            "discord_success_rate":  round(discord_rate, 4),
            "agent_breakdown": {
                agent: {
                    "wins":     self._agent_wins[agent],
                    "total":    self._agent_total[agent],
                    "win_rate": round(self._agent_wins[agent] / self._agent_total[agent], 4),
                    "pnl":      round(self._agent_pnl[agent], 2),
                }
                for agent in self._agent_total
            },
        }


# ---------------------------------------------------------------------------
# 3. FeatureDriftMonitor
# ---------------------------------------------------------------------------

class FeatureDriftMonitor:
    """
    Detects feature distribution drift using Population Stability Index (PSI).
    PSI < 0.10 = stable, 0.10–0.20 = mild drift, > 0.20 = significant drift.
    """

    def __init__(self, reference_stats: dict[str, dict] | None = None) -> None:
        """
        reference_stats: {feature_name: {"mean": ..., "std": ..., "bins": [...], "counts": [...]}}
        Load from a serialized baseline computed at training time.
        """
        self._reference = reference_stats or {}

    # ------------------------------------------------------------------
    def compute_psi(self, feature_name: str, current_values: list[float]) -> float:
        """
        Compute PSI between reference distribution and current observed values.
        Returns PSI score (0 = identical, > 0.2 = significant drift).
        """
        import math

        if feature_name not in self._reference:
            logger.debug("[FeatureDriftMonitor] No reference for %s, skipping", feature_name)
            return 0.0

        ref = self._reference[feature_name]
        bins   = ref["bins"]       # bin edges from reference
        ref_ct = ref["counts"]     # reference counts per bin

        # Bucket current values into same bins
        cur_ct = [0] * _PSI_BINS
        for v in current_values:
            idx = self._find_bin(v, bins)
            cur_ct[idx] += 1

        n_ref = sum(ref_ct)
        n_cur = sum(cur_ct)
        if n_cur == 0 or n_ref == 0:
            return 0.0

        psi = 0.0
        for r, c in zip(ref_ct, cur_ct):
            r_pct = max(r / n_ref, 1e-6)    # avoid log(0)
            c_pct = max(c / n_cur, 1e-6)
            psi  += (c_pct - r_pct) * math.log(c_pct / r_pct)

        return round(psi, 4)

    def _find_bin(self, value: float, bins: list[float]) -> int:
        for i, edge in enumerate(bins[1:], 1):
            if value <= edge:
                return i - 1
        return _PSI_BINS - 1

    # ------------------------------------------------------------------
    def check_all_features(
        self,
        current_data: dict[str, list[float]],
        threshold: float = 0.20,
    ) -> dict[str, float]:
        """
        Returns {feature_name: psi_score} for all monitored features.
        Features above threshold are logged as warnings.
        """
        results: dict[str, float] = {}
        for feat, vals in current_data.items():
            psi = self.compute_psi(feat, vals)
            results[feat] = psi
            if psi > threshold:
                logger.warning(
                    "[FeatureDriftMonitor] PSI=%.3f for '%s' exceeds threshold=%.2f",
                    psi, feat, threshold,
                )
        return results

    def update_reference(self, feature_name: str, values: list[float]) -> None:
        """Update reference distribution (call after retraining). Pure-Python, no numpy required."""
        if not values:
            return
        min_v = min(values)
        max_v = max(values)
        # Build evenly-spaced bin edges
        if min_v == max_v:
            max_v = min_v + 1.0          # avoid zero-width bins
        step  = (max_v - min_v) / _PSI_BINS
        edges = [min_v + i * step for i in range(_PSI_BINS + 1)]
        edges[-1] = max_v + 1e-9        # ensure last value falls in last bin

        counts = [0] * _PSI_BINS
        for v in values:
            idx = self._find_bin(v, edges)
            counts[idx] += 1

        mean = sum(values) / len(values)
        variance = sum((v - mean) ** 2 for v in values) / len(values)
        std  = variance ** 0.5

        self._reference[feature_name] = {
            "mean":   round(mean, 6),
            "std":    round(std, 6),
            "bins":   edges,
            "counts": counts,
        }


# ---------------------------------------------------------------------------
# 4. AlertManager
# ---------------------------------------------------------------------------

class AlertManager:
    """
    Threshold-based alerting that fires Discord embeds for WARNING and CRITICAL conditions.
    Implements cooldown deduplication — same alert won't fire more than once per hour.
    """

    def __init__(
        self,
        webhook_url: str | None = None,
        thresholds: AlertThresholds | None = None,
        cooldown_seconds: int = 3_600,
    ) -> None:
        self._webhook_url     = webhook_url or _DISCORD_WEBHOOK_URL
        self._thresholds      = thresholds or AlertThresholds()
        self._cooldown        = cooldown_seconds
        self._last_fired: dict[str, float] = {}   # alert_key → epoch time

    # ------------------------------------------------------------------
    def check_thresholds(self, metrics: dict[str, Any]) -> list[str]:
        """
        Evaluate metrics dict against thresholds. Fires Discord alerts for violations.
        Returns list of alert keys that fired.
        """
        fired: list[str] = []
        t = self._thresholds

        checks = [
            ("win_rate_low",    metrics.get("win_rate", 1.0) < t.min_win_rate,
             f"Win rate {metrics.get('win_rate', '?'):.1%} below {t.min_win_rate:.1%}",
             "WARNING"),

            ("clv_negative",    metrics.get("clv_avg", 1.0) < t.min_clv_avg,
             f"Avg CLV {metrics.get('clv_avg', '?'):.2%} below minimum {t.min_clv_avg:.2%}",
             "WARNING"),

            ("kelly_high",      metrics.get("kelly_avg", 0.0) > t.max_kelly_avg,
             f"Avg Kelly {metrics.get('kelly_avg', '?'):.2%} exceeds safe ceiling {t.max_kelly_avg:.2%}",
             "CRITICAL"),

            ("discord_low",     metrics.get("discord_success_rate", 1.0) < t.min_discord_success_rate,
             f"Discord delivery rate {metrics.get('discord_success_rate', '?'):.1%} below "
             f"{t.min_discord_success_rate:.1%}",
             "WARNING"),

            ("slips_low",       0 < metrics.get("n_bets", 999) < t.min_slips_per_day,
             f"Only {metrics.get('n_bets', '?')} slips fired — below minimum {t.min_slips_per_day}",
             "WARNING"),

            ("slips_high",      metrics.get("n_bets", 0) > t.max_slips_per_day,
             f"{metrics.get('n_bets', '?')} slips fired — above maximum {t.max_slips_per_day}",
             "WARNING"),
        ]

        for key, condition, message, level in checks:
            if condition and self._can_fire(key):
                self._fire_discord_alert(key, message, level)
                self._last_fired[key] = time.time()
                fired.append(key)

        return fired

    # ------------------------------------------------------------------
    def check_feature_drift(self, psi_scores: dict[str, float]) -> list[str]:
        fired: list[str] = []
        for feat, psi in psi_scores.items():
            level = None
            if psi > 0.35:
                level = "CRITICAL"
            elif psi > 0.20:
                level = "WARNING"

            if level:
                key     = f"psi_{feat}"
                message = f"Feature drift detected: **{feat}** PSI={psi:.3f} ({level})"
                if self._can_fire(key):
                    self._fire_discord_alert(key, message, level)
                    self._last_fired[key] = time.time()
                    fired.append(key)
        return fired

    def check_model_performance(
        self,
        brier_delta: float,
        auc_delta: float,
    ) -> list[str]:
        fired: list[str] = []
        t     = self._thresholds

        if brier_delta > t.max_brier_drift:
            key     = "brier_drift"
            message = f"Brier Score drifted +{brier_delta:.3f} from baseline (>{t.max_brier_drift})"
            level   = "CRITICAL" if brier_delta > t.max_brier_drift * 1.5 else "WARNING"
            if self._can_fire(key):
                self._fire_discord_alert(key, message, level)
                self._last_fired[key] = time.time()
                fired.append(key)

        if auc_delta < -t.max_auc_drop:
            key     = "auc_drop"
            message = f"AUC-ROC dropped {auc_delta:.3f} from 30-day rolling baseline"
            level   = "CRITICAL" if abs(auc_delta) > t.max_auc_drop * 2 else "WARNING"
            if self._can_fire(key):
                self._fire_discord_alert(key, message, level)
                self._last_fired[key] = time.time()
                fired.append(key)

        return fired

    # ------------------------------------------------------------------
    def _can_fire(self, key: str) -> bool:
        last = self._last_fired.get(key, 0.0)
        return (time.time() - last) > self._cooldown

    def _fire_discord_alert(self, key: str, message: str, level: str) -> None:
        if not self._webhook_url:
            logger.warning("[AlertManager] No webhook URL — alert not sent: %s", message)
            return

        color = _ALERT_COLOR_CRITICAL if level == "CRITICAL" else _ALERT_COLOR_WARNING

        payload = {
            "embeds": [{
                "title":       f"🚨 PropIQ Monitor — {level}",
                "description": message,
                "color":       color,
                "footer":      {"text": f"alert_key={key} | {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"},
            }]
        }

        import urllib.request
        data = json.dumps(payload).encode()
        req  = urllib.request.Request(
            self._webhook_url,
            data    = data,
            headers = {"Content-Type": "application/json"},
            method  = "POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status not in (200, 204):
                    logger.warning("[AlertManager] Discord returned %d for alert %s", resp.status, key)
                else:
                    logger.info("[AlertManager] Fired %s alert: %s", level, key)
        except Exception as exc:
            logger.error("[AlertManager] Failed to send alert %s: %s", key, exc)


# ---------------------------------------------------------------------------
# 5. ModelPerformanceMonitor (thin wrapper for scheduled checks)
# ---------------------------------------------------------------------------

class ModelPerformanceMonitor:
    """
    Scheduled monitor that compares current model performance vs stored baseline.
    Call `check()` from the BacktestTasklet or a scheduled job.
    """

    def __init__(
        self,
        alert_manager: AlertManager,
        baseline_brier: float = 0.228,
        baseline_auc:   float = 0.634,
    ) -> None:
        self._alerter        = alert_manager
        self._baseline_brier = baseline_brier
        self._baseline_auc   = baseline_auc

    def check(self, current_brier: float, current_auc: float) -> dict[str, Any]:
        brier_delta = current_brier - self._baseline_brier
        auc_delta   = current_auc   - self._baseline_auc

        fired = self._alerter.check_model_performance(brier_delta, auc_delta)

        result = {
            "baseline_brier": self._baseline_brier,
            "current_brier":  current_brier,
            "brier_delta":    round(brier_delta, 4),
            "baseline_auc":   self._baseline_auc,
            "current_auc":    current_auc,
            "auc_delta":      round(auc_delta, 4),
            "alerts_fired":   fired,
            "status":         "alert" if fired else "ok",
        }
        logger.info("[ModelPerformanceMonitor] %s", result)
        return result
