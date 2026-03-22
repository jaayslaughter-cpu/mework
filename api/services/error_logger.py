"""
PropIQ Analytics — ErrorLogger + Self-Correction Engine
=========================================================
Structured error logging with:
  - JSON-lines error log (rotating)
  - Pattern detection: systematic over/under-confidence
  - Alert thresholds (configurable)
  - Auto-correction hooks that feed back into PropModel
  - Daily summary report

Drop this into: api/services/error_logger.py
"""

import os
import json
import logging
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Callable, Any
from enum import Enum

logger = logging.getLogger(__name__)

LOG_DIR = Path(os.getenv("PROPIQ_LOG_DIR", "logs"))
ERROR_LOG_FILE = LOG_DIR / "propiq_errors.jsonl"
PREDICTION_LOG_FILE = LOG_DIR / "predictions.jsonl"
DAILY_SUMMARY_FILE = LOG_DIR / "daily_summary.json"

# Thresholds
ACCURACY_ALERT_THRESHOLD = float(os.getenv("ACCURACY_ALERT_THRESHOLD", "0.45"))  # below 45% = alert
EDGE_DECAY_THRESHOLD = float(os.getenv("EDGE_DECAY_THRESHOLD", "0.02"))  # avg edge below 2 cents = alert
BIAS_THRESHOLD = float(os.getenv("BIAS_THRESHOLD", "0.06"))  # 6% systematic bias = alert


class Severity(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class AlertType(str, Enum):
    LOW_ACCURACY = "LOW_ACCURACY"
    SYSTEMATIC_BIAS = "SYSTEMATIC_BIAS"
    EDGE_DECAY = "EDGE_DECAY"
    API_FAILURE = "API_FAILURE"
    MODEL_DRIFT = "MODEL_DRIFT"
    RATE_LIMIT = "RATE_LIMIT"


# ─────────────────────────────────────────────
# Core logger
# ─────────────────────────────────────────────
class PropIQLogger:
    """
    Writes structured JSON-lines logs for errors and predictions.
    Provides queryable history for pattern detection.
    """

    def __init__(self):
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        self._handlers: List[Callable] = []   # alert callbacks

    @staticmethod
    def _write(filepath: Path, record: Dict):
        allowed_files = {"error.log", "warning.log"}
        filename = filepath.name
        if filename not in allowed_files:
            raise ValueError(f"Invalid file name: {filename}")
        safe_path = Path("/var/log/prop_iq") / filename
        with open(safe_path, "a") as f:
            f.write(json.dumps(record) + "\n")

    def log_error(
        self,
        severity: Severity,
        error_type: str,
        message: str,
        context: Optional[Dict] = None,
        exc: Optional[Exception] = None,
    ):
        record = {
            "ts": datetime.utcnow().isoformat(),
            "severity": severity.value,
            "error_type": error_type,
            "message": message,
            "context": context or {},
            "traceback": traceback.format_exc() if exc else None,
        }
        self._write(ERROR_LOG_FILE, record)

        # Mirror to Python logger
        log_fn = {
            Severity.DEBUG: logger.debug,
            Severity.INFO: logger.info,
            Severity.WARNING: logger.warning,
            Severity.ERROR: logger.error,
            Severity.CRITICAL: logger.critical,
        }.get(severity, logger.info)
        log_fn(f"[{error_type}] {message}")
    def log_prediction_outcome(
        self,
        player: str,
        prop_type: str,
        model_prob: float,
        book_prob: float,
        actual_result: float,
        edge: float,
        game_date: str,
    ):
        record = {
            "ts": datetime.utcnow().isoformat(),
            "game_date": game_date,
            "player": player,
            "prop_type": prop_type,
            "model_prob": model_prob,
            "book_prob": book_prob,
            "actual": actual_result,
            "edge": edge,
            "correct": int((model_prob > 0.5) == (actual_result > 0.5)),
            "error": round(model_prob - actual_result, 4),
        }
        self._write(PREDICTION_LOG_FILE, record)

    @staticmethod
    def read_prediction_log(days: int = 30) -> List[Dict]:
        """Read recent prediction outcomes."""
        if not PREDICTION_LOG_FILE.exists():
            return []
        cutoff = datetime.utcnow() - timedelta(days=days)
        records = []
        with open(PREDICTION_LOG_FILE) as f:
            for line in f:
                try:
                    r = json.loads(line.strip())
                    if datetime.fromisoformat(r["ts"]) >= cutoff:
                        records.append(r)
                except (json.JSONDecodeError, KeyError):
                    continue
        return records

    @staticmethod
    def read_error_log(days: int = 7, severity: Optional[Severity] = None) -> List[Dict]:
        """Read recent errors."""
        if not ERROR_LOG_FILE.exists():
            return []
        cutoff = datetime.utcnow() - timedelta(days=days)
        records = []
        with open(ERROR_LOG_FILE) as f:
            for line in f:
                try:
                    r = json.loads(line.strip())
                    ts = datetime.fromisoformat(r["ts"])
                    if ts >= cutoff and (severity is None or r["severity"] == severity.value):
                        records.append(r)
                except (json.JSONDecodeError, KeyError):
                    continue
        return records

    def register_alert_handler(self, fn: Callable[[AlertType, str, Dict], None]):
        """Register a callback for alerts. Signature: fn(alert_type, message, data)"""
        self._handlers.append(fn)

    def _fire_alert(self, alert_type: AlertType, message: str, data: Dict):
        self.log_error(Severity.WARNING, alert_type.value, message, data)
        for fn in self._handlers:
            try:
                fn(alert_type, message, data)
            except Exception as e:
                logger.error(f"Alert handler error: {e}")


# ─────────────────────────────────────────────
# Pattern detector + self-correction engine
# ─────────────────────────────────────────────
class SelfCorrectionEngine:
    """
    Analyzes prediction logs and fires alerts when systematic errors are detected.
    Generates correction deltas that feed back into CalibrationLayer.
    """

    def __init__(self, prop_logger: PropIQLogger):
        self.logger = prop_logger

    def run_analysis(self, min_samples: int = 10) -> Dict:
        """
        Run full analysis on recent prediction log.
        Returns: corrections dict + detected alerts.
        """
        records = self.logger.read_prediction_log(days=30)
        if not records:
            return {"status": "no_data", "corrections": {}, "alerts": []}

        # Group by (player, prop_type)
        from collections import defaultdict
        groups: Dict = defaultdict(list)
        for r in records:
            key = (r.get("player", "?"), r.get("prop_type", "?"))
            groups[key].append(r)

        corrections = {}
        alerts = []

        for (player, prop_type), recs in groups.items():
            if len(recs) < min_samples:
                continue

            errors = [r["error"] for r in recs if "error" in r]
            corrects = [r["correct"] for r in recs if "correct" in r]
            edges = [r.get("edge", 0) for r in recs]

            mean_error = sum(errors) / len(errors) if errors else 0
            accuracy = sum(corrects) / len(corrects) if corrects else 0.5
            avg_edge = sum(edges) / len(edges) if edges else 0

            # Detect systematic bias
            if abs(mean_error) > BIAS_THRESHOLD:
                direction = "over-predicting" if mean_error > 0 else "under-predicting"
                msg = f"{player} {prop_type}: {direction} by {mean_error:.1%} (n={len(recs)})"
                corrections[(player, prop_type)] = -round(mean_error, 4)
                alerts.append({
                    "type": AlertType.SYSTEMATIC_BIAS.value,
                    "player": player,
                    "prop_type": prop_type,
                    "mean_error": round(mean_error, 4),
                    "correction": corrections[(player, prop_type)],
                    "samples": len(recs),
                })
                self.logger.fire_alert(AlertType.SYSTEMATIC_BIAS, msg, {
                    "player": player, "prop_type": prop_type,
                    "mean_error": mean_error, "n": len(recs),
                })

            # Low accuracy alert
            if accuracy < ACCURACY_ALERT_THRESHOLD and len(corrects) >= min_samples:
                msg = f"Low accuracy {accuracy:.1%} for {player} {prop_type} over {len(recs)} predictions"
                alerts.append({
                    "type": AlertType.LOW_ACCURACY.value,
                    "player": player,
                    "prop_type": prop_type,
                    "accuracy": round(accuracy, 4),
                    "samples": len(recs),
                })
                self.logger.fire_alert(AlertType.LOW_ACCURACY, msg, {
                    "accuracy": accuracy, "n": len(recs),
                })

            # Edge decay
            if avg_edge < EDGE_DECAY_THRESHOLD and len(edges) >= min_samples:
                msg = f"Edge decaying: avg edge {avg_edge:.3f} for {prop_type}"
                alerts.append({
                    "type": AlertType.EDGE_DECAY.value,
                    "prop_type": prop_type,
                    "avg_edge": round(avg_edge, 4),
                    "samples": len(recs),
                })
                self.logger.fire_alert(AlertType.EDGE_DECAY, msg, {"avg_edge": avg_edge})

        return {
            "status": "ok",
            "analyzed": len(records),
            "corrections": {f"{k[0]}|{k[1]}": v for k, v in corrections.items()},
            "alerts": alerts,
            "alert_count": len(alerts),
        }

    def generate_daily_summary(self) -> Dict:
        """Generate and save daily accuracy + edge summary."""
        records = self.logger.read_prediction_log(days=1)
        if not records:
            summary = {"date": str(datetime.utcnow().date()), "status": "no_predictions_today"}
        else:
            errors = [r.get("error", 0) for r in records]
            corrects = [r.get("correct", 0) for r in records]
            edges = [r.get("edge", 0) for r in records]
            summary = {
                "date": str(datetime.utcnow().date()),
                "total_predictions": len(records),
                "accuracy": round(sum(corrects) / len(corrects), 4) if corrects else None,
                "mae": round(sum(abs(e) for e in errors) / len(errors), 4) if errors else None,
                "avg_edge": round(sum(edges) / len(edges), 4) if edges else None,
                "max_edge": round(max(edges), 4) if edges else None,
                "strong_plays": sum(1 for e in edges if e >= 0.08),
                "fades": sum(1 for e in edges if e <= -0.08),
            }

        with open(DAILY_SUMMARY_FILE, "w") as f:
            json.dump(summary, f, indent=2)

        return summary

    def get_model_health(self) -> Dict:
        """Quick model health check."""
        records = self.logger.read_prediction_log(days=7)
        if len(records) < 5:
            return {"status": "insufficient_data", "n": len(records)}

        errors = [r.get("error", 0) for r in records]
        corrects = [r.get("correct", 0) for r in records]
        accuracy = sum(corrects) / len(corrects)
        mae = sum(abs(e) for e in errors) / len(errors)

        health = "HEALTHY"
        if accuracy < 0.45:
            health = "POOR"
        elif accuracy < 0.52:
            health = "FAIR"
        elif accuracy > 0.58:
            health = "EXCELLENT"

        return {
            "status": health,
            "accuracy_7d": round(accuracy, 4),
            "mae_7d": round(mae, 4),
            "sample_size": len(records),
            "alert_count": len(self.logger.read_error_log(days=7, severity=Severity.WARNING)),
        }


# ─────────────────────────────────────────────
# Convenience wrappers for API error logging
# ─────────────────────────────────────────────
_prop_logger = PropIQLogger()
_self_correction = SelfCorrectionEngine(_prop_logger)


def log_api_error(source: str, message: str, exc: Optional[Exception] = None):
    _prop_logger.log_error(Severity.ERROR, AlertType.API_FAILURE.value, f"[{source}] {message}", exc=exc)


def log_rate_limit(source: str, retry_after: int):
    _prop_logger.log_error(
        Severity.WARNING, AlertType.RATE_LIMIT.value,
        f"[{source}] Rate limited. Retry after {retry_after}s",
        context={"retry_after": retry_after},
    )


def log_prediction_outcome(
    player: str, prop_type: str, model_prob: float, book_prob: float,
    actual_result: float, edge: float, game_date: str,
):
    _prop_logger.log_prediction_outcome(player, prop_type, model_prob, book_prob, actual_result, edge, game_date)


def get_logger() -> PropIQLogger:
    return _prop_logger


def get_self_correction() -> SelfCorrectionEngine:
    return _self_correction
