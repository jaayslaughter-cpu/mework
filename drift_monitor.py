"""
drift_monitor.py — PropIQ Model Drift Detection
================================================
Sends a Discord alert when the Brier score degrades by > 15 % week-over-week
or exceeds the absolute threshold of 0.22.

Triggered by ``calibrate_model.py`` every Monday morning.

When drift is detected:
  • Discord alert fires with red embed
  • The calibration governor in ``calibration_layer.py`` automatically
    shrinks bet sizes 50 % toward market until scores recover
"""
from __future__ import annotations

import json
import logging
import os
from typing import Optional

import requests

logger = logging.getLogger(__name__)

DISCORD_WEBHOOK = os.getenv(
    "DISCORD_WEBHOOK",
    "https://discordapp.com/api/webhooks/1484795164961800374/"
    "jYxCVWeN8F1TFIs9SFjQtr0lZASPitLRnGBwjD3Oo2CknXOqVZB2gmmLqqQ1eH-_2liM",
)
BRIER_LEDGER_PATH = os.getenv("BRIER_LEDGER_PATH", "brier_score_ledger.json")
DRIFT_THRESHOLD_PCT = 15.0   # % increase in Brier score that triggers alert
DRIFT_ABSOLUTE_MAX = 0.22    # absolute Brier ceiling before governor kicks in


# ── Ledger I/O ────────────────────────────────────────────────────────────────

def load_brier_ledger() -> dict:
    try:
        with open(BRIER_LEDGER_PATH) as fh:
            return json.load(fh)
    except Exception:
        return {"history": [], "last_score": 0.18}


def save_brier_ledger(ledger: dict) -> None:
    with open(BRIER_LEDGER_PATH, "w") as fh:
        json.dump(ledger, fh, indent=2)


# ── Drift Check ───────────────────────────────────────────────────────────────

def check_for_model_drift(new_brier: float, old_brier: float) -> bool:
    """Returns True (and fires Discord alert) if drift is detected."""
    if old_brier <= 0:
        return False
    drift_pct = ((new_brier - old_brier) / old_brier) * 100.0
    if drift_pct > DRIFT_THRESHOLD_PCT or new_brier > DRIFT_ABSOLUTE_MAX:
        _send_drift_alert(new_brier, drift_pct)
        return True
    return False


def _send_drift_alert(brier: float, drift_pct: float) -> None:
    payload = {
        "content": "⚠️ **PROPIQ MODEL DRIFT DETECTED** ⚠️",
        "embeds": [
            {
                "title": "Weekly Calibration Alert",
                "color": 15158332,  # red
                "fields": [
                    {
                        "name": "Current Brier Score",
                        "value": f"`{brier:.4f}`",
                        "inline": True,
                    },
                    {
                        "name": "Week-over-Week Drift",
                        "value": f"`+{drift_pct:.1f}%`",
                        "inline": True,
                    },
                    {
                        "name": "Auto-Governor",
                        "value": "✅ Active — stakes reduced 50 % toward market",
                        "inline": False,
                    },
                    {
                        "name": "Recommended Action",
                        "value": "Review `prediction_results.csv` for data source issues",
                        "inline": False,
                    },
                ],
                "footer": {"text": "PropIQ Drift Monitor | calibrate_model.py"},
            }
        ],
    }
    try:
        r = requests.post(DISCORD_WEBHOOK, json=payload, timeout=8)
        r.raise_for_status()
        logger.info("[DriftMonitor] Alert sent (brier=%.4f drift=+%.1f%%)", brier, drift_pct)
    except Exception as exc:
        logger.warning("[DriftMonitor] Failed to send alert: %s", exc)


# ── Public API ────────────────────────────────────────────────────────────────

def record_brier(new_score: float) -> bool:
    """Record a Brier score and check for drift.

    Returns True if drift was detected (Discord alert fired).
    Should be called from ``calibrate_model.py`` weekly.
    """
    ledger = load_brier_ledger()
    old_score = ledger.get("last_score", 0.18)

    ledger.setdefault("history", []).append(round(new_score, 4))
    ledger["history"] = ledger["history"][-52:]  # keep 52 weeks
    ledger["last_score"] = round(new_score, 4)
    save_brier_ledger(ledger)

    drift = check_for_model_drift(new_score, old_score)
    logger.info(
        "[DriftMonitor] Brier recorded: %.4f (prev=%.4f drift=%s)",
        new_score, old_score, "YES" if drift else "NO",
    )
    return drift


def get_current_brier() -> float:
    """Return the most recently recorded Brier score."""
    return float(load_brier_ledger().get("last_score", 0.18))
