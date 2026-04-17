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

PR #334: Added daily dedup guard via ``drift_alert_date_log`` Postgres table.
         Railway startup misfires no longer cause duplicate drift alerts.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Optional

import requests

logger = logging.getLogger(__name__)

DISCORD_WEBHOOK = os.getenv(
    "DISCORD_WEBHOOK_URL",   # matches Railway env var set for all other senders
    "https://discordapp.com/api/webhooks/1484795164961800374/"
    "jYxCVWeN8F1TFIs9SFjQtr0lZASPitLRnGBwjD3Oo2CknXOqVZB2gmmLqqQ1eH-_2liM",
)
DRIFT_THRESHOLD_PCT = 15.0   # % increase in Brier score that triggers alert
DRIFT_ABSOLUTE_MAX = 0.22    # absolute Brier ceiling before governor kicks in

# Legacy JSON path (used only as fallback when DB unavailable)
_BRIER_JSON_FALLBACK = "/tmp/brier_score_ledger.json"


# ── Postgres helpers ───────────────────────────────────────────────────────────

def _get_brier_conn():
    import psycopg2  # noqa: PLC0415
    return psycopg2.connect(os.environ["DATABASE_URL"])


def _ensure_brier_table() -> None:
    try:
        conn = _get_brier_conn()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS brier_ledger (
                id          SERIAL PRIMARY KEY,
                brier_score FLOAT NOT NULL,
                graded_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning(f"brier_ledger table ensure failed: {e}")


# ── Dedup guard — one alert per calendar day (America/Los_Angeles) ─────────────

def _ensure_drift_alert_log_table() -> None:
    """Create drift_alert_date_log if it doesn't exist."""
    try:
        conn = _get_brier_conn()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS drift_alert_date_log (
                alert_date DATE PRIMARY KEY
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as exc:
        logger.warning("[DriftMonitor] drift_alert_date_log ensure failed: %s", exc)


def _drift_alert_already_ran_today() -> bool:
    """Return True if a drift alert was already sent today (PT)."""
    try:
        import pytz  # noqa: PLC0415
    except ImportError:
        pytz = None  # type: ignore[assignment]

    try:
        if pytz:
            pt = pytz.timezone("America/Los_Angeles")
            today_pt = datetime.now(pt).date()
        else:
            # Fallback: UTC-7 offset (good enough for Railway guard)
            from datetime import timezone, timedelta  # noqa: PLC0415
            today_pt = datetime.now(timezone(timedelta(hours=-7))).date()

        _ensure_drift_alert_log_table()
        conn = _get_brier_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM drift_alert_date_log WHERE alert_date = %s",
            (today_pt,),
        )
        exists = cur.fetchone() is not None
        cur.close()
        conn.close()
        return exists
    except Exception as exc:
        logger.warning("[DriftMonitor] dedup check failed: %s — allowing send", exc)
        return False  # fail open: allow send rather than suppress


def _record_drift_alert_ran_today() -> None:
    """Stamp today's PT date so subsequent runs skip the alert."""
    try:
        import pytz  # noqa: PLC0415
    except ImportError:
        pytz = None  # type: ignore[assignment]

    try:
        if pytz:
            pt = pytz.timezone("America/Los_Angeles")
            today_pt = datetime.now(pt).date()
        else:
            from datetime import timezone, timedelta  # noqa: PLC0415
            today_pt = datetime.now(timezone(timedelta(hours=-7))).date()

        _ensure_drift_alert_log_table()
        conn = _get_brier_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO drift_alert_date_log (alert_date) VALUES (%s) ON CONFLICT DO NOTHING",
            (today_pt,),
        )
        conn.commit()
        cur.close()
        conn.close()
        logger.info("[DriftMonitor] Drift alert date logged: %s", today_pt)
    except Exception as exc:
        logger.warning("[DriftMonitor] Failed to record alert date: %s", exc)


def _save_brier_pg(score: float) -> None:
    """Persist Brier score to Postgres. Falls back to JSON if DB unavailable."""
    try:
        _ensure_brier_table()
        conn = _get_brier_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO brier_ledger (brier_score) VALUES (%s)",
            (score,),
        )
        conn.commit()
        cur.close()
        conn.close()
        logger.debug("[DriftMonitor] Brier %.4f saved to Postgres", score)
    except Exception as e:
        logger.error(f"brier_ledger DB save failed, falling back to JSON: {e}")
        _save_brier_json(score)


def _load_last_brier_pg() -> float:
    """Load the previous Brier score from Postgres (or JSON fallback)."""
    try:
        conn = _get_brier_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT brier_score FROM brier_ledger ORDER BY id DESC LIMIT 2"
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        if len(rows) >= 2:
            return float(rows[1][0])   # second-most-recent = previous week
        elif len(rows) == 1:
            return float(rows[0][0])   # only one record → use it as "old"
        return 0.18  # no history yet
    except Exception as e:
        logger.warning(f"brier_ledger DB load failed, falling back to JSON: {e}")
        return _load_last_brier_json()


def _save_brier_json(score: float) -> None:
    """JSON fallback for Brier persistence."""
    try:
        if os.path.exists(_BRIER_JSON_FALLBACK):
            with open(_BRIER_JSON_FALLBACK) as f:
                ledger = json.load(f)
        else:
            ledger = {"history": [], "last_score": 0.18}
        ledger.setdefault("history", []).append(round(score, 4))
        ledger["history"] = ledger["history"][-52:]
        ledger["last_score"] = round(score, 4)
        with open(_BRIER_JSON_FALLBACK, "w") as f:
            json.dump(ledger, f, indent=2)
    except Exception as e:
        logger.error(f"brier JSON fallback also failed: {e}")


def _load_last_brier_json() -> float:
    try:
        if os.path.exists(_BRIER_JSON_FALLBACK):
            with open(_BRIER_JSON_FALLBACK) as f:
                data = json.load(f)
            return float(data.get("last_score", 0.18))
    except Exception:
        pass
    return 0.18


# ── Drift Check ───────────────────────────────────────────────────────────────

def check_for_model_drift(new_brier: float, old_brier: float) -> bool:
    """Returns True (and fires Discord alert) if drift is detected.

    PR #334: Alert is deduped — only one Discord message per calendar day (PT).
    Railway startup misfires trigger the same code path but the second call
    finds today's date already in drift_alert_date_log and skips.
    """
    if old_brier <= 0:
        return False
    drift_pct = ((new_brier - old_brier) / old_brier) * 100.0
    if drift_pct > DRIFT_THRESHOLD_PCT or new_brier > DRIFT_ABSOLUTE_MAX:
        # Dedup: only send once per PT calendar day
        if _drift_alert_already_ran_today():
            logger.info(
                "[DriftMonitor] Drift detected but alert already sent today — skipping duplicate"
            )
            return True  # still return True so governor stays active
        _send_drift_alert(new_brier, drift_pct)
        _record_drift_alert_ran_today()
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

    Persists to Postgres ``brier_ledger`` table. Falls back to JSON file
    at /tmp/brier_score_ledger.json if DATABASE_URL is unavailable.

    Returns True if drift was detected (Discord alert fired).
    Should be called from ``calibrate_model.py`` weekly.
    """
    old_score = _load_last_brier_pg()
    _save_brier_pg(round(new_score, 4))

    drift = check_for_model_drift(new_score, old_score)
    logger.info(
        "[DriftMonitor] Brier recorded: %.4f (prev=%.4f drift=%s)",
        new_score, old_score, "YES" if drift else "NO",
    )
    return drift


def get_current_brier() -> float:
    """Return the most recently recorded Brier score."""
    try:
        conn = _get_brier_conn()
        cur = conn.cursor()
        cur.execute("SELECT brier_score FROM brier_ledger ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            return float(row[0])
    except Exception:
        pass
    return _load_last_brier_json()
