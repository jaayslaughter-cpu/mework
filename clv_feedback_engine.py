"""
clv_feedback_engine.py
======================
Step 2 of the architectural modeling audit.

Purpose
-------
Close the learning loop: after each night's bets are settled (status=WIN/LOSS),
query the ledger grouped by sim_edge_reason tag, compute win-rate + ROI + avg CLV
per tag, and persist a threshold override to the `edge_thresholds` table.

`_BaseAgent` calls `get_threshold(edge_reasons)` instead of the bare MIN_EV_THRESH
constant, so agents that consistently find real edge lower their bar, and agents
whose edge type has historically been noise raise theirs.

Architecture
------------
  bet_ledger (settled rows, sim_edge_reasons TEXT[])
        ↓ rebuild_thresholds() — run nightly after grading
  edge_thresholds (one row per tag)
        ↓ get_threshold([tag, ...]) — called per bet in _BaseAgent
  ev_pct >= threshold  →  queue or drop

Threshold rules (minimum 25 settled bets required per tag before deviation):
  win_rate >= 0.60  AND  avg_clv >= 0      → threshold = 0.020  (lower bar — edge is real)
  win_rate >= 0.55  AND  avg_clv >= -1.0   → threshold = 0.025  (mild confidence)
  win_rate  < 0.48  OR   avg_clv < -2.0    → threshold = 0.050  (raise bar — mostly noise)
  otherwise                                → threshold = 0.030  (neutral / default)
  < 25 samples                             → threshold = 0.030  (not enough data)

The function get_threshold() returns the MINIMUM threshold across all edge reasons
present on a prop — i.e. if any one reason earns a lower threshold, use it.
"""

from __future__ import annotations
import json
import logging
import os
from typing import List, Optional

import psycopg2

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────
MIN_EV_THRESH   = 0.030   # global default — matches tasklets.py constant
MIN_SAMPLES     = 25      # minimum settled bets before deviation allowed

# Threshold tiers
TIER_LOW        = 0.020   # proven edge type
TIER_MILD       = 0.025   # above-average edge type
TIER_DEFAULT    = 0.030   # neutral
TIER_HIGH       = 0.050   # noisy / negative edge type

# ── DB helpers ───────────────────────────────────────────────────────────────
def _get_conn():
    url = os.environ.get("DATABASE_URL", "")
    if not url:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(url, sslmode="require")


def _ensure_tables() -> None:
    """Create edge_thresholds and add sim_edge_reasons to bet_ledger if absent."""
    ddl = [
        # Per-tag performance + threshold override
        """
        CREATE TABLE IF NOT EXISTS edge_thresholds (
            edge_reason   TEXT         PRIMARY KEY,
            sample_size   INTEGER      DEFAULT 0,
            win_rate      FLOAT        DEFAULT 0.5,
            avg_clv       FLOAT        DEFAULT 0.0,
            roi           FLOAT        DEFAULT 0.0,
            threshold     FLOAT        DEFAULT 0.030,
            updated_at    TIMESTAMP    DEFAULT NOW()
        )
        """,
        # FIX PR#278: bet_ledger CREATE TABLE removed — schema owned by Flyway migrations.
        # Old schema used "direction" column (should be "side") causing Error 1/3 on fresh deploys.
        # Safe migration: add any columns introduced after initial deploy
        """
        ALTER TABLE bet_ledger
            ADD COLUMN IF NOT EXISTS sim_edge_reasons TEXT DEFAULT '[]'
        """,
        """
        ALTER TABLE bet_ledger
            ADD COLUMN IF NOT EXISTS actual_outcome INTEGER
        """,
        """
        ALTER TABLE bet_ledger
            ADD COLUMN IF NOT EXISTS profit_loss FLOAT
        """,
        """
        ALTER TABLE bet_ledger
            ADD COLUMN IF NOT EXISTS clv FLOAT
        """,
        """
        ALTER TABLE bet_ledger
            ADD COLUMN IF NOT EXISTS kelly_units FLOAT
        """,
    ]
    conn = _get_conn()
    try:
        cur = conn.cursor()
        for stmt in ddl:
            cur.execute(stmt)
        conn.commit()
    except Exception as exc:
        logger.warning("[CLVFeedback] Schema migration error: %s", exc)
    finally:
        conn.close()


# ── Core: rebuild thresholds ─────────────────────────────────────────────────
def rebuild_thresholds() -> dict:
    """
    Query all settled bets in the ledger, unnest sim_edge_reasons, and compute
    per-tag win-rate / ROI / avg-CLV.  Persist results to edge_thresholds table.

    Returns dict of {edge_reason: threshold} for logging.
    """
    _ensure_tables()

    # Pull all settled (WIN/LOSS) rows with edge reasons
    sql = """
        SELECT
            sim_edge_reasons,
            actual_outcome,        -- 1=WIN, 0=LOSS
            profit_loss,
            clv,
            kelly_units
        FROM bet_ledger
        WHERE status IN ('WIN', 'LOSS')
          AND sim_edge_reasons IS NOT NULL
          AND sim_edge_reasons != '[]'
          AND sim_edge_reasons != ''
    """
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
    except Exception as exc:
        logger.error("[CLVFeedback] Query failed: %s", exc)
        conn.close()
        return {}

    # Aggregate per tag
    tag_stats: dict = {}  # tag → {wins, losses, pl, clv_sum, units}

    for (reasons_raw, outcome, pl, clv_val, units) in rows:
        try:
            reasons: list = json.loads(reasons_raw) if reasons_raw else []
        except (ValueError, TypeError):
            reasons = []

        if not reasons:
            continue

        for tag in reasons:
            if not tag:
                continue
            s = tag_stats.setdefault(tag, {
                "wins": 0, "losses": 0, "pl": 0.0,
                "clv_sum": 0.0, "units": 0.0
            })
            s["wins"]    += int(outcome == 1)
            s["losses"]  += int(outcome == 0)
            s["pl"]      += float(pl or 0)
            s["clv_sum"] += float(clv_val or 0)
            s["units"]   += abs(float(units or 0))

    if not tag_stats:
        logger.info("[CLVFeedback] No settled bets with edge tags — thresholds unchanged.")
        conn.close()
        return {}

    # Compute metrics + pick threshold tier
    results = {}
    for tag, s in tag_stats.items():
        total = s["wins"] + s["losses"]
        win_rate = s["wins"] / total if total > 0 else 0.5
        avg_clv  = s["clv_sum"] / total if total > 0 else 0.0
        roi      = s["pl"] / s["units"] if s["units"] > 0 else 0.0

        if total < MIN_SAMPLES:
            threshold = TIER_DEFAULT
            reason    = "insufficient_samples"
        elif win_rate >= 0.60 and avg_clv >= 0.0:
            threshold = TIER_LOW
            reason    = "proven_edge"
        elif win_rate >= 0.55 and avg_clv >= -1.0:
            threshold = TIER_MILD
            reason    = "above_avg_edge"
        elif win_rate < 0.48 or avg_clv < -2.0:
            threshold = TIER_HIGH
            reason    = "noise_edge"
        else:
            threshold = TIER_DEFAULT
            reason    = "neutral"

        results[tag] = {
            "threshold": threshold,
            "win_rate":  round(win_rate, 4),
            "avg_clv":   round(avg_clv, 3),
            "roi":       round(roi, 4),
            "sample_size": total,
            "tier_reason": reason,
        }

        logger.info(
            "[CLVFeedback] %-30s  n=%-4d  WR=%.1f%%  CLV=%+.2f  ROI=%+.1f%%  → threshold=%.3f  (%s)",
            tag, total, win_rate * 100, avg_clv, roi * 100, threshold, reason
        )

    # Upsert into edge_thresholds
    try:
        cur = conn.cursor()
        for tag, m in results.items():
            cur.execute(
                """
                INSERT INTO edge_thresholds
                    (edge_reason, sample_size, win_rate, avg_clv, roi, threshold, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (edge_reason) DO UPDATE SET
                    sample_size = EXCLUDED.sample_size,
                    win_rate    = EXCLUDED.win_rate,
                    avg_clv     = EXCLUDED.avg_clv,
                    roi         = EXCLUDED.roi,
                    threshold   = EXCLUDED.threshold,
                    updated_at  = NOW()
                """,
                (tag, m["sample_size"], m["win_rate"],
                 m["avg_clv"], m["roi"], m["threshold"]),
            )
        conn.commit()
        logger.info("[CLVFeedback] Persisted %d edge threshold overrides.", len(results))
    except Exception as exc:
        logger.error("[CLVFeedback] Upsert failed: %s", exc)
    finally:
        conn.close()

    return {t: m["threshold"] for t, m in results.items()}


# ── Runtime: get threshold for a prop ────────────────────────────────────────
_threshold_cache: dict = {}   # tag → threshold (refreshed each rebuild call)


def load_thresholds() -> dict:
    """
    Load current thresholds from DB into the in-process cache.
    Called once at startup and after each rebuild.
    """
    global _threshold_cache
    try:
        _ensure_tables()
        conn = _get_conn()
        cur  = conn.cursor()
        cur.execute("SELECT edge_reason, threshold FROM edge_thresholds")
        rows = cur.fetchall()
        conn.close()
        _threshold_cache = {r[0]: float(r[1]) for r in rows}
        logger.info("[CLVFeedback] Loaded %d threshold overrides from DB.", len(_threshold_cache))
    except Exception as exc:
        logger.warning("[CLVFeedback] Could not load thresholds (using defaults): %s", exc)
        _threshold_cache = {}
    return _threshold_cache


def get_threshold(edge_reasons: Optional[List[str]] = None) -> float:
    """
    Return the minimum (most permissive) threshold across all edge_reasons.

    Logic:
      - If no reasons or no overrides exist for those reasons → MIN_EV_THRESH
      - Otherwise → min threshold found across active reasons

    The minimum rule means: if even ONE structural reason on a prop has a proven
    track record, we keep the lower bar.  This rewards specificity.
    """
    if not edge_reasons:
        return MIN_EV_THRESH

    overrides = [
        _threshold_cache[r]
        for r in edge_reasons
        if r in _threshold_cache
    ]
    if not overrides:
        return MIN_EV_THRESH

    return min(overrides)


# ── Discord summary helper ───────────────────────────────────────────────────
def build_discord_summary() -> str:
    """
    Return a short Discord-ready text block summarising the current edge_thresholds.
    Called by nightly_recap.py.
    """
    if not _threshold_cache:
        load_thresholds()

    if not _threshold_cache:
        return "No edge threshold data yet."

    lines = ["**Edge Threshold Report**"]
    tier_labels = {
        TIER_LOW:     "🟢 Proven",
        TIER_MILD:    "🔵 Above-avg",
        TIER_DEFAULT: "⚪ Neutral",
        TIER_HIGH:    "🔴 Noisy",
    }
    # Sort: best first
    sorted_tags = sorted(_threshold_cache.items(), key=lambda x: x[1])
    for tag, thr in sorted_tags:
        label = tier_labels.get(round(thr, 3), "⚪")
        lines.append(f"  {label} `{tag}` → {thr:.3f}")
    return "\n".join(lines)


# ── Bootstrap ────────────────────────────────────────────────────────────────
def _bootstrap():
    """Called at module import — loads cache from DB so get_threshold() works immediately."""
    try:
        load_thresholds()
    except Exception:
        pass  # DB not yet available at import time on Railway — that's fine


_bootstrap()
