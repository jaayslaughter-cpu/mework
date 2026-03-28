"""
temperature_calibration.py
==========================
Phase 47 — Live Temperature Calibration Loop.

After each night's settlement, nightly_recap.py writes per-leg outcomes to
the `agent_calibration_data` table.  This module reads that history and fits
an agent-specific temperature scalar T (Platt scaling) whenever an agent has
accumulated ≥ MIN_SAMPLES graded picks.

T > 1 compresses overconfident probabilities downward (most agents will land
here — the Phase 46 backtest showed T=3.0 on raw Bayesian signal).
T = 1 means the model is already well-calibrated — no change applied.
T < 1 (rare) would expand probabilities — only fires if the model is
systematically underconfident.

Usage:
    from temperature_calibration import run as calibrate_temperatures
    updates = calibrate_temperatures()   # dict {agent_name: T}

Schema (auto-migrated on first run):
    agent_calibration_data (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        agent_name    TEXT NOT NULL,
        parlay_id     TEXT,
        date          TEXT NOT NULL,          -- YYYY-MM-DD
        prop_type     TEXT,
        raw_prob      REAL NOT NULL,          -- implied_prob before T-scaling
        outcome       INTEGER NOT NULL,       -- 1=WIN (leg hit), 0=LOSS
        created_at    TEXT DEFAULT (datetime('now'))
    )

    agent_unit_sizing.temperature REAL DEFAULT 1.0   -- added in Phase 47 migration
"""

from __future__ import annotations

import logging
import math
import os
import sys
from typing import Any

logger = logging.getLogger("propiq.temperature_calibration")

# ── SQL DB access — Postgres via psycopg2 (same backend as agent_unit_sizing) ─
import os as _os
import psycopg2 as _psycopg2

_DATABASE_URL = _os.environ.get("DATABASE_URL", "")

def _get_conn():
    """Open a Postgres connection to the Railway DATABASE_URL."""
    return _psycopg2.connect(_DATABASE_URL, sslmode="require")

_DB_AVAILABLE = bool(_DATABASE_URL)
if not _DB_AVAILABLE:
    logger.warning("[TempCal] DATABASE_URL not set — DB operations will be skipped.")

# ── Temperature scaling math (from temperature_scaling.py) ───────────────────
try:
    from temperature_scaling import fit_temperature, apply_temperature
    _TS_AVAILABLE = True
except ImportError:
    _TS_AVAILABLE = False
    logger.warning("[TempCal] temperature_scaling.py not found — using identity T=1.0.")

    def fit_temperature(probs: list[float], outcomes: list[int]) -> float:  # noqa: E302
        return 1.0

    def apply_temperature(raw_prob: float, T: float) -> float:  # noqa: E302
        return raw_prob


# ── Config ────────────────────────────────────────────────────────────────────
MIN_SAMPLES: int = 30          # minimum graded picks before fitting T
T_FLOOR: float = 0.5           # never compress below this
T_CEILING: float = 4.0         # never expand above this (safety cap)
T_DEFAULT: float = 1.5         # conservative prior for new agents (slightly compressed)
VAL_FRACTION: float = 0.30     # hold-out fraction for validation when splitting


# ── DB helpers ────────────────────────────────────────────────────────────────

def _ensure_schema() -> None:
    """Create calibration table + temperature column if they don't exist."""
    if not _DB_AVAILABLE:
        return
    conn = _get_conn()
    cur = conn.cursor()

    # Create calibration data table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS agent_calibration_data (
            id          SERIAL PRIMARY KEY,
            agent_name  TEXT    NOT NULL,
            parlay_id   TEXT,
            date        TEXT    NOT NULL,
            prop_type   TEXT,
            raw_prob    REAL    NOT NULL,
            outcome     INTEGER NOT NULL,
            created_at  TEXT    DEFAULT (now()::text)
        )
    """)

    # Add temperature column to agent_unit_sizing if not present
    # SQLite ALTER TABLE ADD COLUMN is safe to run repeatedly — it no-ops if
    # the column already exists in some runtimes, but we catch the error here.
    try:
        cur.execute(
            "ALTER TABLE agent_unit_sizing ADD COLUMN temperature REAL DEFAULT 1.5"
        )
        logger.info("[TempCal] Added temperature column to agent_unit_sizing.")
    except Exception:
        # Column already exists — no-op
        pass

    conn.commit()
    conn.close()


def write_leg_outcomes(
    agent_name: str,
    parlay_id: str,
    date: str,
    legs: list[dict],
) -> int:
    """
    Write per-leg calibration data after settlement.

    Each leg dict should have:
        {prop_type, raw_prob, outcome}  where outcome ∈ {"WIN", "LOSS", "PUSH"}

    PUSH legs are skipped (no information about calibration).
    Returns count of rows written.
    """
    if not _DB_AVAILABLE:
        return 0
    conn = _get_conn()
    cur = conn.cursor()
    written = 0
    for leg in legs:
        outcome_raw = leg.get("outcome", "")
        if outcome_raw not in ("WIN", "LOSS"):
            continue  # skip PUSH — ambiguous
        outcome_int = 1 if outcome_raw == "WIN" else 0
        raw_prob = float(leg.get("raw_prob", leg.get("implied_prob", 0.5)))
        prop_type = leg.get("prop_type", "")
        cur.execute(
            """
            INSERT INTO agent_calibration_data
                (agent_name, parlay_id, date, prop_type, raw_prob, outcome)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (agent_name, parlay_id, date, prop_type, raw_prob, outcome_int),
        )
        written += 1
    conn.commit()
    conn.close()
    return written


def load_calibration_data(agent_name: str) -> tuple[list[float], list[int]]:
    """
    Load all graded (raw_prob, outcome) pairs for an agent.
    Returns (probs, outcomes) sorted chronologically.
    """
    if not _DB_AVAILABLE:
        return [], []
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT raw_prob, outcome
        FROM   agent_calibration_data
        WHERE  agent_name = ?
        ORDER  BY created_at ASC
        """,
        (agent_name,),
    )
    rows = cur.fetchall()
    conn.close()
    probs = [float(r[0]) for r in rows]
    outcomes = [int(r[1]) for r in rows]
    return probs, outcomes


def get_current_temperatures() -> dict[str, float]:
    """
    Return {agent_name: T} for all agents in agent_unit_sizing.
    Missing entries default to T_DEFAULT.
    """
    if not _DB_AVAILABLE:
        return {}
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("SELECT agent_name, temperature FROM agent_unit_sizing")
        rows = cur.fetchall()
        conn.close()
        return {
            r[0]: float(r[1]) if r[1] is not None else T_DEFAULT
            for r in rows
        }
    except Exception as exc:
        logger.warning("[TempCal] Could not load temperatures: %s", exc)
        return {}


def update_agent_temperature(agent_name: str, T: float) -> bool:
    """Write the fitted T back to agent_unit_sizing.temperature."""
    if not _DB_AVAILABLE:
        return False
    T_clamped = max(T_FLOOR, min(T_CEILING, T))
    try:
        conn = _get_conn()
        cur = conn.cursor()
        # Upsert: update if exists, insert if not (temperature column may be new)
        cur.execute(
            """
            UPDATE agent_unit_sizing
            SET    temperature = ?
            WHERE  agent_name  = ?
            """,
            (T_clamped, agent_name),
        )
        if cur.rowcount == 0:
            # Agent not in table yet — insert minimal row
            cur.execute(
                """
                INSERT INTO agent_unit_sizing (agent_name, temperature)
                VALUES (?, ?)
                """,
                (agent_name, T_clamped),
            )
        conn.commit()
        conn.close()
        return True
    except Exception as exc:
        logger.error("[TempCal] DB write failed for %s: %s", agent_name, exc)
        return False


# ── Calibration logic ─────────────────────────────────────────────────────────

def _log_loss(probs: list[float], outcomes: list[int]) -> float:
    """Compute binary log-loss."""
    eps = 1e-9
    total = 0.0
    for p, y in zip(probs, outcomes):
        p = max(eps, min(1 - eps, p))
        total += -(y * math.log(p) + (1 - y) * math.log(1 - p))
    return total / len(probs)


def _calibration_error(probs: list[float], outcomes: list[int], n_bins: int = 5) -> float:
    """
    Expected Calibration Error (ECE) — average bin-level gap between
    mean predicted probability and actual win rate.
    Lower is better (0.0 = perfect calibration).
    """
    if not probs:
        return 0.0
    bin_size = 1.0 / n_bins
    ece = 0.0
    n = len(probs)
    for i in range(n_bins):
        lo = i * bin_size
        hi = lo + bin_size
        indices = [j for j, p in enumerate(probs) if lo <= p < hi]
        if not indices:
            continue
        bin_probs = [probs[j] for j in indices]
        bin_outcomes = [outcomes[j] for j in indices]
        avg_conf = sum(bin_probs) / len(bin_probs)
        avg_acc = sum(bin_outcomes) / len(bin_outcomes)
        ece += (len(indices) / n) * abs(avg_conf - avg_acc)
    return round(ece, 4)


def fit_agent_temperature(
    agent_name: str,
    probs: list[float],
    outcomes: list[int],
) -> dict[str, Any]:
    """
    Fit temperature T for one agent given its graded history.

    Strategy:
    - If n < MIN_SAMPLES: return T_DEFAULT (not enough data)
    - If MIN_SAMPLES ≤ n < 60: fit on all data (no val split — small sample)
    - If n ≥ 60: split last 30% as held-out validation, fit on training portion

    Returns metadata dict:
        {agent, T, n_samples, log_loss_before, log_loss_after, ece_after,
         data_regime, updated}
    """
    n = len(probs)
    result: dict[str, Any] = {
        "agent": agent_name,
        "n_samples": n,
        "data_regime": "insufficient",
        "updated": False,
        "T": T_DEFAULT,
    }

    if n < MIN_SAMPLES:
        logger.info(
            "[TempCal] %-20s  n=%d < %d — using default T=%.2f",
            agent_name, n, MIN_SAMPLES, T_DEFAULT,
        )
        result["T"] = T_DEFAULT
        return result

    # Decide split
    if n >= 60:
        split_idx = int(n * (1 - VAL_FRACTION))
        train_probs    = probs[:split_idx]
        train_outcomes = outcomes[:split_idx]
        val_probs      = probs[split_idx:]
        val_outcomes   = outcomes[split_idx:]
        regime = "train_val_split"
    else:
        # Small sample: fit on all, no held-out val
        train_probs = probs
        train_outcomes = outcomes
        val_probs = probs
        val_outcomes = outcomes
        regime = "full_data_fit"

    result["data_regime"] = regime

    # Baseline log-loss (T=1.0, no scaling)
    ll_before = _log_loss(val_probs, val_outcomes)
    result["log_loss_before"] = round(ll_before, 4)

    # Fit T on training window
    try:
        T = fit_temperature(train_probs, train_outcomes)
    except Exception as exc:
        logger.warning("[TempCal] fit_temperature failed for %s: %s", agent_name, exc)
        T = T_DEFAULT

    T = max(T_FLOOR, min(T_CEILING, T))

    # Apply T to val probs and measure improvement
    scaled_val = [apply_temperature(p, T) for p in val_probs]
    ll_after = _log_loss(scaled_val, val_outcomes)
    ece_after = _calibration_error(scaled_val, val_outcomes)

    result.update({
        "T": round(T, 4),
        "log_loss_after": round(ll_after, 4),
        "log_loss_delta": round(ll_after - ll_before, 4),
        "ece_after": ece_after,
        "updated": True,
    })

    logger.info(
        "[TempCal] %-20s  n=%d  T=%.3f  LL: %.4f→%.4f (Δ%+.4f)  ECE=%.4f  [%s]",
        agent_name, n, T, ll_before, ll_after,
        ll_after - ll_before, ece_after, regime,
    )
    return result


# ── Public API ────────────────────────────────────────────────────────────────

def run(quiet: bool = False) -> dict[str, float]:
    """
    Run temperature calibration for all agents with sufficient data.

    1. Ensure DB schema is up to date.
    2. Load all agents that have calibration data.
    3. Fit T per agent.
    4. Write T back to agent_unit_sizing.temperature.
    5. Return {agent_name: T} for all agents processed.

    Called by nightly_recap.py after settlement.
    """
    _ensure_schema()

    if not _DB_AVAILABLE:
        logger.warning("[TempCal] DB unavailable — calibration skipped.")
        return {}

    # Get all agents with calibration data
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT agent_name, COUNT(*) as n
            FROM   agent_calibration_data
            GROUP  BY agent_name
            ORDER  BY n DESC
            """
        )
        agent_counts = cur.fetchall()
        conn.close()
    except Exception as exc:
        logger.error("[TempCal] Could not query calibration data: %s", exc)
        return {}

    if not agent_counts:
        logger.info("[TempCal] No calibration data yet — will accumulate from settlements.")
        return {}

    updates: dict[str, float] = {}
    results: list[dict] = []

    for agent_name, n in agent_counts:
        probs, outcomes = load_calibration_data(agent_name)
        result = fit_agent_temperature(agent_name, probs, outcomes)
        T = result["T"]
        updates[agent_name] = T
        results.append(result)

        if result["updated"]:
            update_agent_temperature(agent_name, T)

    # Summary log
    updated_count = sum(1 for r in results if r["updated"])
    skipped_count = len(results) - updated_count

    if not quiet:
        logger.info(
            "[TempCal] Calibration complete — %d updated / %d skipped (insufficient data)",
            updated_count, skipped_count,
        )
        for r in results:
            if r["updated"]:
                ll_delta = r.get("log_loss_delta", 0.0)
                direction = "improved" if ll_delta < 0 else "degraded"
                logger.info(
                    "  %-20s  T=%.3f  LL %s by %.4f  ECE=%.4f  [%s]",
                    r["agent"], r["T"], direction, abs(ll_delta),
                    r.get("ece_after", 0.0), r.get("data_regime", "?"),
                )

    return updates


def get_temperature(agent_name: str) -> float:
    """
    Get the current temperature for a specific agent.
    Returns T_DEFAULT if agent not found or DB unavailable.
    Fast path: single-row query, called at dispatcher startup.
    """
    if not _DB_AVAILABLE:
        return T_DEFAULT
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT temperature FROM agent_unit_sizing WHERE agent_name = %s",
            (agent_name,),
        )
        row = cur.fetchone()
        conn.close()
        if row and row[0] is not None:
            return max(T_FLOOR, min(T_CEILING, float(row[0])))
        return T_DEFAULT
    except Exception:
        return T_DEFAULT


def load_all_temperatures(agent_names: list[str]) -> dict[str, float]:
    """
    Bulk-load temperatures for a list of agents in a single DB query.
    Returns {agent_name: T} with T_DEFAULT for any missing agent.
    Called once at dispatcher startup.
    """
    if not _DB_AVAILABLE:
        return {name: T_DEFAULT for name in agent_names}
    try:
        conn = _get_conn()
        cur = conn.cursor()
        placeholders = ",".join("?" * len(agent_names))
        cur.execute(
            f"SELECT agent_name, temperature FROM agent_unit_sizing "
            f"WHERE agent_name IN ({placeholders})",
            agent_names,
        )
        rows = cur.fetchall()
        conn.close()
        result = {name: T_DEFAULT for name in agent_names}
        for agent_name, T in rows:
            if T is not None:
                result[agent_name] = max(T_FLOOR, min(T_CEILING, float(T)))
        return result
    except Exception as exc:
        logger.warning("[TempCal] Bulk temperature load failed: %s", exc)
        return {name: T_DEFAULT for name in agent_names}


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    )
    updates = run(quiet=False)
    print("\n--- Temperature Summary ---")
    for agent, T in sorted(updates.items()):
        cal_status = "CALIBRATED" if T != T_DEFAULT else "DEFAULT"
        print(f"  {agent:<22} T={T:.3f}  [{cal_status}]")
