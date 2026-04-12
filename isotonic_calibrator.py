"""
isotonic_calibrator.py
=======================
PropIQ — Secondary Calibration Model (Isotonic Regression).

Trains per-bucket isotonic regression on model_prob vs actual win rate.
Bucket key: (prop_type, line_level) — line_level = round(line / 0.5) * 0.5

ACTIVATION
----------
Only active when env var ISOTONIC_CALIBRATION_ACTIVE=true.
Build now, activate after April 20 retrain once calibrated probs are reliable.

CALIBRATION MAP FORMAT (same as calibration_map.json used by calibration_layer.py)
-----------------------------------------------------------------------------------
{
  "global": {"0.45": 0.44, "0.50": 0.51, ...},    <- global isotonic fit
  "hits__1.5":     {"0.45": 0.43, ...},             <- per-bucket fits
  "strikeouts__5.5": {"0.50": 0.52, ...},
  ...
}

The global map is a drop-in replacement for the existing calibration_map.json.
Per-bucket keys override the global when both prop_type and line_level match.

DB TABLE
--------
isotonic_cal_buckets:
  id, bucket_key, prop_type, line_level, n_samples, brier, created_at

WIRE INTO run_grading_tasklet():
    try:
        from isotonic_calibrator import rebuild_isotonic_calibration as _rebuild_iso
        _rebuild_iso()
        logger.info("[Grading] Isotonic calibration rebuilt.")
    except Exception as _iso_err:
        logger.warning("[Grading] Isotonic calibration failed: %s", _iso_err)
"""

from __future__ import annotations

import json
import logging
import os

logger = logging.getLogger("propiq.isotonic_calibrator")

_ACTIVATION_ENV   = "ISOTONIC_CALIBRATION_ACTIVE"
_MIN_BUCKET_ROWS  = 30      # minimum samples per bucket for isotonic fit
_MIN_GLOBAL_ROWS  = 100     # minimum total rows for global fit
_PROB_POINTS      = 20      # calibration curve resolution (5pp bins from 0 to 100)
_CAL_MAP_PATH     = os.getenv("CALIBRATION_MAP_PATH", "calibration_map.json")


def _is_active() -> bool:
    return os.getenv(_ACTIVATION_ENV, "").lower() in ("true", "1", "yes")


def _get_pg_conn():
    import psycopg2
    url = os.getenv("DATABASE_URL", "")
    if url:
        return psycopg2.connect(url)
    return psycopg2.connect(
        host     = os.getenv("POSTGRES_HOST",     "postgres"),
        port     = int(os.getenv("POSTGRES_PORT", 5432)),
        dbname   = os.getenv("POSTGRES_DB",       "propiq"),
        user     = os.getenv("POSTGRES_USER",     "propiq"),
        password = os.getenv("POSTGRES_PASSWORD", "propiq"),
    )


def _ensure_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS isotonic_cal_buckets (
                id          SERIAL PRIMARY KEY,
                bucket_key  VARCHAR(120) NOT NULL,
                prop_type   VARCHAR(60),
                line_level  FLOAT,
                n_samples   INTEGER,
                brier       FLOAT,
                created_at  TIMESTAMPTZ  DEFAULT NOW()
            )
        """)
    conn.commit()


def _line_level(line: float) -> float:
    """Round line to nearest 0.5 for bucketing. Cap at 5.0+."""
    lvl = round(line * 2) / 2
    return min(lvl, 5.0)


def _fetch_graded_rows(conn) -> list[dict]:
    """
    Pull all graded rows from bet_ledger.
    Returns list of dicts: {model_prob, actual_outcome, prop_type, line}
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                COALESCE(model_prob, 50.0)  AS model_prob,
                actual_outcome,
                COALESCE(prop_type, '')     AS prop_type,
                COALESCE(line, 1.5)         AS line
            FROM bet_ledger
            WHERE discord_sent    = TRUE
              AND result          IN ('WIN', 'LOSS')
              AND actual_outcome  IS NOT NULL
              AND model_prob      IS NOT NULL
        """)
        rows = cur.fetchall()
    return [
        {
            "model_prob":      float(r[0]),
            "actual_outcome":  int(r[1]),
            "prop_type":       str(r[2]).lower().strip(),
            "line":            float(r[3]),
        }
        for r in rows
    ]


def _fit_isotonic(probs: list[float], outcomes: list[int]) -> dict[str, float]:
    """
    Fit isotonic regression using sklearn.
    Returns a dict mapping str(prob) → calibrated_prob for points in 5pp bins.
    Falls back to identity map on sklearn import failure.
    """
    try:
        from sklearn.isotonic import IsotonicRegression  # noqa: PLC0415
        import numpy as np                               # noqa: PLC0415
        ir = IsotonicRegression(out_of_bounds="clip")
        ir.fit(np.array(probs), np.array(outcomes))
        # Evaluate at resolution points
        x_pts = [i * (100.0 / _PROB_POINTS) for i in range(_PROB_POINTS + 1)]
        y_pts = ir.predict(np.array(x_pts)).tolist()
        return {str(round(x, 1)): round(float(y), 4) for x, y in zip(x_pts, y_pts)}
    except ImportError:
        # sklearn not available — return identity map
        logger.debug("[Isotonic] sklearn not available — using identity map")
        return {str(round(p, 1)): round(p / 100.0, 4) for p in
                [i * (100.0 / _PROB_POINTS) for i in range(_PROB_POINTS + 1)]}


def _brier_score(probs: list[float], outcomes: list[int]) -> float:
    if not probs:
        return 0.0
    n = len(probs)
    return sum((p / 100.0 - o) ** 2 for p, o in zip(probs, outcomes)) / n


def _log_bucket_diagnostic(conn, bucket_key: str, prop_type: str,
                            line_level: float, n: int, brier: float) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO isotonic_cal_buckets
                (bucket_key, prop_type, line_level, n_samples, brier)
            VALUES (%s, %s, %s, %s, %s)
        """, (bucket_key, prop_type, line_level, n, round(brier, 4)))
    conn.commit()


def rebuild_isotonic_calibration() -> None:
    """
    Main entry point — called from run_grading_tasklet() at 2 AM.

    1. Checks ISOTONIC_CALIBRATION_ACTIVE env var — exits silently if not set.
    2. Pulls graded rows from bet_ledger.
    3. Fits global isotonic regression on all rows.
    4. Fits per-bucket isotonic regression on (prop_type × line_level) groups.
    5. Writes combined calibration map to calibration_map.json.
    6. Logs per-bucket Brier scores to isotonic_cal_buckets table.
    """
    if not _is_active():
        logger.debug("[Isotonic] Calibration inactive (set ISOTONIC_CALIBRATION_ACTIVE=true to enable).")
        return

    conn = _get_pg_conn()
    try:
        _ensure_table(conn)
        rows = _fetch_graded_rows(conn)
        n_total = len(rows)

        if n_total < _MIN_GLOBAL_ROWS:
            logger.info(
                "[Isotonic] Only %d graded rows — need %d for calibration. Skipping.",
                n_total, _MIN_GLOBAL_ROWS,
            )
            return

        logger.info("[Isotonic] Rebuilding calibration from %d graded rows.", n_total)

        cal_map: dict[str, dict] = {}

        # ── Global fit ───────────────────────────────────────────────────────
        all_probs    = [r["model_prob"] for r in rows]
        all_outcomes = [r["actual_outcome"] for r in rows]
        global_brier = _brier_score(all_probs, all_outcomes)
        cal_map["global"] = _fit_isotonic(all_probs, all_outcomes)
        logger.info("[Isotonic] Global fit — n=%d Brier=%.4f", n_total, global_brier)

        # ── Per-bucket fits ──────────────────────────────────────────────────
        buckets: dict[str, list[dict]] = {}
        for row in rows:
            pt  = row["prop_type"]
            lvl = _line_level(row["line"])
            key = f"{pt}__{lvl}"
            buckets.setdefault(key, []).append(row)

        for bucket_key, bucket_rows in buckets.items():
            n = len(bucket_rows)
            if n < _MIN_BUCKET_ROWS:
                logger.debug("[Isotonic] Bucket %s: n=%d < %d — skip", bucket_key, n, _MIN_BUCKET_ROWS)
                continue
            b_probs    = [r["model_prob"]     for r in bucket_rows]
            b_outcomes = [r["actual_outcome"] for r in bucket_rows]
            b_brier    = _brier_score(b_probs, b_outcomes)
            cal_map[bucket_key] = _fit_isotonic(b_probs, b_outcomes)
            pt   = bucket_rows[0]["prop_type"]
            lvl  = _line_level(bucket_rows[0]["line"])
            _log_bucket_diagnostic(conn, bucket_key, pt, lvl, n, b_brier)
            logger.info(
                "[Isotonic] Bucket %s — n=%d Brier=%.4f", bucket_key, n, b_brier,
            )

        # ── Write calibration_map.json ───────────────────────────────────────
        try:
            with open(_CAL_MAP_PATH, "w") as f:
                json.dump(cal_map, f, indent=2)
            logger.info(
                "[Isotonic] Wrote calibration_map.json — %d keys (%d buckets + global).",
                len(cal_map), len(cal_map) - 1,
            )
        except Exception as write_err:
            logger.warning("[Isotonic] Failed to write calibration_map.json: %s", write_err)

    finally:
        conn.close()


def apply_isotonic_calibration(model_prob: float,
                                prop_type: str = "",
                                line: float = 1.5) -> float:
    """
    Apply the isotonic calibration map to a raw model_prob (0–100 scale).

    Lookup order:
      1. Per-bucket key: "{prop_type}__{line_level}"
      2. Global map
      3. Identity (no calibration)

    Returns calibrated probability on 0–100 scale.
    Only active when ISOTONIC_CALIBRATION_ACTIVE=true.
    """
    if not _is_active():
        return model_prob

    try:
        with open(_CAL_MAP_PATH) as f:
            cal_map: dict = json.load(f)
    except Exception:
        return model_prob

    lvl       = _line_level(float(line))
    bucket_key = f"{str(prop_type).lower().strip()}__{lvl}"
    prob_str   = str(round(float(model_prob) / 5) * 5)   # bin to nearest 5pp

    # Try bucket-specific map first
    bucket_map = cal_map.get(bucket_key) or cal_map.get("global") or {}
    if not bucket_map:
        return model_prob

    cal_prob = bucket_map.get(prob_str)
    if cal_prob is None:
        # Interpolate from nearest keys
        keys = sorted(float(k) for k in bucket_map.keys())
        mp   = float(model_prob)
        lo   = max((k for k in keys if k <= mp), default=keys[0]  if keys else mp)
        hi   = min((k for k in keys if k >= mp), default=keys[-1] if keys else mp)
        if lo == hi:
            cal_prob = bucket_map.get(str(lo)) or model_prob / 100.0
        else:
            alpha    = (mp - lo) / (hi - lo)
            lo_cal   = bucket_map.get(str(lo), lo / 100.0)
            hi_cal   = bucket_map.get(str(hi), hi / 100.0)
            cal_prob = lo_cal + alpha * (hi_cal - lo_cal)

    # cal_prob is 0–1 scale (isotonic output), return as 0–100
    return round(float(cal_prob) * 100.0, 2)
