"""
calibrate_model.py — Weekly PropIQ Isotonic Regression Calibration
====================================================================
Run every Monday morning via GitHub Actions ``weekly_calibration.yml``.

Steps:
  1. Load ``bet_history.csv`` (columns: ``model_prob``, ``outcome``)
  2. Fit Isotonic Regression on raw model probabilities vs actual outcomes
  3. Build a 51-point calibration lookup table (0.40 → 0.90)
  4. Write ``calibration_map.json`` → used by ``calibration_layer.py``
  5. Calculate Brier score → ``drift_monitor.py`` checks for degradation
  6. Update ``brier_score_ledger.json``

Usage::

    python calibrate_model.py
    python calibrate_model.py --history path/to/history.csv
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

BET_HISTORY_PATH = os.getenv("BET_HISTORY_PATH", "bet_history.csv")
CALIBRATION_MAP_PATH = "calibration_map.json"
MIN_SAMPLE = 20  # below this, identity map is used (too few data points)


def _write_identity_map() -> None:
    """Write a pass-through identity map when we lack training data."""
    try:
        import numpy as np  # noqa: PLC0415
    except ImportError:
        import json as _json  # noqa: PLC0415
        pts = [round(0.40 + i * 0.01, 2) for i in range(51)]
        m = {str(p): p for p in pts}
        with open(CALIBRATION_MAP_PATH, "w") as fh:
            _json.dump(m, fh, indent=2)
        return

    pts = [round(float(p), 3) for p in list(map(lambda i: 0.40 + i * 0.01, range(51)))]
    mapping = {str(p): p for p in pts}
    with open(CALIBRATION_MAP_PATH, "w") as fh:
        json.dump(mapping, fh, indent=2)
    logger.info("Identity calibration map written to %s", CALIBRATION_MAP_PATH)


def generate_calibration_map(csv_path: str = BET_HISTORY_PATH) -> None:
    """Fit Isotonic Regression and write calibration_map.json."""
    try:
        import numpy as np              # noqa: PLC0415
        import pandas as pd             # noqa: PLC0415
        from sklearn.isotonic import IsotonicRegression  # noqa: PLC0415
    except ImportError as exc:
        logger.error("Missing dependency: %s — run: pip install pandas numpy scikit-learn", exc)
        _write_identity_map()
        return

    # ── Load bet history ──────────────────────────────────────────────────────
    try:
        df = pd.read_csv(csv_path)
    except Exception as exc:
        logger.warning("Cannot load %s (%s) — writing identity map", csv_path, exc)
        _write_identity_map()
        return

    required = {"model_prob", "outcome"}
    if not required.issubset(df.columns):
        logger.warning("CSV missing columns %s — writing identity map", required - set(df.columns))
        _write_identity_map()
        return

    df = df.dropna(subset=["model_prob", "outcome"])
    if len(df) < MIN_SAMPLE:
        logger.warning("Only %d rows (need %d) — writing identity map", len(df), MIN_SAMPLE)
        _write_identity_map()
        return

    # ── Fit Isotonic Regression ───────────────────────────────────────────────
    X = df["model_prob"].astype(float).values
    y = df["outcome"].astype(int).values

    ir = IsotonicRegression(out_of_bounds="clip")
    ir.fit(X, y)

    test_points = np.linspace(0.40, 0.90, 51)
    calibrated = ir.predict(test_points)

    calibration_map = {
        str(round(float(p), 3)): round(float(v), 4)
        for p, v in zip(test_points, calibrated)
    }
    with open(CALIBRATION_MAP_PATH, "w") as fh:
        json.dump(calibration_map, fh, indent=2)
    logger.info("✅ Calibration map written → %s (%d points, n=%d bets)", CALIBRATION_MAP_PATH, len(calibration_map), len(df))

    # ── Brier Score + Drift Check ─────────────────────────────────────────────
    try:
        from calibration_layer import calculate_brier_score  # noqa: PLC0415
        from drift_monitor import record_brier               # noqa: PLC0415

        preds = [
            {"prob": float(row["model_prob"]), "outcome": int(row["outcome"])}
            for _, row in df.iterrows()
        ]
        brier = calculate_brier_score(preds)
        if brier is not None:
            logger.info("Brier Score: %.4f", brier)
            drifted = record_brier(brier)
            if drifted:
                logger.warning("⚠️  Model drift detected — calibration governor activated")
            else:
                logger.info("✅ No drift detected")
    except Exception as exc:
        logger.warning("Brier/drift check failed: %s", exc)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PropIQ weekly model calibration")
    parser.add_argument("--history", default=BET_HISTORY_PATH, help="Path to bet_history.csv")
    args = parser.parse_args()
    generate_calibration_map(args.history)
