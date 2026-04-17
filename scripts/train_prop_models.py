"""
scripts/train_prop_models.py
============================
Prop-family XGBoost model trainer (C-1b fix).

Problem: prop_model_v1.json was trained on a single pool of bets that happened
to be dominated by contact props (hits, total_bases, H+R+RBI). When that model
scores a pitcher K prop or ER prop, the contact-heavy features (wRC+, ISO,
xwOBA, BABIP) are the strongest predictors — completely wrong for strikeout
or run-suppression props.

Fix: Train three separate models, one per prop family:
  - contact_model.json    : hits, rbis, runs, total_bases, hits_runs_rbis
  - strikeout_model.json  : strikeouts (pitcher Ks), pitching_outs
  - run_suppress_model.json: earned_runs, hits_allowed, pitcher_er

The existing monolithic prop_model_v1.json is kept as a final fallback when
a family-specific model has <50 training rows.

Usage:
  python scripts/train_prop_models.py [--min-rows 50] [--dry-run]

Scheduled: runs automatically at Sunday 2 AM alongside run_xgboost_tasklet().
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

# ── Prop-family definitions ──────────────────────────────────────────────────

PROP_FAMILIES: dict[str, list[str]] = {
    "contact": [
        "hits", "rbis", "runs", "total_bases", "hits_runs_rbis",
        "singles", "hitter_strikeouts",
    ],
    "strikeout": [
        "strikeouts", "pitcher_strikeouts", "pitching_outs",
    ],
    "run_suppression": [
        "earned_runs", "pitcher_er", "hits_allowed",
    ],
}

# Model file locations (same directory as prop_model_v1.json)
MODEL_DIR = os.getenv("MODEL_DIR", "/app/api/models")

FAMILY_MODEL_PATHS = {
    family: os.path.join(MODEL_DIR, f"prop_model_{family}.json")
    for family in PROP_FAMILIES
}

# Minimum training rows before a family model is used over the fallback
DEFAULT_MIN_ROWS = 50


# ── Feature columns (must match tasklets.py FEATURE_COLS, slots 0–26) ────────

FEATURE_COLS = [
    "model_prob", "line", "ev_pct", "confidence",
    "csw_pct", "swstr_pct",                                # slots 4-5 (pitching physics)
    "k_pct", "bb_pct", "era_adj", "fip_adj",
    "wrc_plus", "iso", "xwoba", "babip",                   # batter stats
    "barrel_pct", "hard_hit_pct", "exit_velo",             # Statcast
    "park_factor", "weather_score", "umpire_k_rate",
    "sb_implied_prob", "sb_line_gap",                       # sportsbook reference
    "rolling_avg", "rolling_trend", "lineup_position",
    "temp_f", "wind_mph",
]


def _pg_conn():
    """Return a Postgres connection using DATABASE_URL."""
    import psycopg2  # noqa: PLC0415
    url = os.getenv("DATABASE_URL", "")
    if not url:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(url)


def _load_training_rows(prop_types: list[str], min_rows: int) -> tuple[list, list]:
    """
    Load graded rows from bet_ledger for specific prop_types.
    Returns (X_rows, y_labels) — list of 27-float feature vectors + 0/1 labels.
    """
    try:
        conn = _pg_conn()
        cur  = conn.cursor()
        placeholders = ", ".join(["%s"] * len(prop_types))
        cur.execute(
            f"""
            SELECT features_json, actual_outcome
            FROM bet_ledger
            WHERE discord_sent = TRUE
              AND actual_outcome IS NOT NULL
              AND prop_type IN ({placeholders})
            ORDER BY id
            """,
            prop_types,
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except Exception as exc:
        logger.error("[TrainPropModels] DB fetch failed: %s", exc)
        return [], []

    X, y = [], []
    for features_json, outcome in rows:
        try:
            feats = json.loads(features_json) if isinstance(features_json, str) else features_json
            vec = [float(feats.get(col, 0.0) or 0.0) for col in FEATURE_COLS]
            X.append(vec)
            y.append(int(outcome))
        except Exception:
            continue

    logger.info("[TrainPropModels] Loaded %d rows for %s", len(X), prop_types)
    return X, y


def _train_family_model(family: str, prop_types: list[str], min_rows: int, dry_run: bool) -> bool:
    """
    Train one XGBoost model for a prop family.
    Returns True if model was trained and saved.
    """
    try:
        import xgboost as xgb  # noqa: PLC0415
    except ImportError:
        logger.error("[TrainPropModels] xgboost not installed")
        return False

    X, y = _load_training_rows(prop_types, min_rows)

    if len(X) < min_rows:
        logger.warning(
            "[TrainPropModels] %s: only %d rows (need %d) — skipping, fallback to monolithic model",
            family, len(X), min_rows,
        )
        return False

    wins = sum(y)
    logger.info(
        "[TrainPropModels] %s: training on %d rows (%d wins / %d losses)",
        family, len(X), wins, len(y) - wins,
    )

    if dry_run:
        logger.info("[DRY-RUN] Would train %s model on %d rows — skipping actual fit", family, len(X))
        return True

    import numpy as np  # noqa: PLC0415
    X_np = np.array(X, dtype=float)
    y_np = np.array(y, dtype=int)

    dtrain = xgb.DMatrix(X_np, label=y_np, feature_names=FEATURE_COLS)

    params = {
        "objective":        "binary:logistic",
        "eval_metric":      "logloss",
        "max_depth":        4,
        "learning_rate":    0.05,
        "n_estimators":     200,
        "subsample":        0.8,
        "colsample_bytree": 0.8,
        "min_child_weight": 5,
        "gamma":            0.1,
        "scale_pos_weight": max(1.0, (len(y) - wins) / max(wins, 1)),
        "seed":             42,
    }

    model = xgb.train(
        params,
        dtrain,
        num_boost_round=200,
        verbose_eval=False,
    )

    out_path = FAMILY_MODEL_PATHS[family]
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    model.save_model(out_path)
    logger.info("[TrainPropModels] %s model saved → %s", family, out_path)

    # Persist to Postgres xgb_model_store (same as run_xgboost_tasklet does)
    try:
        with open(out_path, "rb") as f:
            model_bytes = f.read()
        conn = _pg_conn()
        cur  = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS xgb_model_store (
                id           SERIAL PRIMARY KEY,
                model_name   VARCHAR(60) NOT NULL UNIQUE,
                model_bytes  BYTEA       NOT NULL,
                trained_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                n_rows       INTEGER
            )
        """)
        cur.execute("""
            INSERT INTO xgb_model_store (model_name, model_bytes, n_rows)
            VALUES (%s, %s, %s)
            ON CONFLICT (model_name) DO UPDATE
                SET model_bytes = EXCLUDED.model_bytes,
                    trained_at  = NOW(),
                    n_rows      = EXCLUDED.n_rows
        """, (f"prop_model_{family}", model_bytes, len(X)))
        conn.commit()
        cur.close()
        conn.close()
        logger.info("[TrainPropModels] %s model persisted to Postgres xgb_model_store", family)
    except Exception as exc:
        logger.warning("[TrainPropModels] Postgres persist failed for %s: %s", family, exc)

    return True


def train_all_prop_family_models(min_rows: int = DEFAULT_MIN_ROWS, dry_run: bool = False) -> dict:
    """
    Train all three prop-family models.
    Returns {family: True/False} indicating which were trained successfully.
    """
    results = {}
    for family, prop_types in PROP_FAMILIES.items():
        logger.info("[TrainPropModels] === Training %s model ===", family)
        results[family] = _train_family_model(family, prop_types, min_rows, dry_run)
    return results


def get_family_for_prop(prop_type: str) -> str | None:
    """Return the prop family for a given prop_type, or None if not found."""
    prop_lower = prop_type.lower().strip()
    for family, types in PROP_FAMILIES.items():
        if prop_lower in types:
            return family
    return None


def load_family_model(prop_type: str):
    """
    Load the family-specific XGBoost model for a given prop_type.

    Load priority:
    1. Family-specific model file on disk (prop_model_{family}.json)
    2. Family-specific model from Postgres xgb_model_store
    3. Monolithic prop_model_v1.json (existing fallback)

    Returns xgboost.Booster or None.
    """
    try:
        import xgboost as xgb  # noqa: PLC0415
    except ImportError:
        return None

    family = get_family_for_prop(prop_type)
    if family:
        # Try disk
        model_path = FAMILY_MODEL_PATHS[family]
        if os.path.exists(model_path):
            try:
                model = xgb.Booster()
                model.load_model(model_path)
                logger.debug("[PropModels] Loaded %s model from disk for prop=%s", family, prop_type)
                return model
            except Exception as exc:
                logger.warning("[PropModels] Failed to load %s from disk: %s", family, exc)

        # Try Postgres
        try:
            conn = _pg_conn()
            cur  = conn.cursor()
            cur.execute(
                "SELECT model_bytes FROM xgb_model_store WHERE model_name = %s",
                (f"prop_model_{family}",),
            )
            row = cur.fetchone()
            cur.close(); conn.close()
            if row:
                import tempfile  # noqa: PLC0415
                with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tf:
                    tf.write(bytes(row[0]))
                    tf_path = tf.name
                model = xgb.Booster()
                model.load_model(tf_path)
                os.unlink(tf_path)
                # Restore to disk for next call
                try:
                    with open(model_path, "wb") as f:
                        f.write(bytes(row[0]))
                except Exception:
                    pass
                logger.info("[PropModels] Loaded %s model from Postgres for prop=%s", family, prop_type)
                return model
        except Exception as exc:
            logger.debug("[PropModels] Postgres model load failed: %s", exc)

    # Final fallback: monolithic model
    mono_path = os.path.join(MODEL_DIR, "prop_model_v1.json")
    if os.path.exists(mono_path):
        try:
            model = xgb.Booster()
            model.load_model(mono_path)
            logger.debug("[PropModels] Using monolithic fallback for prop=%s", prop_type)
            return model
        except Exception:
            pass

    return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Train PropIQ prop-family XGBoost models")
    parser.add_argument("--min-rows", type=int, default=DEFAULT_MIN_ROWS,
                        help=f"Minimum training rows per family (default: {DEFAULT_MIN_ROWS})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Load data and log stats without actually training")
    args = parser.parse_args()

    results = train_all_prop_family_models(min_rows=args.min_rows, dry_run=args.dry_run)

    print("\n=== Prop-Family Model Training Results ===")
    for family, trained in results.items():
        status = "✅ TRAINED" if trained else "⏸  SKIPPED (insufficient rows)"
        print(f"  {family:20s} {status}")

    trained_count = sum(results.values())
    print(f"\n{trained_count}/{len(PROP_FAMILIES)} models trained.")
    if trained_count == 0:
        print("No models trained — monolithic prop_model_v1.json continues as fallback.")
        print(f"Re-run after accumulating ≥{args.min_rows} graded rows per prop family.")
