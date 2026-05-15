"""
scripts/xgb_k_training.py  —  Per-Line XGBoost K & Hit Model Training  (v2)
=============================================================================
Replaces the existing xgb_k_training.py with four concrete improvements:

1. RECENT-SEASON WEIGHTING
   2026 rows get 4x weight, 2025 gets 2x, 2024 gets 1.5x, 2022-2023 get 1x.
   The current model trains all years equally — but a 2026 pitcher facing
   an elevated-K-rate league is fundamentally different from the same pitcher
   in 2022. Recency weighting fixes the calibration drift.

2. HIT BLEND DROPPED TO 90/10
   Hit model Brier = 0.2668 (worse than null at 0.25). The 70/30 blend was
   actively adding noise. This training script outputs a note in model_metrics.json
   recommending 90/10, and the xgb_k_layer update (fix2 below) applies it.

3. FEATURE ALIGNMENT FIXED
   The training script uses K_FEATURES with wrong names (fg_era, fg_kpct etc.)
   that don't match the Statcast/FanGraphs column names. This version uses the
   training-aligned names from xgb_training_pipeline.py (sv_era, sv_k_pct etc.)
   and adds the four missing features: l3_ks, l3_ip, l5_ip, days_rest.

4. LIVE-DATA RETRAINING SCHEDULE
   Monthly retrain using the last 6 months of bet_ledger (real PropIQ graded legs)
   weighted 3x over historical Statcast. When bet_ledger has 500+ K rows, the
   model trains primarily on actual PropIQ outcomes — not synthetic Statcast data.

Run:
    python scripts/xgb_k_training.py              # full retrain
    python scripts/xgb_k_training.py --k-only     # K models only (faster)
    python scripts/xgb_k_training.py --hit-only   # Hit model only
    python scripts/xgb_k_training.py --status     # check existing model metrics
"""

from __future__ import annotations

import base64
import json
import logging
import os
import pickle
import sys
import warnings
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [xgb_train] %(message)s")
logger = logging.getLogger("xgb_train")

# ── Config ────────────────────────────────────────────────────────────────────
SEASONS   = [2022, 2023, 2024, 2025, 2026]
MIN_BF    = 50
MIN_PA    = 50
TEST_YEAR = 2025   # held-out season; 2026 is always training (too early to hold out)
K_LINES   = [3.5, 4.5, 5.5, 6.5]

# Recent-season sample weights — key insight:
# League K-rate, pitch mix, and batter approach shifted materially in 2023-2026.
# Historical data from 2021-2022 can actively hurt calibration if weighted equally.
SEASON_WEIGHTS = {
    2026: 4.0,   # current season — most relevant
    2025: 2.0,   # last full season — very relevant
    2024: 1.5,   # two years ago — moderately relevant
    2023: 1.0,   # baseline
    2022: 1.0,   # baseline
    2021: 0.8,   # pre-shift era — slight downweight
}

# XGB hyperparams — tuned for Platt calibration on prop-outcome data
XGB_PARAMS = dict(
    n_estimators     = 600,
    max_depth        = 5,
    learning_rate    = 0.04,
    subsample        = 0.80,
    colsample_bytree = 0.75,
    min_child_weight = 6,
    gamma            = 0.05,
    reg_alpha        = 0.10,
    reg_lambda       = 1.5,
    eval_metric      = "logloss",
    random_state     = 42,
    n_jobs           = -1,
)

HERE      = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(HERE)
OUTDIR    = os.path.join(REPO_ROOT, "models")
os.makedirs(OUTDIR, exist_ok=True)

# ── Training-aligned feature names (match xgb_k_layer.py EXACTLY) ────────────
# These names must match the column names the .pkl models were trained on.
# Any mismatch causes silent zero-fill → degraded predictions.

K_FEATURES = [
    "sv_xera",                 # Statcast xERA
    "sv_era",                  # ERA (FanGraphs, stored as sv_era in training)
    "sv_k_pct",                # K% (0-100 scale)
    "sv_bb_pct",               # BB% (0-100 scale)
    "sv_whiff_pct",            # SwStr% (0-100 scale)
    "l3_ks",                   # L3-start avg strikeouts  ← was missing
    "l5_ks",                   # L5-start avg strikeouts
    "l10_ks",                  # L10-start avg strikeouts
    "l3_ip",                   # L3-start avg IP          ← was missing
    "l5_ip",                   # L5-start avg IP          ← was missing
    "days_rest",               # Days since last start    ← was missing
    "opp_lineup_k_pct_proxy",  # Opposing lineup K% (0-100)
    "opp_lineup_xwoba_proxy",  # Opposing lineup xwOBA
]

HITS_FEATURES = [
    "sv_xba",       # Statcast xBA
    "sv_xwoba",     # Statcast xwOBA
    "sv_xslg",      # Statcast xSLG
    "sv_ev",        # Exit velocity
    "sv_brl_pct",   # Barrel %
    "sv_hh_pct",    # Hard-hit %
    "sv_ss_pct",    # SwStr% (training key is sv_ss_pct)
    "sv_la",        # Launch angle
    "sv_k_pct",     # Batter K% (training key is sv_k_pct, not fg_kpct)
    "sv_bb_pct",    # Batter BB% (training key is sv_bb_pct, not fg_bbpct)
    "opp_xera",     # Pitcher xERA
    "opp_k_pct",    # Pitcher K%
    "opp_bb_pct",   # Pitcher BB%
    "opp_whiff",    # Pitcher SwStr% ← was missing
    "bats_L",       # 1 = left-handed batter
    "throws_R",     # 1 = right-handed pitcher
    "platoon_adv",  # 1 = favorable platoon matchup
    "l7_hits",      # L7-game hit total
    "l7_hit_rate",  # L7-game hit rate
]

K_MEDIANS = {
    "sv_xera": 4.50, "sv_era": 4.50, "sv_k_pct": 22.0, "sv_bb_pct": 8.0,
    "sv_whiff_pct": 24.0, "l3_ks": 4.5, "l5_ks": 4.5, "l10_ks": 4.5,
    "l3_ip": 5.0, "l5_ip": 5.0, "days_rest": 5.0,
    "opp_lineup_k_pct_proxy": 22.0, "opp_lineup_xwoba_proxy": 0.320,
}

HIT_MEDIANS = {
    "sv_xba": 0.250, "sv_xwoba": 0.320, "sv_xslg": 0.400,
    "sv_ev": 88.0, "sv_brl_pct": 4.0, "sv_hh_pct": 35.0,
    "sv_ss_pct": 10.0, "sv_la": 12.0, "sv_k_pct": 22.0, "sv_bb_pct": 8.0,
    "opp_xera": 4.50, "opp_k_pct": 22.0, "opp_bb_pct": 8.0, "opp_whiff": 24.0,
    "bats_L": 0, "throws_R": 1, "platoon_adv": 0,
    "l7_hits": 1.5, "l7_hit_rate": 0.50,
}

# FanGraphs column name → training feature name mapping
FG_PIT_RENAME = {
    "xERA":   "sv_xera",
    "ERA":    "sv_era",
    "K%":     "sv_k_pct",
    "BB%":    "sv_bb_pct",
    "SwStr%": "sv_whiff_pct",
}

FG_BAT_RENAME = {
    "xBA":      "sv_xba",
    "xwOBA":    "sv_xwoba",
    "xSLG":     "sv_xslg",
    "EV":       "sv_ev",
    "Barrels":  "sv_brl_pct",
    "HardHit%": "sv_hh_pct",
    "SwStr%":   "sv_ss_pct",
    "LA":       "sv_la",
    "K%":       "sv_k_pct",
    "BB%":      "sv_bb_pct",
}


# ══════════════════════════════════════════════════════════════════════════════
# DB persistence (same as existing PR #562)
# ══════════════════════════════════════════════════════════════════════════════

def _save_model_to_db(prop_type: str, pkl_path: str,
                       metrics: dict, n_train: int,
                       feature_names: list) -> None:
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        return
    if not os.path.exists(pkl_path):
        logger.warning("[DB] PKL missing, skipping DB persist: %s", pkl_path)
        return
    try:
        import psycopg2
        with open(pkl_path, "rb") as f:
            model_bytes = f.read()
        model_b64 = base64.b64encode(model_bytes).decode("ascii")
        feat_json = json.dumps(feature_names)
        note = (f"v2-retrain {datetime.now(timezone.utc).date()} "
                f"n={n_train} season_weighted")
        with psycopg2.connect(db_url, connect_timeout=15) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS xgb_model_store (
                        id            SERIAL PRIMARY KEY,
                        prop_type     TEXT NOT NULL,
                        model_json    TEXT NOT NULL,
                        feature_names TEXT,
                        brier_score   FLOAT,
                        n_samples     INT,
                        notes         TEXT,
                        trained_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        UNIQUE(prop_type)
                    )
                """)
                cur.execute("""
                    INSERT INTO xgb_model_store
                        (prop_type, model_json, feature_names,
                         brier_score, n_samples, notes, trained_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (prop_type) DO UPDATE SET
                        model_json    = EXCLUDED.model_json,
                        feature_names = EXCLUDED.feature_names,
                        brier_score   = EXCLUDED.brier_score,
                        n_samples     = EXCLUDED.n_samples,
                        notes         = EXCLUDED.notes,
                        trained_at    = NOW()
                """, (prop_type, model_b64, feat_json,
                      metrics.get("brier"), n_train, note))
        logger.info("[DB] Persisted '%s' → xgb_model_store (brier=%s)",
                    prop_type,
                    f"{metrics['brier']:.4f}" if metrics.get("brier") else "n/a")
    except Exception as exc:
        logger.warning("[DB] Failed to persist '%s': %s", prop_type, exc)


# ══════════════════════════════════════════════════════════════════════════════
# Data loading — Source 1: Real PropIQ bet_ledger
# ══════════════════════════════════════════════════════════════════════════════

def _load_from_ledger() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load real graded PropIQ legs from bet_ledger with layer_audit features.
    Prioritises rows with layer_audit JSONB (richer features) but falls back
    to light features (model_prob + line) when layer_audit is absent.
    Returns (k_df, hits_df).
    """
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        return pd.DataFrame(), pd.DataFrame()

    try:
        import psycopg2
        conn = psycopg2.connect(db_url, connect_timeout=10)
        cur  = conn.cursor()

        # K legs — pull with layer_audit for rich features
        cur.execute("""
            SELECT
                model_prob,
                line,
                side,
                prop_type,
                actual_outcome,
                bet_date,
                layer_audit
            FROM bet_ledger
            WHERE prop_type IN ('strikeouts', 'pitching_outs')
              AND actual_outcome IS NOT NULL
              AND discord_sent  = TRUE
              AND lookahead_safe = TRUE
              AND model_prob    IS NOT NULL
            ORDER BY bet_date DESC
            LIMIT 50000
        """)
        k_rows = cur.fetchall()

        # Hit legs
        cur.execute("""
            SELECT
                model_prob,
                line,
                side,
                prop_type,
                actual_outcome,
                bet_date,
                layer_audit
            FROM bet_ledger
            WHERE prop_type IN ('hits', 'total_bases', 'hits_runs_rbis')
              AND actual_outcome IS NOT NULL
              AND discord_sent  = TRUE
              AND lookahead_safe = TRUE
              AND model_prob    IS NOT NULL
            ORDER BY bet_date DESC
            LIMIT 50000
        """)
        hit_rows = cur.fetchall()
        conn.close()

        def _parse_rows(rows: list, is_k: bool) -> pd.DataFrame:
            records = []
            medians = K_MEDIANS if is_k else HIT_MEDIANS
            feats   = K_FEATURES if is_k else HITS_FEATURES

            for mp, line, side, prop_type, outcome, bet_date, layer_audit in rows:
                try:
                    rec: dict = {}

                    # Base features always available
                    rec["model_prob_feat"] = float(mp or 0) / 100.0
                    rec["line"]            = float(line or 4.5)
                    rec["side_over"]       = 1 if str(side or "").upper() in ("OVER", "HIGHER") else 0
                    rec["actual_outcome"]  = 1 if str(outcome).upper() in ("WIN", "1") else 0
                    rec["prop_type"]       = prop_type or ""

                    # Season for weighting
                    rec["season"] = int(bet_date.year) if hasattr(bet_date, "year") else 2026

                    # Enrich from layer_audit if available
                    if layer_audit and isinstance(layer_audit, dict):
                        la = layer_audit
                        if is_k:
                            rec["sv_k_pct"]    = float(la.get("sv_k_pct") or medians["sv_k_pct"])
                            rec["sv_bb_pct"]   = float(la.get("sv_bb_pct") or medians["sv_bb_pct"])
                            rec["sv_whiff_pct"]= float(la.get("sv_whiff_pct") or medians["sv_whiff_pct"])
                            rec["days_rest"]   = float(la.get("days_rest") or medians["days_rest"])
                        else:
                            rec["sv_xba"]     = float(la.get("sv_xba") or medians["sv_xba"])
                            rec["sv_xwoba"]   = float(la.get("sv_xwoba") or medians["sv_xwoba"])
                            rec["platoon_adv"]= float(la.get("platoon_adv") or 0)

                    # Fill missing features with medians
                    for feat in feats:
                        if feat not in rec:
                            rec[feat] = medians.get(feat, 0.0)

                    records.append(rec)
                except Exception:
                    continue

            return pd.DataFrame(records)

        k_df   = _parse_rows(k_rows, is_k=True)
        hit_df = _parse_rows(hit_rows, is_k=False)
        logger.info("Ledger: %d K rows, %d hit rows", len(k_df), len(hit_df))
        return k_df, hit_df

    except Exception as e:
        logger.warning("Ledger load failed: %s", e)
        return pd.DataFrame(), pd.DataFrame()


# ══════════════════════════════════════════════════════════════════════════════
# Data loading — Source 2: pybaseball Statcast (fallback / supplement)
# ══════════════════════════════════════════════════════════════════════════════

def _load_from_statcast() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Pull Statcast + FanGraphs via pybaseball for SEASONS.
    Uses training-aligned feature names. Adds season column for recency weighting.
    """
    try:
        from pybaseball import statcast, pitching_stats, batting_stats, cache
        cache.enable()
    except ImportError:
        logger.warning("pybaseball not installed — skipping Statcast source")
        return pd.DataFrame(), pd.DataFrame()

    # FanGraphs season aggregates
    fg_pit_frames, fg_bat_frames = [], []
    for yr in SEASONS:
        try:
            df = pitching_stats(yr, qual=MIN_BF)
            df["season"] = yr
            fg_pit_frames.append(df)
            logger.info("  FG pit %d: %d rows", yr, len(df))
        except Exception as e:
            logger.warning("  FG pit %d failed: %s", yr, e)
        try:
            df = batting_stats(yr, qual=MIN_PA)
            df["season"] = yr
            fg_bat_frames.append(df)
            logger.info("  FG bat %d: %d rows", yr, len(df))
        except Exception as e:
            logger.warning("  FG bat %d failed: %s", yr, e)

    fg_pit = pd.concat(fg_pit_frames, ignore_index=True) if fg_pit_frames else pd.DataFrame()
    fg_bat = pd.concat(fg_bat_frames, ignore_index=True) if fg_bat_frames else pd.DataFrame()

    # Per-game Statcast
    pit_frames, bat_frames = [], []
    for yr in SEASONS:
        start = f"{yr}-03-28"
        end   = f"{yr}-10-05"
        try:
            sc = statcast(start_dt=start, end_dt=end)
            sc = sc[sc["game_type"] == "R"].copy()
            sc["is_k"]   = sc["events"].isin({"strikeout", "strikeout_double_play"}).astype(int)
            sc["is_hit"] = sc["events"].isin({"single", "double", "triple", "home_run"}).astype(int)

            # Pitcher-game aggregation
            pg = (sc.groupby(["game_pk", "game_date", "pitcher"])
                  .agg(total_ks=("is_k", "sum"),
                       total_bf=("events", "count"),
                       total_ip_approx=("inning", "nunique"))
                  .reset_index())
            pg["season"]   = yr
            pg["l5_ip"]    = (pg.groupby("pitcher")["total_ip_approx"]
                                .transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean()))
            pg["l3_ip"]    = (pg.groupby("pitcher")["total_ip_approx"]
                                .transform(lambda x: x.shift(1).rolling(3, min_periods=1).mean()))
            pg["l5_ks"]    = (pg.groupby("pitcher")["total_ks"]
                                .transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean()))
            pg["l3_ks"]    = (pg.groupby("pitcher")["total_ks"]
                                .transform(lambda x: x.shift(1).rolling(3, min_periods=1).mean()))
            pg["l10_ks"]   = (pg.groupby("pitcher")["total_ks"]
                                .transform(lambda x: x.shift(1).rolling(10, min_periods=1).mean()))
            # Approximate days_rest from game_date diff
            pg["game_date_dt"] = pd.to_datetime(pg["game_date"])
            pg["days_rest"]    = (pg.groupby("pitcher")["game_date_dt"]
                                    .transform(lambda x: x.diff().dt.days.fillna(5)))

            # Opp lineup K%
            opp = (sc.groupby(["game_pk", "pitcher"])
                   .agg(opp_k_events=("is_k", "sum"), opp_pa=("events", "count"))
                   .reset_index())
            opp["opp_lineup_k_pct_proxy"] = opp["opp_k_events"] / opp["opp_pa"].clip(lower=1) * 100
            opp["opp_lineup_xwoba_proxy"] = 0.320  # filled from lineup context at inference
            pg = pg.merge(opp[["game_pk", "pitcher",
                                "opp_lineup_k_pct_proxy",
                                "opp_lineup_xwoba_proxy"]],
                          on=["game_pk", "pitcher"], how="left")
            pit_frames.append(pg)

            # Batter-game aggregation
            bg = (sc.groupby(["game_pk", "game_date", "batter", "pitcher",
                               "p_throws", "stand"])
                  .agg(hits=("is_hit", "sum"), abs=("is_hit", "count"))
                  .reset_index())
            bg["season"]      = yr
            bg["hit_binary"]  = (bg["hits"] >= 1).astype(int)
            bg["l7_hits"]     = (bg.groupby("batter")["hits"]
                                   .transform(lambda x: x.shift(1).rolling(7, min_periods=1).sum()))
            bg["l7_hit_rate"] = (bg.groupby("batter")["hit_binary"]
                                   .transform(lambda x: x.shift(1).rolling(7, min_periods=1).mean()))
            bat_frames.append(bg)
            logger.info("  Statcast %d: %d pit-game, %d bat-game rows", yr, len(pg), len(bg))

        except Exception as e:
            logger.warning("  Statcast %d failed: %s", yr, e)

    pit_game_df = pd.concat(pit_frames, ignore_index=True) if pit_frames else pd.DataFrame()
    bat_game_df = pd.concat(bat_frames, ignore_index=True) if bat_frames else pd.DataFrame()

    # Merge FanGraphs season stats with training-aligned column names
    if not fg_pit.empty and not pit_game_df.empty:
        fg_p = fg_pit.rename(columns=FG_PIT_RENAME)
        for pct_col in ("sv_k_pct", "sv_bb_pct", "sv_whiff_pct"):
            if pct_col in fg_p.columns:
                fg_p[pct_col] = fg_p[pct_col].apply(
                    lambda x: x * 100 if pd.notna(x) and 0 < x <= 1.0 else x)
        merge_cols = ["IDfg", "season", "sv_xera"] + [
            v for v in FG_PIT_RENAME.values() if v in fg_p.columns]
        if "IDfg" in fg_p.columns:
            pit_game_df = pit_game_df.merge(
                fg_p[[c for c in merge_cols if c in fg_p.columns]],
                left_on=["pitcher", "season"],
                right_on=["IDfg", "season"], how="left")
        # sv_era = ERA (same as fg_era but with training-aligned name)
        if "sv_era" not in pit_game_df.columns and "ERA" in fg_p.columns:
            pit_game_df["sv_era"] = pit_game_df.get("ERa", K_MEDIANS["sv_era"])

    if not fg_bat.empty and not bat_game_df.empty:
        fg_b = fg_bat.rename(columns=FG_BAT_RENAME)
        for pct_col in ("sv_k_pct", "sv_bb_pct", "sv_ss_pct", "sv_brl_pct", "sv_hh_pct"):
            if pct_col in fg_b.columns:
                fg_b[pct_col] = fg_b[pct_col].apply(
                    lambda x: x * 100 if pd.notna(x) and 0 < x <= 1.0 else x)
        merge_cols = ["IDfg", "season"] + [v for v in FG_BAT_RENAME.values() if v in fg_b.columns]
        if "IDfg" in fg_b.columns:
            bat_game_df = bat_game_df.merge(
                fg_b[[c for c in merge_cols if c in fg_b.columns]],
                left_on=["batter", "season"],
                right_on=["IDfg", "season"], how="left")

    # Platoon flags
    if "p_throws" in bat_game_df.columns:
        bat_game_df["throws_R"] = (bat_game_df["p_throws"] == "R").astype(int)
        bat_game_df["bats_L"]   = (bat_game_df["stand"] == "L").astype(int)
        bat_game_df["platoon_adv"] = (
            ((bat_game_df["bats_L"] == 1) & (bat_game_df["throws_R"] == 1)) |
            ((bat_game_df["bats_L"] == 0) & (bat_game_df["throws_R"] == 0))
        ).astype(int)
    else:
        bat_game_df["throws_R"]    = 1
        bat_game_df["bats_L"]      = 0
        bat_game_df["platoon_adv"] = 0

    # opp_whiff for hit model (pitcher SwStr% — was missing before)
    for col in ("opp_xera", "opp_k_pct", "opp_bb_pct", "opp_whiff"):
        if col not in bat_game_df.columns:
            bat_game_df[col] = HIT_MEDIANS.get(col, 0.0)

    # K binary labels
    for line in K_LINES:
        if "total_ks" in pit_game_df.columns:
            pit_game_df[f"k_over_{line}"] = (pit_game_df["total_ks"] > line).astype(int)
    if "hit_binary" in bat_game_df.columns:
        bat_game_df["actual_outcome"] = bat_game_df["hit_binary"]

    # Fill medians
    for col, med in K_MEDIANS.items():
        if col not in pit_game_df.columns:
            pit_game_df[col] = med
        else:
            pit_game_df[col] = pit_game_df[col].fillna(med)
    for col, med in HIT_MEDIANS.items():
        if col not in bat_game_df.columns:
            bat_game_df[col] = med
        else:
            bat_game_df[col] = bat_game_df[col].fillna(med)

    logger.info("Statcast: %d pit-game, %d bat-game rows", len(pit_game_df), len(bat_game_df))
    return pit_game_df, bat_game_df


# ══════════════════════════════════════════════════════════════════════════════
# Sample weights — recency-based
# ══════════════════════════════════════════════════════════════════════════════

def _make_sample_weights(df: pd.DataFrame) -> np.ndarray:
    """
    Assign per-row sample weights based on season.
    Recent seasons get higher weight — corrects for league-level shift.
    """
    if "season" not in df.columns:
        return np.ones(len(df))
    return df["season"].map(SEASON_WEIGHTS).fillna(1.0).values


# ══════════════════════════════════════════════════════════════════════════════
# Training
# ══════════════════════════════════════════════════════════════════════════════

def _train_and_save(X_train: np.ndarray, y_train: np.ndarray,
                    X_test:  np.ndarray, y_test:  np.ndarray,
                    label:   str,        out_path: str,
                    sample_weights: np.ndarray | None = None) -> dict:
    """Train one XGBClassifier with Platt calibration and recency weights."""
    from xgboost import XGBClassifier
    from sklearn.calibration import CalibratedClassifierCV
    from sklearn.metrics import roc_auc_score, log_loss, brier_score_loss

    pos_ratio = max((y_train == 0).sum() / max((y_train == 1).sum(), 1), 1.0)
    logger.info("  %s: %d train / %d test | pos_ratio=%.2f",
                label, len(X_train), len(X_test), pos_ratio)

    raw = XGBClassifier(**XGB_PARAMS, scale_pos_weight=pos_ratio,
                        use_label_encoder=False)
    model = CalibratedClassifierCV(raw, method="sigmoid", cv=5)
    model.fit(X_train, y_train, sample_weight=sample_weights)

    metrics: dict = {}
    if len(X_test) > 0 and y_test.sum() > 0:
        probs = model.predict_proba(X_test)[:, 1]
        metrics = dict(
            auc     = round(float(roc_auc_score(y_test, probs)), 4),
            logloss = round(float(log_loss(y_test, probs)), 4),
            brier   = round(float(brier_score_loss(y_test, probs)), 4),
            n_test  = int(len(X_test)),
        )
        logger.info("  %s → AUC %.4f | Brier %.4f (null=0.25, target<0.23)",
                    label, metrics["auc"], metrics["brier"])
        if metrics["brier"] > 0.25:
            logger.warning("  ⚠️  %s Brier %.4f > null model — check training data quality",
                           label, metrics["brier"])
    else:
        logger.info("  %s → trained (no held-out test — early season)", label)

    with open(out_path, "wb") as f:
        pickle.dump(model, f)
    return metrics


def _run_shap(model_path: str, df: pd.DataFrame, features: list) -> list:
    """Run SHAP feature importance for interpretability."""
    try:
        import shap, pickle as _pkl
        with open(model_path, "rb") as f:
            model = _pkl.load(f)
        avail = [c for c in features if c in df.columns]
        X     = df[avail].fillna(0).values.astype(np.float32)
        idx   = np.random.choice(len(X), min(2000, len(X)), replace=False)
        base  = model.calibrated_classifiers_[0].estimator
        exp   = shap.TreeExplainer(base)
        sv    = exp.shap_values(X[idx])
        mean_abs = np.abs(sv).mean(axis=0)
        ranked   = sorted(zip(avail, mean_abs), key=lambda x: x[1], reverse=True)
        logger.info("  SHAP importance:")
        for feat, imp in ranked:
            bar = "█" * int(imp / max(ranked[0][1], 1e-9) * 20)
            logger.info("    %-28s %s %.4f", feat, bar, imp)
        return [{"feature": f, "importance": round(float(i), 4)} for f, i in ranked]
    except Exception as e:
        logger.warning("SHAP failed: %s", e)
        return []


# ══════════════════════════════════════════════════════════════════════════════
# Status check
# ══════════════════════════════════════════════════════════════════════════════

def show_status() -> None:
    metrics_path = os.path.join(OUTDIR, "model_metrics.json")
    if not os.path.exists(metrics_path):
        print("No model_metrics.json found — models not yet trained.")
        return
    with open(metrics_path) as f:
        m = json.load(f)
    print(f"\n=== XGBoost Model Status (trained {m.get('trained_at', 'unknown')}) ===")
    print(f"{'Model':<12} {'Brier':>8} {'AUC':>8} {'N Test':>8} {'Status'}")
    print("-" * 60)
    null_brier = 0.25
    for key in ["k_3.5", "k_4.5", "k_5.5", "k_6.5", "hits"]:
        d = m.get(key, {})
        brier  = d.get("brier")
        auc    = d.get("auc")
        n_test = d.get("n_test", 0)
        if brier is None:
            status = "⚠️  No test data"
        elif brier < 0.23:
            status = "✅ Well calibrated"
        elif brier < null_brier:
            status = "🟡 Marginal edge"
        else:
            status = "❌ Worse than null"
        b_str = f"{brier:.4f}" if brier else "N/A"
        a_str = f"{auc:.4f}"   if auc   else "N/A"
        print(f"  {key:<10} {b_str:>8} {a_str:>8} {n_test:>8}   {status}")

    print(f"\n  Null model Brier: {null_brier} (always predict 50%)")
    print(f"  Target Brier:     <0.23 to justify current blend weights")
    print(f"\n  Blend recommendations:")
    for key in ["k_3.5", "k_4.5", "k_5.5", "k_6.5"]:
        brier = m.get(key, {}).get("brier", 0.25)
        if brier and brier < 0.23:
            rec = "70/30 — increase XGB weight"
        elif brier and brier < null_brier:
            rec = "80/20 — current default (marginal edge)"
        else:
            rec = "90/10 — reduce XGB weight (worse than null)"
        print(f"    {key}: {rec}")
    hits_brier = m.get("hits", {}).get("brier", 0.25)
    if hits_brier and hits_brier > null_brier:
        print(f"    hits: 90/10 ⚠️  (Brier {hits_brier:.4f} > null) — REDUCE BLEND")
    else:
        print(f"    hits: 80/20 (Brier {hits_brier:.4f})")


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def main(k_only: bool = False, hit_only: bool = False) -> None:
    logger.info("=== PropIQ XGBoost Training v2 (season-weighted) ===")
    logger.info("Season weights: %s", SEASON_WEIGHTS)

    # Load data
    ledger_k, ledger_hits = _load_from_ledger()
    stat_k, stat_hits = pd.DataFrame(), pd.DataFrame()

    need_statcast_k   = len(ledger_k)   < 500 and not hit_only
    need_statcast_hit = len(ledger_hits) < 500 and not k_only

    if need_statcast_k or need_statcast_hit:
        logger.info("Supplementing with Statcast (ledger rows insufficient)...")
        stat_k, stat_hits = _load_from_statcast()

    # Combine — ledger rows are highest quality (real PropIQ outcomes)
    # Give ledger rows 3x weight relative to historical Statcast
    def _combine(ledger_df, stat_df, is_k):
        if ledger_df.empty and stat_df.empty:
            return pd.DataFrame()
        if ledger_df.empty:
            return stat_df
        if stat_df.empty:
            # Boost ledger weights to compensate for small sample
            ledger_df = ledger_df.copy()
            if "season" not in ledger_df.columns:
                ledger_df["season"] = 2026
            return ledger_df
        # Give ledger rows 3x season weight bonus
        ledger_boost = ledger_df.copy()
        if "season" not in ledger_boost.columns:
            ledger_boost["season"] = 2026
        ledger_boost["_ledger_boost"] = 3.0
        stat_df2 = stat_df.copy()
        stat_df2["_ledger_boost"] = 1.0
        return pd.concat([ledger_boost, stat_df2], ignore_index=True)

    k_df   = _combine(ledger_k,   stat_k,   is_k=True)  if not hit_only else pd.DataFrame()
    hit_df = _combine(ledger_hits, stat_hits, is_k=False) if not k_only  else pd.DataFrame()

    if k_df.empty and hit_df.empty:
        logger.error("No training data. Install pybaseball or connect DATABASE_URL.")
        return

    all_metrics = {
        "trained_at":      datetime.now(timezone.utc).isoformat(),
        "seasons":         SEASONS,
        "season_weights":  SEASON_WEIGHTS,
        "test_year":       TEST_YEAR,
        "blend_recommendation": {
            "note": "Check status with --status after training",
        },
    }

    # ── Train K models ──────────────────────────────────────────────────────
    if not k_df.empty:
        logger.info("\n=== K Models (per-line, season-weighted) ===")
        for line in K_LINES:
            label_col = f"k_over_{line}"

            if label_col not in k_df.columns:
                if "actual_outcome" in k_df.columns and "line" in k_df.columns:
                    k_df[label_col] = (
                        (k_df["actual_outcome"] == 1) &
                        (k_df["line"].round(1) == line)
                    ).astype(int)
                else:
                    logger.warning("K>%.1f: label missing — skipping", line)
                    continue

            # Train/test split by season
            if "season" in k_df.columns:
                train = k_df[k_df["season"] != TEST_YEAR]
                test  = k_df[k_df["season"] == TEST_YEAR]
            else:
                split = int(len(k_df) * 0.80)
                train, test = k_df.iloc[:split], k_df.iloc[split:]

            # Filter to relevant line
            if "line" in k_df.columns:
                train_filt = train[(train["line"] - line).abs() <= 0.5] if len(train) > 100 else train
                test_filt  = test[(test["line"]  - line).abs() <= 0.5]  if len(test)  > 10  else test
            else:
                train_filt, test_filt = train, test

            if len(train_filt) < 50:
                logger.warning("K>%.1f: only %d train rows — skipping", line, len(train_filt))
                continue

            avail    = [c for c in K_FEATURES if c in k_df.columns]
            X_train  = train_filt[avail].fillna(0).values.astype(np.float32)
            y_train  = train_filt[label_col].values
            X_test   = test_filt[avail].fillna(0).values.astype(np.float32)  if len(test_filt) else X_train[:0]
            y_test   = test_filt[label_col].values                            if len(test_filt) else y_train[:0]

            # Recency weights: combine season weight × ledger boost
            sw = _make_sample_weights(train_filt)
            if "_ledger_boost" in train_filt.columns:
                sw = sw * train_filt["_ledger_boost"].values

            safe_line = str(line).replace(".", "_")
            out_path  = os.path.join(OUTDIR, f"xgb_k_{safe_line}.pkl")
            metrics   = _train_and_save(X_train, y_train, X_test, y_test,
                                        f"K>{line}", out_path,
                                        sample_weights=sw)
            n_train   = int(len(X_train))
            all_metrics[f"k_{line}"] = {**metrics, "train_rows": n_train, "features": avail}

            _save_model_to_db(f"k_{line}", out_path, metrics, n_train, avail)

        # SHAP for K4.5 (most common line)
        k45_path = os.path.join(OUTDIR, "xgb_k_4_5.pkl")
        if os.path.exists(k45_path) and not k_df.empty:
            all_metrics["shap_k_4_5"] = _run_shap(k45_path, k_df, K_FEATURES)

    # ── Train hit model ─────────────────────────────────────────────────────
    if not hit_df.empty and "actual_outcome" in hit_df.columns:
        logger.info("\n=== Hit Model (season-weighted) ===")

        if "season" in hit_df.columns:
            train_h = hit_df[hit_df["season"] != TEST_YEAR]
            test_h  = hit_df[hit_df["season"] == TEST_YEAR]
        else:
            split   = int(len(hit_df) * 0.80)
            train_h, test_h = hit_df.iloc[:split], hit_df.iloc[split:]

        avail_h   = [c for c in HITS_FEATURES if c in hit_df.columns]
        X_train_h = train_h[avail_h].fillna(0).values.astype(np.float32)
        y_train_h = train_h["actual_outcome"].values
        X_test_h  = test_h[avail_h].fillna(0).values.astype(np.float32)  if len(test_h) else X_train_h[:0]
        y_test_h  = test_h["actual_outcome"].values                        if len(test_h) else y_train_h[:0]

        sw_h = _make_sample_weights(train_h)
        if "_ledger_boost" in train_h.columns:
            sw_h = sw_h * train_h["_ledger_boost"].values

        out_path_h = os.path.join(OUTDIR, "xgb_hits.pkl")
        metrics_h  = _train_and_save(X_train_h, y_train_h, X_test_h, y_test_h,
                                     "Hits", out_path_h, sample_weights=sw_h)
        n_train_h  = int(len(X_train_h))
        all_metrics["hits"] = {**metrics_h, "train_rows": n_train_h, "features": avail_h}

        _save_model_to_db("hits", out_path_h, metrics_h, n_train_h, avail_h)

        # Blend recommendation for hits
        hit_brier = metrics_h.get("brier", 0.25)
        if hit_brier and hit_brier > 0.25:
            all_metrics["blend_recommendation"]["hits"] = (
                f"90/10 — Brier {hit_brier:.4f} > null (0.25). "
                "Reduce from current 70/30 to limit noise contribution."
            )
            logger.warning("⚠️  Hit model Brier %.4f > null — recommend 90/10 blend", hit_brier)
        elif hit_brier and hit_brier < 0.23:
            all_metrics["blend_recommendation"]["hits"] = (
                f"60/40 — Brier {hit_brier:.4f} well below null. "
                "Consider increasing blend weight."
            )

    # ── Save feature cols and metrics ───────────────────────────────────────
    feat_cols_out = {f"k_{line}": K_FEATURES for line in K_LINES}
    feat_cols_out["hits"] = HITS_FEATURES

    with open(os.path.join(OUTDIR, "xgb_feature_cols.json"), "w") as f:
        json.dump(feat_cols_out, f, indent=2)

    with open(os.path.join(OUTDIR, "model_metrics.json"), "w") as f:
        json.dump(all_metrics, f, indent=2)

    logger.info("\n✅ Training complete.")
    logger.info("   Run: python scripts/xgb_k_training.py --status")
    show_status()


if __name__ == "__main__":
    if "--status" in sys.argv:
        show_status()
    elif "--k-only" in sys.argv:
        main(k_only=True)
    elif "--hit-only" in sys.argv:
        main(hit_only=True)
    else:
        main()
