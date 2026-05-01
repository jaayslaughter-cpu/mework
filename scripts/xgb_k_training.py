"""
scripts/xgb_k_training.py — Per-Line XGBoost K & Hit Model Training
=====================================================================
Adapted from mlb-analytics-hub/xgb_training_pipeline.py
Source: github.com/johnmsimo/mlb-analytics-hub

Trains 4 separate K models (one per line: 3.5/4.5/5.5/6.5) and one
batter-hit model, each with Platt-sigmoid calibration.

Insight: K > 3.5 and K > 6.5 have DIFFERENT optimal feature importance.
  - 3.5 line: dominated by SwStr% and platoon adjustment
  - 6.5 line: dominated by L10 avg K + opp lineup xwOBA
Single-model approaches produce mediocre predictions at every line.

Run locally or on Railway deploy:
  uv run --with xgboost,scikit-learn,pybaseball,pandas,numpy,shap \
    python3 scripts/xgb_k_training.py

Outputs (saved to models/):
  xgb_k_3_5.pkl, xgb_k_4_5.pkl, xgb_k_5_5.pkl, xgb_k_6_5.pkl
  xgb_hits.pkl
  xgb_feature_cols.json
  model_metrics.json

Uses our Postgres bet_ledger (real graded legs) when available,
falling back to pybaseball Statcast (2021–2025) for initial training.
"""

from __future__ import annotations

import json
import logging
import os
import pickle
import warnings
from datetime import datetime, timezone

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [xgb_train] %(message)s")
logger = logging.getLogger("xgb_train")

# ── Config ──────────────────────────────────────────────────────────────────
SEASONS       = [2021, 2022, 2023, 2024, 2025]
MIN_BF        = 50       # minimum batters-faced for pitcher inclusion
MIN_PA        = 50       # minimum PA for batter inclusion
TEST_YEAR     = 2025     # held-out season for evaluation
K_LINES       = [3.5, 4.5, 5.5, 6.5]

XGB_PARAMS = dict(
    n_estimators    = 600,
    max_depth       = 5,
    learning_rate   = 0.04,
    subsample       = 0.80,
    colsample_bytree= 0.75,
    min_child_weight= 6,
    gamma           = 0.05,
    reg_alpha       = 0.10,
    reg_lambda      = 1.5,
    eval_metric     = "logloss",
    random_state    = 42,
    n_jobs          = -1,
)

HERE      = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(HERE)
OUTDIR    = os.path.join(REPO_ROOT, "models")
os.makedirs(OUTDIR, exist_ok=True)

# Feature lists — must match xgb_k_layer.py exactly
K_FEATURES = [
    "sv_xera", "fg_era", "fg_kpct", "fg_bbpct", "sv_swstr_pct",
    "l5_ks", "l5_k_rate", "l10_ks", "opp_k_pct", "opp_xwoba",
]

HITS_FEATURES = [
    "sv_xba", "sv_xwoba", "sv_xslg", "sv_ev", "sv_brl_pct", "sv_hh_pct",
    "sv_swstr_pct", "sv_la", "fg_kpct", "fg_bbpct",
    "opp_xera", "opp_k_pct", "opp_bb_pct", "opp_swstr_pct",
    "bats_L", "throws_R", "platoon_adv",
    "l7_hits", "l7_hit_rate",
]

K_MEDIANS = {
    "sv_xera": 4.50, "fg_era": 4.50, "fg_kpct": 22.0, "fg_bbpct": 8.0,
    "sv_swstr_pct": 24.0, "l5_ks": 4.5, "l5_k_rate": 22.0, "l10_ks": 4.5,
    "opp_k_pct": 22.0, "opp_xwoba": 0.320,
}

HIT_MEDIANS = {
    "sv_xba": 0.250, "sv_xwoba": 0.320, "sv_xslg": 0.400,
    "sv_ev": 88.0, "sv_brl_pct": 4.0, "sv_hh_pct": 35.0,
    "sv_swstr_pct": 10.0, "sv_la": 12.0, "fg_kpct": 22.0, "fg_bbpct": 8.0,
    "opp_xera": 4.50, "opp_k_pct": 22.0, "opp_bb_pct": 8.0, "opp_swstr_pct": 24.0,
    "bats_L": 0, "throws_R": 1, "platoon_adv": 0,
    "l7_hits": 1.5, "l7_hit_rate": 0.50,
}


# ── Source 1: Postgres bet_ledger (real PropIQ graded legs) ─────────────────

def _load_from_ledger() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load real graded K and hit legs from bet_ledger.
    Returns (k_df, hits_df) — may be empty if DB unavailable or insufficient rows.
    """
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        logger.info("DATABASE_URL not set — skipping ledger source")
        return pd.DataFrame(), pd.DataFrame()

    try:
        import psycopg2
        conn = psycopg2.connect(db_url, connect_timeout=10)
        cur  = conn.cursor()

        # K legs: actual_outcome + features_json
        cur.execute("""
            SELECT features_json, actual_outcome, prop_type, line, side
              FROM bet_ledger
             WHERE prop_type IN ('strikeouts', 'pitching_outs')
               AND actual_outcome IS NOT NULL
               AND discord_sent = TRUE
               AND lookahead_safe = TRUE
               AND features_json IS NOT NULL
               AND features_json::text NOT LIKE '%%backfilled%%'
             LIMIT 25000
        """)
        k_rows = cur.fetchall()

        # Hit legs
        cur.execute("""
            SELECT features_json, actual_outcome, prop_type, line, side
              FROM bet_ledger
             WHERE prop_type IN ('hits', 'fantasy_score')
               AND actual_outcome IS NOT NULL
               AND discord_sent = TRUE
               AND lookahead_safe = TRUE
               AND features_json IS NOT NULL
               AND features_json::text NOT LIKE '%%backfilled%%'
             LIMIT 25000
        """)
        hit_rows = cur.fetchall()
        conn.close()

        def _rows_to_df(rows: list, feature_names: list) -> pd.DataFrame:
            records = []
            for fj, outcome, prop_type, line, side in rows:
                try:
                    if isinstance(fj, str):
                        vec = json.loads(fj)
                    else:
                        vec = fj
                    if not isinstance(vec, list) or len(vec) < len(feature_names):
                        continue
                    rec = {feature_names[i]: vec[i] for i in range(len(feature_names))}
                    rec["actual_outcome"] = 1 if str(outcome).upper() in ("WIN", "1") else 0
                    rec["line"]           = float(line or 4.5)
                    records.append(rec)
                except Exception:
                    continue
            return pd.DataFrame(records)

        k_df   = _rows_to_df(k_rows, K_FEATURES)
        hit_df = _rows_to_df(hit_rows, HITS_FEATURES)
        logger.info("Ledger: %d K rows, %d hit rows", len(k_df), len(hit_df))
        return k_df, hit_df

    except Exception as e:
        logger.warning("Ledger load failed: %s", e)
        return pd.DataFrame(), pd.DataFrame()


# ── Source 2: pybaseball Statcast (fallback / supplemental) ─────────────────

def _load_from_statcast() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Pull Statcast + FanGraphs via pybaseball for 2021–2025.
    Returns (k_df, hits_df).
    """
    try:
        from pybaseball import (
            statcast, pitching_stats, batting_stats, cache,
        )
        cache.enable()
    except ImportError:
        logger.warning("pybaseball not installed — skipping Statcast source")
        return pd.DataFrame(), pd.DataFrame()

    logger.info("Fetching FanGraphs batting leaderboards...")
    fg_bat_frames: list[pd.DataFrame] = []
    for yr in SEASONS:
        try:
            df = batting_stats(yr, qual=MIN_PA)
            df["season"] = yr
            fg_bat_frames.append(df)
            logger.info("  FG bat %d: %d rows", yr, len(df))
        except Exception as e:
            logger.warning("  FG bat %d failed: %s", yr, e)
    fg_bat = pd.concat(fg_bat_frames, ignore_index=True) if fg_bat_frames else pd.DataFrame()

    logger.info("Fetching FanGraphs pitching leaderboards...")
    fg_pit_frames: list[pd.DataFrame] = []
    for yr in SEASONS:
        try:
            df = pitching_stats(yr, qual=MIN_BF)
            df["season"] = yr
            fg_pit_frames.append(df)
            logger.info("  FG pit %d: %d rows", yr, len(df))
        except Exception as e:
            logger.warning("  FG pit %d failed: %s", yr, e)
    fg_pit = pd.concat(fg_pit_frames, ignore_index=True) if fg_pit_frames else pd.DataFrame()

    # ── Pull per-game Statcast outcomes ──────────────────────────────────────
    logger.info("Pulling per-game Statcast (this takes ~10 min for 5 seasons)...")
    pit_frames: list[pd.DataFrame] = []
    bat_frames: list[pd.DataFrame] = []

    for yr in SEASONS:
        start = f"{yr}-03-28"
        end   = f"{yr}-10-05"
        try:
            sc = statcast(start_dt=start, end_dt=end)
            sc = sc[sc["game_type"] == "R"].copy()

            sc["is_hit"] = sc["events"].isin(
                {"single", "double", "triple", "home_run"}).astype(int)
            sc["is_k"]   = sc["events"].isin(
                {"strikeout", "strikeout_double_play"}).astype(int)

            # Pitcher-game
            pg = (sc.groupby(["game_pk", "game_date", "pitcher"])
                  .agg(total_ks=("is_k", "sum"), total_bf=("events", "count"))
                  .reset_index())
            pg["season"] = yr
            opp_agg = (sc.groupby(["game_pk", "pitcher"])
                       .agg(opp_k_events=("is_k", "sum"),
                            opp_pa=("events", "count"))
                       .reset_index())
            opp_agg["opp_k_pct"] = (opp_agg["opp_k_events"]
                                     / opp_agg["opp_pa"].clip(lower=1) * 100)
            pg = pg.merge(opp_agg[["game_pk", "pitcher", "opp_k_pct"]],
                          on=["game_pk", "pitcher"], how="left")
            pit_frames.append(pg)
            logger.info("  %d pit-game rows %d", len(pg), yr)

            # Batter-game
            bg = (sc.groupby(["game_pk", "game_date", "batter",
                              "pitcher", "p_throws", "stand"])
                  .agg(hits=("is_hit", "sum"), abs=("is_hit", "count"))
                  .reset_index())
            bg["season"]     = yr
            bg["hit_binary"] = (bg["hits"] >= 1).astype(int)
            bat_frames.append(bg)
            logger.info("  %d bat-game rows %d", len(bg), yr)

        except Exception as e:
            logger.warning("  %d Statcast failed: %s", yr, e)

    pit_game_df = pd.concat(pit_frames, ignore_index=True) if pit_frames else pd.DataFrame()
    bat_game_df = pd.concat(bat_frames, ignore_index=True) if bat_frames else pd.DataFrame()

    # ── Rolling features ──────────────────────────────────────────────────────
    if not pit_game_df.empty:
        pit_game_df = pit_game_df.sort_values(["pitcher", "game_date"])
        pit_game_df["l5_ks"]     = (pit_game_df.groupby("pitcher")["total_ks"]
                                    .transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean()))
        pit_game_df["l10_ks"]    = (pit_game_df.groupby("pitcher")["total_ks"]
                                    .transform(lambda x: x.shift(1).rolling(10, min_periods=1).mean()))
        pit_game_df["l5_k_rate"] = (pit_game_df.groupby("pitcher")["total_ks"]
                                    .transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
                                    / pit_game_df.groupby("pitcher")["total_bf"]
                                    .transform(lambda x: x.shift(1).rolling(5, min_periods=1).mean())
                                    .clip(lower=1) * 100)

    if not bat_game_df.empty:
        bat_game_df = bat_game_df.sort_values(["batter", "game_date"])
        bat_game_df["l7_hits"]     = (bat_game_df.groupby("batter")["hits"]
                                      .transform(lambda x: x.shift(1).rolling(7, min_periods=1).sum()))
        bat_game_df["l7_hit_rate"] = (bat_game_df.groupby("batter")["hit_binary"]
                                      .transform(lambda x: x.shift(1).rolling(7, min_periods=1).mean()))

    # ── Merge FanGraphs season stats ──────────────────────────────────────────
    FG_PIT_MAP = {
        "xERA": "sv_xera", "ERA": "fg_era",
        "K%": "fg_kpct", "BB%": "fg_bbpct", "SwStr%": "sv_swstr_pct",
    }
    FG_BAT_MAP = {
        "xBA": "sv_xba", "xwOBA": "sv_xwoba", "xSLG": "sv_xslg",
        "EV": "sv_ev", "Barrels": "sv_brl_pct", "HardHit%": "sv_hh_pct",
        "SwStr%": "sv_swstr_pct", "LA": "sv_la",
        "K%": "fg_kpct", "BB%": "fg_bbpct",
    }

    if not fg_pit.empty and not pit_game_df.empty:
        fg_p = fg_pit.rename(columns={k: v for k, v in FG_PIT_MAP.items() if k in fg_pit})
        for pct_col in ("fg_kpct", "fg_bbpct", "sv_swstr_pct"):
            if pct_col in fg_p.columns:
                fg_p[pct_col] = fg_p[pct_col].apply(
                    lambda x: x * 100 if pd.notna(x) and 0 < x <= 1.0 else x)
        merge_cols = ["IDfg", "season"] + [v for v in FG_PIT_MAP.values() if v in fg_p.columns]
        if "IDfg" in fg_p.columns:
            pit_game_df = pit_game_df.merge(
                fg_p[merge_cols],
                left_on=["pitcher", "season"],
                right_on=["IDfg", "season"], how="left")
        pit_game_df["opp_xwoba"] = 0.320  # populated from lineup context at inference time

    if not fg_bat.empty and not bat_game_df.empty:
        fg_b = fg_bat.rename(columns={k: v for k, v in FG_BAT_MAP.items() if k in fg_bat})
        for pct_col in ("fg_kpct", "fg_bbpct", "sv_swstr_pct", "sv_brl_pct", "sv_hh_pct"):
            if pct_col in fg_b.columns:
                fg_b[pct_col] = fg_b[pct_col].apply(
                    lambda x: x * 100 if pd.notna(x) and 0 < x <= 1.0 else x)
        merge_cols = ["IDfg", "season"] + [v for v in FG_BAT_MAP.values() if v in fg_b.columns]
        if "IDfg" in fg_b.columns:
            bat_game_df = bat_game_df.merge(
                fg_b[merge_cols],
                left_on=["batter", "season"],
                right_on=["IDfg", "season"], how="left")

    # ── Platoon flags ────────────────────────────────────────────────────────
    if "p_throws" in bat_game_df.columns:
        bat_game_df["throws_R"] = (bat_game_df["p_throws"] == "R").astype(int)
    else:
        bat_game_df["throws_R"] = 1
    if "stand" in bat_game_df.columns:
        bat_game_df["bats_L"] = (bat_game_df["stand"] == "L").astype(int)
    else:
        bat_game_df["bats_L"] = 0
    bat_game_df["platoon_adv"] = (
        ((bat_game_df.get("bats_L", 0) == 1) & (bat_game_df.get("throws_R", 1) == 1)) |
        ((bat_game_df.get("bats_L", 0) == 0) & (bat_game_df.get("throws_R", 1) == 0))
    ).astype(int)

    # Pitcher opp columns
    for col in ("opp_xera", "opp_k_pct", "opp_bb_pct", "opp_swstr_pct"):
        if col not in bat_game_df.columns:
            bat_game_df[col] = HIT_MEDIANS.get(col, 0.0)

    # ── Fill medians ─────────────────────────────────────────────────────────
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

    # ── K binary labels ───────────────────────────────────────────────────────
    if not pit_game_df.empty and "total_ks" in pit_game_df.columns:
        for line in K_LINES:
            pit_game_df[f"k_over_{line}"] = (pit_game_df["total_ks"] > line).astype(int)
        pit_game_df["line"] = 4.5  # representative

    logger.info("Statcast: %d pit-game rows, %d bat-game rows",
                len(pit_game_df), len(bat_game_df))
    return pit_game_df, bat_game_df


# ── Train & save ─────────────────────────────────────────────────────────────

def _train_and_save(X_train: np.ndarray, y_train: np.ndarray,
                    X_test: np.ndarray,  y_test: np.ndarray,
                    label: str, out_path: str) -> dict:
    """Train one XGBClassifier with Platt calibration. Returns metrics dict."""
    from xgboost import XGBClassifier
    from sklearn.calibration import CalibratedClassifierCV
    from sklearn.metrics import roc_auc_score, log_loss, brier_score_loss

    pos_ratio = max((y_train == 0).sum() / max((y_train == 1).sum(), 1), 1.0)
    logger.info("  %s: %d train / %d test | pos_ratio=%.2f",
                label, len(X_train), len(X_test), pos_ratio)

    raw = XGBClassifier(**XGB_PARAMS, scale_pos_weight=pos_ratio,
                        use_label_encoder=False)
    model = CalibratedClassifierCV(raw, method="sigmoid", cv=5)
    model.fit(X_train, y_train)

    metrics: dict = {}
    if len(X_test) > 0 and y_test.sum() > 0:
        probs = model.predict_proba(X_test)[:, 1]
        metrics = dict(
            auc    = round(float(roc_auc_score(y_test, probs)), 4),
            logloss= round(float(log_loss(y_test, probs)), 4),
            brier  = round(float(brier_score_loss(y_test, probs)), 4),
            n_test = int(len(X_test)),
        )
        logger.info("  %s → AUC %.4f | LogLoss %.4f | Brier %.4f",
                    label, metrics["auc"], metrics["logloss"], metrics["brier"])
    else:
        logger.info("  %s → trained (no held-out test data yet)", label)

    with open(out_path, "wb") as f:
        pickle.dump(model, f)
    logger.info("  Saved → %s", out_path)
    return metrics


def main() -> None:
    logger.info("=== PropIQ Per-Line K & Hit Model Training ===")
    logger.info("Output dir: %s", OUTDIR)

    # ── Load data ────────────────────────────────────────────────────────────
    ledger_k, ledger_hits = _load_from_ledger()
    stat_k,   stat_hits   = pd.DataFrame(), pd.DataFrame()

    # Use Statcast when ledger has < 500 rows (not enough for calibrated training)
    if len(ledger_k) < 500 or len(ledger_hits) < 500:
        logger.info("Ledger rows insufficient — supplementing with Statcast...")
        stat_k, stat_hits = _load_from_statcast()

    # Combine sources: ledger first (real lines), then Statcast
    k_df   = pd.concat([ledger_k,   stat_k],   ignore_index=True) if not stat_k.empty   else ledger_k
    hit_df = pd.concat([ledger_hits, stat_hits], ignore_index=True) if not stat_hits.empty else ledger_hits

    if k_df.empty and hit_df.empty:
        logger.error("No training data available. Exiting.")
        return

    all_metrics: dict = {
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "seasons":    SEASONS,
        "test_year":  TEST_YEAR,
    }

    # ── Train K models (per line) ────────────────────────────────────────────
    if not k_df.empty:
        logger.info("\n=== K Models ===")
        for line in K_LINES:
            label_col = f"k_over_{line}"
            if label_col not in k_df.columns:
                if "actual_outcome" in k_df.columns and "line" in k_df.columns:
                    # Ledger source: reconstruct binary label from line
                    k_df[label_col] = (
                        (k_df["actual_outcome"] == 1) &
                        (k_df["line"].round(1) == line)
                    ).astype(int)
                else:
                    logger.warning("  K>%.1f: label column missing, skipping", line)
                    continue

            # Split by season (test on TEST_YEAR when season column available)
            if "season" in k_df.columns:
                train = k_df[k_df["season"] != TEST_YEAR]
                test  = k_df[k_df["season"] == TEST_YEAR]
            else:
                split = int(len(k_df) * 0.80)
                train = k_df.iloc[:split]
                test  = k_df.iloc[split:]

            # Filter to rows where this line was the actual line
            if "line" in k_df.columns:
                # Include all rows where line is within 0.5 of this target line
                train_filt = train[(train["line"] - line).abs() <= 0.5] if len(train) > 100 else train
                test_filt  = test[(test["line"]  - line).abs() <= 0.5] if len(test) > 10  else test
            else:
                train_filt, test_filt = train, test

            if len(train_filt) < 50:
                logger.warning("  K>%.1f: only %d train rows, skipping", line, len(train_filt))
                continue

            available_cols = [c for c in K_FEATURES if c in k_df.columns]
            X_train = train_filt[available_cols].fillna(0).values.astype(np.float32)
            y_train = train_filt[label_col].values
            X_test  = test_filt[available_cols].fillna(0).values.astype(np.float32) if len(test_filt) else X_train[:0]
            y_test  = test_filt[label_col].values if len(test_filt) else y_train[:0]

            safe_line = str(line).replace(".", "_")
            out_path  = os.path.join(OUTDIR, f"xgb_k_{safe_line}.pkl")
            metrics   = _train_and_save(X_train, y_train, X_test, y_test,
                                        f"K>{line}", out_path)
            all_metrics[f"k_{line}"] = {**metrics, "train_rows": int(len(X_train)),
                                         "features": available_cols}

    # ── Train hit model ──────────────────────────────────────────────────────
    if not hit_df.empty and "actual_outcome" in hit_df.columns:
        logger.info("\n=== Hit Model ===")
        if "season" in hit_df.columns:
            train_h = hit_df[hit_df["season"] != TEST_YEAR]
            test_h  = hit_df[hit_df["season"] == TEST_YEAR]
        else:
            split   = int(len(hit_df) * 0.80)
            train_h = hit_df.iloc[:split]
            test_h  = hit_df.iloc[split:]

        available_cols = [c for c in HITS_FEATURES if c in hit_df.columns]
        X_train_h = train_h[available_cols].fillna(0).values.astype(np.float32)
        y_train_h = train_h["actual_outcome"].values
        X_test_h  = test_h[available_cols].fillna(0).values.astype(np.float32) if len(test_h) else X_train_h[:0]
        y_test_h  = test_h["actual_outcome"].values if len(test_h) else y_train_h[:0]

        out_path  = os.path.join(OUTDIR, "xgb_hits.pkl")
        metrics   = _train_and_save(X_train_h, y_train_h, X_test_h, y_test_h,
                                    "Hits", out_path)
        all_metrics["hits"] = {**metrics, "train_rows": int(len(X_train_h)),
                               "features": available_cols}

    # ── SHAP importance for K 4.5 model ──────────────────────────────────────
    k45_path = os.path.join(OUTDIR, "xgb_k_4_5.pkl")
    if os.path.exists(k45_path) and not k_df.empty:
        try:
            import shap, pickle as _pickle
            with open(k45_path, "rb") as f:
                k45 = _pickle.load(f)
            base_model = k45.calibrated_classifiers_[0].estimator
            avail = [c for c in K_FEATURES if c in k_df.columns]
            X_shap = k_df[avail].fillna(0).values.astype(np.float32)
            idx    = np.random.choice(len(X_shap), min(2000, len(X_shap)), replace=False)
            exp    = shap.TreeExplainer(base_model)
            sv     = exp.shap_values(X_shap[idx])
            mean_s = np.abs(sv).mean(axis=0)
            ranked = sorted(zip(avail, mean_s), key=lambda x: x[1], reverse=True)
            logger.info("\n=== K4.5 SHAP Feature Importance ===")
            for feat, imp in ranked:
                bar = "█" * int(imp / ranked[0][1] * 20)
                logger.info("  %-22s %s %.4f", feat, bar, imp)
            all_metrics["shap_k_4_5"] = [{"feature": f, "importance": round(float(i), 4)}
                                          for f, i in ranked]
        except Exception as e:
            logger.warning("SHAP failed: %s", e)

    # ── Save metadata ────────────────────────────────────────────────────────
    feat_cols_out = {
        f"k_{line}": K_FEATURES for line in K_LINES
    }
    feat_cols_out["hits"] = HITS_FEATURES

    with open(os.path.join(OUTDIR, "xgb_feature_cols.json"), "w") as f:
        json.dump(feat_cols_out, f, indent=2)
    logger.info("\nSaved → models/xgb_feature_cols.json")

    with open(os.path.join(OUTDIR, "model_metrics.json"), "w") as f:
        json.dump(all_metrics, f, indent=2)
    logger.info("Saved → models/model_metrics.json")

    logger.info("\n✅ Training complete. Saved to %s", OUTDIR)
    logger.info("   Models auto-load on next Railway redeploy (xgb_k_layer.py).")


if __name__ == "__main__":
    main()
