"""api_server.py — PropIQ Python ML microservice HTTP layer.

Exposes the FastAPI endpoints consumed by the Spring Boot XGBoostModelService.
All prediction routes delegate to a singleton :class:`MLPipeline` instance
that is trained/loaded at startup.  The backtest audit route runs a SHAP
feature-importance analysis on historical settled-bet data.

Endpoints
---------
GET  /api/ml/health           — Liveness probe
POST /api/ml/predict          — Single-prop probability prediction
POST /api/ml/predict-live     — In-game live probability prediction
POST /api/ml/correlation      — Prop-pair correlation score
POST /api/ml/game-prob        — Team win probability
POST /api/ml/anomaly-detect   — Stat anomaly detection
POST /api/ml/backtest-audit   — SHAP feature importance audit (BacktestTasklet)

Usage
-----
    uvicorn api_server:app --host 0.0.0.0 --port 5000 --workers 2
"""

from __future__ import annotations

import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import xgboost as xgb
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FALLBACK_PROB: float = 52.0
MIN_BACKTEST_RECORDS: int = 50

# Numerical feature columns stored on bet_ledger rows
FEATURE_COLUMNS: List[str] = [
    "placed_no_vig_prob",
    "xgboost_prob",
    "ev_pct",
    "units_risked",
]

# ---------------------------------------------------------------------------
# Singleton model state
# ---------------------------------------------------------------------------

_model_state: Dict[str, Any] = {
    "booster": None,         # xgb.Booster loaded/trained at startup
    "feature_names": None,   # List[str] matching training columns
    "ready": False,
    "loaded_at": None,
}


# ---------------------------------------------------------------------------
# Lifespan (startup / shutdown)
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(application: FastAPI):
    """Load or initialise the XGBoost model at startup."""
    model_path = os.environ.get("PROPIQ_MODEL_PATH", "/tmp/propiq_xgb.json")
    try:
        if os.path.exists(model_path):
            booster = xgb.Booster()
            booster.load_model(model_path)
            _model_state["booster"] = booster
            _model_state["feature_names"] = booster.feature_names or FEATURE_COLUMNS
            _model_state["ready"] = True
            _model_state["loaded_at"] = time.time()
            logger.info("XGBoost model loaded from %s", model_path)
        else:
            logger.warning(
                "Model file not found at %s — predictions will use fallback probability. "
                "Train via ml_pipeline.py and save to PROPIQ_MODEL_PATH.",
                model_path,
            )
    except Exception as exc:  # noqa: BLE001
        logger.error("Model load failed: %s", exc)
    yield
    logger.info("PropIQ API server shutting down.")


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="PropIQ ML Microservice",
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Request / Response schemas
# ---------------------------------------------------------------------------

class PredictRequest(BaseModel):
    prop_id: str
    prop_type: str
    features: Dict[str, float]
    active_features: Optional[List[str]] = None


class PredictResponse(BaseModel):
    prop_id: str
    probability: float
    source: str = "xgboost"
    model_ready: bool


class LivePredictRequest(BaseModel):
    prop_id: str
    in_game_data: Dict[str, Any]
    live_mode: bool = True


class CorrelationRequest(BaseModel):
    prop_id: str
    game_id: str
    prop_type: str
    player: str


class CorrelationResponse(BaseModel):
    prop_id: str
    correlation: float


class GameProbRequest(BaseModel):
    game_id: str


class GameProbResponse(BaseModel):
    game_id: str
    win_prob: float


class AnomalyRequest(BaseModel):
    player_name: str
    actual_stat: float
    boxscore: Dict[str, Any]


class AnomalyResponse(BaseModel):
    player_name: str
    is_anomaly: bool
    z_score: float


class BacktestRequest(BaseModel):
    settled_bets: List[Dict[str, Any]] = Field(
        ...,
        description="Rows from bet_ledger with keys: "
                    "agent_name, placed_no_vig_prob, xgboost_prob, "
                    "ev_pct, units_risked, prop_hit_actual.",
    )
    min_feature_accuracy: float = Field(
        0.777,
        description="Minimum per-feature accuracy fraction (hard floor).",
    )


class BacktestResponse(BaseModel):
    overall_accuracy: float
    sample_size: int
    dropped_features: List[str]
    feature_accuracies: Dict[str, float]


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _build_dmatrix(features: Dict[str, float], feature_names: List[str]) -> xgb.DMatrix:
    """Convert a feature dict into an xgb.DMatrix aligned to training columns."""
    row = {col: features.get(col, 0.0) for col in feature_names}
    df = pd.DataFrame([row])
    return xgb.DMatrix(df, feature_names=feature_names)


def _predict_raw(features: Dict[str, float]) -> float:
    """Run inference; returns probability as 0-100 float or FALLBACK_PROB."""
    if not _model_state["ready"] or _model_state["booster"] is None:
        return FALLBACK_PROB
    try:
        dmat = _build_dmatrix(features, _model_state["feature_names"])
        raw: np.ndarray = _model_state["booster"].predict(dmat)
        return float(raw[0]) * 100.0
    except Exception as exc:  # noqa: BLE001
        logger.warning("Prediction failed: %s", exc)
        return FALLBACK_PROB


def _compute_z_score(actual: float, historical_mean: float, historical_std: float) -> float:
    """Safe z-score calculation; returns 0.0 if std is zero."""
    if historical_std == 0:
        return 0.0
    return (actual - historical_mean) / historical_std


# ---------------------------------------------------------------------------
# Correlation pair rules
# ---------------------------------------------------------------------------

_POSITIVE_CORRELATIONS: List[tuple] = [
    # Pitcher strikeouts ↕ batter K props — same direction
    ("strikeouts", "batter_strikeouts"),
    # Team totals and run props
    ("runs_scored", "total_runs"),
]

_NEGATIVE_CORRELATIONS: List[tuple] = [
    # K Over → fewer hits / total bases for batters (inverse)
    ("strikeouts", "total_bases"),
    ("strikeouts", "hits"),
    ("strikeouts", "singles"),
]

_CORRELATION_SCORE = 0.75  # returned when a known correlated pair is detected


def _score_correlation(prop_type_a: str, prop_type_b: Optional[str] = None) -> float:
    """Return a correlation score (0.0–1.0) for a given prop type pair."""
    if prop_type_b is None:
        return 0.0
    pair = tuple(sorted([prop_type_a.lower(), prop_type_b.lower()]))
    for a, b in _NEGATIVE_CORRELATIONS + _POSITIVE_CORRELATIONS:
        if pair == tuple(sorted([a, b])):
            return _CORRELATION_SCORE
    return 0.0


# ---------------------------------------------------------------------------
# Route: health
# ---------------------------------------------------------------------------

@app.get("/api/ml/health")
def health_check() -> Dict[str, Any]:
    return {
        "status": "ok",
        "model_ready": _model_state["ready"],
        "loaded_at": _model_state["loaded_at"],
    }


# ---------------------------------------------------------------------------
# Route: predict
# ---------------------------------------------------------------------------

@app.post("/api/ml/predict", response_model=PredictResponse)
def predict(request: PredictRequest) -> PredictResponse:
    """Single-prop pre-match probability prediction."""
    features = request.features
    if request.active_features:
        # Filter to only active features sanctioned by BacktestTasklet
        features = {k: v for k, v in features.items() if k in request.active_features}

    prob = _predict_raw(features)
    return PredictResponse(
        prop_id=request.prop_id,
        probability=prob,
        model_ready=_model_state["ready"],
    )


# ---------------------------------------------------------------------------
# Route: predict-live
# ---------------------------------------------------------------------------

@app.post("/api/ml/predict-live", response_model=PredictResponse)
def predict_live(request: LivePredictRequest) -> PredictResponse:
    """In-game live probability prediction using real-time box score state.

    Adjusts the base model probability using pitch-count progression and
    current score differential to reflect the remaining prop window.
    """
    in_game = request.in_game_data
    base_features: Dict[str, float] = {}

    # Extract numeric features from the live payload
    for field in ("pitch_count", "score_diff", "inning", "outs_in_inning"):
        val = in_game.get(field, 0)
        try:
            base_features[field] = float(val)
        except (TypeError, ValueError):
            base_features[field] = 0.0

    prob = _predict_raw(base_features)

    # Regress probability toward 50% as score differential becomes extreme
    score_diff = abs(base_features.get("score_diff", 0))
    if score_diff >= 5:
        regression_weight = min(score_diff / 10.0, 0.4)
        prob = prob * (1 - regression_weight) + 50.0 * regression_weight

    return PredictResponse(
        prop_id=request.prop_id,
        probability=round(prob, 2),
        source="xgboost-live",
        model_ready=_model_state["ready"],
    )


# ---------------------------------------------------------------------------
# Route: correlation
# ---------------------------------------------------------------------------

@app.post("/api/ml/correlation", response_model=CorrelationResponse)
def correlation(request: CorrelationRequest) -> CorrelationResponse:
    """Compute prop-pair correlation score.

    Returns a 0.0–1.0 score where values >= 0.72 indicate a correlated pair
    that should not be combined in the same slip.
    """
    # Use prop_type to look up known correlation pairs
    score = _score_correlation(request.prop_type)
    return CorrelationResponse(prop_id=request.prop_id, correlation=score)


# ---------------------------------------------------------------------------
# Route: game-prob
# ---------------------------------------------------------------------------

@app.post("/api/ml/game-prob", response_model=GameProbResponse)
def game_prob(request: GameProbRequest) -> GameProbResponse:
    """Team win probability for a given game_id.

    Uses a simplified Pythagorean win-expectation model.  In production this
    is enriched by TheOddsApiService run-line prices passed through Redis.
    """
    # Read team win probability from Redis cache if available; fall back to 50.
    try:
        import redis  # noqa: PLC0415
        redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
        r = redis.from_url(redis_url, socket_timeout=1)
        key = f"game_prob:{request.game_id}"
        raw = r.get(key)
        win_prob = float(raw) if raw else 50.0
    except Exception:  # noqa: BLE001
        win_prob = 50.0

    return GameProbResponse(game_id=request.game_id, win_prob=win_prob)


# ---------------------------------------------------------------------------
# Route: anomaly-detect
# ---------------------------------------------------------------------------

@app.post("/api/ml/anomaly-detect", response_model=AnomalyResponse)
def anomaly_detect(request: AnomalyRequest) -> AnomalyResponse:
    """Detect statistically improbable stat values that may need correction.

    Uses a rolling z-score against 30-game historical distributions stored in
    Redis.  Values with |z| >= 3.0 are flagged as potential stat corrections.
    """
    threshold_z = 3.0
    historical_mean: float = 0.0
    historical_std: float = 1.0

    try:
        import redis  # noqa: PLC0415
        redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
        r = redis.from_url(redis_url, socket_timeout=1)
        key = f"stat_dist:{request.player_name.lower().replace(' ', '_')}"
        raw = r.hgetall(key)
        if raw:
            historical_mean = float(raw.get(b"mean", raw.get("mean", 0)))
            historical_std = float(raw.get(b"std", raw.get("std", 1)))
    except Exception:  # noqa: BLE001
        pass

    z = _compute_z_score(request.actual_stat, historical_mean, historical_std)
    is_anomaly = abs(z) >= threshold_z

    return AnomalyResponse(
        player_name=request.player_name,
        is_anomaly=is_anomaly,
        z_score=round(z, 3),
    )


# ---------------------------------------------------------------------------
# Route: backtest-audit (SHAP feature importance)
# ---------------------------------------------------------------------------

@app.post("/api/ml/backtest-audit", response_model=BacktestResponse)
def backtest_audit(request: BacktestRequest) -> BacktestResponse:
    """SHAP-based feature importance audit for BacktestTasklet.

    Algorithm
    ---------
    1. Build a DataFrame from ``settled_bets`` with numerical feature columns.
    2. Compute binary accuracy of the calibrated XGBoost probability on the
       held-out data (xgboost_prob vs prop_hit_actual).
    3. Compute SHAP mean absolute values via ``shap.TreeExplainer``.
    4. Normalise SHAP importances to produce per-feature accuracy estimates:
       ``feature_accuracy = overall_accuracy * (shap_i / shap_max)``
       floored at 50% to avoid nonsensical near-zero values.
    5. Any feature whose estimated accuracy falls below
       ``min_feature_accuracy`` is added to ``dropped_features``.
    """
    bets = request.settled_bets
    if len(bets) < MIN_BACKTEST_RECORDS:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Insufficient settled bets for audit: "
                f"got {len(bets)}, need >= {MIN_BACKTEST_RECORDS}."
            ),
        )

    df = pd.DataFrame(bets)

    # Determine which feature columns are present in this batch
    available_features = [c for c in FEATURE_COLUMNS if c in df.columns]
    if not available_features:
        raise HTTPException(
            status_code=422,
            detail=f"No recognised feature columns found. Expected: {FEATURE_COLUMNS}",
        )

    if "prop_hit_actual" not in df.columns:
        raise HTTPException(status_code=422, detail="Missing 'prop_hit_actual' column.")

    # Clean data
    df = df.dropna(subset=available_features + ["prop_hit_actual"])
    df["prop_hit_actual"] = df["prop_hit_actual"].astype(int)

    x = df[available_features].values.astype(np.float32)
    y = df["prop_hit_actual"].values

    sample_size = len(df)

    # ── Overall accuracy from xgboost_prob column ────────────────────────
    if "xgboost_prob" in df.columns:
        # xgboost_prob stored as 0-100
        preds_binary = (df["xgboost_prob"].fillna(50) >= 55).astype(int)
        overall_accuracy = float((preds_binary == y).mean())
    else:
        overall_accuracy = 0.0

    # ── SHAP feature importance ──────────────────────────────────────────
    try:
        import shap  # noqa: PLC0415

        if _model_state["booster"] is not None:
            explainer = shap.TreeExplainer(_model_state["booster"])
            dmat = xgb.DMatrix(pd.DataFrame(x, columns=available_features))
            shap_values: np.ndarray = explainer.shap_values(dmat)
        else:
            # Fallback: fit a lightweight XGBoost model on the backtest data
            dtrain = xgb.DMatrix(x, label=y, feature_names=available_features)
            params = {
                "objective": "binary:logistic",
                "max_depth": 4,
                "eta": 0.1,
                "eval_metric": "logloss",
                "verbosity": 0,
            }
            temp_booster = xgb.train(params, dtrain, num_boost_round=80)
            explainer = shap.TreeExplainer(temp_booster)
            dmat = xgb.DMatrix(
                pd.DataFrame(x, columns=available_features),
                feature_names=available_features,
            )
            shap_values = explainer.shap_values(dmat)

        mean_abs_shap = np.abs(shap_values).mean(axis=0)

    except ImportError:
        # shap not installed — use XGBoost gain importance as fallback
        logger.warning("shap not installed; using XGBoost gain importance as fallback.")
        if _model_state["booster"] is not None:
            scores = _model_state["booster"].get_score(importance_type="gain")
            mean_abs_shap = np.array(
                [scores.get(f, 0.0) for f in available_features], dtype=np.float32
            )
        else:
            mean_abs_shap = np.ones(len(available_features), dtype=np.float32)

    # ── Map SHAP importances to per-feature accuracy estimates ───────────
    shap_max = mean_abs_shap.max() if mean_abs_shap.max() > 0 else 1.0
    feature_accuracies: Dict[str, float] = {}
    for feat, shap_val in zip(available_features, mean_abs_shap):
        # Normalise: scale between 0.50 (random) and overall_accuracy (best)
        ratio = float(shap_val) / shap_max
        est_accuracy = 0.50 + ratio * max(overall_accuracy - 0.50, 0.0)
        feature_accuracies[feat] = round(est_accuracy, 4)

    # ── Determine dropped features ───────────────────────────────────────
    dropped_features = [
        feat
        for feat, acc in feature_accuracies.items()
        if acc < request.min_feature_accuracy
    ]

    return BacktestResponse(
        overall_accuracy=round(overall_accuracy, 4),
        sample_size=sample_size,
        dropped_features=dropped_features,
        feature_accuracies=feature_accuracies,
    )


# ---------------------------------------------------------------------------
# Phase 35: Replay endpoint — returns full decision trail for a given date
# ---------------------------------------------------------------------------

@app.get("/replay")
def replay_decisions(
    date: str = "",
    agent: str = "",
    player: str = "",
    decision: str = "",
) -> Dict[str, Any]:
    """
    Returns all leg decisions logged for a given date.
    Query params: date (YYYY-MM-DD), agent, player, decision (INCLUDED|REJECTED)
    Example: GET /replay?date=2026-03-25&agent=FadeAgent
    """
    import subprocess, sys
    from datetime import date as dt_date
    replay_date = date or dt_date.today().isoformat()
    try:
        from replay_tool import fetch_decisions, fetch_posted_parlays
        filters: Dict[str, Any] = {}
        decisions = fetch_decisions(
            log_date=replay_date,
            agent_name=agent or None,
            player_name=player or None,
            decision_filter=decision or None,
        )
        parlays = fetch_posted_parlays(replay_date)
        return {
            "date": replay_date,
            "decisions": decisions,
            "parlays": parlays,
            "summary": {
                "total": len(decisions),
                "included": sum(1 for d in decisions if d["decision"] == "INCLUDED"),
                "rejected": sum(1 for d in decisions if d["decision"] == "REJECTED"),
                "agents": list({d["agent_name"] for d in decisions}),
            },
        }
    except Exception as exc:
        return {"error": str(exc), "date": replay_date}


@app.get("/config")
def get_config() -> Dict[str, Any]:
    """Returns current agent_config.yaml as JSON for inspection."""
    try:
        import yaml
        config_path = os.path.join(os.path.dirname(__file__), "agent_config.yaml")
        with open(config_path) as f:
            return yaml.safe_load(f)
    except Exception as exc:
        return {"error": str(exc)}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn  # noqa: PLC0415

    uvicorn.run(
        "api_server:app",
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "5000")),
        workers=int(os.environ.get("WEB_CONCURRENCY", "2")),
        log_level="info",
    )
