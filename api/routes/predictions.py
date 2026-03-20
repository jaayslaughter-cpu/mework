"""
PropIQ Analytics — Predictions Router (FastAPI)
================================================
Updated router wiring PropModelWithCalibration + OddsFetcher + MLBDataAggregator.
Replaces the old predictions.py that returned hardcoded 0.55.

Drop this into: api/routes/predictions.py
"""

from typing import List, Optional
from fastapi import APIRouter, HTTPException, BackgroundTasks, Query
from pydantic import BaseModel, Field

# Internal imports (adjust path if needed)
from api.services.prop_model import get_model, american_to_implied
from api.services.odds_fetcher import get_fetcher
from api.services.mlb_data import get_mlb_data
from api.services.error_logger import log_prediction_outcome, get_self_correction

router = APIRouter(prefix="/predictions", tags=["predictions"])


# ─────────────────────────────────────────────
# Request / Response models
# ─────────────────────────────────────────────
class PropPredictionRequest(BaseModel):
    player: str = Field(..., example="Aaron Judge")
    prop_type: str = Field(..., example="hits")
    line_value: float = Field(..., example=1.5)
    book_over_odds: int = Field(..., example=-115, description="American odds for Over")
    book_under_odds: int = Field(..., example=-105, description="American odds for Under")
    book_name: Optional[str] = Field(None, example="DraftKings")
    event_id: Optional[str] = Field(None, description="Odds API event ID for auto-fetching features")
    game_date: Optional[str] = Field(None, example="2026-03-26")
    # Optional feature overrides
    recent_avg: Optional[float] = None
    season_avg: Optional[float] = None
    park_factor: Optional[float] = None
    pitcher_era: Optional[float] = None
    is_home: Optional[bool] = None
    fatigue_score: Optional[float] = Field(None, description="0-1, 0=fresh, 1=fatigued")
    usage_vacuum: Optional[float] = Field(None, description="Expected role expansion (0-1)")
    defensive_contrast: Optional[float] = Field(None, description="Defense quality factor (0-1)")


class PropPredictionResponse(BaseModel):
    player: str
    prop_type: str
    line_value: float
    book_prob: float
    raw_prob: float
    calibrated_prob: float
    edge: float
    recommendation: str
    confidence_tier: str
    prediction_id: Optional[int]
    game_date: Optional[str]
    kelly_fraction: Optional[float]
    half_kelly: Optional[float]


class BatchPredictionRequest(BaseModel):
    props: List[PropPredictionRequest]
    auto_fetch_features: bool = False


class ResultFeedbackRequest(BaseModel):
    prediction_id: int
    player: str
    prop_type: str
    book_prob: float
    actual_result: float = Field(..., ge=0.0, le=1.0, description="1.0 = hit, 0.0 = miss")
    model_prob: Optional[float] = None
    edge: Optional[float] = None
    game_date: Optional[str] = None
    dfs_points: Optional[float] = None


# ─────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────
@router.post("/predict", response_model=PropPredictionResponse)
async def predict_prop(req: PropPredictionRequest):
    """
    Predict a single player prop with full calibration pipeline.
    Automatically builds features from MLB data if event_id is provided.
    """
    model = get_model()
    mlb = get_mlb_data()

    # Convert American odds to implied probability
    over_raw = american_to_implied(req.book_over_odds)
    under_raw = american_to_implied(req.book_under_odds)
    total = over_raw + under_raw
    book_prob = over_raw / total  # vig-removed

    # Build features — auto-enrich from MLB data if possible
    features = {}
    if req.event_id:
        try:
            player_features = mlb.build_player_features(req.player, req.game_date)
            features.update(player_features)
        except Exception as e:
            pass  # gracefully degrade

    # Apply request overrides
    feature_overrides = {
        "recent_avg": req.recent_avg,
        "season_avg": req.season_avg,
        "park_factor": req.park_factor,
        "pitcher_era": req.pitcher_era,
        "is_home": req.is_home,
        "fatigue_score": req.fatigue_score,
        "usage_vacuum": req.usage_vacuum,
        "defensive_contrast": req.defensive_contrast,
    }
    features.update({k: v for k, v in feature_overrides.items() if v is not None})

    # Predict
    result = model.predict(
        player=req.player,
        prop_type=req.prop_type,
        features=features,
        book_prob=book_prob,
        line_value=req.line_value,
        game_date=req.game_date,
        book_name=req.book_name,
    )

    # Kelly sizing
    from api.services.dfs_tracker import PropBacktester
    bt = PropBacktester()
    kelly = bt.kelly_criterion(result["calibrated_prob"], book_prob)
    half_k = bt.half_kelly(result["calibrated_prob"], book_prob)

    return PropPredictionResponse(
        **{k: v for k, v in result.items() if k in PropPredictionResponse.model_fields},
        kelly_fraction=kelly,
        half_kelly=half_k,
    )


@router.post("/predict/batch", response_model=List[PropPredictionResponse])
async def predict_batch(req: BatchPredictionRequest):
    """Predict multiple props at once."""
    results = []
    for prop in req.props:
        try:
            result = await predict_prop(prop)
            results.append(result)
        except Exception as e:
            results.append(PropPredictionResponse(
                player=prop.player,
                prop_type=prop.prop_type,
                line_value=prop.line_value,
                book_prob=0,
                raw_prob=0,
                calibrated_prob=0,
                edge=0,
                recommendation="ERROR",
                confidence_tier="X",
                prediction_id=None,
                game_date=prop.game_date,
                kelly_fraction=None,
                half_kelly=None,
            ))
    return results


@router.get("/live")
async def get_live_props(
    markets: Optional[str] = Query(None, description="Comma-separated markets, e.g. batter_hits,pitcher_strikeouts"),
    max_events: int = Query(15, le=30),
):
    """
    Fetch live MLB props from The Odds API and run predictions on all of them.
    """
    fetcher = get_fetcher()
    model = get_model()

    # Get events
    events = fetcher.get_mlb_events()
    if not events:
        raise HTTPException(status_code=503, detail="Could not fetch MLB events")

    market_list = markets.split(",") if markets else ["batter_hits", "batter_strikeouts", "batter_home_runs"]

    all_predictions = []
    for event in events[:max_events]:
        event_id = event["id"]
        props = fetcher.get_player_props(event_id, market_list)

        # Group into Over lines only
        seen = set()
        for prop in props:
            if prop["side"] != "Over":
                continue
            key = (prop["player"], prop["market"])
            if key in seen:
                continue
            seen.add(key)

            try:
                result = model.predict(
                    player=prop["player"],
                    prop_type=prop["market"],
                    features={},
                    book_prob=prop["implied_prob"],
                    line_value=prop["line"],
                    book_name=prop["book"],
                    persist=False,
                )
                result["event_id"] = event_id
                result["home_team"] = event.get("home_team")
                result["away_team"] = event.get("away_team")
                all_predictions.append(result)
            except Exception:
                continue

    # Sort by edge descending
    all_predictions.sort(key=lambda x: x.get("edge", 0), reverse=True)
    return {"count": len(all_predictions), "predictions": all_predictions}


@router.post("/result")
async def record_result(req: ResultFeedbackRequest, background_tasks: BackgroundTasks):
    """
    Record actual game result to update model calibration.
    Call this after games complete.
    """
    model = get_model()
    model.record_result(
        prediction_id=req.prediction_id,
        actual_result=req.actual_result,
        player=req.player,
        prop_type=req.prop_type,
        book_prob=req.book_prob,
        dfs_points=req.dfs_points,
    )

    # Log to error/prediction logger
    if req.model_prob is not None:
        background_tasks.add_task(
            log_prediction_outcome,
            player=req.player,
            prop_type=req.prop_type,
            model_prob=req.model_prob,
            book_prob=req.book_prob,
            actual_result=req.actual_result,
            edge=req.edge or 0,
            game_date=req.game_date or "",
        )

    return {"status": "recorded", "prediction_id": req.prediction_id}


@router.get("/accuracy")
async def get_accuracy_report():
    """Model accuracy + active calibration corrections."""
    model = get_model()
    return model.get_accuracy_report()


@router.get("/health")
async def model_health():
    """Quick model health check — accuracy, MAE, active corrections."""
    sc = get_self_correction()
    return sc.get_model_health()


@router.post("/calibration/refresh")
async def refresh_calibration(background_tasks: BackgroundTasks):
    """
    Manually trigger calibration refresh — re-detects error patterns.
    Normally triggered automatically every 50 results.
    """
    model = get_model()
    background_tasks.add_task(model.calibration.refresh_corrections)
    return {"status": "refreshing"}


@router.get("/api-status")
async def api_status():
    """Check Odds API quota remaining."""
    fetcher = get_fetcher()
    return fetcher.get_api_status()
