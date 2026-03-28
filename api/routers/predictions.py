from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
from services.predictor import evaluate_edge
from services.statcast import get_recent_statcast_data
# FIX BUG 3: Import ABSContext so it can be accepted in PropRequest
from services.abs_challenge import ABSContext

router = APIRouter(prefix="/api/predict", tags=["Predictions"])


class FatigueContext(BaseModel):
    player_type: str = "batter"  # "batter" or "pitcher"
    days_rest: int = 4
    recent_pitches: int = 0
    is_starter: bool = True
    home_team: str = "UNK"
    away_team: str = "UNK"
    previous_city: str = "UNK"
    team_rest_days: int = 1


class VacuumContext(BaseModel):
    player_id: str
    standard_lineup: list = []
    confirmed_lineup: list = []
    usage_weights: dict = {}
    standard_lineup_spot: int = 9
    confirmed_lineup_spot: int = 9


class ContrastContext(BaseModel):
    pitcher_fb_rate: float = 0.38
    hitter_fb_rate: float = 0.38
    park_hr_factor: int = 100


# FIX BUG 3: Pydantic model wrapping ABSContext dataclass fields
class ABSContextRequest(BaseModel):
    batter_height_in: float = Field(
        72.0,
        description="Batter height in inches. Default 72 (6'0\" league avg).",
    )
    is_2026_season: bool = Field(
        True,
        description="Set False to disable ABS modelling for pre-2026 props.",
    )

    def to_abs_context(self) -> ABSContext:
        return ABSContext(
            batter_height_in=self.batter_height_in,
            is_2026_season=self.is_2026_season,
        )


class PropRequest(BaseModel):
    player_id: int
    prop_category: str = Field(
        ...,
        description=(
            "pitcher_strikeouts | batter_total_bases | "
            "batter_home_runs | batter_hits_runs_rbis"
        ),
    )
    line: float
    over_odds: int
    under_odds: int
    # Optional enrichment contexts
    fatigue_context: Optional[FatigueContext] = None
    vacuum_context: Optional[VacuumContext] = None
    contrast_context: Optional[ContrastContext] = None
    # FIX BUG 3: ABS context now accepted and forwarded to evaluate_edge
    abs_context: Optional[ABSContextRequest] = None
    # Optional: include recent statcast data window for live inference
    statcast_start_date: Optional[str] = None
    statcast_end_date: Optional[str] = None


class BatchPropRequest(BaseModel):
    props: list[PropRequest]


@router.post("/edge")
async def calculate_market_edge(request: PropRequest):
    """Calculate +EV edge for a single player prop."""
    try:
        statcast_data = []
        if request.statcast_start_date and request.statcast_end_date:
            statcast_data = get_recent_statcast_data(
                request.statcast_start_date, request.statcast_end_date
            )

        # FIX BUG 3: Convert Pydantic model → ABSContext dataclass
        abs_ctx = request.abs_context.to_abs_context() if request.abs_context else None

        result = evaluate_edge(
            sportsbook_line=request.line,
            over_odds=request.over_odds,
            under_odds=request.under_odds,
            statcast_data=statcast_data,
            fatigue_context=request.fatigue_context.model_dump() if request.fatigue_context else None,
            vacuum_context=request.vacuum_context.model_dump() if request.vacuum_context else None,
            contrast_context=request.contrast_context.model_dump() if request.contrast_context else None,
            prop_category=request.prop_category,
            abs_context=abs_ctx,
        )
        return {"player_id": request.player_id, **result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/batch")
async def calculate_batch_edge(request: BatchPropRequest):
    """Calculate +EV edge for multiple props in one call. Max 20 props."""
    if len(request.props) > 20:
        raise HTTPException(status_code=400, detail="Max 20 props per batch request.")

    results = []
    errors = []
    for prop in request.props:
        try:
            statcast_data = []
            if prop.statcast_start_date and prop.statcast_end_date:
                statcast_data = get_recent_statcast_data(
                    prop.statcast_start_date, prop.statcast_end_date
                )
            # FIX BUG 3: Forward abs_context in batch path too
            abs_ctx = prop.abs_context.to_abs_context() if prop.abs_context else None

            result = evaluate_edge(
                sportsbook_line=prop.line,
                over_odds=prop.over_odds,
                under_odds=prop.under_odds,
                statcast_data=statcast_data,
                fatigue_context=prop.fatigue_context.model_dump() if prop.fatigue_context else None,
                vacuum_context=prop.vacuum_context.model_dump() if prop.vacuum_context else None,
                contrast_context=prop.contrast_context.model_dump() if prop.contrast_context else None,
                prop_category=prop.prop_category,
                abs_context=abs_ctx,
            )
            results.append({"player_id": prop.player_id, **result})
        except Exception as e:
            errors.append({"player_id": prop.player_id, "error": str(e)})

    playable = [r for r in results if r.get("is_playable")]
    return {
        "total": len(request.props),
        "processed": len(results),
        "playable_count": len(playable),
        "errors": errors,
        "results": sorted(results, key=lambda x: x.get("edge_percentage", 0), reverse=True),
    }
