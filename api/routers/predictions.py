from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from services.predictor import evaluate_edge

router = APIRouter(prefix="/api/predict", tags=["Predictions"])

class PropRequest(BaseModel):
    player_id: int
    prop_category: str
    line: float
    over_odds: int
    under_odds: int

@router.post("/edge")
async def calculate_market_edge(request: PropRequest):
    try:
        # In the next phase, we will dynamically fetch the statcast_data here
        mock_statcast_features = [] 
        
        result = evaluate_edge(
            sportsbook_line=request.line,
            over_odds=request.over_odds,
            under_odds=request.under_odds,
            statcast_data=mock_statcast_features
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
