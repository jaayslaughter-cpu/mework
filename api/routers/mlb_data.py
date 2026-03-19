from fastapi import APIRouter, HTTPException
from services.statcast import get_player_id, get_recent_statcast_data

router = APIRouter(prefix="/api/mlb", tags=["MLB Data"])

@router.get("/player")
async def lookup_player(first_name: str, last_name: str):
    player_id = get_player_id(first_name, last_name)
    if not player_id:
        raise HTTPException(status_code=404, detail="Player not found in MLB registry.")
    return {"first_name": first_name, "last_name": last_name, "mlbam_id": player_id}

@router.get("/statcast")
async def fetch_statcast(start_date: str, end_date: str):
    # Note: Statcast queries can be slow. Keep date ranges small (1-3 days).
    data = get_recent_statcast_data(start_date, end_date)
    return {"start_date": start_date, "end_date": end_date, "records_returned": len(data), "data": data}
