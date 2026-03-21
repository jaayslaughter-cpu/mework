"""
BetAnalyzerController — FastAPI REST endpoints
POST /analyze/bet          → single leg EV analysis
POST /analyze/parlay       → multi-leg parlay analysis
GET  /analyze/live         → real-time market movement
GET  /analyze/leaderboard  → agent consensus trends
GET  /analyze/result/{id}  → fetch async result from Redis
"""

import os
import json
import redis
import logging
import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional

from tasklets.bet_analyzer_tasklet import (
    BetAnalyzerTasklet,
    submit_bet_for_analysis,
    is_spring_training,
    OPENING_DAY,
    SPRING_TRAINING_WEIGHT,
)

logger = logging.getLogger(__name__)

app = FastAPI(title="PropIQ Bet Analyzer API", version="2.0")

_redis = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True,
)

_tasklet = BetAnalyzerTasklet()


# ── request / response models ──────────────────────────────────────────────
class SingleBetRequest(BaseModel):
    players: list[str]
    props: list[str]
    odds: dict = {}
    timestamp: Optional[str] = None
    async_mode: bool = False


class ParlayBetRequest(BaseModel):
    players: list[str]
    props: list[str]
    odds: dict = {}
    parlay_odds: str = "+300"
    timestamp: Optional[str] = None


# ── POST /analyze/bet ──────────────────────────────────────────────────────
@app.post("/analyze/bet")
def analyze_bet(req: SingleBetRequest):
    """
    Synchronous single-leg EV analysis.
    Example: Judge O1.5H at DK +120  GREEN 7.2% EV
    """
    if not req.players or not req.props:
        raise HTTPException(400, "players and props required")

    payload = {
        "players": req.players[:1],
        "props":   req.props[:1],
        "odds":    req.odds,
        "parlay":  False,
    }

    if req.async_mode:
        rid = submit_bet_for_analysis(payload)
        return {"request_id": rid, "status": "queued", "poll": f"/analyze/result/{rid}"}

    # synchronous path
    result = _tasklet.analyze(payload)
    result["request_id"] = "sync"
    return result


# ── POST /analyze/parlay ───────────────────────────────────────────────────
@app.post("/analyze/parlay")
def analyze_parlay(req: ParlayBetRequest):
    """
    Multi-leg parlay EV analysis.
    Example: Judge O1.5H + Devers O0.5HR combined.
    """
    if len(req.players) != len(req.props):
        raise HTTPException(400, "players and props must match in length")
    if not 2 <= len(req.players) <= 4:
        raise HTTPException(400, "parlay must have 2-4 legs")

    payload = {
        "players":     req.players,
        "props":       req.props,
        "odds":        req.odds,
        "parlay":      True,
        "parlay_odds": req.parlay_odds,
    }
    result = _tasklet.analyze(payload)
    result["request_id"] = "sync"
    return result


# ── GET /analyze/live ──────────────────────────────────────────────────────
@app.get("/analyze/live")
def live_market_movement():
    """
    Real-time line movement from Redis mlb_hub.
    Returns props with >2% movement in last 5 minutes.
    """
    hub_raw = _redis.get("mlb_hub")
    if not hub_raw:
        return {"movements": [], "note": "Hub data loading — check back in 15s"}

    hub = json.loads(hub_raw)
    movements = hub.get("line_movements", [])

    # filter to significant moves
    significant = [m for m in movements if abs(m.get("pct_change", 0)) >= 2.0]
    significant.sort(key=lambda x: abs(x.get("pct_change", 0)), reverse=True)

    spring_note = ""
    if is_spring_training():
        days_left = (OPENING_DAY - datetime.date.today()).days
        spring_note = f"Spring Training — {days_left} days until Opening Day (2026-03-26). Records 0-0."

    return {
        "movements": significant[:20],
        "spring_training": is_spring_training(),
        "note": spring_note,
        "updated_at": hub.get("updated_at", ""),
    }


# ── GET /analyze/leaderboard ───────────────────────────────────────────────
@app.get("/analyze/leaderboard")
def agent_consensus_leaderboard():
    """
    Which agents are agreeing most → consensus signals.
    """
    leaderboard_raw = _redis.get("leaderboard")
    if not leaderboard_raw:
        return {"agents": [], "note": "Leaderboard populating..."}

    leaderboard = json.loads(leaderboard_raw)

    # add consensus signal per agent
    for agent in leaderboard.get("agents", []):
        queue_raw = _redis.lrange("bet_queue", 0, 100)
        agent_bets = [b for b in queue_raw if agent["name"].lower() in b.lower()]
        agent["pending_bets"] = len(agent_bets)

    return leaderboard


# ── GET /analyze/result/{id} ───────────────────────────────────────────────
@app.get("/analyze/result/{request_id}")
def get_result(request_id: str):
    """Fetch async analysis result from Redis cache."""
    key = f"bet_analyzer_cache:{request_id}"
    raw = _redis.get(key)
    if not raw:
        return {"status": "pending", "request_id": request_id}
    return json.loads(raw)


# ── GET /analyze/spring-training ──────────────────────────────────────────
@app.get("/analyze/spring-training")
def spring_training_status():
    """
    Current spring training data. All records 0-0 until first game.
    Stats weighted at 30% until Opening Day.
    """
    days_left = max(0, (OPENING_DAY - datetime.date.today()).days)
    return {
        "mode": "spring_training" if is_spring_training() else "regular_season",
        "opening_day": str(OPENING_DAY),
        "days_until_opening": days_left,
        "stat_weight": SPRING_TRAINING_WEIGHT if is_spring_training() else 1.0,
        "all_records": "0-0",
        "note": (
            f"All team/player records reset to 0-0. "
            f"Spring Training stats carry {int(SPRING_TRAINING_WEIGHT*100)}% weight. "
            f"Full weight activates {OPENING_DAY}."
        ) if is_spring_training() else "Regular season active — full stat weight",
    }


# ── health ─────────────────────────────────────────────────────────────────
@app.get("/analyze/health")
def health():
    return {"status": "ok", "tasklet": "BetAnalyzerTasklet", "cycle_ms": 5000}
