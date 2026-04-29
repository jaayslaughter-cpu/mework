import os
import time
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import mlb_data, predictions

app = FastAPI(title="PropIQ Analytics Engine", version="1.0")

# CORS: reads FRONTEND_URL from env (comma-separated for multiple origins).
# Falls back to localhost for local dev. Set FRONTEND_URL in Railway to your
# deployed frontend URL (e.g. https://mework.up.railway.app).
_cors_env = os.getenv("FRONTEND_URL", "")
_allowed_origins: list[str] = (
    [o.strip() for o in _cors_env.split(",") if o.strip()]
    if _cors_env
    else ["http://localhost:3000", "http://localhost:3002"]
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

start_time = time.time()

# Register routers (after middleware)
app.include_router(mlb_data.router)
app.include_router(predictions.router)


@app.get("/")
async def root():
    return {"status": "ok", "service": "PropIQ ML Engine"}


@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "uptime": int(time.time() - start_time),
        "timestamp": int(time.time() * 1000),
    }
