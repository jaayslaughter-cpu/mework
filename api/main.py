import time
from datetime import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import mlb_data, predictions

app = FastAPI(title="PropIQ Analytics Engine", version="1.0")

# Register routers
app.include_router(mlb_data.router)
app.include_router(predictions.router)

start_time = time.time()

# Configure CORS to allow the Streamlit dashboard and Node Hub to communicate
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8501", "http://localhost:3000", "http://localhost:3002"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"status": "ok", "service": "PropIQ ML Engine"}

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "uptime": int(time.time() - start_time),
        "timestamp": int(time.time() * 1000)
    }
