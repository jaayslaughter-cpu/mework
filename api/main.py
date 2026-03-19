import time
from datetime import datetime
from fastapi import FastAPI

app = FastAPI(title="PropIQ API")

start_time = time.time()

@app.get("/health")
def health():
    return {
        "status": "ok",
        "uptime": int(time.time() - start_time),
        "timestamp": datetime.utcnow().isoformat()
    }
