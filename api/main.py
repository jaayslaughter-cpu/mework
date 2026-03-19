from fastapi import FastAPI

app = FastAPI(title="PropIQ API")

@app.get("/health")
def health():
    return {"status": "ok"}
