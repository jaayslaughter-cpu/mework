FROM python:3.11-slim

WORKDIR /app

# Install dependencies from root requirements files
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt || true

# Copy Python ML pipeline files
COPY ml_pipeline/ ./ml_pipeline/
COPY ml_engine/ ./ml_engine/
COPY api/ ./api/

# Copy supporting modules
COPY live_dispatcher.py .
COPY nightly_recap.py .
COPY monthly_leaderboard.py .

ENV PYTHONUNBUFFERED=1

EXPOSE 5000

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD python -c "import sys; sys.exit(0)"

CMD ["python", "live_dispatcher.py"]
