FROM python:3.11-slim

WORKDIR /app

# Install ML dependencies
COPY propiq_modules/requirements-ml.txt .
RUN pip install --no-cache-dir -r requirements-ml.txt

# Copy the ML service files
COPY propiq_modules/ml_service/ ./ml_service/
COPY propiq_modules/agents/ ./agents/
COPY propiq_modules/analytics/ ./analytics/

ENV PYTHONUNBUFFERED=1

EXPOSE 5000

CMD ["python", "-m", "ml_service.app"]
