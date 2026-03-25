FROM python:3.12-slim

WORKDIR /app

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        git \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
# requirements_army.txt is the production superset
COPY requirements_army.txt requirements.txt ./
RUN pip install --no-cache-dir -r requirements_army.txt

# Copy application source
COPY . .

# Create runtime directories
RUN mkdir -p /app/data /app/models /app/logs

# Run as non-root for security
RUN useradd -m -r propiq && chown -R propiq:propiq /app
USER propiq

# FastAPI port
EXPOSE 8080

# railway.toml startCommand overrides this in production.
# Local docker run fallback uses api_server (ML inference layer).
CMD ["uvicorn", "api_server:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]
