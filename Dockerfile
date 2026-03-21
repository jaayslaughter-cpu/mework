FROM python:3.12-slim

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential=12.8 \
    curl=7.74.0-1.3+deb11u4 \
    git=1:2.30.2-1+deb11u1 \
    && rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements_army.txt .
RUN pip install --no-cache-dir -r requirements_army.txt

# Copy app
COPY . .

# Data directories
RUN mkdir -p /app/data /app/models /app/logs

# Non-root user
RUN useradd -m -r propiq && chown -R propiq:propiq /app
USER propiq

EXPOSE 8080

CMD ["python", "-m", "uvicorn", "orchestrator:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]
