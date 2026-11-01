# syntax=docker/dockerfile:1
FROM python:3.11-slim AS runtime

# Set production environment defaults
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    API_HOST=0.0.0.0 \
    API_PORT=8000 \
    CHECKPOINT_DB_PATH=/app/data/orchestrator_checkpoints.db \
    CHECKPOINT_ENABLED=true

# Install minimal OS dependencies: curl for health probes, procps for signals
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Create unprivileged non-root system user and group for container security
RUN groupadd -r -g 10001 orchestrator && \
    useradd -r -u 10001 -g orchestrator -d /app -s /sbin/nologin orchestrator

# Establish application directory and persistent SQLite data directory
WORKDIR /app
RUN mkdir -p /app/data && chown -R orchestrator:orchestrator /app

# Copy dependency specifications first to leverage Docker layer caching
COPY --chown=orchestrator:orchestrator pyproject.toml requirements.txt ./

# Install pinned runtime dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
COPY --chown=orchestrator:orchestrator README.md ./
COPY --chown=orchestrator:orchestrator src/ ./src/

# Install application package in editable mode without reinstalling dependencies
RUN pip install --no-cache-dir --no-deps -e .

# Declare persistent volume mount point for SQLite database and WAL files
VOLUME ["/app/data"]

# Switch to unprivileged non-root user
USER orchestrator

# Expose API port
EXPOSE 8000

# Default command launches the API server bound to 0.0.0.0:8000
CMD ["python", "-m", "orchestrator", "--serve", "--host", "0.0.0.0", "--port", "8000"]
