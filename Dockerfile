# ---------------------------------------------------------------------------
# Springer Capital - Referral Fraud Detection Pipeline
# ---------------------------------------------------------------------------
# Base image: Python 3.11 slim
FROM python:3.11-slim

# PySpark needs a JVM at runtime
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless procps && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java \
    PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=python3

# Set working directory
WORKDIR /app

# Copy and install Python dependencies first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# Default (overridable) locations. Mount host folders here at `docker run` time.
ENV INPUT_DIR=/app/data/raw \
    PROFILING_OUTPUT_DIR=/app/output/profiling \
    REPORT_OUTPUT_DIR=/app/output/report

# The report/profiling output must be visible outside the container, so we
# rely on the caller mounting a host volume at /app/output (see README).
VOLUME ["/app/data", "/app/output"]

ENTRYPOINT ["./entrypoint.sh"]
