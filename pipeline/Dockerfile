# Start with slim Python 3.13 image
FROM python:3.13.10-slim

# Copy uv binary from official uv image (multi-stage build pattern)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# Set working directory
WORKDIR /app

# Add virtual environment to PATH so we can use installed packages
ENV PATH="/app/.venv/bin:$PATH"

# Install JDK so Java is available for the image
RUN apt-get update \
    && apt-get install -y default-jdk \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME="/usr/lib/jvm/default-java"
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# COPY postgresql-42.7.2.jar /app/postgresql-42.7.2.jar

# Copy dependency files first (better layer caching)
COPY "pyproject.toml" "uv.lock" ".python-version" ./
# Install dependencies from lock file (ensures reproducible builds)
RUN uv sync --locked

# Copy application code
COPY pipeline.py pipeline.py

# Set entry point
ENTRYPOINT ["uv", "run", "python", "pipeline.py"]

# docker run -it --network de-zoomcamp-final-project_default -v $(pwd)/datalake:/app/datalake pyspark:postgre 