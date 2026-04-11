# Start with slim Python 3.13 image
FROM python:3.13.10-slim

# Install system dependencies (Required for many SQL drivers)
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy uv binary
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# Set working directory
WORKDIR /app

# Ensure the virtualenv is used
ENV PATH="/app/.venv/bin:$PATH"

# --- CRITICAL FIX: Files must be in the same folder as Dockerfile ---
# Copy dependency files (Make sure these are in the same directory as this Dockerfile)
COPY "pyproject.toml" "uv.lock" ".python-version" ./

# Install dependencies
RUN uv sync --locked

# Copy application code
COPY app.py app.py

# Streamlit uses port 8501 by default
EXPOSE 8501

# Streamlit-specific optimizations
# Using CMD instead of ENTRYPOINT makes it easier to pass arguments
ENTRYPOINT ["uv", "run", "streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]

# run with: 
# docker run -it \
#   --network de-zoomcamp-final-project_default \
#   -p 8501:8501 \
#   streamlit:test1