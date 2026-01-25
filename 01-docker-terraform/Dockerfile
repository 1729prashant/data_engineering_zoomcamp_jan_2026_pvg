# -------------------------------- OLD 1 START --------------------------------
# base Docker image that we will build on
#FROM python:3.13.11-slim

# Copy uv binary from official uv image (multi-stage build pattern)
#COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# set up the working directory inside the container
#WORKDIR /app

# Add virtual environment to PATH so we can use installed packages
#ENV PATH="/app/.venv/bin:$PATH"

# Copy dependency files first (better layer caching)
#COPY "pyproject.toml" "uv.lock" ".python-version" ./

# Install dependencies from lock file (ensures reproducible builds)
#RUN uv sync --locked

# Copy application code
#COPY pipeline.py pipeline.py

# define what to do first when the container runs
# in this example, we will just run the script
#ENTRYPOINT ["python", "pipeline.py"]
# -------------------------------- OLD 1 END.  --------------------------------


# Start with slim Python 3.13 image for smaller size
FROM python:3.13.11-slim

# Copy uv binary from official uv image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# Set working directory inside container
WORKDIR /code

# Add virtual environment to PATH
ENV PATH="/code/.venv/bin:$PATH"

# Copy dependency files first (better caching)
COPY pyproject.toml .python-version uv.lock ./

# Install all dependencies from lock file (ensures reproducible builds)
RUN uv sync --locked

#Copy ingestion script
COPY ingest_data.py .

# Set entry point to run the ingestion script
ENTRYPOINT ["uv", "run", "python", "ingest_data.py"]