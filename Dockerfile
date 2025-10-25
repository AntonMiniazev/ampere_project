# syntax=docker/dockerfile:1.7

############################
# Stage: dev (VS Code devcontainer)
############################
FROM mcr.microsoft.com/devcontainers/python:3.11 AS dev

# Keep dev image lean: only essential CLI
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl jq \
 && rm -rf /var/lib/apt/lists/*

# Initialize Microsoft ODBC 18
RUN curl -sSL -O https://packages.microsoft.com/config/ubuntu/22.04/packages-microsoft-prod.deb && \
    dpkg -i packages-microsoft-prod.deb || true && \
    apt-get update && \
    apt-get install -y --fix-broken && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev && \
    rm -f packages-microsoft-prod.deb && \
    rm -rf /var/lib/apt/lists/*

# Use uv for fast, reproducible deps
WORKDIR /workspace
COPY pyproject.toml uv.lock ./
RUN pipx install uv && uv sync

############################
# Stage: dbt-runner
############################
FROM python:3.11-slim AS dbt-runner
ENV PIP_NO_CACHE_DIR=1 PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl tini \
 && rm -rf /var/lib/apt/lists/* \
 && pip install --no-cache-dir uv

# Initialize Microsoft ODBC 18
RUN curl -sSL -O https://packages.microsoft.com/config/ubuntu/22.04/packages-microsoft-prod.deb && \
    dpkg -i packages-microsoft-prod.deb || true && \
    apt-get update && \
    apt-get install -y --fix-broken && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev && \
    rm -f packages-microsoft-prod.deb && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install runtime deps
COPY pyproject.toml uv.lock /app/
RUN uv sync --no-dev  # creates /app/.venv

# Copy full dbt project (dbt_project.yml, models, selectors.yml, etc.)
COPY dbt/ /app/project/

# Resolve dbt packages at build time
RUN . /app/.venv/bin/activate && dbt --version && dbt deps --project-dir /app/project

# Entrypoints
ENV PATH="/app/.venv/bin:${PATH}" DBT_PROFILES_DIR="/app/profiles"
COPY docker/entrypoints/ /usr/local/bin/
RUN chmod +x /usr/local/bin/*.sh

ENTRYPOINT ["/usr/bin/tini","--"]
CMD ["/usr/local/bin/run_dbt.sh"]
