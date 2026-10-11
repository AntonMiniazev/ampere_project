FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV POLARS_SKIP_CPU_CHECK=1

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY docker/init_source_preparation/pyproject.toml /app/pyproject.toml
COPY docker/init_source_preparation/model /app/model
COPY tools/__init__.py tools/source_batch_staging.py /app/tools/
COPY tools/contracts/ampere_tables.json /app/tools/contracts/ampere_tables.json

RUN uv pip install --system --no-cache-dir .

CMD ["python", "-m", "init_source_preparation"]
