FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY pyproject.toml /app/pyproject.toml
COPY model /app/model

RUN uv pip install --system --no-cache-dir .

CMD ["python", "-m", "init_source_preparation"]
