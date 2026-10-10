FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV POLARS_SKIP_CPU_CHECK=1

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY pyproject.toml /app/pyproject.toml
COPY model /app/model

RUN uv pip install --system --no-cache-dir . \
    && python -c "import numpy; assert numpy.__version__ == '2.3.4'; import order_data_generator.main; print('Generator and NumPy imports succeeded:', numpy.__version__)"

CMD ["python", "-m", "order_data_generator"]

