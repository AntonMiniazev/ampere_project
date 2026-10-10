FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /opt/ampere/app
ENV PYTHONPATH=/opt/ampere/app

# Spark Connect's Python client needs no local Spark distribution, JVM, or JARs.
RUN python -m pip install --no-cache-dir "numpy==2.3.4" "pyspark-client==4.1.0" \
    && python -c "import numpy; from pyspark.sql import SparkSession; from pyspark.sql import functions; assert numpy.__version__ == '2.3.4'; assert callable(SparkSession.builder.remote)"

COPY tools/__init__.py /opt/ampere/app/tools/__init__.py
COPY tools/contracts/ /opt/ampere/app/tools/contracts/
COPY docker/spark/iceberg_raw_etl/app/initialize_iceberg_catalog.py /opt/ampere/app/
COPY docker/spark/iceberg_raw_etl/app/iceberg_housekeeping_connect.py /opt/ampere/app/

RUN python -m py_compile \
    /opt/ampere/app/initialize_iceberg_catalog.py \
    /opt/ampere/app/iceberg_housekeeping_connect.py \
    && python -c "import initialize_iceberg_catalog, iceberg_housekeeping_connect; print('Spark Connect applications import successfully')"

ENV HOME=/tmp
USER 185
