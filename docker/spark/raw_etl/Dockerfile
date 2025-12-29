FROM apache/spark:4.1.0

ENV PYTHONUNBUFFERED=1
ENV PYSPARK_PYTHON=python3

WORKDIR /opt/spark/app

RUN mkdir -p /opt/spark/ivy

COPY app /opt/spark/app
