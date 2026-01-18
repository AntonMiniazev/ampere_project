FROM apache/spark:4.1.0

ENV PYTHONUNBUFFERED=1
ENV PYSPARK_PYTHON=python3

WORKDIR /opt/spark/app

RUN python3 -m pip install --no-cache-dir delta-spark==4.0.1

RUN mkdir -p /opt/spark/ivy

COPY app /opt/spark/app
