FROM apache/spark:4.1.0

COPY --from=ghcr.io/astral-sh/uv:0.13.0 /uv /uvx /bin/

ENV PYTHONUNBUFFERED=1 \
    UV_PYTHON_INSTALL_DIR=/opt/ampere-python \
    PYSPARK_PYTHON=/usr/local/bin/python3.14 \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.14

USER root
ENV HOME=/root
RUN uv python install 3.14 \
    && ln -s "$(uv python find 3.14)" /usr/local/bin/python3.14 \
    && uv pip install --system --python /usr/local/bin/python3.14 --no-cache \
    boto3==1.40.18 \
    pandas==2.3.3 \
    pyarrow==26.0.0 \
    "pyspark[connect]==4.1.0" \
    zstandard==0.25.0

# Pin Iceberg's Spark 4.1 runtime and the same Hadoop S3A libraries used by
# the current Raw reader. The image stays independent of the shared Ivy cache.
RUN mkdir -p /opt/spark/iceberg-jars /opt/spark/app
ADD https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.12.0/iceberg-spark-runtime-4.1_2.13-1.12.0.jar /opt/spark/iceberg-jars/
ADD https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar /opt/spark/iceberg-jars/
ADD https://repo.maven.apache.org/maven2/software/amazon/awssdk/bundle/2.35.4/bundle-2.35.4.jar /opt/spark/iceberg-jars/
ADD https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar /opt/spark/iceberg-jars/
RUN chmod 644 /opt/spark/iceberg-jars/*.jar

WORKDIR /opt/spark/app
COPY docker/spark/iceberg_raw_etl/app/ /opt/spark/app/
COPY tools/__init__.py /opt/spark/app/tools/__init__.py
COPY tools/contracts/ /opt/spark/app/tools/contracts/
USER 185
