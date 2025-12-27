FROM apache/spark:3.5.1

ENV PYTHONUNBUFFERED=1
ENV PYSPARK_PYTHON=python3

WORKDIR /opt/spark/app

RUN curl -fsSL -o /opt/spark/jars/postgresql-42.7.3.jar \
  https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

COPY app /opt/spark/app
