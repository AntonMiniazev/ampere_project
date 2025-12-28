FROM apache/spark:4.1.0

ENV PYTHONUNBUFFERED=1
ENV PYSPARK_PYTHON=python3

WORKDIR /opt/spark/app

ARG HADOOP_AWS_VERSION=3.4.2
ARG AWS_SDK_VERSION=2.40.16

RUN curl -fsSL -o /opt/spark/jars/postgresql-42.7.3.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar \
  && curl -fsSL -o /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
  && curl -fsSL -o /opt/spark/jars/aws-sdk-${AWS_SDK_VERSION}.jar \
    https://repo1.maven.org/maven2/software/amazon/awssdk/s3/${AWS_SDK_VERSION}/s3-${AWS_SDK_VERSION}.jar

COPY app /opt/spark/app
