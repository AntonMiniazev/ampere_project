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
  && for module in annotations arns auth aws-core aws-query-protocol aws-xml-protocol checksums checksums-spi crt-core endpoints-spi http-auth http-auth-aws http-auth-aws-eventstream http-auth-spi http-client-spi identity-spi json-utils metrics-spi profiles protocol-core regions retries retries-spi s3 sdk-core third-party-jackson-core utils utils-lite; do \
      curl -fsSL -o /opt/spark/jars/${module}-${AWS_SDK_VERSION}.jar \
        https://repo1.maven.org/maven2/software/amazon/awssdk/${module}/${AWS_SDK_VERSION}/${module}-${AWS_SDK_VERSION}.jar; \
    done

COPY app /opt/spark/app
