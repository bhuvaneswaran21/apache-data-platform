FROM apache/airflow:3.2.1

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        wget \
        ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV SPARK_VERSION=3.5.6
ENV HADOOP_VERSION=3

RUN wget -q \
    https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="${SPARK_HOME}/bin:${PATH}"

USER airflow

RUN pip install --no-cache-dir \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.2.1/constraints-3.13.txt \
  apache-airflow-providers-apache-spark \
  apache-airflow-providers-apache-kafka \
  kafka-python \
  psycopg2-binary    