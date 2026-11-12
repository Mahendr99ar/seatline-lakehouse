# Airflow runs the batch steps itself, with Spark in local mode inside the
# task. On a 4 GB machine that is cheaper than keeping a Spark master and a
# worker up all day for a job that runs a few minutes an hour.
FROM apache/airflow:2.10.5-python3.10

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
 && rm -rf /var/lib/apt/lists/* \
 && mkdir -p /opt/ivy /dashboard \
 && chown -R airflow:root /opt/ivy /dashboard && chmod -R g+rwX /opt/ivy /dashboard
USER airflow

RUN pip install --no-cache-dir pyspark==3.5.3
COPY conf/spark-defaults.conf /opt/lakehouse/conf/spark-defaults.conf
COPY lakehouse /opt/lakehouse/lakehouse

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_CONF_DIR=/opt/lakehouse/conf \
    PYTHONPATH=/opt/lakehouse
