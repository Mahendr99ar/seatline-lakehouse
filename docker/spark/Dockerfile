FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root
COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY lakehouse /app/lakehouse
COPY bin /app/bin
RUN mkdir -p /opt/ivy /checkpoints && chown -R spark:spark /opt/ivy /checkpoints
USER spark

ENV PYTHONPATH=/app
WORKDIR /app
CMD ["/opt/spark/bin/spark-submit", "--master", "local[2]", "--driver-memory", "900m", "/app/bin/stream.py"]
