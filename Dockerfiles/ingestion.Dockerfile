FROM apache/spark-py:latest

USER root
RUN pip install --upgrade pip 

# Install Delta Lake Python package
RUN pip install delta-spark

RUN mkdir -p /tmp/.ivy2 && chmod -R 777 /tmp/.ivy2
USER 185
WORKDIR /app

COPY ../src/Ingestion /app/Ingestion
COPY ../src/common /app/common

ENV PYTHONPATH=/app


CMD ["/opt/spark/bin/spark-submit","--packages", "io.delta:delta-core_2.12:2.4.0","--conf", "spark.jars.ivy=/tmp/.ivy2","/app/Ingestion/main.py"]