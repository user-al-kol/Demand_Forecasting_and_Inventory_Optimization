# Dockerfiles/spark_livy.Dockerfile
FROM apache/spark-py:latest

USER root

# ── Python deps ───────────────────────────────────────────────────────────────
RUN pip install --upgrade pip
RUN pip install delta-spark==2.4.0

# ── Ivy cache (same as your ingestion image) ──────────────────────────────────
RUN mkdir -p /tmp/.ivy2 && chmod -R 777 /tmp/.ivy2

# ── Install Livy ──────────────────────────────────────────────────────────────
ARG LIVY_VERSION=0.8.0-incubating
ARG SCALA_VERSION=2.12

RUN apt-get update && apt-get install -y curl unzip wget && \
    wget -q https://archive.apache.org/dist/incubator/livy/${LIVY_VERSION}/apache-livy-${LIVY_VERSION}_${SCALA_VERSION}-bin.zip && \
    unzip -q apache-livy-${LIVY_VERSION}_${SCALA_VERSION}-bin.zip -d /opt && \
    mv /opt/apache-livy-${LIVY_VERSION}_${SCALA_VERSION}-bin /opt/livy && \
    rm apache-livy-${LIVY_VERSION}_${SCALA_VERSION}-bin.zip && \
    mkdir -p /opt/livy/logs && \
    chmod -R 777 /opt/livy/logs

# ── Delta Lake JARs directly into Spark's classpath ──────────────────────────
RUN wget -q https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar \
    -O /opt/spark/jars/delta-core_2.12-2.4.0.jar && \
    wget -q https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar \
    -O /opt/spark/jars/delta-storage-2.4.0.jar

ENV LIVY_HOME=/opt/livy
ENV PATH="${LIVY_HOME}/bin:${PATH}"

# ── Livy config ───────────────────────────────────────────────────────────────
COPY ../conf/livy.conf /opt/livy/conf/livy.conf
COPY ../conf/livy-env.sh /opt/livy/conf/livy-env.sh
RUN chmod +x /opt/livy/conf/livy-env.sh

# ── Your pipeline code ────────────────────────────────────────────────────────
# Mirrors the structure of your ingestion image but for the batch jobs
COPY ../src/jobs   /app/jobs
COPY ../src/common /app/common
COPY ../src/Ingestion/ /app/Ingestion

ENV PYTHONPATH=/app

EXPOSE 8998

# Start Livy in the foreground so Docker can manage the process
CMD ["/opt/livy/bin/livy-server"]