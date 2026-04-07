# ingestion-container
FROM apache/spark-py:latest
# Upgrade pip
USER root
RUN pip install --upgrade pip 
# Create working directory

# Install Delta Lake Python package
RUN pip install delta-spark

WORKDIR /app
# Copy the script
COPY ../src/Ingestion /app/Ingestion
COPY ../src/common /app/common

ENV PYTHONPATH=/app
USER 185
# Default command (Spark job)
CMD ["/opt/spark/bin/spark-submit", "/app/Ingestion/main.py"]