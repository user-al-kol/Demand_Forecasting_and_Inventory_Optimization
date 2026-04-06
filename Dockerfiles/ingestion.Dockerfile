# ingestion-container
FROM apache/spark-py:latest
# Upgrade pip 
# Create working directory
WORKDIR /app
# Copy the script
COPY ../src/Ingestion /app/Ingestion
COPY ../src/common /app/common

ENV PYTHONPATH=/app

# Default command (Spark job)
CMD ["/opt/spark/bin/spark-submit", "/app/Ingestion/main.py"]