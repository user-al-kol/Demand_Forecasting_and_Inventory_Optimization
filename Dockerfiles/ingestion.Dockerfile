# ingestion-container
FROM apache/spark-py:latest
# Upgrade pip 
# Create working directory
WORKDIR /app
# Copy the script
COPY ../Ingestion/main.py .
COPY ../Ingestion/ingestion.py .
# Default command (Spark job)
CMD ["/opt/spark/bin/spark-submit", "/app/main.py"]