# bronze-container
FROM apache/spark-py:latest

# Create working directory
WORKDIR /app
# Copy the script
COPY ../src/Bronze/main.py .

# Default command (Spark job)
CMD ["/opt/spark/bin/spark-submit", "/app/main.py"]