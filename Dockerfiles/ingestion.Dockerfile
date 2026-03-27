# ingestion-container
FROM bitnami/spark:3.5.3
# Upgrade pip
RUN pip install --no-cache-dir --upgrade pip 
# Create working directory
WORKDIR /app
# Copy the script
COPY ../Ingestion/ingestion.py .
# Command
CMD ["python","ingestion.py"]