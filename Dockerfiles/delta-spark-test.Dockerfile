FROM apache/spark-py:latest

# Upgrade pip
RUN pip install --upgrade pip

# Install Delta Lake Python package
RUN pip install delta-spark

# Create working directory
WORKDIR /app

# Copy the scripts
COPY ../src/Ingestion/main.py .
COPY ../src/Ingestion/ingestion.py .

# Set Spark environment variables for Delta support
ENV SPARK_EXTRA_PACKAGES="io.delta:delta-core_2.12:2.5.0"
ENV PYSPARK_PYTHON=python3

# Default command to run your Spark job with Delta support
CMD ["/opt/spark/bin/spark-submit", "--packages", "io.delta:delta-core_2.12:2.5.0", "/app/main.py"]