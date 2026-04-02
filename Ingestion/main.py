import os
import logging
from pyspark.sql import SparkSession
from ingestion import get_files, make_partition

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

if __name__ == "__main__":

    SOURCE_DIR = os.environ.get("SOURCE_DIR")

    logging.info("File ingestion process is starting.")

    spark = SparkSession.builder.appName("PartitionFiles").getOrCreate()
    logging.info("Spark session started.")
    # Call ingestor
    todays_files = get_files(SOURCE_DIR)

    logging.info("Files to be ingested:")
    logging.info(todays_files)

    make_partition(SOURCE_DIR,todays_files,spark)

    spark.stop()