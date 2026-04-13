from datetime import datetime
import logging
from medallion import bronze_layer,silver_layer
from pyspark.sql import SparkSession
from common.spark_utils import display_bronze_tables
from common.utils import get_logger 
from common.config import *

if __name__ == "__main__":

    logger = get_logger(logging.INFO)

    present_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    logger.info("File ingestion process is starting.")

    builder = SparkSession.builder \
    .appName("BatchProcessing") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.warehouse.dir", DELTA_PATH)

    spark = builder.getOrCreate()

    logger.info("Spark session started.")

    bronze_layer(present_date,spark,logger)
    display_bronze_tables(spark)

    silver_layer(present_date,spark,logger)
    # gold_layer(spark,logger)

    spark.stop()