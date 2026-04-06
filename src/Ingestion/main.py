import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from common.config import *
from common.utils import get_logger, get_todays_files, divide_files, find_latest_file
from common.spark_utils import partition, transform_inventory


if __name__ == "__main__":

    logger = get_logger(logging.INFO)

    present_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Bronze Layer
    # Ingestion process
    logger.info("File ingestion process is starting.")

    spark = SparkSession.builder.appName("PartitionFiles").getOrCreate()
    logger.info("Spark session started.")

    # Call ingestor
    todays_files = get_todays_files(SOURCE_DIR,LOGICAL_DATE,logger)

    logger.info("Files to be ingested:")
    logger.info(todays_files)

    # Divide the files
    inventory_movement_file, sales_file = divide_files(todays_files)

    partition(SOURCE_DIR, IM_DESTINATION_DIR, inventory_movement_file, spark, logger)
    partition(SOURCE_DIR, S_DESTINATION_DIR, sales_file, spark, logger)

    logger.info("Files ingested successfully.")

    # Save files in delta tables.
    inventory_movement_file_partitioned = find_latest_file(IM_SOURCE_DIR, LOGICAL_DATE, logger)
    sales_file_partitioned = find_latest_file(IM_SOURCE_DIR, LOGICAL_DATE, logger)

    # Define the schema #schema

    inventory_movement_file_partitioned_df = spark.read.parquet(inventory_movement_file_partitioned)
    sales_file_partitioned_df = spark.read.parquet(sales_file_partitioned)

    inventory_movement_transformed = transform_inventory(inventory_movement_file_partitioned_df,"inventory", present_date, spark)
    #sales_transformed = 

    # Test
    inventory_movement_transformed.select(["movement_id","movement_ts","movement_date","movement_type"]).show(5)
    inventory_movement_transformed.select(["product_id","sku","location_id","location_code"]).show(5)
    inventory_movement_transformed.select(["qty_delta","ref_order_id","ref_order_type","notes"]).show(5)
    inventory_movement_transformed.select(["erp_export_ts","processed_date"]).show(5)
    inventory_movement_transformed.printSchema()
    
    spark.stop()

    