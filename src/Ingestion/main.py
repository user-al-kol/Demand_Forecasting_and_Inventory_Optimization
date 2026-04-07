import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from common.config import *
from common.utils import get_logger, get_todays_files, divide_files, find_latest_file
from common.spark_utils import partition, add_processed_date_deduplicate


if __name__ == "__main__":

    logger = get_logger(logging.INFO)

    present_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Bronze Layer
    # Ingestion process
    logger.info("File ingestion process is starting.")

    # spark = SparkSession.builder.appName("PartitionFiles").getOrCreate()
    builder = SparkSession.builder \
    .appName("PartitionFiles") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

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
    sales_file_partitioned = find_latest_file(S_SOURCE_DIR, LOGICAL_DATE, logger)

    # Define the schema #schema

    inventory_movement_file_partitioned_df = spark.read.parquet(inventory_movement_file_partitioned)
    sales_file_partitioned_df = spark.read.parquet(sales_file_partitioned)

    inventory_movement_transformed = add_processed_date_deduplicate(inventory_movement_file_partitioned_df,\
                                                         "inventory",["movement_id, movement_date"], present_date, spark)
    sales_transformed = add_processed_date_deduplicate(sales_file_partitioned_df,"sales",["order_id, order_date"], present_date,spark)

    # Test
    inventory_movement_transformed.select(["movement_id","movement_ts","movement_date","movement_type"]).show(5)
    inventory_movement_transformed.select(["product_id","sku","location_id","location_code"]).show(5)
    inventory_movement_transformed.select(["qty_delta","ref_order_id","ref_order_type","notes"]).show(5)
    inventory_movement_transformed.select(["erp_export_ts","processed_date"]).show(5)
    inventory_movement_transformed.printSchema()

    sales_transformed.select(["order_id","order_ts","order_date","status"]).show(5)
    sales_transformed.select(["source","customer_id","customer_code","customer_name"]).show(5)
    sales_transformed.select(["customer_type","location_id","location_code","product_id"]).show(5)
    sales_transformed.select(["sku","qty_ordered","qty_fulfilled","unit_price_eur"]).show(5)
    sales_transformed.select(["line_total_eur","order_total_eur","erp_export_ts","processed_date"]).show(5)
    sales_transformed.printSchema()

    delta_path = "demand_forecasting_optimisation_inventory/data/delta_lake/bronze/bronze_delta_tables"
    inventory_movement_transformed.write.format("delta").mode("overwrite").save(delta_path)

    spark.stop()