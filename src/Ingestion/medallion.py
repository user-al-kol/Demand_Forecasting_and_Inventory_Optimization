from common.config import *
from common.schema import inventory_movements_schema, sales_schema
from common.utils import get_todays_files, divide_files, find_latest_file
from common.spark_utils import partition, add_processed_date_deduplicate, upsert

def bronze_layer(present_date,spark,logger):    
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

    inventory_movement_file_partitioned_df = spark.read.parquet(inventory_movement_file_partitioned)
    sales_file_partitioned_df = spark.read.parquet(sales_file_partitioned)

    inventory_movement_transformed = add_processed_date_deduplicate(inventory_movement_file_partitioned_df,\
                                                         "inventory",["movement_id", "movement_date"], present_date, spark)
    sales_transformed = add_processed_date_deduplicate(sales_file_partitioned_df,"sales",["order_id", "order_date"], present_date,spark)

    upsert(inventory_movement_transformed,\
           "bronze_inventory_movements",\
            inventory_movements_schema(),\
            ["movement_id", "movement_date"],\
            spark,\
            logger)
    
    upsert(sales_transformed,\
           "bronze_sales",\
            sales_schema(),\
            ["order_id", "order_date"],\
            spark,\
            logger)
    
    
def silver_layer():
    pass


def gold_layer():
    pass