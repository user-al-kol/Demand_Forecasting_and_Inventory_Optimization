from pyspark.sql.functions import col,to_date, lit
from common.config import *
from common.schema import inventory_movements_schema, sales_schema
from common.utils import get_todays_files, divide_files, find_latest_file
from common.spark_utils import partition, add_processed_date_deduplicate,add_processed_date,drop_null_keys,bronze_table_monitoring_insert,upsert

def bronze_layer(present_date,spark,logger):    
    # Call ingestor
    todays_files = get_todays_files(SOURCE_DIR,LOGICAL_DATE,logger)

    logger.info("Files to be ingested:")
    logger.info(todays_files)

    # Divide the files
    inventory_movement_file, sales_file = divide_files(todays_files)

    im_monitoring_date,im_source_file,im_number_of_rows = partition(SOURCE_DIR, IM_DESTINATION_DIR, inventory_movement_file, spark, logger)
    sales_monitoring_date,sales_source_file,sales_number_of_rows = partition(SOURCE_DIR, S_DESTINATION_DIR, sales_file, spark, logger)

    logger.info("Files ingested successfully.")

    # Save files in delta tables.
    inventory_movement_file_partitioned = find_latest_file(IM_SOURCE_DIR, LOGICAL_DATE, logger)
    sales_file_partitioned = find_latest_file(S_SOURCE_DIR, LOGICAL_DATE, logger)

    inventory_movement_file_partitioned_df = spark.read.parquet(inventory_movement_file_partitioned)
    sales_file_partitioned_df = spark.read.parquet(sales_file_partitioned)

    # inventory_movement_transformed = add_processed_date_deduplicate(inventory_movement_file_partitioned_df,\
    #                                                      "inventory",["movement_id", "movement_date"], present_date, spark)
    # sales_transformed = add_processed_date_deduplicate(sales_file_partitioned_df,"sales",["order_id", "order_date"], present_date,spark)

    inventory_movement_file_partitioned_no_nulls_df,im_null_counts = drop_null_keys(inventory_movement_file_partitioned_df,\
                                                                    ["movement_id", "movement_date"],\
                                                                    logger)
    
    bronze_table_monitoring_insert(im_monitoring_date,\
                                   im_source_file,\
                                   im_number_of_rows,\
                                   ["movement_id", "movement_date"],\
                                   im_null_counts,\
                                   spark,\
                                   logger)
    
    sales_file_partitioned__no_nulls_df,sales_null_counts = drop_null_keys(sales_file_partitioned_df,\
                                                        ["order_id", "order_date"],\
                                                        logger)
    
    
    bronze_table_monitoring_insert(sales_monitoring_date,\
                                   sales_source_file,\
                                   sales_number_of_rows,\
                                   ["order_id", "order_date"],\
                                   sales_null_counts,\
                                   spark,\
                                   logger)
    

    inventory_movement_transformed = add_processed_date(inventory_movement_file_partitioned_no_nulls_df,\
                                                        "inventory", \
                                                        present_date, \
                                                        "ERP",
                                                        spark)


    sales_transformed = add_processed_date(sales_file_partitioned__no_nulls_df,\
                                           "sales", \
                                            present_date,\
                                            "ERP",\
                                            spark)


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
    
    
def silver_layer(present_date,spark,logger):

    logger.info("Reading today's bronze data.")

    bronze_inventory_movements = spark.read.table("bronze_inventory_movements")
    bronze_inventory_movements_today = bronze_inventory_movements.filter(to_date(col("processed_date")) == to_date(lit(present_date)))

    bronze_sales = spark.read.table("bronze_sales")
    bronze_sales_today = bronze_sales.filter(to_date(col("processed_date")) == to_date(lit(present_date)))



    upsert(bronze_inventory_movements_today,\
           "silver_inventory_movements",\
            inventory_movements_schema(),\
            ["movement_id", "movement_date"],\
            spark,\
            logger)
    
    upsert(bronze_sales_today,\
           "silver_sales",\
            sales_schema(),\
            ["order_id", "order_date"],\
            spark,\
            logger)

def gold_layer():
    pass