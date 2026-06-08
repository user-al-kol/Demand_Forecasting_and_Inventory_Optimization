from pyspark.sql.functions import col,to_date, lit, to_timestamp
from common.config import *
from common.schema import inventory_movements_schema, sales_schema
from common.utils import get_todays_files, divide_files,log_metrics
from common.spark_utils import upsert,validate_row_count
from common.spark_utils import process_with_retry,process_bronze_dataset,process_silver_dataset
from datetime import datetime


def bronze_layer(present_date,spark,logger):

    # Call ingestor
    todays_files = get_todays_files(SOURCE_DIR, LOGICAL_DATE, logger)

    logger.info("Files to be ingested:")
    logger.info(todays_files)

    # Divide the files
    inventory_movement_file, sales_file = divide_files(todays_files)

    configs = [
        DatasetConfig(
            file=inventory_movement_file,
            destination_dir=IM_DESTINATION_DIR,
            source_partitioned=IM_SOURCE_DIR,
            source_table="",
            target_table="bronze_inventory_movements",
            schema_fn=inventory_movements_schema,
            keys=["movement_id", "movement_ts"],
            entity="inventory_movements",
            monitoring_match="inventory"
        ),
        DatasetConfig(
            file=sales_file,
            destination_dir=S_DESTINATION_DIR,
            source_partitioned=S_SOURCE_DIR,
            source_table="",
            target_table="bronze_sales",
            schema_fn=sales_schema,
            keys=["order_id","product_id"],
            entity="sales",
            monitoring_match="sales"
        )
    ]

    for config in configs:
        try:
            metrics = process_with_retry(
                process_bronze_dataset,
                config,
                retries=1,
                delay=3,
                present_date=present_date,
                spark=spark,
                logger=logger,
                logical_date=LOGICAL_DATE,
                source_dir=SOURCE_DIR
            )

            log_metrics(logger, config.entity, metrics)
            logger.info("End of the bronze layer.")

        except Exception as e:
            logger.error(f"[{config.entity}] Failed after retries: {str(e)}")
       
        
def silver_layer(present_date,spark,logger):

    file_date_time = datetime.fromisoformat(LOGICAL_DATE).strftime("%Y-%m-%d %H:%M:%S")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS bronze_table_monitoring 
        USING DELTA
        LOCATION '/app/data/delta_lake/warehouse/bronze_table_monitoring'
    """)    

    bronze_table_monitoring = (
        spark.read.table("bronze_table_monitoring")
        .filter((col("date")) >= to_timestamp(lit(file_date_time)))
    )

    safe_rows_by_file = {
        row["source_file"]: row["safe_rows"]
        for row in (
            bronze_table_monitoring
            .select("source_file", "safe_rows")
            .distinct()
            .collect()
        )
    }

    logger.info(f"Safe rows: {safe_rows_by_file}")

    # Define dataset configs

    configs = [
        DatasetConfig(
            file="",
            destination_dir="",
            source_partitioned="",
            source_table="bronze_inventory_movements",
            target_table="silver_inventory_movements",
            schema_fn=inventory_movements_schema,
            keys=["movement_id", "movement_ts"],
            entity="inventory_movements on the silver layer",
            monitoring_match="inventory"
        ),
        DatasetConfig(
            file="",
            destination_dir="",
            source_partitioned="",
            source_table="bronze_sales",
            target_table="silver_sales",
            schema_fn=sales_schema,
            keys=["order_id", "product_id"],
            entity="sales on the silver layer",
            monitoring_match="sales"
        )
    ]

    for config in configs:
        try:
            process_with_retry(
                process_silver_dataset,
                config,
                retries=1,
                delay=3,
                safe_rows_by_file=safe_rows_by_file,
                present_date=present_date,
                spark=spark,
                logger=logger
            )

            logger.info("End of the silver layer.")

        except Exception as e:
            logger.error(
                f"[{config.entity}] Failed after retries: {str(e)}"
            )


def gold_layer():
    pass