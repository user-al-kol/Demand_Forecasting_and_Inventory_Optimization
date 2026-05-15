from pyspark.sql.functions import col,to_date, lit, to_timestamp
from common.config import *
from common.schema import inventory_movements_schema, sales_schema
from common.utils import get_todays_files, divide_files,log_metrics
from common.spark_utils import upsert,validate_row_count
from common.spark_utils import process_with_retry
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
            table="bronze_inventory_movements",
            schema_fn=inventory_movements_schema,
            keys=["movement_id", "movement_ts"],
            entity="inventory_movements",
            monitoring_match="inventory"
        ),
        DatasetConfig(
            file=sales_file,
            destination_dir=S_DESTINATION_DIR,
            source_partitioned=S_SOURCE_DIR,
            table="bronze_sales",
            schema_fn=sales_schema,
            keys=["order_id","product_id"],
            entity="sales",
            monitoring_match="sales"
        )
    ]

    for config in configs:
        try:
            metrics = process_with_retry(
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
            file="inventory",
            destination_dir="",
            source_partitioned="",
            table="bronze_inventory_movements",
            schema_fn=inventory_movements_schema,
            keys=["movement_id", "movement_ts"],
            entity="inventory_movements",
            monitoring_match="inventory"
        ),
        DatasetConfig(
            file="sales",
            destination_dir="",
            source_partitioned="",
            table="bronze_sales",
            schema_fn=sales_schema,
            keys=["order_id", "product_id"],
            entity="sales",
            monitoring_match="sales"
        )
    ]

    for config in configs:

        logger.info(f"Processing {config.entity}")

        # Get expected safe rows
        safe_rows = next(
            (
                v for k, v in safe_rows_by_file.items()
                if config.monitoring_match in k
            ),
            None
        )

        if safe_rows is None:
            logger.warning(f"No monitoring data for {config.entity}")
            continue

        # Load bronze table
        df = (
            spark.read.table(config.table)
            .filter(to_timestamp(col("processed_date")) == to_timestamp(lit(present_date)))
        )
        
        # Validation pipeline (extensible)
        is_valid = True

        # is_valid &= (actual_count == safe_rows)
        is_valid &= validate_row_count(df,safe_rows,logger,config.entity)
        # is_valid &= validate_duplicates
        # is_valid &= validate_schema
        # is_valid &= validate_nulls

        if not is_valid:
            logger.warning(f"[{config.entity}] validation failed, skipping")
            continue

        # Upsert
        logger.info(f"[{config.entity}] validations passed → upserting")

        upsert(
            df,
            f"silver_{config.table.split('bronze_')[1]}",
            config.schema_fn(),
            config.keys,
            spark,
            logger
        )
            

    # logger.info("Reading today's bronze data.")

    # bronze_inventory_movements_today = (
    #     spark.read.table("bronze_inventory_movements")
    #     .filter(to_timestamp(col("processed_date")) == to_timestamp(lit(present_date)))
    # )

    # bronze_sales_today = (
    #     spark.read.table("bronze_sales")
    #     .filter(to_timestamp(col("processed_date")) == to_timestamp(lit(present_date)))
    # )

    # inventory_count = bronze_inventory_movements_today.count()
    # sales_count = bronze_sales_today.count()

    # for key, safe_rows in safe_rows_by_file.items():

    #     if "inventory" in key:

    #         if inventory_count == safe_rows:

    #             logger.info("Safe rows match bronze_inventory_movements rows")

    #             logger.info("Upsert bronze_inventory_movements to silver layer")

    #             upsert(bronze_inventory_movements_today,\
    #                 "silver_inventory_movements",\
    #                 inventory_movements_schema(),\
    #                 ["movement_id", "movement_ts"],\
    #                 spark,\
    #                 logger)
    #         else:
    #             logger.warning("Safe rows do not match bronze_inventory_movements rows")
    #             logger.warning(f"Inventory count: {inventory_count}")
    #             logger.warning(f"Safe rows: {safe_rows}")

    #     elif "sales" in key:
            
    #         if sales_count == safe_rows:

    #             logger.info("Safe rows match bronze_sales rows")

    #             logger.info("Upsert bronze_sales to silver layer")

    #             upsert(bronze_sales_today,\
    #                 "silver_sales",\
    #                 sales_schema(),\
    #                 ["order_id","product_id"],\
    #                 spark,\
    #                 logger)
    #         else:
    #             logger.warning("Safe rows do not match bronze_sales rows")
    #             logger.warning(f"Sales count: {sales_count}")
    #             logger.warning(f"Safe rows: {safe_rows}")

def gold_layer():
    pass