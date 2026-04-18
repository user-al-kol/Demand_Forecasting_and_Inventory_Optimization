from pyspark.sql.functions import col,to_date, lit
from common.config import *
from common.schema import inventory_movements_schema, sales_schema
from common.utils import get_todays_files, divide_files,log_metrics
from common.spark_utils import upsert
from common.spark_utils import process_with_retry


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
            entity="inventory_movements"
        ),
        DatasetConfig(
            file=sales_file,
            destination_dir=S_DESTINATION_DIR,
            source_partitioned=S_SOURCE_DIR,
            table="bronze_sales",
            schema_fn=sales_schema,
            keys=["order_id", "order_date","product_id"],
            entity="sales"
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

    logger.info("Reading today's bronze data.")

    bronze_inventory_movements = spark.read.table("bronze_inventory_movements")
    bronze_inventory_movements_today = bronze_inventory_movements.filter(to_date(col("processed_date")) == to_date(lit(present_date)))

    bronze_sales = spark.read.table("bronze_sales")
    bronze_sales_today = bronze_sales.filter(to_date(col("processed_date")) == to_date(lit(present_date)))



    upsert(bronze_inventory_movements_today,\
           "silver_inventory_movements",\
            inventory_movements_schema(),\
            ["movement_id", "movement_ts"],\
            spark,\
            logger)
    try:
        upsert(bronze_sales_today,\
            "silver_sales",\
            sales_schema(),\
            ["order_id", "order_date","product_id"],\
            spark,\
            logger)
    except Exception as e:
        logger.info(f"Exception: {e}")

def gold_layer():
    pass