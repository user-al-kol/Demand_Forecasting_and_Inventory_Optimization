import os
import logging
from datetime import datetime 
from urllib.parse import unquote
from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

if __name__ == '__main__':

    INVENTORY_MOVEMENT_SOURCE = os.environ.get("IM_SOURCE_DIR")
    LOGICAL_DATE = os.environ.get("LOGICAL_DATE")
    inventory_movement_files = os.listdir(INVENTORY_MOVEMENT_SOURCE)
    now = datetime.now()
    present_date = now.strftime("%Y-%m-%d %H:%M:%S")
    found_it = False

    spark = SparkSession.builder.appName("UpsertBronzeData").getOrCreate()
    logging.info("Spark Session successfully started.")

    logging.info("The inventory movement files are: ")
    logging.info(inventory_movement_files)

    for file in inventory_movement_files:

        if 'ingestion_date' in file and found_it == False:

            #logging.debug(f"This file is: {file}")
            file_date = file.split("=")[1]
            logging.info(f"The date of the file: {file_date}")

            decoded = unquote(file_date)
            dt = datetime.strptime(decoded, "%Y-%m-%d %H:%M:%S")
            iso_file_date = dt.isoformat()

            if iso_file_date >= LOGICAL_DATE:
                found_it = True
                logging.info(f"That's your new file: {file}")
                logging.info(f"ISO: {iso_file_date}")
                logging.info(f"Logical Date: {LOGICAL_DATE}")

                full_file_path = os.path.join(INVENTORY_MOVEMENT_SOURCE,file)
                logging.info(f"Full file path: {full_file_path}")

                new_inventory_movements_df = spark.read.parquet(full_file_path)

                new_inventory_movements_df.createOrReplaceTempView("inventory")
                
                new_inventory_movements_view = spark.sql(
                    f"""
                        SELECT
                            *, 
                            TIMESTAMP('{present_date}') AS processed_date
                        FROM
                            inventory
                    """
                )

                new_inventory_movements_view.select(["movement_id","movement_ts","movement_date","movement_type"]).show(5)
                new_inventory_movements_view.select(["product_id","sku","location_id","location_code"]).show(5)
                new_inventory_movements_view.select(["qty_delta","ref_order_id","ref_order_type","notes"]).show(5)
                new_inventory_movements_view.select(["erp_export_ts","processed_date"]).show(5)
                new_inventory_movements_view.printSchema()

                # new_inventory_movements_view = spark.sql(
                #     f"""
                #         WITH ranked AS (
                #             SELECT *,
                #                 TIMESTAMP('{present_date}') AS processed_date,
                #                 ROW_NUMBER() OVER (
                #                     PARTITION BY movement_id, movement_date
                #                     ORDER BY movement_id
                #                 ) AS rn
                #             FROM 
                #                 inventory
                #         )
                #         SELECT 
                #             *
                #         FROM 
                #             ranked
                #         WHERE 
                #             rn = 1
                # """)

    spark.stop()