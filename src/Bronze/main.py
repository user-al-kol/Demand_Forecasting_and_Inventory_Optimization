import os
import logging 
from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

if __name__ == '__main__':

    INVENTORY_MOVEMENT_SOURCE = os.environ.get("IM_SOURCE_DIR")
    inventory_movement_files = os.listdir(INVENTORY_MOVEMENT_SOURCE)

    logging.info("The inventory movement files are: ")
    logging.info(inventory_movement_files)