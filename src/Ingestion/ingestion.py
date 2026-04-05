import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession  
from pyspark.sql.functions import lit

# SOURCE_DIR = os.environ.get("SOURCE_DIR")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

def get_files(source_dir):
    """ Function to recognize which files are the latest in order to ingest them."""


    LOGICAL_DATE = os.environ.get("LOGICAL_DATE")
    files = os.listdir(source_dir)

    # Get the Logical Date and split it to date and time
    logical_date = LOGICAL_DATE.split("T")[0]
    logical_time = LOGICAL_DATE.split("T")[1].split("+")[0]

    movement_files = []
    sales_files = []

    for file in files:

        logging.debug(f"Current files: {file}")

        if file.split('_')[0] == 'inventory':
            movement_files.append(file)
        else:
            sales_files.append(file)


    logging.debug(f"Airflow logical date: {LOGICAL_DATE}")
    logging.debug(f'Logical Date Time: {LOGICAL_DATE.split("T")}')

    logging.debug(f"Logical date: {logical_date}")
    logging.debug(f"Logical time: {logical_time}")

    logging.debug("Movement files")
    logging.debug(movement_files)
    logging.debug("Sales files")
    logging.debug(sales_files)

    todays_files = []

    logging.debug("Movement dates")

    for file in movement_files:
        movement_date_str = file.split('_')[2]
        movement_date = datetime.strptime(movement_date_str, "%Y%m%d").strftime("%Y-%m-%d")

        logging.debug(movement_date)

        movement_time_str = file.split('_')[3].split(".")[0]
        movement_time = datetime.strptime(movement_time_str, "%H%M%S").strftime("%H:%M:%S")

        logging.debug(movement_time)


        if movement_date == logical_date and movement_time >= logical_time:
            todays_files.append(file)

    logging.debug("Sales dates")

    for file in sales_files:
        sales_date_str = file.split('_')[1]
        sales_date = datetime.strptime(sales_date_str, "%Y%m%d").strftime("%Y-%m-%d")

        logging.debug(sales_date)

        sales_time_str = file.split('_')[2].split(".")[0]
        sales_time = datetime.strptime(sales_time_str, "%H%M%S").strftime("%H:%M:%S")

        logging.debug(sales_time)


        if sales_date == logical_date and sales_time >= logical_time:
            todays_files.append(file)


    logging.debug(f"Today's files: {todays_files}")

    return todays_files

def make_partition(source_dir, files, spark):

    """Function that partition the files by date."""

    logging.debug("Files to partition:")
    logging.debug(files)

    for file in files:

        if file.split('_')[0] == 'inventory':
                
            full_file_path = os.path.join(source_dir,file)

            logging.debug("Inventory Movements Files")
            logging.debug("Absolute file path: ")
            logging.debug(full_file_path)

            inventory_movements_df = spark.read.csv(
                path=full_file_path,
                header=True,
                inferSchema=True
            )

            logging.debug(f"File {file} loaded. Number of rows: {inventory_movements_df.count()}")

            movement_date_str = file.split('_')[2]
            movement_time_str = file.split('_')[3].split(".")[0]
            timestamp = datetime.strptime(movement_date_str + movement_time_str,"%Y%m%d%H%M%S")

            new_inventory_movements_df = inventory_movements_df.withColumn("ingestion_date",lit(timestamp))
            logging.info("=======================================================================")
            new_inventory_movements_df.select(["movement_id","product_id","sku","ingestion_date"]).show(5)

            output_dir = os.environ.get("IM_DESTINATION_DIR")

            logging.debug("Output Directory")
            logging.debug(output_dir)

            new_inventory_movements_df.write\
                                      .format('parquet')\
                                      .option("header","true")\
                                      .partitionBy("ingestion_date")\
                                      .mode("append")\
                                      .save(output_dir)
            
        elif file.split('_')[0] == 'sales':

            full_file_path = os.path.join(source_dir,file)

            logging.debug("Sales Files")
            logging.debug("Absolute file path: ")
            logging.debug(full_file_path)

            sales_df = spark.read.csv(
                path=full_file_path,
                header=True,
                inferSchema=True
            )

            logging.debug(f"File {file} loaded. Number of rows: {sales_df.count()}")

            sales_date_str = file.split('_')[1]
            sales_time_str = file.split('_')[2].split(".")[0]
            timestamp = datetime.strptime(sales_date_str + sales_time_str,"%Y%m%d%H%M%S")

            new_sales_df = sales_df.withColumn("ingestion_date",lit(timestamp))
            logging.debug("=======================================================================")
            new_sales_df.select(["order_id","source","status","ingestion_date"]).show(5) 

            output_dir = os.environ.get("S_DESTINATION_DIR")

            logging.debug("Output Directory")
            logging.debug(output_dir)

            new_sales_df.write\
                                      .format('parquet')\
                                      .option("header","true")\
                                      .partitionBy("ingestion_date")\
                                      .mode("append")\
                                      .save(output_dir)