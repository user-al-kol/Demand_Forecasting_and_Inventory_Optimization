import os
import logging
from datetime import datetime
from urllib.parse import unquote

def get_logger(level):
    """Function that sets the logger"""
    logging.basicConfig(
    level=level,
    format="%(asctime)s [%(levelname)s] %(message)s"
    )
    return logging.getLogger(__name__)

def find_latest_file(source_dir, logical_date, logger):
    """ Function to find the latest files in order to append them into the delta lake."""

    files = os.listdir(source_dir)

    logger.info(f"Files found: {files}")

    for file in files:
        if 'ingestion_date' in file:
            file_date = file.split("=")[1]
            decoded = unquote(file_date)

            dt = datetime.strptime(decoded, "%Y-%m-%d %H:%M:%S")
            iso_file_date = dt.isoformat()

            if iso_file_date >= logical_date:
                logger.info(f"Selected file: {file}")
                return os.path.join(source_dir, file)

    return None

def get_todays_files(source_dir,logical_date_time,logger):
    """ Function to recognize which are today's files in order to ingest them."""

    files = os.listdir(source_dir)

    # Get the Logical Date and split it to date and time
    logger.info(f"Logical Date: {logical_date_time}")
    logical_date = logical_date_time.split("T")[0]
    logical_time = logical_date_time.split("T")[1].split("+")[0]

    logger.debug(f"The files are: {files}")

    todays_files = []

    for file in files:
        if file.endswith(".csv"):
            file_date_str = file.split("_")[-2]
            file_date = datetime.strptime(file_date_str, "%Y%m%d").strftime("%Y-%m-%d")

            file_time_str = file.split("_")[-1].split(".")[0]
            file_time = datetime.strptime(file_time_str, "%H%M%S").strftime("%H:%M:%S")

            if file_date == logical_date and file_time >= logical_time:
                todays_files.append(file)
        else:
            logging.warning(f"{file} is no a CSV file, skipping.")

    logger.debug(f"Today's files: {todays_files}")

    return todays_files

def divide_files (todays_files):
    """Divides the files into inventory_movement_file and sales_file"""

    inventory_movement_file = None # This way I don't get UnboundLocalError: local variable 'inventory_movement_file' referenced before assignment
    sales_file = None   # This way I don't get UnboundLocalError: local variable 'sales_file' referenced before assignment
    
    for file in todays_files:

        prefix = file.split('_')[0]

        if prefix == 'inventory':
            inventory_movement_file = file

        elif prefix == 'sales':
            sales_file = file

    return inventory_movement_file,sales_file

def parse_columns(schema_str):
    """Function that parses the schema and extracts the columns."""
    return [col.strip().split()[0] for col in schema_str.strip().split(",")]