import os
import logging
from datetime import datetime, timedelta

SOURCE_DIR = os.environ.get("SOURCE_DIR")
files = os.listdir(SOURCE_DIR)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# Get the Logical Date and split it to date and time
logical_date_time = os.environ.get("LOGICAL_DATE")
logical_date = logical_date_time.split("T")[0]
logical_time = logical_date_time.split("T")[1].split("+")[0]
# Take the Logical Time and add one second
dt = datetime.strptime(logical_time, "%H:%M:%S")
dt_plus_one_sec = dt + timedelta(seconds=1)
new_logical_time = dt_plus_one_sec.strftime("%H:%M:%S")

movement_files = []
sales_files = []

for file in files:

    logging.debug(f"Current files: {file}")

    if file.split('_')[0] == 'inventory':
        movement_files.append(file)
    else:
        sales_files.append(file)


logging.info(f"Airflow logical date: {logical_date_time}")

logging.debug(f'Logical Date Time: {logical_date_time.split("T")}')


logging.debug(f"Logical date: {logical_date}")
logging.debug(f"Logical time: {logical_time}")

logging.info("Movement files")
logging.info(movement_files)
logging.info("Sales files")
logging.info(sales_files)

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


logging.info(f"Today's files: {todays_files}")