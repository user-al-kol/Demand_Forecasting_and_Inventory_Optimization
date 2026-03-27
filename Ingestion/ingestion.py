import os

files = os.listdir("SOURCE_DIR")
logical_date = os.environ.get("LOGICAL_DATE")

for file in files:

    print(f"Current files: {file}")

print(f"Airflow logical date: {logical_date}")