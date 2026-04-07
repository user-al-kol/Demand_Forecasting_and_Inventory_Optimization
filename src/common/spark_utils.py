import os

from datetime import datetime
from pyspark.sql import SparkSession  
from pyspark.sql.functions import lit


def partition(source_dir,destination_dir,file,spark,logger):
    """Function that partitions a given file list by date"""
    
    #for file in files:

    if file.endswith(".csv"):
    
        input_path = os.path.join(source_dir,file)

        file_df = spark.read.csv(
                path=input_path,
                header=True,
                inferSchema=True
            )

        if file_df.count() > 1:

            logger.info(f"File {file} loaded. Number of rows: {file_df.count()}")

            file_date_str = file.split('_')[-2]
            file_time_str = file.split('_')[-1].split(".")[0]
            timestamp = datetime.strptime(file_date_str + file_time_str,"%Y%m%d%H%M%S")

            new_file_df = file_df.withColumn("ingestion_date",lit(timestamp))

            new_file_df.write\
                                .format('parquet')\
                                .option("header","true")\
                                .partitionBy("ingestion_date")\
                                .mode("append")\
                                .save(destination_dir)
        else:
            logger.warning(f"{file} contains only header row, skipping.")

    else:
        logger.warning(f"{file} is not a CSV file, skipping.")

    return None


def add_processed_date_deduplicate(df, table_name,partition_cols, present_date, spark):
    """Function that add processed_date column and deduplicates"""

    df.createOrReplaceTempView(table_name)

    # Build PARTITION BY clause dynamically
    partition_clause = (
    "PARTITION BY " + ", ".join(partition_cols)
    if partition_cols else "")

    return spark.sql(f"""
        WITH add_date AS (
            SELECT *,
                TIMESTAMP('{present_date}') AS processed_date
            FROM {table_name}
        ),
        ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    {partition_clause}
                    ORDER BY processed_date DESC
                ) AS rn
            FROM add_date
        )
        SELECT *
        FROM ranked
        WHERE rn = 1
    """)