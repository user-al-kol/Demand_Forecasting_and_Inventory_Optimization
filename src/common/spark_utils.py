import os

from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql import Row  
from pyspark.sql.functions import lit
from pyspark.sql.utils import AnalysisException
from common.utils import parse_columns
from common.schema import bronze_table_monitoring_schema
from common.config import DELTA_PATH

def bronze_table_monitoring_insert(monitoring_date,source_file,number_of_rows,merge_keys,null_counts,problematic_rows,safe_rows,spark,logger):
    
    schema = bronze_table_monitoring_schema()

    read_or_create_delta_table("bronze_table_monitoring", schema, spark, logger)

    for key in merge_keys:

        monitoring_data = [Row(
            date=monitoring_date,
            source_file=source_file,
            rows=number_of_rows,
            merge_key=key,
            nulls_dropped=null_counts.get(key, 0),
            problematic_rows=problematic_rows,
            safe_rows=safe_rows
        )]

        monitoring_df = spark.createDataFrame(
            monitoring_data,
            schema=schema
        )
        
        monitoring_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable("bronze_table_monitoring")

    logger.info(f"Inserted monitoring rows for {merge_keys} merge keys.")
    

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
        number_of_rows = file_df.count()

        if number_of_rows > 1:
            source_file = file
            number_of_rows = file_df.count()
            logger.info(f"File {file} loaded. Number of rows: {number_of_rows}")

            file_date_str = file.split('_')[-2]
            file_time_str = file.split('_')[-1].split(".")[0]
            timestamp = datetime.strptime(file_date_str + file_time_str,"%Y%m%d%H%M%S")
            monitoring_date = timestamp

            new_file_df = file_df.withColumn("ingestion_date",lit(timestamp))

            new_file_df.write\
                                .format('parquet')\
                                .option("header","true")\
                                .partitionBy("ingestion_date")\
                                .mode("append")\
                                .save(destination_dir)
            
            return monitoring_date,source_file,number_of_rows 
        else:
            logger.warning(f"{file} contains only header row, skipping.")
            return None, None, None
    else:
        logger.warning(f"{file} is not a CSV file, skipping.")
        return None, None, None


def add_processed_date(df, table_name, present_date,source_system, spark):
    """Function that adds processed_date and source_system columns."""
    
    df.createOrReplaceTempView(table_name)

    return spark.sql(f"""
            SELECT *,
                TIMESTAMP('{present_date}') AS processed_date,
                '{source_system}' AS source_system
            FROM {table_name}
    """)


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

def drop_null_keys(df, merge_keys, logger):
    """Function that drop the row where the business keys are Null."""
    null_counts = {} 

    for k in merge_keys:
        null_count = df.filter(f"{k} IS NULL").count()
        if null_count > 0:
            logger.warning(f"Dropping {null_count} rows with NULL in business key: {k}")
        null_counts[k] = null_count

    return df.filter(" AND ".join([f"{k} IS NOT NULL" for k in merge_keys])),null_counts


def upsert(df, table_name, schema, merge_keys, spark, logger):
    """Function that upserts data."""
    df.createOrReplaceTempView("new_data")

    columns = parse_columns(schema)
    merge_keys = merge_keys

    # Ensure Delta table exists
    read_or_create_delta_table(table_name,schema,spark,logger)

    # Build ON condition
    on_clause = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])

    # Build UPDATE SET
    update_set = ",\n        ".join([f"target.{col} = source.{col}" for col in columns])

    # Build INSERT columns and VALUES
    insert_cols = ",\n        ".join(columns)
    insert_vals = ",\n        ".join([f"source.{col}" for col in columns])

    # MERGE using table name
    merge_sql = f"""
        MERGE INTO {table_name} AS target
        USING new_data AS source
        ON {on_clause}

        WHEN MATCHED THEN
            UPDATE SET
                {update_set}

        WHEN NOT MATCHED THEN
            INSERT (
                {insert_cols}
            )
            VALUES (
                {insert_vals}
            )
    """

    logger.info(f"Executing MERGE for {table_name}")
    spark.sql(merge_sql)


def read_or_create_delta_table(table_name, schema, spark, logger):

    table_path = f"{DELTA_PATH}/{table_name}"

    try:
        spark.read.format("delta").load(table_path)
        logger.info(f"Delta table {table_name} exists at path {table_path}.")

    except:
        logger.info(f"Creating new Delta table {table_name} at path {table_path}.")

        empty_df = spark.createDataFrame([], schema=schema)

        empty_df.write \
            .format("delta") \
            .mode("errorifexists") \
            .save(table_path)

    # always register in metastore
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        USING DELTA
        LOCATION '{table_path}'
    """)
    logger.info(f"Registered {table_name} at {table_path}")


def detect_merge_conflicts_with_target(source_df,target_name,schema,merge_keys,spark,logger):

    read_or_create_delta_table(target_name,schema,spark,logger)

    target_df = spark.read.table(f"{target_name}")

    joined = source_df.alias("source").join(target_df.alias("target"),on=merge_keys,how="inner")

    conflicting_keys = (
        joined.groupBy([f"source.{k}" for k in merge_keys])
            .count()
            .filter("count > 1")
        )
    problematic_rows = source_df.join(conflicting_keys, on=merge_keys, how="inner")\
                        .withColumn("error_reason", lit("MULTIPLE_MATCH"))
    
    safe_rows = source_df.join(conflicting_keys, on=merge_keys, how="left_anti")

    return problematic_rows,safe_rows

        
def display_bronze_tables(spark):

    bronze_inventory_movements = spark.read.table("bronze_inventory_movements")
    bronze_sales = spark.read.table("bronze_sales")

    bronze_inventory_movements.select(["movement_id","movement_ts","movement_date","movement_type"]).show(5)
    bronze_inventory_movements.select(["product_id","sku","location_id","location_code"]).show(5)
    bronze_inventory_movements.select(["qty_delta","ref_order_id","ref_order_type","notes"]).show(5)
    bronze_inventory_movements.select(["erp_export_ts","source_system","processed_date"]).show(5)
    bronze_inventory_movements.printSchema()

    bronze_sales.select(["order_id","order_ts","order_date","status"]).show(5)
    bronze_sales.select(["source","customer_id","customer_code","customer_name"]).show(5)
    bronze_sales.select(["customer_type","location_id","location_code","product_id"]).show(5)
    bronze_sales.select(["sku","qty_ordered","qty_fulfilled","unit_price_eur"]).show(5)
    bronze_sales.select(["line_total_eur","order_total_eur","erp_export_ts","source_system","processed_date"]).show(5)
    bronze_sales.printSchema()