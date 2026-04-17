import os
import time
from datetime import datetime
from pyspark.sql import Row  
from pyspark.sql.functions import lit
from common.utils import parse_columns, find_latest_file
from common.schema import bronze_table_monitoring_schema
from common.config import DELTA_PATH

# def read_or_create_bronze_problematic_rows():

#     spark.sql(f"""
#     CREATE TABLE IF NOT EXISTS {table_name} (
#         {schema}
#     )
#     USING DELTA
#     LOCATION '{table_path}'
# """)

def update_problematic_table(df,table_name,spark,logger):

    problematic_table_name = f"problematic_{table_name}"

    table_path = f"{DELTA_PATH}/{problematic_table_name}"

    try:
        spark.read.format("delta").load(table_path)
        logger.info(f"Delta table {problematic_table_name} exists at path {table_path}.")

        # Register in metastore
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {problematic_table_name} 
            USING DELTA
            LOCATION '{table_path}'
        """)

        logger.info(f"Registered {problematic_table_name} at {table_path}")

        df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(problematic_table_name)
        
    except:

        df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(problematic_table_name)


def update_null_table(df,table_name,spark,logger):
    """Function that stores all the dropped row because of null merge keys to a table."""

    null_table_name = f"null_{table_name}"

    table_path = f"{DELTA_PATH}/{null_table_name}"

    try:
        spark.read.format("delta").load(table_path)
        logger.info(f"Delta table {null_table_name} exists at path {table_path}.")

        # Register in metastore
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {null_table_name} 
            USING DELTA
            LOCATION '{table_path}'
        """)

        logger.info(f"Registered {null_table_name} at {table_path}")

        df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(null_table_name)
        
    except Exception as e:

        df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(null_table_name)


def process_dataset(config, present_date, spark, logger, logical_date, source_dir):
    """Function that processes the raw partition data and upserts them into the bronze tables."""

    total_problematic = 0
    total_safe = 0
    
    monitoring_date, source_file, number_of_rows = partition(
        source_dir, config.destination_dir, config.file, spark, logger
    )

    latest_file = find_latest_file(config.source_partitioned, logical_date, logger)

    df = spark.read.parquet(latest_file)


    df_transformed = add_processed_date_source_system(
        df,
        config.entity,
        present_date,
        "ERP",
        spark
    )

    df_clean,df_null,null_counts = drop_null_keys(df_transformed, config.keys, logger)

    if df_null.count() > 0:
        update_null_table(df_null,config.table,spark,logger)


    problematic_rows, safe_rows = detect_merge_conflicts_with_target(
        df_clean,
        config.table,
        config.schema_fn(),
        config.keys,
        spark,
        logger
    )

    total_problematic = problematic_rows.count()
    total_safe = safe_rows.count()

    bronze_table_monitoring_insert(
        monitoring_date,
        source_file,
        number_of_rows,
        config.keys,
        null_counts,
        total_problematic,
        total_safe,
        spark,
        logger
    )

    if total_problematic > 0:
        update_problematic_table(problematic_rows,config.table,spark,logger)

    df_to_upsert = df_clean if total_problematic == 0 else safe_rows

    upsert(
        df_to_upsert,
        config.table,
        config.schema_fn(),
        config.keys,
        spark,
        logger
    )

    return {
        "rows_read": number_of_rows,
        "null_counts": null_counts,
        "problematic_rows": total_problematic,
        "safe_rows": total_safe
    }


def process_with_retry(config, retries, delay, **kwargs):
    for attempt in range(1, retries + 1):
        try:
            return process_dataset(config=config, **kwargs)
        except Exception as e:
            kwargs["logger"].error(
                f"[{config.entity}] Attempt {attempt} failed: {str(e)}"
            )
            if attempt == retries:
                raise
            time.sleep(delay)


def bronze_table_monitoring_insert(monitoring_date,source_file,number_of_rows,merge_keys,null_counts,problematic_rows,safe_rows,spark,logger):
    """Function that updates the bronze monitoring table."""

    schema = bronze_table_monitoring_schema()

    read_or_create_delta_table("bronze_table_monitoring", schema, spark, logger)

    for key in merge_keys:

        monitoring_data = [Row(
            date=monitoring_date,
            source_file=source_file,
            rows=number_of_rows,
            problematic_rows=problematic_rows,
            safe_rows=safe_rows,
            merge_key=key,
            nulls_dropped=null_counts.get(key, 0)   
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
                inferSchema=False
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


def add_processed_date_source_system(df, table_name, present_date,source_system, spark):
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
    return df.filter(" AND ".join([f"{k} IS NOT NULL" for k in merge_keys])),df.filter(" OR ".join([f"{k} IS NULL" for k in merge_keys])),null_counts
    #return df.filter(" AND ".join([f"{k} IS NOT NULL" for k in merge_keys])),null_counts


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
        logger.info(f"Delta table {table_name} doesn't exist at path {table_path}.")

        logger.info(f"Creating new Delta table {table_name} at path {table_path}.")

        empty_df = spark.createDataFrame([], schema=schema)

        empty_df.write \
            .format("delta") \
            .mode("errorifexists") \
            .save(table_path)
        logger.info(f"{table_name}'s schema: ")
        
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