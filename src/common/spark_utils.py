import os

from datetime import datetime  
from pyspark.sql.functions import lit
from pyspark.sql.utils import AnalysisException
from common.utils import parse_columns


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

# def upsert(df,table_name,spark,logger):

#     df.createOrReplaceTempView("new_data")

#     try:
#         spark.read.format("delta").load(f"/app/bronze_delta_tables/{table_name}").createOrReplaceTempView("bronze_data")
#         logger.info(f"{table_name} data were successfully read and written into the bronze_data Temp View.")
#     except Exception as e:
#         logger.warning(f"Exception: {e} - {table_name} does not exist.")
#         logger.info(f"Creating table {table_name}")

#         create_table = f"""
#             CREATE TABLE IF NOT EXISTS {table_name} ({inventory_movements_schema})
#         """
#         spark.sql(create_table)
#         spark.read.format("delta").load(f"/app/bronze_delta_tables/{table_name}").createOrReplaceTempView("bronze_data")

#         logger.info(f"{table_name} were created successfully and was written into the bronze_data Temp View.")

#     merge = """
#         MERGE INTO bronze_data AS target
#         USING new_data AS source
#         ON target.movement_id = source.movement_id 
#         AND target.movement_date = source.movement_date

#         WHEN MATCHED THEN
#             UPDATE SET
#                 target.movement_id        = source.movement_id,
#                 target.movement_ts        = source.movement_ts,
#                 target.movement_date      = source.movement_date,
#                 target.movement_type      = source.movement_type,
#                 target.product_id         = source.product_id,
#                 target.sku                = source.sku,
#                 target.location_id        = source.location_id,
#                 target.location_code      = source.location_code,
#                 target.qty_delta          = source.qty_delta,
#                 target.ref_order_id       = source.ref_order_id,
#                 target.ref_order_type     = source.ref_order_type,
#                 target.notes              = source.notes,
#                 target.erp_export_ts      = source.erp_export_ts,
#                 target.processed_date     = source.processed_date

#         WHEN NOT MATCHED THEN
#             INSERT (
#                 movement_id,
#                 movement_ts,
#                 movement_date,
#                 movement_type,
#                 product_id,
#                 sku,
#                 location_id,
#                 location_code,
#                 qty_delta,
#                 ref_order_id,
#                 ref_order_type,
#                 notes,
#                 erp_export_ts,
#                 processed_date
#             )
#             VALUES (
#                 source.movement_id,
#                 source.movement_ts,
#                 source.movement_date,
#                 source.movement_type,
#                 source.product_id,
#                 source.sku,
#                 source.location_id,
#                 source.location_code,
#                 source.qty_delta,
#                 source.ref_order_id,
#                 source.ref_order_type,
#                 source.notes,
#                 source.erp_export_ts,
#                 source.processed_date
#             )
# """

# def upsert(df, table_name, spark, logger):

#     def parse_columns(schema_str):
#         return [col.strip().split()[0] for col in schema_str.strip().split(",")]

#     def get_schema(table_name):
#         if table_name == "bronze_inventory_movements":
#             return inventory_movements_schema()
#         elif table_name == "bronze_sales":
#             return sales_schema()
#         else:
#             raise ValueError(f"Unknown table: {table_name}")

#     def get_merge_keys(table_name):
#         if table_name == "bronze_inventory_movements":
#             return ["movement_id", "movement_date"]
#         elif table_name == "bronze_sales":
#             return ["order_id", "order_date"]
#         else:
#             raise ValueError(f"No merge keys defined for {table_name}")

#     delta_path = f"/app/bronze_delta_tables/{table_name}"
#     df.createOrReplaceTempView("new_data")

#     schema_str = get_schema(table_name)
#     columns = parse_columns(schema_str)
#     merge_keys = get_merge_keys(table_name)

# # Ensure Delta table exists
#     try:
#         spark.read.format("delta").load(delta_path)
#         logger.info(f"{table_name} exists at {delta_path}.")
#     except AnalysisException:
#         logger.info(f"{table_name} does not exist. Creating empty Delta table at {delta_path}...")
#         # Initialize empty Delta table with the schema
#         empty_df = spark.createDataFrame([], schema=inventory_movements_spark_schema)
#         empty_df.write.format("delta").mode("append").save(delta_path)

    # # Build ON condition
    # on_clause = " AND ".join(
    #     [f"target.{k} = source.{k}" for k in merge_keys]
    # )

    # # Build UPDATE SET (exclude keys if you want)
    # update_set = ",\n        ".join(
    #     [f"target.{col} = source.{col}" for col in columns]
    # )

    # # Build INSERT columns
    # insert_cols = ",\n        ".join(columns)

    # # Build VALUES
    # insert_vals = ",\n        ".join([f"source.{col}" for col in columns])

    # merge = f"""
    #     MERGE INTO delta.`{delta_path}` AS target
    #     USING new_data AS source
    #     ON {on_clause}

    #     WHEN MATCHED THEN
    #         UPDATE SET
    #             {update_set}

    #     WHEN NOT MATCHED THEN
    #         INSERT (
    #             {insert_cols}
    #         )
    #         VALUES (
    #             {insert_vals}
    #         )
    # """

    # logger.info(f"Executing MERGE for {table_name}")
    # spark.sql(merge)


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


def read_or_create_delta_table(table_name, schema, spark ,logger):
    """Function that reads or creates a delta table."""
    try:
        spark.table(table_name)
        logger.info(f"Table {table_name} exists.")

    except AnalysisException:

        logger.info(f"Table {table_name} does not exist. Creating empty Delta table...")

        try:
            empty_df = spark.createDataFrame([], schema=schema)
            empty_df.write.format("delta").mode("overwrite").saveAsTable(table_name)
            logger.info(f"Empty table {table_name} created.")

        except ValueError:

            raise ValueError(f"Cannot create table {table_name}, schema unknown.")
        
def display_bronze_tables(spark):

    bronze_inventory_movements = spark.read.table("bronze_inventory_movements")
    bronze_sales = spark.read.table("bronze_sales")

    bronze_inventory_movements.select(["movement_id","movement_ts","movement_date","movement_type"]).show(5)
    bronze_inventory_movements.select(["product_id","sku","location_id","location_code"]).show(5)
    bronze_inventory_movements.select(["qty_delta","ref_order_id","ref_order_type","notes"]).show(5)
    bronze_inventory_movements.select(["erp_export_ts","processed_date"]).show(5)
    bronze_inventory_movements.printSchema()

    bronze_sales.select(["order_id","order_ts","order_date","status"]).show(5)
    bronze_sales.select(["source","customer_id","customer_code","customer_name"]).show(5)
    bronze_sales.select(["customer_type","location_id","location_code","product_id"]).show(5)
    bronze_sales.select(["sku","qty_ordered","qty_fulfilled","unit_price_eur"]).show(5)
    bronze_sales.select(["line_total_eur","order_total_eur","erp_export_ts","processed_date"]).show(5)
    bronze_sales.printSchema()