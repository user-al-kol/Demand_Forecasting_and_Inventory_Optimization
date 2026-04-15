from pyspark.sql.types import StructType,StructField,StringType,TimestampType,DateType,DoubleType

def bronze_table_monitoring_schema():
    return """
        date TIMESTAMP,
        source_file STRING,
        rows INTEGER,
        problematic_rows INTEGER,
        safe_rows INTEGER,
        merge_key STRING,
        nulls_dropped INTEGER
"""

def inventory_movements_schema():
    return """
        movement_id STRING NOT NULL,
        movement_ts TIMESTAMP,
        movement_date DATE NOT NULL,
        movement_type STRING,
        product_id STRING,
        sku STRING,
        location_id STRING,
        location_code STRING,
        qty_delta DOUBLE,
        ref_order_id STRING,
        ref_order_type STRING,
        notes STRING,
        erp_export_ts TIMESTAMP,
        source_system STRING,
        processed_date TIMESTAMP 
    """

def null_bronze_inventory_movements_schema():
    return"""
        movement_id STRING NOT NULL,
        movement_ts TIMESTAMP,
        movement_date DATE NOT NULL,
        movement_type STRING,
        product_id STRING,
        sku STRING,
        location_id STRING,
        location_code STRING,
        qty_delta DOUBLE,
        ref_order_id STRING,
        ref_order_type STRING,
        notes STRING,
        erp_export_ts TIMESTAMP
    """
# def inventory_movements_schema():
#     return StructType([
#         StructField("movement_id", StringType(), False),
#         StructField("movement_ts", TimestampType(), True),
#         StructField("movement_date", DateType(), False),
#         StructField("movement_type", StringType(), True),
#         StructField("product_id", StringType(), True),
#         StructField("sku", StringType(), True),
#         StructField("location_id", StringType(), True),
#         StructField("location_code", StringType(), True),
#         StructField("qty_delta", DoubleType(), True),
#         StructField("ref_order_id", StringType(), True),
#         StructField("ref_order_type", StringType(), True),
#         StructField("notes", StringType(), True),
#         StructField("erp_export_ts", TimestampType(), True),
#         StructField("source_system", StringType(), False),
#         StructField("processed_date",TimestampType(),False)
#     ])


def sales_schema():
    return """
        order_id STRING NOT NULL,
        order_ts TIMESTAMP,
        order_date DATE NOT NULL,
        status STRING,
        source STRING,
        customer_id STRING,
        customer_code STRING,
        customer_name STRING,
        customer_type STRING,
        location_id STRING,
        location_code STRING,
        product_id STRING,
        sku STRING,
        qty_ordered DOUBLE,
        qty_fulfilled DOUBLE,
        unit_price_eur DOUBLE,
        line_total_eur DOUBLE,
        order_total_eur DOUBLE,
        erp_export_ts TIMESTAMP,
        source_system STRING,
        processed_date TIMESTAMP
    """


def null_bronze_sales_schema():
    return """
        order_id STRING NOT NULL,
        order_ts TIMESTAMP,
        order_date DATE NOT NULL,
        status STRING,
        source STRING,
        customer_id STRING,
        customer_code STRING,
        customer_name STRING,
        customer_type STRING,
        location_id STRING,
        location_code STRING,
        product_id STRING,
        sku STRING,
        qty_ordered DOUBLE,
        qty_fulfilled DOUBLE,
        unit_price_eur DOUBLE,
        line_total_eur DOUBLE,
        order_total_eur DOUBLE,
        erp_export_ts TIMESTAMP
    """