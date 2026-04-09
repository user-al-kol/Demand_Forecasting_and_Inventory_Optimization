from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, DateType

def inventory_movements_spark_schema():
    return StructType([
        StructField("movement_id", StringType(), False),
        StructField("movement_ts", TimestampType(), True),
        StructField("movement_date", DateType(), False),
        StructField("movement_type", StringType(), True),

        StructField("product_id", StringType(), True),
        StructField("sku", StringType(), True),
        StructField("location_id", StringType(), True),
        StructField("location_code", StringType(), True),

        StructField("qty_delta", DoubleType(), True),
        StructField("ref_order_id", StringType(), True),
        StructField("ref_order_type", StringType(), True),
        StructField("notes", StringType(), True),

        StructField("erp_export_ts", TimestampType(), True),
        StructField("processed_date", DateType(), True)
    ])