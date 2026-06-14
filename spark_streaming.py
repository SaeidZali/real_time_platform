from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, row_number, current_timestamp
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    LongType
)

# ------------------------------------------------------------------
# Spark Session
# ------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("oracle-cdc-to-iceberg-stream")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ------------------------------------------------------------------
# Database and Iceberg Target Table
# ------------------------------------------------------------------
spark.sql("CREATE DATABASE IF NOT EXISTS nessie.oracle_cdc_db")

spark.sql("""
CREATE TABLE IF NOT EXISTS nessie.oracle_cdc_db.customers (
    id INT,
    name STRING,
    last_updated TIMESTAMP
)
USING iceberg
PARTITIONED BY (bucket(16, id))
""")

import clickhouse_connect
client = clickhouse_connect.get_client(
    host='clickhouse',   # 👈 NOT localhost
    port=8123,
    username='default',
    password='clickhouse123'
)
client.command("SET allow_experimental_database_iceberg = 1")
desc = spark.sql("DESCRIBE EXTENDED nessie.oracle_cdc_db.customers")
location = (
    desc.filter("col_name = 'Location'")
    .select("data_type")
    .collect()[0][0]
)
print("📍 Table location:", location)
ch_location = location.replace("s3://oracle-cdc/", "http://minio:9000/oracle-cdc/")
client.command(f"""
CREATE VIEW IF NOT EXISTS customers_view AS
SELECT * FROM icebergS3(
    '{ch_location}'
)
""")

# ------------------------------------------------------------------
# Debezium CDC Schema
# ------------------------------------------------------------------
cdc_schema = StructType([
    StructField("before", StructType([
        StructField("ID", IntegerType()),
        StructField("NAME", StringType())
    ])),
    StructField("after", StructType([
        StructField("ID", IntegerType()),
        StructField("NAME", StringType())
    ])),
    StructField("op", StringType()),
    StructField("ts_ms", LongType())
])

# ------------------------------------------------------------------
# foreachBatch Function: Optimized with Iceberg MERGE
# ------------------------------------------------------------------
def process_batch(batch_df, batch_id):
    """
    Processes one Spark Structured Streaming micro-batch.
    """
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: empty")
        return

    print(f"Batch {batch_id}: processing started, raw count = {batch_df.count()}")

    # Keep only the latest event per customer id inside this micro-batch
    window_spec = Window.partitionBy("id").orderBy(col("ts_ms").desc())

    latest_changes = (
        batch_df
        .filter(col("id").isNotNull())
        .filter(col("op").isin("c", "u", "d"))
        .withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn")
        .select("id", "name", "op", "ts_ms")
    )

    changes_count = latest_changes.count()
    print(f"Batch {batch_id}: latest changes count = {changes_count}")

    if changes_count == 0:
        print(f"Batch {batch_id}: no CDC operations to process after deduplication")
        return

    # Use GLOBAL temporary view to ensure visibility in MERGE
    latest_changes.createGlobalTempView("cdc_changes_batch")
    print(f"Batch {batch_id}: global temporary view created")

    # Execute atomic MERGE operation using global_temp
    spark.sql("""
    MERGE INTO nessie.oracle_cdc_db.customers AS target
    USING global_temp.cdc_changes_batch AS source
    ON target.id = source.id

    WHEN MATCHED AND source.op = 'd' THEN
        DELETE

    WHEN MATCHED AND source.op = 'u' THEN
        UPDATE SET
            target.name = source.name,
            target.last_updated = current_timestamp()

    WHEN NOT MATCHED AND source.op = 'c' THEN
        INSERT (id, name, last_updated)
        VALUES (source.id, source.name, current_timestamp())
    """)

    print(f"Batch {batch_id}: merge completed successfully")

    # Clean up the global temp view
    spark.catalog.dropGlobalTempView("cdc_changes_batch")


# ------------------------------------------------------------------
# Streaming Source
# ------------------------------------------------------------------
stream_df = (
    spark.readStream
    .schema(cdc_schema)
    .option("maxFilesPerTrigger", 500)
    .parquet("s3a://oracle-cdc/topics/server1.C__DBZUSER.CUSTOMERS")
)

# ------------------------------------------------------------------
# Flatten Debezium Events
# ------------------------------------------------------------------
cdc_df = (
    stream_df
    .select(
        coalesce(col("after.ID"), col("before.ID")).cast("int").alias("id"),
        coalesce(col("after.NAME"), col("before.NAME")).cast("string").alias("name"),
        col("op").cast("string").alias("op"),
        col("ts_ms").cast("long").alias("ts_ms")
    )
    .filter(col("id").isNotNull())
    .filter(col("op").isNotNull())
)

# ------------------------------------------------------------------
# Start Streaming Query
# ------------------------------------------------------------------
# Use a fresh checkpoint location
query = (
    cdc_df.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", "s3a://oracle-cdc/checkpoints/customers_merge_v3")
    .trigger(processingTime="3 seconds")
    .start()
)

print(f"Streaming query started. Query ID: {query.id}")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Stopping streaming query...")
    query.stop()
except Exception as e:
    print(f"Streaming query failed: {e}")
    query.stop()
    raise
finally:
    spark.stop()
