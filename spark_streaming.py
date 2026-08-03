from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, row_number, current_timestamp
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    LongType,
    DoubleType,
    DateType
)
import clickhouse_connect
import time
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
    provider STRING,
    quantity BIGINT,
    gd_barcode STRING,
    gd_name STRING,
    p_date STRING,
    invoice_id STRING,
    ncode_masked STRING,
    mobile_masked STRING,
    year BIGINT,
    month BIGINT,
    day BIGINT,
    city STRING,
    province STRING,
    m_date DATE,
    latitude DOUBLE,
    longitude DOUBLE,
    province_code STRING,
    month_name STRING,
    last_updated TIMESTAMP
)
USING iceberg
PARTITIONED BY (bucket(16, id))
""")

# ------------------------------------------------------------------
# Debezium CDC Schema
# ------------------------------------------------------------------
cdc_schema = StructType([

    StructField("before", StructType([
        StructField("ID", IntegerType()),
        StructField("PROVIDER", StringType()),
        StructField("QUANTITY", LongType()),
        StructField("GD_BARCODE", StringType()),
        StructField("GD_NAME", StringType()),
        StructField("P_DATE", StringType()),
        StructField("INVOICE_ID", StringType()),
        StructField("NCODE_MASKED", StringType()),
        StructField("MOBILE_MASKED", StringType()),
        StructField("YEAR", LongType()),
        StructField("MONTH", LongType()),
        StructField("DAY", LongType()),
        StructField("CITY", StringType()),
        StructField("PROVINCE", StringType()),
        StructField("M_DATE", DateType()),
        StructField("LATITUDE", DoubleType()),
        StructField("LONGITUDE", DoubleType()),
        StructField("PROVINCE_CODE", StringType()),
        StructField("MONTH_NAME", StringType())
    ])),

    StructField("after", StructType([
        StructField("ID", IntegerType()),
        StructField("PROVIDER", StringType()),
        StructField("QUANTITY", LongType()),
        StructField("GD_BARCODE", StringType()),
        StructField("GD_NAME", StringType()),
        StructField("P_DATE", StringType()),
        StructField("INVOICE_ID", StringType()),
        StructField("NCODE_MASKED", StringType()),
        StructField("MOBILE_MASKED", StringType()),
        StructField("YEAR", LongType()),
        StructField("MONTH", LongType()),
        StructField("DAY", LongType()),
        StructField("CITY", StringType()),
        StructField("PROVINCE", StringType()),
        StructField("M_DATE", DateType()),
        StructField("LATITUDE", DoubleType()),
        StructField("LONGITUDE", DoubleType()),
        StructField("PROVINCE_CODE", StringType()),
        StructField("MONTH_NAME", StringType())
    ])),

    StructField("op", StringType()),
    StructField("ts_ms", LongType())
])

# ------------------------------------------------------------------
# Function to create ClickHouse view with retry logic
# ------------------------------------------------------------------
def create_clickhouse_view_with_retry(max_retries=10, delay=5):
    """
    Creates ClickHouse view on top of Iceberg table with retry logic.
    Waits for Iceberg table metadata to be available before creating view.
    """
    print("🔄 Attempting to create ClickHouse view...")
    
    for attempt in range(max_retries):
        try:
            # Connect to ClickHouse
            client = clickhouse_connect.get_client(
                host='clickhouse',
                port=8123,
                username='default',
                password='clickhouse123'
            )
            
            # Enable Iceberg experimental feature
            client.command("SET allow_experimental_database_iceberg = 1")
            print(f"✓ Connected to ClickHouse (attempt {attempt + 1}/{max_retries})")
            
            # Get Iceberg table location
            desc = spark.sql("DESCRIBE EXTENDED nessie.oracle_cdc_db.customers")
            location = (
                desc.filter("col_name = 'Location'")
                .select("data_type")
                .collect()[0][0]
            )
            print(f"📍 Table location: {location}")
            
            # Convert S3 path to HTTP URL for ClickHouse
            ch_location = location.replace("s3://oracle-cdc/", "http://minio:9000/oracle-cdc/")
            print(f"📍 ClickHouse location: {ch_location}")
            
            # Test if Iceberg metadata exists by trying to query a small sample
            # This will fail if metadata isn't ready yet
            test_query = f"""
            SELECT COUNT(*) FROM icebergS3('{ch_location}')
            """
            test_result = client.query(test_query)
            print(f"✓ Iceberg table is accessible, row count: {test_result.result_rows[0][0]}")
            
            # Create or replace the view
            create_view_sql = f"""
            CREATE OR REPLACE VIEW customers_view AS
            SELECT 
                id,
                name,
                last_updated
            FROM icebergS3('{ch_location}')
            """
            
            client.command(create_view_sql)
            print("✅ ClickHouse view 'customers_view' created/updated successfully!")
            
            # Verify the view works
            verify_query = "SELECT COUNT(*) FROM customers_view"
            count = client.query(verify_query).result_rows[0][0]
            print(f"✓ View verification successful, contains {count} rows")
            
            return True
            
        except Exception as e:
            print(f"❌ Attempt {attempt + 1}/{max_retries} failed: {str(e)}")
            
            if attempt < max_retries - 1:
                print(f"⏳ Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print("⚠️ Failed to create ClickHouse view after all retries.")
                print("   The streaming job will continue, but ClickHouse view won't be available.")
                print("   You can create the view manually later using:")
                print(f"   CREATE VIEW customers_view AS SELECT * FROM icebergS3('{ch_location}')")
                return False

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
        .select(
        "id",
        "provider",
        "quantity",
        "gd_barcode",
        "gd_name",
        "p_date",
        "invoice_id",
        "ncode_masked",
        "mobile_masked",
        "year",
        "month",
        "day",
        "city",
        "province",
        "m_date",
        "latitude",
        "longitude",
        "province_code",
        "month_name",
        "op",
        "ts_ms"
    )
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
            target.provider = source.provider,
            target.quantity = source.quantity,
            target.gd_barcode = source.gd_barcode,
            target.gd_name = source.gd_name,
            target.p_date = source.p_date,
            target.invoice_id = source.invoice_id,
            target.ncode_masked = source.ncode_masked,
            target.mobile_masked = source.mobile_masked,
            target.year = source.year,
            target.month = source.month,
            target.day = source.day,
            target.city = source.city,
            target.province = source.province,
            target.m_date = source.m_date,
            target.latitude = source.latitude,
            target.longitude = source.longitude,
            target.province_code = source.province_code,
            target.month_name = source.month_name,
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

print("\n" + "="*60)
print("SETTING UP CLICKHOUSE VIEW")
print("="*60)
create_clickhouse_view_with_retry(max_retries=15, delay=10)

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
