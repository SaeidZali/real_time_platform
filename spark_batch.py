from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, row_number
from pyspark.sql.window import Window
spark = SparkSession.builder \
    .appName("iceberg-test") \
    .getOrCreate()
spark.sql("SHOW CATALOGS").show()
spark.sql("USE nessie")
spark.sql("SELECT CURRENT_CATALOG()").show()
spark.sql("CREATE DATABASE IF NOT EXISTS oracle_cdc_db")
spark.sql("SHOW DATABASES;").show()
spark.sql("USE oracle_cdc_db")
spark.sql("""
CREATE TABLE IF NOT EXISTS nessie.oracle_cdc_db.customers (
    id BIGINT,
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
    latitude DECIMAL(20,10),
    longitude DECIMAL(20,10),
    province_code STRING,
    month_name STRING
)
USING iceberg
""")
spark.sql("""
CREATE TABLE IF NOT EXISTS nessie.oracle_cdc_db.cdc_watermark (
    table_name STRING,
    last_ts BIGINT
)
USING iceberg
""")
last_ts = spark.sql(""" SELECT COALESCE(MAX(last_ts), 0) AS max_ts FROM nessie.oracle_cdc_db.cdc_watermark """).first()[0]
max_ts = spark.sql("""
SELECT COALESCE(MAX(ts_ms), 0) AS max_ts
FROM parquet.`s3a://oracle-cdc/topics/server1.C__DBZUSER.CUSTOMERS`
""").first()[0]
cdc_df = spark.sql(f"""
WITH deduped AS (
    SELECT
        COALESCE(after.ID, before.ID) AS id,
        COALESCE(after.NAME, before.NAME) AS name,
        op,
        ts_ms,
        ROW_NUMBER() OVER (
            PARTITION BY COALESCE(after.ID, before.ID)
            ORDER BY ts_ms DESC
        ) AS rn
    FROM parquet.`s3a://oracle-cdc/topics/server1.C__DBZUSER.CUSTOMERS`
    WHERE COALESCE(after.ID, before.ID) IS NOT NULL
      AND ts_ms > {last_ts}
)
SELECT *
FROM deduped
WHERE rn = 1
""")
cdc_df.createOrReplaceTempView("cdc_changes")
last_ts_from_cdc = spark.sql(f"""
    SELECT COALESCE(MAX(ts_ms), {last_ts}) AS last_ts
    FROM cdc_changes
""").first()[0]
spark.sql(f"""
MERGE INTO nessie.oracle_cdc_db.cdc_watermark t
USING (
    SELECT 'customers' AS table_name, {last_ts_from_cdc} AS last_ts
) s
ON t.table_name = s.table_name
WHEN MATCHED THEN
    UPDATE SET t.last_ts = s.last_ts
WHEN NOT MATCHED THEN
    INSERT (table_name, last_ts)
    VALUES (s.table_name, 0)
""")
spark.sql("""
DELETE FROM nessie.oracle_cdc_db.customers
WHERE id IN (
    SELECT id
    FROM cdc_changes
    WHERE op IN ('d','u')
)
""")
spark.sql("""
INSERT INTO nessie.oracle_cdc_db.customers
SELECT id, name
FROM cdc_changes
WHERE op IN ('c','u')
""")
spark.sql("show tables").show()
spark.sql("SELECT * FROM nessie.oracle_cdc_db.customers").show()
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
