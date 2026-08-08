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
    ID BIGINT,
    PROVIDER STRING,
    QUANTITY BIGINT,
    GD_BARCODE STRING,
    GD_NAME STRING,
    P_DATE STRING,
    INVOICE_ID STRING,
    NCODE_MASKED STRING,
    MOBILE_MASKED STRING,
    YEAR BIGINT,
    MONTH BIGINT,
    DAY BIGINT,
    CITY STRING,
    PROVINCE STRING,
    M_DATE DATE,
    LATITUDE DECIMAL(20,10),
    LONGITUDE DECIMAL(20,10),
    PROVINCE_CODE STRING,
    MONTH_NAME STRING
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
        COALESCE(after.ID,before.ID) AS id,

        COALESCE(after.PROVIDER,before.PROVIDER) AS provider,

        COALESCE(after.QUANTITY,before.QUANTITY) AS quantity,

        COALESCE(after.GD_BARCODE,before.GD_BARCODE) AS gd_barcode,

        COALESCE(after.GD_NAME,before.GD_NAME) AS gd_name,

        COALESCE(after.P_DATE,before.P_DATE) AS p_date,

        COALESCE(after.INVOICE_ID,before.INVOICE_ID) AS invoice_id,

        COALESCE(after.NCODE_MASKED,before.NCODE_MASKED) AS ncode_masked,

        COALESCE(after.MOBILE_MASKED,before.MOBILE_MASKED) AS mobile_masked,

        COALESCE(after.YEAR,before.YEAR) AS year,

        COALESCE(after.MONTH,before.MONTH) AS month,

        COALESCE(after.DAY,before.DAY) AS day,

        COALESCE(after.CITY,before.CITY) AS city,

        COALESCE(after.PROVINCE,before.PROVINCE) AS province,

        COALESCE(after.M_DATE,before.M_DATE) AS m_date,

        COALESCE(after.LATITUDE,before.LATITUDE) AS latitude,

        COALESCE(after.LONGITUDE,before.LONGITUDE) AS longitude,

        COALESCE(after.PROVINCE_CODE,before.PROVINCE_CODE) AS province_code,

        COALESCE(after.MONTH_NAME,before.MONTH_NAME) AS month_name,
        
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
SELECT 
ID,
PROVIDER,
QUANTITY,
GD_BARCODE,
GD_NAME,
P_DATE,
INVOICE_ID,
NCODE_MASKED,
MOBILE_MASKED,
YEAR,
MONTH,
DAY,
CITY,
PROVINCE,
CAST(M_DATE AS DATE),
LATITUDE,
LONGITUDE,
PROVINCE_CODE,
MONTH_NAME
FROM cdc_changes
WHERE op IN ('c','u')
""")
spark.sql("show tables").show()
spark.sql("SELECT * FROM nessie.oracle_cdc_db.customers").show()
import clickhouse_connect
client = clickhouse_connect.get_client(
    host='172.16.1.4',#'clickhouse',   # 👈 NOT localhost
    port=20322#8123,
    username='default',
    password='clickhouse123'
)
#client.command("SET allow_experimental_database_iceberg = 1")
client.command("SET allow_experimental_database_atomic = 1")  # ✅ Works
desc = spark.sql("DESCRIBE EXTENDED nessie.oracle_cdc_db.customers")
location = (
    desc.filter("col_name = 'Location'")
    .select("data_type")
    .collect()[0][0]
)
print("📍 Table location:", location)
ch_location = location.replace("s3://oracle-cdc/", "http://minio:9000/oracle-cdc/")
#icebergS3(
client.command(f"""
CREATE VIEW IF NOT EXISTS FACT_SALES AS
SELECT * FROM iceberg( 
    '{ch_location}'
)
""")
