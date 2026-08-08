from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, row_number, current_timestamp, trim, from_unixtime, to_date
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, DateType, DecimalType
import clickhouse_connect
import time

spark = (
    SparkSession.builder
    .appName("oracle-cdc-to-iceberg-stream-A")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ------------------------------------------------------------------
# Iceberg Table - Matches Oracle DDL
# ------------------------------------------------------------------
spark.sql("CREATE DATABASE IF NOT EXISTS nessie.oracle_cdc_db")
spark.sql("""
CREATE TABLE IF NOT EXISTS nessie.oracle_cdc_db.customers (
    id INT, provider STRING, quantity BIGINT, gd_barcode STRING, gd_name STRING,
    p_date STRING, invoice_id STRING, ncode_masked STRING, mobile_masked STRING,
    year BIGINT, month BIGINT, day BIGINT, city STRING, province STRING,
    m_date DATE, latitude DECIMAL(20,10), longitude DECIMAL(20,10),
    province_code STRING, month_name STRING, gd_cat STRING, gd_brand STRING, last_updated TIMESTAMP
) USING iceberg PARTITIONED BY (bucket(16, id))
""")

# ------------------------------------------------------------------
# CORRECT CDC SCHEMA - Must match Parquet file exactly from your log
# QUANTITY/YEAR/MONTH/DAY = decimal(19,0) NOT Long
# M_DATE = long (epoch ms) NOT Date
# ------------------------------------------------------------------
cdc_schema = StructType([
    StructField("before", StructType([
        StructField("ID", IntegerType()), StructField("PROVIDER", StringType()),
        StructField("QUANTITY", DecimalType(19,0)), StructField("GD_BARCODE", StringType()),
        StructField("GD_NAME", StringType()), StructField("P_DATE", StringType()),
        StructField("INVOICE_ID", StringType()), StructField("NCODE_MASKED", StringType()),
        StructField("MOBILE_MASKED", StringType()), StructField("YEAR", DecimalType(19,0)),
        StructField("MONTH", DecimalType(19,0)), StructField("DAY", DecimalType(19,0)),
        StructField("CITY", StringType()), StructField("PROVINCE", StringType()),
        StructField("M_DATE", LongType()), StructField("LATITUDE", DecimalType(20,10)),
        StructField("LONGITUDE", DecimalType(20,10)), StructField("PROVINCE_CODE", StringType()),
        StructField("MONTH_NAME", StringType()), StructField("GD_CAT", StringType()),
        StructField("GD_BRAND", StringType())
    ])),
    StructField("after", StructType([
        StructField("ID", IntegerType()), StructField("PROVIDER", StringType()),
        StructField("QUANTITY", DecimalType(19,0)), StructField("GD_BARCODE", StringType()),
        StructField("GD_NAME", StringType()), StructField("P_DATE", StringType()),
        StructField("INVOICE_ID", StringType()), StructField("NCODE_MASKED", StringType()),
        StructField("MOBILE_MASKED", StringType()), StructField("YEAR", DecimalType(19,0)),
        StructField("MONTH", DecimalType(19,0)), StructField("DAY", DecimalType(19,0)),
        StructField("CITY", StringType()), StructField("PROVINCE", StringType()),
        StructField("M_DATE", LongType()), StructField("LATITUDE", DecimalType(20,10)),
        StructField("LONGITUDE", DecimalType(20,10)), StructField("PROVINCE_CODE", StringType()),
        StructField("MONTH_NAME", StringType()), StructField("GD_CAT", StringType()),
        StructField("GD_BRAND", StringType())
    ])),
    StructField("op", StringType()), StructField("ts_ms", LongType())
])

def create_clickhouse_view_with_retry(max_retries=10, delay=5):
    for attempt in range(max_retries):
        try:
            client = clickhouse_connect.get_client(host='172.16.1.4', port=20322, username='default', password='clickhouse123')
            client.command("SET allow_experimental_database_atomic = 1")
            location = spark.sql("DESCRIBE EXTENDED nessie.oracle_cdc_db.customers").filter("col_name = 'Location'").collect()[0][1]
            ch_location = location.replace("s3://oracle-cdc/", "http://minio:9000/oracle-cdc/")
            client.command(f"CREATE OR REPLACE VIEW FACT_SALES AS SELECT provider AS PROVIDER,quantity AS QUANTITY,gd_barcode AS GD_BARCODE,gd_name AS GD_NAME,p_date AS P_DATE,invoice_id AS INVOICE_ID,ncode_masked AS NCODE_MASKED,mobile_masked AS MOBILE_MASKED,year AS YEAR,month AS MONTH,day AS DAY,city AS CITY,province AS PROVINCE,m_date AS M_DATE,toFloat64(latitude) AS LATITUDE,toFloat64(longitude) AS LONGITUDE,province_code AS PROVINCE_CODE,month_name AS MONTH_NAME,gd_cat AS GD_CAT,gd_brand AS GD_BRAND FROM iceberg('{ch_location}')")
            print("✅ ClickHouse view FACT_SALES created"); return True
        except Exception as e:
            print(f"Retry {attempt+1} failed: {e}"); time.sleep(delay)
    return False

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: empty"); return
    print(f"Batch {batch_id}: count = {batch_df.count()}")
    w = Window.partitionBy("id").orderBy(col("ts_ms").desc())
    latest = batch_df.filter(col("id").isNotNull()).filter(col("op").isin("c","u","d")).withColumn("rn", row_number().over(w)).filter(col("rn")==1).drop("rn")
    if latest.isEmpty(): return
    latest.createGlobalTempView("cdc_changes_batch")
    spark.sql("""
    MERGE INTO nessie.oracle_cdc_db.customers AS t USING global_temp.cdc_changes_batch AS s ON t.id=s.id
    WHEN MATCHED AND s.op='d' THEN DELETE
    WHEN MATCHED AND s.op='u' THEN UPDATE SET t.provider=s.provider, t.quantity=s.quantity, t.gd_barcode=s.gd_barcode, t.gd_name=s.gd_name, t.p_date=s.p_date, t.invoice_id=s.invoice_id, t.ncode_masked=trim(s.ncode_masked), t.mobile_masked=trim(s.mobile_masked), t.year=s.year, t.month=s.month, t.day=s.day, t.city=s.city, t.province=s.province, t.m_date=s.m_date, t.latitude=s.latitude, t.longitude=s.longitude, t.province_code=s.province_code, t.month_name=s.month_name, t.gd_cat=s.gd_cat, t.gd_brand=s.gd_brand ,t.last_updated=current_timestamp()
    WHEN NOT MATCHED AND s.op='c' THEN INSERT (id,provider,quantity,gd_barcode,gd_name,p_date,invoice_id,ncode_masked,mobile_masked,year,month,day,city,province,m_date,latitude,longitude,province_code,month_name,last_updated) VALUES (s.id,s.provider,s.quantity,s.gd_barcode,s.gd_name,s.p_date,s.invoice_id,trim(s.ncode_masked),trim(s.mobile_masked),s.year,s.month,s.day,s.city,s.province,s.m_date,s.latitude,s.longitude,s.province_code,s.month_name,s.gd_cat,s.gd_brand,current_timestamp())
    """)
    spark.catalog.dropGlobalTempView("cdc_changes_batch")
    print(f"Batch {batch_id}: merge done")

source_path = "s3a://oracle-cdc/topics/server1.C__DBZUSER.CUSTOMERS"
stream_df = spark.readStream.schema(cdc_schema).option("maxFilesPerTrigger", 500).parquet(source_path)

cdc_df = stream_df.select(
    coalesce(col("after.ID"), col("before.ID")).cast("int").alias("id"),
    coalesce(col("after.PROVIDER"), col("before.PROVIDER")).cast("string").alias("provider"),
    coalesce(col("after.QUANTITY"), col("before.QUANTITY")).cast("long").alias("quantity"),
    coalesce(col("after.GD_BARCODE"), col("before.GD_BARCODE")).cast("string").alias("gd_barcode"),
    coalesce(col("after.GD_NAME"), col("before.GD_NAME")).cast("string").alias("gd_name"),
    coalesce(col("after.P_DATE"), col("before.P_DATE")).cast("string").alias("p_date"),
    coalesce(col("after.INVOICE_ID"), col("before.INVOICE_ID")).cast("string").alias("invoice_id"),
    trim(coalesce(col("after.NCODE_MASKED"), col("before.NCODE_MASKED"))).alias("ncode_masked"),
    trim(coalesce(col("after.MOBILE_MASKED"), col("before.MOBILE_MASKED"))).alias("mobile_masked"),
    coalesce(col("after.YEAR"), col("before.YEAR")).cast("long").alias("year"),
    coalesce(col("after.MONTH"), col("before.MONTH")).cast("long").alias("month"),
    coalesce(col("after.DAY"), col("before.DAY")).cast("long").alias("day"),
    coalesce(col("after.CITY"), col("before.CITY")).cast("string").alias("city"),
    coalesce(col("after.PROVINCE"), col("before.PROVINCE")).cast("string").alias("province"),
    to_date(from_unixtime(coalesce(col("after.M_DATE"), col("before.M_DATE"))/1000)).alias("m_date"),
    coalesce(col("after.LATITUDE"), col("before.LATITUDE")).cast("decimal(20,10)").alias("latitude"),
    coalesce(col("after.LONGITUDE"), col("before.LONGITUDE")).cast("decimal(20,10)").alias("longitude"),
    coalesce(col("after.PROVINCE_CODE"), col("before.PROVINCE_CODE")).cast("string").alias("province_code"),
    coalesce(col("after.MONTH_NAME"), col("before.MONTH_NAME")).cast("string").alias("month_name"),
    col("op").cast("string").alias("op"), col("ts_ms").cast("long").alias("ts_ms")
).filter(col("id").isNotNull()).filter(col("op").isNotNull())

create_clickhouse_view_with_retry()
query = cdc_df.writeStream.foreachBatch(process_batch).option("checkpointLocation", "s3a://oracle-cdc/checkpoints/customers_merge_v7_A").trigger(processingTime="30 seconds").start()
print(f"Started {query.id}")
query.awaitTermination()
