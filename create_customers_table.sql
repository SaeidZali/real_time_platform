-- create_customers_table.sql
-- Run this script as c##dbzuser in the PDB container
-- Usage: docker exec -i -e ORACLE_SID=ORCLPDB1 dbz_oracle19 sqlplus c##dbzuser/dbz@ORCLPDB1 < create_customers_table.sql

WHENEVER SQLERROR EXIT SQL.SQLCODE;

PROMPT Connecting to ORCLPDB1...
PROMPT Creating customers table...

-- Create the customers table
CREATE TABLE customers (
  id NUMBER(9,0) PRIMARY KEY, 
  provider      VARCHAR2(255),
  quantity      NUMBER(19),
  gd_barcode    VARCHAR2(255),
  gd_name       VARCHAR2(500),
  p_date        VARCHAR2(50),
  invoice_id    VARCHAR2(100),
  ncode_masked  CHAR(10),
  mobile_masked CHAR(11),
  year          NUMBER(19),
  month         NUMBER(19),
  day           NUMBER(19),
  city          VARCHAR2(255),
  province      VARCHAR2(255),
  m_date        DATE,
  latitude      NUMBER(20,10),
  longitude     NUMBER(20,10),
  province_code VARCHAR2(50),
  month_name    VARCHAR2(50),
  gd_cat        VARCHAR2(500),
  gd_brand      VARCHAR2(500)
);

PROMPT Enabling supplemental logging...

-- Enable supplemental logging for all columns
ALTER TABLE customers ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

PROMPT Verifying the data...
SELECT COUNT(*) AS total_records FROM customers;

PROMPT Table customers created successfully with 0 records!
EXIT;
