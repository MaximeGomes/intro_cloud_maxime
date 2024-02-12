-- Databricks notebook source
SELECT 
  tm.*,
  tl.name,
  tl.`exchange`,
  tl.assetType
FROM 
  bronze.ticker_metadata tm
LEFT JOIN 
  default.ticker_listing tl
ON 
  tm.ticker = tl.symbol;


-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW ticker_metadata_update AS 
SELECT 
  tm.*,
  tl.name,
  tl.`exchange`,
  tl.assetType
FROM 
  bronze.ticker_metadata tm
LEFT JOIN 
  default.ticker_listing tl
ON 
  tm.ticker = tl.symbol;

-- COMMAND ----------

MERGE INTO silver.s_ticker_metadata
USING ticker_metadata_update
ON silver.s_ticker_metadata.Ticker=ticker_metadata_update.Ticker
WHEN MATCHED THEN
UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- %python
-- s_ticker_metadata = spark.sql('''
--     SELECT tm.*, tl.name, tl.`exchange`, tl.assetType
--     FROM 
--         bronze.ticker_metadata tm
--     LEFT JOIN 
--         default.ticker_listing tl
--     ON 
--         tm.ticker = tl.symbol;
-- ''')

-- s_ticker_metadata.distinct().write.mode("Overwrite").option("OverwriteSchema", "true").format("delta").save("/FileStore/silver/s_ticker_metadata") 
-- spark.sql("CREATE TABLE IF NOT EXISTS silver.s_ticker_metadata USING DELTA LOCATION '/FileStore/silver/s_ticker_metadata'")

