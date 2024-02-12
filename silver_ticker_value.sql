-- Databricks notebook source
SELECT * FROM bronze.ticker_value
ORDER BY Ticker, variable

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW ticker_value_update AS
SELECT DISTINCT
  tv.Ticker, CAST(variable AS DATE) AS stock_date, Open, Close, High, Low, Volume, tv1.min_variable
FROM 
  bronze.ticker_value tv
PIVOT(
  MAX(CAST(value AS DECIMAL(20,2)))
  FOR Value_Type IN ('1. open' AS Open, '4. close' AS Close, '2. high' AS High, '3. low' AS Low, '5. volume' AS Volume)
)
LEFT JOIN (SELECT MIN(CAST(variable AS DATE)) AS min_variable, Ticker
  FROM
    bronze.ticker_value
  WHERE
    value IS NOT NULL
  GROUP BY Ticker
) tv1
  ON tv.Ticker = tv1.Ticker
WHERE CAST(variable AS Date) >= CAST(tv1.min_variable AS Date)


-- COMMAND ----------

-- DBTITLE 1,Upsert Ticker value delta table
MERGE INTO silver.s_ticker_value
USING ticker_value_update
ON silver.s_ticker_value.Ticker=ticker_value_update.Ticker
and silver.s_ticker_value.stock_date = ticker_value_update.stock_date
WHEN MATCHED THEN
UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- %python
-- sp_silver_ticker_value = spark.sql('''
--  SELECT DISTINCT
--     tv.Ticker, CAST(variable as DATE) as stock_date, Open, Close, High, Low, Volume 
-- FROM
--   bronze.ticker_value tv
-- PIVOT (
--   MAX(CAST(value AS DECIMAL(20,2)))
--   FOR Value_Type IN ('1. open' AS Open, '4. close' AS Close, '2. high' as High, '3. low' as Low, '5. volume' as Volume)
-- )
-- LEFT JOIN (SELECT MIN(CAST(variable AS DATE)) AS min_variable, Ticker FROM bronze.ticker_value WHERE value IS NOT NULL GROUP BY Ticker) tv1
--   ON tv.Ticker = tv1.Ticker --AND tv.variable = tv1.min_variable
-- WHERE 
--     CAST(variable as DATE) > CAST(tv1.min_variable AS DATE) 
-- '''
-- )
-- sp_silver_ticker_value.distinct().write.mode("Overwrite").option("OverwriteSchema", "true").format("delta").save("/FileStore/silver/s_ticker_value") 
-- spark.sql("CREATE TABLE IF NOT EXISTS silver.s_ticker_value USING DELTA LOCATION '/FileStore/silver/s_ticker_value'")


-- COMMAND ----------


