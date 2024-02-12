# Databricks notebook source
# MAGIC %sql
# MAGIC select * from default.ticker_listing
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     symbol, name, exchange, assetType, ipoDate, delistingDate
# MAGIC FROM
# MAGIC     default.ticker_listing
# MAGIC WHERE 
# MAGIC     symbol IN ('AAPL', 'GOOGL', 'AMZN', 'MSFT')

# COMMAND ----------

sp_ticker_list = spark.sql('''
            SELECT DISTINCT 
              symbol 
            FROM 
              default.ticker_listing
            WHERE 
              symbol IN ('AAPL', 'GOOGL', 'AMZN', 'MSFT')
              ''')

# COMMAND ----------

display(sp_ticker_list)

# COMMAND ----------

import requests
import pandas as pd
import json
import os
from datetime import datetime
pd.set_option('display.max_columns', 500)
# Set your Alpha Vantage API Key
AV_API_Key='L5DZR3FLVQWA9BIX'

# COMMAND ----------

# Convert PySpark DataFrame to a Pandas DataFrame and store the value in a list
ticker_list = sp_ticker_list.toPandas().values.tolist()
# Initialize empty DataFrame
ticker_time_series=pd.DataFrame()
ticker_metadata=pd.DataFrame()
# Loop on the ticker list
for ticker in ticker_list :
    r_stock = requests.get('https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&outputsize=full&symbol={ticker}&apikey={apiKey}'.format(apiKey=AV_API_Key, ticker=ticker[0]))
    js_stock = r_stock.json()
    try:
        # Ticker time series to DataFrame
        df_time_series=pd.DataFrame(js_stock['Time Series (Daily)'])
        df_time_series['Ticker'] = ticker[0]
        ticker_time_series = pd.concat([ticker_time_series, df_time_series])
        # Ticker metadata to DataFrame
        df_metadata=pd.json_normalize(js_stock['Meta Data'])
        df_metadata['Ticker'] = ticker[0]
        df_metadata['Status'] = 'active'
        df_metadata['Date'] = datetime.now()
    except:
        df_metadata = pd.DataFrame({"Ticker" : [ticker[0]], "Status": ["inactive"] , "Date":[ datetime.now()]})
    ticker_metadata = pd.concat([ticker_metadata, df_metadata])
display(ticker_metadata.head())

# COMMAND ----------

ticker_time_series=ticker_time_series.reset_index().rename({'index':'Value_type'}, axis=1)
lg_ticker_ts = ticker_time_series.melt(id_vars=['Ticker','Value_type'])
display(lg_ticker_ts.head())

# COMMAND ----------

sp_ticker_ts=spark.createDataFrame(lg_ticker_ts)
sp_ticker_ts.distinct().createOrReplaceTempView('sp_ticker_ts')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     * 
# MAGIC FROM
# MAGIC     sp_ticker_ts

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS gold

# COMMAND ----------

# ticker time series value
sp_ticker_ts.distinct().write.mode("Overwrite").option("OverwriteSchema", "true").format("delta").save("/FileStore/bronze/ticker_value") 
spark.sql("CREATE TABLE IF NOT EXISTS bronze.ticker_value USING DELTA LOCATION '/FileStore/bronze/ticker_value'")
# ticker metadata
ticker_metadata = ticker_metadata.rename(columns={"1. Information": "Information", "2. Symbol": "Symbol", "3. Last Refreshed": "Last_Refreshed", "4. Output Size" : "Output_Size", "5. Time Zone" : "Time_Zone"})
sp_ticker_metadata = spark.createDataFrame(ticker_metadata)
sp_ticker_metadata.distinct().write.mode("Overwrite").option("OverwriteSchema", "true").format("delta").save("/FileStore/bronze/ticker_metadata") 
spark.sql("CREATE TABLE IF NOT EXISTS bronze.ticker_metadata USING DELTA LOCATION '/FileStore/bronze/ticker_metadata'")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.ticker_metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.ticker_value
