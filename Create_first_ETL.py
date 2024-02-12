# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM bronze.ticker_metadata

# COMMAND ----------

sp_ticker_list = spark.sql('''
            SELECT DISTINCT 
              tl.symbol 
            FROM 
              default.ticker_listing tl
            LEFT JOIN bronze.ticker_metadata tm
                ON tl.symbol=tm.Ticker
            WHERE 
              tm.Ticker IS NULL
            LIMIT 10
              ''')

# COMMAND ----------

import requests
import pandas as pd
import json
import os
from datetime import datetime
pd.set_option('display.max_columns', 500)
AV_API_Key = 'L5DZR3FLVQWA9BIX'

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
        df_metadata = pd.DataFrame({"Ticker" : [ticker[0]], "Status": ["inactive"] , "Date": [datetime.now()], '1. Information':[None], '2. Symbol': [None], '3. Last_Refreshed' : [None], '4. Output_Size' : [None], '5. Time_Zone' : [None]})
    ticker_metadata = pd.concat([ticker_metadata, df_metadata])
display(ticker_metadata.head())

# COMMAND ----------

try:
  ticker_time_series=ticker_time_series.reset_index().rename({'index':'Value_type'}, axis=1)
  lg_ticker_ts = ticker_time_series.melt(id_vars=['Ticker','Value_type'])
  display(lg_ticker_ts.head())
  sp_ticker_ts=spark.createDataFrame(lg_ticker_ts)
  sp_ticker_ts.distinct().createOrReplaceTempView('sp_ticker_temp')
  spark.sql(''' 
            MERGE INTO 
              bronze.ticker_value 
            USING  sp_ticker_temp 
              ON bronze.ticker_value.Ticker = sp_ticker_temp.Ticker
            WHEN MATCHED THEN
              UPDATE SET  *
            WHEN NOT MATCHED THEN
              INSERT  *
            ''')
except:
  pass

# COMMAND ----------

ticker_metadata = ticker_metadata.rename(columns={"1. Information": "Information", "2. Symbol": "Symbol", "3. Last Refreshed": "Last_Refreshed", "4. Output Size" : "Output_Size", "5. Time Zone" : "Time_Zone"})
sp_ticker_metadata=spark.createDataFrame(ticker_metadata)
sp_ticker_metadata.distinct().createOrReplaceTempView('ticker_metadata_temp')

# COMMAND ----------

# MAGIC %sql MERGE INTO bronze.ticker_metadata USING ticker_metadata_temp ON bronze.ticker_metadata.Ticker = ticker_metadata_temp.Ticker
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE
# MAGIC SET
# MAGIC   *
# MAGIC   WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC   *
