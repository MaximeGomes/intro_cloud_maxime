-- Databricks notebook source
SELECT 
  * 
FROM 
  gold.g_ticker_value
WHERE Ticker = 'AAPL'
AND stock_date BETWEEN '2014-01-01' AND '2018-01-01'
