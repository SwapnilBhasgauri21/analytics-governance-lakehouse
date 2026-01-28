-- Databricks notebook source
SHOW TABLES IN workspace.default
LIKE '*_raw';


-- COMMAND ----------

-- Gives a quick data volume sanity check right after ingestion.

SELECT 'accounts_raw'     AS table_name, COUNT(*) AS row_count FROM workspace.default.accounts_raw
UNION ALL
SELECT 'customers_raw'    AS table_name, COUNT(*) AS row_count FROM workspace.default.customers_raw
UNION ALL
SELECT 'fx_rates_raw'     AS table_name, COUNT(*) AS row_count FROM workspace.default.fx_rates_raw
UNION ALL
SELECT 'gl_mapping_raw'   AS table_name, COUNT(*) AS row_count FROM workspace.default.gl_mapping_raw
UNION ALL
SELECT 'transactions_raw' AS table_name, COUNT(*) AS row_count FROM workspace.default.transactions_raw;


-- COMMAND ----------

