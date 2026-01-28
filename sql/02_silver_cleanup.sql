-- Databricks notebook source
-- Standardizes date types and keeps key account attributes.

CREATE OR REPLACE TABLE workspace.default.accounts_silver AS
SELECT
  account_id,
  customer_id,
  account_type,
  currency,
  CAST(open_date  AS DATE) AS open_date,
  CAST(close_date AS DATE) AS close_date,
  status
FROM workspace.default.accounts_raw;


-- COMMAND ----------

-- Keeps core customer attributes used for downstream dimensional modeling.

CREATE OR REPLACE TABLE workspace.default.customers_silver AS
SELECT
  customer_id,
  first_name,
  last_name,
  email,
  city,
  country
FROM workspace.default.customers_raw;

-- COMMAND ----------

-- Normalizes the FX rate date to a proper DATE type.

CREATE OR REPLACE TABLE workspace.default.fx_rates_silver AS
SELECT
  from_currency,
  to_currency,
  fx_rate,
  CAST(rate_date AS DATE) AS rate_date
FROM workspace.default.fx_rates_raw;


-- COMMAND ----------

-- The GL mapping is already in good shape; we simply surface it as Silver.

CREATE OR REPLACE TABLE workspace.default.gl_mapping_silver AS
SELECT *
FROM workspace.default.gl_mapping_raw;


-- COMMAND ----------

-- Converts transaction timestamp to a proper TIMESTAMP and keeps core transaction attributes.

CREATE OR REPLACE TABLE workspace.default.transactions_silver AS
SELECT
    transaction_id,
    account_id,
    amount,
    currency,
    transaction_type,
    merchant_category,
    direction,
    description,
    CAST(txn_timestamp AS TIMESTAMP) AS transaction_timestamp
FROM workspace.default.transactions_raw;

-- COMMAND ----------


-- Silver vs Bronze: row count comparison

SELECT 'accounts'      AS entity, 'raw' AS layer, COUNT(*) AS row_count
FROM workspace.default.accounts_raw
UNION ALL
SELECT 'accounts', 'silver', COUNT(*) FROM workspace.default.accounts_silver
UNION ALL
SELECT 'customers', 'raw', COUNT(*) FROM workspace.default.customers_raw
UNION ALL
SELECT 'customers', 'silver', COUNT(*) FROM workspace.default.customers_silver
UNION ALL
SELECT 'fx_rates', 'raw', COUNT(*) FROM workspace.default.fx_rates_raw
UNION ALL
SELECT 'fx_rates', 'silver', COUNT(*) FROM workspace.default.fx_rates_silver
UNION ALL
SELECT 'gl_mapping', 'raw', COUNT(*) FROM workspace.default.gl_mapping_raw
UNION ALL
SELECT 'gl_mapping', 'silver', COUNT(*) FROM workspace.default.gl_mapping_silver
UNION ALL
SELECT 'transactions', 'raw', COUNT(*) FROM workspace.default.transactions_raw
UNION ALL
SELECT 'transactions', 'silver', COUNT(*) FROM workspace.default.transactions_silver;


-- COMMAND ----------

-- Create a complete FX rate mapping including reverse pairs
CREATE OR REPLACE TABLE workspace.default.fx_rates_silver_complete AS
WITH base AS (
    SELECT 
        from_currency,
        to_currency,
        fx_rate,
        rate_date
    FROM workspace.default.fx_rates_silver
),
reverse_pairs AS (
    SELECT
        to_currency AS from_currency,
        from_currency AS to_currency,
        1 / fx_rate AS fx_rate,
        rate_date
    FROM workspace.default.fx_rates_silver
)
SELECT * FROM base
UNION ALL
SELECT * FROM reverse_pairs;


-- COMMAND ----------

