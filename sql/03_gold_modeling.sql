-- Databricks notebook source
-- Customer dimension used across analytics for segmentation and customer-level reporting.

CREATE OR REPLACE TABLE workspace.default.dim_customers_gold AS
SELECT
  c.customer_id,
  c.first_name,
  c.last_name,
  c.email,
  c.city,
  c.country
FROM workspace.default.customers_silver c;


-- COMMAND ----------

-- Account dimension including lifecycle dates and high-level account attributes.

CREATE OR REPLACE TABLE workspace.default.dim_accounts_gold AS
SELECT
  a.account_id,
  a.customer_id,
  a.account_type,
  a.currency,
  a.open_date,
  a.close_date,
  a.status
FROM workspace.default.accounts_silver a;


-- COMMAND ----------

-- Gold Fact: Transactions with normalized USD amounts
CREATE OR REPLACE TABLE workspace.default.fact_transactions_gold AS
SELECT
    t.transaction_id,
    t.account_id,
    a.customer_id,
    t.transaction_timestamp,
    CAST(t.transaction_timestamp AS DATE) AS transaction_date,
    t.amount,
    t.currency AS source_currency,
    fx.fx_rate,
    
    -- Convert all amounts to USD using enhanced FX table
    CASE 
        WHEN t.currency = 'USD' THEN t.amount  
        ELSE t.amount * fx.fx_rate
    END AS amount_usd,

    t.transaction_type,
    t.merchant_category,
    t.direction,
    t.description
FROM workspace.default.transactions_silver t
LEFT JOIN workspace.default.dim_accounts_gold a 
    ON t.account_id = a.account_id
LEFT JOIN workspace.default.fx_rates_silver_complete fx
    ON fx.from_currency = t.currency
    AND fx.to_currency = 'USD'
    AND fx.rate_date = CAST(t.transaction_timestamp AS DATE);


-- COMMAND ----------

-- Daily revenue / volume snapshot by GL product line and revenue/cost flag.
-- This feeds executive dashboards and trend charts.

CREATE OR REPLACE TABLE workspace.default.fact_daily_revenue_gold AS
SELECT
    transaction_date,
    COALESCE(product_line,        'UNKNOWN') AS product_line,
    COALESCE(revenue_or_cost_flag,'UNKNOWN') AS revenue_or_cost_flag,
    SUM(amount)      AS total_amount_local,
    SUM(amount_usd)  AS total_amount_usd,
    COUNT(*)         AS transaction_count
FROM workspace.default.fact_transactions_gold
GROUP BY
    transaction_date,
    COALESCE(product_line,        'UNKNOWN'),
    COALESCE(revenue_or_cost_flag,'UNKNOWN');


-- COMMAND ----------

-- Sanity check: Gold tables

SELECT 'dim_customers_gold'        AS table_name, COUNT(*) AS row_count FROM workspace.default.dim_customers_gold
UNION ALL
SELECT 'dim_accounts_gold', COUNT(*) FROM workspace.default.dim_accounts_gold
UNION ALL
SELECT 'fact_transactions_gold', COUNT(*) FROM workspace.default.fact_transactions_gold
UNION ALL
SELECT 'fact_daily_revenue_gold', COUNT(*) FROM workspace.default.fact_daily_revenue_gold;


-- COMMAND ----------

-- Gold Fact: Transactions with normalized USD amounts
CREATE OR REPLACE TABLE workspace.default.fact_transactions_gold AS
SELECT
    t.transaction_id,
    t.account_id,
    a.customer_id,
    t.transaction_timestamp,
    CAST(t.transaction_timestamp AS DATE) AS transaction_date,
    t.amount,
    t.currency AS source_currency,
    fx.fx_rate,
    
    -- Convert all amounts to USD using enhanced FX table
    CASE 
        WHEN t.currency = 'USD' THEN t.amount  
        ELSE t.amount * fx.fx_rate
    END AS amount_usd,

    t.transaction_type,
    t.merchant_category,
    t.direction,
    t.description
FROM workspace.default.transactions_silver t
LEFT JOIN workspace.default.dim_accounts_gold a 
    ON t.account_id = a.account_id
LEFT JOIN workspace.default.fx_rates_silver_complete fx
    ON fx.from_currency = t.currency
    AND fx.to_currency = 'USD'
    AND fx.rate_date = CAST(t.transaction_timestamp AS DATE);


-- COMMAND ----------

SELECT 
    transaction_id,
    amount,
    source_currency,
    fx_rate,
    amount_usd
FROM workspace.default.fact_transactions_gold
ORDER BY transaction_timestamp
LIMIT 20;


-- COMMAND ----------

