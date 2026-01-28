-- FX validation: ensure USD transactions carry their original amount into amount_usd

SELECT
  source_currency,
  COUNT(*)                                        AS txn_count,
  SUM(CASE WHEN amount_usd IS NULL THEN 1 ELSE 0 END) AS null_amount_usd_cnt
FROM workspace.default.fact_transactions_gold
GROUP BY source_currency
ORDER BY source_currency;


-- COMMAND ----------

-- Dashboard Metric 1: Daily FX-normalized revenue in USD

SELECT
  transaction_date,
  SUM(amount_usd) AS total_revenue_usd,
  COUNT(*)        AS transaction_count
FROM workspace.default.fact_transactions_gold
GROUP BY transaction_date
ORDER BY transaction_date;


-- COMMAND ----------

-- Dashboard Metric 2: Monthly revenue by customer country (USD normalized)

SELECT
  DATE_TRUNC('month', ft.transaction_date) AS month,
  dc.country                               AS customer_country,
  SUM(ft.amount_usd)                       AS total_revenue_usd,
  COUNT(*)                                 AS transaction_count
FROM workspace.default.fact_transactions_gold ft
JOIN workspace.default.dim_accounts_gold da
  ON ft.account_id = da.account_id
JOIN workspace.default.dim_customers_gold dc
  ON da.customer_id = dc.customer_id
GROUP BY
  DATE_TRUNC('month', ft.transaction_date),
  dc.country
ORDER BY
  month,
  total_revenue_usd DESC;


-- COMMAND ----------

-- Dashboard Metric 3: Revenue by account type (USD normalized)

SELECT
  da.account_type,
  SUM(ft.amount_usd) AS total_revenue_usd,
  COUNT(*)           AS transaction_count
FROM workspace.default.fact_transactions_gold ft
JOIN workspace.default.dim_accounts_gold da
  ON ft.account_id = da.account_id
GROUP BY da.account_type
ORDER BY total_revenue_usd DESC;


-- COMMAND ----------

-- Alternative: derive product_line from GL mapping on the fly

SELECT
  gm.product_line,
  SUM(ft.amount_usd) AS total_revenue_usd,
  COUNT(*)           AS transaction_count
FROM workspace.default.fact_transactions_gold ft
JOIN workspace.default.gl_mapping_silver gm
  ON ft.transaction_type = gm.transaction_type
GROUP BY gm.product_line
ORDER BY total_revenue_usd DESC;


-- COMMAND ----------

-- Dashboard Metric 5: Transaction mix by type and direction (debit/credit)

SELECT
  transaction_type,
  direction,
  COUNT(*)           AS transaction_count,
  SUM(amount_usd)    AS total_amount_usd
FROM workspace.default.fact_transactions_gold
GROUP BY transaction_type, direction
ORDER BY total_amount_usd DESC;

-- COMMAND ----------

-- Dashboard Metric 6: Top 20 customers by total revenue in USD

SELECT
  dc.customer_id,
  dc.first_name,
  dc.last_name,
  dc.country,
  SUM(ft.amount_usd) AS total_revenue_usd,
  COUNT(*)           AS transaction_count
FROM workspace.default.fact_transactions_gold ft
JOIN workspace.default.dim_accounts_gold da
  ON ft.account_id = da.account_id
JOIN workspace.default.dim_customers_gold dc
  ON da.customer_id = dc.customer_id
GROUP BY
  dc.customer_id,
  dc.first_name,
  dc.last_name,
  dc.country
ORDER BY total_revenue_usd DESC
LIMIT 20;


-- COMMAND ----------

-- Dashboard Metric 7: Daily transaction volume (count of transactions)

SELECT
  transaction_date,
  COUNT(*) AS transaction_count
FROM workspace.default.fact_transactions_gold
GROUP BY transaction_date
ORDER BY transaction_date;
