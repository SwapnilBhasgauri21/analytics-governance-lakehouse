# Analytics Governance Lakehouse (Databricks)

## Overview
This project demonstrates an end-to-end **Lakehouse analytics pipeline** built using **Databricks SQL** and **Delta Lake**, following the **Bronze → Silver → Gold** data modeling pattern.

The goal was to ingest raw financial transaction data, apply structured data quality and transformation rules, and produce analytics-ready datasets powering business dashboards.

---

## Architecture
**Bronze Layer**
- Raw CSV ingestion into Delta tables
- Schema preservation and minimal validation
- Acts as the system of record

**Silver Layer**
- Data cleansing and normalization
- Currency standardization using FX rates
- Type casting, null handling, and consistency checks

**Gold Layer**
- Analytics-ready fact and dimension tables
- USD-normalized revenue metrics
- Optimized for BI and dashboard consumption

---

## Data Model
- **fact_transactions_gold**
  - Transaction-level revenue in USD
  - Joined with FX rates and customer/account attributes

- Supporting dimensions:
  - Customers
  - Accounts
  - Calendar (derived)

---

## Dashboards & Analytics
The Gold layer powers multiple dashboard tiles, including:
- Daily Revenue (USD)
- Transaction Volume Trends
- Revenue by Currency
- Customer Activity Distribution
- Account-level Performance

Dashboards were built using **Databricks SQL Dashboards**, optimized for business stakeholders.

---

## Tools & Technologies
- Databricks SQL
- Delta Lake
- Lakehouse Architecture
- SQL-based transformations
- Data Quality Validation
- BI-ready Gold modeling

---

## Why This Project
This project showcases:
- Strong understanding of **modern analytics engineering**
- Hands-on experience with **Databricks Lakehouse**
- Ability to design **governed, scalable data models**
- Clear separation between ingestion, transformation, and analytics layers

---

## Next Enhancements (Planned)
- Incremental loads
- Data quality metrics table
- Orchestration via Jobs
- Integration with Power BI / external BI tools
