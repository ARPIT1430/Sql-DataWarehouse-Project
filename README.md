## 📊 Sales Data Warehouse & Business Analytics Platform

A SQL-based data warehouse that integrates disconnected CRM and ERP systems into a single, trustworthy source of truth — enabling customer, product, and revenue analysis for data-driven decision-making.

Welcome to the **Data Warehouse and Analytics Project** repository! 🚀
This project demonstrates a comprehensive data warehousing and analytics solution, from building a data warehouse to generating actionable insights. 
Designed as a portfolio project, it highlights industry best practices in data engineering and analytics.

----------------------------------------------------
## 📖 Project Overview

Organizations rarely keep all their data in one place. In this project, sales data was split across two disconnected source systems — a CRM system (customer and product master data, sales transactions) and an ERP system (customer demographics, location, product categorization). This is one of the most common real-world data problems in analytics: the data needed to answer a business question exists, but it's fragmented, inconsistently formatted, and not trustworthy enough to report on directly.

----------------------------------------------------
## The Business Problem

Customer and product information lived in two systems with different formats, different codes, and no single reliable source of truth.
Raw sales data contained data quality issues — inconsistent date formats, invalid values, and inconsistencies between quantity, price, and sales fields.
There was no unified, analysis-ready model that business stakeholders could query to answer questions about customers, products, or revenue with confidence.

----------------------------------------------------
## Why This Warehouse Was Built

This project designs and implements a SQL Server data warehouse using the Medallion Architecture (Bronze → Silver → Gold) to solve exactly this problem: ingest raw data reliably, clean and standardize it once, and model it into a business-ready structure that can be queried directly for reporting and analysis.

----------------------------------------------------
## Decisions This Warehouse Supports

Once built, the warehouse is designed to help answer questions such as:

Which customers drive the majority of revenue, and where should retention efforts be focused?
Which product categories and specific products are top performers — and which carry weaker margins despite strong sales?
How does customer value differ across countries, and does that justify a region-specific strategy?
What do purchasing patterns reveal about customer loyalty versus one-time buyers?

------------------------------------------------------
## 🎯 Business Objectives

Rather than technical milestones, this project was scoped around business outcomes:

✅ Create a single, trustworthy view of customers and products by integrating CRM and ERP data.
✅ Eliminate data quality issues (duplicates, inconsistent codes, invalid values) before they reach any report.
✅ Enable self-service analysis by modeling data into a structure business stakeholders and analysts can query directly.
✅ Surface actionable business insight — not just clean data, but specific findings that inform real decisions (e.g., retention strategy, regional focus, product profitability).

------------------------------------------------------
## 🏗️ Solution Architecture

This project follows the Medallion Architecture, a layered design pattern that separates raw ingestion, data cleansing, and business-ready modeling into distinct stages — improving reliability, maintainability, and auditability at every step.
## 🛡️License
This project is licensed under the MIT License. You are free to use, modify, and share this project with proper attribution.

Source Systems              Bronze              Silver              Gold
┌──────────────┐          ┌───────────┐       ┌───────────┐      ┌──────────────┐
│  CRM (CSV)   │  ───────▶│   Raw     │──────▶│  Cleaned  │─────▶│  Star Schema │
│  ERP (CSV)   │          │  Landing  │       │Standardized│      │ (Reporting)  │
└──────────────┘          └───────────┘       └───────────┘      └──────────────┘

------------------------------------------------------
## 🥉 Bronze Layer — Raw Data Landing

Raw CSV data from both source systems is ingested as-is, with zero transformation. This preserves an exact, auditable copy of what the source systems provided.

Data loaded via BULK INSERT stored procedures with full error handling (TRY/CATCH)
Per-table and batch-level load duration logging for pipeline observability
Full truncate-and-reload strategy on every run, ensuring the warehouse always reflects the latest available source data

------------------------------------------------------
## 🥈 Silver Layer — Cleansing & Standardization

This is where raw data is transformed into something trustworthy. Key operations include:

Deduplication — resolving duplicate customer records using window functions to retain only the most recent entry per customer
Standardization — converting coded values (e.g., 'M', 'S') into readable business terms ('Married', 'Single')
Data reconstruction — deriving missing product validity end-dates using the LEAD() window function, since the source system didn't reliably provide this
Business rule enforcement — validating that sales = quantity × price, recalculating when source values violated this rule
Data quality validation — a dedicated quality-check script verifying null/duplicate keys, whitespace issues, and invalid date ordering after every load

------------------------------------------------------
## 🥇 Gold Layer — Business-Ready Star Schema

The final layer models cleansed Silver data into a star schema, exposed as SQL views for direct analytical querying:

Integrates CRM and ERP data into unified customer and product views, with defined survivorship rules for conflicting fields (e.g., CRM treated as the master source for gender, with ERP used as a fallback)
Filters to only current/active product records, using the effective-dating logic built in Silver
Validated for referential integrity — ensuring no orphaned fact records exist without a matching dimension

💡 Design choice: The Gold layer is implemented as views, not physical tables. This means any correction made in Silver automatically propagates through to Gold without requiring a separate reload step.

------------------------------------------------------
## 🔄 Data Pipeline

The complete ETL workflow, from raw files to analytics-ready tables:

Extract — Raw CSV files from CRM (cust_info, prd_info, sales_details) and ERP (CUST_AZ12, LOC_A101, PX_CAT_G1V2) source systems.
Load (Bronze) — Files are bulk-loaded into staging tables via stored procedures, with logging and error handling.
Transform (Silver) — Data is cleaned, deduplicated, standardized, validated against business rules, and enriched with audit metadata (dwh_create_date).
Model (Gold) — Cleaned data is joined, integrated, and reshaped into a star schema of fact and dimension views.
Analyze — SQL analytical queries are run directly against the Gold layer to generate business insights.


------------------------------------------------------
## 🌟 Data Model: Star Schema

The Gold layer implements a classic star schema — the industry-standard model for analytical and BI workloads, chosen specifically because it optimizes for the types of queries business analysts run most often (aggregations, filters, and joins across a small number of tables).

TableTypeDescriptiongold.fact_salesFact TableTransactional sales data — order/ship/due dates, sales amount, quantity, price. Connects to dimensions via surrogate keys.gold.dim_customersDimension TableUnified customer profile — merges CRM and ERP data, including demographics, location, and marital status.gold.dim_productsDimension TableUnified product profile — category, subcategory, cost, product line, and lifecycle dates.

Why this model improves analytical performance:

Surrogate keys (system-generated, not source-system IDs) keep joins stable even if source systems change their internal ID schemes.
A small number of wide dimension tables means fewer joins are needed to answer typical business questions, compared to a normalized transactional schema.
Separating descriptive attributes (dimensions) from measurable events (facts) is the standard pattern BI tools and analysts expect — making this warehouse immediately compatible with tools like Power BI, Tableau, or direct SQL reporting.

------------------------------------------------------
## 📈 Business Analytics

With the Gold layer in place, a series of SQL-based analyses were performed to extract business insight — moving beyond a working pipeline into actual decision support.

------------------------------------------------------
## 👥 Customer Analysis & Segmentation

Customers were segmented by total revenue contribution using quintile analysis (NTILE()), revealing how concentrated revenue is across the customer base.

------------------------------------------------------
## 🛒 Product Performance

Products and categories were evaluated on revenue, margin percentage, and unit sales to identify top performers and profitability outliers.

------------------------------------------------------
## 💰 Revenue & Trend Analysis

Monthly revenue trends were analyzed by category using window functions (LAG()) to calculate month-over-month growth and identify seasonality.

------------------------------------------------------
## 🌍 Geographic Analysis

Revenue, customer count, and average order value were compared across countries to evaluate regional performance differences.

------------------------------------------------------
## 🔁 Customer Retention Behavior

Customers were segmented into one-time, repeat, and loyal buyers based on order frequency, to assess retention health.

------------------------------------------------------
📋 KPI Reporting

Core KPIs were calculated directly from the Gold layer, including average order value, average revenue per customer, and margin percentage by category — metrics designed to be pulled directly into stakeholder reporting.

------------------------------------------------------
## Key Business Insights

- **Revenue Concentration:** The top 20% of customers generate 66.4% of total revenue — indicating heavy dependence on a core customer base.
- **Customer Retention Risk:** 62.9% of customers are one-time buyers, while only 0.5% qualify as loyal (5+ order) customers.
- **Regional Performance:** Australian customers generate more than double the average revenue per customer compared to U.S. customers, despite having less than half the customer count.
- **Category Profitability:** Road Bikes generate the highest category revenue but carry the lowest margin percentage among bike types.
- **Product Performance:** Mountain-200 series bikes are consistently the top-selling individual products by revenue.

These findings illustrate the core value of the warehouse: once data is integrated and cleaned, specific, decision-relevant patterns emerge that would be difficult or unreliable to surface from the fragmented raw source data.

------------------------------------------------------
## 📜 License

This project is licensed under the MIT License — you are free to use, modify, and share this project with proper attribution.

------------------------------------------------------
## 🙋‍♂️ About Me
Hi there! I'm Arpit Khandelwal a Mechanical Engineeering student at NSUT,Delhi
Final-year Mechanical Engineering student at Netaji Subhas University of Technology (NSUT), Delhi, transitioning into Business Analytics and Data Analytics roles. Experienced in SQL-based data warehousing, business intelligence, and analytical problem-solving, with hands-on internship experience at Bharat Heavy Electricals Limited (BHEL) and The Outlook Group.



