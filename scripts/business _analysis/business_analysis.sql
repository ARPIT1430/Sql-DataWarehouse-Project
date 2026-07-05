/*
=======================================================================================================
Business Analysis Queries : Gold Layer Insights
=======================================================================================================
Script Purpose :
  This script runs analytical queries on top of the Gold layer star schema to surface
  business insights - revenue trends, customer segmentation, product profitability,
  and geographic performance.

Usage Notes :
  - Run this AFTER the gold.dim_customers, gold.dim_products, and gold.fact_sales
    views have been created.
  - Each section is independent; run them individually or all together.
  - Replace hard filters (dates, top N, etc.) as needed for your dataset.
=======================================================================================================
*/


-- =========================================================================================
-- 1. Revenue Trend by Product Category (Month-over-Month)
-- Business Question: Which product categories are growing or declining over time?
-- =========================================================================================

SELECT 
    p.category,
    YEAR(f.order_date)  AS order_year,
    MONTH(f.order_date) AS order_month,
    SUM(f.sales_amount) AS total_revenue,
    SUM(f.quantity)     AS total_units_sold
FROM gold.fact_sales f
JOIN gold.dim_products p 
    ON f.product_key = p.product_key
WHERE f.order_date IS NOT NULL
GROUP BY p.category, YEAR(f.order_date), MONTH(f.order_date)
ORDER BY p.category, order_year, order_month;


-- Optional: Month-over-Month % change per category, using LAG()
WITH monthly_revenue AS (
    SELECT 
        p.category,
        YEAR(f.order_date)  AS order_year,
        MONTH(f.order_date) AS order_month,
        SUM(f.sales_amount) AS total_revenue
    FROM gold.fact_sales f
    JOIN gold.dim_products p 
        ON f.product_key = p.product_key
    WHERE f.order_date IS NOT NULL
    GROUP BY p.category, YEAR(f.order_date), MONTH(f.order_date)
)
SELECT 
    category,
    order_year,
    order_month,
    total_revenue,
    LAG(total_revenue) OVER (PARTITION BY category ORDER BY order_year, order_month) AS prev_month_revenue,
    CAST(
        (total_revenue - LAG(total_revenue) OVER (PARTITION BY category ORDER BY order_year, order_month)) 
        * 100.0 / NULLIF(LAG(total_revenue) OVER (PARTITION BY category ORDER BY order_year, order_month), 0)
        AS DECIMAL(10,2)
    ) AS mom_pct_change
FROM monthly_revenue
ORDER BY category, order_year, order_month;


-- =========================================================================================
-- 2. Customer Segmentation by Revenue Contribution (80/20 / Pareto Analysis)
-- Business Question: Is revenue concentrated in a small group of high-value customers?
-- =========================================================================================

WITH customer_revenue AS (
    SELECT 
        c.customer_key,
        c.first_name,
        c.last_name,
        c.country,
        SUM(f.sales_amount) AS total_spent,
        COUNT(DISTINCT f.order_number) AS total_orders
    FROM gold.fact_sales f
    JOIN gold.dim_customers c 
        ON f.customer_key = c.customer_key
    GROUP BY c.customer_key, c.first_name, c.last_name, c.country
),
ranked_customers AS (
    SELECT 
        *,
        NTILE(5) OVER (ORDER BY total_spent DESC) AS revenue_quintile  -- 1 = top 20%
    FROM customer_revenue
)
SELECT 
    revenue_quintile,
    COUNT(*) AS customer_count,
    SUM(total_spent) AS quintile_revenue,
    CAST(SUM(total_spent) * 100.0 / SUM(SUM(total_spent)) OVER () AS DECIMAL(10,2)) AS pct_of_total_revenue
FROM ranked_customers
GROUP BY revenue_quintile
ORDER BY revenue_quintile;

-- Drill-down: raw top 20 customers by spend
SELECT TOP 20
    customer_key,
    first_name,
    last_name,
    country,
    total_spent,
    total_orders
FROM (
    SELECT 
        c.customer_key,
        c.first_name,
        c.last_name,
        c.country,
        SUM(f.sales_amount) AS total_spent,
        COUNT(DISTINCT f.order_number) AS total_orders
    FROM gold.fact_sales f
    JOIN gold.dim_customers c 
        ON f.customer_key = c.customer_key
    GROUP BY c.customer_key, c.first_name, c.last_name, c.country
) t
ORDER BY total_spent DESC;


-- =========================================================================================
-- 3. Product Cost vs. Sales Margin by Category
-- Business Question: Which categories drive revenue but have thin margins?
-- =========================================================================================

SELECT 
    p.category,
    p.subcategory,
    AVG(p.product_cost)               AS avg_cost,
    AVG(f.price)                      AS avg_selling_price,
    AVG(f.price - p.product_cost)     AS avg_margin,
    CAST(
        AVG(f.price - p.product_cost) * 100.0 / NULLIF(AVG(f.price), 0) 
        AS DECIMAL(10,2)
    )                                  AS avg_margin_pct,
    SUM(f.sales_amount)               AS total_revenue
FROM gold.fact_sales f
JOIN gold.dim_products p 
    ON f.product_key = p.product_key
GROUP BY p.category, p.subcategory
ORDER BY total_revenue DESC;


-- =========================================================================================
-- 4. Customer Country Distribution + Average Order Value
-- Business Question: Where do we have the most customers vs. the highest-value customers?
-- =========================================================================================

SELECT 
    c.country,
    COUNT(DISTINCT c.customer_key)     AS customer_count,
    COUNT(DISTINCT f.order_number)     AS total_orders,
    SUM(f.sales_amount)                AS total_revenue,
    CAST(
        SUM(f.sales_amount) * 1.0 / NULLIF(COUNT(DISTINCT f.order_number), 0) 
        AS DECIMAL(10,2)
    )                                   AS avg_order_value,
    CAST(
        SUM(f.sales_amount) * 1.0 / NULLIF(COUNT(DISTINCT c.customer_key), 0) 
        AS DECIMAL(10,2)
    )                                   AS avg_revenue_per_customer
FROM gold.dim_customers c
LEFT JOIN gold.fact_sales f 
    ON c.customer_key = f.customer_key
GROUP BY c.country
ORDER BY total_revenue DESC;


-- =========================================================================================
-- 5. (Bonus) Customer Purchase Frequency / Repeat Purchase Rate
-- Business Question: What share of customers are repeat buyers vs. one-time buyers?
-- =========================================================================================

WITH order_counts AS (
    SELECT 
        c.customer_key,
        COUNT(DISTINCT f.order_number) AS num_orders
    FROM gold.dim_customers c
    LEFT JOIN gold.fact_sales f 
        ON c.customer_key = f.customer_key
    GROUP BY c.customer_key
)
SELECT 
    CASE 
        WHEN num_orders = 0 THEN 'No Orders'
        WHEN num_orders = 1 THEN 'One-Time Buyer'
        WHEN num_orders BETWEEN 2 AND 4 THEN 'Repeat Buyer (2-4 orders)'
        ELSE 'Loyal Buyer (5+ orders)'
    END AS customer_type,
    COUNT(*) AS customer_count,
    CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS DECIMAL(10,2)) AS pct_of_customers
FROM order_counts
GROUP BY 
    CASE 
        WHEN num_orders = 0 THEN 'No Orders'
        WHEN num_orders = 1 THEN 'One-Time Buyer'
        WHEN num_orders BETWEEN 2 AND 4 THEN 'Repeat Buyer (2-4 orders)'
        ELSE 'Loyal Buyer (5+ orders)'
    END
ORDER BY customer_count DESC;


-- =========================================================================================
-- 6. (Bonus) Top 10 Best-Selling Products by Revenue
-- Business Question: Which specific products should we prioritize for stock/marketing?
-- =========================================================================================

SELECT TOP 10
    p.product_name,
    p.category,
    p.subcategory,
    SUM(f.sales_amount) AS total_revenue,
    SUM(f.quantity)     AS total_units_sold,
    COUNT(DISTINCT f.order_number) AS num_orders
FROM gold.fact_sales f
JOIN gold.dim_products p 
    ON f.product_key = p.product_key
GROUP BY p.product_name, p.category, p.subcategory
ORDER BY total_revenue DESC;
