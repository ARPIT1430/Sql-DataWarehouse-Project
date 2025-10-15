/*
===============================================================================
Data Segmentation Analysis
===============================================================================
Purpose:
    - To group data into meaningful categories for targeted insights.
    - For customer segmentation, product categorization, or regional analysis.

SQL Functions Used:
    - CASE: Defines custom segmentation logic.
    - GROUP BY: Groups data into segments.

--Segment products into cost ranges and count how many products fall into each segment

-- Group customers into three segments based on their spending behavior:
	- VIP: Customers with at least 12 months of history and spending more than €5,000.
	- Regular: Customers with at least 12 months of history but spending €5,000 or less.
	- New: Customers with a lifespan less than 12 months.
And find the total number of customers by each group

===============================================================================
*/

--Segment products into cost ranges and count how many products fall into each segment
WITH product_segments AS (
    SELECT
        product_key,
        product_name,
        product_cost,
        CASE 
            WHEN product_cost < 100 THEN 'Below 100'
            WHEN product_cost BETWEEN 100 AND 500 THEN '100-500'
            WHEN product_cost BETWEEN 500 AND 1000 THEN '500-1000'
            ELSE 'Above 1000'
        END AS cost_range
    FROM gold.dim_products
)
SELECT 
    cost_range,
    COUNT(product_key) AS total_products
FROM product_segments
GROUP BY cost_range
ORDER BY total_products DESC;



/*
-- Group customers into three segments based on their spending behavior:
	- VIP: Customers with at least 12 months of history and spending more than €5,000.
	- Regular: Customers with at least 12 months of history but spending €5,000 or less.
	- New: Customers with a lifespan less than 12 months.
And find the total number of customers by each group
*/

WITH customer_spending AS (

SELECT 
    c.customer_key , 
    SUM(f.sales_amount) AS Total_sales ,
    MIN(f.order_date) AS first_order , 
    MAX(f.order_date) AS last_order , 
    DATEDIFF(month ,MIN(f.order_date) , MAX(f.order_date) ) AS lifespan 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c 
ON f.customer_key = c.customer_key
GROUP BY c.customer_key
)

SELECT 
    customer_segment , 
    COUNT(customer_key) AS Total_customers 

FROM(
    SELECT 
       customer_key ,
       lifespan ,
       total_sales ,
       CASE 
          WHEN lifespan > 12 AND total_sales >5000 THEN 'VIP'
          WHEN lifespan < 12 AND total_sales <=5000 THEN 'Regular'
          ELSE 'New' 
        END AS customer_segment 
        FROM customer_spending ) AS segmented_csutomers 
GROUP BY customer_segment
ORDER BY Total_customers 







