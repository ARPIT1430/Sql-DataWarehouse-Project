/*
===============================================================================
Ranking Analysis
===============================================================================
Purpose:
    - To rank items (e.g., products, customers) based on performance or other metrics.
    - To identify top performers or laggards.

SQL Functions Used:
    - Window Ranking Functions: RANK(), DENSE_RANK(), ROW_NUMBER(), TOP
    - Clauses: GROUP BY, ORDER BY


-- Which 5 products Generating the Highest Revenue?
-- What are the 5 worst-performing products in terms of sales?
-- Find the top 10 customers who have generated the highest revenue
-- The 3 customers with the fewest orders placed

===============================================================================
*/


-- Which 5 products Generating the Highest Revenue?


SELECT TOP 5 
    p.product_name , 
    p.product_number , 
    SUM(f.sales_amount) AS Total_revenue 
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON f.product_key =p.product_key
GROUP BY
    p.product_name , 
    p.product_number
ORDER BY Total_revenue DESC


----------------------------------------------------------------------------------------------------
  

 -- What are the 5 worst-performing products in terms of sales? 

 
SELECT TOP 5 
    p.product_name , 
    p.product_number , 
    SUM(f.sales_amount) AS Total_revenue 
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON f.product_key =p.product_key
GROUP BY
      p.product_name , 
    p.product_number
ORDER BY Total_revenue 



-------------------------------------------------------------------------------------------------------------



 -- Find the top 10 customers who have generated the highest revenue ?
  

 SELECT *
 FROM (
    SELECT  
        c.customer_key , 
        c.first_name,
        c.last_name ,
        SUM(f.sales_amount) AS Total_revenue_per_customer ,
        ROW_NUMBER() OVER (ORDER BY SUM(f.sales_amount) DESC ) AS Customer_ranking_revenue 
    FROM gold.fact_sales f 
    LEFT JOIN gold.dim_customers c 
    ON f.customer_key = c.customer_key
    GROUP BY
        c.customer_key ,
        c.first_name,
        c.last_name
) t 
WHERE Customer_ranking_revenue <=10 




--------------------------------------------------------------------------------------------------



-- The 3 customers with the fewest orders placed ?

  
SELECT *
FROM (
SELECT 
    c.customer_key , 
    c.first_name,
    c.last_name ,
    COUNT(f.order_number) AS Total_orders_per_customer ,
    ROW_NUMBER () OVER (ORDER BY COUNT(f.order_number)  )  AS Customer_ranking_orders
FROM gold.fact_sales f 
LEFT JOIN gold.dim_customers c 
ON f.customer_key = c.customer_key
GROUP BY
    c.customer_key ,
    c.first_name,
    c.last_name
) t 
WHERE Customer_ranking_orders <=3 


-------------------------------------------------------------------------------------------------------
