
-- E-Commerce Sales Analysis
-- These queries are for SQLite.
-- First, import ecommerce_sales.csv into a table named sales.

-- 1. Total sales, profit, and quantity
SELECT
    ROUND(SUM(Sales), 2) AS total_sales,
    ROUND(SUM(Profit), 2) AS total_profit,
    SUM(Quantity) AS total_quantity
FROM sales;

-- 2. Sales by region
SELECT
    Region,
    ROUND(SUM(Sales), 2) AS total_sales,
    ROUND(SUM(Profit), 2) AS total_profit
FROM sales
GROUP BY Region
ORDER BY total_sales DESC;

-- 3. Sales by product category
SELECT
    Category,
    ROUND(SUM(Sales), 2) AS total_sales,
    ROUND(SUM(Profit), 2) AS total_profit
FROM sales
GROUP BY Category
ORDER BY total_sales DESC;

-- 4. Top 10 products by sales
SELECT
    Product,
    SUM(Quantity) AS units_sold,
    ROUND(SUM(Sales), 2) AS total_sales
FROM sales
GROUP BY Product
ORDER BY total_sales DESC
LIMIT 10;

-- 5. Monthly sales trend
SELECT
    SUBSTR(Order_Date, 1, 7) AS sales_month,
    ROUND(SUM(Sales), 2) AS monthly_sales,
    ROUND(SUM(Profit), 2) AS monthly_profit
FROM sales
GROUP BY SUBSTR(Order_Date, 1, 7)
ORDER BY sales_month;

-- 6. Discount impact
SELECT
    Discount,
    COUNT(*) AS order_count,
    ROUND(AVG(Sales), 2) AS average_sales,
    ROUND(AVG(Profit), 2) AS average_profit
FROM sales
GROUP BY Discount
ORDER BY Discount;