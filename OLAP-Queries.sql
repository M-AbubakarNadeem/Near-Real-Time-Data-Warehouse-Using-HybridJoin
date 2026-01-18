-- Walmart Data Warehouse - OLAP Queries
-- Task 3: DW Analysis

USE walmart_dw;

-- Q1. Top Revenue-Generating Products on Weekdays and Weekends with Monthly Drill-Down
-- Top 5 products by revenue, split by weekdays/weekends, with monthly breakdowns

SELECT * FROM (
    SELECT 
        CASE 
            WHEN d.is_weekend = 1 THEN 'Weekend'
            ELSE 'Weekday'
        END as day_type,
        d.year,
        d.month,
        d.month_name,
        p.product_id,
        p.product_category,
        COUNT(*) as transactions,
        SUM(f.total_amount) as total_revenue,
        RANK() OVER (
            PARTITION BY 
                CASE WHEN d.is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END,
                d.year,
                d.month
            ORDER BY SUM(f.total_amount) DESC
        ) as revenue_rank
    FROM fact_sales f
    JOIN dim_product p ON f.product_id = p.product_id
    JOIN dim_date d ON f.date_id = d.date_id
    WHERE d.year = 2020  -- Specified year
    GROUP BY 
        d.is_weekend,
        d.year,
        d.month,
        d.month_name,
        p.product_id,
        p.product_category
) ranked_products
WHERE revenue_rank <= 5
ORDER BY year, month, day_type, revenue_rank;

-- Q2. Customer Demographics by Purchase Amount with City Category Breakdown
-- Total purchase amounts by gender and age, detailed by city category

SELECT 
    c.gender,
    c.age,
    c.city_category,
    COUNT(DISTINCT c.customer_id) as unique_customers,
    COUNT(*) as total_purchases,
    SUM(f.total_amount) as total_purchase_amount,
    AVG(f.total_amount) as avg_purchase_amount,
    ROUND(SUM(f.total_amount) / SUM(SUM(f.total_amount)) OVER () * 100, 2) as revenue_percentage
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.gender, c.age, c.city_category
ORDER BY total_purchase_amount DESC;

-- Q3. Product Category Sales by Occupation
-- Total sales for each product category based on customer occupation

SELECT 
    c.occupation,
    p.product_category,
    COUNT(*) as total_sales,
    SUM(f.total_amount) as total_revenue,
    AVG(f.total_amount) as avg_sale,
    SUM(f.quantity) as total_quantity_sold
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY c.occupation, p.product_category
ORDER BY c.occupation, total_revenue DESC;

-- Q4. Total Purchases by Gender and Age Group with Quarterly Trend
-- Purchase amounts by gender and age across quarters for current year

SELECT 
    d.year,
    d.quarter,
    CONCAT('Q', d.quarter, '-', d.year) as period,
    c.gender,
    c.age,
    COUNT(*) as total_purchases,
    SUM(f.total_amount) as total_amount,
    AVG(f.total_amount) as avg_purchase,
    ROUND(SUM(f.total_amount) / SUM(SUM(f.total_amount)) OVER (PARTITION BY d.year, d.quarter) * 100, 2) as pct_of_quarter
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
JOIN dim_date d ON f.date_id = d.date_id
WHERE d.year = 2020  -- Current year
GROUP BY d.year, d.quarter, c.gender, c.age
ORDER BY d.year, d.quarter, total_amount DESC;

-- Q5. Top Occupations by Product Category Sales
-- Top 5 occupations driving sales within each product category

SELECT * FROM (
    SELECT 
        p.product_category,
        c.occupation,
        COUNT(*) as transactions,
        SUM(f.total_amount) as total_revenue,
        RANK() OVER (PARTITION BY p.product_category ORDER BY SUM(f.total_amount) DESC) as occupation_rank
    FROM fact_sales f
    JOIN dim_customer c ON f.customer_id = c.customer_id
    JOIN dim_product p ON f.product_id = p.product_id
    GROUP BY p.product_category, c.occupation
) ranked_occupations
WHERE occupation_rank <= 5
ORDER BY product_category, occupation_rank;

-- Q6. City Category Performance by Marital Status with Monthly Breakdown
-- Purchase amounts by city category and marital status over past 6 months

SELECT 
    c.city_category,
    c.marital_status,
    d.year,
    d.month,
    d.month_name,
    COUNT(*) as purchases,
    SUM(f.total_amount) as total_purchase_amount,
    AVG(f.total_amount) as avg_purchase,
    ROUND(SUM(f.total_amount) / SUM(SUM(f.total_amount)) OVER () * 100, 2) as pct_of_total
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
JOIN dim_date d ON f.date_id = d.date_id
WHERE d.year = 2020 AND d.month >= 7
GROUP BY c.city_category, c.marital_status, d.year, d.month, d.month_name
ORDER BY d.year, d.month, c.city_category, total_purchase_amount DESC;

-- Q7. Average Purchase Amount by Stay Duration and Gender
-- Average purchase amount based on years stayed in city and gender

SELECT 
    c.stay_in_current_city_years,
    c.gender,
    COUNT(*) as total_purchases,
    COUNT(DISTINCT c.customer_id) as unique_customers,
    SUM(f.total_amount) as total_revenue,
    AVG(f.total_amount) as avg_purchase_amount,
    ROUND(AVG(f.total_amount), 2) as avg_purchase_rounded
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.stay_in_current_city_years, c.gender
ORDER BY c.stay_in_current_city_years, c.gender;

-- Q8. Top 5 Revenue-Generating Cities by Product Category
-- Top 5 city categories by revenue, grouped by product category

SELECT * FROM (
    SELECT 
        p.product_category,
        c.city_category,
        COUNT(*) as transactions,
        SUM(f.total_amount) as total_revenue,
        AVG(f.total_amount) as avg_sale,
        RANK() OVER (PARTITION BY p.product_category ORDER BY SUM(f.total_amount) DESC) as city_rank
    FROM fact_sales f
    JOIN dim_customer c ON f.customer_id = c.customer_id
    JOIN dim_product p ON f.product_id = p.product_id
    GROUP BY p.product_category, c.city_category
) ranked_cities
WHERE city_rank <= 5
ORDER BY product_category, city_rank;

-- Q9. Monthly Sales Growth by Product Category
-- Month-over-month sales growth percentage for each product category

SELECT 
    p.product_category,
    d.year,
    d.month,
    d.month_name,
    SUM(f.total_amount) as current_month_sales,
    LAG(SUM(f.total_amount)) OVER (PARTITION BY p.product_category ORDER BY d.year, d.month) as previous_month_sales,
    ROUND(
        ((SUM(f.total_amount) - LAG(SUM(f.total_amount)) OVER (PARTITION BY p.product_category ORDER BY d.year, d.month)) / 
        LAG(SUM(f.total_amount)) OVER (PARTITION BY p.product_category ORDER BY d.year, d.month)) * 100, 
        2
    ) as growth_percentage
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
JOIN dim_date d ON f.date_id = d.date_id
WHERE d.year = 2020  -- Current year
GROUP BY p.product_category, d.year, d.month, d.month_name
ORDER BY p.product_category, d.year, d.month;

-- Q10. Weekend vs. Weekday Sales by Age Group
-- Compare total sales by age group for weekends vs weekdays

SELECT 
    c.age,
    CASE 
        WHEN d.is_weekend = 1 THEN 'Weekend'
        ELSE 'Weekday'
    END as day_type,
    COUNT(*) as transactions,
    SUM(f.total_amount) as total_sales,
    AVG(f.total_amount) as avg_transaction,
    COUNT(DISTINCT c.customer_id) as unique_customers
FROM fact_sales f
JOIN dim_customer c ON f.customer_id = c.customer_id
JOIN dim_date d ON f.date_id = d.date_id
WHERE d.year = 2020  -- Current year
GROUP BY c.age, d.is_weekend
ORDER BY c.age, day_type;

-- Q11. Top Revenue-Generating Products on Weekdays and Weekends with Monthly Drill-Down
-- Same as Q1 but with different presentation

SELECT * FROM (
    SELECT 
        p.product_id,
        p.product_category,
        CASE 
            WHEN d.is_weekend = 1 THEN 'Weekend'
            ELSE 'Weekday'
        END as day_type,
        d.year,
        d.month,
        d.month_name,
        SUM(f.total_amount) as monthly_revenue,
        COUNT(*) as transactions,
        RANK() OVER (
            PARTITION BY d.year, d.month, d.is_weekend 
            ORDER BY SUM(f.total_amount) DESC
        ) as product_rank
    FROM fact_sales f
    JOIN dim_product p ON f.product_id = p.product_id
    JOIN dim_date d ON f.date_id = d.date_id
    WHERE d.year = 2017  -- Specified year
    GROUP BY p.product_id, p.product_category, d.is_weekend, d.year, d.month, d.month_name
) ranked_products
WHERE product_rank <= 5
ORDER BY year, month, day_type, product_rank;

-- Q12. Trend Analysis of Store Revenue Growth Rate Quarterly for 2017
-- Revenue growth rate for each store quarterly in 2017

SELECT 
    s.store_name,
    d.quarter,
    CONCAT('Q', d.quarter, '-2017') as period,
    SUM(f.total_amount) as quarterly_revenue,
    LAG(SUM(f.total_amount)) OVER (PARTITION BY s.store_name ORDER BY d.quarter) as previous_quarter_revenue,
    ROUND(
        ((SUM(f.total_amount) - LAG(SUM(f.total_amount)) OVER (PARTITION BY s.store_name ORDER BY d.quarter)) / 
        LAG(SUM(f.total_amount)) OVER (PARTITION BY s.store_name ORDER BY d.quarter)) * 100,
        2
    ) as growth_rate_percentage
FROM fact_sales f
JOIN dim_store s ON f.store_id = s.store_id
JOIN dim_date d ON f.date_id = d.date_id
WHERE d.year = 2017
GROUP BY s.store_name, d.quarter
ORDER BY s.store_name, d.quarter;

-- Q13. Detailed Supplier Sales Contribution by Store and Product Name
-- Total sales contribution of each supplier by store and product

SELECT 
    s.store_name,
    sp.supplier_name,
    p.product_id,
    p.product_category,
    COUNT(*) as transactions,
    SUM(f.total_amount) as total_sales_contribution,
    SUM(f.quantity) as total_quantity,
    ROUND(SUM(f.total_amount) / SUM(SUM(f.total_amount)) OVER (PARTITION BY s.store_name) * 100, 2) as pct_of_store_sales
FROM fact_sales f
JOIN dim_store s ON f.store_id = s.store_id
JOIN dim_supplier sp ON f.supplier_id = sp.supplier_id
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY s.store_name, sp.supplier_name, p.product_id, p.product_category
ORDER BY s.store_name, sp.supplier_name, total_sales_contribution DESC;

-- Q14. Seasonal Analysis of Product Sales Using Dynamic Drill-Down
-- Total sales for each product by season

SELECT 
    p.product_id,
    p.product_category,
    d.season,
    d.year,
    COUNT(*) as transactions,
    SUM(f.total_amount) as seasonal_sales,
    AVG(f.total_amount) as avg_transaction,
    SUM(f.quantity) as total_quantity_sold,
    RANK() OVER (PARTITION BY d.season, d.year ORDER BY SUM(f.total_amount) DESC) as seasonal_rank
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY p.product_id, p.product_category, d.season, d.year
ORDER BY d.year, 
    CASE d.season
        WHEN 'Winter' THEN 1
        WHEN 'Spring' THEN 2
        WHEN 'Summer' THEN 3
        WHEN 'Fall' THEN 4
    END,
    seasonal_sales DESC;

-- Q15. Store-Wise and Supplier-Wise Monthly Revenue Volatility
-- Month-to-month revenue volatility for store and supplier pairs

SELECT 
    s.store_name,
    sp.supplier_name,
    d.year,
    d.month,
    d.month_name,
    SUM(f.total_amount) as monthly_revenue,
    LAG(SUM(f.total_amount)) OVER (
        PARTITION BY s.store_name, sp.supplier_name 
        ORDER BY d.year, d.month
    ) as previous_month_revenue,
    ROUND(
        ((SUM(f.total_amount) - LAG(SUM(f.total_amount)) OVER (
            PARTITION BY s.store_name, sp.supplier_name 
            ORDER BY d.year, d.month
        )) / LAG(SUM(f.total_amount)) OVER (
            PARTITION BY s.store_name, sp.supplier_name 
            ORDER BY d.year, d.month
        )) * 100,
        2
    ) as volatility_percentage
FROM fact_sales f
JOIN dim_store s ON f.store_id = s.store_id
JOIN dim_supplier sp ON f.supplier_id = sp.supplier_id
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY s.store_name, sp.supplier_name, d.year, d.month, d.month_name
ORDER BY s.store_name, sp.supplier_name, d.year, d.month;

-- Q16. Top 5 Products Purchased Together (Product Affinity Analysis)
-- Returns EMPTY because each order contains only 1 product

SELECT 
    f1.product_id as product_1,
    p1.product_category as category_1,
    f2.product_id as product_2,
    p2.product_category as category_2,
    COUNT(DISTINCT f1.order_id) as times_purchased_together,
    SUM(f1.total_amount + f2.total_amount) as combined_revenue
FROM fact_sales f1
JOIN fact_sales f2 ON f1.order_id = f2.order_id 
    AND f1.product_id < f2.product_id
JOIN dim_product p1 ON f1.product_id = p1.product_id
JOIN dim_product p2 ON f2.product_id = p2.product_id
GROUP BY f1.product_id, p1.product_category, f2.product_id, p2.product_category
ORDER BY times_purchased_together DESC, combined_revenue DESC
LIMIT 5;

-- Q16. Product Affinity Analysis
-- Top 20 Sequential Category Purchase Patterns
-- Analyzes which categories customers buy after each other within 7 days

SELECT 
    p.product_category,
    COUNT(*) as total_purchases,
    COUNT(DISTINCT f.customer_id) as unique_customers,
    ROUND(COUNT(*) / COUNT(DISTINCT f.customer_id), 1) as avg_purchases_per_customer,
    SUM(f.total_amount) as total_revenue,
    ROUND(SUM(f.total_amount) / COUNT(*) , 2) as avg_transaction_value,
    CASE 
        WHEN COUNT(*) / COUNT(DISTINCT f.customer_id) > 10 THEN 'High Repeat'
        WHEN COUNT(*) / COUNT(DISTINCT f.customer_id) > 5 THEN 'Medium Repeat'
        ELSE 'Low Repeat'
    END as repeat_category
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
GROUP BY p.product_category
ORDER BY avg_purchases_per_customer DESC;

-- Q17. Yearly Revenue Trends by Store, Supplier, and Product with ROLLUP
-- Hierarchical aggregation from product level to store total

SELECT 
    d.year,
    s.store_name,
    sp.supplier_name,
    p.product_id,
    COUNT(*) as transactions,
    SUM(f.total_amount) as total_revenue,
    SUM(f.quantity) as total_quantity
FROM fact_sales f
JOIN dim_store s ON f.store_id = s.store_id
JOIN dim_supplier sp ON f.supplier_id = sp.supplier_id
JOIN dim_product p ON f.product_id = p.product_id
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.year, s.store_name, sp.supplier_name, p.product_id WITH ROLLUP
ORDER BY d.year, s.store_name, sp.supplier_name, p.product_id;

-- Q18. Revenue and Volume-Based Sales Analysis for H1 and H2
-- First half vs second half of year analysis

SELECT 
    p.product_id,
    p.product_category,
    d.year,
    SUM(CASE WHEN d.month <= 6 THEN f.total_amount ELSE 0 END) as h1_revenue,
    SUM(CASE WHEN d.month > 6 THEN f.total_amount ELSE 0 END) as h2_revenue,
    SUM(CASE WHEN d.month <= 6 THEN f.quantity ELSE 0 END) as h1_quantity,
    SUM(CASE WHEN d.month > 6 THEN f.quantity ELSE 0 END) as h2_quantity,
    SUM(f.total_amount) as yearly_total_revenue,
    SUM(f.quantity) as yearly_total_quantity,
    ROUND(
        ((SUM(CASE WHEN d.month > 6 THEN f.total_amount ELSE 0 END) - 
          SUM(CASE WHEN d.month <= 6 THEN f.total_amount ELSE 0 END)) /
         SUM(CASE WHEN d.month <= 6 THEN f.total_amount ELSE 0 END)) * 100,
        2
    ) as h2_vs_h1_growth_pct
FROM fact_sales f
JOIN dim_product p ON f.product_id = p.product_id
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY p.product_id, p.product_category, d.year
ORDER BY d.year, yearly_total_revenue DESC;

-- Q19. Identify High Revenue Spikes in Product Sales and Highlight Outliers
-- Flag days where sales exceed twice the daily average

SELECT * FROM (
    SELECT 
        p.product_id,
        p.product_category,
        d.date,
        SUM(f.total_amount) as daily_sales,
        AVG(SUM(f.total_amount)) OVER (PARTITION BY p.product_id) as avg_daily_sales,
        ROUND(
            SUM(f.total_amount) / AVG(SUM(f.total_amount)) OVER (PARTITION BY p.product_id),
            2
        ) as spike_multiplier,
        CASE 
            WHEN SUM(f.total_amount) > 2 * AVG(SUM(f.total_amount)) OVER (PARTITION BY p.product_id)
            THEN 'OUTLIER/SPIKE'
            ELSE 'Normal'
        END as spike_flag
    FROM fact_sales f
    JOIN dim_product p ON f.product_id = p.product_id
    JOIN dim_date d ON f.date_id = d.date_id
    GROUP BY p.product_id, p.product_category, d.date
) sales_with_flags
WHERE spike_flag = 'OUTLIER/SPIKE'
ORDER BY spike_multiplier DESC;

-- Q20. Create View STORE_QUARTERLY_SALES for Optimized Sales Analysis
-- Materialized view for quarterly sales by store

DROP VIEW IF EXISTS STORE_QUARTERLY_SALES;

CREATE VIEW STORE_QUARTERLY_SALES AS
SELECT 
    s.store_name,
    d.year,
    d.quarter,
    CONCAT('Q', d.quarter, '-', d.year) as period,
    COUNT(*) as total_transactions,
    SUM(f.total_amount) as quarterly_sales,
    AVG(f.total_amount) as avg_transaction,
    SUM(f.quantity) as total_quantity_sold,
    COUNT(DISTINCT f.customer_id) as unique_customers,
    COUNT(DISTINCT f.product_id) as unique_products
FROM fact_sales f
JOIN dim_store s ON f.store_id = s.store_id
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY s.store_name, d.year, d.quarter
ORDER BY s.store_name, d.year, d.quarter;

-- the view
SELECT * FROM STORE_QUARTERLY_SALES
ORDER BY store_name, year, quarter;

SHOW FULL TABLES WHERE Table_type = 'VIEW';