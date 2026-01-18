-- ============================================
-- Walmart Data Warehouse - Star Schema
-- ============================================

-- Use the walmart_dw database
-- CREATE DATABASE IF NOT EXISTS walmart_dw;
USE walmart_dw;

-- Drop existing tables (in reverse order due to foreign keys)
DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_store;
DROP TABLE IF EXISTS dim_supplier;
DROP TABLE IF EXISTS dim_date;

-- ============================================
-- DIMENSION TABLES
-- ============================================

-- 1. Customer Dimension
CREATE TABLE dim_customer (
    customer_id VARCHAR(50) PRIMARY KEY,
    gender VARCHAR(10),
    age VARCHAR(20),
    occupation VARCHAR(50),
    city_category VARCHAR(10),
    stay_in_current_city_years VARCHAR(10),
    marital_status VARCHAR(10),
    INDEX idx_gender (gender),
    INDEX idx_age (age),
    INDEX idx_occupation (occupation),
    INDEX idx_city_category (city_category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 
COMMENT='Customer demographic information';

-- 2. Product Dimension
CREATE TABLE dim_product (
    product_id VARCHAR(50) PRIMARY KEY,
    product_category VARCHAR(100),
    price DECIMAL(10, 2),
    INDEX idx_product_category (product_category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT='Product catalog information';

-- 3. Store Dimension
CREATE TABLE dim_store (
    store_id VARCHAR(50) PRIMARY KEY,
    store_name VARCHAR(200),
    INDEX idx_store_name (store_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT='Store location information';

-- 4. Supplier Dimension
CREATE TABLE dim_supplier (
    supplier_id VARCHAR(50) PRIMARY KEY,
    supplier_name VARCHAR(200),
    INDEX idx_supplier_name (supplier_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT='Supplier information';

-- 5. Date Dimension
CREATE TABLE dim_date (
    date_id INTEGER PRIMARY KEY,
    date DATE NOT NULL,
    day_of_week INTEGER COMMENT '1=Sunday, 7=Saturday',
    day_name VARCHAR(15),
    is_weekend BOOLEAN,
    week_of_year INTEGER,
    month INTEGER,
    month_name VARCHAR(15),
    quarter INTEGER,
    year INTEGER,
    season VARCHAR(15),
    UNIQUE KEY uk_date (date),
    INDEX idx_year (year),
    INDEX idx_quarter (year, quarter),
    INDEX idx_month (year, month),
    INDEX idx_weekend (is_weekend)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT='Date dimension for time-based analysis';

-- ============================================
-- FACT TABLE
-- ============================================

CREATE TABLE fact_sales (
    sale_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    date_id INTEGER NOT NULL,
    store_id VARCHAR(50) NOT NULL,
    supplier_id VARCHAR(50) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    total_amount DECIMAL(12, 2) NOT NULL,
    
    -- Foreign Key Constraints
    CONSTRAINT fk_customer FOREIGN KEY (customer_id) 
        REFERENCES dim_customer(customer_id)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_product FOREIGN KEY (product_id) 
        REFERENCES dim_product(product_id)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_date FOREIGN KEY (date_id) 
        REFERENCES dim_date(date_id)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_store FOREIGN KEY (store_id) 
        REFERENCES dim_store(store_id)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_supplier FOREIGN KEY (supplier_id) 
        REFERENCES dim_supplier(supplier_id)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    
    -- Performance Indexes
    INDEX idx_fact_customer (customer_id),
    INDEX idx_fact_product (product_id),
    INDEX idx_fact_date (date_id),
    INDEX idx_fact_store (store_id),
    INDEX idx_fact_supplier (supplier_id),
    INDEX idx_fact_order (order_id),
    
    -- Composite indexes for common query patterns
    INDEX idx_customer_date (customer_id, date_id),
    INDEX idx_product_date (product_id, date_id),
    INDEX idx_store_date (store_id, date_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT='Fact table containing sales transactions';

-- ============================================
-- POPULATE DATE DIMENSION
-- MySQL version using a stored procedure
-- ============================================

DROP PROCEDURE IF EXISTS populate_date_dimension;

DELIMITER //

CREATE PROCEDURE populate_date_dimension(
    IN start_date DATE,
    IN end_date DATE
)
BEGIN
    DECLARE v_current_date DATE;
    DECLARE v_date_id_val INT;
    DECLARE v_dow INT;
    DECLARE v_season_val VARCHAR(15);
    
    SET v_current_date = start_date;
    
    -- Disable foreign key checks temporarily
    SET FOREIGN_KEY_CHECKS = 0;
    
    -- Truncate existing data
    TRUNCATE TABLE dim_date;
    
    -- Re-enable foreign key checks
    SET FOREIGN_KEY_CHECKS = 1;
    
    WHILE v_current_date <= end_date DO
        SET v_date_id_val = CAST(DATE_FORMAT(v_current_date, '%Y%m%d') AS UNSIGNED);
        SET v_dow = DAYOFWEEK(v_current_date);
        
        -- Determine season based on month (Northern Hemisphere)
        SET v_season_val = CASE 
            WHEN MONTH(v_current_date) IN (3, 4, 5) THEN 'Spring'
            WHEN MONTH(v_current_date) IN (6, 7, 8) THEN 'Summer'
            WHEN MONTH(v_current_date) IN (9, 10, 11) THEN 'Fall'
            ELSE 'Winter'
        END;
        
        INSERT INTO dim_date (
            date_id,
            date,
            day_of_week,
            day_name,
            is_weekend,
            week_of_year,
            month,
            month_name,
            quarter,
            year,
            season
        ) VALUES (
            v_date_id_val,
            v_current_date,
            v_dow,
            DAYNAME(v_current_date),
            IF(v_dow IN (1, 7), TRUE, FALSE),
            WEEK(v_current_date, 3),
            MONTH(v_current_date),
            MONTHNAME(v_current_date),
            QUARTER(v_current_date),
            YEAR(v_current_date),
            v_season_val
        );
        
        SET v_current_date = DATE_ADD(v_current_date, INTERVAL 1 DAY);
    END WHILE;
    
    SELECT CONCAT('Date dimension populated with ', COUNT(*), ' records') AS Status
    FROM dim_date;
END//

DELIMITER ;

-- Call the procedure to populate date dimension
-- Covering 10 years: 2016-2025
CALL populate_date_dimension('2016-01-01', '2025-12-31');

-- ============================================
-- Verification Queries
-- ============================================

-- Check table creation
SELECT 
    TABLE_NAME,
    TABLE_ROWS,
    ROUND(DATA_LENGTH / 1024 / 1024, 2) AS Size_MB,
    TABLE_COMMENT
FROM information_schema.TABLES 
WHERE TABLE_SCHEMA = 'walmart_dw'
  AND TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_NAME;

-- Check date dimension population
SELECT 
    COUNT(*) as total_dates, 
    MIN(date) as earliest_date, 
    MAX(date) as latest_date,
    COUNT(DISTINCT year) as years_covered
FROM dim_date;

-- Sample date records
SELECT * FROM dim_date 
WHERE date BETWEEN '2017-01-01' AND '2017-01-07'
ORDER BY date;

-- Display confirmation message
SELECT '✓ Star Schema created successfully!' as Status;
