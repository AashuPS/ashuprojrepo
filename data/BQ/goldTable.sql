-- 1. Sales Summary (sales_summary)
CREATE TABLE IF NOT EXISTS `ashuproj-454704.gold_dataset.sales_summary`
AS
SELECT 
    o.order_date,
    p.category_id,
    c.name AS category_name,
    oi.product_id,
    p.name AS product_name,
    SUM(oi.quantity) AS total_units_sold,
    SUM(oi.price * oi.quantity) AS total_sales,
    COUNT(DISTINCT o.customer_id) AS unique_customers
FROM `ashuproj-454704.silver_dataset.orders` o
JOIN `ashuproj-454704.silver_dataset.order_items` oi ON o.order_id = oi.order_id
JOIN `ashuproj-454704.silver_dataset.products` p ON oi.product_id = p.product_id
JOIN `ashuproj-454704.silver_dataset.categories` c ON p.category_id = c.category_id
WHERE o.is_active = TRUE
GROUP BY o.order_date, p.category_id, c.name, oi.product_id, p.name;

-----------------------------------------------------------------------------------------------------------

-- 2. Customer Engagement Metrics (customer_engagement)
CREATE TABLE IF NOT EXISTS `ashuproj-454704.gold_dataset.customer_engagement`
AS
SELECT 
    c.customer_id,
    c.name AS customer_name,
    COUNT(o.order_id) AS total_orders,
    COALESCE(SUM(oi.price * oi.quantity), 0) AS total_spent,
    MAX(o.order_date) AS last_order_date,
    IFNULL(DATE_DIFF(CURRENT_DATE(), MAX(o.order_date), DAY), NULL) AS days_since_last_order,
    COALESCE(AVG(oi.price * oi.quantity), 0) AS avg_order_value
FROM `ashuproj-454704.silver_dataset.customers` c
LEFT JOIN `ashuproj-454704.silver_dataset.orders` o ON c.customer_id = o.customer_id
LEFT JOIN `ashuproj-454704.silver_dataset.order_items` oi ON o.order_id = oi.order_id
WHERE c.is_active = TRUE
GROUP BY c.customer_id, c.name;

-----------------------------------------------------------------------------------------------------------

-- 3. Product Performance (product_performance)
CREATE TABLE IF NOT EXISTS `ashuproj-454704.gold_dataset.product_performance`
AS
SELECT 
    p.product_id,
    p.name AS product_name,
    p.category_id,
    c.name AS category_name,
    ps.supplier_id,
    s.supplier_name,
    COALESCE(SUM(oi.quantity), 0) AS total_units_sold,
    COALESCE(SUM(oi.price * oi.quantity), 0) AS total_revenue,
    COALESCE(AVG(cr.rating), 0) AS avg_rating,
    COUNT(cr.review_text) AS total_reviews
FROM `ashuproj-454704.silver_dataset.products` p
LEFT JOIN `ashuproj-454704.silver_dataset.categories` c ON p.category_id = c.category_id
LEFT JOIN `ashuproj-454704.silver_dataset.product_suppliers` ps ON p.product_id = ps.product_id
LEFT JOIN `ashuproj-454704.silver_dataset.suppliers` s ON ps.supplier_id = s.supplier_id
LEFT JOIN `ashuproj-454704.silver_dataset.order_items` oi ON p.product_id = oi.product_id
LEFT JOIN `ashuproj-454704.silver_dataset.customer_reviews` cr ON p.product_id = cr.product_id
WHERE p.is_quarantined = FALSE
GROUP BY p.product_id, p.name, p.category_id, c.name, ps.supplier_id, s.supplier_name;

-----------------------------------------------------------------------------------------------------------

-- 4. Supplier Performance (supplier_analysis)
CREATE TABLE IF NOT EXISTS `ashuproj-454704.gold_dataset.supplier_analysis`
AS
SELECT 
    s.supplier_id,
    s.supplier_name,
    COUNT(DISTINCT ps.product_id) AS total_products_supplied,
    COALESCE(SUM(oi.quantity), 0) AS total_units_sold,
    COALESCE(SUM(oi.price * oi.quantity), 0) AS total_revenue
FROM `ashuproj-454704.silver_dataset.suppliers` s
LEFT JOIN `ashuproj-454704.silver_dataset.product_suppliers` ps ON s.supplier_id = ps.supplier_id
LEFT JOIN `ashuproj-454704.silver_dataset.order_items` oi ON ps.product_id = oi.product_id
WHERE s.is_quarantined = FALSE
GROUP BY s.supplier_id, s.supplier_name;

-----------------------------------------------------------------------------------------------------------

-- 5. Customer Reviews Summary (customer_reviews_summary)
CREATE TABLE IF NOT EXISTS `ashuproj-454704.gold_dataset.customer_reviews_summary`
AS
SELECT 
    p.product_id,
    p.name AS product_name,
    COALESCE(AVG(cr.rating), 0) AS avg_rating,
    COUNT(cr.review_text) AS total_reviews,
    COUNT(CASE WHEN cr.rating >= 4 THEN 1 END) AS positive_reviews,
    COUNT(CASE WHEN cr.rating < 3 THEN 1 END) AS negative_reviews
FROM `ashuproj-454704.silver_dataset.products` p
LEFT JOIN `ashuproj-454704.silver_dataset.customer_reviews` cr ON p.product_id = cr.product_id
WHERE p.is_quarantined = FALSE
GROUP BY p.product_id, p.name;
