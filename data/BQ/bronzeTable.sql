-- Orders Table
CREATE EXTERNAL TABLE IF NOT EXISTS `ashuproj-454704.bronze_dataset.orders`(
    order_id INT64,
    customer_id INT64,
    order_date DATE, 
    total_amount FLOAT64,
    updated_at TIMESTAMP
)
OPTIONS (
  format = 'JSON',
  uris = ['gs://gcsprojectbkt/landing/retailer-db/orders/*.json']
);

-- Customers Table
CREATE EXTERNAL TABLE IF NOT EXISTS `ashuproj-454704.bronze_dataset.customers`
(
    customer_id INT64,
    name STRING,
    email STRING,
    updated_at TIMESTAMP
)
OPTIONS (
    format = 'JSON',
    uris = ['gs://gcsprojectbkt/landing/retailer-db/customers/*.json']
);

-- Products Table
CREATE EXTERNAL TABLE IF NOT EXISTS `ashuproj-454704.bronze_dataset.products`
(
    product_id INT64,
    name STRING,
    category_id INT64,
    price FLOAT64,
    updated_at TIMESTAMP
)
OPTIONS (
    format = 'JSON',
    uris = ['gs://gcsprojectbkt/landing/retailer-db/products/*.json']
);

-- Categories Table
CREATE EXTERNAL TABLE IF NOT EXISTS `ashuproj-454704.bronze_dataset.categories`
(
    category_id INT64,
    name STRING,
    updated_at TIMESTAMP
)
OPTIONS (
    format = 'JSON',
    uris = ['gs://gcsprojectbkt/landing/retailer-db/categories/*.json']
);

-- Order Items Table
CREATE EXTERNAL TABLE IF NOT EXISTS `ashuproj-454704.bronze_dataset.order_items`
(
    order_item_id INT64,
    order_id INT64,
    product_id INT64,
    quantity INT64,
    price FLOAT64,
    updated_at TIMESTAMP
)
OPTIONS (
    format = 'JSON',
    uris = ['gs://gcsprojectbkt/landing/retailer-db/order_items/*.json']
);

-------------------------------------------------------------------------------------------------------------

-- Suppliers Table
CREATE EXTERNAL TABLE IF NOT EXISTS `ashuproj-454704.bronze_dataset.suppliers` (
    supplier_id INT64,
    supplier_name STRING,
    contact_name STRING,
    phone STRING,
    email STRING,
    address STRING,
    city STRING,
    country STRING,
    created_at TIMESTAMP
)
OPTIONS (
  format = 'JSON',
  uris = ['gs://gcsprojectbkt/landing/supplier-db/suppliers/*.json']
);

-- Product Suppliers Table (Mapping suppliers to products)
CREATE EXTERNAL TABLE IF NOT EXISTS `ashuproj-454704.bronze_dataset.product_suppliers` (
    supplier_id INT64,
    product_id INT64,
    supply_price FLOAT64,
    last_updated TIMESTAMP
)
OPTIONS (
  format = 'JSON',
  uris = ['gs://gcsprojectbkt/landing/supplier-db/product_suppliers/*.json']
);

-------------------------------------------------------------------------------------------------------------

-- Customer Reviews (Using PARQUET for better performance)
CREATE OR REPLACE EXTERNAL TABLE `ashuproj-454704.bronze_dataset.customer_reviews` (
  id INT64,
  customer_id INT64,
  product_id INT64,
  rating INT64,
  review_text STRING,
  review_date TIMESTAMP
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://gcsprojectbkt/landing/customer_reviews/customer_reviews_*.parquet']
);
