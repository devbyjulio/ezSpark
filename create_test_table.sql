CREATE TABLE Orders (
    order_id VARCHAR(255) NOT NULL,
    customer_name VARCHAR(255),
    order_date DATE,
    ship_date DATE,
    product_category VARCHAR(100),
    quantity_ordered INT,
    order_value DECIMAL(10, 2),
    discount_rate DECIMAL(5, 4),
    is_priority BIT,
    country VARCHAR(100),
    city VARCHAR(255),
    postal_code VARCHAR(10),
    product_code VARCHAR(255),
    sales_person VARCHAR(255),
    customer_email VARCHAR(255),
    delivery_status VARCHAR(100),
    warehouse_code VARCHAR(50),
    shipping_company VARCHAR(255),
    payment_method VARCHAR(100),
    transaction_id VARCHAR(255),
    currency VARCHAR(3),
    is_return BIT,
    product_weight DECIMAL(6, 2),
    product_dimensions VARCHAR(50),
    shipping_cost DECIMAL(10, 2),
    profit_margin DECIMAL(5, 4),
    vendor_name VARCHAR(255),
    warehouse_address VARCHAR(255),
    vendor_code VARCHAR(255),
    purchase_order_number VARCHAR(255),
    product_warranty INT,
    return_reason VARCHAR(255),
    return_date DATE,
    warehouse_manager VARCHAR(255),
    customer_rating DECIMAL(3, 2),
    product_review TEXT,
    shipping_duration INT,
    currency_rate DECIMAL(5, 4),
    product_volume DECIMAL(6, 2),
    stock_availability BIT,
    warehouse_location VARCHAR(255),
    vendor_rating DECIMAL(3, 2),
    shipping_mode VARCHAR(100),
    discount_code VARCHAR(50),
    transaction_status VARCHAR(50),
    order_source VARCHAR(100),
    customer_feedback TEXT,
    customer_segment VARCHAR(100),
    delivery_instructions TEXT,
    is_gift BIT
);