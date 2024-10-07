import time
from ezSpark.uploader import DatabaseFactory
from pyspark.sql.types import StringType, IntegerType, DecimalType, DateType, BooleanType, TimestampType 

# Define the path to your data file
file_path = r"test_data.csv"  # Adjust to your file's actual path

# Source file type ('excel', 'csv')
file_type = "csv"

# Database details
db_type = "mssql"  # Supported types: 'mssql'
db_details = {
    "db_url": "jdbc:sqlserver://your_server;databaseName=your_database",
    "db_table": "your_table",
    "db_user": "your_username",
    "db_password": "your_password"
}

# Column mappings (source column to SQL column)
column_mapping = {
    "Order ID": "order_id",
    "Customer Name": "customer_name",
    "Order Date": "order_date",
    "Ship Date": "ship_date",
    "Product Category": "product_category",
    "Quantity Ordered": "quantity_ordered",
    "Order Value": "order_value",
    "Discount Rate": "discount_rate",
    "Is Priority": "is_priority",
    "Country": "country",
    "City": "city",
    "Postal Code": "postal_code",
    "Product Code": "product_code",
    "Sales Person": "sales_person",
    "Customer Email": "customer_email",
    "Delivery Status": "delivery_status",
    "Warehouse Code": "warehouse_code",
    "Shipping Company": "shipping_company",
    "Payment Method": "payment_method",
    "Transaction ID": "transaction_id",
    "Currency": "currency",
    "Is Return": "is_return",
    "Product Weight": "product_weight",
    "Product Dimensions": "product_dimensions",
    "Shipping Cost": "shipping_cost",
    "Profit Margin": "profit_margin",
    "Vendor Name": "vendor_name",
    "Warehouse Address": "warehouse_address",
    "Vendor Code": "vendor_code",
    "Purchase Order Number": "purchase_order_number",
    "Product Warranty": "product_warranty",
    "Return Reason": "return_reason",
    "Return Date": "return_date",
    "Warehouse Manager": "warehouse_manager",
    "Customer Rating": "customer_rating",
    "Product Review": "product_review",
    "Shipping Duration": "shipping_duration",
    "Currency Rate": "currency_rate",
    "Product Volume": "product_volume",
    "Stock Availability": "stock_availability",
    "Warehouse Location": "warehouse_location",
    "Vendor Rating": "vendor_rating",
    "Shipping Mode": "shipping_mode",
    "Discount Code": "discount_code",
    "Transaction Status": "transaction_status",
    "Order Source": "order_source",
    "Customer Feedback": "customer_feedback",
    "Customer Segment": "customer_segment",
    "Delivery Instructions": "delivery_instructions",
    "Is Gift": "is_gift"
}

# Column data types (based on SQL types)
column_types = {
    "order_id": StringType(),
    "customer_name": StringType(),
    "order_date": DateType(),
    "ship_date": DateType(),
    "product_category": StringType(),
    "quantity_ordered": IntegerType(),
    "order_value": DecimalType(10, 2),
    "discount_rate": DecimalType(5, 4),
    "is_priority": BooleanType(),
    "country": StringType(),
    "city": StringType(),
    "postal_code": StringType(),
    "product_code": StringType(),
    "sales_person": StringType(),
    "customer_email": StringType(),
    "delivery_status": StringType(),
    "warehouse_code": StringType(),
    "shipping_company": StringType(),
    "payment_method": StringType(),
    "transaction_id": StringType(),
    "currency": StringType(),
    "is_return": BooleanType(),
    "product_weight": DecimalType(6, 2),
    "product_dimensions": StringType(),
    "shipping_cost": DecimalType(10, 2),
    "profit_margin": DecimalType(5, 4),
    "vendor_name": StringType(),
    "warehouse_address": StringType(),
    "vendor_code": StringType(),
    "purchase_order_number": StringType(),
    "product_warranty": IntegerType(),
    "return_reason": StringType(),
    "return_date": DateType(),
    "warehouse_manager": StringType(),
    "customer_rating": DecimalType(3, 2),
    "product_review": StringType(),
    "shipping_duration": IntegerType(),
    "currency_rate": DecimalType(5, 4),
    "product_volume": DecimalType(6, 2),
    "stock_availability": BooleanType(),
    "warehouse_location": StringType(),
    "vendor_rating": DecimalType(3, 2),
    "shipping_mode": StringType(),
    "discount_code": StringType(),
    "transaction_status": StringType(),
    "order_source": StringType(),
    "customer_feedback": StringType(),
    "customer_segment": StringType(),
    "delivery_instructions": StringType(),
    "is_gift": BooleanType()
}

# Date formats for date columns (only needed for uploading csv)
date_formats = {
    "order_date": "dd/MM/yyyy",
    "ship_date": "dd/MM/yyyy",
    "return_date": "dd/MM/yyyy"
}


# Optional parameters
sheet_name = "Sheet1"  # For ExcelUploader
spark_configs = {
    "spark.driver.memory": "12g",
    "spark.executor.memory": "12g"
}
repartition_num = 200

# Measure the start time
start_time = time.time()

# Create an uploader instance using the factory
uploader = DatabaseFactory.get_database_uploader(
    file_type, file_path, db_type, db_details,
    sheet_name=sheet_name, date_formats=date_formats, spark_configs=spark_configs, repartition_num=repartition_num
)

# Process and upload the data
uploader.process_and_upload(column_mapping, column_types)

# Close the Spark session
uploader.close()

# Measure the end time
end_time = time.time()

# Calculate the total time taken
upload_time = end_time - start_time

print(f"Data upload completed in {upload_time:.2f} seconds.")