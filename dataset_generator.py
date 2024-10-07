import pandas as pd
import numpy as np
import random
from datetime import datetime
# Function to create random dates within a specified range
def random_dates(start, end, num_dates):
    start_u = start.timestamp()
    end_u = end.timestamp()
    return [datetime.fromtimestamp(random.uniform(start_u, end_u)) for _ in range(num_dates)]

# Function to create random data for testing
def generate_test_data(num_rows):
    # Generating random data for 50 columns
    data = {
        'Order ID': [f'ORD{i}' for i in range(num_rows)],
        'Customer Name': [f'Customer {i}' for i in range(num_rows)],
        'Order Date': pd.date_range('2020-01-01', periods=num_rows, freq='min'),  # Updated frequency for Order Date
        'Ship Date': pd.date_range('2020-01-02', periods=num_rows, freq='min'),  # Updated frequency for Ship Date
        'Product Category': [random.choice(['Electronics', 'Clothing', 'Furniture']) for _ in range(num_rows)],
        'Quantity Ordered': np.random.randint(1, 100, size=num_rows),
        'Order Value': np.random.uniform(10.0, 1000.0, size=num_rows),
        'Discount Rate': np.random.uniform(0.0, 0.3, size=num_rows),
        'Is Priority': np.random.choice([True, False], size=num_rows),
        'Country': np.random.choice(['USA', 'UK', 'Germany', 'France'], size=num_rows),
        'City': [f'City {i % 100}' for i in range(num_rows)],
        'Postal Code': np.random.randint(10000, 99999, size=num_rows),
        'Product Code': [f'P{i}' for i in range(num_rows)],
        'Sales Person': [f'Sales {i % 50}' for i in range(num_rows)],
        'Customer Email': [f'customer{i}@example.com' for i in range(num_rows)],
        'Delivery Status': np.random.choice(['Delivered', 'In Transit', 'Canceled'], size=num_rows),
        'Warehouse Code': [f'WH{i % 20}' for i in range(num_rows)],
        'Shipping Company': [f'Company {i % 10}' for i in range(num_rows)],
        'Payment Method': np.random.choice(['Credit Card', 'PayPal', 'Wire Transfer'], size=num_rows),
        'Transaction ID': [f'TRAN{i}' for i in range(num_rows)],
        'Currency': np.random.choice(['USD', 'EUR', 'GBP'], size=num_rows),
        'Is Return': np.random.choice([True, False], size=num_rows),
        'Product Weight': np.random.uniform(0.5, 20.0, size=num_rows),
        'Product Dimensions': [f'{np.random.randint(10, 100)}x{np.random.randint(10, 100)}x{np.random.randint(10, 100)}' for _ in range(num_rows)],
        'Shipping Cost': np.random.uniform(5.0, 50.0, size=num_rows),
        'Profit Margin': np.random.uniform(0.05, 0.5, size=num_rows),
        'Vendor Name': [f'Vendor {i % 30}' for i in range(num_rows)],
        'Warehouse Address': [f'Address {i % 20}' for i in range(num_rows)],
        'Vendor Code': [f'VENDOR{i % 30}' for i in range(num_rows)],
        'Purchase Order Number': [f'PO{i}' for i in range(num_rows)],
        'Product Warranty': np.random.choice([6, 12, 24], size=num_rows),
        'Return Reason': np.random.choice(['Damaged', 'Not as Described', 'Other'], size=num_rows),
        # Random dates for 'Return Date' within a specific range
        'Return Date': random_dates(datetime(2020, 2, 1), datetime(2021, 2, 1), num_rows),
        'Warehouse Manager': [f'Manager {i % 20}' for i in range(num_rows)],
        'Customer Rating': np.random.uniform(1.0, 5.0, size=num_rows),
        'Product Review': [f'Review {i % 50}' for i in range(num_rows)],
        'Shipping Duration': np.random.randint(1, 10, size=num_rows),
        'Currency Rate': np.random.uniform(0.8, 1.2, size=num_rows),
        'Product Volume': np.random.uniform(0.1, 2.0, size=num_rows),
        'Stock Availability': np.random.choice([True, False], size=num_rows),
        'Warehouse Location': [f'Location {i % 20}' for i in range(num_rows)],
        'Vendor Rating': np.random.uniform(1.0, 5.0, size=num_rows),
        'Shipping Mode': np.random.choice(['Air', 'Sea', 'Ground'], size=num_rows),
        'Discount Code': [f'DC{i % 100}' for i in range(num_rows)],
        'Transaction Status': np.random.choice(['Completed', 'Failed', 'Pending'], size=num_rows),
        'Order Source': np.random.choice(['Online', 'In-Store'], size=num_rows),
        'Customer Feedback': [f'Feedback {i % 100}' for i in range(num_rows)],
        'Customer Segment': np.random.choice(['Retail', 'Wholesale'], size=num_rows),
        'Delivery Instructions': [f'Instructions {i % 50}' for i in range(num_rows)],
        'Is Gift': np.random.choice([True, False], size=num_rows)
    }

    return pd.DataFrame(data)

# Generate test data (50 columns, 500,000 rows)
df = generate_test_data(100000)

# Save to Excel
# df.to_excel('test_data.xlsx', index=False)
df.to_csv('test_data.csv', index=False)
print("Test data saved to 'test_data.xlsx'")
