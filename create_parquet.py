import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

# Create data directory if it doesn't exist
os.makedirs("data", exist_ok=True)

# 1. Orders data
orders_data = {
    'order_id': list(range(1001, 1021)),
    'customer_id': [1, 2, 3, 1, 4, 2, 5, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5],
    'order_date': pd.date_range(start='2023-01-01', periods=20, freq='D'),
    'total_amount': [120.50, 65.25, 210.75, 45.00, 180.25, 95.50, 150.75, 55.25, 220.00, 65.75, 
                     130.50, 75.25, 190.75, 85.00, 170.25, 105.50, 140.75, 45.25, 200.00, 75.75],
    'status': ['completed', 'completed', 'processing', 'completed', 'shipped', 'completed', 
               'processing', 'completed', 'shipped', 'completed', 'processing', 'completed', 
               'shipped', 'completed', 'processing', 'completed', 'shipped', 'completed', 
               'processing', 'shipped']
}
orders_df = pd.DataFrame(orders_data)

# Create orders directory
os.makedirs("data/orders", exist_ok=True)
# Save as parquet
pq.write_table(pa.Table.from_pandas(orders_df), "data/orders.parquet")
print("Created orders.parquet")

# 2. Products data
products_data = {
    'product_id': list(range(101, 121)),
    'name': [f'Product {i}' for i in range(1, 21)],
    'category': ['Electronics'] * 5 + ['Clothing'] * 5 + ['Home'] * 5 + ['Books'] * 5,
    'price': [199.99, 299.99, 149.99, 249.99, 179.99, 49.99, 39.99, 59.99, 29.99, 69.99,
              89.99, 99.99, 129.99, 149.99, 79.99, 19.99, 24.99, 15.99, 29.99, 34.99],
    'inventory': [45, 20, 60, 15, 30, 100, 200, 75, 150, 50, 40, 25, 35, 15, 55, 300, 250, 400, 150, 200]
}
products_df = pd.DataFrame(products_data)

# Create products directory
os.makedirs("data/products", exist_ok=True)
# Save as parquet
pq.write_table(pa.Table.from_pandas(products_df), "data/products.parquet")
print("Created products.parquet")

# 3. Order details data (links orders and products)
order_details_data = {
    'order_id': [1001, 1001, 1002, 1003, 1003, 1003, 1004, 1005, 1005, 1006, 1007, 1007, 1008,
                 1009, 1009, 1010, 1011, 1012, 1013, 1014, 1015, 1016, 1017, 1018, 1019, 1020],
    'product_id': [101, 105, 103, 102, 107, 110, 104, 106, 108, 109, 112, 115, 111, 113, 114, 116,
                   117, 118, 119, 120, 101, 102, 103, 104, 105, 106],
    'quantity': [1, 2, 1, 1, 3, 2, 1, 2, 1, 1, 2, 1, 3, 2, 1, 2, 1, 3, 2, 1, 1, 1, 2, 3, 1, 2],
    'unit_price': [199.99, 179.99, 149.99, 299.99, 39.99, 69.99, 249.99, 49.99, 59.99, 29.99, 99.99,
                   79.99, 89.99, 149.99, 129.99, 19.99, 24.99, 15.99, 29.99, 34.99, 199.99, 299.99,
                   149.99, 249.99, 179.99, 49.99],
    'subtotal': [199.99, 359.98, 149.99, 299.99, 119.97, 139.98, 249.99, 99.98, 59.99, 29.99, 199.98,
                 79.99, 269.97, 299.98, 129.99, 39.98, 24.99, 47.97, 59.98, 34.99, 199.99, 299.99,
                 299.98, 749.97, 179.99, 99.98]
}
order_details_df = pd.DataFrame(order_details_data)

# Create order_details directory
os.makedirs("data/order_details", exist_ok=True)
# Save as parquet
pq.write_table(pa.Table.from_pandas(order_details_df), "data/order_details.parquet")
print("Created order_details.parquet")

# 4. Update existing customers data with more fields
customers_data = {
    'id': list(range(1, 6)),
    'name': ['John Smith', 'Jane Doe', 'Robert Johnson', 'Maria Garcia', 'David Kim'],
    'age': [35, 28, 42, 31, 25],
    'email': ['john.smith@example.com', 'jane.doe@example.com', 'robert.j@example.com', 
              'maria.g@example.com', 'david.kim@example.com'],
    'city': ['New York', 'Los Angeles', 'Chicago', 'Miami', 'Seattle'],
    'join_date': pd.date_range(start='2023-01-01', periods=5, freq='W'),
    'total_spend': [450.75, 235.50, 320.25, 540.75, 185.50]
}
customers_df = pd.DataFrame(customers_data)

# Create customers directory if it doesn't exist already
os.makedirs("data/customers", exist_ok=True)
# Save as parquet
pq.write_table(pa.Table.from_pandas(customers_df), "data/customers/customers.parquet")
print("Updated customers.parquet in data/customers directory")

print("All parquet files created successfully!")