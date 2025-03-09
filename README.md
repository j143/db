# db
implementation of a columnar storage SQL database


## Data



## 1. Orders Table

```py
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
```

| order_id | customer_id | order_date  | total_amount | status     |
|----------|-------------|-------------|--------------|------------|
| 1001     | 1           | 2023-01-01  | 120.50       | completed  |
| 1002     | 2           | 2023-01-02  | 65.25        | completed  |
| 1003     | 3           | 2023-01-03  | 210.75       | processing |
| 1004     | 1           | 2023-01-04  | 45.00        | completed  |
| 1005     | 4           | 2023-01-05  | 180.25       | shipped    |
| 1006     | 2           | 2023-01-06  | 95.50        | completed  |
| 1007     | 5           | 2023-01-07  | 150.75       | processing |
| 1008     | 3           | 2023-01-08  | 55.25        | completed  |
| 1009     | 4           | 2023-01-09  | 220.00       | shipped    |
| 1010     | 5           | 2023-01-10  | 65.75        | completed  |
| 1011     | 1           | 2023-01-11  | 130.50       | processing |
| 1012     | 2           | 2023-01-12  | 75.25        | completed  |
| 1013     | 3           | 2023-01-13  | 190.75       | shipped    |
| 1014     | 4           | 2023-01-14  | 85.00        | completed  |
| 1015     | 5           | 2023-01-15  | 170.25       | processing |
| 1016     | 1           | 2023-01-16  | 105.50       | completed  |
| 1017     | 2           | 2023-01-17  | 140.75       | shipped    |
| 1018     | 3           | 2023-01-18  | 45.25        | completed  |
| 1019     | 4           | 2023-01-19  | 200.00       | processing |
| 1020     | 5           | 2023-01-20  | 75.75        | shipped    |


## 2. Products Table

```py
products_data = {
    'product_id': list(range(101, 121)),
    'name': [f'Product {i}' for i in range(1, 21)],
    'category': ['Electronics'] * 5 + ['Clothing'] * 5 + ['Home'] * 5 + ['Books'] * 5,
    'price': [199.99, 299.99, 149.99, 249.99, 179.99, 49.99, 39.99, 59.99, 29.99, 69.99,
              89.99, 99.99, 129.99, 149.99, 79.99, 19.99, 24.99, 15.99, 29.99, 34.99],
    'inventory': [45, 20, 60, 15, 30, 100, 200, 75, 150, 50, 40, 25, 35, 15, 55, 300, 250, 400, 150, 200]
}
products_df = pd.DataFrame(products_data)
```

| product_id | name       | category    | price  | inventory |
|------------|------------|-------------|--------|-----------|
| 101        | Product 1  | Electronics | 199.99 | 45        |
| 102        | Product 2  | Electronics | 299.99 | 20        |
| 103        | Product 3  | Electronics | 149.99 | 60        |
| 104        | Product 4  | Electronics | 249.99 | 15        |
| 105        | Product 5  | Electronics | 179.99 | 30        |
| 106        | Product 6  | Clothing    | 49.99  | 100       |
| 107        | Product 7  | Clothing    | 39.99  | 200       |
| 108        | Product 8  | Clothing    | 59.99  | 75        |
| 109        | Product 9  | Clothing    | 29.99  | 150       |
| 110        | Product 10 | Clothing    | 69.99  | 50        |
| 111        | Product 11 | Home        | 89.99  | 40        |
| 112        | Product 12 | Home        | 99.99  | 25        |
| 113        | Product 13 | Home        | 129.99 | 35        |
| 114        | Product 14 | Home        | 149.99 | 15        |
| 115        | Product 15 | Home        | 79.99  | 55        |
| 116        | Product 16 | Books       | 19.99  | 300       |
| 117        | Product 17 | Books       | 24.99  | 250       |
| 118        | Product 18 | Books       | 15.99  | 400       |
| 119        | Product 19 | Books       | 29.99  | 150       |
| 120        | Product 20 | Books       | 34.99  | 200       |


## 3. Order Details Table

```py
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
```

| order_id | product_id | quantity | unit_price | subtotal |
|----------|------------|----------|------------|----------|
| 1001     | 101        | 1        | 199.99     | 199.99   |
| 1001     | 105        | 2        | 179.99     | 359.98   |
| 1002     | 103        | 1        | 149.99     | 149.99   |
| 1003     | 102        | 1        | 299.99     | 299.99   |
| 1003     | 107        | 3        | 39.99      | 119.97   |
| 1003     | 110        | 2        | 69.99      | 139.98   |
| 1004     | 104        | 1        | 249.99     | 249.99   |
| 1005     | 106        | 2        | 49.99      | 99.98    |
| 1005     | 108        | 1        | 59.99      | 59.99    |
| 1006     | 109        | 1        | 29.99      | 29.99    |
| 1007     | 112        | 2        | 99.99      | 199.98   |
| 1007     | 115        | 1        | 79.99      | 79.99    |
| 1008     | 111        | 3        | 89.99      | 269.97   |
| 1009     | 113        | 2        | 149.99     | 299.98   |
| 1009     | 114        | 1        | 129.99     | 129.99   |
| 1010     | 116        | 2        | 19.99      | 39.98    |
| 1011     | 117        | 1        | 24.99      | 24.99    |
| 1012     | 118        | 3        | 15.99      | 47.97    |
| 1013     | 119        | 2        | 29.99      | 59.98    |
| 1014     | 120        | 1        | 34.99      | 34.99    |
| 1015     | 101        | 1        | 199.99     | 199.99   |
| 1016     | 102        | 1        | 299.99     | 299.99   |
| 1017     | 103        | 2        | 149.99     | 299.98   |
| 1018     | 104        | 3        | 249.99     | 749.97   |
| 1019     | 105        | 1        | 179.99     | 179.99   |
| 1020     | 106        | 2        | 49.99      | 99.98    |


## 4. Customers Table

```py
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
```

| id  | name           | age | email                  | city        | join_date   | total_spend |
|-----|----------------|-----|------------------------|-------------|-------------|-------------|
| 1   | John Smith     | 35  | john.smith@example.com | New York    | 2023-01-01  | 450.75      |
| 2   | Jane Doe       | 28  | jane.doe@example.com   | Los Angeles | 2023-01-08  | 235.50      |
| 3   | Robert Johnson | 42  | robert.j@example.com   | Chicago     | 2023-01-15  | 320.25      |
| 4   | Maria Garcia   | 31  | maria.g@example.com    | Miami       | 2023-01-22  | 540.75      |
| 5   | David Kim      | 25  | david.kim@example.com  | Seattle     | 2023-01-29  | 185.50      |
