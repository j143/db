import pyarrow as pa
import pyarrow.parquet as pq

# sample data
data = {
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [30, 25, 35]
}


# create a pyarrow table
table = pa.table(data)

# save as parquet (columnar format)
pq.write_table(table, 'data/customers.parquet')
print("Data written in columnar format successfully.")

