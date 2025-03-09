import pyarrow.parquet as pq

class QueryEngine:
    def __init__(self, data_path):
        self.data_path = data_path
    
    def execute_query(self, query):
        table = pq.read_table(f"{self.data_path}/customers.parquet")
        df = table.to_pandas()

        if "WHERE" in query:
            column, condition = query.split("WHERE")[1].strip().split(">")
            column = column.strip()
            value = int(condition.strip())
            df = df[df[column] > value]
        
        return df

# example use
engine = QueryEngine("data")
result = engine.execute_query("SELECT name, age FROM customers WHERE age > 30")

print(result)
