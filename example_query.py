import sys
from query_engine import QueryEngine

# Create the query engine
engine = QueryEngine("data")

# Example queries to demonstrate the engine's capabilities
QUERIES = [
    # Basic query
    "SELECT name, age FROM customers WHERE age > 30",
    
    # Join-like query (will need to be manually joined)
    "SELECT * FROM orders WHERE customer_id = 1",
    
    # Query with multiple conditions
    "SELECT * FROM products WHERE price > 100 AND category = 'Electronics'",
    
    # Query with OR condition
    "SELECT product_id, name, price FROM products WHERE category = 'Clothing' OR price < 30",
    
    # Aggregation query (will need manual aggregation)
    "SELECT * FROM order_details WHERE order_id = 1001",
    
    # Query with LIMIT
    "SELECT * FROM orders LIMIT 5"
]

def run_all_examples():
    """Run all example queries and print results."""
    for i, query in enumerate(QUERIES, 1):
        print(f"\n{'='*80}")
        print(f"Query {i}: {query}")
        print(f"{'='*80}")
        
        try:
            result = engine.execute_query(query)
            print(f"Result: {len(result)} rows")
            print(result)
        except Exception as e:
            print(f"Error executing query: {str(e)}")

def main():
    """Main function to run examples or a specific query."""
    if len(sys.argv) > 1:
        # If arguments provided, treat as a SQL query
        query = ' '.join(sys.argv[1:])
        print(f"Executing: {query}")
        try:
            result = engine.execute_query(query)
            print(result)
        except Exception as e:
            print(f"Error: {str(e)}")
    else:
        # Otherwise run the examples
        run_all_examples()

if __name__ == "__main__":
    main()