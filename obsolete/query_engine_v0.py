import sqlparse
import re
import pandas as pd
import pyarrow.parquet as pq
import os
from typing import List, Tuple, Optional, Dict, Any


class QueryEngine:
    """A lightweight SQL query engine for Parquet files.
    
    This engine supports basic SQL operations:
    - SELECT column1, column2, ... FROM table
    - WHERE with comparison operators (>, <, =, >=, <=, !=)
    - LIMIT
    - Simple AND/OR conditions
    """
    
    def __init__(self, data_path: str):
        """Initialize the query engine with a data directory.
        
        Args:
            data_path: Directory path where parquet files are stored
        """
        self.data_path = data_path
        # Cache for previously loaded tables to improve performance
        self.table_cache = {}
        
    def _extract_columns(self, select_stmt: str) -> List[str]:
        """Extract column names from SELECT statement.
        
        Args:
            select_stmt: The SELECT part of the SQL query
            
        Returns:
            List of column names
        """
        if '*' in select_stmt:
            return ['*']
        
        # Remove 'SELECT' and split by commas
        columns_part = select_stmt.strip().replace('SELECT', '', 1).strip()
        # Handle function calls like COUNT(), SUM(), etc.
        # This is a simplified approach; a production system would need more robust parsing
        columns = []
        
        # Simple parsing for comma-separated columns
        current_col = ""
        paren_count = 0
        
        for char in columns_part:
            if char == '(' and paren_count == 0:
                paren_count += 1
                current_col += char
            elif char == ')' and paren_count > 0:
                paren_count -= 1
                current_col += char
            elif char == ',' and paren_count == 0:
                columns.append(current_col.strip())
                current_col = ""
            else:
                current_col += char
                
        if current_col.strip():
            columns.append(current_col.strip())
            
        return columns
    
    def _extract_table_name(self, from_stmt: str) -> str:
        """Extract table name from FROM statement.
        
        Args:
            from_stmt: The FROM part of the SQL query
            
        Returns:
            Table name
        """
        # Remove 'FROM' and extract the table name
        match = re.search(r'FROM\s+([^\s;]+)', from_stmt, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        raise ValueError(f"Could not extract table name from: {from_stmt}")
    
    def _parse_condition(self, condition: str) -> Tuple[str, str, str]:
        """Parse a WHERE condition into column, operator, and value.
        
        Args:
            condition: A single condition from WHERE clause
            
        Returns:
            Tuple of (column_name, operator, value)
        """
        # Handle different operators
        operators = ['>=', '<=', '!=', '=', '>', '<']
        
        for op in operators:
            if op in condition:
                parts = condition.split(op, 1)
                column = parts[0].strip()
                value = parts[1].strip()
                
                # Handle quotes in string values
                if (value.startswith("'") and value.endswith("'")) or \
                   (value.startswith('"') and value.endswith('"')):
                    value = value[1:-1]
                else:
                    # Try to convert to numeric if possible
                    try:
                        value = int(value)
                    except ValueError:
                        try:
                            value = float(value)
                        except ValueError:
                            pass
                
                return column, op, value
        
        raise ValueError(f"No valid operator found in condition: {condition}")
    
    def _apply_condition(self, df: pd.DataFrame, column: str, op: str, value: Any) -> pd.DataFrame:
        """Apply a WHERE condition to the DataFrame.
        
        Args:
            df: Input DataFrame
            column: Column name to filter on
            op: Operator (>, <, =, >=, <=, !=)
            value: Value to compare against
            
        Returns:
            Filtered DataFrame
        """
        if op == '>':
            return df[df[column] > value]
        elif op == '<':
            return df[df[column] < value]
        elif op == '=':
            return df[df[column] == value]
        elif op == '>=':
            return df[df[column] >= value]
        elif op == '<=':
            return df[df[column] <= value]
        elif op == '!=':
            return df[df[column] != value]
        else:
            raise ValueError(f"Unsupported operator: {op}")
    
    def _apply_where_conditions(self, df: pd.DataFrame, where_clause: str) -> pd.DataFrame:
        """Apply WHERE conditions to the DataFrame.
        
        Args:
            df: Input DataFrame
            where_clause: The WHERE part of the SQL query
            
        Returns:
            Filtered DataFrame
        """
        if not where_clause:
            return df
            
        # Remove 'WHERE' keyword
        where_clause = re.sub(r'^\s*WHERE\s+', '', where_clause, flags=re.IGNORECASE)
        
        # Simple handling of AND conditions (doesn't handle OR or nested conditions)
        if ' AND ' in where_clause.upper():
            conditions = where_clause.split(' AND ')
            for condition in conditions:
                column, op, value = self._parse_condition(condition)
                df = self._apply_condition(df, column, op, value)
            return df
        # Simple handling of OR conditions
        elif ' OR ' in where_clause.upper():
            conditions = where_clause.split(' OR ')
            mask = pd.Series(False, index=df.index)
            for condition in conditions:
                column, op, value = self._parse_condition(condition)
                if op == '>':
                    mask |= (df[column] > value)
                elif op == '<':
                    mask |= (df[column] < value)
                elif op == '=':
                    mask |= (df[column] == value)
                elif op == '>=':
                    mask |= (df[column] >= value)
                elif op == '<=':
                    mask |= (df[column] <= value)
                elif op == '!=':
                    mask |= (df[column] != value)
            return df[mask]
        else:
            # Single condition
            column, op, value = self._parse_condition(where_clause)
            return self._apply_condition(df, column, op, value)
    
    def _extract_limit(self, query: str) -> Optional[int]:
        """Extract LIMIT value from query.
        
        Args:
            query: SQL query
            
        Returns:
            Limit value or None if not specified
        """
        match = re.search(r'LIMIT\s+(\d+)', query, re.IGNORECASE)
        if match:
            return int(match.group(1))
        return None
    
    def parse_query(self, query: str) -> Dict[str, Any]:
        """Parse SQL query into components.
        
        Args:
            query: SQL query string
            
        Returns:
            Dictionary with query components (select, from, where, limit)
        """
        # Remove comments and normalize whitespace
        query = re.sub(r'--.*?(\n|$)', ' ', query)
        query = re.sub(r'/\*.*?\*/', ' ', query, flags=re.DOTALL)
        query = ' '.join(query.split())
        
        # Extract the main parts of the query using regex
        select_match = re.search(r'(SELECT\s+.+?)\s+FROM', query, re.IGNORECASE)
        from_match = re.search(r'FROM\s+([^\s;]+)', query, re.IGNORECASE)
        where_match = re.search(r'WHERE\s+(.+?)(?:\s+LIMIT\s+\d+\s*|$)', query, re.IGNORECASE)
        
        if not select_match or not from_match:
            raise ValueError("Query must contain both SELECT and FROM clauses")
        
        select_part = select_match.group(1)
        from_part = from_match.group(0)  # Include 'FROM' keyword
        where_part = where_match.group(0) if where_match else ""
        
        # Extract LIMIT
        limit_value = self._extract_limit(query)
        
        # Extract columns and table name
        columns = self._extract_columns(select_part)
        table_name = self._extract_table_name(from_part)
        
        return {
            "columns": columns,
            "table": table_name,
            "where": where_part,
            "limit": limit_value
        }
    
    def _load_table(self, table_name: str) -> pd.DataFrame:
        """Load a parquet table into a DataFrame.
        
        Args:
            table_name: Name of the table (file name without extension)
            
        Returns:
            Pandas DataFrame
        """
        # Check if we have it cached
        if table_name in self.table_cache:
            return self.table_cache[table_name]
        
        # Possible locations to check
        possible_paths = [
            # Direct file in data_path
            os.path.join(self.data_path, f"{table_name}.parquet"),
            # File in a subdirectory with same name as table
            os.path.join(self.data_path, table_name, f"{table_name}.parquet"),
            # If table_name already has .parquet extension
            os.path.join(self.data_path, table_name),
            # In a subdirectory
            os.path.join(self.data_path, table_name)
        ]
        
        # Try all possible paths
        for file_path in possible_paths:
            if os.path.exists(file_path):
                try:
                    # If it's a directory, try to find a parquet file inside
                    if os.path.isdir(file_path):
                        parquet_files = [f for f in os.listdir(file_path) if f.endswith('.parquet')]
                        if parquet_files:
                            file_path = os.path.join(file_path, parquet_files[0])
                        else:
                            continue
                    
                    table = pq.read_table(file_path)
                    df = table.to_pandas()
                    # Cache the table for future use
                    self.table_cache[table_name] = df
                    print(f"Loaded table from: {file_path}")
                    return df
                except Exception as e:
                    print(f"Error reading {file_path}: {str(e)}")
                    continue
        
        # If we get here, we couldn't find the table
        raise FileNotFoundError(f"Parquet file not found for table: {table_name}. Searched paths: {possible_paths}")
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a SQL query on parquet files.
        
        Args:
            query: SQL query string
            
        Returns:
            Pandas DataFrame with query results
        """
        try:
            # Parse the query
            parsed = self.parse_query(query)
            
            # Load the table
            df = self._load_table(parsed["table"])
            
            # Apply WHERE conditions
            if parsed["where"]:
                df = self._apply_where_conditions(df, parsed["where"])
            
            # Select columns
            if parsed["columns"] != ['*']:
                # Handle case sensitivity by finding the actual column names
                actual_columns = []
                for col in parsed["columns"]:
                    # Try exact match first
                    if col in df.columns:
                        actual_columns.append(col)
                    else:
                        # Try case-insensitive match
                        matches = [c for c in df.columns if c.lower() == col.lower()]
                        if matches:
                            actual_columns.append(matches[0])
                        else:
                            raise ValueError(f"Column not found: {col}")
                
                df = df[actual_columns]
            
            # Apply LIMIT
            if parsed["limit"] is not None:
                df = df.head(parsed["limit"])
            
            return df
            
        except Exception as e:
            raise RuntimeError(f"Query execution failed: {str(e)}")

    def clear_cache(self):
        """Clear the table cache."""
        self.table_cache = {}


# Example usage
if __name__ == "__main__":
    engine = QueryEngine("data")
    
    # Simple query
    result1 = engine.execute_query("SELECT name, age FROM customers WHERE age > 30")
    print("Query 1 result:")
    print(result1)
    
    # Query with multiple conditions
    result2 = engine.execute_query("SELECT * FROM orders WHERE total > 100 AND status = 'completed'")
    print("\nQuery 2 result:")
    print(result2)
    
    # Query with LIMIT
    result3 = engine.execute_query("SELECT product_id, name, price FROM products WHERE price > 50 LIMIT 5")
    print("\nQuery 3 result:")
    print(result3)