import os
import re
from typing import List, Dict, Tuple, Optional, Any, Union

import pandas as pd
import pyarrow.parquet as pq


class QueryEngine:
    """A lightweight SQL query engine for Parquet files.
    
    This engine supports basic SQL operations:
    - SELECT with column selection
    - FROM for table (parquet file) selection
    - WHERE with comparison operators (>, <, =, >=, <=, !=)
    - AND/OR conditions
    - LIMIT clause
    """
    
    def __init__(self, data_path: str):
        """Initialize the query engine with a data directory.
        
        Args:
            data_path: Directory path where parquet files are stored
        """
        self.data_path = data_path
        self.table_cache = {}  # Cache for loaded tables
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a SQL query on parquet files.
        
        Args:
            query: SQL query string
            
        Returns:
            DataFrame with query results
        
        Raises:
            RuntimeError: If query execution fails
        """
        try:
            # Parse the query
            parsed_query = self._parse_query(query)
            
            # Load the table
            df = self._load_table(parsed_query["table"])
            
            # Apply WHERE conditions
            if parsed_query["where"]:
                df = self._apply_where_conditions(df, parsed_query["where"])
            
            # Select columns
            if parsed_query["columns"] != ['*']:
                df = self._select_columns(df, parsed_query["columns"])
            
            # Apply LIMIT
            if parsed_query["limit"] is not None:
                df = df.head(parsed_query["limit"])
            
            return df
            
        except Exception as e:
            raise RuntimeError(f"Query execution failed: {str(e)}")
    
    def clear_cache(self):
        """Clear the table cache."""
        self.table_cache = {}

    # ---------- Query Parsing Methods ----------
    
    def _parse_query(self, query: str) -> Dict[str, Any]:
        """Parse SQL query into components.
        
        Args:
            query: SQL query string
            
        Returns:
            Dictionary with query components (columns, table, where, limit)
        
        Raises:
            ValueError: If query is invalid
        """
        # Clean query
        query = self._clean_query(query)
        
        # Extract query parts
        select_match = re.search(r'(SELECT\s+.+?)\s+FROM', query, re.IGNORECASE)
        from_match = re.search(r'FROM\s+([^\s;]+)', query, re.IGNORECASE)
        where_match = re.search(r'WHERE\s+(.+?)(?:\s+LIMIT\s+\d+\s*|$)', query, re.IGNORECASE)
        
        if not select_match or not from_match:
            raise ValueError("Query must contain both SELECT and FROM clauses")
        
        select_part = select_match.group(1)
        from_part = from_match.group(0)
        where_part = where_match.group(0) if where_match else ""
        
        # Parse components
        columns = self._extract_columns(select_part)
        table_name = self._extract_table_name(from_part)
        limit_value = self._extract_limit(query)
        
        return {
            "columns": columns,
            "table": table_name,
            "where": where_part,
            "limit": limit_value
        }
    
    def _clean_query(self, query: str) -> str:
        """Remove comments and normalize whitespace.
        
        Args:
            query: Raw SQL query
            
        Returns:
            Cleaned query
        """
        # Remove single-line comments
        query = re.sub(r'--.*?(\n|$)', ' ', query)
        # Remove multi-line comments
        query = re.sub(r'/\*.*?\*/', ' ', query, flags=re.DOTALL)
        # Normalize whitespace
        return ' '.join(query.split())
    
    def _extract_columns(self, select_stmt: str) -> List[str]:
        """Extract column names from SELECT statement.
        
        Args:
            select_stmt: The SELECT part of the SQL query
            
        Returns:
            List of column names
        """
        if '*' in select_stmt:
            return ['*']
        
        # Remove 'SELECT' and parse columns
        columns_part = select_stmt.strip().replace('SELECT', '', 1).strip()
        columns = []
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
        
        Raises:
            ValueError: If table name cannot be extracted
        """
        match = re.search(r'FROM\s+([^\s;]+)', from_stmt, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        raise ValueError(f"Could not extract table name from: {from_stmt}")
    
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
    
    # ---------- Data Loading Methods ----------
    
    def _load_table(self, table_name: str) -> pd.DataFrame:
        """Load a parquet table into a DataFrame.
        
        Args:
            table_name: Name of the table (file name without extension)
            
        Returns:
            Pandas DataFrame
        
        Raises:
            FileNotFoundError: If parquet file is not found
        """
        # Check cache first
        if table_name in self.table_cache:
            return self.table_cache[table_name]
        
        # Possible file paths to search
        possible_paths = [
            os.path.join(self.data_path, f"{table_name}.parquet"),
            os.path.join(self.data_path, table_name, f"{table_name}.parquet"),
            os.path.join(self.data_path, table_name),
        ]
        
        # Try all possible paths
        for file_path in possible_paths:
            if not os.path.exists(file_path):
                continue
                
            try:
                # Handle directory case
                if os.path.isdir(file_path):
                    parquet_files = [f for f in os.listdir(file_path) if f.endswith('.parquet')]
                    if not parquet_files:
                        continue
                    file_path = os.path.join(file_path, parquet_files[0])
                
                # Load the parquet file
                table = pq.read_table(file_path)
                df = table.to_pandas()
                self.table_cache[table_name] = df
                return df
                
            except Exception as e:
                continue  # Try next path
        
        # If we get here, we couldn't find the table
        raise FileNotFoundError(f"Parquet file not found for table: {table_name}")
    
    def _select_columns(self, df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """Select columns from DataFrame, handling case sensitivity.
        
        Args:
            df: Input DataFrame
            columns: List of column names to select
            
        Returns:
            DataFrame with selected columns
            
        Raises:
            ValueError: If column is not found
        """
        actual_columns = []
        
        for col in columns:
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
        
        return df[actual_columns]
    
    # ---------- WHERE Clause Handling ----------
    
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
        
        # Handle AND conditions
        if ' AND ' in where_clause.upper():
            conditions = where_clause.split(' AND ')
            for condition in conditions:
                column, op, value = self._parse_condition(condition)
                df = self._apply_condition(df, column, op, value)
            return df
            
        # Handle OR conditions
        elif ' OR ' in where_clause.upper():
            conditions = where_clause.split(' OR ')
            mask = pd.Series(False, index=df.index)
            
            for condition in conditions:
                column, op, value = self._parse_condition(condition)
                mask |= self._create_condition_mask(df, column, op, value)
                
            return df[mask]
            
        else:
            # Single condition
            column, op, value = self._parse_condition(where_clause)
            return self._apply_condition(df, column, op, value)
    
    def _parse_condition(self, condition: str) -> Tuple[str, str, Any]:
        """Parse a WHERE condition into column, operator, and value.
        
        Args:
            condition: A single condition from WHERE clause
            
        Returns:
            Tuple of (column_name, operator, value)
            
        Raises:
            ValueError: If no valid operator is found
        """
        operators = ['>=', '<=', '!=', '=', '>', '<']
        
        for op in operators:
            if op in condition:
                parts = condition.split(op, 1)
                column = parts[0].strip()
                value = parts[1].strip()
                
                # Handle quoted strings
                if (value.startswith("'") and value.endswith("'")) or \
                   (value.startswith('"') and value.endswith('"')):
                    value = value[1:-1]
                else:
                    # Try numeric conversion
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
            
        Raises:
            ValueError: If operator is not supported
        """
        mask = self._create_condition_mask(df, column, op, value)
        return df[mask]
    
    def _create_condition_mask(self, df: pd.DataFrame, column: str, op: str, value: Any) -> pd.Series:
        """Create a boolean mask for a condition.
        
        Args:
            df: Input DataFrame
            column: Column name
            op: Operator
            value: Value to compare against
            
        Returns:
            Boolean mask
            
        Raises:
            ValueError: If operator is not supported
        """
        if op == '>':
            return df[column] > value
        elif op == '<':
            return df[column] < value
        elif op == '=':
            return df[column] == value
        elif op == '>=':
            return df[column] >= value
        elif op == '<=':
            return df[column] <= value
        elif op == '!=':
            return df[column] != value
        else:
            raise ValueError(f"Unsupported operator: {op}")


# Example usage
if __name__ == "__main__":
    # Create engine instance
    engine = QueryEngine("data")
    
    # Example queries
    queries = [
        "SELECT name, age FROM customers WHERE age > 30",
        "SELECT * FROM orders WHERE total_amount > 100 AND status = 'completed'",
        "SELECT product_id, name, price FROM products WHERE price > 50.0 LIMIT 5"
    ]
    
    # Execute queries
    for i, query in enumerate(queries, 1):
        print(f"\nQuery {i}: {query}")
        try:
            result = engine.execute_query(query)
            print(f"Results ({len(result)} rows):")
            print(result)
        except Exception as e:
            print(f"Error: {e}")