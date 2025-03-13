import os
import re
import concurrent.futures
from typing import List, Dict, Tuple, Optional, Any, Union

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc


class DremelQueryEngine:
    """A SQL query engine for Parquet files inspired by Google's Dremel architecture.
    
    This implementation incorporates key Dremel concepts:
    - Columnar storage and processing
    - Parallel execution
    - Projection pushdown
    - Predicate pushdown
    - Nested schema support
    """
    
    def __init__(self, data_path: str, max_workers: int = None):
        """Initialize the Dremel-inspired query engine.
        
        Args:
            data_path: Directory path where parquet files are stored
            max_workers: Maximum number of parallel workers (defaults to CPU count)
        """
        self.data_path = data_path
        self.max_workers = max_workers
        self.table_cache = {}  # Cache for table metadata
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a SQL query using Dremel-inspired execution.
        
        Args:
            query: SQL query string
            
        Returns:
            DataFrame with query results
        """
        try:
            # Parse query
            parsed_query = self._parse_query(query)
            
            # Get table schema and metadata
            table_info = self._get_table_info(parsed_query["table"])
            
            # Perform projection pushdown (select only needed columns)
            needed_columns = self._get_required_columns(
                table_info, 
                parsed_query["columns"], 
                parsed_query["where"]
            )
            
            # Perform predicate pushdown (filter at storage level when possible)
            filters = self._extract_pushdown_filters(parsed_query["where"])
            
            # Execute query with parallel processing
            result = self._execute_distributed_query(
                table_info,
                needed_columns,
                filters,
                parsed_query
            )
            
            return result
            
        except Exception as e:
            raise RuntimeError(f"Query execution failed: {str(e)}")
    
    # ---------- Dremel-inspired Processing Methods ----------
    
    def _get_table_info(self, table_name: str) -> Dict:
        """Get table metadata and file location information.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary with table metadata
        """
        # Check cache first
        if table_name in self.table_cache:
            return self.table_cache[table_name]
            
        # Locate the parquet file(s)
        table_files = self._find_table_files(table_name)
        if not table_files:
            raise FileNotFoundError(f"Parquet file not found for table: {table_name}")
        
        # Read schema from the first file
        schema = pq.read_schema(table_files[0])
        
        # Get metadata for all files (sizes, row counts, etc.)
        file_metadata = []
        for file_path in table_files:
            metadata = pq.read_metadata(file_path)
            file_metadata.append({
                'path': file_path,
                'num_rows': metadata.num_rows,
                'size_bytes': os.path.getsize(file_path)
            })
        
        table_info = {
            'name': table_name,
            'schema': schema,
            'files': file_metadata,
            'total_rows': sum(m['num_rows'] for m in file_metadata)
        }
        
        # Cache the table info
        self.table_cache[table_name] = table_info
        return table_info
    
    def _find_table_files(self, table_name: str) -> List[str]:
        """Find all parquet files for a table, supporting partitioning.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of file paths
        """
        # Possible locations to check
        base_paths = [
            os.path.join(self.data_path, f"{table_name}.parquet"),
            os.path.join(self.data_path, table_name),
            os.path.join(self.data_path, f"{table_name}.parquet.dir")
        ]
        
        parquet_files = []
        
        for base_path in base_paths:
            if os.path.isfile(base_path) and base_path.endswith('.parquet'):
                # Single file case
                parquet_files.append(base_path)
            elif os.path.isdir(base_path):
                # Directory case (could be partitioned data)
                for root, _, files in os.walk(base_path):
                    for file in files:
                        if file.endswith('.parquet'):
                            parquet_files.append(os.path.join(root, file))
        
        return parquet_files
    
    def _get_required_columns(self, table_info: Dict, select_columns: List[str], where_clause: str) -> List[str]:
        """Determine which columns are needed for the query (projection pushdown).
        
        Args:
            table_info: Table metadata
            select_columns: Columns in SELECT clause
            where_clause: WHERE clause
            
        Returns:
            List of column names needed
        """
        needed_columns = set()
        
        # Add columns from SELECT
        if select_columns == ['*']:
            # All columns needed
            needed_columns.update(table_info['schema'].names)
        else:
            needed_columns.update(select_columns)
        
        # Add columns from WHERE
        if where_clause:
            where_columns = self._extract_columns_from_where(where_clause)
            needed_columns.update(where_columns)
        
        return list(needed_columns)
    
    def _extract_columns_from_where(self, where_clause: str) -> List[str]:
        """Extract column names referenced in WHERE clause.
        
        Args:
            where_clause: WHERE clause of the query
            
        Returns:
            List of column names
        """
        # Simple extraction using regex
        # In a real implementation, this would use a proper SQL parser
        where_clause = re.sub(r'^\s*WHERE\s+', '', where_clause, flags=re.IGNORECASE)
        
        # Split on AND/OR
        conditions = re.split(r'\s+AND\s+|\s+OR\s+', where_clause, flags=re.IGNORECASE)
        
        # Extract column names from each condition
        columns = []
        for condition in conditions:
            # Look for column name (assumed to be before operator)
            operators = ['>=', '<=', '!=', '=', '>', '<']
            for op in operators:
                if op in condition:
                    column = condition.split(op)[0].strip()
                    columns.append(column)
                    break
        
        return columns
    
    def _extract_pushdown_filters(self, where_clause: str) -> List[Tuple]:
        """Extract filters that can be pushed down to the storage layer.
        
        Args:
            where_clause: WHERE clause
            
        Returns:
            List of (column, op, value) tuples for pushdown
        """
        if not where_clause:
            return []
            
        # Extract basic filters that can be pushed down
        pushdown_filters = []
        where_clause = re.sub(r'^\s*WHERE\s+', '', where_clause, flags=re.IGNORECASE)
        
        # Check for AND conditions (we can push these down)
        if ' AND ' in where_clause.upper():
            conditions = where_clause.split(' AND ')
            for condition in conditions:
                try:
                    column, op, value = self._parse_condition(condition)
                    pushdown_filters.append((column, op, value))
                except ValueError:
                    # Complex condition we can't push down
                    continue
        # For OR conditions, we typically can't push down the entire filter
        elif ' OR ' not in where_clause.upper():
            # Single condition
            try:
                column, op, value = self._parse_condition(where_clause)
                pushdown_filters.append((column, op, value))
            except ValueError:
                pass
        
        return pushdown_filters
    
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
    
    def _execute_distributed_query(self, table_info: Dict, columns: List[str], 
                               filters: List[Tuple], parsed_query: Dict) -> pd.DataFrame:
        """Execute query in parallel across multiple files/partitions.
        
        Args:
            table_info: Table metadata
            columns: Columns to read
            filters: Filters to push down
            parsed_query: Parsed query information
            
        Returns:
            Combined query results
        """
        file_paths = [meta['path'] for meta in table_info['files']]
        
        # Execute in parallel using thread pool
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_file = {
                executor.submit(
                    self._process_file_partition, 
                    file_path, 
                    columns, 
                    filters, 
                    parsed_query
                ): file_path 
                for file_path in file_paths
            }
            
            # Collect results
            results = []
            for future in concurrent.futures.as_completed(future_to_file):
                try:
                    result = future.result()
                    if result is not None and len(result) > 0:
                        results.append(result)
                except Exception as e:
                    # Log error but continue with other partitions
                    print(f"Error processing partition: {e}")
        
        # Combine results from all partitions
        if not results:
            # Create empty DataFrame with correct columns
            return pd.DataFrame(columns=columns if columns != ['*'] else table_info['schema'].names)
        
        combined_df = pd.concat(results, ignore_index=True)
        
        # Apply LIMIT (after combining to ensure correct results)
        if parsed_query["limit"] is not None:
            combined_df = combined_df.head(parsed_query["limit"])
            
        return combined_df
    
    def _process_file_partition(self, file_path: str, columns: List[str], 
                           filters: List[Tuple], parsed_query: Dict) -> pd.DataFrame:
        """Process a single file partition with pushdown optimizations.
        
        Args:
            file_path: Path to parquet file
            columns: Columns to read
            filters: Filters to push down
            parsed_query: Parsed query
            
        Returns:
            DataFrame with results from this partition
        """
        # Convert our filters to PyArrow filter format
        pyarrow_filters = self._convert_to_pyarrow_filters(filters)
        
        # Use PyArrow for columnar processing with pushdown
        try:
            # Read only needed columns (projection pushdown)
            table = pq.read_table(
                file_path,
                columns=columns if columns != ['*'] else None, 
                filters=pyarrow_filters
            )
            
            # Apply any remaining filters that couldn't be pushed down
            if parsed_query["where"] and not pyarrow_filters:
                # Convert to pandas to apply remaining filters
                df = table.to_pandas()
                df = self._apply_where_conditions(df, parsed_query["where"])
            else:
                df = table.to_pandas()
                
            # Select columns if needed
            if parsed_query["columns"] != ['*']:
                df = self._select_columns(df, parsed_query["columns"])
                
            return df
            
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
            return None
    
    def _convert_to_pyarrow_filters(self, filters: List[Tuple]) -> List:
        """Convert our filter format to PyArrow's filter format.
        
        Args:
            filters: List of (column, op, value) tuples
            
        Returns:
            PyArrow compatible filter list
        """
        if not filters:
            return None
            
        pyarrow_filters = []
        
        # Convert each filter to PyArrow format
        for column, op, value in filters:
            if op == '=':
                pyarrow_filters.append((column, '==', value))
            elif op in ['>', '<', '>=', '<=', '!=']:
                pyarrow_filters.append((column, op, value))
        
        return pyarrow_filters
    
    # ---------- Legacy methods from original QueryEngine ----------
    
    def _parse_query(self, query: str) -> Dict[str, Any]:
        """Parse SQL query into components.
        
        Args:
            query: SQL query string
            
        Returns:
            Dictionary with query components
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
        
        # Extract components
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
        """Remove comments and normalize whitespace."""
        query = re.sub(r'--.*?(\n|$)', ' ', query)
        query = re.sub(r'/\*.*?\*/', ' ', query, flags=re.DOTALL)
        return ' '.join(query.split())
    
    def _extract_columns(self, select_stmt: str) -> List[str]:
        """Extract column names from SELECT statement."""
        if '*' in select_stmt:
            return ['*']
        
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
        """Extract table name from FROM statement."""
        match = re.search(r'FROM\s+([^\s;]+)', from_stmt, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        raise ValueError(f"Could not extract table name from: {from_stmt}")
    
    def _extract_limit(self, query: str) -> Optional[int]:
        """Extract LIMIT value from query."""
        match = re.search(r'LIMIT\s+(\d+)', query, re.IGNORECASE)
        if match:
            return int(match.group(1))
        return None
    
    def _select_columns(self, df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """Select columns from DataFrame, handling case sensitivity."""
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
    
    def _apply_where_conditions(self, df: pd.DataFrame, where_clause: str) -> pd.DataFrame:
        """Apply WHERE conditions to the DataFrame."""
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
    
    def _apply_condition(self, df: pd.DataFrame, column: str, op: str, value: Any) -> pd.DataFrame:
        """Apply a WHERE condition to the DataFrame."""
        mask = self._create_condition_mask(df, column, op, value)
        return df[mask]
    
    def _create_condition_mask(self, df: pd.DataFrame, column: str, op: str, value: Any) -> pd.Series:
        """Create a boolean mask for a condition."""
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
    
    def clear_cache(self):
        """Clear the table cache."""
        self.table_cache = {}


# Example usage
if __name__ == "__main__":
    # Create Dremel-inspired engine instance
    engine = DremelQueryEngine("data")
    
    # Example queries
    queries = [
        "SELECT name, age FROM customers WHERE age > 30",
        "SELECT * FROM orders WHERE subtotal > 100 AND status = 'completed'",
        "SELECT product_id, name, price FROM products WHERE price > 50 LIMIT 5"
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