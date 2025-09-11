"""
Response formatting utilities for multi-database MCP server
"""

import json
import time
from typing import Any, Dict, List, Optional, Union
from datetime import datetime


class ResponseFormatter:
    """Formats responses consistently across all tools"""
    
    @staticmethod
    def success_response(data: Any, message: str = "Operation successful", 
                        metadata: Optional[Dict[str, Any]] = None) -> str:
        """
        Format a successful response.
        
        Args:
            data: Response data
            message: Success message
            metadata: Additional metadata
            
        Returns:
            str: JSON formatted response
        """
        response = {
            "success": True,
            "message": message,
            "timestamp": datetime.utcnow().isoformat(),
            "data": data
        }
        
        if metadata:
            response["metadata"] = metadata
        
        return json.dumps(response, indent=2, default=ResponseFormatter._json_serializer)
    
    @staticmethod
    def error_response(error: str, error_code: Optional[str] = None, 
                      details: Optional[Dict[str, Any]] = None) -> str:
        """
        Format an error response.
        
        Args:
            error: Error message
            error_code: Optional error code
            details: Additional error details
            
        Returns:
            str: JSON formatted error response
        """
        response = {
            "success": False,
            "error": error,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        if error_code:
            response["error_code"] = error_code
        
        if details:
            response["details"] = details
        
        return json.dumps(response, indent=2, default=ResponseFormatter._json_serializer)
    
    @staticmethod
    def query_response(query: str, columns: List[str], rows: List[Dict], 
                      execution_time: Optional[float] = None,
                      connection_info: Optional[Dict[str, Any]] = None) -> str:
        """
        Format a query execution response.
        
        Args:
            query: Executed query
            columns: Column names
            rows: Result rows
            execution_time: Query execution time
            connection_info: Connection information
            
        Returns:
            str: JSON formatted query response
        """
        response_data = {
            "query": query,
            "columns": columns,
            "rows": rows,
            "row_count": len(rows)
        }
        
        metadata = {}
        
        if execution_time is not None:
            metadata["execution_time"] = f"{execution_time:.3f}s"
        
        if connection_info:
            metadata["connection"] = connection_info
        
        return ResponseFormatter.success_response(
            data=response_data,
            message=f"Query executed successfully. {len(rows)} rows returned.",
            metadata=metadata if metadata else None
        )
    
    @staticmethod
    def connection_response(connection_id: str, db_type: str, host: str, 
                          database: str, port: Optional[int] = None) -> str:
        """
        Format a connection response.
        
        Args:
            connection_id: Generated connection ID
            db_type: Database type
            host: Database host
            database: Database name
            port: Database port
            
        Returns:
            str: JSON formatted connection response
        """
        connection_data = {
            "connection_id": connection_id,
            "db_type": db_type,
            "host": host,
            "database": database
        }
        
        if port:
            connection_data["port"] = port
        
        return ResponseFormatter.success_response(
            data=connection_data,
            message=f"Successfully connected to {db_type} database"
        )
    
    @staticmethod
    def schema_response(schema_info: Dict[str, Any], schema_name: Optional[str] = None) -> str:
        """
        Format a schema information response.
        
        Args:
            schema_info: Schema information
            schema_name: Schema name
            
        Returns:
            str: JSON formatted schema response
        """
        message = f"Schema information retrieved"
        if schema_name:
            message += f" for schema '{schema_name}'"
        
        return ResponseFormatter.success_response(
            data=schema_info,
            message=message
        )
    
    @staticmethod
    def table_response(table_info: Dict[str, Any], table_name: str) -> str:
        """
        Format a table information response.
        
        Args:
            table_info: Table information
            table_name: Table name
            
        Returns:
            str: JSON formatted table response
        """
        return ResponseFormatter.success_response(
            data=table_info,
            message=f"Table information retrieved for '{table_name}'"
        )
    
    @staticmethod
    def status_response(status_info: Dict[str, Any]) -> str:
        """
        Format a status response.
        
        Args:
            status_info: Status information
            
        Returns:
            str: JSON formatted status response
        """
        return ResponseFormatter.success_response(
            data=status_info,
            message="System status retrieved"
        )
    
    @staticmethod
    def validation_response(query: str, is_valid: bool, violations: List[str] = None,
                          security_report: Optional[Dict[str, Any]] = None) -> str:
        """
        Format a query validation response.
        
        Args:
            query: Validated query
            is_valid: Whether query is valid
            violations: List of security violations
            security_report: Detailed security report
            
        Returns:
            str: JSON formatted validation response
        """
        validation_data = {
            "query": query[:100] + "..." if len(query) > 100 else query,
            "is_valid": is_valid,
            "status": "safe" if is_valid else "blocked"
        }
        
        if violations:
            validation_data["violations"] = violations
        
        if security_report:
            validation_data["security_report"] = security_report
        
        message = "Query is safe for execution" if is_valid else "Query blocked by security filter"
        
        return ResponseFormatter.success_response(
            data=validation_data,
            message=message
        )
    
    @staticmethod
    def list_response(items: List[Dict[str, Any]], item_type: str, 
                     total_count: Optional[int] = None) -> str:
        """
        Format a list response.
        
        Args:
            items: List of items
            item_type: Type of items (e.g., "connections", "tables")
            total_count: Total count if different from items length
            
        Returns:
            str: JSON formatted list response
        """
        count = total_count if total_count is not None else len(items)
        
        list_data = {
            item_type: items,
            "count": count
        }
        
        return ResponseFormatter.success_response(
            data=list_data,
            message=f"Retrieved {count} {item_type}"
        )
    
    @staticmethod
    def _json_serializer(obj):
        """JSON serializer for special types"""
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif hasattr(obj, 'isoformat'):
            return obj.isoformat()
        elif hasattr(obj, '__dict__'):
            return obj.__dict__
        else:
            return str(obj)


class TableFormatter:
    """Formats data in table format for better readability"""
    
    @staticmethod
    def format_table(columns: List[str], rows: List[Dict], max_width: int = 100) -> str:
        """
        Format query results as an ASCII table.
        
        Args:
            columns: Column headers
            rows: Data rows
            max_width: Maximum column width
            
        Returns:
            str: ASCII table representation
        """
        if not rows:
            return "No data to display"
        
        # Calculate column widths
        col_widths = {}
        for col in columns:
            col_widths[col] = len(col)
        
        for row in rows:
            for col in columns:
                value = str(row.get(col, ''))
                col_widths[col] = max(col_widths[col], len(value))
        
        # Limit column width
        for col in col_widths:
            col_widths[col] = min(col_widths[col], max_width)
        
        # Build table
        lines = []
        
        # Header
        header = " | ".join(col.ljust(col_widths[col]) for col in columns)
        lines.append(header)
        lines.append("-" * len(header))
        
        # Rows
        for row in rows:
            row_line = " | ".join(
                str(row.get(col, '')).ljust(col_widths[col])[:col_widths[col]] 
                for col in columns
            )
            lines.append(row_line)
        
        return "\n".join(lines)
    
    @staticmethod
    def format_summary(total_rows: int, execution_time: Optional[float] = None,
                      connection_info: Optional[Dict[str, Any]] = None) -> str:
        """
        Format a query summary.
        
        Args:
            total_rows: Number of rows returned
            execution_time: Query execution time
            connection_info: Connection information
            
        Returns:
            str: Formatted summary
        """
        summary_parts = [f"Rows: {total_rows}"]
        
        if execution_time:
            summary_parts.append(f"Time: {execution_time:.3f}s")
        
        if connection_info:
            db_type = connection_info.get('db_type', 'unknown')
            host = connection_info.get('host', 'unknown')
            summary_parts.append(f"DB: {db_type}@{host}")
        
        return " | ".join(summary_parts)


class LogFormatter:
    """Formats log messages consistently"""
    
    @staticmethod
    def format_connection_log(action: str, db_type: str, host: str, 
                            success: bool, details: str = "") -> str:
        """
        Format connection log message.
        
        Args:
            action: Action performed (connect, disconnect, etc.)
            db_type: Database type
            host: Database host
            success: Whether action was successful
            details: Additional details
            
        Returns:
            str: Formatted log message
        """
        status = "SUCCESS" if success else "FAILED"
        message = f"[{action.upper()}] {db_type}@{host} - {status}"
        
        if details:
            message += f" - {details}"
        
        return message
    
    @staticmethod
    def format_query_log(query: str, db_type: str, execution_time: Optional[float] = None,
                        row_count: Optional[int] = None, error: Optional[str] = None) -> str:
        """
        Format query execution log message.
        
        Args:
            query: Executed query
            db_type: Database type
            execution_time: Query execution time
            row_count: Number of rows returned
            error: Error message if query failed
            
        Returns:
            str: Formatted log message
        """
        query_preview = query[:50] + "..." if len(query) > 50 else query
        
        if error:
            return f"[QUERY_ERROR] {db_type} - {query_preview} - ERROR: {error}"
        
        message = f"[QUERY_SUCCESS] {db_type} - {query_preview}"
        
        if row_count is not None:
            message += f" - Rows: {row_count}"
        
        if execution_time is not None:
            message += f" - Time: {execution_time:.3f}s"
        
        return message
    
    @staticmethod
    def format_security_log(query: str, violation: str, action: str = "BLOCKED") -> str:
        """
        Format security violation log message.
        
        Args:
            query: Query that triggered violation
            violation: Security violation description
            action: Action taken (BLOCKED, ALLOWED, etc.)
            
        Returns:
            str: Formatted security log message
        """
        query_preview = query[:30] + "..." if len(query) > 30 else query
        return f"[SECURITY_{action}] {query_preview} - {violation}"


class MetricsFormatter:
    """Formats performance and usage metrics"""
    
    @staticmethod
    def format_performance_metrics(metrics: Dict[str, Any]) -> str:
        """
        Format performance metrics.
        
        Args:
            metrics: Performance metrics
            
        Returns:
            str: JSON formatted metrics
        """
        return ResponseFormatter.success_response(
            data=metrics,
            message="Performance metrics retrieved"
        )
    
    @staticmethod
    def calculate_query_stats(execution_times: List[float]) -> Dict[str, float]:
        """
        Calculate query performance statistics.
        
        Args:
            execution_times: List of query execution times
            
        Returns:
            Dict[str, float]: Performance statistics
        """
        if not execution_times:
            return {
                "count": 0,
                "avg_time": 0.0,
                "min_time": 0.0,
                "max_time": 0.0
            }
        
        return {
            "count": len(execution_times),
            "avg_time": sum(execution_times) / len(execution_times),
            "min_time": min(execution_times),
            "max_time": max(execution_times),
            "total_time": sum(execution_times)
        }