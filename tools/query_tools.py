"""
Query execution tools for multi-database MCP server
"""

import logging
import time
from typing import Optional
from ..utils.formatters import ResponseFormatter
from ..utils.validators import QueryValidator
from ..utils.security import SecurityValidator

logger = logging.getLogger(__name__)


def register_tools(mcp, db_manager):
    """Register query execution tools with the MCP server"""
    
    @mcp.tool()
    def execute_query(connection_id: str, query: str, schema: str = None, 
                     limit: int = 100) -> str:
        """
        Execute a SELECT query on a connected database.
        
        Args:
            connection_id: Database connection ID from connect_database
            query: SQL SELECT query to execute
            schema: Schema name to use (optional)
            limit: Maximum number of rows to return (default: 100, max: 1000)
            
        Returns:
            str: JSON response with query results
        """
        try:
            start_time = time.time()
            logger.info(f"Executing query on connection {connection_id}")
            
            # Validate query parameters
            query_validator = QueryValidator()
            is_valid, message = query_validator.validate_query_params(query, limit, schema)
            
            if not is_valid:
                return ResponseFormatter.error_response(
                    error=f"Invalid query parameters: {message}",
                    error_code="INVALID_QUERY_PARAMS"
                )
            
            # Apply limit if not already in query
            query_lower = query.lower().strip()
            if 'limit' not in query_lower and limit:
                query += f" LIMIT {min(limit, 1000)}"
            
            # Get connection info for logging
            conn_info = db_manager.get_connection_info(connection_id)
            if not conn_info:
                return ResponseFormatter.error_response(
                    error="Invalid connection ID",
                    error_code="INVALID_CONNECTION_ID"
                )
            
            # Execute query
            result = db_manager.execute_query(connection_id, query, schema)
            execution_time = time.time() - start_time
            
            if result.get('success'):
                logger.info(f"Query executed successfully in {execution_time:.3f}s")
                
                return ResponseFormatter.query_response(
                    query=query,
                    columns=result.get('columns', []),
                    rows=result.get('rows', []),
                    execution_time=execution_time,
                    connection_info={
                        'db_type': conn_info.get('db_type'),
                        'host': conn_info.get('host'),
                        'database': conn_info.get('database')
                    }
                )
            else:
                error_msg = result.get('error', 'Unknown error')
                logger.error(f"Query execution failed: {error_msg}")
                return ResponseFormatter.error_response(
                    error=f"Query execution failed: {error_msg}",
                    error_code="QUERY_EXECUTION_FAILED"
                )
                
        except Exception as e:
            error_msg = f"Error executing query: {str(e)}"
            logger.error(error_msg)
            return ResponseFormatter.error_response(
                error=error_msg,
                error_code="QUERY_ERROR"
            )
    
    @mcp.tool()
    def validate_query(query: str) -> str:
        """
        Validate if a query would be allowed by the security filter.
        
        Args:
            query: SQL query to validate
            
        Returns:
            str: JSON response with validation results
        """
        try:
            logger.info("Validating query security")
            
            security_validator = SecurityValidator()
            
            # Get detailed security report
            security_report = security_validator.get_security_report(query)
            
            return ResponseFormatter.validation_response(
                query=query,
                is_valid=security_report['is_safe'],
                violations=security_report.get('violations', []),
                security_report=security_report
            )
            
        except Exception as e:
            error_msg = f"Error validating query: {str(e)}"
            logger.error(error_msg)
            return ResponseFormatter.error_response(
                error=error_msg,
                error_code="VALIDATION_ERROR"
            )
    
    @mcp.tool()
    def explain_query(connection_id: str, query: str, schema: str = None) -> str:
        """
        Get the execution plan for a query without executing it.
        
        Args:
            connection_id: Database connection ID
            query: SQL query to explain
            schema: Schema name to use (optional)
            
        Returns:
            str: JSON response with query execution plan
        """
        try:
            logger.info(f"Getting execution plan for query on connection {connection_id}")
            
            # Get connection info
            conn_info = db_manager.get_connection_info(connection_id)
            if not conn_info:
                return ResponseFormatter.error_response(
                    error="Invalid connection ID",
                    error_code="INVALID_CONNECTION_ID"
                )
            
            db_type = conn_info.get('db_type')
            
            # Construct EXPLAIN query based on database type
            if db_type == 'postgresql':
                explain_query = f"EXPLAIN (FORMAT JSON) {query}"
            elif db_type == 'mysql':
                explain_query = f"EXPLAIN FORMAT=JSON {query}"
            elif db_type == 'sqlite':
                explain_query = f"EXPLAIN QUERY PLAN {query}"
            else:
                return ResponseFormatter.error_response(
                    error=f"EXPLAIN not supported for {db_type}",
                    error_code="EXPLAIN_NOT_SUPPORTED"
                )
            
            # Execute explain query
            result = db_manager.execute_query(connection_id, explain_query, schema)
            
            if result.get('success'):
                return ResponseFormatter.success_response(
                    data={
                        "original_query": query,
                        "explain_query": explain_query,
                        "execution_plan": result.get('rows', []),
                        "db_type": db_type
                    },
                    message="Query execution plan retrieved"
                )
            else:
                error_msg = result.get('error', 'Unknown error')
                return ResponseFormatter.error_response(
                    error=f"Failed to get execution plan: {error_msg}",
                    error_code="EXPLAIN_FAILED"
                )
                
        except Exception as e:
            error_msg = f"Error explaining query: {str(e)}"
            logger.error(error_msg)
            return ResponseFormatter.error_response(
                error=error_msg,
                error_code="EXPLAIN_ERROR"
            )
    
    @mcp.tool()
    def execute_batch_queries(connection_id: str, queries: list, schema: str = None,
                             stop_on_error: bool = True) -> str:
        """
        Execute multiple queries in sequence.
        
        Args:
            connection_id: Database connection ID
            queries: List of SQL queries to execute
            schema: Schema name to use (optional)
            stop_on_error: Whether to stop execution on first error (default: True)
            
        Returns:
            str: JSON response with batch execution results
        """
        try:
            logger.info(f"Executing batch of {len(queries)} queries on connection {connection_id}")
            
            # Validate batch size
            if len(queries) > 10:  # Configurable limit
                return ResponseFormatter.error_response(
                    error="Batch size too large (max 10 queries)",
                    error_code="BATCH_TOO_LARGE"
                )
            
            if not queries:
                return ResponseFormatter.error_response(
                    error="No queries provided",
                    error_code="EMPTY_BATCH"
                )
            
            # Get connection info
            conn_info = db_manager.get_connection_info(connection_id)
            if not conn_info:
                return ResponseFormatter.error_response(
                    error="Invalid connection ID",
                    error_code="INVALID_CONNECTION_ID"
                )
            
            batch_results = []
            total_start_time = time.time()
            
            for i, query in enumerate(queries):
                try:
                    start_time = time.time()
                    result = db_manager.execute_query(connection_id, query, schema)
                    execution_time = time.time() - start_time
                    
                    query_result = {
                        "query_index": i,
                        "query": query[:100] + "..." if len(query) > 100 else query,
                        "success": result.get('success', False),
                        "execution_time": execution_time
                    }
                    
                    if result.get('success'):
                        query_result.update({
                            "row_count": result.get('row_count', 0),
                            "columns": result.get('columns', [])
                        })
                        # Only include actual data for small result sets
                        if result.get('row_count', 0) <= 10:
                            query_result["rows"] = result.get('rows', [])
                    else:
                        query_result["error"] = result.get('error', 'Unknown error')
                        if stop_on_error:
                            batch_results.append(query_result)
                            break
                    
                    batch_results.append(query_result)
                    
                except Exception as e:
                    error_result = {
                        "query_index": i,
                        "query": query[:100] + "..." if len(query) > 100 else query,
                        "success": False,
                        "error": str(e)
                    }
                    batch_results.append(error_result)
                    
                    if stop_on_error:
                        break
            
            total_execution_time = time.time() - total_start_time
            successful_queries = sum(1 for result in batch_results if result.get('success'))
            
            batch_summary = {
                "total_queries": len(queries),
                "executed_queries": len(batch_results),
                "successful_queries": successful_queries,
                "failed_queries": len(batch_results) - successful_queries,
                "total_execution_time": total_execution_time,
                "results": batch_results
            }
            
            return ResponseFormatter.success_response(
                data=batch_summary,
                message=f"Batch execution completed: {successful_queries}/{len(queries)} queries successful"
            )
            
        except Exception as e:
            error_msg = f"Error executing batch queries: {str(e)}"
            logger.error(error_msg)
            return ResponseFormatter.error_response(
                error=error_msg,
                error_code="BATCH_EXECUTION_ERROR"
            )
    
    @mcp.tool()
    def get_query_history(connection_id: str, limit: int = 10) -> str:
        """
        Get query execution history for a connection.
        
        Args:
            connection_id: Database connection ID
            limit: Maximum number of history entries to return
            
        Returns:
            str: JSON response with query history
        """
        try:
            logger.info(f"Retrieving query history for connection {connection_id}")
            
            # Get connection info
            conn_info = db_manager.get_connection_info(connection_id)
            if not conn_info:
                return ResponseFormatter.error_response(
                    error="Invalid connection ID",
                    error_code="INVALID_CONNECTION_ID"
                )
            
            # For now, return basic connection statistics
            # In a full implementation, you would maintain query history
            history_data = {
                "connection_id": connection_id,
                "db_type": conn_info.get('db_type'),
                "total_queries": conn_info.get('query_count', 0),
                "last_used": conn_info.get('last_used'),
                "connection_created": conn_info.get('created_at'),
                "note": "Full query history tracking would require additional implementation"
            }
            
            return ResponseFormatter.success_response(
                data=history_data,
                message="Query history information retrieved"
            )
            
        except Exception as e:
            error_msg = f"Error retrieving query history: {str(e)}"
            logger.error(error_msg)
            return ResponseFormatter.error_response(
                error=error_msg,
                error_code="HISTORY_ERROR"
            )
    
    @mcp.tool()
    def analyze_query_performance(query: str) -> str:
        """
        Analyze a query for potential performance issues.
        
        Args:
            query: SQL query to analyze
            
        Returns:
            str: JSON response with performance analysis
        """
        try:
            logger.info("Analyzing query performance")
            
            analysis = {
                "query": query[:200] + "..." if len(query) > 200 else query,
                "length": len(query),
                "potential_issues": [],
                "recommendations": [],
                "complexity_score": 0
            }
            
            query_upper = query.upper()
            
            # Check for potential performance issues
            if "SELECT *" in query_upper:
                analysis["potential_issues"].append("Using SELECT * - consider specifying columns")
                analysis["recommendations"].append("Specify only needed columns instead of SELECT *")
                analysis["complexity_score"] += 1
            
            if query_upper.count("JOIN") > 3:
                analysis["potential_issues"].append("Multiple JOINs detected - may impact performance")
                analysis["recommendations"].append("Review JOIN operations and consider optimization")
                analysis["complexity_score"] += 2
            
            if "ORDER BY" in query_upper and "LIMIT" not in query_upper:
                analysis["potential_issues"].append("ORDER BY without LIMIT - may sort large result set")
                analysis["recommendations"].append("Consider adding LIMIT clause with ORDER BY")
                analysis["complexity_score"] += 1
            
            if query_upper.count("(") > 5:
                analysis["potential_issues"].append("Complex nested query detected")
                analysis["recommendations"].append("Consider breaking down complex subqueries")
                analysis["complexity_score"] += 2
            
            if "WHERE" not in query_upper and "FROM" in query_upper:
                analysis["potential_issues"].append("No WHERE clause - may return large result set")
                analysis["recommendations"].append("Consider adding WHERE clause to filter results")
                analysis["complexity_score"] += 1
            
            # Determine complexity level
            if analysis["complexity_score"] == 0:
                analysis["complexity_level"] = "Low"
            elif analysis["complexity_score"] <= 3:
                analysis["complexity_level"] = "Medium"
            else:
                analysis["complexity_level"] = "High"
            
            if not analysis["potential_issues"]:
                analysis["recommendations"].append("Query appears well-structured")
            
            return ResponseFormatter.success_response(
                data=analysis,
                message="Query performance analysis completed"
            )
            
        except Exception as e:
            error_msg = f"Error analyzing query performance: {str(e)}"
            logger.error(error_msg)
            return ResponseFormatter.error_response(
                error=error_msg,
                error_code="ANALYSIS_ERROR"
            )
    
    logger.info("Query execution tools registered")