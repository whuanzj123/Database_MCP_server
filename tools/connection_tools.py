"""
Connection management tools for multi-database MCP server
"""

import logging
from typing import Optional
from ..utils.formatters import ResponseFormatter
from ..utils.validators import CredentialValidator, ConnectionValidator

logger = logging.getLogger(__name__)


def register_tools(mcp, db_manager):
    """Register connection management tools with the MCP server"""
    
    @mcp.tool()
    def connect_database(db_type: str, host: str = "localhost", port: int = None, 
                        username: str = "", password: str = "", database: str = "",
                        database_path: str = "") -> str:
        """
        Connect to a database with provided credentials.
        
        Args:
            db_type: Database type (postgresql, mysql, sqlite, mongodb)
            host: Database host address (not needed for SQLite)
            port: Database port (optional, uses default if not specified)
            username: Database username
            password: Database password
            database: Database name
            database_path: Database file path (SQLite only)
            
        Returns:
            str: JSON response with connection details
        """
        try:
            logger.info(f"Attempting to connect to {db_type} database at {host}")
            
            # Prepare credentials
            credentials = {
                'username': username,
                'password': password,
                'database': database,
                'database_path': database_path
            }
            
            # Remove empty credentials
            credentials = {k: v for k, v in credentials.items() if v}
            
            # Connect to database
            connection_id = db_manager.connect_database(db_type, host, port, credentials)
            
            logger.info(f"Successfully connected to {db_type} database: {connection_id}")
            
            return ResponseFormatter.connection_response(
                connection_id=connection_id,
                db_type=db_type,
                host=host,
                database=database,
                port=port
            )
            
        except Exception as e:
            error_msg = f"Failed to connect to {db_type} database: {str(e)}"
            logger.error(error_msg)
            return ResponseFormatter.error_response(
                error=error_msg,
                error_code="CONNECTION_FAILED"
            )
    
    @mcp.tool()
    def disconnect_database(connection_id: str) -> str:
        """
        Disconnect from a database.
        
        Args:
            connection_id: Database connection ID to disconnect
            
        Returns:
            str: JSON response confirming disconnection
        """
        try:
            logger.info(f"Attempting to disconnect from database: {connection_id}")
            
            success = db_manager.disconnect(connection_id)
            
            if success:
                logger.info(f"Successfully disconnected from database: {connection_id}")
                return ResponseFormatter.success_response(
                    data={"connection_id": connection_id},
                    message="Successfully disconnected from database"
                )
            else:
                return ResponseFormatter.error_response(
                    error="Failed to disconnect from database",
                    error_code="DISCONNECT_FAILED",
                    details={"connection_id": connection_id}
                )
                
        except Exception as e:
            error_msg = f"Error disconnecting from database: {str(e)}"
            logger.error(error_msg)
            return ResponseFormatter.error_response(
                error=error_msg,
                error_code="DISCONNECT_ERROR"
            )
    
    @mcp.tool()
    def list_connections() -> str:
        """
        List all active database connections.
        
        Returns:
            str: JSON response with list of active connections
        """
        try:
            logger.info("Retrieving list of active connections")
            
            connections = db_manager.list_connections()
            
            return ResponseFormatter.list_response(
                items=connections,
                item_type="connections",
                total_count=len(connections)
            )
            
        except Exception as e:
            error_msg = f"Error listing connections: {str(e)}"
            logger.error(error_msg)
            return ResponseFormatter.error_response(
                error=error_msg,
                error_code="LIST_CONNECTIONS_ERROR"
            )
    
    @mcp.tool()
    def test_connection(connection_id: str) -> str:
        """
        Test a database connection to verify it's still active.
        
        Args:
            connection_id: Database connection ID to test
            
        Returns:
            str: JSON response with connection test results
        """
        try:
            logger.info(f"Testing database connection: {connection_id}")
            
            # Get connection info
            conn_info = db_manager.get_connection_info(connection_id)
            if not conn_info:
                return ResponseFormatter.error_response(
                    error="Invalid connection ID",
                    error_code="INVALID_CONNECTION_ID"
                )
            
            db_type = conn_info['db_type']
            
            # Choose appropriate test query
            test_queries = {
                'postgresql': "SELECT version()",
                'mysql': "SELECT VERSION()",
                'sqlite': "SELECT sqlite_version()",
                'mongodb': "SHOW COLLECTIONS"
            }
            
            test_query = test_queries.get(db_type, "SELECT 1")
            
            # Execute test query
            result = db_manager.execute_query(connection_id, test_query)
            
            if result.get('success'):
                test_data = {
                    "connection_id": connection_id,
                    "status": "active",
                    "db_type": db_type,
                    "host": conn_info.get('host'),
                    "database": conn_info.get('database'),
                    "last_used": conn_info.get('last_used'),
                    "query_count": conn_info.get('query_count', 0)
                }
                
                return ResponseFormatter.success_response(
                    data=test_data,
                    message=f"Connection to {db_type} database is active"
                )
            else:
                return ResponseFormatter.error_response(
                    error=f"Connection test failed: {result.get('error', 'Unknown error')}",
                    error_code="CONNECTION_TEST_FAILED"
                )
                
        except Exception as e:
            error_msg = f"Error testing connection: {str(e)}"
            logger.error(error_msg)
            return ResponseFormatter.error_response(
                error=error_msg,
                error_code="CONNECTION_TEST_ERROR"
            )
    
    @mcp.tool()
    def get_connection_info(connection_id: str) -> str:
        """
        Get detailed information about a specific connection.
        
        Args:
            connection_id: Database connection ID
            
        Returns:
            str: JSON response with connection information
        """
        try:
            logger.info(f"Retrieving connection info: {connection_id}")
            
            conn_info = db_manager.get_connection_info(connection_id)
            
            if conn_info:
                return ResponseFormatter.success_response(
                    data=conn_info,
                    message="Connection information retrieved"
                )
            else:
                return ResponseFormatter.error_response(
                    error="Connection not found",
                    error_code="CONNECTION_NOT_FOUND"
                )
                
        except Exception as e:
            error_msg = f"Error retrieving connection info: {str(e)}"
            logger.error(error_msg)
            return ResponseFormatter.error_response(
                error=error_msg,
                error_code="CONNECTION_INFO_ERROR"
            )
    
    @mcp.tool()
    def cleanup_stale_connections(max_idle_hours: int = 1) -> str:
        """
        Clean up connections that have been idle for too long.
        
        Args:
            max_idle_hours: Maximum idle time in hours before cleanup (default: 1)
            
        Returns:
            str: JSON response with cleanup results
        """
        try:
            logger.info(f"Cleaning up connections idle for more than {max_idle_hours} hours")
            
            max_idle_seconds = max_idle_hours * 3600
            cleaned_count = db_manager.cleanup_stale_connections(max_idle_seconds)
            
            cleanup_data = {
                "cleaned_connections": cleaned_count,
                "max_idle_hours": max_idle_hours,
                "remaining_connections": len(db_manager.connections)
            }
            
            return ResponseFormatter.success_response(
                data=cleanup_data,
                message=f"Cleaned up {cleaned_count} stale connections"
            )
            
        except Exception as e:
            error_msg = f"Error cleaning up connections: {str(e)}"
            logger.error(error_msg)
            return ResponseFormatter.error_response(
                error=error_msg,
                error_code="CLEANUP_ERROR"
            )
    
    @mcp.tool()
    def validate_connection_params(db_type: str, host: str = "localhost", 
                                 port: int = None, username: str = "", 
                                 password: str = "", database: str = "",
                                 database_path: str = "") -> str:
        """
        Validate connection parameters without actually connecting.
        
        Args:
            db_type: Database type (postgresql, mysql, sqlite, mongodb)
            host: Database host address
            port: Database port
            username: Database username
            password: Database password
            database: Database name
            database_path: Database file path (SQLite only)
            
        Returns:
            str: JSON response with validation results
        """
        try:
            logger.info(f"Validating connection parameters for {db_type}")
            
            # Prepare credentials
            credentials = {
                'username': username,
                'password': password,
                'database': database,
                'database_path': database_path
            }
            
            # Remove empty credentials
            credentials = {k: v for k, v in credentials.items() if v}
            
            # Validate using existing validators
            credential_validator = CredentialValidator()
            connection_validator = ConnectionValidator(db_manager.max_connections)
            
            # Validate credentials
            is_valid_creds = credential_validator.validate_credentials(db_type, host, credentials)
            
            # Validate port if provided
            port_valid = True
            port_message = "No port specified"
            if port is not None:
                port_valid, port_message = connection_validator.validate_port(port, db_type)
            
            # Validate connection limits
            limits_valid, limits_message = connection_validator.validate_connection_limits(
                len(db_manager.connections)
            )
            
            validation_results = {
                "db_type": db_type,
                "host": host,
                "port": port,
                "validation": {
                    "credentials_valid": is_valid_creds,
                    "port_valid": port_valid,
                    "port_message": port_message,
                    "limits_valid": limits_valid,
                    "limits_message": limits_message
                },
                "overall_valid": is_valid_creds and port_valid and limits_valid
            }
            
            message = "Connection parameters are valid" if validation_results["overall_valid"] else "Connection parameters have issues"
            
            return ResponseFormatter.success_response(
                data=validation_results,
                message=message
            )
            
        except Exception as e:
            error_msg = f"Error validating connection parameters: {str(e)}"
            logger.error(error_msg)
            return ResponseFormatter.error_response(
                error=error_msg,
                error_code="VALIDATION_ERROR"
            )
    
    logger.info("Connection management tools registered")