#!/usr/bin/env python3
"""
Secure Multi-Database MCP Server using FastMCP
A Model Context Protocol server that provides secure database access to LLMs.
Supports multiple database engines with credential management and schema introspection.
"""

import asyncio
import json
import logging
import hashlib
import time
import re
import os
import sys
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlparse

# FastMCP and web dependencies
from mcp.server.fastmcp import FastMCP
from starlette.applications import Starlette
from starlette.middleware.cors import CORSMiddleware
from starlette.routing import Mount

# Database drivers
try:
    import psycopg2
    import psycopg2.extras
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False

try:
    import mysql.connector
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False

try:
    import sqlite3
    SQLITE_AVAILABLE = True
except ImportError:
    SQLITE_AVAILABLE = False

try:
    import pymongo
    MONGODB_AVAILABLE = True
except ImportError:
    MONGODB_AVAILABLE = False

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("secure-multi-db-mcp")

# Create FastMCP server
mcp = FastMCP("Secure Multi-Database MCP")

class DatabaseConnectionManager:
    """Manages database connections with security and credential validation."""
    
    def __init__(self):
        self.connections = {}
        self.connection_timeout = 30
        self.max_connections = 10
        self.allowed_operations = {
            'SELECT', 'SHOW', 'DESCRIBE', 'EXPLAIN', 'WITH', 'PRAGMA'
        }
    
    def _validate_credentials(self, db_type: str, host: str, credentials: Dict[str, Any]) -> bool:
        """Validate database credentials and connection parameters."""
        required_fields = {
            'postgresql': ['username', 'password', 'database'],
            'mysql': ['username', 'password', 'database'],
            'sqlite': ['database_path'],
            'mongodb': ['username', 'password', 'database']
        }
        
        if db_type not in required_fields:
            return False
            
        for field in required_fields[db_type]:
            if field not in credentials or not credentials[field]:
                return False
        
        # Basic host validation
        if db_type != 'sqlite':
            try:
                parsed = urlparse(f"http://{host}")
                if not parsed.hostname:
                    return False
            except:
                return False
        
        return True
    
    def _generate_connection_id(self, db_type: str, host: str, credentials: Dict[str, Any]) -> str:
        """Generate a unique connection ID for caching."""
        key_data = f"{db_type}:{host}:{credentials.get('username', '')}:{credentials.get('database', '')}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def _is_safe_query(self, query: str) -> bool:
        """Check if query is safe (read-only operations)."""
        query_upper = query.strip().upper()
        
        # Check for dangerous operations
        dangerous_ops = [
            'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 
            'TRUNCATE', 'REPLACE', 'MERGE', 'CALL', 'EXEC'
        ]
        
        for op in dangerous_ops:
            if query_upper.startswith(op):
                return False
        
        # Additional security patterns from your implementation
        dangerous_patterns = [
            r'\bdrop\s+database\b',
            r'\bdrop\s+user\b', 
            r'\bcreate\s+user\b',
            r'\balter\s+user\b',
            r'\bgrant\b',
            r'\brevoke\b',
            r'pg_sleep',
            r';\s*\w+',  # Multiple statements
            r'--',  # Comments
            r'/\*.*\*/',  # Block comments
            r'UNION.*SELECT',  # SQL injection
            r'INFORMATION_SCHEMA'
        ]
        
        for pattern in dangerous_patterns:
            if re.search(pattern, query_upper):
                return False
        
        return True
    
    def connect_database(self, db_type: str, host: str, port: Optional[int], credentials: Dict[str, Any]) -> str:
        """Establish a database connection and return connection ID."""
        if not self._validate_credentials(db_type, host, credentials):
            raise ValueError("Invalid credentials or connection parameters")
        
        connection_id = self._generate_connection_id(db_type, host, credentials)
        
        # Check if connection already exists
        if connection_id in self.connections:
            return connection_id
        
        # Limit number of connections
        if len(self.connections) >= self.max_connections:
            raise ValueError("Maximum number of connections reached")
        
        try:
            if db_type == 'postgresql' and POSTGRES_AVAILABLE:
                conn = psycopg2.connect(
                    host=host,
                    port=port or 5432,
                    database=credentials['database'],
                    user=credentials['username'],
                    password=credentials['password'],
                    connect_timeout=self.connection_timeout
                )
                
            elif db_type == 'mysql' and MYSQL_AVAILABLE:
                conn = mysql.connector.connect(
                    host=host,
                    port=port or 3306,
                    database=credentials['database'],
                    user=credentials['username'],
                    password=credentials['password'],
                    connection_timeout=self.connection_timeout
                )
                
            elif db_type == 'sqlite' and SQLITE_AVAILABLE:
                conn = sqlite3.connect(credentials['database_path'], timeout=self.connection_timeout)
                
            elif db_type == 'mongodb' and MONGODB_AVAILABLE:
                conn = pymongo.MongoClient(
                    host=host,
                    port=port or 27017,
                    username=credentials['username'],
                    password=credentials['password'],
                    serverSelectionTimeoutMS=self.connection_timeout * 1000
                )
                # Test connection
                conn.admin.command('ping')
                
            else:
                raise ValueError(f"Unsupported database type: {db_type}")
            
            self.connections[connection_id] = {
                'connection': conn,
                'db_type': db_type,
                'host': host,
                'port': port,
                'database': credentials.get('database', ''),
                'created_at': time.time()
            }
            
            logger.info(f"Successfully connected to {db_type} database: {connection_id}")
            return connection_id
            
        except Exception as e:
            logger.error(f"Failed to connect to {db_type} database: {str(e)}")
            raise ValueError(f"Database connection failed: {str(e)}")
    
    def execute_query(self, connection_id: str, query: str, schema: Optional[str] = None) -> Dict[str, Any]:
        """Execute a query on the specified connection."""
        if connection_id not in self.connections:
            raise ValueError("Invalid connection ID")
        
        if not self._is_safe_query(query):
            raise ValueError("Query not allowed - only SELECT operations are permitted")
        
        conn_info = self.connections[connection_id]
        conn = conn_info['connection']
        db_type = conn_info['db_type']
        
        try:
            if db_type == 'postgresql':
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # Set schema if provided
                if schema:
                    cursor.execute(f"SET search_path TO {schema}")
                
                cursor.execute(query)
                
                if cursor.description:
                    results = cursor.fetchall()
                    cursor.close()
                    
                    return {
                        'success': True,
                        'columns': [desc.name for desc in cursor.description],
                        'rows': [dict(row) for row in results],
                        'row_count': len(results)
                    }
                else:
                    cursor.close()
                    return {
                        'success': True,
                        'message': 'Query executed successfully',
                        'rows_affected': cursor.rowcount
                    }
                    
            elif db_type == 'mysql':
                cursor = conn.cursor(dictionary=True)
                
                # Set schema if provided
                if schema:
                    cursor.execute(f"USE {schema}")
                
                cursor.execute(query)
                
                if cursor.description:
                    results = cursor.fetchall()
                    cursor.close()
                    
                    return {
                        'success': True,
                        'columns': [desc[0] for desc in cursor.description],
                        'rows': results,
                        'row_count': len(results)
                    }
                else:
                    cursor.close()
                    return {
                        'success': True,
                        'message': 'Query executed successfully',
                        'rows_affected': cursor.rowcount
                    }
                    
            elif db_type == 'sqlite':
                cursor = conn.cursor()
                cursor.execute(query)
                
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    
                    return {
                        'success': True,
                        'columns': columns,
                        'rows': [dict(zip(columns, row)) for row in rows],
                        'row_count': len(rows)
                    }
                else:
                    return {
                        'success': True,
                        'message': 'Query executed successfully'
                    }
                    
            elif db_type == 'mongodb':
                # For MongoDB, convert SQL-like queries to MongoDB operations
                db = conn[conn_info['database']]
                
                if query.upper().startswith('SHOW COLLECTIONS'):
                    collections = db.list_collection_names()
                    return {
                        'success': True,
                        'collections': collections
                    }
                else:
                    return {
                        'success': False,
                        'error': 'Only SHOW COLLECTIONS is supported for MongoDB'
                    }
                    
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_schema_info(self, connection_id: str, schema_name: Optional[str] = None) -> Dict[str, Any]:
        """Get schema information for the database."""
        if connection_id not in self.connections:
            raise ValueError("Invalid connection ID")
        
        conn_info = self.connections[connection_id]
        db_type = conn_info['db_type']
        
        try:
            if db_type == 'postgresql':
                schema_query = """
                SELECT schemaname as schema_name, tablename as table_name, 
                       'table' as object_type
                FROM pg_tables 
                WHERE schemaname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                """
                if schema_name:
                    schema_query += f" AND schemaname = '{schema_name}'"
                
                result = self.execute_query(connection_id, schema_query)
                
            elif db_type == 'mysql':
                schema_query = "SHOW TABLES"
                if schema_name:
                    schema_query += f" FROM {schema_name}"
                
                result = self.execute_query(connection_id, schema_query)
                
            elif db_type == 'sqlite':
                schema_query = "SELECT name FROM sqlite_master WHERE type='table'"
                result = self.execute_query(connection_id, schema_query)
                
            elif db_type == 'mongodb':
                result = self.execute_query(connection_id, "SHOW COLLECTIONS")
                
            return result
            
        except Exception as e:
            logger.error(f"Schema info retrieval failed: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_table_info(self, connection_id: str, table_name: str, schema_name: Optional[str] = None) -> Dict[str, Any]:
        """Get detailed information about a specific table."""
        if connection_id not in self.connections:
            raise ValueError("Invalid connection ID")
        
        conn_info = self.connections[connection_id]
        db_type = conn_info['db_type']
        
        try:
            if db_type == 'postgresql':
                query = f"""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
                """
                if schema_name:
                    query += f" AND table_schema = '{schema_name}'"
                
            elif db_type == 'mysql':
                query = f"DESCRIBE {table_name}"
                if schema_name:
                    query = f"DESCRIBE {schema_name}.{table_name}"
                
            elif db_type == 'sqlite':
                query = f"PRAGMA table_info({table_name})"
                
            else:
                return {
                    'success': False,
                    'error': f'Table info not supported for {db_type}'
                }
            
            result = self.execute_query(connection_id, query, schema_name)
            return result
            
        except Exception as e:
            logger.error(f"Table info retrieval failed: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def disconnect(self, connection_id: str) -> bool:
        """Disconnect from database."""
        if connection_id not in self.connections:
            return False
        
        try:
            conn_info = self.connections[connection_id]
            conn = conn_info['connection']
            
            if conn_info['db_type'] in ['postgresql', 'mysql', 'sqlite']:
                conn.close()
            elif conn_info['db_type'] == 'mongodb':
                conn.close()
            
            del self.connections[connection_id]
            logger.info(f"Disconnected from database: {connection_id}")
            return True
            
        except Exception as e:
            logger.error(f"Disconnect failed: {str(e)}")
            return False
    
    def list_connections(self) -> List[Dict[str, Any]]:
        """List all active connections."""
        connections = []
        for conn_id, conn_info in self.connections.items():
            connections.append({
                'connection_id': conn_id,
                'db_type': conn_info['db_type'],
                'host': conn_info['host'],
                'port': conn_info['port'],
                'database': conn_info['database'],
                'created_at': conn_info['created_at']
            })
        return connections

# Initialize the database manager
db_manager = DatabaseConnectionManager()

@mcp.tool()
def connect_database(db_type: str, host: str = "localhost", port: int = None, 
                    username: str = "", password: str = "", database: str = "",
                    database_path: str = "") -> str:
    """Connect to a database with provided credentials.
    
    Args:
        db_type: Database type (postgresql, mysql, sqlite, mongodb)
        host: Database host address (not needed for SQLite)
        port: Database port (optional, uses default if not specified)
        username: Database username
        password: Database password
        database: Database name
        database_path: Database file path (SQLite only)
    """
    try:
        credentials = {
            'username': username,
            'password': password,
            'database': database,
            'database_path': database_path
        }
        
        connection_id = db_manager.connect_database(db_type, host, port, credentials)
        
        return json.dumps({
            "success": True,
            "connection_id": connection_id,
            "message": f"Successfully connected to {db_type} database",
            "db_type": db_type,
            "host": host,
            "database": database
        }, indent=2)
        
    except Exception as e:
        logger.error(f"Connection failed: {str(e)}")
        return json.dumps({
            "success": False,
            "error": str(e)
        }, indent=2)

@mcp.tool()
def execute_query(connection_id: str, query: str, schema: str = None, limit: int = 100) -> str:
    """Execute a SELECT query on a connected database.
    
    Args:
        connection_id: Database connection ID from connect_database
        query: SQL SELECT query to execute
        schema: Schema name to use (optional)
        limit: Maximum number of rows to return (default: 100)
    """
    try:
        # Apply limit if not already in query
        query_lower = query.lower().strip()
        if 'limit' not in query_lower:
            query += f" LIMIT {min(limit, 1000)}"
        
        result = db_manager.execute_query(connection_id, query, schema)
        
        return json.dumps(result, indent=2, default=str)
        
    except Exception as e:
        logger.error(f"Query execution failed: {str(e)}")
        return json.dumps({
            "success": False,
            "error": str(e)
        }, indent=2)

@mcp.tool()
def get_schema_info(connection_id: str, schema_name: str = None) -> str:
    """Get schema and table information from a database.
    
    Args:
        connection_id: Database connection ID
        schema_name: Specific schema name to inspect (optional)
    """
    try:
        result = db_manager.get_schema_info(connection_id, schema_name)
        return json.dumps(result, indent=2, default=str)
        
    except Exception as e:
        logger.error(f"Schema info failed: {str(e)}")
        return json.dumps({
            "success": False,
            "error": str(e)
        }, indent=2)

@mcp.tool()
def get_table_info(connection_id: str, table_name: str, schema_name: str = None) -> str:
    """Get detailed information about a specific table.
    
    Args:
        connection_id: Database connection ID
        table_name: Name of the table to inspect
        schema_name: Schema containing the table (optional)
    """
    try:
        result = db_manager.get_table_info(connection_id, table_name, schema_name)
        return json.dumps(result, indent=2, default=str)
        
    except Exception as e:
        logger.error(f"Table info failed: {str(e)}")
        return json.dumps({
            "success": False,
            "error": str(e)
        }, indent=2)

@mcp.tool()
def list_connections() -> str:
    """List all active database connections."""
    try:
        connections = db_manager.list_connections()
        return json.dumps({
            "success": True,
            "connections": connections,
            "total_connections": len(connections)
        }, indent=2, default=str)
        
    except Exception as e:
        logger.error(f"List connections failed: {str(e)}")
        return json.dumps({
            "success": False,
            "error": str(e)
        }, indent=2)

@mcp.tool()
def disconnect_database(connection_id: str) -> str:
    """Disconnect from a database.
    
    Args:
        connection_id: Database connection ID to disconnect
    """
    try:
        success = db_manager.disconnect(connection_id)
        return json.dumps({
            "success": success,
            "message": f"Disconnected from database: {connection_id}" if success else "Failed to disconnect"
        }, indent=2)
        
    except Exception as e:
        logger.error(f"Disconnect failed: {str(e)}")
        return json.dumps({
            "success": False,
            "error": str(e)
        }, indent=2)

@mcp.tool()
def test_connection(connection_id: str) -> str:
    """Test a database connection.
    
    Args:
        connection_id: Database connection ID to test
    """
    try:
        if connection_id not in db_manager.connections:
            return json.dumps({
                "success": False,
                "error": "Invalid connection ID"
            }, indent=2)
        
        conn_info = db_manager.connections[connection_id]
        db_type = conn_info['db_type']
        
        if db_type == 'postgresql':
            test_query = "SELECT version()"
        elif db_type == 'mysql':
            test_query = "SELECT VERSION()"
        elif db_type == 'sqlite':
            test_query = "SELECT sqlite_version()"
        elif db_type == 'mongodb':
            test_query = "SHOW COLLECTIONS"
        else:
            test_query = "SELECT 1"
        
        result = db_manager.execute_query(connection_id, test_query)
        
        if result.get('success'):
            return json.dumps({
                "success": True,
                "message": f"Connection to {db_type} database is active",
                "connection_info": {
                    "db_type": db_type,
                    "host": conn_info['host'],
                    "database": conn_info['database'],
                    "connected_since": conn_info['created_at']
                }
            }, indent=2, default=str)
        else:
            return json.dumps({
                "success": False,
                "error": f"Connection test failed: {result.get('error', 'Unknown error')}"
            }, indent=2)
            
    except Exception as e:
        logger.error(f"Connection test failed: {str(e)}")
        return json.dumps({
            "success": False,
            "error": str(e)
        }, indent=2)

@mcp.tool()
def get_database_status() -> str:
    """Get status information about available database drivers and active connections."""
    try:
        status = {
            "success": True,
            "available_drivers": {
                "postgresql": POSTGRES_AVAILABLE,
                "mysql": MYSQL_AVAILABLE,
                "sqlite": SQLITE_AVAILABLE,
                "mongodb": MONGODB_AVAILABLE
            },
            "active_connections": len(db_manager.connections),
            "max_connections": db_manager.max_connections,
            "connection_timeout": db_manager.connection_timeout
        }
        
        return json.dumps(status, indent=2)
        
    except Exception as e:
        logger.error(f"Status check failed: {str(e)}")
        return json.dumps({
            "success": False,
            "error": str(e)
        }, indent=2)

if __name__ == "__main__":
    # Check if run with --web flag
    if len(sys.argv) > 1 and sys.argv[1] == "--web":
        # Web mode (SSE) - for Chainlit or web interfaces
        import uvicorn
        
        # Create Starlette application
        app = Starlette(
            routes=[
                Mount('/', app=mcp.sse_app()),
            ]
        )
        
        # Add CORS middleware
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],  # For development; restrict in production
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        
        # Get port from arguments or environment or default
        port = int(os.environ.get("MCP_PORT", 8001))
        if len(sys.argv) > 2:
            try:
                port = int(sys.argv[2])
            except ValueError:
                pass
                
        logger.info(f"Starting Secure Multi-Database MCP Web server on port {port}")
        logger.info(f"Available database drivers:")
        logger.info(f"  PostgreSQL: {POSTGRES_AVAILABLE}")
        logger.info(f"  MySQL: {MYSQL_AVAILABLE}")
        logger.info(f"  SQLite: {SQLITE_AVAILABLE}")
        logger.info(f"  MongoDB: {MONGODB_AVAILABLE}")
        
        uvicorn.run(app, host="127.0.0.1", port=port)
    else:
        # STDIO mode - for Claude Desktop
        logger.info("Starting Secure Multi-Database MCP STDIO server")
        logger.info(f"Available database drivers:")
        logger.info(f"  PostgreSQL: {POSTGRES_AVAILABLE}")
        logger.info(f"  MySQL: {MYSQL_AVAILABLE}")
        logger.info(f"  SQLite: {SQLITE_AVAILABLE}")
        logger.info(f"  MongoDB: {MONGODB_AVAILABLE}")
        
        mcp.run(transport='stdio')