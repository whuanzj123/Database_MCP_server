"""
Database connection management for multi-database MCP server
"""

import hashlib
import time
import logging
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from .config import DatabaseConfig
from ..utils.security import SecurityValidator
from ..utils.validators import CredentialValidator

logger = logging.getLogger(__name__)

# Database driver imports with availability checking
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


class ConnectionInfo:
    """Container for database connection information"""
    
    def __init__(self, connection_id: str, db_type: str, host: str, port: int, 
                 database: str, created_at: float, connection_obj: Any):
        self.connection_id = connection_id
        self.db_type = db_type
        self.host = host
        self.port = port
        self.database = database
        self.created_at = created_at
        self.connection = connection_obj
        self.last_used = created_at
        self.query_count = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "connection_id": self.connection_id,
            "db_type": self.db_type,
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "created_at": self.created_at,
            "last_used": self.last_used,
            "query_count": self.query_count
        }


class MultiDatabaseManager:
    """Manages connections to multiple database engines"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or DatabaseConfig.load_from_env()
        self.connections: Dict[str, ConnectionInfo] = {}
        self.credential_validator = CredentialValidator()
        self.security_validator = SecurityValidator()
        
        # Connection limits and timeouts
        self.connection_timeout = self.config["connection_timeout"]
        self.max_connections = self.config["max_connections"]
        self.query_timeout = self.config["query_timeout"]
        
        # Driver availability
        self.POSTGRES_AVAILABLE = POSTGRES_AVAILABLE
        self.MYSQL_AVAILABLE = MYSQL_AVAILABLE
        self.SQLITE_AVAILABLE = SQLITE_AVAILABLE
        self.MONGODB_AVAILABLE = MONGODB_AVAILABLE
        
        logger.info(f"MultiDatabaseManager initialized with {self.max_connections} max connections")
    
    def _generate_connection_id(self, db_type: str, host: str, credentials: Dict[str, Any]) -> str:
        """Generate a unique connection ID for caching"""
        key_data = f"{db_type}:{host}:{credentials.get('username', '')}:{credentials.get('database', '')}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def _validate_connection_request(self, db_type: str, host: str, credentials: Dict[str, Any]) -> None:
        """Validate connection request parameters"""
        # Validate database type
        if not DatabaseConfig.validate_db_type(db_type):
            raise ValueError(f"Unsupported database type: {db_type}")
        
        # Check driver availability
        if db_type == 'postgresql' and not self.POSTGRES_AVAILABLE:
            raise ValueError("PostgreSQL driver not available. Install psycopg2-binary")
        elif db_type == 'mysql' and not self.MYSQL_AVAILABLE:
            raise ValueError("MySQL driver not available. Install mysql-connector-python")
        elif db_type == 'sqlite' and not self.SQLITE_AVAILABLE:
            raise ValueError("SQLite driver not available")
        elif db_type == 'mongodb' and not self.MONGODB_AVAILABLE:
            raise ValueError("MongoDB driver not available. Install pymongo")
        
        # Validate credentials
        if not self.credential_validator.validate_credentials(db_type, host, credentials):
            raise ValueError("Invalid credentials or connection parameters")
        
        # Check connection limits
        if len(self.connections) >= self.max_connections:
            raise ValueError(f"Maximum number of connections reached ({self.max_connections})")
    
    def connect_database(self, db_type: str, host: str, port: Optional[int], 
                        credentials: Dict[str, Any]) -> str:
        """Establish a database connection and return connection ID"""
        # Validate request
        self._validate_connection_request(db_type, host, credentials)
        
        # Use default port if not specified
        if port is None:
            port = DatabaseConfig.get_default_port(db_type)
        
        # Generate connection ID
        connection_id = self._generate_connection_id(db_type, host, credentials)
        
        # Check if connection already exists
        if connection_id in self.connections:
            logger.info(f"Reusing existing connection: {connection_id}")
            return connection_id
        
        try:
            # Create connection based on database type
            if db_type == 'postgresql':
                conn = self._connect_postgresql(host, port, credentials)
            elif db_type == 'mysql':
                conn = self._connect_mysql(host, port, credentials)
            elif db_type == 'sqlite':
                conn = self._connect_sqlite(credentials)
            elif db_type == 'mongodb':
                conn = self._connect_mongodb(host, port, credentials)
            else:
                raise ValueError(f"Unsupported database type: {db_type}")
            
            # Store connection info
            conn_info = ConnectionInfo(
                connection_id=connection_id,
                db_type=db_type,
                host=host,
                port=port,
                database=credentials.get('database', ''),
                created_at=time.time(),
                connection_obj=conn
            )
            
            self.connections[connection_id] = conn_info
            
            logger.info(f"Successfully connected to {db_type} database: {connection_id}")
            return connection_id
            
        except Exception as e:
            logger.error(f"Failed to connect to {db_type} database: {str(e)}")
            raise ValueError(f"Database connection failed: {str(e)}")
    
    def _connect_postgresql(self, host: str, port: int, credentials: Dict[str, Any]):
        """Connect to PostgreSQL database"""
        return psycopg2.connect(
            host=host,
            port=port,
            database=credentials['database'],
            user=credentials['username'],
            password=credentials['password'],
            connect_timeout=self.connection_timeout
        )
    
    def _connect_mysql(self, host: str, port: int, credentials: Dict[str, Any]):
        """Connect to MySQL database"""
        return mysql.connector.connect(
            host=host,
            port=port,
            database=credentials['database'],
            user=credentials['username'],
            password=credentials['password'],
            connection_timeout=self.connection_timeout
        )
    
    def _connect_sqlite(self, credentials: Dict[str, Any]):
        """Connect to SQLite database"""
        return sqlite3.connect(
            credentials['database_path'], 
            timeout=self.connection_timeout
        )
    
    def _connect_mongodb(self, host: str, port: int, credentials: Dict[str, Any]):
        """Connect to MongoDB database"""
        client = pymongo.MongoClient(
            host=host,
            port=port,
            username=credentials['username'],
            password=credentials['password'],
            serverSelectionTimeoutMS=self.connection_timeout * 1000
        )
        # Test connection
        client.admin.command('ping')
        return client
    
    def execute_query(self, connection_id: str, query: str, schema: Optional[str] = None) -> Dict[str, Any]:
        """Execute a query on the specified connection"""
        if connection_id not in self.connections:
            raise ValueError("Invalid connection ID")
        
        # Validate query security
        if not self.security_validator.is_safe_query(query):
            raise ValueError("Query not allowed - only read-only operations are permitted")
        
        conn_info = self.connections[connection_id]
        conn_info.last_used = time.time()
        conn_info.query_count += 1
        
        try:
            if conn_info.db_type == 'postgresql':
                return self._execute_postgresql_query(conn_info, query, schema)
            elif conn_info.db_type == 'mysql':
                return self._execute_mysql_query(conn_info, query, schema)
            elif conn_info.db_type == 'sqlite':
                return self._execute_sqlite_query(conn_info, query)
            elif conn_info.db_type == 'mongodb':
                return self._execute_mongodb_query(conn_info, query)
            else:
                raise ValueError(f"Unsupported database type: {conn_info.db_type}")
                
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _execute_postgresql_query(self, conn_info: ConnectionInfo, query: str, schema: Optional[str]) -> Dict[str, Any]:
        """Execute PostgreSQL query"""
        cursor = conn_info.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        try:
            # Set schema if provided
            if schema:
                cursor.execute(f"SET search_path TO {schema}")
            
            cursor.execute(query)
            
            if cursor.description:
                results = cursor.fetchall()
                return {
                    'success': True,
                    'columns': [desc.name for desc in cursor.description],
                    'rows': [dict(row) for row in results],
                    'row_count': len(results)
                }
            else:
                return {
                    'success': True,
                    'message': 'Query executed successfully',
                    'rows_affected': cursor.rowcount
                }
        finally:
            cursor.close()
    
    def _execute_mysql_query(self, conn_info: ConnectionInfo, query: str, schema: Optional[str]) -> Dict[str, Any]:
        """Execute MySQL query"""
        cursor = conn_info.connection.cursor(dictionary=True)
        
        try:
            # Set schema if provided
            if schema:
                cursor.execute(f"USE {schema}")
            
            cursor.execute(query)
            
            if cursor.description:
                results = cursor.fetchall()
                return {
                    'success': True,
                    'columns': [desc[0] for desc in cursor.description],
                    'rows': results,
                    'row_count': len(results)
                }
            else:
                return {
                    'success': True,
                    'message': 'Query executed successfully',
                    'rows_affected': cursor.rowcount
                }
        finally:
            cursor.close()
    
    def _execute_sqlite_query(self, conn_info: ConnectionInfo, query: str) -> Dict[str, Any]:
        """Execute SQLite query"""
        cursor = conn_info.connection.cursor()
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
    
    def _execute_mongodb_query(self, conn_info: ConnectionInfo, query: str) -> Dict[str, Any]:
        """Execute MongoDB query (limited SQL-like operations)"""
        db = conn_info.connection[conn_info.database]
        
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
    
    def disconnect(self, connection_id: str) -> bool:
        """Disconnect from database"""
        if connection_id not in self.connections:
            return False
        
        try:
            conn_info = self.connections[connection_id]
            
            if conn_info.db_type in ['postgresql', 'mysql', 'sqlite']:
                conn_info.connection.close()
            elif conn_info.db_type == 'mongodb':
                conn_info.connection.close()
            
            del self.connections[connection_id]
            logger.info(f"Disconnected from database: {connection_id}")
            return True
            
        except Exception as e:
            logger.error(f"Disconnect failed: {str(e)}")
            return False
    
    def list_connections(self) -> List[Dict[str, Any]]:
        """List all active connections"""
        return [conn_info.to_dict() for conn_info in self.connections.values()]
    
    def get_connection_info(self, connection_id: str) -> Optional[Dict[str, Any]]:
        """Get information about a specific connection"""
        if connection_id in self.connections:
            return self.connections[connection_id].to_dict()
        return None
    
    def cleanup_stale_connections(self, max_idle_time: int = 3600) -> int:
        """Clean up connections that have been idle for too long"""
        current_time = time.time()
        stale_connections = []
        
        for conn_id, conn_info in self.connections.items():
            if current_time - conn_info.last_used > max_idle_time:
                stale_connections.append(conn_id)
        
        for conn_id in stale_connections:
            self.disconnect(conn_id)
        
        return len(stale_connections)
    
    def get_status(self) -> Dict[str, Any]:
        """Get manager status"""
        return {
            "available_drivers": {
                "postgresql": self.POSTGRES_AVAILABLE,
                "mysql": self.MYSQL_AVAILABLE,
                "sqlite": self.SQLITE_AVAILABLE,
                "mongodb": self.MONGODB_AVAILABLE
            },
            "active_connections": len(self.connections),
            "max_connections": self.max_connections,
            "connection_timeout": self.connection_timeout,
            "config": self.config
        }