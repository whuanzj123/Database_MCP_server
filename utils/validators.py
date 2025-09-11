"""
Validation utilities for multi-database MCP server
"""

import re
import os
from typing import Dict, Any, Tuple
from urllib.parse import urlparse
from ..database.config import DatabaseConfig


class CredentialValidator:
    """Validates database credentials and connection parameters"""
    
    def validate_credentials(self, db_type: str, host: str, credentials: Dict[str, Any]) -> bool:
        """
        Validate database credentials and connection parameters.
        
        Args:
            db_type: Database type (postgresql, mysql, sqlite, mongodb)
            host: Database host
            credentials: Dictionary containing credentials
            
        Returns:
            bool: True if valid, False otherwise
        """
        # Validate database type
        if not DatabaseConfig.validate_db_type(db_type):
            return False
        
        # Get required credentials for this database type
        required_fields = DatabaseConfig.get_required_credentials(db_type)
        
        # Check all required fields are present and non-empty
        for field in required_fields:
            if field not in credentials or not credentials[field]:
                return False
        
        # Database-specific validation
        if db_type == 'sqlite':
            return self._validate_sqlite_credentials(credentials)
        else:
            return self._validate_network_db_credentials(db_type, host, credentials)
    
    def _validate_sqlite_credentials(self, credentials: Dict[str, Any]) -> bool:
        """Validate SQLite-specific credentials"""
        database_path = credentials.get('database_path', '')
        
        # Basic path validation
        if not database_path:
            return False
        
        # Check if path is reasonable (not trying to access system files)
        if self._is_suspicious_path(database_path):
            return False
        
        # Check if directory exists (for file creation)
        directory = os.path.dirname(database_path)
        if directory and not os.path.exists(directory):
            return False
        
        return True
    
    def _validate_network_db_credentials(self, db_type: str, host: str, credentials: Dict[str, Any]) -> bool:
        """Validate network database credentials"""
        # Validate host
        if not self._validate_host(host):
            return False
        
        # Validate username
        username = credentials.get('username', '')
        if not self._validate_username(username):
            return False
        
        # Validate password (basic checks)
        password = credentials.get('password', '')
        if not self._validate_password(password):
            return False
        
        # Validate database name
        database = credentials.get('database', '')
        if not self._validate_database_name(database):
            return False
        
        return True
    
    def _validate_host(self, host: str) -> bool:
        """Validate database host"""
        if not host or len(host) > 255:
            return False
        
        # Allow localhost variations
        localhost_variants = {'localhost', '127.0.0.1', '::1'}
        if host in localhost_variants:
            return True
        
        # Validate hostname/IP format
        try:
            parsed = urlparse(f"http://{host}")
            if not parsed.hostname:
                return False
        except:
            return False
        
        # Block suspicious hosts
        suspicious_patterns = [
            r'.*\.onion$',  # Tor hidden services
            r'^\d+\.\d+\.\d+\.\d+$',  # Raw IP addresses (optional restriction)
        ]
        
        for pattern in suspicious_patterns:
            if re.match(pattern, host, re.IGNORECASE):
                # Allow localhost IP
                if host != '127.0.0.1':
                    return False
        
        return True
    
    def _validate_username(self, username: str) -> bool:
        """Validate database username"""
        if not username or len(username) > 100:
            return False
        
        # Basic character validation
        if not re.match(r'^[a-zA-Z0-9_\-\.@]+$', username):
            return False
        
        # Block common attack patterns
        dangerous_patterns = [
            r'.*[;\'"\\].*',  # SQL injection characters
            r'.*(union|select|drop|create|alter|insert|update|delete).*'
        ]
        
        for pattern in dangerous_patterns:
            if re.match(pattern, username, re.IGNORECASE):
                return False
        
        return True
    
    def _validate_password(self, password: str) -> bool:
        """Validate database password"""
        if not password or len(password) > 200:
            return False
        
        # Check for null bytes or control characters
        if '\x00' in password or any(ord(c) < 32 for c in password if c not in '\t\n\r'):
            return False
        
        return True
    
    def _validate_database_name(self, database: str) -> bool:
        """Validate database name"""
        if not database or len(database) > 100:
            return False
        
        # Basic character validation for database names
        if not re.match(r'^[a-zA-Z0-9_\-]+$', database):
            return False
        
        # Block system databases
        system_databases = {
            'mysql', 'information_schema', 'performance_schema', 'sys',
            'postgres', 'template0', 'template1',
            'admin', 'local', 'config'
        }
        
        if database.lower() in system_databases:
            return False
        
        return True
    
    def _is_suspicious_path(self, path: str) -> bool:
        """Check if file path is suspicious"""
        path = path.lower()
        
        # Block system directories and files
        suspicious_paths = [
            '/etc/', '/proc/', '/sys/', '/dev/', '/root/',
            'c:\\windows\\', 'c:\\system32\\', 
            '/var/lib/mysql/', '/var/lib/postgresql/',
            '..', '~', '$'
        ]
        
        for suspicious in suspicious_paths:
            if suspicious in path:
                return True
        
        return False


class ConnectionValidator:
    """Validates connection parameters and limits"""
    
    def __init__(self, max_connections: int = 10):
        self.max_connections = max_connections
    
    def validate_connection_limits(self, current_connections: int) -> Tuple[bool, str]:
        """
        Validate connection limits.
        
        Args:
            current_connections: Number of current active connections
            
        Returns:
            Tuple[bool, str]: (is_valid, message)
        """
        if current_connections >= self.max_connections:
            return False, f"Maximum connections ({self.max_connections}) reached"
        
        return True, "Connection limit OK"
    
    def validate_port(self, port: int, db_type: str) -> Tuple[bool, str]:
        """
        Validate database port number.
        
        Args:
            port: Port number
            db_type: Database type
            
        Returns:
            Tuple[bool, str]: (is_valid, message)
        """
        if not isinstance(port, int):
            return False, "Port must be an integer"
        
        if port < 1 or port > 65535:
            return False, "Port must be between 1 and 65535"
        
        # Check if port is in reserved range
        if port < 1024:
            return False, "Port numbers below 1024 are reserved"
        
        # Warn about non-standard ports
        standard_ports = DatabaseConfig.DEFAULT_PORTS
        expected_port = standard_ports.get(db_type)
        
        if expected_port and port != expected_port:
            return True, f"Non-standard port (expected {expected_port})"
        
        return True, "Port is valid"


class QueryValidator:
    """Validates query parameters and structure"""
    
    def validate_query_params(self, query: str, limit: int = None, 
                            schema: str = None) -> Tuple[bool, str]:
        """
        Validate query execution parameters.
        
        Args:
            query: SQL query
            limit: Row limit
            schema: Schema name
            
        Returns:
            Tuple[bool, str]: (is_valid, message)
        """
        # Validate query
        if not query or not query.strip():
            return False, "Query cannot be empty"
        
        if len(query) > 10000:
            return False, "Query too long (max 10000 characters)"
        
        # Validate limit
        if limit is not None:
            if not isinstance(limit, int) or limit < 1:
                return False, "Limit must be a positive integer"
            
            if limit > 1000:
                return False, "Limit cannot exceed 1000 rows"
        
        # Validate schema name
        if schema is not None:
            if not re.match(r'^[a-zA-Z0-9_]+$', schema):
                return False, "Invalid schema name format"
            
            if len(schema) > 100:
                return False, "Schema name too long"
        
        return True, "Query parameters are valid"
    
    def validate_table_name(self, table_name: str) -> Tuple[bool, str]:
        """
        Validate table name format.
        
        Args:
            table_name: Table name to validate
            
        Returns:
            Tuple[bool, str]: (is_valid, message)
        """
        if not table_name or not table_name.strip():
            return False, "Table name cannot be empty"
        
        if len(table_name) > 100:
            return False, "Table name too long"
        
        # Basic format validation
        if not re.match(r'^[a-zA-Z0-9_\-\.]+$', table_name):
            return False, "Invalid characters in table name"
        
        # Check for SQL injection patterns
        dangerous_patterns = [
            r'.*;.*', r'.*--.*', r'.*/\*.*', r'.*\*/.*',
            r'.*(union|select|drop|create|alter).*'
        ]
        
        for pattern in dangerous_patterns:
            if re.match(pattern, table_name, re.IGNORECASE):
                return False, "Potentially dangerous table name"
        
        return True, "Table name is valid"


class InputSanitizer:
    """Sanitizes and normalizes input data"""
    
    @staticmethod
    def sanitize_string(value: str, max_length: int = 1000) -> str:
        """
        Sanitize string input.
        
        Args:
            value: String to sanitize
            max_length: Maximum allowed length
            
        Returns:
            str: Sanitized string
        """
        if not isinstance(value, str):
            value = str(value)
        
        # Remove null bytes and control characters
        value = ''.join(char for char in value if ord(char) >= 32 or char in '\t\n\r')
        
        # Limit length
        if len(value) > max_length:
            value = value[:max_length]
        
        return value.strip()
    
    @staticmethod
    def sanitize_identifier(identifier: str) -> str:
        """
        Sanitize database identifiers (table names, column names, etc.).
        
        Args:
            identifier: Identifier to sanitize
            
        Returns:
            str: Sanitized identifier
        """
        if not isinstance(identifier, str):
            identifier = str(identifier)
        
        # Remove dangerous characters
        identifier = re.sub(r'[^a-zA-Z0-9_\-\.]', '', identifier)
        
        # Limit length
        if len(identifier) > 100:
            identifier = identifier[:100]
        
        return identifier.strip()
    
    @staticmethod
    def validate_json_input(data: Any, max_depth: int = 10) -> Tuple[bool, str]:
        """
        Validate JSON input data.
        
        Args:
            data: Data to validate
            max_depth: Maximum nesting depth
            
        Returns:
            Tuple[bool, str]: (is_valid, message)
        """
        def check_depth(obj, depth=0):
            if depth > max_depth:
                return False
            
            if isinstance(obj, dict):
                return all(check_depth(v, depth + 1) for v in obj.values())
            elif isinstance(obj, list):
                return all(check_depth(item, depth + 1) for item in obj)
            else:
                return True
        
        try:
            if not check_depth(data):
                return False, f"Data nesting exceeds maximum depth of {max_depth}"
            
            return True, "JSON input is valid"
            
        except Exception as e:
            return False, f"Invalid JSON data: {str(e)}"