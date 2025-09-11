"""
Database configuration module for multi-database MCP server
"""

import os
from typing import Dict, Any, Optional

class DatabaseConfig:
    """Database configuration management"""
    
    # Default connection settings
    DEFAULT_SETTINGS = {
        "connection_timeout": 30,
        "max_connections": 10,
        "query_timeout": 60,
        "enable_logging": True
    }
    
    # Database-specific default ports
    DEFAULT_PORTS = {
        "postgresql": 5432,
        "mysql": 3306,
        "mongodb": 27017,
        "sqlite": None  # File-based
    }
    
    # Required credentials by database type
    REQUIRED_CREDENTIALS = {
        "postgresql": ["username", "password", "database"],
        "mysql": ["username", "password", "database"],
        "sqlite": ["database_path"],
        "mongodb": ["username", "password", "database"]
    }
    
    # Security settings
    SECURITY_SETTINGS = {
        "allowed_operations": {
            "SELECT", "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "WITH", "PRAGMA"
        },
        "blocked_operations": {
            "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", 
            "TRUNCATE", "REPLACE", "MERGE", "CALL", "EXEC"
        },
        "max_query_length": 10000,
        "max_nested_queries": 3,
        "allow_information_schema": True,
        "allow_system_catalogs": True
    }
    
    @classmethod
    def get_default_port(cls, db_type: str) -> Optional[int]:
        """Get default port for database type"""
        return cls.DEFAULT_PORTS.get(db_type.lower())
    
    @classmethod
    def get_required_credentials(cls, db_type: str) -> list:
        """Get required credential fields for database type"""
        return cls.REQUIRED_CREDENTIALS.get(db_type.lower(), [])
    
    @classmethod
    def validate_db_type(cls, db_type: str) -> bool:
        """Validate if database type is supported"""
        supported_types = {"postgresql", "mysql", "sqlite", "mongodb"}
        return db_type.lower() in supported_types
    
    @classmethod
    def get_connection_string_template(cls, db_type: str) -> str:
        """Get connection string template for database type"""
        templates = {
            "postgresql": "postgresql://{username}:{password}@{host}:{port}/{database}",
            "mysql": "mysql://{username}:{password}@{host}:{port}/{database}",
            "mongodb": "mongodb://{username}:{password}@{host}:{port}/{database}",
            "sqlite": "sqlite:///{database_path}"
        }
        return templates.get(db_type.lower(), "")
    
    @classmethod
    def get_driver_info(cls, db_type: str) -> Dict[str, Any]:
        """Get driver information and requirements"""
        driver_info = {
            "postgresql": {
                "driver": "psycopg2",
                "package": "psycopg2-binary",
                "cursor_factory": "psycopg2.extras.RealDictCursor",
                "features": ["schemas", "transactions", "prepared_statements"]
            },
            "mysql": {
                "driver": "mysql.connector",
                "package": "mysql-connector-python", 
                "cursor_factory": "mysql.connector.cursor.MySQLCursorDict",
                "features": ["schemas", "transactions", "prepared_statements"]
            },
            "sqlite": {
                "driver": "sqlite3",
                "package": "built-in",
                "cursor_factory": "sqlite3.Row",
                "features": ["file_based", "transactions"]
            },
            "mongodb": {
                "driver": "pymongo",
                "package": "pymongo",
                "cursor_factory": None,
                "features": ["collections", "documents", "aggregation"]
            }
        }
        return driver_info.get(db_type.lower(), {})
    
    @classmethod
    def load_from_env(cls) -> Dict[str, Any]:
        """Load configuration from environment variables"""
        config = cls.DEFAULT_SETTINGS.copy()
        
        # Override with environment variables if present
        config.update({
            "connection_timeout": int(os.getenv("DB_CONNECTION_TIMEOUT", config["connection_timeout"])),
            "max_connections": int(os.getenv("DB_MAX_CONNECTIONS", config["max_connections"])),
            "query_timeout": int(os.getenv("DB_QUERY_TIMEOUT", config["query_timeout"])),
            "enable_logging": os.getenv("DB_ENABLE_LOGGING", "true").lower() == "true"
        })
        
        return config