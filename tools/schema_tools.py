"""
Schema exploration tools for multi-database MCP server
"""

import logging
from typing import Optional
from ..utils.formatters import ResponseFormatter
from ..utils.validators import QueryValidator

logger = logging.getLogger(__name__)


def register_tools(mcp, db_manager):
    """Register schema exploration tools with the MCP server"""
    
    @mcp.tool()
    def get_schema_info(connection_id: str, schema_name: str = None) -> str:
        """
        Get schema and table information from a database.
        
        Args:
            connection_id: Database connection ID
            schema_name: Specific schema name to inspect (optional)
            
        Returns:
            str: JSON response with schema information
        """
        try:
            logger.info(f"Getting schema info for connection {connection_id}")
            
            # Get connection info
            conn_info = db_manager.get_connection_info(connection_id)
            if not conn_info:
                return ResponseFormatter.error_response(
                    error="Invalid connection ID",
                    error_code="INVALID_CONNECTION_ID"
                )
            
            db_type = conn_info.get('db_type')
            
            # Build schema query based on database type
            if db_type == 'postgresql':
                if schema_name:
                    query = f"""
                    SELECT schemaname as schema_name, tablename as table_name, 
                           'table' as object_type
                    FROM pg_tables 
                    WHERE schemaname = '{schema_name}'
                    ORDER BY tablename
                    """
                else:
                    query = """
                    SELECT schemaname as schema_name, tablename as table_name, 
                           'table' as object_type
                    FROM pg_tables 
                    WHERE schemaname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                    ORDER BY schemaname, tablename
                    """
                    
            elif db_type == 'mysql':
                if schema_name:
                    query = f"""
                    SELECT table_schema as schema_name, table_name, table_type as object_type
                    FROM information_schema.tables 
                    WHERE table_schema = '{schema_name}'
                    ORDER BY table_name
                    """
                else:
                    query = """
                    SELECT table_schema as schema_name, table_name, table_type as object_type
                    FROM information_schema.tables 
                    WHERE table_schema NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')
                    ORDER BY table_schema, table_name
                    """
                    
            elif db_type == 'sqlite':
                query = """
                SELECT 'main' as schema_name, name as table_name, type as object_type
                FROM sqlite_master 
                WHERE type IN ('table', 'view')
                ORDER BY name
                """
                
            elif db_type == 'mongodb':
                # For MongoDB, we'll handle this differently
                result = db_manager.execute_query(connection_id, "SHOW COLLECTIONS")
                if result.get('success'):
                    collections = result.get('collections', [])
                    schema_data = {
                        "db_type": db_type,
                        "database": conn_info.get('database'),
                        "collections": collections,
                        "collection_count": len(collections)
                    }
                    return ResponseFormatter.schema_response(schema_data, conn_info.get('database'))
                else:
                    return ResponseFormatter.error_response(
                        error=f"Failed to get MongoDB collections: {result.get('error')}",
                        error_code="MONGODB_SCHEMA_ERROR"
                    )
            else:
                return ResponseFormatter.error_response(
                    error=f"Schema exploration not supported for {db_type}",
                    error_code="UNSUPPORTED_DB_TYPE"
                )
            
            # Execute the schema query
            result = db_manager.execute_query(connection_id, query)
            
            if result.get('success'):
                rows = result.get('rows', [])
                
                # Process results for better structure
                schema_data = {
                    "db_type": db_type,
                    "schema_name": schema_name,
                    "objects": rows,
                    "object_count": len(rows)
                }
                
                # Group by schema if multiple schemas
                if not schema_name and db_type in ['postgresql', 'mysql']:
                    schemas = {}
                    for row in rows:
                        schema = row.get('schema_name', 'unknown')
                        if schema not in schemas:
                            schemas[schema] = []
                        schemas[schema].append({
                            'table_name': row.get('table_name'),
                            'object_type': row.get('object_type', 'table')
                        })
                    schema_data['schemas'] = schemas
                
                return ResponseFormatter.schema_response(schema_data, schema_name)
            else:
                error_msg = result.get('error', 'Unknown error')
                return ResponseFormatter.error_response(
                    error=f"Failed to retrieve schema info: {error_msg}",
                    error_code="SCHEMA_QUERY_FAILED"
                )
                
        except Exception as e:
            error_msg = f"Error retrieving schema info: {str(e)}"
            logger.error(error_msg)
            return ResponseFormatter.error_response(
                error=error_msg,
                error_code="SCHEMA_INFO_ERROR"
            )
    
    @mcp.tool()
    def get_table_info(connection_id: str, table_name: str, schema_name: str = None) -> str:
        """
        Get detailed information about a specific table.
        
        Args:
            connection_id: Database connection ID
            table_name: Name of the table to inspect
            schema_name: Schema containing the table (optional)
            
        Returns:
            str: JSON response with table information
        """
        try:
            logger.info(f"Getting table info for {table_name} on connection {connection_id}")
            
            # Validate table name
            query_validator = QueryValidator()
            is_valid, message = query_validator.validate_table_name(table_name)
            if not is_valid:
                return ResponseFormatter.error_response(
                    error=f"Invalid table name: {message}",
                    error_code="INVALID_TABLE_NAME"
                )
            
            # Get connection info
            conn_info = db_manager.get_connection_info(connection_id)
            if not conn_info:
                return ResponseFormatter.error_response(
                    error="Invalid connection ID",
                    error_code="INVALID_CONNECTION_ID"
                )
            
            db_type = conn_info.get('db_type')
            
            # Build table info query based on database type
            if db_type == 'postgresql':
                schema_clause = f"AND table_schema = '{schema_name}'" if schema_name else "AND table_schema = 'public'"
                query = f"""
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale,
                    ordinal_position
                FROM information_schema.columns
                WHERE table_name = '{table_name}' {schema_clause}
                ORDER BY ordinal_position
                """
                
            elif db_type == 'mysql':
                schema_clause = f"AND table_schema = '{schema_name}'" if schema_name else f"AND table_schema = '{conn_info.get('database')}'"
                query = f"""
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale,
                    ordinal_position,
                    column_key,
                    extra
                FROM information_schema.columns
                WHERE table_name = '{table_name}' {schema_clause}
                ORDER BY ordinal_position
                """
                
            elif db_type == 'sqlite':
                query = f"PRAGMA table_info({table_name})"
                
            else:
                return ResponseFormatter.error_response(
                    error=f"Table info not supported for {db_type}",
                    error_code="UNSUPPORTED_DB_TYPE"
                )
            
            # Execute the table info query
            result = db_manager.execute_query(connection_id, query, schema_name)
            
            if result.get('success'):
                columns = result.get('rows', [])
                
                # Get additional table metadata
                table_metadata = {}
                
                # Get table constraints (for PostgreSQL and MySQL)
                if db_type in ['postgresql', 'mysql']:
                    try:
                        constraints_query = _get_constraints_query(db_type, table_name, schema_name, conn_info)
                        constraints_result = db_manager.execute_query(connection_id, constraints_query, schema_name)
                        if constraints_result.get('success'):
                            table_metadata['constraints'] = constraints_result.get('rows', [])
                    except Exception as e:
                        logger.warning(f"Could not retrieve constraints: {str(e)}")
                        table_metadata['constraints'] = []
                
                # Get table statistics
                try:
                    stats_query = _get_table_stats_query(db_type, table_name, schema_name)
                    if stats_query:
                        stats_result = db_manager.execute_query(connection_id, stats_query, schema_name)
                        if stats_result.get('success') and stats_result.get('rows'):
                            table_metadata['statistics'] = stats_result.get('rows')[0]
                except Exception as e:
                    logger.warning(f"Could not retrieve table statistics: {str(e)}")
                
                table_data = {
                    "table_name": table_name,
                    "schema_name": schema_name or "default",
                    "db_type": db_type,
                    "columns": columns,
                    "column_count": len(columns),
                    "metadata": table_metadata
                }
                
                return ResponseFormatter.table_response(table_data, table_name)
            else:
                error_msg = result.get('error', 'Unknown error')
                return ResponseFormatter.error_response(
                    error=f"Failed to retrieve table info: {error_msg}",
                    error_code="TABLE_INFO_FAILED"
                )
                
        except Exception as e:
            error_msg = f"Error retrieving table info: {str(e)}"
            logger.error(error_msg)
            return ResponseFormatter.error_response(
                error=error_msg,
                error_code="TABLE_INFO_ERROR"
            )
    
    @mcp.tool()
    def explore_schema_advanced(connection_id: str, schema_name: str = "public") -> str:
        """
        Get comprehensive schema information including tables, views, and relationships.
        
        Args:
            connection_id: Database connection ID
            schema_name: Schema name to explore (default: public)
            
        Returns:
            str: JSON response with comprehensive schema information
        """
        try:
            logger.info(f"Advanced schema exploration for {schema_name} on connection {connection_id}")
            
            # Get connection info
            conn_info = db_manager.get_connection_info(connection_id)
            if not conn_info:
                return ResponseFormatter.error_response(
                    error="Invalid connection ID",
                    error_code="INVALID_CONNECTION_ID"
                )
            
            db_type = conn_info.get('db_type')
            
            # Build comprehensive schema query
            if db_type == 'postgresql':
                query = f"""
                SELECT 
                    t.table_name,
                    t.table_type,
                    obj_description(c.oid, 'pg_class') as comment,
                    pg_size_pretty(pg_total_relation_size(c.oid)) as size
                FROM information_schema.tables t
                LEFT JOIN pg_class c ON c.relname = t.table_name
                LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE t.table_schema = '{schema_name}'
                ORDER BY t.table_type, t.table_name
                """
                
            elif db_type == 'mysql':
                query = f"""
                SELECT 
                    table_name,
                    table_type,
                    table_comment as comment,
                    ROUND(((data_length + index_length) / 1024 / 1024), 2) as size_mb
                FROM information_schema.tables 
                WHERE table_schema = '{schema_name}'
                ORDER BY table_type, table_name
                """
                
            elif db_type == 'sqlite':
                query = """
                SELECT 
                    name as table_name,
                    type as table_type,
                    sql as definition
                FROM sqlite_master 
                WHERE type IN ('table', 'view')
                ORDER BY type, name
                """
                
            else:
                return ResponseFormatter.error_response(
                    error=f"Advanced schema exploration not supported for {db_type}",
                    error_code="UNSUPPORTED_DB_TYPE"
                )
            
            # Execute main schema query
            result = db_manager.execute_query(connection_id, query)
            
            if result.get('success'):
                tables_info = []
                
                for row in result.get('rows', []):
                    table_name = row.get('table_name')
                    if table_name:
                        # Get column count for each table
                        try:
                            col_count_query = _get_column_count_query(db_type, table_name, schema_name)
                            col_result = db_manager.execute_query(connection_id, col_count_query)
                            
                            if col_result.get('success') and col_result.get('rows'):
                                if db_type == 'sqlite':
                                    column_count = len(col_result.get('rows', []))
                                else:
                                    column_count = col_result.get('rows')[0].get('column_count', 0)
                            else:
                                column_count = 0
                                
                        except Exception as e:
                            logger.warning(f"Could not get column count for {table_name}: {str(e)}")
                            column_count = 0
                        
                        # Get row count for tables (not views)
                        row_count = 0
                        if row.get('table_type', '').lower() == 'table':
                            try:
                                count_query = f"SELECT COUNT(*) as row_count FROM {table_name}"
                                if schema_name and db_type in ['postgresql', 'mysql']:
                                    count_query = f"SELECT COUNT(*) as row_count FROM {schema_name}.{table_name}"
                                
                                count_result = db_manager.execute_query(connection_id, count_query)
                                if count_result.get('success') and count_result.get('rows'):
                                    row_count = count_result.get('rows')[0].get('row_count', 0)
                            except Exception as e:
                                logger.warning(f"Could not get row count for {table_name}: {str(e)}")
                        
                        table_info = dict(row)
                        table_info.update({
                            'column_count': column_count,
                            'row_count': row_count
                        })
                        tables_info.append(table_info)
                
                schema_data = {
                    "schema_name": schema_name,
                    "db_type": db_type,
                    "tables": tables_info,
                    "table_count": len(tables_info),
                    "total_columns": sum(t.get('column_count', 0) for t in tables_info),
                    "total_rows": sum(t.get('row_count', 0) for t in tables_info)
                }
                
                return ResponseFormatter.success_response(
                    data=schema_data,
                    message=f"Advanced schema exploration completed for '{schema_name}'"
                )
            else:
                error_msg = result.get('error', 'Unknown error')
                return ResponseFormatter.error_response(
                    error=f"Failed to explore schema: {error_msg}",
                    error_code="SCHEMA_EXPLORATION_FAILED"
                )
                
        except Exception as e:
            error_msg = f"Error in advanced schema exploration: {str(e)}"
            logger.error(error_msg)
            return ResponseFormatter.error_response(
                error=error_msg,
                error_code="ADVANCED_SCHEMA_ERROR"
            )
    
    @mcp.tool()
    def get_table_relationships(connection_id: str, table_name: str, schema_name: str = None) -> str:
        """
        Get foreign key relationships for a specific table.
        
        Args:
            connection_id: Database connection ID
            table_name: Name of the table
            schema_name: Schema containing the table (optional)
            
        Returns:
            str: JSON response with table relationships
        """
        try:
            logger.info(f"Getting relationships for table {table_name} on connection {connection_id}")
            
            # Get connection info
            conn_info = db_manager.get_connection_info(connection_id)
            if not conn_info:
                return ResponseFormatter.error_response(
                    error="Invalid connection ID",
                    error_code="INVALID_CONNECTION_ID"
                )
            
            db_type = conn_info.get('db_type')
            
            if db_type == 'postgresql':
                schema_clause = f"AND tc.table_schema = '{schema_name}'" if schema_name else "AND tc.table_schema = 'public'"
                query = f"""
                SELECT
                    tc.constraint_name,
                    tc.table_name,
                    kcu.column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name,
                    rc.update_rule,
                    rc.delete_rule
                FROM information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu
                    ON ccu.constraint_name = tc.constraint_name
                    AND ccu.table_schema = tc.table_schema
                LEFT JOIN information_schema.referential_constraints AS rc
                    ON tc.constraint_name = rc.constraint_name
                    AND tc.table_schema = rc.constraint_schema
                WHERE tc.constraint_type = 'FOREIGN KEY'
                    AND tc.table_name = '{table_name}' {schema_clause}
                """
                
            elif db_type == 'mysql':
                schema_clause = f"AND tc.table_schema = '{schema_name}'" if schema_name else f"AND tc.table_schema = '{conn_info.get('database')}'"
                query = f"""
                SELECT
                    tc.constraint_name,
                    tc.table_name,
                    kcu.column_name,
                    kcu.referenced_table_name AS foreign_table_name,
                    kcu.referenced_column_name AS foreign_column_name,
                    rc.update_rule,
                    rc.delete_rule
                FROM information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                LEFT JOIN information_schema.referential_constraints AS rc
                    ON tc.constraint_name = rc.constraint_name
                    AND tc.table_schema = rc.constraint_schema
                WHERE tc.constraint_type = 'FOREIGN KEY'
                    AND tc.table_name = '{table_name}' {schema_clause}
                """
                
            elif db_type == 'sqlite':
                query = f"PRAGMA foreign_key_list({table_name})"
                
            else:
                return ResponseFormatter.error_response(
                    error=f"Relationship discovery not supported for {db_type}",
                    error_code="UNSUPPORTED_DB_TYPE"
                )
            
            # Execute relationships query
            result = db_manager.execute_query(connection_id, query, schema_name)
            
            if result.get('success'):
                relationships = result.get('rows', [])
                
                relationship_data = {
                    "table_name": table_name,
                    "schema_name": schema_name or "default",
                    "db_type": db_type,
                    "foreign_keys": relationships,
                    "relationship_count": len(relationships)
                }
                
                return ResponseFormatter.success_response(
                    data=relationship_data,
                    message=f"Retrieved {len(relationships)} relationships for table '{table_name}'"
                )
            else:
                error_msg = result.get('error', 'Unknown error')
                return ResponseFormatter.error_response(
                    error=f"Failed to retrieve relationships: {error_msg}",
                    error_code="RELATIONSHIPS_QUERY_FAILED"
                )
                
        except Exception as e:
            error_msg = f"Error retrieving table relationships: {str(e)}"
            logger.error(error_msg)
            return ResponseFormatter.error_response(
                error=error_msg,
                error_code="RELATIONSHIPS_ERROR"
            )
    
    def _get_constraints_query(db_type: str, table_name: str, schema_name: str, conn_info: dict) -> str:
        """Generate constraints query based on database type"""
        if db_type == 'postgresql':
            schema_clause = f"AND tc.table_schema = '{schema_name}'" if schema_name else "AND tc.table_schema = 'public'"
            return f"""
            SELECT 
                tc.constraint_name,
                tc.constraint_type,
                kcu.column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.table_name = '{table_name}' {schema_clause}
            """
        elif db_type == 'mysql':
            schema_clause = f"AND tc.table_schema = '{schema_name}'" if schema_name else f"AND tc.table_schema = '{conn_info.get('database')}'"
            return f"""
            SELECT 
                tc.constraint_name,
                tc.constraint_type,
                kcu.column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.table_name = '{table_name}' {schema_clause}
            """
        return ""
    
    def _get_table_stats_query(db_type: str, table_name: str, schema_name: str) -> str:
        """Generate table statistics query based on database type"""
        if db_type == 'postgresql':
            return f"SELECT COUNT(*) as row_count FROM {table_name}"
        elif db_type == 'mysql':
            return f"SELECT COUNT(*) as row_count FROM {table_name}"
        elif db_type == 'sqlite':
            return f"SELECT COUNT(*) as row_count FROM {table_name}"
        return ""
    
    def _get_column_count_query(db_type: str, table_name: str, schema_name: str) -> str:
        """Generate column count query based on database type"""
        if db_type == 'postgresql':
            schema_clause = f"AND table_schema = '{schema_name}'" if schema_name else "AND table_schema = 'public'"
            return f"""
            SELECT COUNT(*) as column_count
            FROM information_schema.columns 
            WHERE table_name = '{table_name}' {schema_clause}
            """
        elif db_type == 'mysql':
            schema_clause = f"AND table_schema = '{schema_name}'" if schema_name else ""
            return f"""
            SELECT COUNT(*) as column_count
            FROM information_schema.columns 
            WHERE table_name = '{table_name}' {schema_clause}
            """
        elif db_type == 'sqlite':
            return f"PRAGMA table_info({table_name})"
        return ""
    
    logger.info("Schema exploration tools registered")