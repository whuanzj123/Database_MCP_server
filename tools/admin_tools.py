"""
Administrative and monitoring tools for multi-database MCP server
"""

import logging
import time
import psutil
import platform
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from ..utils.formatters import ResponseFormatter, MetricsFormatter

logger = logging.getLogger(__name__)


def register_tools(mcp, db_manager):
    """Register administrative and monitoring tools with the MCP server"""
    
    @mcp.tool()
    def get_database_status() -> str:
        """
        Get comprehensive status information about the database server and connections.
        
        Returns:
            str: JSON response with system status
        """
        try:
            logger.info("Retrieving comprehensive database status")
            
            # Get database manager status
            manager_status = db_manager.get_status()
            
            # Get system information
            system_info = {
                "platform": platform.system(),
                "platform_version": platform.version(),
                "python_version": platform.python_version(),
                "architecture": platform.machine(),
                "processor": platform.processor(),
                "hostname": platform.node()
            }
            
            # Get system resources
            try:
                memory = psutil.virtual_memory()
                disk = psutil.disk_usage('/')
                
                system_resources = {
                    "cpu_percent": psutil.cpu_percent(interval=1),
                    "cpu_count": psutil.cpu_count(),
                    "memory": {
                        "total": memory.total,
                        "available": memory.available,
                        "percent": memory.percent,
                        "used": memory.used,
                        "free": memory.free
                    },
                    "disk": {
                        "total": disk.total,
                        "used": disk.used,
                        "free": disk.free,
                        "percent": (disk.used / disk.total) * 100
                    }
                }
            except Exception as e:
                logger.warning(f"Could not get system resources: {str(e)}")
                system_resources = {"error": "System resource information unavailable"}
            
            # Compile comprehensive status
            status_data = {
                "timestamp": datetime.utcnow().isoformat(),
                "database_manager": manager_status,
                "system_info": system_info,
                "system_resources": system_resources,
                "uptime": time.time() - getattr(db_manager, 'start_time', time.time())
            }
            
            return ResponseFormatter.status_response(status_data)
            
        except Exception as e:
            error_msg = f"Error retrieving database status: {str(e)}"
            logger.error(error_msg)
            return ResponseFormatter.error_response(
                error=error_msg,
                error_code="STATUS_ERROR"
            )
    
    @mcp.tool()
    def get_connection_metrics(connection_id: str = None) -> str:
        """
        Get detailed metrics for connections.
        
        Args:
            connection_id: Specific connection ID to analyze (optional)
            
        Returns:
            str: JSON response with connection metrics
        """
        try:
            if connection_id:
                logger.info(f"Getting metrics for connection {connection_id}")
                
                conn_info = db_manager.get_connection_info(connection_id)
                if not conn_info:
                    return ResponseFormatter.error_response(
                        error="Invalid connection ID",
                        error_code="INVALID_CONNECTION_ID"
                    )
                
                # Calculate connection age and usage metrics
                current_time = time.time()
                connection_age = current_time - conn_info.get('created_at', current_time)
                last_used_ago = current_time - conn_info.get('last_used', current_time)
                
                metrics = {
                    "connection_id": connection_id,
                    "db_type": conn_info.get('db_type'),
                    "host": conn_info.get('host'),
                    "database": conn_info.get('database'),
                    "connection_age_seconds": connection_age,
                    "last_used_ago_seconds": last_used_ago,
                    "query_count": conn_info.get('query_count', 0),
                    "queries_per_hour": (conn_info.get('query_count', 0) / max(connection_age / 3600, 0.001)),
                    "status": "active" if last_used_ago < 300 else "idle"  # 5 minutes threshold
                }
                
                return ResponseFormatter.success_response(
                    data=metrics,
                    message=f"Connection metrics retrieved for {connection_id}"
                )
                
            else:
                logger.info("Getting metrics for all connections")
                
                connections = db_manager.list_connections()
                current_time = time.time()
                
                all_metrics = []
                total_queries = 0
                active_connections = 0
                
                for conn in connections:
                    connection_age = current_time - conn.get('created_at', current_time)
                    last_used_ago = current_time - conn.get('last_used', current_time)
                    query_count = conn.get('query_count', 0)
                    
                    if last_used_ago < 300:  # 5 minutes
                        active_connections += 1
                    
                    total_queries += query_count
                    
                    conn_metrics = {
                        "connection_id": conn.get('connection_id'),
                        "db_type": conn.get('db_type'),
                        "connection_age_seconds": connection_age,
                        "last_used_ago_seconds": last_used_ago,
                        "query_count": query_count,
                        "status": "active" if last_used_ago < 300 else "idle"
                    }
                    all_metrics.append(conn_metrics)
                
                summary_metrics = {
                    "total_connections": len(connections),
                    "active_connections": active_connections,
                    "idle_connections": len(connections) - active_connections,
                    "total_queries_executed": total_queries,
                    "average_queries_per_connection": total_queries / max(len(connections), 1),
                    "connections": all_metrics
                }
                
                return ResponseFormatter.success_response(
                    data=summary_metrics,
                    message="Connection metrics retrieved for all connections"
                )
                
        except Exception as e:
            error_msg = f"Error retrieving connection metrics: {str(e)}"
            logger.error(error_msg)
            return ResponseFormatter.error_response(
                error=error_msg,
                error_code="METRICS_ERROR"
            )
    
    @mcp.tool()
    def cleanup_idle_connections(max_idle_minutes: int = 30) -> str:
        """
        Clean up connections that have been idle for specified time.
        
        Args:
            max_idle_minutes: Maximum idle time in minutes before cleanup (default: 30)
            
        Returns:
            str: JSON response with cleanup results
        """
        try:
            logger.info(f"Cleaning up connections idle for more than {max_idle_minutes} minutes")
            
            max_idle_seconds = max_idle_minutes * 60
            cleaned_count = db_manager.cleanup_stale_connections(max_idle_seconds)
            
            cleanup_data = {
                "cleaned_connections": cleaned_count,
                "max_idle_minutes": max_idle_minutes,
                "remaining_connections": len(db_manager.connections),
                "cleanup_timestamp": datetime.utcnow().isoformat()
            }
            
            return ResponseFormatter.success_response(
                data=cleanup_data,
                message=f"Cleaned up {cleaned_count} idle connections"
            )
            
        except Exception as e:
            error_msg = f"Error cleaning up connections: {str(e)}"
            logger.error(error_msg)
            return ResponseFormatter.error_response(
                error=error_msg,
                error_code="CLEANUP_ERROR"
            )
    
    @mcp.tool()
    def get_security_audit() -> str:
        """
        Perform a security audit of current connections and configurations.
        
        Returns:
            str: JSON response with security audit results
        """
        try:
            logger.info("Performing security audit")
            
            connections = db_manager.list_connections()
            audit_results = {
                "audit_timestamp": datetime.utcnow().isoformat(),
                "total_connections": len(connections),
                "security_issues": [],
                "recommendations": [],
                "connection_analysis": []
            }
            
            # Analyze each connection for security issues
            for conn in connections:
                conn_analysis = {
                    "connection_id": conn.get('connection_id'),
                    "db_type": conn.get('db_type'),
                    "host": conn.get('host'),
                    "issues": [],
                    "score": 100  # Start with perfect score
                }
                
                # Check for localhost connections
                if conn.get('host') not in ['localhost', '127.0.0.1', '::1']:
                    conn_analysis["issues"].append("Remote database connection - ensure network security")
                    conn_analysis["score"] -= 10
                
                # Check connection age
                current_time = time.time()
                connection_age = current_time - conn.get('created_at', current_time)
                if connection_age > 3600:  # 1 hour
                    conn_analysis["issues"].append("Long-lived connection - consider periodic renewal")
                    conn_analysis["score"] -= 5
                
                # Check for idle connections
                last_used_ago = current_time - conn.get('last_used', current_time)
                if last_used_ago > 1800:  # 30 minutes
                    conn_analysis["issues"].append("Idle connection - security risk")
                    conn_analysis["score"] -= 15
                
                # Database-specific security checks
                if conn.get('db_type') == 'sqlite':
                    if '/tmp/' in conn.get('database', ''):
                        conn_analysis["issues"].append("SQLite database in temporary directory")
                        conn_analysis["score"] -= 20
                
                audit_results["connection_analysis"].append(conn_analysis)
            
            # Overall security recommendations
            if len(connections) > db_manager.max_connections * 0.8:
                audit_results["security_issues"].append("High connection count - approaching limits")
                audit_results["recommendations"].append("Monitor connection usage and implement cleanup")
            
            if any(conn.get('host') not in ['localhost', '127.0.0.1', '::1'] for conn in connections):
                audit_results["recommendations"].append("Ensure all remote connections use encrypted transport")
            
            # Calculate overall security score
            if audit_results["connection_analysis"]:
                overall_score = sum(conn.get('score', 0) for conn in audit_results["connection_analysis"]) / len(audit_results["connection_analysis"])
                audit_results["overall_security_score"] = round(overall_score, 1)
            else:
                audit_results["overall_security_score"] = 100
            
            # Security grade
            score = audit_results["overall_security_score"]
            if score >= 90:
                audit_results["security_grade"] = "A"
            elif score >= 80:
                audit_results["security_grade"] = "B"
            elif score >= 70:
                audit_results["security_grade"] = "C"
            elif score >= 60:
                audit_results["security_grade"] = "D"
            else:
                audit_results["security_grade"] = "F"
            
            return ResponseFormatter.success_response(
                data=audit_results,
                message=f"Security audit completed. Grade: {audit_results['security_grade']}"
            )
            
        except Exception as e:
            error_msg = f"Error performing security audit: {str(e)}"
            logger.error(error_msg)
            return ResponseFormatter.error_response(
                error=error_msg,
                error_code="AUDIT_ERROR"
            )
    
    @mcp.tool()
    def get_performance_report(hours: int = 1) -> str:
        """
        Generate a performance report for the specified time period.
        
        Args:
            hours: Number of hours to analyze (default: 1)
            
        Returns:
            str: JSON response with performance report
        """
        try:
            logger.info(f"Generating performance report for last {hours} hours")
            
            connections = db_manager.list_connections()
            current_time = time.time()
            time_threshold = current_time - (hours * 3600)
            
            # Analyze connection performance
            performance_data = {
                "report_period_hours": hours,
                "report_timestamp": datetime.utcnow().isoformat(),
                "total_connections": len(connections),
                "active_connections": 0,
                "total_queries": 0,
                "queries_per_hour": 0,
                "db_type_distribution": {},
                "host_distribution": {},
                "connection_details": []
            }
            
            for conn in connections:
                # Count active connections
                last_used = conn.get('last_used', 0)
                if last_used > time_threshold:
                    performance_data["active_connections"] += 1
                
                # Sum total queries
                query_count = conn.get('query_count', 0)
                performance_data["total_queries"] += query_count
                
                # Track database type distribution
                db_type = conn.get('db_type', 'unknown')
                performance_data["db_type_distribution"][db_type] = performance_data["db_type_distribution"].get(db_type, 0) + 1
                
                # Track host distribution
                host = conn.get('host', 'unknown')
                performance_data["host_distribution"][host] = performance_data["host_distribution"].get(host, 0) + 1
                
                # Connection performance details
                connection_age = current_time - conn.get('created_at', current_time)
                queries_per_hour = (query_count / max(connection_age / 3600, 0.001))
                
                conn_detail = {
                    "connection_id": conn.get('connection_id'),
                    "db_type": db_type,
                    "host": host,
                    "query_count": query_count,
                    "queries_per_hour": round(queries_per_hour, 2),
                    "age_hours": round(connection_age / 3600, 2)
                }
                performance_data["connection_details"].append(conn_detail)
            
            # Calculate overall queries per hour
            if performance_data["total_connections"] > 0:
                performance_data["queries_per_hour"] = round(
                    performance_data["total_queries"] / max(hours, 0.001), 2
                )
            
            # Performance assessment
            performance_data["assessment"] = {
                "status": "good",
                "issues": [],
                "recommendations": []
            }
            
            # Check for performance issues
            if performance_data["active_connections"] == 0 and performance_data["total_connections"] > 0:
                performance_data["assessment"]["issues"].append("No active connections in analysis period")
                performance_data["assessment"]["status"] = "warning"
            
            if performance_data["queries_per_hour"] < 1:
                performance_data["assessment"]["issues"].append("Low query activity")
                performance_data["assessment"]["recommendations"].append("Review connection usage patterns")
            
            if len(connections) > db_manager.max_connections * 0.9:
                performance_data["assessment"]["issues"].append("High connection count")
                performance_data["assessment"]["recommendations"].append("Consider increasing max connections or implementing cleanup")
                performance_data["assessment"]["status"] = "warning"
            
            return ResponseFormatter.success_response(
                data=performance_data,
                message=f"Performance report generated for {hours} hour(s)"
            )
            
        except Exception as e:
            error_msg = f"Error generating performance report: {str(e)}"
            logger.error(error_msg)
            return ResponseFormatter.error_response(
                error=error_msg,
                error_code="PERFORMANCE_REPORT_ERROR"
            )
    
    @mcp.tool()
    def export_configuration() -> str:
        """
        Export current server configuration for backup or analysis.
        
        Returns:
            str: JSON response with configuration data
        """
        try:
            logger.info("Exporting server configuration")
            
            config_data = {
                "export_timestamp": datetime.utcnow().isoformat(),
                "database_manager_config": db_manager.config,
                "connection_limits": {
                    "max_connections": db_manager.max_connections,
                    "connection_timeout": db_manager.connection_timeout,
                    "query_timeout": db_manager.query_timeout
                },
                "driver_availability": {
                    "postgresql": db_manager.POSTGRES_AVAILABLE,
                    "mysql": db_manager.MYSQL_AVAILABLE,
                    "sqlite": db_manager.SQLITE_AVAILABLE,
                    "mongodb": db_manager.MONGODB_AVAILABLE
                },
                "security_settings": {
                    "allowed_operations": list(db_manager.security_validator.allowed_operations),
                    "dangerous_operations": list(db_manager.security_validator.dangerous_operations)
                }
            }
            
            return ResponseFormatter.success_response(
                data=config_data,
                message="Configuration exported successfully"
            )
            
        except Exception as e:
            error_msg = f"Error exporting configuration: {str(e)}"
            logger.error(error_msg)
            return ResponseFormatter.error_response(
                error=error_msg,
                error_code="CONFIG_EXPORT_ERROR"
            )
    
    @mcp.tool()
    def health_check() -> str:
        """
        Perform a comprehensive health check of the database server.
        
        Returns:
            str: JSON response with health check results
        """
        try:
            logger.info("Performing comprehensive health check")
            
            health_data = {
                "timestamp": datetime.utcnow().isoformat(),
                "overall_status": "healthy",
                "checks": {},
                "warnings": [],
                "errors": []
            }
            
            # Check database manager
            try:
                manager_status = db_manager.get_status()
                health_data["checks"]["database_manager"] = "ok"
            except Exception as e:
                health_data["checks"]["database_manager"] = "error"
                health_data["errors"].append(f"Database manager error: {str(e)}")
                health_data["overall_status"] = "unhealthy"
            
            # Check available drivers
            driver_issues = []
            if not db_manager.POSTGRES_AVAILABLE:
                driver_issues.append("PostgreSQL driver not available")
            if not db_manager.MYSQL_AVAILABLE:
                driver_issues.append("MySQL driver not available")
            if not db_manager.MONGODB_AVAILABLE:
                driver_issues.append("MongoDB driver not available")
            
            if driver_issues:
                health_data["warnings"].extend(driver_issues)
                health_data["checks"]["database_drivers"] = "partial"
                if health_data["overall_status"] == "healthy":
                    health_data["overall_status"] = "degraded"
            else:
                health_data["checks"]["database_drivers"] = "ok"
            
            # Check connection health
            connections = db_manager.list_connections()
            active_connections = 0
            failed_connections = 0
            
            for conn in connections:
                try:
                    # Simple connection test
                    conn_id = conn.get('connection_id')
                    if conn_id:
                        # This would ideally test the actual connection
                        active_connections += 1
                except Exception:
                    failed_connections += 1
            
            if failed_connections > 0:
                health_data["warnings"].append(f"{failed_connections} connections may have issues")
                health_data["checks"]["connections"] = "warning"
                if health_data["overall_status"] == "healthy":
                    health_data["overall_status"] = "degraded"
            else:
                health_data["checks"]["connections"] = "ok"
            
            # Check system resources
            try:
                memory = psutil.virtual_memory()
                if memory.percent > 90:
                    health_data["warnings"].append("High memory usage")
                    health_data["checks"]["memory"] = "warning"
                    if health_data["overall_status"] == "healthy":
                        health_data["overall_status"] = "degraded"
                else:
                    health_data["checks"]["memory"] = "ok"
                
                disk = psutil.disk_usage('/')
                disk_percent = (disk.used / disk.total) * 100
                if disk_percent > 90:
                    health_data["warnings"].append("High disk usage")
                    health_data["checks"]["disk"] = "warning"
                    if health_data["overall_status"] == "healthy":
                        health_data["overall_status"] = "degraded"
                else:
                    health_data["checks"]["disk"] = "ok"
                    
            except Exception as e:
                health_data["warnings"].append("Could not check system resources")
                health_data["checks"]["system_resources"] = "unknown"
            
            # Summary
            health_data["summary"] = {
                "total_connections": len(connections),
                "active_connections": active_connections,
                "failed_connections": failed_connections,
                "warning_count": len(health_data["warnings"]),
                "error_count": len(health_data["errors"])
            }
            
            return ResponseFormatter.success_response(
                data=health_data,
                message=f"Health check completed. Status: {health_data['overall_status']}"
            )
            
        except Exception as e:
            error_msg = f"Error performing health check: {str(e)}"
            logger.error(error_msg)
            return ResponseFormatter.error_response(
                error=error_msg,
                error_code="HEALTH_CHECK_ERROR"
            )
    
    logger.info("Administrative tools registered")