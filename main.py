# ===== main.py =====
import logging
import os
import sys
import platform
from pathlib import Path
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
from starlette.applications import Starlette
from starlette.routing import Route, Mount
from starlette.middleware.cors import CORSMiddleware

# Import tool modules
from tools import connection_tools, query_tools, schema_tools, admin_tools
from database.connection import MultiDatabaseManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("secure-multi-db-mcp")

# Load environment variables
load_dotenv()

# Create MCP server
mcp = FastMCP("secure-multi-db-mcp",
              message_path="/mcp/messages/",
              sse_path="/mcp/sse")

# Initialize database manager
db_manager = MultiDatabaseManager()

def register_all_tools():
    """Register tools from all modules"""
    logger.info("Registering all MCP tools...")
    
    # Connection management tools
    connection_tools.register_tools(mcp, db_manager)
    
    # Query execution tools
    query_tools.register_tools(mcp, db_manager)
    
    # Schema exploration tools
    schema_tools.register_tools(mcp, db_manager)
    
    # Administrative tools
    admin_tools.register_tools(mcp, db_manager)
    
    logger.info("All tools registered successfully")

async def health_check(request):
    """Health check endpoint"""
    return {
        "status": "Multi-Database MCP server is working",
        "supported_databases": {
            "postgresql": "Available" if hasattr(db_manager, 'POSTGRES_AVAILABLE') and db_manager.POSTGRES_AVAILABLE else "Not Available",
            "mysql": "Available" if hasattr(db_manager, 'MYSQL_AVAILABLE') and db_manager.MYSQL_AVAILABLE else "Not Available", 
            "sqlite": "Available",
            "mongodb": "Available" if hasattr(db_manager, 'MONGODB_AVAILABLE') and db_manager.MONGODB_AVAILABLE else "Not Available"
        },
        "active_connections": len(db_manager.connections),
        "max_connections": db_manager.max_connections,
        "modules": [
            "connection_tools",
            "query_tools",
            "schema_tools", 
            "admin_tools"
        ],
        "endpoints": {
            "mcp_sse": "/mcp/sse",
            "mcp_messages": "/mcp/messages/",
            "health": "/health"
        }
    }

def create_app():
    """Create Starlette application for web mode"""
    routes = [
        Route('/health', endpoint=health_check, methods=["GET"]),
        Mount('/', app=mcp.sse_app())
    ]
    
    app = Starlette(routes=routes)
    
    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Configure appropriately for production
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    return app

if __name__ == "__main__":
    print("🚀 Starting Secure Multi-Database MCP Server")
    print(f"Platform: {platform.system()}")
    
    # Register all tools
    register_all_tools()
    
    # Log available database drivers
    logger.info("Available database drivers:")
    try:
        import psycopg2
        logger.info("  ✅ PostgreSQL: Available")
    except ImportError:
        logger.info("  ❌ PostgreSQL: Not available")
        
    try:
        import mysql.connector
        logger.info("  ✅ MySQL: Available")
    except ImportError:
        logger.info("  ❌ MySQL: Not available")
        
    logger.info("  ✅ SQLite: Available (built-in)")
    
    try:
        import pymongo
        logger.info("  ✅ MongoDB: Available")
    except ImportError:
        logger.info("  ❌ MongoDB: Not available")
    
    # Determine run mode
    if platform.system() != "Windows":
        # Linux container mode
        import uvicorn
        app = create_app()
        port = int(os.environ.get("MCP_PORT", 8000))
        logger.info(f"Starting Multi-Database MCP server in container mode on port {port}")
        uvicorn.run(app, host="0.0.0.0", port=port)
        
    elif len(sys.argv) > 1 and sys.argv[1] == "--web":
        # Web mode
        import uvicorn
        app = create_app()
        port = int(sys.argv[2]) if len(sys.argv) > 2 else int(os.environ.get("MCP_PORT", 8001))
        logger.info(f"Starting Multi-Database MCP server in web mode on port {port}")
        uvicorn.run(app, host="127.0.0.1", port=port)
        
    else:
        # STDIO mode for Claude Desktop
        logger.info("Starting STDIO mode for Claude Desktop")
        mcp.run(transport='stdio')