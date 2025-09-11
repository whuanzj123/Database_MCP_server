# Secure Multi-Database MCP Server

A comprehensive, modular Model Context Protocol (MCP) server that provides secure database access to LLMs across multiple database engines. Built with FastMCP for both STDIO and web modes, featuring advanced security, schema exploration, and administrative tools.

## 🚀 Features

### Multi-Database Support
- ✅ **PostgreSQL** - Full support with advanced schema management
- ✅ **MySQL** - Complete implementation with database switching
- ✅ **SQLite** - File-based database support with local access
- ✅ **MongoDB** - Basic collection operations and document queries

### Security First
- 🔒 **Read-only operations** - Only SELECT, SHOW, DESCRIBE, EXPLAIN queries allowed
- 🛡️ **Advanced SQL injection protection** with contextual analysis
- 🚫 **Query validation** - Blocks dangerous operations and file access
- 🔐 **Credential validation** - Comprehensive parameter checking
- 📊 **Security auditing** - Built-in security assessment tools

### Modular Architecture
- 📦 **Connection Tools** - Database connection management
- 🔍 **Query Tools** - Safe query execution with performance analysis
- 🗂️ **Schema Tools** - Comprehensive database exploration
- ⚙️ **Admin Tools** - Monitoring, metrics, and maintenance

### Dual Mode Operation
- 💻 **STDIO Mode** - Direct integration with Claude Desktop
- 🌐 **Web Mode** - HTTP/SSE server for web applications and Chainlit
- 🐳 **Container Mode** - Docker deployment support

## 📁 Project Structure

```
secure_multi_db_mcp/
├─ .env.example                 # Environment configuration template
├─ main.py                      # Main server entry point
├─ requirements.txt             # Python dependencies
├─ README.md                    # This file
├─ database/
│  ├─ __init__.py
│  ├─ config.py                 # Database configuration management
│  └─ connection.py             # Multi-database connection manager
├─ tools/
│  ├─ __init__.py
│  ├─ connection_tools.py       # Connection management tools
│  ├─ query_tools.py           # Query execution tools
│  ├─ schema_tools.py          # Schema exploration tools
│  └─ admin_tools.py           # Administrative and monitoring tools
└─ utils/
   ├─ __init__.py
   ├─ formatters.py            # Response formatting utilities
   ├─ security.py              # Security validation and filtering
   └─ validators.py            # Input validation utilities
```

## 🛠 Installation

### Prerequisites

- Python 3.8 or higher
- Database drivers (install as needed):

```bash
# PostgreSQL
pip install psycopg2-binary

# MySQL
pip install mysql-connector-python

# MongoDB
pip install pymongo

# SQLite is included with Python
```

### Quick Setup

1. **Clone and install dependencies:**
```bash
git clone <repository>
cd secure_multi_db_mcp
pip install -r requirements.txt
```

2. **Configure environment:**
```bash
cp .env.example .env
# Edit .env with your settings
```

3. **Run the server:**
```bash
# STDIO mode (Claude Desktop)
python main.py

# Web mode (Chainlit/web interfaces)
python main.py --web

# Custom port
python main.py --web 8080
```

## ⚙️ Configuration

### Environment Variables

Key configuration options in `.env`:

```bash
# Server settings
MCP_PORT=8001
DB_MAX_CONNECTIONS=10
DB_CONNECTION_TIMEOUT=30

# Security settings
MAX_QUERY_LENGTH=10000
ALLOW_INFORMATION_SCHEMA=true

# Logging
LOG_LEVEL=INFO
LOG_QUERIES=true
```

### MCP Client Configuration

**For Claude Desktop (`claude_desktop_config.json`):**
```json
{
  "mcpServers": {
    "secure-multi-db": {
      "command": "python",
      "args": ["/path/to/main.py"],
      "env": {
        "LOG_LEVEL": "INFO"
      }
    }
  }
}
```

**For Web Mode:**
```json
{
  "mcpServers": {
    "secure-multi-db-web": {
      "command": "python",
      "args": ["/path/to/main.py", "--web", "8001"]
    }
  }
}
```

## 🔧 Available Tools

### Connection Management

#### `connect_database`
Establish database connections with credentials.

```python
connect_database(
    db_type="postgresql",
    host="localhost",
    port=5432,
    username="user",
    password="password",
    database="mydb"
)
```

#### `disconnect_database`
Clean up database connections.

#### `list_connections`
View all active database connections.

#### `test_connection`
Verify connection health and status.

### Query Execution

#### `execute_query`
Execute safe SELECT queries with automatic limits.

```python
execute_query(
    connection_id="abc123...",
    query="SELECT * FROM users WHERE active = true",
    schema="public",
    limit=100
)
```

#### `validate_query`
Check query safety before execution.

#### `explain_query`
Get query execution plans without running queries.

#### `execute_batch_queries`
Run multiple queries in sequence with error handling.

### Schema Exploration

#### `get_schema_info`
Explore database schemas and tables.

#### `get_table_info`
Get detailed table structure and metadata.

#### `explore_schema_advanced`
Comprehensive schema analysis with relationships.

#### `get_table_relationships`
Discover foreign key relationships.

### Administrative Tools

#### `get_database_status`
Comprehensive system and database status.

#### `get_connection_metrics`
Performance metrics and usage statistics.

#### `cleanup_idle_connections`
Remove stale connections automatically.

#### `get_security_audit`
Security assessment and recommendations.

#### `health_check`
Complete system health validation.

## 💡 Usage Examples

### Example 1: PostgreSQL Analysis

```python
# Connect to PostgreSQL
response = connect_database(
    db_type="postgresql",
    host="db.company.com",
    port=5432,
    username="analyst",
    password="secret123",
    database="analytics"
)

# Explore schema
schema_info = get_schema_info(connection_id="abc123...")

# Query data
results = execute_query(
    connection_id="abc123...",
    query="SELECT category, COUNT(*) FROM products GROUP BY category"
)
```

### Example 2: Multi-Database Comparison

```python
# Connect to multiple databases
mysql_conn = connect_database(
    db_type="mysql",
    host="prod-db.company.com",
    username="readonly",
    database="ecommerce"
)

postgres_conn = connect_database(
    db_type="postgresql", 
    host="analytics-db.company.com",
    username="analyst",
    database="warehouse"
)

# Compare data across databases
mysql_data = execute_query(mysql_conn, "SELECT COUNT(*) as orders FROM orders")
postgres_data = execute_query(postgres_conn, "SELECT COUNT(*) as events FROM user_events")
```

### Example 3: Schema Exploration

```python
# Advanced schema exploration
schema_details = explore_schema_advanced(
    connection_id="abc123...",
    schema_name="public"
)

# Table relationships
relationships = get_table_relationships(
    connection_id="abc123...",
    table_name="orders",
    schema_name="public"
)
```

## 🔒 Security Features

### Query Filtering
- **Whitelist approach** - Only allow safe operations
- **Pattern matching** - Advanced regex-based filtering
- **Contextual analysis** - Intelligent metadata access validation
- **Injection protection** - Multiple layers of SQL injection prevention

### Connection Security
- **Credential validation** - Comprehensive parameter checking
- **Connection limits** - Prevent resource exhaustion
- **Timeout management** - Automatic cleanup of stale connections
- **Audit logging** - Track all database operations

### Security Monitoring
- **Real-time validation** - Every query checked before execution
- **Security scoring** - Automated security assessment
- **Threat detection** - Pattern-based attack identification
- **Compliance reporting** - Detailed security audit trails

## 📊 Monitoring and Administration

### Performance Monitoring
- Connection usage metrics
- Query execution statistics
- Resource utilization tracking
- Performance bottleneck identification

### Health Checks
- Database connectivity validation
- System resource monitoring
- Security compliance verification
- Automated issue detection

### Administrative Features
- Connection lifecycle management
- Automated cleanup procedures
- Configuration export/import
- Comprehensive logging system

## 🚀 Deployment Options

### Development Mode
```bash
python main.py
# Runs in STDIO mode for Claude Desktop
```

### Web Application Mode
```bash
python main.py --web 8001
# Starts HTTP/SSE server for web integration
```

### Container Deployment
```bash
# Using Docker
docker build -t secure-multi-db-mcp .
docker run -p 8001:8000 -e MCP_MODE=web secure-multi-db-mcp

# Using docker-compose
docker-compose up -d
```

### Production Deployment
```bash
# With environment configuration
export ENVIRONMENT=production
export LOG_LEVEL=WARNING
export DB_MAX_CONNECTIONS=20
python main.py --web 8001
```

## 🔧 Development

### Adding New Database Types

1. **Update configuration** in `database/config.py`:
```python
REQUIRED_CREDENTIALS = {
    "newdb": ["username", "password", "database"]
}
```

2. **Implement connection logic** in `database/connection.py`:
```python
def _connect_newdb(self, host, port, credentials):
    # Implementation here
    pass
```

3. **Add query execution** support:
```python
def _execute_newdb_query(self, conn_info, query):
    # Implementation here
    pass
```

### Extending Security Rules

Modify `utils/security.py` to add custom validation:

```python
class SecurityValidator:
    def _custom_validation(self, query):
        # Add custom security logic
        return is_safe
```

### Adding New Tools

Create tools in appropriate modules:

```python
@mcp.tool()
def my_custom_tool(parameter: str) -> str:
    """Custom tool description"""
    try:
        # Implementation
        return ResponseFormatter.success_response(data, message)
    except Exception as e:
        return ResponseFormatter.error_response(str(e))
```

## 🐛 Troubleshooting

### Common Issues

1. **Connection Failed**
   - Check database host and port
   - Verify credentials
   - Ensure database is running
   - Check firewall settings

2. **Query Blocked**
   - Verify query uses only SELECT operations
   - Check for dangerous patterns
   - Use `validate_query` tool to diagnose

3. **Driver Not Available**
   - Install appropriate database driver
   - Check Python environment
   - Verify import statements

4. **Permission Denied**
   - Check database user permissions
   - Verify schema access rights
   - Review connection credentials

### Debug Mode

Enable detailed logging:
```bash
export LOG_LEVEL=DEBUG
python main.py
```

### Health Check

Use built-in diagnostics:
```python
health_status = health_check()
security_audit = get_security_audit()
performance_report = get_performance_report()
```

## 📋 API Reference

### Response Format

All tools return JSON responses with consistent structure:

```json
{
  "success": true,
  "message": "Operation successful", 
  "timestamp": "2025-01-16T10:30:00Z",
  "data": {
    // Tool-specific data
  },
  "metadata": {
    // Additional metadata
  }
}
```

### Error Handling

Error responses include detailed information:

```json
{
  "success": false,
  "error": "Detailed error message",
  "error_code": "ERROR_CODE",
  "timestamp": "2025-01-16T10:30:00Z",
  "details": {
    // Additional error details
  }
}
```

## 🤝 Contributing

1. **Fork the repository**
2. **Create feature branch**: `git checkout -b feature/amazing-feature`
3. **Add tests** for new functionality
4. **Ensure security compliance**
5. **Submit pull request**

### Code Standards
- Follow PEP 8 style guidelines
- Add comprehensive docstrings
- Include error handling
- Write unit tests
- Update documentation

### Security Guidelines
- Never expose sensitive data
- Validate all inputs
- Follow principle of least privilege
- Log security events
- Test for SQL injection

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Built with [FastMCP](https://github.com/jlowin/fastmcp)
- Inspired by secure database access patterns
- Community feedback and contributions
- Security best practices from OWASP

## 📞 Support

- **Documentation**: Check this README and inline comments
- **Issues**: Create GitHub issues for bugs
- **Security**: Report security issues privately
- **Community**: Join discussions and share feedback

## 🗺️ Roadmap

### Version 1.1
- [ ] Query result caching
- [ ] Connection pooling
- [ ] Advanced metrics dashboard
- [ ] Custom query templates

### Version 1.2
- [ ] GraphQL support
- [ ] Real-time query monitoring
- [ ] Advanced security analytics
- [ ] Multi-tenant support

### Version 2.0
- [ ] Distributed deployment
- [ ] Advanced caching layer
- [ ] Machine learning query optimization
- [ ] Enterprise SSO integration

---

**⭐ Star this project if you find it useful!**

**🔐 Security Notice**: This server implements read-only database access with comprehensive security filtering. Always review queries and maintain proper database permissions.