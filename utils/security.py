"""
Security validation module for multi-database MCP server
"""

import re
import logging
from typing import Set, List, Pattern

logger = logging.getLogger(__name__)


class SecurityValidator:
    """Validates SQL queries for security and safety"""
    
    def __init__(self):
        # Allowed operations (read-only)
        self.allowed_operations: Set[str] = {
            'SELECT', 'SHOW', 'DESCRIBE', 'DESC', 'EXPLAIN', 'WITH', 'PRAGMA'
        }
        
        # Dangerous operations that should be blocked
        self.dangerous_operations: Set[str] = {
            'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 
            'TRUNCATE', 'REPLACE', 'MERGE', 'CALL', 'EXEC', 'GRANT', 
            'REVOKE', 'SET', 'DECLARE'
        }
        
        # Compiled regex patterns for performance
        self._compile_patterns()
    
    def _compile_patterns(self) -> None:
        """Compile regex patterns for better performance"""
        
        # Dangerous operations that might appear anywhere in query
        self.dangerous_ops_patterns: List[Pattern] = [
            re.compile(rf'\b{op}\b', re.IGNORECASE) 
            for op in self.dangerous_operations
        ]
        
        # Security patterns for injection and dangerous functions
        self.security_patterns: List[Pattern] = [
            # User/database management
            re.compile(r'\bdrop\s+database\b', re.IGNORECASE),
            re.compile(r'\bdrop\s+user\b', re.IGNORECASE),
            re.compile(r'\bcreate\s+user\b', re.IGNORECASE),
            re.compile(r'\balter\s+user\b', re.IGNORECASE),
            
            # Privilege operations
            re.compile(r'\bgrant\b', re.IGNORECASE),
            re.compile(r'\brevoke\b', re.IGNORECASE),
            
            # Delay/sleep functions
            re.compile(r'\bpg_sleep\b', re.IGNORECASE),
            re.compile(r'\bsleep\b', re.IGNORECASE),
            re.compile(r'\bwaitfor\b', re.IGNORECASE),
            re.compile(r'\bdelay\b', re.IGNORECASE),
            
            # Multiple statements
            re.compile(r';\s*\w+', re.IGNORECASE),
            
            # SQL comments
            re.compile(r'--\s*[^\r\n]*', re.IGNORECASE),
            re.compile(r'/\*.*?\*/', re.IGNORECASE | re.DOTALL),
            
            # UNION-based injection patterns
            re.compile(r'\bunion\s+all\s+select.*\bfrom\s+information_schema\b', re.IGNORECASE),
            re.compile(r'\bunion\s+select.*\bpassword\b', re.IGNORECASE),
            re.compile(r'\bunion\s+select.*\buser\b.*\bfrom\b', re.IGNORECASE),
            
            # System function calls
            re.compile(r'\bxp_cmdshell\b', re.IGNORECASE),
            re.compile(r'\bsp_executesql\b', re.IGNORECASE),
            re.compile(r'\bdbms_output\b', re.IGNORECASE),
            re.compile(r'\bsys\._eval\b', re.IGNORECASE),
            
            # File operations
            re.compile(r'\binto\s+outfile\b', re.IGNORECASE),
            re.compile(r'\binto\s+dumpfile\b', re.IGNORECASE),
            re.compile(r'\bload_file\b', re.IGNORECASE),
            re.compile(r'\bselect.*into.*from\b', re.IGNORECASE),
            
            # Network operations
            re.compile(r'\bmaster\.\.xp_', re.IGNORECASE),
            re.compile(r'\bopenrowset\b', re.IGNORECASE),
            re.compile(r'\bopendatasource\b', re.IGNORECASE),
        ]
        
        # Patterns for legitimate metadata access
        self.metadata_patterns: List[Pattern] = [
            re.compile(r'\binformation_schema\b', re.IGNORECASE),
            re.compile(r'\bpg_catalog\b', re.IGNORECASE),
            re.compile(r'\bpg_tables\b', re.IGNORECASE),
            re.compile(r'\bpg_class\b', re.IGNORECASE),
            re.compile(r'\bpg_namespace\b', re.IGNORECASE),
            re.compile(r'\bpg_attribute\b', re.IGNORECASE),
            re.compile(r'\bpg_stats\b', re.IGNORECASE),
            re.compile(r'\bsqlite_master\b', re.IGNORECASE),
            re.compile(r'\bmysql\b', re.IGNORECASE),
            re.compile(r'\bperformance_schema\b', re.IGNORECASE),
        ]
    
    def is_safe_query(self, query: str) -> bool:
        """
        Check if query is safe (read-only operations) with improved pattern matching.
        
        Args:
            query: SQL query to validate
            
        Returns:
            bool: True if query is safe, False otherwise
        """
        if not query or not query.strip():
            return False
        
        query_upper = query.strip().upper()
        
        # First check: Must start with allowed read-only operations
        if not any(query_upper.startswith(op) for op in self.allowed_operations):
            logger.warning(f"Query doesn't start with allowed operation: {query[:50]}...")
            return False
        
        # Check query length
        if len(query) > 10000:  # Configurable limit
            logger.warning("Query too long, potential DoS attempt")
            return False
        
        # Priority check: File operations (highest security risk)
        if self._contains_file_operations(query_upper):
            logger.warning("Query contains file operations - blocked")
            return False
        
        # Check for dangerous operations anywhere in query
        if self._contains_dangerous_operations(query_upper):
            return False
        
        # Check for SQL injection patterns
        if self._contains_injection_patterns(query_upper):
            return False
        
        # Special handling for metadata schema access
        if self._contains_metadata_access(query_upper):
            return self._validate_metadata_access(query_upper, query)
        
        # Check for suspicious nested queries
        if self._has_excessive_nesting(query_upper):
            logger.warning("Query has excessive nesting - potential complexity attack")
            return False
        
        return True
    
    def _contains_file_operations(self, query_upper: str) -> bool:
        """Check for file operation patterns"""
        file_patterns = [
            r'\binto\s+outfile\b',
            r'\binto\s+dumpfile\b', 
            r'\bload_file\b',
            r'\bselect.*into.*from\b'
        ]
        
        for pattern in file_patterns:
            if re.search(pattern, query_upper):
                return True
        return False
    
    def _contains_dangerous_operations(self, query_upper: str) -> bool:
        """Check for dangerous operations using compiled patterns"""
        for pattern in self.dangerous_ops_patterns:
            if pattern.search(query_upper):
                logger.warning(f"Dangerous operation detected: {pattern.pattern}")
                return True
        return False
    
    def _contains_injection_patterns(self, query_upper: str) -> bool:
        """Check for SQL injection patterns using compiled patterns"""
        for pattern in self.security_patterns:
            if pattern.search(query_upper):
                logger.warning(f"Security pattern detected: {pattern.pattern}")
                return True
        return False
    
    def _contains_metadata_access(self, query_upper: str) -> bool:
        """Check if query accesses metadata schemas"""
        for pattern in self.metadata_patterns:
            if pattern.search(query_upper):
                return True
        return False
    
    def _validate_metadata_access(self, query_upper: str, original_query: str) -> bool:
        """
        Validate metadata access queries with contextual analysis.
        
        Args:
            query_upper: Uppercased query
            original_query: Original query for analysis
            
        Returns:
            bool: True if metadata access is legitimate
        """
        # Block if combined with file operations
        if self._contains_file_operations(query_upper):
            logger.warning("Metadata access combined with file operations - blocked")
            return False
        
        # Block if combined with dangerous UNION operations
        if re.search(r'\bunion\s+.*\binformation_schema\b', query_upper):
            logger.warning("Suspicious UNION with information_schema - blocked")
            return False
        
        # Allow simple metadata queries with limited nesting
        nesting_count = query_upper.count('(')
        if nesting_count <= 2:
            logger.debug("Allowing legitimate information_schema query")
            return True
        else:
            logger.warning(f"Complex information_schema query with {nesting_count} nested operations - blocked")
            return False
    
    def _has_excessive_nesting(self, query_upper: str) -> bool:
        """Check for excessive query nesting"""
        nesting_limit = 3  # Configurable
        nesting_count = query_upper.count('(')
        return nesting_count > nesting_limit
    
    def get_security_report(self, query: str) -> dict:
        """
        Generate a detailed security analysis report for a query.
        
        Args:
            query: SQL query to analyze
            
        Returns:
            dict: Security analysis report
        """
        report = {
            "query": query[:100] + "..." if len(query) > 100 else query,
            "is_safe": False,
            "checks": {
                "starts_with_allowed_operation": False,
                "no_dangerous_operations": True,
                "no_file_operations": True,
                "no_injection_patterns": True,
                "acceptable_nesting": True,
                "legitimate_metadata_access": True
            },
            "violations": [],
            "recommendations": []
        }
        
        if not query or not query.strip():
            report["violations"].append("Empty query")
            return report
        
        query_upper = query.strip().upper()
        
        # Check allowed operations
        if any(query_upper.startswith(op) for op in self.allowed_operations):
            report["checks"]["starts_with_allowed_operation"] = True
        else:
            report["violations"].append("Query doesn't start with allowed operation")
            report["recommendations"].append("Use SELECT, SHOW, DESCRIBE, or EXPLAIN")
        
        # Check for dangerous operations
        for pattern in self.dangerous_ops_patterns:
            if pattern.search(query_upper):
                report["checks"]["no_dangerous_operations"] = False
                report["violations"].append(f"Contains dangerous operation: {pattern.pattern}")
                break
        
        # Check for file operations
        if self._contains_file_operations(query_upper):
            report["checks"]["no_file_operations"] = False
            report["violations"].append("Contains file operations")
        
        # Check for injection patterns
        for pattern in self.security_patterns:
            if pattern.search(query_upper):
                report["checks"]["no_injection_patterns"] = False
                report["violations"].append(f"Security risk: {pattern.pattern}")
                break
        
        # Check nesting
        if self._has_excessive_nesting(query_upper):
            report["checks"]["acceptable_nesting"] = False
            report["violations"].append("Excessive query nesting")
        
        # Check metadata access
        if self._contains_metadata_access(query_upper):
            if not self._validate_metadata_access(query_upper, query):
                report["checks"]["legitimate_metadata_access"] = False
                report["violations"].append("Suspicious metadata access pattern")
        
        # Determine overall safety
        report["is_safe"] = (
            report["checks"]["starts_with_allowed_operation"] and
            report["checks"]["no_dangerous_operations"] and
            report["checks"]["no_file_operations"] and 
            report["checks"]["no_injection_patterns"] and
            report["checks"]["acceptable_nesting"] and
            report["checks"]["legitimate_metadata_access"]
        )
        
        if report["is_safe"]:
            report["recommendations"].append("Query appears safe for execution")
        else:
            report["recommendations"].append("Query should be modified to address security violations")
        
        return report


class QuerySanitizer:
    """Sanitizes and normalizes SQL queries"""
    
    @staticmethod
    def normalize_whitespace(query: str) -> str:
        """Normalize whitespace in query"""
        return re.sub(r'\s+', ' ', query.strip())
    
    @staticmethod
    def remove_comments(query: str) -> str:
        """Remove SQL comments from query"""
        # Remove single-line comments
        query = re.sub(r'--.*?$', '', query, flags=re.MULTILINE)
        # Remove multi-line comments
        query = re.sub(r'/\*.*?\*/', '', query, flags=re.DOTALL)
        return query.strip()
    
    @staticmethod
    def limit_query_length(query: str, max_length: int = 10000) -> str:
        """Limit query length"""
        if len(query) > max_length:
            raise ValueError(f"Query exceeds maximum length of {max_length} characters")
        return query