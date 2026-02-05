#!/usr/bin/env python3
"""
Schema migration tool for Apache Cassandra and AWS Keyspaces.
Supports exporting and importing schema definitions with Keyspaces-specific transformations.

Commands:
    export    Export schema from database to CQL file
    import    Import schema from CQL file to database

EXPORT Command Parameters:
    --hosts (required): Comma-separated list of Cassandra host addresses
        Examples: "localhost", "192.168.1.100", "cassandra.us-east-1.amazonaws.com"
        For multi-node clusters: "host1,host2,host3"
    
    --port (optional): Cassandra port number (default: 9042)
        For AWS Keyspaces, use port 9142 (SSL port)
        Example: --port 9142
    
    --username (optional): Username for authentication
        For regular Cassandra: database username
        For AWS Keyspaces with SigV4: AWS access key ID (if using access keys)
        If not provided and no password, SigV4 authentication will be used for AWS Keyspaces
    
    --password (optional): Password for authentication
        For regular Cassandra: database password
        For AWS Keyspaces with SigV4: AWS secret access key (if using access keys)
        If not provided, SigV4 authentication will be used (requires AWS credentials configured)
    
    --keyspace (optional): Specific keyspace to export
        If not specified, exports all non-system keyspaces
        Example: --keyspace mykeyspace
    
    --output (optional): Output file path (default: schema.cql)
        Example: --output my_schema.cql
    
    --ssl (optional): Enable SSL connection (flag, no value required)
        Required for AWS Keyspaces
        Example: --ssl

IMPORT Command Parameters:
    --hosts (required): Comma-separated list of Cassandra host addresses
        Examples: "localhost", "192.168.1.100", "cassandra.us-east-1.amazonaws.com"
    
    --port (optional): Cassandra port number (default: 9042)
        For AWS Keyspaces, use port 9142 (SSL port)
    
    --username (optional): Username for authentication (same as export)
    
    --password (optional): Password for authentication (same as export)
    
    --file, --input (optional): Input CQL file path (default: schema.cql)
        Example: --file my_schema.cql
    
    --ssl (optional): Enable SSL connection (required for AWS Keyspaces)
    
    --dc-region (optional): DC to region mapping for NetworkTopologyStrategy
        Can be specified multiple times
        Format: --dc-region dc_name=region_name
        Example: --dc-region dc1=us-east-1 --dc-region dc2=us-west-2
        Required if source schema uses NetworkTopologyStrategy with multiple DCs
        (unless --force-single-region is used)
    
    --warm-throughput-read (optional): Warm throughput read units per second
        Default: 12000, Minimum: 12000
        Example: --warm-throughput-read 20000
    
    --warm-throughput-write (optional): Warm throughput write units per second
        Default: 4000, Minimum: 4000
        Example: --warm-throughput-write 8000
    
    --if-not-exists (optional): Add IF NOT EXISTS clause to all CREATE statements
        Prevents errors if resources already exist
        Example: --if-not-exists
    
    --force-single-region (optional): Force all keyspaces to use SingleRegionStrategy
        Ignores NetworkTopologyStrategy and DC mappings
        Converts all keyspaces to SingleRegionStrategy
        Example: --force-single-region

EXPORT Examples:
    # Export all keyspaces from AWS Keyspaces (SigV4 authentication)
    python schema_migration.py export --hosts cassandra.us-east-1.amazonaws.com --port 9142 --ssl --output schema.cql
    
    # Export specific keyspace from AWS Keyspaces
    python schema_migration.py export --hosts cassandra.us-east-1.amazonaws.com --port 9142 --ssl \\
        --keyspace mykeyspace --output mykeyspace.cql
    
    # Export all keyspaces from regular Cassandra cluster
    python schema_migration.py export --hosts 192.168.1.100 --username myuser --password mypass \\
        --output schema.cql
    
    # Export from multi-node Cassandra cluster
    python schema_migration.py export --hosts 192.168.1.100,192.168.1.101,192.168.1.102 \\
        --username myuser --password mypass --output schema.cql

IMPORT Examples:
    # Import to AWS Keyspaces with SigV4 authentication
    python schema_migration.py import --hosts cassandra.us-east-1.amazonaws.com --port 9142 --ssl \\
        --file schema.cql
    
    # Import to AWS Keyspaces with DC-to-region mappings
    python schema_migration.py import --hosts cassandra.us-east-1.amazonaws.com --port 9142 --ssl \\
        --dc-region dc1=us-east-1 --dc-region dc2=us-west-2 --file schema.cql
    
    # Import to AWS Keyspaces forcing SingleRegionStrategy for all keyspaces
    python schema_migration.py import --hosts cassandra.us-east-1.amazonaws.com --port 9142 --ssl \\
        --force-single-region --file schema.cql
    
    # Import to AWS Keyspaces with custom warm throughput settings
    python schema_migration.py import --hosts cassandra.us-east-1.amazonaws.com --port 9142 --ssl \\
        --warm-throughput-read 20000 --warm-throughput-write 8000 --file schema.cql
    
    # Import to regular Cassandra cluster
    python schema_migration.py import --hosts 192.168.1.100 --username myuser --password mypass \\
        --file schema.cql
    
    # Import with IF NOT EXISTS to avoid errors if resources already exist
    python schema_migration.py import --hosts cassandra.us-east-1.amazonaws.com --port 9142 --ssl \\
        --if-not-exists --file schema.cql

Notes:
    - For AWS Keyspaces, SSL is required (use --ssl flag)
    - For AWS Keyspaces, use port 9142 (SSL port)
    - If password is not provided, SigV4 authentication will be used for AWS Keyspaces
    - System keyspaces are automatically excluded from export and import
    - NetworkTopologyStrategy keyspaces require --dc-region mappings or --force-single-region
    - SimpleStrategy keyspaces are automatically converted to SingleRegionStrategy for Keyspaces
    - Progress bars show real-time status during import operations
    - Failed operations are summarized at the end with detailed error messages
"""

import sys
import ssl
import re
import time
import threading
from queue import Queue
from typing import Dict, List, Tuple, Optional, NamedTuple
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    # Fallback: create a simple progress display
    class tqdm:
        def __init__(self, total=None, desc=None, unit=None, bar_format=None, position=None, leave=True, **kwargs):
            self.total = total
            self.desc = desc or ""
            self.n = 0
            self.unit = unit or ""
            self.position = position
            self.leave = leave if leave is not None else True
            self.postfix_str = ""
        
        def update(self, n=1):
            self.n += n
            if self.total:
                pct = (self.n / self.total) * 100
                print(f"\r{self.desc}: {self.n}/{self.total} ({pct:.1f}%) {self.postfix_str}", end='', flush=True)
            else:
                print(f"\r{self.desc}: {self.n} {self.postfix_str}", end='', flush=True)
        
        def set_postfix(self, postfix=None, **kwargs):
            if isinstance(postfix, str):
                self.postfix_str = postfix
            elif postfix:
                self.postfix_str = str(postfix)
            elif kwargs:
                self.postfix_str = ', '.join([f"{k}={v}" for k, v in kwargs.items()])
        
        def close(self):
            if self.leave:
                print()  # New line after progress
            else:
                print("\r" + " " * 80 + "\r", end='', flush=True)  # Clear line
        
        def __enter__(self):
            return self
        
        def __exit__(self, *args):
            self.close()

try:
    from cassandra_sigv4.auth import SigV4AuthProvider
    SIGV4_AVAILABLE = True
except ImportError:
    SIGV4_AVAILABLE = False


# Concurrent DDL operations limit (Keyspaces service quota)
MAX_CONCURRENT_DDL = 45


class DDLStatement(NamedTuple):
    """Represents a DDL statement to be executed."""
    statement_type: str  # 'keyspace', 'type', or 'table'
    statement: str
    keyspace_name: Optional[str] = None
    type_name: Optional[str] = None
    table_name: Optional[str] = None
    cdc_enabled: bool = False

# System keyspaces to ignore (lowercase for case-insensitive matching)
SYSTEM_KEYSPACES = {
    'system', 'system_schema', 'system_traces', 'system_auth', 'system_multiregion_info',
    'system_distributed', 'dse_auth', 'dse_security', 'dse_leases',
    'dse_perf', 'dse_system', 'opscenter', 'cfs', 'cfs_archive',
    'dsefs', 'hivemetastore', 'spark_system', 'system_schema_mcs'
}


def connect_to_cluster(hosts, port=9042, username=None, password=None, use_ssl=False):
    """Connect to Cassandra cluster with appropriate authentication."""
    # Convert single host to list if needed
    if isinstance(hosts, str):
        hosts = [hosts]
    
    # Setup authentication
    auth_provider = None
    if username and password:
        # Use username/password authentication
        auth_provider = PlainTextAuthProvider(username=username, password=password)
    elif not password:
        # Use SigV4 authentication for AWS Keyspaces when password is not provided
        if SIGV4_AVAILABLE:
            auth_provider = SigV4AuthProvider()
            print("Using AWS SigV4 authentication for Keyspaces")
        else:
            print("Warning: cassandra-sigv4 not installed. Install it with: pip install cassandra-sigv4", file=sys.stderr)
            print("Attempting connection without authentication...", file=sys.stderr)
    
    # Setup SSL if requested
    ssl_context = None
    if use_ssl:
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS)
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        print("SSL enabled")
    
    # Connect to cluster
    cluster = Cluster(
        hosts,
        port=port,
        auth_provider=auth_provider,
        ssl_context=ssl_context
    )
    session = cluster.connect()
    print(f"Connected to Cassandra cluster at {hosts}:{port}")
    
    return cluster, session


def detect_keyspaces(session):
    """Detect if the target cluster is AWS Keyspaces by checking system.local."""
    try:
        result = session.execute("SELECT cluster_name FROM system.local")
        row = result.one()
        if row and row.cluster_name == 'Amazon Keyspaces':
            print("Detected AWS Keyspaces cluster")
            return True
        else:
            print(f"Detected regular Cassandra cluster (cluster_name: {row.cluster_name if row else 'unknown'})")
            return False
    except Exception as e:
        print(f"Warning: Could not detect cluster type: {e}", file=sys.stderr)
        print("Assuming regular Cassandra cluster", file=sys.stderr)
        return False


def extract_keyspace_from_statement(statement: str, statement_type: str) -> Optional[str]:
    """Extract keyspace name from a CREATE statement."""
    statement_upper = statement.upper()
    
    if statement_type == 'KEYSPACE':
        # CREATE KEYSPACE name ...
        match = re.match(r"CREATE\s+KEYSPACE\s+(\S+)", statement, re.IGNORECASE)
        if match:
            return match.group(1).strip('"')
    elif statement_type == 'TABLE':
        # CREATE TABLE [keyspace.]table ...
        match = re.match(r"CREATE\s+TABLE\s+(?:(\S+)\.)?(\S+)", statement, re.IGNORECASE)
        if match:
            keyspace = match.group(1)
            if keyspace:
                return keyspace.strip('"')
    elif statement_type == 'TYPE':
        # CREATE TYPE [keyspace.]type ...
        match = re.match(r"CREATE\s+TYPE\s+(?:(\S+)\.)?(\S+)", statement, re.IGNORECASE)
        if match:
            keyspace = match.group(1)
            if keyspace:
                return keyspace.strip('"')
    
    return None


def parse_cql_file(file_path: str) -> Dict[str, List[str]]:
    """Parse CQL file and categorize statements, filtering out system keyspaces."""
    with open(file_path, 'r') as f:
        content = f.read()
    
    statements = {
        'keyspaces': [],
        'types': [],
        'tables': [],
        'indexes': [],
        'views': [],
        'functions': [],
        'aggregates': [],
        'other': []
    }
    
    ignored_keyspaces = set()
    ignored_tables = []
    ignored_types = []
    
    # Split by lines and process each statement
    # Statements can be on single lines or multiple lines
    # They may or may not end with semicolons
    lines = content.split('\n')
    current_statement = []
    
    for line in lines:
        stripped = line.strip()
        
        # Skip empty lines and comments
        if not stripped or stripped.startswith('--'):
            # If we have accumulated a statement and hit an empty line, process it
            if current_statement:
                statement = ' '.join(current_statement).strip()
                if statement:
                    # Remove trailing semicolon if present
                    if statement.endswith(';'):
                        statement = statement[:-1].strip()
                    
                    # Categorize statement and filter system keyspaces
                    statement_upper = statement.upper()
                    if statement_upper.startswith('CREATE KEYSPACE'):
                        keyspace_name = extract_keyspace_from_statement(statement, 'KEYSPACE')
                        if keyspace_name and keyspace_name.lower() in SYSTEM_KEYSPACES:
                            ignored_keyspaces.add(keyspace_name)
                        else:
                            statements['keyspaces'].append(statement)
                    elif statement_upper.startswith('CREATE TYPE'):
                        keyspace_name = extract_keyspace_from_statement(statement, 'TYPE')
                        if keyspace_name and keyspace_name.lower() in SYSTEM_KEYSPACES:
                            ignored_types.append(statement)
                        else:
                            statements['types'].append(statement)
                    elif statement_upper.startswith('CREATE TABLE'):
                        keyspace_name = extract_keyspace_from_statement(statement, 'TABLE')
                        if keyspace_name and keyspace_name.lower() in SYSTEM_KEYSPACES:
                            ignored_tables.append(statement)
                        else:
                            statements['tables'].append(statement)
                    elif statement_upper.startswith('CREATE INDEX'):
                        statements['indexes'].append(statement)
                    elif statement_upper.startswith('CREATE MATERIALIZED VIEW'):
                        statements['views'].append(statement)
                    elif statement_upper.startswith('CREATE FUNCTION'):
                        statements['functions'].append(statement)
                    elif statement_upper.startswith('CREATE AGGREGATE'):
                        statements['aggregates'].append(statement)
                    else:
                        statements['other'].append(statement)
                
                current_statement = []
            continue
        
        # Add non-empty line to current statement
        current_statement.append(stripped)
        
        # If line ends with semicolon, process the statement
        if stripped.endswith(';'):
            statement = ' '.join(current_statement).rstrip(';').strip()
            current_statement = []
            
            if not statement:
                continue
            
            # Categorize statement and filter system keyspaces
            statement_upper = statement.upper()
            if statement_upper.startswith('CREATE KEYSPACE'):
                keyspace_name = extract_keyspace_from_statement(statement, 'KEYSPACE')
                if keyspace_name and keyspace_name.lower() in SYSTEM_KEYSPACES:
                    ignored_keyspaces.add(keyspace_name)
                else:
                    statements['keyspaces'].append(statement)
            elif statement_upper.startswith('CREATE TYPE'):
                keyspace_name = extract_keyspace_from_statement(statement, 'TYPE')
                if keyspace_name and keyspace_name.lower() in SYSTEM_KEYSPACES:
                    ignored_types.append(statement)
                else:
                    statements['types'].append(statement)
            elif statement_upper.startswith('CREATE TABLE'):
                keyspace_name = extract_keyspace_from_statement(statement, 'TABLE')
                if keyspace_name and keyspace_name.lower() in SYSTEM_KEYSPACES:
                    ignored_tables.append(statement)
                else:
                    statements['tables'].append(statement)
            elif statement_upper.startswith('CREATE INDEX'):
                statements['indexes'].append(statement)
            elif statement_upper.startswith('CREATE MATERIALIZED VIEW'):
                statements['views'].append(statement)
            elif statement_upper.startswith('CREATE FUNCTION'):
                statements['functions'].append(statement)
            elif statement_upper.startswith('CREATE AGGREGATE'):
                statements['aggregates'].append(statement)
            else:
                statements['other'].append(statement)
    
    # Process any remaining statement at end of file
    if current_statement:
        statement = ' '.join(current_statement).strip()
        if statement:
            if statement.endswith(';'):
                statement = statement[:-1].strip()
            
            statement_upper = statement.upper()
            if statement_upper.startswith('CREATE KEYSPACE'):
                keyspace_name = extract_keyspace_from_statement(statement, 'KEYSPACE')
                if keyspace_name and keyspace_name.lower() in SYSTEM_KEYSPACES:
                    ignored_keyspaces.add(keyspace_name)
                else:
                    statements['keyspaces'].append(statement)
            elif statement_upper.startswith('CREATE TYPE'):
                keyspace_name = extract_keyspace_from_statement(statement, 'TYPE')
                if keyspace_name and keyspace_name.lower() in SYSTEM_KEYSPACES:
                    ignored_types.append(statement)
                else:
                    statements['types'].append(statement)
            elif statement_upper.startswith('CREATE TABLE'):
                keyspace_name = extract_keyspace_from_statement(statement, 'TABLE')
                if keyspace_name and keyspace_name.lower() in SYSTEM_KEYSPACES:
                    ignored_tables.append(statement)
                else:
                    statements['tables'].append(statement)
            elif statement_upper.startswith('CREATE INDEX'):
                statements['indexes'].append(statement)
            elif statement_upper.startswith('CREATE MATERIALIZED VIEW'):
                statements['views'].append(statement)
            elif statement_upper.startswith('CREATE FUNCTION'):
                statements['functions'].append(statement)
            elif statement_upper.startswith('CREATE AGGREGATE'):
                statements['aggregates'].append(statement)
            else:
                statements['other'].append(statement)
    
    # Log ignored system keyspaces
    if ignored_keyspaces:
        print(f"Ignoring {len(ignored_keyspaces)} system keyspace(s): {', '.join(sorted(ignored_keyspaces))}")
    if ignored_tables:
        print(f"Ignoring {len(ignored_tables)} table(s) from system keyspaces")
    if ignored_types:
        print(f"Ignoring {len(ignored_types)} type(s) from system keyspaces")
    
    return statements


def add_if_not_exists(statement: str, statement_type: str, if_not_exists: bool) -> str:
    """Add IF NOT EXISTS clause to CREATE statement if flag is set."""
    if not if_not_exists:
        return statement
    
    # Check if IF NOT EXISTS already exists
    if re.search(r'\bIF\s+NOT\s+EXISTS\b', statement, re.IGNORECASE):
        return statement
    
    # Add IF NOT EXISTS after CREATE KEYSPACE/TABLE/TYPE
    pattern_map = {
        'keyspace': r'(CREATE\s+KEYSPACE)\s+',
        'table': r'(CREATE\s+TABLE)\s+',
        'type': r'(CREATE\s+TYPE)\s+'
    }
    
    if statement_type in pattern_map:
        pattern = pattern_map[statement_type]
        statement = re.sub(pattern, r'\1 IF NOT EXISTS ', statement, flags=re.IGNORECASE)
    
    return statement


def extract_keyspace_replication_info(statement: str) -> Optional[Tuple[str, str, Dict[str, str]]]:
    """Extract keyspace name, strategy class, and DC mappings from CREATE KEYSPACE statement."""
    # Pattern: CREATE KEYSPACE [IF NOT EXISTS] name WITH replication = {...} AND durable_writes = ...
    match = re.match(r"CREATE\s+KEYSPACE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)\s+WITH\s+replication\s*=\s*({[^}]+})\s*(?:AND\s+durable_writes\s*=\s*(\S+))?", statement, re.IGNORECASE)
    if not match:
        return None
    
    keyspace_name = match.group(1)
    replication_str = match.group(2)
    
    # Extract class
    class_match = re.search(r"'class'\s*:\s*'([^']+)'", replication_str, re.IGNORECASE)
    if not class_match:
        return None
    
    strategy_class = class_match.group(1)
    
    # Extract DC mappings if NetworkTopologyStrategy
    dc_map = {}
    if strategy_class == 'NetworkTopologyStrategy':
        dc_matches = re.findall(r"'([^']+)'\s*:\s*'(\d+)'", replication_str)
        for dc_name, rf in dc_matches:
            if dc_name != 'class':
                dc_map[dc_name] = rf
    
    return (keyspace_name, strategy_class, dc_map)


def validate_dc_mappings(statements: List[str], dc_region_map: Dict[str, str]) -> Tuple[List[str], bool]:
    """
    Validate that all DC mappings are provided for NetworkTopologyStrategy keyspaces.
    
    Returns:
        Tuple of (missing_dc_list, all_valid)
        - missing_dc_list: List of DC names that are missing mappings
        - all_valid: True if all DCs have mappings
    """
    all_missing_dcs = set()
    
    for statement in statements:
        info = extract_keyspace_replication_info(statement)
        if not info:
            continue
        
        keyspace_name, strategy_class, dc_map = info
        
        if strategy_class == 'NetworkTopologyStrategy':
            for dc_name in dc_map.keys():
                if dc_name not in dc_region_map:
                    all_missing_dcs.add(dc_name)
    
    return list(all_missing_dcs), len(all_missing_dcs) == 0


def transform_keyspace_statement(statement: str, is_keyspaces: bool, dc_region_map: Dict[str, str], if_not_exists: bool = False, force_single_region: bool = False) -> Tuple[Optional[str], List[str]]:
    """
    Transform keyspace statement for Keyspaces compatibility.
    
    Returns:
        Tuple of (transformed_statement, missing_dcs_list)
        - transformed_statement: The transformed statement, or None if transformation failed
        - missing_dcs_list: List of DC names that were not found in the mapping
    """
    if not is_keyspaces:
        return statement, []
    
    # Extract keyspace name and replication strategy
    # Pattern: CREATE KEYSPACE [IF NOT EXISTS] name WITH replication = {...} AND durable_writes = ...
    match = re.match(r"CREATE\s+KEYSPACE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)\s+WITH\s+replication\s*=\s*({[^}]+})\s*(?:AND\s+durable_writes\s*=\s*(\S+))?", statement, re.IGNORECASE)
    if not match:
        return statement, []
    
    keyspace_name = match.group(1)
    has_if_not_exists = bool(re.search(r'\bIF\s+NOT\s+EXISTS\b', statement, re.IGNORECASE))
    # Use the flag if not already present
    use_if_not_exists = if_not_exists or has_if_not_exists
    replication_str = match.group(2)
    durable_writes = match.group(3) if match.group(3) else 'true'
    
    # Parse replication strategy
    # Extract class and replication_factor or DC mappings
    class_match = re.search(r"'class'\s*:\s*'([^']+)'", replication_str, re.IGNORECASE)
    if not class_match:
        return statement, []
    
    strategy_class = class_match.group(1)
    missing_dcs = []
    
    # If force_single_region is set, convert everything to SingleRegionStrategy
    if force_single_region:
        new_replication = "{'class': 'SingleRegionStrategy'}"
        #print(f"Converting keyspace {keyspace_name} from {strategy_class} to SingleRegionStrategy (--force-single-region)")
    elif strategy_class == 'NetworkTopologyStrategy':
        # Map DCs to regions - only extract DC mappings for NetworkTopologyStrategy
        dc_matches = re.findall(r"'([^']+)'\s*:\s*'(\d+)'", replication_str)
        new_dc_map = {}
        
        for dc_name, rf in dc_matches:
            if dc_name == 'class':
                continue
            if dc_name in dc_region_map:
                region = dc_region_map[dc_name]
                new_dc_map[region] = '3'  # Keyspaces only supports replication factor 3
                if rf != '3':
                    print(f"Warning: Keyspaces only supports replication_factor of 3. Converting DC '{dc_name}' (rf={rf}) to region '{region}' with rf=3")
            else:
                missing_dcs.append(dc_name)
        
        # If any DCs are missing, return error
        if missing_dcs:
            return None, missing_dcs
        
        # If no valid mappings found, return error
        if not new_dc_map:
            print(f"Error: No valid DC mappings found for keyspace {keyspace_name}. Cannot create keyspace.", file=sys.stderr)
            return None, []
        
        dc_str = ', '.join([f"'{k}': '{v}'" for k, v in new_dc_map.items()])
        new_replication = f"{{'class': 'NetworkTopologyStrategy', {dc_str}}}"
    elif strategy_class == 'SimpleStrategy':
        # Convert SimpleStrategy to SingleRegionStrategy (no DC mappings needed)
        new_replication = "{'class': 'SingleRegionStrategy'}"
        #print(f"Converting keyspace {keyspace_name} from SimpleStrategy to SingleRegionStrategy")
    elif strategy_class == 'SingleRegionStrategy':
        # Already SingleRegionStrategy, ensure it's in the correct format
        new_replication = "{'class': 'SingleRegionStrategy'}"
    else:
        # Unknown strategy, return as-is
        return statement, []
    
    # Reconstruct statement
    if use_if_not_exists:
        result = f"CREATE KEYSPACE IF NOT EXISTS {keyspace_name} WITH replication = {new_replication} AND durable_writes = {durable_writes}"
    else:
        result = f"CREATE KEYSPACE {keyspace_name} WITH replication = {new_replication} AND durable_writes = {durable_writes}"
    return result, []


def extract_default_time_to_live(statement: str) -> int:
    """Extract default_time_to_live value from table statement."""
    # Look for default_time_to_live = <value> in WITH clause
    match = re.search(r"default_time_to_live\s*=\s*(\d+)", statement, re.IGNORECASE)
    if match:
        return int(match.group(1))
    return 0


def check_cdc_enabled(statement: str) -> bool:
    """Check if CDC is enabled in the table statement."""
    # Look for cdc = true in WITH clause
    match = re.search(r"cdc\s*=\s*(true|false)", statement, re.IGNORECASE)
    if match:
        return match.group(1).lower() == 'true'
    return False


def transform_table_statement(statement: str, is_keyspaces: bool, warm_read: int, warm_write: int, if_not_exists: bool = False) -> Tuple[str, bool]:
    """Transform table statement for Keyspaces compatibility. Returns (transformed_statement, cdc_enabled)."""
    # Add IF NOT EXISTS if flag is set (for both Keyspaces and regular Cassandra)
    has_if_not_exists = bool(re.search(r'\bIF\s+NOT\s+EXISTS\b', statement, re.IGNORECASE))
    if if_not_exists and not has_if_not_exists:
        statement = re.sub(r'(CREATE\s+TABLE)\s+', r'\1 IF NOT EXISTS ', statement, flags=re.IGNORECASE)
    
    if not is_keyspaces:
        return statement, False
    
    # Extract table definition (everything before WITH)
    with_match = re.search(r"\s+WITH\s+", statement, re.IGNORECASE)
    if not with_match:
        return statement, False
    
    table_def = statement[:with_match.start()]
    default_ttl = extract_default_time_to_live(statement)
    cdc_enabled = check_cdc_enabled(statement)
    
    # Build new WITH clause
    # Format CUSTOM_PROPERTIES as specified in the plan (multi-line with proper indentation)
    custom_properties = (
        "{\n"
        "\t'capacity_mode': {\n"
        "\t\t'throughput_mode': 'PAY_PER_REQUEST'\n"
        "\t},\n"
        f"\t'warm_throughput': {{\n"
        f"\t\t'read_units_per_second': {warm_read},\n"
        f"\t\t'write_units_per_second': {warm_write}\n"
        "\t},\n"
        "\t'point_in_time_recovery': {\n"
        "\t\t'status': 'enabled'\n"
        "\t},\n"
        "\t'encryption_specification': {\n"
        "\t\t'encryption_type': 'AWS_OWNED_KMS_KEY'\n"
        "\t}\n"
        "}"
    )
    
    new_with = f"WITH default_time_to_live = {default_ttl}\nAND CUSTOM_PROPERTIES = {custom_properties}"
    
    result = f"{table_def} {new_with}"
    return result, cdc_enabled


def wait_for_keyspace(session, keyspace_name: str, max_wait: int = 60):
    """Poll system_schema.keyspaces to check if keyspace is created."""
    start_time = time.time()
    while time.time() - start_time < max_wait:
        try:
            result = session.execute(
                "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = %s",
                [keyspace_name]
            )
            if result.one():
                return True
        except Exception:
            pass
        time.sleep(5)
    return False


def wait_for_type(session, keyspace_name: str, type_name: str, max_wait: int = 60):
    """Poll system_schema.types to check if type is created."""
    start_time = time.time()
    while time.time() - start_time < max_wait:
        try:
            result = session.execute(
                "SELECT keyspace_name, type_name FROM system_schema.types WHERE keyspace_name = %s AND type_name = %s",
                [keyspace_name, type_name]
            )
            if result.one():
                return True
        except Exception:
            pass
        time.sleep(5)
    return False


def wait_for_table(session, keyspace_name: str, table_name: str, is_keyspaces: bool, max_wait: int = 60):
    """Poll system_schema.tables to check if table is created."""
    start_time = time.time()
    while time.time() - start_time < max_wait:
        try:
            if is_keyspaces:
                result = session.execute(
                    "SELECT status,keyspace_name, table_name FROM system_schema_mcs.tables WHERE keyspace_name = %s AND table_name = %s",
                    [keyspace_name, table_name]
                )
                if result.one() and result.one().status == 'ACTIVE':
                    return True
                #else:
                #   debug_print(f"Table {keyspace_name}.{table_name} is not active. Status: {result.one().status}")
            else:
                return True
        except Exception:
            pass
        time.sleep(5)
    return False


def extract_keyspace_name(statement: str) -> Optional[str]:
    """Extract keyspace name from CREATE KEYSPACE statement."""
    match = re.match(r"CREATE\s+KEYSPACE\s+(\S+)", statement, re.IGNORECASE)
    if match:
        return match.group(1).strip('"')
    return None


def extract_type_info(statement: str) -> Optional[Tuple[str, str]]:
    """Extract keyspace and type name from CREATE TYPE statement."""
    # Pattern: CREATE TYPE [keyspace.]type_name
    match = re.match(r"CREATE\s+TYPE\s+(?:(\S+)\.)?(\S+)", statement, re.IGNORECASE)
    if match:
        keyspace = match.group(1)
        type_name = match.group(2).strip('"')
        if keyspace:
            return (keyspace.strip('"'), type_name)
        # If no keyspace, we need to use the current keyspace context
        return (None, type_name)
    return None


class ProgressTracker:
    """Thread-safe progress tracker for DDL operations."""
    def __init__(self, total_keyspaces=0, total_types=0, total_tables=0):
        self.lock = threading.Lock()
        self.keyspaces_created = 0
        self.keyspaces_failed = 0
        self.keyspaces_ignored = 0
        self.types_created = 0
        self.types_failed = 0
        self.tables_created = 0
        self.tables_failed = 0
        
        # Create progress bars with custom format
        self.keyspace_pbar = tqdm(total=total_keyspaces, desc="Keyspaces", unit="", 
                                  bar_format='{desc}: {n_fmt}/{total_fmt} | Created: {postfix}',
                                  position=0, leave=True)
        self.keyspace_pbar.set_postfix('0 created, 0 failed, 0 ignored')
        
        self.type_pbar = tqdm(total=total_types, desc="Types    ", unit="",
                              bar_format='{desc}: {n_fmt}/{total_fmt} | Created: {postfix}',
                              position=1, leave=True)
        self.type_pbar.set_postfix('0 created, 0 failed')
        
        self.table_pbar = tqdm(total=total_tables, desc="Tables   ", unit="",
                                bar_format='{desc}: {n_fmt}/{total_fmt} | Created: {postfix}',
                                position=2, leave=True)
        self.table_pbar.set_postfix('0 created, 0 failed')
    
    def update_keyspace(self, success=True, ignored=False):
        with self.lock:
            if ignored:
                self.keyspaces_ignored += 1
            elif success:
                self.keyspaces_created += 1
            else:
                self.keyspaces_failed += 1
            self.keyspace_pbar.update(1)
            self.keyspace_pbar.set_postfix(f'{self.keyspaces_created} created, {self.keyspaces_failed} failed, {self.keyspaces_ignored} ignored')
    
    def update_type(self, success=True):
        with self.lock:
            if success:
                self.types_created += 1
            else:
                self.types_failed += 1
            self.type_pbar.update(1)
            self.type_pbar.set_postfix(f'{self.types_created} created, {self.types_failed} failed')
    
    def update_table(self, success=True):
        with self.lock:
            if success:
                self.tables_created += 1
            else:
                self.tables_failed += 1
            self.table_pbar.update(1)
            self.table_pbar.set_postfix(f'{self.tables_created} created, {self.tables_failed} failed')
    
    def close(self):
        self.keyspace_pbar.close()
        self.type_pbar.close()
        self.table_pbar.close()


def print_failure_summary(results: List):
    """Print detailed summary of failed keyspaces, types, and tables."""
    failed_keyspaces = []
    failed_types = []
    failed_tables = []
    
    for result in results:
        ddl_stmt, success, error_message = result
        if not success:
            resource_name = None
            if ddl_stmt.statement_type == 'keyspace':
                resource_name = ddl_stmt.keyspace_name or 'unknown'
                failed_keyspaces.append((resource_name, error_message))
            elif ddl_stmt.statement_type == 'type':
                if ddl_stmt.keyspace_name and ddl_stmt.type_name:
                    resource_name = f"{ddl_stmt.keyspace_name}.{ddl_stmt.type_name}"
                elif ddl_stmt.type_name:
                    resource_name = ddl_stmt.type_name
                else:
                    resource_name = 'unknown'
                failed_types.append((resource_name, error_message))
            elif ddl_stmt.statement_type == 'table':
                if ddl_stmt.keyspace_name and ddl_stmt.table_name:
                    resource_name = f"{ddl_stmt.keyspace_name}.{ddl_stmt.table_name}"
                elif ddl_stmt.table_name:
                    resource_name = ddl_stmt.table_name
                else:
                    resource_name = 'unknown'
                failed_tables.append((resource_name, error_message))
    
    # Print summary if there are any failures
    if failed_keyspaces or failed_types or failed_tables:
        print("\n" + "="*60, file=sys.stderr)
        print("FAILURE SUMMARY", file=sys.stderr)
        print("="*60, file=sys.stderr)
        
        if failed_keyspaces:
            print(f"\nFailed Keyspaces ({len(failed_keyspaces)}):", file=sys.stderr)
            for name, error in failed_keyspaces:
                print(f"  - {name}: {error}", file=sys.stderr)
        
        if failed_types:
            print(f"\nFailed Types ({len(failed_types)}):", file=sys.stderr)
            for name, error in failed_types:
                print(f"  - {name}: {error}", file=sys.stderr)
        
        if failed_tables:
            print(f"\nFailed Tables ({len(failed_tables)}):", file=sys.stderr)
            for name, error in failed_tables:
                print(f"  - {name}: {error}", file=sys.stderr)
        
        print("="*60 + "\n", file=sys.stderr)


def ddl_worker(session, queue: Queue, semaphore: threading.Semaphore, results: List, is_keyspaces: bool, progress: ProgressTracker):
    """Worker thread that consumes DDL statements from the queue."""
    while True:
        item = queue.get()
        if item is None:  # Poison pill to stop worker
            queue.task_done()
            break
        
        ddl_stmt = item
        semaphore.acquire()  # Block until a slot is available
        
        try:
            # Execute the statement
            session.execute(ddl_stmt.statement)
            
            # Wait for the resource to be available based on type
            if ddl_stmt.statement_type == 'keyspace':
                if ddl_stmt.keyspace_name:
                    wait_for_keyspace(session, ddl_stmt.keyspace_name)
                    progress.update_keyspace(success=True)
            elif ddl_stmt.statement_type == 'type':
                if ddl_stmt.keyspace_name and ddl_stmt.type_name:
                    wait_for_type(session, ddl_stmt.keyspace_name, ddl_stmt.type_name)
                    progress.update_type(success=True)
                elif ddl_stmt.type_name:
                    progress.update_type(success=True)
            elif ddl_stmt.statement_type == 'table':
                if ddl_stmt.keyspace_name and ddl_stmt.table_name:
                    wait_for_table(session, ddl_stmt.keyspace_name, ddl_stmt.table_name, is_keyspaces)
                    progress.update_table(success=True)
                elif ddl_stmt.table_name:
                    progress.update_table(success=True)
            
            results.append((ddl_stmt, True, None))
            
        except Exception as e:
            error_message = str(e)
            if ddl_stmt.statement_type == 'keyspace':
                progress.update_keyspace(success=False)
            elif ddl_stmt.statement_type == 'type':
                progress.update_type(success=False)
            elif ddl_stmt.statement_type == 'table':
                progress.update_table(success=False)
            
            results.append((ddl_stmt, False, error_message))
        finally:
            semaphore.release()  # Release the semaphore slot
            queue.task_done()


def export_schema(hosts, port=9042, username=None, password=None, keyspace=None, output_file='schema.cql', use_ssl=False):
    """
    Export Cassandra schema to a CQL file.
    
    Examples:
        # Export from AWS Keyspaces with SigV4 authentication (no password)
        python schema_migration.py export --hosts cassandra.us-east-1.amazonaws.com --ssl --output schema.cql
        
        # Export all keyspaces from regular Cassandra cluster
        python schema_migration.py export --hosts 192.168.1.100 --username myuser --password mypass --output schema.cql
        
        # Export a specific keyspace only
        python schema_migration.py export --hosts 192.168.1.100 --username myuser --password mypass \\
            --keyspace mykeyspace --output mykeyspace.cql
        
        # Export from multi-node Cassandra cluster
        python schema_migration.py export --hosts 192.168.1.100,192.168.1.101,192.168.1.102 \\
            --output schema.cql
    """
    cluster, session = connect_to_cluster(hosts, port, username, password, use_ssl)
    
    try:
        metadata = cluster.metadata
        cql_statements = []
        
        # Get keyspaces to export
        if keyspace:
            if keyspace not in metadata.keyspaces:
                print(f"Error: Keyspace '{keyspace}' not found", file=sys.stderr)
                sys.exit(1)
            keyspaces_to_export = [metadata.keyspaces[keyspace]]
        else:
            # Export all keyspaces (excluding system keyspaces)
            keyspaces_to_export = [
                ks for ks in metadata.keyspaces.values()
                if ks.name not in ['system', 'system_schema', 'system_traces', 'system_auth', 'system_distributed']
            ]
        
        # Export each keyspace
        for ks in keyspaces_to_export:
            # Create keyspace statement
            ks_cql = ks.as_cql_query()
            cql_statements.append(ks_cql)
            cql_statements.append("")  # Empty line for readability
            
            # Export tables in this keyspace
            for table_name, table in ks.tables.items():
                table_cql = table.as_cql_query()
                cql_statements.append(table_cql)
                cql_statements.append("")  # Empty line for readability
            
            # Export views in this keyspace
            for view_name, view in ks.views.items():
                view_cql = view.as_cql_query()
                cql_statements.append(view_cql)
                cql_statements.append("")  # Empty line for readability
            
            # Export user-defined types
            for udt_name, udt in ks.user_types.items():
                udt_cql = udt.as_cql_query()
                cql_statements.append(udt_cql)
                cql_statements.append("")  # Empty line for readability
            
            # Export functions
            for function_name, function in ks.functions.items():
                function_cql = function.as_cql_query()
                cql_statements.append(function_cql)
                cql_statements.append("")  # Empty line for readability
            
            # Export aggregates
            for aggregate_name, aggregate in ks.aggregates.items():
                aggregate_cql = aggregate.as_cql_query()
                cql_statements.append(aggregate_cql)
                cql_statements.append("")  # Empty line for readability
        
        # Write to file
        with open(output_file, 'w') as f:
            f.write('\n'.join(cql_statements))
        
        print(f"Schema exported successfully to {output_file}")
        print(f"Exported {len(keyspaces_to_export)} keyspace(s)")
        
    except Exception as e:
        print(f"Error exporting schema: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        cluster.shutdown()


def import_schema(hosts, port=9042, username=None, password=None, input_file='schema.cql', 
                  use_ssl=False, dc_region_map=None, warm_throughput_read=12000, warm_throughput_write=4000, if_not_exists=False, force_single_region=False):
    """
    Import schema from CQL file to target cluster.
    
    Examples:
        # Import to AWS Keyspaces with SigV4 authentication (no password)
        python schema_migration.py import --hosts cassandra.us-east-1.amazonaws.com --ssl --file schema.cql
        
        # Import to AWS Keyspaces with DC-to-region mappings
        python schema_migration.py import --hosts cassandra.us-east-1.amazonaws.com --ssl \\
            --dc-region dc1=us-east-1 --dc-region dc2=us-west-2 --file schema.cql
        
        # Import to AWS Keyspaces forcing SingleRegionStrategy for all keyspaces
        python schema_migration.py import --hosts cassandra.us-east-1.amazonaws.com --ssl \\
            --force-single-region --file schema.cql
        
        # Import to AWS Keyspaces with custom warm throughput settings
        python schema_migration.py import --hosts cassandra.us-east-1.amazonaws.com --ssl \\
            --warm-throughput-read 20000 --warm-throughput-write 8000 --file schema.cql
        
        # Import to regular Cassandra cluster with username/password
        python schema_migration.py import --hosts 192.168.1.100 --username myuser --password mypass \\
            --file schema.cql
        
        # Import with IF NOT EXISTS to avoid errors if resources already exist
        python schema_migration.py import --hosts cassandra.us-east-1.amazonaws.com --ssl \\
            --if-not-exists --file schema.cql
    """
    cluster, session = connect_to_cluster(hosts, port, username, password, use_ssl)
    
    try:
        # Detect if target is Keyspaces
        is_keyspaces = detect_keyspaces(session)
        
        # Parse CQL file
        print(f"Parsing CQL file: {input_file}")
        statements = parse_cql_file(input_file)
        
        # Log unsupported statements
        if statements['indexes']:
            print(f"\nWarning: Found {len(statements['indexes'])} CREATE INDEX statement(s). INDEX is not supported and will be skipped.")
        if statements['views']:
            print(f"\nWarning: Found {len(statements['views'])} CREATE MATERIALIZED VIEW statement(s). MATERIALIZED_VIEWS are not supported and will be skipped.")
        if statements['functions']:
            print(f"\nWarning: Found {len(statements['functions'])} CREATE FUNCTION statement(s). FUNCTIONS are not supported and will be skipped.")
        if statements['aggregates']:
            print(f"\nWarning: Found {len(statements['aggregates'])} CREATE AGGREGATE statement(s). AGGREGATIONS are not supported and will be skipped.")
        
        # Validate DC mappings if not forcing single region
        if is_keyspaces and not force_single_region:
            missing_dcs, all_valid = validate_dc_mappings(statements['keyspaces'], dc_region_map or {})
            if not all_valid:
                print(f"\nError: Missing DC-to-region mappings for the following DC(s): {', '.join(missing_dcs)}", file=sys.stderr)
                print("Please provide mappings using --dc-region flags, or use --force-single-region to convert all keyspaces to SingleRegionStrategy.", file=sys.stderr)
                sys.exit(1)
        
        # Setup queue and semaphore for concurrent DDL operations
        ddl_queue = Queue()
        semaphore = threading.Semaphore(MAX_CONCURRENT_DDL)
        results = []
        cdc_tables = []  # Track tables that need CDC enabled
        
        # Create progress tracker
        progress = ProgressTracker(
            total_keyspaces=len(statements['keyspaces']),
            total_types=len(statements['types']),
            total_tables=len(statements['tables'])
        )
        
        # Start worker threads (use multiple workers for better throughput)
        num_workers = min(MAX_CONCURRENT_DDL, 10)  # Use up to 10 worker threads
        workers = []
        for i in range(num_workers):
            worker = threading.Thread(target=ddl_worker, args=(session, ddl_queue, semaphore, results, is_keyspaces, progress))
            worker.daemon = True
            worker.start()
            workers.append(worker)
        
        skipped_keyspaces = []
        
        # Step 1: Create keyspaces
        for statement in statements['keyspaces']:
            transformed, missing_dcs = transform_keyspace_statement(statement, is_keyspaces, dc_region_map or {}, if_not_exists, force_single_region)
            
            # Check if transformation failed due to missing DCs
            if transformed is None:
                keyspace_name = extract_keyspace_name(statement)
                if keyspace_name:
                    skipped_keyspaces.append((keyspace_name, missing_dcs))
                    progress.update_keyspace(success=False, ignored=True)
                continue
            
            keyspace_name = extract_keyspace_name(transformed)
            ddl_stmt = DDLStatement(
                statement_type='keyspace',
                statement=transformed,
                keyspace_name=keyspace_name
            )
            ddl_queue.put(ddl_stmt)
        
        # Wait for all keyspaces to complete before moving to types
        ddl_queue.join()
        
        if skipped_keyspaces:
            print(f"\nWarning: Skipped {len(skipped_keyspaces)} keyspace(s) due to missing DC mappings:", file=sys.stderr)
            for ks_name, missing_dcs in skipped_keyspaces:
                if missing_dcs:
                    print(f"  - {ks_name}: Missing DCs: {', '.join(missing_dcs)}", file=sys.stderr)
                else:
                    print(f"  - {ks_name}: Transformation failed", file=sys.stderr)
        
        # Step 2: Create types (only after all keyspaces are done)
        for statement in statements['types']:
            # Add IF NOT EXISTS to type statement if flag is set
            type_statement = statement
            if if_not_exists and not re.search(r'\bIF\s+NOT\s+EXISTS\b', type_statement, re.IGNORECASE):
                type_statement = re.sub(r'(CREATE\s+TYPE)\s+', r'\1 IF NOT EXISTS ', type_statement, flags=re.IGNORECASE)
            
            type_info = extract_type_info(statement)
            keyspace_name = None
            type_name = None
            if type_info:
                keyspace_name, type_name = type_info
            
            ddl_stmt = DDLStatement(
                statement_type='type',
                statement=type_statement,
                keyspace_name=keyspace_name,
                type_name=type_name
            )
            ddl_queue.put(ddl_stmt)
        
        # Wait for all types to complete before moving to tables
        ddl_queue.join()
        
        # Step 3: Create tables (only after all types are done)
        for statement in statements['tables']:
            transformed, cdc_enabled = transform_table_statement(statement, is_keyspaces, warm_throughput_read, warm_throughput_write, if_not_exists)
            
            # Extract table name for logging
            table_match = re.search(r"CREATE\s+TABLE\s+(?:(\S+)\.)?(\S+)", transformed, re.IGNORECASE)
            keyspace_name = None
            table_name = None
            if table_match:
                keyspace_name = table_match.group(1)
                table_name = table_match.group(2)
                if keyspace_name:
                    keyspace_name = keyspace_name.strip('"')
                if table_name:
                    table_name = table_name.strip('"')
                    if cdc_enabled and keyspace_name and table_name:
                        cdc_tables.append((keyspace_name, table_name))
            
            ddl_stmt = DDLStatement(
                statement_type='table',
                statement=transformed,
                keyspace_name=keyspace_name,
                table_name=table_name,
                cdc_enabled=cdc_enabled
            )
            ddl_queue.put(ddl_stmt)
        
        # Wait for all tables to complete
        ddl_queue.join()
        
        # Close progress bars
        progress.close()
        
        # Stop workers by sending poison pills
        for _ in workers:
            ddl_queue.put(None)
        
        # Wait for all workers to finish
        for worker in workers:
            worker.join()
        
        # Log CDC reminder if needed
        if cdc_tables:
            print(f"\nNote: The following {len(cdc_tables)} table(s) had CDC enabled in the source schema:")
            for ks, tbl in cdc_tables:
                print(f"  - {ks}.{tbl}")
            print("CDC should be enabled manually after table creation using ALTER TABLE statements.")
        
        # Print failure summary if there are any failures
        print_failure_summary(results)
        
        # Check if there were any failures
        failed_count = sum(1 for _, success, _ in results if not success)
        if failed_count > 0:
            print(f"\nSchema import completed with {failed_count} failure(s). See failure summary above.", file=sys.stderr)
        else:
            print(f"\nSchema import completed successfully!")
        
    except Exception as e:
        print(f"Error importing schema: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        cluster.shutdown()


def main():
    """Main function with command-line argument parsing."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Schema migration tool for Apache Cassandra and AWS Keyspaces',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Export subcommand
    export_parser = subparsers.add_parser('export', help='Export schema from database to CQL file')
    export_parser.add_argument('--hosts', required=True,
                               help='Comma-separated list of Cassandra host addresses')
    export_parser.add_argument('--port', type=int, default=9042,
                               help='Cassandra port (default: 9042)')
    export_parser.add_argument('--username',
                               help='Username for authentication (optional)')
    export_parser.add_argument('--password',
                               help='Password for authentication (optional)')
    export_parser.add_argument('--keyspace',
                               help='Specific keyspace to export (optional, exports all if not specified)')
    export_parser.add_argument('--output', default='schema.cql',
                               help='Output file path (default: schema.cql)')
    export_parser.add_argument('--ssl', action='store_true',
                               help='Enable SSL connection (required for AWS Keyspaces)')
    
    # Import subcommand
    import_parser = subparsers.add_parser('import', help='Import schema from CQL file to database')
    import_parser.add_argument('--hosts', required=True,
                              help='Comma-separated list of Cassandra host addresses')
    import_parser.add_argument('--port', type=int, default=9042,
                              help='Cassandra port (default: 9042)')
    import_parser.add_argument('--username',
                              help='Username for authentication (optional)')
    import_parser.add_argument('--password',
                              help='Password for authentication (optional)')
    import_parser.add_argument('--file', '--input', dest='input_file', default='schema.cql',
                              help='Input CQL file path (default: schema.cql)')
    import_parser.add_argument('--ssl', action='store_true',
                              help='Enable SSL connection (required for AWS Keyspaces)')
    import_parser.add_argument('--dc-region', action='append', dest='dc_region',
                              help='DC to region mapping (can be specified multiple times, e.g., --dc-region dc1=us-east-1)')
    import_parser.add_argument('--warm-throughput-read', type=int, default=12000,
                              help='Warm throughput read units per second (default: 12000, minimum: 12000)')
    import_parser.add_argument('--warm-throughput-write', type=int, default=4000,
                              help='Warm throughput write units per second (default: 4000, minimum: 4000)')
    import_parser.add_argument('--if-not-exists', action='store_true',
                              help='Add IF NOT EXISTS clause to all CREATE statements (KEYSPACE, TABLE, TYPE)')
    import_parser.add_argument('--force-single-region', action='store_true',
                              help='Force all keyspaces to use SingleRegionStrategy, ignoring NetworkTopologyStrategy and DC mappings')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Parse hosts
    hosts = [h.strip() for h in args.hosts.split(',')]
    
    if args.command == 'export':
        export_schema(
            hosts=hosts,
            port=args.port,
            username=args.username,
            password=args.password,
            keyspace=args.keyspace,
            output_file=args.output,
            use_ssl=args.ssl
        )
    elif args.command == 'import':
        # Parse DC region mappings
        dc_region_map = {}
        if args.dc_region:
            for mapping in args.dc_region:
                if '=' not in mapping:
                    print(f"Error: Invalid DC-region mapping format: {mapping}. Expected format: dc_name=region_name", file=sys.stderr)
                    sys.exit(1)
                dc_name, region_name = mapping.split('=', 1)
                dc_region_map[dc_name.strip()] = region_name.strip()
        
        # Validate warm throughput parameters
        if args.warm_throughput_read < 12000:
            print(f"Error: --warm-throughput-read must be greater than or equal to 12000. Got: {args.warm_throughput_read}", file=sys.stderr)
            sys.exit(1)
        
        if args.warm_throughput_write < 4000:
            print(f"Error: --warm-throughput-write must be greater than or equal to 4000. Got: {args.warm_throughput_write}", file=sys.stderr)
            sys.exit(1)
        
        import_schema(
            hosts=hosts,
            port=args.port,
            username=args.username,
            password=args.password,
            input_file=args.input_file,
            use_ssl=args.ssl,
            dc_region_map=dc_region_map,
            warm_throughput_read=args.warm_throughput_read,
            warm_throughput_write=args.warm_throughput_write,
            if_not_exists=args.if_not_exists,
            force_single_region=args.force_single_region
        )


if __name__ == '__main__':
    main()
