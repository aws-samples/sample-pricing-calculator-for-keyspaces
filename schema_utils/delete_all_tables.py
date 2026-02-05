#!/usr/bin/env python3
"""
Script to delete all keyspaces (tables) from Apache Cassandra or AWS Keyspaces.
Uses the same connection parameters as schema_migration.py.

Parameters:
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
    
    --ssl (optional): Enable SSL connection (flag, no value required)
        Required for AWS Keyspaces
        Example: --ssl
    
    --dry-run (optional): Preview what would be deleted without actually deleting (flag, no value required)
        Shows list of keyspaces that would be deleted
        Example: --dry-run

Examples:
    # Dry run to preview what would be deleted from AWS Keyspaces
    python delete_all_tables.py --hosts cassandra.us-east-1.amazonaws.com --port 9142 --ssl --dry-run
    
    # Delete all keyspaces from AWS Keyspaces (using SigV4 authentication)
    python delete_all_tables.py --hosts cassandra.us-east-1.amazonaws.com --port 9142 --ssl
    
    # Delete all keyspaces from AWS Keyspaces with explicit credentials
    python delete_all_tables.py --hosts cassandra.us-east-1.amazonaws.com --port 9142 \\
        --username YOUR_ACCESS_KEY --password YOUR_SECRET_KEY --ssl
    
    # Delete all keyspaces from regular Cassandra cluster
    python delete_all_tables.py --hosts localhost --username myuser --password mypass
    
    # Delete all keyspaces from multi-node Cassandra cluster
    python delete_all_tables.py --hosts 192.168.1.100,192.168.1.101,192.168.1.102 \\
        --username myuser --password mypass
    
    # Delete all keyspaces from Cassandra with custom port
    python delete_all_tables.py --hosts 192.168.1.100 --port 9043 --username myuser --password mypass

Notes:
    - Reserved system keyspaces are automatically skipped and will not be deleted
    - The script will prompt for confirmation before deleting keyspaces
    - Deletion progress is tracked and displayed in real-time
    - The script waits for all keyspace deletions to complete before finishing
"""

import sys
import ssl
import argparse
import re
import time
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

try:
    from cassandra_sigv4.auth import SigV4AuthProvider
    SIGV4_AVAILABLE = True
except ImportError:
    SIGV4_AVAILABLE = False

try:
    import boto3
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False


# Reserved keyspaces that should not be deleted
RESERVED_KEYSPACES = {'system', 'system_schema', 'system_schema_mcs', 'system_traces', 'system_auth', 'system_multiregion_info', 'system_distributed', 'dse_auth', 'dse_security', 'dse_leases', 'dse_perf', 'dse_system', 'opscenter', 'cfs', 'cfs_archive', 'dsefs', 'hivemetastore', 'spark_system'}


def get_aws_region() -> str:
    """Get AWS region from boto3 session."""
    if not BOTO3_AVAILABLE:
        return None
    
    try:
        session = boto3.Session()
        region = session.region_name
        return region
    except Exception as e:
        print(f"Warning: Could not get AWS region from boto3: {e}", file=sys.stderr)
        return None


def get_cluster_info(session):
    """Retrieve cluster_name and dc_name from system.local."""
    try:
        result = session.execute("SELECT cluster_name, data_center FROM system.local")
        row = result.one()
        if row:
            cluster_name = row.cluster_name if hasattr(row, 'cluster_name') else None
            dc_name = row.data_center if hasattr(row, 'data_center') else None
            return cluster_name, dc_name
    except Exception as e:
        print(f"Warning: Could not retrieve cluster info: {e}", file=sys.stderr)
    return None, None


def check_keyspace_exists(session, keyspace_name: str) -> bool:
    """Check if a keyspace still exists."""
    try:
        result = session.execute(
            "SELECT keyspace_name FROM system_schema_mcs.keyspaces WHERE keyspace_name = %s",
            [keyspace_name]
        )
        return result.one() is not None
    except Exception:
        return False


def wait_for_keyspace_deletion(session, keyspace_status_dict: dict, max_wait: int = 3600):
    """
    Wait for all keyspaces to be deleted and update status.
    
    Args:
        session: Cassandra session
        keyspace_status_dict: Dictionary mapping keyspace names to their status
        max_wait: Maximum time to wait in seconds (default: 3600 = 1 hour)
    
    Returns:
        Updated keyspace_status_dict with COMPLETE status for deleted keyspaces
    """
    # Filter to only keyspaces that are in DROPPING status
    dropping_keyspaces = {ks: status for ks, status in keyspace_status_dict.items() 
                         if status == 'DROPPING'}
    
    if not dropping_keyspaces:
        return keyspace_status_dict
    
    total = len(keyspace_status_dict)
    start_time = time.time()
    check_interval = 5  # Check every 5 seconds
    
    print(f"\nWaiting for {len(dropping_keyspaces)} keyspace(s) to be deleted...")
    print("Status updates will be shown every 5 seconds.\n")
    
    while dropping_keyspaces and (time.time() - start_time) < max_wait:
        # Check each dropping keyspace
        for keyspace_name in list(dropping_keyspaces.keys()):
            if not check_keyspace_exists(session, keyspace_name):
                keyspace_status_dict[keyspace_name] = 'COMPLETE'
                del dropping_keyspaces[keyspace_name]
        
        # Calculate counts - ensure we're counting correctly
        complete_count = sum(1 for status in keyspace_status_dict.values() if status == 'COMPLETE')
        dropping_count = len(dropping_keyspaces)  # Only currently dropping keyspaces
        error_count = sum(1 for status in keyspace_status_dict.values() 
                         if status.startswith('ERROR'))
        reserved_count = sum(1 for status in keyspace_status_dict.values() if status == 'RESERVED')
        
        # Validate counts add up correctly (accounting for currently dropping keyspaces)
        # Total should equal: complete + dropping + error + reserved + would_delete (if dry run)
        calculated_total = complete_count + dropping_count + error_count + reserved_count
        would_delete_count = sum(1 for status in keyspace_status_dict.values() if status == 'WOULD_DELETE')
        if would_delete_count > 0:
            calculated_total += would_delete_count
        
        # Print status
        print(f"\rTotal: {total} | Dropping: {dropping_count} | Complete: {complete_count} | Errors: {error_count} | Reserved: {reserved_count}", 
              end='', flush=True)
        
        if not dropping_keyspaces:
            break
        
        time.sleep(check_interval)
    
    print()  # New line after status updates
    
    if dropping_keyspaces:
        elapsed = time.time() - start_time
        print(f"\nWarning: {len(dropping_keyspaces)} keyspace(s) still dropping after {elapsed:.0f} seconds.")
        print("Keyspaces may take longer to delete. You can rerun this script to check status.")
    else:
        print("\nAll keyspaces have been successfully deleted!")
    
    return keyspace_status_dict


def get_confirmation(cluster_name: str = None, dc_name: str = None, region: str = None, host: str = None) -> bool:
    """Prompt user for confirmation before deleting keyspaces."""
    message = "\n⚠️  WARNING: You are about to delete ALL keyspaces\n"
    
    if cluster_name:
        message += f"Cluster Name: {cluster_name}\n"
    if dc_name:
        message += f"Data Center: {dc_name}\n"
    if region:
        message += f"Region: {region.upper()}\n"
    if host:
        message += f"Host: {host}\n"
    
    message += "\nThis action cannot be undone!\n"
    message += "Type 'Yes' or 'Y' to confirm, anything else to cancel: "
    
    response = input(message).strip()
    return response.upper() in ['YES', 'Y']


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


def delete_all_keyspaces(hosts, port=9042, username=None, password=None, use_ssl=False, dry_run=False):
    """
    Delete all non-reserved keyspaces from the cluster.
    
    Args:
        hosts: List of host addresses or single host string
        port: Port number (default: 9042)
        username: Optional username for authentication
        password: Optional password for authentication
        use_ssl: Enable SSL connection (default: False)
        dry_run: If True, only show what would be deleted without actually deleting
    """
    # Connect first to get cluster information
    cluster, session = connect_to_cluster(hosts, port, username, password, use_ssl)
    
    try:
        # Retrieve cluster information
        cluster_name, dc_name = get_cluster_info(session)
        
        # Get AWS region from boto3
        region = get_aws_region()
        
        # Fallback to extracting from hostname if boto3 region not available
        if not region:
            primary_host = hosts[0] if isinstance(hosts, list) else hosts
            match = re.search(r'cassandra\.([^.]+)\.amazonaws\.com', primary_host)
            if match:
                region = match.group(1)
        
        # Request confirmation unless it's a dry run
        if not dry_run:
            primary_host = hosts[0] if isinstance(hosts, list) else hosts
            if not get_confirmation(cluster_name=cluster_name, dc_name=dc_name, region=region, host=primary_host if not region else None):
                print("Deletion cancelled by user.")
                return {}
    
    except Exception as e:
        print(f"Error during connection setup: {e}", file=sys.stderr)
        cluster.shutdown()
        sys.exit(1)
    
    try:
        # Retrieve a list of keyspace resources
        print("Retrieving list of keyspaces...")
        keyspace_names = session.execute('SELECT keyspace_name FROM system_schema_mcs.keyspaces')
        
        # Dictionary to hold keyspace name and current status
        keyspace_status_dict = {}
        all_keyspace_names = []  # Track all keyspaces found for debugging
        
        # Iterate through all keyspace resources
        for one_keyspace in keyspace_names:
            keyspace_name = one_keyspace.keyspace_name
            all_keyspace_names.append(keyspace_name)
            
            # Skip if already processed (avoid duplicates)
            if keyspace_name in keyspace_status_dict:
                print(f"Warning: Duplicate keyspace found: {keyspace_name}", file=sys.stderr)
                continue
            
            # Skip reserved keyspaces (case-insensitive)
            if keyspace_name.lower() in RESERVED_KEYSPACES:
                keyspace_status_dict[keyspace_name] = 'RESERVED'
                continue
            
            if dry_run:
                print(f"[DRY RUN] Would delete keyspace: {keyspace_name}")
                keyspace_status_dict[keyspace_name] = 'WOULD_DELETE'
                continue
            
            # Execute the delete command. If no exception, capture the result status as 'DROPPING'
            # This represents acknowledgement that the service has received the request.
            try:
                result = session.execute('DROP KEYSPACE ' + '"' + keyspace_name + '"')
                keyspace_status_dict[keyspace_name] = 'DROPPING'
                
                # If there is an error it's possible that:
                #       a. The keyspace is a reserved keyspace
                #       b. The keyspace is in the process of deleting
                #       c. The keyspace has already dropped
                #       d. The program has exceeded 50 concurrent DDL operations service quota
            except Exception as err:
                print(f"Error deleting keyspace {keyspace_name}: {err}", file=sys.stderr)
                print("Sleeping for 30 seconds and trying again...")
                time.sleep(30)
                try:
                    result = session.execute('DROP KEYSPACE ' + '"' + keyspace_name + '"')
                    keyspace_status_dict[keyspace_name] = 'DROPPING'
                except Exception as err:
                    keyspace_status_dict[keyspace_name] = f'ERROR: {str(err)}'
                    print(f"Error deleting keyspace {keyspace_name}: {err}", file=sys.stderr)
                
        # Debug: Print summary of what was found
        print(f"\nFound {len(all_keyspace_names)} total keyspace(s) in query results")
        print(f"Processed {len(keyspace_status_dict)} unique keyspace(s)")
        initial_reserved = sum(1 for status in keyspace_status_dict.values() if status == 'RESERVED')
        print(f"Initial reserved count: {initial_reserved}")
        
        # Wait for keyspaces to be deleted (unless dry run)
        if not dry_run:
            keyspace_status_dict = wait_for_keyspace_deletion(session, keyspace_status_dict)
        
        # Print final summary
        print("\n" + "="*60)
        print("Final Deletion Summary:")
        print("="*60)
        
        total = len(keyspace_status_dict)
        complete_count = sum(1 for status in keyspace_status_dict.values() if status == 'COMPLETE')
        dropping_count = sum(1 for status in keyspace_status_dict.values() if status == 'DROPPING')
        error_count = sum(1 for status in keyspace_status_dict.values() 
                         if status.startswith('ERROR'))
        reserved_count = sum(1 for status in keyspace_status_dict.values() if status == 'RESERVED')
        
        print(f"Total keyspaces: {total}")
        print(f"  - Complete (deleted): {complete_count}")
        print(f"  - Dropping (in progress): {dropping_count}")
        print(f"  - Errors: {error_count}")
        print(f"  - Reserved (skipped): {reserved_count}")
        
        # Debug: List reserved keyspaces if count seems incorrect
        reserved_keyspaces = [ks for ks, status in keyspace_status_dict.items() if status == 'RESERVED']
        if reserved_count > 10:  # Only show if count is suspiciously high
            print(f"\n  Reserved keyspaces ({len(reserved_keyspaces)}): {', '.join(sorted(reserved_keyspaces))}")
            print(f"  Expected reserved keyspaces: {', '.join(sorted(RESERVED_KEYSPACES))}")
            # Check for any keyspaces that shouldn't be reserved
            unexpected_reserved = [ks for ks in reserved_keyspaces if ks.lower() not in RESERVED_KEYSPACES]
            if unexpected_reserved:
                print(f"  WARNING: Unexpected reserved keyspaces found: {', '.join(sorted(unexpected_reserved))}", file=sys.stderr)
        
        if dry_run:
            would_delete_count = sum(1 for status in keyspace_status_dict.values() if status == 'WOULD_DELETE')
            print(f"  - Would delete (dry run): {would_delete_count}")
            print("\n[DRY RUN] No keyspaces were actually deleted.")
        else:
            print("\nDetailed status:")
            for keyspace_name, status in sorted(keyspace_status_dict.items()):
                if status != 'RESERVED':
                    print(f"  {keyspace_name}: {status}")
        
        return keyspace_status_dict
        
    except Exception as e:
        print(f"Error retrieving keyspaces: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        cluster.shutdown()


def main():
    """Main function with command-line argument parsing."""
    parser = argparse.ArgumentParser(
        description='Delete all non-reserved keyspaces from Apache Cassandra or AWS Keyspaces',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run to see what would be deleted
  python delete_all_tables.py --hosts cassandra.us-east-1.amazonaws.com --port 9142 --ssl --dry-run
  
  # Delete all keyspaces from Keyspaces
  python delete_all_tables.py --hosts cassandra.us-east-1.amazonaws.com --port 9142 --ssl
  
  # Delete all keyspaces from regular Cassandra
  python delete_all_tables.py --hosts localhost --username user --password pass
        """
    )
    
    parser.add_argument(
        '--hosts',
        required=True,
        help='Comma-separated list of Cassandra host addresses (e.g., "localhost" or "host1,host2")'
    )
    parser.add_argument(
        '--port',
        type=int,
        default=9042,
        help='Cassandra port (default: 9042)'
    )
    parser.add_argument(
        '--username',
        help='Username for authentication (optional). For SigV4, this is the AWS access key ID'
    )
    parser.add_argument(
        '--password',
        help='Password for authentication (optional). If not provided, SigV4 authentication will be used for AWS Keyspaces'
    )
    parser.add_argument(
        '--ssl',
        action='store_true',
        help='Enable SSL connection (required for AWS Keyspaces)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be deleted without actually deleting'
    )
    
    args = parser.parse_args()
    
    # Parse hosts
    hosts = [h.strip() for h in args.hosts.split(',')]
    
    delete_all_keyspaces(
        hosts=hosts,
        port=args.port,
        username=args.username,
        password=args.password,
        use_ssl=args.ssl,
        dry_run=args.dry_run
    )


if __name__ == '__main__':
    main()
