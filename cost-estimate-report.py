import math
from math import isclose
from decimal import *
import argparse
from typing import List
import sys
from pathlib import Path
import json
import re
from typing import Dict
from tabulate import tabulate
import json

# ## Overview
#
# This script analyzes metrics from Cassandra (or Amazon Keyspaces) by using the outputs of:
# - `nodetool tablestats`
# - `nodetool info`
# - A custom row size sampler.
#
# It then generates a report summarizing:
# - Compressed and uncompressed data sizes
# - Compression ratios
# - Read/write request units per second
# - TTL delete counts per second
#
# These metrics can help estimate Amazon Keyspaces costs and inform capacity planning.
# python3 cost-estimate-report.py \
#   --report-name "Keyspaces Cost Analysis" \
#   --table-stats-file tablestats_output.txt \
#   --info-file info_output.txt \
#   --row-size-file row_size_info.txt \
#   --number-of-nodes 6


GIGABYTE = Decimal(1000000000)

GOSSIP_OUT_BYTES = Decimal(1638)
GOSSIP_IN_BYTES = Decimal(3072)

REPLICATION_FACTOR = Decimal(3)
# Constants for Keyspaces calculations
SECONDS_PER_MONTH =  Decimal(365)/Decimal(12) * Decimal(24 * 60 * 60) 
HOURS_PER_MONTH = Decimal(365)/Decimal(12) * Decimal(24) 
WRITE_UNIT_SIZE = Decimal(1024)  # 1KB
READ_UNIT_SIZE = Decimal(4096)   # 4KB
ONE_MILLION = Decimal(1000000)

system_keyspaces = {
        'OpsCenter', 'dse_insights_local', 'solr_admin',
        'dse_system', 'HiveMetaStore', 'system_auth',
        'dse_analytics', 'system_traces', 'dse_audit', 'system',
        'dse_system_local', 'dsefs', 'system_distributed', 'system_schema',
        'dse_perf', 'dse_insights', 'system_backups', 'dse_security',
        'dse_leases', 'system_distributed_everywhere', 'reaper_db'
    }
# Parse row size file
def parse_row_size_info(lines):

    result = {}

    for line in lines:
        line = line.strip()
        # Skip lines that don't contain '=' or look like error lines
        if '=' not in line or 'NoHostAvailable' in line:
            continue

        # Split keyspace.table from the rest
        left, right = line.split('=', 1)
        key_name = left.strip()

        # The right side should be something like:
        # { lines: 1986, columns: 12, average: 849 bytes, ... }
        right = right.strip()
        if not right.startswith('{') or not right.endswith('}'):
            continue

        # Remove the braces
        inner = right[1:-1].strip()

        # Split by commas that separate fields
        # Each field looks like "lines: 1986" or "average: 849 bytes"
        fields = inner.split(',')

        value_dict = {}
        for field in fields:
            field = field.strip()
            if ': ' not in field:
                # Skip malformed fields
                continue
            k, v = field.split(':', 1)
            key = k.strip()
            val = v.strip()
            # Store as is (string), cast later as needed.
            value_dict[key] = val

        result[key_name] = value_dict

    return result
def parse_nodetool_info(lines):
    """
    Parse nodetool info output lines to extract the node's uptime in seconds.
    Returns the uptime as a Decimal.
    """
    uptime_seconds = Decimal(1)

    for line in lines:
        line = line.strip()
        # Look for the line containing "Uptime (seconds)"
        if "Uptime (seconds)" in line:
            print(f"{line}")
            # Format is something like: "Uptime (seconds): X"
            parts = line.replace('\n', ' ').replace('\\', '').split(':', 1)
            if len(parts) == 2:
                space_used_str = parts[1].strip()
                try:
                    uptime_seconds = Decimal(space_used_str)
                except Exception:

                    raise Exception(f"Error parsing uptime in seconds: {parts[1]}") 
        if "ID" in line:
            print(f"{line}")
            # Format is something like: "Uptime (seconds): X"
            id_parts = line.replace('\n', ' ').replace('\\', '').split(':', 1)
            if len(id_parts) == 2:
                id = id_parts[1].strip()

        if "Data Center" in line:
            print(f"{line}")
            # Format is something like: "Uptime (seconds): X"
            dcparts = line.replace('\n', ' ').replace('\\', '').split(':', 1)
            if len(dcparts) == 2:
                dc = dcparts[1].strip()       
                    # If parsing fails, default to one second
    # If not found, return 1 by default (1 second)
    return {'uptime_seconds':uptime_seconds, 'dc':dc, 'id':id}


def parse_nodetool_output(lines):
    """
    Parse the nodetool cfstats/tablestats output and return a dictionary of keyspaces and their tables.
    The structure returned is:
    {
        keyspace_name: {
            table_name: {
                'space_used': Decimal,
                'compression_ratio': Decimal,
                'write_count': Decimal,
                'read_count': Decimal
            },
            ...
        },
        ...
    }

    We collect:
    - space_used: The live space used by the table (in bytes)
    - compression_ratio: The SSTable compression ratio (unitless)
    - write_count: The total number of local writes recorded
    - read_count: The total number of local reads recorded

    Assumes that each table block starts after a line "Keyspace : <ks>" and "Table: <tablename>"
    When all data is collected for a table, it is stored in the keyspace's table map.
    """
    data = {}
    current_keyspace = None
    current_table = None
    space_used = None
    compression_ratio = None
    write_count = None
    read_count = None

    for line in lines:
        line = line.strip()

        # Identify when we start a new keyspace block
        if line.startswith("Keyspace"):
            # Format: "Keyspace : keyspace_name"
            parts = line.split(':', 1)
            if len(parts) == 2:
                current_keyspace = parts[1].strip()
                # Initialize the keyspace in the dictionary if new
                if current_keyspace not in data:
                    data[current_keyspace] = {}
            else:
                current_keyspace = None
            current_table = None

        # Identify when we start a new table block within the current keyspace
        if current_keyspace and (line.startswith("Table:") or line.startswith("Table (index):")):
            # Format: "Table: table_name"
            parts = line.split(':', 1)
            if len(parts) == 2:
                current_table = parts[1].strip()
                # Reset collected stats for this new table
                space_used = None
                compression_ratio = None
                write_count = None
                read_count = None

        # For lines within a table block, parse the required stats
        if current_keyspace and current_table:
            if "Space used (live):" in line:
                # Format: "Space used (live): X"
                parts = line.split(':', 1)
                if len(parts) == 2:
                    space_used_str = parts[1].strip()
                    try:
                        space_used = Decimal(space_used_str)
                    except ValueError:
                        space_used = Decimal(0)

            elif "SSTable Compression Ratio:" in line:
                # Format: "SSTable Compression Ratio: X"
                parts = line.split(':', 1)
                if len(parts) == 2:
                    ratio_str = parts[1].strip()
                    try:
                        compression_ratio = Decimal(ratio_str)
                    except ValueError:
                        compression_ratio = Decimal(1)

            elif "Local read count:" in line:
                # Format: "Local read count: X"
                parts = line.split(':', 1)
                if len(parts) == 2:
                    read_str = parts[1].strip()
                    try:
                        read_count = Decimal(read_str)
                    except ValueError:
                        read_count = Decimal(0)

            elif "Local write count:" in line:
                # Format: "Local write count: X"
                parts = line.split(':', 1)
                if len(parts) == 2:
                    write_str = parts[1].strip()
                    try:
                        write_count = Decimal(write_str)
                    except ValueError:
                        write_count = Decimal(0)

                # After identifying a write_count line, we expect that we now have all necessary metrics.
                # Only store the table data once all required fields (space_used, compression_ratio, read_count, write_count) are found.
                if (space_used is not None and
                        compression_ratio is not None and
                        read_count is not None and
                        write_count is not None):
                    data[current_keyspace][current_table] = {
                        'space_used': space_used,
                        'compression_ratio': compression_ratio,
                        'read_count': read_count,
                        'write_count': write_count
                    }

                    # Reset for the next table
                    current_table = None
                    space_used = None
                    compression_ratio = None
                    write_count = None
                    read_count = None

    return data


def _read_input(path: str) -> List[str]:
    if path == "-":
        return sys.stdin.readlines()
    p = Path(path)
    if not p.is_file():
        sys.exit(f"Error: '{path}' does not exist or is not a file.")
    return p.read_text().splitlines(True)



# ── helpers ────────────────────────────────────────────────────────────────────

_UNIT_TO_GIB = {
    "kib": 1 / (1024 * 1024),
    "mib": 1 / 1024,
    "gib": 1,
    "tib": 1024,
    "pib": 1024 * 1024,
}


def _load_to_gib(text: str) -> float:
    num, unit = text.split()
    return float(num) * _UNIT_TO_GIB[unit.lower()]


# Regex: grab IP, Load (any unit), and the 36-char UUID hostid
_NODE_RE = re.compile(
    r"""
    ^\s*[UD][NLJMRS\*]?          # UN / DN / UL … (status+state) ─ ignored
    \s+(?P<ip>\d+\.\d+\.\d+\.\d+)
    \s+(?P<load>\d+(?:\.\d+)?\s+[kmgpt]iB)
    .*?                          # tokens/owns columns – skip non-greedily
    (?P<hostid>[0-9a-fA-F\-]{36})
    """,
    re.IGNORECASE | re.VERBOSE,
)

# ── core parser ────────────────────────────────────────────────────────────────


def parse_nodetool_status(lines: List[str]) -> Dict:
    """
    Parse `nodetool status` output.

    For every datacenter it returns:
    * node_count
    * a list of nodes, each with
        - ip          (str)
        - load_gib    (float, numeric GiB)
        - hostid      (str, UUID)

    The top-level dictionary also reports the overall datacenter_count.
    """

    current_dc = None
    dc_map = {}

    for line in lines:
        line = line.rstrip("\n")

        if line.lower().startswith("datacenter:"):
            current_dc = line.split(":", 1)[1].strip()
            dc_map.setdefault(current_dc, {"node_count": 0, "nodes": []})
            continue
        if not current_dc:
            continue  # waiting for first DC header

        m = _NODE_RE.match(line)
        if m:
            ip = m.group("ip")
            load_gib = round(_load_to_gib(m.group("load")), 2)
            hostid = m.group("hostid")
            dc_entry = dc_map[current_dc]
            dc_entry["nodes"].append(
                {"ip": ip, "load_gib": load_gib, "hostid": hostid}
            )
            dc_entry["node_count"] += 1

    return {"datacenter_count": len(dc_map), "datacenters": dc_map}

def parse_cassandra_schema(scehma_content):
    """
    Returns:
        dict: A dictionary representing the parsed Cassandra schema with the following structure:

        {
        "<file_path>": {
            "<keyspace_name>": {
            "class": "<replication_class>",            # e.g., "NetworkTopologyStrategy"
            "datacenters": {
                "<dc_name>": <replication_factor>,       # e.g., "us-west-2": 3
                ...
            },
            "tables": [
                "<table_name>",                          # e.g., "users"
                ...
            ]
            },
            ...
        }
        }
    """
   
    ks_pattern = re.compile(
        r"CREATE KEYSPACE (\w+)\s+WITH replication = \{[^}]*'class': '(\w+)'(?:,\s*)?([^}]*)\}",
        re.IGNORECASE)
    table_pattern = re.compile(
        r"CREATE TABLE (\w+)\.(\w+)", re.IGNORECASE)

    # Extract keyspaces
    keyspaces = ks_pattern.findall(scehma_content)
    
    tables = table_pattern.findall(scehma_content)

    # Build dictionary
    ks_info = {}
    for ks_name, ks_class, rest in keyspaces:
        dc_repl = {}
        if ks_class == "NetworkTopologyStrategy":
            dc_entries = re.findall(r"'([^']+)':\s*'(\d+)'", rest)
            dc_repl = {dc: int(rf) for dc, rf in dc_entries}
        ks_info[ks_name] = {
            "class": ks_class,
            "datacenters": dc_repl,
            "tables": []
        }

    # Attach tables
    for ks, tbl in tables:
        if ks in ks_info:
            ks_info[ks]["tables"].append(tbl)

    return ks_info





def build_cassandra_local_set(samples, status_data, single_keyspace=None):
    """
    Build a unified data structure from samples collected from multiple nodes.
    Returns a dictionary with the following structure:
    {
        'data': {
            'keyspaces': {
                'keyspace_name': {
                    'type': 'system' or 'user',
                    'dcs': {
                        'dc_name': {
                            'number_of_nodes': Decimal,
                            'replication_factor': Decimal,
                            'tables': {
                                'table_name': {
                                    'total_compressed_bytes': Decimal,
                                    'total_uncompressed_bytes': Decimal,
                                    'avg_row_size_bytes': Decimal,
                                    'writes_monthly': Decimal,
                                    'reads_monthly': Decimal,
                                    'has_ttl': Boolean,
                                    'sample_count': Decimal,
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    """
    result = {
        'data': {
            'keyspaces': {}
        }
    }
   
    # Process each datacenter's samples
    for dc_name, dc_data in samples.items():
        for node_id, node_data in dc_data['nodes'].items():
            tablestats_data = node_data['tablestats_data']
            schema = node_data['schema']
            info_data = node_data['info_data']
            row_size_data = node_data['row_size_data']
            
            uptime_seconds = info_data['uptime_seconds']
            # Process each keyspace
            for keyspace_name, keyspace_data in tablestats_data.items():
                # Skip if filtering for a single keyspace
                if single_keyspace and keyspace_name != single_keyspace:
                    continue

                # Initialize keyspace structure if it doesn't exist
                if keyspace_name not in result['data']['keyspaces']:
                    result['data']['keyspaces'][keyspace_name] = {
                        'type': 'system' if keyspace_name in system_keyspaces else 'user',
                        'dcs': {}
                    }

                number_of_nodes = status_data['datacenters'][dc_name]['node_count']

                if keyspace_name in schema:
                    replication_factor = schema[keyspace_name]['datacenters'][dc_name]
                else:
                    replication_factor = REPLICATION_FACTOR

                # Initialize datacenter structure if it doesn't exist
                if dc_name not in result['data']['keyspaces'][keyspace_name]['dcs']:
                    result['data']['keyspaces'][keyspace_name]['dcs'][dc_name] = {
                        'number_of_nodes': number_of_nodes,
                        'replication_factor': replication_factor,
                        'tables': {}
                    }

                # Process each table in the keyspace
                for table_name, table_data in keyspace_data.items():
                    # Initialize table structure if it doesn't exist
                    if table_name not in result['data']['keyspaces'][keyspace_name]['dcs'][dc_name]['tables']:
                        result['data']['keyspaces'][keyspace_name]['dcs'][dc_name]['tables'][table_name] = {
                            'total_compressed_bytes': Decimal(0),
                            'total_uncompressed_bytes': Decimal(0),
                            'avg_row_size_bytes': Decimal(0),
                            'writes_monthly': Decimal(0),
                            'reads_monthly': Decimal(0),
                            'has_ttl': False,
                            'dcs': {}
                        }

                    # Get table data
                    space_used = table_data['space_used']  # compressed bytes
                    ratio = table_data['compression_ratio'] if table_data['space_used'] > 0 else Decimal(1)
                    read_count = table_data['read_count']
                    write_count = table_data['write_count']

                    # Calculate uncompressed size
                    uncompressed_size = space_used / ratio

                    if table_name in result['data']['keyspaces'][keyspace_name]['dcs'][dc_name]['tables'].keys():
                        # Get row size and TTL info
                        fully_qualified_table_name = f"{keyspace_name}.{table_name}"
                        if fully_qualified_table_name in row_size_data:
                            avg_str = row_size_data[fully_qualified_table_name].get('average', '0 bytes')
                            avg_number_str = avg_str.split()[0]
                            average_bytes = Decimal(avg_number_str)
                            ttl_str = row_size_data[fully_qualified_table_name].get('default-ttl', 'y')
                            has_ttl = (ttl_str.strip() == 'n')
                        else:
                            has_ttl = False
                            average_bytes = Decimal(1)
                        result['data']['keyspaces'][keyspace_name]['dcs'][dc_name]['tables'][table_name] = {
                            'total_compressed_bytes': Decimal(0),
                            'total_uncompressed_bytes': Decimal(0),
                            'avg_row_size_bytes': Decimal(0),
                            'writes_monthly': Decimal(0),
                            'reads_monthly': Decimal(0),
                            'has_ttl': has_ttl,
                            'sample_count': Decimal(0)
                        }                    
                    
                    # Update table data
                    table = result['data']['keyspaces'][keyspace_name]['dcs'][dc_name]['tables'][table_name]
                    table['total_compressed_bytes'] += space_used
                    table['total_uncompressed_bytes'] += uncompressed_size
                    table['avg_row_size_bytes'] = average_bytes
                    table['writes_monthly'] += write_count/uptime_seconds * SECONDS_PER_MONTH
                    table['reads_monthly'] += read_count/uptime_seconds * SECONDS_PER_MONTH
                    table['has_ttl'] = has_ttl
                    table['sample_count'] += Decimal(1)

                
                

            # Add missing datacenters to the keyspace data
            '''for keyspace_name, keyspace_data in result['data']['keyspaces'].items():
                for dc_name, dc_data in samples.items():
                    for node_id, node_data in dc_data['nodes'].items():
                        schema = node_data['schema']
                        if keyspace_name in schema:
                            for replicated_dc_name, replicated_dc in schema[keyspace_name]['datacenters'].items():
                                if replicated_dc_name not in keyspace_data['dcs']:
                                    keyspace_data['dcs'][replicated_dc_name] = {
                                        'number_of_nodes': status_data['datacenters'][replicated_dc_name]['node_count'],
                                        'replication_factor': replicated_dc,
                                        'tables': {}
                                    }'''

    return result

def build_keyspaces_set(cassandra_set, region_map):
    """
    Calculate totals and build a hierarchical data structure.
    Returns a dictionary with the following structure:

    {
        'data': {
            'keyspaces': {
                'keyspace_name': {
                    'regions': {
                        'region_name': {
                            'tables': {
                                'table_name': {
                                    'write_units_monthly': Decimal,
                                    'read_units_monthly': Decimal,
                                    'ttl_units_monthly': Decimal,
                                    'storage_bytes': Decimal,
                                    'backups-pitr': Boolean,
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    """
    result = {
        'data': {
            'keyspaces': {}
        }
    }

   
    # Process each keyspace
    for keyspace_name, keyspace_data in cassandra_set['data']['keyspaces'].items():
        # Skip system keyspaces
        if keyspace_data['type'] == 'system':
            continue

        # Initialize keyspace structure
        result['data']['keyspaces'][keyspace_name] = {
            'regions': {}
        }

        # Process each datacenter as a region
        for dc_name, dc_data in keyspace_data['dcs'].items():
            #region_name = dc_name
            
            region_name = region_map[dc_name]
            # Initialize region structure if it doesn't exist
            if region_name not in result['data']['keyspaces'][keyspace_name]['regions']:
                result['data']['keyspaces'][keyspace_name]['regions'][region_name] = {
                    'tables': {}
                }

            # Process each table in the datacenter
            for table_name, table_data in dc_data['tables'].items():
                
                # Calculate Keyspaces units
                row_size_bytes = table_data['avg_row_size_bytes']
                has_ttl = table_data['has_ttl']
                replication_factor = dc_data.get('replication_factor', REPLICATION_FACTOR)
                number_of_nodes = dc_data['number_of_nodes']
                number_of_samples = table_data['sample_count']

                # Calculate write units
                write_units_per_write = Decimal(1) if row_size_bytes < WRITE_UNIT_SIZE else math.ceil(row_size_bytes / WRITE_UNIT_SIZE)
                write_units_monthly = table_data['writes_monthly']/number_of_samples * write_units_per_write * number_of_nodes / replication_factor

                # Calculate read units
                read_units_per_read = Decimal(1) if row_size_bytes < READ_UNIT_SIZE else math.ceil(row_size_bytes / READ_UNIT_SIZE)
                read_units_monthly = table_data['reads_monthly']/number_of_samples * read_units_per_read * number_of_nodes / ((replication_factor -1) if replication_factor - 1 > 0 else 1)

                # Calculate TTL units (same as writes if TTL is enabled)
                ttl_units_monthly = write_units_monthly if has_ttl else Decimal(0)

                # Calculate storage bytes (uncompressed)
                storage_bytes = table_data['total_uncompressed_bytes']/number_of_samples * number_of_nodes / replication_factor

                # Store table data
                result['data']['keyspaces'][keyspace_name]['regions'][region_name]['tables'][table_name] = {
                    'write_units_monthly': write_units_monthly,
                    'read_units_monthly': read_units_monthly,
                    'ttl_units_monthly': ttl_units_monthly,
                    'storage_bytes': storage_bytes,
                    'backups-pitr': True  # Default to True for all tables
                }

    return result

def build_keyspaces_pricing(keyspaces_set, mcs_json=None):
    """
    Build a pricing data structure using region-specific rates from mcs_json.
    Returns a dictionary with the following structure:
    {
        'data': {
            'keyspaces': {
                'keyspace_name': {
                    'regions': {
                        'region_name': {
                            'tables': {
                                'table_name': {
                                    'ondemand-writes': Decimal,
                                    'ondemand-reads': Decimal,
                                    'ttl-deletes': Decimal,
                                    'storage': Decimal,
                                    'backup-pitr': Decimal
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    """
    # Helper to get price from mcs_json for a region and key
    def get_price(region, key, default=Decimal('0')):
        #try:
       #     return Decimal(mcs_json['regions'][region][key]['price'])
       # except Exception:
        return default

    # Try to map region names from keyspaces_set to mcs_json regions
    #mcs_regions = list(mcs_json['regions'].keys())
    #def map_region(region_name):
        # Try exact match first
    #    if region_name in mcs_regions:
    #        return region_name
        # Try partial match (e.g., region1 -> US West (Oregon))
    #    for mcs_region in mcs_regions:
    #        if region_name.lower() in mcs_region.lower() or mcs_region.lower() in region_name.lower():
    #            return mcs_region
        # Fallback: first region
    #    return mcs_regions['US East (N. Virginia)']

    result = {'data': {'keyspaces': {}}}
    for keyspace_name, keyspace_data in keyspaces_set['data']['keyspaces'].items():
        result['data']['keyspaces'][keyspace_name] = {'regions': {}}
        for region_name, region_data in keyspace_data['regions'].items():
            # Map region name to mcs_json region
            #mcs_region = map_region(region_name)
            mcs_region = region_name
            result['data']['keyspaces'][keyspace_name]['regions'][region_name] = {'tables': {}}
            for table_name, table_data in region_data['tables'].items():
                write_units_monthly = table_data['write_units_monthly']
                read_units_monthly = table_data['read_units_monthly']
                ttl_units_monthly = table_data['ttl_units_monthly']
                storage_bytes = table_data['storage_bytes']
                backups = table_data['backups-pitr']

                # Get region-specific prices
                ondemand_write_price = get_price(mcs_region, 'On-Demand Write Units', Decimal('0.0000006250'))
                ondemand_read_price = get_price(mcs_region, 'On-Demand Read Units', Decimal('0.0000001250'))
                price_write = get_price(mcs_region, 'Provisioned Write Units', Decimal('0.0006500000'))
                price_read = get_price(mcs_region, 'Provisioned Read Units', Decimal('0.0001300000'))
                price_ttl = get_price(mcs_region, 'Time to Live', Decimal('0.0000002750'))
                price_storage = get_price(mcs_region, 'AmazonMCS - Indexed DataStore per GB-Mo', Decimal('0.25'))
                price_pitr = get_price(mcs_region, 'Point-In-Time-Restore PITR Backup Storage per GB-Mo', Decimal('0.20'))

                # Calculate costs
                ondemand_writes = write_units_monthly * ondemand_write_price
                ondemand_reads = read_units_monthly * ondemand_read_price
                ondemand_ec_reads = read_units_monthly * ondemand_read_price/2 
                ttl_deletes = ttl_units_monthly * price_ttl
                
                provisioned_writes = write_units_monthly/SECONDS_PER_MONTH * HOURS_PER_MONTH * price_write
                provisioned_reads = read_units_monthly/SECONDS_PER_MONTH * HOURS_PER_MONTH * price_read
                provisioned_ec_reads = read_units_monthly/SECONDS_PER_MONTH  * HOURS_PER_MONTH/2 * price_read
                
                storage_cost = storage_bytes / GIGABYTE * price_storage
                backup_pitr_cost = storage_bytes / GIGABYTE * price_pitr if (backups)  else 0

                result['data']['keyspaces'][keyspace_name]['regions'][region_name]['tables'][table_name] = {
                    'ondemand-writes': ondemand_writes,
                    'ondemand-reads': ondemand_reads,
                    'ondemand-ec-reads': ondemand_ec_reads,
                    'provisioned-writes': provisioned_writes,
                    'provisioned-reads': provisioned_reads,
                    'provisioned-ec-reads': provisioned_ec_reads,
                    'ttl-deletes': ttl_deletes,
                    'storage': storage_cost,
                    'backup-pitr': backup_pitr_cost
                }
    return result

def print_keyspaces_sizes(keyspaces_set):
    """
    Print keyspaces sizes similar to the print_rows2 function, print_cassandra_sizes, but with keyspaces_set info. You should print tables,
    then keyspaces, then cluster. Keyspaces are aggregets of all tables and cluster is aggregate of all tables. 
    {
        'data': {
            'keyspaces': {
                'keyspace_name': {
                    'regions': {
                        'region_name': {
                            'tables': {
                                'table_name': {
                                    'write_units_monthly': Decimal,
                                    'read_units_monthly': Decimal,
                                    'ttl_units_monthly': Decimal,
                                    'storage_bytes': Decimal,
                                    'backups-pitr': Boolean,
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    """
    headers = ["Keyspace", "Table", "Region", "Storage Bytes", "Write Units p/s", "Read Units p/s", "TTL Units Monthly", "Backup-PITR"]
    
    # Initialize totals
    cluster_total = {
        'storage_bytes': Decimal(0),
        'write_units_monthly': Decimal(0),
        'read_units_monthly': Decimal(0),
        'ttl_units_monthly': Decimal(0)
    }
    
    table_rows = []
    keyspace_rows = []
    
    # Process each keyspace
    for keyspace_name, keyspace_data in keyspaces_set['data']['keyspaces'].items():
        keyspace_total = {
            'storage_bytes': Decimal(0),
            'write_units_monthly': Decimal(0),
            'read_units_monthly': Decimal(0),
            'ttl_units_monthly': Decimal(0)
        }
        
        # Process each region in the keyspace
        for region_name, region_data in keyspace_data['regions'].items():
            # Process each table in the region
            for table_name, table_data in region_data['tables'].items():
                # Create table row
                row = [
                    keyspace_name,
                    table_name,
                    region_name,
                    f"{table_data['storage_bytes']/GIGABYTE:,.0f}",
                    f"{table_data['write_units_monthly']/SECONDS_PER_MONTH:,.0f}",
                    f"{table_data['read_units_monthly']/SECONDS_PER_MONTH:,.0f}",
                    f"{table_data['ttl_units_monthly']/SECONDS_PER_MONTH:,.0f}",
                    "Yes" if table_data['backups-pitr'] else "No"
                ]
                table_rows.append(row)
                
                # Update keyspace totals
                keyspace_total['storage_bytes'] += table_data['storage_bytes']
                keyspace_total['write_units_monthly'] += table_data['write_units_monthly']
                keyspace_total['read_units_monthly'] += table_data['read_units_monthly']
                keyspace_total['ttl_units_monthly'] += table_data['ttl_units_monthly']
        
        # Create keyspace summary row
        keyspace_row = [
            keyspace_name,
            '',  # No table name for keyspace summary
            '',  # No region for keyspace summary
            f"{keyspace_total['storage_bytes']/GIGABYTE:,.0f}",
            f"{keyspace_total['write_units_monthly']/SECONDS_PER_MONTH:,.0f}",
            f"{keyspace_total['read_units_monthly']/SECONDS_PER_MONTH:,.0f}",
            f"{keyspace_total['ttl_units_monthly']/SECONDS_PER_MONTH:,.0f}",
            ''   # No backup-pitr for keyspace summary
        ]
        keyspace_rows.append(keyspace_row)
        
        # Update cluster totals
        cluster_total['storage_bytes'] += keyspace_total['storage_bytes']
        cluster_total['write_units_monthly'] += keyspace_total['write_units_monthly']
        cluster_total['read_units_monthly'] += keyspace_total['read_units_monthly']
        cluster_total['ttl_units_monthly'] += keyspace_total['ttl_units_monthly']
    
    # Create cluster summary row
    cluster_row = [
        'CLUSTER TOTAL',
        '',  # No table name for cluster summary
        '',  # No region for cluster summary
        f"{cluster_total['storage_bytes']/GIGABYTE:,.0f}",
        f"{cluster_total['write_units_monthly']/SECONDS_PER_MONTH:,.0f}",
        f"{cluster_total['read_units_monthly']/SECONDS_PER_MONTH:,.0f}",
        f"{cluster_total['ttl_units_monthly']/SECONDS_PER_MONTH:,.0f}",
        ''   # No backup-pitr for cluster summary
    ]
    
    # Print the tables
    print("\n-----Table Details-----")
    print(tabulate(table_rows, headers=headers, tablefmt="grid", 
                  colalign=("left", "left", "left", "right", "right", "right", "right", "center")))
    
    print("\n-----Keyspace Summary-----")
    print(tabulate(keyspace_rows, headers=headers, tablefmt="grid",
                  colalign=("left", "left", "left", "right", "right", "right", "right", "center")))
    
    print("\n-----Cluster Summary-----")
    print(tabulate([cluster_row], headers=headers, tablefmt="grid",
                  colalign=("left", "left", "left", "right", "right", "right", "right", "center")))

def print_cassnadra_sizes(cassandra_set):
    """
    Print Cassandra sizes similar to the print_rows2 function, but with cassandra_set info. You should print tables,
    then keyspaces, then cluster. Keyspaces are aggregets of all tables and cluster is aggregate of all tables. 
    aggregates for keyspace and account do not need to include ratio, row size, samople, or ttl. 
    """
    headers = ["Keyspace", "Table", "Region", "Compressed GB", "Ratio", "Uncompressed GB", "writes monthly", "reads monthly", "row size", "ttl", "sample count"]
    
    # Initialize totals
    cluster_total = {
        'compressed_gb': Decimal(0),
        'uncompressed_gb': Decimal(0),
        'writes_monthly': Decimal(0),
        'reads_monthly': Decimal(0)
    }
    
    table_rows = []
    keyspace_rows = []
    
    # Process each keyspace
    for keyspace_name, keyspace_data in cassandra_set['data']['keyspaces'].items():
        keyspace_total = {
            'compressed_gb': Decimal(0),
            'uncompressed_gb': Decimal(0),
            'writes_monthly': Decimal(0),
            'reads_monthly': Decimal(0)
        }
        
        # Process each datacenter in the keyspace
        for dc_name, dc_data in keyspace_data['dcs'].items():
            # Process each table in the datacenter
            for table_name, table_data in dc_data['tables'].items():
                # Calculate GB values
                compressed_gb = table_data['total_compressed_bytes'] / GIGABYTE
                uncompressed_gb = table_data['total_uncompressed_bytes'] / GIGABYTE
                ratio = compressed_gb / uncompressed_gb if uncompressed_gb > 0 else Decimal(0)
                
                # Create table row
                row = [
                    keyspace_name,
                    table_name,
                    dc_name,
                    f"{compressed_gb:,.2f}",
                    f"{ratio:,.2f}",
                    f"{uncompressed_gb:,.2f}",
                    f"{table_data['writes_monthly']:,.0f}",
                    f"{table_data['reads_monthly']:,.0f}",
                    f"{table_data['avg_row_size_bytes']:,.0f}",
                    "Yes" if table_data['has_ttl'] else "No",
                    f"{table_data['sample_count']:,.0f}"
                ]
                table_rows.append(row)
                
                # Update keyspace totals
                keyspace_total['compressed_gb'] += compressed_gb
                keyspace_total['uncompressed_gb'] += uncompressed_gb
                keyspace_total['writes_monthly'] += table_data['writes_monthly']
                keyspace_total['reads_monthly'] += table_data['reads_monthly']
        
        # Create keyspace summary row
        keyspace_ratio = keyspace_total['compressed_gb'] / keyspace_total['uncompressed_gb'] if keyspace_total['uncompressed_gb'] > 0 else Decimal(0)
        keyspace_row = [
            keyspace_name,
            '',  # No table name for keyspace summary
            '',  # No datacenter for keyspace summary
            f"{keyspace_total['compressed_gb']:,.2f}",
            f"{keyspace_ratio:,.2f}",
            f"{keyspace_total['uncompressed_gb']:,.2f}",
            f"{keyspace_total['writes_monthly']:,.0f}",
            f"{keyspace_total['reads_monthly']:,.0f}",
            '',  # No row size for keyspace summary
            '',  # No TTL for keyspace summary
            ''   # No sample count for keyspace summary
        ]
        keyspace_rows.append(keyspace_row)
        
        # Update cluster totals
        cluster_total['compressed_gb'] += keyspace_total['compressed_gb']
        cluster_total['uncompressed_gb'] += keyspace_total['uncompressed_gb']
        cluster_total['writes_monthly'] += keyspace_total['writes_monthly']
        cluster_total['reads_monthly'] += keyspace_total['reads_monthly']
    
    # Create cluster summary row
    cluster_ratio = cluster_total['compressed_gb'] / cluster_total['uncompressed_gb'] if cluster_total['uncompressed_gb'] > 0 else Decimal(0)
    cluster_row = [
        'CLUSTER TOTAL',
        '',  # No table name for cluster summary
        '',  # No datacenter for cluster summary
        f"{cluster_total['compressed_gb']:,.2f}",
        f"{cluster_ratio:,.2f}",
        f"{cluster_total['uncompressed_gb']:,.2f}",
        f"{cluster_total['writes_monthly']:,.0f}",
        f"{cluster_total['reads_monthly']:,.0f}",
        '',  # No row size for cluster summary
        '',  # No TTL for cluster summary
        ''   # No sample count for cluster summary
    ]
    
    # Print the tables
    print("\n-----Table Details-----")
    print(tabulate(table_rows, headers=headers, tablefmt="grid", 
                  colalign=("left", "left", "left", "right", "right", "right", "right", "right", "right", "center", "right")))
    
    print("\n-----Keyspace Summary-----")
    print(tabulate(keyspace_rows, headers=headers, tablefmt="grid",
                  colalign=("left", "left", "left", "right", "right", "right", "right", "right", "right", "center", "right")))
    
    print("\n-----Cluster Summary-----")
    print(tabulate([cluster_row], headers=headers, tablefmt="grid",
                  colalign=("left", "left", "left", "right", "right", "right", "right", "right", "right", "center", "right")))

def print_rows2(keyspaces_pricing):
    """
    Print the data in a formatted table using the totals dictionary.
   
    
    {
        'data': {
            'keyspaces': {
                'keyspace_name': {
                    'regions': {
                        'region_name': {
                            'tables': {
                                'table_name': {
                                    'ondemand-writes': Decimal,
                                    'ondemand-reads': Decimal,
                                    'ttl-deletes': Decimal,
                                    'storage': Decimal,
                                    'backup-pitr': Decimal
                                }
                            }
                        }
                    }
                }
            }
        }
    } """

    # Table headers
    table_headers = [
        "Keyspace", "Table", "Region", "Keyspaces GB",
        "On-Demand Writes", "On-Demand Reads", "On-Demand EC Reads",
        "Provisioned Writes", "Provisioned Reads", "Provisioned EC Reads",
        "TTL deletes", "Backup PITR"
    ]
    
    
    account_total = {
        'storage': Decimal(0),
        'ondemand-writes': Decimal(0),
        'ondemand-reads': Decimal(0),
        'ondemand-ec-reads': Decimal(0),
        'provisioned-writes': Decimal(0),
        'provisioned-reads': Decimal(0),
        'provisioned-ec-reads': Decimal(0),
        'ttl-deletes': Decimal(0),
        'backup-pitr': Decimal(0)
    }
    account_rows = []
    table_rows = []
    keyspace_rows = []
    for keyspace_name, keyspace_data in keyspaces_pricing['data']['keyspaces'].items():
        keyspace_total = {
            'storage': Decimal(0),
            'ondemand-writes': Decimal(0),
            'ondemand-reads': Decimal(0),
            'ondemand-ec-reads': Decimal(0),
            'provisioned-writes': Decimal(0),
            'provisioned-reads': Decimal(0),
            'provisioned-ec-reads': Decimal(0),
            'ttl-deletes': Decimal(0),
            'backup-pitr': Decimal(0)
        }
        
        for region_name, region_data in keyspace_data['regions'].items():
            for table_name, table_data in region_data['tables'].items():

                row = [
                keyspace_name,
                table_name,
                region_name,
                f"{table_data['storage']:,.0f}",
                f"{table_data['ondemand-writes']:,.2f}",
                f"{table_data['ondemand-reads']:.5f}",
                f"{table_data['ondemand-ec-reads']:,.2f}",
                f"{table_data['provisioned-writes']:,.2f}",
                f"{table_data['provisioned-reads']:.5f}",
                f"{table_data['provisioned-ec-reads']:,.2f}",
                f"{table_data['ttl-deletes']:,.2f}",
                f"{table_data['backup-pitr']:,.0f}"
                ]

                table_rows.append(row)
                keyspace_total['storage'] += table_data['storage']
                keyspace_total['ondemand-writes'] += table_data['ondemand-writes']
                keyspace_total['ondemand-reads'] += table_data['ondemand-reads']
                keyspace_total['ondemand-ec-reads'] += table_data['ondemand-ec-reads']
                keyspace_total['provisioned-writes'] += table_data['provisioned-writes']
                keyspace_total['provisioned-reads'] += table_data['provisioned-reads']
                keyspace_total['provisioned-ec-reads'] += table_data['provisioned-ec-reads']
                keyspace_total['ttl-deletes'] += table_data['ttl-deletes']
                keyspace_total['backup-pitr'] += table_data['backup-pitr']

        keyspace_row = [
            keyspace_name,
            '',
            '',
            f"{keyspace_total['storage']:,.0f}",
            f"{keyspace_total['ondemand-writes']:,.2f}",
            f"{keyspace_total['ondemand-reads']:.5f}",
            f"{keyspace_total['ondemand-ec-reads']:,.2f}",
            f"{keyspace_total['provisioned-writes']:,.2f}",
            f"{keyspace_total['provisioned-reads']:.5f}",
            f"{keyspace_total['provisioned-ec-reads']:,.2f}",
            f"{keyspace_total['ttl-deletes']:,.2f}",
            f"{keyspace_total['backup-pitr']:,.0f}"
                ]
        
        keyspace_rows.append(keyspace_row)
        account_total['storage'] += keyspace_total['storage']
        account_total['ondemand-writes'] += keyspace_total['ondemand-writes']
        account_total['ondemand-reads'] += keyspace_total['ondemand-reads']
        account_total['ondemand-ec-reads'] += keyspace_total['ondemand-ec-reads']
        account_total['provisioned-writes'] += keyspace_total['provisioned-writes']
        account_total['provisioned-reads'] += keyspace_total['provisioned-reads']
        account_total['provisioned-ec-reads'] += keyspace_total['provisioned-ec-reads']
        account_total['ttl-deletes'] += keyspace_total['ttl-deletes']
        account_total['backup-pitr'] += keyspace_total['backup-pitr']
    
    account_row = [
                'account',
                '',
                '',
                f"{account_total['storage']:,.0f}",
                f"{account_total['ondemand-writes']:,.2f}",
                f"{account_total['ondemand-reads']:.5f}",
                f"{account_total['ondemand-ec-reads']:,.2f}",
                f"{account_total['provisioned-writes']:,.2f}",
                f"{account_total['provisioned-reads']:.5f}",
                f"{account_total['provisioned-ec-reads']:,.2f}",
                f"{account_total['ttl-deletes']:,.2f}",
                f"{account_total['backup-pitr']:,.0f}"
                ]
    account_rows.append(account_row)

    print("-----Table-----")
    print(tabulate(table_rows, headers=table_headers, tablefmt="grid", 
                  colalign=("left", "left", "left", "right", "right", "right", "right", "right", "right", "right", "right")))

    print("-----Keyspace-----")
    print(tabulate(keyspace_rows, headers=table_headers, tablefmt="grid", 
                  colalign=("left", "left", "left", "right", "right", "right", "right", "right", "right", "right", "right")))

    print("-----Account-----")
    print(tabulate(account_rows, headers=table_headers, tablefmt="grid", 
                  colalign=("left", "left", "left", "right", "right", "right", "right", "right", "right", "right", "right")))

    



def calculate_totals(data, uptime_sec, row_size_data, number_of_nodes=Decimal(1), filter_keyspace=None):
    """
    Calculate totals and build a hierarchical data structure.
    Returns a dictionary with the following structure:
    {
        'system_keyspaces': set(),
        'cluster': {
            'system': {
                'compressed_bytes': Decimal,
                'uncompressed_bytes': Decimal,
                'writes_units': Decimal,
                'read_units': Decimal,
                'ttl_units': Decimal,
                'network_traffic_bytes': Decimal,
                'network_repair_bytes': Decimal
            },
            'user': {
                'compressed_bytes': Decimal,
                'uncompressed_bytes': Decimal,
                'writes_units': Decimal,
                'read_units': Decimal,
                'ttl_units': Decimal,
                'network_traffic_bytes': Decimal,
                'network_repair_bytes': Decimal
            }
        },
        'keyspaces': {
            'keyspace_name': {
                'type': 'system' or 'user',
                'compressed_bytes': Decimal,
                'uncompressed_bytes': Decimal,
                'writes_units': Decimal,
                'read_units': Decimal,
                'ttl_units': Decimal,
                'network_traffic_bytes': Decimal,
                'network_repair_bytes': Decimal,
                'tables': {
                    'table_name': {
                        'compressed_bytes': Decimal,
                        'uncompressed_bytes': Decimal,
                        'ratio': Decimal,
                        'writes_units': Decimal,
                        'read_units': Decimal,
                        'ttl_units': Decimal,
                        'row_size_bytes': Decimal,
                        'network_traffic_bytes': Decimal,
                        'network_repair_bytes': Decimal
                    }
                }
            }
        },
        'stats': {
            'total_user_tables': Decimal,
            'uptime_seconds': Decimal,
            'number_of_nodes_per_dc': Decimal,
            'tables_without_writes': {
                'uncompressed_bytes': Decimal,
                'read_units': Decimal
            },
            'tables_without_reads': {
                'uncompressed_bytes': Decimal,
                'writes_units': Decimal,
                'ttl_units': Decimal
            },
            'tables_without_writes_and_reads': {
                'uncompressed_bytes': Decimal
            },
            'monthly_network': {
                'traffic_gb': Decimal,
                'repair_gb': Decimal,
                'gossip_gb': Decimal
            }
        }
    }
    """
    

    result = {
        'system_keyspaces': system_keyspaces,
        'cluster': {
            'system': {
                'compressed_bytes': Decimal(0),
                'uncompressed_bytes': Decimal(0),
                'writes_units': Decimal(0),
                'read_units': Decimal(0),
                'ttl_units': Decimal(0),
                'network_traffic_bytes': Decimal(0),
                'network_repair_bytes': Decimal(0)
            },
            'user': {
                'compressed_bytes': Decimal(0),
                'uncompressed_bytes': Decimal(0),
                'writes_units': Decimal(0),
                'read_units': Decimal(0),
                'ttl_units': Decimal(0),
                'network_traffic_bytes': Decimal(0),
                'network_repair_bytes': Decimal(0)
            }
        },
        'keyspaces': {},
        'stats': {
            'total_user_tables': Decimal(0),
            'uptime_seconds': uptime_sec,
            'number_of_nodes_per_dc': number_of_nodes,
            'tables_without_writes': {
                'uncompressed_bytes': Decimal(0),
                'read_units': Decimal(0)
            },
            'tables_without_reads': {
                'uncompressed_bytes': Decimal(0),
                'writes_units': Decimal(0),
                'ttl_units': Decimal(0)
            },
            'tables_without_writes_and_reads': {
                'uncompressed_bytes': Decimal(0)
            },
            'monthly_network': {
                'traffic_gb': Decimal(0),
                'repair_gb': Decimal(0),
                'gossip_gb': Decimal(0)
            }
        }
    }

    keyspaces_to_print = [filter_keyspace] if filter_keyspace else data.keys()

    for keyspace in keyspaces_to_print:
        if keyspace not in data or not data[keyspace]:
            continue

        keyspace_type = 'system' if keyspace in system_keyspaces else 'user'
        result['keyspaces'][keyspace] = {
            'type': keyspace_type,
            'compressed_bytes': Decimal(0),
            'uncompressed_bytes': Decimal(0),
            'writes_units': Decimal(0),
            'read_units': Decimal(0),
            'ttl_units': Decimal(0),
            'network_traffic_bytes': Decimal(0),
            'network_repair_bytes': Decimal(0),
            'tables': {}
        }

        for (table, space_used, ratio, read_count, write_count) in data[keyspace]:
            if ratio <= 0 or isclose(ratio, 0):
                ratio = Decimal(1)

            uncompressed_size = space_used / ratio
            fully_qualified_table_name = keyspace + "." + table

            if fully_qualified_table_name in row_size_data:
                avg_str = row_size_data[fully_qualified_table_name].get('average', '0 bytes')
                avg_number_str = avg_str.split()[0]
                average_bytes = Decimal(avg_number_str)
                ttl_str = row_size_data[fully_qualified_table_name].get('default-ttl', 'y')
                has_ttl = (ttl_str.strip() == 'n')
            else:
                has_ttl = False
                average_bytes = Decimal(0)

            write_unit_per_write = Decimal(1) if average_bytes < 1024 else math.ceil(average_bytes/Decimal(1024))
            read_unit_per_read = Decimal(1) if average_bytes < 4096 else math.ceil(average_bytes/Decimal(4096))

            write_traffic_bytes = Decimal(2)/Decimal(3) * write_count * (average_bytes + Decimal(100))
            read_traffic_bytes = Decimal(2)/Decimal(3) * read_count * (average_bytes + Decimal(100))

            cass_network_traffic_bytes = write_traffic_bytes + read_traffic_bytes
            cass_network_repair_bytes = space_used * Decimal(0.05)

            write_units = write_count * write_unit_per_write
            ttl_units = write_units if has_ttl else 0
            read_units = read_count * read_unit_per_read

            
            # Store table data
            result['keyspaces'][keyspace]['tables'][table] = {
                'compressed_bytes': space_used,
                'uncompressed_bytes': uncompressed_size,
                'ratio': ratio,
                'writes_units': write_units,
                'read_units': read_units,
                'ttl_units': ttl_units,
                'row_size_bytes': average_bytes,
                'network_traffic_bytes': cass_network_traffic_bytes,
                'network_repair_bytes': cass_network_repair_bytes
            }

            # Update keyspace totals
            result['keyspaces'][keyspace]['compressed_bytes'] += space_used
            result['keyspaces'][keyspace]['uncompressed_bytes'] += uncompressed_size
            result['keyspaces'][keyspace]['writes_units'] += write_units
            result['keyspaces'][keyspace]['read_units'] += read_units
            result['keyspaces'][keyspace]['ttl_units'] += ttl_units
            result['keyspaces'][keyspace]['network_traffic_bytes'] += cass_network_traffic_bytes 
            result['keyspaces'][keyspace]['network_repair_bytes'] += cass_network_repair_bytes

            # Update cluster totals
            cluster_key = 'system' if keyspace_type == 'system' else 'user'
            result['cluster'][cluster_key]['compressed_bytes'] += space_used
            result['cluster'][cluster_key]['uncompressed_bytes'] += uncompressed_size
            result['cluster'][cluster_key]['writes_units'] += write_units
            result['cluster'][cluster_key]['read_units'] += read_units
            result['cluster'][cluster_key]['ttl_units'] += ttl_units
            result['cluster'][cluster_key]['network_traffic_bytes'] += cass_network_traffic_bytes
            result['cluster'][cluster_key]['network_repair_bytes'] += cass_network_repair_bytes

            # Update stats
            if keyspace_type == 'user':
                result['stats']['total_user_tables'] += Decimal(1)

                if write_units <= 0:
                    result['stats']['tables_without_writes']['uncompressed_bytes'] += uncompressed_size
                    result['stats']['tables_without_writes']['read_units'] += read_units
                    if read_units <= 0:
                        result['stats']['tables_without_writes_and_reads']['uncompressed_bytes'] += uncompressed_size

                if read_units <= 0:
                    result['stats']['tables_without_reads']['uncompressed_bytes'] += uncompressed_size
                    result['stats']['tables_without_reads']['writes_units'] += write_units
                    result['stats']['tables_without_reads']['ttl_units'] += ttl_units

    
    result['stats']['monthly_network']['gossip_gb'] = (
        (GOSSIP_OUT_BYTES + GOSSIP_IN_BYTES) * number_of_nodes * 365/12*24*60*60/GIGABYTE
    )

    return result

def print_rows(report_name, totals):
    """
    Print the data in a formatted table using the totals dictionary.
    """
    # Table headers
    table_headers = [
        "Keyspace", "Table", "Cassandra GB", "Ratio", "Keyspaces GB",
        "Writes Unit p/s", "Reads Unit p/s", "TTL deletes p/s", "Row size bytes",
        "Cass Network Traffic GB", "Cass Network Repair GB" 
    ]

    # List to store all rows
    all_rows = []
    #Calculate per-second rates
    number_of_nodes = totals['stats']['number_of_nodes_per_dc']
    uptime_sec = totals['stats']['uptime_seconds']
    
    # Add table rows
    for keyspace, keyspace_data in totals['keyspaces'].items():
        for table, table_data in keyspace_data['tables'].items():

            
            writes_per_sec = table_data['writes_units'] * number_of_nodes/Decimal(3)/uptime_sec
            reads_per_sec = table_data['read_units'] * number_of_nodes/Decimal(2)/uptime_sec
            ttls_per_sec = table_data['ttl_units'] * number_of_nodes/Decimal(3)/uptime_sec

            #Calculate size in GB
            compressed_gb = table_data['compressed_bytes'] * number_of_nodes/GIGABYTE
            uncompressed_gb = table_data['uncompressed_bytes'] * number_of_nodes/Decimal(3)/GIGABYTE
            network_traffic_bytes = table_data['network_traffic_bytes'] * number_of_nodes/GIGABYTE/uptime_sec * 365/12*24*60*60
            network_repair_bytes = table_data['network_repair_bytes'] * number_of_nodes/GIGABYTE/uptime_sec * 365/12*24*60*60

            row = [
                keyspace,
                table,
                f"{compressed_gb:,.2f}",
                f"{table_data['ratio']:.5f}",
                f"{uncompressed_gb:,.0f}",
                f"{writes_per_sec:,.0f}",
                f"{reads_per_sec:,.0f}",
                f"{ttls_per_sec:,.0f}",
                f"{table_data['row_size_bytes']:,.0f}",
                f"{network_traffic_bytes:,.0f}",
                f"{network_repair_bytes:,.0f}"
            ]
            all_rows.append(row)


        writes_per_sec = keyspace_data['writes_units'] * number_of_nodes/Decimal(3)/uptime_sec
        reads_per_sec = keyspace_data['read_units'] * number_of_nodes/Decimal(2)/uptime_sec
        ttls_per_sec = keyspace_data['ttl_units'] * number_of_nodes/Decimal(3)/uptime_sec
        # Add keyspace subtotal row

        #Calculate size in GB
        compressed_gb = keyspace_data['compressed_bytes'] * number_of_nodes/GIGABYTE
        uncompressed_gb = keyspace_data['uncompressed_bytes'] * number_of_nodes/Decimal(3)/GIGABYTE
        ratio = compressed_gb/uncompressed_gb if uncompressed_gb > 0 else Decimal(1)

        network_traffic_bytes = keyspace_data['network_traffic_bytes'] * number_of_nodes/uptime_sec * 365/12*24*60*60/GIGABYTE
        network_repair_bytes = keyspace_data['network_repair_bytes'] * number_of_nodes /GIGABYTE

        subtotal_row = [
            keyspace + " subtotal (GB)",
            "",
            f"{compressed_gb:,.4f}",
            f"{ratio:.5f}",
            f"{uncompressed_gb:,.4f}",
            f"{writes_per_sec:,.0f}",
            f"{reads_per_sec:,.0f}",
            f"{ttls_per_sec:,.0f}",
            "",
            f"{network_traffic_bytes:,.0f}",
            f"{network_repair_bytes:,.0f}"
        ]
        all_rows.append(subtotal_row)

    # Print the main table
    print("\nDetailed Table Statistics:")
    print(tabulate(all_rows, headers=table_headers, tablefmt="grid", 
                  colalign=("left", "left", "right", "right", "right", "right", "right", "right", "right")))

    # Print summary statistics
    summary_headers = ["Category", "Cassandra size (GB)", "Ratio", "Keyspaces size(GB)", "Writes p/s", "Reads p/s", "TTL deletes p/s", "Cass Network Traffic GB", "Cass Network Repair GB"]
    summary_rows = []

    for category in ['system', 'user']:
        cluster_data = totals['cluster'][category]

        writes_per_sec = cluster_data['writes_units'] * number_of_nodes/Decimal(3)/uptime_sec
        reads_per_sec = cluster_data['read_units'] * number_of_nodes/Decimal(2)/uptime_sec
        ttls_per_sec = cluster_data['ttl_units'] * number_of_nodes/Decimal(3)/uptime_sec

        compressed_gb = cluster_data['compressed_bytes'] * number_of_nodes/GIGABYTE
        uncompressed_gb = cluster_data['uncompressed_bytes'] * number_of_nodes/Decimal(3)/GIGABYTE
        ratio = compressed_gb/uncompressed_gb if uncompressed_gb > 0 else Decimal(1)

        network_traffic_bytes = cluster_data['network_traffic_bytes'] * number_of_nodes/uptime_sec * 365/12*24*60*60/GIGABYTE
        network_repair_bytes = cluster_data['network_repair_bytes'] * number_of_nodes /GIGABYTE

        summary_rows.append([
            category.capitalize(),
            f"{compressed_gb:,.2f}",
            f"{ratio:.5f}",
            f"{uncompressed_gb:,.2f}",
            f"{writes_per_sec:,.0f}",
            f"{reads_per_sec:,.0f}",
            f"{ttls_per_sec:,.0f}" if category == 'user' else "",
            f"{network_traffic_bytes:,.0f}",
            f"{network_repair_bytes:,.0f}"
        ])

    print("\nSummary Statistics:")
    print(tabulate(summary_rows, headers=summary_headers, tablefmt="grid",
                  colalign=("left", "right", "right", "right", "right", "right", "right")))

    # Print additional statistics
    stats_headers = ["Metric", "Value", "Description"]

    uptime_days = totals['stats']['uptime_seconds']/86400
    uncompresseed_size_without_writes_gb = totals['stats']['tables_without_writes']['uncompressed_bytes'] * number_of_nodes/Decimal(3)/GIGABYTE
    uncompresseed_size_without_reads_gb = totals['stats']['tables_without_reads']['uncompressed_bytes'] * number_of_nodes/Decimal(3)/GIGABYTE
    uncompresseed_size_without_reads_and_writes_gb = totals['stats']['tables_without_writes_and_reads']['uncompressed_bytes'] * number_of_nodes/Decimal(3)/GIGABYTE
    total_read_units_without_writes = totals['stats']['tables_without_writes']['read_units'] * number_of_nodes/Decimal(2)/uptime_sec
    total_write_units_without_reads = totals['stats']['tables_without_reads']['writes_units'] * number_of_nodes/Decimal(3)/uptime_sec
    total_ttl_units_without_reads = totals['stats']['tables_without_reads']['ttl_units'] * number_of_nodes/Decimal(3)/uptime_sec
    
    network_traffic_bytes =  (totals['cluster']['system']['network_traffic_bytes'] + totals['cluster']['user']['network_traffic_bytes']) * number_of_nodes/GIGABYTE/uptime_sec * 365/12*24*60*60
    network_repair_bytes =  (totals['cluster']['system']['network_repair_bytes'] + totals['cluster']['user']['network_repair_bytes']) * number_of_nodes/GIGABYTE
    gossip_gb = totals['stats']['monthly_network']['gossip_gb']

    
    stats_rows = [

        ["Number of tables to migrate", f"{totals['stats']['total_user_tables']:,.2f}", "number of user tables found in tablestats"],
        ["Node uptime in days", f"{uptime_days:,.2f}", "number of days the node has been up"],
        ["Uncompressed estimate for tables without writes", f"{uncompresseed_size_without_writes_gb:,.2f}", "Total size of tables without writes during the node uptime"],
        ["Total read units per second on tables without writes", f"{total_read_units_without_writes:,.2f}", "Average number of read units per second for tables without writes during the node uptime"],
        ["Uncompressed estimate for tables without reads", f"{uncompresseed_size_without_reads_gb:,.2f}", "Total size of tables without reads during the node uptime"],
        ["Total write units per second on tables without reads", f"{total_write_units_without_reads:,.2f}", "Average number of write units per second for tables without reads during the node uptime"],
        ["Total ttl units per second on tables without reads", f"{total_ttl_units_without_reads:,.2f}", "Average number of ttl units per second for tables without reads during the node uptime"],
        ["Uncompressed estimate for tables without writes and reads", f"{uncompresseed_size_without_reads_and_writes_gb:,.2f}", "Total size of tables without writes and reads during the node uptime"],
        ["Monthly network traffic estimate for writes and reads GB", f"{network_traffic_bytes:,.2f}", "Total network traffic estimate for tables without writes and reads for the month"],
        ["Monthly network traffic estimate for repair and compaction GB", f"{network_repair_bytes:,.2f}", "Total network traffic estimate for repair for the month"],
        ["Monthly network traffic estimate for gossip GB", f"{gossip_gb:,.2f}", "Total network traffic estimate for gossip for the month"]
    ]

    print("\nAdditional Statistics:")
    print(tabulate(stats_rows, headers=stats_headers, tablefmt="grid",
                  colalign=("left", "right", "left")))

 

def calcualteCassandraSizeGB (total_compressed, number_of_nodes):
    return (total_compressed * number_of_nodes)/Decimal(1000000000)

def calcualteKeyspacesSizeGB (total_uncompressed, number_of_nodes, replication_factor):
    return ((total_compressed * number_of_nodes)/Decimal(replication_factor))/Decimal(1000000000)

def calcualteWriteUnits (total_writes, number_of_nodes, replication_factor, uptime_sec):
    return ((total_writes * number_of_nodes)/Decimal(replication_factor))/uptime_sec

def calcualteTTLUnits (total_ttl, number_of_nodes, replication_factor, uptime_sec):
    return ((total_writes * number_of_nodes)/Decimal(replication_factor))/uptime_sec

def calcualteReadUnits (total_reads, number_of_nodes, replication_factor, uptime_sec):
    return ((total_writes * number_of_nodes)/Decimal(replication_factor))/uptime_sec

def decimal_to_str(obj):
    """
    Convert Decimal objects to strings for JSON serialization.
    """
    if isinstance(obj, Decimal):
        return str(obj)
    elif isinstance(obj, dict):
        return {key: decimal_to_str(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [decimal_to_str(item) for item in obj]
    return obj

def main():
    # Set decimal precision if needed
    getcontext().prec = 10

    parser = argparse.ArgumentParser(
        description='Generate a report from nodetool tablestats and nodetool info and row size sampler outputs.'
    )
    parser.add_argument('--report-name', help='Name of the generated report', default='Amazon Keyspaces sizing')
    parser.add_argument('--table-stats-file', help='Path to the nodetool tablestats output file', required=True)
    parser.add_argument('--info-file', help='Path to the nodetool info output file', required=True)
    parser.add_argument('--status-file', help='Path to the nodetool status output file')
    parser.add_argument('--row-size-file', help='Path to the file containing row size information')
    parser.add_argument('--number-of-nodes', type=Decimal,
                        help='Number of nodes in the cluster (must be a number)', default=0)
    parser.add_argument('--number-of-datacenters', type=Decimal,
                        help='Number of datacenters in the cluster (must be a number)', default=1)
    parser.add_argument('--single-keyspace', type=str, default=None,
                        help='Calculate a single keyspace. Leave out to calculate all keyspaces')
    parser.add_argument('--schema-file', type=str, default=None,
                        help='Calculate a single keyspace. Leave out to calculate all keyspaces')

    # Parse arguments
    args = parser.parse_args()

    number_of_nodes = args.number_of_nodes

    number_of_datacenters = args.number_of_datacenters
    
    report_name = args.report_name

    # Print parameters for debugging
    print("Parameters:", report_name, args.table_stats_file, args.info_file, args.row_size_file,
          args.status_file, number_of_nodes, number_of_datacenters)

    # Read the tablestats output file
    with open(args.table_stats_file, 'r') as f:
        tablestat_lines = f.readlines()

    # Read the info output file
    with open(args.info_file, 'r') as f:
        info_lines = f.readlines()

    # Read the row size file
    with open(args.row_size_file, 'r') as f:
        row_size_lines = f.readlines()
    
    if args.schema_file:
        with open(args.schema_file, 'r') as f:
            schema_content = f.read()
            schema = parse_cassandra_schema(schema_content)
    else:
        schema = None

    # Parse the nodetool cfstats data
    tablestats_data = parse_nodetool_output(tablestat_lines)
    # Parse the nodetool info data (to get uptime)
    info_data = parse_nodetool_info(info_lines)
    # Parse the rowsize data (to get uptime)
    row_size_data = parse_row_size_info(row_size_lines)
    
    if(number_of_datacenters > 0):
        status_data['datacenters'] = {
            'dc1': {
                'node_count': number_of_nodes
            }
        }
    else:
        with open(args.status_file, 'r') as f:
            status_lines = f.readlines()
    
        status_data = parse_nodetool_status(status_lines)
    
        if status_data['datacenters']:
            # Get the first datacenter's node count from the dictionary
            first_dc = next(iter(status_data['datacenters'].values()))

            number_of_nodes = first_dc['node_count']

    if(number_of_nodes == 0):
        print("Error: Number of nodes is not set. Please pass in status file using --status-file or set the number of nodes using the --number-of-nodes argument.")
        exit(1)
    
    single_keyspace = args.single_keyspace
    
    
    #totals = calculate_totals(tablestats_data, uptime_seconds, row_size_data, number_of_nodes, single_keyspace)
    
    #print_rows(report_name, totals)
    # Print the compiled data
   
    
    samples = {}
    samples[info_data['dc']] = {
        'nodes': {}
    }
    samples[info_data['dc']]['nodes'][info_data['id']] = {
        'tablestats_data': tablestats_data,
        'schema': schema,
        'info_data': info_data,
        'row_size_data': row_size_data
    }
    region_map = {info_data['dc']: "US East (N. Virginia)"}
    
    res = build_cassandra_local_set(samples, status_data, single_keyspace)

    kes_res = build_keyspaces_set(res, region_map)

    print("------Cassandra Sizes------")
    print_cassnadra_sizes(res)

    print("------Keyspaces Sizes------")
    print_keyspaces_sizes(kes_res)

    print("------Keyspaces Pricing------")
    print_rows2(build_keyspaces_pricing(kes_res))

if __name__ == "__main__":
    main()


# ── CLI glue ───────────────────────────────────────────────────────────────────




