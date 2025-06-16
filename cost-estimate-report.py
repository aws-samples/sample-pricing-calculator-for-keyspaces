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
                    # If parsing fails, default to one second
    # If not found, return 1 by default (1 second)
    return uptime_seconds


def parse_nodetool_output(lines):
    """
    Parse the nodetool cfstats/tablestats output and return a dictionary of keyspaces and their tables.
    The structure returned is:
    {
        keyspace_name: [
            (table_name, space_used (Decimal), compression_ratio (Decimal), write_count (Decimal), read_count (Decimal)),
            ...
        ],
        ...
    }

    We collect:
    - space_used: The live space used by the table (in bytes)
    - compression_ratio: The SSTable compression ratio (unitless)
    - write_count: The total number of local writes recorded
    - read_count: The total number of local reads recorded

    Assumes that each table block starts after a line "Keyspace : <ks>" and "Table: <tablename>"
    When all data is collected for a table, it is appended to the keyspace's list.
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
                    data[current_keyspace] = []
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
                    data[current_keyspace].append(
                        (current_table, space_used, compression_ratio, read_count, write_count))

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
                'network_traffic_gb': Decimal,
                'network_repair_gb': Decimal
            },
            'user': {
                'compressed_bytes': Decimal,
                'uncompressed_bytes': Decimal,
                'writes_units': Decimal,
                'read_units': Decimal,
                'ttl_units': Decimal,
                'network_traffic_gb': Decimal,
                'network_repair_gb': Decimal
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
                'network_traffic_gb': Decimal,
                'network_repair_gb': Decimal,
                'tables': {
                    'table_name': {
                        'compressed_bytes': Decimal,
                        'uncompressed_bytes': Decimal,
                        'ratio': Decimal,
                        'writes_units': Decimal,
                        'read_units': Decimal,
                        'ttl_units': Decimal,
                        'row_size_bytes': Decimal,
                        'network_traffic_gb': Decimal,
                        'network_repair_gb': Decimal
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
    system_keyspaces = {
        'OpsCenter', 'dse_insights_local', 'solr_admin',
        'dse_system', 'HiveMetaStore', 'system_auth',
        'dse_analytics', 'system_traces', 'dse_audit', 'system',
        'dse_system_local', 'dsefs', 'system_distributed', 'system_schema',
        'dse_perf', 'dse_insights', 'system_backups', 'dse_security',
        'dse_leases', 'system_distributed_everywhere', 'reaper_db'
    }

    result = {
        'system_keyspaces': system_keyspaces,
        'cluster': {
            'system': {
                'compressed_bytes': Decimal(0),
                'uncompressed_bytes': Decimal(0),
                'writes_units': Decimal(0),
                'read_units': Decimal(0),
                'ttl_units': Decimal(0),
                'network_traffic_gb': Decimal(0),
                'network_repair_gb': Decimal(0)
            },
            'user': {
                'compressed_bytes': Decimal(0),
                'uncompressed_bytes': Decimal(0),
                'writes_units': Decimal(0),
                'read_units': Decimal(0),
                'ttl_units': Decimal(0),
                'network_traffic_gb': Decimal(0),
                'network_repair_gb': Decimal(0)
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
            'network_traffic_gb': Decimal(0),
            'network_repair_gb': Decimal(0),
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

            write_traffic_bytes = Decimal(2) * write_count * (average_bytes + Decimal(100))
            read_traffic_bytes = Decimal(2) * read_count * (average_bytes + Decimal(100))

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
                'network_traffic_gb': cass_network_traffic_bytes,
                'network_repair_gb': cass_network_repair_bytes
            }

            # Update keyspace totals
            result['keyspaces'][keyspace]['compressed_bytes'] += space_used
            result['keyspaces'][keyspace]['uncompressed_bytes'] += uncompressed_size
            result['keyspaces'][keyspace]['writes_units'] += write_units
            result['keyspaces'][keyspace]['read_units'] += read_units
            result['keyspaces'][keyspace]['ttl_units'] += ttl_units
            result['keyspaces'][keyspace]['network_traffic_gb'] += cass_network_traffic_bytes 
            result['keyspaces'][keyspace]['network_repair_gb'] += cass_network_repair_bytes

            # Update cluster totals
            cluster_key = 'system' if keyspace_type == 'system' else 'user'
            result['cluster'][cluster_key]['compressed_bytes'] += space_used
            result['cluster'][cluster_key]['uncompressed_bytes'] += uncompressed_size
            result['cluster'][cluster_key]['writes_units'] += write_units
            result['cluster'][cluster_key]['read_units'] += read_units
            result['cluster'][cluster_key]['ttl_units'] += ttl_units
            result['cluster'][cluster_key]['network_traffic_gb'] += cass_network_traffic_bytes
            result['cluster'][cluster_key]['network_repair_gb'] += cass_network_repair_bytes

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
            network_traffic_gb = table_data['network_traffic_gb'] * number_of_nodes/GIGABYTE/uptime_sec * 365/12*24*60*60
            network_repair_gb = table_data['network_repair_gb'] * number_of_nodes/GIGABYTE/uptime_sec * 365/12*24*60*60

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
                f"{network_traffic_gb:,.0f}",
                f"{network_repair_gb:,.0f}"
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

        network_traffic_gb = keyspace_data['network_traffic_gb'] * number_of_nodes/uptime_sec * 365/12*24*60*60/GIGABYTE
        network_repair_gb = keyspace_data['network_repair_gb'] * number_of_nodes /GIGABYTE

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
            f"{network_traffic_gb:,.0f}",
            f"{network_repair_gb:,.0f}"
        ]
        all_rows.append(subtotal_row)

    # Print the main table
    print("\nDetailed Table Statistics:")
    print(tabulate(all_rows, headers=table_headers, tablefmt="grid", 
                  colalign=("left", "left", "right", "right", "right", "right", "right", "right", "right", "right", "right")))

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

        network_traffic_gb = cluster_data['network_traffic_gb'] * number_of_nodes/uptime_sec * 365/12*24*60*60/GIGABYTE
        network_repair_gb = cluster_data['network_repair_gb'] * number_of_nodes /GIGABYTE

        summary_rows.append([
            category.capitalize(),
            f"{compressed_gb:,.2f}",
            f"{ratio:.5f}",
            f"{uncompressed_gb:,.2f}",
            f"{writes_per_sec:,.0f}",
            f"{reads_per_sec:,.0f}",
            f"{ttls_per_sec:,.0f}" if category == 'user' else "",
            f"{network_traffic_gb:,.0f}",
            f"{network_repair_gb:,.0f}"
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
    
    network_traffic_gb =  (totals['cluster']['system']['network_traffic_gb'] + totals['cluster']['user']['network_traffic_gb']) * number_of_nodes/GIGABYTE/uptime_sec * 365/12*24*60*60
    network_repair_gb =  (totals['cluster']['system']['network_repair_gb'] + totals['cluster']['user']['network_repair_gb']) * number_of_nodes/GIGABYTE
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
        ["Monthly network traffic estimate for writes and reads GB", f"{network_traffic_gb:,.2f}", "Total network traffic estimate for tables without writes and reads for the month"],
        ["Monthly network traffic estimate for repair and compaction GB", f"{network_repair_gb:,.2f}", "Total network traffic estimate for repair for the month"],
        ["Monthly network traffic estimate for gossip GB", f"{gossip_gb:,.2f}", "Total network traffic estimate for gossip for the month"]
    ]

    print("\nAdditional Statistics:")
    print(tabulate(stats_rows, headers=stats_headers, tablefmt="grid",
                  colalign=("left", "right", "left")))

def print_data(report_name, data, uptime_sec, row_size_data, status_data, number_of_nodes=Decimal(1), filter_keyspace=None):
    """
    Main function that coordinates the calculation and printing of data.
    """
    totals = calculate_totals(data, uptime_sec, row_size_data, number_of_nodes, filter_keyspace)
    print_rows(report_name, totals)

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
    parser.add_argument('--row-size-file', help='Path to the file containing row size information', required=True)
    parser.add_argument('--number-of-nodes', type=Decimal,
                        help='Number of nodes in the cluster (must be a number)', default=0)
    parser.add_argument('--single-keyspace', type=str, default=None,
                        help='Calculate a single keyspace. Leave out to calculate all keyspaces')

    # Parse arguments
    args = parser.parse_args()

    number_of_nodes = args.number_of_nodes
    
    report_name = args.report_name

    # Print parameters for debugging
    print("Parameters:", report_name, args.table_stats_file, args.info_file, args.row_size_file,
          args.status_file, number_of_nodes)

    # Read the tablestats output file
    with open(args.table_stats_file, 'r') as f:
        tablestat_lines = f.readlines()

    # Read the info output file
    with open(args.info_file, 'r') as f:
        info_lines = f.readlines()

    # Read the row size file
    with open(args.row_size_file, 'r') as f:
        row_size_lines = f.readlines()

    with open(args.status_file, 'r') as f:
        status_lines = f.readlines()

    # Parse the nodetool cfstats data
    tablestats_data = parse_nodetool_output(tablestat_lines)
    # Parse the nodetool info data (to get uptime)
    uptime_seconds = parse_nodetool_info(info_lines)
    # Parse the rowsize data (to get uptime)
    row_size_data = parse_row_size_info(row_size_lines)

    status_data = parse_nodetool_status(status_lines)
    print(status_data)
    number_of_nodes = 0
    if status_data['datacenters']:
        # Get the first datacenter's node count from the dictionary
        first_dc = next(iter(status_data['datacenters'].values()))
        print(first_dc)
        number_of_nodes = first_dc['node_count']

    if(args.number_of_nodes > 0):
        number_of_nodes = args.number_of_nodes

    if(number_of_nodes == 0):
        print("Error: Number of nodes is not set. Please pass in status file using --status-file or set the number of nodes using the --number-of-nodes argument.")
        exit(1)

    # Print the compiled data
    print_data(report_name, tablestats_data, uptime_seconds, row_size_data, status_data, number_of_nodes)

if __name__ == "__main__":
    main()


# ── CLI glue ───────────────────────────────────────────────────────────────────




