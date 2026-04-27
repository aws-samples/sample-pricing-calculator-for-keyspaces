#!/usr/bin/env python3

"""
Script: cassandra_benchmark.py
Description: Small benchmark tool that introspects a Cassandra table schema from
             system tables, generates synthetic data, and performs randomized
             insert/read operations using prepared statements.

Usage:
    python cassandra_benchmark.py <keyspace> <table> [options]

Examples:
    python cassandra_benchmark.py mykeyspace mytable
    python cassandra_benchmark.py mykeyspace mytable --host 10.0.0.5 --port 9042
    python cassandra_benchmark.py mykeyspace mytable --inserts 5000 --reads 2000
    python cassandra_benchmark.py mykeyspace mytable --username admin --password secret --ssl
    python cassandra_benchmark.py mykeyspace mytable --sigv4 --sigv4-region us-east-1 --ssl --ssl-certfile sf-class2-root.crt
"""

import argparse
import random
import string
import sys
import time
import uuid
from collections import deque
from datetime import datetime, timedelta

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel
from ssl import SSLContext, PROTOCOL_TLS_CLIENT, CERT_REQUIRED


# ---------------------------------------------------------------------------
# Schema introspection
# ---------------------------------------------------------------------------

def get_table_schema(session, keyspace, table):
    """
    Query system_schema tables to retrieve column metadata, partition keys,
    and clustering keys for the target table.
    """
    columns_query = (
        "SELECT column_name, type, kind, position "
        "FROM system_schema.columns "
        "WHERE keyspace_name = %s AND table_name = %s"
    )
    rows = session.execute(columns_query, (keyspace, table))

    partition_keys = []
    clustering_keys = []
    regular_columns = []
    static_columns = []
    column_types = {}

    for row in rows:
        column_types[row.column_name] = row.type

        if row.kind == 'partition_key':
            partition_keys.append((row.position, row.column_name))
        elif row.kind == 'clustering':
            clustering_keys.append((row.position, row.column_name))
        elif row.kind == 'static':
            static_columns.append(row.column_name)
        else:
            regular_columns.append(row.column_name)

    partition_keys.sort(key=lambda x: x[0])
    clustering_keys.sort(key=lambda x: x[0])

    partition_keys = [name for _, name in partition_keys]
    clustering_keys = [name for _, name in clustering_keys]

    if not partition_keys:
        raise ValueError(f"Table {keyspace}.{table} not found or has no partition keys")

    return {
        'partition_keys': partition_keys,
        'clustering_keys': clustering_keys,
        'regular_columns': regular_columns,
        'static_columns': static_columns,
        'column_types': column_types,
    }


# ---------------------------------------------------------------------------
# Synthetic data generation
# ---------------------------------------------------------------------------

def generate_value(cql_type):
    """Generate a single random value appropriate for the given CQL data type."""
    t = cql_type.lower()

    if t in ('text', 'varchar', 'ascii'):
        return ''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(8, 32)))
    elif t == 'int':
        return random.randint(-2_147_483_648, 2_147_483_647)
    elif t == 'bigint':
        return random.randint(-2**62, 2**62)
    elif t == 'smallint':
        return random.randint(-32_768, 32_767)
    elif t == 'tinyint':
        return random.randint(-128, 127)
    elif t == 'varint':
        return random.randint(-2**62, 2**62)
    elif t == 'float':
        return round(random.uniform(-1e6, 1e6), 4)
    elif t == 'double':
        return round(random.uniform(-1e12, 1e12), 8)
    elif t in ('decimal',):
        from decimal import Decimal
        return Decimal(str(round(random.uniform(-1e6, 1e6), 6)))
    elif t == 'boolean':
        return random.choice([True, False])
    elif t == 'uuid':
        return uuid.uuid4()
    elif t == 'timeuuid':
        return uuid.uuid1()
    elif t == 'timestamp':
        base = datetime.now()
        return base - timedelta(seconds=random.randint(0, 86400 * 365))
    elif t == 'date':
        from cassandra.util import Date
        base = datetime.now()
        d = base - timedelta(days=random.randint(0, 3650))
        return Date(d)
    elif t == 'time':
        from cassandra.util import Time
        return Time(random.randint(0, 86399999999999))
    elif t == 'blob':
        return bytes(random.getrandbits(8) for _ in range(random.randint(8, 64)))
    elif t == 'inet':
        return f"{random.randint(1,254)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"
    elif t.startswith('frozen') or t.startswith('list') or t.startswith('set') or t.startswith('map'):
        return None
    else:
        return ''.join(random.choices(string.ascii_letters, k=16))


def generate_key_pool(column_names, column_types, count):
    """
    Generate a pool of unique key-value tuples for a set of key columns.
    Returns a list of dicts, each mapping column_name -> value.
    """
    seen = set()
    pool = []

    max_attempts = count * 10
    attempts = 0
    while len(pool) < count and attempts < max_attempts:
        attempts += 1
        values = {}
        hashable_parts = []
        for col in column_names:
            val = generate_value(column_types[col])
            if val is None:
                break
            values[col] = val
            hashable_parts.append((col, str(val)))
        else:
            key = tuple(hashable_parts)
            if key not in seen:
                seen.add(key)
                pool.append(values)

    return pool


def generate_regular_values(column_names, column_types):
    """Generate a single dict of random values for non-key columns."""
    values = {}
    for col in column_names:
        val = generate_value(column_types[col])
        if val is not None:
            values[col] = val
    return values


# ---------------------------------------------------------------------------
# Prepared statement builders
# ---------------------------------------------------------------------------

def build_insert_statement(session, keyspace, table, all_columns):
    """
    Build and return a prepared INSERT statement covering all provided columns.
    """
    col_names = ', '.join(all_columns)
    placeholders = ', '.join(['?'] * len(all_columns))
    cql = f"INSERT INTO {keyspace}.{table} ({col_names}) VALUES ({placeholders})"
    print(f"  Preparing INSERT: {cql}")
    try:
        stmt = session.prepare(cql)
        print(f"  INSERT prepared successfully")
        return stmt
    except Exception as e:
        print(f"  ERROR preparing INSERT: {e}")
        raise


def build_read_partition_statement(session, keyspace, table, partition_keys):
    """
    Build and return a prepared SELECT * by partition key only.
    """
    where_clause = ' AND '.join([f"{col} = ?" for col in partition_keys])
    cql = f"SELECT * FROM {keyspace}.{table} WHERE {where_clause}"
    return session.prepare(cql)


def build_read_partition_clustering_statement(session, keyspace, table, partition_keys, clustering_keys):
    """
    Build and return a prepared SELECT * by partition key + clustering key(s).
    The number of clustering key columns included varies per call.
    Returns a dict mapping the number of clustering columns used -> prepared statement.
    """
    stmts = {}
    for i in range(1, len(clustering_keys) + 1):
        cols = partition_keys + clustering_keys[:i]
        where_clause = ' AND '.join([f"{col} = ?" for col in cols])
        cql = f"SELECT * FROM {keyspace}.{table} WHERE {where_clause}"
        stmts[i] = session.prepare(cql)
    return stmts


# ---------------------------------------------------------------------------
# Benchmark runner
# ---------------------------------------------------------------------------

def run_benchmark(session, keyspace, table, schema, num_inserts, num_reads,
                  pk_pool_size, ck_pool_size):
    """
    Main benchmark loop:
    1. Generate value pools for partition keys and clustering keys
    2. Build all combinations as the insert pool
    3. Prepare statements
    4. Randomly insert (no duplicates until all combinations exhausted)
    5. Randomly read (partition-only or partition+clustering)
    """
    column_types = schema['column_types']
    partition_keys = schema['partition_keys']
    clustering_keys = schema['clustering_keys']
    regular_columns = schema['regular_columns']

    # Determine which regular columns we can write to (skip complex types)
    writable_regular = [c for c in regular_columns if generate_value(column_types[c]) is not None]

    print(f"\n--- Schema for {keyspace}.{table} ---")
    print(f"  Partition keys:  {partition_keys}")
    print(f"  Clustering keys: {clustering_keys}")
    print(f"  Regular columns: {regular_columns}")
    print(f"  Static columns:  {schema['static_columns']}")
    print(f"  Column types:    {column_types}")

    # Step 1: Generate key value pools
    print(f"\nGenerating {pk_pool_size} partition key values...")
    pk_pool = generate_key_pool(partition_keys, column_types, pk_pool_size)
    if not pk_pool:
        print("ERROR: Could not generate partition key values. Check data types.")
        return
    print(f"  Generated {len(pk_pool)} unique partition key combinations")

    ck_pool = []
    if clustering_keys:
        print(f"Generating {ck_pool_size} clustering key values...")
        ck_pool = generate_key_pool(clustering_keys, column_types, ck_pool_size)
        if not ck_pool:
            print("ERROR: Could not generate clustering key values. Check data types.")
            return
        print(f"  Generated {len(ck_pool)} unique clustering key combinations")

    # Step 2: Build all insert combinations (pk x ck)
    if ck_pool:
        all_combos = [(pk, ck) for pk in pk_pool for ck in ck_pool]
    else:
        all_combos = [(pk, {}) for pk in pk_pool]

    total_combos = len(all_combos)
    print(f"\nTotal unique row combinations: {total_combos}")

    effective_inserts = min(num_inserts, total_combos)
    print(f"Inserts to perform: {effective_inserts} (requested: {num_inserts})")
    print(f"Reads to perform:   {num_reads}")

    # Step 3: Prepare statements
    all_insert_columns = partition_keys + clustering_keys + writable_regular
    print(f"\nPreparing statements...")

    prep_insert = build_insert_statement(session, keyspace, table, all_insert_columns)
    prep_read_pk = build_read_partition_statement(session, keyspace, table, partition_keys)

    prep_read_ck = {}
    if clustering_keys:
        prep_read_ck = build_read_partition_clustering_statement(
            session, keyspace, table, partition_keys, clustering_keys
        )

    print(f"  Prepared INSERT statement")
    print(f"  Prepared {1 + len(prep_read_ck)} SELECT statements")

    # Step 4: Randomized inserts (no repeats until pool exhausted)
    print(f"\n--- Running {effective_inserts} inserts ---")
    insert_queue = deque(random.sample(all_combos, total_combos))
    inserted_rows = []

    insert_latencies = []
    insert_errors = 0
    start_time = time.time()

    for i in range(effective_inserts):
        if not insert_queue:
            # Reshuffle all combos for another pass
            insert_queue = deque(random.sample(all_combos, total_combos))

        pk_vals, ck_vals = insert_queue.popleft()
        reg_vals = generate_regular_values(writable_regular, column_types)

        bind_values = []
        for col in partition_keys:
            bind_values.append(pk_vals[col])
        for col in clustering_keys:
            bind_values.append(ck_vals[col])
        for col in writable_regular:
            bind_values.append(reg_vals.get(col))

        try:
            t0 = time.monotonic()
            session.execute(prep_insert, bind_values)
            latency_ms = (time.monotonic() - t0) * 1000
            insert_latencies.append(latency_ms)
            inserted_rows.append((pk_vals, ck_vals))
        except Exception as e:
            insert_errors += 1
            if insert_errors <= 3:
                print(f"  Insert error ({insert_errors}): {e}")

        if (i + 1) % 500 == 0 or (i + 1) == effective_inserts:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            print(f"  {i + 1}/{effective_inserts} inserts  ({rate:.0f} ops/sec)")

    insert_elapsed = time.time() - start_time

    # Step 5: Randomized reads
    print(f"\n--- Running {num_reads} reads ---")
    if not inserted_rows:
        print("  No rows inserted, skipping reads.")
        return

    read_latencies = []
    read_errors = 0
    start_time = time.time()

    for i in range(num_reads):
        pk_vals, ck_vals = random.choice(inserted_rows)

        # Randomly choose: partition-only read or partition+clustering read
        use_clustering = clustering_keys and ck_vals and random.random() < 0.5

        try:
            t0 = time.monotonic()
            if use_clustering:
                # Pick a random depth of clustering columns (1..N)
                depth = random.randint(1, len(clustering_keys))
                stmt = prep_read_ck[depth]
                bind_values = [pk_vals[col] for col in partition_keys]
                bind_values += [ck_vals[col] for col in clustering_keys[:depth]]
                session.execute(stmt, bind_values)
            else:
                bind_values = [pk_vals[col] for col in partition_keys]
                session.execute(prep_read_pk, bind_values)
            latency_ms = (time.monotonic() - t0) * 1000
            read_latencies.append(latency_ms)
        except Exception as e:
            read_errors += 1
            if read_errors <= 3:
                print(f"  Read error ({read_errors}): {e}")

        if (i + 1) % 500 == 0 or (i + 1) == num_reads:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            print(f"  {i + 1}/{num_reads} reads  ({rate:.0f} ops/sec)")

    read_elapsed = time.time() - start_time

    # Step 6: Print results
    print_results(
        effective_inserts, insert_latencies, insert_errors, insert_elapsed,
        num_reads, read_latencies, read_errors, read_elapsed
    )


def print_results(num_inserts, insert_latencies, insert_errors, insert_elapsed,
                  num_reads, read_latencies, read_errors, read_elapsed):
    """Print a summary of benchmark results."""
    print("\n" + "=" * 60)
    print("BENCHMARK RESULTS")
    print("=" * 60)

    if insert_latencies:
        insert_latencies.sort()
        print(f"\n  INSERTS:")
        print(f"    Total:      {num_inserts}")
        print(f"    Errors:     {insert_errors}")
        print(f"    Duration:   {insert_elapsed:.2f}s")
        print(f"    Throughput: {num_inserts / insert_elapsed:.0f} ops/sec")
        print(f"    Latency (ms):")
        print(f"      Min:  {insert_latencies[0]:.2f}")
        print(f"      Avg:  {sum(insert_latencies) / len(insert_latencies):.2f}")
        print(f"      P50:  {percentile(insert_latencies, 50):.2f}")
        print(f"      P90:  {percentile(insert_latencies, 90):.2f}")
        print(f"      P99:  {percentile(insert_latencies, 99):.2f}")
        print(f"      Max:  {insert_latencies[-1]:.2f}")

    if read_latencies:
        read_latencies.sort()
        print(f"\n  READS:")
        print(f"    Total:      {num_reads}")
        print(f"    Errors:     {read_errors}")
        print(f"    Duration:   {read_elapsed:.2f}s")
        print(f"    Throughput: {num_reads / read_elapsed:.0f} ops/sec")
        print(f"    Latency (ms):")
        print(f"      Min:  {read_latencies[0]:.2f}")
        print(f"      Avg:  {sum(read_latencies) / len(read_latencies):.2f}")
        print(f"      P50:  {percentile(read_latencies, 50):.2f}")
        print(f"      P90:  {percentile(read_latencies, 90):.2f}")
        print(f"      P99:  {percentile(read_latencies, 99):.2f}")
        print(f"      Max:  {read_latencies[-1]:.2f}")

    print("=" * 60)


def percentile(sorted_data, pct):
    """Return the value at the given percentile from a sorted list."""
    if not sorted_data:
        return 0
    idx = int(len(sorted_data) * pct / 100)
    idx = min(idx, len(sorted_data) - 1)
    return sorted_data[idx]


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

def create_session(args):
    """Create and return a Cassandra session from CLI arguments."""
    auth_provider = None

    if args.sigv4:
        try:
            from cassandra_sigv4.auth import SigV4AuthProvider
        except ImportError:
            print("ERROR: cassandra-sigv4 package is required for SigV4 authentication.",
                  file=sys.stderr)
            print("  Install it with: pip install cassandra-sigv4", file=sys.stderr)
            sys.exit(1)

        import boto3
        sigv4_region = args.sigv4_region
        if not sigv4_region:
            boto_session = boto3.session.Session()
            sigv4_region = boto_session.region_name
        if not sigv4_region:
            print("ERROR: SigV4 region not specified and could not be resolved from AWS config.",
                  file=sys.stderr)
            print("  Use --sigv4-region or set AWS_DEFAULT_REGION.", file=sys.stderr)
            sys.exit(1)

        boto_session = boto3.session.Session(region_name=sigv4_region)
        auth_provider = SigV4AuthProvider(boto_session)
        print(f"  Auth:    SigV4 (region: {sigv4_region})")

    elif args.username and args.password:
        auth_provider = PlainTextAuthProvider(
            username=args.username,
            password=args.password
        )

    ssl_context = None
    if args.ssl:
        ssl_context = SSLContext(PROTOCOL_TLS_CLIENT)
        if args.ssl_certfile:
            ssl_context.load_verify_locations(args.ssl_certfile)
            # Keyspaces endpoints resolve to IPs that won't match the hostname
            # in the certificate, so disable hostname check when a cert is provided.
            ssl_context.check_hostname = False
            ssl_context.verify_mode = CERT_REQUIRED
        else:
            ssl_context.check_hostname = False
            ssl_context.verify_mode = 0  # CERT_NONE

    contact_points = args.host.split(',')

    cluster = Cluster(
        contact_points=contact_points,
        port=args.port,
        auth_provider=auth_provider,
        ssl_context=ssl_context,
        protocol_version=args.protocol_version,
    )

    session = cluster.connect()
    session.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM

    return cluster, session


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Cassandra/Keyspaces small benchmark tool. '
                    'Introspects table schema, generates synthetic data, '
                    'and runs randomized insert/read operations.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument('keyspace', help='Target keyspace name')
    parser.add_argument('table', help='Target table name')

    conn = parser.add_argument_group('connection')
    conn.add_argument('--host', default='127.0.0.1',
                       help='Cassandra contact point(s), comma-separated (default: 127.0.0.1)')
    conn.add_argument('--port', type=int, default=9042,
                       help='CQL native transport port (default: 9042)')
    conn.add_argument('--username', default=None, help='Authentication username')
    conn.add_argument('--password', default=None, help='Authentication password')
    conn.add_argument('--ssl', action='store_true', help='Enable SSL/TLS')
    conn.add_argument('--ssl-certfile', default=None,
                       help='Path to CA certificate file for SSL verification')
    conn.add_argument('--protocol-version', type=int, default=4,
                       help='CQL protocol version (default: 4)')
    conn.add_argument('--sigv4', action='store_true',
                       help='Use AWS SigV4 authentication (for Amazon Keyspaces). '
                            'Requires cassandra-sigv4 and boto3 packages.')
    conn.add_argument('--sigv4-region', default=None,
                       help='AWS region for SigV4 auth (default: resolved from AWS config)')

    bench = parser.add_argument_group('benchmark')
    bench.add_argument('--inserts', type=int, default=10000,
                        help='Number of insert operations (default: 1000)')
    bench.add_argument('--reads', type=int, default=10000,
                        help='Number of read operations (default: 1000)')
    bench.add_argument('--pk-pool-size', type=int, default=100000,
                        help='Number of unique partition key values to generate (default: 1000)')
    bench.add_argument('--ck-pool-size', type=int, default=10,
                        help='Number of unique clustering key values to generate (default: 10)')

    return parser.parse_args()


def main():
    args = parse_arguments()

    print(f"Cassandra Benchmark Tool")
    print(f"  Target:  {args.keyspace}.{args.table}")
    print(f"  Host:    {args.host}:{args.port}")
    print(f"  SSL:     {args.ssl}")

    cluster = None
    try:
        cluster, session = create_session(args)
        print(f"  Connected successfully")

        schema = get_table_schema(session, args.keyspace, args.table)

        run_benchmark(
            session=session,
            keyspace=args.keyspace,
            table=args.table,
            schema=schema,
            num_inserts=args.inserts,
            num_reads=args.reads,
            pk_pool_size=args.pk_pool_size,
            ck_pool_size=args.ck_pool_size,
        )

    except KeyboardInterrupt:
        print("\nBenchmark interrupted by user.")
    except Exception as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if cluster:
            cluster.shutdown()
            print("\nConnection closed.")


if __name__ == '__main__':
    main()
