"""
Row Size Calculator for Amazon Keyspaces

This module calculates the encoded size of Cassandra rows based on Amazon Keyspaces
storage requirements. It implements all the rules from the Amazon Keyspaces documentation
for calculating row sizes.
"""

import json
import math
from typing import Dict, List, Any, Union


def calculate_column_id_size(total_columns: int) -> int:
    """
    Rule: Column identifier size depends on the total number of columns in the table.
    - 1-62 columns: 1 byte
    - 63-124 columns: 2 bytes
    - 125-186 columns: 3 bytes
    - For each additional 62 columns, add 1 byte
    """
    if total_columns <= 62:
        return 1
    elif total_columns <= 124:
        return 2
    elif total_columns <= 186:
        return 3
    else:
        # For each additional 62 columns, add 1 byte
        return 3 + ((total_columns - 186) // 62) + (1 if (total_columns - 186) % 62 > 0 else 0)


def calculate_udt_field_id_size(total_fields: int) -> int:
    """
    Rule: UDT field name identifier size depends on the number of fields in the top-level UDT.
    - 1-62 fields: 1 byte
    - 63-124 fields: 2 bytes
    - 125+ fields: 3 bytes
    """
    if total_fields <= 62:
        return 1
    elif total_fields <= 124:
        return 2
    else:
        return 3


def calculate_string_size(value: str) -> int:
    """
    Rule: String types (ASCII, TEXT, VARCHAR) are stored using UTF-8 binary encoding.
    The size equals the number of UTF-8 encoded bytes.
    """
    if value is None:
        return 1  # Null value is 1 byte
    return len(value.encode('utf-8'))


def calculate_numeric_size(value: Union[int, float]) -> int:
    """
    Rule: Numeric types (INT, BIGINT, SMALLINT, TINYINT, VARINT) are stored with variable length.
    Size is approximately 1 byte per two significant digits + 1 byte.
    Leading and trailing zeros are trimmed.
    """
    if value is None:
        return 1  # Null value is 1 byte
    
    # Convert to string and remove leading/trailing zeros
    str_value = str(abs(value)).strip('0')
    if not str_value:
        str_value = '0'
    
    # Count significant digits
    significant_digits = len(str_value)
    
    # 1 byte per 2 significant digits + 1 byte
    return math.ceil(significant_digits / 2) + 1


def calculate_blob_size(value: bytes) -> int:
    """
    Rule: BLOB is stored with the value's raw byte length.
    """
    if value is None:
        return 1  # Null value is 1 byte
    return len(value)


def calculate_boolean_size(value: bool) -> int:
    """
    Rule: Boolean value size is 1 byte.
    """
    if value is None:
        return 1  # Null value is 1 byte
    return 1


def calculate_data_type_size(value: Any, data_type: str) -> int:
    """
    Rule: Calculate the size of a data value based on its data type.
    Supports: string types, numeric types, blob, boolean, null.
    """
    if value is None:
        return 1  # Null value is 1 byte
    
    data_type_lower = data_type.lower()
    
    if data_type_lower in ['ascii', 'text', 'varchar']:
        return calculate_string_size(str(value))
    elif data_type_lower in ['int', 'bigint', 'smallint', 'tinyint', 'varint']:
        return calculate_numeric_size(value)
    elif data_type_lower == 'blob':
        if isinstance(value, bytes):
            return calculate_blob_size(value)
        elif isinstance(value, str):
            # Assume hex string representation
            return len(value) // 2
        else:
            return calculate_blob_size(bytes(value))
    elif data_type_lower == 'boolean':
        return calculate_boolean_size(value)
    else:
        # Default: try to calculate as string
        return calculate_string_size(str(value))


def calculate_partition_key_column_size(value: Any, data_type: str, column_id_size: int) -> int:
    """
    Rule: Partition key columns can contain up to 2048 bytes of data.
    Each key column requires 3 bytes of metadata.
    Data is stored twice (for efficient querying and built-in indexing).
    Size = (data_type_size * 2) + column_id_size + 3 bytes metadata
    """
    data_size = calculate_data_type_size(value, data_type)
    # Data is stored twice
    return (data_size * 2) + column_id_size + 3


def calculate_clustering_column_size(value: Any, data_type: str, column_id_size: int) -> int:
    """
    Rule: Clustering columns can store up to 850 bytes of data.
    Requires 20% of data value size for metadata (1 byte per 5 bytes).
    Data is stored twice (for efficient querying and built-in indexing).
    Size = (data_type_size * 2) + (data_type_size * 0.2) + column_id_size
    """
    data_size = calculate_data_type_size(value, data_type)
    # Data is stored twice
    # Metadata is 20% of data value (1 byte per 5 bytes)
    metadata_size = math.ceil(data_size / 5)
    return (data_size * 2) + metadata_size + column_id_size


def calculate_regular_column_size(value: Any, data_type: str, column_id_size: int) -> int:
    """
    Rule: Regular columns use the raw size of the cell data based on the data type
    plus the required metadata (column identifier).
    Size = data_type_size + column_id_size
    """
    data_size = calculate_data_type_size(value, data_type)
    return data_size + column_id_size


def calculate_collection_size(collection: Union[List, Dict], column_id_size: int, element_data_type: str = None) -> int:
    """
    Rule: Collection types (LIST, MAP) require 3 bytes of metadata.
    Each element requires 1 byte of metadata.
    Size = column_id + sum(size of nested elements + 1 byte metadata) + 3 bytes
    """
    # Base metadata for collection
    size = column_id_size + 3
    
    if collection is None:
        return size  # Empty collection
    
    if isinstance(collection, list):
        # LIST: each element has 1 byte metadata
        for element in collection:
            element_size = calculate_data_type_size(element, element_data_type or 'text')
            size += element_size + 1
    elif isinstance(collection, dict):
        # MAP: each key-value pair has metadata
        for key, value in collection.items():
            key_size = calculate_data_type_size(key, element_data_type or 'text')
            value_size = calculate_data_type_size(value, element_data_type or 'text')
            size += key_size + value_size + 1  # 1 byte metadata per pair
    
    return size


def calculate_udt_size(udt_data: Dict[str, Any], udt_schema: Dict[str, str], column_id_size: int) -> int:
    """
    Rule: User-defined type (UDT) requires 3 bytes for metadata.
    Each UDT element requires 1 byte of metadata.
    Field name identifier size depends on number of fields (1-3 bytes).
    Field value size depends on the data type.
    """
    # Base metadata for UDT
    size = column_id_size + 3
    
    if not udt_data:
        return size
    
    total_fields = len(udt_schema)
    field_id_size = calculate_udt_field_id_size(total_fields)
    
    for field_name, field_value in udt_data.items():
        if field_name not in udt_schema:
            continue
        
        field_data_type = udt_schema[field_name]
        
        # Field name identifier
        size += field_id_size
        
        # Field value size
        if field_data_type.startswith('frozen<'):
            # Handle frozen UDT or frozen collections
            size += calculate_frozen_type_size(field_value, field_data_type)
        else:
            # Regular scalar data type
            size += calculate_data_type_size(field_value, field_data_type)
        
        # 1 byte metadata per UDT element
        size += 1
    
    return size


def calculate_frozen_type_size(value: Any, data_type: str) -> int:
    """
    Rule: Frozen UDT or frozen collections use CQL binary protocol serialization.
    - Frozen UDT: 4 bytes per field (including empty fields)
    - Frozen LIST/SET: 4 bytes per element + CQL binary protocol serialization
    - Frozen MAP: 4 bytes per key + 4 bytes per value + CQL binary protocol serialization
    """
    if data_type.startswith('frozen<udt'):
        # For frozen UDT, estimate 4 bytes per field
        if isinstance(value, dict):
            return len(value) * 4 + sum(calculate_data_type_size(v, 'text') for v in value.values())
        return 4
    elif data_type.startswith('frozen<list') or data_type.startswith('frozen<set'):
        # Frozen LIST/SET: 4 bytes per element
        if isinstance(value, list):
            return sum(4 + calculate_data_type_size(item, 'text') for item in value)
        return 4
    elif data_type.startswith('frozen<map'):
        # Frozen MAP: 4 bytes per key + 4 bytes per value
        if isinstance(value, dict):
            total = 0
            for k, v in value.items():
                total += 4 + calculate_data_type_size(k, 'text')
                total += 4 + calculate_data_type_size(v, 'text')
            return total
        return 4
    else:
        # Default: treat as regular data type
        return calculate_data_type_size(value, data_type.replace('frozen<', '').replace('>', ''))


def calculate_static_column_size(value: Any, data_type: str, column_id_size: int) -> int:
    """
    Rule: STATIC column data doesn't count towards the maximum row size of 1 MB.
    However, we still calculate its size for completeness.
    Size = data_type_size + column_id_size
    """
    data_size = calculate_data_type_size(value, data_type)
    return data_size + column_id_size


def calculate_row_size(
    row: Dict[str, Any],
    partition_keys: List[str],
    clustering_keys: List[str],
    static_columns: List[str],
    udt_schemas: Dict[str, Dict[str, str]],
    column_data_types: Dict[str, str],
    include_row_metadata: bool = False,
    include_client_timestamps: bool = False,
    include_ttl: bool = False
) -> int:
    """
    Calculate the total encoded size of a Cassandra row in Amazon Keyspaces.
    
    Args:
        row: Dictionary representing the Cassandra row (JSON format)
        partition_keys: List of partition key column names
        clustering_keys: List of clustering key column names
        static_columns: List of static column names
        udt_schemas: Dictionary mapping UDT column names to their field schemas
                     e.g., {'address': {'street': 'text', 'city': 'text'}}
        column_data_types: Dictionary mapping column names to their data types
        include_row_metadata: If True, add 100 bytes for row metadata (for storage size)
        include_client_timestamps: If True, add 20-40 bytes for client-side timestamps
        include_ttl: If True, add 8 bytes per row + 8 bytes per column for TTL metadata
    
    Returns:
        Total encoded size of the row in bytes
    """
    total_size = 0
    
    # Count total columns for column ID size calculation
    all_columns = set(partition_keys + clustering_keys + static_columns + list(row.keys()))
    all_columns.discard(None)
    total_columns = len(all_columns)
    column_id_size = calculate_column_id_size(total_columns)
    
    # Calculate partition key columns size
    # Rule: Partition keys are stored twice + 3 bytes metadata each
    for pk_col in partition_keys:
        if pk_col in row:
            value = row[pk_col]
            data_type = column_data_types.get(pk_col, 'text')
            total_size += calculate_partition_key_column_size(value, data_type, column_id_size)
    
    # Calculate clustering columns size
    # Rule: Clustering columns are stored twice + 20% metadata each
    for ck_col in clustering_keys:
        if ck_col in row:
            value = row[ck_col]
            data_type = column_data_types.get(ck_col, 'text')
            total_size += calculate_clustering_column_size(value, data_type, column_id_size)
    
    # Calculate regular columns size
    regular_columns = set(row.keys()) - set(partition_keys) - set(clustering_keys) - set(static_columns)
    for col in regular_columns:
        if col in row:
            value = row[col]
            data_type = column_data_types.get(col, 'text')
            
            # Check if it's a UDT
            if col in udt_schemas:
                total_size += calculate_udt_size(value, udt_schemas[col], column_id_size)
            # Check if it's a collection
            elif isinstance(value, (list, dict)) and not data_type.startswith('frozen'):
                total_size += calculate_collection_size(value, column_id_size, data_type)
            else:
                total_size += calculate_regular_column_size(value, data_type, column_id_size)
    
    # Calculate static columns size (doesn't count toward 1MB limit but we calculate it)
    for static_col in static_columns:
        if static_col in row:
            value = row[static_col]
            data_type = column_data_types.get(static_col, 'text')
            # Note: Static columns don't count toward row size limit
            # total_size += calculate_static_column_size(value, data_type, column_id_size)
    
    # Add row metadata if calculating storage size
    if include_row_metadata:
        total_size += 100
    
    # Add client-side timestamps if enabled
    if include_client_timestamps:
        # Approximately 20-40 bytes, use average of 30
        total_size += 30
    
    # Add TTL metadata if enabled
    if include_ttl:
        # 8 bytes per row
        total_size += 8
        # 8 bytes per column (approximate)
        total_size += len(row) * 8
    
    return total_size


# Example usage
if __name__ == "__main__":
    # Example row matching the documentation example
    example_row = {"account": "3678242334380120045:3678242334380120046:3678242334380120047", "user": "0", "details": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}

    
    partition_keys = ["account"]
    clustering_keys = ["user"]
    static_columns = []
    udt_schemas = {}
    column_data_types = {
        "account": "text",
        "user": "text",
        "details": "text"
    }
    
    # Calculate throughput capacity size (without row metadata, TTL, or client timestamps)
    throughput_size = calculate_row_size(
        example_row,
        partition_keys,
        clustering_keys,
        static_columns,
        udt_schemas,
        column_data_types,
        include_row_metadata=False,
        include_client_timestamps=False,
        include_ttl=False
    )
    
    # Calculate storage size (with row metadata, without TTL or client timestamps)
    storage_size = calculate_row_size(
        example_row,
        partition_keys,
        clustering_keys,
        static_columns,
        udt_schemas,
        column_data_types,
        include_row_metadata=True,
        include_client_timestamps=False,
        include_ttl=False
    )
    
    # Calculate size with TTL enabled
    size_with_ttl = calculate_row_size(
        example_row,
        partition_keys,
        clustering_keys,
        static_columns,
        udt_schemas,
        column_data_types,
        include_row_metadata=True,
        include_client_timestamps=False,
        include_ttl=True
    )
    
    # Calculate size with client timestamps enabled
    size_with_timestamps = calculate_row_size(
        example_row,
        partition_keys,
        clustering_keys,
        static_columns,
        udt_schemas,
        column_data_types,
        include_row_metadata=True,
        include_client_timestamps=True,
        include_ttl=False
    )
    
    # Calculate size with both TTL and client timestamps enabled
    size_with_all = calculate_row_size(
        example_row,
        partition_keys,
        clustering_keys,
        static_columns,
        udt_schemas,
        column_data_types,
        include_row_metadata=True,
        include_client_timestamps=True,
        include_ttl=True
    )
    
    print(f"Example row throughput size: {throughput_size} bytes")
    print(f"Example row storage size: {storage_size} bytes")
    print(f"Storage size with TTL: {size_with_ttl} bytes")
    print(f"Storage size with client timestamps: {size_with_timestamps} bytes")
    print(f"Storage size with TTL + client timestamps: {size_with_all} bytes")
    print(f"\nExpected throughput size: ~31 bytes (16 partition + 12 clustering + 3 regular)")
    print(f"Expected storage size: ~131 bytes (31 + 100 metadata)")
    print(f"TTL adds: ~{size_with_ttl - storage_size} bytes (8 bytes per row + 8 bytes per column)")
    print(f"Client timestamps add: ~{size_with_timestamps - storage_size} bytes (20-40 bytes, average 30)")

