#!/usr/bin/env python3

"""
Script: cassandra_tco_analyzer.py
Description: Execute  TCO analysis - captures instance data and calculates costs in one run
Usage: python ec2_tco_analyzer.py <region> <instance-id> [--snapshot-retention <days>] [--change-rate <percentage>] [--utilization <percentage>]
"""

import sys
import json
import subprocess
import argparse
import boto3
from decimal import Decimal
from datetime import datetime, timedelta

# Storage type mappings for pricing API
STORAGE_TYPES = {
    "ebs-gp2": {
        "service_code": "AmazonEC2",
        "product_family": "Storage",
        "volume_api_name": "General Purpose",
        "supports_provisioned_iops": False
    },
    "ebs-gp3": {
        "service_code": "AmazonEC2", 
        "product_family": "Storage",
        "volume_api_name": "General Purpose",
        "supports_provisioned_iops": True,
        "free_iops": 3000,
        "max_iops": 16000
    },
    "ebs-io1": {
        "service_code": "AmazonEC2",
        "product_family": "Storage", 
        "volume_api_name": "Provisioned IOPS",
        "supports_provisioned_iops": True,
        "iops_per_gb_limit": 50
    },
    "ebs-io2": {
        "service_code": "AmazonEC2",
        "product_family": "Storage",
        "volume_api_name": "Provisioned IOPS",
        "supports_provisioned_iops": True,
        "iops_per_gb_limit": 500
    }
}

def map_volume_type_to_storage_type(volume_type):
    """Map EC2 volume type to storage type used in pricing."""
    mapping = {
        'gp2': 'ebs-gp2',
        'gp3': 'ebs-gp3',
        'io1': 'ebs-io1',
        'io2': 'ebs-io2'
    }
    return mapping.get(volume_type)

def get_instance_details(instance_id, region):
    """Get EC2 instance details using boto3."""
    try:
        ec2_client = boto3.client('ec2', region_name=region)
        
        response = ec2_client.describe_instances(InstanceIds=[instance_id])
        
        if response['Reservations'] and response['Reservations'][0]['Instances']:
            return response['Reservations'][0]['Instances'][0]
        else:
            return None
    except Exception as e:
        return None

def get_volume_details(volume_id, region):
    """Get EBS volume details using boto3."""
    try:
        ec2_client = boto3.client('ec2', region_name=region)
        
        response = ec2_client.describe_volumes(VolumeIds=[volume_id])
        
        if response['Volumes']:
            return response['Volumes'][0]
        else:
            return None
    except Exception as e:
        return None

def get_network_metrics(instance_id, region):
    """Get network metrics for the instance from CloudWatch using boto3."""
    # Get last 24 hours of data
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=14)
    
    try:
        cloudwatch_client = boto3.client('cloudwatch', region_name=region)
        
        response = cloudwatch_client.get_metric_statistics(
            Namespace='AWS/EC2',
            MetricName='NetworkOut',
            Dimensions=[
                {'Name': 'InstanceId', 'Value': instance_id}
            ],
            StartTime=start_time,
            EndTime=end_time,
            Period=604800,
            Statistics=['Sum']
        )
        
        # Calculate daily average from hourly data
        datapoints = response.get('Datapoints', [])
        if datapoints:
            total_bytes = sum(point.get('Sum', 0) for point in datapoints)
            daily_gb = total_bytes / (1024 * 1024 * 1024)  # Convert to GB
            monthly_gb = (daily_gb/14) * (365/12)  # Estimate monthly
            return {
                "daily_gb_out": round(daily_gb, 4),
                "monthly_gb_out": round(monthly_gb, 2)
            }
    except Exception as e:
        pass
    
    return {"daily_gb_out": 0, "monthly_gb_out": 0}

def get_network_metrics_in(instance_id, region):
    """Get network metrics for the instance from CloudWatch using boto3."""
    # Get last 24 hours of data
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=14)
    
    try:
        cloudwatch_client = boto3.client('cloudwatch', region_name=region)
        
        response = cloudwatch_client.get_metric_statistics(
            Namespace='AWS/EC2',
            MetricName='NetworkIn',
            Dimensions=[
                {'Name': 'InstanceId', 'Value': instance_id}
            ],
            StartTime=start_time,
            EndTime=end_time,
            Period=604800,
            Statistics=['Sum']
        )
        
        # Calculate daily average from hourly data
        datapoints = response.get('Datapoints', [])
        if datapoints:
            total_bytes = sum(point.get('Sum', 0) for point in datapoints)
            daily_gb = total_bytes / (1024 * 1024 * 1024)  # Convert to GB
            monthly_gb = (daily_gb/14) * (365/12)  # Estimate monthly
            return {
                "daily_gb_in": round(daily_gb, 4),
                "monthly_gb_in": round(monthly_gb, 2)
            }
    except Exception as e:
        pass
    
    return {"daily_gb_in": 0, "monthly_gb_in": 0}

def get_ec2_price(instance_type, region_name, processor_type, tenancy="default"):
    """Get EC2 instance price using AWS Pricing API via boto3."""
    tenancyDictionary = {
        "dedicated": "Dedicated",
        "default": "Shared",
        "host": "Host"
    }

    if tenancy in tenancyDictionary:
        tenancyFilter = tenancyDictionary[tenancy]
    else:
        tenancyFilter = "Shared"
    
    # AWS Pricing API is only available in us-east-1 and ap-south-1
    try:
        pricing_client = boto3.client('pricing', region_name='us-east-1')
        
        response = pricing_client.get_products(
            ServiceCode='AmazonEC2',
            Filters=[
                {'Type': 'TERM_MATCH', 'Field': 'instanceType', 'Value': instance_type},
                 {'Type': 'TERM_MATCH', 'Field': 'operatingSystem', 'Value': 'Linux'},
                 {'Type': 'TERM_MATCH', 'Field': 'regionCode', 'Value': region_name},
                   {'Type': 'TERM_MATCH', 'Field': 'tenancy', 'Value': tenancyFilter},
                   {'Type': 'TERM_MATCH', 'Field': 'preInstalledSw', 'Value': 'NA'}
               
            ]
        )
        
        for price_list_item in response.get('PriceList', []):
            price_data = json.loads(price_list_item)
            terms = price_data.get('terms', {}).get('OnDemand', {})
            
            for term_key, term_data in terms.items():
                price_dimensions = term_data.get('priceDimensions', {})
                for dim_key, dim_data in price_dimensions.items():
                    price_per_unit = dim_data.get('pricePerUnit', {}).get('USD')
                    if price_per_unit:
                        float_price_per_unit = float(price_per_unit)
                        if float_price_per_unit > 0:
                            return float_price_per_unit
        
        return None
    except Exception as e:
        return None

def get_storage_price(storage_type):
    """Get EBS storage price using AWS Pricing API."""
    # Use known prices for common storage types
    storage_prices = {
        "ebs-gp3": 0.08,
        "ebs-gp2": 0.10,
        "ebs-io1": 0.125,
        "ebs-io2": 0.125
    }
    
    if storage_type in storage_prices:
        return storage_prices[storage_type]
    
    return 0.10  # Default fallback

def calculate_snapshot_costs(volume_size_gb, utilization_percent, change_rate_percent, retention_days):
    """Calculate snapshot backup costs."""
    # Calculate actual data size based on utilization
    actual_data_gb = volume_size_gb * (utilization_percent / 100)
    
    # Calculate daily change in GB
    daily_change_gb = actual_data_gb * (change_rate_percent / 100)
    
    # Calculate total size after retention period
    total_change_gb = daily_change_gb * retention_days
    final_size_gb = actual_data_gb + total_change_gb
    
    # EBS snapshot pricing (approximately $0.05 per GB-month)
    snapshot_price_per_gb = 0.05
    
    # Calculate costs
    monthly_cost = Decimal(str(final_size_gb * snapshot_price_per_gb))
    daily_cost = monthly_cost / Decimal('30.4')
    yearly_cost = monthly_cost * 12
    
    return {
        'initial_size': actual_data_gb,
        'final_size': final_size_gb,
        'daily_cost': daily_cost,
        'monthly_cost': monthly_cost,
        'yearly_cost': yearly_cost
    }


def calculate_data_transfer_costs(monthly_gb_out):
    """Calculate data transfer costs based on AWS pricing tiers."""
    return monthly_gb_out * 0.01

def capture_and_calculate_costs(instance_id, region, snapshot_retention=7, change_rate=5, utilization=50):
    """Capture instance data and calculate costs in one operation."""
    
    # Get instance details
    instance = get_instance_details(instance_id, region)
    if not instance:
        return None
    
    # Extract instance type
    instance_type = instance.get('InstanceType', '')
    processor_type = instance.get('Architecture', '')
    placement = instance.get('Placement', {})
    tenancy = placement.get('Tenancy', 'default') if placement else 'default'
    
    # Get attached volumes (assuming primary volume)
    storage_type = ''
    storage_size_gb = 0
    
    block_devices = instance.get('BlockDeviceMappings', [])
    if block_devices:
        # Get the first (primary) volume
        first_device = block_devices[0]
        if 'Ebs' in first_device:
            volume_id = first_device['Ebs'].get('VolumeId')
            if volume_id:
                volume = get_volume_details(volume_id, region)
                if volume:
                    storage_type = volume.get('VolumeType', '')
                    storage_size_gb = volume.get('Size', 0)
    
    # Get network metrics
    network_data = get_network_metrics(instance_id, region)
    network_data_in = get_network_metrics_in(instance_id, region)
    
    # Get pricing data
    compute_price = get_ec2_price(instance_type, region, processor_type, tenancy)
    if not compute_price:
        return None
    
    storage_type_key = map_volume_type_to_storage_type(storage_type)
    storage_price = get_storage_price(storage_type_key) if storage_type_key else 0.10
    
    # Calculate costs
    hourly = Decimal(str(compute_price))
    monthly_compute = hourly * 730
    monthly_storage = Decimal(str(storage_price)) * storage_size_gb
    monthly_network_out = Decimal(str(calculate_data_transfer_costs(network_data["monthly_gb_out"])))
    monthly_network_in = Decimal(str(calculate_data_transfer_costs(network_data_in["monthly_gb_in"])))
    monthly_network = monthly_network_out + monthly_network_in
    
    # Calculate snapshot costs
    snapshot_costs = calculate_snapshot_costs(storage_size_gb, utilization, change_rate, snapshot_retention)
    
    # Create the output structure with costs
    result = {
        "single_node": {
            "instance": {"instance_types": instance_type, "monthly_cost": float(monthly_compute)},
            "storage": {"storage_type": storage_type, "size_gb": storage_size_gb, "monthly_cost": float(monthly_storage)},
            "backup": {"backup_type": "ebs_snapshot", "size_gb": storage_size_gb, "monthly_cost": float(snapshot_costs['monthly_cost'])},
            "network_out": {
                "network_out_daily_gb": network_data["daily_gb_out"], 
                "network_out_monthly_gb": network_data["monthly_gb_out"],
                "monthly_cost": float(monthly_network_out)
            },
            "network_in": {
                "network_in_daily_gb": network_data_in["daily_gb_in"],
                "network_in_monthly_gb": network_data_in["monthly_gb_in"],
                "monthly_cost": float(monthly_network_in)
            }
        },
        "operations": {
            "operator_hours": {"operators": 2, "avg_operator_hours_per_operator_per_week": 10, "hourly_operator_cost": 100, "monthly_cost": 8000}
        }
    }
    
    # Calculate totals for display
    total_monthly = monthly_compute + monthly_storage + monthly_network + snapshot_costs['monthly_cost']
    
    costs_summary = {
        'instance_type': instance_type,
        'compute': {'hourly': hourly, 'monthly': monthly_compute},
        'storage': {'type': storage_type, 'size_gb': storage_size_gb, 'price_per_gb': Decimal(str(storage_price)), 'monthly': monthly_storage},
        'network_out': {
            'monthly_gb_out': network_data["monthly_gb_out"],
            'monthly': monthly_network_out
        },
        'network_in': {
            'monthly_gb_in': network_data_in["monthly_gb_in"],
            'monthly': monthly_network_in
        },
        'snapshots': snapshot_costs,
        'total': {'monthly': total_monthly}
    }
    
    return result, costs_summary

def display_cost_summary(costs, snapshot_params):
    """Display formatted cost summary."""
    print("\n" + "="*50)
    print("COST SUMMARY")
    print("="*50)
    
    print(f"\nInstance Type: {costs['instance_type']}")
    
    print(f"\nCOMPUTE COSTS:")
    print(f"Hourly:  ${costs['compute']['hourly']:.4f}")
    print(f"Monthly: ${costs['compute']['monthly']:.2f}")
    
    print(f"\nSTORAGE COSTS:")
    print(f"Type: {costs['storage']['type']}, Size: {costs['storage']['size_gb']} GB")
    print(f"Price per GB: ${costs['storage']['price_per_gb']:.4f}")
    print(f"Monthly: ${costs['storage']['monthly']:.2f}")
    
    if costs.get('network_out', {}).get('monthly_gb_out', 0) > 0 or costs.get('network_in', {}).get('monthly_gb_in', 0) > 0:
        print(f"\nNETWORK TRANSFER COSTS:")
        if costs.get('network_out', {}).get('monthly_gb_out', 0) > 0:
            print(f"Monthly Data Out: {costs['network_out']['monthly_gb_out']:.2f} GB")
            print(f"Monthly Cost Out: ${costs['network_out']['monthly']:.2f}")
        if costs.get('network_in', {}).get('monthly_gb_in', 0) > 0:
            print(f"Monthly Data In: {costs['network_in']['monthly_gb_in']:.2f} GB")
            print(f"Monthly Cost In: ${costs['network_in']['monthly']:.2f}")
        total_network_cost = costs.get('network_out', {}).get('monthly', 0) + costs.get('network_in', {}).get('monthly', 0)
        print(f"Total Monthly Network Cost: ${total_network_cost:.2f}")
    
    if costs['snapshots']:
        print(f"\nSNAPSHOT BACKUP COSTS:")
        print(f"Utilization:       {snapshot_params['utilization']}%")
        print(f"Daily Change Rate: {snapshot_params['change_rate']}%")
        print(f"Retention Period:  {snapshot_params['retention_days']} days")
        print(f"Initial Size: {costs['snapshots']['initial_size']:.2f} GB")
        print(f"Final Size:   {costs['snapshots']['final_size']:.2f} GB")
        print(f"Monthly: ${costs['snapshots']['monthly_cost']:.2f}")
    
    print(f"\nTOTAL COSTS:")
    print(f"Monthly: ${costs['total']['monthly']:.2f}")

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Complete EC2 TCO analysis - capture data and calculate costs')
    parser.add_argument('region', help='AWS region (e.g., us-east-1)')
    parser.add_argument('instance_id', type=str, help='EC2 instance ID')
    parser.add_argument('--snapshot-retention', type=int, default=7, help='Number of days to retain snapshots (default: 7)')
    parser.add_argument('--change-rate', type=float, default=5.0, help='Daily data change rate as percentage (default: 5)')
    parser.add_argument('--utilization', type=float, default=50.0, help='Percentage of volume that contains actual data (default: 50)')
    
    return parser.parse_args()

def main():
    args = parse_arguments()
    
    # Capture instance data and calculate costs
    result = capture_and_calculate_costs(
        args.instance_id, 
        args.region, 
        args.snapshot_retention, 
        args.change_rate, 
        args.utilization
    )
    
    if result:
        data, costs = result
        
        # Output only JSON to stdout
        print(json.dumps(data, indent=2))
    else:
        error_data = {"error": "Failed to capture instance data and calculate costs"}
        print(json.dumps(error_data, indent=2))
        sys.exit(1)

if __name__ == "__main__":
    main()
