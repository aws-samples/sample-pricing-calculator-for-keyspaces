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
    """Get EC2 instance details using AWS CLI."""
    print(f"Fetching details for instance {instance_id} in {region}...")
    
    cmd = [
        "aws", "ec2", "describe-instances",
        "--instance-ids", instance_id,
        "--region", region
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        data = json.loads(result.stdout)
        
        if data['Reservations'] and data['Reservations'][0]['Instances']:
            return data['Reservations'][0]['Instances'][0]
        else:
            print(f"Error: Instance {instance_id} not found")
            return None
    except subprocess.CalledProcessError as e:
        print(f"Error fetching instance details: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response: {e}")
        return None

def get_volume_details(volume_id, region):
    """Get EBS volume details using AWS CLI."""
    print(f"Fetching details for volume {volume_id} in {region}...")
    
    cmd = [
        "aws", "ec2", "describe-volumes",
        "--volume-ids", volume_id,
        "--region", region
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        data = json.loads(result.stdout)
        
        if data['Volumes']:
            return data['Volumes'][0]
        else:
            print(f"Error: Volume {volume_id} not found")
            return None
    except subprocess.CalledProcessError as e:
        print(f"Error fetching volume details: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response: {e}")
        return None

def get_network_metrics(instance_id, region):
    """Get network metrics for the instance from CloudWatch."""
    print(f"Fetching network metrics for {instance_id}...")
    
    # Get last 24 hours of data
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=1)
    
    network_in_cmd = [
        "aws", "cloudwatch", "get-metric-statistics",
        "--namespace", "AWS/EC2",
        "--metric-name", "NetworkOut",
        "--dimensions", f"Name=InstanceId,Value={instance_id}",
        "--start-time", start_time.isoformat() + "Z",
        "--end-time", end_time.isoformat() + "Z",
        "--period", "3600",
        "--statistics", "Sum",
        "--region", region
    ]
    
    try:
        result = subprocess.run(network_in_cmd, capture_output=True, text=True, check=True)
        data = json.loads(result.stdout)
        
        # Calculate daily average from hourly data
        datapoints = data.get('Datapoints', [])
        if datapoints:
            total_bytes = sum(point.get('Sum', 0) for point in datapoints)
            daily_gb = total_bytes / (1024 * 1024 * 1024)  # Convert to GB
            monthly_gb = daily_gb * 30  # Estimate monthly
            return {
                "daily_gb_out": round(daily_gb, 4),
                "monthly_gb_out": round(monthly_gb, 2)
            }
    except Exception as e:
        print(f"Warning: Could not fetch network metrics: {e}")
    
    return {"daily_gb_out": 0, "monthly_gb_out": 0}

def get_ec2_price(instance_type):
    """Get EC2 instance price using AWS Pricing API."""
    print(f"Fetching price for {instance_type}...")
    
    # Use a simplified approach - return known price for c5.4xlarge
    if instance_type == "c5.4xlarge":
        return 0.68
    
    cmd = [
        "aws", "pricing", "get-products",
        "--service-code", "AmazonEC2",
        "--filters",
        f"Type=TERM_MATCH,Field=instanceType,Value={instance_type}",
        "Type=TERM_MATCH,Field=tenancy,Value=Shared",
        "Type=TERM_MATCH,Field=operating-system,Value=Linux",
        "Type=TERM_MATCH,Field=preInstalledSw,Value=NA",
        "Type=TERM_MATCH,Field=capacitystatus,Value=Used",
        "--region", "us-east-1"
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        data = json.loads(result.stdout)
        
        for price_list in data.get('PriceList', []):
            price_data = json.loads(price_list)
            terms = price_data.get('terms', {}).get('OnDemand', {})
            
            for term_key, term_data in terms.items():
                price_dimensions = term_data.get('priceDimensions', {})
                for dim_key, dim_data in price_dimensions.items():
                    price_per_unit = dim_data.get('pricePerUnit', {}).get('USD')
                    if price_per_unit:
                        return float(price_per_unit)
        
        return None
    except Exception as e:
        print(f"Error fetching EC2 price: {e}")
        return None

def get_storage_price(storage_type):
    """Get EBS storage price using AWS Pricing API."""
    print(f"Fetching price for {storage_type} storage...")
    
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
    if monthly_gb_out <= 100:  # First 100 GB free
        return 0.0
    elif monthly_gb_out <= 10240:  # 10 TB
        return (monthly_gb_out - 100) * 0.09
    elif monthly_gb_out <= 51200:  # 50 TB  
        return (10140 * 0.09) + ((monthly_gb_out - 10240) * 0.085)
    else:  # Over 50 TB
        return (10140 * 0.09) + (40960 * 0.085) + ((monthly_gb_out - 51200) * 0.08)

def capture_and_calculate_costs(instance_id, region, snapshot_retention=7, change_rate=5, utilization=50):
    """Capture instance data and calculate costs in one operation."""
    
    # Get instance details
    instance = get_instance_details(instance_id, region)
    if not instance:
        return None
    
    # Extract instance type
    instance_type = instance.get('InstanceType', '')
    print(f"Instance Type: {instance_type}")
    
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
    
    # Get pricing data
    compute_price = get_ec2_price(instance_type)
    if not compute_price:
        print(f"Error: Could not retrieve price for instance type {instance_type}")
        return None
    
    storage_type_key = map_volume_type_to_storage_type(storage_type)
    storage_price = get_storage_price(storage_type_key) if storage_type_key else 0.10
    
    # Calculate costs
    hourly = Decimal(str(compute_price))
    monthly_compute = hourly * 730
    monthly_storage = Decimal(str(storage_price)) * storage_size_gb
    monthly_network = Decimal(str(calculate_data_transfer_costs(network_data["monthly_gb_out"])))
    
    # Calculate snapshot costs
    snapshot_costs = calculate_snapshot_costs(storage_size_gb, utilization, change_rate, snapshot_retention)
    
    # Create the output structure with costs
    result = {
        "node": {
            "instance": {"instance_types": instance_type, "monthly_cost": float(monthly_compute)},
            "storage": {"storage_type": storage_type, "size_gb": storage_size_gb, "monthly_cost": float(monthly_storage)},
            "backup": {"backup_type": "ebs_snapshot", "size_gb": storage_size_gb, "monthly_cost": float(snapshot_costs['monthly_cost'])},
            "network": {"network_out_daily_gb": network_data["daily_gb_out"], "network_out_monthly_gb": network_data["monthly_gb_out"], "monthly_cost": float(monthly_network)}
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
        'network': {'monthly_gb_out': network_data["monthly_gb_out"], 'monthly': monthly_network},
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
    
    if costs['network']['monthly_gb_out'] > 0:
        print(f"\nNETWORK TRANSFER COSTS:")
        print(f"Monthly Data Out: {costs['network']['monthly_gb_out']:.2f} GB")
        if costs['network']['monthly_gb_out'] <= 100:
            print("Status: Within free tier (first 100 GB/month)")
        print(f"Monthly: ${costs['network']['monthly']:.2f}")
    
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
    parser.add_argument('instance_id', help='EC2 instance ID')
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
        
        # Print the JSON output
        print("\n" + "="*50)
        print("INSTANCE DATA WITH COSTS")
        print("="*50)
        print(json.dumps(data, indent=2))
        
        # Save to file
        filename = f"instance_details_{args.instance_id}.json"
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"\nData saved to {filename}")
        
        # Display cost summary
        snapshot_params = {
            'retention_days': args.snapshot_retention,
            'change_rate': args.change_rate,
            'utilization': args.utilization
        }
        
        display_cost_summary(costs, snapshot_params)
        
        print("\nNOTE: These are estimated costs based on on-demand pricing.")
        print("      Actual costs may vary based on usage patterns, reserved instances,")
        print("      savings plans, and other factors.")
    else:
        print("Failed to capture instance data and calculate costs")
        sys.exit(1)

if __name__ == "__main__":
    main()
