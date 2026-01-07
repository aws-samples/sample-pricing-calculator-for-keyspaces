 #!/bin/bash

curl --compressed https://b0.p.awsstatic.com/pricing/2.0/meteredUnitMaps/mcs/USD/current/mcs.json | jq '.' > src/data/mcs.json

# Get all savings plans with pagination
{
  allResults=()
  nextToken=""
  
  while true; do
    if [ -z "$nextToken" ]; then
      response=$(aws savingsplans describe-savings-plans-offering-rates \
        --region us-east-1 \
        --savings-plan-types Database \
        --products Keyspaces \
        --output json 2>/dev/null)
    else
      response=$(aws savingsplans describe-savings-plans-offering-rates \
        --region us-east-1 \
        --savings-plan-types Database \
        --products Keyspaces \
        --next-token "$nextToken" \
        --output json 2>/dev/null)
    fi
    
    if [ $? -ne 0 ] || [ -z "$response" ]; then
      echo "Error fetching savings plans data" >&2
      break
    fi
    
    # Extract searchResults and add to array
    results=$(echo "$response" | jq -c '.searchResults[]?' 2>/dev/null)
    if [ -n "$results" ]; then
      while IFS= read -r result; do
        if [ -n "$result" ]; then
          allResults+=("$result")
        fi
      done <<< "$results"
    fi
    
    # Get nextToken for next iteration
    nextToken=$(echo "$response" | jq -r '.nextToken // empty' 2>/dev/null)
    
    # Break if no more pages
    if [ -z "$nextToken" ] || [ "$nextToken" == "null" ]; then
      break
    fi
  done
  
  # Output combined JSON
  echo "{"
  echo '  "searchResults": ['
  for i in "${!allResults[@]}"; do
    if [ $i -gt 0 ]; then
      echo ","
    fi
    printf "    %s" "${allResults[$i]}"
  done
  echo
  echo '  ]'
  echo '}'
} > src/data/savings-plans.json

# Get all pricing products with pagination
{
  allPriceList=()
  nextToken=""
  
  while true; do
    if [ -z "$nextToken" ]; then
      response=$(aws pricing get-products \
        --region us-east-1 \
        --service-code AmazonMCS \
        --output json 2>/dev/null)
    else
      response=$(aws pricing get-products \
        --region us-east-1 \
        --service-code AmazonMCS \
        --next-token "$nextToken" \
        --output json 2>/dev/null)
    fi
    
    if [ $? -ne 0 ] || [ -z "$response" ]; then
      echo "Error fetching pricing data" >&2
      break
    fi
    
    # Extract PriceList and add to array (each item is a JSON string)
    priceList=$(echo "$response" | jq -r '.PriceList[]?' 2>/dev/null)
    if [ -n "$priceList" ]; then
      while IFS= read -r item; do
        if [ -n "$item" ] && [ "$item" != "null" ]; then
          allPriceList+=("$item")
        fi
      done <<< "$priceList"
    fi
    
    # Get NextToken for next iteration
    nextToken=$(echo "$response" | jq -r '.NextToken // empty' 2>/dev/null)
    
    # Break if no more pages
    if [ -z "$nextToken" ] || [ "$nextToken" == "null" ]; then
      break
    fi
  done
  
  # Output combined JSON
  echo "{"
  echo '  "PriceList": ['
  for i in "${!allPriceList[@]}"; do
    if [ $i -gt 0 ]; then
      echo ","
    fi
    # Each PriceList item is a JSON string, so we need to quote it properly
    printf "    "
    printf '%s' "${allPriceList[$i]}" | jq -R '.'
  done
  echo
  echo '  ]'
  echo '}'
} > src/data/keyspaces-pricing.json

{
  echo "{"
  first=1

  for i in $(aws ec2 describe-regions --all-regions --query 'Regions[].RegionName' --output text); do
    long_name=$(aws ssm get-parameter \
      --name "/aws/service/global-infrastructure/regions/$i/longName" \
      --query "Parameter.Value" \
      --output text)

    # print comma *before* each entry except the first
    if [ $first -eq 0 ]; then
      echo ","
    else
      first=0
    fi

    modified_name="${long_name/#Europe/EU}"
    
    # JSON key/value line
    printf '  "%s": "%s"' "$i" "$modified_name"
    echo ","
    printf '  "%s": "%s"' "$modified_name" "$i"
  done
  echo  ","
  printf '  "AWS GovCloud (US)": "us-gov-west-1",\n'
  printf '  "us-gov-west-1": "AWS GovCloud (US)",\n'
  printf '  "us-gov-east-1": "AWS GovCloud (US-East)",\n'
  printf '  "AWS GovCloud (US-East)": "us-gov-east-1"'

  echo
  echo "}"
} > src/data/regions.json