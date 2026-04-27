 #!/bin/bash

curl --compressed https://b0.p.awsstatic.com/pricing/2.0/meteredUnitMaps/mcs/USD/current/mcs.json | jq '.' > src/calculator/data/mcs.json

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
} > src/calculator/data/savings-plans.json

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
} > src/calculator/data/regions.json