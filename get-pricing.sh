 #!/bin/bash

curl --compressed https://b0.p.awsstatic.com/pricing/2.0/meteredUnitMaps/mcs/USD/current/mcs.json | jq '.' > src/data/mcs.json

aws savingsplans describe-savings-plans-offering-rates --region us-east-1 --savings-plan-types Database --products Keyspaces > src/data/savings-plans.json

aws pricing get-products --region us-east-1 --service-code AmazonMCS > src/data/keyspaces-pricing.json

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

    # JSON key/value line
    printf '  "%s": "%s"' "$i" "$long_name"
    echo ","
    printf '  "%s": "%s"' "$long_name" "$i"
  done

  echo
  echo "}"
} > src/data/regions.json