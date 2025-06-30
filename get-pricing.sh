 #!/bin/bash

curl --compressed https://b0.p.awsstatic.com/pricing/2.0/meteredUnitMaps/mcs/USD/current/mcs.json | jq '.' > src/data/mcs.json

