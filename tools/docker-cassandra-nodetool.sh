#!/usr/bin/env bash
# Run nodetool against the local cass-bench container (works even when host JMX/RMI is finicky).
# Usage: ./tools/docker-cassandra-nodetool.sh status
#        ./tools/docker-cassandra-nodetool.sh tablestats
set -euo pipefail
exec docker exec cass-bench nodetool "$@"
