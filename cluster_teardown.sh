#!/bin/bash
set -euo pipefail

docker rm -f $(docker ps -a --filter "label=demo=dredis-demo" --format "{{.ID}}") 2>/dev/null || true
docker network rm dredis-demo 2>/dev/null || true
