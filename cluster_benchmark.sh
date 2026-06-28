docker rm -f $(docker ps -a -q --filter name=bench-node) 2>/dev/null
for i in $(seq 0 9); do
  target=$(( RANDOM % 10 ))
  docker run -d --name=bench-node$i --network=dredis-demo redis:alpine redis-benchmark -h node$target -p 6380 -c 10 -n 2000 -t ping,set,get -q
done
echo "Running..." && sleep 16
for i in {0..9}; do echo "=== node$i (target: say) ==="; docker logs bench-node$i 2>&1 | grep -E "PING|SET|GET|Error"; done
docker rm -f $(docker ps -a -q --filter name=bench-node) 2>/dev/null