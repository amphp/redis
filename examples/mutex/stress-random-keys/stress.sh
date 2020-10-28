#!/usr/bin/env bash

redis-cli flushall

pids=""

for i in `seq 0 49`; do
  php stress.php &
  pids="$pids $!"
done

wait $pids

echo ""
echo ""
echo "Number of locking attempts needed for 5000 locks:"
echo ""

redis-cli get attempts
