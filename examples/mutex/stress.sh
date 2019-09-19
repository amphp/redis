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
echo "Expecting a count of 5000:"
echo ""

redis-cli get foo

echo ""
echo "Number of locking attempts needed:"
echo ""

redis-cli get attempts
