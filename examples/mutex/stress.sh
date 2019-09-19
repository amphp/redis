#!/usr/bin/env bash

redis-cli flushall

pids=""

for i in `seq 0 49`; do
  php stress.php &
  pids="$pids $!"
done

wait $pids

php stress-check.php
