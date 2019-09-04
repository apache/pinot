#!/usr/bin/env bash
if [[ $(nc -z  localhost 9000) != 0 ]]; then
  kubectl port-forward service/pinot-controller 9000:9000 -n pinot-quickstart > /dev/null &
fi
sleep 2
open http://localhost:9000/query
# Just for blocking
tail -f /dev/null
pkill -f "kubectl port-forward service/pinot-controller 9000:9000 -n pinot-quickstart"
