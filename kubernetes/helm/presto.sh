if [[ $(nc -z  localhost 8080) != 0 ]]; then
  kubectl port-forward service/presto-coordinator 8080:8080 -n pinot-quickstart > /dev/null &
fi
sleep 2
open http://localhost:8080/

# Just for blocking
tail -f /dev/null
pkill -f "kubectl port-forward service/presto-coordinator 8080:8080 -n pinot-quickstart"
