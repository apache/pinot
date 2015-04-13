#!/bin/bash

if pwd | grep -qP '/scripts$'; then
  cd ..
fi

pid=`cat logs/webui.pid`

if [ -z "$pid" ]; then
  echo pid not found
  exit 1
fi

if ps aux | grep "$pid" | grep -q python; then
  echo killing pid $pid
  kill -9 "$pid"
  rm -f logs/webui.pid
fi
