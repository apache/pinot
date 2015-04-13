#!/bin/bash

if pwd | grep -qP '/scripts$'; then
  cd ..
fi

if [ ! -f "activate" ]; then
  echo activate script not found. Did you run ./bootstrap.sh?
  exit 1
fi

. activate

nohup python run.py &>/dev/null &

pid=$!

sleep 1

if [ -f "/proc/$pid/status" ]; then 
  echo Started with pid $pid
  echo $pid > logs/webui.pid
else
  echo Proc died after starting. Check logs/run in foreground.
  exit 1
fi
