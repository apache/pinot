#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# A script that does rolling upgrade of Pinot components
# from one version to the other given 2 commit hashes. It first builds  
# Pinot in the 2 given directories and then upgrades in the following order:
# Controller -> Broker -> Server

# verify correct usage of this script 
if [[ $# -ne 4 ]]; then
    printf "You used %s arguments \n" "$#"
    printf "Usage: \n ./comp-verifier.sh commitHash1 target_dir_1 commitHash2 target_dir_2 \n"
    exit 1
fi

# get arguments
commitHash1=$1
target_dir_1=$2
commitHash2=$3
target_dir_2=$4
pinot_version_1="0.5.0"
pinot_version_2="0.5.0"

read -rep $'\n' -p "What is the Pinot version for first commitHash? [default: 0.5.0] " -r
if [[ ! $REPLY == "" ]]; then
  pinot_version_1=$REPLY
fi
read -rep $'\n' -p "What is the Pinot version for second commitHash? [default: 0.5.0] " -r
if [[ ! $REPLY == "" ]]; then
  pinot_version_2=$REPLY
fi

# Building targets
read -rep $'\n' -p "Do you want to build the first target? [default: no] " -n 1 -r
if [[ $REPLY =~ ^[Yy]$ ]]; then
  printf "Building the first target ... \n"
  ./build-release.sh "$commitHash1" "$target_dir_1"
fi

read -rep $'\n' -p "Do you want to build the second target? [default: no] " -n 1 -r
if [[ $REPLY =~ ^[Yy]$ ]]; then
  printf "Building the second target ... \n"
  ./build-release.sh "$commitHash2" "$target_dir_2"
fi

if [[ $(lsof -t -i:7001 -s TCP:LISTEN) || $(lsof -t -i:8001 -sTCP:LISTEN) || $(lsof -t -i:9001 -sTCP:LISTEN) || 
      $(lsof -t -i:2181 -sTCP:LISTEN) || $(lsof -t -i:8011 -sTCP:LISTEN) ]]; then
  read -rep $'\n' -p "The ports need to be open. Do you want to kill existing processes on these ports? " -n 1 -r
  if [[ $REPLY =~ ^[Yy]$ ]]; then
    ## Clean up the ports and kill any processes running on them
    if [[ $(lsof -t -i:7001 -s TCP:LISTEN) ]]; then
      kill -9 "$(lsof -t -i:7001 -s TCP:LISTEN)"
    fi
    if [[ $(lsof -t -i:8001 -s TCP:LISTEN) ]]; then
      kill -9 "$(lsof -t -i:8001 -s TCP:LISTEN)"
    fi
    if [[ $(lsof -t -i:9001 -s TCP:LISTEN) ]]; then
      kill -9 "$(lsof -t -i:9001 -s TCP:LISTEN)" 
    fi
    if [[ $(lsof -t -i:2181 -s TCP:LISTEN) ]]; then
      kill -9 "$(lsof -t -i:2181 -s TCP:LISTEN)"
    fi
    if [[ $(lsof -t -i:8011 -s TCP:LISTEN) ]]; then
      kill -9 "$(lsof -t -i:8011 -s TCP:LISTEN)"
    fi
  fi
fi

read -rep $'\n' -p "Do you want to build the cluster with first commit? [default: no] " -n 1 -r
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
  exit 0;
fi


# Setup initial cluster (with commit in target_dir_1)

printf "\n\nStarting Zookeeper ...\n"
./control-service.sh start zookeeper "$target_dir_1" "$pinot_version_1" &
printf "\n\nZookeeper started\n\n"
sleep 10
printf "\n\nStarting Controller ...\n"
./control-service.sh start controller "$target_dir_1" "$pinot_version_1" &
printf "\n\nController started\n\n"
sleep 10
printf "\n\nStarting Broker ...\n"
./control-service.sh start broker "$target_dir_1" "$pinot_version_1" &
printf "\n\nBroker started\n\n"
sleep 10
printf "\n\nStarting Server ...\n"
./control-service.sh start server "$target_dir_1" "$pinot_version_1" &
printf "\n\nServer started\n\n"
sleep 10

# Begin rolling upgrade (Sequence is controller, broker, and server)
read -rep $'\n\n' -p "Initial cluster setup complete. Do you want to upgrade controller? [default: no] " -n 1 -r
if [[ $REPLY =~ ^[Yy]$ ]]; then
  ./control-service.sh stop controller "$target_dir_1" "$pinot_version_1"
  ./control-service.sh start controller "$target_dir_2" "$pinot_version_2" &
  sleep 10
fi

read -rep $'\n\n' -p "Do you want to upgrade broker? [default: no] " -n 1 -r
if [[ $REPLY =~ ^[Yy]$ ]]; then
  ./control-service.sh stop broker "$target_dir_1" "$pinot_version_1"
  ./control-service.sh start broker "$target_dir_2" "$pinot_version_2" &
  sleep 10
fi

read -rep $'\n\n' -p "Do you want to upgrade server? [default: no] " -n 1 -r
if [[ $REPLY =~ ^[Yy]$ ]]; then
  ./control-service.sh stop server "$target_dir_1" "$pinot_version_1"
  ./control-service.sh start server "$target_dir_2" "$pinot_version_2" &
  sleep 10
fi

printf "Rolling upgrade finished\n"

read -rep $'\n\n' -p "Do you want to stop the cluster? [default: no] " -n 1 -r
if [[ $REPLY =~ ^[Yy]$ ]]; then
    #stop the cluster
  ./control-service.sh stop server "$target_dir_2" "$pinot_version_2"
  ./control-service.sh stop broker "$target_dir_2" "$pinot_version_2"
  ./control-service.sh stop controller "$target_dir_2" "$pinot_version_2"
  ./control-service.sh stop zookeeper "$target_dir_1" "$pinot_version_1"
fi


exit 0
