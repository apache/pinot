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

# A helper script that builds Pinot given a specific commit hash and target directory
# This script is intended to be called stand-alone or from comp-verifier.sh 
# command syntax: ./comp-verifier.sh commitHash1 target_dir_1 commitHash2 target_dir_2

# verify correct usage of this script 
if [[ $# -ne 2 ]]; then
    printf "You used %s arguments \n" "$#"
    printf "Usage: \n ./build-release.sh commit_hash target_dir \n"
    exit 1
fi

# get arguments
commit_hash=$1
target_dir=$2

# check if directory already exists and checkout  
# using the given commit
if [[ ! -d $target_dir ]]; then
    mkdir -p "$target_dir"
fi
if [[ ! -d $target_dir/incubator-pinot ]]; then
    mkdir -p  "$target_dir"/incubator-pinot
    pushd "$target_dir"/incubator-pinot || exit 1
    git init
    git remote add origin https://github.com/apache/incubator-pinot
    git fetch --depth 1 origin "$commit_hash"
    git checkout FETCH_HEAD
    popd || exit 1
else
    pushd "$target_dir"/incubator-pinot || exit 1
    git fetch
    git checkout "$commit_hash" 
    popd || exit 1
fi

# build pinot
pushd "$target_dir"/incubator-pinot || exit 1
mvn install package -DskipTests -Pbin-dist
popd || exit 1
