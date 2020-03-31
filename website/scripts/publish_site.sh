#!/bin/bash
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

set -e

ROOT_DIR=$(git rev-parse --show-toplevel)
WORK_DIR=${ROOT_DIR}/build
ME=`basename $0`
echo "Basename $ME"

# ORIGIN_REPO=$(git remote show origin | grep 'Push  URL' | awk -F// '{print $NF}')
ORIGIN_REPO="https://github.com/apache/incubator-pinot-site"
echo "ORIGIN_REPO: $ORIGIN_REPO"

SITE_TMP=/tmp/pinot-site 
(

  cd $ROOT_DIR
  rm -rf $SITE_TMP
  mkdir $SITE_TMP
  cd $SITE_TMP

  git clone "https://$GH_TOKEN@$ORIGIN_REPO" .
  git config user.name "Pinot Site Updater"
  git config user.email "dev@pinot.apache.org"
  git checkout asf-master

  # Clean content directory
  rm -rf $SITE_TMP/content/
  mkdir $SITE_TMP/content

  # Copy the generated directory to asf folder
  cp -r $WORK_DIR/* $SITE_TMP/content
)