#!/usr/bin/env bash
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

set -e
if [[ -z "${GCLOUD_EMAIL}" ]] ||  [[ -z "${GCLOUD_PROJECT}" ]]
then
  echo "Please set both \$GCLOUD_EMAIL and \$GCLOUD_PROJECT variables. E.g. GCLOUD_PROJECT=pinot-demo GCLOUD_EMAIL=pinot-demo@example.com ./setup.sh"
  exit 1
fi

GCLOUD_ZONE=us-west1-b
GCLOUD_CLUSTER=pinot-quickstart
GCLOUD_MACHINE_TYPE=n1-standard-8
GCLOUD_NUM_NODES=2
gcloud container clusters create ${GCLOUD_CLUSTER} \
  --num-nodes=${GCLOUD_NUM_NODES} \
  --machine-type=${GCLOUD_MACHINE_TYPE} \
  --zone=${GCLOUD_ZONE} \
  --project=${GCLOUD_PROJECT}
gcloud container clusters get-credentials ${GCLOUD_CLUSTER} --zone ${GCLOUD_ZONE} --project ${GCLOUD_PROJECT}

kubectl create namespace pinot-quickstart

kubectl create clusterrolebinding cluster-admin-binding --clusterrole cluster-admin --user ${GCLOUD_EMAIL}
kubectl create clusterrolebinding add-on-cluster-admin --clusterrole=cluster-admin --serviceaccount=pinot-quickstart:default
