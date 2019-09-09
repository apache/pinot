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

if [[ -z "${GCLOUD_PROJECT}" ]]
then
  echo "Please set \$GCLOUD_PROJECT variable. E.g. GCLOUD_PROJECT=pinot-demo ./cleanup.sh"
  exit 1
fi

GCLOUD_ZONE=us-west1-b
GCLOUD_CLUSTER=pinot-quickstart

gcloud container clusters delete ${GCLOUD_CLUSTER} --zone=${GCLOUD_ZONE} --project=${GCLOUD_PROJECT} -q

for diskname in `gcloud compute disks list --zones=${GCLOUD_ZONE} --project ${GCLOUD_PROJECT} |grep  gke-${GCLOUD_CLUSTER}|awk -F ' ' '{print $1}'`;
do
  echo $diskname;
  gcloud compute disks delete $diskname --zone=${GCLOUD_ZONE} --project ${GCLOUD_PROJECT} -q
done

