#!/usr/bin/env bash
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

