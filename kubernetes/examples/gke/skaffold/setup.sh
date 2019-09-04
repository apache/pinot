#!/usr/bin/env bash
set -e
if [[ -z "${GCLOUD_EMAIL}" ]] ||  [[ -z "${GCLOUD_PROJECT}" ]]
then
      echo "Please set both \$GCLOUD_EMAIL and \$GCLOUD_PROJECT variables. E.g. GCLOUD_PROJECT=pinot-demo GCLOUD_EMAIL=pinot-demo@example.com ./setup.sh"
      exit 1
fi

GCLOUD_ZONE=us-west1-b
GCLOUD_CLUSTER=pinot-quickstart
GCLOUD_MACHINE_TYPE=n1-standard-2
GCLOUD_NUM_NODES=1
gcloud container clusters create ${GCLOUD_CLUSTER} \
        --num-nodes=${GCLOUD_NUM_NODES} \
        --machine-type=${GCLOUD_MACHINE_TYPE} \
        --zone=${GCLOUD_ZONE} \
        --project=${GCLOUD_PROJECT}
gcloud container clusters get-credentials ${GCLOUD_CLUSTER} --zone ${GCLOUD_ZONE} --project ${GCLOUD_PROJECT}

 
GCLOUD_MACHINE_TYPE=n1-standar-8
gcloud container node-pools create pinot-server-pool \
  --cluster=${GCLOUD_CLUSTER} \
  --machine-type=${GCLOUD_MACHINE_TYPE} \
  --num-nodes=${GCLOUD_NUM_NODES} \
  --zone=${GCLOUD_ZONE} \
  --project=${GCLOUD_PROJECT}


kubectl create namespace pinot-quickstart

kubectl create clusterrolebinding cluster-admin-binding --clusterrole cluster-admin --user ${GCLOUD_EMAIL}
kubectl create clusterrolebinding add-on-cluster-admin --clusterrole=cluster-admin --serviceaccount=pinot-quickstart:default
