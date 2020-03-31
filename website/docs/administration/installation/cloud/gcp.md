---
title: GCP Quickstart
sidebar_label: GCP
description: Run Pinot on GCP
---

import Alert from '@site/src/components/Alert';

To have basic set with [Google Kubernetes Engine(GKE)](https://cloud.google.com/kubernetes-engine) on GCP:

## Tooling Installation

### Install Kubectl

Please follow this link (https://kubernetes.io/docs/tasks/tools/install-kubectl) to install kubectl.

For Mac User

```bash
brew install kubernetes-cli
```

Please check kubectl version after installation.

```bash
kubectl version
```

QuickStart scripts are tested under kubectl client version v1.16.3 and server version v1.13.12

### Install Helm

Please follow this [link to install helm](https://helm.sh/docs/using_helm/#installing-helm)

For Mac User

```bash
brew install kubernetes-helm
```

Please check helm version after installation.

```bash
helm version
```

<Alert icon={false} type="info">
This QuickStart provides helm supports for helm v3.0.0 and v2.12.1.
Please pick the script based on your helm version.
</Alert>


## Install Google Cloud SDK

Please follow this link to [install Google Cloud SDK](https://cloud.google.com/sdk/install)

### For Mac User

- Install Google Cloud SDK

```bash
curl https://sdk.cloud.google.com | bash
```

- Restart your shell

```bash
exec -l $SHELL
```

### (Optional) Initialize Google Cloud Environment

```bash
gcloud init
```

### (Optional) Create a Kubernetes cluster(GKE) in Google Cloud

Below script will create a 3 nodes cluster named pinot-quickstart in `us-west1-b` with `n1-standard-2` machines for demo purposes.

Please modify the parameters in the example command below:

```bash
GCLOUD_PROJECT=[your gcloud project name]
GCLOUD_ZONE=us-west1-b
GCLOUD_CLUSTER=pinot-quickstart
GCLOUD_MACHINE_TYPE=n1-standard-2
GCLOUD_NUM_NODES=3
gcloud container clusters create ${GCLOUD_CLUSTER} \
  --num-nodes=${GCLOUD_NUM_NODES} \
  --machine-type=${GCLOUD_MACHINE_TYPE} \
  --zone=${GCLOUD_ZONE} \
  --project=${GCLOUD_PROJECT}
```

You can monitor cluster status by command:

```bash
gcloud compute instances list
```

Once the cluster is in RUNNING status, it's ready to be used.

### Connect to an existing cluster

Simply run below command to get the credential for the cluster pinot-quickstart that you just created or your existing cluster.

```bash
GCLOUD_PROJECT=[your gcloud project name]
GCLOUD_ZONE=us-west1-b
GCLOUD_CLUSTER=pinot-quickstart
gcloud container clusters get-credentials ${GCLOUD_CLUSTER} --zone ${GCLOUD_ZONE} --project ${GCLOUD_PROJECT}
```

To verify the connection, you can run:

```bash
kubectl get nodes
```

### Pinot Quickstart

Please follow this [Kubernetes QuickStart](/docs/administration/installation/cloud/on-premises) to deploy your Pinot Demo.

import Jump from '@site/src/components/Jump';

<Jump to="/docs/administration/installation/cloud/on-premises">Kubernetes Quickstart</Jump>

### Delete a Kubernetes Cluster

```bash
GCLOUD_ZONE=us-west1-b
gcloud container clusters delete pinot-quickstart --zone=${GCLOUD_ZONE}
```
