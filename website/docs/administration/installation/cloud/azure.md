---
title: Azure Quickstart
sidebar_label: Azure
description: Run Pinot on Azure
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import Alert from '@site/src/components/Alert';
import Jump from '@site/src/components/Jump';

To set up a Kubernetes Cluster on [Azure Kubernetes Service (AKS)](https://azure.microsoft.com/en-us/services/kubernetes-service/)

## Install Kubectl

Please follow this link (https://kubernetes.io/docs/tasks/tools/install-kubectl) to install kubectl.

- For Mac User

```bash
brew install kubernetes-cli
```

Please check kubectl version after installation.

```bash
kubectl version
```

<Alert icon={false} type="info">
QuickStart scripts are tested under kubectl client version v1.16.3 and server version v1.13.12
</Alert>

- Install Helm

Please follow this link (https://helm.sh/docs/using_helm/#installing-helm) to install helm.

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

- Install Azure CLI

Please follow this link to [install Azure CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli?view=azure-cli-latest)

- For Mac User

```bash
brew update && brew install azure-cli
```

## (Optional) Login to your Azure account

Below script will open default browser to sign-in to **your Azure Account**.

```bash
az login
```

## (Optional) Create a Resource Group

Below script will create a resource group in location `eastus`.

```bash
AKS_RESOURCE_GROUP=pinot-demo
AKS_RESOURCE_GROUP_LOCATION=eastus
az group create --name ${AKS_RESOURCE_GROUP} \
                --location ${AKS_RESOURCE_GROUP_LOCATION}
```

## (Optional) Create a Kubernetes cluster(AKS) in Azure

Below script will create a **3 nodes** cluster named **pinot-quickstart** for demo purposes.
Please modify the parameters in the example command below:

```bash
AKS_RESOURCE_GROUP=pinot-demo
AKS_CLUSTER_NAME=pinot-quickstart
az aks create --resource-group ${AKS_RESOURCE_GROUP} \
              --name ${AKS_CLUSTER_NAME} \
              --node-count 3
```

Once the command is succeed, it's ready to be used.

## Connect to an existing cluster

Simply run below command to get the credential for the cluster **pinot-quickstart** that you just created or your existing cluster.

```bash
AKS_RESOURCE_GROUP=pinot-demo
AKS_CLUSTER_NAME=pinot-quickstart
az aks get-credentials --resource-group ${AKS_RESOURCE_GROUP} \
                       --name ${AKS_CLUSTER_NAME}
```

To verify the connection, you can run:

```bash
kubectl get nodes
```

## Pinot Quickstart

Please follow this [Kubernetes QuickStart](/docs/administration/installation/cloud/on-premises) to deploy your Pinot Demo.

<Jump to="/docs/administration/installation/cloud/on-premises">Kubernetes Quickstart</Jump>

## Delete a Kubernetes Cluster

```bash
AKS_RESOURCE_GROUP=pinot-demo
AKS_CLUSTER_NAME=pinot-quickstart
az aks delete --resource-group ${AKS_RESOURCE_GROUP} \
              --name ${AKS_CLUSTER_NAME}
```
