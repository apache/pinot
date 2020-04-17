---
title: AWS Quickstart
sidebar_label: AWS
description: Run Pinot on AWS
---

This document provides the basic instruction to set up a Kubernetes Cluster on [Amazon Elastic Kubernetes Service (Amazon EKS)](https://aws.amazon.com/eks/)

import Alert from '@site/src/components/Alert';

<Alert type="info"> Because Pinot must be manually updated on Nix, new Pinot releases will be
delayed. Generally new Pinot releases are made available within a few days.</Alert>

## Tooling Installation

1. Install Pinot

    Please follow this link (https://kubernetes.io/docs/tasks/tools/install-kubectl) to install kubectl.

    For Mac User

    ```bash
    brew install kubernetes-cli
    ```

    Please check kubectl version after installation:

    ```bash
    kubectl version
    ```

    <Alert icon={false} type="info">
      QuickStart scripts are tested under kubectl client version v1.16.3 and server version v1.13.12
    </Alert>

1. Install Helm

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

     * This QuickStart provides helm supports for helm v3.0.0 and v2.12.1.
     * Please pick the script based on your helm version.

  </Alert>

1. Install AWS CLI

  [Install AWS CLI]((https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html#install-tool-bundled))

  For Mac User

  ```bash
  curl "https://d1vvhvl2y92vvt.cloudfront.net/awscli-exe-macos.zip" -o "awscliv2.zip"
  unzip awscliv2.zip
  sudo ./aws/install
  ```

1. Install Eksctl

  Install [AWS CLI](https://docs.aws.amazon.com/eks/latest/userguide/eksctl.html#installing-eksctl)

  For Mac User

  ```bash
  brew tap weaveworks/tap
  brew install weaveworks/tap/eksctl
  ```

## (Optional) Login to your AWS account

For first time AWS user, please register your [aws account](https://aws.amazon.com/)

Once created the account, you can go to [AWS Identity and Access Management (IAM)](https://console.aws.amazon.com/iam/home#/home) to create a user and create access keys under Security Credential tab.

```bash
aws configure
```

<Alert type="info"> Environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY will override  AWS configuration stored in file ~/.aws/credentials</Alert>

### (Optional) Create a Kubernetes cluster(EKS) in AWS

Below script will create a 3 nodes cluster named pinot-quickstart in us-west-2 with t3.small machines for demo purposes.
Please modify the parameters in the example command below:

```bash
EKS_CLUSTER_NAME=pinot-quickstart
eksctl create cluster \
  --name ${EKS_CLUSTER_NAME} \
  --version 1.14 \
  --region us-west-2 \
  --nodegroup-name standard-workers \
  --node-type t3.small \
  --nodes 3 \
  --nodes-min 3 \
  --nodes-max 4 \
  --node-ami auto
```

You can monitor cluster status by command:

```bash
EKS_CLUSTER_NAME=pinot-quickstart
aws eks describe-cluster --name ${EKS_CLUSTER_NAME}
```

Once the cluster is in ACTIVE status, it's ready to be used.

## Connect to an existing cluster

Simply run below command to get the credential for the cluster pinot-quickstart that you just created or your existing cluster.

```bash
EKS_CLUSTER_NAME=pinot-quickstart
aws eks update-kubeconfig --name ${EKS_CLUSTER_NAME}
```

To verify the connection, you can run:

```bash
kubectl get nodes
```

## Pinot Quickstart

Please follow this [Kubernetes QuickStart](/docs/administration/installation/cloud/on-premises) to deploy your Pinot Demo.

import Jump from '@site/src/components/Jump';

<Jump to="/docs/administration/installation/cloud/on-premises">Kubernetes Quickstart</Jump>

## Delete a Kubernetes Cluster

```bash
EKS_CLUSTER_NAME=pinot-quickstart
aws eks delete-cluster --name ${EKS_CLUSTER_NAME}
```
