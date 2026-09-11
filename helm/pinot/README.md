<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Pinot Quickstart on Kubernetes with Helm

## Prerequisite

- kubectl (<https://kubernetes.io/docs/tasks/tools/install-kubectl>)
- Helm (<https://helm.sh/docs/using_helm/#installing-helm>)
- Configure kubectl to connect to the Kubernetes cluster.
  - Skip to [Section: How to setup a Pinot cluster for demo](#How to setup a Pinot cluster for demo) if a k8s cluster is already setup.


## (Optional) Setup a Kubernetes cluster on Amazon Elastic Kubernetes Service (Amazon EKS)

### (Optional) Create a new k8s cluster on AWS EKS

- Install AWS CLI (<https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html#install-tool-bundled>)
- Install AWS-IAM-AUTHENTICATOR (<https://docs.aws.amazon.com/eks/latest/userguide/install-aws-iam-authenticator.html>)
- Install eksctl (<https://docs.aws.amazon.com/eks/latest/userguide/eksctl.html#installing-eksctl>)

- Login to your AWS account.

```bash
aws configure
```

Note that environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` will override the aws configuration in file `~/.aws/credentials`.

- Create an EKS cluster

Please modify the parameters in the example command below:

```bash
eksctl create cluster \
--name pinot-quickstart \
--version 1.14 \
--region us-west-2 \
--nodegroup-name standard-workers \
--node-type t3.small \
--nodes 3 \
--nodes-min 3 \
--nodes-max 4 \
--node-ami auto
```

For k8s 1.23+ we need to run the following commands to allow the containers to provision their storage
```
eksctl utils associate-iam-oidc-provider --region=us-east-2 --cluster=pinot-quickstart --approve

eksctl create iamserviceaccount \
  --name ebs-csi-controller-sa \
  --namespace kube-system \
  --cluster pinot-quickstart \
  --attach-policy-arn arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy \
  --approve \
  --role-only \
  --role-name AmazonEKS_EBS_CSI_DriverRole

eksctl create addon --name aws-ebs-csi-driver --cluster pinot-quickstart --service-account-role-arn arn:aws:iam::$(aws sts get-caller-identity --query Account --output text):role/AmazonEKS_EBS_CSI_DriverRole --force
```

You can monitor cluster status by command:

```bash
EKS_CLUSTER_NAME=pinot-quickstart
aws eks describe-cluster --name ${EKS_CLUSTER_NAME}
```

Once the cluster is in `ACTIVE` status, it's ready to be used.

### (Optional) How to connect to an existing cluster

Simply run below command to get the credential for the cluster you just created or your existing cluster.

```bash
EKS_CLUSTER_NAME=pinot-quickstart
aws eks update-kubeconfig --name ${EKS_CLUSTER_NAME}
```

To verify the connection, you can run

```bash
kubectl get nodes
```

## (Optional) Setup a Kubernetes cluster on Google Kubernetes Engine(GKE)

### (Optional) Create a new k8s cluster on GKE

- Google Cloud SDK (<https://cloud.google.com/sdk/install>)
- Enable Google Cloud Account and create a project, e.g. `pinot-demo`.
  - `pinot-demo` will be used as example value for `${GCLOUD_PROJECT}` variable in script example.
  - `pinot-demo@example.com` will be used as example value for `${GCLOUD_EMAIL}`.

Below script will:

- Create a gCloud cluster `pinot-quickstart`
- Request 2 servers of type `n1-standard-8` for demo.

Please fill both environment variables: `${GCLOUD_PROJECT}` and `${GCLOUD_EMAIL}` with your gcloud project and gcloud account email in below script.

```bash
GCLOUD_PROJECT=[your gcloud project name]
GCLOUD_EMAIL=[Your gcloud account email]
./setup_gke.sh
```

E.g.

```bash
GCLOUD_PROJECT=pinot-demo
GCLOUD_EMAIL=pinot-demo@example.com
./setup_gke.sh
```

### (Optional) How to connect to an existing GKE cluster

Simply run below command to get the credential for the cluster you just created or your existing cluster.
Please modify the Env variables `${GCLOUD_PROJECT}`, `${GCLOUD_ZONE}`, `${GCLOUD_CLUSTER}` accordingly in below script.

```bash
GCLOUD_PROJECT=pinot-demo
GCLOUD_ZONE=us-west1-b
GCLOUD_CLUSTER=pinot-quickstart
gcloud container clusters get-credentials ${GCLOUD_CLUSTER} --zone ${GCLOUD_ZONE} --project ${GCLOUD_PROJECT}
```

## (Optional) Setup a Kubernetes cluster on Microsoft Azure

### (Optional) Create a new k8s cluster on Azure

- Install Azure CLI (<https://docs.microsoft.com/en-us/cli/azure/install-azure-cli?view=azure-cli-latest>)
- Login to your Azure account.

```bash
az login
```

- Create Resource Group

```bash
AKS_RESOURCE_GROUP=pinot-demo
AKS_RESOURCE_GROUP_LOCATION=eastus
az group create --name ${AKS_RESOURCE_GROUP} --location ${AKS_RESOURCE_GROUP_LOCATION}
```

- Create an AKS cluster

```bash
AKS_RESOURCE_GROUP=pinot-demo
AKS_CLUSTER_NAME=pinot-quickstart
az aks create --resource-group ${AKS_RESOURCE_GROUP}  --name ${AKS_CLUSTER_NAME} --node-count 3
```

(Optional) Please register default provider if above command failed for error: `MissingSubscriptionRegistration`

```bash
az provider register --namespace Microsoft.Network
```

### (Optional) How to connect to an existing AKS cluster

Simply run below command to get the credential for the cluster you just created or your existing cluster.

```bash
AKS_RESOURCE_GROUP=pinot-demo
AKS_CLUSTER_NAME=pinot-quickstart
az aks get-credentials --resource-group ${AKS_RESOURCE_GROUP} --name ${AKS_CLUSTER_NAME}
```

To verify the connection, you can run

```bash
kubectl get nodes
```

## How to setup a Pinot cluster for demo

### Start Pinot with Helm

```bash
kubectl create ns pinot-quickstart

# First run dry-run with debug to verify:
helm install -n pinot-quickstart pinot . --dry-run --debug

# Install the Helm chart with:
helm install -n pinot-quickstart pinot .
```

#### To check deployment status

```bash
kubectl get all -n pinot-quickstart
```

### Pinot Realtime QuickStart

#### Bring up a Kafka Cluster for realtime data ingestion

- For helm v3.X.X

```bash
helm repo add incubator https://charts.helm.sh/incubator
helm install -n pinot-quickstart kafka incubator/kafka --set replicas=1
```

- For helm v2.12.1

```bash
helm repo add incubator https://charts.helm.sh/incubator
helm install --namespace "pinot-quickstart"  --name kafka incubator/kafka --set replicas=1
```

#### Create Kafka topic

```bash
kubectl -n pinot-quickstart exec kafka-0 -- kafka-topics.sh --bootstrap-server kafka-0:9092 --topic flights-realtime --create --partitions 1 --replication-factor 1
kubectl -n pinot-quickstart exec kafka-0 -- kafka-topics.sh --bootstrap-server kafka-0:9092 --topic flights-realtime-avro --create --partitions 1 --replication-factor 1
```

#### Load data into Kafka and create Pinot schema/table

```bash
kubectl apply -f pinot-realtime-quickstart.yml
```

### How to query pinot data

Please use below script to do local port-forwarding and open Pinot query console on your web browser.

```bash
./query-pinot-data.sh
```

## Configuring the Chart

This chart includes a built-in ZooKeeper StatefulSet using the
[official Apache ZooKeeper Docker image](https://hub.docker.com/_/zookeeper).
The chart can be customized using the following configurable parameters:

| Parameter                                      | Description                                                                                                                                                                | Default                                                            |
|------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------|
| `image.repository`                             | Pinot Container image repo                                                                                                                                                 | `apachepinot/pinot`                                                |
| `image.tag`                                    | Pinot Container image tag                                                                                                                                                  | `release-0.7.1`                                                   |
| `image.pullPolicy`                             | Pinot Container image pull policy                                                                                                                                          | `IfNotPresent`                                                     |
| `cluster.name`                                 | Pinot Cluster name                                                                                                                                                         | `pinot-quickstart`                                                 |
|------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------|
| `jfr.configuration`                            | JFR event settings: `default`, `profile`, or a path to a `.jfc` inside the container                                                                                        | `default`                                                          |
| `jfr.recordingName`                            | Name of the recording, as used by `jcmd ... JFR.dump name=...`                                                                                                             | `pinot`                                                            |
| `jfr.maxSize`                                  | Recording data kept for the current JVM run (Kubernetes quantity)                                                                                                          | `2Gi`                                                              |
| `jfr.maxAge`                                   | Wall-clock history kept for the current run; empty means bound by size alone                                                                                               | `""`                                                               |
| `jfr.maxChunkSize`                             | Size of an individual chunk file; the unit of eviction and of loss on SIGKILL                                                                                              | `12Mi`                                                             |
| `jfr.mountPath`                                | Where the JFR repository is mounted in the container                                                                                                                       | `/var/pinot/jfr`                                                   |
| `jfr.persistence.enabled`                      | Keep recordings on a PVC (one per pod) rather than an emptyDir; ignored by `minionStateless`                                                                                | `false`                                                            |
| `jfr.persistence.accessMode`                   | Access mode for the JFR PVC                                                                                                                                                | `ReadWriteOnce`                                                    |
| `jfr.persistence.size`                         | Size of the JFR PVC, per pod                                                                                                                                               | `10Gi`                                                             |
| `jfr.persistence.restartHeadroom`              | In-place container restarts to reserve room for in the sizing check                                                                                                        | `1`                                                                |
| `jfr.persistence.storageClass`                 | StorageClass for the JFR PVC; `-` means the empty class                                                                                                                    | `""`                                                               |
| `jfr.persistence.emptyDirSizeLimit`            | `sizeLimit` for the emptyDir; empty derives `maxSize + 2 * maxChunkSize`                                                                                                    | `""`                                                               |
| `jfr.janitor.enabled`                          | Run the init container that reclaims repositories left by previous JVM runs                                                                                                | `true`                                                             |
| `jfr.janitor.maxAge`                           | Drop leftover repositories older than this (`<n>m`/`<n>h`/`<n>d`); empty skips the pass                                                                                    | `7d`                                                               |
| `jfr.janitor.maxTotalSize`                     | Trim oldest-first until the repository fits this; empty skips the pass                                                                                                     | `4Gi`                                                              |
| `jfr.janitor.minIdleMinutes`                   | Never delete a repository written to this recently                                                                                                                         | `15`                                                               |
| `jfr.janitor.image.repository`                 | Image for the cleanup init container; defaults to the Pinot image                                                                                                          | `""`                                                               |
| `jfr.janitor.image.tag`                        | Tag for the cleanup init container image                                                                                                                                   | `""`                                                               |
| `jfr.janitor.image.pullPolicy`                 | Pull policy for the cleanup init container image                                                                                                                           | `""`                                                               |
| `jfr.janitor.securityContext`                  | Security context for the cleanup init container                                                                                                                            | `{}`                                                               |
| `jfr.janitor.resources`                        | Resources for the cleanup init container                                                                                                                                   | `{}`                                                               |
| `controller.jfr.enabled` / `broker.jfr.enabled` / `server.jfr.enabled` / `minion.jfr.enabled` / `minionStateless.jfr.enabled` | Enable continuous JFR for that role | `false` |
|------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------|
| `controller.name`                              | Name of Pinot Controller                                                                                                                                                   | `controller`                                                       |
| `controller.port`                              | Pinot controller port                                                                                                                                                      | `9000`                                                             |
| `controller.replicaCount`                      | Pinot controller replicas                                                                                                                                                  | `1`                                                                |
| `controller.data.dir`                          | Pinot controller data directory, should be same as `controller.persistence.mountPath` or a sub directory of it                                                             | `/var/pinot/controller/data`                                       |
| `controller.vip.enabled`                       | Enable Pinot controller Vip host                                                                                                                                           | `false`                                                            |
| `controller.vip.host`                          | Pinot controller Vip host                                                                                                                                                  | `pinot-controller`                                                 |
| `controller.vip.port`                          | Pinot controller Vip port                                                                                                                                                  | `9000`                                                             |
| `controller.persistence.enabled`               | Use a PVC to persist Pinot Controller data                                                                                                                                 | `true`                                                             |
| `controller.persistence.accessMode`            | Access mode of data volume                                                                                                                                                 | `ReadWriteOnce`                                                    |
| `controller.persistence.size`                  | Size of data volume                                                                                                                                                        | `1G`                                                               |
| `controller.persistence.mountPath`             | Mount path of controller data volume                                                                                                                                       | `/var/pinot/controller/data`                                       |
| `controller.persistence.storageClass`          | Storage class of backing PVC                                                                                                                                               | `""`                                                               |
| `controller.jvmOpts`                           | Pinot Controller JVM Options                                                                                                                                               | `-Xms256M -Xmx1G -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -Xloggc:/opt/pinot/gc-pinot-controller.log`                                                  |
| `controller.log4j2ConfFile`                    | Pinot Controller log4j2 configuration file                                                                                                                                 | `/opt/pinot/conf/log4j2.xml`                                       |
| `controller.pluginsDir`                        | Pinot Controller plugins directory                                                                                                                                         | `/opt/pinot/plugins`                                               |
| `controller.service.port`                      | Service Port                                                                                                                                                               | `9000`                                                             |
| `controller.external.enabled`                  | If True, exposes Pinot Controller externally                                                                                                                               | `true`                                                             |
| `controller.external.type`                     | Service Type                                                                                                                                                               | `LoadBalancer`                                                     |
| `controller.external.port`                     | Service Port                                                                                                                                                               | `9000`                                                             |
| `controller.resources`                         | Pinot Controller resource requests and limits                                                                                                                              | `{}`                                                               |
| `controller.nodeSelector`                      | Node labels for controller pod assignment                                                                                                                                  | `{}`                                                               |
| `controller.affinity`                          | Defines affinities and anti-affinities for pods as defined in: <https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#affinity-and-anti-affinity> preferences | `{}`                                                               |
| `controller.tolerations`                       | List of node tolerations for the pods. <https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/>                                                           | `[]`                                                               |
| `controller.podAnnotations`                    | Annotations to be added to controller pod                                                                                                                                  | `{}`                                                               |
| `controller.updateStrategy.type`               | StatefulSet update strategy to use.                                                                                                                                        | `RollingUpdate`                                                    |
| `controller.extra.configs`                     | Extra configs append to 'pinot-controller.conf' file to start Pinot Controller                                                                                             | `pinot.set.instance.id.to.hostname=true`                           |
|------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------|
| `broker.name`                                  | Name of Pinot Broker                                                                                                                                                       | `broker`                                                           |
| `broker.port`                                  | Pinot broker port                                                                                                                                                          | `8099`                                                             |
| `broker.replicaCount`                          | Pinot broker replicas                                                                                                                                                      | `1`                                                                |
| `broker.jvmOpts`                               | Pinot Broker JVM Options                                                                                                                                                   | `-Xms256M -Xmx1G -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -Xloggc:/opt/pinot/gc-pinot-broker.log`                                                  |
| `broker.log4j2ConfFile`                        | Pinot Broker log4j2 configuration file                                                                                                                                     | `/opt/pinot/conf/log4j2.xml`                                       |
| `broker.pluginsDir`                            | Pinot Broker plugins directory                                                                                                                                             | `/opt/pinot/plugins`                                               |
| `broker.service.port`                          | Service Port                                                                                                                                                               | `8099`                                                             |
| `broker.external.enabled`                      | If True, exposes Pinot Broker externally                                                                                                                                   | `true`                                                             |
| `broker.external.type`                         | External service Type                                                                                                                                                      | `LoadBalancer`                                                     |
| `broker.external.port`                         | External service Port                                                                                                                                                      | `8099`                                                             |
| `broker.routingTable.builderClass`             | Routing Table Builder Class                                                                                                                                                | `random`                                                           |
| `broker.resources`                             | Pinot Broker resource requests and limits                                                                                                                                  | `{}`                                                               |
| `broker.nodeSelector`                          | Node labels for broker pod assignment                                                                                                                                      | `{}`                                                               |
| `broker.affinity`                              | Defines affinities and anti-affinities for pods as defined in: <https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#affinity-and-anti-affinity> preferences | `{}`                                                               |
| `broker.tolerations`                           | List of node tolerations for the pods. <https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/>                                                           | `[]`                                                               |
| `broker.podAnnotations`                        | Annotations to be added to broker pod                                                                                                                                      | `{}`                                                               |
| `broker.updateStrategy.type`                   | StatefulSet update strategy to use.                                                                                                                                        | `RollingUpdate`                                                    |
| `broker.extra.configs`                         | Extra configs append to 'pinot-broker.conf' file to start Pinot Broker                                                                                                     | `pinot.set.instance.id.to.hostname=true`                           |
|------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------|
| `server.name`                                  | Name of Pinot Server                                                                                                                                                       | `server`                                                           |
| `server.port.netty`                            | Pinot server netty port                                                                                                                                                    | `8098`                                                             |
| `server.port.admin`                            | Pinot server admin port                                                                                                                                                    | `8097`                                                             |
| `server.replicaCount`                          | Pinot server replicas                                                                                                                                                      | `1`                                                                |
| `server.dataDir`                               | Pinot server data directory, should be same as `server.persistence.mountPath` or a sub directory of it                                                                     | `/var/pinot/server/data/index`                                     |
| `server.segmentTarDir`                         | Pinot server segment directory, should be same as `server.persistence.mountPath` or a sub directory of it                                                                  | `/var/pinot/server/data/segments`                                  |
| `server.persistence.enabled`                   | Use a PVC to persist Pinot Server data                                                                                                                                     | `true`                                                             |
| `server.persistence.accessMode`                | Access mode of data volume                                                                                                                                                 | `ReadWriteOnce`                                                    |
| `server.persistence.size`                      | Size of data volume                                                                                                                                                        | `4G`                                                               |
| `server.persistence.mountPath`                 | Mount path of server data volume                                                                                                                                           | `/var/pinot/server/data`                                           |
| `server.persistence.storageClass`              | Storage class of backing PVC                                                                                                                                               | `""`                                                               |
| `server.probes.endpoint`                       | Pinot server liveness and readiness probes endpoint                                                                                                                                             | `"/health"`                                                               |
| `server.probes.livenessEnabled`                | Whether to enable Pinot server liveness probe                                                                                                                                             | `false`                                                               |
| `server.probes.livenessProbe.endpoint`          | Optional parameter. Specify a specific Pinot server liveness probe endpoint instead of the shared `server.probes.endpoint`, You should use `"/health?checkType=liveness"`                                                                                                                               | Optional param, no default value                                                              |
| `server.probes.readinessEnabled`                | Whether to enable Pinot server readiness probe                                                                                                                                             | `false`                                                               |
| `server.probes.readinessProbe.endpoint`          | Optional parameter. Specify a specific Pinot server readiness probe endpoint instead of the shared `server.probes.endpoint`, You should use `"/health?checkType=readiness"`                                                                                                                               | Optional param, no default value                                                              |
| `server.jvmOpts`                               | Pinot Server JVM Options                                                                                                                                                   | `-Xms512M -Xmx1G -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -Xloggc:/opt/pinot/gc-pinot-server.log` |
| `server.log4j2ConfFile`                        | Pinot Server log4j2 configuration file                                                                                                                                     | `/opt/pinot/conf/log4j2.xml`                                       |
| `server.pluginsDir`                            | Pinot Server plugins directory                                                                                                                                             | `/opt/pinot/plugins`                                               |
| `server.service.port`                          | Service Port                                                                                                                                                               | `8098`                                                             |
| `server.resources`                             | Pinot Server resource requests and limits                                                                                                                                  | `{}`                                                               |
| `server.nodeSelector`                          | Node labels for server pod assignment                                                                                                                                      | `{}`                                                               |
| `server.affinity`                              | Defines affinities and anti-affinities for pods as defined in: <https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#affinity-and-anti-affinity> preferences | `{}`                                                               |
| `server.tolerations`                           | List of node tolerations for the pods. <https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/>                                                           | `[]`                                                               |
| `server.podAnnotations`                        | Annotations to be added to server pod                                                                                                                                      | `{}`                                                               |
| `server.updateStrategy.type`                   | StatefulSet update strategy to use.                                                                                                                                        | `RollingUpdate`                                                    |
| `server.extra.configs`                         | Extra configs append to 'pinot-server.conf' file to start Pinot Server                                                                                                     | `pinot.set.instance.id.to.hostname=true`                           |
|------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------|
| `minion.name`                                  | Name of Pinot Minion                                                                                                                                                       | `minion`                                                           |
| `minion.port`                                  | Pinot minion netty port                                                                                                                                                    | `9514`                                                             |
| `minion.replicaCount`                          | Pinot minion replicas                                                                                                                                                      | `1`                                                                |
| `minion.dataDir`                               | Pinot minion data directory, should be same as `minion.persistence.mountPath` or a sub directory of it                                                                     | `/var/pinot/minion/data`                                           |
| `minion.persistence.enabled`                   | Use a PVC to persist Pinot minion data                                                                                                                                     | `true`                                                             |
| `minion.persistence.accessMode`                | Access mode of data volume                                                                                                                                                 | `ReadWriteOnce`                                                    |
| `minion.persistence.size`                      | Size of data volume                                                                                                                                                        | `4G`                                                               |
| `minion.persistence.mountPath`                 | Mount path of minion data volume                                                                                                                                           | `/var/pinot/minion/data`                                           |
| `minion.persistence.storageClass`              | Storage class of backing PVC                                                                                                                                               | `""`                                                               |
| `minion.jvmOpts`                               | Pinot minion JVM Options                                                                                                                                                   | `-Xms512M -Xmx1G -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -Xloggc:/opt/pinot/gc-pinot-minion.log` |
| `minion.log4j2ConfFile`                        | Pinot minion log4j2 configuration file                                                                                                                                     | `/opt/pinot/conf/log4j2.xml`                                       |
| `minion.pluginsDir`                            | Pinot minion plugins directory                                                                                                                                             | `/opt/pinot/plugins`                                               |
| `minion.service.port`                          | Service Port                                                                                                                                                               | `9514`                                                             |
| `minion.resources`                             | Pinot minion resource requests and limits                                                                                                                                  | `{}`                                                               |
| `minion.nodeSelector`                          | Node labels for minion pod assignment                                                                                                                                      | `{}`                                                               |
| `minion.affinity`                              | Defines affinities and anti-affinities for pods as defined in: <https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#affinity-and-anti-affinity> preferences | `{}`                                                               |
| `minion.tolerations`                           | List of node tolerations for the pods. <https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/>                                                           | `[]`                                                               |
| `minion.podAnnotations`                        | Annotations to be added to minion pod                                                                                                                                      | `{}`                                                               |
| `minion.updateStrategy.type`                   | StatefulSet update strategy to use.                                                                                                                                        | `RollingUpdate`                                                    |
| `minion.extra.configs`                         | Extra configs append to 'pinot-minion.conf' file to start Pinot Minion                                                                                                     | `pinot.set.instance.id.to.hostname=true`                           |
|------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------|
| `zookeeper.enabled`                            | If True, installs ZooKeeper StatefulSet                                                                                                                                    | `true`                                                             |
| `zookeeper.urlOverride`                        | URL of external ZooKeeper cluster (used when `zookeeper.enabled` is false)                                                                                                 | `"my-zookeeper:2181/my-pinot"`                                     |
| `zookeeper.port`                               | ZooKeeper client port                                                                                                                                                      | `2181`                                                             |
| `zookeeper.replicaCount`                       | Number of ZooKeeper replicas                                                                                                                                               | `1`                                                                |
| `zookeeper.image.repository`                   | ZooKeeper Docker image repository                                                                                                                                          | `zookeeper`                                                        |
| `zookeeper.image.tag`                          | ZooKeeper Docker image tag                                                                                                                                                 | `3.9.3`                                                            |
| `zookeeper.image.pullPolicy`                   | ZooKeeper container pull policy                                                                                                                                            | `IfNotPresent`                                                     |
| `zookeeper.resources`                          | ZooKeeper resource requests and limits                                                                                                                                     | `{requests: {memory: "1.25Gi"}}`                                   |
| `zookeeper.heapSize`                           | ZooKeeper JVM heap size in MB                                                                                                                                              | `"1024"`                                                           |
| `zookeeper.jvmFlags`                           | Extra JVM flags for ZooKeeper                                                                                                                                              | `"-Djute.maxbuffer=4000000"`                                       |
| `zookeeper.persistence.enabled`                | Use PVCs to persist ZooKeeper data                                                                                                                                         | `true`                                                             |
| `zookeeper.persistence.size`                   | Default PV storage size for ZooKeeper                                                                                                                                      | `"8Gi"`                                                            |
| `zookeeper.persistence.storageClass`           | Storage class of backing PVC                                                                                                                                               | `""`                                                               |
| `zookeeper.affinity`                           | Defines affinities and anti-affinities for pods as defined in: <https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#affinity-and-anti-affinity> preferences | `{}`                                                               |
| `zookeeper.nodeSelector`                       | Node labels for ZooKeeper pod assignment                                                                                                                                   | `{}`                                                               |
| `zookeeper.tolerations`                        | List of node tolerations for ZooKeeper pods                                                                                                                                | `[]`                                                               |
|------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------|

Specify parameters using `--set key=value[,key=value]` argument to `helm install`

```bash
helm install --name pinot -f values.yaml . --set server.replicaCount=2
```

Alternatively a YAML file that specifies the values for the parameters can be provided like this:

```bash
helm install --name pinot -f values.yaml .
```

If you are using GKE, Create a storageClass:

```
kubectl apply -f gke-ssd.yaml
```

or If you want to use pd-standard storageClass:

```bash
kubectl apply -f gke-pd.yaml
```

## Continuous profiling with Java Flight Recorder

Each Pinot role can run a continuous JFR recording, so that when something goes wrong the
profile of the minutes leading up to it is already on disk. Enable it per role and tune the
shared `jfr` block:

```yaml
jfr:
  configuration: default   # or `profile` for much more detail, at a higher cost
  maxSize: 2Gi             # recording data kept for the current JVM run
  persistence:
    enabled: false         # true keeps recordings on a PVC across rescheduling
    size: 10Gi
  janitor:                 # only used when persistence.enabled is true
    maxAge: 7d             # drop repositories left by runs older than this
    maxTotalSize: 4Gi      # ...and trim the oldest until the volume fits

server:
  jfr:
    enabled: true
broker:
  jfr:
    enabled: true
```

The recording is started by the JVM itself: the chart appends `-XX:StartFlightRecording` and
`-XX:FlightRecorderOptions` to that role's `JAVA_OPTS`. It is therefore live from the first
instruction, and it does not depend on ZooKeeper, Helix or any Pinot config being reachable —
which matters, because those are not safe assumptions during the incidents you would most want a
profile for.

### Sizing

JFR's `maxSize` makes the recording roll like a log file: once the repository exceeds it, the
oldest chunks are evicted. Nothing has to rotate files by hand. Steady-state disk use for a
running JVM is about `maxSize + 2 * maxChunkSize`.

Prefer to bound the recording by **size** rather than by age. The event rate depends almost
entirely on the workload — GC frequency above all — so picking `maxAge` up front means guessing a
number you do not know. Set the disk budget you can afford, leave `jfr.maxAge` empty, and read the
window you actually got back off the recording:

```bash
jfr summary recording.jfr   # prints Start and Duration
```

### Choosing where recordings are stored

By default (`jfr.persistence.enabled: false`) recordings go to an `emptyDir`. They survive a
container restart but not the pod being rescheduled, the chart caps the volume with a `sizeLimit`,
and no cleanup init container is needed. This applies in place with a normal rolling restart, which
makes it the right choice for "turn on profiling now".

Set `jfr.persistence.enabled: true` to keep recordings on a PersistentVolume, so a profile survives
the node loss or eviction that destroyed the pod. Two costs come with it:

- It provisions one volume of `jfr.persistence.size` **per pod** — 50 servers at the default `10Gi`
  is 500Gi — and pods stay `Pending` on a cluster with no default StorageClass unless you set
  `jfr.persistence.storageClass`.
- It adds an entry to the StatefulSet's `volumeClaimTemplates`, a field Kubernetes **forbids
  changing in place**, so it cannot be switched on with a plain `helm upgrade`. See
  [UPGRADING.md](UPGRADING.md) for the one-time `kubectl delete statefulset --cascade=orphan`
  procedure.

### The stateless minion is different

`minionStateless` is a Deployment, not a StatefulSet, so it has no `volumeClaimTemplates` and its
replicas would have to share a single claim. Its recordings therefore always go to an `emptyDir`,
whatever `jfr.persistence.enabled` says.

This is a correctness constraint, not a limitation we could lift by trying harder. The janitor
below is safe because when it runs, every repository on the volume belongs to a JVM that has
already exited. On a shared claim that stops being true: with `replicaCount > 1`, or during any
rolling update, a starting pod would see a running pod's live repository. (A single
`ReadWriteOnce` claim would also stall the rollout with a Multi-Attach error.)

### Why there is an init container

`maxSize` bounds the repository of the JVM that is *running*. Nothing inside the JVM ever reclaims
the repository of a JVM that has already exited, and `preserve-repository=true` — which is what
keeps recordings across a restart in the first place — means those directories survive on the
volume forever. Left alone they accumulate one `maxSize` per restart until the volume is full.

The `jfr-janitor` init container reclaims them. Running it as an init container is what makes it
safe: init containers finish before the Pinot process starts, so every directory it sees belongs to
a run that is already over and there is no "is this one still in use?" question to get wrong.

As a backstop, the janitor also refuses to delete any repository written to within
`jfr.janitor.minIdleMinutes` (default 15). A live JFR repository is flushed at least once a second,
so anything idle that long belongs to a JVM that is gone. Nothing the janitor does is required for
Pinot to run, so it tolerates every failure and always exits 0 — a failed cleanup must never keep a
role from starting.

Note that Kubernetes does **not** re-run init containers when it restarts a container in place (an
OOMKilled process, for example) — only when the pod itself is recreated. Each such restart strands
another repository until the next pod-level restart. The chart refuses to render if
`jfr.janitor.maxTotalSize + jfr.maxSize + 2 * jfr.maxChunkSize` exceeds `jfr.persistence.size`,
which guarantees room for the run that follows a cleanup; raise `jfr.persistence.size` beyond that
if your workload restarts in place often.

### If the volume fills up

Worth knowing before you enable this. If the JFR volume runs out of space, the JVM cannot create its
repository and **fails to start** — and because Kubernetes restarts the container rather than the
pod, the janitor init container does not re-run to clear it. The role stays down until you delete
the pod:

```bash
kubectl delete pod <pod>     # recreates the pod, which re-runs the janitor
```

The chart's sizing check exists to keep you out of that state: it refuses to render unless
`jfr.janitor.maxTotalSize + (1 + jfr.persistence.restartHeadroom) * (jfr.maxSize + 2 * jfr.maxChunkSize)`
fits in `jfr.persistence.size`. Raise `jfr.persistence.restartHeadroom` if your workload restarts in
place (OOMKills, liveness failures) more than occasionally.

### Getting a recording out

```bash
# Snapshot a running JVM without interrupting the recording
kubectl exec <pod> -- jcmd 1 JFR.dump name=pinot filename=/tmp/snap.jfr
kubectl cp <pod>:/tmp/snap.jfr ./snap.jfr

# After a crash: rebuild a recording from the repository left behind, including the
# chunk that was still open when the JVM died
kubectl exec <pod> -- ls /var/pinot/jfr
kubectl exec <pod> -- jfr assemble /var/pinot/jfr/<repository-dir> /tmp/crash.jfr
```

You never need to copy the whole repository. Every `*.jfr` chunk in it is a valid recording on its
own and is named with its start timestamp, so you can pull only the chunks covering the window you
care about; concatenating chunks is a valid merge. To split a large file after the fact, use
`jfr disassemble --max-size 100M recording.jfr`.

### A note on units

Every size in the `jfr` block is a Kubernetes quantity, the same as everywhere else in this chart:
`2Gi` is 2^30 and `2G` is 10^9. The chart converts to the byte counts the JVM wants, so JFR's own
unit table never leaks into your values file.

One trap the chart rejects outright: a lowercase `m` means *milli* in Kubernetes, so `500m` is half
a byte rather than 500 MB. Use `M` or `Mi`.

### Migrating tuned `pinot.jfr.*` values

| Cluster config | Chart value |
|---|---|
| `pinot.jfr.enabled` | `<role>.jfr.enabled` |
| `pinot.jfr.configuration` | `jfr.configuration` |
| `pinot.jfr.name` | `jfr.recordingName` |
| `pinot.jfr.directory` | `jfr.mountPath` |
| `pinot.jfr.maxSize` | `jfr.maxSize` |
| `pinot.jfr.maxAge` | `jfr.maxAge` |
| `pinot.jfr.preserveRepository` | set automatically from `jfr.persistence.enabled` |
| `pinot.jfr.repositoryMaxTotalSize` | `jfr.janitor.maxTotalSize` |
| `pinot.jfr.toDisk`, `pinot.jfr.dumpOnExit`, `pinot.jfr.dumpPath` | no equivalent; see above |

**The value formats differ**, so do not copy values across verbatim:

| Old format | New format |
|---|---|
| `P7D`, `PT12H` (ISO-8601) | `7d`, `12h` |
| `2GB`, `20GB` | `2Gi`, `20Gi` (Kubernetes quantities) |

### Relationship to `pinot.jfr.*` cluster configs

This replaces the `pinot.jfr.*` cluster configs, which are deprecated. Those started the recording
from inside the JVM after it had connected to Helix, which meant startup was never captured and any
change to a `pinot.jfr.*` key restarted the recording — discarding all recorded history.

## How to clean up Pinot deployment

```bash
kubectl delete ns pinot-quickstart
```
