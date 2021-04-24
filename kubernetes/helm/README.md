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

### (Optional) How to connect to an existing cluster

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

### (Optional) How to connect to an existing cluster

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

### Update helm dependency

```bash
helm dependency update
```

### Start Pinot with Helm

- For helm v3.0.0

```bash
kubectl create ns pinot-quickstart
helm install -n pinot-quickstart pinot .
```

- For helm v2.12.1

If cluster is just initialized, ensure helm is initialized by running:

```bash
helm init --service-account tiller
```

Then deploy pinot cluster by:

```bash
helm install --namespace "pinot-quickstart" --name "pinot" .
```

#### Troubleshooting (For helm v2.12.1)
- Error: Please run below command if encountering issue:

```
Error: could not find tiller".
```

- Resolution:

```bash
kubectl -n kube-system delete deployment tiller-deploy
kubectl -n kube-system delete service/tiller-deploy
helm init --service-account tiller
```

- Error: Please run below command if encountering permission issue:

```Error: release pinot failed: namespaces "pinot-quickstart" is forbidden: User "system:serviceaccount:kube-system:default" cannot get resource "namespaces" in API group "" in the namespace "pinot-quickstart"```

- Resolution:

```bash
kubectl apply -f helm-rbac.yaml
```

#### To check deployment status

```bash
kubectl get all -n pinot-quickstart
```

### Pinot Realtime QuickStart

#### Bring up a Kafka Cluster for realtime data ingestion

- For helm v3.0.0

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
kubectl -n pinot-quickstart exec kafka-0 -- kafka-topics --zookeeper kafka-zookeeper:2181 --topic flights-realtime --create --partitions 1 --replication-factor 1
kubectl -n pinot-quickstart exec kafka-0 -- kafka-topics --zookeeper kafka-zookeeper:2181 --topic flights-realtime-avro --create --partitions 1 --replication-factor 1
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

This chart includes a ZooKeeper chart as a dependency to the Pinot
cluster in its `requirement.yaml` by default. The chart can be customized using the
following configurable parameters:

| Parameter                                      | Description                                                                                                                                                                | Default                                                            |
|------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------|
| `image.repository`                             | Pinot Container image repo                                                                                                                                                 | `apachepinot/pinot`                                                |
| `image.tag`                                    | Pinot Container image tag                                                                                                                                                  | `release-0.7.1`                                                   |
| `image.pullPolicy`                             | Pinot Container image pull policy                                                                                                                                          | `IfNotPresent`                                                     |
| `cluster.name`                                 | Pinot Cluster name                                                                                                                                                         | `pinot-quickstart`                                                 |
|------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------|
| `controller.name`                              | Name of Pinot Controller                                                                                                                                                   | `controller`                                                       |
| `controller.port`                              | Pinot controller port                                                                                                                                                      | `9000`                                                             |
| `controller.replicaCount`                      | Pinot controller replicas                                                                                                                                                  | `1`                                                                |
| `controller.data.dir`                          | Pinot controller data directory, should be same as `controller.persistence.mountPath` or a sub directory of it                                                             | `/var/pinot/controller/data`                                       |
| `controller.vip.host`                          | Pinot Vip host                                                                                                                                                             | `pinot-controller`                                                 |
| `controller.vip.port`                          | Pinot Vip port                                                                                                                                                             | `9000`                                                             |
| `controller.persistence.enabled`               | Use a PVC to persist Pinot Controller data                                                                                                                                 | `true`                                                             |
| `controller.persistence.accessMode`            | Access mode of data volume                                                                                                                                                 | `ReadWriteOnce`                                                    |
| `controller.persistence.size`                  | Size of data volume                                                                                                                                                        | `1G`                                                               |
| `controller.persistence.mountPath`             | Mount path of controller data volume                                                                                                                                       | `/var/pinot/controller/data`                                       |
| `controller.persistence.storageClass`          | Storage class of backing PVC                                                                                                                                               | `""`                                                               |
| `controller.jvmOpts`                           | Pinot Controller JVM Options                                                                                                                                               | `-Xms256M -Xmx1G`                                                  |
| `controller.log4j2ConfFile`                    | Pinot Controller log4j2 configuration file                                                                                                                                 | `/opt/pinot/conf/pinot-controller-log4j2.xml`                      |
| `controller.pluginsDir`                        | Pinot Controller plugins directory                                                                                                                                         | `/opt/pinot/plugins`                                               |
| `controller.service.port`                      | Service Port                                                                                                                                                               | `9000`                                                             |
| `controller.external.enabled`                  | If True, exposes Pinot Controller externally                                                                                                                               | `false`                                                            |
| `controller.external.type`                     | Service Type                                                                                                                                                               | `LoadBalancer`                                                     |
| `controller.external.port`                     | Service Port                                                                                                                                                               | `9000`                                                             |
| `controller.resources`                         | Pinot Controller resource requests and limits                                                                                                                              | `{}`                                                               |
| `controller.nodeSelector`                      | Node labels for controller pod assignment                                                                                                                                  | `{}`                                                               |
| `controller.affinity`                          | Defines affinities and anti-affinities for pods as defined in: <https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#affinity-and-anti-affinity> preferences | `{}`                                                               |
| `controller.tolerations`                       | List of node tolerations for the pods. <https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/>                                                           | `[]`                                                               |
| `controller.podAnnotations`                    | Annotations to be added to controller pod                                                                                                                                  | `{}`                                                               |
| `controller.updateStrategy.type`               | StatefulSet update strategy to use.                                                                                                                                        | `RollingUpdate`                                                    |
|------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------|
| `broker.name`                                  | Name of Pinot Broker                                                                                                                                                       | `broker`                                                           |
| `broker.port`                                  | Pinot broker port                                                                                                                                                          | `8099`                                                             |
| `broker.replicaCount`                          | Pinot broker replicas                                                                                                                                                      | `1`                                                                |
| `broker.jvmOpts`                               | Pinot Broker JVM Options                                                                                                                                                   | `-Xms256M -Xmx1G`                                                  |
| `broker.log4j2ConfFile`                        | Pinot Broker log4j2 configuration file                                                                                                                                     | `/opt/pinot/conf/pinot-broker-log4j2.xml`                          |
| `broker.pluginsDir`                            | Pinot Broker plugins directory                                                                                                                                             | `/opt/pinot/plugins`                                               |
| `broker.service.port`                          | Service Port                                                                                                                                                               | `8099`                                                             |
| `broker.external.enabled`                      | If True, exposes Pinot Broker externally                                                                                                                                   | `false`                                                            |
| `broker.external.type`                         | External service Type                                                                                                                                                      | `LoadBalancer`                                                     |
| `broker.external.port`                         | External service Port                                                                                                                                                      | `8099`                                                             |
| `broker.routingTable.builderClass`             | Routing Table Builder Class                                                                                                                                                | `random`                                                           |
| `broker.resources`                             | Pinot Broker resource requests and limits                                                                                                                                  | `{}`                                                               |
| `broker.nodeSelector`                          | Node labels for broker pod assignment                                                                                                                                      | `{}`                                                               |
| `broker.affinity`                              | Defines affinities and anti-affinities for pods as defined in: <https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#affinity-and-anti-affinity> preferences | `{}`                                                               |
| `broker.tolerations`                           | List of node tolerations for the pods. <https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/>                                                           | `[]`                                                               |
| `broker.podAnnotations`                        | Annotations to be added to broker pod                                                                                                                                      | `{}`                                                               |
| `broker.updateStrategy.type`                   | StatefulSet update strategy to use.                                                                                                                                        | `RollingUpdate`                                                    |
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
| `server.jvmOpts`                               | Pinot Server JVM Options                                                                                                                                                   | `-Xms512M -Xmx1G`                                                  |
| `server.log4j2ConfFile`                        | Pinot Server log4j2 configuration file                                                                                                                                     | `/opt/pinot/conf/pinot-server-log4j2.xml`                          |
| `server.pluginsDir`                            | Pinot Server plugins directory                                                                                                                                             | `/opt/pinot/plugins`                                               |
| `server.service.port`                          | Service Port                                                                                                                                                               | `8098`                                                             |
| `server.resources`                             | Pinot Server resource requests and limits                                                                                                                                  | `{}`                                                               |
| `server.nodeSelector`                          | Node labels for server pod assignment                                                                                                                                      | `{}`                                                               |
| `server.affinity`                              | Defines affinities and anti-affinities for pods as defined in: <https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#affinity-and-anti-affinity> preferences | `{}`                                                               |
| `server.tolerations`                           | List of node tolerations for the pods. <https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/>                                                           | `[]`                                                               |
| `server.podAnnotations`                        | Annotations to be added to server pod                                                                                                                                      | `{}`                                                               |
| `server.updateStrategy.type`                   | StatefulSet update strategy to use.                                                                                                                                        | `RollingUpdate`                                                    |
|------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------|
| `zookeeper.enabled`                            | If True, installs Zookeeper Chart                                                                                                                                          | `true`                                                             |
| `zookeeper.resources`                          | Zookeeper resource requests and limits                                                                                                                                     | `{}`                                                               |
| `zookeeper.env`                                | Environmental variables provided to Zookeeper Zookeeper                                                                                                                    | `{ZK_HEAP_SIZE: "256M"}`                                           |
| `zookeeper.storage`                            | Zookeeper Persistent volume size                                                                                                                                           | `2Gi`                                                              |
| `zookeeper.image.PullPolicy`                   | Zookeeper Container pull policy                                                                                                                                            | `IfNotPresent`                                                     |
| `zookeeper.url`                                | URL of Zookeeper Cluster (unneeded if installing Zookeeper Chart)                                                                                                          | `""`                                                               |
| `zookeeper.port`                               | Port of Zookeeper Cluster                                                                                                                                                  | `2181`                                                             |
| `zookeeper.affinity`                           | Defines affinities and anti-affinities for pods as defined in: <https://kubernetes.io/docs/concepts/configuration/assign-pod-node/#affinity-and-anti-affinity> preferences | `{}`                                                               |
|------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------|

Specify parameters using `--set key=value[,key=value]` argument to `helm install`

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

## Use superset to query Pinot

### Bring up Superset

```bash
kubectl apply -f superset.yaml
```

### Set up Admin account (First time)

```bash
kubectl exec -it pod/superset-0 -n pinot-quickstart -- bash -c 'export FLASK_APP=superset:app && flask fab create-admin'
```

### Init Superset (First time)

```bash
kubectl exec -it pod/superset-0 -n pinot-quickstart -- bash -c 'superset db upgrade'
kubectl exec -it pod/superset-0 -n pinot-quickstart -- bash -c 'superset init'
```

### Load Demo Data source

```bash
kubectl exec -it pod/superset-0 -n pinot-quickstart -- bash -c 'superset import_datasources -p /etc/superset/pinot_example_datasource.yaml'
kubectl exec -it pod/superset-0 -n pinot-quickstart -- bash -c 'superset import_dashboards -p /etc/superset/pinot_example_dashboard.json'
```

### Access Superset UI

You can run below command to navigate superset in your browser with the previous admin credential.

```bash
./open-superset-ui.sh
```

You can open the imported dashboard by click `Dashboards` banner then click on `AirlineStats`.

## Access Pinot Using Presto

### Deploy Presto with Pinot Plugin

You can run below command to deploy a customized Presto with Pinot plugin.

```bash
kubectl apply -f presto-coordinator.yaml
```

### Presto UI
Please use below script to do local port-forwarding and open Presto UI on your web browser.
```bash
./launch-presto-ui.sh
```

### Query Presto using Presto CLI

Once Presto is deployed, you could run below command.

```bash
./pinot-presto-cli.sh
```

### Sample queries to execute

- List all catalogs

```
presto:default> show catalogs;
```
```
 Catalog
---------
 pinot
 system
(2 rows)

Query 20191112_050827_00003_xkm4g, FINISHED, 1 node
Splits: 19 total, 19 done (100.00%)
0:01 [0 rows, 0B] [0 rows/s, 0B/s]

```

- List All tables

```
presto:default> show tables;
```
```
    Table
--------------
 airlinestats
(1 row)

Query 20191112_050907_00004_xkm4g, FINISHED, 1 node
Splits: 19 total, 19 done (100.00%)
0:01 [1 rows, 29B] [1 rows/s, 41B/s]
```

- Show schema

```
presto:default> DESCRIBE pinot.dontcare.airlinestats;
```
```
        Column        |  Type   | Extra | Comment
----------------------+---------+-------+---------
 flightnum            | integer |       |
 origin               | varchar |       |
 quarter              | integer |       |
 lateaircraftdelay    | integer |       |
 divactualelapsedtime | integer |       |
......

Query 20191112_051021_00005_xkm4g, FINISHED, 1 node
Splits: 19 total, 19 done (100.00%)
0:02 [80 rows, 6.06KB] [35 rows/s, 2.66KB/s]
```

- Count total documents

```
presto:default> select count(*) as cnt from pinot.dontcare.airlinestats limit 10;
```
```
 cnt
------
 9745
(1 row)

Query 20191112_051114_00006_xkm4g, FINISHED, 1 node
Splits: 17 total, 17 done (100.00%)
0:00 [1 rows, 8B] [2 rows/s, 19B/s]
```

### (Optional) Deploy more Presto workers

You can run below command to deploy more presto workers if needed.

```bash
kubectl apply -f presto-worker.yaml
```

Then you could verify the new worker nodes are added by:

```bash
presto:default> select * from system.runtime.nodes;
               node_id                |         http_uri         |      node_version      | coordinator | state
--------------------------------------+--------------------------+------------------------+-------------+--------
 38959968-6262-46a1-a321-ee0db6cbcbd3 | http://10.244.0.182:8080 | 0.230-SNAPSHOT-4e66289 | false       | active
 83851b8c-fe7f-49fe-ae0c-e3daf6d92bef | http://10.244.2.183:8080 | 0.230-SNAPSHOT-4e66289 | false       | active
 presto-coordinator                   | http://10.244.1.25:8080  | 0.230-SNAPSHOT-4e66289 | true        | active
(3 rows)

Query 20191206_095812_00027_na99c, FINISHED, 2 nodes
Splits: 17 total, 17 done (100.00%)
0:00 [3 rows, 248B] [11 rows/s, 984B/s]
```

## How to clean up Pinot deployment

```bash
kubectl delete ns pinot-quickstart
```
