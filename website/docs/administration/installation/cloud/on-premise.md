---
title: Running Pinot in Kubernetes
sidebar_label: Kubernetes
description: Running Pinot in Kubernetes
---

This QuickStart assumes the existence of Kubernetes Cluster. Please follow below links to setup your Kubernetes cluster in local or major cloud vendors.

> <b>Prerequisites:</b> <br/> 
> <p>&nbsp; <a href="https://docs.docker.com/docker-for-mac/kubernetes/" target="_blank">Enable Kubernetes on Docker-Desktop</a><br/> 
> &nbsp; <a href="https://kubernetes.io/docs/tasks/tools/install-minikube/" target="_blank">Setup a Kubernetes Cluster using Amazon Elastic Kubernetes Service (Amazon EKS)</a> <br/>
> &nbsp; <a href="https://kubernetes.io/docs/tasks/tools/install-minikube/" target="_blank">Setup a Kubernetes Cluster using Google Kubernetes Engine(GKE)</a> <br/>
> &nbsp; <a href="https://kubernetes.io/docs/tasks/tools/install-minikube/" target="_blank">Setup a Kubernetes Cluster using Azure Kubernetes Service (AKS)</a> <br/> </p>

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Setup a Pinot cluster for demo

### Update helm dependency

```bash
helm dependency update
```

### Start Pinot with Helm

- For Helm v2.12.1

If cluster is just initialized, ensure helm is initialized by running:

```bash
helm init --service-account tiller
```

Then deploy pinot cluster by:

```bash
helm install --namespace "pinot-quickstart" --name "pinot" .
```

- For Helm v3.0.0

```bash
kubectl create ns pinot-quickstart
helm install -n pinot-quickstart pinot .
```

## Troubleshooting (For helm v2.12.1)

- Error: Please run below command if encountering issue:

```bash
Error: could not find tiller".
```

- Resolution:

```bash
kubectl -n kube-system delete deployment tiller-deploy
kubectl -n kube-system delete service/tiller-deploy
helm init --service-account tiller
```

- Error: Please run below command if encountering permission issue:

```bash
Error: release pinot failed: namespaces "pinot-quickstart" is forbidden: User "system:serviceaccount:kube-system:default" cannot get resource "namespaces" in API group "" in the namespace "pinot-quickstart"
```

- Resolution:

```bash
kubectl apply -f helm-rbac.yaml
```

- Check deployment status

```bash
kubectl get all -n pinot-quickstart
```

## Load data into Pinot through Kafka

### Bring up a Kafka Cluster for realtime data ingestion

- For helm v2.12.1

```bash
helm repo add incubator https://charts.helm.sh/incubator
helm install --namespace "pinot-quickstart"  --name kafka incubator/kafka
```

- For helm v3.0.0

```bash
helm repo add incubator https://charts.helm.sh/incubator
helm install -n pinot-quickstart kafka incubator/kafka --set replicas=1
```

### Check Kafka deployment status

```bash
kubectl get all -n pinot-quickstart |grep kafka
```

Ensure Kafka deployment is ready before executing the scripts in next steps:

```bash
pod/kafka-0                                                 1/1     Running     0          2m
pod/kafka-zookeeper-0                                       1/1     Running     0          10m
pod/kafka-zookeeper-1                                       1/1     Running     0          9m
pod/kafka-zookeeper-2                                       1/1     Running     0          8m
```


### Create Kafka topics

Below scripts will create two Kafka topics for data ingestion:

```bash
kubectl -n pinot-quickstart exec kafka-0 -- kafka-topics --zookeeper kafka-zookeeper:2181 --topic flights-realtime --create --partitions 1 --replication-factor 1
kubectl -n pinot-quickstart exec kafka-0 -- kafka-topics --zookeeper kafka-zookeeper:2181 --topic flights-realtime-avro --create --partitions 1 --replication-factor 1
```

### Load data into Kafka and Create Pinot Schema/Tables

Below script will deploy 3 batch jobs

- Ingest 19492 Json messages to Kafka topic `flights-realtime`  at a speed of 1 msg/sec
- Ingest 19492 Avro messages to Kafka topic `flights-realtime-avro`  at a speed of 1 msg/sec
- Upload Pinot schema `airlineStats`
- Create Pinot Table `airlineStats` to ingest data from Json encoded Kafka topic `flights-realtime`
- Create Pinot Table `airlineStatsAvro`  to ingest data from Avro encoded Kafka topic `flights-realtime-avro`

```bash
kubectl apply -f pinot-realtime-quickstart.yml
```

## How to query pinot data

### Pinot Query Console

Please use below script to do local port-forwarding and open Pinot query console on your web browser.

```bash
./query-pinot-data.sh
```

## Use Superset to query Pinot

### Bring up Superset

```bash
kubectl apply -f superset.yaml
```

### (First time) Set up Admin account

```bash
kubectl exec -it pod/superset-0 -n pinot-quickstart -- bash -c 'export FLASK_APP=superset:app && flask fab create-admin'
```

### (First time) Init Superset

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

```bash
./open-superset-ui.sh
```

You can open the imported dashboard by click Dashboards banner then click on AirlineStats.

## Access Pinot Using Presto

### Deploy Presto with Pinot Plugin

You can run below command to deploy a customized Presto with Pinot plugin.

```bash
kubectl apply -f presto-coordinator.yaml
```

### Query Presto using Presto CLI

Once Presto is deployed, you could run below command.

```bash
./pinot-presto-cli.sh
```

### Sample queries to execute

- List all catalogs

```SQL
presto:default> show catalogs;
```

```bash
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

```SQL
presto:default> show tables;
```  

```bash
    Table
--------------
 airlinestats
(1 row)

Query 20191112_050907_00004_xkm4g, FINISHED, 1 node
Splits: 19 total, 19 done (100.00%)
0:01 [1 rows, 29B] [1 rows/s, 41B/s]
```

- Show schema

```sql
presto:default> DESCRIBE pinot.dontcare.airlinestats;
```

```bash
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

```SQL
presto:default> select count(*) as cnt from pinot.dontcare.airlinestats limit 10;
```

```bash
 cnt
------
 9745
(1 row)

Query 20191112_051114_00006_xkm4g, FINISHED, 1 node
Splits: 17 total, 17 done (100.00%)
0:00 [1 rows, 8B] [2 rows/s, 19B/s]
```

### How to clean up Pinot deployment

```bash
kubectl delete ns pinot-quickstart
```

import Jump from '@site/src/components/Jump';

<Jump to="/docs/administration/">Administration</Jump>
