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
# Pinot Quickstart on Kubernetes on Google Kubernetes Engine(GKE)

## Prerequisite

- kubectl (https://kubernetes.io/docs/tasks/tools/install-kubectl/)
- Google Cloud SDK (https://cloud.google.com/sdk/install)
- Helm (https://helm.sh/docs/using_helm/#installing-helm)
- Enable Google Cloud Account and create a project, e.g. `pinot-demo`.
  - `pinot-demo` will be used as example value for `${GCLOUD_PROJECT}` variable in script example.
  - `pinot-demo@example.com` will be used as example value for `${GCLOUD_EMAIL}`.
- Configure kubectl to connect to the Kubernetes cluster.

## Create a cluster on GKE

Below script will:
- Create a gCloud cluster `pinot-quickstart`
- Request 1 server of type `n1-standard-2` for zookeeper, kafka, pinot controller, pinot broker.
- Request 1 server of type `n1-standard-8` for Pinot server.

Please fill both environment variables: `${GCLOUD_PROJECT}` and `${GCLOUD_EMAIL}` with your gcloud project and gcloud account email in below script.
```
GCLOUD_PROJECT=[your gcloud project name]
GCLOUD_EMAIL=[Your gcloud account email]
./setup.sh
```

E.g.
```
GCLOUD_PROJECT=pinot-demo
GCLOUD_EMAIL=pinot-demo@example.com
./setup.sh
```

Feel free to modify the script to pick your preferred sku, e.g. `n1-highmem-32` for Pinot server.


## How to connect to an existing cluster
Simply run below command to get the credential for the cluster you just created or your existing cluster.
Please modify the Env variables `${GCLOUD_PROJECT}`, `${GCLOUD_ZONE}`, `${GCLOUD_CLUSTER}` accordingly in below script.
```
GCLOUD_PROJECT=pinot-demo
GCLOUD_ZONE=us-west1-b
GCLOUD_CLUSTER=pinot-quickstart
gcloud container clusters get-credentials ${GCLOUD_CLUSTER} --zone ${GCLOUD_ZONE} --project ${GCLOUD_PROJECT}
```

Look for cluster status
```
kubectl get all -n pinot-quickstart -o wide
```

## How to setup a Pinot cluster for demo

### Update helm dependency
```
helm dependency update
```

### Start Pinot with Helm
```
helm install --namespace "pinot-quickstart" --name "pinot" .
```

###  Pinot Realtime QuickStart

#### Bring up a Kafka Cluster for realtime data ingestion
```bash
helm repo add incubator http://storage.googleapis.com/kubernetes-charts-incubator
helm install --namespace "pinot-quickstart"  --name kafka incubator/kafka
```

#### Create Kafka topic
```bash
kubectl -n pinot-quickstart exec kafka-0 -- kafka-topics --zookeeper kafka-zookeeper:2181 --topic flights-realtime --create --partitions 1 --replication-factor 1
```

#### Load data into Kafka and create Pinot schema/table
```bash
kubectl apply -f pinot-example-loader.yml
```

### How to query pinot data

Please use below script to do local port-forwarding and open Pinot query console on your web browser.
```
./query-pinot-data.sh
```

### How to clean up Pinot deployment
```
kubectl delete -f pinot-example-loader.yml
helm del --purge kafka
helm del --purge pinot
```
