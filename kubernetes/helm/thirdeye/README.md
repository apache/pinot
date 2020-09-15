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

# ThirdEye (Alpha)


This helm chart installs ThirdEye with a Pinot Quickstart Cluster. 
It sets up the following components.
- MySQL 5.7 server
- ThirdEye Frontend server
- ThirdEye Backend server


#### Prerequisites

- kubectl (<https://kubernetes.io/docs/tasks/tools/install-kubectl>)
- Helm (<https://helm.sh/docs/using_helm/#installing-helm>)
- Configure kubectl to connect to the Kubernetes cluster.
- An already Setup Pinot quickstart cluster.

## Installing ThirdEye

This installs thirdeye in the default namespace. 
```bash
./install.sh
```

All arguments passed to this script are forwarded to helm. So to install in namespace `te`, 
you can simply pass on the helm arguments directly.  

```bash
./install.sh --namespace te
```

## Uninstalling ThirdEye

Simply run one of the commands below depending on whether you are using a namespace or not.

```bash
helm uninstall thirdeye
helm uninstall thirdeye --namespace te
```


## Configuration

> Warning: The initdb.sql used for setting up the db may be out of date. Please note that this chart is currently a work in progress.  
 
Please see `values.yaml` for configurable parameters. Specify parameters using `--set key=value[,key=value]` argument to `helm install`

Alternatively a YAML file that specifies the values for the parameters can be provided like this:

```bash
./install.sh --name thirdeye -f values.yaml .
```

## Holiday Events

ThirdEye allows you to display events from external Google Calendars. To enable this feature, 
simply provide a JSON key. Check https://docs.simplecalendar.io/google-api-key/

```bash
./install.sh  --set-file backend.holidayLoaderKey="/path/to/holiday-loader-key.json"
```
