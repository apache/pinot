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

# Publish helm repo

## 1. Update Helm Version for release:

Update `Chart.yaml` file for fields:
- `appVersion`: for helm or k8s related changes
- `version`: for pinot related changes, remove `-SNAPSHOT` suffix to get to the release version, e.g. `0.3.4-SNAPSHOT` -> `0.3.4`.

## 2. Package Helm Charts

Run below command to package Pinot Chart.
```
helm package pinot
```
This step will generate a `pinot-${version}.tgz` file.

## 3. Index all the packages:

```
helm repo index .
```
This step will generate an `index.yaml` file which contains all the Charts information.

Update generated `index.yaml` accordingly:
- Revert the changes for all previous Charts;
- Change `entries.pinot.source` to `https://github.com/apache/pinot/tree/master/helm`.

## 4. Update Helm Version for development:

Update `Chart.yaml` file for making next `version` with `-SNAPSHOT` suffix. E.g. `0.3.4` -> `0.3.5-SNAPSHOT`

## Reference

Here is a reference PR: https://github.com/apache/pinot/pull/16292
