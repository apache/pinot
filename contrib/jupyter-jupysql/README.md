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

# Query Pinot from Jupyter with JupySQL

This example shows how to query Apache Pinot from a Jupyter notebook using
[JupySQL](https://jupysql.ploomber.io/) and the [pinotdb](https://pypi.org/project/pinotdb/)
Python client. It is meant for local EDA: SQL magics, pandas DataFrames, and simple plots.

Related issue: https://github.com/apache/pinot/issues/10160

## Prerequisites

A running batch quickstart (loads the `baseballStats` table). The broker SQL
endpoint is **port 8000** (not 8099, which appears in some older client snippets).
The controller UI is port 9000.

### Option A — local binary (this checkout)

From the Pinot repo root, after `./mvnw clean install -DskipTests -Pbin-dist`:

```bash
./build/bin/quick-start-batch.sh
```

### Option B — Docker

```bash
docker run --name pinot-quickstart \
  -p 2123:2123 -p 9000:9000 -p 8000:8000 \
  -d apachepinot/pinot:latest QuickStart -type batch
```

Wait until the controller UI at http://localhost:9000 is up.

## Run the notebook

```bash
cd contrib/jupyter-jupysql
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
jupyter notebook pinot_jupysql_eda.ipynb
```

Connection string used in the notebook (broker **8000**, controller **9000**).
The engine is created with `use_multistage_engine=true` so JupySQL `%sqlplot`
CTEs are accepted:

```text
pinot://localhost:8000/query/sql?controller=http://localhost:9000/
```

To execute all cells headlessly (quickstart must already be running):

```bash
jupyter nbconvert --to notebook --execute pinot_jupysql_eda.ipynb --output pinot_jupysql_eda.executed.ipynb
```
