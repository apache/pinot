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
# Pinot Batch Ingestion for Spark 3

Runs Pinot segment generation and segment push as a Spark job, built for **Apache Spark 3.5.x**.

## ⚠️ Deprecated — migrate to `pinot-batch-ingestion-spark-4`

This module is **deprecated** and slated for removal in the next minor Pinot release. New users
should adopt [`pinot-batch-ingestion-spark-4`](../pinot-batch-ingestion-spark-4); existing users
should plan their migration during this release cycle.

## Runtime requirements (this release)

This module and its Pinot dependencies inherit the default Java 25 baseline (class file
major version 69). Build with JDK 25 or newer and use JDK 25 or newer on the Spark driver
and executors. An older Java runtime cannot load these jars; use artifacts from a Pinot
release compatible with your deployment if you cannot update the runtime.

## Migration path to Spark 4

`pinot-batch-ingestion-spark-4` is a faithful port of this module: the runner classes
(`SparkSegmentGenerationJobRunner`, `SparkSegmentMetadataPushJobRunner`,
`SparkSegmentTarPushJobRunner`, `SparkSegmentUriPushJobRunner`) keep the same FQN structure
under the `org.apache.pinot.plugin.ingestion.batch.spark4` package, and the
`SegmentGenerationJobSpec` YAML format is identical. To migrate:

1. Replace `pinot-batch-ingestion-spark-3-*-shaded.jar` with `pinot-batch-ingestion-spark-4-*-shaded.jar`.
2. Update any explicit class references in your job spec from
   `org.apache.pinot.plugin.ingestion.batch.spark3.*` to
   `org.apache.pinot.plugin.ingestion.batch.spark4.*`.
3. Switch your Spark cluster from Spark 3.5.x to Spark 4.1.x with JDK 25 or newer.
