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
# Pinot Batch Ingestion for Spark 4

Runs Pinot segment generation and segment push as a Spark job, built for **Apache Spark 4.x on
JDK 21**. If you are on Spark 3.5.x or an older JDK, use
[pinot-batch-ingestion-spark-3](../../pinot-plugins/pinot-batch-ingestion/pinot-batch-ingestion-spark-3/)
instead.

The runners in this module are straight ports of the Spark 3 runners; they target the same
ingestion spec format and shared base classes in
[pinot-batch-ingestion-spark-base](../../pinot-plugins/pinot-batch-ingestion/pinot-batch-ingestion-spark-base/).

## Compatibility

| Pinot module                        | Spark | Scala       | JDK (build & runtime) |
|-------------------------------------|-------|-------------|------------------------|
| `pinot-batch-ingestion-spark-3`     | 3.5.x | 2.12 / 2.13 | 8 / 11 / 17            |
| `pinot-batch-ingestion-spark-4`     | 4.0.x | 2.13 only   | **21 only**            |

The produced jar is compiled with `--release 21` (class file major version 65), so it will
not load on a JDK 17 runtime despite Spark 4 itself supporting JDK 17. If JDK 17 runtime is
required, use the Spark 3 batch ingestion module.

This module sits next to [`pinot-batch-ingestion-spark-3`](../pinot-batch-ingestion-spark-3)
under `pinot-plugins/pinot-batch-ingestion/` and is only registered in the reactor when the
active JDK is 21 or later (see the `pinot-batch-ingestion-spark-4` profile in
[`pinot-plugins/pinot-batch-ingestion/pom.xml`](../pom.xml)). Automatically excluded under
`-Pscala-2.12` — Apache Spark 4 is Scala 2.13 only.

## Runners

The module ships four runners that parallel the Spark 3 set:

| Runner class                         | Purpose |
|--------------------------------------|---------|
| `SparkSegmentGenerationJobRunner`    | Create segments from input files in parallel |
| `SparkSegmentTarPushJobRunner`       | Push tarred segment files to the controller |
| `SparkSegmentUriPushJobRunner`       | Push segment URIs to the controller |
| `SparkSegmentMetadataPushJobRunner`  | Push segment metadata to the controller |

They live under the `org.apache.pinot.plugin.ingestion.batch.spark4` package.

## Job spec usage

The bundled YAML templates in `src/main/resources/` reference the runners by fully-qualified
class name, for example:

```yaml
executionFrameworkSpec:
  name: 'spark'
  segmentGenerationJobRunnerClassName: 'org.apache.pinot.plugin.ingestion.batch.spark4.SparkSegmentGenerationJobRunner'
  segmentTarPushJobRunnerClassName:    'org.apache.pinot.plugin.ingestion.batch.spark4.SparkSegmentTarPushJobRunner'
  segmentUriPushJobRunnerClassName:    'org.apache.pinot.plugin.ingestion.batch.spark4.SparkSegmentUriPushJobRunner'
  segmentMetadataPushJobRunnerClassName: 'org.apache.pinot.plugin.ingestion.batch.spark4.SparkSegmentMetadataPushJobRunner'
```

Copy one of the templates and adapt it to your table and input paths.

## Running under Spark 4

Spark 4 requires the standard JDK 17+ `--add-opens` set. A typical `spark-submit` looks like:

```
spark-submit \
  --driver-java-options "\
    --add-opens=java.base/java.lang=ALL-UNNAMED \
    --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
    --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
    --add-opens=java.base/java.io=ALL-UNNAMED \
    --add-opens=java.base/java.net=ALL-UNNAMED \
    --add-opens=java.base/java.nio=ALL-UNNAMED \
    --add-opens=java.base/java.util=ALL-UNNAMED \
    --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
    --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
    --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
    --add-opens=java.base/sun.security.action=ALL-UNNAMED \
    --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
    --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED" \
  --class org.apache.pinot.spi.ingestion.batch.IngestionJobLauncher \
  /path/to/pinot-all-<VERSION>-jar-with-dependencies.jar \
  -jobSpecFile /path/to/segmentCreationAndTarPushJobSpec.yaml
```

## Tests

The module's test (`SparkSegmentGenerationJobRunnerTest`) spins up a local Spark 4 context
with the Spark UI disabled (`spark.ui.enabled=false`). The UI is disabled because Spark 4's
Jetty 12 expects the Jakarta Servlet 5 namespace, which conflicts with Pinot's transitive
Jersey 2.x (javax namespace). The runners themselves do not touch the UI, so this has no
impact on production behavior.

Run the tests:

```
./mvnw -pl pinot-plugins/pinot-batch-ingestion/pinot-batch-ingestion-spark-4 -am test
```

### Known coverage gap

The Spark 3 counterpart has a `SparkSegmentMetadataPushIntegrationTest` end-to-end test in
`pinot-integration-tests` that exercises the runner against a real broker / controller / server.
Spark 4 does not yet have an equivalent: the Spark 4 runtime (Jetty 12 / Jakarta Servlet 5) and
`pinot-integration-tests`' existing Spark 3 / Jersey 2 / `javax.servlet-*` classpath cannot
currently coexist in a single Maven module's test classpath. Restoring parity needs either a new
`pinot-spark-4-integration-tests` module (isolated reactor) or per-test classpath realms via
failsafe. Tracked separately; unit tests here cover the in-process driver path but not the
segment-push wire flow across role boundaries.
