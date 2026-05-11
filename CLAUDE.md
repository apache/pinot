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
# CLAUDE.md - Apache Pinot

## What is this project?
Apache Pinot is a real-time distributed OLAP datastore for low-latency analytics over streaming and batch data. Core runtime roles: **broker** (query routing), **server** (segment storage/execution), **controller** (cluster metadata/management), **minion** (async tasks).

## Repository layout
| Directory | Purpose |
|---|---|
| `pinot-broker` | Broker query planning and scatter-gather |
| `pinot-controller` | Controller APIs, table/segment metadata, Helix management |
| `pinot-server` | Server query execution, segment loading, indexing |
| `pinot-minion` | Background tasks (segment conversion, purge, etc.) |
| `pinot-common` / `pinot-spi` | Shared utils, config, and SPI interfaces |
| `pinot-segment-local` / `pinot-segment-spi` | Segment generation, indexes, storage |
| `pinot-query-planner` / `pinot-query-runtime` | Multi-stage query engine (MSQE) |
| `pinot-connectors` | External tooling to connect to Pinot |
| `pinot-plugins` | All Pinot plugins (input formats, filesystems, stream/batch ingestion, metrics, etc.) |
| `pinot-tools` | CLI and quickstart scripts |
| `pinot-integration-tests` | End-to-end validation suites |
| `pinot-distribution` | Packaging artifacts |

## Plugin modules (`pinot-plugins`)
- **pinot-input-format**: input format plugin family.
  - `pinot-arrow`: Apache Arrow input format support.
  - `pinot-avro`: Avro input format support.
  - `pinot-avro-base`: shared Avro utilities and base classes.
  - `pinot-clp-log`: CLP log input format support.
  - `pinot-confluent-avro`: Confluent Schema Registry Avro input support.
  - `pinot-confluent-json`: Confluent Schema Registry JSON input support.
  - `pinot-confluent-protobuf`: Confluent Schema Registry Protobuf input support.
  - `pinot-orc`: ORC input format support.
  - `pinot-json`: JSON input format support.
  - `pinot-parquet`: Parquet input format support.
  - `pinot-csv`: CSV input format support.
  - `pinot-thrift`: Thrift input format support.
  - `pinot-protobuf`: Protobuf input format support.
- **pinot-file-system**: filesystem plugin family.
  - `pinot-adls`: Azure Data Lake Storage (ADLS) filesystem support.
  - `pinot-hdfs`: Hadoop HDFS filesystem support.
  - `pinot-gcs`: Google Cloud Storage filesystem support.
  - `pinot-s3`: Amazon S3 filesystem support.
- **pinot-batch-ingestion**: batch ingestion plugin family.
  - `pinot-batch-ingestion-common`: shared batch ingestion APIs and utilities.
  - `pinot-batch-ingestion-spark-base`: shared Spark ingestion base classes.
  - `pinot-batch-ingestion-spark-3`: Spark 3 ingestion implementation.
  - `pinot-batch-ingestion-hadoop`: Hadoop MapReduce ingestion implementation.
  - `pinot-batch-ingestion-standalone`: standalone batch ingestion implementation.
- **pinot-stream-ingestion**: stream ingestion plugin family.
  - `pinot-kafka-base`: shared Kafka ingestion base classes.
  - `pinot-kafka-3.0`: Kafka 3.x ingestion implementation.
  - `pinot-kafka-4.0`: Kafka 4.x ingestion implementation.
  - `pinot-kinesis`: AWS Kinesis ingestion implementation.
  - `pinot-pulsar`: Apache Pulsar ingestion implementation.
- **pinot-minion-tasks**: minion task plugin family.
  - `pinot-minion-builtin-tasks`: built-in minion task implementations.
- **pinot-metrics**: metrics reporter plugin family.
  - `pinot-dropwizard`: Dropwizard Metrics reporter implementation.
  - `pinot-yammer`: Yammer Metrics reporter implementation.
  - `pinot-compound-metrics`: compound metrics implementation.
- **pinot-segment-writer**: segment writer plugin family.
  - `pinot-segment-writer-file-based`: file-based segment writer implementation.
- **pinot-segment-uploader**: segment uploader plugin family.
  - `pinot-segment-uploader-default`: default segment uploader implementation.
- **pinot-environment**: environment provider plugin family.
  - `pinot-azure`: Azure environment provider implementation.
- **pinot-timeseries-lang**: time series language plugin family.
  - `pinot-timeseries-m3ql`: M3QL language plugin implementation.
- **assembly-descriptor**: Maven assembly descriptor for plugin packaging.

## Build commands
- **JDK**: Use JDK 21+ for Pinot services and the default build; client and SPI artifacts still target Java 11 bytecode.
- **Default build**: `./mvnw clean install`
- **Fast dev build**: `./mvnw verify -Ppinot-fastdev`
- **Full binary/shaded build**: `./mvnw clean install -DskipTests -Pbin-dist -Pbuild-shaded-jar`
- **Build a module with deps**: `./mvnw -pl pinot-server -am test`
- **Single test**: `./mvnw -pl pinot-segment-local -Dtest=RangeIndexTest test`
- **Single integration test**: `./mvnw -pl pinot-integration-tests -am -Dtest=OfflineClusterIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test`
- **Quickstart (after build)**: `build/bin/quick-start-batch.sh`

## Code style and formatting
- Run `./mvnw spotless:apply` to auto-format code.
- Run `./mvnw checkstyle:check` to validate style. Checkstyle config is in `config/checkstyle.xml`.
- Run `./mvnw license:format` to add license headers to new files.
- Run `./mvnw license:check` to validate license headers.
- Always use the Maven wrapper (`./mvnw`) rather than a system `mvn`.

## Coding conventions
- Add class-level Javadoc for new classes; describe behavior and thread-safety.
- Use Javadoc comments (`/** ... */` or `///` syntax); service code targets Java 21 by default.
- Keep Apache 2.0 license headers on all new source files.
- Preserve backward compatibility across mixed-version broker/server/controller.
- Prefer imports over fully qualified class names (e.g., use `import com.foo.Bar` and refer to `Bar`, not `com.foo.Bar` inline).
- Prefer targeted unit tests; use integration tests when behavior crosses roles.
- Avoid deprecated APIs in new code. If you must reference one (e.g., for backward-compat serialization or to test the deprecated path), justify it with a comment.

## Pre-commit checks
Before pushing a commit, run the following checks on the affected modules and fix any failures:
1. `./mvnw spotless:apply -pl <module>` — auto-format code.
2. `./mvnw checkstyle:check -pl <module>` — validate style conformance.
3. `./mvnw license:format -pl <module>` — add missing license headers to new files.
4. `./mvnw license:check -pl <module>` — verify all files have correct license headers.

Do not push until all four checks pass cleanly.

Additionally, run the compiler warning check and fix what you can:
5. `./mvnw test-compile -pl <module> -am -Dmaven.compiler.showDeprecation=true -Dmaven.compiler.showWarnings=true` — review warnings (deprecation, unchecked, etc.) in your changed code and fix where possible.

Claude Code users can invoke `/precommit` to automate all of the above.

## Change guidance
- **Query changes** often touch broker planning and server execution; verify both.
- **Segment/index changes** usually live under `pinot-segment-local` and `pinot-segment-spi`.
- **Config or API changes** should update relevant configs and docs where applicable.

## Mandatory code review

After completing any coding task (bug fix, feature, refactor, etc.), you MUST run the `code-reviewer` agent before presenting the work as done. This is non-negotiable.

- Pass ONLY the review scope and a one-line change description. Do NOT pass your analysis, reasoning, or opinions — the reviewer must judge the code independently.
- Example invocation: `"Review unstaged changes in pinot-broker. Change: added timeout to scatter-gather calls."`
- If the reviewer finds CRITICAL issues, fix them before proceeding. MAJOR issues should be fixed unless you have strong justification. MINOR issues are at your discretion.
- Do not skip the review even if the change seems trivial.

## Common gotchas
- This is a large multi-module Maven project. Building the entire project takes a long time — prefer building only the modules you need with `-pl <module> -am`.
- When running tests, use `-Dtest=ClassName` to run a specific test class rather than the full suite.
- Mixed-version compatibility matters — do not break wire protocols or serialization formats without careful consideration.
