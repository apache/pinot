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
# Apache Pinot - AGENTS Guide

This file provides quick, practical guidance for coding agents working in this
repo. It is intentionally short and focused on day-to-day work.

## Project overview
- Apache Pinot is a real-time distributed OLAP datastore for low-latency
  analytics over streaming and batch data.
- Core runtime roles: broker (query routing), server (segment storage/execution),
  controller (cluster metadata/management), minion (async tasks).

## Repository layout (high level)
- pinot-broker: broker query planning and scatter-gather.
- pinot-controller: controller APIs, table/segment metadata, Helix management.
- pinot-server: server query execution, segment loading, indexing.
- pinot-minion: background tasks (segment conversion, purge, etc).
- pinot-common / pinot-spi: shared utils, config, and SPI interfaces.
- pinot-segment-local / pinot-segment-spi: segment generation, indexes, storage.
- pinot-query-planner / pinot-query-runtime: multi-stage query (MSQ) engine.
- pinot-connectors: external tooling to connect to Pinot
- pinot-plugins: all pinot plugins.
- pinot-tools: CLI and quickstart scripts.
- pinot-integration-tests: end-to-end validation suites.
- pinot-distribution: packaging artifacts.

## pinot-plugins modules
- pinot-input-format: input format plugin family.
  - pinot-arrow: Apache Arrow input format support.
  - pinot-avro: Avro input format support.
  - pinot-avro-base: shared Avro utilities and base classes.
  - pinot-clp-log: CLP log input format support.
  - pinot-confluent-avro: Confluent Schema Registry Avro input support.
  - pinot-confluent-json: Confluent Schema Registry JSON input support.
  - pinot-confluent-protobuf: Confluent Schema Registry Protobuf input support.
  - pinot-orc: ORC input format support.
  - pinot-json: JSON input format support.
  - pinot-parquet: Parquet input format support.
  - pinot-csv: CSV input format support.
  - pinot-thrift: Thrift input format support.
  - pinot-protobuf: Protobuf input format support.
- pinot-file-system: filesystem plugin family.
  - pinot-adls: Azure Data Lake Storage (ADLS) filesystem support.
  - pinot-hdfs: Hadoop HDFS filesystem support.
  - pinot-gcs: Google Cloud Storage filesystem support.
  - pinot-s3: Amazon S3 filesystem support.
- pinot-batch-ingestion: batch ingestion plugin family.
  - pinot-batch-ingestion-common: shared batch ingestion APIs and utilities.
  - pinot-batch-ingestion-spark-base: shared Spark ingestion base classes.
  - pinot-batch-ingestion-spark-2.4: Spark 2.4 ingestion implementation.
  - pinot-batch-ingestion-spark-3: Spark 3 ingestion implementation.
  - pinot-batch-ingestion-hadoop: Hadoop MapReduce ingestion implementation.
  - pinot-batch-ingestion-standalone: standalone batch ingestion implementation.
- pinot-stream-ingestion: stream ingestion plugin family.
  - pinot-kafka-base: shared Kafka ingestion base classes.
  - pinot-kafka-2.0: Kafka 2.x ingestion implementation.
  - pinot-kafka-3.0: Kafka 3.x ingestion implementation.
  - pinot-kinesis: AWS Kinesis ingestion implementation.
  - pinot-pulsar: Apache Pulsar ingestion implementation.
- pinot-minion-tasks: minion task plugin family.
  - pinot-minion-builtin-tasks: built-in minion task implementations.
- pinot-metrics: metrics reporter plugin family.
  - pinot-dropwizard: Dropwizard Metrics reporter implementation.
  - pinot-yammer: Yammer Metrics reporter implementation.
  - pinot-compound-metrics: compound metrics implementation.
- pinot-segment-writer: segment writer plugin family.
  - pinot-segment-writer-file-based: file-based segment writer implementation.
- pinot-segment-uploader: segment uploader plugin family.
  - pinot-segment-uploader-default: default segment uploader implementation.
- pinot-environment: environment provider plugin family.
  - pinot-azure: Azure environment provider implementation.
- pinot-timeseries-lang: time series language plugin family.
  - pinot-timeseries-m3ql: M3QL language plugin implementation.
- assembly-descriptor: Maven assembly descriptor for plugin packaging.

## Build and test
- Build JDK: Use JDK 11+ (CI runs 11/21); code targets Java 11.
- Runtime JRE: Broker/server/controller/minion run on Java 11+.
- Default build: `./mvnw clean install`
- Faster dev build: `./mvnw verify -Ppinot-fastdev`
- Full binary/shaded build:
  `./mvnw clean install -DskipTests -Pbin-dist -Pbuild-shaded-jar`
- Build a module with deps: `./mvnw -pl pinot-server -am test`
- Single test example: `./mvnw -pl pinot-segment-local -Dtest=RangeIndexTest test`
- Quickstart (after build): `build/bin/quick-start-batch.sh`

## Integration tests
- Single integration test example: `./mvnw -pl pinot-integration-tests -am -Dtest=OfflineClusterIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test`

## Coding conventions and hygiene
- Add class-level Javadoc for new classes; describe behavior and thread-safety.
- Use Javadoc comments with either `/** ... */` or `///` syntax (per JEP-467); code targets Java 11.
- Keep license headers on all new source files.
- Use `./mvnw license:format` to add headers to new files.
- Preserve backward compatibility across mixed-version broker/server/controller.
- Prefer targeted unit tests; use integration tests when behavior crosses roles.

## Checkstyle config
- Checkstyle rules and related config files live under `config/`.
- Use the Maven wrapper (`./mvnw` on Unix-like systems or `mvnw.cmd` on Windows) to run `spotless:apply` to format code and `checkstyle:check` to validate style.
- Run `./mvnw license:check` to validate license headers.

## Change guidance
- Query changes often touch broker planning and server execution; verify both.
- Segment/index changes usually live under `pinot-segment-local` and
  `pinot-segment-spi`.
- Config or API changes should update relevant configs and docs where applicable.

## Reference docs
- `README.md` for build and quickstart details.
- `CONTRIBUTING.md` for style, licensing, and contribution guidance.
