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
# Pinot plugin modules

- pinot-input-format: input format plugin family.
  - pinot-arrow: Apache Arrow input format support.
  - pinot-avro: Avro input format support.
  - pinot-avro-base: shared Avro utilities and base classes.
  - pinot-bson: MongoDB BSON input format support.
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
  - pinot-batch-ingestion-spark-3: Spark 3 ingestion implementation.
  - pinot-batch-ingestion-hadoop: Hadoop MapReduce ingestion implementation.
  - pinot-batch-ingestion-standalone: standalone batch ingestion implementation.
- pinot-stream-ingestion: stream ingestion plugin family.
  - pinot-kafka-base: shared Kafka ingestion base classes.
  - pinot-kafka-3.0: Kafka 3.x ingestion implementation.
  - pinot-kafka-4.0: Kafka 4.x ingestion implementation.
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
