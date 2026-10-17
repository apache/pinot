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
# Parquet ingestion fast paths

Both new fast paths are disabled by default. To enable direct native value conversion and column-oriented segment generation in a batch ingestion job, use:

```yaml
recordReaderSpec:
  dataFormat: parquet
  className: org.apache.pinot.plugin.inputformat.parquet.ParquetRecordReader
  configClassName: org.apache.pinot.plugin.inputformat.parquet.ParquetRecordReaderConfig
  configs:
    useParquetNativeRecordReader: "true"
    useParquetNativeDirectReader: "true"
    useColumnMajorSegmentBuilder: "true"
```

Column-oriented generation applies to eligible offline, single-value schemas with supported transforms and indexes. Unsupported inputs or table settings use the existing row-oriented path. Setting `tableIndexConfig.columnMajorSegmentBuilderEnabled` to `false` also disables column-oriented generation.

To defer eligible RAW STRING/JSON statistics until index writing, enable the existing `tableIndexConfig.optimizeNoDictStatsCollection` setting. Dictionary columns retain their statistics pass. Exact RAW statistics are finalized before segment metadata is written.

Required native string maps can produce canonical JSON directly. Repeated maps reuse immutable values through a per-column cache bounded to 4,096 entries and an estimated 4 MiB; oversized values use the uncached path. Cache contents are invalidated when dictionary or row-group state changes.
