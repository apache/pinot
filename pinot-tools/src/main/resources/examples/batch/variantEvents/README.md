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

# Parquet VARIANT quickstart table

`variantEvents_data.parquet` stores `payload` using the Apache Parquet
`VARIANT(1)` logical type. This sample verifies Pinot's current reader for a
top-level, non-repeated Variant column. Pinot retains the encoded value in a
raw `VARIANT` column and materializes `$.eventType` into an indexed STRING
column while ingesting the file. The ingestion spec explicitly selects
`ParquetNativeRecordReader`; use the native reader for VARIANT files that also
contain Avro schema metadata so existing Parquet reader selection remains
backward compatible.

The persisted format, query semantics, compatibility rules, and rollout gate are
documented in the [VARIANT design document](../../../../../../../pinot-spi/VARIANT_DESIGN.md).

Build and start the dedicated quickstart:

```shell
./mvnw clean install -DskipTests -Pbin-dist -Pbuild-shaded-jar
build/bin/quick-start-variant-batch.sh
```

The quickstart creates the schema and offline table, runs the standalone
Parquet ingestion job, uploads the segment, and executes these representative
queries:

```sql
SELECT eventType, COUNT(*)
FROM variantEvents
GROUP BY eventType
ORDER BY eventType;

SELECT
  eventId,
  variant_get(payload, '$.user.id', 'STRING') AS userId,
  variant_get(payload, '$.amount', 'DOUBLE') AS amount
FROM variantEvents
WHERE eventType = 'checkout'
ORDER BY eventId;

SELECT eventId, variantToJson(payload)
FROM variantEvents
ORDER BY eventId;
```

The sample schema and table both enable storage null handling. VARIANT tables
must enable either schema column-based null handling or table-level null
handling so that SQL null, Variant null, and missing paths stay distinct.
All sample queries also use `SET enableNullHandling=true`; Pinot rejects
VARIANT SQL functions without that query option instead of returning ambiguous
null results.

## Production rollout

`VARIANT` introduces schema and query-wire type names that older Pinot
processes do not recognize. Upgrade every controller, broker, server, minion,
and external ingestion job before registering a schema containing `VARIANT`.
Keep VARIANT columns out of queries until every broker and server has been
upgraded, and do not roll back to a pre-VARIANT build while a VARIANT table is
active. Existing tables and queries are unaffected during the rolling upgrade.

This follows the same user workflow and the supported subset of the
[Spark VARIANT function contract](https://spark.apache.org/docs/latest/api/sql/variant-functions/):
the source file carries a typed Variant value, frequently filtered paths can
be materialized during ingestion, and other paths remain available for
late-bound query-time extraction. The committed fixture was written with
parquet-java. A Spark-produced interoperability golden, nested or repeated
Variant columns, quoted path keys, streaming ingestion, and rolling
mixed-version queries over VARIANT columns are outside this sample's verified
scope.
