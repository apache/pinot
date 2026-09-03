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
# Apache Pinot VARIANT — Design Document

Status: draft for community and component-owner review.

## 1. Motivation and goals

Apache Parquet `VARIANT(1)` provides a portable binary representation for
semi-structured values. Pinot should ingest that representation without converting
every row through a JSON tree, retain the complete value for late-bound queries, and
let users materialize frequently queried paths into ordinary Pinot columns.

This design has five goals:

1. Define a first-class Pinot `VARIANT` logical type with an explicit persisted and
   query-wire contract.
2. Ingest top-level, non-repeated Parquet `VARIANT(1)` values, including unshredded,
   fully shredded, and partially shredded layouts.
3. Provide Spark-style parse, extraction, existence, type, null, and JSON-rendering
   functions in both Pinot query engines.
4. Fail explicitly when a raw Variant value reaches an operation whose semantics are
   undefined, while allowing users to extract a typed scalar for that operation.
5. Ship an executable table-definition, ingestion, and query story with integration
   coverage.

### 1.1 Non-goals

The initial contract does not include:

- nested or repeated Parquet Variant columns;
- Pinot multi-value `VARIANT` columns;
- streaming-format Variant ingestion;
- quoted object keys in Variant paths;
- semantic equality, hashing, or ordering of raw Variant values;
- mixed-version execution for queries that contain a Variant type; or
- automatic rollback after a Variant table has been activated.

## 2. User model

A Variant column is declared as a single-value dimension:

```json
{
  "name": "payload",
  "dataType": "VARIANT"
}
```

The table stores `payload` in a raw forward index without a dictionary. Storage null
handling must be enabled. A common layout retains the source value while materializing
an indexed scalar during ingestion:

```json
{
  "columnName": "eventType",
  "transformFunction": "variant_get(payload, '$.eventType', 'STRING')"
}
```

Late-bound paths remain queryable:

```sql
SET enableNullHandling=true;

SELECT
  eventId,
  variant_get(payload, '$.user.id', 'STRING') AS userId,
  variant_get(payload, '$.amount', 'DOUBLE') AS amount
FROM variantEvents
WHERE eventType = 'checkout';
```

## 3. Data flow and ownership

```text
Parquet VARIANT(1)
        |
        | pinot-parquet: validate shape and reconstruct shredded values
        v
metadata ByteBuffer + value ByteBuffer
        |
        | VariantEnvelope: add stable Pinot PVAR framing
        v
single-value raw BYTES forward index in a Pinot segment
        |
        | DataSchema/protobuf: carry logical VARIANT, not generic BYTES
        v
single-stage or multi-stage Variant functions
        |
        +--> typed scalar for filtering/grouping/joining/aggregation
        |
        +--> canonical JSON text for response clients
```

Ownership is deliberately split by module:

- `pinot-spi` owns the public logical type, capability contract, and stable `PVAR`
  envelope.
- `pinot-segment-local` owns schema, table, index, null, and segment-metadata
  validation.
- `pinot-parquet` owns Parquet logical-type validation and reconstruction.
- `pinot-common`, `pinot-core`, `pinot-query-planner`, and `pinot-query-runtime` own
  function semantics and defensive query guards.
- client, response, and DDL modules carry the logical type without redefining its
  semantics.

`pinot-common` depends only on the small `parquet-variant` codec. Parquet column,
schema, Hadoop, and reader dependencies remain isolated to the input-format plugin.

## 4. Persisted `PVAR` envelope

Pinot segments need one byte sequence that contains both buffers in a Parquet Variant
value. `VariantEnvelope` owns that framing. Version 1 is:

```text
offset   size   field
0        4      ASCII magic "PVAR"
4        1      envelope version (1)
5        1      flags (0)
6        2      reserved (0)
8        4      metadata length, big-endian signed int restricted to >= 0
12       4      value length, big-endian signed int restricted to >= 0
16       M      Parquet Variant metadata bytes
16 + M   V      Parquet Variant value bytes
```

The complete envelope must fit in a Java byte array. Decoders reject unknown versions,
non-zero flags or reserved bytes, negative lengths, bad magic, and length mismatches.
Metadata and value bytes are copied without interpretation by the envelope layer; the
Parquet Variant codec validates their internal representation.

Version 1 is immutable and protected by a golden-byte test. A future incompatible
framing change must use a new envelope version and retain an explicit reader for every
supported old version. Unknown versions fail closed; they must never fall back to
generic `BYTES`.

`decode(byte[])` returns zero-copy read-only views that alias the input array. The input
must therefore remain immutable for the lifetime of those views. Array, direct, and
read-only source buffers are supported without changing their positions. Parquet
ingestion copies payload bytes directly into the final envelope and must not allocate a
full intermediate payload array.

## 5. Logical type and segment contract

`FieldSpec.DataType.VARIANT` is logically distinct from `BYTES` but uses variable-width
bytes as its physical stored type.

The initial schema and table constraints are:

- dimension field only;
- single-value only;
- the reserved empty-byte default null value only;
- no truncating or default-substitution max-length policy;
- storage null handling enabled at schema or table level;
- enabled raw forward index;
- no dictionary or secondary index on the raw Variant column;
- not a primary key, partition column, sorted column, upsert comparison column,
  metrics-aggregation key, or star-tree dimension; and
- partial upsert may use only the `OVERWRITE` strategy.

Segment creation validates every non-null value as a complete `PVAR` envelope. Raw
Variant columns do not publish logical minimum or maximum values and are never marked
sorted, including single-row segments.

### 5.1 Capability policy

The Parquet representation is not a canonical semantic encoding: equivalent logical
objects can use different metadata dictionaries or shredded layouts. Byte equality,
byte hashing, and lexicographic byte order would therefore expose storage-layout
accidents as SQL semantics.

The logical type reports:

| Capability on raw value | Supported |
|---|---|
| equality / `IN` / pattern predicates | no |
| hashing / `GROUP BY` / `DISTINCT` | no |
| ordering / min-max / sort / ASOF match key | no |
| join, lookup, primary, or partition key | no |
| direct value-consuming aggregation | no |
| non-distinct `COUNT(raw_variant)` | yes |

Planner validation provides the primary error. Runtime and single-stage guards remain
in place for mixed plans or future planner changes. Error messages direct users to
`variant_get` a typed path first.

## 6. Parquet ingestion

Reader initialization scans the file schema before publishing reader state.

- A supported field is a top-level, non-repeated group annotated `VARIANT(1)`.
- `metadata` is required binary; at least one of `value` or `typed_value` is present.
- The parquet-java `VariantConverters` tree reconstructs fully or partially shredded
  values.
- The unshredded path retains the exact metadata and value buffers and adds only the
  `PVAR` envelope.
- Converter plans and field indexes are immutable and created once per reader.
- A failed reinitialization leaves the previous reader usable. A failure while closing
  the old reader does not invalidate the successfully initialized replacement.

Automatic reader selection preserves existing Avro-metadata precedence for backward
compatibility. A Parquet file with Avro metadata must explicitly select the native
reader to ingest Variant.

Malformed rows follow the existing record-reader skip policy and do not corrupt reader
progress. Unsupported Variant spec versions and unsupported nested/repeated shapes fail
during initialization rather than appearing as ordinary structs.

## 7. Query functions

Both query engines expose:

- `variant_get(value, path[, targetType])`
- `try_variant_get(value, path[, targetType])`
- `variant_exists(value, path)`
- `is_variant_null(value[, path])`
- `variant_typeof(value[, path])`
- `variant_to_json(value)`
- `parse_json(text)` / `parse_json_to_variant(text)`
- `try_parse_json(text)` / `try_parse_json_to_variant(text)`

Function lookup remains case-insensitive under Pinot's existing registration rules.
The v1 path grammar supports root `$`, dot-separated object fields, and non-negative
array indexes. Supported extraction targets are `BOOLEAN`, `INT`, `LONG`, `FLOAT`,
`DOUBLE`, `BIG_DECIMAL`, `STRING`, `BYTES`, `UUID`, `TIMESTAMP`, `VARIANT`, and `JSON`.

Strict functions reject malformed encodings, paths, incompatible conversions, numeric
overflow, and malformed JSON. `try_` functions return SQL null for those failures.
Literal paths, target types, and JSON inputs are compiled or parsed once per query.
Vectorized execution reuses thread-confined cursors and unboxed result holders.

JSON parsing is bounded to 100 nesting levels. Parquet Variant decimal encoding is
bounded to 38 digits of precision and a scale of at most 38 after exact normalization.
Exponent bounds are checked before expansion so hostile inputs cannot request
pathological intermediate allocations.

### 7.1 Null states

Storage and query null handling are required because four states must remain distinct:

| State | Stored bytes / null vector | `variant_exists` | `is_variant_null` | `variant_typeof` | scalar `variant_get` |
|---|---|---:|---:|---|---|
| SQL null | empty bytes, null bit set | SQL null | false | SQL null | SQL null |
| Variant null | non-empty `PVAR`, null bit clear | true | true | `NULL` | SQL null |
| missing path | valid non-empty `PVAR` | false | false | SQL null | SQL null |
| Variant string `"null"` | non-empty `PVAR` | true | false | `STRING` | `"null"` |

Extracting a Variant null as `VARIANT` retains its non-empty envelope.
`variant_to_json` returns SQL null for SQL null, JSON text `null` for Variant null, and
JSON text `"null"` (including quotes) for the Variant string.

## 8. Query wire and client compatibility

The protobuf expression enum assigns `VARIANT` permanent number `24`. Numbers `22` and
`23` remain reserved for the separately allocated UUID contract. Existing numbers are
not renumbered. A frozen pre-Variant proto test verifies that an old peer reads value
`24` as `UNRECOGNIZED`, while a current peer continues to read every legacy value.

Data blocks and `DataSchema` carry the logical `VARIANT` token, with a `ByteArray`
internal representation. JSON, Arrow, Java, HTTP JDBC, and gRPC JDBC response paths
preserve the distinction among SQL null, Variant null, and the Variant string `"null"`.

There is no type negotiation or safe downgrade to `BYTES`. An old node can read
existing non-Variant traffic, but it cannot plan or execute an active Variant query.

## 9. Deployment and rollback

This feature has an activation gate:

1. Upgrade controllers, brokers, servers, minions, clients, and every external segment
   generation or ingestion job.
2. Verify all processes use the Variant-capable build.
3. Only then register a schema containing `VARIANT` or upload a segment containing a
   `PVAR` column.

Existing schemas, segments, and queries are unaffected during the rolling binary
upgrade. Variant columns must remain inactive until the fleet is homogeneous.

Do not roll back to a pre-Variant binary while a Variant schema, segment, or in-flight
query is active. A rollback first requires draining Variant queries and removing or
migrating every active Variant table and segment.

## 10. Review and delivery decomposition

The implementation touches multiple ownership areas because no partial state is safe
to activate. The review dependency order is:

1. public type, capability policy, `PVAR` framing, and query-wire number;
2. schema/table/index validation and Parquet ingestion;
3. function semantics plus single-stage and multi-stage guards;
4. DDL, response, Java, JDBC, gRPC, JSON, and Arrow propagation; and
5. quickstart, integration tests, compatibility tests, and benchmarks.

The initial implementation is presented as one end-to-end draft so reviewers can
evaluate one activation contract and run one acceptance test. It is not merge-ready
until SPI/wire, Parquet, both query engines, and client/response owners approve their
areas. If maintainers prefer independent rollback units, the five groups above form a
dependency-ordered PR stack; every intermediate PR must compile, keep Variant
unactivatable until the safety guards land, and preserve the permanent wire allocation.

## 11. Verification and future work

The acceptance suite creates a table, ingests a real Parquet `VARIANT(1)` fixture, and
queries it through both engines. Unit suites cover envelope golden bytes, old/new proto
behavior, direct/read-only buffers, unshredded and shredded reconstruction, decimal
bounds, null states, every raw-value guard, clients, DDL, and reader lifecycle.

Future proposals can independently address nested/repeated Variant, streaming formats,
quoted path keys, indexes over materialized subpaths, a canonical semantic equality
contract, and version negotiation. None may silently widen the v1 persisted or raw-query
semantics described here.
