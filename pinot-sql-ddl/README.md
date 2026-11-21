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
# pinot-sql-ddl

Pinot SQL DDL compiler and reverse-emitter. Translates `CREATE TABLE`, `CREATE TABLE LIKE`, `DROP TABLE`,
`SHOW TABLES`, and `SHOW CREATE TABLE` into the existing `Schema` + `TableConfig` model
and back.

## Quick example

```sql
CREATE TABLE events (
  id    INT NOT NULL DIMENSION,
  city  STRING DIMENSION,
  amount DOUBLE METRIC,
  ts    TIMESTAMP DATETIME FORMAT 'TIMESTAMP' GRANULARITY '1:MILLISECONDS'
)
TABLE_TYPE = OFFLINE
PROPERTIES (
  'timeColumnName' = 'ts',
  'replication' = '3'
);
```

POST the SQL to `/sql/ddl` on the controller (see [`DESIGN.md`](DESIGN.md) §6 and the
PR description for the full REST contract and more examples).

## Create a table from an existing definition

```sql
CREATE TABLE events_copy LIKE events;
CREATE TABLE IF NOT EXISTS events_offline_copy LIKE events TYPE OFFLINE;
CREATE TABLE archive.events_copy LIKE production.events;
```

Without `TYPE`, the target inherits all source variants: OFFLINE creates OFFLINE,
REALTIME creates REALTIME, and a hybrid source creates both variants with one copied
schema. `TYPE OFFLINE` or `TYPE REALTIME` selects only that source variant. Source
and target identifiers must be raw names without `_OFFLINE` or `_REALTIME` suffixes.
Source and target may be in different databases in the same cluster. An unqualified source
uses the request's database header, or the default database, independently of the
target's explicit database qualifier.

The schema and table configs are deep copies of the persisted definitions. Table
and schema names change; stream settings, task settings, external references, and
environment-variable placeholders in supported settings (such as topics, credentials,
and custom values) are inherited. Target-side tuning and existing validation rules
still apply. Materialized views and Pinot logical-table objects are not supported
as sources.

An absent source variant is omitted. If a selected source variant's stored config
cannot be read or parsed, the request fails before creating any target resources.
An explicit `TYPE` reads only the selected variant.

Existing segments, stream offsets, and runtime state are not copied. REALTIME
creation starts normal ingestion using the copied stream configuration, so the
target may receive data immediately. Use `?dryRun=true` to inspect the copied
`tableConfigs` without creating resources.

Any existing target variant causes a conflict, or a no-op with `IF NOT EXISTS`.
The no-op does not add missing variants to an existing table. Hybrid creation is
sequential after validation of both configs; it is not atomic. On a write failure,
the error response includes `createdTables`, listing variants whose creation
completed. Successful variants and the schema are retained. Inspect that state
before cleanup or retry; `IF NOT EXISTS` does not repair a partial creation.

## Module layout

| Sub-package | Responsibility |
|---|---|
| `compile/` | Forward path: AST → `(Schema, TableConfig)`. Entry point `DdlCompiler`. |
| `resolved/` | Typed intermediate representation between AST and final config. |
| `reverse/` | Reverse path: stored `(Schema, TableConfig)` → canonical DDL string. Entry point `CanonicalDdlEmitter`. |

## Where to read more

- [`DESIGN.md`](DESIGN.md) — design document covering grammar, routing rules, validation
  pipeline, exception → HTTP status contract, and decision log.
- [`src/main/java/org/apache/pinot/sql/ddl/package-info.java`](src/main/java/org/apache/pinot/sql/ddl/package-info.java)
  — module-level Javadoc with the same structure for IDE discoverability.
- [PR #18241](https://github.com/apache/pinot/pull/18241) — landing PR with the full
  user-manual-style examples.
