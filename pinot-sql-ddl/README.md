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

Pinot SQL DDL compiler and reverse-emitter. Translates `CREATE TABLE`, `DROP TABLE`,
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
