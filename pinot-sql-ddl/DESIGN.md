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
# Pinot SQL DDL — Design Document

Status: implemented.
Tracked PR: [apache/pinot#18241](https://github.com/apache/pinot/pull/18241).

## 1. Goals

1. Add `CREATE TABLE`, `DROP TABLE`, `SHOW TABLES`, `SHOW CREATE TABLE` as first-class
   SQL DDL on the controller.
2. Make DDL a **thin sugar layer** over the existing `Schema` + `TableConfig` model: a DDL
   statement compiles into the exact same `(Schema, TableConfig)` pair that the JSON
   `/schemas` and `/tables` endpoints accept, and the controller persists through the
   same `PinotHelixResourceManager` paths and the same `TableConfigValidationUtils`
   pipeline. There must be **one** state machine, not two.
3. `SHOW CREATE TABLE` must be a deterministic, idempotent inverse of `CREATE TABLE`:
   `emit → parse → compile → emit` is a fixed point for any DDL-expressible
   `(Schema, TableConfig)` pair.
4. The grammar and `PROPERTIES (...)` routing must be **forward-compatible** with future
   TableConfig evolution — adding a new nested config or a new stream key must not
   require grammar changes in the common case.
5. The DDL endpoint must not introduce a parallel validation surface — every check the
   JSON API runs (min replicas, storage quota, hybrid pair, instance assignment, tenant
   tags, task configs, registry validators, tuners) must run for DDL too.

### 1.1 Non-goals

- No new query semantics. DDL is admin-only; DQL/DML are unchanged.
- No new JSON wire format for TableConfig — the canonical persisted shape stays
  identical. DDL is a presentation/grammar layer.
- No automatic schema migration on `CREATE TABLE`. The hybrid second-variant case
  is supported, but altering an existing column or shape change is out of scope
  (deferred — would surface via a future `ALTER TABLE`).
- No `ALTER TABLE`, no `INSERT INTO ... VALUES`. (`INSERT INTO ... FROM FILE` already
  exists separately.)

## 2. Architecture

```
                ┌─────────────────────────────┐
   POST /sql/ddl│   PinotDdlRestletResource   │  (pinot-controller)
                └──────────────┬──────────────┘
                               │ DdlCompiler.compile(sql)
                               ▼
                ┌─────────────────────────────┐
                │   pinot-sql-ddl module      │  ← new module, this PR
                │  ┌───────────────────────┐  │
                │  │   compile/            │  │
                │  │     DdlCompiler       │  │
                │  │     PropertyMapping   │  │
                │  │     DataTypeMapper    │  │
                │  │     CompiledDdl …     │  │
                │  ├───────────────────────┤  │
                │  │   resolved/          │  │
                │  │     ResolvedColumnDef │  │
                │  │     ResolvedTableDef  │  │
                │  ├───────────────────────┤  │
                │  │   reverse/            │  │
                │  │     CanonicalDdlEmit  │  │
                │  │     SchemaEmitter     │  │
                │  │     PropertyExtractor │  │
                │  │     SqlIdentifiers    │  │
                │  └───────────────────────┘  │
                └──────────────┬──────────────┘
                               │ uses
                               ▼
                ┌─────────────────────────────┐
                │  Calcite parser             │  (pinot-common)
                │  parserImpls.ftl            │
                │  + 6 SqlPinot* AST classes  │
                └─────────────────────────────┘
```

Module boundary rationale:
- The Calcite AST nodes live in **`pinot-common`** so the parser can be invoked by other
  modules without pulling in the DDL compile/reverse logic. The broker query path uses
  the same parser to classify SQL into DQL/DML/DDL.
- The compile/reverse logic lives in **`pinot-sql-ddl`** — a new module that depends only
  on `pinot-spi` (for `Schema`, `TableConfig`, `FieldSpec`), `pinot-common` (for the AST
  nodes), and `calcite-babel` (for the parser builder). No reverse dependencies.
- The REST endpoint and integration tests live in **`pinot-controller`**. The controller
  depends on `pinot-sql-ddl` one-way; nothing else imports it.

## 3. Grammar

The grammar lives in `pinot-common/src/main/codegen/includes/parserImpls.ftl` and is
generated into `SqlParserImpl` by the existing JavaCC + FMPP build chain.

### 3.1 Productions

```
SqlPinotCreateTable :=
    CREATE TABLE [IF NOT EXISTS] CompoundIdentifier
    PinotColumnList
    [PinotPrimaryKeyList]
    TABLE_TYPE = (OFFLINE | REALTIME)
    [PROPERTIES (PinotPropertyList)]

SqlPinotDropTable :=
    DROP TABLE [IF EXISTS] CompoundIdentifier
    [TYPE (OFFLINE | REALTIME)]

SqlPinotShow :=
    SHOW (
        TABLES [FROM SimpleIdentifier]
      | CREATE TABLE CompoundIdentifier [TYPE (OFFLINE | REALTIME)]
    )

PinotColumnDeclaration :=
    SimpleIdentifier DataType [NOT NULL | NULL] [DEFAULT Literal]
    [ DIMENSION [ARRAY] | METRIC | DATETIME FORMAT StringLiteral GRANULARITY StringLiteral ]
```

### 3.2 New tokens (config.fmpp)

`DIMENSION`, `METRIC`, `GRANULARITY`, `OFFLINE`, `REALTIME`, `PROPERTIES`, `TABLES`,
`TABLE_TYPE`, `IF`, plus the Pinot-native data type names `LONG`, `BIG_DECIMAL`, `STRING`,
`BYTES`. **All are added to both `keywords` and `nonReservedKeywordsToAdd`** so existing
identifiers using these names continue to parse — they remain usable as table/column names.

### 3.3 Parser disambiguation

- `LOOKAHEAD(3)` for `IF NOT EXISTS` (3 tokens needed to disambiguate from `IF`-as-identifier).
- `LOOKAHEAD(2)` for `IF EXISTS`.
- `LOOKAHEAD(2)` for the optional PRIMARY KEY clause (`<PRIMARY> <KEY>` is enough; the
  body fails clearly with "expected `(`" on malformed `PRIMARY KEY id` syntax rather
  than backtracking to a misleading "expected TABLE_TYPE").
- The `SHOW` branches are united under a single `SqlPinotShow` entry so JavaCC choice
  logic doesn't need cross-statement lookahead.

## 4. Compile path: AST → (Schema, TableConfig)

### 4.1 DdlCompiler entry points

`DdlCompiler.compile(String sql)` — single static entry point, returns a `CompiledDdl`
discriminated by `DdlOperation`:

- `CREATE_TABLE → CompiledCreateTable { Schema, TableConfig, isIfNotExists, warnings }`
- `DROP_TABLE → CompiledDropTable { rawTableName, databaseName?, tableType?, ifExists }`
- `SHOW_TABLES → CompiledShowTables { databaseName? }`
- `SHOW_CREATE_TABLE → CompiledShowCreateTable { rawTableName, databaseName?, tableType? }`

### 4.2 Resolved IR

The compiler first resolves the parser AST into a typed intermediate representation
(`ResolvedColumnDefinition`, `ResolvedTableDefinition`) that consolidates duplicate-name
detection, role inference, default-value extraction, and warnings. The IR is what
populates the `Schema` (via `toFieldSpec`) and the `TableConfig` (via
`PropertyMapping.apply`).

### 4.3 Type mapping

`DataTypeMapper.resolve(String)` is case-insensitive (`Locale.ROOT`) and accepts both
ANSI-SQL and Pinot-native names:

| DDL alias | Pinot type |
|---|---|
| `INT`, `INTEGER` | INT |
| `BIGINT`, `LONG` | LONG |
| `FLOAT`, `REAL` | FLOAT |
| `DOUBLE` | DOUBLE |
| `DECIMAL`, `NUMERIC`, `BIG_DECIMAL` | BIG_DECIMAL (precision/scale not enforced; warning emitted) |
| `BOOLEAN` | BOOLEAN |
| `TIMESTAMP` | TIMESTAMP |
| `VARCHAR`, `CHAR`, `STRING` | STRING |
| `VARBINARY`, `BINARY`, `BYTES` | BYTES |
| `JSON` | JSON |

`SMALLINT` and `TINYINT` are **rejected explicitly**. Silently widening them to INT today
would lock those DDLs into INT semantics if Pinot later adds INT8/INT16. A rejected type
can become accepted later; a silently-promoted type cannot be narrowed without breaking
users (Hyrum's law / [C1.2](../kb/code-review-principles.md)).

### 4.4 Default-value validation

`DDL DEFAULT 'foo'` for an `INT` column is rejected at compile time, not at first
ingestion. The compiler runs `dataType.convert(literalString)` and surfaces a
`DdlCompilationException` mapped to HTTP 400 if it fails. `DEFAULT NULL` is also
rejected explicitly — Pinot's "default null value" semantic is the value used **when the
source row is null**, so DEFAULT NULL is meaningless and we fail loudly rather than
silently no-op.

### 4.5 Property routing — `PropertyMapping.apply`

The `PROPERTIES (...)` clause is the escape hatch for everything not expressed by
first-class column attributes. Routing is applied in this order:

1. **Promoted scalar.** Lower-cased key matches a known builder field — call
   `TableConfigBuilder.setX(value)`. List-typed builders (e.g. `invertedIndexColumns`)
   accept comma-separated values.
2. **JSON blob.** Lower-cased key matches a known nested-config name —
   `JsonUtils.stringToObject(value, ConcreteClass.class)` and call the builder setter.
3. **Stream config.** Key is `streamType` or starts with `stream.` or `realtime.` —
   store in `IndexingConfig.streamConfigs` verbatim (prefix preserved). REALTIME-only.
4. **Task prefix.** Key matches `task.<taskType>.<key>` — route the remainder into
   `TableTaskConfig.taskTypeConfigsMap[taskType]`.
5. **Custom (fallback).** Anything else — store in `TableCustomConfig.customConfigs`
   verbatim.

Rules 2-5 are the forward-compat hook: stream and minion-task config schemas evolve
independently and need not be in lock-step with the DDL grammar. Adding a new
`fooBarConfig` to TableConfig today and supplying it via DDL **just works** through rule
2 (after a one-line addition to `applyJsonBlob`) or via rule 5 (zero changes — round-trips
through `TableCustomConfig`).

The promoted-scalar catalog and the JSON-blob catalog are kept in `RESERVED_ROUND_TRIP_KEYS`
so the reverse compiler can detect a `TableCustomConfig` entry whose key would be
re-routed to a different home on round-trip and reject it at emission time. This prevents
silent loss: a user who legitimately needs a custom-config key named `replication`
(say, for some downstream tooling) is told at SHOW CREATE TABLE time to rename it.

### 4.6 Hybrid-table second-variant CREATE

When the second physical variant of a hybrid pair is created via DDL with a column list,
the compiled schema is compared against the **stored** schema (which already exists from
the first variant) using `describeColumnShapeMismatch`:

- Compares per-column attributes the DDL column list expresses: name, dataType,
  fieldType (DIMENSION/METRIC/DATETIME), single-value flag, NOT NULL, default null
  value (typed comparison — BIG_DECIMAL via `compareTo`, BYTES via `Arrays.equals`),
  DATETIME format, DATETIME granularity.
- Ignores schema-level metadata the DDL column list cannot express:
  `primaryKeyColumns`, `tags`, `description`, `enableColumnBasedNullHandling`. These
  are already set by the first variant; the second-variant DDL inherits them.

`validateTableConfig` is then called against the **stored** schema (when one exists), not
the compiled schema, so upsert/dedup PK validation sees the canonical PK list rather
than the synthesized empty list from the DDL column-list-only projection.

### 4.7 Validation pipeline

DDL CREATE delegates to `TableConfigValidationUtils.validateTableConfig` — the same
helper `POST /tables` uses. This pipeline:

1. `TableConfigUtils.validateTableName`.
2. `TableConfigUtils.validate(tableConfig, schema, typesToSkip)`.
3. `TableConfigUtils.ensureMinReplicas(tableConfig, controllerConf.getDefaultTableMinReplicas())`.
4. `TableConfigUtils.ensureStorageQuotaConstraints(tableConfig, controllerConf.getDimTableMaxSize())`.
5. `checkHybridTableConfig` (verifies hybrid pair compatibility).
6. `TaskConfigUtils.validateTaskConfigs`.
7. `validateInstanceAssignment` (calls `TableRebalancer.getInstancePartitionsMap`).
8. `validateTableTenantConfig`, `validateTableTaskMinionInstanceTagConfig`.
9. `TableConfigValidatorRegistry.validate`.

`TableConfigTunerUtils.applyTunerConfigs` runs **before** validation, mirroring
`POST /tables`. Tuner mutations on the in-flight TableConfig are reflected back into the
response's `tableConfig` snapshot before returning, so dry-run accurately predicts
the post-persist shape.

### 4.8 Exception → HTTP-status contract

| Exception type | HTTP status |
|---|---|
| `DdlCompilationException` | 400 |
| `IllegalArgumentException` from `CanonicalDdlEmitter.emit` (unsupported column type, reserved-key collision) | 400 |
| `IllegalArgumentException`/`IllegalStateException` from `validateTableConfig` | 400 |
| `TableAlreadyExistsException` (CREATE race) | 409 |
| `SchemaAlreadyExistsException` (CREATE race) | 409 |
| Logical-table reference blocks DROP | 409 |
| `tableTasksCleanup` raises CAE (e.g. active tasks) | preserved (typically 400) |
| Any other RuntimeException from emit | 500 |
| Any other Exception from validate / addSchema / addTable | 500 |

The DROP loop tracks the integer status code rather than the `Response.Status` enum so
non-standard 4xx codes (422, 423, 451) are preserved end-to-end instead of collapsing to 500.

## 5. Reverse path: (Schema, TableConfig) → canonical DDL

`CanonicalDdlEmitter.emit(schema, tableConfig, databaseName?)` produces a deterministic
DDL string. The contract:

- **Lexicographic property ordering.** `PROPERTIES (...)` entries are sorted by key —
  same input → same output.
- **Identifier quoting.** Column and table names that collide with reserved/data-type/DDL
  keywords are double-quoted (e.g. a column named `int` becomes `"int"`).
  `SqlIdentifiers.ALWAYS_QUOTE` is intentionally over-cautious: a few extra quotes never
  break a re-parse, but a missing quote would.
- **Natural-default elision.** A column at its data-type's natural default does **not**
  emit a `DEFAULT` clause; this matches Pinot's JSON serialization rule. Type-aware
  comparison: `Arrays.equals` for BYTES; `BigDecimal.compareTo == 0` for BIG_DECIMAL
  (scale-insensitive).
- **Type-canonical default emission.**
  - BOOLEAN: emit `TRUE` / `FALSE` from internal Integer 0/1.
  - TIMESTAMP: emit quoted UTC ISO-8601 (`Instant.ofEpochMilli(…).toString()`) — never
    `java.sql.Timestamp.toString()` which is JVM-timezone-dependent.
  - BIG_DECIMAL: emit via `toPlainString()` — never scientific notation.
  - BYTES: emit quoted hex via `BytesUtils.toHexString` — never the byte[] identity-hash form.
- **Database qualifier.** If the table's effective database is non-default OR the
  caller passed an explicit database name, emit `db.tableName` so the DDL replays
  correctly without the `Database:` header.
- **Hybrid disambiguation.** `SHOW CREATE TABLE foo` on a hybrid pair returns 400 with
  the message "Use 'TYPE OFFLINE' or 'TYPE REALTIME' to specify which" — the user must
  disambiguate explicitly.
- **Unsupported column types fail loudly.** MAP/LIST/STRUCT/UNKNOWN/COMPLEX FieldSpecs
  throw `IllegalArgumentException` at emission time (mapped to 400) — silent column
  drop is forbidden.
- **Unsupported schema/table metadata fails loudly.** Schema metadata that CREATE DDL
  cannot express yet (`description`, `tags`, `enableColumnBasedNullHandling`) and
  long-tail `TableConfig` fields not covered by `PropertyExtractor` return 400 when
  set to non-default values. Returning incomplete DDL that replays to a weaker schema
  or table config is forbidden.

### 5.1 Reserved-key collision detection

`PropertyExtractor` calls `PropertyMapping.isReservedRoundTripKey(lowerKey)` on every
custom-config entry it encounters. If the user's JSON-API CREATE put a custom-config key
that would shadow a promoted scalar or JSON-blob key on round-trip, SHOW CREATE TABLE
returns 400 with a clear message naming the offending key. This is preferable to a
silent corruption where the DDL replay would route the value to a different field.

## 6. REST endpoint

```
POST /sql/ddl[?dryRun=true|false]
Content-Type: application/json
Authorization: <Bearer | Basic …>
Database: <optional database name>

{ "sql": "<single DDL statement>" }
```

### 6.1 Pipeline

1. **Input size guard.** Reject SQL longer than `MAX_DDL_SQL_CHARS = 256 * 1024` Java
   characters with 400 to bound parser allocation.
2. **Compile.** `DdlCompiler.compile(sql)` → `CompiledDdl`. Compile errors → 400.
3. **Database resolution.** Translate `(databaseName-from-SQL, Database header)` into
   the effective database via `DatabaseUtils.translateTableName` (which rejects
   conflicts).
4. **Authorize.** `ResourceUtils.checkPermissionAndAccess` against the **post-translation**
   resource name, with the operation-appropriate `AccessType` and `Action`.
5. **Dispatch.** Switch on `DdlOperation` to `executeCreate`, `executeDrop`, `executeShow`,
   or `executeShowCreate`.

### 6.2 CREATE flow

```
hasTable(typed)? --yes IF NOT EXISTS --> 200 no-op
              \-- yes !IF NOT EXISTS --> 409
              \-- no --> readStoredSchema(rawName)
                         schemaPreexisted? --> describeColumnShapeMismatch
                         applyTunerConfigs
                         refresh response.tableConfig snapshot
                         validateTableConfig(stored or compiled, tableConfig)
                         if dryRun: 200
                         else: addSchema (if not preexisted) → addTable → 201
```

On `addTable` failure: do **not** roll back `addSchema`. The two-read race window
(`hasOfflineTable + hasRealtimeTable` to decide if the schema is orphaned) could orphan
a sibling's live table. Pinot already lets schemas outlive tables; stale schemas can be
removed via `DELETE /schemas/{name}` if needed.

### 6.3 DROP flow

```
candidates = [foo_OFFLINE, foo_REALTIME]   ── from TYPE clause or both
authorize(candidates) up-front              ── preserves no-fingerprinting
targets = candidates.filter(hasTable)
if targets empty: IF EXISTS? 200 : 404
checkLogicalTableReferences(targets)        ── 409 if any target referenced
for target in targets:
    tableTasksCleanup(target)              ── may throw CAE (e.g. active tasks → 400)
    deleteTable(rawName, type)
    track success/failure + tasksCleared
if all success: 200
else: surface partial-failure error with first-failure status code preserved,
      list of dropped/failed targets, and a recovery hint pointing at
      task-schedule restoration if tasksCleared is non-empty
```

### 6.4 SHOW CREATE TABLE flow

```
resolvedType = TYPE clause? : auto
if resolvedType:                            ── single-variant path
    authorize(resolvedType.typedName)
    hasTable? : 404 otherwise
else:                                       ── no-TYPE: authorize BOTH variants up-front
    authorize(off, rt)                      ── preserves no-fingerprinting under per-type ACLs
    offExists, rtExists = hasTable(off), hasTable(rt)
    both?  -> 400 disambiguation (must specify TYPE)
    off?   -> resolved = OFFLINE
    rt?    -> resolved = REALTIME
    none?  -> 404
fetch tableConfig (null after hasTable=true => 500 ZK inconsistency)
fetch schema     (null  -> 404; schemas can be deleted independently)
emit canonical DDL
```

### 6.5 Authorization contract

- All authorization runs **after** database-name translation, against the
  post-translation `db.tableName_TYPE` resource. This prevents a Database header from
  substituting the resource that auth was checked against (cross-DB privilege
  escalation).
- The no-TYPE forms of DROP and SHOW CREATE require permission on **both** variants
  up-front. Under per-type ACL plugins, this prevents a partial-permission caller from
  fingerprinting the existence of the variant they don't have access to. Partial-permission
  callers must use the explicit `TYPE OFFLINE | REALTIME` clause.

## 7. Forward-compatibility

- **New TableConfig nested config (e.g. `fooBarConfig`):** add one `case "foobarconfig"`
  to `applyJsonBlob` and one branch to `PropertyExtractor.extract` — no grammar change.
  Until that's done, a stored first-class `TableConfig` field should be guarded by the
  reverse path so SHOW CREATE TABLE fails fast instead of emitting incomplete DDL.
  Unknown custom metadata still round-trips through `TableCustomConfig` (rule 5).
- **New Kafka stream property:** zero changes — rule 3 routes `streamType` and any
  `stream.*` / `realtime.*` key verbatim.
- **New minion task type:** zero changes — rule 4 routes any `task.<taskType>.<key>`.
- **New simple TableConfig builder field:** add one case to `applyPromoted` and one to
  `PropertyExtractor`. Forgetting one half is a CI failure: `RoundTripTest.promotedScalarsAddedInSlice2RoundTrip`
  is the kitchen-sink test that fails when a key is emitted by the extractor but not
  consumed by the mapping.

## 8. Backward-compatibility

- The DDL feature is purely additive. No existing endpoint, SPI signature, enum, or
  wire format is renamed or removed.
- New SQL keywords (`DIMENSION`, `METRIC`, `GRANULARITY`, `OFFLINE`, `REALTIME`,
  `PROPERTIES`, `TABLES`, `TABLE_TYPE`, `IF`) are added as **non-reserved**, so existing
  identifier usage continues to parse. A column named `metric` works in DQL exactly as
  before; on canonical-DDL emission it is double-quoted.

### 8.1 Rolling-upgrade safety

The DDL surface is safe under any rolling-upgrade ordering because the only persisted
artifacts it produces (Schema + TableConfig in ZK) are the same shape that
`POST /tables` and `POST /schemas` already produce. Concretely:

- **No ZK schema change.** `DdlCompiler` returns a stock `(Schema, TableConfig)` pair
  and persists them through the existing `PinotHelixResourceManager` paths. There are
  no new ZK property-store paths, no new fields on `TableConfig`/`Schema`, and no
  versioning bumps. A new-controller CREATE produces a ZK record indistinguishable
  from one written by an old controller via the JSON API.
- **Old controllers, brokers, and servers see no difference at runtime.** They read
  the same `TableConfig`/`Schema` JSON they always read. The DDL-created table is a
  regular Pinot table from their perspective.
- **Pre-DDL controllers simply 404 on `POST /sql/ddl`.** Clients that probe the new
  endpoint during the rolling window get a clean 404 from the old binary and a
  normal response from the new binary. There is no in-between state in which the
  endpoint half-exists.
- **Direction of rollout is irrelevant.** Upgrading controllers first, brokers
  first, servers first, or any interleaving all produce the same on-disk
  representation. A subsequent rollback to a pre-DDL controller continues to serve
  tables created via DDL identically to JSON-API-created tables.
- **New SQL keywords do not break DQL.** Because every new keyword is added under
  `nonReservedKeywordsToAdd` in `config.fmpp`, existing user queries that use words
  like `dimension`, `metric`, `format`, or `granularity` as identifiers continue to
  parse on the new binary. A pre-DDL binary never saw the tokens at all.

## 9. Concurrency and consistency

- All DDL helpers (`DdlCompiler`, `CanonicalDdlEmitter`, `SchemaEmitter`,
  `PropertyExtractor`, `PropertyMapping`, `SqlIdentifiers`) are stateless static utilities
  and thread-safe.
- The REST resource is request-scoped (JAX-RS); the per-target outcome-tracking
  collections in `executeDrop` are local to the request thread.
- ZK writes (`addSchema`, `addTable`, `deleteTable`) go through the existing
  `PinotHelixResourceManager` paths — no new ZK write surface introduced. Concurrency
  semantics match `POST /tables` and `DELETE /tables/{name}` exactly.
- The `hasTable → addTable` race window during CREATE is detected via
  `TableAlreadyExistsException` (mapped to 409). The schema is intentionally **not**
  rolled back on `addTable` failure to avoid the non-atomic two-read race that could
  delete a concurrent sibling's schema.

## 10. Testing strategy

| Suite | LOC | Focus |
|---|---|---|
| `PinotDdlParserTest` | 399 | Grammar — every syntax variant, `IF NOT EXISTS`, `IF EXISTS`, `SHOW CREATE TABLE`, `SHOW TABLES FROM`, keyword-as-identifier, malformed `PRIMARY KEY` error |
| `DdlCompilerTest` | 483 | Compile pipeline — every property routing rule, all data type aliases, role inference, default-value type compatibility, negative cases, DECIMAL precision warning, JSON-blob round-trip |
| `CanonicalDdlEmitterTest` | 433 | Reverse path golden-output — canonical clause order, lexicographic property order, BIG_DECIMAL plain-string, BOOLEAN literals, TIMESTAMP ISO-8601, BYTES hex, ComplexFieldSpec rejection, custom-config key shadowing rejection |
| `RoundTripTest` | 441 | `emit → parse → compile → emit` idempotence across stream/task/custom configs, ingestion JSON-blob, MV dimensions, primary keys, identifiers needing quoting, kitchen-sink promoted-scalars |
| `PinotDdlRestletResourceUnitTest` | 290 | `describeColumnShapeMismatch` — every column attribute, BYTES content equality, BIG_DECIMAL `compareTo` equivalence, DATETIME format/granularity |
| `PinotDdlRestletResourceTest` | 362 | Controller integration on a real `ControllerTest` cluster — CREATE/DROP/SHOW round-trip, dry-run, IF [NOT] EXISTS, 201/200/404/409/400 status codes, DB-qualified DDL, oversize input rejection |

## 11. Known limitations and future work

- **No `ALTER TABLE`.** Schema/config evolution still goes through `PUT /tables` and
  `PUT /schemas`. Adding `ALTER TABLE` is a natural follow-up slice.
- **PropertyExtractor long-tail.** Some `IndexingConfig` /
  `SegmentsValidationAndRetentionConfig` fields are not yet emitted by the reverse
  path. When those fields are set to non-default values, SHOW CREATE TABLE fails fast
  with a 400 instead of returning DDL that would silently drop them. Tracked as Slice 4.
- **PRIMARY KEY round-trip on legacy `TimeFieldSpec`.** The legacy time field path emits
  as `DATETIME` but doesn't yet carry NOT NULL / DEFAULT through. Narrow case;
  fixable in a follow-up.
- **Auth integration tests.** Current integration tests run with the default
  AllowAll access controller. A follow-up should add tests with a per-type ACL mock
  to lock in the no-fingerprinting auth-ordering invariants.
- **Mixed-version classifier test.** No automated test exercises a mixed-version
  cluster (old broker, new controller) hitting the new endpoint. Documented behavior
  is that an old controller 404s on `/sql/ddl`, which is rolling-upgrade-safe.

## 12. Decision log

- **Single dispatch endpoint vs one-per-operation.** Chose single `POST /sql/ddl`. The
  client surface is smaller, dispatch logic is in one place, and the response shape is
  uniform (single DTO with operation-specific fields filtered via `@JsonInclude(NON_NULL)`).
- **`TABLE_TYPE = X PROPERTIES (...)` vs SQL-standard `WITH (k=v, ...)`.** Chose the
  Pinot-flavored form. Reasoning: `TABLE_TYPE` is required and semantically unique to
  Pinot; making it a positional clause separates it from arbitrary key/value properties.
  Trino and Snowflake users will recognize the shape; Postgres users will not. We
  accepted this divergence for clarity over portability.
- **No-rollback on CREATE failure.** Chose to leave the schema in place if `addTable`
  fails. The alternative (rollback) requires non-atomic existence checks that race a
  concurrent sibling CREATE. Stale schemas are recoverable (`DELETE /schemas/{name}`);
  orphan tables (with their schema deleted) are not.
- **No-fingerprinting auth.** Chose up-front double-auth on no-TYPE forms. The
  alternative (auth-after-existence) leaks variant existence to partial-permission
  callers under per-type ACL plugins.
- **Reject SMALLINT/TINYINT.** Chose explicit rejection over silent INT widening.
  Reversible (we can later accept these names with narrower semantics); the inverse
  is not.
- **`pinot-sql-ddl` as a separate module vs folding into `pinot-common`.** Chose
  separate module. The compile/reverse logic is substantial (1500+ LOC) and pulls
  Calcite-Babel; isolating it lets `pinot-common` consumers (e.g. broker query path)
  use the parser without paying for the compiler.

## 13. References

- Code review principles: [`kb/code-review-principles.md`](../kb/code-review-principles.md)
- Pinot table config reference: [`pinot-spi/.../TableConfig.java`](../pinot-spi/src/main/java/org/apache/pinot/spi/config/table/TableConfig.java)
- Validation pipeline: [`pinot-controller/.../TableConfigValidationUtils.java`](../pinot-controller/src/main/java/org/apache/pinot/controller/api/resources/TableConfigValidationUtils.java)
- Calcite parser extension docs: [Calcite SQL Parser](https://calcite.apache.org/docs/reference.html)
