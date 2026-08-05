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

Productions are listed in the canonical grouping (Catalog → Table → Materialized View)
where the JavaCC LL(*) unification (`SqlPinotShow`, `SqlPinotDrop` — both single
entries that branch internally on `TABLE` vs `MATERIALIZED VIEW`) permits.

```
SqlPinotShow :=                                                ── unified entry under one SHOW token
    SHOW (
        TABLES [FROM SimpleIdentifier]                          ── Catalog DDL
      | CREATE TABLE CompoundIdentifier [TYPE (OFFLINE | REALTIME)]
      | CREATE MATERIALIZED VIEW CompoundIdentifier             ── no TYPE clause: MV is always OFFLINE
    )

SqlPinotCreateTable :=
    CREATE TABLE [IF NOT EXISTS] CompoundIdentifier
    PinotColumnList
    [PinotPrimaryKeyList]
    TABLE_TYPE = (OFFLINE | REALTIME)
    [PROPERTIES (PinotPropertyList)]

SqlPinotDrop :=                                                ── unified entry under one DROP token
    DROP (
        TABLE [IF EXISTS] CompoundIdentifier [TYPE (OFFLINE | REALTIME)]
      | MATERIALIZED VIEW [IF EXISTS] CompoundIdentifier        ── no TYPE clause: MV is always OFFLINE
    )

SqlPinotCreateMaterializedView :=
    CREATE MATERIALIZED VIEW [IF NOT EXISTS] CompoundIdentifier
    PinotColumnList
    [REFRESH EVERY StringLiteral]
    [PROPERTIES (PinotPropertyList)]
    AS Query                                                   ── query-only: SELECT/VALUES/WITH/UNION/…; NOT DML

PinotColumnDeclaration :=
    SimpleIdentifier DataType [NOT NULL | NULL] [DEFAULT Literal]
    [ DIMENSION [ARRAY] | METRIC | DATETIME FORMAT StringLiteral GRANULARITY StringLiteral ]
```

The MV `AS <query>` clause is bound to Calcite's `OrderedQueryOrExpr(ACCEPT_QUERY)` production,
**not** the default `SqlQueryOrDml()`. DML (`INSERT`/`UPDATE`/`DELETE`/`MERGE`) is rejected at
parse time so an `AS INSERT ...` body never reaches the MV analyzer (where it would either be
silently ignored or fail with a non-actionable error well below the grammar). A materialized
view's body is always a projection-style query whose output rows are persisted; widening this
clause back to DML would mean two different storage semantics behind one statement form.

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
  logic doesn't need cross-statement lookahead. The same pattern applies to `DROP`:
  `SqlPinotDrop` is a single entry that branches on `TABLE` vs `MATERIALIZED VIEW`, so JavaCC
  never needs cross-statement lookahead across two productions that both start with `DROP`.

## 4. Compile path: AST → (Schema, TableConfig)

### 4.1 DdlCompiler entry points

`DdlCompiler.compile(String sql)` — single static entry point, returns a `CompiledDdl`
discriminated by `DdlOperation`. Entries below follow the canonical ordering
(Catalog → Table → Materialized View; lifecycle CREATE → SHOW CREATE → DROP inside each
object-level family):

- `SHOW_TABLES → CompiledShowTables { databaseName? }`
- `CREATE_TABLE → CompiledCreateTable { Schema, TableConfig, isIfNotExists, warnings }`
- `SHOW_CREATE_TABLE → CompiledShowCreateTable { rawTableName, databaseName?, tableType? }`
- `DROP_TABLE → CompiledDropTable { rawTableName, databaseName?, tableType?, ifExists }`
- `CREATE_MATERIALIZED_VIEW → CompiledCreateMaterializedView { Schema, TableConfig, isIfNotExists, warnings }`
- `SHOW_CREATE_MATERIALIZED_VIEW → CompiledShowCreateMaterializedView { rawTableName, databaseName? }` — no `tableType` field because the grammar has no `TYPE` clause for this form.
- `DROP_MATERIALIZED_VIEW → CompiledDropMaterializedView { rawTableName, databaseName?, ifExists }` — no `tableType` field; the grammar has no `TYPE` clause for this form because an MV is always realized as an OFFLINE table.

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

### 5.2 Materialized View reverse path

`CanonicalDdlEmitter.emit` dispatches on the canonical `TableConfig#isMaterializedView`
flag (single source of truth, introduced in PR #18564). A config whose flag is set always
emits `CREATE MATERIALIZED VIEW`; everything else emits `CREATE TABLE`. The SPI invariant
`TableConfigUtils.validateMaterializedViewInvariants` keeps the flag and the
`task.MaterializedViewTask.definedSQL` body consistent, so the dispatch never has to
inspect the task block to decide identity.

The MV branch emits:

```
CREATE MATERIALIZED VIEW [db.]name (
  <column block>           ── same SchemaEmitter output the table form uses
)
[REFRESH EVERY '<period>'] ── emitted iff schedule is round-trippable (see Q1=A below)
[PROPERTIES (
  '<key>' = '<value>',     ── lexicographic order, MV-extractor output (see §5.2.2)
  …
)]
AS <definedSQL>;           ── verbatim substring captured at CREATE time (see §5.2.3)
```

#### 5.2.1 Cron ↔ period inversion (Q1=A: reject non-round-trippable schedules)

`MaterializedViewPropertyRouter.periodToCron` rewrites every accepted
`REFRESH EVERY '<N><unit>'` value into a Quartz cron expression that the scheduler
ingests. The reverse direction (`cronToPeriod`) must be the **exact inverse**:

| period string | Quartz cron |
|---|---|
| `<N>m` (1 ≤ N ≤ 59)   | `0 0/N * * * ?` (N=1 collapses to `0 * * * * ?`) |
| `<N>h` (1 ≤ N ≤ 23)   | `0 0 0/N * * ?` (N=1 collapses to `0 0 * * * ?`) |
| `<N>d` (1 ≤ N ≤ 28)   | `0 0 0 1/N * ?` (N=1 collapses to `0 0 0 * * ?`) |

Any cron that does **not** match one of these patterns (a hand-typed
`0 30 9 * * MON-FRI`, an N=60 minute interval, etc.) makes `cronToPeriod` return null;
the MV branch then throws `IllegalArgumentException` mapped to **HTTP 400** rather than
silently dropping the schedule. The DDL grammar has no syntax for raw cron, so emitting
DDL without `REFRESH EVERY` for a non-standard schedule would re-parse into a config
that runs on the cluster-wide cron — a user-invisible behavioural change. The 400
response points the operator at the JSON `/tables` API for inspection until either the
grammar grows raw-cron support or the MV is recreated with a `REFRESH EVERY '<period>'`
clause.

#### 5.2.2 `PROPERTIES` flattening — `CanonicalDdlEmitter.extractMaterializedViewProperties`

The MV-emission helper wraps the regular `PropertyExtractor` and applies three MV-specific
transformations:

1. **Drop synthetic clause keys.** `task.MaterializedViewTask.definedSQL` and
   `task.MaterializedViewTask.schedule` are rendered by dedicated DDL clauses (`AS` and
   `REFRESH EVERY`). Leaving them in `PROPERTIES` would make the re-parse refuse the DDL
   because the forward router treats both keys as reserved-by-clause.
2. **Flatten canonical task knobs.**
   `task.MaterializedViewTask.<bucketTimePeriod|bufferTimePeriod|maxNumRecordsPerSegment>`
   are flattened back to the bare DDL form the forward router accepts. Keeping them
   prefixed would still round-trip semantically but cycle the canonical form between two
   equivalent strings — breaking any caller (config-drift detector, golden-file tests)
   that diffs `SHOW CREATE` output.
3. **Keep arbitrary task entries verbatim.** Any other `task.MaterializedViewTask.<key>`
   or `task.<otherTaskType>.<key>` survives as-is so a hand-authored experimental knob
   (or a future task type composed onto an MV table) does not silently disappear on
   round-trip. Adding a knob to `MaterializedViewPropertyRouter.TASK_CONFIG_KEYS` (the
   forward-router map; the reverse extractor consults it via `canonicalKnobName(...)`)
   is the **only** edit needed to promote a future knob to bare-DDL form.

#### 5.2.3 `AS <query>` substring capture

The compiler stores the user's `definedSQL` as the **verbatim substring** of the
original DDL covered by the `AS <query>` parser node (see
`DdlCompiler.extractDefinedSql`). The MV emitter reproduces that text exactly — comments,
identifier casing, and whitespace are preserved — rather than re-unparsing the parsed
SELECT. Re-unparsing would normalize all of those things and would also strip top-level
`LIMIT` / `ORDER BY` clauses because Calcite wraps them in a `SqlOrderBy` whose
parser position covers only the `LIMIT` token. `extractDefinedSql` therefore walks the
AST (`SqlCall` + `SqlNodeList`) and unions every descendant position via
`SqlParserPos.sum`, capturing the full span across `SqlOrderBy`, `SqlWith`, and
`SqlExplain` wrappers.

#### 5.2.4 Q2=B contract: `SHOW CREATE TABLE` refuses an MV; the inverse also holds

The two reverse DDLs are **strictly partitioned by what the underlying TableConfig is**:

- `SHOW CREATE MATERIALIZED VIEW <mv>` is the only way to inspect an MV.
- `SHOW CREATE TABLE <mv>` returns **HTTP 400** with a message pointing at the
  MV-specific form. Silent fallback to the MV emitter is forbidden because the response
  DDL header (`CREATE TABLE` vs `CREATE MATERIALIZED VIEW`) must always match the
  request shape — otherwise any tooling that compares the two would see a phantom drift.
- The inverse is symmetric: `SHOW CREATE MATERIALIZED VIEW <plain-table>` also returns
  **HTTP 400** with a message pointing at the plain-table form.

The check lives in the controller (`PinotDdlRestletResource.executeShowCreate` /
`executeShowCreateMaterializedView`) and delegates to the canonical
`TableConfig#isMaterializedView` flag — the same predicate the emitter dispatches on —
so a config that the emitter would have routed through the MV branch is caught before
any rendering happens.

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
5. **Dispatch.** Switch on `DdlOperation` to one of `executeShow`, `executeCreate`,
   `executeShowCreate`, `executeDrop`, `executeCreateMaterializedView`,
   `executeShowCreateMaterializedView`, `executeDropMaterializedView`.

Sub-sections below follow the same Catalog → Table → Materialized View grouping as
`DdlOperation`, with lifecycle CREATE → SHOW CREATE → DROP inside each object-level family.

### 6.2 Catalog DDL: SHOW TABLES

```
scopedDatabase = sqlDatabase ?: Database-header ?: DEFAULT_DATABASE
authorize(TargetType.CLUSTER, scopedDatabase, Actions.Cluster.GET_TABLE)
return PinotHelixResourceManager.getAllRawTables(scopedDatabase)
```

Single-database scope is deliberate: returning the union across all databases would
silently leak table names from databases the caller may not have access to. The
authorization pair (`CLUSTER` + `GET_TABLE`) matches the existing
`@Authorize`-annotated `GET /tables` endpoint, so secured deployments grant SHOW TABLES
to callers who already have cluster-level table-listing access rather than requiring a
fictitious per-table READ on a resource named after the database.

### 6.3 Table DDL: CREATE TABLE

```
Q2=B preflight (raw-name exclusivity with MVs):
    offlineConfigOfRawName = getTableConfig(<raw>_OFFLINE)      ── checked for OFFLINE OR REALTIME create
    isMaterializedView(offlineConfigOfRawName)? -> 400 "drop the existing materialized view first"
                                                  (applies under IF NOT EXISTS too — type mismatch ≠ absence)
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

**Q2=B preflight.** The MV-marker check is the CREATE-TABLE symmetric twin of §6.6's CREATE-MV
checks. Without it, a `CREATE TABLE OFFLINE foo` against a name whose OFFLINE znode is already
an MV would 409 with a generic "already exists" — true but unhelpful, because the operator
would not know they were colliding with a derived MV that has its own DROP form, refresh
schedule, and minion task wiring. The same check fires for `CREATE TABLE REALTIME foo` against
an existing MV at `foo_OFFLINE`: the resulting "hybrid pair whose OFFLINE half is a materialized
view" has no defined semantics in the broker rewrite, minion task generator, or consistency
manager. `IF NOT EXISTS` does **not** suppress these errors — it suppresses "object not found
at the requested type", not "wrong DDL verb for the conflicting object" (mirror of `DROP TABLE`
IF EXISTS on an MV → 400 in §6.5).

On `addTable` failure: do **not** roll back `addSchema`. The two-read race window
(`hasOfflineTable + hasRealtimeTable` to decide if the schema is orphaned) could orphan
a sibling's live table. Pinot already lets schemas outlive tables; stale schemas can be
removed via `DELETE /schemas/{name}` if needed.

### 6.4 Table DDL: SHOW CREATE TABLE

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
isMaterializedView(tableConfig)? -> 400 "use SHOW CREATE MATERIALIZED VIEW"  (Q2=B primary)
fetch schema     (null  -> 404; schemas can be deleted independently)
emit canonical DDL
```

### 6.5 Table DDL: DROP TABLE

```
candidates = [foo_OFFLINE, foo_REALTIME]   ── from TYPE clause or both
authorize(candidates) up-front              ── preserves no-fingerprinting
targets = candidates.filter(hasTable)
if targets empty: IF EXISTS? 200 : 404
for target in targets:                      ── Q2=B primary preflight (see below)
    isMaterializedView(target.tableConfig)? -> 400 "use DROP MATERIALIZED VIEW"
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

**Q2=B primary preflight.** The MV-marker check is the symmetric twin of §6.4's
MV-in-SHOW-CREATE-TABLE rejection. Without it the DDL surface would be asymmetric —
`SHOW CREATE TABLE` already refuses to render an MV, but a `DROP TABLE` of the same
target would happily destroy it. The check happens **before** the logical-table-reference
and active-task scans so a pure `DROP TABLE` typo on an MV name returns a fast,
deterministic 400 rather than first paying for two ZK scans.

### 6.6 Materialized View DDL: CREATE MATERIALIZED VIEW

Inherits the §6.3 CREATE pipeline (validate → addSchema → addTable) with three deltas:

1. **MV-specific compile route.** `DdlCompiler.compileCreateMaterializedView` routes
   `REFRESH EVERY '<period>'`, `AS <query>`, and MV-specific knobs through
   `MaterializedViewPropertyRouter` before invoking the shared
   `validateTableConfig` (see §4 / §5.2).
2. **MV authorization.** Same `CREATE_TABLE` action — introducing a separate
   `CREATE_MATERIALIZED_VIEW` action would silently lock operators already granted
   `CREATE_TABLE` out of MV creation on rolling upgrade. The action is intentionally
   reused; future fine-grained MV permissions can layer a `CREATE_MATERIALIZED_VIEW`
   action on top with controller config defaulting to the table grant.
3. **MV is always OFFLINE.** No dual-variant handshake; the typed name is wired through
   `TableNameBuilder.OFFLINE`.

```
Q2=B preflight (raw-name exclusivity, symmetric mirror of §6.3):
    offlineConfig = getTableConfig(<raw>_OFFLINE)
    if offlineConfig is materialized view:
        IF NOT EXISTS? -> 200 no-op (idempotent re-deployment)
        otherwise      -> 409 "already exists"
    else if offlineConfig != null (a plain OFFLINE table holds the name):
        -> 400 "drop the existing table first"   (applies under IF NOT EXISTS too)
    else if hasTable(<raw>_REALTIME):
        -> 400 "drop the existing REALTIME table first"
inherited §6.3 pipeline (validate → addSchema → addTable)
```

All §6.3 invariants (idempotent `IF NOT EXISTS`, dry-run, schema-race recovery,
mismatched-pre-existing-schema → 409) apply unchanged. The two type-mismatch errors above
intentionally return 400 (not 409) because the response code maps to the **kind** of
conflict (type mismatch vs same-kind duplicate), not to whether `IF NOT EXISTS` was
present — see §6.5 for the symmetric `DROP MV IF EXISTS on plain table → 400` rule.

### 6.7 Materialized View DDL: SHOW CREATE MATERIALIZED VIEW

```
typedName = TableNameBuilder.OFFLINE.tableNameWithType(rawName)  ── MV is always OFFLINE
authorize(typedName)                         ── reuses GET_TABLE_CONFIG; an MV is realized as an OFFLINE table
hasTable(typedName)? : 404 otherwise
fetch tableConfig (null after hasTable=true => 500 ZK inconsistency)
isMaterializedView(tableConfig)? : 400 "use SHOW CREATE TABLE"  (Q2=B mirror)
fetch schema     (null  -> 404)
emit canonical DDL via CanonicalDdlEmitter (dispatches to the MV branch)
```

Both §6.4 and §6.7 share the same canonical `TableConfig#isMaterializedView` flag the
emitter dispatches on, so the controller never asks the emitter to render a form that
disagrees with the underlying TableConfig shape.

A previous version of this endpoint also gated on a "corrupted MV" shape (a config that
carried a `MaterializedViewTask` block but was missing the `definedSQL` body) and
returned a distinct 400 with a fix hint. That shape is no longer reachable for any
persisted config: the SPI invariant
`TableConfigUtils.validateMaterializedViewInvariants` (PR #18564) rejects it at addTable
/ updateTable time, and the canonical `isMaterializedView` flag — not the task block —
is now the identity source.

### 6.8 Materialized View DDL: DROP MATERIALIZED VIEW

```
typedName = TableNameBuilder.OFFLINE.tableNameWithType(rawName)  ── MV is always OFFLINE
authorize(typedName)                            ── reuses DELETE_TABLE; an MV is realized as an OFFLINE table
exists = hasTable(typedName) || getTableConfig(typedName) != null
if !exists:
    ifExists?  -> 200 no-op (SQL-standard IF EXISTS semantics)
    otherwise  -> 404 "materialized view not found"
fetch tableConfig (null after hasTable=true => 500 ZK inconsistency)
isMaterializedView(tableConfig)? -> proceed
otherwise -> 400 "use DROP TABLE"  (Q2=B mirror, applies even under IF EXISTS — see below)
assertNoLogicalTableReferences([typedName])     ── 409 if any logical table union references it
assertNoActiveTasksBeforeDrop([typedName])      ── 400 if a MaterializedViewTask is in-flight
if dryRun: 200 "would drop ..."
cleanupTableTasksBeforeDrop(typedName)          ── removes the task schedule; same code path as DROP TABLE
deleteTable(rawName, OFFLINE, null)             ── PinotHelixResourceManager.deleteTable handles MV znodes:
                                                ──   • MaterializedViewDefinitionMetadataUtils.delete
                                                ──   • MaterializedViewRuntimeMetadataUtils.delete
                                                ──   • notifyMaterializedViewConsistencyManagerForTableDrop
                                                ── plus the regular table teardown (ideal state, segments, etc.)
return 200 with deletedTables=[typedName]
```

Cleanup is delegated entirely to `PinotHelixResourceManager#deleteTable` for the same reason
PR4's SHOW CREATE was deliberately small: that path is already the single source of truth
for "drop a Pinot table, including any MV metadata it carries" — adding a parallel cleanup
in the controller would risk drift between the two doors. The legacy
`DELETE /materializedViews/{name}` REST endpoint uses the same call.

**Q2=B mirror under IF EXISTS.** A request like `DROP MATERIALIZED VIEW IF EXISTS foo` against
a name that exists as a plain OFFLINE table returns 400, not 200. `IF EXISTS` exists to make
idempotent provisioning scripts tolerate "already gone"; it is not a license to silently
accept a name that resolves to a different kind of object. Returning 200 in that case would
leave a real OFFLINE table named `foo` standing while the script thinks it cleaned up an
MV — an actively dangerous failure mode. 400 forces the operator to inspect cluster state
before acting.

### 6.9 Authorization contract

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

| Suite | Focus |
|---|---|
| `PinotDdlParserTest` | Grammar — every syntax variant, `IF NOT EXISTS`, `IF EXISTS`, `SHOW CREATE TABLE`, `SHOW CREATE MATERIALIZED VIEW` (rejects `TYPE` clause), `DROP MATERIALIZED VIEW` (rejects `TYPE` clause), `SHOW TABLES FROM`, keyword-as-identifier, malformed `PRIMARY KEY` error |
| `DdlCompilerTest` | Compile pipeline for tables — every property routing rule, all data type aliases, role inference, default-value type compatibility, negative cases, DECIMAL precision warning, JSON-blob round-trip |
| `DdlCompilerMaterializedViewTest` | Compile pipeline for MVs — `REFRESH EVERY` ↔ cron forward routing, `cronToPeriod` inversion (every period the forward table can produce + non-standard cron rejection), `definedSQL` extraction regressions for top-level `LIMIT` and `ORDER BY` (SqlOrderBy wrapper), `SHOW CREATE MATERIALIZED VIEW` compile, `DROP MATERIALIZED VIEW` compile (+ `IF EXISTS`, database-qualified) |
| `CanonicalDdlEmitterTest` | Reverse path golden-output for tables — canonical clause order, lexicographic property order, BIG_DECIMAL plain-string, BOOLEAN literals, TIMESTAMP ISO-8601, BYTES hex, ComplexFieldSpec rejection, custom-config key shadowing rejection |
| `CanonicalDdlEmitterMaterializedViewTest` | Reverse path golden-output for MVs — `REFRESH EVERY` emitted only when round-trippable, non-standard cron 400, MV canonical knobs flattened bare, non-canonical MV task knobs kept under `task.*` prefix, `definedSQL` rendered only in `AS` clause, dispatch sanity check |
| `RoundTripTest` | `emit → parse → compile → emit` idempotence — tables (stream/task/custom configs, ingestion JSON-blob, MV dimensions, primary keys, identifiers needing quoting, kitchen-sink promoted-scalars) plus MVs (no REFRESH, REFRESH EVERY `'1d'`/`'15m'`, full canonical-knob set, database-qualified) |
| `PinotDdlRestletResourceUnitTest` | `describeColumnShapeMismatch` — every column attribute, BYTES content equality, BIG_DECIMAL `compareTo` equivalence, DATETIME format/granularity |
| `PinotDdlRestletResourceMaterializedViewUnitTest` | MV-specific controller wiring — CREATE MV happy path (schedule key present iff REFRESH EVERY), idempotent CREATE on existing MV, dry-run, pre-existing-schema reuse/mismatch, schema race recovery, **CREATE MV on plain OFFLINE table → 400 ± IF NOT EXISTS (Q2=B)**, **CREATE MV when REALTIME half exists → 400 (Q2=B raw-name exclusivity)**, **SHOW CREATE MATERIALIZED VIEW** happy / 404 / 400-on-plain-table, **SHOW CREATE MATERIALIZED VIEW on corrupted MV (task block but no `definedSQL`) → 400 with fix hint**, **SHOW CREATE TABLE on MV → 400 (Q2=B)**, **DROP MATERIALIZED VIEW** happy / 404 ± IF EXISTS / 400-on-plain-table-even-under-IF-EXISTS / dry-run / active-task block, **DROP TABLE on MV → 400 (Q2=B)** |
| `PinotDdlRestletResourceUnitTest` (Q2=B Table side) | **CREATE TABLE OFFLINE on existing MV → 400 ± IF NOT EXISTS (Q2=B)**, **CREATE TABLE REALTIME when MV occupies OFFLINE half → 400 (Q2=B raw-name exclusivity)** |
| `PinotDdlRestletResourceTest` | Controller integration on a real `ControllerTest` cluster — CREATE/DROP/SHOW round-trip, dry-run, IF [NOT] EXISTS, 201/200/404/409/400 status codes, DB-qualified DDL, oversize input rejection |
| `MaterializedViewDdlAnalyzerIntegrationTest` | End-to-end DDL → analyzer hand-off — happy path proving DDL-compiled `(definedSql, TableConfig, Schema, taskConfigs)` reach `MaterializedViewAnalyzer.analyze` in a shape the analyzer accepts; analyzer-reject path proving the analyzer's own error message ("is not produced by any SELECT expression") survives the compile-then-analyze hop unwrapped |

## 11. Known limitations and future work

- **No `ALTER TABLE`.** Schema/config evolution still goes through `PUT /tables` and
  `PUT /schemas`. Adding `ALTER TABLE` is a natural follow-up slice.
- **MV schema ↔ SELECT-projection consistency (column type / role / nullability /
  cardinality).** `MaterializedViewAnalyzer` validates source-table existence,
  time-column TIMESTAMP contract, `bucketTimePeriod` alignment with any `DATETRUNC` in
  the SELECT, definedSQL parseability, nested-SELECT rejection, and column-name coverage
  between the MV schema and the SELECT projection — but does **not** today validate that
  the MV schema's declared per-column types, roles (DIMENSION / METRIC / DATETIME),
  nullability flags, or cardinality (single- vs multi-value) match the corresponding
  SELECT-expression results. This gap exists in the existing JSON `POST /tables` MV
  flow too, but the DDL surface makes it more visible because the schema is declared
  inline in the column list and the query is one clause away. A follow-up should
  thread the projection type information back through the analyzer; until then,
  operators are responsible for keeping the two sides aligned manually.
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
