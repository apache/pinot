/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.sql.ddl.compile;

import java.util.Map;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.utils.CommonConstants.MaterializedViewTask;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// End-to-end compiler tests for `CREATE MATERIALIZED VIEW` → `(Schema, OFFLINE TableConfig)`.
public class DdlCompilerMaterializedViewTest {

  // -------------------------------------------------------------------------------------------
  // Happy path: shape & defaults
  // -------------------------------------------------------------------------------------------

  @Test
  public void minimalCreateMaterializedView() {
    CompiledCreateMaterializedView c = compileMaterializedView(
        "CREATE MATERIALIZED VIEW mv ("
            + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:DAYS',"
            + "  carrier STRING"
            + ")"
            + " REFRESH EVERY 1 DAY"
            + " PROPERTIES ('timeColumnName' = 'ts', 'bucketTimePeriod' = '1d')"
            + " AS SELECT ts, carrier FROM src");
    assertEquals(c.getOperation(), DdlOperation.CREATE_MATERIALIZED_VIEW);
    assertEquals(c.getTableConfig().getTableType(), TableType.OFFLINE);
    assertEquals(c.getTableConfig().getTableName(), "mv_OFFLINE");
    assertEquals(c.getSchema().getSchemaName(), "mv");

    Map<String, String> mv = materializedViewTaskConfig(c.getTableConfig());
    assertEquals(mv.get(MaterializedViewTask.DEFINED_SQL_KEY), "SELECT ts, carrier FROM src");
    assertEquals(mv.get(MaterializedViewPropertyRouter.SCHEDULE_KEY), "0 0 0 * * ?");
    assertEquals(mv.get(MaterializedViewTask.BUCKET_TIME_PERIOD_KEY), "1d");
    assertEquals(c.getTableConfig().getValidationConfig().getTimeColumnName(), "ts");
    assertTrue(c.getSchema().getFieldSpecFor("ts") instanceof DateTimeFieldSpec);
  }

  /// Omitting REFRESH entirely must elide the per-table schedule. Without this contract a
  /// REFRESH-less MV would silently default to a synthesized cron, which is exactly the
  /// invisible behaviour the schema/optionality redesign was meant to avoid: callers who
  /// want the cluster-wide cron must be able to ask for it by saying nothing.
  @Test
  public void omittingRefreshClauseElidesScheduleKey() {
    CompiledCreateMaterializedView c = compileMaterializedView(
        "CREATE MATERIALIZED VIEW mv ("
            + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:DAYS',"
            + "  carrier STRING"
            + ")"
            + " PROPERTIES ('timeColumnName' = 'ts', 'bucketTimePeriod' = '1d')"
            + " AS SELECT ts, carrier FROM src");
    Map<String, String> mv = materializedViewTaskConfig(c.getTableConfig());
    // `definedSQL` must still be present — the AS clause is independent of REFRESH. Same for
    // the unrelated `bucketTimePeriod`, which is owned by the PROPERTIES path.
    assertEquals(mv.get(MaterializedViewTask.DEFINED_SQL_KEY), "SELECT ts, carrier FROM src");
    assertEquals(mv.get(MaterializedViewTask.BUCKET_TIME_PERIOD_KEY), "1d");
    assertFalse(mv.containsKey(MaterializedViewPropertyRouter.SCHEDULE_KEY),
        "Without REFRESH the schedule key must not appear; PinotTaskManager falls back to "
            + "the cluster-wide MV task cron when the key is absent. Found: " + mv);
  }

  @Test
  public void databaseQualifiedNamePreservedInTableConfig() {
    CompiledCreateMaterializedView c = compileMaterializedView(
        "CREATE MATERIALIZED VIEW analytics.mv ("
            + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:DAYS'"
            + ")"
            + " REFRESH EVERY 1 DAY"
            + " PROPERTIES ('timeColumnName' = 'ts', 'bucketTimePeriod' = '1d')"
            + " AS SELECT ts FROM src");
    assertEquals(c.getDatabaseName(), "analytics");
    assertEquals(c.getTableConfig().getTableName(), "analytics.mv_OFFLINE");
    assertEquals(c.getSchema().getSchemaName(), "mv");
  }

  @Test
  public void ifNotExistsPropagates() {
    CompiledCreateMaterializedView c = compileMaterializedView(
        "CREATE MATERIALIZED VIEW IF NOT EXISTS mv ("
            + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:DAYS'"
            + ")"
            + " REFRESH EVERY 1 DAY"
            + " PROPERTIES ('timeColumnName' = 'ts', 'bucketTimePeriod' = '1d')"
            + " AS SELECT ts FROM src");
    assertTrue(c.isIfNotExists());
  }

  // -------------------------------------------------------------------------------------------
  // REFRESH EVERY → Quartz cron
  //
  // Forward-mapping happy paths (every standard period: 1m..28d) are exercised by
  // {@link #cronToPeriodRoundTripsEveryForwardOutput} below, which round-trips every
  // period periodToCron supports through cronToPeriod and back. Per-period unit tests
  // here would only re-assert that subset.
  // -------------------------------------------------------------------------------------------

  @Test
  public void everySixtyMinutesRejected() {
    DdlCompilationException e = expectThrows(DdlCompilationException.class,
        () -> MaterializedViewPropertyRouter.periodToCron("60m"));
    assertTrue(e.getMessage().contains("exceeds 59 minutes"),
        "Expected hint to use '1h', got: " + e.getMessage());
  }

  @Test
  public void everyTwentyFourHoursRejected() {
    DdlCompilationException e = expectThrows(DdlCompilationException.class,
        () -> MaterializedViewPropertyRouter.periodToCron("24h"));
    assertTrue(e.getMessage().contains("exceeds 23 hours"));
  }

  @Test
  public void everyTwentyNineDaysRejected() {
    DdlCompilationException e = expectThrows(DdlCompilationException.class,
        () -> MaterializedViewPropertyRouter.periodToCron("29d"));
    assertTrue(e.getMessage().contains("28 days"));
  }

  @Test
  public void everyZeroRejected() {
    expectThrows(DdlCompilationException.class,
        () -> MaterializedViewPropertyRouter.periodToCron("0d"));
  }

  @Test
  public void everyUnknownUnitRejected() {
    expectThrows(DdlCompilationException.class,
        () -> MaterializedViewPropertyRouter.periodToCron("1y"));
  }

  @Test
  public void everyNumericPeriodFromDdlNormalizesToCron() {
    // EVERY 15 MINUTES → parser normalizes to '15m' → periodToCron → "0 0/15 * * * ?"
    CompiledCreateMaterializedView c = compileMaterializedView(
        "CREATE MATERIALIZED VIEW mv ("
            + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:HOURS'"
            + ")"
            + " REFRESH EVERY 15 MINUTES"
            + " PROPERTIES ('timeColumnName' = 'ts', 'bucketTimePeriod' = '15m')"
            + " AS SELECT ts FROM src");
    assertEquals(materializedViewTaskConfig(c.getTableConfig())
        .get(MaterializedViewPropertyRouter.SCHEDULE_KEY), "0 0/15 * * * ?");
  }

  // -------------------------------------------------------------------------------------------
  // PROPERTIES routing
  // -------------------------------------------------------------------------------------------

  @Test
  public void taskKnobsRouteToMaterializedViewTaskConfig() {
    CompiledCreateMaterializedView c = compileMaterializedView(
        "CREATE MATERIALIZED VIEW mv ("
            + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:DAYS'"
            + ")"
            + " REFRESH EVERY 1 DAY"
            + " PROPERTIES ("
            + "   'timeColumnName' = 'ts',"
            + "   'bucketTimePeriod' = '1d',"
            + "   'bufferTimePeriod' = '2h',"
            + "   'maxNumRecordsPerSegment' = '500000',"
            + "   'maxTasksPerBatch' = '8',"
            + "   'taskMode' = 'APPEND',"
            + "   'stalenessThresholdMs' = '60000'"
            + " )"
            + " AS SELECT ts FROM src");
    Map<String, String> mv = materializedViewTaskConfig(c.getTableConfig());
    assertEquals(mv.get(MaterializedViewTask.BUCKET_TIME_PERIOD_KEY), "1d");
    assertEquals(mv.get(MaterializedViewTask.BUFFER_TIME_PERIOD_KEY), "2h");
    assertEquals(mv.get(MaterializedViewTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY), "500000");
    assertEquals(mv.get(MaterializedViewTask.MAX_TASKS_PER_BATCH_KEY), "8");
    assertEquals(mv.get(MaterializedViewTask.TASK_MODE_KEY), "APPEND");
    assertEquals(mv.get(MaterializedViewTask.STALENESS_THRESHOLD_MS_KEY), "60000");
  }

  @Test
  public void taskKeysAreCaseInsensitiveButOnWireKeysAreCanonical() {
    // The user writes 'BUCKETTIMEPERIOD' / 'maxtasksperbatch'; downstream consumers read
    // the canonical-cased constants. Router must re-canonicalize.
    CompiledCreateMaterializedView c = compileMaterializedView(
        "CREATE MATERIALIZED VIEW mv ("
            + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:DAYS'"
            + ")"
            + " REFRESH EVERY 1 DAY"
            + " PROPERTIES ('timeColumnName' = 'ts', 'BUCKETTIMEPERIOD' = '1d',"
            + "   'maxtasksperbatch' = '4')"
            + " AS SELECT ts FROM src");
    Map<String, String> mv = materializedViewTaskConfig(c.getTableConfig());
    assertEquals(mv.get(MaterializedViewTask.BUCKET_TIME_PERIOD_KEY), "1d");
    assertEquals(mv.get(MaterializedViewTask.MAX_TASKS_PER_BATCH_KEY), "4");
  }

  @Test
  public void prefixedFormCanonicalizesCaseLikeBareForm() {
    // task.MaterializedViewTask.BUCKETTIMEPERIOD must land under the same canonical-cased
    // on-wire key as the bare 'BUCKETTIMEPERIOD' form; otherwise downstream consumers that
    // read MaterializedViewTask.BUCKET_TIME_PERIOD_KEY miss the value.
    CompiledCreateMaterializedView c = compileMaterializedView(
        "CREATE MATERIALIZED VIEW mv ("
            + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:DAYS'"
            + ")"
            + " REFRESH EVERY 1 DAY"
            + " PROPERTIES ('timeColumnName' = 'ts',"
            + "   'task.MaterializedViewTask.BUCKETTIMEPERIOD' = '1d')"
            + " AS SELECT ts FROM src");
    Map<String, String> mv = materializedViewTaskConfig(c.getTableConfig());
    assertEquals(mv.get(MaterializedViewTask.BUCKET_TIME_PERIOD_KEY), "1d");
  }

  @Test
  public void rawTaskPrefixOnMaterializedViewTaskIsAccepted() {
    CompiledCreateMaterializedView c = compileMaterializedView(
        "CREATE MATERIALIZED VIEW mv ("
            + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:DAYS'"
            + ")"
            + " REFRESH EVERY 1 DAY"
            + " PROPERTIES ('timeColumnName' = 'ts', 'bucketTimePeriod' = '1d',"
            + "   'task.MaterializedViewTask.someExperimentalKnob' = 'on')"
            + " AS SELECT ts FROM src");
    assertEquals(materializedViewTaskConfig(c.getTableConfig()).get("someExperimentalKnob"), "on");
  }

  @Test
  public void otherTaskTypePassesThroughForForwardCompat() {
    CompiledCreateMaterializedView c = compileMaterializedView(
        "CREATE MATERIALIZED VIEW mv ("
            + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:DAYS'"
            + ")"
            + " REFRESH EVERY 1 DAY"
            + " PROPERTIES ('timeColumnName' = 'ts', 'bucketTimePeriod' = '1d',"
            + "   'task.SegmentGenerationAndPushTask.foo' = 'bar')"
            + " AS SELECT ts FROM src");
    Map<String, String> other = c.getTableConfig().getTaskConfig()
        .getConfigsForTaskType("SegmentGenerationAndPushTask");
    assertNotNull(other);
    assertEquals(other.get("foo"), "bar");
  }

  @Test
  public void unknownPropertyFallsBackToCustomConfig() {
    CompiledCreateMaterializedView c = compileMaterializedView(
        "CREATE MATERIALIZED VIEW mv ("
            + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:DAYS'"
            + ")"
            + " REFRESH EVERY 1 DAY"
            + " PROPERTIES ('timeColumnName' = 'ts', 'bucketTimePeriod' = '1d',"
            + "   'org.example.tag' = 'experimental')"
            + " AS SELECT ts FROM src");
    assertNotNull(c.getTableConfig().getCustomConfig());
    assertEquals(c.getTableConfig().getCustomConfig().getCustomConfigs().get("org.example.tag"),
        "experimental");
  }

  // -------------------------------------------------------------------------------------------
  // Reserved-key / conflict rejections
  // -------------------------------------------------------------------------------------------

  @Test
  public void streamPropertyOnMvRejected() {
    DdlCompilationException e = expectThrows(DdlCompilationException.class,
        () -> compileMaterializedView(
            "CREATE MATERIALIZED VIEW mv ("
                + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:DAYS'"
                + ")"
                + " REFRESH EVERY 1 DAY"
                + " PROPERTIES ('timeColumnName' = 'ts', 'bucketTimePeriod' = '1d',"
                + "   'stream.kafka.topic.name' = 't')"
                + " AS SELECT ts FROM src"));
    assertTrue(e.getMessage().contains("REALTIME-only"), e.getMessage());
  }

  @Test
  public void scheduleInPropertiesConflictsWithRefreshEvery() {
    DdlCompilationException e = expectThrows(DdlCompilationException.class,
        () -> compileMaterializedView(
            "CREATE MATERIALIZED VIEW mv ("
                + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:DAYS'"
                + ")"
                + " REFRESH EVERY 1 DAY"
                + " PROPERTIES ('timeColumnName' = 'ts', 'bucketTimePeriod' = '1d',"
                + "   'schedule' = '0 0 * * * ?')"
                + " AS SELECT ts FROM src"));
    assertTrue(e.getMessage().contains("reserved"), e.getMessage());
  }

  @Test
  public void definedSqlInPropertiesConflictsWithAsQuery() {
    DdlCompilationException e = expectThrows(DdlCompilationException.class,
        () -> compileMaterializedView(
            "CREATE MATERIALIZED VIEW mv ("
                + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:DAYS'"
                + ")"
                + " REFRESH EVERY 1 DAY"
                + " PROPERTIES ('timeColumnName' = 'ts', 'bucketTimePeriod' = '1d',"
                + "   'definedSQL' = 'SELECT * FROM src')"
                + " AS SELECT ts FROM src"));
    assertTrue(e.getMessage().contains("reserved"), e.getMessage());
  }

  @Test
  public void taskPrefixedScheduleAlsoRejected() {
    DdlCompilationException e = expectThrows(DdlCompilationException.class,
        () -> compileMaterializedView(
            "CREATE MATERIALIZED VIEW mv ("
                + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:DAYS'"
                + ")"
                + " REFRESH EVERY 1 DAY"
                + " PROPERTIES ('timeColumnName' = 'ts', 'bucketTimePeriod' = '1d',"
                + "   'task.MaterializedViewTask.schedule' = '0 0 * * * ?')"
                + " AS SELECT ts FROM src"));
    assertTrue(e.getMessage().contains("reserved"), e.getMessage());
  }

  /// `isMaterializedView` is identity (decided by the CREATE statement choice + the canonical
  /// flag on TableConfig, PR #18564), not a knob. The compiler stamps the flag automatically
  /// on this path. Accepting it again as a user PROPERTY would silently land in
  /// TableCustomConfig (harmless for identity but a confusing knob to leave dangling) — reject
  /// up front for symmetry with the CREATE TABLE rejection, so the operator does not believe
  /// the value affected anything.
  @Test
  public void isMaterializedViewPropertyRejectedOnCreateMv() {
    DdlCompilationException e = expectThrows(DdlCompilationException.class,
        () -> compileMaterializedView(
            "CREATE MATERIALIZED VIEW mv ("
                + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:DAYS'"
                + ")"
                + " REFRESH EVERY 1 DAY"
                + " PROPERTIES ('timeColumnName' = 'ts', 'bucketTimePeriod' = '1d',"
                + "   'isMaterializedView' = 'true')"
                + " AS SELECT ts FROM src"));
    assertTrue(e.getMessage().contains("isMaterializedView"), e.getMessage());
    assertTrue(e.getMessage().contains("reserved"), e.getMessage());
  }

  // -------------------------------------------------------------------------------------------
  // Consistency
  // -------------------------------------------------------------------------------------------

  @Test
  public void missingTimeColumnNameRejected() {
    DdlCompilationException e = expectThrows(DdlCompilationException.class,
        () -> compileMaterializedView(
            "CREATE MATERIALIZED VIEW mv ("
                + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:DAYS'"
                + ")"
                + " REFRESH EVERY 1 DAY"
                + " PROPERTIES ('bucketTimePeriod' = '1d')"
                + " AS SELECT ts FROM src"));
    assertTrue(e.getMessage().contains("timeColumnName"), e.getMessage());
  }

  @Test
  public void timeColumnNameMustReferenceDatetimeColumn() {
    DdlCompilationException e = expectThrows(DdlCompilationException.class,
        () -> compileMaterializedView(
            "CREATE MATERIALIZED VIEW mv ("
                + "  ts STRING,"
                + "  carrier STRING"
                + ")"
                + " REFRESH EVERY 1 DAY"
                + " PROPERTIES ('timeColumnName' = 'ts', 'bucketTimePeriod' = '1d')"
                + " AS SELECT ts, carrier FROM src"));
    assertTrue(e.getMessage().contains("DATETIME"), e.getMessage());
  }

  @Test
  public void missingBucketTimePeriodRejected() {
    DdlCompilationException e = expectThrows(DdlCompilationException.class,
        () -> compileMaterializedView(
            "CREATE MATERIALIZED VIEW mv ("
                + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:DAYS'"
                + ")"
                + " REFRESH EVERY 1 DAY"
                + " PROPERTIES ('timeColumnName' = 'ts')"
                + " AS SELECT ts FROM src"));
    assertTrue(e.getMessage().contains("bucketTimePeriod"), e.getMessage());
  }

  // -------------------------------------------------------------------------------------------
  // AS <query> raw-substring extraction
  // -------------------------------------------------------------------------------------------

  @Test
  public void definedSqlPreservesUserOriginalText() {
    // Mixed case keywords, block comment, and explicit aliases should all survive intact
    // because we substring the raw user SQL rather than re-printing the AST.
    String sql =
        "CREATE MATERIALIZED VIEW mv ("
            + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:DAYS',"
            + "  carrier STRING"
            + ")"
            + " REFRESH EVERY 1 DAY"
            + " PROPERTIES ('timeColumnName' = 'ts', 'bucketTimePeriod' = '1d')"
            + " AS Select Carrier /* hot column */, ts From src";
    CompiledCreateMaterializedView c = compileMaterializedView(sql);
    assertEquals(materializedViewTaskConfig(c.getTableConfig()).get(MaterializedViewTask.DEFINED_SQL_KEY),
        "Select Carrier /* hot column */, ts From src");
  }

  @Test
  public void definedSqlAcrossMultipleLinesPreserved() {
    String sql =
        "CREATE MATERIALIZED VIEW mv (\n"
            + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:DAYS',\n"
            + "  carrier STRING\n"
            + ") REFRESH EVERY 1 DAY\n"
            + "PROPERTIES ('timeColumnName' = 'ts', 'bucketTimePeriod' = '1d')\n"
            + "AS SELECT ts, carrier\n"
            + "   FROM src\n"
            + "   GROUP BY ts, carrier";
    CompiledCreateMaterializedView c = compileMaterializedView(sql);
    String definedSql = materializedViewTaskConfig(c.getTableConfig())
        .get(MaterializedViewTask.DEFINED_SQL_KEY);
    assertTrue(definedSql.startsWith("SELECT ts, carrier"), definedSql);
    assertTrue(definedSql.contains("GROUP BY ts, carrier"), definedSql);
    // No "AS " keyword leaked into the captured slice.
    assertTrue(!definedSql.toUpperCase().startsWith("AS "), definedSql);
  }

  @Test
  public void trailingSemicolonStrippedFromDefinedSql() {
    CompiledCreateMaterializedView c = compileMaterializedView(
        "CREATE MATERIALIZED VIEW mv ("
            + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:DAYS'"
            + ")"
            + " REFRESH EVERY 1 DAY"
            + " PROPERTIES ('timeColumnName' = 'ts', 'bucketTimePeriod' = '1d')"
            + " AS SELECT ts FROM src;");
    assertEquals(materializedViewTaskConfig(c.getTableConfig()).get(MaterializedViewTask.DEFINED_SQL_KEY),
        "SELECT ts FROM src");
  }

  /// Regression for the SqlOrderBy wrapper bug: when the AS clause carries a top-level LIMIT,
  /// Calcite wraps the SqlSelect in a SqlOrderBy whose parser position covers only the LIMIT
  /// substring. A naive `queryNode.getParserPosition()` slice would store `LIMIT 1000` as the
  /// `definedSQL` and the scheduler's auto-LIMIT injection would re-parse garbage. The fixed
  /// extractor walks the AST and unions every descendant position, so the captured slice spans
  /// the whole `SELECT ... LIMIT 1000`.
  @Test
  public void definedSqlPreservesTopLevelLimitWrapper() {
    CompiledCreateMaterializedView c = compileMaterializedView(
        "CREATE MATERIALIZED VIEW mv ("
            + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:DAYS',"
            + "  carrier STRING"
            + ")"
            + " REFRESH EVERY 1 DAY"
            + " PROPERTIES ('timeColumnName' = 'ts', 'bucketTimePeriod' = '1d')"
            + " AS SELECT ts, carrier FROM src LIMIT 1000");
    String definedSql = materializedViewTaskConfig(c.getTableConfig())
        .get(MaterializedViewTask.DEFINED_SQL_KEY);
    assertEquals(definedSql, "SELECT ts, carrier FROM src LIMIT 1000",
        "Top-level LIMIT must be included in the captured definedSQL (SqlOrderBy wrapper "
            + "regression). Got: " + definedSql);
  }

  /// Sibling regression: ORDER BY also wraps the SELECT in a SqlOrderBy. Without the AST walk
  /// the captured definedSQL would just be the ORDER BY clause.
  @Test
  public void definedSqlPreservesTopLevelOrderByWrapper() {
    CompiledCreateMaterializedView c = compileMaterializedView(
        "CREATE MATERIALIZED VIEW mv ("
            + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:DAYS',"
            + "  carrier STRING"
            + ")"
            + " REFRESH EVERY 1 DAY"
            + " PROPERTIES ('timeColumnName' = 'ts', 'bucketTimePeriod' = '1d')"
            + " AS SELECT ts, carrier FROM src ORDER BY ts");
    String definedSql = materializedViewTaskConfig(c.getTableConfig())
        .get(MaterializedViewTask.DEFINED_SQL_KEY);
    assertEquals(definedSql, "SELECT ts, carrier FROM src ORDER BY ts");
  }

  // NOTE on `WITH` (CTE) and `EXPLAIN` wrappers: the AST walker in `extractDefinedSql` handles
  // `SqlWith` and `SqlExplain` exactly like `SqlOrderBy` (collect every descendant position,
  // union via `SqlParserPos.sum`). We deliberately do NOT add round-trip tests for those forms
  // because `verifyDefinedSqlIsParseable` calls `CalciteSqlParser.compileToPinotQuery`, which
  // does not accept CTEs today (it casts the root to `SqlSelect`) and `EXPLAIN` is parsed as
  // its own statement (it can't appear after `AS`). The walker's generality is intentional —
  // the day Pinot's query layer grows CTE support, MV `definedSQL` extraction needs no edit.

  // -------------------------------------------------------------------------------------------
  // AS <query>: JOIN is not supported (compile-time pre-scan)
  // -------------------------------------------------------------------------------------------

  /// JOIN in the AS clause must be rejected at compile time with a clear, MV-specific
  /// message. Without the AST pre-scan in `DdlCompiler#rejectJoinInDefinedSql` the
  /// downstream substring-reparse path (`verifyDefinedSqlIsParseable` →
  /// `CalciteSqlParser#compileToPinotQuery` → `compileToDataSource`) trips a latent
  /// `ClassCastException` (SqlIdentifier → SqlSelect) the moment the JOIN operands carry
  /// table aliases, which would surface to the operator as an opaque 500.
  @Test
  public void joinWithoutAliasRejected() {
    DdlCompilationException e = expectThrows(DdlCompilationException.class,
        () -> compileMaterializedView(
            "CREATE MATERIALIZED VIEW mv ("
                + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:DAYS'"
                + ")"
                + " REFRESH EVERY 1 DAY"
                + " PROPERTIES ('timeColumnName' = 'ts', 'bucketTimePeriod' = '1d')"
                + " AS SELECT a.ts FROM a JOIN b ON a.id = b.id"));
    assertTrue(e.getMessage().contains("JOIN"), e.getMessage());
    assertTrue(e.getMessage().contains("single source table")
            || e.getMessage().contains("pre-join"),
        "Message must guide the operator to the supported pattern; got: " + e.getMessage());
  }

  /// The aliased variant is the one that previously surfaced as `ClassCastException` from
  /// the downstream re-parse path; pinning it here guards against a regression that would
  /// re-expose the CCE if the pre-scan were ever removed or weakened.
  @Test
  public void joinWithTableAliasRejected() {
    DdlCompilationException e = expectThrows(DdlCompilationException.class,
        () -> compileMaterializedView(
            "CREATE MATERIALIZED VIEW mv ("
                + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:DAYS'"
                + ")"
                + " REFRESH EVERY 1 DAY"
                + " PROPERTIES ('timeColumnName' = 'ts', 'bucketTimePeriod' = '1d')"
                + " AS SELECT x.ts FROM a AS x JOIN b AS y ON x.id = y.id"));
    assertTrue(e.getMessage().contains("JOIN"), e.getMessage());
  }

  /// LEFT / RIGHT / FULL OUTER / CROSS all parse as the same `SqlKind.JOIN` and so flow
  /// through the same rejection. One representative is enough; if a future Calcite upgrade
  /// changes any of these to a different SqlKind, this test will start passing for the wrong
  /// reason — but the `joinWithoutAlias` / `joinWithTableAlias` cases above will catch the
  /// regression of the *primary* INNER JOIN case.
  @Test
  public void leftJoinRejected() {
    DdlCompilationException e = expectThrows(DdlCompilationException.class,
        () -> compileMaterializedView(
            "CREATE MATERIALIZED VIEW mv ("
                + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:DAYS'"
                + ")"
                + " REFRESH EVERY 1 DAY"
                + " PROPERTIES ('timeColumnName' = 'ts', 'bucketTimePeriod' = '1d')"
                + " AS SELECT a.ts FROM a LEFT JOIN b ON a.id = b.id"));
    assertTrue(e.getMessage().contains("JOIN"), e.getMessage());
  }

  // -------------------------------------------------------------------------------------------
  // cronToPeriod (inverse of periodToCron)
  //
  // Inverse-mapping happy paths are covered by
  // {@link #cronToPeriodRoundTripsEveryForwardOutput} below; per-period inverse tests
  // here would only re-assert that same subset.
  // -------------------------------------------------------------------------------------------

  @Test
  public void cronToPeriodReturnsNullForNonStandardCron() {
    // 0 0/60 * * * ? is legal Quartz (every 60 minutes ≈ hourly) but the forward direction
    // would have rejected `60m` as out-of-range, so the inverse must refuse it too — otherwise
    // round-trip would silently change semantics.
    assertNull(MaterializedViewPropertyRouter.cronToPeriod("0 0/60 * * * ?"));
    assertNull(MaterializedViewPropertyRouter.cronToPeriod("0 0 0/24 * * ?"));
    assertNull(MaterializedViewPropertyRouter.cronToPeriod("0 0 0 1/29 * ?"));
  }

  @Test
  public void cronToPeriodReturnsNullForHandTypedCron() {
    // Cron expressions that look nothing like the patterns periodToCron produces.
    assertNull(MaterializedViewPropertyRouter.cronToPeriod("0 30 9 * * MON-FRI"));
    assertNull(MaterializedViewPropertyRouter.cronToPeriod("not a cron"));
    assertNull(MaterializedViewPropertyRouter.cronToPeriod(""));
    assertNull(MaterializedViewPropertyRouter.cronToPeriod(null));
  }

  /// Belt-and-braces: every cron periodToCron can produce must invert cleanly. If a future
  /// edit adds a row to the forward mapping table without adding a matching pattern to
  /// cronToPeriod, this round-trip test will catch the asymmetry.
  @Test
  public void cronToPeriodRoundTripsEveryForwardOutput() {
    String[] periods = {"1m", "2m", "5m", "15m", "30m", "59m", "1h", "2h", "6h", "12h", "23h",
        "1d", "2d", "7d", "14d", "28d"};
    for (String period : periods) {
      String cron = MaterializedViewPropertyRouter.periodToCron(period);
      String back = MaterializedViewPropertyRouter.cronToPeriod(cron);
      assertEquals(back, period,
          "Round-trip lost period '" + period + "' (cron='" + cron + "', back='" + back + "')");
    }
  }

  // -------------------------------------------------------------------------------------------
  // SHOW CREATE MATERIALIZED VIEW
  // -------------------------------------------------------------------------------------------

  @Test
  public void showCreateMaterializedViewCompiles() {
    CompiledDdl compiled = DdlCompiler.compile("SHOW CREATE MATERIALIZED VIEW mv");
    assertEquals(compiled.getOperation(), DdlOperation.SHOW_CREATE_MATERIALIZED_VIEW);
    CompiledShowCreateMaterializedView show = (CompiledShowCreateMaterializedView) compiled;
    assertEquals(show.getRawTableName(), "mv");
    assertNull(show.getDatabaseName());
  }

  @Test
  public void showCreateMaterializedViewPreservesDatabaseQualifier() {
    CompiledDdl compiled = DdlCompiler.compile("SHOW CREATE MATERIALIZED VIEW analytics.mv");
    CompiledShowCreateMaterializedView show = (CompiledShowCreateMaterializedView) compiled;
    assertEquals(show.getDatabaseName(), "analytics");
    assertEquals(show.getRawTableName(), "mv");
  }

  // -------------------------------------------------------------------------------------------
  // DROP MATERIALIZED VIEW
  // -------------------------------------------------------------------------------------------

  @Test
  public void dropMaterializedViewCompiles() {
    CompiledDdl compiled = DdlCompiler.compile("DROP MATERIALIZED VIEW mv");
    assertEquals(compiled.getOperation(), DdlOperation.DROP_MATERIALIZED_VIEW);
    CompiledDropMaterializedView drop = (CompiledDropMaterializedView) compiled;
    assertEquals(drop.getRawTableName(), "mv");
    assertNull(drop.getDatabaseName());
    assertFalse(drop.isIfExists());
  }

  @Test
  public void dropMaterializedViewIfExistsCompiles() {
    CompiledDdl compiled = DdlCompiler.compile("DROP MATERIALIZED VIEW IF EXISTS mv");
    CompiledDropMaterializedView drop = (CompiledDropMaterializedView) compiled;
    assertTrue(drop.isIfExists());
    assertEquals(drop.getRawTableName(), "mv");
  }

  @Test
  public void dropMaterializedViewPreservesDatabaseQualifier() {
    CompiledDdl compiled =
        DdlCompiler.compile("DROP MATERIALIZED VIEW IF EXISTS analytics.mv");
    CompiledDropMaterializedView drop = (CompiledDropMaterializedView) compiled;
    assertEquals(drop.getDatabaseName(), "analytics");
    assertEquals(drop.getRawTableName(), "mv");
    assertTrue(drop.isIfExists());
  }

  // -------------------------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------------------------

  private static CompiledCreateMaterializedView compileMaterializedView(String sql) {
    CompiledDdl c = DdlCompiler.compile(sql);
    assertEquals(c.getOperation(), DdlOperation.CREATE_MATERIALIZED_VIEW);
    return (CompiledCreateMaterializedView) c;
  }

  private static Map<String, String> materializedViewTaskConfig(TableConfig tableConfig) {
    Map<String, String> mv = tableConfig.getTaskConfig() == null ? null
        : tableConfig.getTaskConfig().getConfigsForTaskType(MaterializedViewTask.TASK_TYPE);
    assertNotNull(mv, "MaterializedViewTask config block should be present after compile");
    return mv;
  }
}
