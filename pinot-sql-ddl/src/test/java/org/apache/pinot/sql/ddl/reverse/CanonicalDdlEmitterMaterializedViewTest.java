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
package org.apache.pinot.sql.ddl.reverse;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants.MaterializedViewTask;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/// Golden-output unit tests for [CanonicalDdlEmitter] on Materialized View configs.
///
/// Mirrors the table-side [CanonicalDdlEmitterTest]: every assertion locks in the exact
/// emitted text (or a clearly motivated substring) so any drift in MV canonical formatting
/// forces a paired edit to the emitter and the test. TableConfig fixtures are built directly
/// (no detour through the compiler) so a regression here is unambiguously the emitter's
/// fault; full emit → parse → emit fidelity is exercised separately in `RoundTripTest`.
public class CanonicalDdlEmitterMaterializedViewTest {

  // -------------------------------------------------------------------------------------------
  // No REFRESH clause (cluster-wide cron path)
  // -------------------------------------------------------------------------------------------

  /// PR3.5 contract: a CREATE MATERIALIZED VIEW without `REFRESH EVERY` produces a TableConfig
  /// with no `schedule` knob, falling back to the cluster-wide MV cron. Round-trip must keep
  /// the REFRESH clause absent — emitting `REFRESH EVERY ''` or any default would silently
  /// pin the MV to a fixed period on re-parse.
  @Test
  public void noScheduleElidesRefreshClause() {
    Schema schema = mvSchema();
    Map<String, String> mvTask = new LinkedHashMap<>();
    mvTask.put(MaterializedViewTask.DEFINED_SQL_KEY, "SELECT ts, carrier FROM src");
    mvTask.put(MaterializedViewTask.BUCKET_TIME_PERIOD_KEY, "1d");
    TableConfig config = mvConfig("mv", mvTask);

    String emitted = CanonicalDdlEmitter.emit(schema, config);

    assertTrue(emitted.startsWith("CREATE MATERIALIZED VIEW mv ("), emitted);
    assertFalse(emitted.contains("REFRESH"),
        "no schedule knob must NOT emit a REFRESH clause; got:\n" + emitted);
    assertTrue(emitted.contains("AS SELECT ts, carrier FROM src;"), emitted);
  }

  // -------------------------------------------------------------------------------------------
  // REFRESH EVERY (cron → period inverse)
  // -------------------------------------------------------------------------------------------

  /// Schedule = `0 0 0 * * ?` (daily at midnight) — periodToCron's exact daily template.
  /// cronToPeriod recovers `'1d'` so the canonical form carries the user-friendly period
  /// string rather than the raw cron expression.
  @Test
  public void scheduleEmittedAsRefreshEveryPeriodWhenRoundTrippable() {
    Schema schema = mvSchema();
    Map<String, String> mvTask = new LinkedHashMap<>();
    mvTask.put(MaterializedViewTask.DEFINED_SQL_KEY, "SELECT ts, carrier FROM src");
    mvTask.put(MaterializedViewTask.BUCKET_TIME_PERIOD_KEY, "1d");
    mvTask.put("schedule", "0 0 0 * * ?");
    TableConfig config = mvConfig("mv", mvTask);

    String emitted = CanonicalDdlEmitter.emit(schema, config);

    assertTrue(emitted.contains("REFRESH EVERY '1d'\n"),
        "REFRESH EVERY clause must use the recovered period; got:\n" + emitted);
    // The flatten + dedupe path must remove `schedule` from PROPERTIES — leaving it would
    // make the re-parse refuse the DDL (RESERVED_BY_CLAUSE_KEYS).
    assertFalse(emitted.contains("'schedule'"),
        "schedule key must be consumed by REFRESH EVERY, not also surface in PROPERTIES; got:\n"
            + emitted);
  }

  /// Q1=A contract: any cron that `cronToPeriod` does not recognise must hard-fail rather
  /// than silently lose the schedule on the round-trip. The DDL grammar today has no syntax
  /// for raw cron, so dropping the field would re-emit a config that runs on the cluster cron
  /// instead of the operator's chosen schedule — a behaviour change with no user-visible
  /// signal. Surface as IllegalArgumentException so the controller returns 400 with the cause.
  @Test
  public void nonStandardCronRejected() {
    Schema schema = mvSchema();
    Map<String, String> mvTask = new LinkedHashMap<>();
    mvTask.put(MaterializedViewTask.DEFINED_SQL_KEY, "SELECT ts, carrier FROM src");
    mvTask.put(MaterializedViewTask.BUCKET_TIME_PERIOD_KEY, "1d");
    mvTask.put("schedule", "0 30 9 * * MON-FRI");
    TableConfig config = mvConfig("mv", mvTask);

    try {
      CanonicalDdlEmitter.emit(schema, config);
      fail("Expected non-standard cron to be rejected on SHOW CREATE MATERIALIZED VIEW");
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("0 30 9 * * MON-FRI"), expected.getMessage());
      assertTrue(expected.getMessage().contains("REFRESH EVERY"),
          "error must point the user at the supported form (REFRESH EVERY); got: "
              + expected.getMessage());
    }
  }

  /// `isMaterializedView()` is just a flag read on `_materializedView`; it does NOT validate
  /// the task-config block.  A TableConfig with the flag set but `getTaskConfig() == null`
  /// (operator-typed JSON, half-written config, znode written by a tool that doesn't go
  /// through the SPI invariant validator) reaches the emitter as legal-shaped input.  The
  /// previous `getTaskConfig().getConfigsForTaskType(...)` chain would NPE here and surface
  /// to the controller as a 500; the fix must produce a 400 with a message that names the
  /// missing layer so the operator can target the right znode field.
  @Test
  public void missingTaskConfigRejected() {
    Schema schema = mvSchema();
    // Deliberately do NOT call setTaskConfig — this is the "operator left the task block off
    // entirely" shape.  setIsMaterializedView(true) is what makes the dispatch route here, so
    // omitting it would not exercise the regression.
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_no_task")
        .setIsMaterializedView(true)
        .setTimeColumnName("ts")
        .build();

    try {
      CanonicalDdlEmitter.emit(schema, config);
      fail("Expected IllegalArgumentException when MV TableConfig has null taskConfig");
    } catch (IllegalArgumentException expected) {
      // The message must (1) be operator-actionable — naming the missing layer so they know
      // which znode field to repair, (2) include the table name so a noisy log can be grepped,
      // and (3) explicitly mention the isMaterializedView flag so the operator understands
      // why this code path was reached at all.
      assertTrue(expected.getMessage().contains("taskConfig block is null"), expected.getMessage());
      assertTrue(expected.getMessage().contains("mv_no_task"), expected.getMessage());
      assertTrue(expected.getMessage().contains("isMaterializedView=true"), expected.getMessage());
    } catch (NullPointerException npe) {
      // Pin the regression vector reviewer flagged: a future refactor that drops the upstream
      // null-guard must fail this test with a clear message rather than slip through.
      fail("Regression: NPE leaked through the emitter — must surface as 400/IllegalArgumentException, not 500. "
          + "Cause: " + npe);
    }
  }

  /// Sibling of [#missingTaskConfigRejected]: the task block exists (and may even carry other
  /// task types like `SegmentRefreshTask`) but the MV-specific `MaterializedViewTask` entry is
  /// absent.  This is the second of the two upstream NPE sites the chained
  /// `getTaskConfig().getConfigsForTaskType(MaterializedViewTask.TASK_TYPE).get(...)`
  /// dereference would have hit — `getConfigsForTaskType` returns the raw `Map.get` of the
  /// task-type map and so returns null when the MV task type isn't registered.
  @Test
  public void missingMaterializedViewTaskBlockRejected() {
    Schema schema = mvSchema();
    Map<String, Map<String, String>> tasks = new LinkedHashMap<>();
    Map<String, String> unrelatedTask = new LinkedHashMap<>();
    unrelatedTask.put("schedule", "0 0 * * * ?");
    // A non-MV task type — the MV entry is completely absent.  This is the shape a TableConfig
    // takes if `setIsMaterializedView(true)` was applied to a config that already had a
    // SegmentRefreshTask schedule but never had its MV task block populated.
    tasks.put("SegmentRefreshTask", unrelatedTask);
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_no_mv_block")
        .setIsMaterializedView(true)
        .setTimeColumnName("ts")
        .setTaskConfig(new TableTaskConfig(tasks))
        .build();

    try {
      CanonicalDdlEmitter.emit(schema, config);
      fail("Expected IllegalArgumentException when MV TableConfig has no MaterializedViewTask block");
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("no '" + MaterializedViewTask.TASK_TYPE + "' entry"),
          expected.getMessage());
      assertTrue(expected.getMessage().contains("mv_no_mv_block"), expected.getMessage());
      // Distinct message from the null-taskConfig case — the operator must be able to tell
      // these two repair paths apart from the log line alone.
      assertFalse(expected.getMessage().contains("taskConfig block is null"),
          "Layer-specific messaging must distinguish `taskConfig is null` from `MV entry "
              + "missing inside taskConfig`; got: " + expected.getMessage());
    } catch (NullPointerException npe) {
      fail("Regression: NPE leaked through the emitter — must surface as 400/IllegalArgumentException, not 500. "
          + "Cause: " + npe);
    }
  }

  // -------------------------------------------------------------------------------------------
  // PROPERTIES flattening for canonical MV task knobs
  // -------------------------------------------------------------------------------------------

  /// MV canonical knobs (`bucketTimePeriod`, `bufferTimePeriod`, `maxNumRecordsPerSegment`)
  /// live under the `MaterializedViewTask` block on disk but are accepted bare by the forward
  /// router. The reverse path must emit them bare so emit → parse → emit produces identical
  /// canonical text — emitting them as `task.MaterializedViewTask.<key>` would still round-
  /// trip semantically but cycle the canonical form between two equivalent strings, breaking
  /// any caller (e.g. a config-drift detector) that diffs `SHOW CREATE` output.
  @Test
  public void canonicalKnobsFlattenedBareNotPrefixed() {
    Schema schema = mvSchema();
    Map<String, String> mvTask = new LinkedHashMap<>();
    mvTask.put(MaterializedViewTask.DEFINED_SQL_KEY, "SELECT ts, carrier FROM src");
    mvTask.put(MaterializedViewTask.BUCKET_TIME_PERIOD_KEY, "1d");
    mvTask.put(MaterializedViewTask.BUFFER_TIME_PERIOD_KEY, "2h");
    mvTask.put(MaterializedViewTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY, "5000000");
    TableConfig config = mvConfig("mv", mvTask);

    String emitted = CanonicalDdlEmitter.emit(schema, config);

    assertTrue(emitted.contains("'bucketTimePeriod' = '1d'"), emitted);
    assertTrue(emitted.contains("'bufferTimePeriod' = '2h'"), emitted);
    assertTrue(emitted.contains("'maxNumRecordsPerSegment' = '5000000'"), emitted);
    assertFalse(emitted.contains("'task.MaterializedViewTask.bucketTimePeriod'"),
        "canonical knob must be flattened, not double-emitted under task.* prefix; got:\n"
            + emitted);
    assertFalse(emitted.contains("'task.MaterializedViewTask.bufferTimePeriod'"), emitted);
    assertFalse(emitted.contains("'task.MaterializedViewTask.maxNumRecordsPerSegment'"), emitted);
  }

  /// Experimental / future MV task knobs (anything not recognized by
  /// [MaterializedViewPropertyRouter#canonicalKnobName]) are kept under the
  /// `task.MaterializedViewTask.*` prefix on emit. The forward router's task-prefix rule
  /// will route them back into the same MV task block on re-parse, so the round-trip is
  /// preserved even though the canonical form is not bare.
  @Test
  public void unknownMvTaskKnobRetainsTaskPrefix() {
    Schema schema = mvSchema();
    Map<String, String> mvTask = new LinkedHashMap<>();
    mvTask.put(MaterializedViewTask.DEFINED_SQL_KEY, "SELECT ts, carrier FROM src");
    mvTask.put(MaterializedViewTask.BUCKET_TIME_PERIOD_KEY, "1d");
    mvTask.put("experimentalKnob", "value42");
    TableConfig config = mvConfig("mv", mvTask);

    String emitted = CanonicalDdlEmitter.emit(schema, config);

    assertTrue(emitted.contains("'task.MaterializedViewTask.experimentalKnob' = 'value42'"),
        "non-canonical MV task knob must keep its task.* prefix so re-parse routes it back; "
            + "got:\n" + emitted);
    assertFalse(emitted.contains("'experimentalKnob' = "),
        "non-canonical knob must NOT be flattened; that would re-parse as an unknown bare "
            + "property; got:\n" + emitted);
  }

  // -------------------------------------------------------------------------------------------
  // Synthetic-key suppression
  // -------------------------------------------------------------------------------------------

  /// `definedSQL` is rendered by the trailing `AS <query>` clause; surfacing it in PROPERTIES
  /// would (a) double-render the query body in canonical form and (b) make the re-parse fail
  /// because the forward router rejects `definedSQL` in PROPERTIES.
  @Test
  public void definedSqlNeverAppearsInProperties() {
    Schema schema = mvSchema();
    Map<String, String> mvTask = new LinkedHashMap<>();
    mvTask.put(MaterializedViewTask.DEFINED_SQL_KEY, "SELECT ts, carrier FROM src");
    mvTask.put(MaterializedViewTask.BUCKET_TIME_PERIOD_KEY, "1d");
    TableConfig config = mvConfig("mv", mvTask);

    String emitted = CanonicalDdlEmitter.emit(schema, config);

    assertFalse(emitted.contains("'definedSQL'"),
        "definedSQL must only appear inside the AS clause; got:\n" + emitted);
    assertFalse(emitted.contains("'task.MaterializedViewTask.definedSQL'"), emitted);
    assertTrue(emitted.endsWith("AS SELECT ts, carrier FROM src;\n"), emitted);
  }

  // -------------------------------------------------------------------------------------------
  // Database-qualified name
  // -------------------------------------------------------------------------------------------

  @Test
  public void databaseQualifiedNameRendered() {
    Schema schema = mvSchema();
    Map<String, String> mvTask = new LinkedHashMap<>();
    mvTask.put(MaterializedViewTask.DEFINED_SQL_KEY, "SELECT ts, carrier FROM src");
    mvTask.put(MaterializedViewTask.BUCKET_TIME_PERIOD_KEY, "1d");
    TableConfig config = mvConfig("analytics.mv", mvTask);

    String emitted = CanonicalDdlEmitter.emit(schema, config, "analytics");

    assertTrue(emitted.startsWith("CREATE MATERIALIZED VIEW analytics.mv ("), emitted);
  }

  // -------------------------------------------------------------------------------------------
  // Dispatch
  // -------------------------------------------------------------------------------------------

  /// Sanity check that the MV dispatch in [CanonicalDdlEmitter#emit] does NOT fire for a
  /// regular OFFLINE table that happens to carry some other task (e.g.
  /// RealtimeToOfflineSegmentsTask). Symmetric guard against the inverse failure of
  /// [#canonicalKnobsFlattenedBareNotPrefixed].
  @Test
  public void nonMaterializedViewWithOtherTaskEmittedAsCreateTable() {
    Schema schema = mvSchema();
    Map<String, String> rto = new LinkedHashMap<>();
    rto.put("bucketTimePeriod", "1d");
    Map<String, Map<String, String>> tasks = new LinkedHashMap<>();
    tasks.put("RealtimeToOfflineSegmentsTask", rto);
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("events")
        .setTimeColumnName("ts")
        .setTaskConfig(new TableTaskConfig(tasks))
        .build();

    String emitted = CanonicalDdlEmitter.emit(schema, config);

    assertTrue(emitted.startsWith("CREATE TABLE events ("),
        "with isMaterializedView=false (default) the dispatch must stay on CREATE TABLE "
            + "regardless of other task blocks; got:\n" + emitted);
    assertFalse(emitted.contains("MATERIALIZED VIEW"), emitted);
  }

  // -------------------------------------------------------------------------------------------
  // Regression: TreeMap delete-with-two-children corrupted canonical knob values
  // -------------------------------------------------------------------------------------------

  /// Caught on live cluster traffic, fixed in `CanonicalDdlEmitter.extractMaterializedViewProperties`:
  /// `TreeMap.deleteEntry` for an internal node with two children copies the in-order
  /// successor's key+value INTO the current Entry node before unlinking the successor — so
  /// the `Map.Entry` reference handed back by the iterator has its `value` field mutated by
  /// the time you call `entry.getValue()` after `it.remove()`. The visible symptom on
  /// `SHOW CREATE MATERIALIZED VIEW` was `'maxNumRecordsPerSegment' = 'dayMillis'` — i.e. the
  /// successor `timeColumnName`'s value bleeding into the canonical knob.
  ///
  /// The repro fixture mirrors the production `orders_daily_mv` shape: enough entries above
  /// and below the canonical knob in lexicographic order that the doomed TreeMap node ends up
  /// internal with two children at deletion time. Each canonical knob is asserted with a
  /// strict `'<knob>' = '<value>'` substring so a single character of value drift fails the
  /// test loudly. The bare `contains("'<value>'")` style would let `'10000'` masquerade as
  /// `'dayMillis'` and miss the corruption entirely.
  @Test
  public void canonicalKnobValuesSurviveTreeMapDeleteRebalance() {
    Schema schema = mvSchema();
    Map<String, String> mvTask = new LinkedHashMap<>();
    mvTask.put(MaterializedViewTask.DEFINED_SQL_KEY,
        "SELECT ts, carrier, COUNT(*) AS c FROM src GROUP BY ts, carrier");
    mvTask.put(MaterializedViewTask.BUCKET_TIME_PERIOD_KEY, "1d");
    mvTask.put(MaterializedViewTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY, "10000");

    // Production-shaped sibling fields: each adds an entry to the top-level TreeMap so the
    // canonical knob nodes end up internal (not leaf) at iteration time. Without these the
    // tree is shallow enough that deletes hit only leaf nodes and never trigger the swap.
    TableConfig config = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_repro")
        .setIsMaterializedView(true)
        .setTimeColumnName("dayMillis")  // alphabetically later than maxNumRecordsPerSegment
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("365")
        .setBrokerTenant("DefaultTenant")
        .setServerTenant("DefaultTenant")
        .setTaskConfig(new TableTaskConfig(Map.of(
            MaterializedViewTask.TASK_TYPE, mvTask)))
        .build();

    String emitted = CanonicalDdlEmitter.emit(schema, config);

    assertTrue(emitted.contains("'bucketTimePeriod' = '1d'"),
        "bucketTimePeriod value must survive TreeMap delete rebalance; got:\n" + emitted);
    assertTrue(emitted.contains("'maxNumRecordsPerSegment' = '10000'"),
        "maxNumRecordsPerSegment must keep its real value '10000', not a sibling's; got:\n"
            + emitted);
    assertTrue(emitted.contains("'timeColumnName' = 'dayMillis'"),
        "timeColumnName must still be emitted normally; got:\n" + emitted);
    assertFalse(emitted.contains("'maxNumRecordsPerSegment' = 'dayMillis'"),
        "REGRESSION: TreeMap.Entry mutation after it.remove() bled timeColumnName's value "
            + "into the canonical knob; got:\n" + emitted);
  }

  // -------------------------------------------------------------------------------------------
  // Regression: trailing semicolon doubling in AS <query>
  // -------------------------------------------------------------------------------------------

  /// Legacy MV configs stored a `definedSQL` ending in `;` (operators copy-pasted the full
  /// SQL into the JSON table-config API, semicolon and all). The emitter unconditionally
  /// appends its own `;` so the canonical DDL ended in `;;\n`, which fails strict SQL parsing
  /// on replay. Strip any trailing `;` / whitespace before terminating with exactly one `;`.
  @Test
  public void trailingSemicolonInDefinedSqlNotDoubledOnEmit() {
    Schema schema = mvSchema();
    Map<String, String> mvTask = new LinkedHashMap<>();
    mvTask.put(MaterializedViewTask.DEFINED_SQL_KEY,
        "select ts, carrier from src group by ts, carrier;");
    mvTask.put(MaterializedViewTask.BUCKET_TIME_PERIOD_KEY, "1d");
    TableConfig config = mvConfig("mv", mvTask);

    String emitted = CanonicalDdlEmitter.emit(schema, config);

    assertFalse(emitted.contains(";;\n"),
        "trailing `;` in stored definedSQL must be normalised to exactly one; got:\n" + emitted);
    assertTrue(emitted.endsWith("AS select ts, carrier from src group by ts, carrier;\n"),
        "canonical DDL must end with a single `;\\n`; got:\n" + emitted);
  }

  /// Defensive variant: multiple trailing `;` and whitespace must all be stripped, then
  /// re-terminated with one `;`.
  @Test
  public void multipleTrailingSemicolonsAndWhitespaceNormalisedToOne() {
    Schema schema = mvSchema();
    Map<String, String> mvTask = new LinkedHashMap<>();
    mvTask.put(MaterializedViewTask.DEFINED_SQL_KEY,
        "select ts, carrier from src;  ; \n; ");
    mvTask.put(MaterializedViewTask.BUCKET_TIME_PERIOD_KEY, "1d");
    TableConfig config = mvConfig("mv", mvTask);

    String emitted = CanonicalDdlEmitter.emit(schema, config);

    assertTrue(emitted.endsWith("AS select ts, carrier from src;\n"),
        "got:\n" + emitted);
  }

  /// Round-trip determinism for the MV path: two `emit` calls on the same (schema, config)
  /// must produce byte-identical strings. PropertyExtractor uses a TreeMap so iteration order
  /// is stable, but the MV extractor adds a LinkedHashMap pass on top — this test locks the
  /// invariant in case anyone introduces a HashMap downstream.
  @Test
  public void deterministicOutputAcrossRepeatedCalls() {
    Schema schema = mvSchema();
    Map<String, String> mvTask = new LinkedHashMap<>();
    mvTask.put(MaterializedViewTask.DEFINED_SQL_KEY, "SELECT ts, carrier FROM src");
    mvTask.put(MaterializedViewTask.BUCKET_TIME_PERIOD_KEY, "1d");
    mvTask.put(MaterializedViewTask.BUFFER_TIME_PERIOD_KEY, "2h");
    mvTask.put("schedule", "0 0 0 * * ?");
    TableConfig config = mvConfig("mv", mvTask);

    String first = CanonicalDdlEmitter.emit(schema, config);
    String second = CanonicalDdlEmitter.emit(schema, config);
    assertEquals(first, second, "MV canonical emit must be deterministic");
  }

  // -------------------------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------------------------

  /// Minimal MV-shaped schema: one datetime column (used as the MV time column) and one
  /// dimension. Kept identical across every test so per-test deltas land squarely on the
  /// emitter's MV-specific branches.
  private static Schema mvSchema() {
    return new Schema.SchemaBuilder()
        .setSchemaName("mv")
        .addDateTime("ts", DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:DAYS")
        .addSingleValueDimension("carrier", DataType.STRING)
        .build();
  }

  /// Builds an OFFLINE TableConfig with the given MV task block plus the canonical
  /// `isMaterializedView` identity flag (PR #18564 — the single source of truth the emitter
  /// dispatches on). The `timeColumnName` is fixed to `ts` to match [#mvSchema]; tests that
  /// need a different time column should rebuild directly and remember to set the flag too.
  private static TableConfig mvConfig(String tableName, Map<String, String> mvTask) {
    Map<String, Map<String, String>> tasks = new LinkedHashMap<>();
    tasks.put(MaterializedViewTask.TASK_TYPE, mvTask);
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(tableName)
        .setIsMaterializedView(true)
        .setTimeColumnName("ts")
        .setTaskConfig(new TableTaskConfig(tasks))
        .build();
  }
}
