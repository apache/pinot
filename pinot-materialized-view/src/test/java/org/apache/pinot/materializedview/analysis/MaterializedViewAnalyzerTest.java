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
package org.apache.pinot.materializedview.analysis;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.pinot.materializedview.context.MaterializedViewTaskGeneratorContext;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants.MaterializedViewTask;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class MaterializedViewAnalyzerTest {

  private static final String SOURCE_TABLE = "orders";
  private static final String SOURCE_TABLE_OFFLINE = "orders_OFFLINE";
  private static final String TIME_COLUMN = "DaysSinceEpoch";
  /// Appended to every test SQL that is expected to reach validations beyond the LIMIT check.
  private static final String DEFAULT_LIMIT = " LIMIT 1000";

  private MaterializedViewTaskGeneratorContext _mockAccessor;
  private TableConfig _sourceTableConfig;
  private Schema _sourceSchema;

  @BeforeMethod
  public void setUp() {
    _mockAccessor = mock(MaterializedViewTaskGeneratorContext.class);

    _sourceTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(SOURCE_TABLE_OFFLINE)
        .setTimeColumnName(TIME_COLUMN)
        .build();

    _sourceSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addSingleValueDimension("status", FieldSpec.DataType.STRING)
        .addMetric("amount", FieldSpec.DataType.DOUBLE)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    stubTable(SOURCE_TABLE_OFFLINE, _sourceTableConfig, _sourceSchema);
  }

  /// Stubs `tableExists`, `getTableConfig`, and `getTableSchema` in lock-step so callers don't
  /// have to remember the three calls. Pass `null` for the config or schema when the test
  /// specifically wants the absence case (e.g. probing a non-existent variant).
  private void stubTable(String tableNameWithType, @Nullable TableConfig config, @Nullable Schema schema) {
    boolean exists = config != null;
    when(_mockAccessor.tableExists(tableNameWithType)).thenReturn(exists);
    if (exists) {
      when(_mockAccessor.getTableConfig(tableNameWithType)).thenReturn(config);
    } else {
      when(_mockAccessor.getTableConfig(tableNameWithType))
          .thenThrow(new IllegalStateException("Table config not found for: " + tableNameWithType));
    }
    if (schema != null) {
      when(_mockAccessor.getTableSchema(tableNameWithType)).thenReturn(schema);
    } else if (exists) {
      // Table exists but no schema: matches the "cluster-state inconsistency" branch.
      when(_mockAccessor.getTableSchema(tableNameWithType))
          .thenThrow(new IllegalStateException("Schema not found for table: " + tableNameWithType));
    }
  }

  // -----------------------------------------------------------------------
  //  Happy path
  // -----------------------------------------------------------------------

  @Test
  public void testValidSqlWithMatchingSchema() {
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt, sum(amount) AS total_amount "
        + "FROM orders GROUP BY DaysSinceEpoch, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addMetric("total_amount", FieldSpec.DataType.DOUBLE)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    TableConfig viewTableConfig = buildMaterializedViewTableConfig();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    MaterializedViewAnalyzer.AnalysisResult result =
        MaterializedViewAnalyzer.analyze(withLimit(sql), viewTableConfig, viewSchema, taskConfigs, _mockAccessor);

    assertNotNull(result);
    assertEquals(result.getSourceTableName(), SOURCE_TABLE);
    assertTrue(result.getSelectFields().contains("city"));
    assertTrue(result.getSelectFields().contains("cnt"));
    assertTrue(result.getSelectFields().contains("total_amount"));
    assertTrue(result.getSelectFields().contains(TIME_COLUMN));
    assertEquals(result.getSelectFields().size(), 4);

    // Verify partitionExprMaps
    assertNotNull(result.getPartitionExprMaps());
    assertEquals(result.getPartitionExprMaps().size(), 1);
    assertEquals(result.getPartitionExprMaps().get(TIME_COLUMN), TIME_COLUMN);
  }

  @Test
  public void testValidSqlBareColumnsOnly() {
    String sql = "SELECT DaysSinceEpoch, city, status FROM orders";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addSingleValueDimension("status", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    TableConfig viewTableConfig = buildMaterializedViewTableConfig();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    MaterializedViewAnalyzer.AnalysisResult result =
        MaterializedViewAnalyzer.analyze(withLimit(sql), viewTableConfig, viewSchema, taskConfigs, _mockAccessor);

    assertNotNull(result);
    assertEquals(result.getSelectFields().size(), 3);
    assertEquals(result.getPartitionExprMaps().get(TIME_COLUMN), TIME_COLUMN);
  }

  @Test
  public void testValidSqlWithTimeTransformFunction() {
    String sql = "SELECT DATETRUNC('DAY', DaysSinceEpoch) AS dayBucket, city, count(*) AS cnt "
        + "FROM orders GROUP BY DATETRUNC('DAY', DaysSinceEpoch), city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime("dayBucket", FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    // The MV renames the time column via DATETRUNC, so segmentsConfig.timeColumnName must
    // point to the SELECT alias 'dayBucket' — not the inherited base name.
    TableConfig viewTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_orders")
        .setIsMaterializedView(true)
        .setTimeColumnName("dayBucket")
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    MaterializedViewAnalyzer.AnalysisResult result =
        MaterializedViewAnalyzer.analyze(withLimit(sql), viewTableConfig, viewSchema, taskConfigs, _mockAccessor);

    assertNotNull(result);
    assertEquals(result.getPartitionExprMaps().size(), 1);
    assertEquals(result.getPartitionExprMaps().get("datetrunc('DAY', DaysSinceEpoch)"), "dayBucket");
  }

  @Test
  public void testTimeColumnMissingFromSelect() {
    String sql = "SELECT city, count(*) AS cnt FROM orders GROUP BY city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    expectError(sql, viewSchema, "is not produced by any SELECT expression");
  }

  @Test
  public void testTimeColumnMissingFromGroupBy() {
    // Calcite enforces that non-aggregated SELECT columns must appear in GROUP BY,
    // so this SQL fails at syntax validation with Calcite's own error message.
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders GROUP BY city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    expectError(sql, viewSchema, "functionally dependent");
  }

  // -----------------------------------------------------------------------
  //  Step 1: SQL syntax errors
  // -----------------------------------------------------------------------

  @Test
  public void testInvalidSqlSyntax() {
    String sql = "SELCT city FROM orders";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .build();

    expectError(sql, viewSchema, "Invalid SQL syntax");
  }

  @Test
  public void testNullSql() {
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .build();

    expectError(null, viewSchema, "definedSQL must be specified");
  }

  @Test
  public void testEmptySql() {
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .build();

    expectError("", viewSchema, "definedSQL must be specified");
  }

  // -----------------------------------------------------------------------
  //  Step 1b: LIMIT is optional; when present it must be strictly positive
  // -----------------------------------------------------------------------

  @Test
  public void testMissingLimitAllowed() {
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders GROUP BY DaysSinceEpoch, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();
    TableConfig viewTableConfig = buildMaterializedViewTableConfig();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    // No LIMIT is now allowed — truncation check is simply disabled.
    MaterializedViewAnalyzer.AnalysisResult result =
        MaterializedViewAnalyzer.analyze(sql, viewTableConfig, viewSchema, taskConfigs, _mockAccessor);
    assertNotNull(result);
  }

  @Test
  public void testExplicitLimitAllowed() {
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders "
        + "GROUP BY DaysSinceEpoch, city "
        + "LIMIT 10000";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();
    TableConfig viewTableConfig = buildMaterializedViewTableConfig();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    MaterializedViewAnalyzer.AnalysisResult result =
        MaterializedViewAnalyzer.analyze(sql, viewTableConfig, viewSchema, taskConfigs, _mockAccessor);
    assertNotNull(result);
  }

  @Test
  public void testZeroLimitRejected() {
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders "
        + "GROUP BY DaysSinceEpoch, city LIMIT 0";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);
    expectErrorRaw(sql, viewSchema, taskConfigs, "LIMIT must be strictly positive");
  }

  @Test
  public void testLargeLimitAccepted() {
    // MAX_MATERIALIZED_VIEW_QUERY_LIMIT = 100_000_000: large user-declared LIMITs up to the cap
    // are honored. The saturation gate still fires when a window's actual row count reaches
    // the declared LIMIT, so users opt into the truncation guarantee at their chosen LIMIT.
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders "
        + "GROUP BY DaysSinceEpoch, city LIMIT 99000000";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();
    TableConfig viewTableConfig = buildMaterializedViewTableConfig();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);
    MaterializedViewAnalyzer.AnalysisResult result =
        MaterializedViewAnalyzer.analyze(sql, viewTableConfig, viewSchema, taskConfigs, _mockAccessor);
    assertNotNull(result);
  }

  @Test
  public void testLimitAboveCapRejected() {
    // Per MAJOR fix: LIMITs above MAX_MATERIALIZED_VIEW_QUERY_LIMIT (100M) must be rejected at
    // create time so the executor cannot accumulate that many rows in memory before the
    // saturation gate fires.
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders "
        + "GROUP BY DaysSinceEpoch, city LIMIT 200000000";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);
    expectErrorRaw(sql, viewSchema, taskConfigs, "exceeds maximum 100000000");
  }

  @Test
  public void testOffsetRejected() {
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders "
        + "GROUP BY DaysSinceEpoch, city LIMIT 100 OFFSET 50";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);
    expectErrorRaw(sql, viewSchema, taskConfigs, "must not declare OFFSET");
  }

  @Test
  public void testTrailingLineCommentBlocksAutoLimitInjection() {
    // No LIMIT in definedSQL + trailing line comment would swallow the auto-injected LIMIT
    // at task-generation time. Analyzer simulates the append at create time and rejects.
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders "
        + "GROUP BY DaysSinceEpoch, city -- daily aggregation";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);
    expectErrorRaw(sql, viewSchema, taskConfigs, "swallow the auto-injected LIMIT");
  }

  @Test
  public void testTrailingUnterminatedBlockCommentRejected() {
    // An unterminated block comment fails Calcite's initial SQL syntax check (Step 1) —
    // the analyzer surfaces this at create time before the auto-inject probe even runs.
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders "
        + "GROUP BY DaysSinceEpoch, city /* trailing";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);
    expectErrorRaw(sql, viewSchema, taskConfigs, "Invalid SQL syntax");
  }

  @Test
  public void testTrailingTerminatedBlockCommentAllowed() {
    // A terminated block comment is stripped by Calcite before parsing — the auto-inject
    // probe sees clean SQL ending in the GROUP BY clause and successfully verifies the LIMIT.
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders "
        + "GROUP BY DaysSinceEpoch, city /* trailing comment */";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();
    TableConfig viewTableConfig = buildMaterializedViewTableConfig();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);
    MaterializedViewAnalyzer.AnalysisResult result =
        MaterializedViewAnalyzer.analyze(sql, viewTableConfig, viewSchema, taskConfigs, _mockAccessor);
    assertNotNull(result);
  }

  @Test
  public void testNegativeBufferRejected() {
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders "
        + "GROUP BY DaysSinceEpoch, city LIMIT 100";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);
    taskConfigs.put(MaterializedViewTask.BUFFER_TIME_PERIOD_KEY, "-1d");
    expectErrorRaw(sql, viewSchema, taskConfigs, "bufferTimePeriod");
  }

  @Test
  public void testTryExtractDeclaredLimitPresent() {
    String sql = "SELECT DaysSinceEpoch, city FROM orders LIMIT 2500";
    assertEquals(MaterializedViewAnalyzer.tryExtractDeclaredLimit(sql), Optional.of(2500));
  }

  @Test
  public void testTryExtractDeclaredLimitAbsent() {
    String sql = "SELECT DaysSinceEpoch, city FROM orders";
    assertEquals(MaterializedViewAnalyzer.tryExtractDeclaredLimit(sql), Optional.empty());
  }

  // -----------------------------------------------------------------------
  //  Step 2: Source table validation
  // -----------------------------------------------------------------------

  @Test
  public void testSourceTableNotFound() {
    String sql = "SELECT DaysSinceEpoch, city FROM nonexistent_table GROUP BY DaysSinceEpoch, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    stubTable("nonexistent_table_OFFLINE", null, null);
    stubTable("nonexistent_table_REALTIME", null, null);

    expectError(sql, viewSchema, "does not exist");
  }

  @Test
  public void testSourceTableNoTimeColumn() {
    TableConfig noTimeConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("no_time_table_OFFLINE")
        .build();
    stubTable("no_time_table_OFFLINE", noTimeConfig, null);

    String sql = "SELECT DaysSinceEpoch, city FROM no_time_table GROUP BY DaysSinceEpoch, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    expectError(sql, viewSchema, "has no time column configured");
  }

  @Test
  public void testSourceTableNoDateTimeFieldSpec() {
    TableConfig withTimeConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("missing_spec_table_OFFLINE")
        .setTimeColumnName("missingCol")
        .build();
    Schema schemaWithoutSpec = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .build();
    stubTable("missing_spec_table_OFFLINE", withTimeConfig, schemaWithoutSpec);

    String sql = "SELECT DaysSinceEpoch, city FROM missing_spec_table GROUP BY DaysSinceEpoch, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    expectError(sql, viewSchema, "No DateTimeFieldSpec found");
  }

  @Test
  public void testRejectsJoinInFromClause() {
    // CalciteSqlParser routes JOIN into DataSource.join rather than DataSource.tableName.
    // Without the explicit JOIN guard the test would still fail, but with the misleading
    // "Could not extract source table name from SQL" message. The new guard surfaces the
    // actual unsupported-construct cause directly.
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders "
        + "JOIN products ON orders.product_id = products.id "
        + "GROUP BY DaysSinceEpoch, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    expectError(sql, viewSchema, "JOIN queries are not supported");
  }

  // -----------------------------------------------------------------------
  //  Source-table type eligibility (Step 2): MV's coverage model assumes the base table is
  //  append-only with monotonically advancing time. Tables whose contents can be replaced or
  //  rewritten silently — upsert, dedup, dimension, REFRESH-push — must be rejected at create
  //  time so a known-broken MV cannot land in cluster metadata.
  // -----------------------------------------------------------------------

  @Test
  public void testRejectsUpsertSourceTable() {
    // Upsert: in-place row replacement breaks the assumption that a VALID time partition is
    // immutable; a late update to a covered interval would silently diverge from the MV.
    String mutableTable = "orders_upsert";
    TableConfig upsertCfg = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(mutableTable)
        .setTimeColumnName(TIME_COLUMN)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
        .build();
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();
    stubTable(mutableTable + "_OFFLINE", null, null);
    stubTable(mutableTable + "_REALTIME", upsertCfg, schema);

    String sql = "SELECT DaysSinceEpoch, city FROM " + mutableTable + " GROUP BY DaysSinceEpoch, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();
    expectError(sql, viewSchema, "upsert enabled");
  }

  @Test
  public void testRejectsDedupSourceTable() {
    // Dedup: the de-duplicated view is server-managed and not stable across reloads/TTLs;
    // MV would aggregate over a snapshot that the runtime can later disagree with.
    String dedupTable = "orders_dedup";
    TableConfig dedupCfg = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(dedupTable)
        .setTimeColumnName(TIME_COLUMN)
        .setDedupConfig(new DedupConfig(true, null))
        .build();
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();
    stubTable(dedupTable + "_OFFLINE", null, null);
    stubTable(dedupTable + "_REALTIME", dedupCfg, schema);

    String sql = "SELECT DaysSinceEpoch, city FROM " + dedupTable + " GROUP BY DaysSinceEpoch, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();
    expectError(sql, viewSchema, "dedup enabled");
  }

  @Test
  public void testRejectsDimensionSourceTable() {
    // Dimension table: fully replaced on every refresh and has no monotonic time concept;
    // the MV's time-partitioned coverage model is meaningless here.
    String dimTable = "dim_lookup";
    TableConfig dimCfg = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(dimTable)
        .setTimeColumnName(TIME_COLUMN)
        .setIsDimTable(true)
        .build();
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();
    stubTable(dimTable + "_OFFLINE", dimCfg, schema);

    String sql = "SELECT DaysSinceEpoch, city FROM " + dimTable + " GROUP BY DaysSinceEpoch, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();
    expectError(sql, viewSchema, "dimension table");
  }

  @Test
  public void testRejectsRefreshPushTable() {
    // REFRESH push: each push wholesale replaces base segments, so any MV partition already
    // marked VALID becomes immediately suspect after the next push.
    String refreshTable = "orders_refresh";
    IngestionConfig ingestionCfg = new IngestionConfig();
    ingestionCfg.setBatchIngestionConfig(new BatchIngestionConfig(null, "REFRESH", "DAILY"));
    TableConfig refreshCfg = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(refreshTable)
        .setTimeColumnName(TIME_COLUMN)
        .setIngestionConfig(ingestionCfg)
        .build();
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();
    stubTable(refreshTable + "_OFFLINE", refreshCfg, schema);

    String sql = "SELECT DaysSinceEpoch, city FROM " + refreshTable + " GROUP BY DaysSinceEpoch, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();
    expectError(sql, viewSchema, "REFRESH push type");
  }

  @Test
  public void testRejectsRefreshPushTableViaLegacyField() {
    // Legacy: REFRESH was set via the deprecated SegmentsValidationAndRetentionConfig field.
    // resolveSegmentPushType must fall through to it so older configs cannot bypass the guard.
    String refreshTable = "orders_refresh_legacy";
    TableConfig refreshCfg = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(refreshTable)
        .setTimeColumnName(TIME_COLUMN)
        .setSegmentPushType("REFRESH")
        .build();
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();
    stubTable(refreshTable + "_OFFLINE", refreshCfg, schema);

    String sql = "SELECT DaysSinceEpoch, city FROM " + refreshTable + " GROUP BY DaysSinceEpoch, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();
    expectError(sql, viewSchema, "REFRESH push type");
  }

  @Test
  public void testSourceColumnNotExist() {
    String sql = "SELECT DaysSinceEpoch, city, sum(nonexistent_col) AS total FROM orders "
        + "GROUP BY DaysSinceEpoch, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("total", FieldSpec.DataType.DOUBLE)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    expectError(sql, viewSchema, "does not exist in source table");
  }

  @Test
  public void testWhereClauseColumnNotExist() {
    // A typo in a WHERE predicate (here `nonexistent_col` instead of an actual source column)
    // would previously slip past create-time validation because validateSourceColumns only
    // walked SELECT + GROUP BY, then surface as a broker error at task-execution time. The
    // analyzer now walks the filter tree so the operator sees the diagnostic at DDL time.
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders "
        + "WHERE nonexistent_col = 42 "
        + "GROUP BY DaysSinceEpoch, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    expectError(sql, viewSchema, "does not exist in source table");
  }

  @Test
  public void testHavingClauseColumnNotExist() {
    // Same hazard as the WHERE case but for HAVING — exercised separately because the
    // analyzer reads HAVING from a different PinotQuery accessor.
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders "
        + "GROUP BY DaysSinceEpoch, city "
        + "HAVING sum(nonexistent_col) > 0";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    expectError(sql, viewSchema, "does not exist in source table");
  }

  // -----------------------------------------------------------------------
  //  Step 3: MV schema column validation
  // -----------------------------------------------------------------------

  @Test
  public void testMaterializedViewSchemaColumnNotCoveredBySelect() {
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders GROUP BY DaysSinceEpoch, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addMetric("extra_column", FieldSpec.DataType.DOUBLE)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    expectError(sql, viewSchema, "is not produced by any SELECT expression");
  }

  @Test
  public void testSelectFieldNotInMaterializedViewSchema() {
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt, sum(amount) AS total "
        + "FROM orders GROUP BY DaysSinceEpoch, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    expectError(sql, viewSchema, "does not match any column in the MV table schema");
  }

  @Test
  public void testRejectsAliasCaseMismatch() {
    /// Case-sensitive cluster regression: an MV defined with `SUM(x) AS sum_x` against a schema
    /// that has the column declared as `Sum_X` must be rejected at analyzer time so the broker
    /// FULL_REWRITE re-canonicalization never has to deal with the mismatch at query time.  The
    /// existing `selectFields.contains` / `schemaColumns.contains` checks are case-sensitive,
    /// so a case-different SELECT alias fails Check 2 ("SELECT field does not match any column").
    String sql = "SELECT DaysSinceEpoch, city, sum(amount) AS sum_amount "
        + "FROM orders GROUP BY DaysSinceEpoch, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("Sum_Amount", FieldSpec.DataType.DOUBLE)  // case differs from SELECT alias
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    /// The existing `schemaColumns.contains(selectField)` check in Step 3 is case-sensitive
    /// because both sides are HashSet<String>; the analyzer fails Check 1 ("MV schema column not
    /// produced by any SELECT expression") before reaching Check 2, but either failure pin the
    /// case-sensitivity contract.
    expectError(sql, viewSchema, "is not produced by any SELECT expression");
  }

  @Test
  public void testAggregateWithoutAlias() {
    String sql = "SELECT DaysSinceEpoch, city, count(*) FROM orders GROUP BY DaysSinceEpoch, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    // Helper now lives in RequestUtils and emits a slightly broader message that covers
    // both accepted shapes (bare column or AS <alias>).
    expectError(sql, viewSchema, "use AS <alias>");
  }

  @Test
  public void testRejectsSelectStar() {
    // `SELECT *` would otherwise reach the schema-coverage check with a single SELECT field
    // literally named "*", producing the misleading "MV schema column 'X' is not produced
    // by any SELECT expression" error. The explicit guard names the unsupported construct
    // so the operator immediately knows to enumerate columns with aliases.
    String sql = "SELECT * FROM orders";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addSingleValueDimension("status", FieldSpec.DataType.STRING)
        .addMetric("amount", FieldSpec.DataType.DOUBLE)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    expectError(sql, viewSchema, "does not support `SELECT *`");
  }

  // -----------------------------------------------------------------------
  //  Step 4: Task config parameter validation
  // -----------------------------------------------------------------------

  @Test
  public void testAnalyzeRequiresIsMaterializedViewFlag() {
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders GROUP BY DaysSinceEpoch, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    TableConfig viewTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_orders")
        .setTimeColumnName(TIME_COLUMN)
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    try {
      MaterializedViewAnalyzer.analyze(withLimit(sql), viewTableConfig, viewSchema, taskConfigs, _mockAccessor);
      fail("Expected IllegalStateException when isMaterializedView is false");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("isMaterializedView=true"),
          "Unexpected message: " + e.getMessage());
    }
  }

  @Test
  public void testNonOfflineTableType() {
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders GROUP BY DaysSinceEpoch, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    TableConfig realtimeConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("mv_orders")
        .setIsMaterializedView(true)
        .setTimeColumnName(TIME_COLUMN)
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    try {
      MaterializedViewAnalyzer.analyze(withLimit(sql), realtimeConfig, viewSchema, taskConfigs, _mockAccessor);
      fail("Expected IllegalStateException for non-OFFLINE table");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("only supports OFFLINE"), "Unexpected message: " + e.getMessage());
    }
  }

  @Test
  public void testInvalidBucketTimePeriod() {
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders GROUP BY DaysSinceEpoch, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    Map<String, String> taskConfigs = buildTaskConfigs(sql);
    taskConfigs.put(MaterializedViewTask.BUCKET_TIME_PERIOD_KEY, "not_a_period");

    expectError(sql, viewSchema, taskConfigs, "Invalid bucketTimePeriod");
  }

  @Test
  public void testInvalidMaxNumRecordsPerSegment() {
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders GROUP BY DaysSinceEpoch, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    Map<String, String> taskConfigs = buildTaskConfigs(sql);
    taskConfigs.put(MaterializedViewTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY, "-5");

    expectError(sql, viewSchema, taskConfigs, "maxNumRecordsPerSegment must be positive");
  }

  @Test
  public void testNonNumericMaxNumRecordsPerSegment() {
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders GROUP BY DaysSinceEpoch, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    Map<String, String> taskConfigs = buildTaskConfigs(sql);
    taskConfigs.put(MaterializedViewTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY, "abc");

    expectError(sql, viewSchema, taskConfigs, "Invalid maxNumRecordsPerSegment");
  }

  @Test
  public void testNonNumericStalenessThresholdMs() {
    // A typo like '60s' would silently fall back to the default at the builder/scheduler
    // level, masking the operator's intent. Analyzer must reject loudly at CREATE time.
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders GROUP BY DaysSinceEpoch, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);
    taskConfigs.put(MaterializedViewTask.STALENESS_THRESHOLD_MS_KEY, "60s");
    expectError(sql, viewSchema, taskConfigs, "Invalid stalenessThresholdMs");
  }

  @Test
  public void testNegativeStalenessThresholdMsRejected() {
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders GROUP BY DaysSinceEpoch, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);
    taskConfigs.put(MaterializedViewTask.STALENESS_THRESHOLD_MS_KEY, "-1");
    expectError(sql, viewSchema, taskConfigs, "stalenessThresholdMs must be non-negative");
  }

  @Test
  public void testZeroStalenessThresholdMsAccepted() {
    // 0 is the documented "no SLO" sentinel — the rewrite engine treats <= 0 as disabled.
    // An explicit '0' must round-trip without rejection.
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders GROUP BY DaysSinceEpoch, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();
    TableConfig viewTableConfig = buildMaterializedViewTableConfig();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);
    taskConfigs.put(MaterializedViewTask.STALENESS_THRESHOLD_MS_KEY, "0");
    MaterializedViewAnalyzer.analyze(withLimit(sql), viewTableConfig, viewSchema, taskConfigs, _mockAccessor);
  }

  // -----------------------------------------------------------------------
  //  Complex SQL
  // -----------------------------------------------------------------------

  @Test
  public void testComplexSqlWithMultipleAggregations() {
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt, sum(amount) AS total, "
        + "min(amount) AS min_amt, max(amount) AS max_amt FROM orders GROUP BY DaysSinceEpoch, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addMetric("total", FieldSpec.DataType.DOUBLE)
        .addMetric("min_amt", FieldSpec.DataType.DOUBLE)
        .addMetric("max_amt", FieldSpec.DataType.DOUBLE)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    TableConfig viewTableConfig = buildMaterializedViewTableConfig();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    MaterializedViewAnalyzer.AnalysisResult result =
        MaterializedViewAnalyzer.analyze(withLimit(sql), viewTableConfig, viewSchema, taskConfigs, _mockAccessor);

    assertNotNull(result);
    assertEquals(result.getSelectFields().size(), 6);
  }

  @Test
  public void testRejectsRealtimeSourceTable() {
    // Realtime source tables are rejected until the controller-side notify path supports realtime
    // segment commits (LLC). The fallback OFFLINE-then-REALTIME lookup in resolveSourceTableWithType
    // will resolve a realtime-only base; the analyzer must catch that here so a misconfigured MV
    // never reaches cluster metadata.
    String realtimeTable = "rt_orders_REALTIME";
    TableConfig rtConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(realtimeTable)
        .setTimeColumnName(TIME_COLUMN)
        .build();
    Schema rtSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    stubTable("rt_orders_OFFLINE", null, null);
    stubTable(realtimeTable, rtConfig, rtSchema);

    String sql = "SELECT DaysSinceEpoch, city FROM rt_orders";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    TableConfig viewTableConfig = buildMaterializedViewTableConfig();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    try {
      MaterializedViewAnalyzer.analyze(withLimit(sql), viewTableConfig, viewSchema, taskConfigs, _mockAccessor);
      fail("Expected IllegalStateException for REALTIME source table");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("REALTIME"),
          "Unexpected message: " + e.getMessage());
    }
  }

  @Test
  public void testRejectsHybridSourceTable() {
    // When both `_OFFLINE` and `_REALTIME` variants exist, the raw source name is ambiguous:
    // the broker would silently hybrid-route the persisted definedSQL at query time while
    // STALE-marking only covered the OFFLINE half (LLC realtime commits bypass the MV
    // consistency manager).  The resolver MUST fail fast so a misconfigured MV never reaches
    // cluster metadata — the OFFLINE-first preference that used to silently let hybrid bases
    // through is exactly the silent-drift trap this guard closes.
    String hybridTable = "hybrid_orders";
    TableConfig offlineCfg = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(hybridTable + "_OFFLINE")
        .setTimeColumnName(TIME_COLUMN)
        .build();
    TableConfig realtimeCfg = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(hybridTable + "_REALTIME")
        .setTimeColumnName(TIME_COLUMN)
        .build();
    Schema hybridSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();
    stubTable(hybridTable + "_OFFLINE", offlineCfg, hybridSchema);
    stubTable(hybridTable + "_REALTIME", realtimeCfg, hybridSchema);

    String sql = "SELECT DaysSinceEpoch, city FROM " + hybridTable;
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();
    TableConfig viewTableConfig = buildMaterializedViewTableConfig();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    try {
      MaterializedViewAnalyzer.analyze(withLimit(sql), viewTableConfig, viewSchema, taskConfigs, _mockAccessor);
      fail("Expected IllegalStateException for hybrid source table");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("hybrid"),
          "Expected 'hybrid' in error message, got: " + e.getMessage());
      assertTrue(e.getMessage().contains(hybridTable),
          "Expected source table name in error message, got: " + e.getMessage());
    }
  }

  @Test
  public void testAcceptsOfflineOnlySource() {
    // Regression guard for the hybrid-rejection change: when only the OFFLINE variant exists,
    // the resolver must still succeed.  The default `setUp()` already stubs SOURCE_TABLE_OFFLINE
    // only; explicitly stub the REALTIME variant as absent to make the OFFLINE-only intent
    // unambiguous against future mock setup churn.
    stubTable(SOURCE_TABLE + "_REALTIME", null, null);

    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders GROUP BY DaysSinceEpoch, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();
    TableConfig viewTableConfig = buildMaterializedViewTableConfig();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    MaterializedViewAnalyzer.AnalysisResult result =
        MaterializedViewAnalyzer.analyze(withLimit(sql), viewTableConfig, viewSchema, taskConfigs, _mockAccessor);
    assertNotNull(result);
    assertEquals(result.getSourceTableName(), SOURCE_TABLE);
  }

  // -----------------------------------------------------------------------
  //  Step 6: MV time-column alignment (segmentsConfig.timeColumnName)
  // -----------------------------------------------------------------------

  @Test
  public void testRejectsWhenMaterializedViewTimeColumnMissing() {
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders GROUP BY DaysSinceEpoch, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    TableConfig viewTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_orders")
        .setIsMaterializedView(true)
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    try {
      MaterializedViewAnalyzer.analyze(withLimit(sql), viewTableConfig, viewSchema, taskConfigs, _mockAccessor);
      fail("Expected IllegalStateException for unset MV timeColumnName");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("segmentsConfig.timeColumnName must be set"),
          "Unexpected message: " + e.getMessage());
    }
  }

  @Test
  public void testRejectsWhenMaterializedViewTimeColumnNotInSchema() {
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders GROUP BY DaysSinceEpoch, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    // timeColumnName points to a column that doesn't exist in the MV schema at all.
    TableConfig viewTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_orders")
        .setIsMaterializedView(true)
        .setTimeColumnName("nonexistent_time_col")
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    try {
      MaterializedViewAnalyzer.analyze(withLimit(sql), viewTableConfig, viewSchema, taskConfigs, _mockAccessor);
      fail("Expected IllegalStateException for MV timeColumnName missing from schema");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("MV time column 'nonexistent_time_col' does not exist in MV schema"),
          "Unexpected message: " + e.getMessage());
    }
  }

  @Test
  public void testRejectsWhenMaterializedViewTimeColumnIsNotDateTime() {
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders GROUP BY DaysSinceEpoch, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    // timeColumnName points to a plain dimension, not a registered dateTime column.
    TableConfig viewTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_orders")
        .setIsMaterializedView(true)
        .setTimeColumnName("city")
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    try {
      MaterializedViewAnalyzer.analyze(withLimit(sql), viewTableConfig, viewSchema, taskConfigs, _mockAccessor);
      fail("Expected IllegalStateException for MV timeColumnName not being a dateTime column");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("is not a dateTime field in the MV schema"),
          "Unexpected message: " + e.getMessage());
    }
  }

  @Test
  public void testRejectsWhenMaterializedViewTimeColumnNotProducedBySelect() {
    // Simulates the real-world misconfig: base table time column is DaysSinceEpoch; the
    // definedSql transforms it via date_trunc into a coarser 'day' column; but the MV
    // TableConfig inherited timeColumnName=DaysSinceEpoch from the base table without
    // updating it. The MV will not physically contain DaysSinceEpoch.
    String sql = "SELECT date_trunc('DAY', DaysSinceEpoch) AS day, city, count(*) AS cnt "
        + "FROM orders GROUP BY day, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime("day", FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    TableConfig viewTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_orders")
        .setIsMaterializedView(true)
        .setTimeColumnName(TIME_COLUMN)
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    try {
      MaterializedViewAnalyzer.analyze(withLimit(sql), viewTableConfig, viewSchema, taskConfigs, _mockAccessor);
      fail("Expected IllegalStateException for MV timeColumnName not produced by SELECT");
    } catch (IllegalStateException e) {
      // The column is absent from the MV schema entirely, so invariant (b) fires first
      // with a message that still points the user to the root cause.
      assertTrue(e.getMessage().contains("MV time column '" + TIME_COLUMN + "' does not exist in MV schema"),
          "Unexpected message: " + e.getMessage());
    }
  }

  @Test
  public void testAcceptsWhenMaterializedViewTimeColumnIsSelectAlias() {
    // Happy path: MV renames the time column via DATETRUNC, segmentsConfig.timeColumnName
    // points to the SELECT alias.  DATETRUNC unit 'DAY' matches bucketTimePeriod '1d'.
    String sql = "SELECT DATETRUNC('DAY', DaysSinceEpoch) AS day, city, count(*) AS cnt "
        + "FROM orders GROUP BY day, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime("day", FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    TableConfig viewTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_orders")
        .setIsMaterializedView(true)
        .setTimeColumnName("day")
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    MaterializedViewAnalyzer.AnalysisResult result =
        MaterializedViewAnalyzer.analyze(withLimit(sql), viewTableConfig, viewSchema, taskConfigs, _mockAccessor);

    assertNotNull(result);
    assertEquals(result.getPartitionExprMaps().size(), 1);
    assertTrue(result.getPartitionExprMaps().containsValue("day"),
        "Expected partitionExprMaps to map some base-table expression -> 'day', got: "
            + result.getPartitionExprMaps());
  }

  // -----------------------------------------------------------------------
  //  Step 7: MV time column TIMESTAMP-only contract (TimeExprValidator)
  //
  //  Per-rule behavior of the validator is covered in TimeExprValidatorTest; here we
  //  exercise only the end-to-end wiring through analyze(): unsupported function paths,
  //  nested function paths, and the data-type guard.  Format/granularity inference is
  //  no longer a thing — both base and MV time columns must be TIMESTAMP.
  // -----------------------------------------------------------------------

  @Test
  public void testStep7RejectsNonTimestampBaseColumn() {
    // setUp() now declares the base column as TIMESTAMP, so override it back to LONG/EPOCH-days
    // to confirm Step 7 rejects non-TIMESTAMP base columns.
    _sourceSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addSingleValueDimension("status", FieldSpec.DataType.STRING)
        .addMetric("amount", FieldSpec.DataType.DOUBLE)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();
    stubTable(SOURCE_TABLE_OFFLINE, _sourceTableConfig, _sourceSchema);

    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt "
        + "FROM orders GROUP BY DaysSinceEpoch, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    expectError(sql, viewSchema, "TIMESTAMP");
  }

  @Test
  public void testStep7RejectsNonTimestampMaterializedViewColumn() {
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt "
        + "FROM orders GROUP BY DaysSinceEpoch, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();

    expectError(sql, viewSchema, "TIMESTAMP");
  }

  @Test
  public void testStep7DatetruncHappyOnTimestampBase() {
    String sql = "SELECT DATETRUNC('DAY', DaysSinceEpoch) AS day, city, count(*) AS cnt "
        + "FROM orders GROUP BY day, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime("day", FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    TableConfig viewTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_orders")
        .setIsMaterializedView(true)
        .setTimeColumnName("day")
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    MaterializedViewAnalyzer.AnalysisResult result =
        MaterializedViewAnalyzer.analyze(withLimit(sql), viewTableConfig, viewSchema, taskConfigs, _mockAccessor);
    assertNotNull(result);
    assertTrue(result.getPartitionExprMaps().containsValue("day"));
  }

  @Test
  public void testStep7DatetruncUnitMismatchesBucketRejected() {
    String sql = "SELECT DATETRUNC('HOUR', DaysSinceEpoch) AS hr, city, count(*) AS cnt "
        + "FROM orders GROUP BY hr, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime("hr", FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    TableConfig viewTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_orders")
        .setIsMaterializedView(true)
        .setTimeColumnName("hr")
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    expectErrorRaw(withLimit(sql), viewSchema, viewTableConfig, taskConfigs,
        "does not match the declared bucketTimePeriod");
  }

  @Test
  public void testStep7UnsupportedFunctionRejected() {
    String sql = "SELECT fromEpochDays(DaysSinceEpoch) AS ts_ms, city, count(*) AS cnt "
        + "FROM orders GROUP BY ts_ms, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime("ts_ms", FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    TableConfig viewTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_orders")
        .setIsMaterializedView(true)
        .setTimeColumnName("ts_ms")
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    expectErrorRaw(withLimit(sql), viewSchema, viewTableConfig, taskConfigs, "unsupported function");
  }

  @Test
  public void testStep7NestedFunctionRejected() {
    // Nested DATETRUNC inside another DATETRUNC: not an identity, not a top-level supported
    // function — must be rejected with a TIMESTAMP-only style message.
    String sql = "SELECT DATETRUNC('DAY', DATETRUNC('HOUR', DaysSinceEpoch)) AS day, city, count(*) AS cnt "
        + "FROM orders GROUP BY day, city";
    Schema viewSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime("day", FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    TableConfig viewTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_orders")
        .setIsMaterializedView(true)
        .setTimeColumnName("day")
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    // Wording comes from MaterializedViewTimeExpressionParser, which the validator delegates
    // to for shape recognition; matching on the substring
    // "second argument" keeps this test stable across parser-message wording tweaks while
    // still asserting the rejection lands on the right argument.
    expectErrorRaw(withLimit(sql), viewSchema, viewTableConfig, taskConfigs,
        "second argument");
  }


  // -----------------------------------------------------------------------
  //  Helpers
  // -----------------------------------------------------------------------

  private TableConfig buildMaterializedViewTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_orders")
        .setIsMaterializedView(true)
        .setTimeColumnName(TIME_COLUMN)
        .build();
  }

  private Map<String, String> buildTaskConfigs(String sql) {
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put(MaterializedViewTask.DEFINED_SQL_KEY, sql);
    taskConfigs.put(MaterializedViewTask.BUCKET_TIME_PERIOD_KEY, "1d");
    return taskConfigs;
  }

  private void expectError(String sql, Schema viewSchema, String expectedMessageFragment) {
    expectError(sql, viewSchema, buildTaskConfigs(sql), expectedMessageFragment);
  }

  private void expectError(String sql, Schema viewSchema, Map<String, String> taskConfigs,
      String expectedMessageFragment) {
    expectErrorRaw(withLimit(sql), viewSchema, taskConfigs, expectedMessageFragment);
  }

  /// Same as [Schema, Map, String)][#expectError(String,] but does not append a default
  /// LIMIT.  Used by tests that intentionally exercise the LIMIT-validation path.
  private void expectErrorRaw(String sql, Schema viewSchema, Map<String, String> taskConfigs,
      String expectedMessageFragment) {
    expectErrorRaw(sql, viewSchema, buildMaterializedViewTableConfig(), taskConfigs, expectedMessageFragment);
  }

  /// Variant that lets the caller supply a custom MV [TableConfig] (e.g. with a
  /// SELECT-alias time column name). Step-7 tests need this because the time column is
  /// usually an alias of the base time column.
  private void expectErrorRaw(String sql, Schema viewSchema, TableConfig viewTableConfig,
      Map<String, String> taskConfigs, String expectedMessageFragment) {
    try {
      MaterializedViewAnalyzer.analyze(sql, viewTableConfig, viewSchema, taskConfigs, _mockAccessor);
      fail("Expected IllegalStateException containing: " + expectedMessageFragment);
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains(expectedMessageFragment),
          "Expected message containing '" + expectedMessageFragment + "', got: " + e.getMessage());
    }
  }

  /// Returns `sql` as-is if it already ends with a LIMIT clause, otherwise appends one.
  private static String withLimit(String sql) {
    if (sql == null || sql.isEmpty()) {
      return sql;
    }
    return sql.toUpperCase().contains(" LIMIT ") ? sql : sql + DEFAULT_LIMIT;
  }
}
