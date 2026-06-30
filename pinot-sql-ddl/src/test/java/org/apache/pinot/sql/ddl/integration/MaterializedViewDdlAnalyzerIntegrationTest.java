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
package org.apache.pinot.sql.ddl.integration;

import java.util.Map;
import org.apache.pinot.materializedview.analysis.MaterializedViewAnalyzer;
import org.apache.pinot.materializedview.context.MaterializedViewTaskGeneratorContext;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants.MaterializedViewTask;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.sql.ddl.compile.CompiledCreateMaterializedView;
import org.apache.pinot.sql.ddl.compile.DdlCompiler;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;


/// End-to-end test that exercises the full `CREATE MATERIALIZED VIEW` DDL pipeline against the
/// production [MaterializedViewAnalyzer]. The pipeline under test is:
///
/// `SQL string → DdlCompiler.compile(...) → CompiledCreateMaterializedView → extract
/// (definedSql, TableConfig, Schema, taskConfigs) → MaterializedViewAnalyzer.analyze(...)`.
///
/// This closes a coverage gap that the unit-test surfaces deliberately leave open: the
/// [PinotDdlRestletResourceMaterializedViewUnitTest] suite stubs out the entire validation
/// chain (TableConfigValidationUtils, PinotTableRestletResource.tableTasksValidation,
/// TableConfigTunerUtils) because the controller's test classpath does not transitively
/// include `pinot-minion-builtin-tasks` and therefore cannot resolve
/// `MaterializedViewTaskGenerator.validateTaskConfigs`. The trade-off is that those tests
/// pin the controller's own dispatch / authorization / Q2=B behaviour without touching the
/// analyzer; this test fills the gap from the opposite side.
///
/// The MaterializedViewAnalyzer itself has dense unit coverage in
/// `MaterializedViewAnalyzerTest` (over 50 cases, every error branch), so this integration
/// test deliberately stays minimal: one happy path proving the compiled artifacts reach the
/// analyzer in a shape the analyzer accepts, and one analyzer-reject path proving that a
/// genuine validation error (an MV column declared in the schema but absent from the SELECT
/// projection) surfaces up the stack with the analyzer's own message — i.e. the DDL layer
/// neither swallows nor rewraps the validation result.
///
/// We invoke the analyzer directly rather than going through
/// `MaterializedViewTaskGenerator.validateTaskConfigs` (which would route through SPI plugin
/// loading) because the wrapper is a thin pass-through whose own contract is pinned by
/// `MaterializedViewTaskGeneratorTest`. The boundary this test pins is the data shape
/// flowing OUT of the DDL compiler INTO the analyzer.
public class MaterializedViewDdlAnalyzerIntegrationTest {

  private static final String SOURCE_TABLE_RAW = "orders";
  private static final String SOURCE_TABLE_OFFLINE = "orders_OFFLINE";

  private MaterializedViewTaskGeneratorContext _stubContext;

  @BeforeMethod
  public void setUp() {
    _stubContext = mock(MaterializedViewTaskGeneratorContext.class);

    TableConfig sourceConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(SOURCE_TABLE_OFFLINE)
        .setTimeColumnName("ts")
        .build();
    Schema sourceSchema = new Schema.SchemaBuilder()
        .setSchemaName(SOURCE_TABLE_RAW)
        .addDateTime("ts", FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("amount", FieldSpec.DataType.DOUBLE)
        .build();

    when(_stubContext.tableExists(SOURCE_TABLE_OFFLINE)).thenReturn(true);
    when(_stubContext.getTableConfig(SOURCE_TABLE_OFFLINE)).thenReturn(sourceConfig);
    when(_stubContext.getTableSchema(SOURCE_TABLE_OFFLINE)).thenReturn(sourceSchema);
  }

  /// Happy path: a DDL whose SELECT projection covers every column declared in the MV column
  /// list (and whose time column / bucket period match the analyzer's contract) round-trips
  /// through compile → analyze without error. Assertions focus on the analyzer's own outputs
  /// (the resolved source table and the SELECT-field list) rather than re-asserting fields
  /// that the DDL compiler tests already cover, so this test only fails if the DDL-to-
  /// analyzer hand-off itself regresses.
  @Test
  public void compiledMaterializedViewDdlPassesRealAnalyzer() {
    CompiledCreateMaterializedView compiled = (CompiledCreateMaterializedView) DdlCompiler.compile(
        "CREATE MATERIALIZED VIEW mv_orders ("
            + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:MILLISECONDS',"
            + "  city STRING DIMENSION,"
            + "  cnt LONG METRIC"
            + ") REFRESH EVERY '1d' "
            + "PROPERTIES ("
            + "  'timeColumnName' = 'ts',"
            + "  'bucketTimePeriod' = '1d',"
            + "  'replication' = '1'"
            + ") "
            + "AS SELECT ts, city, count(*) AS cnt FROM orders GROUP BY ts, city LIMIT 100000");

    TableConfig viewConfig = compiled.getTableConfig();
    Schema viewSchema = compiled.getSchema();
    Map<String, String> taskConfigs = viewConfig.getTaskConfig()
        .getConfigsForTaskType(MaterializedViewTask.TASK_TYPE);
    String definedSql = taskConfigs.get(MaterializedViewTask.DEFINED_SQL_KEY);
    assertNotNull(definedSql,
        "DDL compiler must populate task.MaterializedViewTask.definedSQL — the analyzer "
            + "rejects null/empty inputs with 'definedSQL must be specified'.");

    MaterializedViewAnalyzer.AnalysisResult result = MaterializedViewAnalyzer.analyze(
        definedSql, viewConfig, viewSchema, taskConfigs, _stubContext);

    assertNotNull(result, "Analyzer must produce a non-null result for valid compiled DDL.");
    assertEquals(result.getSourceTableName(), SOURCE_TABLE_RAW,
        "Source-table resolution must yield the raw name parsed from the SELECT FROM clause.");
    assertTrue(result.getSelectFields().contains("ts")
            && result.getSelectFields().contains("city")
            && result.getSelectFields().contains("cnt"),
        "Select-field list must round-trip every column the MV schema declares; got: "
            + result.getSelectFields());
  }

  /// Analyzer-reject path: an MV schema declares a time column (`ts`) that the SELECT
  /// projection does not produce. The DDL compiler accepts this — schema/projection
  /// cross-validation is deferred to the analyzer (the open follow-up tracked in the PR
  /// description under "Open for Discussion") — and the analyzer rejects it via the
  /// `validateSchemaCoveredBySelect` branch with the stable phrase "is not produced by any
  /// SELECT expression". The point of this test is NOT to re-cover every analyzer error
  /// branch (the unit-level `MaterializedViewAnalyzerTest` does that comprehensively),
  /// but to prove:
  ///
  /// 1. DDL compilation does not pre-empt the analyzer with its own cross-validation —
  ///    callers must rely on the analyzer for column-coverage checks, both today and after
  ///    any future tightening of the DDL grammar.
  /// 1. The analyzer's own message survives the compile-then-analyze hop unwrapped — i.e.
  ///    the DDL layer never silently swallows or rewraps a validation result.
  ///
  /// The SQL deliberately omits the time column from SELECT (the closest analog of the
  /// `MaterializedViewAnalyzerTest#testTimeColumnMissingFromSelect` unit case), because
  /// that error branch has a stable, well-tested wording the analyzer has carried since
  /// the original MV PR; matching on it keeps this test from going brittle when the
  /// analyzer is extended with new validation messages over time.
  @Test
  public void compiledMaterializedViewDdlSurfacesAnalyzerRejection() {
    CompiledCreateMaterializedView compiled = (CompiledCreateMaterializedView) DdlCompiler.compile(
        "CREATE MATERIALIZED VIEW mv_orders ("
            + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:MILLISECONDS',"
            + "  city STRING DIMENSION,"
            + "  cnt LONG METRIC"
            + ") REFRESH EVERY '1d' "
            + "PROPERTIES ("
            + "  'timeColumnName' = 'ts',"
            + "  'bucketTimePeriod' = '1d',"
            + "  'replication' = '1'"
            + ") "
            + "AS SELECT city, count(*) AS cnt FROM orders GROUP BY city LIMIT 100000");

    TableConfig viewConfig = compiled.getTableConfig();
    Schema viewSchema = compiled.getSchema();
    Map<String, String> taskConfigs = viewConfig.getTaskConfig()
        .getConfigsForTaskType(MaterializedViewTask.TASK_TYPE);
    String definedSql = taskConfigs.get(MaterializedViewTask.DEFINED_SQL_KEY);

    IllegalStateException e = expectThrows(IllegalStateException.class,
        () -> MaterializedViewAnalyzer.analyze(definedSql, viewConfig, viewSchema, taskConfigs,
            _stubContext));
    if (!e.getMessage().contains("is not produced by any SELECT expression")) {
      fail("Expected the analyzer's `validateSchemaCoveredBySelect` phrasing ('is not "
          + "produced by any SELECT expression') to survive the compile-then-analyze hop "
          + "unwrapped, got: " + e.getMessage());
    }
  }
}
