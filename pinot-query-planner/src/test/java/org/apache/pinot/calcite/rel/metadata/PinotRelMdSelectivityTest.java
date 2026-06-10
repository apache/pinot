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
package org.apache.pinot.calcite.rel.metadata;

import java.util.OptionalLong;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.core.routing.MockRoutingManagerFactory;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.planner.spi.stats.ColumnStatistics;
import org.apache.pinot.query.planner.spi.stats.NoOpStatisticsProvider;
import org.apache.pinot.query.planner.spi.stats.PinotStatisticsProvider;
import org.apache.pinot.query.planner.spi.stats.StatConfidence;
import org.apache.pinot.query.planner.spi.stats.TableStatistics;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


/**
 * Tests for {@link PinotRelMdSelectivity}.
 *
 * <p>Tests are structured as unit tests that build {@link RelNode} trees directly using Calcite
 * APIs, plugging in mock {@link PinotStatisticsProvider}s for fine-grained control.
 */
public class PinotRelMdSelectivityTest {

  private static final String TABLE_NAME = "myTable";
  private static final String TIME_COL = "ts";
  private static final String NON_TIME_COL = "userId";
  private static final double DELTA = 1e-6;
  private static final long ROW_COUNT = 1000L;

  // --------------------------------------------------------------------------
  // Fixture helpers
  // --------------------------------------------------------------------------

  /** Schema with a millisecond time column and an integer non-time column. */
  private static Schema buildSchema() {
    return new Schema.SchemaBuilder()
        .addSingleValueDimension(NON_TIME_COL, FieldSpec.DataType.INT, 0)
        .addDateTime(TIME_COL, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS")
        .setSchemaName(TABLE_NAME)
        .build();
  }

  /** Schema with a seconds-granularity time column (should fall back to default selectivity). */
  private static Schema buildSecondsSchema() {
    return new Schema.SchemaBuilder()
        .addSingleValueDimension(NON_TIME_COL, FieldSpec.DataType.INT, 0)
        .addDateTime(TIME_COL, FieldSpec.DataType.LONG, "1:SECONDS:EPOCH", "1:HOURS")
        .setSchemaName(TABLE_NAME)
        .build();
  }

  /** Build a QueryEnvironment backed by the given schema and statistics provider. */
  private static QueryEnvironment buildEnv(Schema schema, PinotStatisticsProvider statsProvider) {
    MockRoutingManagerFactory factory = new MockRoutingManagerFactory(1, 2);
    factory.registerTable(schema, TABLE_NAME);
    factory.registerSegment(1, TABLE_NAME + "_OFFLINE", "seg1");

    TableCache tableCache = factory.buildTableCache();
    // The planner resolves the table's primary time column from the table config; the factory's
    // bare TableConfig mock has no validation config, so re-stub it with a real config.
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COL).build();
    when(tableCache.getTableConfig(TABLE_NAME + "_OFFLINE")).thenReturn(tableConfig);

    return new QueryEnvironment(QueryEnvironment.configBuilder()
        .requestId(1L)
        .database(CommonConstants.DEFAULT_DATABASE)
        .tableCache(tableCache)
        .statisticsProvider(statsProvider)
        .build());
  }

  /** Build a mock statistics provider: tableStats with given rowCount, time range returns estimate. */
  private static PinotStatisticsProvider mockProviderWithTimeRange(
      long rowCount, long estimatedRows) {
    PinotStatisticsProvider provider = mock(PinotStatisticsProvider.class);
    when(provider.getTableStatistics(TABLE_NAME)).thenReturn(
        TableStatistics.builder().rowCount(rowCount, StatConfidence.EXACT).build());
    when(provider.estimateRowsInTimeRange(eq(TABLE_NAME), anyLong(), anyLong()))
        .thenReturn(OptionalLong.of(estimatedRows));
    return provider;
  }

  /** Find the first Filter node in the RelNode tree (DFS). */
  @Nullable
  private static Filter findFirstFilter(RelNode node) {
    if (node instanceof Filter) {
      return (Filter) node;
    }
    for (RelNode input : node.getInputs()) {
      Filter found = findFirstFilter(input);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  /** Find the first TableScan in the RelNode tree (DFS). */
  @Nullable
  private static TableScan findFirstTableScan(RelNode node) {
    if (node instanceof TableScan) {
      return (TableScan) node;
    }
    for (RelNode input : node.getInputs()) {
      TableScan found = findFirstTableScan(input);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  // --------------------------------------------------------------------------
  // Tests: time-range selectivity
  // --------------------------------------------------------------------------

  /**
   * A time-range predicate {@code ts >= ? AND ts < ?} where the provider returns 10 rows out of
   * 1000 should yield selectivity = 10/1000 = 0.01.
   */
  @Test
  public void testTimeRangeSelectivity() {
    long estimatedRows = 10L;
    PinotStatisticsProvider provider = mockProviderWithTimeRange(ROW_COUNT, estimatedRows);
    QueryEnvironment env = buildEnv(buildSchema(), provider);

    // A range query on the time column.
    try (QueryEnvironment.CompiledQuery compiled = env.compile(
        "SELECT " + NON_TIME_COL + " FROM " + TABLE_NAME
            + " WHERE " + TIME_COL + " >= 1000000 AND " + TIME_COL + " < 2000000")) {
      RelNode relNode = compiled.getRelNode();
      Filter filter = findFirstFilter(relNode);
      assertNotNull(filter, "A Filter node must be present in the plan");

      RelMetadataQuery mq = filter.getCluster().getMetadataQuery();
      // Selectivity is called with null predicate to ask for the filter's own condition.
      Double sel = mq.getSelectivity(filter, null);
      assertNotNull(sel, "Selectivity must not be null");
      double expected = (double) estimatedRows / ROW_COUNT;
      assertEquals(sel, expected, DELTA,
          "Time-range selectivity must equal estimatedRows/rowCount");
    }
  }

  /**
   * Time bounds are tracked internally as inclusive but {@code estimateRowsInTimeRange} takes a
   * half-open {@code [start, end)} interval. An equality predicate {@code ts = X} must therefore
   * query {@code [X, X+1)} — NOT the empty {@code [X, X)}. This pins the exact bounds passed to
   * the provider (regression test: equality used to yield selectivity 0).
   */
  @Test
  public void testEqualityPredicateBoundsConversion() {
    long ts = 1_000_000L;
    PinotStatisticsProvider provider = mockProviderWithTimeRange(ROW_COUNT, 10L);
    QueryEnvironment env = buildEnv(buildSchema(), provider);

    try (QueryEnvironment.CompiledQuery compiled = env.compile(
        "SELECT " + NON_TIME_COL + " FROM " + TABLE_NAME + " WHERE " + TIME_COL + " = " + ts)) {
      RelNode relNode = compiled.getRelNode();
      Filter filter = findFirstFilter(relNode);
      assertNotNull(filter, "A Filter node must be present in the plan");

      RelMetadataQuery mq = filter.getCluster().getMetadataQuery();
      Double sel = mq.getSelectivity(filter, null);
      assertNotNull(sel, "Selectivity must not be null");
      assertEquals(sel, 10.0 / ROW_COUNT, DELTA,
          "Equality on the time column must use the row estimate, not collapse to 0");

      ArgumentCaptor<Long> startCaptor = ArgumentCaptor.forClass(Long.class);
      ArgumentCaptor<Long> endCaptor = ArgumentCaptor.forClass(Long.class);
      verify(provider, atLeastOnce())
          .estimateRowsInTimeRange(eq(TABLE_NAME), startCaptor.capture(), endCaptor.capture());
      assertEquals((long) startCaptor.getValue(), ts, "Inclusive lower bound must be ts");
      assertEquals((long) endCaptor.getValue(), ts + 1,
          "Inclusive upper bound ts must convert to exclusive end ts+1");
    }
  }

  /**
   * A time-column with seconds granularity (non-millis) should fall back to Calcite's default
   * selectivity guess rather than using estimateRowsInTimeRange.
   */
  @Test
  public void testNonMillisTimeColumnFallsBackToDefault() {
    PinotStatisticsProvider provider = mockProviderWithTimeRange(ROW_COUNT, 10L);
    QueryEnvironment env = buildEnv(buildSecondsSchema(), provider);

    try (QueryEnvironment.CompiledQuery compiled = env.compile(
        "SELECT " + NON_TIME_COL + " FROM " + TABLE_NAME
            + " WHERE " + TIME_COL + " >= 1000000")) {
      RelNode relNode = compiled.getRelNode();
      Filter filter = findFirstFilter(relNode);
      assertNotNull(filter, "A Filter node must be present in the plan");

      RelMetadataQuery mq = filter.getCluster().getMetadataQuery();
      Double pinotSel = mq.getSelectivity(filter, null);
      assertNotNull(pinotSel, "Selectivity must not be null");

      // With seconds-granularity, the column is NOT recognized as millis — should fall back.
      // The default guess for a comparison is RelMdUtil.guessSelectivity which returns 0.25.
      // We verify the value is close to the default guess.
      Double defaultSel = RelMdUtil.guessSelectivity(filter.getCondition());
      assertEquals(pinotSel, defaultSel, DELTA,
          "Non-millis time column must fall back to Calcite default selectivity");
    }
  }

  // --------------------------------------------------------------------------
  // Tests: NDV equality selectivity
  // --------------------------------------------------------------------------

  /**
   * An equality predicate on a non-time column with NDV=50 (EXACT confidence) should yield
   * selectivity = 1/50 = 0.02.
   */
  @Test
  public void testNdvEqualitySelectivity() {
    long ndv = 50L;
    PinotStatisticsProvider provider = mock(PinotStatisticsProvider.class);
    when(provider.getTableStatistics(TABLE_NAME)).thenReturn(
        TableStatistics.builder().rowCount(ROW_COUNT, StatConfidence.EXACT).build());
    when(provider.getColumnStatistics(TABLE_NAME, NON_TIME_COL)).thenReturn(
        ColumnStatistics.builder()
            .columnName(NON_TIME_COL)
            .ndv(ndv, StatConfidence.EXACT)
            .build());

    QueryEnvironment env = buildEnv(buildSchema(), provider);

    try (QueryEnvironment.CompiledQuery compiled = env.compile(
        "SELECT " + NON_TIME_COL + " FROM " + TABLE_NAME
            + " WHERE " + NON_TIME_COL + " = 42")) {
      RelNode relNode = compiled.getRelNode();
      Filter filter = findFirstFilter(relNode);
      assertNotNull(filter, "A Filter node must be present in the plan");

      RelMetadataQuery mq = filter.getCluster().getMetadataQuery();
      Double sel = mq.getSelectivity(filter, null);
      assertNotNull(sel, "Selectivity must not be null");
      assertEquals(sel, 1.0 / ndv, DELTA,
          "Equality selectivity must equal 1/NDV when NDV stats are present");
    }
  }

  /**
   * An equality predicate on a column where getColumnStatistics returns null should fall back
   * to the Calcite default guess.
   */
  @Test
  public void testNdvFallsBackToDefaultWhenNoColumnStats() {
    PinotStatisticsProvider provider = mock(PinotStatisticsProvider.class);
    when(provider.getTableStatistics(TABLE_NAME)).thenReturn(
        TableStatistics.builder().rowCount(ROW_COUNT, StatConfidence.EXACT).build());
    when(provider.getColumnStatistics(anyString(), anyString())).thenReturn(null);

    QueryEnvironment env = buildEnv(buildSchema(), provider);

    try (QueryEnvironment.CompiledQuery compiled = env.compile(
        "SELECT " + NON_TIME_COL + " FROM " + TABLE_NAME
            + " WHERE " + NON_TIME_COL + " = 99")) {
      RelNode relNode = compiled.getRelNode();
      Filter filter = findFirstFilter(relNode);
      assertNotNull(filter, "A Filter node must be present in the plan");

      RelMetadataQuery mq = filter.getCluster().getMetadataQuery();
      Double pinotSel = mq.getSelectivity(filter, null);
      Double defaultSel = RelMdUtil.guessSelectivity(filter.getCondition());
      assertEquals(pinotSel, defaultSel, DELTA,
          "No column stats must fall back to Calcite default selectivity");
    }
  }

  // --------------------------------------------------------------------------
  // Tests: NoOp provider parity with Calcite default
  // --------------------------------------------------------------------------

  /**
   * With NoOpStatisticsProvider, selectivity must equal Calcite's own defaults — no behavior
   * change from adding the Pinot metadata provider when stats are absent.
   */
  @Test
  public void testNoOpProviderSelectivityEqualsCalciteDefault() {
    QueryEnvironment noOpEnv = buildEnv(buildSchema(), NoOpStatisticsProvider.INSTANCE);

    try (QueryEnvironment.CompiledQuery compiled = noOpEnv.compile(
        "SELECT " + NON_TIME_COL + " FROM " + TABLE_NAME
            + " WHERE " + TIME_COL + " >= 500000")) {
      RelNode relNode = compiled.getRelNode();
      Filter filter = findFirstFilter(relNode);
      assertNotNull(filter, "A Filter node must be present in the plan");

      // Selectivity via the Pinot chain (NoOp path).
      RelMetadataQuery mqPinot = filter.getCluster().getMetadataQuery();
      Double pinotSel = mqPinot.getSelectivity(filter, null);

      // Expected: Calcite default guess.
      Double defaultSel = RelMdUtil.guessSelectivity(filter.getCondition());

      assertNotNull(pinotSel);
      assertNotNull(defaultSel);
      assertEquals(pinotSel, defaultSel, DELTA,
          "NoOp provider must yield the same selectivity as Calcite defaults");
    }
  }

  // --------------------------------------------------------------------------
  // Tests: end-to-end compile with mock stats provider
  // --------------------------------------------------------------------------

  /**
   * Verifies that a real query compiles successfully end-to-end when a non-trivial statistics
   * provider is configured — no exceptions means the metadata chain is correctly integrated.
   */
  @Test
  public void testEndToEndCompileWithStatsProvider() {
    PinotStatisticsProvider provider = mockProviderWithTimeRange(ROW_COUNT, 10L);
    QueryEnvironment env = buildEnv(buildSchema(), provider);

    // Compile a query touching both the time and non-time columns.
    try (QueryEnvironment.CompiledQuery compiled = env.compile(
        "SELECT " + NON_TIME_COL + " FROM " + TABLE_NAME
            + " WHERE " + TIME_COL + " >= 1000 AND " + TIME_COL + " < 5000"
            + " AND " + NON_TIME_COL + " = 7")) {
      assertNotNull(compiled, "Query must compile without exceptions");
      assertNotNull(compiled.getRelNode(), "Compiled RelNode must be non-null");
    }
  }
}
