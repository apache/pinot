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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.core.routing.MockRoutingManagerFactory;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.catalog.PinotTable;
import org.apache.pinot.query.planner.spi.stats.NoOpStatisticsProvider;
import org.apache.pinot.query.planner.spi.stats.PinotStatisticsProvider;
import org.apache.pinot.query.planner.spi.stats.StatConfidence;
import org.apache.pinot.query.planner.spi.stats.TableStatistics;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Tests that row-count statistics flow from [PinotStatisticsProvider] through
/// [org.apache.pinot.query.catalog.PinotTable#getStatistic()] and into
/// [RelMetadataQuery#getRowCount].
///
/// These tests verify the end-to-end path that does NOT require a custom RelMdRowCount handler —
/// the default Calcite handler already picks up the statistic via TableScan.estimateRowCount(mq) →
/// RelOptTableImpl.getRowCount() → PinotTable.getStatistic().getRowCount().
public class PinotRelMdRowCountTest {

  private static final String TABLE_NAME = "a";
  private static final double DELTA = 0.001;

  // --------------------------------------------------------------------------
  // Helpers
  // --------------------------------------------------------------------------

  /// Build a minimal QueryEnvironment with the given statistics provider.
  private static QueryEnvironment buildEnv(PinotStatisticsProvider statsProvider) {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("col1", FieldSpec.DataType.STRING, "")
        .addSingleValueDimension("col2", FieldSpec.DataType.STRING, "")
        .addMetric("col3", FieldSpec.DataType.INT, 0)
        .addDateTime("ts", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS")
        .setSchemaName(TABLE_NAME)
        .build();

    MockRoutingManagerFactory factory = new MockRoutingManagerFactory(1, 2);
    factory.registerTable(schema, TABLE_NAME);
    factory.registerSegment(1, TABLE_NAME + "_OFFLINE", "seg1");

    TableCache tableCache = factory.buildTableCache();

    return new QueryEnvironment(QueryEnvironment.configBuilder()
        .requestId(1L)
        .database(CommonConstants.DEFAULT_DATABASE)
        .tableCache(tableCache)
        .statisticsProvider(statsProvider)
        .build());
  }

  // --------------------------------------------------------------------------
  // Tests
  // --------------------------------------------------------------------------

  /// Verifies that when a statistics provider returns an EXACT row count, the value flows into
  /// RelMetadataQuery.getRowCount() for a TableScan through the default Calcite machinery.
  @Test
  public void testScanRowCountFlowsFromStatisticsProvider() {
    long expectedRowCount = 12_345L;

    PinotStatisticsProvider provider = mock(PinotStatisticsProvider.class);
    when(provider.getTableStatistics(TABLE_NAME)).thenReturn(
        TableStatistics.builder().rowCount(expectedRowCount, StatConfidence.EXACT).build());

    QueryEnvironment env = buildEnv(provider);
    try (QueryEnvironment.CompiledQuery compiled = env.compile("SELECT col1 FROM " + TABLE_NAME)) {
      TableScan scan = findFirstTableScan(compiled.getRelNode());
      assertNotNull(scan, "Plan must contain a TableScan");
      assertEquals(scan.getCluster().getMetadataQuery().getRowCount(scan), (double) expectedRowCount, DELTA,
          "Row count must come from the statistics provider");
    }
  }

  /// Directly tests that [RelMetadataQuery#getRowCount] on a [TableScan] built from
  /// a [org.apache.pinot.query.catalog.PinotTable] with a known row count returns that count.
  @Test
  public void testDirectRelMetadataQueryRowCount() {
    long expectedRowCount = 9_999L;

    PinotStatisticsProvider provider = mock(PinotStatisticsProvider.class);
    when(provider.getTableStatistics(TABLE_NAME)).thenReturn(
        TableStatistics.builder().rowCount(expectedRowCount, StatConfidence.EXACT).build());

    QueryEnvironment env = buildEnv(provider);

    try (QueryEnvironment.CompiledQuery compiled = env.compile("SELECT col1 FROM " + TABLE_NAME)) {
      TableScan scan = findFirstTableScan(compiled.getRelNode());
      // Unconditional: guarding the assertions behind a null check meant a plan-shape change that
      // removed the scan would leave this test green while verifying nothing.
      assertNotNull(scan, "Plan must contain a TableScan");
      // The scan's own cluster carries the statistics-backed metadata provider from env.
      RelMetadataQuery mq = scan.getCluster().getMetadataQuery();
      Double rowCount = mq.getRowCount(scan);
      assertNotNull(rowCount, "Row count must not be null when provider returns EXACT stats");
      assertEquals(rowCount, (double) expectedRowCount, DELTA,
          "Row count must match the value from the statistics provider");
    }
  }

  /// With NoOpStatisticsProvider the statistic carries no row count, so Calcite falls back to its
  /// own heuristic — which must be a real estimate, and specifically not any provider-supplied
  /// value, since the provider has none to give.
  @Test
  public void testNoOpProviderFallsBackToCalciteDefault() {
    QueryEnvironment env = buildEnv(NoOpStatisticsProvider.INSTANCE);
    try (QueryEnvironment.CompiledQuery compiled = env.compile("SELECT col1 FROM " + TABLE_NAME)) {
      TableScan scan = findFirstTableScan(compiled.getRelNode());
      assertNotNull(scan, "Plan must contain a TableScan");
      assertNull(scan.getTable().unwrapOrThrow(PinotTable.class).getStatistic().getRowCount(),
          "A no-op provider must leave the row count unknown");
      Double rowCount = scan.getCluster().getMetadataQuery().getRowCount(scan);
      assertNotNull(rowCount, "Calcite must still supply a heuristic estimate");
      assertTrue(rowCount > 0, "Calcite's heuristic estimate should be positive, got " + rowCount);
    }
  }

  // --------------------------------------------------------------------------
  // Utilities
  // --------------------------------------------------------------------------

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
}
