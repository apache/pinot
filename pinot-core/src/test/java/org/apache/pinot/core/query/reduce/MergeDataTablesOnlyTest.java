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
package org.apache.pinot.core.query.reduce;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/**
 * Verifies the core merge-only contract: reducing the raw per-server DataTable map produces the same
 * final result as reducing a single re-injected merged intermediate DataTable, i.e.
 * <pre>reduceOnDataTable(rawMap) == reduceOnDataTable({ syntheticKey -> mergeOnDataTable(rawMap) })</pre>
 * for the supported reducers (Aggregation, Group-by, Distinct), including the OBJECT-column
 * (DISTINCTCOUNT) round-trip. Also checks that the merged DataTable carries the intermediate schema and
 * that unsupported reducers (Selection) throw.
 */
public class MergeDataTablesOnlyTest {
  private static final long TIMEOUT_MS = 60_000L;
  private BrokerReduceService _reduceService;

  @BeforeClass
  public void setUp() {
    _reduceService =
        new BrokerReduceService(new PinotConfiguration(Map.of(Broker.CONFIG_OF_MAX_REDUCE_THREADS_PER_QUERY, 2)));
  }

  @AfterClass
  public void tearDown() {
    _reduceService.shutDown();
  }

  @Test
  public void testAggregationScalarRoundTrip() {
    String query = "SELECT COUNT(*), SUM(m), MIN(m), MAX(m) FROM testTable";
    DataSchema schema = new DataSchema(new String[]{"count(*)", "sum(m)", "min(m)", "max(m)"},
        new ColumnDataType[]{ColumnDataType.LONG, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE, ColumnDataType.DOUBLE});
    List<DataTable> serverTables = List.of(
        buildRow(schema, 3L, 10.0, 2.0, 8.0),
        buildRow(schema, 2L, 5.0, 1.0, 9.0),
        buildRow(schema, 4L, 20.0, 3.0, 7.0));
    assertRoundTrip(query, serverTables);
  }

  @Test
  public void testDistinctCountObjectRoundTrip()
      throws IOException {
    String query = "SELECT DISTINCTCOUNT(col1) FROM testTable";
    AggregationFunction aggFunction = aggFunctions(query)[0];
    DataSchema schema =
        new DataSchema(new String[]{"distinctcount(col1)"}, new ColumnDataType[]{ColumnDataType.OBJECT});
    List<DataTable> serverTables = List.of(
        buildObjectRow(schema, aggFunction, new IntOpenHashSet(new int[]{1, 2, 3})),
        buildObjectRow(schema, aggFunction, new IntOpenHashSet(new int[]{3, 4, 5})));
    assertRoundTrip(query, serverTables);
  }

  @Test
  public void testGroupByRoundTrip() {
    String query = "SELECT col1, COUNT(*), SUM(m) FROM testTable GROUP BY col1";
    DataSchema schema = new DataSchema(new String[]{"col1", "count(*)", "sum(m)"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.DOUBLE});
    List<DataTable> serverTables = List.of(
        buildGroupBy(schema, new Object[][]{{1, 2L, 10.0}, {2, 1L, 5.0}}),
        buildGroupBy(schema, new Object[][]{{1, 3L, 20.0}, {3, 4L, 8.0}}));
    assertRoundTrip(query, serverTables);
  }

  /**
   * Regression for the merge-only-reducer GROUP BY + DISTINCTCOUNT defect: prior to the fix,
   * {@code mergeDataTablesOnly} called {@code getIndexedTable} which unconditionally finished the
   * IndexedTable with {@code storeFinalResult=true} — finalizing each row's aggregate (Set → Integer).
   * The subsequent {@code buildIntermediateDataTable} then read {@code _storedColumnDataTypes} (still
   * OBJECT), took the OBJECT branch, and handed the now-Integer value into
   * {@code BaseDistinctAggregateAggregationFunction.serializeIntermediateResult(Set)} →
   * {@link ClassCastException}. This test fails without the fix and passes with it.
   *
   * <p>Builds fresh per-server DataTables for the {@code reduce(...)} baseline and the
   * {@code merge(...)} call separately. The regular reduce path mutates the input DataTable's
   * schema's column-data-types array in place via {@code IndexedTable.finish(_, true)} — that's
   * pre-existing OSS behavior and is fine in production where no caller re-reads the input after
   * a reduce. Reusing the same DataTable list across both calls would falsely surface that
   * mutation as an apparent merge-only bug.
   */
  @Test
  public void testGroupByDistinctCountObjectRoundTrip()
      throws IOException {
    String query = "SELECT col1, DISTINCTCOUNT(col2) FROM testTable GROUP BY col1";
    BrokerRequest brokerRequest = CalciteSqlCompiler.compileToBrokerRequest(query);
    BrokerResponseNative baseline = reduce(brokerRequest, toMap(buildDistinctCountServerTables(query)));
    DataTable merged = merge(brokerRequest, toMap(buildDistinctCountServerTables(query)));
    assertNotNull(merged, "merge produced null");
    BrokerResponseNative viaMerge = reduce(brokerRequest, singletonMap(merged));
    assertResultTablesEquivalent(baseline.getResultTable(), viaMerge.getResultTable());
  }

  /** Two per-server intermediate DataTables for {@code GROUP BY col1, DISTINCTCOUNT(col2)} with one
   * overlapping group across servers so the OBJECT-path per-group Set merge actually runs. */
  private static List<DataTable> buildDistinctCountServerTables(String query)
      throws IOException {
    AggregationFunction aggFunction = aggFunctions(query)[0];
    DataSchema schema = new DataSchema(new String[]{"col1", "distinctcount(col2)"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.OBJECT});
    return List.of(
        buildGroupByWithObject(schema, aggFunction,
            new Object[]{1, new IntOpenHashSet(new int[]{1, 2, 3})},
            new Object[]{2, new IntOpenHashSet(new int[]{4, 5})}),
        buildGroupByWithObject(schema, aggFunction,
            new Object[]{1, new IntOpenHashSet(new int[]{3, 4, 5})},
            new Object[]{3, new IntOpenHashSet(new int[]{6, 7})}));
  }

  @Test
  public void testDistinctRoundTrip() {
    String query = "SELECT DISTINCT col1 FROM testTable";
    DataSchema schema = new DataSchema(new String[]{"col1"}, new ColumnDataType[]{ColumnDataType.INT});
    List<DataTable> serverTables = List.of(
        buildGroupBy(schema, new Object[][]{{1}, {2}}),
        buildGroupBy(schema, new Object[][]{{2}, {3}}));
    assertRoundTrip(query, serverTables);
  }

  @Test
  public void testAggregationServerReturnFinalResultRejected() {
    // Under server.returnFinalResult, the per-server DataTables hold finalized (not intermediate)
    // aggregate state; the merge-only contract cannot be honored, so the reducer must throw rather
    // than silently merge final-typed values as if they were intermediates.
    BrokerRequest brokerRequest = CalciteSqlCompiler.compileToBrokerRequest("SELECT COUNT(*) FROM testTable");
    brokerRequest.getPinotQuery().putToQueryOptions(Broker.Request.QueryOptionKey.SERVER_RETURN_FINAL_RESULT, "true");
    DataSchema schema = new DataSchema(new String[]{"count(*)"}, new ColumnDataType[]{ColumnDataType.LONG});
    Map<ServerRoutingInstance, DataTable> map = singletonMap(buildRow(schema, 5L));
    assertThrows(UnsupportedOperationException.class, () -> merge(brokerRequest, map));
  }

  @Test
  public void testGroupByServerReturnFinalResultRejected() {
    BrokerRequest brokerRequest = CalciteSqlCompiler.compileToBrokerRequest(
        "SELECT col1, COUNT(*) FROM testTable GROUP BY col1");
    brokerRequest.getPinotQuery().putToQueryOptions(Broker.Request.QueryOptionKey.SERVER_RETURN_FINAL_RESULT, "true");
    DataSchema schema = new DataSchema(new String[]{"col1", "count(*)"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG});
    Map<ServerRoutingInstance, DataTable> map = singletonMap(buildGroupBy(schema, new Object[][]{{1, 2L}}));
    assertThrows(UnsupportedOperationException.class, () -> merge(brokerRequest, map));
  }

  @Test
  public void testGroupByLimitZeroRoundTrip() {
    // LIMIT 0 on group-by: mergeDataTablesOnly does NOT apply LIMIT (intermediate is unlimited),
    // so a non-empty intermediate DataTable is produced. The re-injected reduce path applies the
    // LIMIT and produces an empty final result, matching the direct reduce.
    String query = "SELECT col1, COUNT(*) FROM testTable GROUP BY col1 LIMIT 0";
    DataSchema schema = new DataSchema(new String[]{"col1", "count(*)"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG});
    List<DataTable> serverTables = List.of(
        buildGroupBy(schema, new Object[][]{{1, 2L}}),
        buildGroupBy(schema, new Object[][]{{2, 3L}}));
    assertRoundTrip(query, serverTables);
  }

  @Test
  public void testMergedDataTableCarriesIntermediateSchema() {
    String query = "SELECT COUNT(*), SUM(m), DISTINCTCOUNT(col1) FROM testTable";
    AggregationFunction[] aggFunctions = aggFunctions(query);
    // Build intermediate rows: COUNT->LONG, SUM->DOUBLE, DISTINCTCOUNT->OBJECT
    DataSchema schema = new DataSchema(new String[]{"count(*)", "sum(m)", "distinctcount(col1)"},
        new ColumnDataType[]{ColumnDataType.LONG, ColumnDataType.DOUBLE, ColumnDataType.OBJECT});
    DataTableBuilder builder = DataTableBuilderFactory.getDataTableBuilder(schema);
    try {
      builder.startRow();
      builder.setColumn(0, 5L);
      builder.setColumn(1, 12.0);
      builder.setColumn(2, aggFunctions[2].serializeIntermediateResult(new IntOpenHashSet(new int[]{1, 2})));
      builder.finishRow();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    Map<ServerRoutingInstance, DataTable> map = singletonMap(builder.build());

    DataTable merged = merge(query, map);
    assertNotNull(merged);
    ColumnDataType[] mergedTypes = merged.getDataSchema().getColumnDataTypes();
    for (int i = 0; i < aggFunctions.length; i++) {
      assertEquals(mergedTypes[i], aggFunctions[i].getIntermediateResultColumnType(),
          "merged column " + i + " must carry the intermediate type, not the finalized type");
    }
  }

  @Test
  public void testEmptyMapReturnsNull() {
    assertNull(merge("SELECT COUNT(*) FROM testTable", new HashMap<>()));
  }

  @Test
  public void testAllEmptyDataTablesReturnsNull() {
    // A metadata-only (0-row, null schema) data table is dropped, leaving nothing to merge.
    DataSchema schema = new DataSchema(new String[]{"count(*)"}, new ColumnDataType[]{ColumnDataType.LONG});
    DataTable emptyMetadataOnly = DataTableBuilderFactory.getDataTableBuilder(schema).build().toMetadataOnlyDataTable();
    assertNull(merge("SELECT COUNT(*) FROM testTable", singletonMap(emptyMetadataOnly)));
  }

  @Test
  public void testSelectionIsUnsupported() {
    String query = "SELECT col1 FROM testTable";
    DataSchema schema = new DataSchema(new String[]{"col1"}, new ColumnDataType[]{ColumnDataType.INT});
    Map<ServerRoutingInstance, DataTable> map = singletonMap(buildGroupBy(schema, new Object[][]{{1}, {2}}));
    // SelectionDataTableReducer inherits the default-throwing mergeDataTablesOnly (selection is out of
    // scope for merge-only reduction).
    assertThrows(UnsupportedOperationException.class, () -> merge(query, map));
  }

  @Test
  public void testNullHandlingAggregationRoundTrip()
      throws IOException {
    BrokerRequest brokerRequest = CalciteSqlCompiler.compileToBrokerRequest("SELECT MIN(m) FROM testTable");
    brokerRequest.getPinotQuery().putToQueryOptions(Broker.Request.QueryOptionKey.ENABLE_NULL_HANDLING, "true");
    DataSchema schema = new DataSchema(new String[]{"min(m)"}, new ColumnDataType[]{ColumnDataType.DOUBLE});
    // One server has a value, the other contributes a null (e.g. all rows filtered out).
    List<DataTable> serverTables = List.of(buildNullableDouble(schema, 5.0), buildNullableDouble(schema, null));
    assertRoundTrip(brokerRequest, serverTables);
  }

  @Test
  public void testConflictingSchemaSurfacedAsIncompleteMerge() {
    // When one server's column types conflict with the first non-empty table's, filterDataTablesAndPickSchema
    // drops that server. The merge proceeds on the remaining servers and the returned DataTable carries
    // the INCOMPLETE_MERGE flag so a downstream consumer can detect that the merge ran over a strict
    // subset of inputs.
    String query = "SELECT COUNT(*) FROM testTable";
    DataSchema schemaLong = new DataSchema(new String[]{"count(*)"}, new ColumnDataType[]{ColumnDataType.LONG});
    DataSchema schemaInt = new DataSchema(new String[]{"count(*)"}, new ColumnDataType[]{ColumnDataType.INT});
    DataTable t1 = buildRow(schemaLong, 5L);
    DataTable t2 = buildRow(schemaLong, 7L);
    // Conflicting column data type (INT vs LONG) — filterDataTablesAndPickSchema will drop t3.
    DataTable t3 = buildRow(schemaInt, 99);

    DataTable merged = merge(query, toMap(List.of(t1, t2, t3)));
    assertNotNull(merged);
    assertEquals(merged.getMetadata().get(MetadataKey.INCOMPLETE_MERGE.getName()), "true",
        "INCOMPLETE_MERGE must be surfaced when a server is dropped due to schema conflict");
  }

  @Test
  public void testAllSameSchemaDoesNotSetIncompleteMerge() {
    String query = "SELECT COUNT(*) FROM testTable";
    DataSchema schema = new DataSchema(new String[]{"count(*)"}, new ColumnDataType[]{ColumnDataType.LONG});
    DataTable merged = merge(query, toMap(List.of(buildRow(schema, 5L), buildRow(schema, 7L))));
    assertNotNull(merged);
    assertNull(merged.getMetadata().get(MetadataKey.INCOMPLETE_MERGE.getName()),
        "INCOMPLETE_MERGE must not be set on a clean merge");
  }

  @Test
  public void testAdditiveStatsAggregatedOntoMergedMetadata() {
    // Two inputs carry numDocsScanned + threadCpuTimeNs in their metadata; the merged DataTable
    // should sum them so a downstream re-reduce sees the same totals.
    String query = "SELECT COUNT(*) FROM testTable";
    DataSchema schema = new DataSchema(new String[]{"count(*)"}, new ColumnDataType[]{ColumnDataType.LONG});
    DataTable t1 = buildRow(schema, 5L);
    DataTable t2 = buildRow(schema, 7L);
    t1.getMetadata().put(MetadataKey.NUM_DOCS_SCANNED.getName(), "100");
    t1.getMetadata().put(MetadataKey.THREAD_CPU_TIME_NS.getName(), "1000");
    t2.getMetadata().put(MetadataKey.NUM_DOCS_SCANNED.getName(), "250");
    t2.getMetadata().put(MetadataKey.THREAD_CPU_TIME_NS.getName(), "3000");

    DataTable merged = merge(query, toMap(List.of(t1, t2)));
    assertNotNull(merged);
    assertEquals(merged.getMetadata().get(MetadataKey.NUM_DOCS_SCANNED.getName()), "350");
    assertEquals(merged.getMetadata().get(MetadataKey.THREAD_CPU_TIME_NS.getName()), "4000");
  }

  @Test
  public void testAdditiveStatsAbsentFromAllInputsWriteZero() {
    // Mirrors setStats()'s unconditional-write pattern: when none of the inputs carry an additive
    // stats key, the merged DataTable carries "0" for it. Downstream's Long.parseLong("0") and
    // null-check both yield 0, so the user-facing BrokerResponse is identical either way.
    String query = "SELECT COUNT(*) FROM testTable";
    DataSchema schema = new DataSchema(new String[]{"count(*)"}, new ColumnDataType[]{ColumnDataType.LONG});
    DataTable merged = merge(query, toMap(List.of(buildRow(schema, 5L), buildRow(schema, 7L))));
    assertNotNull(merged);
    assertEquals(merged.getMetadata().get(MetadataKey.NUM_DOCS_SCANNED.getName()), "0");
  }

  @Test
  public void testMinConsumingFreshnessTimeMsTakesMin() {
    // MIN_CONSUMING_FRESHNESS_TIME_MS semantically captures the WORST freshness across servers
    // (used by FRESHNESS_LAG_MS); the merge must MIN-reduce, not SUM.
    String query = "SELECT COUNT(*) FROM testTable";
    DataSchema schema = new DataSchema(new String[]{"count(*)"}, new ColumnDataType[]{ColumnDataType.LONG});
    DataTable t1 = buildRow(schema, 5L);
    DataTable t2 = buildRow(schema, 7L);
    t1.getMetadata().put(MetadataKey.MIN_CONSUMING_FRESHNESS_TIME_MS.getName(), "1000");
    t2.getMetadata().put(MetadataKey.MIN_CONSUMING_FRESHNESS_TIME_MS.getName(), "500");

    DataTable merged = merge(query, toMap(List.of(t1, t2)));
    assertNotNull(merged);
    assertEquals(merged.getMetadata().get(MetadataKey.MIN_CONSUMING_FRESHNESS_TIME_MS.getName()), "500");
  }

  @Test
  public void testStatsFromDroppedZeroRowServersStillCounted() {
    // A 0-row metadata-only DataTable is dropped from the merge inputs by filterDataTablesAndPickSchema,
    // but its stats must still be aggregated (matches reduceOnDataTable's behavior — the aggregator
    // runs BEFORE the iterator.remove()).
    String query = "SELECT COUNT(*) FROM testTable";
    DataSchema schema = new DataSchema(new String[]{"count(*)"}, new ColumnDataType[]{ColumnDataType.LONG});
    DataTable withData = buildRow(schema, 5L);
    withData.getMetadata().put(MetadataKey.NUM_DOCS_SCANNED.getName(), "100");
    // A 0-row server (e.g. all rows filtered out) still reports stats.
    DataTable emptyRows = DataTableBuilderFactory.getDataTableBuilder(schema).build();
    emptyRows.getMetadata().put(MetadataKey.NUM_DOCS_SCANNED.getName(), "50");

    DataTable merged = merge(query, toMap(List.of(withData, emptyRows)));
    assertNotNull(merged);
    assertEquals(merged.getMetadata().get(MetadataKey.NUM_DOCS_SCANNED.getName()), "150");
  }

  @Test
  public void testAdditiveStatsRoundTripThroughReduce() {
    // End-to-end: aggregating stats on the merged DataTable means a downstream reduce on the
    // re-injected merged DataTable sees the same numDocsScanned the direct reduce would.
    BrokerRequest brokerRequest = CalciteSqlCompiler.compileToBrokerRequest("SELECT COUNT(*) FROM testTable");
    DataSchema schema = new DataSchema(new String[]{"count(*)"}, new ColumnDataType[]{ColumnDataType.LONG});
    DataTable t1 = buildRow(schema, 5L);
    DataTable t2 = buildRow(schema, 7L);
    t1.getMetadata().put(MetadataKey.NUM_DOCS_SCANNED.getName(), "100");
    t2.getMetadata().put(MetadataKey.NUM_DOCS_SCANNED.getName(), "250");

    BrokerResponseNative direct = reduce(brokerRequest, toMap(List.of(t1, t2)));
    DataTable merged = merge(brokerRequest, toMap(List.of(t1, t2)));
    assertNotNull(merged);
    BrokerResponseNative viaMerge = reduce(brokerRequest, singletonMap(merged));

    assertEquals(viaMerge.getNumDocsScanned(), direct.getNumDocsScanned(),
        "merged DataTable must carry numDocsScanned so a downstream reduce produces the same total");
    assertEquals(viaMerge.getNumDocsScanned(), 350L);
  }

  @Test
  public void testExceptionsCopiedToMergedDataTable() {
    // Per-server exceptions are accumulated by the aggregator and written back onto the merged
    // DataTable's exception map, so a downstream reduce surfaces them on the BrokerResponse.
    BrokerRequest brokerRequest = CalciteSqlCompiler.compileToBrokerRequest("SELECT COUNT(*) FROM testTable");
    DataSchema schema = new DataSchema(new String[]{"count(*)"}, new ColumnDataType[]{ColumnDataType.LONG});
    DataTable t1 = buildRow(schema, 5L);
    DataTable t2 = buildRow(schema, 7L);
    t1.addException(QueryErrorCode.QUERY_EXECUTION, "boom on server 1");
    t2.addException(QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED, "limit on server 2");

    DataTable merged = merge(brokerRequest, toMap(List.of(t1, t2)));
    assertNotNull(merged);
    assertEquals(merged.getExceptions().get(QueryErrorCode.QUERY_EXECUTION.getId()), "boom on server 1");
    assertEquals(merged.getExceptions().get(QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED.getId()),
        "limit on server 2");

    BrokerResponseNative viaMerge = reduce(brokerRequest, singletonMap(merged));
    // Downstream reduce surfaces both as response exceptions (one per distinct error code).
    assertEquals(viaMerge.getExceptions().size(), 2);
  }

  @Test
  public void testTraceInfoCopiedToMergedDataTableWhenEnabled() {
    // When trace is enabled, per-server trace info is JSON-encoded into the merged DataTable's
    // TRACE_INFO metadata entry. The aggregator keys traces by server short-name (hostname-derived),
    // so we use distinct hostnames to preserve attribution.
    BrokerRequest brokerRequest = CalciteSqlCompiler.compileToBrokerRequest("SELECT COUNT(*) FROM testTable");
    brokerRequest.getPinotQuery().putToQueryOptions(Broker.Request.TRACE, "true");
    DataSchema schema = new DataSchema(new String[]{"count(*)"}, new ColumnDataType[]{ColumnDataType.LONG});
    DataTable t1 = buildRow(schema, 5L);
    DataTable t2 = buildRow(schema, 7L);
    t1.getMetadata().put(MetadataKey.TRACE_INFO.getName(), "trace-from-server-1");
    t2.getMetadata().put(MetadataKey.TRACE_INFO.getName(), "trace-from-server-2");
    Map<ServerRoutingInstance, DataTable> map = new HashMap<>();
    map.put(new ServerRoutingInstance("hostA", 1000, TableType.OFFLINE), t1);
    map.put(new ServerRoutingInstance("hostB", 1001, TableType.OFFLINE), t2);

    DataTable merged;
    try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
      merged = _reduceService.mergeOnDataTable(brokerRequest, map, TIMEOUT_MS, mock(BrokerMetrics.class));
    }
    assertNotNull(merged);
    String mergedTrace = merged.getMetadata().get(MetadataKey.TRACE_INFO.getName());
    assertNotNull(mergedTrace, "TRACE_INFO must be present on merged DataTable when trace is enabled");
    // JSON-encoded map; both inputs' trace strings must be present.
    assertTrue(mergedTrace.contains("trace-from-server-1"));
    assertTrue(mergedTrace.contains("trace-from-server-2"));
  }

  @Test
  public void testTraceInfoSkippedWhenTraceDisabled() {
    // Without trace=true, the aggregator skips trace collection, so the merged DataTable has no
    // TRACE_INFO metadata even if inputs carried it.
    String query = "SELECT COUNT(*) FROM testTable";
    DataSchema schema = new DataSchema(new String[]{"count(*)"}, new ColumnDataType[]{ColumnDataType.LONG});
    DataTable t1 = buildRow(schema, 5L);
    t1.getMetadata().put(MetadataKey.TRACE_INFO.getName(), "trace-from-server-1");

    DataTable merged = merge(query, toMap(List.of(t1)));
    assertNotNull(merged);
    assertNull(merged.getMetadata().get(MetadataKey.TRACE_INFO.getName()));
  }

  @Test
  public void testNumGroupsLimitReachedFlagSurfaced() {
    String query = "SELECT col1, COUNT(*) FROM testTable GROUP BY col1";
    DataSchema schema = new DataSchema(new String[]{"col1", "count(*)"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG});
    DataTable t1 = buildGroupBy(schema, new Object[][]{{1, 2L}});
    DataTable t2 = buildGroupBy(schema, new Object[][]{{2, 3L}});
    // Simulate a server that hit numGroupsLimit.
    t2.getMetadata().put(MetadataKey.NUM_GROUPS_LIMIT_REACHED.getName(), "true");

    DataTable merged = merge(query, toMap(List.of(t1, t2)));
    assertNotNull(merged);
    assertEquals(merged.getMetadata().get(MetadataKey.NUM_GROUPS_LIMIT_REACHED.getName()), "true",
        "NUM_GROUPS_LIMIT_REACHED from an input server must be surfaced on the merged DataTable");
  }

  // ---- helpers ----

  /**
   * Asserts the round-trip equivalence for the given query and per-server intermediate DataTables.
   */
  private void assertRoundTrip(String query, List<DataTable> serverTables) {
    assertRoundTrip(CalciteSqlCompiler.compileToBrokerRequest(query), serverTables);
  }

  private void assertRoundTrip(BrokerRequest brokerRequest, List<DataTable> serverTables) {
    BrokerResponseNative baseline = reduce(brokerRequest, toMap(serverTables));

    DataTable merged = merge(brokerRequest, toMap(serverTables));
    assertNotNull(merged, "merge produced null");

    BrokerResponseNative viaMerge = reduce(brokerRequest, singletonMap(merged));

    assertResultTablesEquivalent(baseline.getResultTable(), viaMerge.getResultTable());
  }

  private BrokerResponseNative reduce(BrokerRequest brokerRequest, Map<ServerRoutingInstance, DataTable> map) {
    try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
      return _reduceService.reduceOnDataTable(brokerRequest, brokerRequest, map, TIMEOUT_MS, mock(BrokerMetrics.class));
    }
  }

  private DataTable merge(String query, Map<ServerRoutingInstance, DataTable> map) {
    return merge(CalciteSqlCompiler.compileToBrokerRequest(query), map);
  }

  private DataTable merge(BrokerRequest brokerRequest, Map<ServerRoutingInstance, DataTable> map) {
    try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
      return _reduceService.mergeOnDataTable(brokerRequest, map, TIMEOUT_MS, mock(BrokerMetrics.class));
    }
  }

  private static AggregationFunction[] aggFunctions(String query) {
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext(CalciteSqlCompiler.compileToBrokerRequest(query).getPinotQuery());
    AggregationFunction[] aggregationFunctions = queryContext.getAggregationFunctions();
    assertNotNull(aggregationFunctions);
    return aggregationFunctions;
  }

  private static DataTable buildRow(DataSchema schema, Object... values) {
    DataTableBuilder builder = DataTableBuilderFactory.getDataTableBuilder(schema);
    try {
      appendRow(builder, schema, values);
      return builder.build();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static DataTable buildGroupBy(DataSchema schema, Object[][] rows) {
    DataTableBuilder builder = DataTableBuilderFactory.getDataTableBuilder(schema);
    try {
      for (Object[] row : rows) {
        appendRow(builder, schema, row);
      }
      return builder.build();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Builds a GROUP BY DataTable with one INT key column followed by one OBJECT aggregate column whose
   * value is the intermediate {@code Set} (or other aggregate state) the server would have emitted.
   * Mirrors what {@code GroupByResultsBlock.getDataTable()} produces on the server side for the
   * non-null-handling path: scalar key written directly, OBJECT column written via
   * {@code serializeIntermediateResult}.
   */
  private static DataTable buildGroupByWithObject(DataSchema schema, AggregationFunction aggFunction,
      Object[]... rows)
      throws IOException {
    DataTableBuilder builder = DataTableBuilderFactory.getDataTableBuilder(schema);
    for (Object[] row : rows) {
      builder.startRow();
      // Single INT key column at index 0; OBJECT aggregate intermediate at index 1.
      builder.setColumn(0, (int) row[0]);
      builder.setColumn(1, aggFunction.serializeIntermediateResult(row[1]));
      builder.finishRow();
    }
    return builder.build();
  }

  private static DataTable buildObjectRow(DataSchema schema, AggregationFunction aggFunction, Object intermediate)
      throws IOException {
    DataTableBuilder builder = DataTableBuilderFactory.getDataTableBuilder(schema);
    builder.startRow();
    builder.setColumn(0, aggFunction.serializeIntermediateResult(intermediate));
    builder.finishRow();
    return builder.build();
  }

  /** Builds a single-row, single-DOUBLE-column intermediate DataTable with null-handling encoding. */
  private static DataTable buildNullableDouble(DataSchema schema, Double value)
      throws IOException {
    DataTableBuilder builder = DataTableBuilderFactory.getDataTableBuilder(schema);
    RoaringBitmap nullBitmap = new RoaringBitmap();
    builder.startRow();
    if (value == null) {
      builder.setColumn(0, ((Number) ColumnDataType.DOUBLE.getNullPlaceholder()).doubleValue());
      nullBitmap.add(0);
    } else {
      builder.setColumn(0, (double) value);
    }
    builder.finishRow();
    builder.setNullRowIds(nullBitmap);
    return builder.build();
  }

  private static void appendRow(DataTableBuilder builder, DataSchema schema, Object[] values)
      throws IOException {
    builder.startRow();
    ColumnDataType[] columnDataTypes = schema.getColumnDataTypes();
    for (int i = 0; i < values.length; i++) {
      switch (columnDataTypes[i]) {
        case INT:
          builder.setColumn(i, (int) values[i]);
          break;
        case LONG:
          builder.setColumn(i, (long) values[i]);
          break;
        case DOUBLE:
          builder.setColumn(i, (double) values[i]);
          break;
        case STRING:
          builder.setColumn(i, (String) values[i]);
          break;
        default:
          throw new IllegalArgumentException("Unsupported test column type: " + columnDataTypes[i]);
      }
    }
    builder.finishRow();
  }

  private static Map<ServerRoutingInstance, DataTable> toMap(List<DataTable> serverTables) {
    Map<ServerRoutingInstance, DataTable> map = new HashMap<>();
    for (int i = 0; i < serverTables.size(); i++) {
      map.put(new ServerRoutingInstance("localhost", 1000 + i, TableType.OFFLINE), serverTables.get(i));
    }
    return map;
  }

  private static Map<ServerRoutingInstance, DataTable> singletonMap(DataTable dataTable) {
    Map<ServerRoutingInstance, DataTable> map = new HashMap<>();
    map.put(new ServerRoutingInstance("Server_merge", 0, TableType.OFFLINE), dataTable);
    return map;
  }

  /**
   * Asserts the two result tables have the same schema and the same set of rows (order-independent, so
   * the comparison is robust for unordered group-by / distinct results).
   */
  private static void assertResultTablesEquivalent(ResultTable expected, ResultTable actual) {
    assertNotNull(expected);
    assertNotNull(actual);
    assertEquals(actual.getDataSchema(), expected.getDataSchema());
    assertEquals(actual.getRows().size(), expected.getRows().size());
    assertTrue(sortedRows(actual).equals(sortedRows(expected)),
        "rows differ:\n expected=" + sortedRows(expected) + "\n actual  =" + sortedRows(actual));
  }

  private static List<String> sortedRows(ResultTable resultTable) {
    List<String> rows = new ArrayList<>();
    for (Object[] row : resultTable.getRows()) {
      rows.add(Arrays.deepToString(row));
    }
    return rows.stream().sorted().collect(Collectors.toList());
  }
}
