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
package org.apache.pinot.core.plan.maker;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.core.query.executor.ResultsBlockStreamer;
import org.apache.pinot.core.query.optimizer.QueryOptimizer;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.spi.FetchContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.TerminationException;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.rewriter.RlsUtils;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Tests for server-config versus query-option resolution in [InstancePlanMakerImplV2]: execution threads
/// (`max.execution.threads`, `default.execution.threads`) and the streaming selection ORDER BY merge defaults.
public class InstancePlanMakerImplV2Test {

  private static final String BASE_QUERY = "SELECT * FROM testTable";
  private static final String SELECTION_ORDER_BY_QUERY = "SELECT col FROM testTable ORDER BY col LIMIT 10";

  private static QueryContext buildQueryContext(String maxExecutionThreadsOption) {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(BASE_QUERY);
    if (maxExecutionThreadsOption != null) {
      Map<String, String> queryOptions = queryContext.getQueryOptions();
      queryOptions.put(QueryOptionKey.MAX_EXECUTION_THREADS, maxExecutionThreadsOption);
    }
    return queryContext;
  }

  @Test
  public void testDefaultNotSetFallsBackToMax() {
    InstancePlanMakerImplV2 planMaker = new InstancePlanMakerImplV2();
    planMaker.setMaxExecutionThreads(12);

    QueryContext queryContext = buildQueryContext(null);
    planMaker.applyQueryOptions(queryContext);

    assertEquals(queryContext.getMaxExecutionThreads(), 12);
  }

  @Test
  public void testDefaultExecutionThreadsUsedWhenSet() {
    InstancePlanMakerImplV2 planMaker = new InstancePlanMakerImplV2();
    planMaker.setMaxExecutionThreads(16);
    planMaker.setDefaultExecutionThreads(4);

    QueryContext queryContext = buildQueryContext(null);
    planMaker.applyQueryOptions(queryContext);

    assertEquals(queryContext.getMaxExecutionThreads(), 4);
  }

  @Test
  public void testQueryOverrideOverridesDefault() {
    InstancePlanMakerImplV2 planMaker = new InstancePlanMakerImplV2();
    planMaker.setMaxExecutionThreads(16);
    planMaker.setDefaultExecutionThreads(4);

    QueryContext queryContext = buildQueryContext("10");
    planMaker.applyQueryOptions(queryContext);

    assertEquals(queryContext.getMaxExecutionThreads(), 10);
  }

  @Test
  public void testQueryOverrideCappedByMax() {
    InstancePlanMakerImplV2 planMaker = new InstancePlanMakerImplV2();
    planMaker.setMaxExecutionThreads(8);
    planMaker.setDefaultExecutionThreads(4);

    QueryContext queryContext = buildQueryContext("20");
    planMaker.applyQueryOptions(queryContext);

    assertEquals(queryContext.getMaxExecutionThreads(), 8);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testInitRejectsDefaultExceedingMax() {
    InstancePlanMakerImplV2 planMaker = new InstancePlanMakerImplV2();
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(Server.MAX_EXECUTION_THREADS, 4);
    config.setProperty(Server.DEFAULT_EXECUTION_THREADS, 8);
    planMaker.init(config);
  }

  @Test
  public void testRowFilterExpressionOverrideRefreshesColumns() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable")
        .addSingleValueDimension("rawTenant", DataType.STRING)
        .addSingleValueDimension("tenantLower", DataType.STRING)
        .build();
    QueryContext queryContext = buildRowFilterQueryContext(
        "SELECT COUNT(*) FROM testTable WHERE tenantLower = 'o''hara'", "lower(rawTenant) = 'o''hara'", schema);
    queryContext.setRowFilterExpressionOverrideMap(Map.of(CalciteSqlParser.compileToExpression("lower(rawTenant)"),
        CalciteSqlParser.compileToExpression("tenantLower")));
    assertEquals(queryContext.getColumns(), Set.of("rawTenant", "tenantLower"));

    new InstancePlanMakerImplV2().applyQueryOptions(queryContext);

    assertEquals(queryContext.getRowFilter(),
        RequestContextUtils.getFilter(CalciteSqlParser.compileToExpression("tenantLower = 'o''hara'")));
    assertEquals(queryContext.getColumns(), Set.of("tenantLower"));
  }

  @Test
  public void testRowFilterExpressionOverridesDoNotChainAcrossPlanningCalls() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable")
        .addSingleValueDimension("rawTenant", DataType.STRING)
        .addSingleValueDimension("tenantLower", DataType.STRING)
        .addSingleValueDimension("otherTenant", DataType.STRING)
        .build();
    QueryContext queryContext = buildRowFilterQueryContext(
        "SELECT COUNT(*) FROM testTable WHERE tenantLower = 'a'", "lower(rawTenant) = 'a'", schema);
    Expression originalPolicy = queryContext.getRowFilterExpression().deepCopy();
    Expression replacement = CalciteSqlParser.compileToExpression("tenantLower");
    Map<Expression, Expression> overrides = Map.of(CalciteSqlParser.compileToExpression("lower(rawTenant)"),
        replacement, CalciteSqlParser.compileToExpression("tenantLower"),
        CalciteSqlParser.compileToExpression("otherTenant"));
    queryContext.setRowFilterExpressionOverrideMap(overrides);
    InstancePlanMakerImplV2 planMaker = new InstancePlanMakerImplV2();

    for (int i = 0; i < 2; i++) {
      planMaker.applyQueryOptions(queryContext);
      assertEquals(queryContext.getRowFilter(),
          RequestContextUtils.getFilter(CalciteSqlParser.compileToExpression("tenantLower = 'a'")));
      assertEquals(queryContext.getColumns(), Set.of("tenantLower"));
      assertEquals(queryContext.getRowFilterExpression(), originalPolicy);
      assertEquals(replacement, CalciteSqlParser.compileToExpression("tenantLower"));
    }
  }

  @Test
  public void testRowFilterIgnoresFullPredicateOverridesAndNormalizesNumericLiterals() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable")
        .addSingleValueDimension("rawTenantId", DataType.INT)
        .addSingleValueDimension("tenantId", DataType.INT)
        .build();
    QueryContext queryContext = buildRowFilterQueryContext(
        "SELECT COUNT(*) FROM testTable WHERE rawTenantId = 2", "rawTenantId = 2.0", schema);
    Expression replacement = CalciteSqlParser.compileToExpression("tenantId = 3.0");
    // Use the exact optimized AST so a whole-predicate rewrite would match, even after numeric normalization.
    Expression optimizedPolicy = new QueryOptimizer().optimizeFilter(
        CalciteSqlParser.compileToExpression("rawTenantId = 2.0"), schema);
    queryContext.setRowFilterExpressionOverrideMap(Map.of(
        CalciteSqlParser.compileToExpression("rawTenantId = 2.0"), CalciteSqlParser.compileToExpression("FALSE"),
        optimizedPolicy, replacement));
    InstancePlanMakerImplV2 planMaker = new InstancePlanMakerImplV2();

    for (int i = 0; i < 2; i++) {
      planMaker.applyQueryOptions(queryContext);
      assertEquals(queryContext.getRowFilter(),
          RequestContextUtils.getFilter(CalciteSqlParser.compileToExpression("rawTenantId = 2")));
      assertEquals(queryContext.getColumns(), Set.of("rawTenantId"));
      assertEquals(queryContext.getRowFilterExpression(), CalciteSqlParser.compileToExpression("rawTenantId = 2.0"));
      assertEquals(replacement, CalciteSqlParser.compileToExpression("tenantId = 3.0"));
    }
  }

  @Test
  public void testRowFilterIgnoresOverridesOfPolicyBooleanStructure() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable")
        .addSingleValueDimension("tenant", DataType.STRING)
        .addSingleValueDimension("region", DataType.STRING)
        .build();
    String policy = "tenant = 'a' AND region = 'west'";
    QueryContext queryContext = buildRowFilterQueryContext(
        "SELECT COUNT(*) FROM testTable WHERE tenant = 'a' AND region = 'west' AND region <> 'east'", policy, schema);
    Expression matchAll = CalciteSqlParser.compileToExpression("TRUE");
    queryContext.setRowFilterExpressionOverrideMap(Map.of(
        CalciteSqlParser.compileToExpression(policy), matchAll,
        CalciteSqlParser.compileToExpression("tenant = 'a'"), matchAll,
        CalciteSqlParser.compileToExpression("region = 'west'"), matchAll));
    InstancePlanMakerImplV2 planMaker = new InstancePlanMakerImplV2();

    for (int i = 0; i < 2; i++) {
      planMaker.applyQueryOptions(queryContext);
      assertEquals(queryContext.getRowFilter(),
          RequestContextUtils.getFilter(CalciteSqlParser.compileToExpression(policy)));
      assertFalse(queryContext.getRowFilter().isConstantTrue());
      assertEquals(queryContext.getColumns(), Set.of("tenant", "region"));
      assertEquals(queryContext.getRowFilterExpression(), CalciteSqlParser.compileToExpression(policy));
    }
  }

  @Test
  public void testRowFilterTimestampHintAppliesAfterExpressionOverride() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable")
        .addSingleValueDimension("rawTime", DataType.LONG)
        .addSingleValueDimension("eventTime", DataType.LONG)
        .addSingleValueDimension("eventDay", DataType.LONG)
        .build();
    Expression dayExpression = CalciteSqlParser.compileToExpression("DATETRUNC('DAY', eventTime)");
    // NOT_EQUALS keeps DATETRUNC intact instead of letting the time-predicate optimizer turn it into a range.
    for (boolean streaming : new boolean[]{false, true}) {
      for (boolean hasDayColumn : new boolean[]{false, true}) {
        PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(
            "SELECT COUNT(*) FROM testTable WHERE DATETRUNC('DAY', eventTime) <> 172800000");
        pinotQuery.putToQueryOptions(RlsUtils.buildRlsFilterKey("testTable"), "rawTime <> 172800000");
        pinotQuery.setExpressionOverrideHints(
            Map.of(dayExpression, CalciteSqlParser.compileToExpression("eventDay")));
        QueryContext queryContext = QueryContextConverterUtils.getQueryContext(pinotQuery);
        queryContext.setSchema(schema);
        queryContext.setRowFilterExpressionOverrideMap(
            Map.of(CalciteSqlParser.compileToExpression("rawTime"), dayExpression));
        IndexSegment segment = mock(IndexSegment.class);
        when(segment.getColumnNames()).thenReturn(
            hasDayColumn ? Set.of("eventTime", "eventDay") : Set.of("eventTime"));
        List<SegmentContext> segments = List.of(new SegmentContext(segment));
        InstancePlanMakerImplV2 planMaker = new InstancePlanMakerImplV2();

        for (int i = 0; i < 2; i++) {
          if (streaming) {
            planMaker.makeStreamingInstancePlan(segments, queryContext, null, mock(ResultsBlockStreamer.class));
          } else {
            planMaker.makeInstancePlan(segments, queryContext, null);
          }
          String expectedFilter = hasDayColumn ? "eventDay <> 172800000"
              : "DATETRUNC('DAY', eventTime) <> 172800000";
          assertEquals(queryContext.getRowFilter(),
              RequestContextUtils.getFilter(CalciteSqlParser.compileToExpression(expectedFilter)));
          assertEquals(queryContext.getRowFilter(), queryContext.getFilter());
          assertEquals(queryContext.getColumns(), hasDayColumn ? Set.of("eventDay") : Set.of("eventTime"));
        }
      }
    }
  }

  private static QueryContext buildRowFilterQueryContext(String query, String rowFilter, Schema schema) {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    pinotQuery.putToQueryOptions(RlsUtils.buildRlsFilterKey("testTable"), rowFilter);
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(pinotQuery);
    queryContext.setSchema(schema);
    return queryContext;
  }

  @Test
  public void testGroupingSetsBoundIncludesEverySetAndGrandTotal() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT a, b, COUNT(*) FROM t GROUP BY ROLLUP(a, b)");
    SegmentContext segment = segmentWithTwoColumns(2, 2);

    // Two union columns have at most 4 BASE groups, but ROLLUP can produce 4 + 2 + 1 derived groups.
    queryContext.setNumGroupsLimit(6);
    assertFalse(InstancePlanMakerImplV2.fitsGroupingSetsBaseAggregationLimit(List.of(segment), queryContext));
    queryContext.setNumGroupsLimit(7);
    assertTrue(InstancePlanMakerImplV2.fitsGroupingSetsBaseAggregationLimit(List.of(segment), queryContext));

    // The bounds add across segments with DISJOINT value spaces, even when each segment fits by itself: with
    // unions of 4 values per column, ROLLUP can produce 16 + 4 + 1 derived groups, but the per-segment products
    // (summing to 8 base and 14 derived) prove a limit of 14.
    SegmentContext other = segmentWithTwoColumns(2, 2, false, "other");
    queryContext.setNumGroupsLimit(13);
    assertFalse(InstancePlanMakerImplV2.fitsGroupingSetsBaseAggregationLimit(List.of(segment, other), queryContext));
    queryContext.setNumGroupsLimit(14);
    assertTrue(InstancePlanMakerImplV2.fitsGroupingSetsBaseAggregationLimit(List.of(segment, other), queryContext));
  }

  @Test
  public void testUnionBoundAdmitsSegmentsSharingValues() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT a, b, COUNT(*) FROM t GROUP BY ROLLUP(a, b)");
    // Ten segments over the SAME value space: per-segment products sum to 40 base and 70 derived groups, but the
    // per-column unions prove the real bound of 4 base and 7 derived groups.
    SegmentContext segment = segmentWithTwoColumns(2, 2);
    List<SegmentContext> segments = List.of(segment, segment, segment, segment, segment, segment, segment, segment,
        segment, segment);
    queryContext.setNumGroupsLimit(7);
    assertTrue(InstancePlanMakerImplV2.fitsGroupingSetsBaseAggregationLimit(segments, queryContext));
    queryContext.setNumGroupsLimit(6);
    assertFalse(InstancePlanMakerImplV2.fitsGroupingSetsBaseAggregationLimit(segments, queryContext));

    // Nullable union columns add a distinct null key: (2 + 1)^2 + (2 + 1) + 1 = 13 derived groups.
    queryContext.setNullHandlingEnabled(true);
    segments = List.of(segmentWithTwoColumns(1, 1), segmentWithTwoColumns(1, 1, false, "other"));
    for (SegmentContext nullableSegment : segments) {
      IndexSegment indexSegment = nullableSegment.getIndexSegment();
      when(indexSegment.getDataSourceNullable("a").getNullValueVector()).thenReturn(mock(NullValueVectorReader.class));
      when(indexSegment.getDataSourceNullable("b").getNullValueVector()).thenReturn(mock(NullValueVectorReader.class));
    }
    queryContext.setNumGroupsLimit(13);
    assertTrue(InstancePlanMakerImplV2.fitsGroupingSetsBaseAggregationLimit(segments, queryContext));
    queryContext.setNumGroupsLimit(12);
    assertFalse(InstancePlanMakerImplV2.fitsGroupingSetsBaseAggregationLimit(segments, queryContext));
  }

  @Test
  public void testImpossibleUnionBoundSkipsDictionaryScan() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT a, b, COUNT(*) FROM t GROUP BY ROLLUP(a, b)");
    queryContext.setNumGroupsLimit(6);
    SegmentContext segmentContext = segmentWithTwoColumns(2, 2);
    assertFalse(InstancePlanMakerImplV2.fitsGroupingSetsBaseAggregationLimit(List.of(segmentContext), queryContext));
    IndexSegment segment = segmentContext.getIndexSegment();
    verify(segment, never()).acquire(any());
    verify(segment.getDataSourceNullable("a").getDictionary(), never()).getInternal(anyInt());
  }

  @Test
  public void testUnionScanBudgetIsIndependentOfGroupLimit() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT a, b, COUNT(*) FROM t GROUP BY ROLLUP(a, b)");
    queryContext.setNumGroupsLimit(1_000_000);
    SegmentContext segmentContext = segmentWithTwoColumns(4000, 1);
    // The union proof could fit, but scanning 1.2M entries across all columns exceeds the fixed planning budget.
    assertFalse(InstancePlanMakerImplV2.fitsGroupingSetsBaseAggregationLimit(
        Collections.nCopies(300, segmentContext), queryContext));
    verify(segmentContext.getIndexSegment(), never()).acquire(any());
  }

  @Test
  public void testUnionScanReleasesDictionaryOnCancellation() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT a, b, COUNT(*) FROM t GROUP BY ROLLUP(a, b)");
    queryContext.setNumGroupsLimit(18_001);
    queryContext.setEndTimeMs(Long.MAX_VALUE);
    SegmentContext segmentContext = segmentWithTwoColumns(9000, 1);
    IndexSegment segment = segmentContext.getIndexSegment();
    Dictionary dictionary = segment.getDataSourceNullable("a").getDictionary();
    try (QueryThreadContext threadContext = QueryThreadContext.openForSseTest()) {
      doAnswer(invocation -> {
        threadContext.getExecutionContext().terminate(QueryErrorCode.QUERY_CANCELLATION, "cancel dictionary scan");
        return "a:1";
      }).when(dictionary).getInternal(1);
      assertThrows(TerminationException.class, () -> InstancePlanMakerImplV2.fitsGroupingSetsBaseAggregationLimit(
          List.of(segmentContext, segmentContext), queryContext));
    }
    verify(dictionary, never()).getInternal(QueryThreadContext.CHECK_TERMINATION_AND_SAMPLE_USAGE_RECORD_MASK + 1);
    verify(segment).release(any());
  }

  @Test
  public void testOrderedGroupingSetsFallbackWhenDerivedGroupsExceedLimit() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT a, b, COUNT(*) FROM t GROUP BY ROLLUP(a, b) ORDER BY COUNT(*) DESC LIMIT 5");
    queryContext.getQueryOptions().put(QueryOptionKey.GROUPING_SETS_BASE_AGGREGATION, "true");
    SegmentContext segment = segmentWithTwoColumns(2, 2);

    queryContext.setNumGroupsLimit(7);
    InstancePlanMakerImplV2.boundGroupingSetsBaseAggregation(List.of(segment), queryContext);
    assertTrue(queryContext.isGroupingSetsBaseAggregation(), "all seven derived groups fit");

    queryContext.setNumGroupsLimit(6);
    InstancePlanMakerImplV2.boundGroupingSetsBaseAggregation(List.of(segment), queryContext);
    assertFalse(queryContext.isGroupingSetsBaseAggregation());
  }

  @Test
  public void testMutableSegmentDisablesBaseAggregation() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT a, b, COUNT(*) FROM t GROUP BY ROLLUP(a, b)");
    queryContext.setNumGroupsLimit(1000);

    // The mutable segment's current cardinalities fit with plenty of headroom, but new keys indexed between
    // planning and execution could still push the base groups past the limit, so the proof must fail.
    SegmentContext mutableSegment = segmentWithTwoColumns(2, 2, true, "");
    assertFalse(
        InstancePlanMakerImplV2.fitsGroupingSetsBaseAggregationLimit(List.of(mutableSegment), queryContext));

    // One mutable segment disables the proof for the whole query, even next to immutable segments.
    SegmentContext immutableSegment = segmentWithTwoColumns(2, 2);
    assertTrue(
        InstancePlanMakerImplV2.fitsGroupingSetsBaseAggregationLimit(List.of(immutableSegment), queryContext));
    assertFalse(InstancePlanMakerImplV2.fitsGroupingSetsBaseAggregationLimit(
        List.of(immutableSegment, mutableSegment), queryContext));
  }

  private static SegmentContext segmentWithTwoColumns(int aCardinality, int bCardinality) {
    return segmentWithTwoColumns(aCardinality, bCardinality, false, "");
  }

  /// `valuePrefix` namespaces the mocked dictionary values, so segments built with the same prefix share a value
  /// space (their unions dedup) and segments built with different prefixes are disjoint.
  private static SegmentContext segmentWithTwoColumns(int aCardinality, int bCardinality, boolean mutable,
      String valuePrefix) {
    SegmentContext segmentContext = mock(SegmentContext.class);
    IndexSegment segment = mutable ? mock(MutableSegment.class) : mock(IndexSegment.class);
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(segmentContext.getIndexSegment()).thenReturn(segment);
    when(segment.getSegmentMetadata()).thenReturn(segmentMetadata);
    when(segmentMetadata.getTotalDocs()).thenReturn(10);
    Set<String> acquiredColumns = new HashSet<>();
    Set<UUID> prefetchedFetches = new HashSet<>();
    doAnswer(invocation -> {
      FetchContext fetchContext = invocation.getArgument(0);
      prefetchedFetches.add(fetchContext.getFetchId());
      return null;
    }).when(segment).prefetch(any());
    doAnswer(invocation -> {
      FetchContext fetchContext = invocation.getArgument(0);
      assertTrue(prefetchedFetches.contains(fetchContext.getFetchId()),
          "prefetch must register the fetch before acquire");
      acquiredColumns.addAll(fetchContext.getColumnToIndexList().keySet());
      return null;
    }).when(segment).acquire(any());
    doAnswer(invocation -> {
      FetchContext fetchContext = invocation.getArgument(0);
      assertTrue(acquiredColumns.removeAll(fetchContext.getColumnToIndexList().keySet()));
      assertTrue(prefetchedFetches.remove(fetchContext.getFetchId()));
      return null;
    }).when(segment).release(any());
    DataSource a = dataSource(aCardinality, valuePrefix + "a", "a", acquiredColumns);
    DataSource b = dataSource(bCardinality, valuePrefix + "b", "b", acquiredColumns);
    when(segment.getDataSourceNullable("a")).thenReturn(a);
    when(segment.getDataSourceNullable("b")).thenReturn(b);
    return segmentContext;
  }

  private static DataSource dataSource(int cardinality, String valuePrefix, String column,
      Set<String> acquiredColumns) {
    DataSource dataSource = mock(DataSource.class);
    DataSourceMetadata metadata = mock(DataSourceMetadata.class);
    Dictionary dictionary = mock(Dictionary.class);
    ForwardIndexReader<?> forwardIndex = mock(ForwardIndexReader.class);
    when(dataSource.getDataSourceMetadata()).thenReturn(metadata);
    when(metadata.isSingleValue()).thenReturn(true);
    when(dataSource.getDictionary()).thenReturn(dictionary);
    when(dictionary.length()).thenReturn(cardinality);
    when(dictionary.getInternal(anyInt())).thenAnswer(invocation -> {
      assertTrue(acquiredColumns.contains(column), "dictionary buffers must be acquired before reading values");
      return valuePrefix + ":" + invocation.getArgument(0, Integer.class);
    });
    doReturn(forwardIndex).when(dataSource).getForwardIndex();
    when(forwardIndex.isDictionaryEncoded()).thenReturn(true);
    return dataSource;
  }

  @Test
  public void testSortedSelectionMergeDefaultsWithoutServerConfig() {
    InstancePlanMakerImplV2 planMaker = new InstancePlanMakerImplV2();
    planMaker.init(new PinotConfiguration());

    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(SELECTION_ORDER_BY_QUERY);
    planMaker.applyQueryOptions(queryContext);

    assertEquals(queryContext.getSortedSelectionMergeAutoMinSortedRatio(),
        Server.DEFAULT_SORTED_SELECTION_MERGE_AUTO_MIN_SORTED_RATIO);
    assertEquals(queryContext.getSortedSelectionMergeBlockSize(), Server.DEFAULT_SORTED_SELECTION_MERGE_BLOCK_SIZE);
  }

  @Test
  public void testSortedSelectionMergeServerConfigAppliesWithoutQueryOption() {
    InstancePlanMakerImplV2 planMaker = new InstancePlanMakerImplV2();
    planMaker.init(sortedSelectionMergeConfig(0.6, 2_500));

    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(SELECTION_ORDER_BY_QUERY);
    planMaker.applyQueryOptions(queryContext);

    assertEquals(queryContext.getSortedSelectionMergeAutoMinSortedRatio(), 0.6);
    assertEquals(queryContext.getSortedSelectionMergeBlockSize(), 2_500);
  }

  @Test
  public void testSortedSelectionMergeQueryOptionOverridesServerConfig() {
    InstancePlanMakerImplV2 planMaker = new InstancePlanMakerImplV2();
    planMaker.init(sortedSelectionMergeConfig(0.6, 2_500));

    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(SELECTION_ORDER_BY_QUERY);
    Map<String, String> queryOptions = queryContext.getQueryOptions();
    queryOptions.put(QueryOptionKey.SORTED_SELECTION_MERGE_AUTO_MIN_SORTED_RATIO, "0.9");
    queryOptions.put(QueryOptionKey.SORTED_SELECTION_MERGE_BLOCK_SIZE, "500");
    planMaker.applyQueryOptions(queryContext);

    assertEquals(queryContext.getSortedSelectionMergeAutoMinSortedRatio(), 0.9);
    assertEquals(queryContext.getSortedSelectionMergeBlockSize(), 500);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testInitRejectsSortedSelectionMergeRatioAboveOne() {
    new InstancePlanMakerImplV2().init(sortedSelectionMergeConfig(1.5, 2_500));
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testInitRejectsNegativeSortedSelectionMergeRatio() {
    new InstancePlanMakerImplV2().init(sortedSelectionMergeConfig(-0.1, 2_500));
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testInitRejectsNonPositiveSortedSelectionMergeBlockSize() {
    new InstancePlanMakerImplV2().init(sortedSelectionMergeConfig(0.6, 0));
  }

  private static PinotConfiguration sortedSelectionMergeConfig(double autoMinSortedRatio, int blockSize) {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(Server.SORTED_SELECTION_MERGE_AUTO_MIN_SORTED_RATIO, autoMinSortedRatio);
    config.setProperty(Server.SORTED_SELECTION_MERGE_BLOCK_SIZE, blockSize);
    return config;
  }
}
