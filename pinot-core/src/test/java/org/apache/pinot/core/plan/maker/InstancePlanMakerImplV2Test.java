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

import java.util.List;
import java.util.Map;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Tests for execution-thread resolution in [InstancePlanMakerImplV2], covering the interplay
/// between `max.execution.threads`, `default.execution.threads`, and per-query overrides.
public class InstancePlanMakerImplV2Test {

  private static final String BASE_QUERY = "SELECT * FROM testTable";

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
  public void testGroupingSetsBoundIncludesEverySetAndGrandTotal() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT a, b, COUNT(*) FROM t GROUP BY ROLLUP(a, b)");
    SegmentContext segment = segmentWithTwoColumns(2, 2);

    // Two union columns have at most 4 BASE groups, but ROLLUP can produce 4 + 2 + 1 derived groups.
    queryContext.setNumGroupsLimit(6);
    assertFalse(InstancePlanMakerImplV2.fitsGroupingSetsBaseAggregationLimit(List.of(segment), queryContext));
    queryContext.setNumGroupsLimit(7);
    assertTrue(InstancePlanMakerImplV2.fitsGroupingSetsBaseAggregationLimit(List.of(segment), queryContext));

    // The bounds add across segments, even when each segment fits by itself.
    queryContext.setNumGroupsLimit(13);
    assertFalse(InstancePlanMakerImplV2.fitsGroupingSetsBaseAggregationLimit(List.of(segment, segment), queryContext));
    queryContext.setNumGroupsLimit(14);
    assertTrue(InstancePlanMakerImplV2.fitsGroupingSetsBaseAggregationLimit(List.of(segment, segment), queryContext));
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
    SegmentContext mutableSegment = segmentWithTwoColumns(2, 2, true);
    assertFalse(
        InstancePlanMakerImplV2.fitsGroupingSetsBaseAggregationLimit(List.of(mutableSegment), queryContext));

    // One mutable segment disables the proof for the whole query, even next to immutable segments.
    SegmentContext immutableSegment = segmentWithTwoColumns(2, 2, false);
    assertTrue(
        InstancePlanMakerImplV2.fitsGroupingSetsBaseAggregationLimit(List.of(immutableSegment), queryContext));
    assertFalse(InstancePlanMakerImplV2.fitsGroupingSetsBaseAggregationLimit(
        List.of(immutableSegment, mutableSegment), queryContext));
  }

  private static SegmentContext segmentWithTwoColumns(int aCardinality, int bCardinality) {
    return segmentWithTwoColumns(aCardinality, bCardinality, false);
  }

  private static SegmentContext segmentWithTwoColumns(int aCardinality, int bCardinality, boolean mutable) {
    SegmentContext segmentContext = mock(SegmentContext.class);
    IndexSegment segment = mutable ? mock(MutableSegment.class) : mock(IndexSegment.class);
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(segmentContext.getIndexSegment()).thenReturn(segment);
    when(segment.getSegmentMetadata()).thenReturn(segmentMetadata);
    when(segmentMetadata.getTotalDocs()).thenReturn(10);
    DataSource a = dataSource(aCardinality);
    DataSource b = dataSource(bCardinality);
    when(segment.getDataSourceNullable("a")).thenReturn(a);
    when(segment.getDataSourceNullable("b")).thenReturn(b);
    return segmentContext;
  }

  private static DataSource dataSource(int cardinality) {
    DataSource dataSource = mock(DataSource.class);
    DataSourceMetadata metadata = mock(DataSourceMetadata.class);
    Dictionary dictionary = mock(Dictionary.class);
    ForwardIndexReader<?> forwardIndex = mock(ForwardIndexReader.class);
    when(dataSource.getDataSourceMetadata()).thenReturn(metadata);
    when(metadata.isSingleValue()).thenReturn(true);
    when(dataSource.getDictionary()).thenReturn(dictionary);
    when(dictionary.length()).thenReturn(cardinality);
    doReturn(forwardIndex).when(dataSource).getForwardIndex();
    when(forwardIndex.isDictionaryEncoded()).thenReturn(true);
    return dataSource;
  }
}
