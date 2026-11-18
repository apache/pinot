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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.core.operator.docidsets.AndRestrictionPushdown;
import org.apache.pinot.core.operator.docidsets.AndRestrictionPushdown.Strategy;
import org.apache.pinot.core.query.executor.ResultsBlockStreamer;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.rewriter.RlsUtils;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


/// Tests how [InstancePlanMakerImplV2] resolves [Server.AndRestrictionPushdownMode] into the per-query
/// [AndRestrictionPushdown] read by the AND filter operators.
///
/// Without RLS, `AUTO` enables eager push-down for aggregation, and `STREAMING` opts selections into streaming.
public class AndRestrictionPushdownModeTest {
  private static final String SELECTION_ONLY = "SELECT col1 FROM testTable WHERE col2 = 1";
  private static final String SELECTION_ORDER_BY = "SELECT col1 FROM testTable WHERE col2 = 1 ORDER BY col1";
  private static final String AGGREGATION = "SELECT SUM(col1) FROM testTable WHERE col2 = 1";
  private static final String GROUP_BY = "SELECT col1, SUM(col3) FROM testTable WHERE col2 = 1 GROUP BY col1";
  private static final String DISTINCT = "SELECT DISTINCT col1 FROM testTable WHERE col2 = 1";

  @Test
  public void testDefaultIsNever() {
    assertEquals(resolve(null, AGGREGATION).getStrategy(), Strategy.OFF, "The push-down ships disabled");
    assertEquals(resolve(null, SELECTION_ONLY).getStrategy(), Strategy.OFF);
  }

  @Test
  public void testAutoLeavesNonRlsSelectionsOff() {
    assertEquals(resolve("auto", SELECTION_ONLY + " LIMIT 20 OFFSET 5").getStrategy(), Strategy.OFF);
  }

  @Test
  public void testAutoSkipsQueriesThatMayNotStopEarly() {
    // The LIMIT of a DISTINCT counts values, not rows, so it often reads the whole filter
    assertEquals(resolve("auto", DISTINCT).getStrategy(), Strategy.OFF);
    // Whether an ORDER BY selection can stop early is decided per segment, so AUTO excludes it conservatively
    assertEquals(resolve("auto", SELECTION_ORDER_BY).getStrategy(), Strategy.OFF);
    assertEquals(resolve("auto", AGGREGATION + " LIMIT 0").getStrategy(), Strategy.OFF, "LIMIT 0 reads nothing");
    assertEquals(resolve("auto", SELECTION_ONLY + " LIMIT 0").getStrategy(), Strategy.OFF, "LIMIT 0 reads nothing");
  }

  @Test
  public void testAutoPushesDownEagerlyForQueriesThatReadEveryMatchingDocument() {
    assertEquals(resolve("auto", AGGREGATION).getStrategy(), Strategy.EAGER);
    assertEquals(resolve("auto", GROUP_BY).getStrategy(), Strategy.EAGER);
  }

  @Test
  public void testAlwaysIsEagerEvenForQueriesThatCanStopEarly() {
    assertEquals(resolve("always", SELECTION_ONLY).getStrategy(), Strategy.EAGER);
    assertEquals(resolve("always", DISTINCT).getStrategy(), Strategy.EAGER);
  }

  @Test
  public void testExplicitStreamingRequiresUnorderedLimitSelection() {
    assertEquals(resolve("streaming", SELECTION_ONLY + " LIMIT 10").getStrategy(), Strategy.STREAMING);
    assertEquals(resolve("streaming", SELECTION_ONLY + " AND (col3 = 1 OR col4 = 1) LIMIT 10").getStrategy(),
        Strategy.STREAMING);
    for (String query : new String[]{AGGREGATION, GROUP_BY, DISTINCT, SELECTION_ORDER_BY,
        SELECTION_ONLY + " LIMIT 0", SELECTION_ONLY + " LIMIT " + Integer.MAX_VALUE}) {
      assertEquals(resolve("streaming", query).getStrategy(), Strategy.EAGER, query);
    }
  }

  @Test
  public void testNeverSkipsEvenAggregationQuery() {
    assertEquals(resolve("never", AGGREGATION).getStrategy(), Strategy.OFF);
  }

  /// A query with a row-level security filter always pushes down. NEVER and AUTO pick the strategy by query shape:
  /// eager for every query that is not a selection-only query with a LIMIT.
  @Test
  public void testRowLevelSecurityNeverRunsWithoutPushdown() {
    for (String mode : new String[]{null, "never", "auto"}) {
      assertEquals(resolveWithRowFilter(mode, SELECTION_ONLY).getStrategy(), Strategy.STREAMING, "Mode: " + mode);
      assertEquals(resolveWithRowFilter(mode, AGGREGATION).getStrategy(), Strategy.EAGER, "Mode: " + mode);
      assertEquals(resolveWithRowFilter(mode, DISTINCT).getStrategy(), Strategy.EAGER, "Mode: " + mode);
      assertEquals(resolveWithRowFilter(mode, SELECTION_ORDER_BY).getStrategy(), Strategy.EAGER, "Mode: " + mode);
      // The multi-stage leaf runs its selection with an unbounded limit, so it reads the whole filter
      assertEquals(resolveWithRowFilter(mode, SELECTION_ONLY + " LIMIT " + Integer.MAX_VALUE).getStrategy(),
          Strategy.EAGER, "Mode: " + mode);
    }
    assertEquals(resolveWithRowFilter("always", SELECTION_ONLY).getStrategy(), Strategy.EAGER);
  }

  @Test
  public void testRowLevelSecurityStreamingRequiresSupportedSelection() {
    String filterWithPolicy = "SELECT col1 FROM testTable WHERE col3 = 1 AND col2 = 1 LIMIT 10 OFFSET 5";
    for (String mode : new String[]{null, "never", "auto", "streaming"}) {
      assertEquals(resolveWithRowFilter(mode, SELECTION_ONLY + " LIMIT 10").getStrategy(), Strategy.STREAMING);
      AndRestrictionPushdown pushdown = resolveWithRowFilter(mode, filterWithPolicy);
      assertEquals(pushdown.getStrategy(), Strategy.STREAMING, "Mode: " + mode);
      assertEquals(pushdown.getMaxDocsToRead(), 15, "Mode: " + mode);
      assertEquals(resolveWithRowFilter(mode, SELECTION_ONLY + " OR col4 = 1 LIMIT 10").getStrategy(), Strategy.EAGER,
          "Mode: " + mode);
      assertEquals(resolveWithRowFilter(mode, SELECTION_ONLY + " AND col4 = 1 LIMIT 10").getStrategy(), Strategy.EAGER,
          "Mode: " + mode);
    }
    assertEquals(resolveWithRowFilter("streaming", AGGREGATION).getStrategy(), Strategy.EAGER);
    assertEquals(resolveWithRowFilter("streaming", SELECTION_ORDER_BY).getStrategy(), Strategy.EAGER);
  }

  @Test
  public void testModeIsCaseInsensitive() {
    assertEquals(resolve("AlWaYs", SELECTION_ONLY).getStrategy(), Strategy.EAGER);
    assertEquals(resolve("AuTo", AGGREGATION).getStrategy(), Strategy.EAGER);
    assertEquals(resolve("StReAmInG", SELECTION_ONLY).getStrategy(), Strategy.STREAMING);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidQueryOptionValueIsRejected() {
    // A boolean is the most likely mistake, since the option name reads like a toggle
    resolve("true", AGGREGATION);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidServerConfigValueIsRejected() {
    planMakerWithServerDefault("sometimes");
  }

  @Test
  public void testServerDefaultAppliesWithoutQueryOption() {
    InstancePlanMakerImplV2 planMaker = planMakerWithServerDefault("always");
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(SELECTION_ONLY);
    planMaker.makeInstancePlan(List.of(), queryContext, mock(ExecutorService.class));
    assertEquals(queryContext.getAndRestrictionPushdown().getStrategy(), Strategy.EAGER,
        "The server default must apply");
  }

  @Test
  public void testQueryOptionOverridesServerDefault() {
    InstancePlanMakerImplV2 planMaker = planMakerWithServerDefault("always");
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(AGGREGATION);
    queryContext.getQueryOptions().put(QueryOptionKey.AND_RESTRICTION_PUSHDOWN_MODE, "never");
    planMaker.makeInstancePlan(List.of(), queryContext, mock(ExecutorService.class));
    assertEquals(queryContext.getAndRestrictionPushdown().getStrategy(), Strategy.OFF,
        "The query option must win over the server default");
  }

  @Test
  public void testRlsEligibilityUsesRewrittenPredicate() {
    PinotQuery pinotQuery =
        CalciteSqlParser.compileToPinotQuery("SELECT col1 FROM testTable WHERE ABS(rawCol3) = 1 LIMIT 10");
    pinotQuery.setQueryOptions(Map.of(RlsUtils.buildRlsFilterKey("testTable"), "col3 = 1"));
    pinotQuery.setExpressionOverrideHints(Map.of(CalciteSqlParser.compileToExpression("ABS(rawCol3)"),
        CalciteSqlParser.compileToExpression("col3")));
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(pinotQuery);
    IndexSegment segment = mock(IndexSegment.class);
    when(segment.getColumnNames()).thenReturn(Set.of("col1", "col3"));

    new InstancePlanMakerImplV2().makeInstancePlan(List.of(new SegmentContext(segment)), queryContext,
        mock(ExecutorService.class));

    assertEquals(queryContext.getFilter(), queryContext.getRowFilter());
    assertEquals(queryContext.getAndRestrictionPushdown().getStrategy(), Strategy.EAGER);
  }

  @Test
  public void testStreamingTransportKeepsEagerFallback() {
    InstancePlanMakerImplV2 planMaker = planMakerWithServerDefault("streaming");
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(SELECTION_ONLY);
    planMaker.makeStreamingInstancePlan(List.of(), queryContext, mock(ExecutorService.class),
        mock(ResultsBlockStreamer.class));
    assertEquals(queryContext.getAndRestrictionPushdown().getStrategy(), Strategy.EAGER);
  }

  private static InstancePlanMakerImplV2 planMakerWithServerDefault(String mode) {
    InstancePlanMakerImplV2 planMaker = new InstancePlanMakerImplV2();
    planMaker.init(new PinotConfiguration(Map.of(Server.AND_RESTRICTION_PUSHDOWN_MODE, mode)));
    return planMaker;
  }

  /// Resolves the push-down for the given query, optionally setting the `andRestrictionPushdownMode` query option.
  /// With no option the server default (NEVER) applies.
  private static AndRestrictionPushdown resolve(@Nullable String mode, String query) {
    return resolve(mode, query, false);
  }

  private static AndRestrictionPushdown resolveWithRowFilter(@Nullable String mode, String query) {
    return resolve(mode, query, true);
  }

  private static AndRestrictionPushdown resolve(@Nullable String mode, String query, boolean withRowFilter) {
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    if (withRowFilter) {
      Map<String, String> queryOptions = new HashMap<>();
      if (pinotQuery.getQueryOptions() != null) {
        queryOptions.putAll(pinotQuery.getQueryOptions());
      }
      queryOptions.put(RlsUtils.buildRlsFilterKey("testTable"), "col3 = 1");
      pinotQuery.setQueryOptions(queryOptions);
    }
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(pinotQuery);
    assertEquals(queryContext.getRowFilterExpression() != null, withRowFilter);
    if (mode != null) {
      queryContext.getQueryOptions().put(QueryOptionKey.AND_RESTRICTION_PUSHDOWN_MODE, mode);
    }
    new InstancePlanMakerImplV2().makeInstancePlan(List.of(), queryContext, mock(ExecutorService.class));
    return queryContext.getAndRestrictionPushdown();
  }
}
