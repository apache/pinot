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
package org.apache.pinot.query.runtime.timeseries;

import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.RangeTimeSeriesRequest;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.plan.LeafTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.series.SimpleTimeSeriesBuilderFactory;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBuilderFactoryProvider;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class PhysicalTimeSeriesServerPlanVisitorTest {
  private static final String LANGUAGE = "m3ql";
  private static final int DUMMY_DEADLINE_MS = 10_000;
  private static final int SERIES_LIMIT = 1000;
  private static final Map<String, String> QUERY_OPTIONS = Collections.emptyMap();

  @BeforeClass
  public void setUp() {
    TimeSeriesBuilderFactoryProvider.registerSeriesBuilderFactory(LANGUAGE, new SimpleTimeSeriesBuilderFactory());
  }

  @Test
  public void testCompileQueryContext() {
    final String planId = "id";
    final String tableName = "orderTable";
    final String timeColumn = "orderTime";
    final AggInfo aggInfo = new AggInfo("SUM", false, Collections.emptyMap());
    final String filterExpr = "cityName = 'Chicago'";
    PhysicalTimeSeriesServerPlanVisitor serverPlanVisitor = new PhysicalTimeSeriesServerPlanVisitor(
        mock(QueryExecutor.class), mock(ExecutorService.class), mock(ServerMetrics.class));
    // Case-1: Without offset, simple column based group-by expression, simple column based value, and non-empty filter.
    {
      TimeSeriesExecutionContext context =
          new TimeSeriesExecutionContext(LANGUAGE, TimeBuckets.ofSeconds(1000L, Duration.ofSeconds(10), 100),
              DUMMY_DEADLINE_MS, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
      LeafTimeSeriesPlanNode leafNode =
          new LeafTimeSeriesPlanNode(planId, Collections.emptyList(), tableName, timeColumn, TimeUnit.SECONDS, 0L,
              filterExpr, "orderCount", aggInfo, Collections.singletonList("cityName"), SERIES_LIMIT,
              QUERY_OPTIONS);
      QueryContext queryContext = serverPlanVisitor.compileQueryContext(leafNode, context);
      assertEquals(queryContext.getFilter().toString(),
          "(cityName = 'Chicago' AND orderTime > '990' AND orderTime <= '1990')");
      assertTrue(isNumber(queryContext.getQueryOptions().get(QueryOptionKey.TIMEOUT_MS)));
      assertEquals(queryContext.getLimit(), SERIES_LIMIT);
    }
    // Case-2: With offset, complex group-by expression, complex value, non-empty filter, 0 limit, query options.
    {
      Map<String, String> queryOptions = ImmutableMap.of("numGroupsLimit", "1000");
      TimeSeriesExecutionContext context =
          new TimeSeriesExecutionContext(LANGUAGE, TimeBuckets.ofSeconds(1000L, Duration.ofSeconds(10), 100),
              DUMMY_DEADLINE_MS, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
      LeafTimeSeriesPlanNode leafNode =
          new LeafTimeSeriesPlanNode(planId, Collections.emptyList(), tableName, timeColumn, TimeUnit.SECONDS, 10L,
              filterExpr, "orderCount*2", aggInfo, Collections.singletonList("concat(cityName, stateName, '-')"),
              0 /* limit */, queryOptions);
      QueryContext queryContext = serverPlanVisitor.compileQueryContext(leafNode, context);
      assertNotNull(queryContext);
      assertNotNull(queryContext.getGroupByExpressions());
      assertEquals("concat(cityName,stateName,'-')", queryContext.getGroupByExpressions().get(0).toString());
      assertNotNull(queryContext.getFilter());
      assertEquals(queryContext.getFilter().toString(),
          "(cityName = 'Chicago' AND orderTime > '980' AND orderTime <= '1980')");
      assertTrue(isNumber(queryContext.getQueryOptions().get(QueryOptionKey.TIMEOUT_MS)));
      assertEquals(queryContext.getLimit(), RangeTimeSeriesRequest.DEFAULT_SERIES_LIMIT);
      assertEquals(queryContext.getQueryOptions().get("numGroupsLimit"), "1000");
    }
  }

  private boolean isNumber(String s) {
    try {
      Long.parseLong(s);
      return true;
    } catch (NumberFormatException ignored) {
      return false;
    }
  }
}
