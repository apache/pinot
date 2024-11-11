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

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.plan.LeafTimeSeriesPlanNode;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class PhysicalTimeSeriesPlanVisitorTest {
  @Test
  public void testCompileQueryContext() {
    final String planId = "id";
    final String tableName = "orderTable";
    final String timeColumn = "orderTime";
    final AggInfo aggInfo = new AggInfo("SUM", null);
    final String filterExpr = "cityName = 'Chicago'";
    // Case-1: Without offset, simple column based group-by expression, simple column based value, and non-empty filter.
    {
      TimeSeriesExecutionContext context =
          new TimeSeriesExecutionContext("m3ql", TimeBuckets.ofSeconds(1000L, Duration.ofSeconds(10), 100),
              Collections.emptyMap());
      LeafTimeSeriesPlanNode leafNode =
          new LeafTimeSeriesPlanNode(planId, Collections.emptyList(), tableName, timeColumn, TimeUnit.SECONDS, 0L,
              filterExpr, "orderCount", aggInfo, Collections.singletonList("cityName"));
      QueryContext queryContext = PhysicalTimeSeriesPlanVisitor.INSTANCE.compileQueryContext(leafNode, context);
      assertNotNull(queryContext.getTimeSeriesContext());
      assertEquals(queryContext.getTimeSeriesContext().getLanguage(), "m3ql");
      assertEquals(queryContext.getTimeSeriesContext().getOffsetSeconds(), 0L);
      assertEquals(queryContext.getTimeSeriesContext().getTimeColumn(), timeColumn);
      assertEquals(queryContext.getTimeSeriesContext().getValueExpression().getIdentifier(), "orderCount");
      assertEquals(queryContext.getFilter().toString(),
          "(cityName = 'Chicago' AND orderTime > '990' AND orderTime <= '1990')");
    }
    // Case-2: With offset, complex group-by expression, complex value, and non-empty filter
    {
      TimeSeriesExecutionContext context =
          new TimeSeriesExecutionContext("m3ql", TimeBuckets.ofSeconds(1000L, Duration.ofSeconds(10), 100),
              Collections.emptyMap());
      LeafTimeSeriesPlanNode leafNode =
          new LeafTimeSeriesPlanNode(planId, Collections.emptyList(), tableName, timeColumn, TimeUnit.SECONDS, 10L,
              filterExpr, "orderCount*2", aggInfo, Collections.singletonList("concat(cityName, stateName, '-')"));
      QueryContext queryContext = PhysicalTimeSeriesPlanVisitor.INSTANCE.compileQueryContext(leafNode, context);
      assertNotNull(queryContext);
      assertNotNull(queryContext.getGroupByExpressions());
      assertEquals("concat(cityName,stateName,'-')", queryContext.getGroupByExpressions().get(0).toString());
      assertNotNull(queryContext.getTimeSeriesContext());
      assertEquals(queryContext.getTimeSeriesContext().getLanguage(), "m3ql");
      assertEquals(queryContext.getTimeSeriesContext().getOffsetSeconds(), 10L);
      assertEquals(queryContext.getTimeSeriesContext().getTimeColumn(), timeColumn);
      assertEquals(queryContext.getTimeSeriesContext().getValueExpression().toString(), "times(orderCount,'2')");
      assertNotNull(queryContext.getFilter());
      assertEquals(queryContext.getFilter().toString(),
          "(cityName = 'Chicago' AND orderTime > '980' AND orderTime <= '1980')");
    }
  }
}
