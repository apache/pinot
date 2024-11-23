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
package org.apache.pinot.tsdb.spi.plan;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class LeafTimeSeriesPlanNodeTest {
  private static final String ID = "plan_id123";
  private static final String TABLE = "myTable";
  private static final String TIME_COLUMN = "orderTime";
  private static final TimeUnit TIME_UNIT = TimeUnit.SECONDS;

  @Test
  public void testGetEffectiveFilter() {
    TimeBuckets timeBuckets = TimeBuckets.ofSeconds(1000, Duration.ofSeconds(13), 9);
    final long expectedStartTimeInFilter = timeBuckets.getTimeRangeStartExclusive();
    final long expectedEndTimeInFilter = timeBuckets.getTimeRangeEndInclusive();
    final String nonEmptyFilter = "cityName = 'Chicago'";
    // Case-1: No offset, and empty filter.
    {
      LeafTimeSeriesPlanNode planNode =
          new LeafTimeSeriesPlanNode(ID, Collections.emptyList(), TABLE, TIME_COLUMN, TIME_UNIT, 0L, "", "value_col",
              new AggInfo("SUM", null), Collections.singletonList("cityName"));
      assertEquals(planNode.getEffectiveFilter(timeBuckets),
          "orderTime > " + expectedStartTimeInFilter + " AND orderTime <= " + expectedEndTimeInFilter);
    }
    // Case-2: Offset, but empty filter
    {
      LeafTimeSeriesPlanNode planNode =
          new LeafTimeSeriesPlanNode(ID, Collections.emptyList(), TABLE, TIME_COLUMN, TIME_UNIT, 123L, "", "value_col",
              new AggInfo("SUM", null), Collections.singletonList("cityName"));
      assertEquals(planNode.getEffectiveFilter(timeBuckets),
          "orderTime > " + (expectedStartTimeInFilter - 123) + " AND orderTime <= " + (expectedEndTimeInFilter - 123));
    }
    // Case-3: Offset and non-empty filter
    {
      LeafTimeSeriesPlanNode planNode =
          new LeafTimeSeriesPlanNode(ID, Collections.emptyList(), TABLE, TIME_COLUMN, TIME_UNIT, 123L, nonEmptyFilter,
              "value_col", new AggInfo("SUM", null), Collections.singletonList("cityName"));
      assertEquals(planNode.getEffectiveFilter(timeBuckets),
          String.format("(%s) AND (orderTime > %s AND orderTime <= %s)", nonEmptyFilter,
              (expectedStartTimeInFilter - 123), (expectedEndTimeInFilter - 123)));
    }
    // Case-4: Offset, and non-empty filter, and time-unit that is not seconds
    {
      LeafTimeSeriesPlanNode planNode =
          new LeafTimeSeriesPlanNode(ID, Collections.emptyList(), TABLE, TIME_COLUMN, TimeUnit.MILLISECONDS, 123L,
              nonEmptyFilter, "value_col", new AggInfo("SUM", null), Collections.singletonList("cityName"));
      assertEquals(planNode.getEffectiveFilter(timeBuckets),
          String.format("(%s) AND (orderTime > %s AND orderTime <= %s)", nonEmptyFilter,
              (expectedStartTimeInFilter * 1000 - 123 * 1000), (expectedEndTimeInFilter * 1000 - 123 * 1000)));
    }
  }
}
