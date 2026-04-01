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
package org.apache.pinot.queries;

import java.util.Map;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.testng.annotations.Test;


/**
 * Reuses the inter-segment group-by order-by coverage with the partitioned combine operator enabled.
 */
public class InterSegmentPartitionedGroupBySingleValueQueriesTest
    extends InterSegmentGroupBySingleValueQueriesTest {
  private static final InstancePlanMakerImplV2 TRIM_ENABLED_PLAN_MAKER = new InstancePlanMakerImplV2();
  private static final Map<String, String> QUERY_OPTIONS = Map.of("numGroupByPartitions", "8");

  static {
    TRIM_ENABLED_PLAN_MAKER.setMinSegmentGroupTrimSize(1);
  }

  @Override
  @Test(dataProvider = "groupByOrderByDataProvider")
  public void testGroupByOrderBy(String query, long expectedNumEntriesScannedPostFilter,
      ResultTable expectedResultTable) {
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(query, QUERY_OPTIONS), 120000L, 0L,
        expectedNumEntriesScannedPostFilter, 120000L, expectedResultTable);
  }

  @Override
  @Test(dataProvider = "groupByOrderByDataProvider")
  public void testGroupByOrderByWithTrim(String query, long expectedNumEntriesScannedPostFilter,
      ResultTable expectedResultTable) {
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(query, TRIM_ENABLED_PLAN_MAKER, QUERY_OPTIONS),
        120000L, 0L, expectedNumEntriesScannedPostFilter, 120000L, expectedResultTable);
  }
}
