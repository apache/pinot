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
package org.apache.pinot.query;

import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;


/**
 * Plan-level tests verifying that the multi-stage engine expands GROUP BY ROLLUP / CUBE / GROUPING SETS into a
 * UNION ALL of ordinary aggregates (see {@link org.apache.pinot.calcite.rel.GroupingSetsExpander}).
 */
public class GroupingSetsPlannerTest extends QueryEnvironmentTestBase {

  @Test
  public void testRollupPlans() {
    DispatchableSubPlan plan = _queryEnvironment.planQuery(
        "SELECT col1, col2, SUM(col3) FROM a GROUP BY ROLLUP(col1, col2)");
    assertNotNull(plan);
  }

  @Test
  public void testCubePlans() {
    DispatchableSubPlan plan = _queryEnvironment.planQuery(
        "SELECT col1, col2, SUM(col3) FROM a GROUP BY CUBE(col1, col2)");
    assertNotNull(plan);
  }

  @Test
  public void testGroupingSetsPlans() {
    DispatchableSubPlan plan = _queryEnvironment.planQuery(
        "SELECT col1, col2, SUM(col3) FROM a GROUP BY GROUPING SETS ((col1, col2), (col1), ())");
    assertNotNull(plan);
  }

  @Test
  public void testGroupingFunctionPlans() {
    DispatchableSubPlan plan = _queryEnvironment.planQuery(
        "SELECT col1, col2, SUM(col3), GROUPING(col1), GROUPING_ID(col1, col2) FROM a GROUP BY ROLLUP(col1, col2)");
    assertNotNull(plan);
  }
}
