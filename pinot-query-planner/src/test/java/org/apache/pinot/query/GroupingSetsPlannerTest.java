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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/**
 * Plan-level tests for GROUP BY ROLLUP / CUBE / GROUPING SETS in the multi-stage engine. The grouping-set aggregate is
 * lowered to the native expansion (see {@link org.apache.pinot.calcite.rel.GroupingSetsExpander} and
 * {@link org.apache.pinot.calcite.rel.logical.PinotLogicalGroupingSetsExpand}): a single table scan feeds a
 * {@code GroupingSetsExpand} operator and an ordinary aggregate, rather than a UNION ALL of one aggregate per set.
 */
public class GroupingSetsPlannerTest extends QueryEnvironmentTestBase {

  private String explain(String sql) {
    return _queryEnvironment.explainQuery("EXPLAIN PLAN FOR " + sql, 0);
  }

  /** Number of table scans in the optimized plan; the native expansion uses exactly one. */
  private int numTableScans(String sql) {
    return explain(sql).split("TableScan", -1).length - 1;
  }

  /** Asserts the plan for {@code sql} is the native expansion: a single scan plus an expand node, never a UNION ALL. */
  private void assertNativeExpandSingleScan(String sql) {
    assertEquals(numTableScans(sql), 1, "expected a single scan for the native grouping-set expansion: " + sql);
    String plan = explain(sql).toLowerCase();
    assertTrue(plan.contains("expand"), "plan should contain the native expand node: " + sql);
    assertFalse(plan.contains("union"), "plan must not fall back to UNION ALL: " + sql);
  }

  @Test
  public void testRollupPlans() {
    DispatchableSubPlan plan = _queryEnvironment.planQuery(
        "SELECT col1, col2, SUM(col3) FROM a GROUP BY ROLLUP(col1, col2)");
    assertNotNull(plan);
  }

  @Test
  public void testRollupUsesNativeExpandSingleScan() {
    assertNativeExpandSingleScan("SELECT col1, col2, SUM(col3) FROM a GROUP BY ROLLUP(col1, col2)");
  }

  @Test
  public void testCubeUsesNativeExpandSingleScan() {
    assertNativeExpandSingleScan("SELECT col1, col2, SUM(col3) FROM a GROUP BY CUBE(col1, col2)");
  }

  @Test
  public void testGroupingSetsUsesNativeExpandSingleScan() {
    assertNativeExpandSingleScan(
        "SELECT col1, col2, SUM(col3) FROM a GROUP BY GROUPING SETS ((col1, col2), (col1), ())");
  }

  @Test
  public void testGroupingFunctionsUseNativeExpandSingleScan() {
    // GROUPING / GROUPING_ID are computed in the top project from $groupingId, so indicator queries expand too.
    assertNativeExpandSingleScan(
        "SELECT col1, col2, SUM(col3), GROUPING(col1), GROUPING_ID(col1, col2) FROM a GROUP BY ROLLUP(col1, col2)");
  }

  @Test
  public void testAvgUsesNativeExpandSingleScan() {
    // AVG decomposes to $SUM0 + COUNT before the split; the expansion still uses a single scan.
    assertNativeExpandSingleScan("SELECT col1, col2, AVG(col3) FROM a GROUP BY ROLLUP(col1, col2)");
  }

  @Test
  public void testDistinctCountUsesNativeExpandSingleScan() {
    assertNativeExpandSingleScan("SELECT col1, col2, DISTINCTCOUNT(col3) FROM a GROUP BY ROLLUP(col1, col2)");
  }

  @Test
  public void testCountDistinctUsesNativeExpandSingleScan() {
    // The native expansion hash-partitions on [groupCols, $groupingId], so exact COUNT(DISTINCT ...) is computed on a
    // single scan rather than the per-set UNION ALL the leaf pushdown required.
    assertNativeExpandSingleScan("SELECT col1, col2, COUNT(DISTINCT col3) FROM a GROUP BY ROLLUP(col1, col2)");
  }

  @Test
  public void testGroupingSetsRejectedByPhysicalOptimizer() {
    // The v2 physical optimizer does not support the native expansion, so grouping sets must be rejected clearly there
    // (the broker wraps the planner's UnsupportedOperationException in a QueryException, preserving the message).
    try {
      _queryEnvironment.planQuery("SET usePhysicalOptimizer=true; SELECT col1, SUM(col3) FROM a GROUP BY ROLLUP(col1)");
      fail("expected grouping sets to be rejected by the multi-stage physical optimizer");
    } catch (Exception e) {
      assertTrue(String.valueOf(e.getMessage()).contains("not supported by the multi-stage physical optimizer"),
          "expected a clear physical-optimizer rejection, got: " + e.getMessage());
    }
  }
}
