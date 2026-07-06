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
package org.apache.pinot.query.planner.physical.v2;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.core.routing.MockRoutingManagerFactory;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Verifies that querying an empty table (a table that exists and has a routing entry, but no routable segments) under
 * the multi-stage physical optimizer does not fail. Such a query should be short-circuited to an empty result on the
 * broker, mirroring the single-stage engine and the legacy multi-stage path (see PinotDispatchPlanner and #18538).
 */
public class EmptyTablePlanTest {
  private static final String EMPTY_TABLE = "emptytable_OFFLINE";
  private static final String EMPTY_TABLE_2 = "emptytable2_OFFLINE";

  private QueryEnvironment _queryEnvironment;

  @BeforeClass
  public void setUp() {
    MockRoutingManagerFactory factory = new MockRoutingManagerFactory(1, 2);
    for (Map.Entry<String, Schema> entry : QueryEnvironmentTestBase.TABLE_SCHEMAS.entrySet()) {
      factory.registerTable(entry.getValue(), entry.getKey());
    }
    for (Map.Entry<String, List<String>> entry : QueryEnvironmentTestBase.SERVER1_SEGMENTS.entrySet()) {
      for (String segment : entry.getValue()) {
        factory.registerSegment(1, entry.getKey(), segment);
      }
    }
    for (Map.Entry<String, List<String>> entry : QueryEnvironmentTestBase.SERVER2_SEGMENTS.entrySet()) {
      for (String segment : entry.getValue()) {
        factory.registerSegment(2, entry.getKey(), segment);
      }
    }
    // Register offline tables that have a routing entry but no routable segments (empty tables).
    factory.registerTable(QueryEnvironmentTestBase.TABLE_SCHEMAS.get("c_OFFLINE"), EMPTY_TABLE);
    factory.registerEmptyTable(EMPTY_TABLE);
    factory.registerTable(QueryEnvironmentTestBase.TABLE_SCHEMAS.get("c_OFFLINE"), EMPTY_TABLE_2);
    factory.registerEmptyTable(EMPTY_TABLE_2);

    RoutingManager routingManager = factory.buildRoutingManager(null);
    TableCache tableCache = factory.buildTableCache();
    WorkerManager workerManager = new WorkerManager("Broker_localhost", "localhost", 3, routingManager);

    _queryEnvironment = new QueryEnvironment(
        QueryEnvironment.configBuilder()
            .requestId(-1L)
            .database(CommonConstants.DEFAULT_DATABASE)
            .tableCache(tableCache)
            .workerManager(workerManager)
            .defaultUsePhysicalOptimizer(true)
            .defaultInferPartitionHint(true)
            .build());
  }

  @Test
  public void testSelectStarFromEmptyTable() {
    DispatchableSubPlan plan = _queryEnvironment.planQuery("SELECT * FROM emptytable");
    assertNotNull(plan);
    assertTrue(plan.isAllLeafStagesEmpty(), "All leaf stages should be detected as empty");
    assertShortCircuited(plan);
  }

  @Test
  public void testCountStarFromEmptyTable() {
    DispatchableSubPlan plan = _queryEnvironment.planQuery("SELECT COUNT(*) FROM emptytable");
    assertNotNull(plan);
    assertTrue(plan.isAllLeafStagesEmpty(), "All leaf stages should be detected as empty");
    assertShortCircuited(plan);
  }

  @Test
  public void testFilterFromEmptyTable() {
    DispatchableSubPlan plan = _queryEnvironment.planQuery("SELECT col1, col3 FROM emptytable WHERE col3 > 5");
    assertNotNull(plan);
    assertTrue(plan.isAllLeafStagesEmpty(), "All leaf stages should be detected as empty");
    assertShortCircuited(plan);
  }

  @Test
  public void testGroupByFromEmptyTable() {
    DispatchableSubPlan plan =
        _queryEnvironment.planQuery("SELECT col1, SUM(col3) FROM emptytable GROUP BY col1");
    assertNotNull(plan);
    assertTrue(plan.isAllLeafStagesEmpty(), "All leaf stages should be detected as empty");
    assertShortCircuited(plan);
  }

  @Test
  public void testNonExistentTableStillErrors() {
    // A table that does not exist must still fail (caught during validation / table-cache lookup), not be treated as
    // an empty table and short-circuited to an empty result.
    RuntimeException e = expectThrows(RuntimeException.class,
        () -> _queryEnvironment.planQuery("SELECT * FROM this_table_does_not_exist"));
    assertTrue(e.getMessage().toLowerCase().contains("not found")
            || e.getMessage().toLowerCase().contains("object 'this_table_does_not_exist'")
            || e.getMessage().toLowerCase().contains("this_table_does_not_exist"),
        "Expected a table-not-found error, got: " + e.getMessage());
  }

  @Test
  public void testUnionOfTwoEmptyTablesShortCircuits() {
    // Multiple leaf stages, all empty: still short-circuited (exercises the all-empty boundary with >1 leaf).
    DispatchableSubPlan plan = _queryEnvironment.planQuery(
        "SELECT col1, col2 FROM emptytable UNION ALL SELECT col1, col2 FROM emptytable2");
    assertNotNull(plan);
    assertTrue(plan.isAllLeafStagesEmpty(), "All leaf stages should be detected as empty");
    assertShortCircuited(plan);
  }

  @Test
  public void testUnionWithNonEmptyTableFailsFast() {
    // Combining an empty table with a non-empty table is not yet supported by the physical optimizer. It must fail
    // fast with a clear error rather than silently drop rows (see PinotDispatchPlanner#trackEmptyLeafStages).
    assertPartiallyEmptyError("SELECT col1, col2 FROM emptytable UNION ALL SELECT col1, col2 FROM c");
  }

  @Test
  public void testJoinWithNonEmptyTableFailsFast() {
    assertPartiallyEmptyError("SELECT t.col1 FROM emptytable t JOIN c ON t.col1 = c.col1");
  }

  private void assertPartiallyEmptyError(String query) {
    RuntimeException e = expectThrows(RuntimeException.class, () -> _queryEnvironment.planQuery(query));
    assertTrue(e.getMessage().contains("combine an empty or fully-pruned table with a non-empty table"),
        "Expected a clear partially-empty error, got: " + e.getMessage());
  }

  /**
   * When all leaf stages are empty, the plan is rewritten so that only the broker reduce stage (stage 0) remains,
   * and its plan tree no longer contains any table scan (the leaves are inlined to empty literals).
   */
  private void assertShortCircuited(DispatchableSubPlan plan) {
    assertEquals(plan.getQueryStageMap().keySet(), Set.of(0),
        "Only the reduce stage should remain after short-circuiting empty leaves");
    DispatchablePlanFragment reduceStage = plan.getQueryStageMap().get(0);
    assertFalse(containsTableScan(reduceStage.getPlanFragment().getFragmentRoot()),
        "Reduce stage should not contain a TableScanNode after inlining empty leaves");
  }

  private static boolean containsTableScan(PlanNode node) {
    if (node instanceof TableScanNode) {
      return true;
    }
    for (PlanNode input : node.getInputs()) {
      if (containsTableScan(input)) {
        return true;
      }
    }
    return false;
  }
}
