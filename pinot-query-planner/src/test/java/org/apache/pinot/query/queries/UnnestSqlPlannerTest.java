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
package org.apache.pinot.query.queries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.core.routing.MockRoutingManagerFactory;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.UnnestNode;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class UnnestSqlPlannerTest extends QueryEnvironmentTestBase {

  @DataProvider(name = "unnestSqlProvider")
  public Object[][] provideUnnestSqlQueries() {
    return new Object[][]{
        new Object[]{
            "SELECT e.col1, u.s, u.ord FROM e "
                + "CROSS JOIN UNNEST(e.mcol1) WITH ORDINALITY AS u(s, ord)",
            1,
            true,
            List.of("col1", "s", "ord")
        },
        new Object[]{
            "SELECT e.col1, u.longVal, u.stringVal FROM e "
                + "CROSS JOIN UNNEST(e.mcol2, e.mcol1) AS u(longVal, stringVal)",
            2,
            false,
            List.of("col1", "longVal", "stringVal")
        },
        new Object[]{
            "SELECT e.col1, u.longVal, u.stringVal, u.ord FROM e "
                + "CROSS JOIN UNNEST(e.mcol2, e.mcol1) WITH ORDINALITY AS u(longVal, stringVal, ord)",
            2,
            true,
            List.of("col1", "longVal", "stringVal", "ord")
        }
    };
  }

  @Test(dataProvider = "unnestSqlProvider")
  public void testUnnestSqlPlans(String sql, int expectedArrayExprCount, boolean withOrdinality,
      List<String> expectedOutputColumns) {
    DispatchableSubPlan subPlan = _queryEnvironment.planQuery(sql);
    List<UnnestNode> unnestNodes = findUnnestNodes(subPlan);
    Assert.assertEquals(unnestNodes.size(), 1, "Expected exactly one UnnestNode for query: " + sql);

    UnnestNode unnestNode = unnestNodes.get(0);
    Assert.assertEquals(unnestNode.getArrayExprs().size(), expectedArrayExprCount,
        "Unexpected number of array expressions for query: " + sql);
    DispatchablePlanFragment rootFragment = subPlan.getQueryStageMap().get(0);
    PlanNode rootNode = rootFragment.getPlanFragment().getFragmentRoot();
    if (expectedOutputColumns != null) {
      List<String> actualOutputColumns = Arrays.asList(rootNode.getDataSchema().getColumnNames());
      Assert.assertEquals(actualOutputColumns, expectedOutputColumns,
          "Unexpected output columns for query: " + sql);
    }
    Assert.assertEquals(unnestNode.isWithOrdinality(), withOrdinality,
        "Unexpected ordinality flag for query: " + sql);
    assertOrdinality(unnestNode, withOrdinality);
  }

  @Test
  public void testUnnestColumnPruningDropsSourceArray() {
    // With pruning enabled, the unnested source array (mcol1) must not be carried in the UnnestNode output.
    String sql = "SET unnestColumnPruning=true; "
        + "SELECT e.col1, u.s FROM e CROSS JOIN UNNEST(e.mcol1) AS u(s)";
    DispatchableSubPlan subPlan = _queryEnvironment.planQuery(sql);
    List<UnnestNode> unnestNodes = findUnnestNodes(subPlan);
    Assert.assertEquals(unnestNodes.size(), 1);
    UnnestNode unnestNode = unnestNodes.get(0);

    Assert.assertTrue(unnestNode.isPrunedPassthrough(), "UnnestNode should be pruned");
    List<String> columns = Arrays.asList(unnestNode.getDataSchema().getColumnNames());
    Assert.assertFalse(columns.contains("mcol1"),
        "Source array column should be pruned from UnnestNode output, found: " + columns);
    Assert.assertTrue(columns.contains("col1"), "Referenced passthrough column col1 should be retained: " + columns);
    Assert.assertEquals(unnestNode.getDataSchema().size(), 2, "Expected [col1, element] only: " + columns);
    // col1 is the only retained passthrough column (input index 0); the element lands right after it.
    Assert.assertEquals(unnestNode.getPassthroughInputIndexes(), List.of(0));
    Assert.assertEquals(unnestNode.getElementIndexes(), List.of(1));
  }

  @Test
  public void testUnnestColumnPruningDisabledByDefault() {
    // Without the flag, behavior is unchanged: the source array is still part of the UnnestNode output.
    String sql = "SELECT e.col1, u.s FROM e CROSS JOIN UNNEST(e.mcol1) AS u(s)";
    DispatchableSubPlan subPlan = _queryEnvironment.planQuery(sql);
    List<UnnestNode> unnestNodes = findUnnestNodes(subPlan);
    Assert.assertEquals(unnestNodes.size(), 1);
    UnnestNode unnestNode = unnestNodes.get(0);

    Assert.assertFalse(unnestNode.isPrunedPassthrough(), "UnnestNode should not be pruned by default");
    List<String> columns = Arrays.asList(unnestNode.getDataSchema().getColumnNames());
    Assert.assertTrue(columns.contains("mcol1"),
        "Without pruning, the source array column should remain in the UnnestNode output: " + columns);
  }

  @Test
  public void testUnnestColumnPruningRetainsSelectedSourceArray() {
    // When the user also selects the source array, it must be retained even with pruning enabled.
    String sql = "SET unnestColumnPruning=true; "
        + "SELECT e.col1, e.mcol1, u.s FROM e CROSS JOIN UNNEST(e.mcol1) AS u(s)";
    DispatchableSubPlan subPlan = _queryEnvironment.planQuery(sql);
    List<UnnestNode> unnestNodes = findUnnestNodes(subPlan);
    Assert.assertEquals(unnestNodes.size(), 1);
    UnnestNode unnestNode = unnestNodes.get(0);

    List<String> columns = Arrays.asList(unnestNode.getDataSchema().getColumnNames());
    Assert.assertTrue(columns.contains("mcol1"),
        "Explicitly selected source array must be retained: " + columns);
    Assert.assertTrue(columns.contains("col1"), columns.toString());
  }

  @Test
  public void testUnnestColumnPruningWithOrdinality() {
    // The ordinality index must be recomputed against the pruned (smaller) output.
    String sql = "SET unnestColumnPruning=true; "
        + "SELECT e.col1, u.s, u.ord FROM e CROSS JOIN UNNEST(e.mcol1) WITH ORDINALITY AS u(s, ord)";
    DispatchableSubPlan subPlan = _queryEnvironment.planQuery(sql);
    List<UnnestNode> unnestNodes = findUnnestNodes(subPlan);
    Assert.assertEquals(unnestNodes.size(), 1);
    UnnestNode unnestNode = unnestNodes.get(0);

    Assert.assertTrue(unnestNode.isWithOrdinality());
    Assert.assertTrue(unnestNode.isPrunedPassthrough());
    List<String> columns = Arrays.asList(unnestNode.getDataSchema().getColumnNames());
    Assert.assertFalse(columns.contains("mcol1"), "Source array should be pruned: " + columns);
    // Retained passthrough = [col1] (1), element at index 1, ordinality at index 2.
    Assert.assertEquals(unnestNode.getPassthroughInputIndexes(), List.of(0));
    Assert.assertEquals(unnestNode.getElementIndexes(), List.of(1));
    Assert.assertEquals(unnestNode.getOrdinalityIndex(), 2);
    Assert.assertEquals(unnestNode.getDataSchema().size(), 3);
  }

  @Test
  public void testUnnestColumnPruningMultipleArrays() {
    // Both source arrays must be dropped; element indexes recompute contiguously after the retained passthrough.
    String sql = "SET unnestColumnPruning=true; "
        + "SELECT e.col1, u.longVal, u.stringVal FROM e CROSS JOIN UNNEST(e.mcol2, e.mcol1) AS u(longVal, stringVal)";
    DispatchableSubPlan subPlan = _queryEnvironment.planQuery(sql);
    List<UnnestNode> unnestNodes = findUnnestNodes(subPlan);
    Assert.assertEquals(unnestNodes.size(), 1);
    UnnestNode unnestNode = unnestNodes.get(0);

    Assert.assertTrue(unnestNode.isPrunedPassthrough());
    List<String> columns = Arrays.asList(unnestNode.getDataSchema().getColumnNames());
    Assert.assertFalse(columns.contains("mcol1"), columns.toString());
    Assert.assertFalse(columns.contains("mcol2"), columns.toString());
    Assert.assertTrue(columns.contains("col1"), columns.toString());
    Assert.assertEquals(unnestNode.getPassthroughInputIndexes(), List.of(0));
    Assert.assertEquals(unnestNode.getElementIndexes(), List.of(1, 2));
    Assert.assertEquals(unnestNode.getDataSchema().size(), 3);
  }

  @Test
  public void testUnnestColumnPruningViaBrokerDefault() {
    // With the broker-config default on (and no per-query SET), pruning still applies.
    QueryEnvironment env = buildQueryEnvironment(true);
    DispatchableSubPlan subPlan =
        env.planQuery("SELECT e.col1, u.s FROM e CROSS JOIN UNNEST(e.mcol1) AS u(s)");
    List<UnnestNode> unnestNodes = findUnnestNodes(subPlan);
    Assert.assertEquals(unnestNodes.size(), 1);
    Assert.assertTrue(unnestNodes.get(0).isPrunedPassthrough(),
        "Broker-config default should enable pruning without a SET option");

    // A per-query SET overrides the broker default back off.
    DispatchableSubPlan overridden = env.planQuery(
        "SET unnestColumnPruning=false; SELECT e.col1, u.s FROM e CROSS JOIN UNNEST(e.mcol1) AS u(s)");
    Assert.assertFalse(findUnnestNodes(overridden).get(0).isPrunedPassthrough(),
        "Per-query SET should override the broker-config default");
  }

  private static QueryEnvironment buildQueryEnvironment(boolean defaultUnnestColumnPruning) {
    MockRoutingManagerFactory factory = new MockRoutingManagerFactory(1, 2);
    TABLE_SCHEMAS.forEach((name, schema) -> factory.registerTable(schema, name));
    SERVER1_SEGMENTS.forEach((table, segments) -> segments.forEach(s -> factory.registerSegment(1, table, s)));
    SERVER2_SEGMENTS.forEach((table, segments) -> segments.forEach(s -> factory.registerSegment(2, table, s)));
    RoutingManager routingManager = factory.buildRoutingManager(null);
    TableCache tableCache = factory.buildTableCache();
    return new QueryEnvironment(QueryEnvironment.configBuilder()
        .requestId(-1L)
        .database(CommonConstants.DEFAULT_DATABASE)
        .tableCache(tableCache)
        .workerManager(new WorkerManager("Broker_localhost", "localhost", 3, routingManager))
        .defaultUnnestColumnPruning(defaultUnnestColumnPruning)
        .build());
  }

  @Test
  public void testUnnestColumnPruningToZeroPassthrough() {
    // Selecting only the unnested element retains zero passthrough columns.
    String sql = "SET unnestColumnPruning=true; "
        + "SELECT u.s FROM e CROSS JOIN UNNEST(e.mcol1) AS u(s)";
    DispatchableSubPlan subPlan = _queryEnvironment.planQuery(sql);
    List<UnnestNode> unnestNodes = findUnnestNodes(subPlan);
    Assert.assertEquals(unnestNodes.size(), 1);
    UnnestNode unnestNode = unnestNodes.get(0);

    Assert.assertTrue(unnestNode.isPrunedPassthrough());
    Assert.assertTrue(unnestNode.getPassthroughInputIndexes().isEmpty());
    Assert.assertEquals(unnestNode.getElementIndexes(), List.of(0));
    Assert.assertEquals(unnestNode.getDataSchema().size(), 1);
  }

  @Test
  public void testAggregateWithOrdinality() {
    String sql = "SELECT SUM(w.ord) FROM e CROSS JOIN UNNEST(e.mcol1) WITH ORDINALITY AS w(s, ord)";
    verifyOrdinalityOnlyQuery(sql);
  }

  @Test
  public void testAggregateAndFilterWithOrdinality() {
    String sql =
        "SELECT COUNT(u.elem), SUM(u.idx) FROM e CROSS JOIN UNNEST(e.mcol1) WITH ORDINALITY AS u(elem, idx) "
            + "WHERE idx = 2";
    DispatchableSubPlan subPlan = verifyOrdinalityOnlyQuery(sql);
    AggregateFilterUnnestChain chain = null;
    for (DispatchablePlanFragment fragment : subPlan.getQueryStageMap().values()) {
      chain = findAggregateFilterUnnestChain(fragment.getPlanFragment().getFragmentRoot(), null);
      if (chain != null) {
        break;
      }
    }
    Assert.assertNotNull(chain, "Expected to find Aggregate -> Filter -> UNNEST chain in the plan");
    FilterNode filterNode = chain._filterNode;
    UnnestNode unnestNode = chain._unnestNode;
    List<String> filterColumns = Arrays.asList(filterNode.getDataSchema().getColumnNames());
    int ordIdx = unnestNode.getOrdinalityIndex();
    String ordName = ordIdx >= 0 && ordIdx < filterNode.getDataSchema().size()
        ? filterNode.getDataSchema().getColumnName(ordIdx) : null;
    Assert.assertTrue(filterColumns.contains(ordName),
        "Filter data schema should contain the ordinality column name '" + ordName
            + "', found: " + filterColumns);
    RexExpression.FunctionCall filterCondition = (RexExpression.FunctionCall) filterNode.getCondition();
    Assert.assertEquals(filterCondition.getFunctionName(), "EQUALS", "Expected ordinality equality filter");
    RexExpression.InputRef ordinalityRef = (RexExpression.InputRef) filterCondition.getFunctionOperands().get(0);
    Assert.assertEquals(ordinalityRef.getIndex(), unnestNode.getOrdinalityIndex(),
        "Filter should reference the ordinality column index produced by the UNNEST node");
    Assert.assertTrue(ordinalityRef.getIndex() < filterNode.getDataSchema().size(),
        "Input reference should be smaller than the data schema size");
    Assert.assertFalse(hasFilterBelowUnnest(subPlan),
        "Filter on ordinality should not be pushed below the CROSS JOIN UNNEST");
  }

  @Test
  public void testAggregateAndFilterWithMultiArrayOrdinality() {
    String sql =
        "SELECT COUNT(u.longValue), SUM(u.ord) FROM e "
            + "CROSS JOIN UNNEST(e.mcol2, e.mcol1) WITH ORDINALITY AS u(longValue, stringValue, ord) "
            + "WHERE ord = 3";
    DispatchableSubPlan subPlan = _queryEnvironment.planQuery(sql);
    List<UnnestNode> unnestNodes = findUnnestNodes(subPlan);
    Assert.assertEquals(unnestNodes.size(), 1, "Expected single UNNEST node for multi-array query");
    UnnestNode unnestNode = unnestNodes.get(0);
    Assert.assertEquals(unnestNode.getArrayExprs().size(), 2, "Expected two array expressions to be unnested");

    AggregateFilterUnnestChain chain = null;
    for (DispatchablePlanFragment fragment : subPlan.getQueryStageMap().values()) {
      chain = findAggregateFilterUnnestChain(fragment.getPlanFragment().getFragmentRoot(), null);
      if (chain != null) {
        break;
      }
    }
    Assert.assertNotNull(chain, "Expected Aggregate -> Filter -> UNNEST chain for multi-array ordinality query");
    FilterNode filterNode = chain._filterNode;
    Assert.assertTrue(filterNode.getInputs().get(0) instanceof UnnestNode,
        "Filter node should sit directly above the UNNEST node");
    Assert.assertSame(chain._unnestNode, unnestNode, "Filter should operate on the same UNNEST node");

    List<String> filterColumns = Arrays.asList(filterNode.getDataSchema().getColumnNames());
    int ordIdx = unnestNode.getOrdinalityIndex();
    String ordName = ordIdx >= 0 ? filterNode.getDataSchema().getColumnName(ordIdx) : null;
    Assert.assertTrue(filterColumns.contains(ordName),
        "Filter schema should include the ordinality column name '" + ordName + "'");

    RexExpression.FunctionCall filterCondition = (RexExpression.FunctionCall) filterNode.getCondition();
    Assert.assertEquals(filterCondition.getFunctionName(), "EQUALS", "Expected equality filter on ordinality");
    RexExpression.InputRef ordinalityRef = (RexExpression.InputRef) filterCondition.getFunctionOperands().get(0);
    Assert.assertEquals(ordinalityRef.getIndex(), unnestNode.getOrdinalityIndex(),
        "Filter input index should match ordinality column index");
    Assert.assertTrue(ordinalityRef.getIndex() < filterNode.getDataSchema().size(),
        "Filter InputRef must be within the filter node schema bounds");
    Assert.assertFalse(hasFilterBelowUnnest(subPlan),
        "Ordinality filter should never be pushed below the UNNEST operator");
  }

  private static class AggregateFilterUnnestChain {
    private final FilterNode _filterNode;
    private final UnnestNode _unnestNode;

    AggregateFilterUnnestChain(FilterNode filterNode, UnnestNode unnestNode) {
      _filterNode = filterNode;
      _unnestNode = unnestNode;
    }
  }

  private static AggregateFilterUnnestChain findAggregateFilterUnnestChain(PlanNode node, PlanNode parent) {
    if (node instanceof FilterNode && parent instanceof AggregateNode) {
      PlanNode filterInput = node.getInputs().isEmpty() ? null : node.getInputs().get(0);
      if (filterInput instanceof UnnestNode) {
        return new AggregateFilterUnnestChain((FilterNode) node, (UnnestNode) filterInput);
      }
    }
    for (PlanNode child : node.getInputs()) {
      AggregateFilterUnnestChain chain = findAggregateFilterUnnestChain(child, node);
      if (chain != null) {
        return chain;
      }
    }
    return null;
  }

  private static List<UnnestNode> findUnnestNodes(DispatchableSubPlan subPlan) {
    List<UnnestNode> nodes = new ArrayList<>();
    for (DispatchablePlanFragment fragment : subPlan.getQueryStages()) {
      collectUnnestNodes(fragment.getPlanFragment().getFragmentRoot(), nodes);
    }
    return nodes;
  }

  private static void collectUnnestNodes(PlanNode node, List<UnnestNode> nodes) {
    if (node instanceof UnnestNode) {
      nodes.add((UnnestNode) node);
    }
    for (PlanNode input : node.getInputs()) {
      collectUnnestNodes(input, nodes);
    }
  }

  private static void assertOrdinality(UnnestNode node, boolean expected) {
    Assert.assertEquals(node.isWithOrdinality(), expected, "Unexpected ordinality flag");
    if (expected) {
      Assert.assertTrue(node.getOrdinalityIndex() >= 0, "Ordinality index should be present");
    }
  }

  private DispatchableSubPlan verifyOrdinalityOnlyQuery(String sql) {
    DispatchableSubPlan subPlan = _queryEnvironment.planQuery(sql);
    List<UnnestNode> unnestNodes = findUnnestNodes(subPlan);
    Assert.assertEquals(unnestNodes.size(), 1, "Expected exactly one UnnestNode for query: " + sql);
    UnnestNode node = unnestNodes.get(0);
    Assert.assertEquals(node.getArrayExprs().size(), 1, "Expected single array expression");
    assertOrdinality(node, true);
    return subPlan;
  }

  private static boolean hasFilterBelowUnnest(DispatchableSubPlan subPlan) {
    for (DispatchablePlanFragment fragment : subPlan.getQueryStages()) {
      if (hasFilterBelowUnnest(fragment.getPlanFragment().getFragmentRoot())) {
        return true;
      }
    }
    return false;
  }

  private static boolean hasFilterBelowUnnest(PlanNode node) {
    if (node instanceof UnnestNode) {
      for (PlanNode child : node.getInputs()) {
        if (containsFilter(child)) {
          return true;
        }
      }
    }
    for (PlanNode child : node.getInputs()) {
      if (hasFilterBelowUnnest(child)) {
        return true;
      }
    }
    return false;
  }

  private static boolean containsFilter(PlanNode node) {
    if (node instanceof FilterNode) {
      return true;
    }
    for (PlanNode child : node.getInputs()) {
      if (containsFilter(child)) {
        return true;
      }
    }
    return false;
  }
}
