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
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.UnnestNode;
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
    Assert.assertEquals(unnestNode.getColumnAliases().size(), expectedArrayExprCount,
        "Unexpected number of column aliases for query: " + sql);
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
    Assert.assertTrue(filterColumns.contains(unnestNode.getOrdinalityAlias()),
        "Filter data schema should contain the ordinality column alias '" + unnestNode.getOrdinalityAlias()
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
    Assert.assertEquals(unnestNode.getColumnAliases().size(), 2, "Expected two column aliases for element outputs");
    Assert.assertNotNull(unnestNode.getOrdinalityAlias(), "Ordinality alias should be present");

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
    Assert.assertTrue(filterColumns.contains(unnestNode.getOrdinalityAlias()),
        "Filter schema should include the ordinality alias '" + unnestNode.getOrdinalityAlias() + "'");

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
      Assert.assertNotNull(node.getOrdinalityAlias(), "Ordinality alias should be present");
    } else {
      Assert.assertNull(node.getOrdinalityAlias(), "Ordinality alias should be null when not requested");
    }
  }

  private DispatchableSubPlan verifyOrdinalityOnlyQuery(String sql) {
    DispatchableSubPlan subPlan = _queryEnvironment.planQuery(sql);
    List<UnnestNode> unnestNodes = findUnnestNodes(subPlan);
    Assert.assertEquals(unnestNodes.size(), 1, "Expected exactly one UnnestNode for query: " + sql);
    UnnestNode node = unnestNodes.get(0);
    Assert.assertEquals(node.getArrayExprs().size(), 1, "Expected single array expression");
    Assert.assertEquals(node.getColumnAliases().size(), 1, "Expected single column alias");
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
