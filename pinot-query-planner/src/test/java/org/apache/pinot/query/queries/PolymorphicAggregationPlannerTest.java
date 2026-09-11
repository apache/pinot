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
import java.util.List;
import org.apache.pinot.common.request.context.AggregateCallBinding;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.spi.exception.QueryException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Covers original logical type preservation across direct, distributed and grouping-set aggregation plans.
public class PolymorphicAggregationPlannerTest extends QueryEnvironmentTestBase {
  @DataProvider
  public Object[][] physicalOptimizers() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "physicalOptimizers")
  public void testDistributedTypes(boolean usePhysicalOptimizer) {
    DispatchableSubPlan plan = _queryEnvironment.planQuery("SET usePhysicalOptimizer=" + usePhysicalOptimizer + "; "
        + "SELECT MODE(NULLIF(col1, '')), MODE(ts_timestamp, 'MAX'), MODE(col3, 'AVG'), MODE(col5), "
        + "FIRST_WITH_TIME(ts_timestamp, ts), LAST_WITH_TIME(col5, ts) FROM a");
    ColumnDataType[] resultTypes = {ColumnDataType.STRING, ColumnDataType.TIMESTAMP, ColumnDataType.DOUBLE,
        ColumnDataType.BOOLEAN, ColumnDataType.TIMESTAMP, ColumnDataType.BOOLEAN};
    PlanNode root = plan.getQueryStageMap().get(0).getPlanFragment().getFragmentRoot();
    assertEquals(root.getDataSchema().getColumnDataTypes(), resultTypes);
    List<AggregateNode> aggregates = findAggregates(plan);
    assertFalse(aggregates.isEmpty());
    boolean sawIntermediate = false;
    boolean sawFinal = false;
    int[] argumentCounts = {1, 2, 2, 1, 2, 2};
    for (AggregateNode aggregate : aggregates) {
      List<RexExpression.FunctionCall> calls = aggregate.getAggCalls();
      assertEquals(calls.size(), resultTypes.length);
      for (int i = 0; i < calls.size(); i++) {
        assertBinding(calls.get(i), argumentCounts[i], resultTypes[i]);
      }
      assertEquals(calls.get(2).getAggregationBinding().getArgumentTypes().get(0), ColumnDataType.INT);
      if (aggregate.getAggType().isOutputIntermediateFormat() && !aggregate.isLeafReturnFinalResult()) {
        sawIntermediate = true;
        for (ColumnDataType type : aggregate.getDataSchema().getColumnDataTypes()) {
          assertEquals(type, ColumnDataType.OBJECT);
        }
      } else {
        sawFinal = true;
        assertEquals(aggregate.getDataSchema().getColumnDataTypes(), resultTypes);
      }
    }
    assertTrue(sawIntermediate);
    assertTrue(sawFinal);
  }

  @Test(dataProvider = "physicalOptimizers")
  public void testDirectAndGroupingSets(boolean usePhysicalOptimizer) {
    List<String> queries = List.of(
        "SELECT /*+ aggOptions(is_skip_leaf_stage_group_by='true') */ col2, MODE(col1), "
            + "FIRST_WITH_TIME(ts_timestamp, ts) FROM a GROUP BY col2",
        "SELECT col3, GROUPING(col3), MODE(col1), FIRST_WITH_TIME(ts_timestamp, ts) "
            + "FROM a GROUP BY GROUPING SETS ((col3), ())");
    for (int i = 0; i < queries.size(); i++) {
      List<AggregateNode> aggregates = findAggregates(_queryEnvironment.planQuery(
          "SET usePhysicalOptimizer=" + usePhysicalOptimizer + "; " + queries.get(i)));
      assertFalse(aggregates.isEmpty());
      for (AggregateNode aggregate : aggregates) {
        assertBinding(aggregate.getAggCalls().get(0), 1, ColumnDataType.STRING);
        assertBinding(aggregate.getAggCalls().get(1), 2, ColumnDataType.TIMESTAMP);
      }
      AggType expectedType = i == 0 ? AggType.DIRECT : AggType.FINAL;
      assertTrue(aggregates.stream().anyMatch(aggregate -> aggregate.getAggType() == expectedType));
    }
  }

  @Test(dataProvider = "physicalOptimizers")
  public void testFilteredAndSortedInput(boolean usePhysicalOptimizer) {
    List<String> inputs = List.of(
        "(SELECT col1 FROM a WHERE col3 > 0)",
        "(SELECT col1 FROM a WHERE col3 > 0 ORDER BY col1 LIMIT 20)");
    for (String input : inputs) {
      DispatchableSubPlan plan = _queryEnvironment.planQuery("SET usePhysicalOptimizer=" + usePhysicalOptimizer
          + "; SELECT MODE(col1, 'MAX') FROM " + input);
      List<AggregateNode> aggregates = findAggregates(plan);
      assertFalse(aggregates.isEmpty());
      for (AggregateNode aggregate : aggregates) {
        RexExpression.FunctionCall call = aggregate.getAggCalls().get(0);
        assertBinding(call, 2, ColumnDataType.STRING);
        assertTrue(call.getFunctionOperands().get(1) instanceof RexExpression.Literal);
        assertEquals(((RexExpression.Literal) call.getFunctionOperands().get(1)).getValue(), "MAX");
      }
    }
  }

  @Test(dataProvider = "physicalOptimizers")
  public void testLegacyOverloadRemainsUnbound(boolean usePhysicalOptimizer) {
    DispatchableSubPlan plan = _queryEnvironment.planQuery("SET usePhysicalOptimizer=" + usePhysicalOptimizer
        + "; SELECT FIRST_WITH_TIME(col1, ts, 'STRING') FROM a");
    for (AggregateNode aggregate : findAggregates(plan)) {
      RexExpression.FunctionCall call = aggregate.getAggCalls().get(0);
      assertEquals(call.getFunctionOperands().size(), 3);
      assertNull(call.getAggregationBinding());
    }
  }

  @Test(dataProvider = "physicalOptimizers")
  public void testBoundFinalOutputOverridesLegacyChildType(boolean usePhysicalOptimizer) {
    for (boolean leafReturnFinalResult : List.of(false, true)) {
      // Table a is partitioned on col2. Group on col1 so both planners need the leaf/final aggregation split.
      DispatchableSubPlan plan = _queryEnvironment.planQuery("SET usePhysicalOptimizer=" + usePhysicalOptimizer
          + "; SELECT /*+ aggOptions(is_leaf_return_final_result='" + leafReturnFinalResult + "') */ col1, "
          + "pinotchildaggexprmin(0, col5, col5, ts), "
          + "pinotchildaggexprmax(1, ts_timestamp, ts_timestamp, ts) FROM a GROUP BY col1");
      List<AggregateNode> aggregates = findAggregates(plan);
      assertFalse(aggregates.isEmpty());
      boolean sawIntermediateOutput = false;
      for (AggregateNode aggregate : aggregates) {
        assertBinding(aggregate.getAggCalls().get(0), 4, ColumnDataType.BOOLEAN);
        assertBinding(aggregate.getAggCalls().get(1), 4, ColumnDataType.TIMESTAMP);
        int offset = aggregate.getGroupKeys().size();
        ColumnDataType firstType = aggregate.getDataSchema().getColumnDataType(offset);
        ColumnDataType secondType = aggregate.getDataSchema().getColumnDataType(offset + 1);
        if (aggregate.getAggType().isOutputIntermediateFormat()) {
          sawIntermediateOutput = true;
          assertEquals(aggregate.isLeafReturnFinalResult(), leafReturnFinalResult);
          assertEquals(firstType, leafReturnFinalResult ? ColumnDataType.BOOLEAN : ColumnDataType.OBJECT);
          assertEquals(secondType, leafReturnFinalResult ? ColumnDataType.TIMESTAMP : ColumnDataType.OBJECT);
        } else {
          assertEquals(firstType, ColumnDataType.BOOLEAN);
          assertEquals(secondType, ColumnDataType.TIMESTAMP);
        }
      }
      assertTrue(sawIntermediateOutput);
    }
  }

  @Test
  public void testUnsupportedTypesAndReducers() {
    for (String expression : List.of("MODE(col1, 'AVG')", "MODE(col5, 'AVG')", "MODE(ts_timestamp, 'AVG')",
        "FIRST_WITH_TIME(col4, ts)", "FIRST_WITH_TIME(col1, col2)", "MODE(col4)", "MODE(col1, col2)")) {
      expectThrows(QueryException.class, () -> _queryEnvironment.compile("SELECT " + expression + " FROM a"));
    }
  }

  private static void assertBinding(RexExpression.FunctionCall call, int argumentCount, ColumnDataType resultType) {
    assertEquals(call.getFunctionOperands().size(), argumentCount);
    AggregateCallBinding binding = call.getAggregationBinding();
    assertNotNull(binding);
    assertEquals(binding.getArgumentTypes().size(), argumentCount);
    assertEquals(binding.getResultType(), resultType);
    assertFalse(binding.getArgumentTypes().contains(ColumnDataType.OBJECT));
  }

  private static List<AggregateNode> findAggregates(DispatchableSubPlan plan) {
    List<AggregateNode> aggregates = new ArrayList<>();
    for (DispatchablePlanFragment fragment : plan.getQueryStageMap().values()) {
      findAggregates(fragment.getPlanFragment().getFragmentRoot(), aggregates);
    }
    return aggregates;
  }

  private static void findAggregates(PlanNode node, List<AggregateNode> aggregates) {
    if (node instanceof AggregateNode) {
      aggregates.add((AggregateNode) node);
    }
    for (PlanNode input : node.getInputs()) {
      findAggregates(input, aggregates);
    }
  }
}
