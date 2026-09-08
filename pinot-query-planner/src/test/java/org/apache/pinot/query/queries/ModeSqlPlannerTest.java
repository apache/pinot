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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Verifies automatic distributed MODE type inference with both physical planners.
public class ModeSqlPlannerTest extends QueryEnvironmentTestBase {
  @DataProvider
  public Object[][] physicalOptimizers() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "physicalOptimizers")
  public void testDistributedModeTypes(boolean usePhysicalOptimizer) {
    DispatchableSubPlan plan = _queryEnvironment.planQuery("SET usePhysicalOptimizer=" + usePhysicalOptimizer + "; "
        + "SELECT MODE(NULLIF(col1, '')), MODE(ts_timestamp, 'MAX'), MODE(col3, 'AVG'), "
        + "MODE(col1, 'MAX', 'STRING') FROM a");
    PlanNode root = plan.getQueryStageMap().get(0).getPlanFragment().getFragmentRoot();
    assertEquals(root.getDataSchema().getColumnDataTypes(),
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.TIMESTAMP, ColumnDataType.DOUBLE,
            ColumnDataType.STRING});

    List<AggregateNode> aggregates = findAggregates(plan);
    assertFalse(aggregates.isEmpty());
    boolean sawIntermediate = false;
    boolean sawFinal = false;
    for (AggregateNode aggregate : aggregates) {
      List<RexExpression.FunctionCall> calls = aggregate.getAggCalls();
      assertEquals(calls.stream().map(RexExpression.FunctionCall::getFunctionName).toList(),
          List.of("MODE", "MODE", "MODE", "MODE"));
      assertTypedCall(calls.get(0), "MIN", "STRING");
      assertTypedCall(calls.get(1), "MAX", "TIMESTAMP");
      assertEquals(calls.get(2).getFunctionOperands().size(), 2);
      assertTypedCall(calls.get(3), "MAX", "STRING");
      if (aggregate.getAggType().isOutputIntermediateFormat() && !aggregate.isLeafReturnFinalResult()) {
        sawIntermediate = true;
        assertEquals(aggregate.getDataSchema().getColumnDataTypes(),
            new ColumnDataType[]{ColumnDataType.OBJECT, ColumnDataType.OBJECT, ColumnDataType.OBJECT,
                ColumnDataType.OBJECT});
      } else {
        sawFinal = true;
        assertEquals(aggregate.getDataSchema().getColumnDataTypes(),
            new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.TIMESTAMP, ColumnDataType.DOUBLE,
                ColumnDataType.STRING});
      }
    }
    assertTrue(sawIntermediate, "Distributed MODE must exchange frequency counts");
    assertTrue(sawFinal, "Distributed MODE must retain its final result types");
  }

  @Test(dataProvider = "physicalOptimizers")
  public void testGroupedModeTypes(boolean usePhysicalOptimizer) {
    List<String> queries = List.of(
        "SELECT /*+ aggOptions(is_skip_leaf_stage_group_by='true') */ col2, MODE(col1), MODE(ts_timestamp, 'MAX') "
            + "FROM a GROUP BY col2",
        "SELECT col3, GROUPING(col3), MODE(col1), MODE(ts_timestamp, 'MAX') "
            + "FROM a GROUP BY GROUPING SETS ((col3), ())");
    for (int i = 0; i < queries.size(); i++) {
      DispatchableSubPlan plan = _queryEnvironment.planQuery(
          "SET usePhysicalOptimizer=" + usePhysicalOptimizer + "; " + queries.get(i));
      List<AggregateNode> aggregates = findAggregates(plan);
      assertFalse(aggregates.isEmpty());
      for (AggregateNode aggregate : aggregates) {
        List<RexExpression.FunctionCall> calls = aggregate.getAggCalls();
        assertEquals(calls.size(), 2);
        assertTypedCall(calls.get(0), "MIN", "STRING");
        assertTypedCall(calls.get(1), "MAX", "TIMESTAMP");
        if (aggregate.getAggType() == AggType.FINAL) {
          for (int j = 0; j < calls.size(); j++) {
            RexExpression.InputRef input = (RexExpression.InputRef) calls.get(j).getFunctionOperands().get(0);
            assertEquals(input.getIndex(), aggregate.getGroupKeys().size() + j);
          }
        }
      }
      AggType expectedType = i == 0 ? AggType.DIRECT : AggType.FINAL;
      assertTrue(aggregates.stream().anyMatch(aggregate -> aggregate.getAggType() == expectedType));
    }
  }

  @Test
  public void testInvalidTypeAnnotations() {
    for (String expression : List.of("MODE(col1, 'MIN', 'TIMESTAMP')", "MODE(ts_timestamp, 'MIN', 'STRING')",
        "MODE(col1, 'MIN', 'INVALID')", "MODE(col1, 'MIN', col1)")) {
      QueryException error = expectThrows(QueryException.class,
          () -> _queryEnvironment.compile("SELECT " + expression + " FROM a"));
      assertTrue(error.getMessage().contains("MODE type argument"), error.getMessage());
    }
  }

  private static void assertTypedCall(RexExpression.FunctionCall call, String reducer, String type) {
    assertEquals(call.getFunctionName(), "MODE");
    List<RexExpression> arguments = call.getFunctionOperands();
    assertEquals(arguments.size(), 3);
    assertTrue(arguments.get(1) instanceof RexExpression.Literal);
    assertTrue(arguments.get(2) instanceof RexExpression.Literal);
    assertEquals(((RexExpression.Literal) arguments.get(1)).getValue(), reducer);
    assertEquals(((RexExpression.Literal) arguments.get(2)).getValue(), type);
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
