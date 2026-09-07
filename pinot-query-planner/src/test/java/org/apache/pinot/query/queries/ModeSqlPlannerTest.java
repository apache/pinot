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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Verifies MODE type inference and type-specific dispatch across both multi-stage planner implementations.
public class ModeSqlPlannerTest extends QueryEnvironmentTestBase {
  @DataProvider
  public Object[][] physicalOptimizers() {
    return new Object[][]{{false}, {true}};
  }

  @Test
  public void testModeReturnTypes() {
    RelDataType rowType = _queryEnvironment.compile(
        "SET autoRewriteAggregationType=true; SELECT MODE(col1), MODE(ts_timestamp), MODE(col3), MODE(col7), "
            + "MODE(CAST(col3 AS FLOAT)), MODE(CAST(col3 AS DOUBLE)), "
            + "MODE(NULLIF(JSONEXTRACTSCALAR(col1, '$.user', 'STRING', ''), '')), "
            + "fromTimestamp(MODE(ts_timestamp)) FROM a")
        .getRelRoot().validatedRowType;
    SqlTypeName[] expectedTypes = {SqlTypeName.VARCHAR, SqlTypeName.TIMESTAMP, SqlTypeName.DOUBLE,
        SqlTypeName.DOUBLE, SqlTypeName.DOUBLE, SqlTypeName.DOUBLE, SqlTypeName.VARCHAR, SqlTypeName.BIGINT};
    for (int i = 0; i < expectedTypes.length; i++) {
      assertEquals(rowType.getFieldList().get(i).getType().getSqlTypeName(), expectedTypes[i]);
    }
  }

  @Test
  public void testFilteredModeNullability() {
    RelDataType rowType = _queryEnvironment.compile(
        "SET autoRewriteAggregationType=true; "
            + "SELECT col2, MODE(col1) FILTER (WHERE col3 > 0), MODE(ts_timestamp) FILTER (WHERE col3 > 0) "
            + "FROM a GROUP BY col2").getRelRoot().validatedRowType;
    assertTrue(rowType.getFieldList().get(1).getType().isNullable());
    assertTrue(rowType.getFieldList().get(2).getType().isNullable());
  }

  @Test(dataProvider = "physicalOptimizers")
  public void testDistributedModeTypes(boolean usePhysicalOptimizer) {
    DispatchableSubPlan plan = _queryEnvironment.planQuery("SET usePhysicalOptimizer=" + usePhysicalOptimizer + "; "
        + "SET autoRewriteAggregationType=true; SELECT MODE(col1), MODE(ts_timestamp), MODE(col3) FROM a");
    PlanNode root = plan.getQueryStageMap().get(0).getPlanFragment().getFragmentRoot();
    assertEquals(root.getDataSchema().getColumnDataTypes(),
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.TIMESTAMP, ColumnDataType.DOUBLE});

    List<AggregateNode> aggregates = findAggregates(plan);
    assertFalse(aggregates.isEmpty());
    boolean sawIntermediate = false;
    boolean sawFinal = false;
    for (AggregateNode aggregate : aggregates) {
      List<RexExpression.FunctionCall> calls = aggregate.getAggCalls();
      assertEquals(calls.stream().map(RexExpression.FunctionCall::getFunctionName).toList(),
          List.of("MODESTRING", "MODETIMESTAMP", "MODE"));
      if (aggregate.getAggType().isOutputIntermediateFormat() && !aggregate.isLeafReturnFinalResult()) {
        sawIntermediate = true;
        assertEquals(aggregate.getDataSchema().getColumnDataTypes(),
            new ColumnDataType[]{ColumnDataType.OBJECT, ColumnDataType.OBJECT, ColumnDataType.OBJECT});
      } else {
        sawFinal = true;
        assertEquals(aggregate.getDataSchema().getColumnDataTypes(),
            new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.TIMESTAMP, ColumnDataType.DOUBLE});
      }
    }
    assertTrue(sawIntermediate, "Distributed MODE must exchange frequency counts");
    assertTrue(sawFinal, "Distributed MODE must retain its final result types");
  }

  @Test(dataProvider = "physicalOptimizers")
  public void testModeExpressionsAndTieBreakers(boolean usePhysicalOptimizer) {
    DispatchableSubPlan plan = _queryEnvironment.planQuery("SET usePhysicalOptimizer=" + usePhysicalOptimizer + "; "
        + "SET autoRewriteAggregationType=true; "
        + "SELECT col2, MODE(NULLIF(col1, ''), 'MIN'), MODE(ts_timestamp, 'MAX'), MODE(col3, 'AVG') "
        + "FROM a GROUP BY col2");
    PlanNode root = plan.getQueryStageMap().get(0).getPlanFragment().getFragmentRoot();
    assertEquals(root.getDataSchema().getColumnDataTypes(),
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.TIMESTAMP,
            ColumnDataType.DOUBLE});
    for (AggregateNode aggregate : findAggregates(plan)) {
      assertEquals(aggregate.getAggCalls().stream().map(RexExpression.FunctionCall::getFunctionName).toList(),
          List.of("MODESTRING", "MODETIMESTAMP", "MODE"));
    }
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
