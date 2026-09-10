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
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


/// Verifies array and arbitrary-value aggregate bindings across planner stages while retaining explicit SQL forms.
public class PolymorphicAggregateExtensionsPlannerTest extends QueryEnvironmentTestBase {
  @DataProvider
  public Object[][] physicalOptimizers() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "physicalOptimizers")
  public void testInferredArrayAndScalarSchemas(boolean physicalOptimizer) {
    DispatchableSubPlan plan = plan(physicalOptimizer, "SELECT ARRAY_AGG(col3), ARRAY_AGG(col1, true), "
        + "ARRAY_AGG(col5), ARRAY_AGG(ts_timestamp, true), ARRAY_AGG(col7), ANY_VALUE(col5), "
        + "ANY_VALUE(ts_timestamp), ANY_VALUE(col7) FROM a");
    ColumnDataType[] resultTypes = {ColumnDataType.INT_ARRAY, ColumnDataType.STRING_ARRAY,
        ColumnDataType.BOOLEAN_ARRAY, ColumnDataType.TIMESTAMP_ARRAY, ColumnDataType.LONG_ARRAY,
        ColumnDataType.BOOLEAN, ColumnDataType.TIMESTAMP, ColumnDataType.LONG};
    assertEquals(plan.getQueryStageMap().get(0).getPlanFragment().getFragmentRoot().getDataSchema()
        .getColumnDataTypes(), resultTypes);
    List<AggregateNode> aggregates = aggregates(plan);
    assertFalse(aggregates.isEmpty());
    for (AggregateNode aggregate : aggregates) {
      assertEquals(aggregate.getAggCalls().size(), resultTypes.length);
      for (int i = 0; i < resultTypes.length; i++) {
        AggregateCallBinding binding = aggregate.getAggCalls().get(i).getAggregationBinding();
        assertNotNull(binding);
        assertEquals(binding.getResultType(), resultTypes[i]);
      }
      assertEquals(aggregate.getAggCalls().get(1).getAggregationBinding().getArgumentTypes(),
          List.of(ColumnDataType.STRING, ColumnDataType.BOOLEAN));
    }
  }

  @Test(dataProvider = "physicalOptimizers")
  public void testMultiValueAndDirectBindings(boolean physicalOptimizer) {
    DispatchableSubPlan plan = plan(physicalOptimizer,
        "SELECT /*+ aggOptions(is_skip_leaf_stage_aggregate='true') */ col1, ARRAY_AGG(mcol2), "
            + "ARRAY_AGG(mcol1, true) FROM e GROUP BY col1");
    assertEquals(plan.getQueryStageMap().get(0).getPlanFragment().getFragmentRoot().getDataSchema()
        .getColumnDataTypes(), new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.LONG_ARRAY,
            ColumnDataType.STRING_ARRAY});
    List<AggregateNode> aggregates = aggregates(plan);
    assertFalse(aggregates.isEmpty());
    for (AggregateNode aggregate : aggregates) {
      assertEquals(aggregate.getAggCalls().get(0).getAggregationBinding().getArgumentTypes(),
          List.of(ColumnDataType.LONG_ARRAY));
      assertEquals(aggregate.getAggCalls().get(0).getAggregationBinding().getResultType(), ColumnDataType.LONG_ARRAY);
      assertEquals(aggregate.getAggCalls().get(1).getAggregationBinding().getArgumentTypes(),
          List.of(ColumnDataType.STRING_ARRAY, ColumnDataType.BOOLEAN));
    }
  }

  @Test(dataProvider = "physicalOptimizers")
  public void testLegacyExplicitTypeFormsRemainUnbound(boolean physicalOptimizer) {
    DispatchableSubPlan plan = plan(physicalOptimizer,
        "SELECT ARRAY_AGG(col1, 'STRING'), ARRAY_AGG(col7, 'LONG', true), FIRST_WITH_TIME(col1, ts, 'STRING') FROM a");
    List<AggregateNode> aggregates = aggregates(plan);
    assertFalse(aggregates.isEmpty());
    for (AggregateNode aggregate : aggregates) {
      for (RexExpression.FunctionCall call : aggregate.getAggCalls()) {
        assertNull(call.getAggregationBinding());
      }
    }
  }

  private DispatchableSubPlan plan(boolean physicalOptimizer, String sql) {
    return _queryEnvironment.planQuery("SET usePhysicalOptimizer=" + physicalOptimizer + "; " + sql);
  }

  private static List<AggregateNode> aggregates(DispatchableSubPlan plan) {
    List<AggregateNode> result = new ArrayList<>();
    for (DispatchablePlanFragment fragment : plan.getQueryStageMap().values()) {
      collect(fragment.getPlanFragment().getFragmentRoot(), result);
    }
    return result;
  }

  private static void collect(PlanNode node, List<AggregateNode> result) {
    if (node instanceof AggregateNode) {
      result.add((AggregateNode) node);
    }
    node.getInputs().forEach(input -> collect(input, result));
  }
}
