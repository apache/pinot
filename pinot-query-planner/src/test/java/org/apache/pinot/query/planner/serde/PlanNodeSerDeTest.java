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
package org.apache.pinot.query.planner.serde;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.proto.Plan;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.GroupingSetsExpandNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.UnnestNode;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class PlanNodeSerDeTest extends QueryEnvironmentTestBase {

  @Test(dataProvider = "testQueryDataProvider")
  public void testQueryStagePlanSerDe(String query) {
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(query);
    for (DispatchablePlanFragment dispatchablePlanFragment : dispatchableSubPlan.getQueryStages()) {
      PlanNode stagePlan = dispatchablePlanFragment.getPlanFragment().getFragmentRoot();
      PlanNode deserializedStagePlan = PlanNodeDeserializer.process(PlanNodeSerializer.process(stagePlan));
      assertEquals(stagePlan, deserializedStagePlan);
    }
  }

  @Test
  public void testPrunedUnnestNodeSerDe() {
    // Round-trips the passthrough-pruning wire fields (passthroughInputIndexes, prunedPassthrough). A non-sequential
    // index list plus WITH ORDINALITY exercise the proto repeated/bool fields and ordering.
    DataSchema dataSchema = new DataSchema(new String[]{"col0", "col2", "elem", "ord"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.INT});
    UnnestNode.TableFunctionContext context =
        new UnnestNode.TableFunctionContext(true, List.of(2), 3, List.of(0, 2), true);
    UnnestNode node = new UnnestNode(1, dataSchema, PlanNode.NodeHint.EMPTY, new ArrayList<>(),
        List.of(new RexExpression.InputRef(1)), context);

    PlanNode deserialized = PlanNodeDeserializer.process(PlanNodeSerializer.process(node));
    assertEquals(deserialized, node);
    UnnestNode deserializedUnnest = (UnnestNode) deserialized;
    assertEquals(deserializedUnnest.getPassthroughInputIndexes(), List.of(0, 2));
    assertEquals(deserializedUnnest.isPrunedPassthrough(), true);
    assertEquals(deserializedUnnest.getOrdinalityIndex(), 3);
  }

  @Test
  public void testLegacyUnnestNodeSerDe() {
    // A non-pruned UnnestNode must round-trip with prunedPassthrough=false and an empty passthrough map (the wire
    // default an old broker produces).
    DataSchema dataSchema = new DataSchema(new String[]{"id", "arr", "elem"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT_ARRAY, ColumnDataType.INT});
    UnnestNode node = new UnnestNode(1, dataSchema, PlanNode.NodeHint.EMPTY, new ArrayList<>(),
        new RexExpression.InputRef(1), "elem", false, null);

    UnnestNode deserialized = (UnnestNode) PlanNodeDeserializer.process(PlanNodeSerializer.process(node));
    assertEquals(deserialized, node);
    assertEquals(deserialized.isPrunedPassthrough(), false);
    assertEquals(deserialized.getPassthroughInputIndexes(), List.of());
  }

  @Test
  public void testGroupingSetsExpandSerDe() {
    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(
        "SELECT col1, col2, SUM(col3) FROM a GROUP BY ROLLUP(col1, col2)");
    boolean foundExpand = false;
    for (DispatchablePlanFragment dispatchablePlanFragment : dispatchableSubPlan.getQueryStages()) {
      PlanNode stagePlan = dispatchablePlanFragment.getPlanFragment().getFragmentRoot();
      PlanNode deserializedStagePlan = PlanNodeDeserializer.process(PlanNodeSerializer.process(stagePlan));
      assertEquals(stagePlan, deserializedStagePlan);
      foundExpand |= containsExpand(stagePlan);
    }
    assertTrue(foundExpand, "ROLLUP plan should contain a GroupingSetsExpandNode that round-trips through serde");
  }

  @Test
  public void testUnknownNodeTypeFailsClosed() {
    // A plan node with no recognized oneof case - e.g. a newer broker's node type reaching an older server - must
    // hard-fail rather than be silently mis-read. Underpins the GroupingSetsExpandNode rolling-upgrade contract.
    assertThrows(IllegalStateException.class, () -> PlanNodeDeserializer.process(Plan.PlanNode.newBuilder().build()));
  }

  private static boolean containsExpand(PlanNode node) {
    if (node instanceof GroupingSetsExpandNode) {
      return true;
    }
    for (PlanNode input : node.getInputs()) {
      if (containsExpand(input)) {
        return true;
      }
    }
    return false;
  }
}
