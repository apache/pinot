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
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;
import org.apache.pinot.query.planner.plannode.EnrichedJoinNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.UnnestNode;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


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

  /// Enriched joins have been removed, but {@link EnrichedJoinNode}, proto field 17 and the serde are retained so a
  /// plan produced by an older-version broker still round-trips (see {@link EnrichedJoinNode} deprecation note). The
  /// planner no longer produces this node, so this direct round-trip is the only guard on that wire format. Because
  /// {@code JoinNode#equals} ignores the enriched-specific fields, assert on them explicitly rather than via equals.
  @Test
  @SuppressWarnings("deprecation")
  public void testEnrichedJoinNodeSerDe() {
    DataSchema joinResultSchema = new DataSchema(new String[]{"l0", "r0"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT});
    DataSchema projectResultSchema = new DataSchema(new String[]{"p0"},
        new ColumnDataType[]{ColumnDataType.INT});
    List<EnrichedJoinNode.FilterProjectRex> filterProjectRexes = List.of(
        new EnrichedJoinNode.FilterProjectRex(new RexExpression.InputRef(0)),
        new EnrichedJoinNode.FilterProjectRex(List.of(new RexExpression.InputRef(1)), projectResultSchema));
    EnrichedJoinNode node = new EnrichedJoinNode(1, joinResultSchema, projectResultSchema, PlanNode.NodeHint.EMPTY,
        new ArrayList<>(), JoinRelType.INNER, List.of(0), List.of(0), List.of(), JoinNode.JoinStrategy.HASH, null,
        filterProjectRexes, 10, 5);

    EnrichedJoinNode deserialized = (EnrichedJoinNode) PlanNodeDeserializer.process(PlanNodeSerializer.process(node));
    assertEquals(deserialized.getFetch(), 10);
    assertEquals(deserialized.getOffset(), 5);
    assertEquals(deserialized.getJoinResultSchema(), joinResultSchema);
    assertEquals(deserialized.getDataSchema(), projectResultSchema);
    List<EnrichedJoinNode.FilterProjectRex> roundTripped = deserialized.getFilterProjectRexes();
    assertEquals(roundTripped.size(), 2);
    assertEquals(roundTripped.get(0).getType(), EnrichedJoinNode.FilterProjectRexType.FILTER);
    assertEquals(roundTripped.get(0).getFilter(), new RexExpression.InputRef(0));
    assertEquals(roundTripped.get(1).getType(), EnrichedJoinNode.FilterProjectRexType.PROJECT);
    assertEquals(roundTripped.get(1).getProjectAndResultSchema().getProject(),
        List.of(new RexExpression.InputRef(1)));
    assertEquals(roundTripped.get(1).getProjectAndResultSchema().getSchema(), projectResultSchema);
  }

  @Test
  public void testAggregateGroupingSetsSerDe() {
    /// The grouping sets (member indexes over the union group keys, in ordinal order) must survive serialization
    /// to the worker. ROLLUP(g0, g1) over the union {g0, g1} expands to the sets (g0, g1), (g0), (). The empty
    /// grand-total set in particular must round-trip as an entry (not vanish as a proto default).
    DataSchema schema = new DataSchema(new String[]{"g0", "g1", "sum"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT, ColumnDataType.DOUBLE});
    List<List<Integer>> groupingSets = List.of(List.of(0, 1), List.of(0), List.of());
    AggregateNode node = new AggregateNode(0, schema, PlanNode.NodeHint.EMPTY, List.of(), List.of(), List.of(),
        List.of(0, 1), AggType.DIRECT, false, List.of(), 0, groupingSets);
    AggregateNode deserialized = (AggregateNode) PlanNodeDeserializer.process(PlanNodeSerializer.process(node));
    assertEquals(deserialized.getGroupingSets(), groupingSets);
    assertEquals(deserialized, node);
  }
}
