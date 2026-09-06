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
package org.apache.pinot.query.service.dispatch;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.runtime.PairList;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.SetOpNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


/// Tests the fixed dispatch order and preservation of streaming connectivity.
public class StageDispatchGraphTest {
  private static final DataSchema DATA_SCHEMA =
      new DataSchema(new String[]{"col"}, new ColumnDataType[]{ColumnDataType.INT});

  @Test
  public void testMaterializedBoundaryPreservesStreamingGroups() {
    assertEquals(StageDispatchGraph.create(plan(List.of(0, 1, 2, 3, 4),
        streaming(1, 0), materialized(2, 1), streaming(3, 2), pipelineBreaker(4, 3))),
        List.of(Set.of(2, 3, 4), Set.of(0, 1)));
  }

  @Test
  public void testStreamingOnlyPlanDispatchesTogether() {
    assertEquals(StageDispatchGraph.create(plan(List.of(0, 1, 2),
        streaming(1, 0), pipelineBreaker(2, 1))), List.of(Set.of(0, 1, 2)));
  }

  @Test
  public void testReadyGroupOrderIsDeterministic() {
    List<Edge> edges = List.of(materialized(4, 0), materialized(2, 0), materialized(3, 0));

    List<Set<Integer>> first = StageDispatchGraph.create(plan(List.of(0, 4, 2, 3), edges.toArray(Edge[]::new)));
    List<Set<Integer>> second = StageDispatchGraph.create(plan(List.of(3, 2, 4, 0), edges.toArray(Edge[]::new)));

    List<Set<Integer>> expected = List.of(Set.of(2), Set.of(3), Set.of(4), Set.of(0));
    assertEquals(first, expected);
    assertEquals(second, expected);
  }

  @Test
  public void testCyclesRejectedBeforeDispatch() {
    assertThrows(IllegalArgumentException.class, () -> StageDispatchGraph.create(plan(List.of(0, 1),
        materialized(0, 1), materialized(1, 0))));
    assertThrows(IllegalArgumentException.class, () -> StageDispatchGraph.create(plan(List.of(0, 1, 2),
        streaming(1, 0), streaming(2, 0), materialized(2, 1))));
  }

  private static DispatchableSubPlan plan(List<Integer> stageOrder, Edge... edges) {
    Map<Integer, List<PlanNode>> receivesByStage = new LinkedHashMap<>();
    for (int stageId : stageOrder) {
      receivesByStage.put(stageId, new ArrayList<>());
    }
    for (Edge edge : edges) {
      receivesByStage.get(edge._receiverStageId).add(new MailboxReceiveNode(edge._receiverStageId, DATA_SCHEMA,
          edge._senderStageId, edge._exchangeType, RelDistribution.Type.HASH_DISTRIBUTED, List.of(), List.of(), false,
          false, null, edge._materialized));
    }

    Map<Integer, DispatchablePlanFragment> fragments = new LinkedHashMap<>();
    for (Map.Entry<Integer, List<PlanNode>> entry : receivesByStage.entrySet()) {
      int stageId = entry.getKey();
      List<PlanNode> receives = entry.getValue();
      PlanNode root;
      if (receives.isEmpty()) {
        root = new ValueNode(stageId, DATA_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), List.of());
      } else if (receives.size() == 1) {
        root = receives.get(0);
      } else {
        root = new SetOpNode(stageId, DATA_SCHEMA, PlanNode.NodeHint.EMPTY, receives, SetOpNode.SetOpType.UNION, true);
      }
      fragments.put(stageId, new DispatchablePlanFragment(new PlanFragment(stageId, root, List.of())));
    }
    return new DispatchableSubPlan(PairList.of(0, "col"), fragments, Set.of(), Map.of(), 0);
  }

  private static Edge materialized(int senderStageId, int receiverStageId) {
    return new Edge(senderStageId, receiverStageId, PinotRelExchangeType.STREAMING, true);
  }

  private static Edge streaming(int senderStageId, int receiverStageId) {
    return new Edge(senderStageId, receiverStageId, PinotRelExchangeType.STREAMING, false);
  }

  private static Edge pipelineBreaker(int senderStageId, int receiverStageId) {
    return new Edge(senderStageId, receiverStageId, PinotRelExchangeType.PIPELINE_BREAKER, false);
  }

  private static final class Edge {
    private final int _senderStageId;
    private final int _receiverStageId;
    private final PinotRelExchangeType _exchangeType;
    private final boolean _materialized;

    private Edge(int senderStageId, int receiverStageId, PinotRelExchangeType exchangeType, boolean materialized) {
      _senderStageId = senderStageId;
      _receiverStageId = receiverStageId;
      _exchangeType = exchangeType;
      _materialized = materialized;
    }
  }
}
