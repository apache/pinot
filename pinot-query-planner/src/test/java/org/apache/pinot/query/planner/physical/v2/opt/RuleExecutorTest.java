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
package org.apache.pinot.query.planner.physical.v2.opt;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelNode;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.PinotDataDistribution;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;


public class RuleExecutorTest {
  @Test
  public void testLeftInputFirstRuleExecutor() {
    {
      /*
       *       1
       *     /   \
       *    2     4
       *   /     /
       *  5     3
       */
      PRelNode node5 = pRelNode(5, List.of());
      PRelNode node2 = pRelNode(2, List.of(node5));
      PRelNode node3 = pRelNode(3, List.of());
      PRelNode node4 = pRelNode(4, List.of(node3));
      PRelNode node1 = pRelNode(1, List.of(node2, node4));
      List<Integer> expectedOrder = List.of(5, 2, 1, 3, 4);
      Map<Integer, Collection<PRelNode>> nodeIdToParents = new HashMap<>();
      List<Integer> visitOrder = new ArrayList<>();
      MockPRelOptRule rule = new MockPRelOptRule(nodeIdToParents, visitOrder);
      assertEquals(new LeftInputFirstRuleExecutor(rule, mock(PhysicalPlannerContext.class)).execute(node1), node1);
      assertEquals(visitOrder, expectedOrder);
    }
    {
      /*
       *       1
       *     /   \
       *    2     4
       *   / \   / \
       *  5   8 3   7
       */
      PRelNode node5 = pRelNode(5, List.of());
      PRelNode node8 = pRelNode(8, List.of());
      PRelNode node2 = pRelNode(2, List.of(node5, node8));
      PRelNode node3 = pRelNode(3, List.of());
      PRelNode node7 = pRelNode(7, List.of());
      PRelNode node4 = pRelNode(4, List.of(node3, node7));
      PRelNode node1 = pRelNode(1, List.of(node2, node4));
      List<Integer> expectedOrder = List.of(5, 2, 8, 1, 3, 4, 7);
      Map<Integer, Collection<PRelNode>> nodeIdToParents = new HashMap<>();
      List<Integer> visitOrder = new ArrayList<>();
      MockPRelOptRule rule = new MockPRelOptRule(nodeIdToParents, visitOrder);
      assertEquals(new LeftInputFirstRuleExecutor(rule, mock(PhysicalPlannerContext.class)).execute(node1), node1);
      assertEquals(visitOrder, expectedOrder);
    }
  }

  @Test
  public void testPostOrderRuleExecutor() {
    {
      /*
       *       1
       *     /   \
       *    2     4
       *   /     /
       *  5     3
       */
      PRelNode node5 = pRelNode(5, List.of());
      PRelNode node2 = pRelNode(2, List.of(node5));
      PRelNode node3 = pRelNode(3, List.of());
      PRelNode node4 = pRelNode(4, List.of(node3));
      PRelNode node1 = pRelNode(1, List.of(node2, node4));
      List<Integer> expectedOrder = List.of(5, 2, 3, 4, 1);
      Map<Integer, Collection<PRelNode>> nodeIdToParents = new HashMap<>();
      List<Integer> visitOrder = new ArrayList<>();
      MockPRelOptRule rule = new MockPRelOptRule(nodeIdToParents, visitOrder);
      assertEquals(new PostOrderRuleExecutor(rule, mock(PhysicalPlannerContext.class)).execute(node1), node1);
      assertEquals(visitOrder, expectedOrder);
    }
    {
      /*
       *       1
       *     /   \
       *    2     4
       *   / \   / \
       *  5   8 3   7
       */
      PRelNode node5 = pRelNode(5, List.of());
      PRelNode node8 = pRelNode(8, List.of());
      PRelNode node2 = pRelNode(2, List.of(node5, node8));
      PRelNode node3 = pRelNode(3, List.of());
      PRelNode node7 = pRelNode(7, List.of());
      PRelNode node4 = pRelNode(4, List.of(node3, node7));
      PRelNode node1 = pRelNode(1, List.of(node2, node4));
      List<Integer> expectedOrder = List.of(5, 8, 2, 3, 7, 4, 1);
      Map<Integer, Collection<PRelNode>> nodeIdToParents = new HashMap<>();
      List<Integer> visitOrder = new ArrayList<>();
      MockPRelOptRule rule = new MockPRelOptRule(nodeIdToParents, visitOrder);
      assertEquals(new PostOrderRuleExecutor(rule, mock(PhysicalPlannerContext.class)).execute(node1), node1);
      assertEquals(visitOrder, expectedOrder);
    }
  }

  private static PRelNode pRelNode(int nodeId, List<PRelNode> inputs) {
    return new MockPRelNode(nodeId, inputs);
  }

  static class MockPRelOptRule extends PRelOptRule {
    final Map<Integer, Collection<PRelNode>> _nodeIdToParents;
    final List<Integer> _visitOrder;

    public MockPRelOptRule(Map<Integer, Collection<PRelNode>> nodeIdToParentsSink, List<Integer> visitOrderSink) {
      _nodeIdToParents = nodeIdToParentsSink;
      _visitOrder = visitOrderSink;
    }

    @Override
    public PRelNode onMatch(PRelOptRuleCall call) {
      int nodeId = call._currentNode.getNodeId();
      _nodeIdToParents.put(nodeId, new ArrayList<>(call._parents));
      _visitOrder.add(nodeId);
      return call._currentNode;
    }
  }

  static class MockPRelNode implements PRelNode {
    final int _nodeId;
    final List<PRelNode> _inputs;

    public MockPRelNode(int nodeId, List<PRelNode> inputs) {
      _nodeId = nodeId;
      _inputs = inputs;
    }

    @Override
    public int getNodeId() {
      return _nodeId;
    }

    @Override
    public List<PRelNode> getPRelInputs() {
      return _inputs;
    }

    @Override
    public RelNode unwrap() {
      throw new UnsupportedOperationException("Mock does not support unwrap");
    }

    @Nullable
    @Override
    public PinotDataDistribution getPinotDataDistribution() {
      throw new UnsupportedOperationException("Mock does not support getPinotDataDistribution");
    }

    @Override
    public boolean isLeafStage() {
      throw new UnsupportedOperationException("Mock does not support inspecting leaf stage");
    }

    @Override
    public PRelNode with(int newNodeId, List<PRelNode> newInputs, PinotDataDistribution newDistribution) {
      return new MockPRelNode(newNodeId, newInputs);
    }
  }
}
