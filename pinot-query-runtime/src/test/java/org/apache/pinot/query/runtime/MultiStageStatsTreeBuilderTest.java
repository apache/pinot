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
package org.apache.pinot.query.runtime;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.response.broker.BrokerResponseNativeV2;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OperatorTypeDescriptor;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.StageStatsTreeNode;
import org.testng.Assert;
import org.testng.annotations.Test;


/// Tests the explicit-tree `stageStats` rendering mode of [MultiStageStatsTreeBuilder], in particular
/// with plugin-defined operator types (ids >= [OperatorTypeDescriptor#PLUGIN_ID_FLOOR]) whose
/// [StatMap.Key] enums are unknown to the built-in [MultiStageOperator.Type] world.
public class MultiStageStatsTreeBuilderTest {

  private static final DataSchema SCHEMA =
      new DataSchema(new String[]{"col"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT});

  /// A plugin-only stat enum, intentionally unrelated to any built-in operator's StatKey.
  public enum TestPluginStat implements StatMap.Key {
    EMITTED_ROWS(StatMap.Type.LONG),
    CUSTOM_COUNTER(StatMap.Type.LONG);

    private final StatMap.Type _type;

    TestPluginStat(StatMap.Type type) {
      _type = type;
    }

    @Override
    public StatMap.Type getType() {
      return _type;
    }
  }

  private static OperatorTypeDescriptor pluginType(int id, String name) {
    return new OperatorTypeDescriptor() {
      @Override
      public int getId() {
        return id;
      }

      @Override
      public String name() {
        return name;
      }

      @Override
      @SuppressWarnings("rawtypes")
      public Class getStatKeyClass() {
        return TestPluginStat.class;
      }

      @Override
      public void mergeInto(BrokerResponseNativeV2 response, StatMap<?> map) {
      }
    };
  }

  private static MailboxReceiveNode receiveNode(int stageId, int senderStageId) {
    return new MailboxReceiveNode(stageId, SCHEMA, senderStageId, PinotRelExchangeType.STREAMING,
        RelDistribution.Type.HASH_DISTRIBUTED, null, null, false, false, null);
  }

  private static MailboxSendNode sendNode(int stageId, List<PlanNode> inputs, int receiverStageId) {
    return new MailboxSendNode(stageId, SCHEMA, inputs, receiverStageId, PinotRelExchangeType.STREAMING,
        RelDistribution.Type.HASH_DISTRIBUTED, null, false, null, false, "murmur");
  }

  private static Map<Integer, DispatchablePlanFragment> fragments(Map<Integer, PlanNode> rootsByStage) {
    Map<Integer, DispatchablePlanFragment> result = new java.util.HashMap<>();
    rootsByStage.forEach(
        (stage, root) -> result.put(stage, new DispatchablePlanFragment(new PlanFragment(stage, root, List.of()))));
    return result;
  }

  private static StatMap<TestPluginStat> pluginStats(long emittedRows, long customCounter) {
    StatMap<TestPluginStat> stats = new StatMap<>(TestPluginStat.class);
    stats.merge(TestPluginStat.EMITTED_ROWS, emittedRows);
    stats.merge(TestPluginStat.CUSTOM_COUNTER, customCounter);
    return stats;
  }

  /// A stage tree made exclusively of plugin types must render fully (no node dropped, custom stat fields present)
  /// and must nest the sender stage's tree under the node whose plan-node id resolves to a
  /// [MailboxReceiveNode] — even though the receive node carries a plugin type, not the built-in
  /// MAILBOX_RECEIVE.
  @Test
  public void testPluginTypedTreeRendersWithCrossStageNesting() {
    OperatorTypeDescriptor sendType = pluginType(300, "TEST_PLUGIN_SEND");
    OperatorTypeDescriptor receiveType = pluginType(301, "TEST_PLUGIN_RECEIVE");

    // Stage 1: send(0) -> receive(1) [pre-order ids]; stage 2: send(0) -> value(1).
    MailboxReceiveNode stage1Receive = receiveNode(1, 2);
    PlanNode stage1Root = sendNode(1, List.of(stage1Receive), 0);
    PlanNode stage2Root = sendNode(2,
        List.of(new ValueNode(2, SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), List.of())), 1);

    StageStatsTreeNode stage1Tree = new StageStatsTreeNode(sendType, List.of(0), pluginStats(10, 7), List.of(
        new StageStatsTreeNode(receiveType, List.of(1), pluginStats(10, 3), List.of())));
    StageStatsTreeNode stage2Tree = new StageStatsTreeNode(sendType, List.of(0), pluginStats(5, 1), List.of());

    MultiStageStatsTreeBuilder builder = new MultiStageStatsTreeBuilder(
        fragments(Map.of(1, stage1Root, 2, stage2Root)), List.of(), Map.of(1, stage1Tree, 2, stage2Tree));
    ObjectNode json = builder.jsonStatsByStage(1);

    Assert.assertEquals(json.get("type").asText(), "TEST_PLUGIN_SEND");
    Assert.assertEquals(json.get("customCounter").asLong(), 7);
    Assert.assertEquals(json.get("emittedRows").asLong(), 10);
    Assert.assertEquals(json.get("planNodeIds").get(0).asInt(), 0);

    ObjectNode receive = (ObjectNode) json.get("children").get(0);
    Assert.assertEquals(receive.get("type").asText(), "TEST_PLUGIN_RECEIVE");
    Assert.assertEquals(receive.get("customCounter").asLong(), 3);

    ObjectNode nestedStage2 = (ObjectNode) receive.get("children").get(0);
    Assert.assertEquals(nestedStage2.get("type").asText(), "TEST_PLUGIN_SEND");
    Assert.assertEquals(nestedStage2.get("emittedRows").asLong(), 5);
  }

  /// PIPELINE_BREAKER nodes must be collapsed (children hoisted into the parent) so the JSON shape matches the
  /// legacy renderer, which nests the breaker's receive operators directly under the LEAF.
  @Test
  public void testPipelineBreakerCollapsed() {
    MultiStageOperator.Type leaf = MultiStageOperator.Type.LEAF;
    MultiStageOperator.Type breaker = MultiStageOperator.Type.PIPELINE_BREAKER;
    MultiStageOperator.Type receive = MultiStageOperator.Type.MAILBOX_RECEIVE;

    StageStatsTreeNode tree = new StageStatsTreeNode(leaf, List.of(), statsOf(leaf), List.of(
        new StageStatsTreeNode(breaker, List.of(), statsOf(breaker), List.of(
            new StageStatsTreeNode(receive, List.of(), statsOf(receive), List.of())))));

    MultiStageStatsTreeBuilder builder = new MultiStageStatsTreeBuilder(
        fragments(Map.of(1, sendNode(1, List.of(), 0))), List.of(), Map.of(1, tree));
    ObjectNode json = builder.jsonStatsByStage(1);

    Assert.assertEquals(json.get("type").asText(), "LEAF");
    Assert.assertEquals(json.get("children").size(), 1, "PIPELINE_BREAKER must be collapsed, not rendered");
    Assert.assertEquals(json.get("children").get(0).get("type").asText(), "MAILBOX_RECEIVE");
  }

  /// Legacy-path regression: a flat stats list with a plugin type at the send position must not throw
  /// `ArithmeticException` (the built-in PARALLELISM key cannot be read from a foreign StatMap, which used to
  /// yield a zero divisor).
  @Test
  public void testLegacyPathWithPluginSendTypeDoesNotThrow() {
    OperatorTypeDescriptor sendType = pluginType(300, "TEST_PLUGIN_SEND");
    OperatorTypeDescriptor receiveType = pluginType(301, "TEST_PLUGIN_RECEIVE");

    MailboxReceiveNode stage1Receive = receiveNode(1, 2);
    PlanNode stage1Root = sendNode(1, List.of(stage1Receive), 0);
    PlanNode stage2Root = sendNode(2,
        List.of(new ValueNode(2, SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), List.of())), 1);
    // Inorder flat list: child (receive) first, then the send root.
    MultiStageQueryStats.StageStats.Closed flatStats = new MultiStageQueryStats.StageStats.Closed(
        List.of(receiveType, sendType), List.of(pluginStats(10, 3), pluginStats(10, 7)));

    java.util.List<MultiStageQueryStats.StageStats.Closed> queryStats = new java.util.ArrayList<>();
    queryStats.add(null); // stage 0
    queryStats.add(flatStats); // stage 1
    queryStats.add(null); // stage 2: no stats — renders the EMPTY_MAILBOX_SEND placeholder
    MultiStageStatsTreeBuilder builder =
        new MultiStageStatsTreeBuilder(fragments(Map.of(1, stage1Root, 2, stage2Root)), queryStats);

    // The legacy renderer cannot pair plugin types against the PlanNode tree, so the nodes are not rendered —
    // but it must not throw either.
    ObjectNode json = builder.jsonStatsByStage(1);
    Assert.assertNotNull(json);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static StatMap<?> statsOf(MultiStageOperator.Type type) {
    return new StatMap(type.getStatKeyClass());
  }

  /// The estimate must land on the node whose actual count it should be compared against, so that
  /// reading estimate error off the stats needs no cross-referencing.
  @Test
  public void testEstimatedRowsRenderedNextToActualCounts() {
    OperatorTypeDescriptor sendType = pluginType(300, "TEST_PLUGIN_SEND");
    OperatorTypeDescriptor receiveType = pluginType(301, "TEST_PLUGIN_RECEIVE");
    MailboxReceiveNode stage1Receive = receiveNode(1, 2);
    PlanNode stage1Root = sendNode(1, List.of(stage1Receive), 0);
    PlanNode stage2Root = sendNode(2,
        List.of(new ValueNode(2, SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), List.of())), 1);

    StageStatsTreeNode stage1Tree = new StageStatsTreeNode(sendType, List.of(0), pluginStats(10, 7), List.of(
        new StageStatsTreeNode(receiveType, List.of(1), pluginStats(10, 3), List.of())));
    StageStatsTreeNode stage2Tree = new StageStatsTreeNode(sendType, List.of(0), pluginStats(5, 1), List.of());

    // Identity-keyed, matching what the planner supplies: structurally equal nodes are distinct
    // plan nodes with potentially different estimates.
    Map<PlanNode, Double> estimates = new IdentityHashMap<>();
    estimates.put(stage1Root, 1234.0);
    estimates.put(stage1Receive, 99.0);

    MultiStageStatsTreeBuilder builder = new MultiStageStatsTreeBuilder(
        fragments(Map.of(1, stage1Root, 2, stage2Root)), List.of(), Map.of(1, stage1Tree, 2, stage2Tree),
        estimates);
    ObjectNode json = builder.jsonStatsByStage(1);

    // The actual count stays untouched next to the estimate -- the pair is the whole point.
    Assert.assertEquals(json.get("emittedRows").asLong(), 10);
    Assert.assertEquals(json.get("estimatedRows").asDouble(), 1234.0, 1e-9);

    ObjectNode receive = (ObjectNode) json.get("children").get(0);
    Assert.assertEquals(receive.get("emittedRows").asLong(), 10);
    Assert.assertEquals(receive.get("estimatedRows").asDouble(), 99.0, 1e-9);
  }

  /// Ordinary queries must not carry the extra fields, so an empty map renders nothing at all --
  /// not a zero, which a consumer would read as "the optimizer expected no rows".
  @Test
  public void testEstimatedRowsAbsentWhenNotRequested() {
    OperatorTypeDescriptor sendType = pluginType(300, "TEST_PLUGIN_SEND");
    PlanNode stage1Root = sendNode(1, List.of(receiveNode(1, 2)), 0);
    PlanNode stage2Root = sendNode(2,
        List.of(new ValueNode(2, SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), List.of())), 1);
    StageStatsTreeNode stage1Tree = new StageStatsTreeNode(sendType, List.of(0), pluginStats(10, 7), List.of());

    MultiStageStatsTreeBuilder builder = new MultiStageStatsTreeBuilder(
        fragments(Map.of(1, stage1Root, 2, stage2Root)), List.of(), Map.of(1, stage1Tree));
    ObjectNode json = builder.jsonStatsByStage(1);

    Assert.assertEquals(json.get("emittedRows").asLong(), 10);
    Assert.assertNull(json.get("estimatedRows"), "estimatedRows must be absent unless requested");
  }

  /// A stats node can cover several plan nodes (operators fuse; a leaf stage compiles its whole
  /// pushed-down subtree into one operator). There is one actual count for the group, and it
  /// represents the OUTPUT of the topmost node, so that is the estimate reported. Attributing to
  /// another member would silently pair unrelated numbers.
  ///
  /// This pins the renderer's half of the contract: given a group, it reports the first member's
  /// estimate and not another member's. The other half -- that the first id really is the group's
  /// topmost node -- is established by `PlanNodeToOpChain#collectPlanNodeSubTree`, which walks
  /// pre-order but is private, so it cannot be asserted from here.
  @Test
  public void testFusedNodeReportsTheTopmostEstimate() {
    OperatorTypeDescriptor sendType = pluginType(300, "TEST_PLUGIN_SEND");
    MailboxReceiveNode stage1Receive = receiveNode(1, 2);
    PlanNode stage1Root = sendNode(1, List.of(stage1Receive), 0);
    PlanNode stage2Root = sendNode(2,
        List.of(new ValueNode(2, SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), List.of())), 1);

    // One stats node covering both plan nodes, topmost first.
    StageStatsTreeNode fused = new StageStatsTreeNode(sendType, List.of(0, 1), pluginStats(10, 7), List.of());

    Map<PlanNode, Double> estimates = new IdentityHashMap<>();
    estimates.put(stage1Root, 7777.0);
    estimates.put(stage1Receive, 11.0);

    MultiStageStatsTreeBuilder builder = new MultiStageStatsTreeBuilder(
        fragments(Map.of(1, stage1Root, 2, stage2Root)), List.of(), Map.of(1, fused), estimates);
    ObjectNode json = builder.jsonStatsByStage(1);

    Assert.assertEquals(json.get("estimatedRows").asDouble(), 7777.0, 1e-9);
  }

  /// A plan node with no recorded estimate renders no field, rather than a sentinel a reader would
  /// chart as a real value.
  @Test
  public void testMissingEstimateRendersNoField() {
    OperatorTypeDescriptor sendType = pluginType(300, "TEST_PLUGIN_SEND");
    MailboxReceiveNode stage1Receive = receiveNode(1, 2);
    PlanNode stage1Root = sendNode(1, List.of(stage1Receive), 0);
    PlanNode stage2Root = sendNode(2,
        List.of(new ValueNode(2, SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), List.of())), 1);
    StageStatsTreeNode stage1Tree = new StageStatsTreeNode(sendType, List.of(0), pluginStats(10, 7), List.of());

    // Non-empty map, but nothing recorded for the node being rendered.
    Map<PlanNode, Double> estimates = new IdentityHashMap<>();
    estimates.put(stage1Receive, 11.0);

    MultiStageStatsTreeBuilder builder = new MultiStageStatsTreeBuilder(
        fragments(Map.of(1, stage1Root, 2, stage2Root)), List.of(), Map.of(1, stage1Tree), estimates);
    ObjectNode json = builder.jsonStatsByStage(1);

    Assert.assertNull(json.get("estimatedRows"), "A node with no estimate must render no field");
  }

  /// The plan-visiting renderer must annotate too. A query takes one renderer or the other, so if
  /// only the streaming one did, the field would appear for some queries and silently not others.
  /// Built-in operator types on purpose: this renderer pairs stats against the PlanNode tree and
  /// cannot match plugin types, so a plugin-typed fixture would render nothing and prove nothing.
  @Test
  public void testPlanVisitingRendererAlsoReportsEstimatedRows() {
    MailboxReceiveNode stage1Receive = receiveNode(1, 2);
    PlanNode stage1Root = sendNode(1, List.of(stage1Receive), 0);
    PlanNode stage2Root = sendNode(2,
        List.of(new ValueNode(2, SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), List.of())), 1);

    // Inorder flat list: child (receive) first, then the send root.
    MultiStageQueryStats.StageStats.Closed flatStats = new MultiStageQueryStats.StageStats.Closed(
        List.of(MultiStageOperator.Type.MAILBOX_RECEIVE, MultiStageOperator.Type.MAILBOX_SEND),
        List.of(statsOf(MultiStageOperator.Type.MAILBOX_RECEIVE), statsOf(MultiStageOperator.Type.MAILBOX_SEND)));
    List<MultiStageQueryStats.StageStats.Closed> queryStats = new ArrayList<>();
    queryStats.add(null);
    queryStats.add(flatStats);
    queryStats.add(null);

    Map<PlanNode, Double> estimates = new IdentityHashMap<>();
    estimates.put(stage1Root, 4321.0);
    estimates.put(stage1Receive, 21.0);

    // Null stageStatsTrees: the older stats-in-blocks path, which walks the plan.
    MultiStageStatsTreeBuilder builder = new MultiStageStatsTreeBuilder(
        fragments(Map.of(1, stage1Root, 2, stage2Root)), queryStats, null, estimates);
    ObjectNode json = builder.jsonStatsByStage(1);

    Assert.assertEquals(json.get("estimatedRows").asDouble(), 4321.0, 1e-9);
    // The receive node is annotated from its own visit method, not from recursiveCase.
    ObjectNode receive = (ObjectNode) json.get("children").get(0);
    Assert.assertEquals(receive.get("estimatedRows").asDouble(), 21.0, 1e-9);
  }

  /// When the flat stats and the plan disagree, the plan-visiting renderer returns the *child's*
  /// JSON for the parent's visit. The estimate on it must remain the child's.
  ///
  /// This is the ordinary path, not a corner: it is what happens in stage 0, in leaf stages, and
  /// whenever an operator type is not the one the plan predicted. Annotating the parent's estimate
  /// onto that object pairs one operator's prediction with another operator's emitted rows -- which
  /// is what an earlier version of this code did, on the false premise that every return path built
  /// the JSON for the node being visited.
  @Test
  public void testPlanVisitingRendererKeepsTheChildsEstimateOnTypeMismatch() {
    MailboxReceiveNode stage1Receive = receiveNode(1, 2);
    PlanNode stage1Root = sendNode(1, List.of(stage1Receive), 0);
    PlanNode stage2Root = sendNode(2,
        List.of(new ValueNode(2, SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), List.of())), 1);

    // Stats for the receive operator only -- the send has none, so visiting the send takes the
    // mismatch path and hands back the receive's JSON.
    MultiStageQueryStats.StageStats.Closed flatStats = new MultiStageQueryStats.StageStats.Closed(
        List.of(MultiStageOperator.Type.MAILBOX_RECEIVE),
        List.of(statsOf(MultiStageOperator.Type.MAILBOX_RECEIVE)));
    List<MultiStageQueryStats.StageStats.Closed> queryStats = new ArrayList<>();
    queryStats.add(null);
    queryStats.add(flatStats);
    queryStats.add(null);

    Map<PlanNode, Double> estimates = new IdentityHashMap<>();
    estimates.put(stage1Root, 4321.0);
    estimates.put(stage1Receive, 21.0);

    MultiStageStatsTreeBuilder builder = new MultiStageStatsTreeBuilder(
        fragments(Map.of(1, stage1Root, 2, stage2Root)), queryStats, null, estimates);
    ObjectNode json = builder.jsonStatsByStage(1);

    Assert.assertEquals(json.get("type").asText(), MultiStageOperator.Type.MAILBOX_RECEIVE.name(),
        "the mismatch path should have rendered the receive operator");
    Assert.assertEquals(json.get("estimatedRows").asDouble(), 21.0, 1e-9,
        "the receive's own estimate must survive; the send's estimate must not overwrite it");
  }

  /// And it must stay silent when nothing was requested, for the same reason as the streaming path.
  @Test
  public void testPlanVisitingRendererOmitsEstimatedRowsWhenNotRequested() {
    MailboxReceiveNode stage1Receive = receiveNode(1, 2);
    PlanNode stage1Root = sendNode(1, List.of(stage1Receive), 0);
    PlanNode stage2Root = sendNode(2,
        List.of(new ValueNode(2, SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), List.of())), 1);

    MultiStageQueryStats.StageStats.Closed flatStats = new MultiStageQueryStats.StageStats.Closed(
        List.of(MultiStageOperator.Type.MAILBOX_RECEIVE, MultiStageOperator.Type.MAILBOX_SEND),
        List.of(statsOf(MultiStageOperator.Type.MAILBOX_RECEIVE), statsOf(MultiStageOperator.Type.MAILBOX_SEND)));
    List<MultiStageQueryStats.StageStats.Closed> queryStats = new ArrayList<>();
    queryStats.add(null);
    queryStats.add(flatStats);
    queryStats.add(null);

    MultiStageStatsTreeBuilder builder = new MultiStageStatsTreeBuilder(
        fragments(Map.of(1, stage1Root, 2, stage2Root)), queryStats);
    ObjectNode json = builder.jsonStatsByStage(1);

    Assert.assertNull(json.get("estimatedRows"), "estimatedRows must be absent unless requested");
  }
}
