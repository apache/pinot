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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.StageStatsTreeNode;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Builds the {@code stageStats} JSON tree of the broker response.
 *
 * <p>Two rendering modes per stage:
 * <ul>
 *   <li><b>Explicit tree</b> (stream-mode stats): when a {@link StageStatsTreeNode} is available for the stage, the
 *   JSON is rendered directly from it. This is the only mode that can faithfully render plugin-defined operator
 *   types (ids &gt;= 256), whose shape cannot be re-derived by pairing the flat stats list against the stage's
 *   {@link PlanNode} tree.</li>
 *   <li><b>Plan-node pairing</b> (legacy): the flat per-stage stats list is paired positionally against the stage's
 *   PlanNode tree via {@link InStageStatsTreeBuilder}.</li>
 * </ul>
 */
public class MultiStageStatsTreeBuilder {
  private final Map<Integer, PlanNode> _planNodes;
  private final List<? extends MultiStageQueryStats.StageStats> _queryStats;
  private final Map<Integer, DispatchablePlanFragment> _planFragments;
  @Nullable
  private final Map<Integer, StageStatsTreeNode> _stageStatsTrees;

  public MultiStageStatsTreeBuilder(Map<Integer, DispatchablePlanFragment> planFragments,
      List<? extends MultiStageQueryStats.StageStats> queryStats) {
    this(planFragments, queryStats, null);
  }

  public MultiStageStatsTreeBuilder(Map<Integer, DispatchablePlanFragment> planFragments,
      List<? extends MultiStageQueryStats.StageStats> queryStats,
      @Nullable Map<Integer, StageStatsTreeNode> stageStatsTrees) {
    _planFragments = planFragments;
    _planNodes = Maps.newHashMapWithExpectedSize(planFragments.size());
    for (Map.Entry<Integer, DispatchablePlanFragment> entry : planFragments.entrySet()) {
      _planNodes.put(entry.getKey(), entry.getValue().getPlanFragment().getFragmentRoot());
    }
    _queryStats = queryStats;
    _stageStatsTrees = stageStatsTrees;
  }

  public ObjectNode jsonStatsByStage(int stage) {
    StageStatsTreeNode statsTree = _stageStatsTrees == null ? null : _stageStatsTrees.get(stage);
    if (statsTree != null) {
      return jsonFromStatsTree(statsTree, stage);
    }
    PlanNode planNode = _planNodes.get(stage);

    MultiStageQueryStats.StageStats stageStats = stage < _queryStats.size() ? _queryStats.get(stage) : null;
    if (stageStats == null) {
      // We don't have stats for this stage. This can happen when the stage is not executed. For example, when there
      // are no segments for a table.
      ObjectNode jsonNodes = JsonUtils.newObjectNode();
      jsonNodes.put("type", "EMPTY_MAILBOX_SEND");
      jsonNodes.put("stage", stage);
      jsonNodes.put("description", "No stats available for this stage");
      String tableName = _planFragments.get(stage).getTableName();
      if (tableName != null) {
        jsonNodes.put("table", tableName);
      }
      return jsonNodes;
    }
    InStageStatsTreeBuilder treeBuilder = new InStageStatsTreeBuilder(stageStats, this::jsonStatsByStage);
    return planNode.visit(treeBuilder, new InStageStatsTreeBuilder.Context(1));
  }

  /**
   * Renders a stage's explicit stats tree. The shape (and any plugin operator types) come straight from the decoded
   * tree; the stage's PlanNode tree is consulted only to resolve which plan nodes are {@link MailboxReceiveNode}s so
   * the sender stage's tree can be nested under them, mirroring the legacy renderer's cross-stage nesting.
   */
  private ObjectNode jsonFromStatsTree(StageStatsTreeNode statsTree, int stage) {
    Map<Integer, PlanNode> planNodesById = new HashMap<>();
    PlanNode fragmentRoot = _planNodes.get(stage);
    if (fragmentRoot != null) {
      assignPlanNodeIds(fragmentRoot, planNodesById, new int[]{0});
    }
    // The stage root is the send operator: its parallelism (when reported, e.g. by the built-in MAILBOX_SEND stats)
    // scales cpu-time into wall-clock time for the whole stage. Plugin send types may not report it; default to 1.
    int parallelism = 1;
    JsonNode rootParallelism = statsTree.getStatMap().asJson().get("parallelism");
    if (rootParallelism != null) {
      parallelism = Math.max(1, rootParallelism.asInt(1));
    }
    return jsonFromStatsTreeNode(statsTree, planNodesById, parallelism);
  }

  private ObjectNode jsonFromStatsTreeNode(StageStatsTreeNode node, Map<Integer, PlanNode> planNodesById,
      int parallelism) {
    ObjectNode json = JsonUtils.newObjectNode();
    json.put("type", node.getType().name());
    for (Map.Entry<String, JsonNode> entry : node.getStatMap().asJson().properties()) {
      json.set(entry.getKey(), entry.getValue());
    }
    if (json.get("parallelism") == null) {
      json.put("parallelism", parallelism);
    }
    JsonNode executionTimeMs = json.get("executionTimeMs");
    if (executionTimeMs != null) {
      json.put("clockTimeMs", executionTimeMs.asLong(0) / parallelism);
    }

    if (!node.getPlanNodeIds().isEmpty()) {
      ArrayNode planNodeIds = JsonUtils.newArrayNode();
      node.getPlanNodeIds().forEach(planNodeIds::add);
      json.set("planNodeIds", planNodeIds);
    }

    ArrayNode children = JsonUtils.newArrayNode();
    for (StageStatsTreeNode child : node.getChildren()) {
      // Collapse PIPELINE_BREAKER nodes for parity with the legacy renderer, which nests the
      // breaker's receive operators directly under the LEAF and never renders the breaker itself.
      // The JSON shape is consumed by external tooling, so the explicit-tree mode must not change it.
      if (child.getType().getId() == MultiStageOperator.Type.PIPELINE_BREAKER.getId()) {
        for (StageStatsTreeNode grandChild : child.getChildren()) {
          children.add(jsonFromStatsTreeNode(grandChild, planNodesById, parallelism));
        }
      } else {
        children.add(jsonFromStatsTreeNode(child, planNodesById, parallelism));
      }
    }
    // Cross-stage nesting: a node whose plan node is a mailbox receive gets the sender stage's tree as a child.
    // Type-agnostic on purpose — plugin-defined receive operators carry a different descriptor than the built-in
    // MAILBOX_RECEIVE but map to the same MailboxReceiveNode.
    for (Integer planNodeId : node.getPlanNodeIds()) {
      PlanNode planNode = planNodesById.get(planNodeId);
      if (planNode instanceof MailboxReceiveNode) {
        children.add(jsonStatsByStage(((MailboxReceiveNode) planNode).getSenderStageId()));
      }
    }
    if (!children.isEmpty()) {
      json.set("children", children);
    }

    // self* fields: parent-minus-children derived stats, matching InStageStatsTreeBuilder semantics.
    // These must be omitted when zero to keep parity with the legacy renderer.
    JsonNode execNode = json.get("executionTimeMs");
    if (execNode != null) {
      long selfExecTimeMs = execNode.asLong(0) - sumChildrenStat(node, "executionTimeMs");
      if (selfExecTimeMs != 0) {
        json.put("selfExecutionTimeMs", selfExecTimeMs);
        json.put("selfClockTimeMs", selfExecTimeMs / parallelism);
      }
    }
    JsonNode allocNode = json.get("allocatedMemoryBytes");
    if (allocNode != null) {
      long selfAllocBytes = allocNode.asLong(0) - sumChildrenStat(node, "allocatedMemoryBytes");
      if (selfAllocBytes != 0) {
        json.put("selfAllocatedMB", selfAllocBytes / (1024 * 1024));
      }
    }
    JsonNode gcNode = json.get("gcTimeMs");
    if (gcNode != null) {
      long selfGcTimeMs = gcNode.asLong(0) - sumChildrenStat(node, "gcTimeMs");
      if (selfGcTimeMs != 0) {
        json.put("selfGcTimeMs", selfGcTimeMs);
      }
    }

    return json;
  }

  /**
   * Sums a named stat field across the direct non-pipeline-breaker children of a node.
   *
   * <p>PIPELINE_BREAKER children are skipped entirely, mirroring {@link InStageStatsTreeBuilder}'s
   * {@code adjustWithChildren=false} path for LEAF nodes: the breaker runs pre-stage, so its
   * cumulative time is not part of the parent's time budget and must not be subtracted.
   */
  private static long sumChildrenStat(StageStatsTreeNode node, String statKey) {
    long sum = 0;
    for (StageStatsTreeNode child : node.getChildren()) {
      if (child.getType().getId() == MultiStageOperator.Type.PIPELINE_BREAKER.getId()) {
        continue;
      }
      JsonNode val = child.getStatMap().asJson().get(statKey);
      if (val != null) {
        sum += val.asLong(0);
      }
    }
    return sum;
  }

  /**
   * Pre-order id assignment identical to the server-side walk in {@code PlanNodeToOpChain#assignPlanNodeIds}, so the
   * ids carried by the stats tree resolve to the same plan nodes here.
   */
  private static void assignPlanNodeIds(PlanNode node, Map<Integer, PlanNode> planNodesById, int[] counter) {
    planNodesById.put(counter[0]++, node);
    for (PlanNode child : node.getInputs()) {
      assignPlanNodeIds(child, planNodesById, counter);
    }
  }
}
