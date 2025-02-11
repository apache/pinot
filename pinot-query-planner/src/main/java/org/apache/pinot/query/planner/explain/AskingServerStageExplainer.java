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
package org.apache.pinot.query.planner.explain;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.operator.ExplainAttributeBuilder;
import org.apache.pinot.query.planner.logical.PlanNodeToRelConverter;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.plannode.ExplainedNode;
import org.apache.pinot.query.planner.plannode.PlanNode;


/**
 * A class used to explain a {@link DispatchablePlanFragment} by asking the servers to explain their own execution plan.
 *
 * A {@link OnServerExplainer} is used to ask servers for their plans and the results are merged into a single plan.
 * This merge can be configured to be simplified or verbose. Verbose plans keep a plan per segment while simplified
 * plans merge plans that are similar. Usually the latter is the desired behavior but it can be disabled for debugging
 * purposes (ie to be able to see which segments are not using an index in their plans).
 */
public class AskingServerStageExplainer {
  private final OnServerExplainer _onServerExplainer;
  private final boolean _verbose;
  private final RelBuilder _relBuilder;

  public AskingServerStageExplainer(OnServerExplainer onServerExplainer, boolean verbose, RelBuilder relBuilder) {
    _onServerExplainer = onServerExplainer;
    _verbose = verbose;
    _relBuilder = relBuilder;
  }

  public RelNode explainFragment(DispatchablePlanFragment fragment) {
    _relBuilder.clear();

    PlanNode fragmentRoot = fragment.getPlanFragment().getFragmentRoot();
    int stageId = fragmentRoot.getStageId();
    DataSchema schema = fragmentRoot.getDataSchema();

    HashMap<PlanNode, Integer> planNodesMap = new HashMap<>();
    if (fragment.getWorkerIdToSegmentsMap().isEmpty()) {
      PlanNode mergedNode = new ExplainedNode(stageId, schema, null, Collections.emptyList(), "PlanWithNoSegments",
          new ExplainAttributeBuilder()
              .putString("table", fragment.getTableName())
              .build());
      return PlanNodeToRelConverter.convert(_relBuilder, mergedNode);
    }
    Collection<PlanNode> planNodes = _onServerExplainer.explainOnServers(fragment);
    for (PlanNode planNode : planNodes) {
      PlanNode simplifiedPlan = !_verbose ? ExplainNodeSimplifier.simplifyNode(planNode) : planNode;
      PlanNode sortedPlan = PlanNodeSorter.sort(simplifiedPlan);

      mergePlans(planNodesMap, sortedPlan);
    }

    PlanNode mergedNode;
    switch (planNodesMap.size()) {
      case 0: {
        mergedNode = new ExplainedNode(stageId, schema, null, Collections.emptyList(), "NoPlanInformation",
            Collections.emptyMap());
        break;
      }
      case 1: {
        Map.Entry<PlanNode, Integer> entry = planNodesMap.entrySet().iterator().next();
        mergedNode = entry.getKey();
        break;
      }
      default: {
        List<PlanNode> inputs = new ArrayList<>(planNodesMap.size());

        for (Map.Entry<PlanNode, Integer> entry : planNodesMap.entrySet()) {
          ExplainAttributeBuilder attributes = new ExplainAttributeBuilder();
          attributes.putLong("servers", entry.getValue());

          inputs.add(new ExplainedNode(stageId, entry.getKey().getDataSchema(), null,
              Collections.singletonList(entry.getKey()), "Alternative", attributes.build()));
        }

        mergedNode = new ExplainedNode(stageId, schema, null, inputs, "IntermediateCombine",
            Collections.emptyMap());
        break;
      }
    }

    return PlanNodeToRelConverter.convert(_relBuilder, mergedNode);
  }

  private void mergePlans(Map<PlanNode, Integer> planNodesMap, PlanNode planNode) {
    boolean merged = false;
    for (Map.Entry<PlanNode, Integer> entry : planNodesMap.entrySet()) {
      PlanNode originalPlan = entry.getKey();

      PlanNode mergedPlan = PlanNodeMerger.mergePlans(originalPlan, planNode, _verbose);
      if (mergedPlan != null) {
        planNodesMap.remove(originalPlan);
        planNodesMap.put(mergedPlan, entry.getValue() + 1);
        merged = true;
        break;
      }
    }
    if (!merged) {
      planNodesMap.put(planNode, 1);
    }
  }

  /**
   * An abstraction to explain a {@link DispatchablePlanFragment} on the servers.
   *
   * It is assumed that the fragment is executed on multiple servers and each server may have a different execution
   * plan.
   *
   * Implementations of this interface are expected to not be in pinot-query-planner but in the module that is
   * responsible for executing the plan on the servers (ie pinot-query-runtime).
   *
   * Implementations can return one node per server, or one node per segment, or any other granularity that makes
   * sense, but it is important they follow the indications in {@link ExplainNodeSimplifier}, {@link PlanNodeSorter}
   * and {@link PlanNodeMerger}. This basically means that plans from different segments should be grouped by a
   * {@lin ExplainedNode} with a title that indicates that contains the word {@code Combine}
   */
  public interface OnServerExplainer {
    /**
     * Returns a collection of {@link PlanNode} that explain the given {@link DispatchablePlanFragment}.
     *
     * The reason for returning a collection of them is that the fragment may be executed in parallel on multiple
     * servers.
     * Each server may contain different segments with different values and indexes and therefore may have a different
     * execution plan.
     *
     * @see ExplainNodeSimplifier
     * @see PlanNodeSorter
     */
    Collection<PlanNode> explainOnServers(DispatchablePlanFragment fragment);
  }
}
