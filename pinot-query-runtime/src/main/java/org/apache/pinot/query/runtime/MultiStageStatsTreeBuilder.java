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
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.spi.utils.JsonUtils;


public class MultiStageStatsTreeBuilder {
  private final Map<Integer, PlanNode> _planNodes;
  private final List<? extends MultiStageQueryStats.StageStats> _queryStats;
  private final Map<Integer, DispatchablePlanFragment> _planFragments;

  public MultiStageStatsTreeBuilder(Map<Integer, DispatchablePlanFragment> planFragments,
      List<? extends MultiStageQueryStats.StageStats> queryStats) {
    _planFragments = planFragments;
    _planNodes = Maps.newHashMapWithExpectedSize(planFragments.size());
    for (Map.Entry<Integer, DispatchablePlanFragment> entry : planFragments.entrySet()) {
      _planNodes.put(entry.getKey(), entry.getValue().getPlanFragment().getFragmentRoot());
    }
    _queryStats = queryStats;
  }

  public ObjectNode jsonStatsByStage(int stage) {
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
    return planNode.visit(treeBuilder, null);
  }
}
