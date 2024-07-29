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
import java.util.List;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;


public class MultiStageStatsTreeBuilder {
  private final List<PlanNode> _planNodes;
  private final List<? extends MultiStageQueryStats.StageStats> _queryStats;

  public MultiStageStatsTreeBuilder(List<PlanNode> planNodes,
      List<? extends MultiStageQueryStats.StageStats> queryStats) {
    _planNodes = planNodes;
    _queryStats = queryStats;
  }

  public ObjectNode jsonStatsByStage(int stage) {
    MultiStageQueryStats.StageStats stageStats = _queryStats.get(stage);
    PlanNode planNode = _planNodes.get(stage);
    InStageStatsTreeBuilder treeBuilder = new InStageStatsTreeBuilder(stageStats, this::jsonStatsByStage);
    return planNode.visit(treeBuilder, null);
  }
}
