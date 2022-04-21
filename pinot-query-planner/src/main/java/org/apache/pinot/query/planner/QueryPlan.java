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
package org.apache.pinot.query.planner;

import java.util.Map;
import org.apache.pinot.query.planner.logical.LogicalPlanner;
import org.apache.pinot.query.planner.stage.StageNode;


/**
 * The {@code QueryPlan} is the dispatchable query execution plan from the result of {@link LogicalPlanner}.
 *
 * <p>QueryPlan should contain the necessary stage boundary information and the cross exchange information
 * for:
 * <ul>
 *   <li>dispatch individual stages to executor.</li>
 *   <li>instruct stage executor to establish connection channels to other stages.</li>
 *   <li>encode data blocks for transfer between stages based on partitioning scheme.</li>
 * </ul>
 */
public class QueryPlan {
  private Map<Integer, StageNode> _queryStageMap;
  private Map<Integer, StageMetadata> _stageMetadataMap;

  public QueryPlan(Map<Integer, StageNode> queryStageMap, Map<Integer, StageMetadata> stageMetadataMap) {
    _queryStageMap = queryStageMap;
    _stageMetadataMap = stageMetadataMap;
  }

  /**
   * Get the map between stageID and the stage plan root node.
   * @return stage plan map.
   */
  public Map<Integer, StageNode> getQueryStageMap() {
    return _queryStageMap;
  }

  /**
   * Get the stage metadata information.
   * @return stage metadata info.
   */
  public Map<Integer, StageMetadata> getStageMetadataMap() {
    return _stageMetadataMap;
  }
}
