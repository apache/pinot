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
package org.apache.pinot.query.runtime.plan.pipeline;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;


/**
 * This class used to record the pipeline breaker operator that needs to be run before the main opChain.
 */
public class PipelineBreakerContext {
  private final Map<Integer, PlanNode> _pipelineBreakerMap = new HashMap<>();
  private final Map<PlanNode, Integer> _planNodeObjectToIdMap = new HashMap<>();

  private int _currentNodeId = 0;
  private Map<Integer, List<TransferableBlock>> _pipelineBreakerResultMap;

  public void addPipelineBreaker(MailboxReceiveNode mailboxReceiveNode) {
    _pipelineBreakerMap.put(getNodeId(mailboxReceiveNode), mailboxReceiveNode);
  }

  public void visitedNewPlanNode(PlanNode planNode) {
    _planNodeObjectToIdMap.put(planNode, _currentNodeId);
    _currentNodeId++;
  }

  public int getNodeId(PlanNode planNode) {
    return _planNodeObjectToIdMap.getOrDefault(planNode, -1);
  }

  public Map<Integer, PlanNode> getPipelineBreakerMap() {
    return _pipelineBreakerMap;
  }

  public Map<Integer, List<TransferableBlock>> getResultMap() {
    return _pipelineBreakerResultMap;
  }

  public void setResultMap(Map<Integer, List<TransferableBlock>> resultMap) {
    _pipelineBreakerResultMap = resultMap;
  }
}
