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
package org.apache.pinot.query.planner.logical;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.query.planner.stage.BaseDepthFirstStageNodeVisitor;
import org.apache.pinot.query.planner.stage.JoinNode;
import org.apache.pinot.query.planner.stage.MailboxReceiveNode;
import org.apache.pinot.query.planner.stage.StageNode;
import org.apache.pinot.query.planner.stage.TableScanNode;


public class PhysicalStageVisitor
    extends BaseDepthFirstStageNodeVisitor<Integer, PhysicalStageVisitor.PhysicalStageInfo> {

  public static PhysicalStageInfo go(StageNode rootStageNode) {
    PhysicalStageInfo physicalStageInfo = new PhysicalStageInfo();
    rootStageNode.visit(new PhysicalStageVisitor(), physicalStageInfo);
    return physicalStageInfo;
  }

  @Override
  public Integer visitNode(StageNode stageNode, PhysicalStageInfo context) {
    int currentStageId = stageNode.getStageId();
    if (context.isRootStageNodeSet(currentStageId)) {
      context.setRootStageNode(currentStageId, stageNode);
    }
    if (isLeafNode(stageNode)) {
      context.addLeafNode(currentStageId, stageNode);
    }
    if (CollectionUtils.isNotEmpty(stageNode.getInputs())) {
      for (StageNode childStageNode : stageNode.getInputs()) {
        childStageNode.visit(this, context);
      }
    }
    return 0;
  }

  @Override
  public Integer visitJoin(JoinNode joinNode, PhysicalStageInfo context) {
    context.markJoinStage(joinNode.getStageId());
    visitNode(joinNode, context);
    return 0;
  }

  private boolean isLeafNode(StageNode stageNode) {
    if (CollectionUtils.isEmpty(stageNode.getInputs())) {
      return true;
    }
    return stageNode instanceof TableScanNode || stageNode instanceof MailboxReceiveNode;
  }

  public static class PhysicalStageInfo {
    private final Map<Integer, StageNode> _rootStageNode;
    private final Map<Integer, List<StageNode>> _leafNodes;
    private final Set<Integer> _joinStages;
    private final Map<Integer, Set<Integer>> _partitionKeysCache;

    public PhysicalStageInfo() {
      _rootStageNode = new HashMap<>();
      _leafNodes = new HashMap<>();
      _joinStages =  new HashSet<>();
      _partitionKeysCache = new HashMap<>();
    }

    public Map<Integer, StageNode> getRootStageNode() {
      return _rootStageNode;
    }

    public void setRootStageNode(Integer stageId, StageNode stageNode) {
      _rootStageNode.put(stageId, stageNode);
    }

    public boolean isRootStageNodeSet(Integer stageId) {
      return _rootStageNode.containsKey(stageId);
    }

    public void markJoinStage(Integer stageId) {
      _joinStages.add(stageId);
    }

    public boolean isJoinStage(Integer stageId) {
      return _joinStages.contains(stageId);
    }

    public Map<Integer, List<StageNode>> getLeafNodes() {
      return _leafNodes;
    }

    public void addLeafNode(Integer stageId, StageNode stageNode) {
      _leafNodes.computeIfAbsent(stageId, (x) -> new ArrayList<>()).add(stageNode);
    }

    public void setPartitionKeys(Integer stageId, Set<Integer> partitionKeys) {
      _partitionKeysCache.put(stageId, partitionKeys);
    }

    public Set<Integer> getPartitionKeys(Integer stageId) {
      return _partitionKeysCache.get(stageId);
    }
  }
}
