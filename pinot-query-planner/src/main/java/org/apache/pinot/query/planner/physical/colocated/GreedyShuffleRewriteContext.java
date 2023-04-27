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
package org.apache.pinot.query.planner.physical.colocated;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.query.planner.stage.MailboxSendNode;
import org.apache.pinot.query.planner.stage.StageNode;


/**
 * Context used for running the {@link GreedyShuffleRewriteVisitor}.
 */
class GreedyShuffleRewriteContext {
  private final Map<Integer, StageNode> _rootStageNode;
  private final Map<Integer, List<StageNode>> _leafNodes;
  private final Set<Integer> _joinStages;
  private final Set<Integer> _setOpStages;

  /**
   * A map to track the partition keys for the input to the MailboxSendNode of a given stageId. This is needed
   * because the {@link GreedyShuffleRewriteVisitor} doesn't determine the distribution of the sender if the receiver
   * is a join-stage.
   */
  private final Map<Integer, Set<ColocationKey>> _senderInputColocationKeys;

  GreedyShuffleRewriteContext() {
    _rootStageNode = new HashMap<>();
    _leafNodes = new HashMap<>();
    _joinStages = new HashSet<>();
    _setOpStages = new HashSet<>();
    _senderInputColocationKeys = new HashMap<>();
  }

  /**
   * Returns the root StageNode for a given stageId.
   */
  StageNode getRootStageNode(Integer stageId) {
    return _rootStageNode.get(stageId);
  }

  /**
   * Sets the root StageNode for a given stageId.
   */
  void setRootStageNode(Integer stageId, StageNode stageNode) {
    _rootStageNode.put(stageId, stageNode);
  }

  /**
   * Returns all the leaf StageNode for a given stageId.
   */
  List<StageNode> getLeafNodes(Integer stageId) {
    return _leafNodes.get(stageId);
  }

  /**
   * Adds a leaf StageNode for a given stageId.
   */
  void addLeafNode(Integer stageId, StageNode stageNode) {
    _leafNodes.computeIfAbsent(stageId, (x) -> new ArrayList<>()).add(stageNode);
  }

  /**
   * {@link GreedyShuffleRewriteContext} allows checking whether a given stageId has a JoinNode or not. During
   * pre-computation, this method may be used to mark that the given stageId has a JoinNode.
   */
  void markJoinStage(Integer stageId) {
    _joinStages.add(stageId);
  }

  /**
   * Returns true if the given stageId has a JoinNode.
   */
  boolean isJoinStage(Integer stageId) {
    return _joinStages.contains(stageId);
  }


  /**
   * {@link GreedyShuffleRewriteContext} allows checking whether a given stageId has a SetOpNode or not. During
   * pre-computation, this method may be used to mark that the given stageId has a SetOpNode.
   */
  void markSetOpStage(Integer stageId) {
    _setOpStages.add(stageId);
  }

  /**
   * Returns true if the given stageId has a SetOpNode.
   */
  boolean isSetOpStage(Integer stageId) {
    return _setOpStages.contains(stageId);
  }

  /**
   * This returns the {@link Set<ColocationKey>} for the input to the {@link MailboxSendNode} of the given stageId.
   */
  Set<ColocationKey> getColocationKeys(Integer stageId) {
    return _senderInputColocationKeys.get(stageId);
  }

  /**
   * This sets the {@link Set<ColocationKey>} for the input to the {@link MailboxSendNode} of the given stageId.
   */
  void setColocationKeys(Integer stageId, Set<ColocationKey> colocationKeys) {
    _senderInputColocationKeys.put(stageId, colocationKeys);
  }
}
