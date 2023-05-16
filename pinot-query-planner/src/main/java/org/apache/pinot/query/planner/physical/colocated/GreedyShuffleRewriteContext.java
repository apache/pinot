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
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;


/**
 * Context used for running the {@link GreedyShuffleRewriteVisitor}.
 */
class GreedyShuffleRewriteContext {
  private final Map<Integer, PlanNode> _rootStageNode;
  private final Map<Integer, List<PlanNode>> _leafNodes;
  private final Set<Integer> _joinStages;
  private final Set<Integer> _setOpStages;

  /**
   * A map to track the partition keys for the input to the MailboxSendNode of a given planFragmentId. This is needed
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
   * Returns the root StageNode for a given planFragmentId.
   */
  PlanNode getRootStageNode(Integer planFragmentId) {
    return _rootStageNode.get(planFragmentId);
  }

  /**
   * Sets the root StageNode for a given planFragmentId.
   */
  void setRootStageNode(Integer planFragmentId, PlanNode planNode) {
    _rootStageNode.put(planFragmentId, planNode);
  }

  /**
   * Returns all the leaf StageNode for a given planFragmentId.
   */
  List<PlanNode> getLeafNodes(Integer planFragmentId) {
    return _leafNodes.get(planFragmentId);
  }

  /**
   * Adds a leaf PlanNode for a given planFragmentId.
   */
  void addLeafNode(Integer planFragmentId, PlanNode planNode) {
    _leafNodes.computeIfAbsent(planFragmentId, (x) -> new ArrayList<>()).add(planNode);
  }

  /**
   * {@link GreedyShuffleRewriteContext} allows checking whether a given planFragmentId has a JoinNode or not. During
   * pre-computation, this method may be used to mark that the given planFragmentId has a JoinNode.
   */
  void markJoinStage(Integer planFragmentId) {
    _joinStages.add(planFragmentId);
  }

  /**
   * Returns true if the given planFragmentId has a JoinNode.
   */
  boolean isJoinStage(Integer planFragmentId) {
    return _joinStages.contains(planFragmentId);
  }

  /**
   * {@link GreedyShuffleRewriteContext} allows checking whether a given planFragmentId has a SetOpNode or not. During
   * pre-computation, this method may be used to mark that the given planFragmentId has a SetOpNode.
   */
  void markSetOpStage(Integer planFragmentId) {
    _setOpStages.add(planFragmentId);
  }

  /**
   * Returns true if the given planFragmentId has a SetOpNode.
   */
  boolean isSetOpStage(Integer planFragmentId) {
    return _setOpStages.contains(planFragmentId);
  }

  /**
   * This returns the {@link Set<ColocationKey>} for the input to the {@link MailboxSendNode} of the given
   * planFragmentId.
   */
  Set<ColocationKey> getColocationKeys(Integer planFragmentId) {
    return _senderInputColocationKeys.get(planFragmentId);
  }

  /**
   * This sets the {@link Set<ColocationKey>} for the input to the {@link MailboxSendNode} of the given planFragmentId.
   */
  void setColocationKeys(Integer planFragmentId, Set<ColocationKey> colocationKeys) {
    _senderInputColocationKeys.put(planFragmentId, colocationKeys);
  }
}
