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
package org.apache.pinot.query.service.dispatch;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Classifies a query plan's stages for upstream timing extraction.
 *
 * <p>Two kinds of stages are consulted:
 * <ul>
 *   <li><b>Pure leaf receivers</b>: stages that directly receive from a non-SINGLETON leaf.</li>
 *   <li><b>SINGLETON leaf stages receiving from another leaf</b>: a leaf stage that both scans a
 *       dim table and receives all upstream data via a SINGLETON exchange, but ONLY when its upstream
 *       sender stage is also a leaf.</li>
 * </ul>
 *
 * <p>Only pure leaf servers (non-SINGLETON) are eligible for EMA updates. Intermediate and
 * SINGLETON leaf servers are excluded because their timings reflect cascade delays from upstream
 * rather than their own scan performance.
 */
final class AdaptiveRoutingStageClassification {
  private static final Logger LOGGER = LoggerFactory.getLogger(AdaptiveRoutingStageClassification.class);

  /** Maps "hostname|mailboxPort" -> instanceId for all known servers. */
  final Map<String, String> _senderKeyToInstanceId;
  /** Stage IDs whose UPSTREAM_SERVER_RESPONSE_TIMES_MS we trust. */
  final Set<Integer> _trustedStageIds;
  /** Instance IDs of pure leaf servers eligible for EMA updates in the fallback path. */
  final Set<String> _trackedServers;

  private AdaptiveRoutingStageClassification(Map<String, String> senderKeyToInstanceId, Set<Integer> trustedStageIds,
      Set<String> trackedServers) {
    _senderKeyToInstanceId = senderKeyToInstanceId;
    _trustedStageIds = trustedStageIds;
    _trackedServers = trackedServers;
  }

  /**
   * Classification of plan fragments into consulted stages and tracked servers.
   */
  static AdaptiveRoutingStageClassification classify(DispatchableSubPlan plan) {
    Map<String, String> senderKeyToInstanceId = new HashMap<>();
    Set<Integer> leafStageIds = new HashSet<>();
    Set<Integer> trustedStageIds = new HashSet<>();
    Set<String> trackedServers = new HashSet<>();
    // Stages that receive from any non-leaf stage. Their timing is unreliable: the non-leaf sender
    // may have waited for a slow upstream (cascade), inflating per-sender timings at the receiver.
    Set<Integer> stagesReceivingFromNonLeaf = new HashSet<>();
    // Deferred: SINGLETON leaf stages pending "is sender also a leaf?" resolution.
    List<int[]> pendingSingletonLeaves = new ArrayList<>();

    for (DispatchablePlanFragment fragment : plan.getQueryStagesWithoutRoot()) {
      Set<QueryServerInstance> fragmentServers = fragment.getServerInstanceToWorkerIdMap().keySet();
      for (QueryServerInstance server : fragmentServers) {
        String key = AdaptiveRoutingUpstreamTimings.senderKey(server.getHostname(), server.getQueryMailboxPort());
        senderKeyToInstanceId.putIfAbsent(key, server.getInstanceId());
      }

      PlanFragment planFragment = fragment.getPlanFragment();
      PlanNode fragmentRoot = planFragment != null ? planFragment.getFragmentRoot() : null;

      if (fragment.getTableName() == null) {
        // Non-leaf intermediate stage — mark its receivers as contaminated.
        if (fragmentRoot instanceof MailboxSendNode) {
          for (int receiverStageId : ((MailboxSendNode) fragmentRoot).getReceiverStageIds()) {
            stagesReceivingFromNonLeaf.add(receiverStageId);
          }
        }
        continue;
      }

      if (!(fragmentRoot instanceof MailboxSendNode)) {
        continue;
      }
      MailboxSendNode sendNode = (MailboxSendNode) fragmentRoot;
      leafStageIds.add(sendNode.getStageId());

      int singletonSenderStageId = getSingletonReceiveSenderStageId(fragmentRoot);
      if (singletonSenderStageId >= 0) {
        // SINGLETON leaf: mark its receivers as contaminated, defer consultation decision
        // until we know whether the sender is also a leaf.
        for (int receiverStageId : sendNode.getReceiverStageIds()) {
          stagesReceivingFromNonLeaf.add(receiverStageId);
        }
        pendingSingletonLeaves.add(new int[]{sendNode.getStageId(), singletonSenderStageId});
      } else {
        // Pure leaf: its receivers are consulted and its servers are tracked for EMA updates.
        for (int receiverStageId : sendNode.getReceiverStageIds()) {
          trustedStageIds.add(receiverStageId);
        }
        for (QueryServerInstance server : fragmentServers) {
          trackedServers.add(server.getInstanceId());
        }
      }
    }

    // Resolve deferred SINGLETON leaves: only consult if their sender is also a leaf.
    for (int[] pair : pendingSingletonLeaves) {
      if (leafStageIds.contains(pair[1])) {
        trustedStageIds.add(pair[0]);
      }
    }

    // Exclude stages that receive from any non-leaf stage. Non-leaf senders may carry cascade
    // delay from slow upstream servers, inflating per-sender timings at the receiver.
    trustedStageIds.removeAll(stagesReceivingFromNonLeaf);

    LOGGER.debug("==[UPSTREAM_TIMING]== classifyStages: leafStageIds={} trustedStageIds={} "
        + "trackedServers={} senderKeyToInstanceId.size={}", leafStageIds, trustedStageIds,
        trackedServers.size(), senderKeyToInstanceId.size());

    return new AdaptiveRoutingStageClassification(senderKeyToInstanceId, trustedStageIds, trackedServers);
  }

  /**
   * Returns the sender stage ID of the first {@link MailboxReceiveNode} with
   * {@link RelDistribution.Type#SINGLETON} distribution found in the plan subtree rooted at {@code node},
   * or {@code -1} if none is found.
   */
  @VisibleForTesting
  static int getSingletonReceiveSenderStageId(PlanNode node) {
    if (node instanceof MailboxReceiveNode) {
      MailboxReceiveNode receiveNode = (MailboxReceiveNode) node;
      if (receiveNode.getDistributionType() == RelDistribution.Type.SINGLETON) {
        return receiveNode.getSenderStageId();
      }
    }
    for (PlanNode input : node.getInputs()) {
      int result = getSingletonReceiveSenderStageId(input);
      if (result >= 0) {
        return result;
      }
    }
    return -1;
  }
}
