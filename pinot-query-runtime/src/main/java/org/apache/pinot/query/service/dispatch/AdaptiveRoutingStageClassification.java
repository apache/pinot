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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.physical.FragmentType;
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
   * Derives trusted stages and tracked servers from pre-computed roles set at planning time.
   *
   * <p>For pure leaves: their receivers are trusted (unless also receiving from a non-leaf).
   * For SINGLETON leaf trusted: the stage itself is trusted.
   * Contamination filter: any stage that receives from a non-leaf is excluded from trusted.
   */
  static AdaptiveRoutingStageClassification classify(DispatchableSubPlan plan) {
    Map<String, String> senderKeyToInstanceId = new HashMap<>();
    Set<Integer> trustedStageIds = new HashSet<>();
    Set<String> trackedServers = new HashSet<>();
    Set<Integer> stagesReceivingFromNonLeaf = new HashSet<>();

    for (DispatchablePlanFragment fragment : plan.getQueryStagesWithoutRoot()) {
      Set<QueryServerInstance> fragmentServers = fragment.getServerInstanceToWorkerIdMap().keySet();
      for (QueryServerInstance server : fragmentServers) {
        String key = AdaptiveRoutingUpstreamTimings.senderKey(server.getHostname(), server.getQueryMailboxPort());
        senderKeyToInstanceId.putIfAbsent(key, server.getInstanceId());
      }

      FragmentType role = fragment.getFragmentType();
      PlanFragment planFragment = fragment.getPlanFragment();
      PlanNode fragmentRoot = planFragment != null ? planFragment.getFragmentRoot() : null;
      MailboxSendNode sendNode = fragmentRoot instanceof MailboxSendNode ? (MailboxSendNode) fragmentRoot : null;

      if (role == FragmentType.LEAF) {
        // Leaf: receivers are trusted, servers are tracked.
        if (sendNode != null) {
          for (int receiverStageId : sendNode.getReceiverStageIds()) {
            trustedStageIds.add(receiverStageId);
          }
        }
        for (QueryServerInstance server : fragmentServers) {
          trackedServers.add(server.getInstanceId());
        }
      } else if (role == FragmentType.SINGLETON_LEAF) {
        // SINGLETON leaf with leaf sender: this stage's own stats are trusted.
        trustedStageIds.add(planFragment.getFragmentId());
        // Its receivers are contaminated (SINGLETON cascade).
        if (sendNode != null) {
          for (int receiverStageId : sendNode.getReceiverStageIds()) {
            stagesReceivingFromNonLeaf.add(receiverStageId);
          }
        }
      } else if (role == FragmentType.INTERMEDIATE && sendNode != null) {
        // Intermediate stage: its receivers are contaminated by cascade delays.
        for (int receiverStageId : sendNode.getReceiverStageIds()) {
          stagesReceivingFromNonLeaf.add(receiverStageId);
        }
      } else if (role == null && sendNode != null) {
        LOGGER.debug("Stage {} has null FragmentType; treating as INTERMEDIATE",
            planFragment != null ? planFragment.getFragmentId() : "unknown");
        for (int receiverStageId : sendNode.getReceiverStageIds()) {
          stagesReceivingFromNonLeaf.add(receiverStageId);
        }
      }
    }

    // Exclude stages contaminated by non-leaf/SINGLETON senders.
    trustedStageIds.removeAll(stagesReceivingFromNonLeaf);

    LOGGER.debug("==[UPSTREAM_TIMING]== classifyStages: trustedStageIds={} trackedServers={} "
        + "senderKeyToInstanceId.size={}", trustedStageIds, trackedServers.size(), senderKeyToInstanceId.size());

    return new AdaptiveRoutingStageClassification(senderKeyToInstanceId, trustedStageIds, trackedServers);
  }
}
