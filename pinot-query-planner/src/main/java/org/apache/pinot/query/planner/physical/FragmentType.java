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
package org.apache.pinot.query.planner.physical;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;


/**
 * Classifies dispatchable physical-plan fragments that participate in mailbox-based stage execution.
 * <p>
 * The values describe whether a fragment is a scanning leaf, a leaf reached through singleton exchanges,
 * or an intermediate stage. Fragments outside this scope are left unclassified: {@link #classify(PlanNode,
 * boolean, Map)} returns {@code null} when the fragment root is not a {@link MailboxSendNode}.
 */
public enum FragmentType {
  // Scans a fact table; non-SINGLETON sender
  LEAF,
  // Dim-table leaf via SINGLETON exchange, with a leaf upstream sender
  SINGLETON_LEAF,
  // Non-scanning stage (join, agg, sort); timings reflect upstream cascade delays
  INTERMEDIATE;

  public static FragmentType classify(PlanNode root, boolean hasScannedTable,
      Map<Integer, DispatchablePlanMetadata> metadataMap) {
    if (!(root instanceof MailboxSendNode)) {
      return null;
    }
    if (!hasScannedTable) {
      return INTERMEDIATE;
    }
    List<Integer> singletonSenders = new ArrayList<>();
    List<Integer> nonSingletonSenders = new ArrayList<>();
    collectReceiveSenderStageIds(root, singletonSenders, nonSingletonSenders);

    boolean hasNonLeafNonSingletonSender = nonSingletonSenders.stream().anyMatch(senderStageId -> {
      DispatchablePlanMetadata senderMeta = metadataMap.get(senderStageId);
      return senderMeta == null || senderMeta.getScannedTables().isEmpty();
    });
    if (hasNonLeafNonSingletonSender) {
      return INTERMEDIATE;
    }

    if (singletonSenders.isEmpty()) {
      return LEAF;
    }
    boolean allSingletonSendersAreLeaves = singletonSenders.stream().allMatch(senderStageId -> {
      DispatchablePlanMetadata senderMeta = metadataMap.get(senderStageId);
      return senderMeta != null && !senderMeta.getScannedTables().isEmpty();
    });
    return allSingletonSendersAreLeaves ? SINGLETON_LEAF : INTERMEDIATE;
  }

  private static void collectReceiveSenderStageIds(PlanNode node,
      List<Integer> singletonSenders, List<Integer> nonSingletonSenders) {
    if (node instanceof MailboxReceiveNode) {
      MailboxReceiveNode receiveNode = (MailboxReceiveNode) node;
      if (receiveNode.getDistributionType() == RelDistribution.Type.SINGLETON) {
        singletonSenders.add(receiveNode.getSenderStageId());
      } else {
        nonSingletonSenders.add(receiveNode.getSenderStageId());
      }
    }
    for (PlanNode input : node.getInputs()) {
      collectReceiveSenderStageIds(input, singletonSenders, nonSingletonSenders);
    }
  }
}
