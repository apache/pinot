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
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;


/// Marks one deterministic HASH exchange for file-backed materialization.
///
/// This planner changes no worker assignment or mailbox metadata and marks at most one ordinary, unsorted,
/// single-consumer exchange whose receiver is a server stage. The query option that invokes it is disabled by default.
public final class MaterializedExchangePlanner {
  private MaterializedExchangePlanner() {
  }

  public static boolean markFirstEligible(DispatchableSubPlan subPlan) {
    for (DispatchablePlanFragment receiverFragment : subPlan.getQueryStagesWithoutRoot()) {
      List<MailboxReceiveNode> receiveNodes = new ArrayList<>();
      collectReceiveNodes(receiverFragment.getPlanFragment().getFragmentRoot(), receiveNodes);
      for (MailboxReceiveNode receiveNode : receiveNodes) {
        DispatchablePlanFragment senderFragment = subPlan.getQueryStageMap().get(receiveNode.getSenderStageId());
        if (senderFragment == null
            || !(senderFragment.getPlanFragment().getFragmentRoot() instanceof MailboxSendNode)) {
          continue;
        }
        MailboxSendNode sendNode = (MailboxSendNode) senderFragment.getPlanFragment().getFragmentRoot();
        if (isEligible(sendNode, receiveNode, receiverFragment.getPlanFragment().getFragmentId())) {
          sendNode.setMaterialized(true);
          receiveNode.setMaterialized(true);
          return true;
        }
      }
    }
    return false;
  }

  private static boolean isEligible(MailboxSendNode sendNode, MailboxReceiveNode receiveNode, int receiverStageId) {
    return sendNode.getExchangeType() == PinotRelExchangeType.STREAMING
        && receiveNode.getExchangeType() == PinotRelExchangeType.STREAMING
        && sendNode.getDistributionType() == RelDistribution.Type.HASH_DISTRIBUTED
        && receiveNode.getDistributionType() == RelDistribution.Type.HASH_DISTRIBUTED
        && !sendNode.isMultiSend()
        && !sendNode.isPrePartitioned()
        && !sendNode.isSort()
        && !receiveNode.isSort()
        && sendNode.getReceiverStageId() == receiverStageId;
  }

  private static void collectReceiveNodes(PlanNode node, List<MailboxReceiveNode> receiveNodes) {
    if (node instanceof MailboxReceiveNode) {
      receiveNodes.add((MailboxReceiveNode) node);
    }
    for (PlanNode input : node.getInputs()) {
      collectReceiveNodes(input, receiveNodes);
    }
  }
}
