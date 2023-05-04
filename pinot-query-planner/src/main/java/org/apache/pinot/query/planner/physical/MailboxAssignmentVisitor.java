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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.query.planner.stage.DefaultPostOrderTraversalVisitor;
import org.apache.pinot.query.planner.stage.MailboxReceiveNode;
import org.apache.pinot.query.planner.stage.MailboxSendNode;
import org.apache.pinot.query.planner.stage.StageNode;
import org.apache.pinot.query.routing.MailboxMetadata;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.routing.VirtualServerAddress;


public class MailboxAssignmentVisitor extends DefaultPostOrderTraversalVisitor<Void, DispatchablePlanContext> {
  public static final MailboxAssignmentVisitor INSTANCE = new MailboxAssignmentVisitor();

  @Override
  public Void process(StageNode node, DispatchablePlanContext context) {
    if (node instanceof MailboxSendNode || node instanceof MailboxReceiveNode) {
      int receiverStageId =
          isMailboxReceiveNode(node) ? node.getStageId() : ((MailboxSendNode) node).getReceiverStageId();
      int senderStageId =
          isMailboxReceiveNode(node) ? ((MailboxReceiveNode) node).getSenderStageId() : node.getStageId();
      DispatchablePlanMetadata receiverStagePlanMetadata =
          context.getDispatchablePlanMetadataMap().get(receiverStageId);
      DispatchablePlanMetadata senderStagePlanMetadata = context.getDispatchablePlanMetadataMap().get(senderStageId);
      receiverStagePlanMetadata.getServerInstanceToWorkerIdMap().entrySet().stream().forEach(receiverEntry -> {
        QueryServerInstance receiverServerInstance = receiverEntry.getKey();
        List<Integer> receiverWorkerIds = receiverEntry.getValue();
        for (int receiverWorkerId : receiverWorkerIds) {
          receiverStagePlanMetadata.getWorkerIdToMailBoxIdsMap().putIfAbsent(receiverWorkerId, new HashMap<>());
          senderStagePlanMetadata.getServerInstanceToWorkerIdMap().entrySet().stream().forEach(senderEntry -> {
            QueryServerInstance senderServerInstance = senderEntry.getKey();
            List<Integer> senderWorkerIds = senderEntry.getValue();
            for (int senderWorkerId : senderWorkerIds) {
              MailboxMetadata mailboxMetadata =
                  isMailboxReceiveNode(node)
                      ? getMailboxMetadata(receiverStagePlanMetadata, senderStageId, receiverWorkerId)
                      : getMailboxMetadata(senderStagePlanMetadata, receiverStageId, senderWorkerId);
              mailboxMetadata.getMailBoxIdList().add(
                  MailboxIdUtils.toPlanMailboxId(senderStageId, senderWorkerId, receiverStageId, receiverWorkerId));
              VirtualServerAddress virtualServerAddress =
                  isMailboxReceiveNode(node)
                      ? new VirtualServerAddress(senderServerInstance, senderWorkerId)
                      : new VirtualServerAddress(receiverServerInstance, receiverWorkerId);
              mailboxMetadata.getVirtualAddressList().add(virtualServerAddress);
            }
          });
        }
      });
    }
    return null;
  }

  private static boolean isMailboxReceiveNode(StageNode node) {
    return node instanceof MailboxReceiveNode;
  }

  private MailboxMetadata getMailboxMetadata(DispatchablePlanMetadata stagePlanMetadata, int stageId, int workerId) {
    Map<Integer, Map<Integer, MailboxMetadata>> workerIdToMailBoxIdsMap =
        stagePlanMetadata.getWorkerIdToMailBoxIdsMap();
    if (!workerIdToMailBoxIdsMap.containsKey(workerId)) {
      workerIdToMailBoxIdsMap.put(workerId, new HashMap<>());
    }
    Map<Integer, MailboxMetadata> stageToMailboxMetadataMap = workerIdToMailBoxIdsMap.get(workerId);
    if (!stageToMailboxMetadataMap.containsKey(stageId)) {
      stageToMailboxMetadataMap.put(stageId, new MailboxMetadata());
    }
    return stageToMailboxMetadataMap.get(stageId);
  }
}
