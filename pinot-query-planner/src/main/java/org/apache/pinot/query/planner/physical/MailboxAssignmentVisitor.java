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

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
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
    if (node instanceof MailboxSendNode) {
      processMailboxSendNode((MailboxSendNode) node, context);
    }
    if (node instanceof MailboxReceiveNode) {
      processMailboxReceive((MailboxReceiveNode) node, context);
    }
    return null;
  }

  public Void processMailboxReceive(MailboxReceiveNode node, DispatchablePlanContext context) {
    int receiverStageId = node.getStageId();
    int senderStageId = node.getSenderStageId();
    DispatchablePlanMetadata receiverStagePlanMetadata =
        context.getDispatchablePlanMetadataMap().get(receiverStageId);
    DispatchablePlanMetadata senderStagePlanMetadata = context.getDispatchablePlanMetadataMap().get(senderStageId);
    receiverStagePlanMetadata.getServerInstanceToWorkerIdMap().entrySet().stream().forEach(receiverEntry -> {
      List<Integer> receiverWorkerIds = receiverEntry.getValue();
      for (int receiverWorkerId : receiverWorkerIds) {
        receiverStagePlanMetadata.getWorkerIdToMailBoxIdsMap().putIfAbsent(receiverWorkerId, new HashMap<>());
        senderStagePlanMetadata.getServerInstanceToWorkerIdMap().entrySet().stream().forEach(senderEntry -> {
          QueryServerInstance senderServerInstance = senderEntry.getKey();
          List<Integer> senderWorkerIds = senderEntry.getValue();
          for (int senderWorkerId : senderWorkerIds) {
            String mailboxId =
                MailboxIdUtils.toPlanMailboxId(senderStageId, senderWorkerId, receiverStageId, receiverWorkerId);
            MailboxMetadata senderMailboxMetadata =
                new MailboxMetadata(mailboxId, new VirtualServerAddress(senderServerInstance.getHostname(),
                    senderServerInstance.getQueryMailboxPort(),
                    senderWorkerId).toString(), ImmutableMap.of());
            Map<Integer, List<MailboxMetadata>> stageToMailboxMetadataMap =
                receiverStagePlanMetadata.getWorkerIdToMailBoxIdsMap().get(receiverWorkerId);
            stageToMailboxMetadataMap.putIfAbsent(senderStageId, new ArrayList<>());
            stageToMailboxMetadataMap.get(senderStageId).add(senderMailboxMetadata);
          }
        });
      }
    });
    return null;
  }

  public Void processMailboxSendNode(MailboxSendNode node, DispatchablePlanContext context) {
    int senderStageId = node.getStageId();
    int receiverStageId = node.getReceiverStageId();
    DispatchablePlanMetadata senderStagePlanMetadata = context.getDispatchablePlanMetadataMap().get(senderStageId);
    DispatchablePlanMetadata receiverStagePlanMetadata = context.getDispatchablePlanMetadataMap().get(receiverStageId);

    senderStagePlanMetadata.getServerInstanceToWorkerIdMap().entrySet().stream().forEach(senderEntry -> {
      List<Integer> senderWorkerIds = senderEntry.getValue();
      for (int senderWorkerId : senderWorkerIds) {
        senderStagePlanMetadata.getWorkerIdToMailBoxIdsMap().putIfAbsent(senderWorkerId, new HashMap<>());
        receiverStagePlanMetadata.getServerInstanceToWorkerIdMap().entrySet().stream().forEach(receiverEntry -> {
          QueryServerInstance receiverServerInstance = receiverEntry.getKey();
          List<Integer> receiverWorkerIds = receiverEntry.getValue();
          for (int receiverWorkerId : receiverWorkerIds) {
            String mailboxId =
                MailboxIdUtils.toPlanMailboxId(senderStageId, senderWorkerId, receiverStageId, receiverWorkerId);
            MailboxMetadata receiverMailboxMetadata =
                new MailboxMetadata(mailboxId, new VirtualServerAddress(receiverServerInstance.getHostname(),
                    receiverServerInstance.getQueryMailboxPort(),
                    receiverWorkerId).toString(), ImmutableMap.of());
            Map<Integer, List<MailboxMetadata>> stageToMailboxMetadataMap =
                senderStagePlanMetadata.getWorkerIdToMailBoxIdsMap().get(senderWorkerId);
            stageToMailboxMetadataMap.putIfAbsent(receiverStageId, new ArrayList<>());
            stageToMailboxMetadataMap.get(receiverStageId).add(receiverMailboxMetadata);
          }
        });
      }
    });
    return null;
  }
}
