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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.query.planner.plannode.DefaultPostOrderTraversalVisitor;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.routing.MailboxMetadata;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.routing.VirtualServerAddress;


public class MailboxAssignmentVisitor extends DefaultPostOrderTraversalVisitor<Void, DispatchablePlanContext> {
  public static final MailboxAssignmentVisitor INSTANCE = new MailboxAssignmentVisitor();

  @Override
  public Void process(PlanNode node, DispatchablePlanContext context) {
    if (node instanceof MailboxSendNode) {
      MailboxSendNode sendNode = (MailboxSendNode) node;
      int senderFragmentId = sendNode.getPlanFragmentId();
      int receiverFragmentId = sendNode.getReceiverStageId();
      Map<Integer, DispatchablePlanMetadata> metadataMap = context.getDispatchablePlanMetadataMap();
      DispatchablePlanMetadata senderMetadata = metadataMap.get(senderFragmentId);
      DispatchablePlanMetadata receiverMetadata = metadataMap.get(receiverFragmentId);
      Map<QueryServerInstance, List<Integer>> senderWorkerIdsMap = senderMetadata.getServerInstanceToWorkerIdMap();
      Map<QueryServerInstance, List<Integer>> receiverWorkerIdsMap = receiverMetadata.getServerInstanceToWorkerIdMap();
      Map<Integer, Map<Integer, MailboxMetadata>> senderMailboxesMap = senderMetadata.getWorkerIdToMailBoxIdsMap();
      Map<Integer, Map<Integer, MailboxMetadata>> receiverMailboxesMap = receiverMetadata.getWorkerIdToMailBoxIdsMap();

      if (sendNode.getDistributionType() == RelDistribution.Type.SINGLETON) {
        // For SINGLETON exchange type, send the data to the same instance (same worker id)
        senderWorkerIdsMap.forEach((serverInstance, workerIds) -> {
          for (int workerId : workerIds) {
            MailboxMetadata mailboxMetadata = new MailboxMetadata(Collections.singletonList(
                MailboxIdUtils.toPlanMailboxId(senderFragmentId, workerId, receiverFragmentId, workerId)),
                Collections.singletonList(new VirtualServerAddress(serverInstance, workerId)), Collections.emptyMap());
            senderMailboxesMap.computeIfAbsent(workerId, k -> new HashMap<>()).put(receiverFragmentId, mailboxMetadata);
            receiverMailboxesMap.computeIfAbsent(workerId, k -> new HashMap<>()).put(senderFragmentId, mailboxMetadata);
          }
        });
      } else if (senderMetadata.isPartitionedTableScan()) {
        // For partitioned table scan, send the data to the worker with the same worker id (not necessary the same
        // instance)
        senderWorkerIdsMap.forEach((senderServerInstance, senderWorkerIds) -> {
          for (int workerId : senderWorkerIds) {
            receiverWorkerIdsMap.forEach((receiverServerInstance, receiverWorkerIds) -> {
              for (int receiverWorkerId : receiverWorkerIds) {
                if (receiverWorkerId == workerId) {
                  String mailboxId =
                      MailboxIdUtils.toPlanMailboxId(senderFragmentId, workerId, receiverFragmentId, workerId);
                  MailboxMetadata serderMailboxMetadata = new MailboxMetadata(Collections.singletonList(mailboxId),
                      Collections.singletonList(new VirtualServerAddress(receiverServerInstance, workerId)),
                      Collections.emptyMap());
                  MailboxMetadata receiverMailboxMetadata = new MailboxMetadata(Collections.singletonList(mailboxId),
                      Collections.singletonList(new VirtualServerAddress(senderServerInstance, workerId)),
                      Collections.emptyMap());
                  senderMailboxesMap.computeIfAbsent(workerId, k -> new HashMap<>())
                      .put(receiverFragmentId, serderMailboxMetadata);
                  receiverMailboxesMap.computeIfAbsent(workerId, k -> new HashMap<>())
                      .put(senderFragmentId, receiverMailboxMetadata);
                  break;
                }
              }
            });
          }
        });
      } else {
        // For other exchange types, send the data to all the instances in the receiver fragment
        // TODO:
        //   1. Add support for more exchange types
        //   2. Keep the receiver worker id sequential in the senderMailboxMetadata so that the partitionId aligns with
        //      the workerId. It is useful for JOIN query when only left table is partitioned.
        senderWorkerIdsMap.forEach((senderServerInstance, senderWorkerIds) -> {
          for (int senderWorkerId : senderWorkerIds) {
            Map<Integer, MailboxMetadata> senderMailboxMetadataMap =
                senderMailboxesMap.computeIfAbsent(senderWorkerId, k -> new HashMap<>());
            receiverWorkerIdsMap.forEach((receiverServerInstance, receiverWorkerIds) -> {
              for (int receiverWorkerId : receiverWorkerIds) {
                Map<Integer, MailboxMetadata> receiverMailboxMetadataMap =
                    receiverMailboxesMap.computeIfAbsent(receiverWorkerId, k -> new HashMap<>());
                String mailboxId = MailboxIdUtils.toPlanMailboxId(senderFragmentId, senderWorkerId, receiverFragmentId,
                    receiverWorkerId);
                MailboxMetadata senderMailboxMetadata =
                    senderMailboxMetadataMap.computeIfAbsent(receiverFragmentId, k -> new MailboxMetadata());
                senderMailboxMetadata.getMailBoxIdList().add(mailboxId);
                senderMailboxMetadata.getVirtualAddressList()
                    .add(new VirtualServerAddress(receiverServerInstance, receiverWorkerId));
                MailboxMetadata receiverMailboxMetadata =
                    receiverMailboxMetadataMap.computeIfAbsent(senderFragmentId, k -> new MailboxMetadata());
                receiverMailboxMetadata.getMailBoxIdList().add(mailboxId);
                receiverMailboxMetadata.getVirtualAddressList()
                    .add(new VirtualServerAddress(senderServerInstance, senderWorkerId));
              }
            });
          }
        });
      }
    }
    return null;
  }
}
