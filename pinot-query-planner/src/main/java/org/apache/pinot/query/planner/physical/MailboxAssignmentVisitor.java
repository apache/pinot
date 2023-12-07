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

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.HashMap;
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
      Map<Integer, QueryServerInstance> senderServerMap = senderMetadata.getWorkerIdToServerInstanceMap();
      Map<Integer, QueryServerInstance> receiverServerMap = receiverMetadata.getWorkerIdToServerInstanceMap();
      Map<Integer, Map<Integer, MailboxMetadata>> senderMailboxesMap = senderMetadata.getWorkerIdToMailboxesMap();
      Map<Integer, Map<Integer, MailboxMetadata>> receiverMailboxesMap = receiverMetadata.getWorkerIdToMailboxesMap();

      int numSenders = senderServerMap.size();
      int numReceivers = receiverServerMap.size();
      if (sendNode.getDistributionType() == RelDistribution.Type.SINGLETON) {
        // For SINGLETON exchange type, send the data to the same instance (same worker id)
        Preconditions.checkState(numSenders == numReceivers,
            "Got different number of workers for SINGLETON distribution type, sender: %s, receiver: %s", numSenders,
            numReceivers);
        for (int workerId = 0; workerId < numSenders; workerId++) {
          QueryServerInstance senderServer = senderServerMap.get(workerId);
          QueryServerInstance receiverServer = receiverServerMap.get(workerId);
          Preconditions.checkState(senderServer.equals(receiverServer),
              "Got different server for SINGLETON distribution type for worker id: %s, sender: %s, receiver: %s",
              workerId, senderServer, receiverServer);
          MailboxMetadata mailboxMetadata = new MailboxMetadata(Collections.singletonList(
              MailboxIdUtils.toPlanMailboxId(senderFragmentId, workerId, receiverFragmentId, workerId)),
              Collections.singletonList(new VirtualServerAddress(senderServer, workerId)), Collections.emptyMap());
          senderMailboxesMap.computeIfAbsent(workerId, k -> new HashMap<>()).put(receiverFragmentId, mailboxMetadata);
          receiverMailboxesMap.computeIfAbsent(workerId, k -> new HashMap<>()).put(senderFragmentId, mailboxMetadata);
        }
      } else if (senderMetadata.isPrePartitioned() && isDirectExchangeCompatible(senderMetadata, receiverMetadata)) {
        // - direct exchange possible:
        //   1. send the data to the worker with the same worker id (not necessary the same instance), 1-to-1 mapping
        //   2. When partition parallelism is configured, fanout based on partition parallelism from each sender
        //      workerID to sequentially increment receiver workerIDs
        int partitionParallelism = numReceivers / numSenders;
        if (partitionParallelism == 1) {
          // 1-to-1 mapping
          for (int workerId = 0; workerId < numSenders; workerId++) {
            String mailboxId = MailboxIdUtils.toPlanMailboxId(senderFragmentId, workerId, receiverFragmentId, workerId);
            MailboxMetadata serderMailboxMetadata = new MailboxMetadata(Collections.singletonList(mailboxId),
                Collections.singletonList(new VirtualServerAddress(receiverServerMap.get(workerId), workerId)),
                Collections.emptyMap());
            MailboxMetadata receiverMailboxMetadata = new MailboxMetadata(Collections.singletonList(mailboxId),
                Collections.singletonList(new VirtualServerAddress(senderServerMap.get(workerId), workerId)),
                Collections.emptyMap());
            senderMailboxesMap.computeIfAbsent(workerId, k -> new HashMap<>())
                .put(receiverFragmentId, serderMailboxMetadata);
            receiverMailboxesMap.computeIfAbsent(workerId, k -> new HashMap<>())
                .put(senderFragmentId, receiverMailboxMetadata);
          }
        } else {
          // 1-to-<partition_parallelism> mapping
          int receiverWorkerId = 0;
          for (int senderWorkerId = 0; senderWorkerId < numSenders; senderWorkerId++) {
            VirtualServerAddress senderAddress =
                new VirtualServerAddress(senderServerMap.get(senderWorkerId), senderWorkerId);
            MailboxMetadata senderMailboxMetadata = new MailboxMetadata();
            senderMailboxesMap.computeIfAbsent(senderWorkerId, k -> new HashMap<>())
                .put(receiverFragmentId, senderMailboxMetadata);
            for (int i = 0; i < partitionParallelism; i++) {
              VirtualServerAddress receiverAddress =
                  new VirtualServerAddress(receiverServerMap.get(receiverWorkerId), receiverWorkerId);
              String mailboxId = MailboxIdUtils.toPlanMailboxId(senderFragmentId, senderWorkerId, receiverFragmentId,
                  receiverWorkerId);
              senderMailboxMetadata.getMailBoxIdList().add(mailboxId);
              senderMailboxMetadata.getVirtualAddressList().add(receiverAddress);

              MailboxMetadata receiverMailboxMetadata =
                  receiverMailboxesMap.computeIfAbsent(receiverWorkerId, k -> new HashMap<>())
                      .computeIfAbsent(senderFragmentId, k -> new MailboxMetadata());
              receiverMailboxMetadata.getMailBoxIdList().add(mailboxId);
              receiverMailboxMetadata.getVirtualAddressList().add(senderAddress);

              receiverWorkerId++;
            }
          }
        }
      } else {
        // For other exchange types, send the data to all the instances in the receiver fragment
        // NOTE: Keep the receiver worker id sequential in the senderMailboxMetadata so that the partitionId aligns with
        //       the workerId. It is useful for JOIN query when only left table is partitioned.
        // TODO: Add support for more exchange types
        for (int senderWorkerId = 0; senderWorkerId < numSenders; senderWorkerId++) {
          VirtualServerAddress senderAddress =
              new VirtualServerAddress(senderServerMap.get(senderWorkerId), senderWorkerId);
          MailboxMetadata senderMailboxMetadata = new MailboxMetadata();
          senderMailboxesMap.computeIfAbsent(senderWorkerId, k -> new HashMap<>())
              .put(receiverFragmentId, senderMailboxMetadata);
          for (int receiverWorkerId = 0; receiverWorkerId < numReceivers; receiverWorkerId++) {
            VirtualServerAddress receiverAddress =
                new VirtualServerAddress(receiverServerMap.get(receiverWorkerId), receiverWorkerId);
            String mailboxId =
                MailboxIdUtils.toPlanMailboxId(senderFragmentId, senderWorkerId, receiverFragmentId, receiverWorkerId);
            senderMailboxMetadata.getMailBoxIdList().add(mailboxId);
            senderMailboxMetadata.getVirtualAddressList().add(receiverAddress);

            MailboxMetadata receiverMailboxMetadata =
                receiverMailboxesMap.computeIfAbsent(receiverWorkerId, k -> new HashMap<>())
                    .computeIfAbsent(senderFragmentId, k -> new MailboxMetadata());
            receiverMailboxMetadata.getMailBoxIdList().add(mailboxId);
            receiverMailboxMetadata.getVirtualAddressList().add(senderAddress);
          }
        }
      }
    }
    return null;
  }

  private boolean isDirectExchangeCompatible(DispatchablePlanMetadata sender, DispatchablePlanMetadata receiver) {
    Map<Integer, QueryServerInstance> senderServerMap = sender.getWorkerIdToServerInstanceMap();
    Map<Integer, QueryServerInstance> receiverServerMap = receiver.getWorkerIdToServerInstanceMap();

    int numSenders = senderServerMap.size();
    int numReceivers = receiverServerMap.size();
    if (sender.getScannedTables().size() > 0 && receiver.getScannedTables().size() == 0) {
      // leaf-to-intermediate condition
      return numSenders * sender.getPartitionParallelism() == numReceivers
          && sender.getPartitionFunction() != null
          && sender.getPartitionFunction().equalsIgnoreCase(receiver.getPartitionFunction());
    } else {
      // dynamic-broadcast condition || intermediate-to-intermediate
      return numSenders == numReceivers
          && sender.getPartitionFunction() != null
          && sender.getPartitionFunction().equalsIgnoreCase(receiver.getPartitionFunction());
    }
  }
}
