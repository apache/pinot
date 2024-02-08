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
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.query.planner.plannode.DefaultPostOrderTraversalVisitor;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.routing.MailboxInfo;
import org.apache.pinot.query.routing.MailboxInfos;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.routing.SharedMailboxInfos;


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
      Map<Integer, Map<Integer, MailboxInfos>> senderMailboxesMap = senderMetadata.getWorkerIdToMailboxesMap();
      Map<Integer, Map<Integer, MailboxInfos>> receiverMailboxesMap = receiverMetadata.getWorkerIdToMailboxesMap();

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
          MailboxInfos mailboxInfos = new SharedMailboxInfos(
              new MailboxInfo(senderServer.getHostname(), senderServer.getQueryMailboxPort(),
                  ImmutableList.of(workerId)));
          senderMailboxesMap.computeIfAbsent(workerId, k -> new HashMap<>()).put(receiverFragmentId, mailboxInfos);
          receiverMailboxesMap.computeIfAbsent(workerId, k -> new HashMap<>()).put(senderFragmentId, mailboxInfos);
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
            QueryServerInstance senderServer = senderServerMap.get(workerId);
            QueryServerInstance receiverServer = receiverServerMap.get(workerId);
            List<Integer> workerIds = ImmutableList.of(workerId);
            MailboxInfos senderMailboxInfos;
            MailboxInfos receiverMailboxInfos;
            if (senderServer.equals(receiverServer)) {
              senderMailboxInfos = new SharedMailboxInfos(
                  new MailboxInfo(senderServer.getHostname(), senderServer.getQueryMailboxPort(), workerIds));
              receiverMailboxInfos = senderMailboxInfos;
            } else {
              senderMailboxInfos = new MailboxInfos(
                  new MailboxInfo(senderServer.getHostname(), senderServer.getQueryMailboxPort(), workerIds));
              receiverMailboxInfos = new MailboxInfos(
                  new MailboxInfo(receiverServer.getHostname(), receiverServer.getQueryMailboxPort(), workerIds));
            }
            senderMailboxesMap.computeIfAbsent(workerId, k -> new HashMap<>())
                .put(receiverFragmentId, receiverMailboxInfos);
            receiverMailboxesMap.computeIfAbsent(workerId, k -> new HashMap<>())
                .put(senderFragmentId, senderMailboxInfos);
          }
        } else {
          // 1-to-<partition_parallelism> mapping
          int receiverWorkerId = 0;
          for (int senderWorkerId = 0; senderWorkerId < numSenders; senderWorkerId++) {
            QueryServerInstance senderServer = senderServerMap.get(senderWorkerId);
            QueryServerInstance receiverServer = receiverServerMap.get(receiverWorkerId);
            List<Integer> receiverWorkerIds = new ArrayList<>(partitionParallelism);
            senderMailboxesMap.computeIfAbsent(senderWorkerId, k -> new HashMap<>()).put(receiverFragmentId,
                new MailboxInfos(new MailboxInfo(receiverServer.getHostname(), receiverServer.getQueryMailboxPort(),
                    receiverWorkerIds)));
            MailboxInfos senderMailboxInfos = new SharedMailboxInfos(
                new MailboxInfo(senderServer.getHostname(), senderServer.getQueryMailboxPort(),
                    ImmutableList.of(senderWorkerId)));
            for (int i = 0; i < partitionParallelism; i++) {
              receiverWorkerIds.add(receiverWorkerId);
              receiverMailboxesMap.computeIfAbsent(receiverWorkerId, k -> new HashMap<>())
                  .put(senderFragmentId, senderMailboxInfos);
              receiverWorkerId++;
            }
          }
        }
      } else {
        // For other exchange types, send the data to all the instances in the receiver fragment
        // TODO: Add support for more exchange types
        List<MailboxInfo> receiverMailboxInfoList = getMailboxInfos(receiverServerMap);
        MailboxInfos receiverMailboxInfos = numSenders > 1 ? new SharedMailboxInfos(receiverMailboxInfoList)
            : new MailboxInfos(receiverMailboxInfoList);
        for (int senderWorkerId = 0; senderWorkerId < numSenders; senderWorkerId++) {
          senderMailboxesMap.computeIfAbsent(senderWorkerId, k -> new HashMap<>())
              .put(receiverFragmentId, receiverMailboxInfos);
        }
        List<MailboxInfo> senderMailboxInfoList = getMailboxInfos(senderServerMap);
        MailboxInfos senderMailboxInfos =
            numReceivers > 1 ? new SharedMailboxInfos(senderMailboxInfoList) : new MailboxInfos(senderMailboxInfoList);
        for (int receiverWorkerId = 0; receiverWorkerId < numReceivers; receiverWorkerId++) {
          receiverMailboxesMap.computeIfAbsent(receiverWorkerId, k -> new HashMap<>())
              .put(senderFragmentId, senderMailboxInfos);
        }
      }
    }
    return null;
  }

  private static boolean isDirectExchangeCompatible(DispatchablePlanMetadata sender,
      DispatchablePlanMetadata receiver) {
    Map<Integer, QueryServerInstance> senderServerMap = sender.getWorkerIdToServerInstanceMap();
    Map<Integer, QueryServerInstance> receiverServerMap = receiver.getWorkerIdToServerInstanceMap();

    int numSenders = senderServerMap.size();
    int numReceivers = receiverServerMap.size();
    if (!sender.getScannedTables().isEmpty() && receiver.getScannedTables().isEmpty()) {
      // leaf-to-intermediate condition
      return numSenders * sender.getPartitionParallelism() == numReceivers && sender.getPartitionFunction() != null
          && sender.getPartitionFunction().equalsIgnoreCase(receiver.getPartitionFunction());
    } else {
      // dynamic-broadcast condition || intermediate-to-intermediate
      return numSenders == numReceivers && sender.getPartitionFunction() != null && sender.getPartitionFunction()
          .equalsIgnoreCase(receiver.getPartitionFunction());
    }
  }

  private static List<MailboxInfo> getMailboxInfos(Map<Integer, QueryServerInstance> workerIdToServerMap) {
    Map<QueryServerInstance, List<Integer>> serverToWorkerIdsMap = new HashMap<>();
    int numServers = workerIdToServerMap.size();
    for (int workerId = 0; workerId < numServers; workerId++) {
      serverToWorkerIdsMap.computeIfAbsent(workerIdToServerMap.get(workerId), k -> new ArrayList<>()).add(workerId);
    }
    return serverToWorkerIdsMap.entrySet().stream()
        .map(e -> new MailboxInfo(e.getKey().getHostname(), e.getKey().getQueryMailboxPort(), e.getValue()))
        .collect(Collectors.toList());
  }
}
