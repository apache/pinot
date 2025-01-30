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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
      // NOTE: Using Integer to avoid boxing
      Integer senderStageId = sendNode.getStageId();
      for (Integer receiverStageId : sendNode.getReceiverStageIds()) {
        Map<Integer, DispatchablePlanMetadata> metadataMap = context.getDispatchablePlanMetadataMap();
        DispatchablePlanMetadata senderMetadata = metadataMap.get(senderStageId);
        DispatchablePlanMetadata receiverMetadata = metadataMap.get(receiverStageId);
        Map<Integer, QueryServerInstance> senderServerMap = senderMetadata.getWorkerIdToServerInstanceMap();
        Map<Integer, QueryServerInstance> receiverServerMap = receiverMetadata.getWorkerIdToServerInstanceMap();
        Map<Integer, Map<Integer, MailboxInfos>> senderMailboxesMap = senderMetadata.getWorkerIdToMailboxesMap();
        Map<Integer, Map<Integer, MailboxInfos>> receiverMailboxesMap = receiverMetadata.getWorkerIdToMailboxesMap();

        int numSenders = senderServerMap.size();
        int numReceivers = receiverServerMap.size();
        if (sendNode.getDistributionType() == RelDistribution.Type.SINGLETON) {
          // NOTE: We use SINGLETON to represent local exchange. The actual distribution type is determined by the
          //       parallelism.
          if (numSenders == numReceivers) {
            // Send the data to the same instance (same worker id) with SINGLETON distribution type
            for (int workerId = 0; workerId < numSenders; workerId++) {
              QueryServerInstance senderServer = senderServerMap.get(workerId);
              QueryServerInstance receiverServer = receiverServerMap.get(workerId);
              Preconditions.checkState(senderServer.equals(receiverServer),
                  "Got different server for SINGLETON distribution type for worker id: %s, sender: %s, receiver: %s",
                  workerId, senderServer, receiverServer);
              MailboxInfos mailboxInfos = new SharedMailboxInfos(
                  new MailboxInfo(senderServer.getHostname(), senderServer.getQueryMailboxPort(), List.of(workerId)));
              senderMailboxesMap.computeIfAbsent(workerId, k -> new HashMap<>()).put(receiverStageId, mailboxInfos);
              receiverMailboxesMap.computeIfAbsent(workerId, k -> new HashMap<>()).put(senderStageId, mailboxInfos);
            }
          } else {
            // Hash distribute the data to multiple receivers on the same server instance
            // TODO: Support local exchange with parallelism but no key
            Preconditions.checkState(!sendNode.getKeys().isEmpty(), "Local exchange with parallelism requires keys");
            sendNode.setDistributionType(RelDistribution.Type.HASH_DISTRIBUTED);

            Preconditions.checkState(numReceivers % numSenders == 0,
                "Number of receivers: %s should be a multiple of number of senders: %s for local exchange",
                numReceivers, numSenders);
            int parallelism = numReceivers / numSenders;
            int receiverWorkerId = 0;
            for (int senderWorkerId = 0; senderWorkerId < numSenders; senderWorkerId++) {
              QueryServerInstance senderServer = senderServerMap.get(senderWorkerId);
              QueryServerInstance receiverServer = receiverServerMap.get(receiverWorkerId);
              Preconditions.checkState(senderServer.equals(receiverServer),
                  "Got different server for local exchange, sender %s: %s, receiver %s: %s", senderWorkerId,
                  senderServer, receiverWorkerId, receiverServer);
              computeDirectExchangeWithParallelism(senderMailboxesMap, receiverMailboxesMap, senderStageId,
                  receiverStageId, senderWorkerId, receiverWorkerId, senderServer, receiverServer, parallelism);
              receiverWorkerId += parallelism;
            }
          }
        } else if (senderMetadata.isPrePartitioned() && isDirectExchangeCompatible(senderMetadata, receiverMetadata)) {
          // Direct exchange possible:
          // - Without parallelism:
          //   Send the data to the worker with the same worker id (not necessary the same instance), 1-to-1 mapping
          // - With parallelism:
          //   Fanout based on parallelism from each sender workerID to sequentially increment receiver workerIDs
          int parallelism = numReceivers / numSenders;
          if (parallelism == 1) {
            // 1-to-1 mapping
            for (int workerId = 0; workerId < numSenders; workerId++) {
              QueryServerInstance senderServer = senderServerMap.get(workerId);
              QueryServerInstance receiverServer = receiverServerMap.get(workerId);
              List<Integer> workerIds = List.of(workerId);
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
                  .put(receiverStageId, receiverMailboxInfos);
              receiverMailboxesMap.computeIfAbsent(workerId, k -> new HashMap<>())
                  .put(senderStageId, senderMailboxInfos);
            }
          } else {
            // 1-to-<parallelism> mapping
            int receiverWorkerId = 0;
            for (int senderWorkerId = 0; senderWorkerId < numSenders; senderWorkerId++) {
              QueryServerInstance senderServer = senderServerMap.get(senderWorkerId);
              QueryServerInstance receiverServer = receiverServerMap.get(receiverWorkerId);
              computeDirectExchangeWithParallelism(senderMailboxesMap, receiverMailboxesMap, senderStageId,
                  receiverStageId, senderWorkerId, receiverWorkerId, senderServer, receiverServer, parallelism);
              receiverWorkerId += parallelism;
            }
          }
        } else {
          // For other exchange types, send the data to all the instances in the receiver fragment
          // TODO: Add support for more exchange types
          connectWorkers(receiverStageId, receiverServerMap, senderMailboxesMap, numSenders);
          connectWorkers(senderStageId, senderServerMap, receiverMailboxesMap, numReceivers);
        }
      }
    }
    return null;
  }

  private void computeDirectExchangeWithParallelism(Map<Integer, Map<Integer, MailboxInfos>> senderMailboxesMap,
      Map<Integer, Map<Integer, MailboxInfos>> receiverMailboxesMap, Integer senderStageId, Integer receiverStageId,
      int senderWorkerId, int receiverWorkerId, QueryServerInstance senderServer, QueryServerInstance receiverServer,
      int parallelism) {
    List<Integer> receiverWorkerIds = new ArrayList<>(parallelism);
    senderMailboxesMap.computeIfAbsent(senderWorkerId, k -> new HashMap<>())
        .put(receiverStageId, new MailboxInfos(
            new MailboxInfo(receiverServer.getHostname(), receiverServer.getQueryMailboxPort(), receiverWorkerIds)));
    MailboxInfos senderMailboxInfos = new SharedMailboxInfos(
        new MailboxInfo(senderServer.getHostname(), senderServer.getQueryMailboxPort(), List.of(senderWorkerId)));
    for (int i = 0; i < parallelism; i++) {
      receiverWorkerIds.add(receiverWorkerId);
      receiverMailboxesMap.computeIfAbsent(receiverWorkerId, k -> new HashMap<>())
          .put(senderStageId, senderMailboxInfos);
      receiverWorkerId++;
    }
  }

  private static boolean isDirectExchangeCompatible(DispatchablePlanMetadata sender,
      DispatchablePlanMetadata receiver) {
    int numSenders = sender.getWorkerIdToServerInstanceMap().size();
    int numReceivers = receiver.getWorkerIdToServerInstanceMap().size();
    return numSenders * sender.getPartitionParallelism() == numReceivers && sender.getPartitionFunction() != null
        && sender.getPartitionFunction().equalsIgnoreCase(receiver.getPartitionFunction());
  }

  private void connectWorkers(int stageId, Map<Integer, QueryServerInstance> serverMap,
      Map<Integer, Map<Integer, MailboxInfos>> mailboxesMap, int numWorkers) {
    Map<QueryServerInstance, List<Integer>> serverToWorkerIdsMap = new HashMap<>();
    int numSourceWorkers = serverMap.size();
    for (int workerId = 0; workerId < numSourceWorkers; workerId++) {
      serverToWorkerIdsMap.computeIfAbsent(serverMap.get(workerId), k -> new ArrayList<>()).add(workerId);
    }
    List<MailboxInfo> mailboxInfoList = new ArrayList<>(serverToWorkerIdsMap.size());
    for (Map.Entry<QueryServerInstance, List<Integer>> entry : serverToWorkerIdsMap.entrySet()) {
      QueryServerInstance server = entry.getKey();
      List<Integer> workerIds = entry.getValue();
      mailboxInfoList.add(new MailboxInfo(server.getHostname(), server.getQueryMailboxPort(), workerIds));
    }
    MailboxInfos mailboxInfos =
        numWorkers > 1 ? new SharedMailboxInfos(mailboxInfoList) : new MailboxInfos(mailboxInfoList);
    for (int workerId = 0; workerId < numWorkers; workerId++) {
      mailboxesMap.computeIfAbsent(workerId, k -> new HashMap<>()).put(stageId, mailboxInfos);
    }
  }
}
