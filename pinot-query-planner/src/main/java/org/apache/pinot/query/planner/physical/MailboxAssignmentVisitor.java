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
import java.util.Comparator;
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
          // NOTE: We use SINGLETON to represent a local exchange. The actual distribution type is determined by the
          //       parallelism: 1-to-1 when sender and receiver have the same number of workers, otherwise the data is
          //       hash distributed to the parallel receivers on each server. computeDirectExchange handles the
          //       co-location assumption and its cross-server fallback.
          if (numSenders != numReceivers) {
            // Local exchange with parallelism: hash distribute to the parallel receivers, so keys are required.
            // TODO: Support local exchange with parallelism but no key
            Preconditions.checkState(!sendNode.getKeys().isEmpty(), "Local exchange with parallelism requires keys");
            sendNode.setDistributionType(RelDistribution.Type.HASH_DISTRIBUTED);
            Preconditions.checkState(numReceivers % numSenders == 0,
                "Number of receivers: %s should be a multiple of number of senders: %s for local exchange",
                numReceivers, numSenders);
          }
          int parallelism = numReceivers / numSenders;
          computeDirectExchange(senderMailboxesMap, receiverMailboxesMap, senderStageId, receiverStageId,
              senderServerMap, receiverServerMap, numSenders, parallelism);
        } else if (senderMetadata.isPrePartitioned() && isDirectExchangeCompatible(senderMetadata, receiverMetadata)) {
          // Direct exchange: the data is already pre-partitioned, so send it 1-to-1 to the worker with the same worker
          // id (with parallelism, fan out each sender worker to a contiguous range of receiver workers). The
          // co-location handling is the same as SINGLETON, see computeDirectExchange.
          int parallelism = numReceivers / numSenders;
          computeDirectExchange(senderMailboxesMap, receiverMailboxesMap, senderStageId, receiverStageId,
              senderServerMap, receiverServerMap, numSenders, parallelism);
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

  /// Wires the mailboxes for a direct exchange where sender worker `i` sends to receiver worker `i` (or, with
  /// `parallelism` > 1, fans out to a contiguous range of `parallelism` receiver workers). Shared by the SINGLETON
  /// (local exchange) and pre-partitioned exchange paths.
  ///
  /// Sender worker `i` and receiver worker `i` are normally co-located on the same server, because the receiver worker
  /// map is derived from the sender's (see `WorkerManager#assignWorkersForLocalExchange`). When they are co-located the
  /// data is exchanged in-process via a single shared local mailbox with no network hop.
  ///
  /// They are not guaranteed to be co-located, so this does not assert it. A colocated semi-join is the exception:
  /// `PinotJoinToDynamicBroadcastRule` emits a SINGLETON `PIPELINE_BREAKER` exchange on the build side purely from the
  /// `is_colocated_by_join_keys` hint, and the build (sender) leaf and join (receiver) leaf are routed independently
  /// per table. During a rolling restart some segments are temporarily not queryable, so each side may reroute a
  /// partition to a different replica, leaving worker `i` on different servers. Rather than failing the query, we fall
  /// back to a cross-server send: the exchange stays correct because worker id still maps to the same partition on both
  /// sides, and we only lose locality (one extra network hop) until routing re-stabilizes.
  private void computeDirectExchange(Map<Integer, Map<Integer, MailboxInfos>> senderMailboxesMap,
      Map<Integer, Map<Integer, MailboxInfos>> receiverMailboxesMap, Integer senderStageId, Integer receiverStageId,
      Map<Integer, QueryServerInstance> senderServerMap, Map<Integer, QueryServerInstance> receiverServerMap,
      int numSenders, int parallelism) {
    if (parallelism == 1) {
      // 1-to-1 mapping
      for (int workerId = 0; workerId < numSenders; workerId++) {
        QueryServerInstance senderServer = senderServerMap.get(workerId);
        QueryServerInstance receiverServer = receiverServerMap.get(workerId);
        List<Integer> workerIds = List.of(workerId);
        MailboxInfos senderMailboxInfos;
        MailboxInfos receiverMailboxInfos;
        if (senderServer.equals(receiverServer)) {
          // Co-located: share one local mailbox, exchanged in-process with no network hop.
          MailboxInfos sharedMailboxInfos = new SharedMailboxInfos(
              new MailboxInfo(senderServer.getHostname(), senderServer.getQueryMailboxPort(), workerIds));
          senderMailboxInfos = sharedMailboxInfos;
          receiverMailboxInfos = sharedMailboxInfos;
        } else {
          // Not co-located: cross-server send via separate sender and receiver mailboxes.
          // TODO: Match the sender and receiver worker assignments upfront so co-located data never crosses the wire.
          //   The sender and receiver leaves are routed independently per table today; co-routing them onto a shared
          //   worker-to-server map (picking, per partition, a server that hosts the partition for both sides) would
          //   keep this exchange local even during a rolling restart, falling back to a cross-server send only for
          //   partitions that genuinely cannot be co-located. See PinotJoinToDynamicBroadcastRule and WorkerManager's
          //   leaf worker assignment.
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
    if (numSenders * sender.getPartitionParallelism() != numReceivers) {
      return false;
    }
    if (sender.getPartitionFunction() == null) {
      return receiver.getPartitionFunction() == null;
    }
    return sender.getPartitionFunction().equalsIgnoreCase(receiver.getPartitionFunction());
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
    // Sort by first workerId to ensure deterministic ordering.
    // This is critical for hash-distributed exchanges where (hash % numMailboxes) is used as an index to choose the
    // receiving worker.
    // Without this sorting, different stages could route the same hash value to different workers, resulting in
    // incorrect join/union/intersect results for pre-partitioned sends (where a full partition shuffle is skipped).
    mailboxInfoList.sort(Comparator.comparingInt(info -> info.getWorkerIds().get(0)));
    MailboxInfos mailboxInfos =
        numWorkers > 1 ? new SharedMailboxInfos(mailboxInfoList) : new MailboxInfos(mailboxInfoList);
    for (int workerId = 0; workerId < numWorkers; workerId++) {
      mailboxesMap.computeIfAbsent(workerId, k -> new HashMap<>()).put(stageId, mailboxInfos);
    }
  }
}
