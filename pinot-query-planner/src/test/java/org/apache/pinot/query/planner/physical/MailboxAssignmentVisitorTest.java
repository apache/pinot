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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.routing.MailboxInfo;
import org.apache.pinot.query.routing.MailboxInfos;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.routing.SharedMailboxInfos;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// Tests for mailbox assignment determinism in [MailboxAssignmentVisitor].
///
/// These tests verify that the mailbox info list is sorted by worker ID to ensure
/// deterministic hash-distributed exchange routing. This is critical for correct
/// join results when HashExchange uses (hash % numMailboxes) as an index.
public class MailboxAssignmentVisitorTest extends QueryEnvironmentTestBase {

  @Test
  public void testVariousJoinQueriesHaveSortedMailboxes() {
    String[] queries = {
        // Simple join
        "SELECT * FROM a JOIN b ON a.col1 = b.col1",
        // Join with aggregation
        "SELECT a.col1, COUNT(*) FROM a JOIN b ON a.col1 = b.col1 GROUP BY a.col1",
        "SELECT a.col1, SUM(a.col3), SUM(b.col3) FROM a JOIN b ON a.col1 = b.col1 GROUP BY a.col1",
        // Multi-way join
        "SELECT * FROM a JOIN b ON a.col1 = b.col1 JOIN c ON b.col1 = c.col1",
        // Join with filter
        "SELECT * FROM a JOIN b ON a.col1 = b.col1 WHERE a.col3 > 0",
    };

    for (String query : queries) {
      DispatchableSubPlan subPlan = _queryEnvironment.planQuery(query);
      verifyAllMailboxInfosSorted(subPlan, query);
    }
  }

  @Test
  public void testUnionQueryHasSortedMailboxes() {
    String query = "SELECT col1, SUM(col3) FROM a GROUP BY col1 "
        + "UNION ALL "
        + "SELECT col1, SUM(col3) FROM b GROUP BY col1";

    DispatchableSubPlan subPlan = _queryEnvironment.planQuery(query);
    verifyAllMailboxInfosSorted(subPlan, query);
  }

  private static final int SENDER_STAGE = 1;
  private static final int RECEIVER_STAGE = 0;

  /// A SINGLETON local exchange where sender worker `i` and receiver worker `i` land on different servers (as can
  /// happen for a colocated semi-join during a rolling restart) must not fail: it falls back to a cross-server send
  /// for the diverged worker while keeping the co-located worker local.
  @Test
  public void testSingletonFallsBackToCrossServerWhenWorkersDiverge() {
    // Worker 0 co-located on A; worker 1 diverged (sender on B, receiver on C).
    DispatchablePlanMetadata sender = metadata(Map.of(0, server("A"), 1, server("B")));
    DispatchablePlanMetadata receiver = metadata(Map.of(0, server("A"), 1, server("C")));
    process(singletonSendNode(List.of()), sender, receiver);

    Map<Integer, Map<Integer, MailboxInfos>> senderMailboxes = sender.getWorkerIdToMailboxesMap();
    Map<Integer, Map<Integer, MailboxInfos>> receiverMailboxes = receiver.getWorkerIdToMailboxesMap();

    // Worker 0 is co-located: a single shared local mailbox on host_A on both sides.
    assertTrue(senderMailboxes.get(0).get(RECEIVER_STAGE) instanceof SharedMailboxInfos);
    assertEquals(singleMailbox(senderMailboxes, 0, RECEIVER_STAGE).getHostname(), "host_A");
    assertEquals(singleMailbox(receiverMailboxes, 0, SENDER_STAGE).getHostname(), "host_A");

    // Worker 1 diverged: cross-server send, not a shared mailbox. The sender sends to the receiver's server (C) and
    // the receiver reads from the sender's server (B).
    assertFalse(senderMailboxes.get(1).get(RECEIVER_STAGE) instanceof SharedMailboxInfos);
    assertEquals(singleMailbox(senderMailboxes, 1, RECEIVER_STAGE).getHostname(), "host_C");
    assertEquals(singleMailbox(receiverMailboxes, 1, SENDER_STAGE).getHostname(), "host_B");
  }

  /// A KEYED local exchange (a UNION ALL input carries the projected columns) whose worker counts do not divide
  /// evenly is promoted to a real hash shuffle: HashExchange routes every key consistently across the receivers, so
  /// this is correct for a concatenation and for a keyed join alike.
  @Test
  public void testKeyedSingletonWithUnequalWorkersShuffles() {
    DispatchablePlanMetadata sender = metadata(Map.of(0, server("A"), 1, server("B")));
    DispatchablePlanMetadata receiver = metadata(Map.of(0, server("A"), 1, server("B"), 2, server("C")));
    MailboxSendNode sendNode = singletonSendNode(List.of(0));
    process(sendNode, sender, receiver);

    assertEquals(sendNode.getDistributionType(), RelDistribution.Type.HASH_DISTRIBUTED);
    // Shuffled: every receiver worker reads from every sender worker.
    for (int workerId = 0; workerId < 3; workerId++) {
      assertEquals(expandedWorkerIds(receiver.getWorkerIdToMailboxesMap(), workerId, SENDER_STAGE), List.of(0, 1));
    }
  }

  /// The parallelism path addresses a whole receiver range at the range's FIRST host, which is only valid when the
  /// receiver map was derived from the sender. Here the single sender's two receivers sit on DIFFERENT servers, so
  /// that assumption does not hold: posting both to host_A would strand the receiver on host_B until the deadline.
  /// Co-residency is verified rather than assumed, and the exchange falls back to a full shuffle.
  @Test
  public void testSingletonWithParallelismAcrossHostsShuffles() {
    DispatchablePlanMetadata sender = metadata(Map.of(0, server("A")));
    DispatchablePlanMetadata receiver = metadata(Map.of(0, server("A"), 1, server("B")));
    MailboxSendNode sendNode = singletonSendNode(List.of(0));
    process(sendNode, sender, receiver);

    assertEquals(sendNode.getDistributionType(), RelDistribution.Type.HASH_DISTRIBUTED);
    Set<String> hosts = new HashSet<>();
    for (MailboxInfo mailboxInfo : sender.getWorkerIdToMailboxesMap().get(0).get(RECEIVER_STAGE).getMailboxInfos()) {
      hosts.add(mailboxInfo.getHostname());
    }
    assertEquals(hosts, Set.of("host_A", "host_B"), "Each receiver must be addressed at its own host");
  }

  /// A KEYLESS local exchange must still fail loudly when the workers do not line up. This is the colocated
  /// dynamic-broadcast semi-join build side: every receiver needs the WHOLE build side to build its filter
  /// (the non-colocated variant broadcasts for exactly that reason), so redistributing it would silently
  /// drop matches. A UNION ALL input never reaches here because it carries the projected columns as keys.
  @Test(expectedExceptions = IllegalStateException.class,
      expectedExceptionsMessageRegExp = ".*requires keys.*")
  public void testKeylessSingletonWithUnequalWorkersStillFails() {
    DispatchablePlanMetadata sender = metadata(Map.of(0, server("A"), 1, server("B")));
    DispatchablePlanMetadata receiver = metadata(Map.of(0, server("A"), 1, server("B"), 2, server("C")));
    process(singletonSendNode(List.of()), sender, receiver);
  }

  /// A local exchange whose sender stage has no worker at all (a fully pruned leaf) has nothing to wire 1-to-1, and
  /// computing the parallelism would divide by zero. Every live receiver must still get an entry, holding an empty
  /// mailbox list, or its WorkerMetadata carries a null mailbox map and fails during dispatch serialization.
  @Test
  public void testSingletonWithZeroSendersDoesNotDivideByZero() {
    DispatchablePlanMetadata sender = metadata(Map.of());
    DispatchablePlanMetadata receiver = metadata(Map.of(0, server("A"), 1, server("B")));
    process(singletonSendNode(List.of()), sender, receiver);

    assertTrue(sender.getWorkerIdToMailboxesMap().isEmpty());
    for (int workerId = 0; workerId < 2; workerId++) {
      MailboxInfos mailboxInfos = receiver.getWorkerIdToMailboxesMap().get(workerId).get(SENDER_STAGE);
      assertNotNull(mailboxInfos, "Missing entry for worker: " + workerId);
      assertTrue(mailboxInfos.getMailboxInfos().isEmpty());
    }
  }

  /// A SINGLETON local exchange with parallelism (more receivers than senders) does not assert co-location either: it
  /// rewrites the distribution to HASH and fans each sender worker out to its receiver workers, even cross-server.
  @Test
  public void testSingletonWithParallelismAllowsCrossServer() {
    // 1 sender on A, 2 receivers on B (parallelism 2), so the fan-out is cross-server.
    DispatchablePlanMetadata sender = metadata(Map.of(0, server("A")));
    DispatchablePlanMetadata receiver = metadata(Map.of(0, server("B"), 1, server("B")));
    MailboxSendNode sendNode = singletonSendNode(List.of(0));
    process(sendNode, sender, receiver);

    assertEquals(sendNode.getDistributionType(), RelDistribution.Type.HASH_DISTRIBUTED);
    MailboxInfo senderToReceiver = singleMailbox(sender.getWorkerIdToMailboxesMap(), 0, RECEIVER_STAGE);
    assertEquals(senderToReceiver.getHostname(), "host_B");
    assertEquals(senderToReceiver.getWorkerIds(), List.of(0, 1));
    assertEquals(singleMailbox(receiver.getWorkerIdToMailboxesMap(), 0, SENDER_STAGE).getHostname(), "host_A");
    assertEquals(singleMailbox(receiver.getWorkerIdToMailboxesMap(), 1, SENDER_STAGE).getHostname(), "host_A");
  }

  /// A mismatch means the one-class-list-per-colocated-group invariant regressed (see
  /// [DispatchablePlanMetadata#getPartitionClassIds()]) and must be reported rather than pairing one class with
  /// another.
  @Test(expectedExceptions = IllegalStateException.class,
      expectedExceptionsMessageRegExp = ".*Partition class mismatch.*\\[0, 2\\].*\\[0, 3\\].*")
  public void testDirectExchangeRejectsMismatchedPartitionClasses() {
    DispatchablePlanMetadata sender = metadata(Map.of(0, server("A"), 1, server("B")));
    sender.setPartitionClassIds(new int[]{0, 2});
    DispatchablePlanMetadata receiver = metadata(Map.of(0, server("A"), 1, server("B")));
    receiver.setPartitionClassIds(new int[]{0, 3});
    process(singletonSendNode(List.of()), sender, receiver);
  }

  /// A receiver with no class list took its workers from the candidate servers, so matching worker counts are a
  /// coincidence and the exchange must fall back to a shuffle rather than pair the two 1-to-1.
  @Test
  public void testPrePartitionedSendWithoutMatchingClassesFallsBackToShuffle() {
    DispatchablePlanMetadata sender = prePartitionedSender();
    sender.setPartitionClassIds(new int[]{0, 2});
    DispatchablePlanMetadata receiver = metadata(Map.of(0, server("A"), 1, server("B")));
    process(hashSendNode(), sender, receiver);

    // Shuffled: every receiver worker reads from every sender worker, rather than only from the one with its own id.
    assertEquals(expandedWorkerIds(receiver.getWorkerIdToMailboxesMap(), 0, SENDER_STAGE), List.of(0, 1));
    assertEquals(expandedWorkerIds(receiver.getWorkerIdToMailboxesMap(), 1, SENDER_STAGE), List.of(0, 1));
  }

  /// The control for the test above: with both sides in the same class space the very same shapes are wired 1-to-1.
  @Test
  public void testPrePartitionedSendWithMatchingClassesIsDirect() {
    DispatchablePlanMetadata sender = prePartitionedSender();
    sender.setPartitionClassIds(new int[]{0, 2});
    DispatchablePlanMetadata receiver = metadata(Map.of(0, server("A"), 1, server("B")));
    receiver.setPartitionClassIds(new int[]{0, 2});
    receiver.setPartitionFunction("absHashCodeSum");
    process(hashSendNode(), sender, receiver);

    assertEquals(expandedWorkerIds(receiver.getWorkerIdToMailboxesMap(), 0, SENDER_STAGE), List.of(0));
    assertEquals(expandedWorkerIds(receiver.getWorkerIdToMailboxesMap(), 1, SENDER_STAGE), List.of(1));
  }

  /// A receiver stage with no worker at all (an empty or fully pruned leaf, while another leaf of the plan is not) must
  /// still leave every sender worker an entry holding an empty mailbox list, or the sender's `WorkerMetadata` carries a
  /// null mailbox map and fails while the dispatch request is serialized.
  @Test
  public void testShuffleToReceiverWithoutWorkersKeepsEmptySenderEntry() {
    DispatchablePlanMetadata sender = metadata(Map.of(0, server("A"), 1, server("B")));
    DispatchablePlanMetadata receiver = metadata(Map.of());
    process(hashSendNode(), sender, receiver);

    for (int workerId = 0; workerId < 2; workerId++) {
      MailboxInfos mailboxInfos = sender.getWorkerIdToMailboxesMap().get(workerId).get(RECEIVER_STAGE);
      assertNotNull(mailboxInfos, "Missing entry for worker: " + workerId);
      assertTrue(mailboxInfos.getMailboxInfos().isEmpty(), String.valueOf(mailboxInfos.getMailboxInfos()));
    }
    // Nothing to receive on: the receiver has no worker to hold an entry.
    assertTrue(receiver.getWorkerIdToMailboxesMap().isEmpty());
  }

  /// A pre-partitioned hash exchange whose sender stage has zero workers (all its leaf segments were pruned) must not
  /// be wired as a direct exchange: with the receiver stage also empty (as when every branch of a UNION ALL is fully
  /// pruned), the sender and receiver counts trivially "match" and computing the fan-out parallelism would divide by
  /// zero. It must fall back to the regular wiring, which is a no-op for empty stages.
  @Test
  public void testPrePartitionedExchangeWithZeroWorkersFallsBackToShuffle() {
    DispatchablePlanMetadata sender = metadata(Map.of());
    sender.setPrePartitioned(true);
    DispatchablePlanMetadata receiver = metadata(Map.of());
    process(hashSendNode(), sender, receiver);

    assertTrue(sender.getWorkerIdToMailboxesMap().isEmpty());
    assertTrue(receiver.getWorkerIdToMailboxesMap().isEmpty());
  }

  private static QueryServerInstance server(String id) {
    return new QueryServerInstance(id, "host_" + id, 1, 1);
  }

  private static DispatchablePlanMetadata metadata(Map<Integer, QueryServerInstance> workerIdToServerInstanceMap) {
    DispatchablePlanMetadata metadata = new DispatchablePlanMetadata();
    metadata.setWorkerIdToServerInstanceMap(workerIdToServerInstanceMap);
    return metadata;
  }

  /// A 2 worker sender marked pre-partitioned, i.e. one whose hash send may be wired 1-to-1.
  private static DispatchablePlanMetadata prePartitionedSender() {
    DispatchablePlanMetadata sender = metadata(Map.of(0, server("A"), 1, server("B")));
    sender.setPrePartitioned(true);
    sender.setPartitionFunction("absHashCodeSum");
    return sender;
  }

  private static MailboxSendNode singletonSendNode(List<Integer> keys) {
    DataSchema dataSchema = new DataSchema(new String[]{"col"}, new ColumnDataType[]{ColumnDataType.INT});
    return new MailboxSendNode(SENDER_STAGE, dataSchema, List.of(), RECEIVER_STAGE,
        PinotRelExchangeType.PIPELINE_BREAKER, RelDistribution.Type.SINGLETON, keys, false, null, false, "absHashCode");
  }

  private static MailboxSendNode hashSendNode() {
    DataSchema dataSchema = new DataSchema(new String[]{"col"}, new ColumnDataType[]{ColumnDataType.INT});
    return new MailboxSendNode(SENDER_STAGE, dataSchema, List.of(), RECEIVER_STAGE, PinotRelExchangeType.STREAMING,
        RelDistribution.Type.HASH_DISTRIBUTED, List.of(0), false, null, false, "absHashCode");
  }

  /// The sender worker ids the given receiver worker reads from, in mailbox order.
  private static List<Integer> expandedWorkerIds(Map<Integer, Map<Integer, MailboxInfos>> mailboxesMap, int workerId,
      int stageId) {
    List<Integer> workerIds = new ArrayList<>();
    for (MailboxInfo mailboxInfo : mailboxesMap.get(workerId).get(stageId).getMailboxInfos()) {
      workerIds.addAll(mailboxInfo.getWorkerIds());
    }
    return workerIds;
  }

  private static void process(MailboxSendNode sendNode, DispatchablePlanMetadata sender,
      DispatchablePlanMetadata receiver) {
    DispatchablePlanContext context = Mockito.mock(DispatchablePlanContext.class);
    Mockito.when(context.getDispatchablePlanMetadataMap())
        .thenReturn(Map.of(SENDER_STAGE, sender, RECEIVER_STAGE, receiver));
    MailboxAssignmentVisitor.INSTANCE.process(sendNode, context);
  }

  private static MailboxInfo singleMailbox(Map<Integer, Map<Integer, MailboxInfos>> mailboxesMap, int workerId,
      int stageId) {
    List<MailboxInfo> mailboxInfos = mailboxesMap.get(workerId).get(stageId).getMailboxInfos();
    assertEquals(mailboxInfos.size(), 1);
    return mailboxInfos.get(0);
  }

  private void verifyAllMailboxInfosSorted(DispatchableSubPlan subPlan, String query) {
    for (DispatchablePlanFragment fragment : subPlan.getQueryStages()) {
      List<WorkerMetadata> workerMetadataList = fragment.getWorkerMetadataList();

      for (WorkerMetadata workerMetadata : workerMetadataList) {
        Map<Integer, MailboxInfos> mailboxInfosMap = workerMetadata.getMailboxInfosMap();

        for (Map.Entry<Integer, MailboxInfos> entry : mailboxInfosMap.entrySet()) {
          MailboxInfos mailboxInfos = entry.getValue();
          List<MailboxInfo> infoList = mailboxInfos.getMailboxInfos();

          // Expand all worker IDs from all MailboxInfos
          List<Integer> expandedWorkerIds = new ArrayList<>();
          for (MailboxInfo info : infoList) {
            expandedWorkerIds.addAll(info.getWorkerIds());
          }

          // Verify the expanded list is sorted
          for (int i = 0; i < expandedWorkerIds.size() - 1; i++) {
            assertTrue(expandedWorkerIds.get(i) < expandedWorkerIds.get(i + 1),
                String.format("Expanded worker IDs not sorted: %d at index %d, %d at index %d",
                    expandedWorkerIds.get(i), i, expandedWorkerIds.get(i + 1), i + 1));
          }
        }
      }
    }
  }
}
