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
import static org.testng.Assert.assertTrue;


/**
 * Tests for mailbox assignment determinism in {@link MailboxAssignmentVisitor}.
 *
 * These tests verify that the mailbox info list is sorted by worker ID to ensure
 * deterministic hash-distributed exchange routing. This is critical for correct
 * join results when HashExchange uses (hash % numMailboxes) as an index.
 */
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

  /// An unequal-but-non-multiple worker count (2 senders, 3 receivers) must be rejected rather than rounding the
  /// parallelism down to 1 and silently dropping the extra receiver.
  @Test(expectedExceptions = IllegalStateException.class,
      expectedExceptionsMessageRegExp = ".*multiple of number of senders.*")
  public void testSingletonRejectsNonMultipleReceiverCount() {
    DispatchablePlanMetadata sender = metadata(Map.of(0, server("A"), 1, server("B")));
    DispatchablePlanMetadata receiver = metadata(Map.of(0, server("A"), 1, server("B"), 2, server("C")));
    process(singletonSendNode(List.of(0)), sender, receiver);
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

  private static QueryServerInstance server(String id) {
    return new QueryServerInstance(id, "host_" + id, 1, 1);
  }

  private static DispatchablePlanMetadata metadata(Map<Integer, QueryServerInstance> workerIdToServerInstanceMap) {
    DispatchablePlanMetadata metadata = new DispatchablePlanMetadata();
    metadata.setWorkerIdToServerInstanceMap(workerIdToServerInstanceMap);
    return metadata;
  }

  private static MailboxSendNode singletonSendNode(List<Integer> keys) {
    DataSchema dataSchema = new DataSchema(new String[]{"col"}, new ColumnDataType[]{ColumnDataType.INT});
    return new MailboxSendNode(SENDER_STAGE, dataSchema, List.of(), RECEIVER_STAGE,
        PinotRelExchangeType.PIPELINE_BREAKER, RelDistribution.Type.SINGLETON, keys, false, null, false, "absHashCode");
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
