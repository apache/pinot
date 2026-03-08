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
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.routing.MailboxInfo;
import org.apache.pinot.query.routing.MailboxInfos;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.testng.annotations.Test;

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
