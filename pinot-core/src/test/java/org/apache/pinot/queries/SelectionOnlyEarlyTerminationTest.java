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
package org.apache.pinot.queries;

import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.core.operator.CombineOperator;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests selection only queries
 */
public class SelectionOnlyEarlyTerminationTest extends BaseSingleValueQueriesTest {

  /**
   * Default test setup, duplicate server instance into DataMap.
   *
   * See {@link #getBrokerResponseForBrokerRequest}.
   */
  private static int numDocsPerTestSegment = 30000;
  private static int numTestServers = 2;

  /**
   * In order to ensure each thread is executing more than 1 segment, this test is against
   * (2 * MAX_NUM_THREADS_PER_QUERY) segments.
   */
  @Override
  protected int getNumSegmentDataManagers() {
    return CombineOperator.MAX_NUM_THREADS_PER_QUERY * 2;
  }

  /**
   * With early termination, Selection Only query is scheduled with getNumSegmentDataManagers() threads,
   * total segment processed is same at num threads.
   */
  @Test
  public void testSelectOnlyQuery() {
    int numThreadsPerInstanceRequest = CombineOperator.MAX_NUM_THREADS_PER_QUERY;

    // LIMIT = 5,15,35,75,155,315,635
    for (int limit = 5; limit < 1000; limit += (limit + 5)) {
      String query = String.format("SELECT column1, column7, column9, column6 FROM testTable LIMIT %d", limit);
      int numColumnsInSelection = 4;
      BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
      Assert.assertNotNull(brokerResponse.getSelectionResults());
      Assert.assertNull(brokerResponse.getResultTable());
      Assert.assertEquals(brokerResponse.getNumSegmentsMatched(), numThreadsPerInstanceRequest * numTestServers);
      Assert.assertEquals(brokerResponse.getNumSegmentsProcessed(), numThreadsPerInstanceRequest * numTestServers);
      Assert.assertEquals(brokerResponse.getNumDocsScanned(), numThreadsPerInstanceRequest * numTestServers * limit);
      Assert.assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
      Assert.assertEquals(brokerResponse.getNumEntriesScannedPostFilter(),
          numThreadsPerInstanceRequest * numTestServers * limit * numColumnsInSelection);
      Assert.assertEquals(brokerResponse.getTotalDocs(),
          numThreadsPerInstanceRequest * numTestServers * numDocsPerTestSegment);

      brokerResponse = getBrokerResponseForSqlQuery(query);
      Assert.assertNull(brokerResponse.getSelectionResults());
      Assert.assertNotNull(brokerResponse.getResultTable());
      Assert.assertEquals(brokerResponse.getNumSegmentsMatched(), numThreadsPerInstanceRequest * numTestServers);
      Assert.assertEquals(brokerResponse.getNumSegmentsProcessed(), numThreadsPerInstanceRequest * numTestServers);
      Assert.assertEquals(brokerResponse.getNumDocsScanned(), numThreadsPerInstanceRequest * numTestServers * limit);
      Assert.assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
      Assert.assertEquals(brokerResponse.getNumEntriesScannedPostFilter(),
          numThreadsPerInstanceRequest * numTestServers * limit * numColumnsInSelection);
      Assert.assertEquals(brokerResponse.getTotalDocs(),
          numThreadsPerInstanceRequest * numTestServers * numDocsPerTestSegment);
    }
  }

  /**
   * Without early termination, Selection order by query should hit all segments.
   */
  @Test
  public void testSelectWithOrderByQuery() {
    int numSegmentsPerServer = getNumSegmentDataManagers();
    String query = "SELECT column11, column18, column1 FROM testTable ORDER BY column11";
    int numColumnsInSelection = 3;
    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    Assert.assertNotNull(brokerResponse.getSelectionResults());
    Assert.assertNull(brokerResponse.getResultTable());
    Assert.assertEquals(brokerResponse.getNumSegmentsMatched(), numSegmentsPerServer * numTestServers);
    Assert.assertEquals(brokerResponse.getNumSegmentsProcessed(), numSegmentsPerServer * numTestServers);
    Assert.assertEquals(brokerResponse.getNumDocsScanned(),
        numSegmentsPerServer * numTestServers * numDocsPerTestSegment);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedPostFilter(),
        numSegmentsPerServer * numTestServers * numDocsPerTestSegment * numColumnsInSelection);
    Assert.assertEquals(brokerResponse.getTotalDocs(), numSegmentsPerServer * numTestServers * numDocsPerTestSegment);

    brokerResponse = getBrokerResponseForSqlQuery(query);
    Assert.assertNull(brokerResponse.getSelectionResults());
    Assert.assertNotNull(brokerResponse.getResultTable());
    Assert.assertEquals(brokerResponse.getNumSegmentsMatched(), numSegmentsPerServer * numTestServers);
    Assert.assertEquals(brokerResponse.getNumSegmentsProcessed(), numSegmentsPerServer * numTestServers);
    Assert.assertEquals(brokerResponse.getNumDocsScanned(),
        numSegmentsPerServer * numTestServers * numDocsPerTestSegment);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    Assert.assertEquals(brokerResponse.getNumEntriesScannedPostFilter(),
        numSegmentsPerServer * numTestServers * numDocsPerTestSegment * numColumnsInSelection);
    Assert.assertEquals(brokerResponse.getTotalDocs(), numSegmentsPerServer * numTestServers * numDocsPerTestSegment);
  }
}
