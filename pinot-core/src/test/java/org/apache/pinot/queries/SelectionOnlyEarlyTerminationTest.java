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
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


/**
 * Early termination test for selection-only queries.
 */
public class SelectionOnlyEarlyTerminationTest extends BaseSingleValueQueriesTest {
  private static final int NUM_DOCS_PER_SEGMENT = 30000;
  private static final int NUM_SERVERS = 2;

  /**
   * In order to ensure each thread is executing more than 1 segment, this test is against
   * (2 * MAX_NUM_THREADS_PER_QUERY) segments per server.
   */
  @Override
  protected int getNumSegmentDataManagers() {
    return CombineOperator.MAX_NUM_THREADS_PER_QUERY * 2;
  }

  /**
   * With early termination, selection-only query is scheduled with {@link CombineOperator#MAX_NUM_THREADS_PER_QUERY}
   * threads per server, and the total number of segments matched (segments with non-zero documents scanned) should be
   * the same as the total number of threads for each server.
   */
  @Test
  public void testSelectOnlyQuery() {
    int numThreadsPerServer = CombineOperator.MAX_NUM_THREADS_PER_QUERY;
    int numSegmentsPerServer = getNumSegmentDataManagers();

    // LIMIT = 5, 10, 20, 40, 80, 160, 320, 640, 1280, 2560, 5120, 10240, 20480
    for (int limit = 5; limit < NUM_DOCS_PER_SEGMENT; limit *= 2) {
      String query = String.format("SELECT column1, column7, column9, column6 FROM testTable LIMIT %d", limit);
      int numColumnsInSelection = 4;
      BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
      assertNotNull(brokerResponse.getSelectionResults());
      assertNull(brokerResponse.getResultTable());
      assertEquals(brokerResponse.getNumSegmentsProcessed(), numSegmentsPerServer * NUM_SERVERS);
      assertEquals(brokerResponse.getNumSegmentsMatched(), numThreadsPerServer * NUM_SERVERS);
      assertEquals(brokerResponse.getNumDocsScanned(), numThreadsPerServer * NUM_SERVERS * limit);
      assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
      assertEquals(brokerResponse.getNumEntriesScannedPostFilter(),
          numThreadsPerServer * NUM_SERVERS * limit * numColumnsInSelection);
      assertEquals(brokerResponse.getTotalDocs(), numSegmentsPerServer * NUM_SERVERS * NUM_DOCS_PER_SEGMENT);

      brokerResponse = getBrokerResponseForSqlQuery(query);
      assertNull(brokerResponse.getSelectionResults());
      assertNotNull(brokerResponse.getResultTable());
      assertEquals(brokerResponse.getNumSegmentsProcessed(), numSegmentsPerServer * NUM_SERVERS);
      assertEquals(brokerResponse.getNumSegmentsMatched(), numThreadsPerServer * NUM_SERVERS);
      assertEquals(brokerResponse.getNumDocsScanned(), numThreadsPerServer * NUM_SERVERS * limit);
      assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
      assertEquals(brokerResponse.getNumEntriesScannedPostFilter(),
          numThreadsPerServer * NUM_SERVERS * limit * numColumnsInSelection);
      assertEquals(brokerResponse.getTotalDocs(), numSegmentsPerServer * NUM_SERVERS * NUM_DOCS_PER_SEGMENT);
    }
  }

  /**
   * Without early termination, selection order-by query should hit all segments.
   */
  @Test
  public void testSelectWithOrderByQuery() {
    int numSegmentsPerServer = getNumSegmentDataManagers();
    String query = "SELECT column11, column18, column1 FROM testTable ORDER BY column11";
    int numColumnsInSelection = 3;
    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    assertNotNull(brokerResponse.getSelectionResults());
    assertNull(brokerResponse.getResultTable());
    assertEquals(brokerResponse.getNumSegmentsProcessed(), numSegmentsPerServer * NUM_SERVERS);
    assertEquals(brokerResponse.getNumSegmentsMatched(), numSegmentsPerServer * NUM_SERVERS);
    assertEquals(brokerResponse.getNumDocsScanned(), numSegmentsPerServer * NUM_SERVERS * NUM_DOCS_PER_SEGMENT);
    assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    assertEquals(brokerResponse.getNumEntriesScannedPostFilter(),
        numSegmentsPerServer * NUM_SERVERS * NUM_DOCS_PER_SEGMENT * numColumnsInSelection);
    assertEquals(brokerResponse.getTotalDocs(), numSegmentsPerServer * NUM_SERVERS * NUM_DOCS_PER_SEGMENT);

    brokerResponse = getBrokerResponseForSqlQuery(query);
    assertNull(brokerResponse.getSelectionResults());
    assertNotNull(brokerResponse.getResultTable());
    assertEquals(brokerResponse.getNumSegmentsProcessed(), numSegmentsPerServer * NUM_SERVERS);
    assertEquals(brokerResponse.getNumSegmentsMatched(), numSegmentsPerServer * NUM_SERVERS);
    assertEquals(brokerResponse.getNumDocsScanned(), numSegmentsPerServer * NUM_SERVERS * NUM_DOCS_PER_SEGMENT);
    assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    assertEquals(brokerResponse.getNumEntriesScannedPostFilter(),
        numSegmentsPerServer * NUM_SERVERS * NUM_DOCS_PER_SEGMENT * numColumnsInSelection);
    assertEquals(brokerResponse.getTotalDocs(), numSegmentsPerServer * NUM_SERVERS * NUM_DOCS_PER_SEGMENT);
  }
}
