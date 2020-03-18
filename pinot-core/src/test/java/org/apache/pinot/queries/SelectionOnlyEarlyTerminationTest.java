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
    int numThreadsPerQuery = getNumSegmentDataManagers();
    // LIMIT = 5,15,35,75,155,315,635
    for (int limit = 5; limit < 1000; limit += (limit + 5)) {
      String query = String.format("SELECT column1, column6 FROM testTable LIMIT %d", limit);
      BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
      Assert.assertNotNull(brokerResponse.getSelectionResults());
      Assert.assertNull(brokerResponse.getResultTable());
      Assert.assertEquals(brokerResponse.getNumSegmentsMatched(), numThreadsPerQuery);
      Assert.assertEquals(brokerResponse.getNumSegmentsProcessed(), numThreadsPerQuery);
      Assert.assertEquals(brokerResponse.getNumDocsScanned(), numThreadsPerQuery * limit);
      Assert.assertEquals(brokerResponse.getTotalDocs(), numThreadsPerQuery / 2 * 60000);

      brokerResponse = getBrokerResponseForSqlQuery(query);
      Assert.assertNull(brokerResponse.getSelectionResults());
      Assert.assertNotNull(brokerResponse.getResultTable());
      Assert.assertEquals(brokerResponse.getNumSegmentsMatched(), numThreadsPerQuery);
      Assert.assertEquals(brokerResponse.getNumSegmentsProcessed(), numThreadsPerQuery);
      Assert.assertEquals(brokerResponse.getNumDocsScanned(), numThreadsPerQuery * limit);
      Assert.assertEquals(brokerResponse.getTotalDocs(), numThreadsPerQuery / 2 * 60000);
    }
  }

  /**
   * Without early termination, Selection order by query should hit all segments.
   */
  @Test
  public void testSelectWithOrderByQuery() {
    int numSegments = getNumSegmentDataManagers() * 2;
    String query = "SELECT column11, column1 FROM testTable ORDER BY column11";

    BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
    Assert.assertNotNull(brokerResponse.getSelectionResults());
    Assert.assertNull(brokerResponse.getResultTable());
    Assert.assertEquals(brokerResponse.getNumSegmentsMatched(), numSegments);
    Assert.assertEquals(brokerResponse.getNumSegmentsProcessed(), numSegments);
    Assert.assertEquals(brokerResponse.getNumDocsScanned(), getNumSegmentDataManagers() * 60000);
    Assert.assertEquals(brokerResponse.getTotalDocs(), getNumSegmentDataManagers() * 60000);
  }
}
