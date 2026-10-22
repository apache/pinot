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
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Verifies that a boolean `arrayContains*` predicate on a multi-value column is executed like the equivalent `=`
/// predicate (inverted index / dictionary lookup, no scan) and returns the same result as the scalar function.
public class ArrayContainsMultiValueQueriesTest extends BaseMultiValueQueriesTest {
  // 4 identical segments, 100000 docs each
  private static final long TOTAL_DOCS = 400000L;

  @Test
  public void testArrayContainsUsesIndex() {
    // Unoptimized query evaluates the scalar function on every document, which gives the expected result
    String query = "SELECT COUNT(*) FROM testTable WHERE arrayContainsInt(column7, 225)";
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    long expectedCount = getCount(brokerResponse);
    assertTrue(expectedCount > 0);
    assertEquals(brokerResponse.getNumEntriesScannedInFilter(), TOTAL_DOCS);
    assertEquals(getCount(getBrokerResponseForOptimizedQuery("SELECT COUNT(*) FROM testTable WHERE column7 = 225",
        SCHEMA)), expectedCount);

    brokerResponse = getBrokerResponseForOptimizedQuery(query, SCHEMA);
    assertEquals(getCount(brokerResponse), expectedCount);
    assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0L);

    brokerResponse = getBrokerResponseForOptimizedQuery(query + " = true", SCHEMA);
    assertEquals(getCount(brokerResponse), expectedCount);
    assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0L);
  }

  @Test
  public void testArrayContainsMissingValue() {
    BrokerResponseNative brokerResponse = getBrokerResponseForOptimizedQuery(
        "SELECT COUNT(*) FROM testTable WHERE arrayContainsInt(column7, -12345)", SCHEMA);
    assertEquals(getCount(brokerResponse), 0L);
    assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0L);
  }

  @Test
  public void testNotArrayContainsKeepsSemantics() {
    long containsCount = getCount(getBrokerResponseForOptimizedQuery(
        "SELECT COUNT(*) FROM testTable WHERE arrayContainsInt(column7, 225)", SCHEMA));
    // NOT arrayContains means "no value equals 225", which is the complement of arrayContains
    long notContainsCount = getCount(getBrokerResponseForOptimizedQuery(
        "SELECT COUNT(*) FROM testTable WHERE NOT arrayContainsInt(column7, 225)", SCHEMA));
    assertEquals(notContainsCount, TOTAL_DOCS - containsCount);
  }

  private static long getCount(BrokerResponseNative brokerResponse) {
    return (long) brokerResponse.getResultTable().getRows().get(0)[0];
  }
}
