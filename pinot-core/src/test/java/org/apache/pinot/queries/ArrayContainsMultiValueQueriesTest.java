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

  @Test
  public void testArrayContainsLiteralConversion() {
    // Literals are converted to int the same way the scalar function converts them (truncate / cast / trim)
    long expectedCount = getCount(getBrokerResponse("SELECT COUNT(*) FROM testTable WHERE column7 = 225"));
    assertTrue(expectedCount > 0);
    for (String literal : new String[]{"225.0", "225.5", "' 225'", "4294967521"}) {
      String filter = "arrayContainsInt(column7, " + literal + ")";
      BrokerResponseNative brokerResponse = assertSameAsScalar(filter);
      assertEquals(getCount(brokerResponse), expectedCount, filter);
      assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0L, filter);
    }
  }

  @Test
  public void testArrayContainsCombinedPredicates() {
    String[] filters = {
        "arrayContainsInt(column7, 225) OR arrayContainsInt(column7, 407)",
        "arrayContainsInt(column7, 225) AND arrayContainsInt(column7, 407)",
        "(arrayContainsInt(column7, 225) AND column3 <> 'w') OR (arrayContainsInt(column7, 407) "
            + "AND NOT arrayContainsInt(column7, 225))",
        "arrayContainsInt(column7, 225) AND (arrayContainsInt(column7, 407) OR column8 > 500000)"
    };
    for (String filter : filters) {
      assertSameAsScalar(filter);
    }
    // Both predicates on the same MV column must stay independent (any value matches each), not collapse to empty
    assertTrue(getCount(assertSameAsScalar("arrayContainsInt(column7, 225) OR arrayContainsInt(column7, 407)")) > 0);
  }

  /// Runs the filter both through the optimizer and as the plain scalar function, asserts the counts match, and
  /// returns the optimized response.
  private BrokerResponseNative assertSameAsScalar(String filter) {
    String query = "SELECT COUNT(*) FROM testTable WHERE " + filter;
    BrokerResponseNative scalarResponse = getBrokerResponse(query);
    BrokerResponseNative optimizedResponse = getBrokerResponseForOptimizedQuery(query, SCHEMA);
    assertTrue(optimizedResponse.getExceptions().isEmpty(), filter + ": " + optimizedResponse.getExceptions());
    assertEquals(getCount(optimizedResponse), getCount(scalarResponse), filter);
    return optimizedResponse;
  }

  private static long getCount(BrokerResponseNative brokerResponse) {
    return (long) brokerResponse.getResultTable().getRows().get(0)[0];
  }
}
