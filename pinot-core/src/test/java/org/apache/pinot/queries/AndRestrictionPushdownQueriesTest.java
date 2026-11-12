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

import java.util.List;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// End-to-end test of the AND restriction push-down against a real segment.
///
/// The shared `FILTER` is `col1 > x AND col3 BETWEEN y AND z AND col5 = 'gFuH' AND (col6 < w OR col11 NOT IN (..))
/// AND daysSinceEpoch = d`, i.e. an OR containing scan-based predicates under an AND with index-based ones -- the
/// shape from [issue 19339](https://github.com/apache/pinot/issues/19339). The push-down ships disabled, so this is
/// the only coverage that runs a real query with it on, eagerly (`always`) and as a stream (`streaming`, and `auto`
/// for a selection query).
public class AndRestrictionPushdownQueriesTest extends BaseSingleValueQueriesTest {
  private static final String AGGREGATION = "SELECT SUM(column1) FROM testTable";
  private static final String SELECTION = "SELECT column1, column5, column11 FROM testTable";

  @Test
  public void testPushdownScansFewerEntriesAndKeepsTheSameResult() {
    BrokerResponseNative disabled = getBrokerResponse(withMode("never", AGGREGATION + FILTER));
    BrokerResponseNative enabled = getBrokerResponse(withMode("always", AGGREGATION + FILTER));

    assertRowsEqual(enabled, disabled);
    assertEquals(enabled.getNumDocsScanned(), disabled.getNumDocsScanned(), "The same documents must match");
    assertEquals(enabled.getTotalDocs(), disabled.getTotalDocs());
    assertTrue(enabled.getNumEntriesScannedInFilter() < disabled.getNumEntriesScannedInFilter(),
        "The push-down must reduce the entries scanned in the filter, but scanned "
            + enabled.getNumEntriesScannedInFilter() + " with it and " + disabled.getNumEntriesScannedInFilter()
            + " without");
  }

  @Test
  public void testPushdownKeepsTheSameRowsForASelectionQuery() {
    BrokerResponseNative disabled = getBrokerResponse(withMode("never", SELECTION + FILTER));
    assertRowsEqual(getBrokerResponse(withMode("always", SELECTION + FILTER)), disabled);
    assertRowsEqual(getBrokerResponse(withMode("streaming", SELECTION + FILTER)), disabled);
  }

  /// The streaming push-down reads every matching document of an aggregation too, and must give the same result.
  @Test
  public void testStreamingKeepsTheSameResultForAnAggregationQuery() {
    BrokerResponseNative eager = getBrokerResponse(withMode("always", AGGREGATION + FILTER));
    BrokerResponseNative streaming = getBrokerResponse(withMode("streaming", AGGREGATION + FILTER));
    assertRowsEqual(streaming, eager);
    assertEquals(streaming.getNumDocsScanned(), eager.getNumDocsScanned());
  }

  /// AUTO streams a selection query: it keeps the early termination at the LIMIT, and the restriction still reaches
  /// the scans, so the query scans much less than without the push-down.
  @Test
  public void testAutoStreamsSelectionQueries() {
    // A selection query stops at its LIMIT, so with several threads the number of segments it scans depends on timing.
    // One thread makes the entries scanned in the filter deterministic.
    String query = "SET " + QueryOptionKey.MAX_EXECUTION_THREADS + " = 1; " + SELECTION + FILTER;
    BrokerResponseNative auto = getBrokerResponse(withMode("auto", query));
    BrokerResponseNative streaming = getBrokerResponse(withMode("streaming", query));
    BrokerResponseNative disabled = getBrokerResponse(withMode("never", query));

    assertRowsEqual(auto, disabled);
    assertEquals(auto.getNumEntriesScannedInFilter(), streaming.getNumEntriesScannedInFilter(),
        "AUTO must stream a selection query");
    assertTrue(auto.getNumEntriesScannedInFilter() < disabled.getNumEntriesScannedInFilter(),
        "The streaming push-down must scan less than no push-down, but scanned " + auto.getNumEntriesScannedInFilter()
            + " with it and " + disabled.getNumEntriesScannedInFilter() + " without");
  }

  @Test
  public void testAutoPushesDownForAnAggregationQuery() {
    assertEquals(getBrokerResponse(withMode("auto", AGGREGATION + FILTER)).getNumEntriesScannedInFilter(),
        getBrokerResponse(withMode("always", AGGREGATION + FILTER)).getNumEntriesScannedInFilter());
  }

  @Test
  public void testDefaultLeavesEveryQueryAlone() {
    assertEquals(getBrokerResponse(AGGREGATION + FILTER).getNumEntriesScannedInFilter(),
        getBrokerResponse(withMode("never", AGGREGATION + FILTER)).getNumEntriesScannedInFilter(),
        "The push-down must be off by default");
  }

  /// Filtered aggregations build the outer AND through CombinedFilterOperator, where both the main and the sub
  /// filter can be deferrable, so it can reach the lazy fallback arm of AndDocIdSet#iterator().
  @Test
  public void testPushdownForFilteredAggregation() {
    String query = "SELECT SUM(column1) FILTER (WHERE column3 > 0), COUNT(*) FROM testTable" + FILTER;

    assertRowsEqual(getBrokerResponse(withMode("always", query)), getBrokerResponse(withMode("never", query)));
  }

  private static String withMode(String mode, String query) {
    return "SET " + QueryOptionKey.AND_RESTRICTION_PUSHDOWN_MODE + " = '" + mode + "'; " + query;
  }

  private static void assertRowsEqual(BrokerResponseNative actual, BrokerResponseNative expected) {
    List<Object[]> actualRows = actual.getResultTable().getRows();
    List<Object[]> expectedRows = expected.getResultTable().getRows();
    assertEquals(actual.getResultTable().getDataSchema(), expected.getResultTable().getDataSchema());
    assertEquals(actualRows.size(), expectedRows.size());
    for (int i = 0; i < actualRows.size(); i++) {
      assertEquals(actualRows.get(i), expectedRows.get(i), "Row " + i + " differs");
    }
  }
}
