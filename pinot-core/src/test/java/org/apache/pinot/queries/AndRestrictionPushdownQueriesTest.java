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
/// the only coverage that runs a real query with it on.
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
    // ALWAYS is the only mode that reaches a selection query: AUTO excludes it because it can stop at its LIMIT
    assertRowsEqual(getBrokerResponse(withMode("always", SELECTION + FILTER)),
        getBrokerResponse(withMode("never", SELECTION + FILTER)));
  }

  @Test
  public void testAutoLeavesSelectionQueriesAlone() {
    BrokerResponseNative auto = getBrokerResponse(withMode("auto", SELECTION + FILTER));
    BrokerResponseNative disabled = getBrokerResponse(withMode("never", SELECTION + FILTER));

    assertRowsEqual(auto, disabled);
    assertEquals(auto.getNumEntriesScannedInFilter(), disabled.getNumEntriesScannedInFilter(),
        "AUTO must not push down for a selection query");
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
