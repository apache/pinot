/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.integration.tests;

import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * enables indexes on a bunch of columns
 *
 */
public class InvertedIndexOfflineIntegrationTest extends OfflineClusterIntegrationTest {
  private static final List<String> ORIGIN_INVERTED_INDEX_COLUMNS = Arrays.asList("FlightNum", "Origin", "Quarter");
  private static final List<String> UPDATED_INVERTED_INDEX_COLUMNS =
      Arrays.asList("FlightNum", "Origin", "Quarter", "DivActualElapsedTime");
  private static final String TEST_QUERY = "SELECT COUNT(*) FROM mytable WHERE DivActualElapsedTime = 305";
  private static final long MAX_RELOAD_TIME_IN_MILLIS = 5000L;

  @Override
  protected void createTable()
      throws Exception {
    addOfflineTable("DaysSinceEpoch", "daysSinceEpoch", -1, "", null, null, ORIGIN_INVERTED_INDEX_COLUMNS, null,
        "mytable", SegmentVersion.v1);
  }

  @Test
  public void testInvertedIndexTrigger()
      throws Exception {
    runQuery(TEST_QUERY, Collections.singletonList(TEST_QUERY));
    JSONObject queryResponse = postQuery(TEST_QUERY);
    Assert.assertEquals(queryResponse.getLong("numEntriesScannedInFilter"), TOTAL_DOCS);
    updateOfflineTable("DaysSinceEpoch", -1, "", null, null, UPDATED_INVERTED_INDEX_COLUMNS, null, "mytable",
        SegmentVersion.v1);

    triggerReload();
    long endTime = System.currentTimeMillis() + MAX_RELOAD_TIME_IN_MILLIS;
    while (System.currentTimeMillis() < endTime) {
      runQuery(TEST_QUERY, Collections.singletonList(TEST_QUERY));
      queryResponse = postQuery(TEST_QUERY);
      // Total docs should not change during reload
      Assert.assertEquals(queryResponse.getLong("totalDocs"), TOTAL_DOCS);
      if (queryResponse.getLong("numEntriesScannedInFilter") == 0L) {
        break;
      }
    }
    Assert.assertTrue(System.currentTimeMillis() < endTime);
  }

  private void triggerReload()
      throws Exception {
    sendGetRequest(CONTROLLER_BASE_API_URL + "/tables/mytable/segments/reload?type=offline");
  }
}
