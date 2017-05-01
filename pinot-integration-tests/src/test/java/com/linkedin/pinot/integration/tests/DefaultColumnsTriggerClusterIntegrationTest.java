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

import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration test that extends DefaultColumnsClusterIntegrationTest to test triggering of default columns creation.
 */
public class DefaultColumnsTriggerClusterIntegrationTest extends DefaultColumnsClusterIntegrationTest {
  private static final String SCHEMA_WITH_MISSING_COLUMNS =
      "On_Time_On_Time_Performance_2014_100k_subset_nonulls_default_column_test_missing_columns.schema";

  private static final long MAX_RELOAD_TIME_IN_MILLIS = 5000L;
  private static final String RELOAD_VERIFICATION_QUERY = "SELECT COUNT(*) FROM mytable WHERE NewAddedIntDimension < 0";
  private static final String SELECT_STAR_QUERY = "SELECT * FROM mytable";

  @BeforeClass
  @Override
  public void setUp()
      throws Exception {
    setUp(false);
    triggerReload(true);
  }

  /**
   * Removed the new added columns and the following existing columns:
   * <ul>
   *   <li>"ActualElapsedTime", METRIC, INT</li>
   *   <li>"AirTime", METRIC, INT</li>
   *   <li>"AirlineID", DIMENSION, LONG</li>
   *   <li>"ArrTime", DIMENSION, INT</li>
   * </ul>
   */
  @Test
  public void testRemoveNewAddedColumns()
      throws Exception {
    JSONObject queryResponse = postQuery(SELECT_STAR_QUERY);
    Assert.assertEquals(queryResponse.getLong("totalDocs"), TOTAL_DOCS);
    Assert.assertEquals(queryResponse.getJSONObject("selectionResults").getJSONArray("columns").length(), 88);

    triggerReload(false);
    queryResponse = postQuery(SELECT_STAR_QUERY);
    Assert.assertEquals(queryResponse.getLong("totalDocs"), TOTAL_DOCS);
    Assert.assertEquals(queryResponse.getJSONObject("selectionResults").getJSONArray("columns").length(), 79);

    triggerReload(true);
    queryResponse = postQuery(SELECT_STAR_QUERY);
    Assert.assertEquals(queryResponse.getLong("totalDocs"), TOTAL_DOCS);
    Assert.assertEquals(queryResponse.getJSONObject("selectionResults").getJSONArray("columns").length(), 88);
  }

  private void triggerReload(boolean withExtraColumns)
      throws Exception {
    if (withExtraColumns) {
      sendSchema(SCHEMA_WITH_EXTRA_COLUMNS);
    } else {
      sendSchema(SCHEMA_WITH_MISSING_COLUMNS);
    }

    sendGetRequest(CONTROLLER_BASE_API_URL + "/tables/mytable/segments/reload?type=offline");
    long endTime = System.currentTimeMillis() + MAX_RELOAD_TIME_IN_MILLIS;
    while (System.currentTimeMillis() < endTime) {
      JSONObject queryResponse = postQuery(RELOAD_VERIFICATION_QUERY);
      // Total docs should not change during reload
      Assert.assertEquals(queryResponse.getLong("totalDocs"), TOTAL_DOCS);
      long count = queryResponse.getJSONArray("aggregationResults").getJSONObject(0).getLong("value");
      if (withExtraColumns) {
        if (count == TOTAL_DOCS) {
          break;
        }
      } else {
        if (count == 0) {
          break;
        }
      }
    }
    Assert.assertTrue(System.currentTimeMillis() < endTime);
  }
}
