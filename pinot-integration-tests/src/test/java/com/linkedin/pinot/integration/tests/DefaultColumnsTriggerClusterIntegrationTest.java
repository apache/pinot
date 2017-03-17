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

  @BeforeClass
  @Override
  public void setUp()
      throws Exception {
    setUp(false);
    sendSchema("On_Time_On_Time_Performance_2014_100k_subset_nonulls_default_column_test_extra_columns.schema");
    triggerReload();
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
    JSONObject queryResponse = postQuery("SELECT * FROM mytable");
    Assert.assertEquals(queryResponse.getJSONObject("selectionResults").getJSONArray("columns").length(), 88);

    sendSchema(SCHEMA_WITH_MISSING_COLUMNS);
    triggerReload();
    queryResponse = postQuery("SELECT * FROM mytable");
    Assert.assertEquals(queryResponse.getJSONObject("selectionResults").getJSONArray("columns").length(), 79);

    sendSchema(SCHEMA_WITH_EXTRA_COLUMNS);
    triggerReload();
    queryResponse = postQuery("SELECT * FROM mytable");
    Assert.assertEquals(queryResponse.getJSONObject("selectionResults").getJSONArray("columns").length(), 88);
  }

  private void triggerReload()
      throws Exception {
    sendGetRequest(CONTROLLER_BASE_API_URL + "/tables/mytable?type=offline&state=disable");
    Thread.sleep(1000);
    sendGetRequest(CONTROLLER_BASE_API_URL + "/tables/mytable?type=offline&state=enable");
    waitForSegmentsOnline();
  }
}
