/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.util.List;
import org.json.JSONObject;
import org.testng.Assert;


/**
 * Integration test that converts avro data for 12 segments and runs queries against it.
 */
public class Pql2OfflineClusterIntegrationTest extends OfflineClusterIntegrationTest {
  @Override
  protected void runQuery(String pqlQuery, List<String> sqlQueries) throws Exception {
    JSONObject pql1Response = postQuery(pqlQuery, BROKER_BASE_API_URL, false);
    JSONObject pql2Response = postQuery(pqlQuery, BROKER_BASE_API_URL, true);
    Assert.assertEquals(pql2Response.getInt("numDocsScanned"), pql1Response.getInt("numDocsScanned"));
  }

  @Override
  protected String getHelixClusterName() {
    return "OfflineClusterIntegrationTest";
  }
}
