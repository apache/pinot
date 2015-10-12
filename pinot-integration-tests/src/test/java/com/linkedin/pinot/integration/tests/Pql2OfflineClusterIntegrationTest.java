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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Integration test that converts avro data for 12 segments and runs queries against it.
 */
@Test(enabled = false) // jfim: This is disabled because the new parser exposes some bugs in the old one
public class Pql2OfflineClusterIntegrationTest extends OfflineClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(Pql2OfflineClusterIntegrationTest.class);

  @Override
  protected void runQuery(String pqlQuery, List<String> sqlQueries) throws Exception {
    try {
      JSONObject pql1Response = postQuery(pqlQuery, BROKER_BASE_API_URL, false);
      JSONObject pql2Response = postQuery(pqlQuery, BROKER_BASE_API_URL, true);
      Assert.assertEquals(pql2Response.getInt("numDocsScanned"), pql1Response.getInt("numDocsScanned"));
    } catch (AssertionError assertionError) {
      LOGGER.error("Num docs scanned did not match for query {}", pqlQuery);
      throw assertionError;
    }
  }

  @Override
  protected String getHelixClusterName() {
    return "OfflineClusterIntegrationTest";
  }
}
