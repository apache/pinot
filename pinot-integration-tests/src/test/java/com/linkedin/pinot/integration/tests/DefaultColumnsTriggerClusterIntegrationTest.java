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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;


/**
 * Integration test that extends DefaultColumnsClusterIntegrationTest to test triggering of default columns creation.
 */
public class DefaultColumnsTriggerClusterIntegrationTest extends DefaultColumnsClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultColumnsClusterIntegrationTest.class);

  @BeforeClass
  @Override
  public void setUp()
      throws Exception {
    LOGGER.info("Set up cluster without new schema.");
    setUp(false);

    // Bring table OFFLINE and ONLINE to trigger reloading of the segments.
    LOGGER.info("Disable the table.");
    sendGetRequest(CONTROLLER_BASE_API_URL + "/tables/mytable?type=offline&state=disable");
    Thread.sleep(1000);
    LOGGER.info("Send the new schema.");
    sendSchema();
    Thread.sleep(1000);
    LOGGER.info("Enable the table to reload the segments.");
    sendGetRequest(CONTROLLER_BASE_API_URL + "/tables/mytable?type=offline&state=enable");

    // Wait for all segments to be ONLINE again.
    waitForSegmentsOnline();
  }
}
