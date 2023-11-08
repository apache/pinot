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
package org.apache.pinot.integration.tests;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.broker.broker.BrokerAdminApiApplication;
import org.apache.pinot.controller.api.ControllerAdminApiApplication;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Tests that the controller, broker and server admin consoles return the expected pages.
 */
public class AdminConsoleIntegrationTest extends BaseClusterIntegrationTest {

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);
    // Start an empty Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteQuietly(_tempDir);
  }

  /**
   * Tests resposnes to /api and /help.
   */
  @Test
  public void testApiHelp()
      throws Exception {
    // test controller
    String response = sendGetRequest(getControllerBaseApiUrl() + "/help");
    String expected = IOUtils
        .toString(ControllerAdminApiApplication.class.getClassLoader().getResourceAsStream("api/index.html"), "UTF-8");
    Assert.assertEquals(response, expected);
    // help and api map to the same content
    response = sendGetRequest(getControllerBaseApiUrl() + "/api");
    Assert.assertEquals(response, expected);

    // test broker
    response = sendGetRequest(getBrokerBaseApiUrl() + "/help");
    expected = IOUtils
        .toString(BrokerAdminApiApplication.class.getClassLoader().getResourceAsStream("api/index.html"), "UTF-8");
    Assert.assertEquals(response, expected);
    // help and api map to the same content
    response = sendGetRequest(getBrokerBaseApiUrl() + "/api");
    Assert.assertEquals(response, expected);
    String serverBaseApiUrl = "http://localhost:" + getServerAdminApiPort();
    // test server
    response = sendGetRequest(serverBaseApiUrl + "/help");
    expected = IOUtils
        .toString(BrokerAdminApiApplication.class.getClassLoader().getResourceAsStream("api/index.html"), "UTF-8");
    Assert.assertEquals(response, expected);

    // help and api map to the same content
    response = sendGetRequest(serverBaseApiUrl + "/api");
    Assert.assertEquals(response, expected);
  }
}
