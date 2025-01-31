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

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.broker.broker.BrokerAdminApiApplication;
import org.apache.pinot.controller.api.ControllerAdminApiApplication;
import org.apache.pinot.minion.MinionAdminApiApplication;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
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
    startControllerWithSwagger();
    startBroker();
    startServer();
    startMinion();
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    stopServer();
    stopBroker();
    stopController();
    stopMinion();
    stopZk();
    FileUtils.deleteQuietly(_tempDir);
  }

  /**
   * Tests responses to /api and /help.
   */
  @Test
  public void testApiHelp()
      throws Exception {
    String apiIndexHtmlPage = "api/index.html";

    // test controller
    String response = sendGetRequest(getControllerBaseApiUrl() + "/help");
    String expected =
        IOUtils.toString(Objects.requireNonNull(
                ControllerAdminApiApplication.class.getClassLoader().getResourceAsStream(apiIndexHtmlPage)),
            StandardCharsets.UTF_8);
    Assert.assertEquals(response, expected);
    // help and api map to the same content
    response = sendGetRequest(getControllerBaseApiUrl() + "/api");
    Assert.assertEquals(response, expected);

    // test broker
    response = sendGetRequest(getBrokerBaseApiUrl() + "/help");
    expected = IOUtils.toString(
        Objects.requireNonNull(BrokerAdminApiApplication.class.getClassLoader().getResourceAsStream(apiIndexHtmlPage)),
        StandardCharsets.UTF_8);
    Assert.assertEquals(response, expected);
    // help and api map to the same content
    response = sendGetRequest(getBrokerBaseApiUrl() + "/api");
    Assert.assertEquals(response, expected);

    // test server
    String serverBaseApiUrl = "http://localhost:" + getServerAdminApiPort();
    response = sendGetRequest(serverBaseApiUrl + "/help");
    expected = IOUtils.toString(
        Objects.requireNonNull(BrokerAdminApiApplication.class.getClassLoader().getResourceAsStream(apiIndexHtmlPage)),
        StandardCharsets.UTF_8);
    Assert.assertEquals(response, expected);
    // help and api map to the same content
    response = sendGetRequest(serverBaseApiUrl + "/api");
    Assert.assertEquals(response, expected);

    // test minion
    response = sendGetRequest(getMinionBaseApiUrl() + "/help");
    expected = IOUtils.toString(
        Objects.requireNonNull(MinionAdminApiApplication.class.getClassLoader().getResourceAsStream(apiIndexHtmlPage)),
        StandardCharsets.UTF_8);
    Assert.assertEquals(response, expected);
    // help and api map to the same content
    response = sendGetRequest(getMinionBaseApiUrl() + "/api");
    Assert.assertEquals(response, expected);
  }

  @Test(dataProvider = "endpointBase")
  public void testSwaggerYaml(final String description, final String endpointBase)
      throws Exception {
    String response = sendGetRequest(endpointBase + "/swagger.yaml");
    Assert.assertTrue(response.startsWith("---\nswagger: \"2.0\""));
  }

  @Test(dataProvider = "endpointBase")
  public void testSwaggerJson(final String description, final String endpointBase)
      throws Exception {
    String response = sendGetRequest(endpointBase + "/swagger.json");
    Assert.assertTrue(response.startsWith("{\"swagger\":\"2.0\""));
    Assert.assertTrue(response.endsWith("}"));
  }

  @DataProvider
  public Object[][] endpointBase() {
    return new Object[][] {
        new Object[] { "controller", getControllerBaseApiUrl() },
        new Object[] { "broker", getBrokerBaseApiUrl() },
        new Object[] { "server", "http://localhost:" + getServerAdminApiPort() },
        new Object[] { "minion", getMinionBaseApiUrl()},
    };
  }
}
