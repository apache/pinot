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

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Collections;
import java.util.Map;
import org.apache.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import org.apache.pinot.minion.executor.PinotTaskExecutor;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration test that provides example of {@link PinotTaskGenerator} and {@link PinotTaskExecutor} and tests simple
 * minion functionality.
 */
public class BasicAuthClusterIntegrationTest extends ClusterTest {
  private static final String AUTH_TOKEN = "Basic YWRtaW46dmVyeXNlY3JldA=====";
  private static final Map<String, String> AUTH_HEADER = Collections.singletonMap("Authorization", AUTH_TOKEN);

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController();
    startBroker();
    startServer();
    startMinion(Collections.emptyList(), Collections.emptyList());
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    stopMinion();
    stopServer();
    stopBroker();
    stopController();
    stopZk();
  }

  @Override
  public Map<String, Object> getDefaultControllerConfiguration() {
    Map<String, Object> properties = super.getDefaultControllerConfiguration();
    properties.put("controller.segment.fetcher.auth.token", AUTH_TOKEN);
    properties.put("controller.admin.access.control.factory.class",
        "org.apache.pinot.controller.api.access.BasicAuthAccessControlFactory");
    properties.put("controller.admin.access.control.principals", "admin, user");
    properties.put("controller.admin.access.control.principals.admin.password", "verysecret");
    properties.put("controller.admin.access.control.principals.user.password", "secret");
    properties.put("controller.admin.access.control.principals.user.tables", "baseballStats");
    properties.put("controller.admin.access.control.principals.user.permissions", "read");
    return properties;
  }

  @Override
  protected PinotConfiguration getDefaultBrokerConfiguration() {
    Map<String, Object> properties = super.getDefaultBrokerConfiguration().toMap();
    properties.put("pinot.broker.access.control.class", "org.apache.pinot.broker.broker.BasicAuthAccessControlFactory");
    properties.put("pinot.broker.access.control.principals", "admin, user");
    properties.put("pinot.broker.access.control.principals.admin.password", "verysecret");
    properties.put("pinot.broker.access.control.principals.user.password", "secret");
    properties.put("pinot.broker.access.control.principals.user.tables", "baseballStats");
    properties.put("pinot.broker.access.control.principals.user.permissions", "read");
    return new PinotConfiguration(properties);
  }

  @Override
  protected PinotConfiguration getDefaultServerConfiguration() {
    Map<String, Object> properties = super.getDefaultServerConfiguration().toMap();
    properties.put("pinot.server.segment.fetcher.auth.token", AUTH_TOKEN);
    properties.put("pinot.server.segment.upload.auth.token", AUTH_TOKEN);
    properties.put("pinot.server.instance.auth.token", AUTH_TOKEN);
    return new PinotConfiguration(properties);
  }

  @Override
  protected PinotConfiguration getDefaultMinionConfiguration() {
    Map<String, Object> properties = super.getDefaultMinionConfiguration().toMap();
    properties.put("segment.fetcher.auth.token", AUTH_TOKEN);
    properties.put("task.auth.token", AUTH_TOKEN);
    return new PinotConfiguration(properties);
  }

  @Test
  public void testBrokerNoAuth()
      throws Exception {
    JsonNode response =
        JsonUtils.stringToJsonNode(sendPostRequest("http://localhost:18099/query/sql", "{\"sql\":\"SELECT now()\"}"));
    Assert.assertFalse(response.has("resultTable"), "must not return result table");
    Assert.assertTrue(response.get("exceptions").get(0).get("errorCode").asInt() != 0, "must return error code");
  }

  @Test
  public void testBroker()
      throws Exception {
    JsonNode response = JsonUtils.stringToJsonNode(
        sendPostRequest("http://localhost:18099/query/sql", "{\"sql\":\"SELECT now()\"}", AUTH_HEADER));
    Assert.assertEquals(response.get("resultTable").get("dataSchema").get("columnDataTypes").get(0).asText(), "LONG",
        "must return result with LONG value");
    Assert.assertTrue(response.get("exceptions").isEmpty(), "must not return exception");
  }

  @Test
  public void testControllerGetTables()
      throws Exception {
    JsonNode response = JsonUtils.stringToJsonNode(sendGetRequest("http://localhost:18998/tables", AUTH_HEADER));
    Assert.assertTrue(response.get("tables").isArray(), "must return table array");
  }

//  // TODO this endpoint should be protected once UI supports auth
//  @Test
//  public void testControllerGetTablesNoAuth()
//      throws Exception {
//    System.out.println(sendGetRequest("http://localhost:18998/tables"));
//  }

//  // TODO this endpoint should support literal / non-table queries
//  @Test
//  public void testControllerQueryRelay()
//      throws Exception {
//     System.out.println(sendPostRequest("http://localhost:18998/sql", "{\"sql\":\"SELECT now()\"}", AUTH_HEADER));
//  }
}
