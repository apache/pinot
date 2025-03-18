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
package org.apache.pinot.controller.api;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;

@Test(groups = "integration")
public class PinotMinionResourceIntegrationTest extends ControllerTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String TASK_TYPE = MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE;

  @BeforeClass
  public void setUp() throws Exception {
    startZk();
    startController();
    
    // Add fake instances
    addFakeBrokerInstancesToAutoJoinHelixCluster(1, true);
    addFakeServerInstancesToAutoJoinHelixCluster(1, true);
    addFakeMinionInstancesToAutoJoinHelixCluster(2);
    
    // Create a table with tasks
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(RAW_TABLE_NAME)
        .setTaskConfig(new TableTaskConfig(ImmutableMap.of(
            TASK_TYPE, ImmutableMap.of("schedule", "0 */10 * ? * * *"))))
        .build();
    
    addTableConfig(tableConfig);
  }
  
  @Test
  public void testGetMinionInstances() throws IOException, URISyntaxException {
    // Call the API
    String responseStr = HttpClient.wrapAndThrowHttpException(
        _httpClient.sendGetRequest(new URI(_controllerRequestURLBuilder.forMinionInstances())));
    
    // Verify response
    JsonNode response = JsonUtils.stringToJsonNode(responseStr);
    assertTrue(response.isArray());
    assertEquals(response.size(), 2, "Should have 2 minion instances");
  }
  
  @Test
  public void testGetTaggedMinionInstances() throws IOException, URISyntaxException {
    // Call the API
    String responseStr = HttpClient.wrapAndThrowHttpException(
        _httpClient.sendGetRequest(new URI(_controllerRequestURLBuilder.forTaggedMinionInstances())));
    
    // Verify response
    JsonNode response = JsonUtils.stringToJsonNode(responseStr);
    assertTrue(response.isObject());
    assertTrue(response.has(MinionConstants.UNTAGGED_INSTANCE));
    
    JsonNode untaggedInstances = response.get(MinionConstants.UNTAGGED_INSTANCE);
    assertTrue(untaggedInstances.isArray());
    assertEquals(untaggedInstances.size(), 2, "Should have 2 untagged minion instances");
  }
  
  @Test
  public void testGetPendingTaskCount() throws IOException, URISyntaxException {
    // Call the API
    String responseStr = HttpClient.wrapAndThrowHttpException(
        _httpClient.sendGetRequest(new URI(_controllerRequestURLBuilder.forMinionTaskCount())));
    
    // Verify response
    JsonNode response = JsonUtils.stringToJsonNode(responseStr);
    assertTrue(response.isObject());
    assertTrue(response.has("totalPendingTasks"));
    assertTrue(response.has("pendingTasksByType"));
    assertTrue(response.has("totalMinionInstances"));
    assertTrue(response.has("tasksPerMinion"));
    
    assertEquals(response.get("totalMinionInstances").asInt(), 2, "Should have 2 minion instances");
  }
  
  @Test
  public void testGetPendingTaskCountByType() throws IOException, URISyntaxException {
    // Initialize task queue for the task type
    _controllerStarter.getHelixTaskResourceManager().ensureTaskQueueExists(TASK_TYPE);
    
    // Call the API
    String responseStr = HttpClient.wrapAndThrowHttpException(
        _httpClient.sendGetRequest(new URI(_controllerRequestURLBuilder.forMinionTaskCountByType(TASK_TYPE))));
    
    // Verify response
    JsonNode response = JsonUtils.stringToJsonNode(responseStr);
    assertTrue(response.isObject());
    assertTrue(response.has("pendingTasks"));
    assertTrue(response.has("totalTasks"));
    assertTrue(response.has("taskStateCount"));
    assertTrue(response.has("availableMinionInstances"));
    assertTrue(response.has("tasksPerMinion"));
  }

  private void addTableConfig(TableConfig tableConfig) throws IOException {
    try {
      HttpClient.wrapAndThrowHttpException(
          _httpClient.sendJsonPostRequest(new URI(_controllerRequestURLBuilder.forTableCreate()),
              tableConfig.toJsonString(), new HashMap<>()));
    } catch (URISyntaxException | HttpErrorStatusException e) {
      throw new IOException(e);
    }
  }

  @AfterClass
  public void tearDown() {
    stopFakeInstances();
    stopController();
    stopZk();
  }
}