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
package org.apache.pinot.client.admin;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


/**
 * Unit tests for PinotAdminClient using mocked transport (no real server).
 */
public class PinotAdminClientTest {
  @Mock
  private PinotAdminTransport _mockTransport;

  private PinotAdminClient _adminClient;
  private static final String CONTROLLER_ADDRESS = "localhost:9000";
  private static final Map<String, String> HEADERS = Map.of("Authorization", "Bearer token");

  @BeforeMethod
  public void setUp()
      throws Exception {
    MockitoAnnotations.openMocks(this);
    _adminClient = new PinotAdminClient(CONTROLLER_ADDRESS, _mockTransport, HEADERS);

    // For helper methods on the transport, call real implementations so parsing works
    lenient().when(_mockTransport.parseStringArray(any(), anyString())).thenCallRealMethod();
    lenient().when(_mockTransport.parseStringArraySafe(any(), anyString())).thenCallRealMethod();
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    if (_adminClient != null) {
      _adminClient.close();
    }
  }

  @Test
  public void testListTables()
      throws Exception {
    String jsonResponse = "{\"tables\": [\"tbl1\", \"tbl2\"]}";
    JsonNode mockResponse = new ObjectMapper().readTree(jsonResponse);

    lenient().when(_mockTransport.executeGet(anyString(), anyString(), any(), any()))
        .thenReturn(mockResponse);

    List<String> tables = _adminClient.getTableClient().listTables(null, null, null);

    assertNotNull(tables);
    assertEquals(tables.size(), 2);
    assertEquals(tables.get(0), "tbl1");
  }

  @Test
  public void testGetTableConfig()
      throws Exception {
    String jsonResponse = "{\"tableName\":\"tbl1_OFFLINE\"}";
    JsonNode mockResponse = new ObjectMapper().readTree(jsonResponse);
    lenient().when(_mockTransport.executeGet(anyString(), anyString(), any(), any()))
        .thenReturn(mockResponse);

    String cfg = _adminClient.getTableClient().getTableConfig("tbl1_OFFLINE");
    assertNotNull(cfg);
    assertEquals(new ObjectMapper().readTree(cfg).get("tableName").asText(), "tbl1_OFFLINE");
  }

  @Test
  public void testListSchemas()
      throws Exception {
    String jsonResponse = "{\"schemas\": [\"sch1\", \"sch2\"]}";
    JsonNode mockResponse = new ObjectMapper().readTree(jsonResponse);
    lenient().when(_mockTransport.executeGet(anyString(), anyString(), any(), any()))
        .thenReturn(mockResponse);

    List<String> schemas = _adminClient.getSchemaClient().listSchemaNames();
    assertNotNull(schemas);
    assertEquals(schemas.size(), 2);
    assertEquals(schemas.get(1), "sch2");
  }

  @Test
  public void testAsyncListSchemas()
      throws Exception {
    String jsonResponse = "{\"schemas\": [\"sch1\"]}";
    JsonNode mockResponse = new ObjectMapper().readTree(jsonResponse);
    CompletableFuture<JsonNode> jsonNodeCompletableFuture = CompletableFuture.completedFuture(mockResponse);
    lenient().when(_mockTransport.executeGetAsync(anyString(), anyString(), any(), any()))
        .thenReturn(jsonNodeCompletableFuture);

    List<String> schemas = _adminClient.getSchemaClient().listSchemaNamesAsync().get();
    assertNotNull(schemas);
    assertEquals(schemas.size(), 1);
    assertEquals(schemas.get(0), "sch1");
  }

  @Test
  public void testCreateTable()
      throws Exception {
    JsonNode mockResponse = new ObjectMapper().readTree("{\"status\":\"OK\"}");
    lenient().when(_mockTransport.executePost(anyString(), anyString(), any(), any(), any()))
        .thenReturn(mockResponse);

    String resp = _adminClient.getTableClient().createTable("{}", null);
    assertEquals(new ObjectMapper().readTree(resp).get("status").asText(), "OK");
  }

  @Test
  public void testDeleteTable()
      throws Exception {
    JsonNode mockResponse = new ObjectMapper().readTree("{\"status\":\"DELETED\"}");
    lenient().when(_mockTransport.executeDelete(anyString(), anyString(), any(), any()))
        .thenReturn(mockResponse);

    String resp = _adminClient.getTableClient().deleteTable("tbl1_OFFLINE");
    assertEquals(new ObjectMapper().readTree(resp).get("status").asText(), "DELETED");
  }
}
