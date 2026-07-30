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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.restlet.resources.PauseStatusDetails;
import org.apache.pinot.common.restlet.resources.ServerRebalanceJobStatusResponse;
import org.apache.pinot.common.restlet.resources.TableView;
import org.apache.pinot.common.utils.PinotAppConfigs;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
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
  public void testListTablesIncludesSortAsc()
      throws Exception {
    JsonNode mockResponse = new ObjectMapper().readTree("{\"tables\": [\"tbl1\"]}");
    lenient().when(_mockTransport.executeGet(anyString(), anyString(), any(), any()))
        .thenReturn(mockResponse);

    _adminClient.getTableClient().listTables("OFFLINE", "task", "creationTime", true);

    verify(_mockTransport).executeGet(eq(CONTROLLER_ADDRESS), eq("/tables"),
        eq(Map.of("type", "OFFLINE", "taskType", "task", "sortType", "creationTime", "sortAsc", "true")),
        eq(HEADERS));
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
  public void testGetTableConfigObjectWithTableType()
      throws Exception {
    TableConfig expectedTableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("tbl1").build();
    String jsonResponse = "{\"OFFLINE\":" + JsonUtils.objectToString(expectedTableConfig) + "}";
    JsonNode mockResponse = new ObjectMapper().readTree(jsonResponse);
    lenient().when(_mockTransport.executeGet(anyString(), anyString(), any(), any()))
        .thenReturn(mockResponse);

    TableConfig tableConfig = _adminClient.getTableClient().getTableConfigObject("tbl1", "OFFLINE");

    assertEquals(tableConfig.getTableName(), "tbl1_OFFLINE");
    assertEquals(tableConfig.getTableType(), TableType.OFFLINE);
    verify(_mockTransport).executeGet(eq(CONTROLLER_ADDRESS), eq("/tables/tbl1"), eq(Map.of("type", "OFFLINE")),
        eq(HEADERS));
  }

  @Test
  public void testGetTypedTableConfig()
      throws Exception {
    TableConfig expectedTableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("tbl1").build();
    String jsonResponse = "{\"OFFLINE\":" + JsonUtils.objectToString(expectedTableConfig) + "}";
    JsonNode mockResponse = new ObjectMapper().readTree(jsonResponse);
    lenient().when(_mockTransport.executeGet(anyString(), anyString(), any(), any()))
        .thenReturn(mockResponse);

    TableConfig tableConfig = _adminClient.getTableClient().getTableConfigObjectForType("tbl1", TableType.OFFLINE);

    assertEquals(tableConfig.getTableName(), "tbl1_OFFLINE");
    assertEquals(tableConfig.getTableType(), TableType.OFFLINE);
    verify(_mockTransport).executeGet(eq(CONTROLLER_ADDRESS), eq("/tables/tbl1"), eq(Map.of("type", "OFFLINE")),
        eq(HEADERS));
  }

  @Test
  public void testListSchemas()
      throws Exception {
    /// GET /schemas returns a bare JSON array of schema names (not a {"schemas": [...]} wrapper).
    String jsonResponse = "[\"sch1\", \"sch2\"]";
    JsonNode mockResponse = new ObjectMapper().readTree(jsonResponse);
    lenient().when(_mockTransport.executeGet(anyString(), anyString(), any(), any()))
        .thenReturn(mockResponse);

    List<String> schemas = _adminClient.getSchemaClient().listSchemaNames();
    assertNotNull(schemas);
    assertEquals(schemas.size(), 2);
    assertEquals(schemas.get(1), "sch2");
  }

  @Test
  public void testSchemaGettersPreserveStringAndTypedAccess()
      throws Exception {
    String jsonResponse = "{\"schemaName\":\"sch1\",\"dimensionFieldSpecs\":[]}";
    JsonNode mockResponse = new ObjectMapper().readTree(jsonResponse);
    lenient().when(_mockTransport.executeGet(anyString(), anyString(), any(), any()))
        .thenReturn(mockResponse);

    String schemaJson = _adminClient.getSchemaClient().getSchema("sch1");
    Schema schemaObject = _adminClient.getSchemaClient().getSchemaObject("sch1");

    assertEquals(schemaJson, jsonResponse);
    assertEquals(schemaObject.getSchemaName(), "sch1");
  }

  @Test
  public void testAsyncListSchemas()
      throws Exception {
    String jsonResponse = "[\"sch1\"]";
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
  public void testTypedAdminGetters()
      throws Exception {
    // Use empty JSON objects that can deserialize into typed response classes
    lenient().when(_mockTransport.executeGet(anyString(), anyString(), any(), any()))
        .thenReturn(new ObjectMapper().readTree("{}"));

    String queryWorkloadConfig = _adminClient.getQueryWorkloadClient().getQueryWorkloadConfig("workload");
    PinotAppConfigs appConfigs = _adminClient.getClusterClient().getAppConfigs();
    PauseStatusDetails pauseStatus = _adminClient.getTableClient().getPauseStatusDetails("tbl");
    TableView idealState = _adminClient.getTableClient().getIdealStateObject("tbl_OFFLINE");
    TableView externalView = _adminClient.getTableClient().getExternalViewObject("tbl_OFFLINE");
    String logicalTable = _adminClient.getLogicalTableClient().getLogicalTable("logicalTable");

    assertNotNull(queryWorkloadConfig);
    assertNotNull(appConfigs);
    assertNotNull(pauseStatus);
    assertNotNull(idealState);
    assertNotNull(externalView);
    assertNotNull(logicalTable);
  }

  @Test
  public void testGetStaleSegmentsTyped()
      throws Exception {
    JsonNode mockResponse = new ObjectMapper().readTree("{\"table_OFFLINE\":{\"staleSegmentList\":[]}}");
    lenient().when(_mockTransport.executeGet(anyString(), anyString(), any(), any()))
        .thenReturn(mockResponse);

    Map<String, Map<String, Object>> staleSegments =
        _adminClient.getSegmentClient().getStaleSegments("table_OFFLINE",
            new TypeReference<Map<String, Map<String, Object>>>() { });

    assertNotNull(staleSegments.get("table_OFFLINE"));
    assertNotNull(staleSegments.get("table_OFFLINE").get("staleSegmentList"));
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
  public void testCreateTableMergesAdditionalHeaders()
      throws Exception {
    JsonNode mockResponse = new ObjectMapper().readTree("{\"status\":\"OK\"}");
    lenient().when(_mockTransport.executePost(anyString(), anyString(), any(), any(), any()))
        .thenReturn(mockResponse);

    _adminClient.getTableClient().createTable("{}", null, Map.of("X-Test-Header", "enabled"));

    verify(_mockTransport).executePost(eq(CONTROLLER_ADDRESS), eq("/tables"), eq("{}"), eq(Map.of()),
        eq(Map.of("Authorization", "Bearer token", "X-Test-Header", "enabled")));
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

  @Test
  public void testGetAggregateMetadataUsesMetadataEndpoint()
      throws Exception {
    JsonNode mockResponse = new ObjectMapper().readTree("{\"columnIndexSizeMap\":{}}");
    lenient().when(_mockTransport.executeGet(anyString(), anyString(), any(), any()))
        .thenReturn(mockResponse);

    _adminClient.getTableClient().getAggregateMetadata("tbl1_OFFLINE", "Carrier,TailNum");

    verify(_mockTransport).executeGet(eq(CONTROLLER_ADDRESS),
        eq("/tables/tbl1/metadata?type=OFFLINE&columns=Carrier&columns=TailNum"), isNull(), eq(HEADERS));
  }

  @Test
  public void testGetForceCommitJobStatusUsesPathParameter()
      throws Exception {
    JsonNode mockResponse = new ObjectMapper().readTree("{\"jobId\":\"job-123\"}");
    lenient().when(_mockTransport.executeGet(anyString(), anyString(), any(), any()))
        .thenReturn(mockResponse);

    _adminClient.getTableClient().getForceCommitJobStatus("job-123");

    verify(_mockTransport).executeGet(eq(CONTROLLER_ADDRESS), eq("/tables/forceCommitStatus/job-123"), isNull(),
        eq(HEADERS));
  }

  @Test
  public void testSegmentDownloadUsesControllerDownloadEndpoint()
      throws Exception {
    lenient().when(_mockTransport.executeGetBinary(anyString(), anyString(), any(), any()))
        .thenReturn(new byte[]{1, 2, 3});

    _adminClient.getSegmentClient().downloadSegment("tbl1_OFFLINE", "segmentA");

    verify(_mockTransport).executeGetBinary(eq(CONTROLLER_ADDRESS), eq("/segments/tbl1/segmentA"),
        isNull(), eq(HEADERS));
  }

  @Test
  public void testFilteredSegmentsMetadataUsesRepeatedQueryParams()
      throws Exception {
    JsonNode mockResponse = new ObjectMapper().readTree("{}");
    lenient().when(_mockTransport.executeGet(anyString(), anyString(), any(), any()))
        .thenReturn(mockResponse);

    _adminClient.getSegmentClient()
        .getSegmentsMetadata("tbl1", List.of("Carrier", "TailNum"), List.of("segmentA", "segmentB"), "OFFLINE");

    verify(_mockTransport).executeGet(eq(CONTROLLER_ADDRESS),
        eq("/segments/tbl1/metadata?type=OFFLINE&columns=Carrier&columns=TailNum&segments=segmentA&segments=segmentB"),
        isNull(), eq(HEADERS));
  }

  @Test
  public void testSegmentMetadataUsesRepeatedColumnQueryParams()
      throws Exception {
    JsonNode mockResponse = new ObjectMapper().readTree("{\"segment.total.docs\":\"10\"}");
    lenient().when(_mockTransport.executeGet(anyString(), anyString(), any(), any()))
        .thenReturn(mockResponse);

    _adminClient.getSegmentClient().getSegmentMetadata("tbl1_OFFLINE", "segmentA", List.of("Carrier", "TailNum"));

    verify(_mockTransport).executeGet(eq(CONTROLLER_ADDRESS),
        eq("/segments/tbl1_OFFLINE/segmentA/metadata?columns=Carrier&columns=TailNum"), isNull(), eq(HEADERS));
  }

  @Test
  public void testTableInstancePartitionEndpointsUseExplicitPaths()
      throws Exception {
    JsonNode mockResponse = new ObjectMapper().readTree("{\"status\":\"OK\"}");
    lenient().when(_mockTransport.executeGet(anyString(), anyString(), any(), any()))
        .thenReturn(mockResponse);
    lenient().when(_mockTransport.executePost(anyString(), anyString(), any(), any(), any()))
        .thenReturn(mockResponse);
    lenient().when(_mockTransport.executePut(anyString(), anyString(), any(), any(), any()))
        .thenReturn(mockResponse);
    lenient().when(_mockTransport.executeDelete(anyString(), anyString(), any(), any()))
        .thenReturn(mockResponse);

    _adminClient.getTableClient().getInstancePartitions("tbl1_OFFLINE", "OFFLINE");
    _adminClient.getTableClient().assignInstances("tbl1_OFFLINE", "OFFLINE", true);
    _adminClient.getTableClient().replaceInstance("tbl1_OFFLINE", "OFFLINE", "Server_1", "Server_2");
    _adminClient.getTableClient().updateInstancePartitions("tbl1_OFFLINE", "{\"name\":\"value\"}");
    _adminClient.getTableClient().deleteInstancePartitions("tbl1_OFFLINE", "OFFLINE");

    verify(_mockTransport).executeGet(eq(CONTROLLER_ADDRESS), eq("/tables/tbl1_OFFLINE/instancePartitions"),
        eq(Map.of("type", "OFFLINE")), eq(HEADERS));
    verify(_mockTransport).executePost(eq(CONTROLLER_ADDRESS), eq("/tables/tbl1_OFFLINE/assignInstances"), isNull(),
        eq(Map.of("type", "OFFLINE", "dryRun", "true")), eq(HEADERS));
    verify(_mockTransport).executePost(eq(CONTROLLER_ADDRESS), eq("/tables/tbl1_OFFLINE/replaceInstance"), isNull(),
        eq(Map.of("type", "OFFLINE", "oldInstanceId", "Server_1", "newInstanceId", "Server_2")), eq(HEADERS));
    verify(_mockTransport).executePut(eq(CONTROLLER_ADDRESS), eq("/tables/tbl1_OFFLINE/instancePartitions"),
        eq("{\"name\":\"value\"}"), isNull(), eq(HEADERS));
    verify(_mockTransport).executeDelete(eq(CONTROLLER_ADDRESS), eq("/tables/tbl1_OFFLINE/instancePartitions"),
        eq(Map.of("type", "OFFLINE")), eq(HEADERS));
  }

  @Test
  public void testLogicalTableMethodsMergeAdditionalHeaders()
      throws Exception {
    JsonNode mockResponse = new ObjectMapper().readTree("{\"status\":\"OK\"}");
    lenient().when(_mockTransport.executePost(anyString(), anyString(), any(), any(), any()))
        .thenReturn(mockResponse);
    lenient().when(_mockTransport.executePut(anyString(), anyString(), any(), any(), any()))
        .thenReturn(mockResponse);
    lenient().when(_mockTransport.executeGet(anyString(), anyString(), any(), any()))
        .thenReturn(mockResponse);
    lenient().when(_mockTransport.executeDelete(anyString(), anyString(), any(), any()))
        .thenReturn(mockResponse);

    Map<String, String> extraHeaders = Map.of("X-Test-Header", "enabled");
    _adminClient.getLogicalTableClient().createLogicalTable("{}", extraHeaders);
    _adminClient.getLogicalTableClient().updateLogicalTable("logicalTable", "{}", extraHeaders);
    _adminClient.getLogicalTableClient().getLogicalTable("logicalTable", extraHeaders);
    _adminClient.getLogicalTableClient().listLogicalTables(extraHeaders);
    _adminClient.getLogicalTableClient().deleteLogicalTable("logicalTable", extraHeaders);

    Map<String, String> mergedHeaders = Map.of("Authorization", "Bearer token", "X-Test-Header", "enabled");
    verify(_mockTransport).executePost(eq(CONTROLLER_ADDRESS), eq("/logicalTables"), eq("{}"), isNull(),
        eq(mergedHeaders));
    verify(_mockTransport).executePut(eq(CONTROLLER_ADDRESS), eq("/logicalTables/logicalTable"), eq("{}"), isNull(),
        eq(mergedHeaders));
    verify(_mockTransport).executeGet(eq(CONTROLLER_ADDRESS), eq("/logicalTables/logicalTable"), isNull(),
        eq(mergedHeaders));
    verify(_mockTransport).executeGet(eq(CONTROLLER_ADDRESS), eq("/logicalTables"), isNull(), eq(mergedHeaders));
    verify(_mockTransport).executeDelete(eq(CONTROLLER_ADDRESS), eq("/logicalTables/logicalTable"), isNull(),
        eq(mergedHeaders));
  }

  @Test
  public void testClusterConfigSingleEntryUpdateUsesDedicatedApi()
      throws Exception {
    JsonNode mockResponse = new ObjectMapper().readTree("{\"status\":\"OK\"}");
    lenient().when(_mockTransport.executePost(anyString(), anyString(), any(), any(), any()))
        .thenReturn(mockResponse);

    _adminClient.getClusterClient().updateClusterConfig("pinot.test.config", "enabled");

    verify(_mockTransport).executePost(eq(CONTROLLER_ADDRESS), eq("/cluster/configs"),
        eq("{\"pinot.test.config\":\"enabled\"}"), isNull(), eq(HEADERS));
  }

  @Test
  public void testRebalanceAndQueryClientsUseDedicatedEndpoints()
      throws Exception {
    JsonNode mockResponse = new ObjectMapper().readTree("{}");
    lenient().when(_mockTransport.executeGet(anyString(), anyString(), any(), any()))
        .thenReturn(mockResponse);
    lenient().when(_mockTransport.executeDelete(anyString(), anyString(), any(), any()))
        .thenReturn(mockResponse);

    ServerRebalanceJobStatusResponse rebalanceStatus =
        _adminClient.getRebalanceClient().getRebalanceStatusObject("job-123");
    _adminClient.getQueryClient().cancelQueryByClientId("client-query-1");

    assertNotNull(rebalanceStatus);
    verify(_mockTransport).executeGet(eq(CONTROLLER_ADDRESS), eq("/rebalanceStatus/job-123"), isNull(), eq(HEADERS));
    verify(_mockTransport).executeDelete(eq(CONTROLLER_ADDRESS), eq("/clientQuery/client-query-1"), isNull(),
        eq(HEADERS));
  }

  @Test
  public void testListLiveInstancesUsesInstancesField()
      throws Exception {
    /// GET /liveinstances returns an Instances wrapper serialized as {"instances": [...]}.
    JsonNode mockResponse = new ObjectMapper().readTree("{\"instances\": [\"Server_1\", \"Broker_1\"]}");
    lenient().when(_mockTransport.executeGet(anyString(), anyString(), any(), any())).thenReturn(mockResponse);

    List<String> instances = _adminClient.getInstanceClient().listLiveInstances();

    assertEquals(instances, List.of("Server_1", "Broker_1"));
    verify(_mockTransport).executeGet(eq(CONTROLLER_ADDRESS), eq("/liveinstances"), isNull(), eq(HEADERS));
  }

  @Test
  public void testValidateUpdateInstanceTagsSendsRequestBody()
      throws Exception {
    JsonNode mockResponse = new ObjectMapper().readTree("[]");
    lenient().when(_mockTransport.executePost(anyString(), anyString(), any(), any(), any()))
        .thenReturn(mockResponse);

    _adminClient.getInstanceClient().validateUpdateInstanceTags("Server_1,Server_2", "tag1,tag2");

    /// Controller reads a List<InstanceTagUpdateRequest> from the body; no query params are used.
    List<Map<String, Object>> expectedBody = List.of(
        Map.of("instanceName", "Server_1", "newTags", List.of("tag1", "tag2")),
        Map.of("instanceName", "Server_2", "newTags", List.of("tag1", "tag2")));
    verify(_mockTransport).executePost(eq(CONTROLLER_ADDRESS), eq("/instances/updateTags/validate"), eq(expectedBody),
        isNull(), eq(HEADERS));
  }

  @Test
  public void testCancelRebalanceParsesBareArray()
      throws Exception {
    /// DELETE /tables/{tableName}/rebalance returns a bare JSON array of cancelled job IDs.
    JsonNode mockResponse = new ObjectMapper().readTree("[\"job-1\", \"job-2\"]");
    lenient().when(_mockTransport.executeDelete(anyString(), anyString(), any(), any())).thenReturn(mockResponse);

    List<String> jobIds = _adminClient.getTableClient().cancelRebalance("tbl1_OFFLINE");

    assertEquals(jobIds, List.of("job-1", "job-2"));
    verify(_mockTransport).executeDelete(eq(CONTROLLER_ADDRESS), eq("/tables/tbl1_OFFLINE/rebalance"), isNull(),
        eq(HEADERS));
  }

  @Test
  public void testRebalanceTableMapsDowntimeAndMinAvailableReplicas()
      throws Exception {
    JsonNode mockResponse = new ObjectMapper().readTree("{\"status\":\"OK\"}");
    lenient().when(_mockTransport.executePost(anyString(), anyString(), any(), any(), any()))
        .thenReturn(mockResponse);

    _adminClient.getTableClient().rebalanceTable("tbl1_OFFLINE", true, 2);

    verify(_mockTransport).executePost(eq(CONTROLLER_ADDRESS), eq("/tables/tbl1_OFFLINE/rebalance"), isNull(),
        eq(Map.of("downtime", "false", "minAvailableReplicas", "2")), eq(HEADERS));
  }

  @Test
  public void testSelectSegmentsUsesStartEndTimestampParams()
      throws Exception {
    JsonNode mockResponse = new ObjectMapper().readTree("[]");
    lenient().when(_mockTransport.executeGet(anyString(), anyString(), any(), any())).thenReturn(mockResponse);

    _adminClient.getSegmentClient().selectSegments("tbl1", "OFFLINE", 100L, 200L, true);

    verify(_mockTransport).executeGet(eq(CONTROLLER_ADDRESS), eq("/segments/tbl1/select"),
        eq(Map.of("startTimestamp", "100", "endTimestamp", "200", "excludeReplacedSegments", "true", "type",
            "OFFLINE")), eq(HEADERS));
  }

  @Test
  public void testGetZookeeperMetadataParsesBareMap()
      throws Exception {
    JsonNode mockResponse = new ObjectMapper()
        .readTree("{\"segment_1\":{\"segment.crc\":\"123\"},\"segment_2\":{\"segment.crc\":\"456\"}}");
    lenient().when(_mockTransport.executeGet(anyString(), anyString(), any(), any())).thenReturn(mockResponse);

    Map<String, Map<String, String>> zkMetadata = _adminClient.getSegmentClient().getZookeeperMetadata("tbl1_OFFLINE");

    assertEquals(zkMetadata.size(), 2);
    assertEquals(zkMetadata.get("segment_1").get("segment.crc"), "123");
    verify(_mockTransport).executeGet(eq(CONTROLLER_ADDRESS), eq("/segments/tbl1_OFFLINE/zkmetadata"), isNull(),
        eq(HEADERS));
  }

  @Test
  public void testTaskEndpointsParseBareValues()
      throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    /// Every task endpoint resumes a bare value (array/string/int/map), not a wrapper object.
    lenient().when(_mockTransport.executeGet(eq(CONTROLLER_ADDRESS), eq("/tasks/tasktypes"), any(), any()))
        .thenReturn(mapper.readTree("[\"TaskA\", \"TaskB\"]"));
    lenient().when(_mockTransport.executeGet(eq(CONTROLLER_ADDRESS), eq("/tasks/TaskA/state"), any(), any()))
        .thenReturn(mapper.readTree("\"IN_PROGRESS\""));
    lenient().when(_mockTransport.executeGet(eq(CONTROLLER_ADDRESS), eq("/tasks/TaskA/tasks"), any(), any()))
        .thenReturn(mapper.readTree("[\"Task_TaskA_1\"]"));
    lenient().when(_mockTransport.executeGet(eq(CONTROLLER_ADDRESS), eq("/tasks/TaskA/tasks/count"), any(), any()))
        .thenReturn(mapper.readTree("5"));
    lenient().when(_mockTransport.executeGet(eq(CONTROLLER_ADDRESS), eq("/tasks/TaskA/taskstates"), any(), any()))
        .thenReturn(mapper.readTree("{\"Task_TaskA_1\":\"COMPLETED\"}"));
    lenient().when(_mockTransport.executeGet(eq(CONTROLLER_ADDRESS), eq("/tasks/TaskA/tbl1_OFFLINE/state"), any(),
        any())).thenReturn(mapper.readTree("{\"Task_TaskA_1\":\"COMPLETED\"}"));
    lenient().when(_mockTransport.executeGet(eq(CONTROLLER_ADDRESS), eq("/tasks/subtask/Task_TaskA_1/state"), any(),
        any())).thenReturn(mapper.readTree("{\"Task_TaskA_1_0\":\"RUNNING\"}"));

    TaskAdminClient taskClient = _adminClient.getTaskClient();
    assertEquals(taskClient.listTaskTypes(), Set.of("TaskA", "TaskB"));
    assertEquals(taskClient.getTaskQueueState("TaskA"), TaskState.IN_PROGRESS);
    assertEquals(taskClient.getTasks("TaskA"), Set.of("Task_TaskA_1"));
    assertEquals(taskClient.getTasksCount("TaskA"), 5);
    assertEquals(taskClient.getTaskStates("TaskA"), Map.of("Task_TaskA_1", TaskState.COMPLETED));
    assertEquals(taskClient.getTaskStatesByTable("TaskA", "tbl1_OFFLINE"), Map.of("Task_TaskA_1", TaskState.COMPLETED));
    assertEquals(taskClient.getSubtaskStates("Task_TaskA_1"), Map.of("Task_TaskA_1_0", "RUNNING"));
  }

  @Test
  public void testGetTaskCountsUsesTableParam()
      throws Exception {
    JsonNode mockResponse = new ObjectMapper().readTree("{}");
    lenient().when(_mockTransport.executeGet(anyString(), anyString(), any(), any())).thenReturn(mockResponse);

    _adminClient.getTaskClient().getTaskCounts("TaskA", "IN_PROGRESS", "tbl1_OFFLINE", null);

    verify(_mockTransport).executeGet(eq(CONTROLLER_ADDRESS), eq("/tasks/TaskA/taskcounts"),
        eq(Map.of("state", "IN_PROGRESS", "table", "tbl1_OFFLINE")), eq(HEADERS));
  }

  @Test
  public void testGetTaskDebugInfoUsesTableNameParam()
      throws Exception {
    JsonNode mockResponse = new ObjectMapper().readTree("{}");
    lenient().when(_mockTransport.executeGet(anyString(), anyString(), any(), any())).thenReturn(mockResponse);

    _adminClient.getTaskClient().getTaskDebugInfo("Task_TaskA_1", 1, "tbl1_OFFLINE");

    verify(_mockTransport).executeGet(eq(CONTROLLER_ADDRESS), eq("/tasks/task/Task_TaskA_1/debug"),
        eq(Map.of("verbosity", "1", "tableName", "tbl1_OFFLINE")), eq(HEADERS));
  }

  @Test
  public void testListTenantsAsyncParsesTenantArrays()
      throws Exception {
    /// GET /tenants returns {"SERVER_TENANTS": [...], "BROKER_TENANTS": [...]} -- there is no flat "tenants" field.
    JsonNode mockResponse = new ObjectMapper().readTree(
        "{\"SERVER_TENANTS\":[\"DefaultTenant\"],\"BROKER_TENANTS\":[\"DefaultTenant\",\"brokerTenant\"]}");
    lenient().when(_mockTransport.executeGetAsync(anyString(), anyString(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(mockResponse));

    List<String> tenants = _adminClient.getTenantClient().listTenantsAsync().get();

    assertEquals(Set.copyOf(tenants), Set.of("DefaultTenant", "brokerTenant"));
  }

  @Test
  public void testRebalanceTenantUsesIncludeExcludeTablesAndBody()
      throws Exception {
    JsonNode mockResponse = new ObjectMapper().readTree("{\"status\":\"OK\"}");
    lenient().when(_mockTransport.executePost(anyString(), anyString(), any(), any(), any()))
        .thenReturn(mockResponse);

    _adminClient.getTenantClient().rebalanceTenant("DefaultTenant", 2, "tbl1_OFFLINE", "tbl2_OFFLINE");

    /// The controller requires a TenantRebalanceConfig body; an empty object lets the query params populate it.
    verify(_mockTransport).executePost(eq(CONTROLLER_ADDRESS), eq("/tenants/DefaultTenant/rebalance"), eq("{}"),
        eq(Map.of("degreeOfParallelism", "2", "includeTables", "tbl1_OFFLINE", "excludeTables", "tbl2_OFFLINE")),
        eq(HEADERS));
  }

  @Test
  public void testListSchemaNamesEmptyArrayReturnsEmptyList()
      throws Exception {
    JsonNode mockResponse = new ObjectMapper().readTree("[]");
    lenient().when(_mockTransport.executeGet(anyString(), anyString(), any(), any())).thenReturn(mockResponse);

    assertEquals(_adminClient.getSchemaClient().listSchemaNames(), List.of());
  }

  @Test(expectedExceptions = PinotAdminException.class)
  public void testListSchemaNamesThrowsOnUnexpectedResponse()
      throws Exception {
    /// A non-array response (e.g. an error object) must surface as an exception, not a silently empty list.
    JsonNode mockResponse = new ObjectMapper().readTree("{\"code\":500}");
    lenient().when(_mockTransport.executeGet(anyString(), anyString(), any(), any())).thenReturn(mockResponse);

    _adminClient.getSchemaClient().listSchemaNames();
  }

  @Test
  public void testListTenantsHandlesMissingTenantArrays()
      throws Exception {
    /// Defensive: a response without SERVER_TENANTS/BROKER_TENANTS must yield an empty list, not an NPE.
    JsonNode mockResponse = new ObjectMapper().readTree("{}");
    lenient().when(_mockTransport.executeGet(anyString(), anyString(), any(), any())).thenReturn(mockResponse);

    assertEquals(_adminClient.getTenantClient().listTenants(), List.of());
  }
}
