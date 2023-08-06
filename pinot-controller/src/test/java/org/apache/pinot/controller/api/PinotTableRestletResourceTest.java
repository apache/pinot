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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.controller.api.resources.TableAndSchemaConfig;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceResult;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.StringUtil;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.JsonUtils.stringToJsonNode;
import static org.testng.Assert.*;


/**
 * Test for table creation
 */
public class PinotTableRestletResourceTest extends ControllerTest {
  private static final String OFFLINE_TABLE_NAME = "testOfflineTable";
  private static final String REALTIME_TABLE_NAME = "testRealtimeTable";
  private final TableConfigBuilder _offlineBuilder = new TableConfigBuilder(TableType.OFFLINE);
  private final TableConfigBuilder _realtimeBuilder = new TableConfigBuilder(TableType.REALTIME)
      .setStreamConfigs(Map.of("stream.type", "foo", "consumer.type", "lowlevel"));
  private String _createTableUrl;

  @BeforeClass
  public void setUp()
      throws Exception {
    DEFAULT_INSTANCE.setupSharedStateAndValidate();

    _createTableUrl = DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableCreate();
    _offlineBuilder.setTableName(OFFLINE_TABLE_NAME).setTimeColumnName("timeColumn").setTimeType("DAYS")
        .setRetentionTimeUnit("DAYS").setRetentionTimeValue("5");

    // add schema for realtime table
    DEFAULT_INSTANCE.addDummySchema(REALTIME_TABLE_NAME);
    StreamConfig streamConfig = FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs();
    _realtimeBuilder.setTableName(REALTIME_TABLE_NAME).setTimeColumnName("timeColumn").setTimeType("DAYS")
        .setRetentionTimeUnit("DAYS").setRetentionTimeValue("5").setSchemaName(REALTIME_TABLE_NAME)
        .setStreamConfigs(streamConfig.getStreamConfigsMap());
  }

  @Test
  public void testCreateTable()
      throws Exception {
    // Create an OFFLINE table with an invalid name which should fail
    // NOTE: Set bad table name inside table config builder is not allowed, so have to set in json node
    TableConfig offlineTableConfig = _offlineBuilder.build();
    ObjectNode offlineTableConfigJson = (ObjectNode) offlineTableConfig.toJsonNode();
    offlineTableConfigJson.put(TableConfig.TABLE_NAME_KEY, "bad__table__name");
    try {
      sendPostRequest(_createTableUrl, offlineTableConfigJson.toString());
      fail("Creation of an OFFLINE table with two underscores in the table name does not fail");
    } catch (IOException e) {
      // Expected 400 Bad Request
      assertTrue(e.getMessage().contains("Got error status code: 400"));
    }

    offlineTableConfig = _offlineBuilder.build();
    offlineTableConfigJson = (ObjectNode) offlineTableConfig.toJsonNode();
    offlineTableConfigJson.put(TableConfig.TABLE_NAME_KEY, "bad.table.with.dot");
    try {
      sendPostRequest(_createTableUrl, offlineTableConfigJson.toString());
      fail("Creation of an OFFLINE table with dot in the table name does not fail");
    } catch (IOException e) {
      // Expected 400 Bad Request
      assertTrue(e.getMessage().contains("Got error status code: 400"));
    }

    // Create an OFFLINE table with a valid name which should succeed
    offlineTableConfig = _offlineBuilder.setTableName("valid_table_name").build();
    String offlineTableConfigString = offlineTableConfig.toJsonString();
    sendPostRequest(_createTableUrl, offlineTableConfigString);

    // Create an OFFLINE table that already exists which should fail
    try {
      sendPostRequest(_createTableUrl, offlineTableConfigString);
      fail("Creation of an existing OFFLINE table does not fail");
    } catch (IOException e) {
      // Expected 409 Conflict
      assertTrue(e.getMessage().contains("Got error status code: 409"));
    }

    // Create an OFFLINE table with invalid replication config
    offlineTableConfig.getValidationConfig().setReplication("abc");
    try {
      sendPostRequest(_createTableUrl, offlineTableConfig.toJsonString());
      fail("Creation of an invalid OFFLINE table does not fail");
    } catch (IOException e) {
      // Expected 400 Bad Request
      assertTrue(e.getMessage().contains("Got error status code: 400"));
    }

    // Create a REALTIME table with an invalid name which should fail
    // NOTE: Set bad table name inside table config builder is not allowed, so have to set in json node
    TableConfig realtimeTableConfig = _realtimeBuilder.build();
    ObjectNode realtimeTableConfigJson = (ObjectNode) realtimeTableConfig.toJsonNode();
    realtimeTableConfigJson.put(TableConfig.TABLE_NAME_KEY, "bad__table__name");
    try {
      sendPostRequest(_createTableUrl, realtimeTableConfigJson.toString());
      fail("Creation of a REALTIME table with two underscores in the table name does not fail");
    } catch (IOException e) {
      // Expected 400 Bad Request
      assertTrue(e.getMessage().contains("Got error status code: 400"));
    }

    // Creating a REALTIME table without a valid schema should fail
    realtimeTableConfig = _realtimeBuilder.setTableName("noSchema").setSchemaName("noSchema").build();
    try {
      sendPostRequest(_createTableUrl, realtimeTableConfig.toJsonString());
      fail("Creation of a REALTIME table without a valid schema does not fail");
    } catch (IOException e) {
      // Expected 400 Bad Request
      assertTrue(e.getMessage().contains("Got error status code: 400"));
    }

    // Creating a REALTIME table with a different schema name in the config should succeed (backwards compatibility)
    realtimeTableConfig = _realtimeBuilder.setSchemaName(REALTIME_TABLE_NAME).build();
    sendPostRequest(_createTableUrl, realtimeTableConfig.toJsonString());

    // Create a REALTIME table with the invalid time column name should fail
    realtimeTableConfig =
        _realtimeBuilder.setTableName(REALTIME_TABLE_NAME).setTimeColumnName("invalidTimeColumn").build();
    try {
      sendPostRequest(_createTableUrl, realtimeTableConfig.toJsonString());
      fail("Creation of a REALTIME table with a invalid time column name does not fail");
    } catch (IOException e) {
      // Expected 400 Bad Request
      assertTrue(e.getMessage().contains("Got error status code: 400"));
    }

    // Create a REALTIME table with a valid name and schema which should succeed
    realtimeTableConfig = _realtimeBuilder.setTableName(REALTIME_TABLE_NAME).setTimeColumnName("timeColumn").build();
    String realtimeTableConfigString = realtimeTableConfig.toJsonString();
    sendPostRequest(_createTableUrl, realtimeTableConfigString);
  }

  @Test
  public void testTableCronSchedule() {
    String rawTableName = "test_table_cron_schedule";
    // Failed to create a table
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(rawTableName).setTaskConfig(
        new TableTaskConfig(ImmutableMap.of(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE,
            ImmutableMap.of(PinotTaskManager.SCHEDULE_KEY, "* * * * * * *")))).build();
    try {
      sendPostRequest(_createTableUrl, tableConfig.toJsonString());
      fail("Creation of an OFFLINE table with an invalid cron expression does not fail");
    } catch (IOException e) {
      // Expected 400 Bad Request
      assertTrue(e.getMessage().contains("Got error status code: 400"));
    }

    // Succeed to create a table
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(rawTableName).setTaskConfig(
        new TableTaskConfig(ImmutableMap.of(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE,
            ImmutableMap.of(PinotTaskManager.SCHEDULE_KEY, "0 */10 * ? * * *")))).build();
    try {
      String response = sendPostRequest(_createTableUrl, tableConfig.toJsonString());
      assertEquals(response,
          "{\"unrecognizedProperties\":{},\"status\":\"Table test_table_cron_schedule_OFFLINE successfully added\"}");
    } catch (IOException e) {
      // Expected 400 Bad Request
      fail("This is a valid table config with cron schedule");
    }

    // Failed to update the table
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(rawTableName).setTaskConfig(
        new TableTaskConfig(ImmutableMap.of(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE,
            ImmutableMap.of(PinotTaskManager.SCHEDULE_KEY, "5 5 5 5 5 5 5")))).build();
    try {
      sendPutRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forUpdateTableConfig(rawTableName),
          tableConfig.toJsonString());
      fail("Update of an OFFLINE table with an invalid cron expression does not fail");
    } catch (IOException e) {
      // Expected 400 Bad Request
      assertTrue(e.getMessage().contains("Got error status code: 400"));
    }
  }

  @Test
  public void testTableMinReplication()
      throws Exception {
    testTableMinReplicationInternal("minReplicationOne", 1);
    testTableMinReplicationInternal("minReplicationTwo", DEFAULT_NUM_SERVER_INSTANCES);
  }

  private void testTableMinReplicationInternal(String tableName, int tableReplication)
      throws Exception {
    String tableJSONConfigString =
        _offlineBuilder.setTableName(tableName).setNumReplicas(tableReplication).build().toJsonString();
    sendPostRequest(_createTableUrl, tableJSONConfigString);
    // table creation should succeed
    TableConfig tableConfig = getTableConfig(tableName, "OFFLINE");
    assertEquals(tableConfig.getReplication(),
        Math.max(tableReplication, DEFAULT_MIN_NUM_REPLICAS));

    DEFAULT_INSTANCE.addDummySchema(tableName);
    tableJSONConfigString =
        _realtimeBuilder.setTableName(tableName).setNumReplicas(tableReplication).build().toJsonString();
    sendPostRequest(_createTableUrl, tableJSONConfigString);
    tableConfig = getTableConfig(tableName, "REALTIME");
    assertEquals(tableConfig.getReplication(), Math.max(tableReplication, DEFAULT_MIN_NUM_REPLICAS));

    DEFAULT_INSTANCE.getHelixResourceManager().deleteOfflineTable(tableName);
    DEFAULT_INSTANCE.getHelixResourceManager().deleteRealtimeTable(tableName);
  }

  @Test
  public void testDimTableStorageQuotaConstraints()
      throws Exception {
    // Controller assigns default quota if none provided
    String tableName = "myDimTable_basic";
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName(tableName).addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
            .setPrimaryKeyColumns(Lists.newArrayList("myCol")).build();
    DEFAULT_INSTANCE.addSchema(schema);

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).setIsDimTable(true).build();
    sendPostRequest(_createTableUrl, tableConfig.toJsonString());
    tableConfig = getTableConfig(tableName, "OFFLINE");
    assertEquals(tableConfig.getQuotaConfig().getStorage(),
        DEFAULT_INSTANCE.getControllerConfig().getDimTableMaxSize());

    // Controller throws exception if quote exceed configured max value
    tableName = "myDimTable_broken";
    schema =
        new Schema.SchemaBuilder().setSchemaName(tableName).addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
            .setPrimaryKeyColumns(Lists.newArrayList("myCol")).build();
    DEFAULT_INSTANCE.addSchema(schema);
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).setIsDimTable(true)
        .setQuotaConfig(new QuotaConfig("500G", null)).build();
    try {
      sendPostRequest(_createTableUrl, tableConfig.toJsonString());
      fail("Creation of a DIMENSION table with larger than allowed storage quota should fail");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Got error status code: 400"));
    }

    // Successful creation with proper quota
    tableName = "myDimTable_good";
    schema =
        new Schema.SchemaBuilder().setSchemaName(tableName).addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
            .setPrimaryKeyColumns(Lists.newArrayList("myCol")).build();
    DEFAULT_INSTANCE.addSchema(schema);
    String goodQuota = "100M";
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).setIsDimTable(true)
        .setQuotaConfig(new QuotaConfig(goodQuota, null)).build();
    sendPostRequest(_createTableUrl, tableConfig.toJsonString());
    tableConfig = getTableConfig(tableName, "OFFLINE");
    assertEquals(tableConfig.getQuotaConfig().getStorage(), goodQuota);
  }

  private TableConfig getTableConfig(String tableName, String tableType)
      throws Exception {
    String tableConfigString = sendGetRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableGet(tableName));
    return JsonUtils.jsonNodeToObject(stringToJsonNode(tableConfigString).get(tableType), TableConfig.class);
  }

  @Test
  public void testUpdateTableConfig()
      throws Exception {
    String tableName = "updateTC";
    String tableConfigString = _offlineBuilder.setTableName(tableName).setNumReplicas(2).build().toJsonString();
    sendPostRequest(_createTableUrl, tableConfigString);
    // table creation should succeed
    TableConfig tableConfig = getTableConfig(tableName, "OFFLINE");
    assertEquals(tableConfig.getValidationConfig().getRetentionTimeValue(), "5");
    assertEquals(tableConfig.getValidationConfig().getRetentionTimeUnit(), "DAYS");

    tableConfig.getValidationConfig().setRetentionTimeUnit("HOURS");
    tableConfig.getValidationConfig().setRetentionTimeValue("10");

    JsonNode jsonResponse = stringToJsonNode(
        sendPutRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forUpdateTableConfig(tableName),
            tableConfig.toJsonString()));
    assertTrue(jsonResponse.has("status"));

    TableConfig modifiedConfig = getTableConfig(tableName, "OFFLINE");
    assertEquals(modifiedConfig.getValidationConfig().getRetentionTimeUnit(), "HOURS");
    assertEquals(modifiedConfig.getValidationConfig().getRetentionTimeValue(), "10");

    // Realtime
    DEFAULT_INSTANCE.addDummySchema(tableName);
    tableConfigString = _realtimeBuilder.setTableName(tableName).setNumReplicas(2).build().toJsonString();
    sendPostRequest(_createTableUrl, tableConfigString);
    tableConfig = getTableConfig(tableName, "REALTIME");
    assertEquals(tableConfig.getValidationConfig().getRetentionTimeValue(), "5");
    assertEquals(tableConfig.getValidationConfig().getRetentionTimeUnit(), "DAYS");
    assertNull(tableConfig.getQuotaConfig());

    QuotaConfig quota = new QuotaConfig("10G", "100.0");
    tableConfig.setQuotaConfig(quota);
    sendPutRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forUpdateTableConfig(tableName),
        tableConfig.toJsonString());
    modifiedConfig = getTableConfig(tableName, "REALTIME");
    assertNotNull(modifiedConfig.getQuotaConfig());
    assertEquals(modifiedConfig.getQuotaConfig().getStorage(), "10G");
    assertEquals(modifiedConfig.getQuotaConfig().getMaxQueriesPerSecond(), "100.0");

    try {
      // table does not exist
      ObjectNode tableConfigJson = (ObjectNode) tableConfig.toJsonNode();
      tableConfigJson.put(TableConfig.TABLE_NAME_KEY, "noSuchTable_REALTIME");
      sendPutRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forUpdateTableConfig("noSuchTable"),
          tableConfigJson.toString());
    } catch (Exception e) {
      assertTrue(e instanceof IOException);
    }
  }

  private void deleteAllTables()
      throws IOException {
    List<String> tables = getTableNames(_createTableUrl + "?type=offline");
    tables.addAll(getTableNames(_createTableUrl + "?type=realtime"));
    for (String tableName : tables) {
      sendDeleteRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableDelete(tableName));
    }
  }

  @Test
  public void testListTables()
      throws Exception {
    deleteAllTables();
    List<String> tables = getTableNames(_createTableUrl);
    assertTrue(tables.isEmpty());

    // post 2 offline, 1 realtime
    String rawTableName1 = "pqr";
    TableConfig offlineTableConfig1 = _offlineBuilder.setTableName(rawTableName1).build();
    sendPostRequest(_createTableUrl, offlineTableConfig1.toJsonString());
    DEFAULT_INSTANCE.addDummySchema(rawTableName1);
    TableConfig realtimeTableConfig1 = _realtimeBuilder.setTableName(rawTableName1).setNumReplicas(2).build();
    sendPostRequest(_createTableUrl, realtimeTableConfig1.toJsonString());
    String rawTableName2 = "abc";
    TableConfig offlineTableConfig2 = _offlineBuilder.setTableName(rawTableName2).build();
    sendPostRequest(_createTableUrl, offlineTableConfig2.toJsonString());

    // list
    tables = getTableNames(_createTableUrl);
    assertEquals(tables, Lists.newArrayList("abc", "pqr"));
    tables = getTableNames(_createTableUrl + "?sortAsc=false");
    assertEquals(tables, Lists.newArrayList("pqr", "abc"));
    tables = getTableNames(_createTableUrl + "?sortType=creationTime");
    assertEquals(tables, Lists.newArrayList("pqr_OFFLINE", "pqr_REALTIME", "abc_OFFLINE"));
    tables = getTableNames(_createTableUrl + "?sortType=creationTime&sortAsc=false");
    assertEquals(tables, Lists.newArrayList("abc_OFFLINE", "pqr_REALTIME", "pqr_OFFLINE"));
    tables = getTableNames(_createTableUrl + "?sortType=lastModifiedTime");
    assertEquals(tables, Lists.newArrayList("pqr_OFFLINE", "pqr_REALTIME", "abc_OFFLINE"));
    tables = getTableNames(_createTableUrl + "?sortType=lastModifiedTime&sortAsc=false");
    assertEquals(tables, Lists.newArrayList("abc_OFFLINE", "pqr_REALTIME", "pqr_OFFLINE"));

    // type
    tables = getTableNames(_createTableUrl + "?type=realtime");
    assertEquals(tables, Lists.newArrayList("pqr_REALTIME"));
    tables = getTableNames(_createTableUrl + "?type=offline");
    assertEquals(tables, Lists.newArrayList("abc_OFFLINE", "pqr_OFFLINE"));
    tables = getTableNames(_createTableUrl + "?type=offline&sortAsc=false");
    assertEquals(tables, Lists.newArrayList("pqr_OFFLINE", "abc_OFFLINE"));
    tables = getTableNames(_createTableUrl + "?type=offline&sortType=creationTime");
    assertEquals(tables, Lists.newArrayList("pqr_OFFLINE", "abc_OFFLINE"));
    tables = getTableNames(_createTableUrl + "?type=offline&sortType=creationTime&sortAsc=false");
    assertEquals(tables, Lists.newArrayList("abc_OFFLINE", "pqr_OFFLINE"));
    tables = getTableNames(_createTableUrl + "?type=offline&sortType=lastModifiedTime");
    assertEquals(tables, Lists.newArrayList("pqr_OFFLINE", "abc_OFFLINE"));
    tables = getTableNames(_createTableUrl + "?type=offline&sortType=lastModifiedTime&sortAsc=false");
    assertEquals(tables, Lists.newArrayList("abc_OFFLINE", "pqr_OFFLINE"));

    // update taskType for abc_OFFLINE
    Map<String, Map<String, String>> taskTypeMap = new HashMap<>();
    taskTypeMap.put(MinionConstants.MergeRollupTask.TASK_TYPE, new HashMap<>());
    offlineTableConfig2.setTaskConfig(new TableTaskConfig(taskTypeMap));
    stringToJsonNode(
        sendPutRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forUpdateTableConfig(rawTableName2),
            offlineTableConfig2.toJsonString()));
    // update for pqr_REALTIME
    taskTypeMap = new HashMap<>();
    taskTypeMap.put(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE, new HashMap<>());
    realtimeTableConfig1.setTaskConfig(new TableTaskConfig(taskTypeMap));
    stringToJsonNode(
        sendPutRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forUpdateTableConfig(rawTableName1),
            realtimeTableConfig1.toJsonString()));

    // list lastModified, taskType
    tables = getTableNames(_createTableUrl + "?sortType=lastModifiedTime");
    assertEquals(tables, Lists.newArrayList("pqr_OFFLINE", "abc_OFFLINE", "pqr_REALTIME"));
    tables = getTableNames(_createTableUrl + "?sortType=lastModifiedTime&sortAsc=false");
    assertEquals(tables, Lists.newArrayList("pqr_REALTIME", "abc_OFFLINE", "pqr_OFFLINE"));
    tables = getTableNames(_createTableUrl + "?taskType=MergeRollupTask");
    assertEquals(tables, Lists.newArrayList("abc_OFFLINE"));
    tables = getTableNames(_createTableUrl + "?taskType=MergeRollupTask&type=realtime");
    assertTrue(tables.isEmpty());
    tables = getTableNames(_createTableUrl + "?taskType=RealtimeToOfflineSegmentsTask");
    assertEquals(tables, Lists.newArrayList("pqr_REALTIME"));

    // update taskType for pqr_OFFLINE
    taskTypeMap = new HashMap<>();
    taskTypeMap.put(MinionConstants.MergeRollupTask.TASK_TYPE, new HashMap<>());
    offlineTableConfig1.setTaskConfig(new TableTaskConfig(taskTypeMap));
    stringToJsonNode(
        sendPutRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forUpdateTableConfig(rawTableName1),
            offlineTableConfig1.toJsonString()));

    // list lastModified, taskType
    tables = getTableNames(_createTableUrl + "?taskType=MergeRollupTask");
    assertEquals(tables, Lists.newArrayList("abc_OFFLINE", "pqr_OFFLINE"));
    tables = getTableNames(_createTableUrl + "?taskType=MergeRollupTask&sortAsc=false");
    assertEquals(tables, Lists.newArrayList("pqr_OFFLINE", "abc_OFFLINE"));
    tables = getTableNames(_createTableUrl + "?taskType=MergeRollupTask&sortType=creationTime");
    assertEquals(tables, Lists.newArrayList("pqr_OFFLINE", "abc_OFFLINE"));
  }

  private List<String> getTableNames(String url)
      throws IOException {
    JsonNode tablesJson = stringToJsonNode(sendGetRequest(url)).get("tables");
    return JsonUtils.jsonNodeToObject(tablesJson, new TypeReference<List<String>>() {
    });
  }

  @Test(expectedExceptions = IOException.class)
  public void rebalanceNonExistentTable()
      throws Exception {
    sendPostRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableRebalance(OFFLINE_TABLE_NAME, "realtime"),
        null);
  }

  @Test
  public void rebalanceTableWithoutSegments()
      throws Exception {
    // Create the table
    sendPostRequest(_createTableUrl, _offlineBuilder.build().toJsonString());

    // Rebalance should return status NO_OP
    RebalanceResult rebalanceResult = JsonUtils.stringToObject(sendPostRequest(
            DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableRebalance(OFFLINE_TABLE_NAME, "offline"), null),
        RebalanceResult.class);
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.NO_OP);
  }

  @Test
  public void testDeleteTable()
      throws IOException {
    // Case 1: Create a REALTIME table and delete it directly w/o using query param.
    TableConfig realtimeTableConfig = _realtimeBuilder.setTableName("table0").build();
    String creationResponse = sendPostRequest(_createTableUrl, realtimeTableConfig.toJsonString());
    assertEquals(creationResponse,
        "{\"unrecognizedProperties\":{},\"status\":\"Table table0_REALTIME successfully added\"}");

    // Delete realtime table using REALTIME suffix.
    String deleteResponse = sendDeleteRequest(
        StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables", "table0_REALTIME"));
    assertEquals(deleteResponse, "{\"status\":\"Tables: [table0_REALTIME] deleted\"}");

    // Case 2: Create an offline table and delete it directly w/o using query param.
    TableConfig offlineTableConfig = _offlineBuilder.setTableName("table0").build();
    creationResponse = sendPostRequest(_createTableUrl, offlineTableConfig.toJsonString());
    assertEquals(creationResponse,
        "{\"unrecognizedProperties\":{},\"status\":\"Table table0_OFFLINE successfully added\"}");

    // Delete offline table using OFFLINE suffix.
    deleteResponse =
        sendDeleteRequest(StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables", "table0_OFFLINE"));
    assertEquals(deleteResponse, "{\"status\":\"Tables: [table0_OFFLINE] deleted\"}");

    // Case 3: Create REALTIME and OFFLINE tables and delete both of them.
    TableConfig rtConfig1 = _realtimeBuilder.setTableName("table1").build();
    creationResponse = sendPostRequest(_createTableUrl, rtConfig1.toJsonString());
    assertEquals(creationResponse,
        "{\"unrecognizedProperties\":{},\"status\":\"Table table1_REALTIME successfully added\"}");

    TableConfig offlineConfig1 = _offlineBuilder.setTableName("table1").build();
    creationResponse = sendPostRequest(_createTableUrl, offlineConfig1.toJsonString());
    assertEquals(creationResponse,
        "{\"unrecognizedProperties\":{},\"status\":\"Table table1_OFFLINE successfully added\"}");

    deleteResponse =
        sendDeleteRequest(StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables", "table1"));
    assertEquals(deleteResponse, "{\"status\":\"Tables: [table1_OFFLINE, table1_REALTIME] deleted\"}");

    // Case 4: Create REALTIME and OFFLINE tables and delete the realtime/offline table using query params.
    TableConfig rtConfig2 = _realtimeBuilder.setTableName("table2").build();
    creationResponse = sendPostRequest(_createTableUrl, rtConfig2.toJsonString());
    assertEquals(creationResponse,
        "{\"unrecognizedProperties\":{},\"status\":\"Table table2_REALTIME successfully added\"}");

    TableConfig offlineConfig2 = _offlineBuilder.setTableName("table2").build();
    creationResponse = sendPostRequest(_createTableUrl, offlineConfig2.toJsonString());
    assertEquals(creationResponse,
        "{\"unrecognizedProperties\":{},\"status\":\"Table table2_OFFLINE successfully added\"}");

    // The conflict between param type and table name suffix causes no table being deleted.
    try {
      sendDeleteRequest(
          StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables", "table2_OFFLINE?type=realtime"));
      fail("Deleting a realtime table with OFFLINE suffix.");
    } catch (Exception e) {
      assertTrue(e instanceof IOException);
    }

    deleteResponse = sendDeleteRequest(
        StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables", "table2?type=realtime"));
    assertEquals(deleteResponse, "{\"status\":\"Tables: [table2_REALTIME] deleted\"}");

    deleteResponse = sendDeleteRequest(
        StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables", "table2?type=offline"));
    assertEquals(deleteResponse, "{\"status\":\"Tables: [table2_OFFLINE] deleted\"}");

    // Case 5: Delete a non-existent table and expect a bad request expection.
    try {
      deleteResponse = sendDeleteRequest(
          StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables", "no_such_table_OFFLINE"));
      fail("Deleting a non-existing table should fail.");
    } catch (Exception e) {
      assertTrue(e instanceof IOException);
    }

    // Case 6: Create REALTIME and OFFLINE tables and delete the realtime/offline table using query params and suffixes.
    TableConfig rtConfig3 = _realtimeBuilder.setTableName("table3").build();
    creationResponse = sendPostRequest(_createTableUrl, rtConfig3.toJsonString());
    assertEquals(creationResponse,
        "{\"unrecognizedProperties\":{},\"status\":\"Table table3_REALTIME successfully added\"}");

    TableConfig offlineConfig3 = _offlineBuilder.setTableName("table3").build();
    creationResponse = sendPostRequest(_createTableUrl, offlineConfig3.toJsonString());
    assertEquals(creationResponse,
        "{\"unrecognizedProperties\":{},\"status\":\"Table table3_OFFLINE successfully added\"}");

    deleteResponse = sendDeleteRequest(
        StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables", "table3_REALTIME?type=realtime"));
    assertEquals(deleteResponse, "{\"status\":\"Tables: [table3_REALTIME] deleted\"}");

    deleteResponse = sendDeleteRequest(
        StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables", "table3_OFFLINE?type=offline"));
    assertEquals(deleteResponse, "{\"status\":\"Tables: [table3_OFFLINE] deleted\"}");
  }

  @Test
  public void testDeleteTableAsync()
      throws IOException {
    // Case 1: Create a REALTIME table and delete it directly w/o using query param.
    TableConfig realtimeTableConfig = _realtimeBuilder.setTableName("table0").build();
    String creationResponse = sendPostRequest(_createTableUrl, realtimeTableConfig.toJsonString());
    assertEquals(creationResponse,
        "{\"unrecognizedProperties\":{},\"status\":\"Table table0_REALTIME successfully added\"}");

    // Delete realtime table using REALTIME suffix.
    String deleteResponse = sendDeleteRequest(
        StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables", "table0_REALTIME?async=true"));
//    assertEquals(deleteResponse, "{\"status\":\"Tables: [table0_REALTIME] deleted\"}");
    waitForTableDeletion("table0_REALTIME", deleteResponse);

    // Case 2: Create a OFFLINE table and delete it directly w/o using query param.
    TableConfig offlineTableConfig = _offlineBuilder.setTableName("table0").build();
    creationResponse = sendPostRequest(_createTableUrl, offlineTableConfig.toJsonString());
    assertEquals(creationResponse,
        "{\"unrecognizedProperties\":{},\"status\":\"Table table0_OFFLINE successfully added\"}");

    // Delete offline table using OFFLINE suffix.
    deleteResponse = sendDeleteRequest(
        StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables", "table0_OFFLINE?async=true"));
    waitForTableDeletion("table0_OFFLINE", deleteResponse);

    // Case 3: Create REALTIME and OFFLINE tables and delete both of them.
    TableConfig rtConfig1 = _realtimeBuilder.setTableName("table1").build();
    creationResponse = sendPostRequest(_createTableUrl, rtConfig1.toJsonString());
    assertEquals(creationResponse,
        "{\"unrecognizedProperties\":{},\"status\":\"Table table1_REALTIME successfully added\"}");

    TableConfig offlineConfig1 = _offlineBuilder.setTableName("table1").build();
    creationResponse = sendPostRequest(_createTableUrl, offlineConfig1.toJsonString());
    assertEquals(creationResponse,
        "{\"unrecognizedProperties\":{},\"status\":\"Table table1_OFFLINE successfully added\"}");

    deleteResponse = sendDeleteRequest(
        StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables", "table1?async=true"));
    waitForTableDeletion("table1_OFFLINE", deleteResponse);
    waitForTableDeletion("table1_REALTIME", deleteResponse);

    // Case 4: Create REALTIME and OFFLINE tables and delete the realtime/offline table using query params.
    TableConfig rtConfig2 = _realtimeBuilder.setTableName("table2").build();
    creationResponse = sendPostRequest(_createTableUrl, rtConfig2.toJsonString());
    assertEquals(creationResponse,
        "{\"unrecognizedProperties\":{},\"status\":\"Table table2_REALTIME successfully added\"}");

    TableConfig offlineConfig2 = _offlineBuilder.setTableName("table2").build();
    creationResponse = sendPostRequest(_createTableUrl, offlineConfig2.toJsonString());
    assertEquals(creationResponse,
        "{\"unrecognizedProperties\":{},\"status\":\"Table table2_OFFLINE successfully added\"}");

    // The conflict between param type and table name suffix causes no table being deleted.
    try {
      sendDeleteRequest(StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables",
          "table2_OFFLINE?type=realtime&async=true"));
      fail("Deleting a realtime table with OFFLINE suffix.");
    } catch (Exception e) {
      assertTrue(e instanceof IOException);
    }

    deleteResponse = sendDeleteRequest(
        StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables", "table2?type=realtime&async=true"));
    waitForTableDeletion("table2_REALTIME", deleteResponse);

    deleteResponse = sendDeleteRequest(
        StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables", "table2?type=offline&async=true"));
    waitForTableDeletion("table2_OFFLINE", deleteResponse);

    // Case 5: Delete a non-existent table and expect a bad request expection.
    try {
      deleteResponse = sendDeleteRequest(StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables",
          "no_such_table_OFFLINE?async=true"));
      fail("Deleting a non-existing table should fail.");
    } catch (Exception e) {
      assertTrue(e instanceof IOException);
    }

    // Case 6: Create REALTIME and OFFLINE tables and delete the realtime/offline table using query params and suffixes.
    TableConfig rtConfig3 = _realtimeBuilder.setTableName("table3").build();
    creationResponse = sendPostRequest(_createTableUrl, rtConfig3.toJsonString());
    assertEquals(creationResponse,
        "{\"unrecognizedProperties\":{},\"status\":\"Table table3_REALTIME successfully added\"}");

    TableConfig offlineConfig3 = _offlineBuilder.setTableName("table3").build();
    creationResponse = sendPostRequest(_createTableUrl, offlineConfig3.toJsonString());
    assertEquals(creationResponse,
        "{\"unrecognizedProperties\":{},\"status\":\"Table table3_OFFLINE successfully added\"}");

    deleteResponse = sendDeleteRequest(StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables",
        "table3_REALTIME?type=realtime&async=true"));
    waitForTableDeletion("table3_REALTIME", deleteResponse);

    deleteResponse = sendDeleteRequest(StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables",
        "table3_OFFLINE?type=offline&async=true"));
    waitForTableDeletion("table3_OFFLINE", deleteResponse);
  }

  private boolean isTableDeleted(String tableName, String jobId)
      throws Exception {
    String response = sendGetRequest(
        StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables/deleteTableStatus", jobId));
    JsonNode jsonResponse = JsonUtils.stringToJsonNode(response);
    int segmentsPendingDeletion = jsonResponse.get("segmentsPendingDeletionCount").intValue();
    boolean tableConfigDeleted = jsonResponse.get("tableConfigDeleted").booleanValue();
    return segmentsPendingDeletion == 0 && tableConfigDeleted;
  }

  private void waitForTableDeletion(String tableName, String deleteResponse) {
    int numAttempts = 0;
    while (numAttempts < 10) {
      try {
        JsonNode jsonResponse = JsonUtils.stringToJsonNode(deleteResponse);
        String status = jsonResponse.get("status").textValue();
        JsonNode statusJson = JsonUtils.stringToJsonNode(status);
        String jobId = statusJson.get("deleteTableJobId").textValue();

        if (isTableDeleted(tableName, jobId)) {
          return;
        }
        Thread.sleep(1000);
        numAttempts++;
      } catch (Exception e) {
        fail("Caught exception while waiting for table deletion: " + e.getMessage());
      }
    }
    fail("Table " + tableName + " was not deleted within 10 seconds");
  }

  @Test
  public void testCheckTableState()
      throws IOException {

    // Create a valid REALTIME table
    TableConfig realtimeTableConfig = _realtimeBuilder.setTableName("testTable").build();
    String creationResponse = sendPostRequest(_createTableUrl, realtimeTableConfig.toJsonString());
    assertEquals(creationResponse,
        "{\"unrecognizedProperties\":{},\"status\":\"Table testTable_REALTIME successfully added\"}");

    // Create a valid OFFLINE table
    TableConfig offlineTableConfig = _offlineBuilder.setTableName("testTable").build();
    creationResponse = sendPostRequest(_createTableUrl, offlineTableConfig.toJsonString());
    assertEquals(creationResponse,
        "{\"unrecognizedProperties\":{},\"status\":\"Table testTable_OFFLINE successfully added\"}");

    // Case 1: Check table state with specifying tableType as realtime should return 1 [enabled]
    String realtimeStateResponse = sendGetRequest(
        StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables", "testTable", "state?type=realtime"));
    assertEquals(realtimeStateResponse, "{\"state\":\"enabled\"}");

    // Case 2: Check table state with specifying tableType as offline should return 1 [enabled]
    String offlineStateResponse = sendGetRequest(
        StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables", "testTable", "state?type=offline"));
    assertEquals(offlineStateResponse, "{\"state\":\"enabled\"}");

    // Case 3: Request table state with invalid type should return bad request
    try {
      sendGetRequest(StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables", "testTable",
          "state?type=non_valid_type"));
      fail("Requesting with invalid type should fail");
    } catch (Exception e) {
      // Expected 400 Bad Request
      assertTrue(e.getMessage().contains("Got error status code: 400"));
    }

    // Case 4: Request state for non-existent table should return not found
    boolean notFoundException = false;
    try {
      sendGetRequest(StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables", "table_not_exist",
          "state?type=offline"));
      fail("Requesting state for non-existent table should fail");
    } catch (Exception e) {
      // Expected 404 Not Found
      notFoundException = true;
    }

    assertTrue(notFoundException);
  }

  @Test
  public void testValidate()
      throws IOException {
    String tableName = "verificationTest";
    // Create a dummy schema
    DEFAULT_INSTANCE.addDummySchema(tableName);

    // Create a valid OFFLINE table config
    TableConfig offlineTableConfig =
        _offlineBuilder.setTableName(tableName).setInvertedIndexColumns(Arrays.asList("dimA", "dimB")).build();

    try {
      sendPostRequest(StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables", "validate"),
          offlineTableConfig.toJsonString());
    } catch (IOException e) {
      fail("Valid table config with existing schema validation should succeed.");
    }

    // Create an invalid OFFLINE table config
    offlineTableConfig =
        _offlineBuilder.setTableName(tableName).setInvertedIndexColumns(Arrays.asList("invalidColA", "invalidColB"))
            .build();

    try {
      sendPostRequest(StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables", "validate"),
          offlineTableConfig.toJsonString());
      fail("Validation of an invalid table config should fail.");
    } catch (IOException e) {
      // Expected 400 Bad Request
      assertTrue(e.getMessage().contains("Got error status code: 400"));
    }
  }

  @Test
  public void testValidateTableAndSchema()
      throws IOException {
    String tableName = "verificationTestTableAndSchema";
    // Create a dummy schema
    Schema schema = DEFAULT_INSTANCE.createDummySchema(tableName);

    // Create a valid OFFLINE table config
    TableConfig offlineTableConfig =
        _offlineBuilder.setTableName(tableName).setInvertedIndexColumns(Arrays.asList("dimA", "dimB")).build();
    TableAndSchemaConfig tableAndSchemaConfig = new TableAndSchemaConfig(offlineTableConfig, schema);

    try {
      sendPostRequest(
          StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables", "validateTableAndSchema"),
          tableAndSchemaConfig.toJsonString());
    } catch (IOException e) {
      fail("Valid table config and schema validation should succeed.");
    }

    // Add a dummy schema to Pinot
    DEFAULT_INSTANCE.addDummySchema(tableName);
    tableAndSchemaConfig = new TableAndSchemaConfig(offlineTableConfig, null);
    try {
      sendPostRequest(
          StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables", "validateTableAndSchema"),
          tableAndSchemaConfig.toJsonString());
    } catch (IOException e) {
      fail("Valid table config and existing schema validation should succeed.");
    }

    // Create an invalid table config
    offlineTableConfig =
        _offlineBuilder.setTableName(tableName).setInvertedIndexColumns(Arrays.asList("invalidColA", "invalidColB"))
            .build();
    tableAndSchemaConfig = new TableAndSchemaConfig(offlineTableConfig, schema);
    try {
      sendPostRequest(
          StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables", "validateTableAndSchema"),
          tableAndSchemaConfig.toJsonString());
      fail("Validation of an invalid table config and schema should fail.");
    } catch (IOException e) {
      // Expected
      assertTrue(e.getMessage().contains("Got error status code: 400"));
    }
  }

  @Test
  public void testUnrecognizedProperties()
      throws IOException {
    // Create an OFFLINE table with a valid name but with unrecognizedProperties which should succeed
    // Should have unrecognizedProperties set correctly
    String tableName = "valid_table_name_extra_props";
    TableConfig offlineTableConfig = _realtimeBuilder.setTableName("valid_table_name_extra_props").build();
    JsonNode jsonNode = JsonUtils.objectToJsonNode(offlineTableConfig);
    ((ObjectNode) jsonNode).put("illegalKey1", 1);
    ObjectNode internalObj = JsonUtils.newObjectNode();
    internalObj.put("illegalKey3", 2);
    ((ObjectNode) jsonNode).put("illegalKey2", internalObj);

    String creationResponse = sendPostRequest(_createTableUrl, jsonNode.toString());
    assertEquals(creationResponse,
        "{\"unrecognizedProperties\":{\"/illegalKey1\":1,\"/illegalKey2/illegalKey3\":2},\"status\":\"Table "
            + "valid_table_name_extra_props_REALTIME successfully added\"}");

    // update table with unrecognizedProperties
    String updateResponse =
        sendPutRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forUpdateTableConfig(tableName),
            jsonNode.toString());
    assertEquals(updateResponse,
        "{\"unrecognizedProperties\":{\"/illegalKey1\":1,\"/illegalKey2/illegalKey3\":2},\"status\":\"Table "
            + "config updated for valid_table_name_extra_props\"}");

    // validate table with unrecognizedProperties
    String validationResponse =
        sendPostRequest(StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables", "validate"),
            jsonNode.toString());
    assertTrue(validationResponse.contains(
        "unrecognizedProperties\":{\"/illegalKey1\":1," + "\"/illegalKey2/illegalKey3\":2}}"));
  }

  @AfterClass
  public void tearDown() {
    DEFAULT_INSTANCE.cleanup();
  }
}
