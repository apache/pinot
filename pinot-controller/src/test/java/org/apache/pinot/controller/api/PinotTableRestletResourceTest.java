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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.controller.ControllerTestUtils;
import org.apache.pinot.controller.api.resources.TableAndSchemaConfig;
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
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Test for table creation
 */
public class PinotTableRestletResourceTest {
  private static final int NUM_BROKER_INSTANCES = 2;
  // NOTE: to add HLC realtime table, number of Server instances must be multiple of number of replicas

  private static final String OFFLINE_TABLE_NAME = "testOfflineTable";
  private static final String REALTIME_TABLE_NAME = "testRealtimeTable";
  private final TableConfigBuilder _offlineBuilder = new TableConfigBuilder(TableType.OFFLINE);
  private final TableConfigBuilder _realtimeBuilder = new TableConfigBuilder(TableType.REALTIME);
  private String _createTableUrl;

  @BeforeClass
  public void setUp()
      throws Exception {
    ControllerTestUtils.setupClusterAndValidate();

    _createTableUrl = ControllerTestUtils.getControllerRequestURLBuilder().forTableCreate();
    _offlineBuilder.setTableName(OFFLINE_TABLE_NAME).setTimeColumnName("timeColumn").setTimeType("DAYS")
        .setRetentionTimeUnit("DAYS").setRetentionTimeValue("5");

    // add schema for realtime table
    ControllerTestUtils.addDummySchema(REALTIME_TABLE_NAME);
    StreamConfig streamConfig = FakeStreamConfigUtils.getDefaultHighLevelStreamConfigs();
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
      ControllerTestUtils.sendPostRequest(_createTableUrl, offlineTableConfigJson.toString());
      Assert.fail("Creation of an OFFLINE table with two underscores in the table name does not fail");
    } catch (IOException e) {
      // Expected 400 Bad Request
      Assert.assertTrue(e.getMessage().startsWith("Server returned HTTP response code: 400"));
    }

    offlineTableConfig = _offlineBuilder.build();
    offlineTableConfigJson = (ObjectNode) offlineTableConfig.toJsonNode();
    offlineTableConfigJson.put(TableConfig.TABLE_NAME_KEY, "bad.table.with.dot");
    try {
      ControllerTestUtils.sendPostRequest(_createTableUrl, offlineTableConfigJson.toString());
      Assert.fail("Creation of an OFFLINE table with dot in the table name does not fail");
    } catch (IOException e) {
      // Expected 400 Bad Request
      Assert.assertTrue(e.getMessage().startsWith("Server returned HTTP response code: 400"));
    }

    // Create an OFFLINE table with a valid name which should succeed
    offlineTableConfig = _offlineBuilder.setTableName("valid_table_name").build();
    String offlineTableConfigString = offlineTableConfig.toJsonString();
    ControllerTestUtils.sendPostRequest(_createTableUrl, offlineTableConfigString);

    // Create an OFFLINE table that already exists which should fail
    try {
      ControllerTestUtils.sendPostRequest(_createTableUrl, offlineTableConfigString);
      Assert.fail("Creation of an existing OFFLINE table does not fail");
    } catch (IOException e) {
      // Expected 409 Conflict
      Assert.assertTrue(e.getMessage().startsWith("Server returned HTTP response code: 409"));
    }

    // Create an OFFLINE table with invalid replication config
    offlineTableConfig.getValidationConfig().setReplication("abc");
    try {
      ControllerTestUtils.sendPostRequest(_createTableUrl, offlineTableConfig.toJsonString());
      Assert.fail("Creation of an invalid OFFLINE table does not fail");
    } catch (IOException e) {
      // Expected 400 Bad Request
      Assert.assertTrue(e.getMessage().startsWith("Server returned HTTP response code: 400"));
    }

    // Create a REALTIME table with an invalid name which should fail
    // NOTE: Set bad table name inside table config builder is not allowed, so have to set in json node
    TableConfig realtimeTableConfig = _realtimeBuilder.build();
    ObjectNode realtimeTableConfigJson = (ObjectNode) realtimeTableConfig.toJsonNode();
    realtimeTableConfigJson.put(TableConfig.TABLE_NAME_KEY, "bad__table__name");
    try {
      ControllerTestUtils.sendPostRequest(_createTableUrl, realtimeTableConfigJson.toString());
      Assert.fail("Creation of a REALTIME table with two underscores in the table name does not fail");
    } catch (IOException e) {
      // Expected 400 Bad Request
      Assert.assertTrue(e.getMessage().startsWith("Server returned HTTP response code: 400"));
    }

    // Creating a REALTIME table without a valid schema should fail
    realtimeTableConfig = _realtimeBuilder.setTableName("noSchema").setSchemaName("noSchema").build();
    try {
      ControllerTestUtils.sendPostRequest(_createTableUrl, realtimeTableConfig.toJsonString());
      Assert.fail("Creation of a REALTIME table without a valid schema does not fail");
    } catch (IOException e) {
      // Expected 400 Bad Request
      Assert.assertTrue(e.getMessage().startsWith("Server returned HTTP response code: 400"));
    }

    // Creating a REALTIME table with a different schema name in the config should succeed (backwards compatibility mode)
    realtimeTableConfig = _realtimeBuilder.setSchemaName(REALTIME_TABLE_NAME).build();
    ControllerTestUtils.sendPostRequest(_createTableUrl, realtimeTableConfig.toJsonString());

    // Create a REALTIME table with the invalid time column name should fail
    realtimeTableConfig =
        _realtimeBuilder.setTableName(REALTIME_TABLE_NAME).setTimeColumnName("invalidTimeColumn").build();
    try {
      ControllerTestUtils.sendPostRequest(_createTableUrl, realtimeTableConfig.toJsonString());
      Assert.fail("Creation of a REALTIME table with a invalid time column name does not fail");
    } catch (IOException e) {
      // Expected 400 Bad Request
      Assert.assertTrue(e.getMessage().startsWith("Server returned HTTP response code: 400"));
    }

    // Create a REALTIME table with a valid name and schema which should succeed
    realtimeTableConfig = _realtimeBuilder.setTableName(REALTIME_TABLE_NAME).setTimeColumnName("timeColumn").build();
    String realtimeTableConfigString = realtimeTableConfig.toJsonString();
    ControllerTestUtils.sendPostRequest(_createTableUrl, realtimeTableConfigString);

    // TODO: check whether we should allow POST request to create REALTIME table that already exists
    // Create a REALTIME table that already exists which should succeed
    ControllerTestUtils.sendPostRequest(_createTableUrl, realtimeTableConfigString);
  }

  @Test
  public void testTableCronSchedule() {
    String rawTableName = "test_table_cron_schedule";
    // Failed to create a table
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(rawTableName).setTaskConfig(
        new TableTaskConfig(ImmutableMap.of(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE,
            ImmutableMap.of(PinotTaskManager.SCHEDULE_KEY, "* * * * * * *")))).build();
    try {
      ControllerTestUtils.sendPostRequest(_createTableUrl, tableConfig.toJsonString());
      Assert.fail("Creation of an OFFLINE table with an invalid cron expression does not fail");
    } catch (IOException e) {
      // Expected 400 Bad Request
      Assert.assertTrue(e.getMessage().startsWith("Server returned HTTP response code: 400"));
    }

    // Succeed to create a table
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(rawTableName).setTaskConfig(
        new TableTaskConfig(ImmutableMap.of(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE,
            ImmutableMap.of(PinotTaskManager.SCHEDULE_KEY, "0 */10 * ? * * *")))).build();
    try {
      String response = ControllerTestUtils.sendPostRequest(_createTableUrl, tableConfig.toJsonString());
      Assert.assertEquals(response, "{\"status\":\"Table test_table_cron_schedule_OFFLINE succesfully added\"}");
    } catch (IOException e) {
      // Expected 400 Bad Request
      Assert.fail("This is a valid table config with cron schedule");
    }

    // Failed to update the table
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(rawTableName).setTaskConfig(
        new TableTaskConfig(ImmutableMap.of(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE,
            ImmutableMap.of(PinotTaskManager.SCHEDULE_KEY, "5 5 5 5 5 5 5")))).build();
    try {
      ControllerTestUtils
          .sendPutRequest(ControllerTestUtils.getControllerRequestURLBuilder().forUpdateTableConfig(rawTableName),
              tableConfig.toJsonString());
      Assert.fail("Update of an OFFLINE table with an invalid cron expression does not fail");
    } catch (IOException e) {
      // Expected 400 Bad Request
      Assert.assertTrue(e.getMessage().startsWith("Server returned HTTP response code: 400"));
    }
  }

  @Test
  public void testTableMinReplication()
      throws Exception {
    testTableMinReplicationInternal("minReplicationOne", 1);
    testTableMinReplicationInternal("minReplicationTwo", ControllerTestUtils.NUM_SERVER_INSTANCES);
  }

  private void testTableMinReplicationInternal(String tableName, int tableReplication)
      throws Exception {
    String tableJSONConfigString =
        _offlineBuilder.setTableName(tableName).setNumReplicas(tableReplication).build().toJsonString();
    ControllerTestUtils.sendPostRequest(_createTableUrl, tableJSONConfigString);
    // table creation should succeed
    TableConfig tableConfig = getTableConfig(tableName, "OFFLINE");
    Assert.assertEquals(tableConfig.getValidationConfig().getReplicationNumber(),
        Math.max(tableReplication, ControllerTestUtils.MIN_NUM_REPLICAS));

    ControllerTestUtils.addDummySchema(tableName);
    tableJSONConfigString =
        _realtimeBuilder.setTableName(tableName).setNumReplicas(tableReplication).build().toJsonString();
    ControllerTestUtils.sendPostRequest(_createTableUrl, tableJSONConfigString);
    tableConfig = getTableConfig(tableName, "REALTIME");
    Assert.assertEquals(tableConfig.getValidationConfig().getReplicationNumber(),
        Math.max(tableReplication, ControllerTestUtils.MIN_NUM_REPLICAS));
    // This test can only be done via integration test
//    int replicasPerPartition = Integer.valueOf(tableConfig.getValidationConfig().getReplicasPerPartition());
//    Assert.assertEquals(replicasPerPartition, Math.max(tableReplication, TABLE_MIN_REPLICATION));

    ControllerTestUtils.getHelixResourceManager().deleteOfflineTable(tableName);
    ControllerTestUtils.getHelixResourceManager().deleteRealtimeTable(tableName);
  }

  @Test
  public void testDimTableStorageQuotaConstraints()
      throws Exception {
    // Controller assigns default quota if none provided
    String tableName = "myDimTable_basic";
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName(tableName).addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
            .setPrimaryKeyColumns(Lists.newArrayList("myCol")).build();
    ControllerTestUtils.addSchema(schema);

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).setIsDimTable(true).build();
    ControllerTestUtils.sendPostRequest(_createTableUrl, tableConfig.toJsonString());
    tableConfig = getTableConfig(tableName, "OFFLINE");
    Assert.assertEquals(tableConfig.getQuotaConfig().getStorage(),
        ControllerTestUtils.getControllerConfig().getDimTableMaxSize());

    // Controller throws exception if quote exceed configured max value
    tableName = "myDimTable_broken";
    schema =
        new Schema.SchemaBuilder().setSchemaName(tableName).addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
            .setPrimaryKeyColumns(Lists.newArrayList("myCol")).build();
    ControllerTestUtils.addSchema(schema);
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).setIsDimTable(true)
        .setQuotaConfig(new QuotaConfig("500G", null)).build();
    try {
      ControllerTestUtils.sendPostRequest(_createTableUrl, tableConfig.toJsonString());
      Assert.fail("Creation of a DIMENSION table with larger than allowed storage quota should fail");
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().startsWith("Server returned HTTP response code: 400"));
    }

    // Successful creation with proper quota
    tableName = "myDimTable_good";
    schema =
        new Schema.SchemaBuilder().setSchemaName(tableName).addSingleValueDimension("myCol", FieldSpec.DataType.STRING)
            .setPrimaryKeyColumns(Lists.newArrayList("myCol")).build();
    ControllerTestUtils.addSchema(schema);
    String goodQuota = "100M";
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).setIsDimTable(true)
        .setQuotaConfig(new QuotaConfig(goodQuota, null)).build();
    ControllerTestUtils.sendPostRequest(_createTableUrl, tableConfig.toJsonString());
    tableConfig = getTableConfig(tableName, "OFFLINE");
    Assert.assertEquals(tableConfig.getQuotaConfig().getStorage(), goodQuota);
  }

  private TableConfig getTableConfig(String tableName, String tableType)
      throws Exception {
    String tableConfigString =
        ControllerTestUtils.sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableGet(tableName));
    return JsonUtils.jsonNodeToObject(JsonUtils.stringToJsonNode(tableConfigString).get(tableType), TableConfig.class);
  }

  @Test
  public void testUpdateTableConfig()
      throws Exception {
    String tableName = "updateTC";
    String tableConfigString = _offlineBuilder.setTableName(tableName).setNumReplicas(2).build().toJsonString();
    ControllerTestUtils.sendPostRequest(_createTableUrl, tableConfigString);
    // table creation should succeed
    TableConfig tableConfig = getTableConfig(tableName, "OFFLINE");
    Assert.assertEquals(tableConfig.getValidationConfig().getRetentionTimeValue(), "5");
    Assert.assertEquals(tableConfig.getValidationConfig().getRetentionTimeUnit(), "DAYS");

    tableConfig.getValidationConfig().setRetentionTimeUnit("HOURS");
    tableConfig.getValidationConfig().setRetentionTimeValue("10");

    JsonNode jsonResponse = JsonUtils.stringToJsonNode(ControllerTestUtils
        .sendPutRequest(ControllerTestUtils.getControllerRequestURLBuilder().forUpdateTableConfig(tableName),
            tableConfig.toJsonString()));
    Assert.assertTrue(jsonResponse.has("status"));

    TableConfig modifiedConfig = getTableConfig(tableName, "OFFLINE");
    Assert.assertEquals(modifiedConfig.getValidationConfig().getRetentionTimeUnit(), "HOURS");
    Assert.assertEquals(modifiedConfig.getValidationConfig().getRetentionTimeValue(), "10");

    // Realtime
    ControllerTestUtils.addDummySchema(tableName);
    tableConfigString = _realtimeBuilder.setTableName(tableName).setNumReplicas(2).build().toJsonString();
    ControllerTestUtils.sendPostRequest(_createTableUrl, tableConfigString);
    tableConfig = getTableConfig(tableName, "REALTIME");
    Assert.assertEquals(tableConfig.getValidationConfig().getRetentionTimeValue(), "5");
    Assert.assertEquals(tableConfig.getValidationConfig().getRetentionTimeUnit(), "DAYS");
    Assert.assertNull(tableConfig.getQuotaConfig());

    QuotaConfig quota = new QuotaConfig("10G", "100.0");
    tableConfig.setQuotaConfig(quota);
    ControllerTestUtils
        .sendPutRequest(ControllerTestUtils.getControllerRequestURLBuilder().forUpdateTableConfig(tableName),
            tableConfig.toJsonString());
    modifiedConfig = getTableConfig(tableName, "REALTIME");
    Assert.assertNotNull(modifiedConfig.getQuotaConfig());
    Assert.assertEquals(modifiedConfig.getQuotaConfig().getStorage(), "10G");
    Assert.assertEquals(modifiedConfig.getQuotaConfig().getMaxQueriesPerSecond(), "100.0");

    boolean notFoundException = false;
    try {
      // table does not exist
      ObjectNode tableConfigJson = (ObjectNode) tableConfig.toJsonNode();
      tableConfigJson.put(TableConfig.TABLE_NAME_KEY, "noSuchTable_REALTIME");
      ControllerTestUtils
          .sendPutRequest(ControllerTestUtils.getControllerRequestURLBuilder().forUpdateTableConfig("noSuchTable"),
              tableConfigJson.toString());
    } catch (Exception e) {
      Assert.assertTrue(e instanceof FileNotFoundException);
      notFoundException = true;
    }
    Assert.assertTrue(notFoundException);
  }

  @Test(expectedExceptions = FileNotFoundException.class)
  public void rebalanceNonExistentTable()
      throws Exception {
    ControllerTestUtils.sendPostRequest(
        ControllerTestUtils.getControllerRequestURLBuilder().forTableRebalance(OFFLINE_TABLE_NAME, "realtime"), null);
  }

  @Test
  public void rebalanceTableWithoutSegments()
      throws Exception {
    // Create the table
    ControllerTestUtils.sendPostRequest(_createTableUrl, _offlineBuilder.build().toJsonString());

    // Rebalance should return status NO_OP
    RebalanceResult rebalanceResult = JsonUtils.stringToObject(ControllerTestUtils.sendPostRequest(
        ControllerTestUtils.getControllerRequestURLBuilder().forTableRebalance(OFFLINE_TABLE_NAME, "offline"), null),
        RebalanceResult.class);
    Assert.assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.NO_OP);
  }

  @Test
  public void testDeleteTable()
      throws IOException {
    // Case 1: Create a REALTIME table and delete it directly w/o using query param.
    TableConfig realtimeTableConfig = _realtimeBuilder.setTableName("table0").build();
    String creationResponse = ControllerTestUtils.sendPostRequest(_createTableUrl, realtimeTableConfig.toJsonString());
    Assert.assertEquals(creationResponse, "{\"status\":\"Table table0_REALTIME succesfully added\"}");

    // Delete realtime table using REALTIME suffix.
    String deleteResponse = ControllerTestUtils.sendDeleteRequest(
        StringUtil.join("/", ControllerTestUtils.getControllerBaseApiUrl(), "tables", "table0_REALTIME"));
    Assert.assertEquals(deleteResponse, "{\"status\":\"Tables: [table0_REALTIME] deleted\"}");

    // Case 2: Create an offline table and delete it directly w/o using query param.
    TableConfig offlineTableConfig = _offlineBuilder.setTableName("table0").build();
    creationResponse = ControllerTestUtils.sendPostRequest(_createTableUrl, offlineTableConfig.toJsonString());
    Assert.assertEquals(creationResponse, "{\"status\":\"Table table0_OFFLINE succesfully added\"}");

    // Delete offline table using OFFLINE suffix.
    deleteResponse = ControllerTestUtils.sendDeleteRequest(
        StringUtil.join("/", ControllerTestUtils.getControllerBaseApiUrl(), "tables", "table0_OFFLINE"));
    Assert.assertEquals(deleteResponse, "{\"status\":\"Tables: [table0_OFFLINE] deleted\"}");

    // Case 3: Create REALTIME and OFFLINE tables and delete both of them.
    TableConfig rtConfig1 = _realtimeBuilder.setTableName("table1").build();
    creationResponse = ControllerTestUtils.sendPostRequest(_createTableUrl, rtConfig1.toJsonString());
    Assert.assertEquals(creationResponse, "{\"status\":\"Table table1_REALTIME succesfully added\"}");

    TableConfig offlineConfig1 = _offlineBuilder.setTableName("table1").build();
    creationResponse = ControllerTestUtils.sendPostRequest(_createTableUrl, offlineConfig1.toJsonString());
    Assert.assertEquals(creationResponse, "{\"status\":\"Table table1_OFFLINE succesfully added\"}");

    deleteResponse = ControllerTestUtils
        .sendDeleteRequest(StringUtil.join("/", ControllerTestUtils.getControllerBaseApiUrl(), "tables", "table1"));
    Assert.assertEquals(deleteResponse, "{\"status\":\"Tables: [table1_OFFLINE, table1_REALTIME] deleted\"}");

    // Case 4: Create REALTIME and OFFLINE tables and delete the realtime/offline table using query params.
    TableConfig rtConfig2 = _realtimeBuilder.setTableName("table2").build();
    creationResponse = ControllerTestUtils.sendPostRequest(_createTableUrl, rtConfig2.toJsonString());
    Assert.assertEquals(creationResponse, "{\"status\":\"Table table2_REALTIME succesfully added\"}");

    TableConfig offlineConfig2 = _offlineBuilder.setTableName("table2").build();
    creationResponse = ControllerTestUtils.sendPostRequest(_createTableUrl, offlineConfig2.toJsonString());
    Assert.assertEquals(creationResponse, "{\"status\":\"Table table2_OFFLINE succesfully added\"}");

    // The conflict between param type and table name suffix causes no table being deleted.
    try {
      ControllerTestUtils.sendDeleteRequest(StringUtil
          .join("/", ControllerTestUtils.getControllerBaseApiUrl(), "tables", "table2_OFFLINE?type=realtime"));
      Assert.fail("Deleting a realtime table with OFFLINE suffix.");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IOException);
    }

    deleteResponse = ControllerTestUtils.sendDeleteRequest(
        StringUtil.join("/", ControllerTestUtils.getControllerBaseApiUrl(), "tables", "table2?type=realtime"));
    Assert.assertEquals(deleteResponse, "{\"status\":\"Tables: [table2_REALTIME] deleted\"}");

    deleteResponse = ControllerTestUtils.sendDeleteRequest(
        StringUtil.join("/", ControllerTestUtils.getControllerBaseApiUrl(), "tables", "table2?type=offline"));
    Assert.assertEquals(deleteResponse, "{\"status\":\"Tables: [table2_OFFLINE] deleted\"}");

    // Case 5: Delete a non-existent table and expect a bad request expection.
    try {
      deleteResponse = ControllerTestUtils.sendDeleteRequest(
          StringUtil.join("/", ControllerTestUtils.getControllerBaseApiUrl(), "tables", "no_such_table_OFFLINE"));
      Assert.fail("Deleting a non-existing table should fail.");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IOException);
    }

    // Case 6: Create REALTIME and OFFLINE tables and delete the realtime/offline table using query params and suffixes.
    TableConfig rtConfig3 = _realtimeBuilder.setTableName("table3").build();
    creationResponse = ControllerTestUtils.sendPostRequest(_createTableUrl, rtConfig3.toJsonString());
    Assert.assertEquals(creationResponse, "{\"status\":\"Table table3_REALTIME succesfully added\"}");

    TableConfig offlineConfig3 = _offlineBuilder.setTableName("table3").build();
    creationResponse = ControllerTestUtils.sendPostRequest(_createTableUrl, offlineConfig3.toJsonString());
    Assert.assertEquals(creationResponse, "{\"status\":\"Table table3_OFFLINE succesfully added\"}");

    deleteResponse = ControllerTestUtils.sendDeleteRequest(
        StringUtil.join("/", ControllerTestUtils.getControllerBaseApiUrl(), "tables", "table3_REALTIME?type=realtime"));
    Assert.assertEquals(deleteResponse, "{\"status\":\"Tables: [table3_REALTIME] deleted\"}");

    deleteResponse = ControllerTestUtils.sendDeleteRequest(
        StringUtil.join("/", ControllerTestUtils.getControllerBaseApiUrl(), "tables", "table3_OFFLINE?type=offline"));
    Assert.assertEquals(deleteResponse, "{\"status\":\"Tables: [table3_OFFLINE] deleted\"}");
  }

  @Test
  public void testCheckTableState()
      throws IOException {

    // Create a valid REALTIME table
    TableConfig realtimeTableConfig = _realtimeBuilder.setTableName("testTable").build();
    String creationResponse = ControllerTestUtils.sendPostRequest(_createTableUrl, realtimeTableConfig.toJsonString());
    Assert.assertEquals(creationResponse, "{\"status\":\"Table testTable_REALTIME succesfully added\"}");

    // Create a valid OFFLINE table
    TableConfig offlineTableConfig = _offlineBuilder.setTableName("testTable").build();
    creationResponse = ControllerTestUtils.sendPostRequest(_createTableUrl, offlineTableConfig.toJsonString());
    Assert.assertEquals(creationResponse, "{\"status\":\"Table testTable_OFFLINE succesfully added\"}");

    // Case 1: Check table state with specifying tableType as realtime should return 1 [enabled]
    String realtimeStateResponse = ControllerTestUtils.sendGetRequest(StringUtil
        .join("/", ControllerTestUtils.getControllerBaseApiUrl(), "tables", "testTable", "state?type=realtime"));
    Assert.assertEquals(realtimeStateResponse, "{\"state\":\"enabled\"}");

    // Case 2: Check table state with specifying tableType as offline should return 1 [enabled]
    String offlineStateResponse = ControllerTestUtils.sendGetRequest(StringUtil
        .join("/", ControllerTestUtils.getControllerBaseApiUrl(), "tables", "testTable", "state?type=offline"));
    Assert.assertEquals(offlineStateResponse, "{\"state\":\"enabled\"}");

    // Case 3: Request table state with invalid type should return bad request
    try {
      ControllerTestUtils.sendGetRequest(StringUtil
          .join("/", ControllerTestUtils.getControllerBaseApiUrl(), "tables", "testTable",
              "state?type=non_valid_type"));
      Assert.fail("Requesting with invalid type should fail");
    } catch (Exception e) {
      // Expected 400 Bad Request
      Assert.assertTrue(e.getMessage().startsWith("Server returned HTTP response code: 400"));
    }

    // Case 4: Request state for non-existent table should return not found
    boolean notFoundException = false;
    try {
      ControllerTestUtils.sendGetRequest(StringUtil
          .join("/", ControllerTestUtils.getControllerBaseApiUrl(), "tables", "table_not_exist", "state?type=offline"));
      Assert.fail("Requesting state for non-existent table should fail");
    } catch (Exception e) {
      // Expected 404 Not Found
      notFoundException = true;
    }

    Assert.assertTrue(notFoundException);
  }

  @Test
  public void testValidate()
      throws IOException {
    String tableName = "verificationTest";
    // Create a dummy schema
    ControllerTestUtils.addDummySchema(tableName);

    // Create a valid OFFLINE table config
    TableConfig offlineTableConfig =
        _offlineBuilder.setTableName(tableName).setInvertedIndexColumns(Arrays.asList("dimA", "dimB")).build();

    try {
      ControllerTestUtils
          .sendPostRequest(StringUtil.join("/", ControllerTestUtils.getControllerBaseApiUrl(), "tables", "validate"),
              offlineTableConfig.toJsonString());
    } catch (IOException e) {
      Assert.fail("Valid table config with existing schema validation should succeed.");
    }

    // Create an invalid OFFLINE table config
    offlineTableConfig =
        _offlineBuilder.setTableName(tableName).setInvertedIndexColumns(Arrays.asList("invalidColA", "invalidColB"))
            .build();

    try {
      ControllerTestUtils
          .sendPostRequest(StringUtil.join("/", ControllerTestUtils.getControllerBaseApiUrl(), "tables", "validate"),
              offlineTableConfig.toJsonString());
      Assert.fail("Validation of an invalid table config should fail.");
    } catch (IOException e) {
      // Expected 400 Bad Request
      Assert.assertTrue(e.getMessage().startsWith("Server returned HTTP response code: 400"));
    }
  }

  @Test
  public void testValidateTableAndSchema()
      throws IOException {
    String tableName = "verificationTestTableAndSchema";
    // Create a dummy schema
    Schema schema = ControllerTestUtils.createDummySchema(tableName);

    // Create a valid OFFLINE table config
    TableConfig offlineTableConfig =
        _offlineBuilder.setTableName(tableName).setInvertedIndexColumns(Arrays.asList("dimA", "dimB")).build();
    TableAndSchemaConfig tableAndSchemaConfig = new TableAndSchemaConfig(offlineTableConfig, schema);
    Map<String, String> headerMap = new HashMap<>();
    headerMap.put("Content-Type", "application/json");

    try {
      ControllerTestUtils.sendPostRequest(
          StringUtil.join("/", ControllerTestUtils.getControllerBaseApiUrl(), "tables", "validateTableAndSchema"),
          tableAndSchemaConfig.toJsonString(), headerMap);
    } catch (IOException e) {
      Assert.fail("Valid table config and schema validation should succeed.");
    }

    // Add a dummy schema to Pinot
    ControllerTestUtils.addDummySchema(tableName);
    tableAndSchemaConfig = new TableAndSchemaConfig(offlineTableConfig, null);
    try {
      ControllerTestUtils.sendPostRequest(
          StringUtil.join("/", ControllerTestUtils.getControllerBaseApiUrl(), "tables", "validateTableAndSchema"),
          tableAndSchemaConfig.toJsonString(), headerMap);
    } catch (IOException e) {
      Assert.fail("Valid table config and existing schema validation should succeed.");
    }

    // Create an invalid table config
    offlineTableConfig =
        _offlineBuilder.setTableName(tableName).setInvertedIndexColumns(Arrays.asList("invalidColA", "invalidColB"))
            .build();
    tableAndSchemaConfig = new TableAndSchemaConfig(offlineTableConfig, schema);
    try {
      ControllerTestUtils.sendPostRequest(
          StringUtil.join("/", ControllerTestUtils.getControllerBaseApiUrl(), "tables", "validateTableAndSchema"),
          tableAndSchemaConfig.toJsonString(), headerMap);
      Assert.fail("Validation of an invalid table config and schema should fail.");
    } catch (IOException e) {
      // Expected
      Assert.assertTrue(e.getMessage().startsWith("Server returned HTTP response code: 400"));
    }
  }

  @AfterClass
  public void tearDown() {
    ControllerTestUtils.cleanup();
  }
}
