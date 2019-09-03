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
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.pinot.common.config.QuotaConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.utils.CommonConstants.Helix.TableType;
import org.apache.pinot.common.utils.JsonUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceResult;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.core.realtime.stream.StreamConfig;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Test for table creation
 */
public class PinotTableRestletResourceTest extends ControllerTest {
  private static final int MIN_NUM_REPLICAS = 3;
  private static final int NUM_BROKER_INSTANCES = 2;
  // NOTE: to add HLC realtime table, number of Server instances must be multiple of number of replicas
  private static final int NUM_SERVER_INSTANCES = 6;

  private static final String OFFLINE_TABLE_NAME = "testOfflineTable";
  private static final String REALTIME_TABLE_NAME = "testRealtimeTable";
  private final TableConfig.Builder _offlineBuilder = new TableConfig.Builder(TableType.OFFLINE);
  private final TableConfig.Builder _realtimeBuilder = new TableConfig.Builder(TableType.REALTIME);
  private String _createTableUrl;

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    ControllerConf config = getDefaultControllerConfiguration();
    config.setTableMinReplicas(MIN_NUM_REPLICAS);
    startController(config);
    _createTableUrl = _controllerRequestURLBuilder.forTableCreate();

    addFakeBrokerInstancesToAutoJoinHelixCluster(NUM_BROKER_INSTANCES, true);
    addFakeServerInstancesToAutoJoinHelixCluster(NUM_SERVER_INSTANCES, true);

    _offlineBuilder.setTableName(OFFLINE_TABLE_NAME).setTimeColumnName("timeColumn").setTimeType("DAYS")
        .setRetentionTimeUnit("DAYS").setRetentionTimeValue("5");

    // add schema for realtime table
    addDummySchema(REALTIME_TABLE_NAME);
    StreamConfig streamConfig = FakeStreamConfigUtils.getDefaultHighLevelStreamConfigs();
    _realtimeBuilder.setTableName(REALTIME_TABLE_NAME).setTimeColumnName("timeColumn").setTimeType("DAYS")
        .setRetentionTimeUnit("DAYS").setRetentionTimeValue("5").setSchemaName(REALTIME_TABLE_NAME)
        .setStreamConfigs(streamConfig.getStreamConfigsMap());
  }

  @Test
  public void testCreateTable()
      throws Exception {
    // Create an OFFLINE table with an invalid name which should fail
    // NOTE: Set bad table name inside table config builder is not allowed, so have to explicitly set in table config
    TableConfig offlineTableConfig = _offlineBuilder.build();
    offlineTableConfig.setTableName("bad__table__name");
    try {
      sendPostRequest(_createTableUrl, offlineTableConfig.toJsonConfigString());
      Assert.fail("Creation of an OFFLINE table with two underscores in the table name does not fail");
    } catch (IOException e) {
      // Expected 400 Bad Request
      Assert.assertTrue(e.getMessage().startsWith("Server returned HTTP response code: 400"));
    }

    // Create an OFFLINE table with a valid name which should succeed
    offlineTableConfig.setTableName("valid_table_name");
    String offlineTableJSONConfigString = offlineTableConfig.toJsonConfigString();
    sendPostRequest(_createTableUrl, offlineTableJSONConfigString);

    // Create an OFFLINE table that already exists which should fail
    try {
      sendPostRequest(_createTableUrl, offlineTableJSONConfigString);
      Assert.fail("Creation of an existing OFFLINE table does not fail");
    } catch (IOException e) {
      // Expected 409 Conflict
      Assert.assertTrue(e.getMessage().startsWith("Server returned HTTP response code: 409"));
    }

    // Create an OFFLINE table with invalid replication config
    offlineTableConfig.getValidationConfig().setReplication("abc");
    offlineTableConfig.setTableName("invalid_replication_table");
    try {
      sendPostRequest(_createTableUrl, offlineTableConfig.toJsonConfigString());
      Assert.fail("Creation of an invalid OFFLINE table does not fail");
    } catch (IOException e) {
      // Expected 400 Bad Request
      Assert.assertTrue(e.getMessage().startsWith("Server returned HTTP response code: 400"));
    }

    // Create a REALTIME table with an invalid name which should fail
    // NOTE: Set bad table name inside table config builder is not allowed, so have to explicitly set in table config
    TableConfig realtimeTableConfig = _realtimeBuilder.build();
    realtimeTableConfig.setTableName("bad__table__name");
    try {
      sendPostRequest(_createTableUrl, realtimeTableConfig.toJsonConfigString());
      Assert.fail("Creation of a REALTIME table with two underscores in the table name does not fail");
    } catch (IOException e) {
      // Expected 400 Bad Request
      Assert.assertTrue(e.getMessage().startsWith("Server returned HTTP response code: 400"));
    }

    // Creating a REALTIME table without a valid schema should fail
    _realtimeBuilder.setSchemaName("invalidSchemaName");
    TableConfig invalidConfig = _realtimeBuilder.build();
    try {
      sendPostRequest(_createTableUrl, realtimeTableConfig.toJsonConfigString());
      Assert.fail("Creation of a REALTIME table without a valid schema does not fail");
    } catch (IOException e) {
      // Expected 400 Bad Request
      Assert.assertTrue(e.getMessage().startsWith("Server returned HTTP response code: 400"));
    }

    // Creating a REALTIME table with a different schema name in the config should succeed (backwards compatibility mode)
    String schemaName = "differentRTSchema";
    _realtimeBuilder.setSchemaName(schemaName);
    // use a different table name so it doesn't associate a previously uploaded schema with the table
    _realtimeBuilder.setTableName("RT_TABLE");
    addDummySchema(schemaName);
    TableConfig diffConfig = _realtimeBuilder.build();
    sendPostRequest(_createTableUrl, diffConfig.toJsonConfigString());

    // Create a REALTIME table with a valid name and schema which should succeed
    _realtimeBuilder.setTableName(REALTIME_TABLE_NAME);
    _realtimeBuilder.setSchemaName(REALTIME_TABLE_NAME);
    TableConfig config = _realtimeBuilder.build();
    String realtimeTableJSONConfigString = config.toJsonConfigString();
    sendPostRequest(_createTableUrl, realtimeTableJSONConfigString);

    // TODO: check whether we should allow POST request to create REALTIME table that already exists
    // Create a REALTIME table that already exists which should succeed
    sendPostRequest(_createTableUrl, realtimeTableJSONConfigString);
  }

  @Test
  public void testTableMinReplication()
      throws Exception {
    testTableMinReplicationInternal("minReplicationOne", 1);
    testTableMinReplicationInternal("minReplicationTwo", NUM_SERVER_INSTANCES);
  }

  private void testTableMinReplicationInternal(String tableName, int tableReplication)
      throws Exception {
    String tableJSONConfigString =
        _offlineBuilder.setTableName(tableName).setNumReplicas(tableReplication).build().toJsonConfigString();
    sendPostRequest(_createTableUrl, tableJSONConfigString);
    // table creation should succeed
    TableConfig tableConfig = getTableConfig(tableName, "OFFLINE");
    Assert.assertEquals(tableConfig.getValidationConfig().getReplicationNumber(),
        Math.max(tableReplication, MIN_NUM_REPLICAS));

    addDummySchema(tableName);
    tableJSONConfigString =
        _realtimeBuilder.setTableName(tableName).setNumReplicas(tableReplication).build().toJsonConfigString();
    sendPostRequest(_createTableUrl, tableJSONConfigString);
    tableConfig = getTableConfig(tableName, "REALTIME");
    Assert.assertEquals(tableConfig.getValidationConfig().getReplicationNumber(),
        Math.max(tableReplication, MIN_NUM_REPLICAS));
    // This test can only be done via integration test
//    int replicasPerPartition = Integer.valueOf(tableConfig.getValidationConfig().getReplicasPerPartition());
//    Assert.assertEquals(replicasPerPartition, Math.max(tableReplication, TABLE_MIN_REPLICATION));
  }

  private TableConfig getTableConfig(String tableName, String tableType)
      throws Exception {
    String tableConfigString = sendGetRequest(_controllerRequestURLBuilder.forTableGet(tableName));
    return TableConfig.fromJsonConfig(JsonUtils.stringToJsonNode(tableConfigString).get(tableType));
  }

  @Test
  public void testUpdateTableConfig()
      throws Exception {
    String tableName = "updateTC";
    String tableJSONConfigString =
        _offlineBuilder.setTableName(tableName).setNumReplicas(2).build().toJsonConfigString();
    sendPostRequest(_createTableUrl, tableJSONConfigString);
    // table creation should succeed
    TableConfig tableConfig = getTableConfig(tableName, "OFFLINE");
    Assert.assertEquals(tableConfig.getValidationConfig().getRetentionTimeValue(), "5");
    Assert.assertEquals(tableConfig.getValidationConfig().getRetentionTimeUnit(), "DAYS");

    tableConfig.getValidationConfig().setRetentionTimeUnit("HOURS");
    tableConfig.getValidationConfig().setRetentionTimeValue("10");

    JsonNode jsonResponse = JsonUtils.stringToJsonNode(
        sendPutRequest(_controllerRequestURLBuilder.forUpdateTableConfig(tableName), tableConfig.toJsonConfigString()));
    Assert.assertTrue(jsonResponse.has("status"));

    TableConfig modifiedConfig = getTableConfig(tableName, "OFFLINE");
    Assert.assertEquals(modifiedConfig.getValidationConfig().getRetentionTimeUnit(), "HOURS");
    Assert.assertEquals(modifiedConfig.getValidationConfig().getRetentionTimeValue(), "10");

    // Realtime
    addDummySchema(tableName);
    tableJSONConfigString = _realtimeBuilder.setTableName(tableName).setNumReplicas(2).build().toJsonConfigString();
    sendPostRequest(_createTableUrl, tableJSONConfigString);
    tableConfig = getTableConfig(tableName, "REALTIME");
    Assert.assertEquals(tableConfig.getValidationConfig().getRetentionTimeValue(), "5");
    Assert.assertEquals(tableConfig.getValidationConfig().getRetentionTimeUnit(), "DAYS");
    Assert.assertNull(tableConfig.getQuotaConfig());

    QuotaConfig quota = new QuotaConfig();
    quota.setStorage("10G");
    tableConfig.setQuotaConfig(quota);
    sendPutRequest(_controllerRequestURLBuilder.forUpdateTableConfig(tableName), tableConfig.toJsonConfigString());
    modifiedConfig = getTableConfig(tableName, "REALTIME");
    Assert.assertNotNull(modifiedConfig.getQuotaConfig());
    Assert.assertEquals(modifiedConfig.getQuotaConfig().getStorage(), "10G");
    Assert.assertNull(modifiedConfig.getQuotaConfig().getMaxQueriesPerSecond());

    quota.setMaxQueriesPerSecond("100.00");
    tableConfig.setQuotaConfig(quota);
    sendPutRequest(_controllerRequestURLBuilder.forUpdateTableConfig(tableName), tableConfig.toJsonConfigString());
    modifiedConfig = getTableConfig(tableName, "REALTIME");
    Assert.assertNotNull(modifiedConfig.getQuotaConfig().getMaxQueriesPerSecond());
    Assert.assertEquals(modifiedConfig.getQuotaConfig().getMaxQueriesPerSecond(), "100.00");

    boolean notFoundException = false;
    try {
      // table does not exist
      tableConfig.setTableName("noSuchTable_REALTIME");
      sendPutRequest(_controllerRequestURLBuilder.forUpdateTableConfig("noSuchTable"),
          tableConfig.toJsonConfigString());
    } catch (Exception e) {
      Assert.assertTrue(e instanceof FileNotFoundException);
      notFoundException = true;
    }
    Assert.assertTrue(notFoundException);
  }

  @Test(expectedExceptions = FileNotFoundException.class)
  public void rebalanceNonExistentTable()
      throws Exception {
    sendPostRequest(_controllerRequestURLBuilder.forTableRebalance(OFFLINE_TABLE_NAME, "realtime"), null);
  }

  @Test
  public void rebalanceTableWithoutSegments()
      throws Exception {
    // Create the table
    sendPostRequest(_createTableUrl, _offlineBuilder.build().toJsonConfigString());

    // Rebalance should return status NO_OP
    RebalanceResult rebalanceResult = JsonUtils.stringToObject(
        sendPostRequest(_controllerRequestURLBuilder.forTableRebalance(OFFLINE_TABLE_NAME, "offline"), null),
        RebalanceResult.class);
    Assert.assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.NO_OP);
  }

  @AfterClass
  public void tearDown() {
    stopFakeInstances();
    stopController();
    stopZk();
  }
}
