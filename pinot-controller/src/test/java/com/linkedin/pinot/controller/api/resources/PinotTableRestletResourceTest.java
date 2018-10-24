/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.controller.api.resources;

import com.linkedin.pinot.common.config.QuotaConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.common.utils.ZkStarter;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.ControllerRequestBuilderUtil;
import com.linkedin.pinot.controller.helix.ControllerTest;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaStreamConfigProperties;
import com.linkedin.pinot.core.realtime.stream.StreamConfig;
import com.linkedin.pinot.core.realtime.stream.StreamConfigProperties;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.json.JSONException;
import org.json.JSONObject;
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

  private static final String REALTIME_TABLE_NAME = "testRealtimeTable";
  private final TableConfig.Builder _offlineBuilder = new TableConfig.Builder(TableType.OFFLINE);
  private final TableConfig.Builder _realtimeBuilder = new TableConfig.Builder(TableType.REALTIME);
  private String _createTableUrl;

  @BeforeClass
  public void setUp() throws Exception {
    startZk();
    ControllerConf config = getDefaultControllerConfiguration();
    config.setTableMinReplicas(MIN_NUM_REPLICAS);
    startController(config);
    _createTableUrl = _controllerRequestURLBuilder.forTableCreate();

    ControllerRequestBuilderUtil.addFakeBrokerInstancesToAutoJoinHelixCluster(getHelixClusterName(),
        ZkStarter.DEFAULT_ZK_STR, NUM_BROKER_INSTANCES, true);
    ControllerRequestBuilderUtil.addFakeDataInstancesToAutoJoinHelixCluster(getHelixClusterName(),
        ZkStarter.DEFAULT_ZK_STR, NUM_SERVER_INSTANCES, true);

    _offlineBuilder.setTableName("testOfflineTable")
        .setTimeColumnName("timeColumn")
        .setTimeType("DAYS")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("5");

    // add schema for realtime table
    addDummySchema(REALTIME_TABLE_NAME);

    Map<String, String> streamConfigs = new HashMap<>();
    String streamType = "kafka";
    streamConfigs.put(StreamConfigProperties.STREAM_TYPE, streamType);
    streamConfigs.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
        StreamConfig.ConsumerType.HIGHLEVEL.toString());
    streamConfigs.put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME), "fakeTopic");
    streamConfigs.put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS), "fakeClass");
    streamConfigs.put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS), "fakeClass");
    streamConfigs.put(KafkaStreamConfigProperties.constructStreamProperty(
        KafkaStreamConfigProperties.HighLevelConsumer.KAFKA_HLC_ZK_CONNECTION_STRING), "fakeUrl");
    streamConfigs.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, Integer.toString(1234));
    streamConfigs.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_OFFSET_CRITERIA), "smallest");
    _realtimeBuilder.setTableName(REALTIME_TABLE_NAME)
        .setTimeColumnName("timeColumn")
        .setTimeType("DAYS")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("5")
        .setSchemaName(REALTIME_TABLE_NAME)
        .setStreamConfigs(streamConfigs);
  }

  @Test
  public void testCreateTable() throws Exception {
    // Create an OFFLINE table with an invalid name which should fail
    // NOTE: Set bad table name inside table config builder is not allowed, so have to explicitly set in table config
    TableConfig offlineTableConfig = _offlineBuilder.build();
    offlineTableConfig.setTableName("bad__table__name");
    try {
      sendPostRequest(_createTableUrl, offlineTableConfig.toJSONConfigString());
      Assert.fail("Creation of an OFFLINE table with two underscores in the table name does not fail");
    } catch (IOException e) {
      // Expected 400 Bad Request
      Assert.assertTrue(e.getMessage().startsWith("Server returned HTTP response code: 400"));
    }

    // Create an OFFLINE table with a valid name which should succeed
    offlineTableConfig.setTableName("valid_table_name");
    String offlineTableJSONConfigString = offlineTableConfig.toJSONConfigString();
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
      sendPostRequest(_createTableUrl, offlineTableConfig.toJSONConfigString());
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
      sendPostRequest(_createTableUrl, realtimeTableConfig.toJSONConfigString());
      Assert.fail("Creation of a REALTIME table with two underscores in the table name does not fail");
    } catch (IOException e) {
      // Expected 400 Bad Request
      Assert.assertTrue(e.getMessage().startsWith("Server returned HTTP response code: 400"));
    }

    // Creating a REALTIME table without a valid schema should fail
    _realtimeBuilder.setSchemaName("invalidSchemaName");
    TableConfig invalidConfig = _realtimeBuilder.build();
    try {
      sendPostRequest(_createTableUrl, realtimeTableConfig.toJSONConfigString());
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
    sendPostRequest(_createTableUrl, diffConfig.toJSONConfigString());

    // Create a REALTIME table with a valid name and schema which should succeed
    _realtimeBuilder.setTableName(REALTIME_TABLE_NAME);
    _realtimeBuilder.setSchemaName(REALTIME_TABLE_NAME);
    TableConfig config = _realtimeBuilder.build();
    String realtimeTableJSONConfigString = config.toJSONConfigString();
    sendPostRequest(_createTableUrl, realtimeTableJSONConfigString);

    // TODO: check whether we should allow POST request to create REALTIME table that already exists
    // Create a REALTIME table that already exists which should succeed
    sendPostRequest(_createTableUrl, realtimeTableJSONConfigString);
  }

  @Test
  public void testTableMinReplication() throws Exception {
    testTableMinReplicationInternal("minReplicationOne", 1);
    testTableMinReplicationInternal("minReplicationTwo", NUM_SERVER_INSTANCES);
  }

  private void testTableMinReplicationInternal(String tableName, int tableReplication) throws Exception {
    String tableJSONConfigString =
        _offlineBuilder.setTableName(tableName).setNumReplicas(tableReplication).build().toJSONConfigString();
    sendPostRequest(_createTableUrl, tableJSONConfigString);
    // table creation should succeed
    TableConfig tableConfig = getTableConfig(tableName, "OFFLINE");
    Assert.assertEquals(tableConfig.getValidationConfig().getReplicationNumber(),
        Math.max(tableReplication, MIN_NUM_REPLICAS));

    addDummySchema(tableName);
    tableJSONConfigString =
        _realtimeBuilder.setTableName(tableName).setNumReplicas(tableReplication).build().toJSONConfigString();
    sendPostRequest(_createTableUrl, tableJSONConfigString);
    tableConfig = getTableConfig(tableName, "REALTIME");
    Assert.assertEquals(tableConfig.getValidationConfig().getReplicationNumber(),
        Math.max(tableReplication, MIN_NUM_REPLICAS));
    // This test can only be done via integration test
//    int replicasPerPartition = Integer.valueOf(tableConfig.getValidationConfig().getReplicasPerPartition());
//    Assert.assertEquals(replicasPerPartition, Math.max(tableReplication, TABLE_MIN_REPLICATION));
  }

  private TableConfig getTableConfig(String tableName, String tableType) throws Exception {
    String tableConfigString = sendGetRequest(_controllerRequestURLBuilder.forTableGet(tableName));
    return TableConfig.fromJSONConfig(new JSONObject(tableConfigString).getJSONObject(tableType));
  }

  @Test
  public void testUpdateTableConfig() throws Exception {
    String tableName = "updateTC";
    String tableJSONConfigString =
        _offlineBuilder.setTableName(tableName).setNumReplicas(2).build().toJSONConfigString();
    sendPostRequest(_createTableUrl, tableJSONConfigString);
    // table creation should succeed
    TableConfig tableConfig = getTableConfig(tableName, "OFFLINE");
    Assert.assertEquals(tableConfig.getValidationConfig().getRetentionTimeValue(), "5");
    Assert.assertEquals(tableConfig.getValidationConfig().getRetentionTimeUnit(), "DAYS");

    tableConfig.getValidationConfig().setRetentionTimeUnit("HOURS");
    tableConfig.getValidationConfig().setRetentionTimeValue("10");

    JSONObject jsonResponse = new JSONObject(
        sendPutRequest(_controllerRequestURLBuilder.forUpdateTableConfig(tableName), tableConfig.toJSONConfigString()));
    Assert.assertTrue(jsonResponse.has("status"));
    // TODO Verify success code, not success response string (Jersey API change)
//    Assert.assertEquals(jsonResponse.getString("status"), "Success");

    TableConfig modifiedConfig = getTableConfig(tableName, "OFFLINE");
    Assert.assertEquals(modifiedConfig.getValidationConfig().getRetentionTimeUnit(), "HOURS");
    Assert.assertEquals(modifiedConfig.getValidationConfig().getRetentionTimeValue(), "10");

    // Realtime
    addDummySchema(tableName);
    tableJSONConfigString = _realtimeBuilder.setTableName(tableName).setNumReplicas(2).build().toJSONConfigString();
    sendPostRequest(_createTableUrl, tableJSONConfigString);
    tableConfig = getTableConfig(tableName, "REALTIME");
    Assert.assertEquals(tableConfig.getValidationConfig().getRetentionTimeValue(), "5");
    Assert.assertEquals(tableConfig.getValidationConfig().getRetentionTimeUnit(), "DAYS");
    Assert.assertNull(tableConfig.getQuotaConfig());

    QuotaConfig quota = new QuotaConfig();
    quota.setStorage("10G");
    tableConfig.setQuotaConfig(quota);
    sendPutRequest(_controllerRequestURLBuilder.forUpdateTableConfig(tableName), tableConfig.toJSONConfigString());
    modifiedConfig = getTableConfig(tableName, "REALTIME");
    Assert.assertNotNull(modifiedConfig.getQuotaConfig());
    Assert.assertEquals(modifiedConfig.getQuotaConfig().getStorage(), "10G");
    Assert.assertNull(modifiedConfig.getQuotaConfig().getMaxQueriesPerSecond());

    quota.setMaxQueriesPerSecond("100.00");
    tableConfig.setQuotaConfig(quota);
    sendPutRequest(_controllerRequestURLBuilder.forUpdateTableConfig(tableName), tableConfig.toJSONConfigString());
    modifiedConfig = getTableConfig(tableName, "REALTIME");
    Assert.assertNotNull(modifiedConfig.getQuotaConfig().getMaxQueriesPerSecond());
    Assert.assertEquals(modifiedConfig.getQuotaConfig().getMaxQueriesPerSecond(), "100.00");

    boolean notFoundException = false;
    try {
      // table does not exist
      tableConfig.setTableName("noSuchTable_REALTIME");
      sendPutRequest(_controllerRequestURLBuilder.forUpdateTableConfig("noSuchTable"),
          tableConfig.toJSONConfigString());
    } catch (Exception e) {
      Assert.assertTrue(e instanceof FileNotFoundException);
      notFoundException = true;
    }
    Assert.assertTrue(notFoundException);
  }

  @Test(expectedExceptions = FileNotFoundException.class)
  public void rebalanceNonExistentOfflineTable() throws IOException, JSONException {
    String tableName = "nonExistentTable";
    // should result in file not found exception
    sendPostRequest(_controllerRequestURLBuilder.forTableRebalance(tableName, "offline"), null);
  }

  @Test(expectedExceptions = FileNotFoundException.class)
  public void rebalanceNonExistentRealtimeTable() throws IOException, JSONException {
    String tableName = "nonExistentTable";
    // should result in file not found exception
    sendPostRequest(_controllerRequestURLBuilder.forTableRebalance(tableName, "realtime"), null);
  }

  @Test
  public void rebalanceOfflineTable() {
    String tableName = "testOfflineTable";
    _offlineBuilder.setTableName(tableName);
    // create the table
    try {
      TableConfig offlineTableConfig = _offlineBuilder.build();
      sendPostRequest(_createTableUrl, offlineTableConfig.toJSONConfigString());
    } catch (Exception e) {
      Assert.fail("Failed to create offline table " + tableName + "Error: " + e.getMessage());
    }

    // rebalance should not throw exception
    try {
      sendPostRequest(_controllerRequestURLBuilder.forTableRebalance(tableName, "offline"), null);
    } catch (Exception e) {
      Assert.fail("Failed to rebalance existing offline table " + tableName);
    }

    // rebalance should throw exception because realtime table does not exist
    try {
      sendPostRequest(_controllerRequestURLBuilder.forTableRebalance(tableName, "realtime"), null);
    } catch (Exception e) {
      if (!(e instanceof FileNotFoundException)) {
        Assert.fail("Did not fail to create non existent realtime table " + tableName);
      } else {
        return;
      }
    }
    Assert.fail("Did not fail to create non existent realtime table " + tableName);
  }

  @AfterClass
  public void tearDown() {
    stopController();
    stopZk();
  }
}
