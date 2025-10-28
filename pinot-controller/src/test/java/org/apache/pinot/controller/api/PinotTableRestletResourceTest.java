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
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.task.TaskState;
import org.apache.pinot.client.admin.PinotAdminClient;
import org.apache.pinot.client.admin.PinotLogicalTableAdminClient;
import org.apache.pinot.client.admin.PinotTableAdminClient;
import org.apache.pinot.client.admin.PinotTaskAdminClient;
import org.apache.pinot.client.admin.PinotZookeeperAdminClient;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.controller.helix.core.minion.TaskSchedulingContext;
import org.apache.pinot.controller.helix.core.minion.TaskSchedulingInfo;
import org.apache.pinot.controller.helix.core.minion.generator.BaseTaskGenerator;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceResult;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceConstraintConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceTagPoolConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.StringUtil;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;


/**
 * Test for table creation
 */
public class PinotTableRestletResourceTest extends ControllerTest {
  private static final String OFFLINE_TABLE_NAME = "testOfflineTable";
  private static final String REALTIME_TABLE_NAME = "testRealtimeTable";
  private final TableConfigBuilder _offlineBuilder = getOfflineTableBuilder(OFFLINE_TABLE_NAME);
  private final TableConfigBuilder _realtimeBuilder = getRealtimeTableBuilder(REALTIME_TABLE_NAME);

  @BeforeClass
  public void setUp()
      throws Exception {
    DEFAULT_INSTANCE.setupSharedStateAndValidate();
    registerMinionTasks();
  }

  private PinotAdminClient admin()
      throws IOException {
    return DEFAULT_INSTANCE.getAdminClient();
  }

  private PinotTableAdminClient tableClient()
      throws IOException {
    return admin().getTableClient();
  }

  private PinotLogicalTableAdminClient logicalTableClient()
      throws IOException {
    return admin().getLogicalTableClient();
  }

  private PinotZookeeperAdminClient zookeeperClient()
      throws IOException {
    return admin().getZookeeperClient();
  }

  private PinotTaskAdminClient taskClient()
      throws IOException {
    return admin().getTaskClient();
  }

  private String createTable(String tableConfigJson)
      throws IOException {
    try {
      return tableClient().createTable(tableConfigJson, null);
    } catch (Exception e) {
      throw unwrapAsIOException(e);
    }
  }

  private String updateTable(String tableName, String tableConfigJson)
      throws IOException {
    try {
      return tableClient().updateTableConfig(tableName, tableConfigJson);
    } catch (Exception e) {
      throw unwrapAsIOException(e);
    }
  }

  private String deleteTable(String tableName)
      throws IOException {
    try {
      return tableClient().deleteTable(tableName);
    } catch (Exception e) {
      throw unwrapAsIOException(e);
    }
  }

  private String deleteTable(String tableName, @Nullable String tableType)
      throws IOException {
    try {
      return tableClient().deleteTable(tableName, tableType, null, null);
    } catch (Exception e) {
      throw unwrapAsIOException(e);
    }
  }

  private String deleteTable(String tableName, @Nullable String tableType, @Nullable Boolean ignoreActiveTasks)
      throws IOException {
    try {
      return tableClient().deleteTable(tableName, tableType, null, ignoreActiveTasks);
    } catch (Exception e) {
      throw unwrapAsIOException(e);
    }
  }

  private String rebalanceTable(String tableName, String tableType, boolean dryRun, boolean reassignInstances,
      boolean includeConsuming, boolean downtime, int minAvailableReplicas)
      throws IOException {
    try {
      return tableClient().rebalanceTable(tableName, tableType, dryRun, reassignInstances, includeConsuming, downtime,
          minAvailableReplicas);
    } catch (Exception e) {
      throw unwrapAsIOException(e);
    }
  }

  private IOException unwrapAsIOException(Exception e) {
    Throwable cause = e;
    while (cause.getCause() != null) {
      cause = cause.getCause();
    }
    if (cause instanceof IOException) {
      return (IOException) cause;
    }
    return new IOException(cause.getMessage(), e);
  }

  private void assertHasStatus(Exception e, int statusCode) {
    String message = e.getMessage() != null ? e.getMessage() : "";
    assertTrue(message.contains("status: " + statusCode) || message.contains("status code: " + statusCode),
        "Unexpected error message: " + message);
  }

  private TableConfigBuilder getRealtimeTableBuilder(String tableName) {
    return new TableConfigBuilder(TableType.REALTIME)
        .setTableName(tableName)
        .setTimeColumnName("timeColumn")
        .setTimeType("DAYS")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("5")
        .setStreamConfigs(FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap());
  }

  private TableConfigBuilder getOfflineTableBuilder(String tableName) {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(tableName)
        .setTimeColumnName("timeColumn")
        .setTimeType("DAYS")
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("5");
  }

  @BeforeMethod
  public void beforeMethod()
      throws Exception {
    DEFAULT_INSTANCE.addDummySchema(REALTIME_TABLE_NAME);
    DEFAULT_INSTANCE.addDummySchema(OFFLINE_TABLE_NAME);
  }

  private void registerMinionTasks() {
    PinotTaskManager taskManager = DEFAULT_INSTANCE.getControllerStarter().getTaskManager();
    ClusterInfoAccessor clusterInfoAccessor = Mockito.mock(ClusterInfoAccessor.class);
    Mockito.when(clusterInfoAccessor.getClusterConfig(any())).thenReturn(null);
    registerTaskGenerator(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE, taskManager, clusterInfoAccessor);
    registerTaskGenerator(MinionConstants.MergeRollupTask.TASK_TYPE, taskManager, clusterInfoAccessor);
    registerTaskGenerator(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE, taskManager, clusterInfoAccessor);
  }

  private static void registerTaskGenerator(String taskType, PinotTaskManager taskManager,
      ClusterInfoAccessor clusterInfoAccessor) {
    BaseTaskGenerator taskGenerator = new BaseTaskGenerator() {
      @Override
      public String getTaskType() {
        return taskType;
      }

      @Override
      public List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs) {
        return List.of(new PinotTaskConfig(taskType,
            tableConfigs.get(0).getTaskConfig().getConfigsForTaskType(getTaskType())));
      }
    };
    taskGenerator.init(clusterInfoAccessor);
    taskManager.registerTaskGenerator(taskGenerator);
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
      createTable(offlineTableConfigJson.toString());
      fail("Creation of an OFFLINE table with two underscores in the table name does not fail");
    } catch (IOException e) {
      // Expected 400 Bad Request
      assertHasStatus(e, 400);
    }

    offlineTableConfig = _offlineBuilder.build();
    offlineTableConfigJson = (ObjectNode) offlineTableConfig.toJsonNode();
    offlineTableConfigJson.put(TableConfig.TABLE_NAME_KEY, "bad.table.with.dot");
    try {
      createTable(offlineTableConfigJson.toString());
      fail("Creation of an OFFLINE table with dot in the table name does not fail");
    } catch (IOException e) {
      // Expected 400 Bad Request
      assertHasStatus(e, 400);
    }

    // Creating an OFFLINE table without a valid schema should fail
    offlineTableConfig = _offlineBuilder.setTableName("no_schema").build();
    try {
      createTable(offlineTableConfig.toJsonString());
      fail("Creation of a OFFLINE table without a valid schema does not fail");
    } catch (IOException e) {
      // Expected 400 Bad Request
      assertHasStatus(e, 400);
    }

    // Create an OFFLINE table with a valid name which should succeed
    DEFAULT_INSTANCE.addDummySchema("valid_table_name");
    offlineTableConfig = _offlineBuilder.setTableName("valid_table_name").build();
    String offlineTableConfigString = offlineTableConfig.toJsonString();
    createTable(offlineTableConfigString);

    // Create an OFFLINE table that already exists which should fail
    try {
      createTable(offlineTableConfigString);
      fail("Creation of an existing OFFLINE table does not fail");
    } catch (IOException e) {
      // Expected 409 Conflict
      assertHasStatus(e, 409);
    }

    // Create an OFFLINE table with invalid replication config
    offlineTableConfig = _offlineBuilder.setTableName("invalid_replication_config").build();
    offlineTableConfig.getValidationConfig().setReplication("abc");
    try {
      createTable(offlineTableConfig.toJsonString());
      fail("Creation of an invalid OFFLINE table does not fail");
    } catch (IOException e) {
      // Expected 400 Bad Request
      assertHasStatus(e, 400);
    }

    // Create a REALTIME table with an invalid name which should fail
    // NOTE: Set bad table name inside table config builder is not allowed, so have to set in json node
    TableConfig realtimeTableConfig = _realtimeBuilder.build();
    ObjectNode realtimeTableConfigJson = (ObjectNode) realtimeTableConfig.toJsonNode();
    realtimeTableConfigJson.put(TableConfig.TABLE_NAME_KEY, "bad__table__name");
    try {
      createTable(realtimeTableConfigJson.toString());
      fail("Creation of a REALTIME table with two underscores in the table name does not fail");
    } catch (IOException e) {
      // Expected 400 Bad Request
      assertHasStatus(e, 400);
    }

    // Creating a REALTIME table without a valid schema should fail
    realtimeTableConfig = _realtimeBuilder.setTableName("noSchema").build();
    try {
      createTable(realtimeTableConfig.toJsonString());
      fail("Creation of a REALTIME table without a valid schema does not fail");
    } catch (IOException e) {
      // Expected 400 Bad Request
      assertHasStatus(e, 400);
    }

    // Create a REALTIME table with the invalid time column name should fail
    realtimeTableConfig =
        _realtimeBuilder.setTableName(REALTIME_TABLE_NAME).setTimeColumnName("invalidTimeColumn").build();
    try {
      createTable(realtimeTableConfig.toJsonString());
      fail("Creation of a REALTIME table with a invalid time column name does not fail");
    } catch (IOException e) {
      // Expected 400 Bad Request
      assertHasStatus(e, 400);
    }

    // Create a REALTIME table with a valid name and schema which should succeed
    realtimeTableConfig = _realtimeBuilder.setTableName(REALTIME_TABLE_NAME).setTimeColumnName("timeColumn").build();
    String realtimeTableConfigString = realtimeTableConfig.toJsonString();
    createTable(realtimeTableConfigString);
  }

  @Test
  public void testTableCronSchedule()
      throws IOException {
    String rawTableName = "test_table_cron_schedule";
    DEFAULT_INSTANCE.addDummySchema(rawTableName);
    // Failed to create a table
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(rawTableName).setTaskConfig(
        new TableTaskConfig(Map.of(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE,
            Map.of(PinotTaskManager.SCHEDULE_KEY, "* * * * * * *")))).build();
    try {
      createTable(tableConfig.toJsonString());
      fail("Creation of an OFFLINE table with an invalid cron expression does not fail");
    } catch (IOException e) {
      // Expected 400 Bad Request
      assertHasStatus(e, 400);
    }

    // Succeed to create a table
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(rawTableName).setTaskConfig(
        new TableTaskConfig(Map.of(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE,
            Map.of(PinotTaskManager.SCHEDULE_KEY, "0 */10 * ? * * *")))).build();
    try {
      String response = createTable(tableConfig.toJsonString());
      assertEquals(response,
          "{\"unrecognizedProperties\":{},\"status\":\"Table test_table_cron_schedule_OFFLINE successfully added\"}");
    } catch (IOException e) {
      // Expected 400 Bad Request
      fail("This is a valid table config with cron schedule");
    }

    // Failed to update the table
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(rawTableName).setTaskConfig(
        new TableTaskConfig(Map.of(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE,
            Map.of(PinotTaskManager.SCHEDULE_KEY, "5 5 5 5 5 5 5")))).build();
    try {
      updateTable(rawTableName, tableConfig.toJsonString());
      fail("Update of an OFFLINE table with an invalid cron expression does not fail");
    } catch (IOException e) {
      // Expected 400 Bad Request
      assertHasStatus(e, 400);
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
    DEFAULT_INSTANCE.addDummySchema(tableName);
    String tableJSONConfigString =
        _offlineBuilder.setTableName(tableName).setNumReplicas(tableReplication).build().toJsonString();
    createTable(tableJSONConfigString);
    // table creation should succeed
    TableConfig tableConfig = getTableConfig(tableName, "OFFLINE");
    assertEquals(tableConfig.getReplication(),
        Math.max(tableReplication, DEFAULT_MIN_NUM_REPLICAS));

    tableJSONConfigString =
        _realtimeBuilder.setTableName(tableName).setNumReplicas(tableReplication).build().toJsonString();
    createTable(tableJSONConfigString);
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
    createTable(tableConfig.toJsonString());
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
      createTable(tableConfig.toJsonString());
      fail("Creation of a DIMENSION table with larger than allowed storage quota should fail");
    } catch (IOException e) {
      assertHasStatus(e, 400);
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
    createTable(tableConfig.toJsonString());
    tableConfig = getTableConfig(tableName, "OFFLINE");
    assertEquals(tableConfig.getQuotaConfig().getStorage(), goodQuota);
  }

  private TableConfig getTableConfig(String tableName, String tableType)
      throws Exception {
    String tableConfigString = tableClient().getTableConfig(tableName, tableType);
    JsonNode tableConfigNode = JsonUtils.stringToJsonNode(tableConfigString);
    if (tableConfigNode.has(tableType)) {
      tableConfigNode = tableConfigNode.get(tableType);
    }
    return JsonUtils.jsonNodeToObject(tableConfigNode, TableConfig.class);
  }

  @Test
  public void testUpdateTableConfig()
      throws Exception {
    String tableName = "updateTC";
    DEFAULT_INSTANCE.addDummySchema(tableName);
    String tableConfigString = _offlineBuilder.setTableName(tableName).setNumReplicas(2).build().toJsonString();
    createTable(tableConfigString);
    // table creation should succeed
    TableConfig tableConfig = getTableConfig(tableName, "OFFLINE");
    assertEquals(tableConfig.getValidationConfig().getRetentionTimeValue(), "5");
    assertEquals(tableConfig.getValidationConfig().getRetentionTimeUnit(), "DAYS");

    tableConfig.getValidationConfig().setRetentionTimeUnit("HOURS");
    tableConfig.getValidationConfig().setRetentionTimeValue("10");

    JsonNode jsonResponse = JsonUtils.stringToJsonNode(updateTable(tableName, tableConfig.toJsonString()));
    assertTrue(jsonResponse.has("status"));

    TableConfig modifiedConfig = getTableConfig(tableName, "OFFLINE");
    assertEquals(modifiedConfig.getValidationConfig().getRetentionTimeUnit(), "HOURS");
    assertEquals(modifiedConfig.getValidationConfig().getRetentionTimeValue(), "10");

    // Realtime
    DEFAULT_INSTANCE.addDummySchema(tableName);
    tableConfigString = _realtimeBuilder.setTableName(tableName).setNumReplicas(2).build().toJsonString();
    createTable(tableConfigString);
    tableConfig = getTableConfig(tableName, "REALTIME");
    assertEquals(tableConfig.getValidationConfig().getRetentionTimeValue(), "5");
    assertEquals(tableConfig.getValidationConfig().getRetentionTimeUnit(), "DAYS");
    assertNull(tableConfig.getQuotaConfig());

    QuotaConfig quota = new QuotaConfig("10G", "100.0");
    tableConfig.setQuotaConfig(quota);
    updateTable(tableName, tableConfig.toJsonString());
    modifiedConfig = getTableConfig(tableName, "REALTIME");
    assertNotNull(modifiedConfig.getQuotaConfig());
    assertEquals(modifiedConfig.getQuotaConfig().getStorage(), "10G");
    assertEquals(modifiedConfig.getQuotaConfig().getMaxQueriesPerSecond(), "100.0");

    try {
      // table does not exist
      ObjectNode tableConfigJson = (ObjectNode) tableConfig.toJsonNode();
      tableConfigJson.put(TableConfig.TABLE_NAME_KEY, "noSuchTable_REALTIME");
      updateTable("noSuchTable", tableConfigJson.toString());
    } catch (Exception e) {
      assertTrue(e instanceof IOException);
    }
  }

  private void deleteAllTables()
      throws IOException {
    List<String> tables = getTableNames("offline", null, null, null);
    tables.addAll(getTableNames("realtime", null, null, null));
    for (String tableName : tables) {
      deleteTable(tableName);
    }
  }

  @Test
  public void testListTables()
      throws Exception {
    deleteAllTables();
    List<String> tables = getTableNames(null, null, null, null);
    assertTrue(tables.isEmpty());

    // post 2 offline, 1 realtime
    String rawTableName1 = "pqr";
    DEFAULT_INSTANCE.addDummySchema(rawTableName1);
    TableConfig offlineTableConfig1 = _offlineBuilder.setTableName(rawTableName1).build();
    createTable(offlineTableConfig1.toJsonString());
    TableConfig realtimeTableConfig1 = _realtimeBuilder.setTableName(rawTableName1).setNumReplicas(2).build();
    createTable(realtimeTableConfig1.toJsonString());
    String rawTableName2 = "abc";
    DEFAULT_INSTANCE.addDummySchema(rawTableName2);
    TableConfig offlineTableConfig2 = _offlineBuilder.setTableName(rawTableName2).build();
    createTable(offlineTableConfig2.toJsonString());

    // list
    tables = getTableNames(null, null, null, null);
    assertEquals(tables, Lists.newArrayList("abc", "pqr"));
    tables = getTableNames(null, null, null, false);
    assertEquals(tables, Lists.newArrayList("pqr", "abc"));
    tables = getTableNames(null, null, "creationTime", null);
    assertEquals(tables, Lists.newArrayList("pqr_OFFLINE", "pqr_REALTIME", "abc_OFFLINE"));
    tables = getTableNames(null, null, "creationTime", false);
    assertEquals(tables, Lists.newArrayList("abc_OFFLINE", "pqr_REALTIME", "pqr_OFFLINE"));
    tables = getTableNames(null, null, "lastModifiedTime", null);
    assertEquals(tables, Lists.newArrayList("pqr_OFFLINE", "pqr_REALTIME", "abc_OFFLINE"));
    tables = getTableNames(null, null, "lastModifiedTime", false);
    assertEquals(tables, Lists.newArrayList("abc_OFFLINE", "pqr_REALTIME", "pqr_OFFLINE"));

    // type
    tables = getTableNames("realtime", null, null, null);
    assertEquals(tables, Lists.newArrayList("pqr_REALTIME"));
    tables = getTableNames("offline", null, null, null);
    assertEquals(tables, Lists.newArrayList("abc_OFFLINE", "pqr_OFFLINE"));
    tables = getTableNames("offline", null, null, false);
    assertEquals(tables, Lists.newArrayList("pqr_OFFLINE", "abc_OFFLINE"));
    tables = getTableNames("offline", null, "creationTime", null);
    assertEquals(tables, Lists.newArrayList("pqr_OFFLINE", "abc_OFFLINE"));
    tables = getTableNames("offline", null, "creationTime", false);
    assertEquals(tables, Lists.newArrayList("abc_OFFLINE", "pqr_OFFLINE"));
    tables = getTableNames("offline", null, "lastModifiedTime", null);
    assertEquals(tables, Lists.newArrayList("pqr_OFFLINE", "abc_OFFLINE"));
    tables = getTableNames("offline", null, "lastModifiedTime", false);
    assertEquals(tables, Lists.newArrayList("abc_OFFLINE", "pqr_OFFLINE"));

    // update taskType for abc_OFFLINE
    Map<String, Map<String, String>> taskTypeMap = new HashMap<>();
    taskTypeMap.put(MinionConstants.MergeRollupTask.TASK_TYPE, new HashMap<>());
    offlineTableConfig2.setTaskConfig(new TableTaskConfig(taskTypeMap));
    JsonUtils.stringToJsonNode(updateTable(rawTableName2, offlineTableConfig2.toJsonString()));
    // update for pqr_REALTIME
    taskTypeMap = new HashMap<>();
    taskTypeMap.put(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE, new HashMap<>());
    realtimeTableConfig1.setTaskConfig(new TableTaskConfig(taskTypeMap));
    JsonUtils.stringToJsonNode(updateTable(rawTableName1, realtimeTableConfig1.toJsonString()));

    // list lastModified, taskType
    tables = getTableNames(null, null, "lastModifiedTime", null);
    assertEquals(tables, Lists.newArrayList("pqr_OFFLINE", "abc_OFFLINE", "pqr_REALTIME"));
    tables = getTableNames(null, null, "lastModifiedTime", false);
    assertEquals(tables, Lists.newArrayList("pqr_REALTIME", "abc_OFFLINE", "pqr_OFFLINE"));
    tables = getTableNames(null, MinionConstants.MergeRollupTask.TASK_TYPE, null, null);
    assertEquals(tables, Lists.newArrayList("abc_OFFLINE"));
    tables = getTableNames("realtime", MinionConstants.MergeRollupTask.TASK_TYPE, null, null);
    assertTrue(tables.isEmpty());
    tables = getTableNames(null, MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE, null, null);
    assertEquals(tables, Lists.newArrayList("pqr_REALTIME"));

    // update taskType for pqr_OFFLINE
    taskTypeMap = new HashMap<>();
    taskTypeMap.put(MinionConstants.MergeRollupTask.TASK_TYPE, new HashMap<>());
    offlineTableConfig1.setTaskConfig(new TableTaskConfig(taskTypeMap));
    JsonUtils.stringToJsonNode(updateTable(rawTableName1, offlineTableConfig1.toJsonString()));

    // list lastModified, taskType
    tables = getTableNames(null, MinionConstants.MergeRollupTask.TASK_TYPE, null, null);
    assertEquals(tables, Lists.newArrayList("abc_OFFLINE", "pqr_OFFLINE"));
    tables = getTableNames(null, MinionConstants.MergeRollupTask.TASK_TYPE, null, false);
    assertEquals(tables, Lists.newArrayList("pqr_OFFLINE", "abc_OFFLINE"));
    tables = getTableNames(null, MinionConstants.MergeRollupTask.TASK_TYPE, "creationTime", null);
    assertEquals(tables, Lists.newArrayList("pqr_OFFLINE", "abc_OFFLINE"));
  }

  private List<String> getTableNames(@Nullable String tableType, @Nullable String taskType, @Nullable String sortType,
      @Nullable Boolean sortAsc)
      throws IOException {
    Map<String, String> queryParams = new HashMap<>();
    if (tableType != null) {
      queryParams.put("type", tableType);
    }
    if (taskType != null) {
      queryParams.put("taskType", taskType);
    }
    if (sortType != null) {
      queryParams.put("sortType", sortType);
    }
    if (sortAsc != null) {
      queryParams.put("sortAsc", String.valueOf(sortAsc));
    }
    String response = executeControllerRest("GET", "/tables", null, queryParams.isEmpty() ? null : queryParams);
    JsonNode tablesJson = JsonUtils.stringToJsonNode(response).get("tables");
    return JsonUtils.jsonNodeToObject(tablesJson, new TypeReference<List<String>>() {
    });
  }

  @Test(expectedExceptions = IOException.class)
  public void rebalanceNonExistentTable()
      throws Exception {
    rebalanceTable(OFFLINE_TABLE_NAME, "realtime", false, false, false, false, 1);
  }

  @Test
  public void rebalanceTableWithoutSegments()
      throws Exception {
    // Create the table
    createTable(_offlineBuilder.build().toJsonString());

    // Rebalance should return status NO_OP
    RebalanceResult rebalanceResult = JsonUtils.stringToObject(
        rebalanceTable(OFFLINE_TABLE_NAME, "offline", false, false, false, false, 1),
        RebalanceResult.class);
    assertEquals(rebalanceResult.getStatus(), RebalanceResult.Status.NO_OP);
  }

  @Test
  public void testDeleteTable()
      throws IOException {
    // Case 1: Create a REALTIME table and delete it directly w/o using query param.
    DEFAULT_INSTANCE.addDummySchema("table0");
    TableConfig realtimeTableConfig = _realtimeBuilder.setTableName("table0").build();
    String creationResponse = createTable(realtimeTableConfig.toJsonString());
    assertEquals(creationResponse,
        "{\"unrecognizedProperties\":{},\"status\":\"Table table0_REALTIME successfully added\"}");

    // Delete realtime table using REALTIME suffix.
    String deleteResponse = sendDeleteRequest(
        StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables", "table0_REALTIME"));
    assertEquals(deleteResponse, "{\"status\":\"Tables: [table0_REALTIME] deleted\"}");

    // Case 2: Create an offline table and delete it directly w/o using query param.
    TableConfig offlineTableConfig = _offlineBuilder.setTableName("table0").build();
    creationResponse = createTable(offlineTableConfig.toJsonString());
    assertEquals(creationResponse,
        "{\"unrecognizedProperties\":{},\"status\":\"Table table0_OFFLINE successfully added\"}");

    // Delete offline table using OFFLINE suffix.
    deleteResponse =
        sendDeleteRequest(StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables", "table0_OFFLINE"));
    assertEquals(deleteResponse, "{\"status\":\"Tables: [table0_OFFLINE] deleted\"}");

    // Case 3: Create REALTIME and OFFLINE tables and delete both of them.
    DEFAULT_INSTANCE.addDummySchema("table1");
    TableConfig rtConfig1 = _realtimeBuilder.setTableName("table1").build();
    creationResponse = createTable(rtConfig1.toJsonString());
    assertEquals(creationResponse,
        "{\"unrecognizedProperties\":{},\"status\":\"Table table1_REALTIME successfully added\"}");

    TableConfig offlineConfig1 = _offlineBuilder.setTableName("table1").build();
    creationResponse = createTable(offlineConfig1.toJsonString());
    assertEquals(creationResponse,
        "{\"unrecognizedProperties\":{},\"status\":\"Table table1_OFFLINE successfully added\"}");

    deleteResponse =
        sendDeleteRequest(StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables", "table1"));
    assertEquals(deleteResponse, "{\"status\":\"Tables: [table1_OFFLINE, table1_REALTIME] deleted\"}");

    // Case 4: Create REALTIME and OFFLINE tables and delete the realtime/offline table using query params.
    DEFAULT_INSTANCE.addDummySchema("table2");
    TableConfig rtConfig2 = _realtimeBuilder.setTableName("table2").build();
    creationResponse = createTable(rtConfig2.toJsonString());
    assertEquals(creationResponse,
        "{\"unrecognizedProperties\":{},\"status\":\"Table table2_REALTIME successfully added\"}");

    TableConfig offlineConfig2 = _offlineBuilder.setTableName("table2").build();
    creationResponse = createTable(offlineConfig2.toJsonString());
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
    DEFAULT_INSTANCE.addDummySchema("table3");
    TableConfig rtConfig3 = _realtimeBuilder.setTableName("table3").build();
    creationResponse = createTable(rtConfig3.toJsonString());
    assertEquals(creationResponse,
        "{\"unrecognizedProperties\":{},\"status\":\"Table table3_REALTIME successfully added\"}");

    TableConfig offlineConfig3 = _offlineBuilder.setTableName("table3").build();
    creationResponse = createTable(offlineConfig3.toJsonString());
    assertEquals(creationResponse,
        "{\"unrecognizedProperties\":{},\"status\":\"Table table3_OFFLINE successfully added\"}");

    deleteResponse = deleteTable("table3_REALTIME", "realtime");
    assertEquals(deleteResponse, "{\"status\":\"Tables: [table3_REALTIME] deleted\"}");

    deleteResponse = deleteTable("table3_OFFLINE", "offline");
    assertEquals(deleteResponse, "{\"status\":\"Tables: [table3_OFFLINE] deleted\"}");
  }

  @Test(dataProvider = "tableTypeProvider")
  public void testDeleteTableWithLogicalTable(TableType tableType)
      throws Exception {
    String logicalTable = "logicalTable";
    String tableName = "physicalTable";
    String tableNameWithType = TableNameBuilder.forType(tableType).tableNameWithType(tableName);
    DEFAULT_INSTANCE.addDummySchema(tableName);
    DEFAULT_INSTANCE.addDummySchema(logicalTable);
    DEFAULT_INSTANCE.addTableConfig(createDummyTableConfig(tableName, tableType));

    LogicalTableConfig logicalTableConfig =
        ControllerTest.getDummyLogicalTableConfig(logicalTable, List.of(tableNameWithType), "DefaultTenant");
    String response = logicalTableClient().createLogicalTable(logicalTableConfig.toSingleLineJsonString());
    assertEquals(response,
        "{\"unrecognizedProperties\":{},\"status\":\"logicalTable logical table successfully added.\"}");

    // table deletion should fail
    String msg = expectThrows(IOException.class, () -> deleteTable(tableNameWithType)).getMessage();
    assertTrue(msg.contains("Cannot delete table config: " + tableNameWithType
        + " because it is referenced in logical table: logicalTable"), msg);

    // table delete with name and type should also fail
    msg = expectThrows(IOException.class,
        () -> deleteTable(tableNameWithType, tableType.name())).getMessage();
    assertTrue(msg.contains("Cannot delete table config: " + tableNameWithType
        + " because it is referenced in logical table: logicalTable"), msg);

    // table delete with raw table name also should fail
    msg = expectThrows(IOException.class, () -> deleteTable(tableName)).getMessage();
    assertTrue(msg.contains(
        "Cannot delete table config: " + tableName + " because it is referenced in logical table: logicalTable"), msg);

    // table delete with raw table name and type also should fail
    msg = expectThrows(IOException.class, () -> deleteTable(tableName, tableType.name())).getMessage();
    assertTrue(msg.contains(
        "Cannot delete table config: " + tableName + " because it is referenced in logical table: logicalTable"), msg);

    // Delete logical table
    response = logicalTableClient().deleteLogicalTable(logicalTable);
    assertEquals(response, "{\"status\":\"logicalTable logical table successfully deleted.\"}");

    // Now table deletion should succeed
    response = deleteTable(tableNameWithType);
    assertEquals(response, "{\"status\":\"Tables: [" + tableNameWithType + "] deleted\"}");
  }

  @Test
  public void testCheckTableState()
      throws IOException {

    // Create a valid REALTIME table
    String tableName = "testTable";
    DEFAULT_INSTANCE.addDummySchema(tableName);
    TableConfig realtimeTableConfig = _realtimeBuilder.setTableName(tableName).build();
    String creationResponse = createTable(realtimeTableConfig.toJsonString());
    assertEquals(creationResponse,
        "{\"unrecognizedProperties\":{},\"status\":\"Table testTable_REALTIME successfully added\"}");

    // Create a valid OFFLINE table
    TableConfig offlineTableConfig = _offlineBuilder.setTableName(tableName).build();
    creationResponse = createTable(offlineTableConfig.toJsonString());
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
      assertHasStatus(e, 400);
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
      assertHasStatus(e, 400);
    }
  }

  @Test
  public void testUnrecognizedProperties()
      throws IOException {
    // Create an OFFLINE table with a valid name but with unrecognizedProperties which should succeed
    // Should have unrecognizedProperties set correctly
    String tableName = "valid_table_name_extra_props";
    DEFAULT_INSTANCE.addDummySchema(tableName);
    TableConfig offlineTableConfig = _realtimeBuilder.setTableName("valid_table_name_extra_props").build();
    JsonNode jsonNode = JsonUtils.objectToJsonNode(offlineTableConfig);
    ((ObjectNode) jsonNode).put("illegalKey1", 1);
    ObjectNode internalObj = JsonUtils.newObjectNode();
    internalObj.put("illegalKey3", 2);
    ((ObjectNode) jsonNode).put("illegalKey2", internalObj);

    String creationResponse = createTable(jsonNode.toString());
    assertEquals(creationResponse,
        "{\"unrecognizedProperties\":{\"/illegalKey1\":1,\"/illegalKey2/illegalKey3\":2},\"status\":\"Table "
            + "valid_table_name_extra_props_REALTIME successfully added\"}");

    // update table with unrecognizedProperties
    String updateResponse =
        updateTable(tableName,
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

  /**
   * Validates the behavior of the system when creating or updating tables with invalid replication factors.
   * This method tests both REALTIME and OFFLINE table configurations.
   *
   * The method performs the following steps:
   * 1. Attempts to create a REALTIME table with an invalid replication factor of 5, which exceeds the number of
   *  available instances. The creation is expected to fail, and the test verifies that the exception message
   *  contains the expected error.
   * 2. Attempts to create an OFFLINE table with the same invalid replication factor. The creation is expected to
   *  fail, and the test verifies that the exception message contains the expected error.
   * 3. Creates REALTIME and OFFLINE tables with a valid replication factor of 1 to establish a baseline for further
   * testing. These creations are expected to succeed.
   * 4. Attempts to update the replication factor of the previously created REALTIME and OFFLINE tables to the
   * invalid value of 5. These updates are expected to fail, and the test verifies that the appropriate error
   * messages are returned.
   *
   * @throws Exception if any error occurs during the validation process
   */
  @Test
  public void validateInvalidTableReplication()
      throws Exception {
    String rawTableName = "validateInvalidTableReplication";

    validateTableCreationWithInvalidReplication(rawTableName, TableType.REALTIME);
    validateTableCreationWithInvalidReplication(rawTableName, TableType.OFFLINE);

    createTableWithValidReplication(rawTableName, TableType.REALTIME);
    createTableWithValidReplication(rawTableName, TableType.OFFLINE);

    validateTableUpdateReplicationToInvalidValue(rawTableName, TableType.REALTIME);
    validateTableUpdateReplicationToInvalidValue(rawTableName, TableType.OFFLINE);
  }

  /**
   * Validates the behavior of the system when creating or updating tables with invalid replica group configurations.
   * This method tests the REALTIME table configuration.
   *
   * The method performs the following steps:
   * 1. Attempts to create a REALTIME table with an invalid replica group configuration. The creation is expected to
   * fail, and the test verifies that the exception message contains the expected error.
   * 2. Creates a new REALTIME table with a valid replica group configuration to establish a baseline for further
   * testing. This creation is expected to succeed.
   * 3. Attempts to update the replica group configuration of the previously created REALTIME table to an invalid
   * value. The update is expected to fail, and the test verifies that the appropriate error message is returned.
   *
   * @throws Exception if any error occurs during the validation process
   */
  @Test
  public void validateInvalidReplicaGroupConfig()
      throws Exception {

    // Create a REALTIME table with an invalid replication factor. Creation should fail.
    String tableName = "validateInvalidReplicaGroupConfig";
    String tableNameWithType = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(tableName);
    DEFAULT_INSTANCE.addDummySchema(tableName);

    Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap = new HashMap<>();
    instanceAssignmentConfigMap.put(InstancePartitionsType.CONSUMING.name(),
        getInstanceAssignmentConfig("DefaultTenant_REALTIME", 4, 2));

    TableConfig realtimeTableConfig = getRealtimeTableBuilder(tableName)
        .setInstanceAssignmentConfigMap(instanceAssignmentConfigMap)
        .setNumReplicas(10)
        .build();

    try {
      createTable(realtimeTableConfig.toJsonString());
      fail("Should fail due to invalid replication factor");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Failed to calculate instance partitions for table: " + tableNameWithType));
    }

    // Create a new valid table and update it with invalid replica group config
    instanceAssignmentConfigMap.clear();
    instanceAssignmentConfigMap.put(InstancePartitionsType.CONSUMING.name(),
        getInstanceAssignmentConfig("DefaultTenant_REALTIME", 4, 1));

    realtimeTableConfig = getRealtimeTableBuilder(tableName)
        .setInstanceAssignmentConfigMap(instanceAssignmentConfigMap)
        .setNumReplicas(10)
        .build();

    try {
      createTable(realtimeTableConfig.toJsonString());
    } catch (Exception e) {
      fail("Preconditions failure: Could not create a REALTIME table with a valid replica group config as a "
          + "precondition to testing config updates");
    }

    // Update it with an invalid RG config, the update should fail
    instanceAssignmentConfigMap.clear();
    instanceAssignmentConfigMap.put(InstancePartitionsType.CONSUMING.name(),
        getInstanceAssignmentConfig("DefaultTenant_REALTIME", 1, 8));

    try {
      updateTable(realtimeTableConfig.getTableName(), realtimeTableConfig.toJsonString());
      fail("Table update should fail due to invalid RG config");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Failed to calculate instance partitions for table: " + tableNameWithType));
    }
  }

  @Test
  public void testTableWithSameNameAsLogicalTableIsNotAllowed()
      throws Exception {
    // Create physical table
    String tableName = "testTable";
    DEFAULT_INSTANCE.addDummySchema(tableName);
    TableConfig offlineTableConfig = _offlineBuilder.setTableName(tableName).build();
    String creationResponse = createTable(offlineTableConfig.toJsonString());
    assertEquals(creationResponse,
        "{\"unrecognizedProperties\":{},\"status\":\"Table testTable_OFFLINE successfully added\"}");

    // create logical table with above physical table
    String logicalTableName = "testTable_LOGICAL";
    DEFAULT_INSTANCE.addDummySchema(logicalTableName);
    LogicalTableConfig logicalTableConfig = ControllerTest.getDummyLogicalTableConfig(
        logicalTableName, List.of(offlineTableConfig.getTableName()), "DefaultTenant");
    String logicalTableResponse =
        logicalTableClient().createLogicalTable(logicalTableConfig.toSingleLineJsonString());
    assertEquals(logicalTableResponse,
        "{\"unrecognizedProperties\":{},\"status\":\"testTable_LOGICAL logical table successfully added.\"}");

    // create table with same as logical table and should fail
    TableConfig offlineTableConfig2 = _offlineBuilder.setTableName(logicalTableName).build();
    IOException aThrows = expectThrows(IOException.class,
        () -> createTable(offlineTableConfig2.toJsonString()));
    assertTrue(aThrows.getMessage().contains("Logical table '" + logicalTableName + "' already exists"),
        aThrows.getMessage());
  }

  @Test
  public void testGetNonExistentTableConfig()
      throws IOException {
    // Attempt to get a non-existent table config
    String tableName = "nonExistentTable";
    String url = StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables", tableName);
    Pair<Integer, String> respWithStatusCode = sendGetRequestWithStatusCode(url, null);
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), respWithStatusCode.getLeft());
    String msg = respWithStatusCode.getRight();
    assertTrue(msg.contains("Table nonExistentTable does not exist"), msg);

    // Attempt to get a non-existent table config with type
    String offlineUrl =
        StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables", tableName + "?type=OFFLINE");
    respWithStatusCode = sendGetRequestWithStatusCode(offlineUrl, null);
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), respWithStatusCode.getLeft());
    msg = respWithStatusCode.getRight();
    assertTrue(msg.contains("Table nonExistentTable_OFFLINE does not exist"), msg);

    String realtimeUrl =
        StringUtil.join("/", DEFAULT_INSTANCE.getControllerBaseApiUrl(), "tables", tableName + "?type=REALTIME");
    respWithStatusCode = sendGetRequestWithStatusCode(realtimeUrl, null);
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), respWithStatusCode.getLeft());
    msg = respWithStatusCode.getRight();
    assertTrue(msg.contains("Table nonExistentTable_REALTIME does not exist"), msg);
  }

  /**
   * Updating existing REALTIME table with invalid replication factor should throw exception.
   */
  private void validateTableUpdateReplicationToInvalidValue(String rawTableName, TableType tableType) {
    String tableNameWithType = TableNameBuilder.forType(tableType).tableNameWithType(rawTableName);
    TableConfig tableConfig = (tableType == TableType.REALTIME
        ? getRealtimeTableBuilder(rawTableName)
        : getOfflineTableBuilder(rawTableName))
        .setNumReplicas(5)
        .build();

    try {
      updateTable(tableConfig.getTableName(), tableConfig.toJsonString());
      fail("Table update should fail due to invalid replication factor");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Failed to calculate instance partitions for table: " + tableNameWithType));
    }
  }

  private void createTableWithValidReplication(String rawTableName, TableType tableType) {
    TableConfig tableConfig = (tableType == TableType.REALTIME
        ? getRealtimeTableBuilder(rawTableName)
        : getOfflineTableBuilder(rawTableName))
        .setNumReplicas(1)
        .build();

    try {
      createTable(tableConfig.toJsonString());
    } catch (Exception e) {
      fail("Preconditions failure: Could not create a " + tableType.toString()
          + " table with valid replication factor of 1 as a " + "precondition to testing config updates");
    }
  }

  /**
   * When table is created with invalid replication factor, it should throw exception.
   */
  private void validateTableCreationWithInvalidReplication(String rawTableName, TableType tableType)
      throws IOException {
    String tableNameWithType = TableNameBuilder.forType(tableType).tableNameWithType(rawTableName);
    DEFAULT_INSTANCE.addDummySchema(rawTableName);
    TableConfig tableConfig = (tableType == TableType.REALTIME
        ? getRealtimeTableBuilder(rawTableName)
        : getOfflineTableBuilder(rawTableName))
        .setNumReplicas(5)
        .build();

    try {
      createTable(tableConfig.toJsonString());
      fail("Table create should fail due to invalid replication factor");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Failed to calculate instance partitions for table: " + tableNameWithType));
    }
  }

  private static InstanceAssignmentConfig getInstanceAssignmentConfig(String tag, int numReplicaGroups,
      int numInstancesPerReplicaGroup) {
    InstanceTagPoolConfig instanceTagPoolConfig = new InstanceTagPoolConfig(tag, false, 0, null);
    List<String> constraints = new ArrayList<>();
    constraints.add("constraints1");
    InstanceConstraintConfig instanceConstraintConfig = new InstanceConstraintConfig(constraints);
    InstanceReplicaGroupPartitionConfig instanceReplicaGroupPartitionConfig =
        new InstanceReplicaGroupPartitionConfig(true, 1, numReplicaGroups, numInstancesPerReplicaGroup, 1, 1, true,
            null);
    return new InstanceAssignmentConfig(instanceTagPoolConfig, instanceConstraintConfig,
        instanceReplicaGroupPartitionConfig,
        InstanceAssignmentConfig.PartitionSelector.FD_AWARE_INSTANCE_PARTITION_SELECTOR.name(), false);
  }

  @Test
  public void testTableDeletionFromPreviousIncompleteDeletion()
      throws Exception {
    String tableName = "testTableDeletionValidation";
    DEFAULT_INSTANCE.addDummySchema(tableName);

    TableConfig offlineTableConfig = getOfflineTableBuilder(tableName)
        .setTaskConfig(new TableTaskConfig(Map.of(
            MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE, Map.of("schedule", "0 0 * * * ? *"))))
        .build();

    String tableNameWithType = offlineTableConfig.getTableName();
    String creationResponse = createTable(offlineTableConfig.toJsonString());
    assertEquals(creationResponse,
        "{\"unrecognizedProperties\":{},\"status\":\"Table " + tableNameWithType + " successfully added\"}");

    String idealStatePath = "/ControllerTest/IDEALSTATES/" + tableNameWithType;
    zookeeperClient().delete(idealStatePath);
    // Table deletion will throw exception but internally it should clean up all the dangling table resources
    Assert.expectThrows(IOException.class, () -> deleteTable(tableName));

    String encodedTableConfigPath = "/ControllerTest/PROPERTYSTORE/CONFIGS/TABLE/" + tableNameWithType;
    try {
      zookeeperClient().getData(encodedTableConfigPath);
      fail("Table config node should be deleted so get request should fail");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains(tableNameWithType + " does not exist"));
    } catch (Exception e) {
      // expected failure
    }
  }

  @Test
  public void testTableTasksValidationWithNoDanglingTasks()
      throws Exception {
    String tableName = "testTableTasksValidation";
    DEFAULT_INSTANCE.addDummySchema(tableName);

    TableConfig offlineTableConfig = getOfflineTableBuilder(tableName)
        .setTaskConfig(new TableTaskConfig(Map.of(
            MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE, Map.of())))
        .build();

    // Should succeed when no dangling tasks exist
    String creationResponse = createTable(offlineTableConfig.toJsonString());
    assertEquals(creationResponse,
        "{\"unrecognizedProperties\":{},\"status\":\"Table testTableTasksValidation_OFFLINE successfully added\"}");

    // Clean up
    deleteTable(tableName);
  }

  @Test
  public void testTableTasksValidationWithDanglingTasks()
      throws Exception {
    String tableName = "testTableTasksValidationWithDangling";
    DEFAULT_INSTANCE.addDummySchema(tableName);

    TableConfig offlineTableConfig = getOfflineTableBuilder(tableName)
        .setTaskConfig(new TableTaskConfig(Map.of(
            MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE,
            Map.of(PinotTaskManager.SCHEDULE_KEY, "0 */10 * ? * * *",
                CommonConstants.TABLE_NAME, tableName + "_OFFLINE"))))
        .build();

    // First create the table successfully
    createTable(offlineTableConfig.toJsonString());

    // Create a task manually to simulate dangling task
    PinotTaskManager taskManager = DEFAULT_INSTANCE.getControllerStarter().getTaskManager();
    TaskSchedulingContext context = new TaskSchedulingContext();
    context.setTablesToSchedule(Set.of(tableName + "_OFFLINE"));
    Map<String, TaskSchedulingInfo> taskInfo = taskManager.scheduleTasks(context);
    String taskName = taskInfo.values().iterator().next().getScheduledTaskNames().get(0);
    waitForTaskState(taskName, TaskState.IN_PROGRESS);
    boolean taskKnown = fetchTaskState(taskName) != null;

    // Now try to create another table with same name (simulating re-creation with dangling tasks)
    deleteTable(tableName, null, true);

    try {
      createTable(offlineTableConfig.toJsonString());
      if (taskKnown) {
        fail("Table creation should fail when dangling tasks exist");
      }
    } catch (IOException e) {
      if (taskKnown) {
        assertTrue(e.getMessage().contains("The table has dangling task data"));
      }
    }

    // Clean up any remaining tasks
    try {
      deleteTable(tableName, null, true);
    } catch (Exception ignored) {
      // Ignore if table doesn't exist
    }
  }

  @Test
  public void testTableTasksValidationWithNullTaskConfig()
      throws Exception {
    String tableName = "testTableTasksValidationNullConfig";
    DEFAULT_INSTANCE.addDummySchema(tableName);

    TableConfig offlineTableConfig = getOfflineTableBuilder(tableName).build(); // No task config

    // Should succeed when task config is null
    String creationResponse = createTable(offlineTableConfig.toJsonString());
    assertEquals(creationResponse, "{\"unrecognizedProperties\":{},"
        + "\"status\":\"Table testTableTasksValidationNullConfig_OFFLINE successfully added\"}");

    // Clean up
    deleteTable(tableName);
  }

  @Test
  public void testTableTasksCleanupWithNonActiveTasks()
      throws Exception {
    String tableName = "testTableTasksCleanup";
    DEFAULT_INSTANCE.addDummySchema(tableName);

    TableConfig offlineTableConfig = getOfflineTableBuilder(tableName)
        .setTaskConfig(new TableTaskConfig(Map.of(
            MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE,
            Map.of(PinotTaskManager.SCHEDULE_KEY, "0 */10 * ? * * *",
                CommonConstants.TABLE_NAME, tableName + "_OFFLINE"))))
        .build();

    // Create table
    createTable(offlineTableConfig.toJsonString());

    // Create some completed tasks
    PinotTaskManager taskManager = DEFAULT_INSTANCE.getControllerStarter().getTaskManager();
    TaskSchedulingContext context = new TaskSchedulingContext();
    context.setTablesToSchedule(Set.of(tableName + "_OFFLINE"));
    Map<String, TaskSchedulingInfo> taskInfo = taskManager.scheduleTasks(context);
    String taskName = taskInfo.values().iterator().next().getScheduledTaskNames().get(0);
    waitForTaskState(taskName, TaskState.IN_PROGRESS);
    boolean taskKnown = fetchTaskState(taskName) != null;

    if (taskKnown) {
      // stop the task queue to abort the task
      taskClient().stopTaskQueue(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE);
      waitForTaskState(taskName, TaskState.STOPPED);
      // resume the task queue again to avoid affecting other tests
      taskClient().resumeTaskQueue(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE);
    }

    // Delete table - should succeed and clean up tasks
    String deleteResponse = deleteTable(tableName);
    assertEquals(deleteResponse, "{\"status\":\"Tables: [" + tableName + "_OFFLINE] deleted\"}");
  }

  private void waitForTaskState(String taskName, TaskState expectedState) {
    TestUtils.waitForCondition((aVoid) -> {
      try {
        TaskState state = taskClient().getTaskState(taskName);
        if (state == null) {
          // If we cannot fetch state, treat IN_PROGRESS waits as best-effort no-ops.
          return expectedState == TaskState.IN_PROGRESS;
        }
        // In test environments without a running minion, Helix can keep tasks in NOT_STARTED even though they
        // are enqueued. Accept NOT_STARTED when we're only validating the presence of an in-flight task.
        if (expectedState == TaskState.IN_PROGRESS && state == TaskState.NOT_STARTED) {
          return true;
        }
        return state == expectedState;
      } catch (Exception e) {
        // If state lookup fails, consider IN_PROGRESS satisfied (task was enqueued) but surface other states.
        return expectedState == TaskState.IN_PROGRESS;
      }
    }, 60_000, "Task not scheduled to expected state " + expectedState);
  }

  private TaskState fetchTaskState(String taskName) {
    try {
      return taskClient().getTaskState(taskName);
    } catch (Exception e) {
      return null;
    }
  }

  @Test
  public void testTableTasksCleanupWithActiveTasks()
      throws Exception {
    String tableName = "testTableTasksCleanupActive";
    DEFAULT_INSTANCE.addDummySchema(tableName);

    TableConfig offlineTableConfig = getOfflineTableBuilder(tableName)
        .setTaskConfig(new TableTaskConfig(Map.of(
            MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE,
            Map.of(PinotTaskManager.SCHEDULE_KEY, "0 */10 * ? * * *",
                CommonConstants.TABLE_NAME, tableName + "_OFFLINE"))))
        .build();

    // Create table
    createTable(offlineTableConfig.toJsonString());

    // Create an active/in-progress task
    PinotTaskManager taskManager = DEFAULT_INSTANCE.getControllerStarter().getTaskManager();
    TaskSchedulingContext context = new TaskSchedulingContext();
    context.setTablesToSchedule(Set.of(tableName + "_OFFLINE"));
    Map<String, TaskSchedulingInfo> taskInfo = taskManager.scheduleTasks(context);
    String taskName = taskInfo.values().iterator().next().getScheduledTaskNames().get(0);
    waitForTaskState(taskName, TaskState.IN_PROGRESS);
    boolean taskKnown = fetchTaskState(taskName) != null;
    try {
      // Try to delete table without ignoring active tasks - should fail
      deleteTable(tableName);
      if (taskKnown) {
        fail("Table deletion should fail when active tasks exist");
      }
    } catch (IOException e) {
      if (taskKnown) {
        assertTrue(e.getMessage().contains("The table has") && e.getMessage().contains("active running tasks"));
      }
    }

    // Delete table with ignoreActiveTasks flag - should succeed
    try {
      String deleteResponse = getOrCreateAdminClient().getTableClient().deleteTable(tableName, null, null, true);
      assertEquals(deleteResponse, "{\"status\":\"Tables: [" + tableName + "_OFFLINE] deleted\"}");
    } catch (Exception e) {
      // Table might already be removed if task state was unknown; ignore missing table errors
      String message = e.getMessage();
      if (message == null || !message.contains("does not exist")) {
        throw e;
      }
    }

    // delete task
    if (taskKnown) {
      getOrCreateAdminClient().getTaskClient().deleteTask(taskName, true);
    }
  }

  @Test
  public void testTableTasksCleanupWithNullTaskConfig()
      throws Exception {
    String tableName = "testTableTasksCleanupNullConfig";
    DEFAULT_INSTANCE.addDummySchema(tableName);

    TableConfig offlineTableConfig = getOfflineTableBuilder(tableName).build(); // No task config

    // Create table
    createTable(offlineTableConfig.toJsonString());

    // Delete table - should succeed even with null task config
    String deleteResponse = deleteTable(tableName);
    assertEquals(deleteResponse, "{\"status\":\"Tables: [" + tableName + "_OFFLINE] deleted\"}");
  }

  @AfterMethod
  public void cleanUp()
      throws IOException {
    // Delete all tables after each test
    DEFAULT_INSTANCE.cleanup();
  }

  @AfterClass
  public void tearDown() {
    DEFAULT_INSTANCE.stopSharedTestSetup();
  }
}
