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
import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.client.admin.PinotAdminClient;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.spi.config.TableConfigs;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TunerConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import static org.testng.Assert.fail;


/**
 * Tests for CRUD APIs of {@link TableConfigs}
 */
public class TableConfigsRestletResourceTest extends ControllerTest {
  @BeforeClass
  public void setUp()
      throws Exception {
    DEFAULT_INSTANCE.setupSharedStateAndValidate();
  }

  private TableConfigBuilder getBaseTableConfigBuilder(String tableName, TableType tableType) {
    if (tableType == TableType.OFFLINE) {
      return new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).setTimeColumnName("timeColumn")
          .setRetentionTimeUnit("DAYS").setRetentionTimeValue("50");
    } else {
      StreamConfig streamConfig = FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs();
      return new TableConfigBuilder(TableType.REALTIME).setTableName(tableName).setTimeColumnName("timeColumn")
          .setRetentionTimeUnit("DAYS").setRetentionTimeValue("5").setStreamConfigs(streamConfig.getStreamConfigsMap());
    }
  }

  private TableConfig createOfflineTableConfig(String tableName) {
    return getBaseTableConfigBuilder(tableName, TableType.OFFLINE).build();
  }

  private TableConfig createRealtimeTableConfig(String tableName) {
    return getBaseTableConfigBuilder(tableName, TableType.REALTIME).build();
  }

  private TableConfig createOfflineTunerTableConfig(String tableName) {
    return getBaseTableConfigBuilder(tableName, TableType.OFFLINE).setTunerConfigList(
        Lists.newArrayList(new TunerConfig("realtimeAutoIndexTuner", null))).build();
  }

  private TableConfig createRealtimeTunerTableConfig(String tableName) {
    return getBaseTableConfigBuilder(tableName, TableType.REALTIME).setTunerConfigList(
        Lists.newArrayList(new TunerConfig("realtimeAutoIndexTuner", null))).build();
  }

  private TableConfig createOfflineDimTableConfig(String tableName) {
    return getBaseTableConfigBuilder(tableName, TableType.OFFLINE).setIsDimTable(true).build();
  }

  @Test
  public void testValidateConfig()
      throws Exception {
    PinotAdminClient adminClient = getOrCreateAdminClient();

    String tableName = "testValidate";
    TableConfig offlineTableConfig = createOfflineTableConfig(tableName);
    TableConfig realtimeTableConfig = createRealtimeTableConfig(tableName);
    Schema schema = createDummySchema(tableName);
    TableConfigs tableConfigs;

    // invalid json
    try {
      tableConfigs = new TableConfigs(tableName, schema, offlineTableConfig, realtimeTableConfig);
      adminClient.getTableClient()
          .validateTableConfigs(tableConfigs.toPrettyJsonString().replace("\"offline\"", "offline\""), null);
      fail("Creation of a TableConfigs with invalid json string should have failed");
    } catch (Exception e) {
      // expected
    }

    // null table configs
    try {
      tableConfigs = new TableConfigs(tableName, schema, null, null);
      adminClient.getTableClient().validateTableConfigs(tableConfigs.toPrettyJsonString(), null);
      fail("Creation of an TableConfigs with null table offline tableConfig and realtime tableConfig should have "
          + "failed");
    } catch (Exception e) {
      // expected
    }

    // null schema
    try {
      tableConfigs = new TableConfigs(tableName, null, offlineTableConfig, null);
      adminClient.getTableClient().validateTableConfigs(tableConfigs.toPrettyJsonString(), null);
      fail("Creation of an TableConfigs with null schema should have failed");
    } catch (Exception e) {
      // expected
    }

    // empty config name
    try {
      tableConfigs = new TableConfigs("", schema, offlineTableConfig, realtimeTableConfig);
      adminClient.getTableClient().validateTableConfigs(tableConfigs.toPrettyJsonString(), null);
      fail("Creation of an TableConfigs with empty config name should have failed");
    } catch (Exception e) {
      // expected
    }

    // schema name doesn't match config name
    try {
      tableConfigs =
          new TableConfigs(tableName, createDummySchema("differentName"), offlineTableConfig, realtimeTableConfig);
      adminClient.getTableClient().validateTableConfigs(tableConfigs.toPrettyJsonString(), null);
      fail("Creation of an TableConfigs with schema name different than tableName should have failed");
    } catch (Exception e) {
      // expected
    }

    // schema validation fails
    try {
      Schema schemaWithBlankSpace = createDummySchema(tableName);
      schemaWithBlankSpace.addField(new MetricFieldSpec("blank space", FieldSpec.DataType.LONG));
      tableConfigs = new TableConfigs(tableName, schemaWithBlankSpace, offlineTableConfig, realtimeTableConfig);
      adminClient.getTableClient().validateTableConfigs(tableConfigs.toPrettyJsonString(), null);
      fail("Creation of an TableConfigs with blank space in column should have failed");
    } catch (Exception e) {
      // expected
    }

    // offline table name doesn't match config name
    try {
      tableConfigs = new TableConfigs(tableName, schema, createOfflineTableConfig("differentName"), null);
      adminClient.getTableClient().validateTableConfigs(tableConfigs.toPrettyJsonString(), null);
      fail("Creation of an TableConfigs with offline table name different than tableName should have failed");
    } catch (Exception e) {
      // expected
    }

    // table name validation fails
    try {
      tableConfigs =
          new TableConfigs("blank space", createDummySchema("blank space"), createOfflineTableConfig("blank space"),
              null);
      adminClient.getTableClient().validateTableConfigs(tableConfigs.toPrettyJsonString(), null);
      fail("Creation of an TableConfigs with blank space in table name should have failed");
    } catch (Exception e) {
      // expected
    }

    // table validation fails
    try {
      TableConfig invalidTableConfig = createOfflineTableConfig(tableName);
      invalidTableConfig.getIndexingConfig().setInvertedIndexColumns(Lists.newArrayList("nonExistent"));
      tableConfigs = new TableConfigs(tableName, schema, invalidTableConfig, null);
      adminClient.getTableClient().validateTableConfigs(tableConfigs.toPrettyJsonString(), null);
      fail("Creation of an TableConfigs with invalid table config should have failed");
    } catch (Exception e) {
      // expected
    }

    // realtime table name doesn't match config name
    try {
      tableConfigs = new TableConfigs(tableName, schema, null, createRealtimeTableConfig("differentName"));
      adminClient.getTableClient().validateTableConfigs(tableConfigs.toPrettyJsonString(), null);
      fail("Creation of an TableConfigs with realtime table name different than tableName should have failed");
    } catch (Exception e) {
      // expected
    }

    // table name validation fails
    try {
      tableConfigs = new TableConfigs("blank space", createDummySchema("blank space"), null,
          createRealtimeTableConfig("blank space"));
      adminClient.getTableClient().validateTableConfigs(tableConfigs.toPrettyJsonString(), null);
      fail("Creation of an TableConfigs with blank space in table name should have failed");
    } catch (Exception e) {
      // expected
    }

    // table validation fails
    try {
      TableConfig invalidTableConfig = createRealtimeTableConfig(tableName);
      invalidTableConfig.getIndexingConfig().setInvertedIndexColumns(Lists.newArrayList("nonExistent"));
      tableConfigs = new TableConfigs(tableName, schema, null, invalidTableConfig);
      adminClient.getTableClient().validateTableConfigs(tableConfigs.toPrettyJsonString(), null);
      fail("Creation of an TableConfigs with invalid table config should have failed");
    } catch (Exception e) {
      // expected
    }

    // hybrid config consistency check fails
    try {
      Schema twoTimeColumns = createDummySchema(tableName);
      twoTimeColumns.addField(
          new DateTimeFieldSpec("time1", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS"));
      twoTimeColumns.addField(
          new DateTimeFieldSpec("time2", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS"));
      TableConfig offlineTableConfig1 = createOfflineTableConfig(tableName);
      offlineTableConfig1.getValidationConfig().setTimeColumnName("time1");
      TableConfig realtimeTableConfig1 = createRealtimeTableConfig(tableName);
      realtimeTableConfig1.getValidationConfig().setTimeColumnName("time2");
      tableConfigs = new TableConfigs(tableName, twoTimeColumns, offlineTableConfig1, realtimeTableConfig1);
      adminClient.getTableClient().validateTableConfigs(tableConfigs.toPrettyJsonString(), null);
      fail("Creation of an TableConfigs with inconsistencies across offline and realtime table config should have "
          + "failed");
    } catch (Exception e) {
      // expected
    }

    // table name check fails when database context is not passed in header but one of the configs has database prefix
    Schema dummySchema = createDummySchema(tableName);
    TableConfig offlineTableConfig1 = createOfflineTableConfig("db1." + tableName);
    TableConfig realtimeTableConfig1 = createRealtimeTableConfig(tableName);
    tableConfigs = new TableConfigs(tableName, dummySchema, offlineTableConfig1, realtimeTableConfig1);
    try {
      adminClient.getTableClient().validateTableConfigs(tableConfigs.toPrettyJsonString(), null);
      fail("Creation of an TableConfigs without database context in header but provided in one of the configs should "
          + "fail");
    } catch (Exception e) {
      // expected
    }
    // fails with schema as well
    offlineTableConfig1.setTableName(TableNameBuilder.OFFLINE.tableNameWithType(tableName));
    dummySchema.setSchemaName("db1." + tableName);
    try {
      adminClient.getTableClient().validateTableConfigs(tableConfigs.toPrettyJsonString(), null);
      fail("Creation of an TableConfigs without database context in header but provided in one of the configs should "
          + "fail");
    } catch (Exception e) {
      // expected
    }

    // fails even though both configs and schema have the database prefix in the table names but
    // database context is not passed in header
    tableConfigs.setTableName("db1." + tableName);
    try {
      adminClient.getTableClient().validateTableConfigs(tableConfigs.toPrettyJsonString(), null);
      fail("Creation of an TableConfigs without database context in header but provided in all of the configs should "
          + "fail");
    } catch (Exception e) {
      // expected
    }

    // successfully created with all 3 configs when database context is passed in header and configs may or may not
    // have the database prefix in the table names
    Map<String, String> headers = new HashMap<>();
    tableConfigs.setTableName(tableName);
    headers.put(CommonConstants.DATABASE, "db1");
    // only schema has the database prefix
    dummySchema.setSchemaName("db1." + tableName);
    adminClient.executeRequest("POST", "/tableConfigs/validate", tableConfigs.toPrettyJsonString(), null, headers);
    // one of the table config has database prefix
    offlineTableConfig1.setTableName(TableNameBuilder.OFFLINE.tableNameWithType(tableName));
    adminClient.executeRequest("POST", "/tableConfigs/validate", tableConfigs.toPrettyJsonString(), null, headers);

    // successfully created with all 3 configs
    String tableName1 = "testValidate1";
    tableConfigs = new TableConfigs(tableName1, createDummySchema(tableName1), createOfflineTableConfig(tableName1),
        createRealtimeTableConfig(tableName1));
    adminClient.getTableClient().validateTableConfigs(tableConfigs.toPrettyJsonString(), null);

    // successfully create with offline config
    String tableName2 = "testValidate2";
    tableConfigs =
        new TableConfigs(tableName2, createDummySchema(tableName2), createOfflineTableConfig(tableName2), null);
    adminClient.getTableClient().validateTableConfigs(tableConfigs.toPrettyJsonString(), null);

    // successfully create with realtime config
    String tableName3 = "testValidate3";
    tableConfigs =
        new TableConfigs(tableName3, createDummySchema(tableName3), null, createRealtimeTableConfig(tableName3));
    adminClient.getTableClient().validateTableConfigs(tableConfigs.toPrettyJsonString(), null);

    adminClient.getTableClient().deleteTableConfigs(tableName1, null);
    adminClient.getTableClient().deleteTableConfigs(tableName2, null);
    adminClient.getTableClient().deleteTableConfigs(tableName3, null);
  }

  /**
   * Tests for creation of TableConfigs
   */
  @Test
  public void testCreateConfig()
      throws Exception {
    PinotAdminClient adminClient = getOrCreateAdminClient();
    String tableName = "testCreate";
    TableConfig offlineTableConfig = createOfflineTableConfig(tableName);
    TableConfig realtimeTableConfig = createRealtimeTableConfig(tableName);
    Schema schema = createDummySchema(tableName);
    TableConfigs tableConfigs = new TableConfigs(tableName, schema, offlineTableConfig, realtimeTableConfig);
    adminClient.getTableClient().createTableConfigs(tableConfigs.toPrettyJsonString(), null, null);
    String response = adminClient.getTableClient().getTableConfigs(tableName);
    TableConfigs tableConfigsResponse = JsonUtils.stringToObject(response, TableConfigs.class);
    Assert.assertEquals(tableConfigsResponse.getTableName(), tableName);
    Assert.assertEquals(tableConfigsResponse.getOffline().getTableName(), offlineTableConfig.getTableName());
    Assert.assertEquals(tableConfigsResponse.getRealtime().getTableName(), realtimeTableConfig.getTableName());
    Assert.assertEquals(tableConfigsResponse.getSchema().getSchemaName(), schema.getSchemaName());

    // test POST of existing configs fails
    try {
      adminClient.getTableClient().createTableConfigs(tableConfigs.toPrettyJsonString(), null, null);
      fail("Should fail for trying to add existing config");
    } catch (Exception e) {
      // expected
    }

    adminClient.getTableClient().deleteTableConfigs(tableName, null);

    // replica check
    tableName = "testCreateReplicas";
    TableConfig replicaTestOfflineTableConfig = createOfflineTableConfig(tableName);
    TableConfig replicaTestRealtimeTableConfig = createRealtimeTableConfig(tableName);
    replicaTestOfflineTableConfig.getValidationConfig().setReplication("1");
    replicaTestRealtimeTableConfig.getValidationConfig().setReplication("1");
    tableConfigs = new TableConfigs(tableName, createDummySchema(tableName), replicaTestOfflineTableConfig,
        replicaTestRealtimeTableConfig);
    adminClient.getTableClient().createTableConfigs(tableConfigs.toPrettyJsonString(), null, null);
    response = adminClient.getTableClient().getTableConfigs(tableName);
    tableConfigsResponse = JsonUtils.stringToObject(response, TableConfigs.class);
    Assert.assertEquals(tableConfigsResponse.getTableName(), tableName);
    Assert.assertEquals(tableConfigsResponse.getOffline().getReplication(),
        DEFAULT_MIN_NUM_REPLICAS);
    Assert.assertEquals(tableConfigsResponse.getRealtime().getReplication(),
        DEFAULT_MIN_NUM_REPLICAS);
    adminClient.getTableClient().deleteTableConfigs(tableName, null);

    // quota check
    tableName = "testCreateQuota";
    TableConfig offlineDimTableConfig = createOfflineDimTableConfig(tableName);
    Schema dimSchema = createDummySchemaWithPrimaryKey(tableName);
    tableConfigs = new TableConfigs(tableName, dimSchema, offlineDimTableConfig, null);
    adminClient.getTableClient().createTableConfigs(tableConfigs.toPrettyJsonString(), null, null);
    response = adminClient.getTableClient().getTableConfigs(tableName);
    tableConfigsResponse = JsonUtils.stringToObject(response, TableConfigs.class);
    Assert.assertEquals(tableName, tableConfigsResponse.getTableName());
    Assert.assertEquals(tableConfigsResponse.getOffline().getQuotaConfig().getStorage(),
        DEFAULT_INSTANCE.getControllerConfig().getDimTableMaxSize());
    adminClient.getTableClient().deleteTableConfigs(tableName, null);

    // tuner config
    tableName = "testTunerConfig";
    TableConfig offlineTunerTableConfig = createOfflineTunerTableConfig(tableName);
    TableConfig realtimeTunerTableConfig = createRealtimeTunerTableConfig(tableName);
    tableConfigs =
        new TableConfigs(tableName, createDummySchema(tableName), offlineTunerTableConfig, realtimeTunerTableConfig);
    adminClient.getTableClient().createTableConfigs(tableConfigs.toPrettyJsonString(), null, null);
    response = adminClient.getTableClient().getTableConfigs(tableName);
    tableConfigsResponse = JsonUtils.stringToObject(response, TableConfigs.class);
    Assert.assertEquals(tableName, tableConfigsResponse.getTableName());
    Assert.assertTrue(tableConfigsResponse.getOffline().getIndexingConfig().getInvertedIndexColumns()
        .containsAll(schema.getDimensionNames()));
    Assert.assertTrue(tableConfigsResponse.getOffline().getIndexingConfig().getNoDictionaryColumns()
        .containsAll(schema.getMetricNames()));
    Assert.assertTrue(tableConfigsResponse.getRealtime().getIndexingConfig().getInvertedIndexColumns()
        .containsAll(schema.getDimensionNames()));
    Assert.assertTrue(tableConfigsResponse.getRealtime().getIndexingConfig().getNoDictionaryColumns()
        .containsAll(schema.getMetricNames()));
    adminClient.getTableClient().deleteTableConfigs(tableName, null);
  }

  @Test
  public void testListConfigs()
      throws Exception {
    PinotAdminClient adminClient = getOrCreateAdminClient();
    // create with 1 config
    String tableName1 = "testList1";
    TableConfig offlineTableConfig = createOfflineTableConfig(tableName1);
    TableConfig realtimeTableConfig = createRealtimeTableConfig(tableName1);
    Schema schema = createDummySchema(tableName1);
    TableConfigs tableConfigs = new TableConfigs(tableName1, schema, offlineTableConfig, null);
    adminClient.getTableClient().createTableConfigs(tableConfigs.toPrettyJsonString(), null, null);

    // list
    List<String> configs = adminClient.getTableClient().listTableConfigs();
    Assert.assertEquals(configs.size(), 1);
    Assert.assertTrue(configs.containsAll(Sets.newHashSet(tableName1)));

    // update to 2
    tableConfigs = new TableConfigs(tableName1, schema, offlineTableConfig, realtimeTableConfig);
    adminClient.getTableClient().updateTableConfigs(tableName1, tableConfigs.toPrettyJsonString(), null, false, false);

    // list
    configs = adminClient.getTableClient().listTableConfigs();
    Assert.assertEquals(configs.size(), 1);
    Assert.assertTrue(configs.containsAll(Sets.newHashSet("testList1")));

    // create new
    String tableName2 = "testList2";
    offlineTableConfig = createOfflineTableConfig(tableName2);
    schema = createDummySchema(tableName2);
    tableConfigs = new TableConfigs(tableName2, schema, offlineTableConfig, null);
    adminClient.getTableClient().createTableConfigs(tableConfigs.toPrettyJsonString(), null, null);

    // list
    configs = adminClient.getTableClient().listTableConfigs();
    Assert.assertEquals(configs.size(), 2);
    Assert.assertTrue(configs.containsAll(Sets.newHashSet(tableName1, tableName2)));

    // delete 1
    adminClient.getTableClient().deleteTableConfigs(tableName2, null);

    // list 1
    configs = adminClient.getTableClient().listTableConfigs();
    Assert.assertEquals(configs.size(), 1);
    Assert.assertTrue(configs.containsAll(Sets.newHashSet(tableName1)));

    adminClient.getTableClient().deleteTableConfigs(tableName1, null);
  }

  @Test
  public void testUpdateConfig()
      throws Exception {
    PinotAdminClient adminClient = getOrCreateAdminClient();

    // create with 1
    String tableName = "testUpdate1";
    TableConfig offlineTableConfig = createOfflineTableConfig(tableName);
    TableConfig realtimeTableConfig = createRealtimeTableConfig(tableName);
    Schema schema = createDummySchema(tableName);
    TableConfigs tableConfigs = new TableConfigs(tableName, schema, offlineTableConfig, null);
    // PUT before POST should fail
    try {
      adminClient.getTableClient()
          .updateTableConfigs(tableName, tableConfigs.toPrettyJsonString(), null, false, false);
      fail("Should fail for trying to PUT config before creating via POST");
    } catch (Exception e) {
      // expected
    }
    adminClient.getTableClient().createTableConfigs(tableConfigs.toPrettyJsonString(), null, null);
    String response = adminClient.getTableClient().getTableConfigs(tableName);
    TableConfigs tableConfigsResponse = JsonUtils.stringToObject(response, TableConfigs.class);
    Assert.assertEquals(tableConfigsResponse.getTableName(), tableName);
    Assert.assertEquals(tableConfigsResponse.getOffline().getTableName(), offlineTableConfig.getTableName());
    Assert.assertNull(tableConfigs.getRealtime());
    Assert.assertEquals(tableConfigsResponse.getSchema().getSchemaName(), schema.getSchemaName());

    // list
    List<String> configs = adminClient.getTableClient().listTableConfigs();
    Assert.assertEquals(configs.size(), 1);
    Assert.assertTrue(configs.containsAll(Sets.newHashSet(tableName)));

    // update to 2
    tableConfigs = new TableConfigs(tableName, tableConfigsResponse.getSchema(), tableConfigsResponse.getOffline(),
        realtimeTableConfig);
    adminClient.getTableClient().updateTableConfigs(tableName, tableConfigs.toPrettyJsonString(), null, false, false);
    response = adminClient.getTableClient().getTableConfigs(tableName);
    tableConfigsResponse = JsonUtils.stringToObject(response, TableConfigs.class);
    Assert.assertEquals(tableConfigsResponse.getTableName(), tableName);
    Assert.assertEquals(tableConfigsResponse.getOffline().getTableName(), offlineTableConfig.getTableName());
    Assert.assertEquals(tableConfigsResponse.getRealtime().getTableName(), realtimeTableConfig.getTableName());
    Assert.assertEquals(tableConfigsResponse.getSchema().getSchemaName(), schema.getSchemaName());

    // list
    configs = adminClient.getTableClient().listTableConfigs();
    Assert.assertEquals(configs.size(), 1);
    Assert.assertTrue(configs.containsAll(Sets.newHashSet(tableName)));

    // update existing config
    schema.addField(new MetricFieldSpec("newMetric", FieldSpec.DataType.LONG));
    tableConfigs =
        new TableConfigs(tableName, schema, tableConfigsResponse.getOffline(), tableConfigsResponse.getRealtime());
    adminClient.getTableClient().updateTableConfigs(tableName, tableConfigs.toPrettyJsonString(), null, false, false);
    response = adminClient.getTableClient().getTableConfigs(tableName);
    tableConfigsResponse = JsonUtils.stringToObject(response, TableConfigs.class);
    Assert.assertEquals(tableConfigsResponse.getTableName(), tableName);
    Assert.assertEquals(tableConfigsResponse.getOffline().getTableName(), offlineTableConfig.getTableName());
    Assert.assertEquals(tableConfigsResponse.getRealtime().getTableName(), realtimeTableConfig.getTableName());
    Assert.assertEquals(tableConfigsResponse.getSchema().getSchemaName(), schema.getSchemaName());
    Assert.assertTrue(tableConfigsResponse.getSchema().getMetricNames().contains("newMetric"));

    tableConfigsResponse.getOffline().getIndexingConfig().setInvertedIndexColumns(Lists.newArrayList("dimA"));
    tableConfigsResponse.getRealtime().getIndexingConfig().setInvertedIndexColumns(Lists.newArrayList("dimA"));
    tableConfigs =
        new TableConfigs(tableName, schema, tableConfigsResponse.getOffline(), tableConfigsResponse.getRealtime());
    adminClient.getTableClient().updateTableConfigs(tableName, tableConfigs.toPrettyJsonString(), null, false, false);
    response = adminClient.getTableClient().getTableConfigs(tableName);
    tableConfigsResponse = JsonUtils.stringToObject(response, TableConfigs.class);
    Assert.assertTrue(tableConfigsResponse.getOffline().getIndexingConfig().getInvertedIndexColumns().contains("dimA"));
    Assert.assertTrue(
        tableConfigsResponse.getRealtime().getIndexingConfig().getInvertedIndexColumns().contains("dimA"));

    adminClient.getTableClient().deleteTableConfigs(tableName, null);
  }

  @Test
  public void testForceUpdateTableSchemaAndConfigs()
      throws Exception {
    PinotAdminClient adminClient = getOrCreateAdminClient();
    String tableName = "testUpdate1";
    TableConfig offlineTableConfig = createOfflineTableConfig(tableName);
    Schema schema = createDummySchema(tableName);
    TableConfigs tableConfigs = new TableConfigs(tableName, schema, offlineTableConfig, null);

    adminClient.getTableClient().createTableConfigs(tableConfigs.toPrettyJsonString(), null, null);
    String response = adminClient.getTableClient().getTableConfigs(tableName);
    TableConfigs tableConfigsResponse = JsonUtils.stringToObject(response, TableConfigs.class);
    Assert.assertNotNull(tableConfigs.getOffline());

    // Remove field from schema and try to update schema without the 'forceTableSchemaUpdate' option
    schema.removeField("dimA");
    tableConfigs =
        new TableConfigs(tableName, schema, tableConfigsResponse.getOffline(), tableConfigsResponse.getRealtime());

    try {
      adminClient.getTableClient()
          .updateTableConfigs(tableName, tableConfigs.toPrettyJsonString(), null, false, false);
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage().contains("is not backward-compatible with the existing schema"));
    }

    // Skip validate table configs – Exception is still thrown
    try {
      adminClient.getTableClient()
          .updateTableConfigs(tableName, tableConfigs.toPrettyJsonString(), "ALL", false, false);
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage().contains("is not backward-compatible with the existing schema"));
    }

    // Skip table config validation as well as force update the table schema – no exceptions are thrown
    response =
        adminClient.getTableClient()
            .updateTableConfigs(tableName, tableConfigs.toPrettyJsonString(), "ALL", false, true);
    Assert.assertTrue(response.contains("TableConfigs updated for testUpdate1"));
    adminClient.getTableClient().deleteTableConfigs(tableName, null);
  }

  @Test
  public void testDeleteConfig()
      throws Exception {
    PinotAdminClient adminClient = getOrCreateAdminClient();
    // create with 1 config
    String tableName = "testDelete1";
    TableConfig offlineTableConfig = createOfflineTableConfig(tableName);
    Schema schema = createDummySchema(tableName);
    TableConfigs tableConfigs = new TableConfigs(tableName, schema, offlineTableConfig, null);
    adminClient.getTableClient().createTableConfigs(tableConfigs.toPrettyJsonString(), null, null);
    String response = adminClient.getTableClient().getTableConfigs(tableName);
    TableConfigs tableConfigsResponse = JsonUtils.stringToObject(response, TableConfigs.class);
    Assert.assertEquals(tableConfigsResponse.getTableName(), tableName);

    // delete & check
    adminClient.getTableClient().deleteTableConfigs(tableName, null);
    List<String> configs = adminClient.getTableClient().listTableConfigs();
    Assert.assertEquals(configs.size(), 0);

    tableName = "testDelete2";
    offlineTableConfig = createOfflineTableConfig(tableName);
    TableConfig realtimeTableConfig = createRealtimeTableConfig(tableName);
    schema = createDummySchema(tableName);
    tableConfigs = new TableConfigs(tableName, schema, offlineTableConfig, realtimeTableConfig);
    adminClient.getTableClient().createTableConfigs(tableConfigs.toPrettyJsonString(), null, null);
    response = adminClient.getTableClient().getTableConfigs(tableName);
    tableConfigsResponse = JsonUtils.stringToObject(response, TableConfigs.class);
    Assert.assertEquals(tableConfigsResponse.getTableName(), tableName);

    // delete & check
    adminClient.getTableClient().deleteTableConfigs(tableName, null);
    configs = adminClient.getTableClient().listTableConfigs();
    Assert.assertEquals(configs.size(), 0);
  }

  @Test
  public void testDeleteTableWithLogicalTable()
      throws Exception {
    PinotAdminClient adminClient = getOrCreateAdminClient();
    String logicalTableName = "testDeleteLogicalTable";
    String tableName = "physicalTable";
    TableConfig offlineTableConfig = createOfflineTableConfig(tableName);
    TableConfig realtimeTableConfig = createRealtimeTableConfig(tableName);
    DEFAULT_INSTANCE.addDummySchema(logicalTableName);
    DEFAULT_INSTANCE.addDummySchema(tableName);
    DEFAULT_INSTANCE.addTableConfig(offlineTableConfig);
    DEFAULT_INSTANCE.addTableConfig(realtimeTableConfig);

    // Create logical table
    LogicalTableConfig logicalTableConfig = getDummyLogicalTableConfig(logicalTableName,
        List.of(offlineTableConfig.getTableName(), realtimeTableConfig.getTableName()), "DefaultTenant");
    String response =
        adminClient.getLogicalTableClient().createLogicalTable(logicalTableConfig.toJsonString());
    Assert.assertTrue(response.contains("testDeleteLogicalTable logical table successfully added"), response);

    // Delete table should fail because it is referenced by a logical table
    String msg = Assert.expectThrows(RuntimeException.class,
        () -> adminClient.getTableClient().deleteTableConfigs(tableName, null)).getMessage();
    Assert.assertTrue(msg.contains("Cannot delete table config: " + tableName
        + " because it is referenced in logical table: " + logicalTableName), msg);

    //  Delete logical table
    response = adminClient.getLogicalTableClient().deleteLogicalTable(logicalTableName);
    Assert.assertTrue(response.contains("testDeleteLogicalTable logical table successfully deleted"), response);

    // physical table should be deleted successfully
    response = adminClient.getTableClient().deleteTableConfigs(tableName, null);
    Assert.assertTrue(response.contains("Deleted TableConfigs: physicalTable"), response);
  }

  @Test
  public void testDeleteTableConfigWithTableTypeValidation()
      throws Exception {
    PinotAdminClient adminClient = getOrCreateAdminClient();
    String tableName = "testDeleteWithTypeValidation";
    TableConfig offlineTableConfig = createOfflineTableConfig(tableName);
    DEFAULT_INSTANCE.addDummySchema(tableName);
    DEFAULT_INSTANCE.addTableConfig(offlineTableConfig);

    // Delete table should fail because it is not a raw table name
    String msg = Assert.expectThrows(RuntimeException.class,
            () -> adminClient.getTableClient().deleteTableConfigs(offlineTableConfig.getTableName(), null))
        .getMessage();
    Assert.assertTrue(msg.contains("Invalid table name: testDeleteWithTypeValidation_OFFLINE. Use raw table name."),
        msg);

    // Delete table with raw table name
    String response = adminClient.getTableClient().deleteTableConfigs(tableName, null);
    Assert.assertTrue(response.contains("Deleted TableConfigs: testDeleteWithTypeValidation"), response);
  }

  @Test
  public void testUnrecognizedProperties()
      throws Exception {
    PinotAdminClient adminClient = getOrCreateAdminClient();
    String tableName = "testUnrecognized1";
    TableConfig offlineTableConfig = createOfflineTableConfig(tableName);
    Schema schema = createDummySchema(tableName);
    TableConfigs tableConfigs = new TableConfigs(tableName, schema, offlineTableConfig, null);
    ObjectNode tableConfigsJson = JsonUtils.objectToJsonNode(tableConfigs).deepCopy();
    tableConfigsJson.put("illegalKey1", 1);

    // Validate
    String response = adminClient.getTableClient().validateTableConfigs(tableConfigsJson.toPrettyString(), null);
    JsonNode responseJson = JsonUtils.stringToJsonNode(response);
    Assert.assertTrue(responseJson.has("unrecognizedProperties"));
    Assert.assertTrue(responseJson.get("unrecognizedProperties").has("/illegalKey1"));

    // Create
    response = adminClient.getTableClient().createTableConfigs(tableConfigsJson.toPrettyString(), null, null);
    Assert.assertEquals(response, "{\"unrecognizedProperties\":{\"/illegalKey1\":1},\"status\":\"TableConfigs "
        + "testUnrecognized1 successfully added\"}");

    // Update
    response =
        adminClient.getTableClient().updateTableConfigs(tableName, tableConfigsJson.toPrettyString(), null, false,
            false);
    Assert.assertEquals(response,
        "{\"unrecognizedProperties\":{\"/illegalKey1\":1},\"status\":\"TableConfigs updated for testUnrecognized1\"}");
    // Delete
    adminClient.getTableClient().deleteTableConfigs(tableName, null);
  }

  /**
   * Tests get TableConfigs for backwards compatibility
   */
  @Test
  public void testGetConfigCompatibility()
      throws Exception {
    PinotAdminClient adminClient = getOrCreateAdminClient();
    String tableName = "table1";
    DEFAULT_INSTANCE.addDummySchema(tableName);
    TableConfig offlineTableConfig = createOfflineTableConfig(tableName);
    SegmentsValidationAndRetentionConfig validationConfig = new SegmentsValidationAndRetentionConfig();
    validationConfig.setReplication("1");
    offlineTableConfig.setValidationConfig(validationConfig);
    adminClient.getTableClient().createTable(offlineTableConfig.toJsonString(), null);

    String response = adminClient.getTableClient().getTableConfigs(tableName);
    TableConfigs tableConfigsResponse = JsonUtils.stringToObject(response, TableConfigs.class);
    Assert.assertEquals(tableConfigsResponse.getTableName(), tableName);
    Assert.assertEquals(tableConfigsResponse.getOffline().getTableName(), offlineTableConfig.getTableName());
    Assert.assertEquals(tableConfigsResponse.getSchema().getSchemaName(), tableName);

    // Delete
    adminClient.getTableClient().deleteTable(tableName);
    adminClient.getSchemaClient().deleteSchema(tableName);
  }

  @AfterClass
  public void tearDown() {
    DEFAULT_INSTANCE.cleanup();
  }
}
