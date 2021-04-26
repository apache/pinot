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
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.List;
import org.apache.pinot.controller.ControllerTestUtils;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.spi.config.TableConfigs;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TunerConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Lists;


/**
 * Tests for CRUD APIs of {@link TableConfigs}
 */
public class TableConfigsRestletResourceTest {

  private String _createTableConfigsUrl;

  @BeforeClass
  public void setUp()
      throws Exception {
    ControllerTestUtils.setupClusterAndValidate();
    _createTableConfigsUrl = ControllerTestUtils.getControllerRequestURLBuilder().forTableConfigsCreate();
  }

  private Schema getSchema(String tableName) {
    return ControllerTestUtils.createDummySchema(tableName);
  }

  private Schema getDimSchema(String tableName) {
    Schema schema = ControllerTestUtils.createDummySchema(tableName);
    schema.setPrimaryKeyColumns(Lists.newArrayList(schema.getDimensionNames().get(0)));
    return schema;
  }

  private TableConfigBuilder getBaseTableConfigBuilder(String tableName, TableType tableType) {
    if (tableType == TableType.OFFLINE) {
      return new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).setTimeColumnName("timeColumn")
          .setRetentionTimeUnit("DAYS").setRetentionTimeValue("50");
    } else {
      StreamConfig streamConfig = FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs();
      return new TableConfigBuilder(TableType.REALTIME).setTableName(tableName).setTimeColumnName("timeColumn")
          .setRetentionTimeUnit("DAYS").setLLC(true).setRetentionTimeValue("5")
          .setStreamConfigs(streamConfig.getStreamConfigsMap());
    }
  }

  private TableConfig getOfflineTableConfig(String tableName) {
    return getBaseTableConfigBuilder(tableName, TableType.OFFLINE).build();
  }

  private TableConfig getRealtimeTableConfig(String tableName) {
    return getBaseTableConfigBuilder(tableName, TableType.REALTIME).build();
  }

  private TableConfig getOfflineTunerTableConfig(String tableName) {
    return getBaseTableConfigBuilder(tableName, TableType.OFFLINE)
        .setTunerConfig(new TunerConfig("realtimeAutoIndexTuner", null)).build();
  }

  private TableConfig getRealtimeTunerTableConfig(String tableName) {
    return getBaseTableConfigBuilder(tableName, TableType.REALTIME)
        .setTunerConfig(new TunerConfig("realtimeAutoIndexTuner", null)).build();
  }

  private TableConfig getOfflineDimTableConfig(String tableName) {
    return getBaseTableConfigBuilder(tableName, TableType.OFFLINE).setIsDimTable(true).build();
  }

  @Test
  public void testValidateConfig()
      throws IOException {

    String validateConfigUrl = ControllerTestUtils.getControllerRequestURLBuilder().forTableConfigsValidate();

    String tableName = "testValidate";
    TableConfig offlineTableConfig = getOfflineTableConfig(tableName);
    TableConfig realtimeTableConfig = getRealtimeTableConfig(tableName);
    Schema schema = getSchema(tableName);
    TableConfigs tableConfigs;

    // invalid json
    try {
      tableConfigs = new TableConfigs(tableName, schema, offlineTableConfig, realtimeTableConfig);
      ControllerTestUtils
          .sendPostRequest(validateConfigUrl, tableConfigs.toPrettyJsonString().replace("\"offline\"", "offline\""));
      Assert.fail("Creation of a TableConfigs with invalid json string should have failed");
    } catch (Exception e) {
      // expected
    }

    // null table configs
    try {
      tableConfigs = new TableConfigs(tableName, schema, null, null);
      ControllerTestUtils.sendPostRequest(validateConfigUrl, tableConfigs.toPrettyJsonString());
      Assert.fail(
          "Creation of an TableConfigs with null table offline tableConfig and realtime tableConfig should have failed");
    } catch (Exception e) {
      // expected
    }

    // null schema
    try {
      tableConfigs = new TableConfigs(tableName, null, offlineTableConfig, null);
      ControllerTestUtils.sendPostRequest(validateConfigUrl, tableConfigs.toPrettyJsonString());
      Assert.fail("Creation of an TableConfigs with null schema should have failed");
    } catch (Exception e) {
      // expected
    }

    // empty config name
    try {
      tableConfigs = new TableConfigs("", schema, offlineTableConfig, realtimeTableConfig);
      ControllerTestUtils.sendPostRequest(validateConfigUrl, tableConfigs.toPrettyJsonString());
      Assert.fail("Creation of an TableConfigs with empty config name should have failed");
    } catch (Exception e) {
      // expected
    }

    // schema name doesn't match config name
    try {
      tableConfigs = new TableConfigs(tableName, getSchema("differentName"), offlineTableConfig, realtimeTableConfig);
      ControllerTestUtils.sendPostRequest(validateConfigUrl, tableConfigs.toPrettyJsonString());
      Assert.fail("Creation of an TableConfigs with schema name different than tableName should have failed");
    } catch (Exception e) {
      // expected
    }

    // schema validation fails
    try {
      Schema schemaWithBlankSpace = getSchema(tableName);
      schemaWithBlankSpace.addField(new MetricFieldSpec("blank space", FieldSpec.DataType.LONG));
      tableConfigs = new TableConfigs(tableName, schemaWithBlankSpace, offlineTableConfig, realtimeTableConfig);
      ControllerTestUtils.sendPostRequest(validateConfigUrl, tableConfigs.toPrettyJsonString());
      Assert.fail("Creation of an TableConfigs with blank space in column should have failed");
    } catch (Exception e) {
      // expected
    }

    // offline table name doesn't match config name
    try {
      tableConfigs = new TableConfigs(tableName, schema, getOfflineTableConfig("differentName"), null);
      ControllerTestUtils.sendPostRequest(validateConfigUrl, tableConfigs.toPrettyJsonString());
      Assert.fail("Creation of an TableConfigs with offline table name different than tableName should have failed");
    } catch (Exception e) {
      // expected
    }

    // table name validation fails
    try {
      tableConfigs =
          new TableConfigs("blank space", getSchema("blank space"), getOfflineTableConfig("blank space"), null);
      ControllerTestUtils.sendPostRequest(validateConfigUrl, tableConfigs.toPrettyJsonString());
      Assert.fail("Creation of an TableConfigs with blank space in table name should have failed");
    } catch (Exception e) {
      // expected
    }

    // table validation fails
    try {
      TableConfig invalidTableConfig = getOfflineTableConfig(tableName);
      invalidTableConfig.getIndexingConfig().setInvertedIndexColumns(Lists.newArrayList("nonExistent"));
      tableConfigs = new TableConfigs(tableName, schema, invalidTableConfig, null);
      ControllerTestUtils.sendPostRequest(validateConfigUrl, tableConfigs.toPrettyJsonString());
      Assert.fail("Creation of an TableConfigs with invalid table config should have failed");
    } catch (Exception e) {
      // expected
    }

    // realtime table name doesn't match config name
    try {
      tableConfigs = new TableConfigs(tableName, schema, null, getRealtimeTableConfig("differentName"));
      ControllerTestUtils.sendPostRequest(validateConfigUrl, tableConfigs.toPrettyJsonString());
      Assert.fail("Creation of an TableConfigs with realtime table name different than tableName should have failed");
    } catch (Exception e) {
      // expected
    }

    // table name validation fails
    try {
      tableConfigs =
          new TableConfigs("blank space", getSchema("blank space"), null, getRealtimeTableConfig("blank space"));
      ControllerTestUtils.sendPostRequest(validateConfigUrl, tableConfigs.toPrettyJsonString());
      Assert.fail("Creation of an TableConfigs with blank space in table name should have failed");
    } catch (Exception e) {
      // expected
    }

    // table validation fails
    try {
      TableConfig invalidTableConfig = getRealtimeTableConfig(tableName);
      invalidTableConfig.getIndexingConfig().setInvertedIndexColumns(Lists.newArrayList("nonExistent"));
      tableConfigs = new TableConfigs(tableName, schema, null, invalidTableConfig);
      ControllerTestUtils.sendPostRequest(validateConfigUrl, tableConfigs.toPrettyJsonString());
      Assert.fail("Creation of an TableConfigs with invalid table config should have failed");
    } catch (Exception e) {
      // expected
    }

    // hybrid config consistency check fails
    try {
      Schema twoTimeColumns = getSchema(tableName);
      twoTimeColumns
          .addField(new DateTimeFieldSpec("time1", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS"));
      twoTimeColumns
          .addField(new DateTimeFieldSpec("time2", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS"));
      TableConfig offlineTableConfig1 = getOfflineTableConfig(tableName);
      offlineTableConfig1.getValidationConfig().setTimeColumnName("time1");
      TableConfig realtimeTableConfig1 = getRealtimeTableConfig(tableName);
      realtimeTableConfig1.getValidationConfig().setTimeColumnName("time2");
      tableConfigs = new TableConfigs(tableName, twoTimeColumns, offlineTableConfig1, realtimeTableConfig1);
      ControllerTestUtils.sendPostRequest(validateConfigUrl, tableConfigs.toPrettyJsonString());
      Assert.fail(
          "Creation of an TableConfigs with inconsistencies across offline and realtime table config should have failed");
    } catch (Exception e) {
      // expected
    }

    // successfully created with all 3 configs
    String tableName1 = "testValidate1";
    tableConfigs = new TableConfigs(tableName1, getSchema(tableName1), getOfflineTableConfig(tableName1),
        getRealtimeTableConfig(tableName1));
    ControllerTestUtils.sendPostRequest(validateConfigUrl, tableConfigs.toPrettyJsonString());

    // successfully create with offline config
    String tableName2 = "testValidate2";
    tableConfigs = new TableConfigs(tableName2, getSchema(tableName2), getOfflineTableConfig(tableName2), null);
    ControllerTestUtils.sendPostRequest(validateConfigUrl, tableConfigs.toPrettyJsonString());

    // successfully create with realtime config
    String tableName3 = "testValidate3";
    tableConfigs = new TableConfigs(tableName3, getSchema(tableName3), null, getRealtimeTableConfig(tableName3));
    ControllerTestUtils.sendPostRequest(validateConfigUrl, tableConfigs.toPrettyJsonString());

    ControllerTestUtils
        .sendDeleteRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableConfigsDelete(tableName1));
    ControllerTestUtils
        .sendDeleteRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableConfigsDelete(tableName2));
    ControllerTestUtils
        .sendDeleteRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableConfigsDelete(tableName3));
  }

  /**
   * Tests for creation of TableConfigs
   */
  @Test
  public void testCreateConfig()
      throws IOException {
    String tableName = "testCreate";
    TableConfig offlineTableConfig = getOfflineTableConfig(tableName);
    TableConfig realtimeTableConfig = getRealtimeTableConfig(tableName);
    Schema schema = getSchema(tableName);
    TableConfigs tableConfigs = new TableConfigs(tableName, schema, offlineTableConfig, realtimeTableConfig);
    ControllerTestUtils.sendPostRequest(_createTableConfigsUrl, tableConfigs.toPrettyJsonString());
    String response = ControllerTestUtils
        .sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableConfigsGet(tableName));
    TableConfigs tableConfigsResponse = JsonUtils.stringToObject(response, TableConfigs.class);
    Assert.assertEquals(tableConfigsResponse.getTableName(), tableName);
    Assert.assertEquals(tableConfigsResponse.getOffline().getTableName(), offlineTableConfig.getTableName());
    Assert.assertEquals(tableConfigsResponse.getRealtime().getTableName(), realtimeTableConfig.getTableName());
    Assert.assertEquals(tableConfigsResponse.getSchema().getSchemaName(), schema.getSchemaName());
    ControllerTestUtils
        .sendDeleteRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableConfigsDelete(tableName));

    // replica check
    tableName = "testCreateReplicas";
    TableConfig replicaTestOfflineTableConfig = getOfflineTableConfig(tableName);
    TableConfig replicaTestRealtimeTableConfig = getRealtimeTableConfig(tableName);
    replicaTestOfflineTableConfig.getValidationConfig().setReplication("1");
    replicaTestRealtimeTableConfig.getValidationConfig().setReplicasPerPartition("1");
    tableConfigs = new TableConfigs(tableName, getSchema(tableName), replicaTestOfflineTableConfig,
        replicaTestRealtimeTableConfig);
    ControllerTestUtils.sendPostRequest(_createTableConfigsUrl, tableConfigs.toPrettyJsonString());
    response = ControllerTestUtils
        .sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableConfigsGet(tableName));
    tableConfigsResponse = JsonUtils.stringToObject(response, TableConfigs.class);
    Assert.assertEquals(tableConfigsResponse.getTableName(), tableName);
    Assert.assertEquals(tableConfigsResponse.getOffline().getValidationConfig().getReplicationNumber(),
        ControllerTestUtils.MIN_NUM_REPLICAS);
    Assert.assertEquals(tableConfigsResponse.getRealtime().getValidationConfig().getReplicasPerPartitionNumber(),
        ControllerTestUtils.MIN_NUM_REPLICAS);
    ControllerTestUtils
        .sendDeleteRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableConfigsDelete(tableName));

    // quota check
    tableName = "testCreateQuota";
    TableConfig offlineDimTableConfig = getOfflineDimTableConfig(tableName);
    Schema dimSchema = getDimSchema(tableName);
    tableConfigs = new TableConfigs(tableName, dimSchema, offlineDimTableConfig, null);
    ControllerTestUtils.sendPostRequest(_createTableConfigsUrl, tableConfigs.toPrettyJsonString());
    response = ControllerTestUtils
        .sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableConfigsGet(tableName));
    tableConfigsResponse = JsonUtils.stringToObject(response, TableConfigs.class);
    Assert.assertEquals(tableName, tableConfigsResponse.getTableName());
    Assert.assertEquals(tableConfigsResponse.getOffline().getQuotaConfig().getStorage(),
        ControllerTestUtils.getControllerConfig().getDimTableMaxSize());
    ControllerTestUtils
        .sendDeleteRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableConfigsDelete(tableName));

    // tuner config
    tableName = "testTunerConfig";
    TableConfig offlineTunerTableConfig = getOfflineTunerTableConfig(tableName);
    TableConfig realtimeTunerTableConfig = getRealtimeTunerTableConfig(tableName);
    tableConfigs = new TableConfigs(tableName, getSchema(tableName), offlineTunerTableConfig, realtimeTunerTableConfig);
    ControllerTestUtils.sendPostRequest(_createTableConfigsUrl, tableConfigs.toPrettyJsonString());
    response = ControllerTestUtils
        .sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableConfigsGet(tableName));
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
    ControllerTestUtils
        .sendDeleteRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableConfigsDelete(tableName));
  }

  @Test
  public void testListConfigs()
      throws IOException {
    // create with 1 config
    String tableName1 = "testList1";
    TableConfig offlineTableConfig = getOfflineTableConfig(tableName1);
    TableConfig realtimeTableConfig = getRealtimeTableConfig(tableName1);
    Schema schema = getSchema(tableName1);
    TableConfigs tableConfigs = new TableConfigs(tableName1, schema, offlineTableConfig, null);
    ControllerTestUtils.sendPostRequest(_createTableConfigsUrl, tableConfigs.toPrettyJsonString());

    // list
    String getResponse =
        ControllerTestUtils.sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableConfigsList());
    List<String> configs = JsonUtils.stringToObject(getResponse, new TypeReference<List<String>>() {
    });
    Assert.assertEquals(configs.size(), 1);
    Assert.assertTrue(configs.containsAll(Sets.newHashSet(tableName1)));

    // update to 2
    tableConfigs = new TableConfigs(tableName1, schema, offlineTableConfig, realtimeTableConfig);
    ControllerTestUtils
        .sendPutRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableConfigsUpdate(tableName1),
            tableConfigs.toPrettyJsonString());

    // list
    getResponse =
        ControllerTestUtils.sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableConfigsList());
    configs = JsonUtils.stringToObject(getResponse, new TypeReference<List<String>>() {
    });
    Assert.assertEquals(configs.size(), 1);
    Assert.assertTrue(configs.containsAll(Sets.newHashSet("testList1")));

    // create new
    String tableName2 = "testList2";
    offlineTableConfig = getOfflineTableConfig(tableName2);
    schema = getSchema(tableName2);
    tableConfigs = new TableConfigs(tableName2, schema, offlineTableConfig, null);
    ControllerTestUtils.sendPostRequest(_createTableConfigsUrl, tableConfigs.toPrettyJsonString());

    // list
    getResponse =
        ControllerTestUtils.sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableConfigsList());
    configs = JsonUtils.stringToObject(getResponse, new TypeReference<List<String>>() {
    });
    Assert.assertEquals(configs.size(), 2);
    Assert.assertTrue(configs.containsAll(Sets.newHashSet(tableName1, tableName2)));

    // delete 1
    ControllerTestUtils
        .sendDeleteRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableConfigsDelete(tableName2));

    // list 1
    getResponse =
        ControllerTestUtils.sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableConfigsList());
    configs = JsonUtils.stringToObject(getResponse, new TypeReference<List<String>>() {
    });
    Assert.assertEquals(configs.size(), 1);
    Assert.assertTrue(configs.containsAll(Sets.newHashSet(tableName1)));

    ControllerTestUtils
        .sendDeleteRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableConfigsDelete(tableName1));
  }

  @Test
  public void testUpdateConfig()
      throws IOException {

    // create with 1
    String tableName = "testUpdate1";
    TableConfig offlineTableConfig = getOfflineTableConfig(tableName);
    TableConfig realtimeTableConfig = getRealtimeTableConfig(tableName);
    Schema schema = getSchema(tableName);
    TableConfigs tableConfigs = new TableConfigs(tableName, schema, offlineTableConfig, null);
    ControllerTestUtils.sendPostRequest(_createTableConfigsUrl, tableConfigs.toPrettyJsonString());
    String response = ControllerTestUtils
        .sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableConfigsGet(tableName));
    TableConfigs tableConfigsResponse = JsonUtils.stringToObject(response, TableConfigs.class);
    Assert.assertEquals(tableConfigsResponse.getTableName(), tableName);
    Assert.assertEquals(tableConfigsResponse.getOffline().getTableName(), offlineTableConfig.getTableName());
    Assert.assertNull(tableConfigs.getRealtime());
    Assert.assertEquals(tableConfigsResponse.getSchema().getSchemaName(), schema.getSchemaName());

    // list
    String getResponse =
        ControllerTestUtils.sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableConfigsList());
    List<String> configs = JsonUtils.stringToObject(getResponse, new TypeReference<List<String>>() {
    });
    Assert.assertEquals(configs.size(), 1);
    Assert.assertTrue(configs.containsAll(Sets.newHashSet(tableName)));

    // update to 2
    tableConfigs = new TableConfigs(tableName, tableConfigsResponse.getSchema(), tableConfigsResponse.getOffline(),
        realtimeTableConfig);
    ControllerTestUtils
        .sendPutRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableConfigsUpdate(tableName),
            tableConfigs.toPrettyJsonString());
    response = ControllerTestUtils
        .sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableConfigsGet(tableName));
    tableConfigsResponse = JsonUtils.stringToObject(response, TableConfigs.class);
    Assert.assertEquals(tableConfigsResponse.getTableName(), tableName);
    Assert.assertEquals(tableConfigsResponse.getOffline().getTableName(), offlineTableConfig.getTableName());
    Assert.assertEquals(tableConfigsResponse.getRealtime().getTableName(), realtimeTableConfig.getTableName());
    Assert.assertEquals(tableConfigsResponse.getSchema().getSchemaName(), schema.getSchemaName());

    // list
    getResponse =
        ControllerTestUtils.sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableConfigsList());
    configs = JsonUtils.stringToObject(getResponse, new TypeReference<List<String>>() {
    });
    Assert.assertEquals(configs.size(), 1);
    Assert.assertTrue(configs.containsAll(Sets.newHashSet(tableName)));

    // update existing config
    schema.addField(new MetricFieldSpec("newMetric", FieldSpec.DataType.LONG));
    tableConfigs = new TableConfigs(tableName, schema, tableConfigsResponse.getOffline(), tableConfigs.getRealtime());
    ControllerTestUtils
        .sendPutRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableConfigsUpdate(tableName),
            tableConfigs.toPrettyJsonString());
    response = ControllerTestUtils
        .sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableConfigsGet(tableName));
    tableConfigsResponse = JsonUtils.stringToObject(response, TableConfigs.class);
    Assert.assertEquals(tableConfigsResponse.getTableName(), tableName);
    Assert.assertEquals(tableConfigsResponse.getOffline().getTableName(), offlineTableConfig.getTableName());
    Assert.assertEquals(tableConfigsResponse.getRealtime().getTableName(), realtimeTableConfig.getTableName());
    Assert.assertEquals(tableConfigsResponse.getSchema().getSchemaName(), schema.getSchemaName());
    Assert.assertTrue(tableConfigsResponse.getSchema().getMetricNames().contains("newMetric"));

    ControllerTestUtils
        .sendDeleteRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableConfigsDelete(tableName));
  }

  @Test
  public void testDeleteConfig()
      throws Exception {
    // create with 1 config
    String tableName = "testDelete1";
    TableConfig offlineTableConfig = getOfflineTableConfig(tableName);
    Schema schema = getSchema(tableName);
    TableConfigs tableConfigs = new TableConfigs(tableName, schema, offlineTableConfig, null);
    ControllerTestUtils.sendPostRequest(_createTableConfigsUrl, tableConfigs.toPrettyJsonString());
    String response = ControllerTestUtils
        .sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableConfigsGet(tableName));
    TableConfigs tableConfigsResponse = JsonUtils.stringToObject(response, TableConfigs.class);
    Assert.assertEquals(tableConfigsResponse.getTableName(), tableName);

    // delete & check
    ControllerTestUtils
        .sendDeleteRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableConfigsDelete(tableName));
    String getResponse =
        ControllerTestUtils.sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableConfigsList());
    List<String> configs = JsonUtils.stringToObject(getResponse, new TypeReference<List<String>>() {
    });
    Assert.assertEquals(configs.size(), 0);

    tableName = "testDelete2";
    offlineTableConfig = getOfflineTableConfig(tableName);
    TableConfig realtimeTableConfig = getRealtimeTableConfig(tableName);
    schema = getSchema(tableName);
    tableConfigs = new TableConfigs(tableName, schema, offlineTableConfig, realtimeTableConfig);
    ControllerTestUtils.sendPostRequest(_createTableConfigsUrl, tableConfigs.toPrettyJsonString());
    response = ControllerTestUtils
        .sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableConfigsGet(tableName));
    tableConfigsResponse = JsonUtils.stringToObject(response, TableConfigs.class);
    Assert.assertEquals(tableConfigsResponse.getTableName(), tableName);

    // delete & check
    ControllerTestUtils
        .sendDeleteRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableConfigsDelete(tableName));
    getResponse =
        ControllerTestUtils.sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableConfigsList());
    configs = JsonUtils.stringToObject(getResponse, new TypeReference<List<String>>() {
    });
    Assert.assertEquals(configs.size(), 0);
  }

  @AfterClass
  public void tearDown() {
    ControllerTestUtils.cleanup();
  }
}
