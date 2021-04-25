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
import org.apache.pinot.spi.config.PinotConfig;
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
 * Tests for CRUD APIs of {@link org.apache.pinot.spi.config.PinotConfig}
 */
public class PinotConfigRestletResourceTest {

  private String _createConfigUrl;

  @BeforeClass
  public void setUp()
      throws Exception {
    ControllerTestUtils.setupClusterAndValidate();
    _createConfigUrl = ControllerTestUtils.getControllerRequestURLBuilder().forPinotConfigCreate();
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

    String validateConfigUrl = ControllerTestUtils.getControllerRequestURLBuilder().forPinotConfigValidate();

    String configName = "testValidate";
    TableConfig offlineTableConfig = getOfflineTableConfig(configName);
    TableConfig realtimeTableConfig = getRealtimeTableConfig(configName);
    Schema schema = getSchema(configName);
    PinotConfig pinotConfig;

    // invalid json
    try {
      pinotConfig = new PinotConfig(configName, offlineTableConfig, realtimeTableConfig, schema);
      ControllerTestUtils.sendPostRequest(validateConfigUrl,
          pinotConfig.toJsonString().replace("\"offlineTableConfig", "offlineTableConfig"));
      Assert.fail("Creation of an PinotConfig with invalid json string should have failed");
    } catch (Exception e) {
      // expected
    }

    // null table configs
    try {
      pinotConfig = new PinotConfig(configName, null, null, schema);
      ControllerTestUtils.sendPostRequest(validateConfigUrl, pinotConfig.toJsonString());
      Assert.fail(
          "Creation of an PinotConfig with null table offlineTableConfig and realtimeTableConfig should have failed");
    } catch (Exception e) {
      // expected
    }

    // null schema
    try {
      pinotConfig = new PinotConfig(configName, offlineTableConfig, null, null);
      ControllerTestUtils.sendPostRequest(validateConfigUrl, pinotConfig.toJsonString());
      Assert.fail("Creation of an PinotConfig with null schema should have failed");
    } catch (Exception e) {
      // expected
    }

    // empty config name
    try {
      pinotConfig = new PinotConfig("", offlineTableConfig, realtimeTableConfig, schema);
      ControllerTestUtils.sendPostRequest(validateConfigUrl, pinotConfig.toJsonString());
      Assert.fail("Creation of an PinotConfig with empty config name should have failed");
    } catch (Exception e) {
      // expected
    }

    // schema name doesn't match config name
    try {
      pinotConfig = new PinotConfig(configName, offlineTableConfig, realtimeTableConfig, getSchema("differentName"));
      ControllerTestUtils.sendPostRequest(validateConfigUrl, pinotConfig.toJsonString());
      Assert.fail("Creation of an PinotConfig with schema name different than configName should have failed");
    } catch (Exception e) {
      // expected
    }

    // schema validation fails
    try {
      Schema schemaWithBlankSpace = getSchema(configName);
      schemaWithBlankSpace.addField(new MetricFieldSpec("blank space", FieldSpec.DataType.LONG));
      pinotConfig = new PinotConfig(configName, offlineTableConfig, realtimeTableConfig, schemaWithBlankSpace);
      ControllerTestUtils.sendPostRequest(validateConfigUrl, pinotConfig.toJsonString());
      Assert.fail("Creation of an PinotConfig with blank space in column should have failed");
    } catch (Exception e) {
      // expected
    }

    // offline table name doesn't match config name
    try {
      pinotConfig = new PinotConfig(configName, getOfflineTableConfig("differentName"), null, schema);
      ControllerTestUtils.sendPostRequest(validateConfigUrl, pinotConfig.toJsonString());
      Assert.fail("Creation of an PinotConfig with offline table name different than configName should have failed");
    } catch (Exception e) {
      // expected
    }

    // table name validation fails
    try {
      pinotConfig =
          new PinotConfig("blank space", getOfflineTableConfig("blank space"), null, getSchema("blank space"));
      ControllerTestUtils.sendPostRequest(validateConfigUrl, pinotConfig.toJsonString());
      Assert.fail("Creation of an PinotConfig with blank space in table name should have failed");
    } catch (Exception e) {
      // expected
    }

    // table validation fails
    try {
      TableConfig invalidTableConfig = getOfflineTableConfig(configName);
      invalidTableConfig.getIndexingConfig().setInvertedIndexColumns(Lists.newArrayList("nonExistent"));
      pinotConfig = new PinotConfig(configName, invalidTableConfig, null, schema);
      ControllerTestUtils.sendPostRequest(validateConfigUrl, pinotConfig.toJsonString());
      Assert.fail("Creation of an PinotConfig with invalid table config should have failed");
    } catch (Exception e) {
      // expected
    }

    // realtime table name doesn't match config name
    try {
      pinotConfig = new PinotConfig(configName, null, getRealtimeTableConfig("differentName"), schema);
      ControllerTestUtils.sendPostRequest(validateConfigUrl, pinotConfig.toJsonString());
      Assert.fail("Creation of an PinotConfig with realtime table name different than configName should have failed");
    } catch (Exception e) {
      // expected
    }

    // table name validation fails
    try {
      pinotConfig =
          new PinotConfig("blank space", null, getRealtimeTableConfig("blank space"), getSchema("blank space"));
      ControllerTestUtils.sendPostRequest(validateConfigUrl, pinotConfig.toJsonString());
      Assert.fail("Creation of an PinotConfig with blank space in table name should have failed");
    } catch (Exception e) {
      // expected
    }

    // table validation fails
    try {
      TableConfig invalidTableConfig = getRealtimeTableConfig(configName);
      invalidTableConfig.getIndexingConfig().setInvertedIndexColumns(Lists.newArrayList("nonExistent"));
      pinotConfig = new PinotConfig(configName, null, invalidTableConfig, schema);
      ControllerTestUtils.sendPostRequest(validateConfigUrl, pinotConfig.toJsonString());
      Assert.fail("Creation of an PinotConfig with invalid table config should have failed");
    } catch (Exception e) {
      // expected
    }

    // hybrid config consistency check fails
    try {
      Schema twoTimeColumns = getSchema(configName);
      twoTimeColumns
          .addField(new DateTimeFieldSpec("time1", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS"));
      twoTimeColumns
          .addField(new DateTimeFieldSpec("time2", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS"));
      TableConfig offlineTableConfig1 = getOfflineTableConfig(configName);
      offlineTableConfig1.getValidationConfig().setTimeColumnName("time1");
      TableConfig realtimeTableConfig1 = getRealtimeTableConfig(configName);
      realtimeTableConfig1.getValidationConfig().setTimeColumnName("time2");
      pinotConfig = new PinotConfig(configName, offlineTableConfig1, realtimeTableConfig1, twoTimeColumns);
      ControllerTestUtils.sendPostRequest(validateConfigUrl, pinotConfig.toJsonString());
      Assert.fail(
          "Creation of an PinotConfig with inconsistencies across offline and realtime table config should have failed");
    } catch (Exception e) {
      // expected
    }

    // successfully created with all 3 configs
    String configName1 = "testValidate1";
    pinotConfig = new PinotConfig(configName1, getOfflineTableConfig(configName1), getRealtimeTableConfig(configName1),
        getSchema(configName1));
    ControllerTestUtils.sendPostRequest(validateConfigUrl, pinotConfig.toJsonString());

    // successfully create with offline config
    String configName2 = "testValidate2";
    pinotConfig = new PinotConfig(configName2, getOfflineTableConfig(configName2), null, getSchema(configName2));
    ControllerTestUtils.sendPostRequest(validateConfigUrl, pinotConfig.toJsonString());

    // successfully create with realtime config
    String configName3 = "testValidate3";
    pinotConfig = new PinotConfig(configName3, null, getRealtimeTableConfig(configName3), getSchema(configName3));
    ControllerTestUtils.sendPostRequest(validateConfigUrl, pinotConfig.toJsonString());

    ControllerTestUtils
        .sendDeleteRequest(ControllerTestUtils.getControllerRequestURLBuilder().forPinotConfigDelete(configName1));
    ControllerTestUtils
        .sendDeleteRequest(ControllerTestUtils.getControllerRequestURLBuilder().forPinotConfigDelete(configName2));
    ControllerTestUtils
        .sendDeleteRequest(ControllerTestUtils.getControllerRequestURLBuilder().forPinotConfigDelete(configName3));
  }

  /**
   * Tests for creation of pinot config
   */
  @Test
  public void testCreateConfig()
      throws IOException {
    String configName = "testCreate";
    TableConfig offlineTableConfig = getOfflineTableConfig(configName);
    TableConfig realtimeTableConfig = getRealtimeTableConfig(configName);
    Schema schema = getSchema(configName);
    PinotConfig pinotConfig = new PinotConfig(configName, offlineTableConfig, realtimeTableConfig, schema);
    ControllerTestUtils.sendPostRequest(_createConfigUrl, pinotConfig.toJsonString());
    String response = ControllerTestUtils
        .sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forPinotConfigGet(configName));
    PinotConfig pinotConfigResponse = JsonUtils.stringToObject(response, PinotConfig.class);
    Assert.assertEquals(pinotConfigResponse.getConfigName(), configName);
    Assert.assertEquals(pinotConfigResponse.getOfflineTableConfig().getTableName(), offlineTableConfig.getTableName());
    Assert
        .assertEquals(pinotConfigResponse.getRealtimeTableConfig().getTableName(), realtimeTableConfig.getTableName());
    Assert.assertEquals(pinotConfigResponse.getSchema().getSchemaName(), schema.getSchemaName());
    ControllerTestUtils
        .sendDeleteRequest(ControllerTestUtils.getControllerRequestURLBuilder().forPinotConfigDelete(configName));

    // replica check
    configName = "testCreateReplicas";
    TableConfig replicaTestOfflineTableConfig = getOfflineTableConfig(configName);
    TableConfig replicaTestRealtimeTableConfig = getRealtimeTableConfig(configName);
    replicaTestOfflineTableConfig.getValidationConfig().setReplication("1");
    replicaTestRealtimeTableConfig.getValidationConfig().setReplicasPerPartition("1");
    pinotConfig = new PinotConfig(configName, replicaTestOfflineTableConfig, replicaTestRealtimeTableConfig,
        getSchema(configName));
    ControllerTestUtils.sendPostRequest(_createConfigUrl, pinotConfig.toJsonString());
    response = ControllerTestUtils
        .sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forPinotConfigGet(configName));
    pinotConfigResponse = JsonUtils.stringToObject(response, PinotConfig.class);
    Assert.assertEquals(pinotConfigResponse.getConfigName(), configName);
    Assert.assertEquals(pinotConfigResponse.getOfflineTableConfig().getValidationConfig().getReplicationNumber(),
        ControllerTestUtils.MIN_NUM_REPLICAS);
    Assert.assertEquals(
        pinotConfigResponse.getRealtimeTableConfig().getValidationConfig().getReplicasPerPartitionNumber(),
        ControllerTestUtils.MIN_NUM_REPLICAS);
    ControllerTestUtils
        .sendDeleteRequest(ControllerTestUtils.getControllerRequestURLBuilder().forPinotConfigDelete(configName));

    // quota check
    configName = "testCreateQuota";
    TableConfig offlineDimTableConfig = getOfflineDimTableConfig(configName);
    Schema dimSchema = getDimSchema(configName);
    pinotConfig = new PinotConfig(configName, offlineDimTableConfig, null, dimSchema);
    ControllerTestUtils.sendPostRequest(_createConfigUrl, pinotConfig.toJsonString());
    response = ControllerTestUtils
        .sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forPinotConfigGet(configName));
    pinotConfigResponse = JsonUtils.stringToObject(response, PinotConfig.class);
    Assert.assertEquals(configName, pinotConfigResponse.getConfigName());
    Assert.assertEquals(pinotConfigResponse.getOfflineTableConfig().getQuotaConfig().getStorage(),
        ControllerTestUtils.getControllerConfig().getDimTableMaxSize());
    ControllerTestUtils
        .sendDeleteRequest(ControllerTestUtils.getControllerRequestURLBuilder().forPinotConfigDelete(configName));

    // tuner config
    configName = "testTunerConfig";
    TableConfig offlineTunerTableConfig = getOfflineTunerTableConfig(configName);
    TableConfig realtimeTunerTableConfig = getRealtimeTunerTableConfig(configName);
    pinotConfig = new PinotConfig(configName, offlineTunerTableConfig, realtimeTunerTableConfig, getSchema(configName));
    ControllerTestUtils.sendPostRequest(_createConfigUrl, pinotConfig.toJsonString());
    response = ControllerTestUtils
        .sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forPinotConfigGet(configName));
    pinotConfigResponse = JsonUtils.stringToObject(response, PinotConfig.class);
    Assert.assertEquals(configName, pinotConfigResponse.getConfigName());
    Assert.assertTrue(pinotConfigResponse.getOfflineTableConfig().getIndexingConfig().getInvertedIndexColumns()
        .containsAll(schema.getDimensionNames()));
    Assert.assertTrue(pinotConfigResponse.getOfflineTableConfig().getIndexingConfig().getNoDictionaryColumns()
        .containsAll(schema.getMetricNames()));
    Assert.assertTrue(pinotConfigResponse.getRealtimeTableConfig().getIndexingConfig().getInvertedIndexColumns()
        .containsAll(schema.getDimensionNames()));
    Assert.assertTrue(pinotConfigResponse.getRealtimeTableConfig().getIndexingConfig().getNoDictionaryColumns()
        .containsAll(schema.getMetricNames()));
    ControllerTestUtils
        .sendDeleteRequest(ControllerTestUtils.getControllerRequestURLBuilder().forPinotConfigDelete(configName));
  }

  @Test
  public void testListConfigs()
      throws IOException {
    // create with 1 config
    String configName1 = "testList1";
    TableConfig offlineTableConfig = getOfflineTableConfig(configName1);
    TableConfig realtimeTableConfig = getRealtimeTableConfig(configName1);
    Schema schema = getSchema(configName1);
    PinotConfig pinotConfig = new PinotConfig(configName1, offlineTableConfig, null, schema);
    ControllerTestUtils.sendPostRequest(_createConfigUrl, pinotConfig.toJsonString());

    // list
    String getResponse =
        ControllerTestUtils.sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forPinotConfigsList());
    List<String> configs = JsonUtils.stringToObject(getResponse, new TypeReference<List<String>>() {
    });
    Assert.assertEquals(configs.size(), 1);
    Assert.assertTrue(configs.containsAll(Sets.newHashSet(configName1)));

    // update to 2
    pinotConfig = new PinotConfig(configName1, offlineTableConfig, realtimeTableConfig, schema);
    ControllerTestUtils
        .sendPutRequest(ControllerTestUtils.getControllerRequestURLBuilder().forPinotConfigUpdate(configName1),
            pinotConfig.toJsonString());

    // list
    getResponse =
        ControllerTestUtils.sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forPinotConfigsList());
    configs = JsonUtils.stringToObject(getResponse, new TypeReference<List<String>>() {
    });
    Assert.assertEquals(configs.size(), 1);
    Assert.assertTrue(configs.containsAll(Sets.newHashSet("testList1")));

    // create new
    String configName2 = "testList2";
    offlineTableConfig = getOfflineTableConfig(configName2);
    schema = getSchema(configName2);
    pinotConfig = new PinotConfig(configName2, offlineTableConfig, null, schema);
    ControllerTestUtils.sendPostRequest(_createConfigUrl, pinotConfig.toJsonString());

    // list
    getResponse =
        ControllerTestUtils.sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forPinotConfigsList());
    configs = JsonUtils.stringToObject(getResponse, new TypeReference<List<String>>() {
    });
    Assert.assertEquals(configs.size(), 2);
    Assert.assertTrue(configs.containsAll(Sets.newHashSet(configName1, configName2)));

    // delete 1
    ControllerTestUtils
        .sendDeleteRequest(ControllerTestUtils.getControllerRequestURLBuilder().forPinotConfigDelete(configName2));

    // list 1
    getResponse =
        ControllerTestUtils.sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forPinotConfigsList());
    configs = JsonUtils.stringToObject(getResponse, new TypeReference<List<String>>() {
    });
    Assert.assertEquals(configs.size(), 1);
    Assert.assertTrue(configs.containsAll(Sets.newHashSet(configName1)));

    ControllerTestUtils
        .sendDeleteRequest(ControllerTestUtils.getControllerRequestURLBuilder().forPinotConfigDelete(configName1));
  }

  @Test
  public void testUpdateConfig()
      throws IOException {

    // create with 1
    String configName = "testUpdate1";
    TableConfig offlineTableConfig = getOfflineTableConfig(configName);
    TableConfig realtimeTableConfig = getRealtimeTableConfig(configName);
    Schema schema = getSchema(configName);
    PinotConfig pinotConfig = new PinotConfig(configName, offlineTableConfig, null, schema);
    ControllerTestUtils.sendPostRequest(_createConfigUrl, pinotConfig.toJsonString());
    String response = ControllerTestUtils
        .sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forPinotConfigGet(configName));
    PinotConfig pinotConfigResponse = JsonUtils.stringToObject(response, PinotConfig.class);
    Assert.assertEquals(pinotConfigResponse.getConfigName(), configName);
    Assert.assertEquals(pinotConfigResponse.getOfflineTableConfig().getTableName(), offlineTableConfig.getTableName());
    Assert.assertNull(pinotConfig.getRealtimeTableConfig());
    Assert.assertEquals(pinotConfigResponse.getSchema().getSchemaName(), schema.getSchemaName());

    // list
    String getResponse =
        ControllerTestUtils.sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forPinotConfigsList());
    List<String> configs = JsonUtils.stringToObject(getResponse, new TypeReference<List<String>>() {
    });
    Assert.assertEquals(configs.size(), 1);
    Assert.assertTrue(configs.containsAll(Sets.newHashSet(configName)));

    // update to 2
    pinotConfig = new PinotConfig(configName, pinotConfigResponse.getOfflineTableConfig(), realtimeTableConfig,
        pinotConfigResponse.getSchema());
    ControllerTestUtils
        .sendPutRequest(ControllerTestUtils.getControllerRequestURLBuilder().forPinotConfigUpdate(configName),
            pinotConfig.toJsonString());
    response = ControllerTestUtils
        .sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forPinotConfigGet(configName));
    pinotConfigResponse = JsonUtils.stringToObject(response, PinotConfig.class);
    Assert.assertEquals(pinotConfigResponse.getConfigName(), configName);
    Assert.assertEquals(pinotConfigResponse.getOfflineTableConfig().getTableName(), offlineTableConfig.getTableName());
    Assert
        .assertEquals(pinotConfigResponse.getRealtimeTableConfig().getTableName(), realtimeTableConfig.getTableName());
    Assert.assertEquals(pinotConfigResponse.getSchema().getSchemaName(), schema.getSchemaName());

    // list
    getResponse =
        ControllerTestUtils.sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forPinotConfigsList());
    configs = JsonUtils.stringToObject(getResponse, new TypeReference<List<String>>() {
    });
    Assert.assertEquals(configs.size(), 1);
    Assert.assertTrue(configs.containsAll(Sets.newHashSet(configName)));

    // update existing config
    schema.addField(new MetricFieldSpec("newMetric", FieldSpec.DataType.LONG));
    pinotConfig =
        new PinotConfig(configName, pinotConfigResponse.getOfflineTableConfig(), pinotConfig.getRealtimeTableConfig(),
            schema);
    ControllerTestUtils
        .sendPutRequest(ControllerTestUtils.getControllerRequestURLBuilder().forPinotConfigUpdate(configName),
            pinotConfig.toJsonString());
    response = ControllerTestUtils
        .sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forPinotConfigGet(configName));
    pinotConfigResponse = JsonUtils.stringToObject(response, PinotConfig.class);
    Assert.assertEquals(pinotConfigResponse.getConfigName(), configName);
    Assert.assertEquals(pinotConfigResponse.getOfflineTableConfig().getTableName(), offlineTableConfig.getTableName());
    Assert
        .assertEquals(pinotConfigResponse.getRealtimeTableConfig().getTableName(), realtimeTableConfig.getTableName());
    Assert.assertEquals(pinotConfigResponse.getSchema().getSchemaName(), schema.getSchemaName());
    Assert.assertTrue(pinotConfigResponse.getSchema().getMetricNames().contains("newMetric"));

    ControllerTestUtils
        .sendDeleteRequest(ControllerTestUtils.getControllerRequestURLBuilder().forPinotConfigDelete(configName));
  }

  @Test
  public void testDeleteConfig()
      throws Exception {
    // create with 1 config
    String configName = "testDelete1";
    TableConfig offlineTableConfig = getOfflineTableConfig(configName);
    Schema schema = getSchema(configName);
    PinotConfig pinotConfig = new PinotConfig(configName, offlineTableConfig, null, schema);
    ControllerTestUtils.sendPostRequest(_createConfigUrl, pinotConfig.toJsonString());
    String response = ControllerTestUtils
        .sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forPinotConfigGet(configName));
    PinotConfig pinotConfigResponse = JsonUtils.stringToObject(response, PinotConfig.class);
    Assert.assertEquals(pinotConfigResponse.getConfigName(), configName);

    // delete & check
    ControllerTestUtils
        .sendDeleteRequest(ControllerTestUtils.getControllerRequestURLBuilder().forPinotConfigDelete(configName));
    String getResponse =
        ControllerTestUtils.sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forPinotConfigsList());
    List<String> configs = JsonUtils.stringToObject(getResponse, new TypeReference<List<String>>() {
    });
    Assert.assertEquals(configs.size(), 0);

    configName = "testDelete2";
    offlineTableConfig = getOfflineTableConfig(configName);
    TableConfig realtimeTableConfig = getRealtimeTableConfig(configName);
    schema = getSchema(configName);
    pinotConfig = new PinotConfig(configName, offlineTableConfig, realtimeTableConfig, schema);
    ControllerTestUtils.sendPostRequest(_createConfigUrl, pinotConfig.toJsonString());
    response = ControllerTestUtils
        .sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forPinotConfigGet(configName));
    pinotConfigResponse = JsonUtils.stringToObject(response, PinotConfig.class);
    Assert.assertEquals(pinotConfigResponse.getConfigName(), configName);

    // delete & check
    ControllerTestUtils
        .sendDeleteRequest(ControllerTestUtils.getControllerRequestURLBuilder().forPinotConfigDelete(configName));
    getResponse =
        ControllerTestUtils.sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forPinotConfigsList());
    configs = JsonUtils.stringToObject(getResponse, new TypeReference<List<String>>() {
    });
    Assert.assertEquals(configs.size(), 0);
  }

  private TableConfig getTableConfig(String tableName, String tableType)
      throws Exception {
    String tableConfigString =
        ControllerTestUtils.sendGetRequest(ControllerTestUtils.getControllerRequestURLBuilder().forTableGet(tableName));
    return JsonUtils.jsonNodeToObject(JsonUtils.stringToJsonNode(tableConfigString).get(tableType), TableConfig.class);
  }

  @AfterClass
  public void tearDown() {
    ControllerTestUtils.cleanup();
  }
}
