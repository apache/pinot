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
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.List;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.spi.config.TableConfigs;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
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

import static org.testng.Assert.fail;


/**
 * Tests for CRUD APIs of {@link TableConfigs}
 */
public class TableConfigsRestletResourceTest extends ControllerTest {
  private String _createTableConfigsUrl;

  @BeforeClass
  public void setUp()
      throws Exception {
    DEFAULT_INSTANCE.setupSharedStateAndValidate();
    _createTableConfigsUrl = DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsCreate();
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
      throws IOException {
    String validateConfigUrl = DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsValidate();

    String tableName = "testValidate";
    TableConfig offlineTableConfig = createOfflineTableConfig(tableName);
    TableConfig realtimeTableConfig = createRealtimeTableConfig(tableName);
    Schema schema = createDummySchema(tableName);
    TableConfigs tableConfigs;

    // invalid json
    try {
      tableConfigs = new TableConfigs(tableName, schema, offlineTableConfig, realtimeTableConfig);
      sendPostRequest(validateConfigUrl, tableConfigs.toPrettyJsonString().replace("\"offline\"", "offline\""));
      fail("Creation of a TableConfigs with invalid json string should have failed");
    } catch (Exception e) {
      // expected
    }

    // null table configs
    try {
      tableConfigs = new TableConfigs(tableName, schema, null, null);
      sendPostRequest(validateConfigUrl, tableConfigs.toPrettyJsonString());
      fail("Creation of an TableConfigs with null table offline tableConfig and realtime tableConfig should have "
          + "failed");
    } catch (Exception e) {
      // expected
    }

    // null schema
    try {
      tableConfigs = new TableConfigs(tableName, null, offlineTableConfig, null);
      sendPostRequest(validateConfigUrl, tableConfigs.toPrettyJsonString());
      fail("Creation of an TableConfigs with null schema should have failed");
    } catch (Exception e) {
      // expected
    }

    // empty config name
    try {
      tableConfigs = new TableConfigs("", schema, offlineTableConfig, realtimeTableConfig);
      sendPostRequest(validateConfigUrl, tableConfigs.toPrettyJsonString());
      fail("Creation of an TableConfigs with empty config name should have failed");
    } catch (Exception e) {
      // expected
    }

    // schema name doesn't match config name
    try {
      tableConfigs =
          new TableConfigs(tableName, createDummySchema("differentName"), offlineTableConfig, realtimeTableConfig);
      sendPostRequest(validateConfigUrl, tableConfigs.toPrettyJsonString());
      fail("Creation of an TableConfigs with schema name different than tableName should have failed");
    } catch (Exception e) {
      // expected
    }

    // schema validation fails
    try {
      Schema schemaWithBlankSpace = createDummySchema(tableName);
      schemaWithBlankSpace.addField(new MetricFieldSpec("blank space", FieldSpec.DataType.LONG));
      tableConfigs = new TableConfigs(tableName, schemaWithBlankSpace, offlineTableConfig, realtimeTableConfig);
      sendPostRequest(validateConfigUrl, tableConfigs.toPrettyJsonString());
      fail("Creation of an TableConfigs with blank space in column should have failed");
    } catch (Exception e) {
      // expected
    }

    // offline table name doesn't match config name
    try {
      tableConfigs = new TableConfigs(tableName, schema, createOfflineTableConfig("differentName"), null);
      sendPostRequest(validateConfigUrl, tableConfigs.toPrettyJsonString());
      fail("Creation of an TableConfigs with offline table name different than tableName should have failed");
    } catch (Exception e) {
      // expected
    }

    // table name validation fails
    try {
      tableConfigs =
          new TableConfigs("blank space", createDummySchema("blank space"), createOfflineTableConfig("blank space"),
              null);
      sendPostRequest(validateConfigUrl, tableConfigs.toPrettyJsonString());
      fail("Creation of an TableConfigs with blank space in table name should have failed");
    } catch (Exception e) {
      // expected
    }

    // table validation fails
    try {
      TableConfig invalidTableConfig = createOfflineTableConfig(tableName);
      invalidTableConfig.getIndexingConfig().setInvertedIndexColumns(Lists.newArrayList("nonExistent"));
      tableConfigs = new TableConfigs(tableName, schema, invalidTableConfig, null);
      sendPostRequest(validateConfigUrl, tableConfigs.toPrettyJsonString());
      fail("Creation of an TableConfigs with invalid table config should have failed");
    } catch (Exception e) {
      // expected
    }

    // realtime table name doesn't match config name
    try {
      tableConfigs = new TableConfigs(tableName, schema, null, createRealtimeTableConfig("differentName"));
      sendPostRequest(validateConfigUrl, tableConfigs.toPrettyJsonString());
      fail("Creation of an TableConfigs with realtime table name different than tableName should have failed");
    } catch (Exception e) {
      // expected
    }

    // table name validation fails
    try {
      tableConfigs = new TableConfigs("blank space", createDummySchema("blank space"), null,
          createRealtimeTableConfig("blank space"));
      sendPostRequest(validateConfigUrl, tableConfigs.toPrettyJsonString());
      fail("Creation of an TableConfigs with blank space in table name should have failed");
    } catch (Exception e) {
      // expected
    }

    // table validation fails
    try {
      TableConfig invalidTableConfig = createRealtimeTableConfig(tableName);
      invalidTableConfig.getIndexingConfig().setInvertedIndexColumns(Lists.newArrayList("nonExistent"));
      tableConfigs = new TableConfigs(tableName, schema, null, invalidTableConfig);
      sendPostRequest(validateConfigUrl, tableConfigs.toPrettyJsonString());
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
      sendPostRequest(validateConfigUrl, tableConfigs.toPrettyJsonString());
      fail("Creation of an TableConfigs with inconsistencies across offline and realtime table config should have "
          + "failed");
    } catch (Exception e) {
      // expected
    }

    // successfully created with all 3 configs
    String tableName1 = "testValidate1";
    tableConfigs = new TableConfigs(tableName1, createDummySchema(tableName1), createOfflineTableConfig(tableName1),
        createRealtimeTableConfig(tableName1));
    sendPostRequest(validateConfigUrl, tableConfigs.toPrettyJsonString());

    // successfully create with offline config
    String tableName2 = "testValidate2";
    tableConfigs =
        new TableConfigs(tableName2, createDummySchema(tableName2), createOfflineTableConfig(tableName2), null);
    sendPostRequest(validateConfigUrl, tableConfigs.toPrettyJsonString());

    // successfully create with realtime config
    String tableName3 = "testValidate3";
    tableConfigs =
        new TableConfigs(tableName3, createDummySchema(tableName3), null, createRealtimeTableConfig(tableName3));
    sendPostRequest(validateConfigUrl, tableConfigs.toPrettyJsonString());

    sendDeleteRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsDelete(tableName1));
    sendDeleteRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsDelete(tableName2));
    sendDeleteRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsDelete(tableName3));
  }

  /**
   * Tests for creation of TableConfigs
   */
  @Test
  public void testCreateConfig()
      throws IOException {
    String tableName = "testCreate";
    TableConfig offlineTableConfig = createOfflineTableConfig(tableName);
    TableConfig realtimeTableConfig = createRealtimeTableConfig(tableName);
    Schema schema = createDummySchema(tableName);
    TableConfigs tableConfigs = new TableConfigs(tableName, schema, offlineTableConfig, realtimeTableConfig);
    sendPostRequest(_createTableConfigsUrl, tableConfigs.toPrettyJsonString());
    String response = sendGetRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsGet(tableName));
    TableConfigs tableConfigsResponse = JsonUtils.stringToObject(response, TableConfigs.class);
    Assert.assertEquals(tableConfigsResponse.getTableName(), tableName);
    Assert.assertEquals(tableConfigsResponse.getOffline().getTableName(), offlineTableConfig.getTableName());
    Assert.assertEquals(tableConfigsResponse.getRealtime().getTableName(), realtimeTableConfig.getTableName());
    Assert.assertEquals(tableConfigsResponse.getSchema().getSchemaName(), schema.getSchemaName());

    // test POST of existing configs fails
    try {
      sendPostRequest(_createTableConfigsUrl, tableConfigs.toPrettyJsonString());
      fail("Should fail for trying to add existing config");
    } catch (Exception e) {
      // expected
    }

    sendDeleteRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsDelete(tableName));

    // replica check
    tableName = "testCreateReplicas";
    TableConfig replicaTestOfflineTableConfig = createOfflineTableConfig(tableName);
    TableConfig replicaTestRealtimeTableConfig = createRealtimeTableConfig(tableName);
    replicaTestOfflineTableConfig.getValidationConfig().setReplication("1");
    replicaTestRealtimeTableConfig.getValidationConfig().setReplicasPerPartition("1");
    tableConfigs = new TableConfigs(tableName, createDummySchema(tableName), replicaTestOfflineTableConfig,
        replicaTestRealtimeTableConfig);
    sendPostRequest(_createTableConfigsUrl, tableConfigs.toPrettyJsonString());
    response = sendGetRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsGet(tableName));
    tableConfigsResponse = JsonUtils.stringToObject(response, TableConfigs.class);
    Assert.assertEquals(tableConfigsResponse.getTableName(), tableName);
    Assert.assertEquals(tableConfigsResponse.getOffline().getReplication(),
        DEFAULT_MIN_NUM_REPLICAS);
    Assert.assertEquals(tableConfigsResponse.getRealtime().getReplication(),
        DEFAULT_MIN_NUM_REPLICAS);
    sendDeleteRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsDelete(tableName));

    // quota check
    tableName = "testCreateQuota";
    TableConfig offlineDimTableConfig = createOfflineDimTableConfig(tableName);
    Schema dimSchema = createDummySchemaWithPrimaryKey(tableName);
    tableConfigs = new TableConfigs(tableName, dimSchema, offlineDimTableConfig, null);
    sendPostRequest(_createTableConfigsUrl, tableConfigs.toPrettyJsonString());
    response = sendGetRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsGet(tableName));
    tableConfigsResponse = JsonUtils.stringToObject(response, TableConfigs.class);
    Assert.assertEquals(tableName, tableConfigsResponse.getTableName());
    Assert.assertEquals(tableConfigsResponse.getOffline().getQuotaConfig().getStorage(),
        DEFAULT_INSTANCE.getControllerConfig().getDimTableMaxSize());
    sendDeleteRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsDelete(tableName));

    // tuner config
    tableName = "testTunerConfig";
    TableConfig offlineTunerTableConfig = createOfflineTunerTableConfig(tableName);
    TableConfig realtimeTunerTableConfig = createRealtimeTunerTableConfig(tableName);
    tableConfigs =
        new TableConfigs(tableName, createDummySchema(tableName), offlineTunerTableConfig, realtimeTunerTableConfig);
    sendPostRequest(_createTableConfigsUrl, tableConfigs.toPrettyJsonString());
    response = sendGetRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsGet(tableName));
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
    sendDeleteRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsDelete(tableName));
  }

  @Test
  public void testListConfigs()
      throws IOException {
    // create with 1 config
    String tableName1 = "testList1";
    TableConfig offlineTableConfig = createOfflineTableConfig(tableName1);
    TableConfig realtimeTableConfig = createRealtimeTableConfig(tableName1);
    Schema schema = createDummySchema(tableName1);
    TableConfigs tableConfigs = new TableConfigs(tableName1, schema, offlineTableConfig, null);
    sendPostRequest(_createTableConfigsUrl, tableConfigs.toPrettyJsonString());

    // list
    String getResponse = sendGetRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsList());
    List<String> configs = JsonUtils.stringToObject(getResponse, new TypeReference<List<String>>() {
    });
    Assert.assertEquals(configs.size(), 1);
    Assert.assertTrue(configs.containsAll(Sets.newHashSet(tableName1)));

    // update to 2
    tableConfigs = new TableConfigs(tableName1, schema, offlineTableConfig, realtimeTableConfig);
    sendPutRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsUpdate(tableName1),
        tableConfigs.toPrettyJsonString());

    // list
    getResponse = sendGetRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsList());
    configs = JsonUtils.stringToObject(getResponse, new TypeReference<List<String>>() {
    });
    Assert.assertEquals(configs.size(), 1);
    Assert.assertTrue(configs.containsAll(Sets.newHashSet("testList1")));

    // create new
    String tableName2 = "testList2";
    offlineTableConfig = createOfflineTableConfig(tableName2);
    schema = createDummySchema(tableName2);
    tableConfigs = new TableConfigs(tableName2, schema, offlineTableConfig, null);
    sendPostRequest(_createTableConfigsUrl, tableConfigs.toPrettyJsonString());

    // list
    getResponse = sendGetRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsList());
    configs = JsonUtils.stringToObject(getResponse, new TypeReference<List<String>>() {
    });
    Assert.assertEquals(configs.size(), 2);
    Assert.assertTrue(configs.containsAll(Sets.newHashSet(tableName1, tableName2)));

    // delete 1
    sendDeleteRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsDelete(tableName2));

    // list 1
    getResponse = sendGetRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsList());
    configs = JsonUtils.stringToObject(getResponse, new TypeReference<List<String>>() {
    });
    Assert.assertEquals(configs.size(), 1);
    Assert.assertTrue(configs.containsAll(Sets.newHashSet(tableName1)));

    sendDeleteRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsDelete(tableName1));
  }

  @Test
  public void testUpdateConfig()
      throws IOException {

    // create with 1
    String tableName = "testUpdate1";
    TableConfig offlineTableConfig = createOfflineTableConfig(tableName);
    TableConfig realtimeTableConfig = createRealtimeTableConfig(tableName);
    Schema schema = createDummySchema(tableName);
    TableConfigs tableConfigs = new TableConfigs(tableName, schema, offlineTableConfig, null);
    // PUT before POST should fail
    try {
      sendPutRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsUpdate(tableName),
          tableConfigs.toPrettyJsonString());
      fail("Should fail for trying to PUT config before creating via POST");
    } catch (Exception e) {
      // expected
    }
    sendPostRequest(_createTableConfigsUrl, tableConfigs.toPrettyJsonString());
    String response = sendGetRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsGet(tableName));
    TableConfigs tableConfigsResponse = JsonUtils.stringToObject(response, TableConfigs.class);
    Assert.assertEquals(tableConfigsResponse.getTableName(), tableName);
    Assert.assertEquals(tableConfigsResponse.getOffline().getTableName(), offlineTableConfig.getTableName());
    Assert.assertNull(tableConfigs.getRealtime());
    Assert.assertEquals(tableConfigsResponse.getSchema().getSchemaName(), schema.getSchemaName());

    // list
    String getResponse = sendGetRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsList());
    List<String> configs = JsonUtils.stringToObject(getResponse, new TypeReference<List<String>>() {
    });
    Assert.assertEquals(configs.size(), 1);
    Assert.assertTrue(configs.containsAll(Sets.newHashSet(tableName)));

    // update to 2
    tableConfigs = new TableConfigs(tableName, tableConfigsResponse.getSchema(), tableConfigsResponse.getOffline(),
        realtimeTableConfig);
    sendPutRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsUpdate(tableName),
        tableConfigs.toPrettyJsonString());
    response = sendGetRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsGet(tableName));
    tableConfigsResponse = JsonUtils.stringToObject(response, TableConfigs.class);
    Assert.assertEquals(tableConfigsResponse.getTableName(), tableName);
    Assert.assertEquals(tableConfigsResponse.getOffline().getTableName(), offlineTableConfig.getTableName());
    Assert.assertEquals(tableConfigsResponse.getRealtime().getTableName(), realtimeTableConfig.getTableName());
    Assert.assertEquals(tableConfigsResponse.getSchema().getSchemaName(), schema.getSchemaName());

    // list
    getResponse = sendGetRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsList());
    configs = JsonUtils.stringToObject(getResponse, new TypeReference<List<String>>() {
    });
    Assert.assertEquals(configs.size(), 1);
    Assert.assertTrue(configs.containsAll(Sets.newHashSet(tableName)));

    // update existing config
    schema.addField(new MetricFieldSpec("newMetric", FieldSpec.DataType.LONG));
    tableConfigs =
        new TableConfigs(tableName, schema, tableConfigsResponse.getOffline(), tableConfigsResponse.getRealtime());
    sendPutRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsUpdate(tableName),
        tableConfigs.toPrettyJsonString());
    response = sendGetRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsGet(tableName));
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
    sendPutRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsUpdate(tableName),
        tableConfigs.toPrettyJsonString());
    response = sendGetRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsGet(tableName));
    tableConfigsResponse = JsonUtils.stringToObject(response, TableConfigs.class);
    Assert.assertTrue(tableConfigsResponse.getOffline().getIndexingConfig().getInvertedIndexColumns().contains("dimA"));
    Assert.assertTrue(
        tableConfigsResponse.getRealtime().getIndexingConfig().getInvertedIndexColumns().contains("dimA"));

    sendDeleteRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsDelete(tableName));
  }

  @Test
  public void testForceUpdateTableSchemaAndConfigs()
      throws IOException {
    String tableName = "testUpdate1";
    TableConfig offlineTableConfig = createOfflineTableConfig(tableName);
    Schema schema = createDummySchema(tableName);
    TableConfigs tableConfigs = new TableConfigs(tableName, schema, offlineTableConfig, null);

    sendPostRequest(_createTableConfigsUrl, tableConfigs.toPrettyJsonString());
    String response = sendGetRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsGet(tableName));
    TableConfigs tableConfigsResponse = JsonUtils.stringToObject(response, TableConfigs.class);
    Assert.assertNotNull(tableConfigs.getOffline());

    // Remove field from schema and try to update schema without the 'forceTableSchemaUpdate' option
    schema.removeField("dimA");
    tableConfigs =
        new TableConfigs(tableName, schema, tableConfigsResponse.getOffline(), tableConfigsResponse.getRealtime());

    String tableConfigUpdateUrl = DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsUpdate(tableName);
    try {
      sendPutRequest(tableConfigUpdateUrl, tableConfigs.toPrettyJsonString());
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("is not backward-compatible with the existing schema"));
    }

    // Skip validate table configs – Exception is still thrown
    String newTableConfigUpdateUrl = tableConfigUpdateUrl + "?validationTypesToSkip=ALL";
    try {
      sendPutRequest(newTableConfigUpdateUrl, tableConfigs.toPrettyJsonString());
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("is not backward-compatible with the existing schema"));
    }

    // Skip table config validation as well as force update the table schema – no exceptions are thrown
    newTableConfigUpdateUrl = tableConfigUpdateUrl + "?validationTypesToSkip=ALL&forceTableSchemaUpdate=true";
    response = sendPutRequest(newTableConfigUpdateUrl, tableConfigs.toPrettyJsonString());
    Assert.assertTrue(response.contains("TableConfigs updated for testUpdate1"));
    sendDeleteRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsDelete(tableName));
  }

  @Test
  public void testDeleteConfig()
      throws Exception {
    // create with 1 config
    String tableName = "testDelete1";
    TableConfig offlineTableConfig = createOfflineTableConfig(tableName);
    Schema schema = createDummySchema(tableName);
    TableConfigs tableConfigs = new TableConfigs(tableName, schema, offlineTableConfig, null);
    sendPostRequest(_createTableConfigsUrl, tableConfigs.toPrettyJsonString());
    String response = sendGetRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsGet(tableName));
    TableConfigs tableConfigsResponse = JsonUtils.stringToObject(response, TableConfigs.class);
    Assert.assertEquals(tableConfigsResponse.getTableName(), tableName);

    // delete & check
    sendDeleteRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsDelete(tableName));
    String getResponse = sendGetRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsList());
    List<String> configs = JsonUtils.stringToObject(getResponse, new TypeReference<List<String>>() {
    });
    Assert.assertEquals(configs.size(), 0);

    tableName = "testDelete2";
    offlineTableConfig = createOfflineTableConfig(tableName);
    TableConfig realtimeTableConfig = createRealtimeTableConfig(tableName);
    schema = createDummySchema(tableName);
    tableConfigs = new TableConfigs(tableName, schema, offlineTableConfig, realtimeTableConfig);
    sendPostRequest(_createTableConfigsUrl, tableConfigs.toPrettyJsonString());
    response = sendGetRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsGet(tableName));
    tableConfigsResponse = JsonUtils.stringToObject(response, TableConfigs.class);
    Assert.assertEquals(tableConfigsResponse.getTableName(), tableName);

    // delete & check
    sendDeleteRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsDelete(tableName));
    getResponse = sendGetRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsList());
    configs = JsonUtils.stringToObject(getResponse, new TypeReference<List<String>>() {
    });
    Assert.assertEquals(configs.size(), 0);
  }

  @Test
  public void testUnrecognizedProperties()
      throws IOException {
    String tableName = "testUnrecognized1";
    TableConfig offlineTableConfig = createOfflineTableConfig(tableName);
    Schema schema = createDummySchema(tableName);
    TableConfigs tableConfigs = new TableConfigs(tableName, schema, offlineTableConfig, null);
    ObjectNode tableConfigsJson = JsonUtils.objectToJsonNode(tableConfigs).deepCopy();
    tableConfigsJson.put("illegalKey1", 1);

    // Validate
    DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsValidate();
    String response = sendPostRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsValidate(),
        tableConfigsJson.toPrettyString());
    JsonNode responseJson = JsonUtils.stringToJsonNode(response);
    Assert.assertTrue(responseJson.has("unrecognizedProperties"));
    Assert.assertTrue(responseJson.get("unrecognizedProperties").has("/illegalKey1"));

    // Create
    response = sendPostRequest(_createTableConfigsUrl, tableConfigsJson.toPrettyString());
    Assert.assertEquals(response, "{\"unrecognizedProperties\":{\"/illegalKey1\":1},\"status\":\"TableConfigs "
        + "testUnrecognized1 successfully added\"}");

    // Update
    response = sendPutRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsUpdate(tableName),
        tableConfigsJson.toPrettyString());
    Assert.assertEquals(response,
        "{\"unrecognizedProperties\":{\"/illegalKey1\":1},\"status\":\"TableConfigs updated for testUnrecognized1\"}");
    // Delete
    sendDeleteRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsDelete(tableName));
  }

  /**
   * Tests get TableConfigs for backwards compatibility
   */
  @Test
  public void testGetConfigCompatibility()
      throws IOException {
    // Should not fail if schema name does not match raw table name in the case they are created separately
    String schemaName = "schema1";
    Schema schema = createDummySchema(schemaName);
    sendPostRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forSchemaCreate(), schema.toPrettyJsonString());
    String tableName = "table1";
    TableConfig offlineTableConfig = createOfflineTableConfig(tableName);
    SegmentsValidationAndRetentionConfig validationConfig = new SegmentsValidationAndRetentionConfig();
    validationConfig.setSchemaName(schemaName);
    validationConfig.setReplication("1");
    offlineTableConfig.setValidationConfig(validationConfig);
    sendPostRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableCreate(),
        offlineTableConfig.toJsonString());

    String response = sendGetRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableConfigsGet(tableName));
    TableConfigs tableConfigsResponse = JsonUtils.stringToObject(response, TableConfigs.class);
    Assert.assertEquals(tableConfigsResponse.getTableName(), tableName);
    Assert.assertEquals(tableConfigsResponse.getOffline().getTableName(), offlineTableConfig.getTableName());
    Assert.assertEquals(tableConfigsResponse.getSchema().getSchemaName(), schema.getSchemaName());

    // Delete
    sendDeleteRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forTableDelete(tableName));
    sendDeleteRequest(DEFAULT_INSTANCE.getControllerRequestURLBuilder().forSchemaDelete(schemaName));
  }

  @AfterClass
  public void tearDown() {
    DEFAULT_INSTANCE.cleanup();
  }
}
