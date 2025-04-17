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
package org.apache.pinot.integration.tests.logicaltable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.controller.helix.ControllerRequestClient;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.integration.tests.BaseClusterIntegrationTestSet;
import org.apache.pinot.integration.tests.ClusterIntegrationTestUtils;
import org.apache.pinot.integration.tests.QueryGenerator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public abstract class BaseLogicalTableIntegrationTest extends BaseClusterIntegrationTestSet {
  protected static final Logger LOGGER = LoggerFactory.getLogger(BaseLogicalTableIntegrationTest.class);
  private static final String DEFAULT_TENANT = "DefaultTenant";
  private static final String DEFAULT_LOGICAL_TABLE_NAME = "mytable";
  protected static final String DEFAULT_TABLE_NAME = "physicalTable";
  private static final int NUM_OFFLINE_SEGMENTS = 12;
  protected static BaseLogicalTableIntegrationTest _sharedClusterTestSuite = null;

  @BeforeSuite
  public void setUpSuite()
      throws Exception {
    LOGGER.info("Setting up integration test suite");
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    _sharedClusterTestSuite = this;

    // Start the Pinot cluster
    startZk();
    LOGGER.info("Start Kafka in the integration test suite");
    startKafka();
    startController();
    startBroker();
    startServers(2);
    LOGGER.info("Finished setting up integration test suite");
  }

  @AfterSuite
  public void tearDownSuite()
      throws Exception {
    LOGGER.info("Tearing down integration test suite");
    // Stop Kafka
    LOGGER.info("Stop Kafka in the integration test suite");
    stopKafka();
    // Shutdown the Pinot cluster
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
    LOGGER.info("Finished tearing down integration test suite");
  }

  @Override
  protected String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    if (_sharedClusterTestSuite != this) {
      _controllerRequestURLBuilder = _sharedClusterTestSuite._controllerRequestURLBuilder;
    }

    List<File> avroFiles = getAllAvroFiles();
    int numSegmentsPerTable = NUM_OFFLINE_SEGMENTS / getOfflineTableNames().size();
    int index = 0;
    for (String tableName : getOfflineTableNames()) {
      File tarDir = new File(_tarDir, tableName);

      TestUtils.ensureDirectoriesExistAndEmpty(tarDir);

      // Create and upload the schema and table config
      Schema schema = createSchema(getSchemaFileName());
      schema.setSchemaName(tableName);
      addSchema(schema);
      TableConfig offlineTableConfig = createOfflineTableConfig(tableName);
      addTableConfig(offlineTableConfig);

      List<File> offlineAvroFiles = new ArrayList<>(numSegmentsPerTable);
      for (int i = index; i < index + numSegmentsPerTable; i++) {
        offlineAvroFiles.add(avroFiles.get(i));
      }
      index += numSegmentsPerTable;

      // Create and upload segments
      ClusterIntegrationTestUtils.buildSegmentsFromAvro(offlineAvroFiles, offlineTableConfig, schema, 0, _segmentDir,
          tarDir);
      uploadSegments(tableName, tarDir);
    }

    createLogicalTable();

    // Set up the H2 connection
    setUpH2Connection(avroFiles);

    // Initialize the query generator
    setUpQueryGenerator(avroFiles);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    // Try deleting the tables and check that they have no routing table
    for (String tableName : getOfflineTableNames()) {
      dropOfflineTable(tableName);
    }

    for (String tableName : getOfflineTableNames()) {
      // Routing should be removed after deleting all tables
      TestUtils.waitForCondition(aVoid -> {
        try {
          getDebugInfo("debug/routingTable/" + tableName);
          return false;
        } catch (Exception e) {
          // only return true if 404 not found error is thrown.
          return e.getMessage().contains("Got error status code: 404");
        }
      }, 60_000L, "Routing table is not empty after dropping all tables");
    }

    deleteLogicalTable();
  }

  protected abstract List<String> getOfflineTableNames();

  protected List<String> getPhysicalTableNames() {
    return getOfflineTableNames().stream().map(TableNameBuilder.OFFLINE::tableNameWithType)
        .collect(Collectors.toList());
  }

  protected String getLogicalTableName() {
    return DEFAULT_LOGICAL_TABLE_NAME;
  }

  protected Map<String, String> getHeaders() {
    return Map.of();
  }

  protected String getBrokerTenant() {
    return DEFAULT_TENANT;
  }

  @Override
  protected void setUpH2Connection(List<File> avroFiles)
      throws Exception {
    setUpH2Connection();
    ClusterIntegrationTestUtils.setUpH2TableWithAvro(avroFiles, getLogicalTableName(), _h2Connection);
  }

  /**
   * Creates a new OFFLINE table config.
   */
  protected TableConfig createOfflineTableConfig(String tableName) {
    // @formatter:off
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(tableName)
        .setTimeColumnName(getTimeColumnName())
        .setSortedColumn(getSortedColumn())
        .setInvertedIndexColumns(getInvertedIndexColumns())
        .setNoDictionaryColumns(getNoDictionaryColumns())
        .setRangeIndexColumns(getRangeIndexColumns())
        .setBloomFilterColumns(getBloomFilterColumns())
        .setFieldConfigList(getFieldConfigs())
        .setNumReplicas(getNumReplicas())
        .setSegmentVersion(getSegmentVersion())
        .setLoadMode(getLoadMode())
        .setTaskConfig(getTaskConfig())
        .setBrokerTenant(getBrokerTenant())
        .setServerTenant(getServerTenant())
        .setIngestionConfig(getIngestionConfig())
        .setQueryConfig(getQueryConfig())
        .setNullHandlingEnabled(getNullHandlingEnabled())
        .setSegmentPartitionConfig(getSegmentPartitionConfig())
        .build();
    // @formatter:on
  }

  protected void createLogicalTable()
      throws IOException {
    String addLogicalTableUrl = _controllerRequestURLBuilder.forLogicalTableCreate();
    Schema logicalTableSchema = createSchema(getSchemaFileName());
    logicalTableSchema.setSchemaName(getLogicalTableName());
    addSchema(logicalTableSchema);
    LogicalTableConfig
        logicalTable = getDummyLogicalTableConfig(getLogicalTableName(), getPhysicalTableNames(), getBrokerTenant());
    String resp =
        ControllerTest.sendPostRequest(addLogicalTableUrl, logicalTable.toSingleLineJsonString(), getHeaders());
    assertEquals(resp, "{\"unrecognizedProperties\":{},\"status\":\"" + getLogicalTableName()
        + " logical table successfully added.\"}");
  }

  protected LogicalTableConfig getLogicalTableConfig(String logicalTableName)
      throws IOException {
    String getLogicalTableUrl =
        _controllerRequestURLBuilder.forLogicalTableGet(logicalTableName);
    String resp = ControllerTest.sendGetRequest(getLogicalTableUrl, getHeaders());
    return LogicalTableConfig.fromString(resp);
  }

  protected void deleteLogicalTable()
      throws IOException {
    String deleteLogicalTableUrl = _controllerRequestURLBuilder.forLogicalTableDelete(getLogicalTableName());
    // delete logical table
    String deleteResponse = ControllerTest.sendDeleteRequest(deleteLogicalTableUrl, getHeaders());
    assertEquals(deleteResponse, "{\"status\":\"" + getLogicalTableName() + " logical table successfully deleted.\"}");
  }

  @Override
  protected void pushAvroIntoKafka(List<File> avroFiles)
      throws Exception {
    ClusterIntegrationTestUtils.pushAvroIntoKafka(avroFiles,
        "localhost:" + _sharedClusterTestSuite._kafkaStarters.get(0).getPort(), getKafkaTopic(),
        getMaxNumKafkaMessagesPerBatch(), getKafkaMessageHeader(), getPartitionColumn(), injectTombstones());
  }

  @Override
  public String getZkUrl() {
    if (_sharedClusterTestSuite != this) {
      return _sharedClusterTestSuite.getZkUrl();
    }
    return super.getZkUrl();
  }

  @Override
  public ControllerRequestClient getControllerRequestClient() {
    if (_sharedClusterTestSuite != this) {
      return _sharedClusterTestSuite.getControllerRequestClient();
    }
    return super.getControllerRequestClient();
  }

  @Override
  protected String getBrokerBaseApiUrl() {
    if (_sharedClusterTestSuite != this) {
      return _sharedClusterTestSuite.getBrokerBaseApiUrl();
    }
    return super.getBrokerBaseApiUrl();
  }

  @Override
  protected String getBrokerGrpcEndpoint() {
    if (_sharedClusterTestSuite != this) {
      return _sharedClusterTestSuite.getBrokerGrpcEndpoint();
    }
    return super.getBrokerGrpcEndpoint();
  }

  @Override
  public int getControllerPort() {
    if (_sharedClusterTestSuite != this) {
      return _sharedClusterTestSuite.getControllerPort();
    }
    return super.getControllerPort();
  }

  @Override
  public int getRandomBrokerPort() {
    if (_sharedClusterTestSuite != this) {
      return _sharedClusterTestSuite.getRandomBrokerPort();
    }
    return super.getRandomBrokerPort();
  }

  @Override
  public String getHelixClusterName() {
    return "BaseLogicalTableIntegrationTest";
  }

  @Override
  protected void waitForAllDocsLoaded(long timeoutMs)
      throws Exception {
    waitForDocsLoaded(timeoutMs, true, getLogicalTableName());
  }

  @Override
  protected void setUpQueryGenerator(List<File> avroFiles) {
    Assert.assertNull(_queryGenerator);
    String tableName = getLogicalTableName();
    _queryGenerator = new QueryGenerator(avroFiles, tableName, tableName);
  }

  @Test
  public void verifyLogicalTableConfig()
      throws IOException {
    LogicalTableConfig logicalTableConfig = getLogicalTableConfig(getLogicalTableName());
    assertEquals(logicalTableConfig.getPhysicalTableConfigMap().size(), getPhysicalTableNames().size());
    assertEquals(new HashSet<>(getPhysicalTableNames()), logicalTableConfig.getPhysicalTableConfigMap().keySet());
  }

  @Test
  public void testHardcodedQueries()
      throws Exception {
    super.testHardcodedQueries();
  }

  @Test
  public void testQueriesFromQueryFile()
      throws Exception {
    super.testQueriesFromQueryFile();
  }

  @Test
  public void testGeneratedQueries()
      throws Exception {
    super.testGeneratedQueries(true, false);
  }
}
