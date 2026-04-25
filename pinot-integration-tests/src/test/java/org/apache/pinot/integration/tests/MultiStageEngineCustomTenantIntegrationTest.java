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
package org.apache.pinot.integration.tests;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;


public class MultiStageEngineCustomTenantIntegrationTest extends MultiStageEngineIntegrationTest {
  private static final String TEST_TENANT = "TestTenant";
  private static final String DATABASE_NAME = "db1";
  private static final String TABLE_NAME_WITH_DATABASE = DATABASE_NAME + "." + DEFAULT_TABLE_NAME;
  private static final String DIM_TABLE_DATA_PATH = "dimDayOfWeek_data.csv";
  private static final String DIM_TABLE_SCHEMA_PATH = "dimDayOfWeek_schema.json";
  private static final String DIM_TABLE_TABLE_CONFIG_PATH = "dimDayOfWeek_config.json";
  private static final Integer DIM_NUMBER_OF_RECORDS = 7;
  private static final String DIM_TABLE = "daysOfWeek";

  private File _classTempDir;
  private File _classSegmentDir;
  private File _classTarDir;
  private String _tableName = DEFAULT_TABLE_NAME;
  private String _originalMaxServerQueryThreads;
  private boolean _clusterConfigOverrideApplied;

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    properties.put(ControllerConf.CLUSTER_TENANT_ISOLATION_ENABLE, false);
  }

  @Override
  protected String getBrokerTenant() {
    return TEST_TENANT;
  }

  @Override
  protected String getServerTenant() {
    return TEST_TENANT;
  }

  @Override
  protected int getSharedNumBrokers() {
    return 1;
  }

  @Override
  protected int getSharedNumServers() {
    return 1;
  }

  @Override
  protected boolean shouldStartSharedKafka() {
    return false;
  }

  @Override
  protected boolean shouldStartSharedMinion() {
    return false;
  }

  @Override
  protected String getTableName() {
    return _tableName;
  }

  @BeforeClass
  @Override
  public void setUp()
      throws Exception {
    _classTempDir = getClassTempDir();
    _classSegmentDir = new File(_classTempDir, "segmentDir");
    _classTarDir = new File(_classTempDir, "tarDir");
    TestUtils.ensureDirectoriesExistAndEmpty(_classTempDir, _classSegmentDir, _classTarDir);

    startZk();
    startController();
    applyClusterConfigOverride();

    startBroker();
    startServer();
    validateSharedClusterConfiguration();

    cleanTablesAndSchemas();
    cleanTenants();
    setupTenants();

    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    List<File> avroFiles = unpackAvroData(_classTempDir);

    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _classSegmentDir,
        _classTarDir);
    uploadSegments(getTableName(), _classTarDir);

    setUpH2Connection(avroFiles);
    setUpQueryGenerator(avroFiles);
    waitForAllDocsLoaded(600_000L);

    setupTableWithNonDefaultDatabase(avroFiles);
    setupDimensionTable();
  }

  @AfterClass(alwaysRun = true)
  @Override
  public void tearDown()
      throws Exception {
    Exception exception = null;
    exception = runCleanup(exception, this::cleanTablesAndSchemas);
    exception = runCleanup(exception, this::cleanTenants);
    exception = runCleanup(exception, this::restoreClusterConfigOverride);
    exception = runCleanup(exception, this::closeH2Connection);
    exception = runCleanup(exception, this::closePinotConnections);
    exception = runCleanup(exception, this::stopServer);
    exception = runCleanup(exception, this::stopBroker);
    exception = runCleanup(exception, this::stopController);
    exception = runCleanup(exception, this::stopZk);
    exception = runCleanup(exception, this::cleanClassTempDirectory);
    if (exception != null) {
      throw exception;
    }
  }

  @Override
  protected void setupTenants()
      throws IOException {
    createBrokerTenant(getBrokerTenant(), 1);
    createServerTenant(getServerTenant(), 1, 0);
  }

  private File getClassTempDir() {
    return isSharedRichClusterEnabled() ? new File(_tempDir, "testData") : _tempDir;
  }

  private void setupTableWithNonDefaultDatabase(List<File> avroFiles)
      throws Exception {
    _tableName = TABLE_NAME_WITH_DATABASE;
    String defaultCol = "ActualElapsedTime";
    String customCol = "ActualElapsedTime_2";
    Schema schema = createSchema();
    schema.addField(new MetricFieldSpec(customCol, FieldSpec.DataType.INT));
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    assert tableConfig.getIndexingConfig().getNoDictionaryColumns() != null;
    List<String> noDicCols = new ArrayList<>(DEFAULT_NO_DICTIONARY_COLUMNS);
    noDicCols.add(customCol);
    tableConfig.getIndexingConfig().setNoDictionaryColumns(noDicCols);
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(List.of(new TransformConfig(customCol, defaultCol)));
    tableConfig.setIngestionConfig(ingestionConfig);
    addTableConfig(tableConfig);

    TestUtils.ensureDirectoriesExistAndEmpty(_classSegmentDir, _classTarDir);
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _classSegmentDir,
        _classTarDir);
    uploadSegments(getTableName(), _classTarDir);

    waitForAllDocsLoaded(600_000L);
    _tableName = DEFAULT_TABLE_NAME;
  }

  private void setupDimensionTable()
      throws Exception {
    Schema lookupTableSchema = createSchema(DIM_TABLE_SCHEMA_PATH);
    addSchema(lookupTableSchema);
    TableConfig tableConfig = createTableConfig(DIM_TABLE_TABLE_CONFIG_PATH);
    tableConfig.setTenantConfig(new TenantConfig(getBrokerTenant(), getServerTenant(), null));
    addTableConfig(tableConfig);
    createAndUploadSegmentFromClasspath(tableConfig, lookupTableSchema, DIM_TABLE_DATA_PATH, FileFormat.CSV,
        DIM_NUMBER_OF_RECORDS, 60_000);
  }

  private void cleanTablesAndSchemas()
      throws Exception {
    if (_helixResourceManager == null) {
      return;
    }
    cleanOfflineTableAndSchema(DEFAULT_TABLE_NAME);
    cleanOfflineTableAndSchema(TABLE_NAME_WITH_DATABASE);
    cleanOfflineTableAndSchema(DIM_TABLE);
  }

  private void cleanOfflineTableAndSchema(String tableName)
      throws Exception {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    if (_helixResourceManager.getTableConfig(offlineTableName) != null || _helixResourceManager.hasOfflineTable(
        tableName)) {
      dropOfflineTable(tableName);
      waitForTableDataManagerRemoved(offlineTableName);
      waitForEVToDisappear(offlineTableName);
    }
    if (_helixResourceManager.getSchema(tableName) != null) {
      deleteSchema(tableName);
    }
  }

  private void cleanTenants() {
    if (_helixResourceManager == null) {
      return;
    }
    try {
      getOrCreateAdminClient().getTenantClient().deleteTenant(TEST_TENANT, "BROKER");
    } catch (Exception e) {
      // Tenant may not exist if setup failed before creation.
    }
    try {
      getOrCreateAdminClient().getTenantClient().deleteTenant(TEST_TENANT, "SERVER");
    } catch (Exception e) {
      // Tenant may not exist if setup failed before creation.
    }
  }

  private void applyClusterConfigOverride() {
    HelixConfigScope scope = getClusterConfigScope();
    ConfigAccessor configAccessor = _helixManager.getConfigAccessor();
    _originalMaxServerQueryThreads =
        configAccessor.get(scope, CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_THREADS);
    try {
      configAccessor.set(scope, CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_THREADS, "30");
      _clusterConfigOverrideApplied = true;
    } catch (RuntimeException e) {
      restoreClusterConfig(configAccessor, scope,
          CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_THREADS,
          _originalMaxServerQueryThreads);
      throw e;
    }
  }

  private void restoreClusterConfigOverride() {
    if (!_clusterConfigOverrideApplied || _helixManager == null) {
      return;
    }

    restoreClusterConfig(_helixManager.getConfigAccessor(), getClusterConfigScope(),
        CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_THREADS, _originalMaxServerQueryThreads);
    _clusterConfigOverrideApplied = false;
  }

  private void restoreClusterConfig(ConfigAccessor configAccessor, HelixConfigScope scope, String key,
      String originalValue) {
    if (originalValue == null) {
      configAccessor.remove(scope, key);
    } else {
      configAccessor.set(scope, key, originalValue);
    }
  }

  private HelixConfigScope getClusterConfigScope() {
    return new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER)
        .forCluster(getHelixClusterName())
        .build();
  }

  private void validateSharedClusterConfiguration() {
    if (!isSharedRichClusterEnabled()) {
      return;
    }

    Assert.assertEquals(_brokerStarters.size(), 1,
        "Shared rich cluster must be started with one broker for this test");
    Assert.assertEquals(_serverStarters.size(), 1,
        "Shared rich cluster must be started with one server for this test");
    Assert.assertTrue(_kafkaStarters == null || _kafkaStarters.isEmpty(),
        "Shared rich cluster must be started without Kafka for this test");
    Assert.assertNull(_minionStarter, "Shared rich cluster must be started without a minion for this test");
    Assert.assertFalse(Boolean.parseBoolean(String.valueOf(_controllerConfig.toMap()
        .get(ControllerConf.CLUSTER_TENANT_ISOLATION_ENABLE))),
        "Shared rich cluster must disable controller tenant isolation for this test");
  }

  private void closeH2Connection()
      throws Exception {
    if (_h2Connection != null) {
      _h2Connection.close();
      _h2Connection = null;
    }
  }

  private void closePinotConnections() {
    if (_pinotConnection != null) {
      _pinotConnection.close();
      _pinotConnection = null;
    }
    if (_pinotConnectionV2 != null) {
      _pinotConnectionV2.close();
      _pinotConnectionV2 = null;
    }
  }

  private void cleanClassTempDirectory()
      throws Exception {
    if (_classTempDir != null) {
      FileUtils.deleteDirectory(_classTempDir);
    }
  }

  private Exception runCleanup(Exception firstException, Cleanup cleanup) {
    try {
      cleanup.run();
    } catch (Exception e) {
      if (firstException == null) {
        return e;
      }
      firstException.addSuppressed(e);
    }
    return firstException;
  }

  private interface Cleanup {
    void run()
        throws Exception;
  }
}
