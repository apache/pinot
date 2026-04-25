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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.commons.io.FileUtils;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner.KEY_OF_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class MultiStageEngineSmallBufferTest extends BaseClusterIntegrationTestSet {

  private static final String SCHEMA_FILE_NAME = "On_Time_On_Time_Performance_2014_100k_subset_nonulls.schema";
  private static final String SHARED_TABLE_NAME = "multi_stage_engine_small_buffer";
  private static final int NUM_SERVERS = 4;
  private static final int INBOUND_BLOCK_SIZE = 256;

  private File _classTempDir;
  private File _classSegmentDir;
  private File _classTarDir;
  private String _originalMaxServerQueryThreads;
  private boolean _clusterConfigOverrideApplied;

  @BeforeClass
  public void setUp()
      throws Exception {
    _classTempDir = getClassTempDir();
    _classSegmentDir = new File(_classTempDir, "segmentDir");
    _classTarDir = new File(_classTempDir, "tarDir");
    TestUtils.ensureDirectoriesExistAndEmpty(_classTempDir, _classSegmentDir, _classTarDir);

    // Start the Pinot cluster
    startZk();
    startController();

    // Set the multi-stage max server query threads for the cluster, so that we can test the query queueing logic
    // in the MultiStageBrokerRequestHandler
    applyClusterConfigOverride();

    startBroker();
    startServer();
    validateSharedClusterConfiguration();

    cleanTableAndSchema();

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_classTempDir);

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _classSegmentDir,
        _classTarDir);
    uploadSegments(getTableName(), _classTarDir);

    // Set up the H2 connection
    setUpH2Connection(avroFiles);

    // Initialize the query generator
    setUpQueryGenerator(avroFiles);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  @Override
  protected void startServer()
      throws Exception {
    startServers(NUM_SERVERS);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    Exception exception = null;
    exception = runCleanup(exception, this::cleanTableAndSchema);
    exception = runCleanup(exception, this::restoreClusterConfigOverride);
    exception = runCleanup(exception, this::closeH2Connection);
    exception = runCleanup(exception, this::closePinotConnections);
    exception = runCleanup(exception, this::stopServer);
    exception = runCleanup(exception, this::stopBroker);
    exception = runCleanup(exception, this::stopController);
    exception = runCleanup(exception, this::stopZk);
    exception = runCleanup(exception, this::cleanTempDirectory);
    if (exception != null) {
      throw exception;
    }
  }

  @Override
  protected String getSchemaFileName() {
    return SCHEMA_FILE_NAME;
  }

  @Override
  protected String getTableName() {
    return isSharedRichClusterEnabled() ? SHARED_TABLE_NAME : DEFAULT_TABLE_NAME;
  }

  @BeforeMethod
  @Override
  public void resetMultiStage() {
    setUseMultiStageQueryEngine(true);
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    brokerConf.setProperty(KEY_OF_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES, INBOUND_BLOCK_SIZE);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    serverConf.setProperty(KEY_OF_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES, INBOUND_BLOCK_SIZE);
  }

  @Test(invocationCount = 50)
  public void testConcurrentSplittedMailboxes()
      throws Exception {
    int numClients = 32;

    String query =
        "SELECT ActualElapsedTime FROM " + getTableName() + " ORDER BY ActualElapsedTime DESC LIMIT 100";
    String expected = "[[678],[668],[662],[658],[651],[650],[647],[629],[625],[625],[621],[617],[610],[610],[607],[605]"
        + ",[603],[582],[578],[576],[574],[572],[572],[566],[565],[564],[558],[555],[555],[554],[554],[553],[552],[550]"
        + ",[549],[544],[543],[541],[540],[538],[537],[535],[533],[532],[526],[521],[520],[519],[518],[516],[515],[514]"
        + ",[508],[508],[507],[505],[505],[502],[502],[499],[499],[498],[495],[494],[494],[493],[490],[487],[487],[484]"
        + ",[484],[481],[481],[480],[479],[478],[478],[477],[473],[472],[471],[468],[465],[464],[464],[463],[461],[461]"
        + ",[460],[459],[459],[458],[457],[456],[453],[453],[451],[451],[450],[450]]";

    List<String> results = Collections.synchronizedList(new ArrayList<>(numClients));

    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(numClients);
    for (int i = 0; i < numClients; i++) {
      new Thread(() -> {
        try {
          startLatch.await();
          JsonNode jsonNode = postQuery(query);
          assertNotNull(jsonNode);
          String actual = jsonNode.get("resultTable").get("rows").toString();
          results.add(actual);
        } catch (Exception e) {
          results.add("Error: " + e.getMessage());
          e.printStackTrace();
        } finally {
          doneLatch.countDown();
        }
      }).start();
    }

    startLatch.countDown();
    doneLatch.await();

    assertEquals(results.size(), numClients);
    for (String result : results) {
      assertEquals(result, expected);
    }
  }

  private File getClassTempDir() {
    return isSharedRichClusterEnabled() ? new File(_tempDir, "testData") : _tempDir;
  }

  private void cleanTableAndSchema()
      throws Exception {
    if (_helixResourceManager == null) {
      return;
    }

    String tableName = getTableName();
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    if (_helixResourceManager.getAllTables().contains(offlineTableName) || _helixResourceManager.hasOfflineTable(
        tableName)) {
      dropOfflineTable(tableName);
      waitForTableDataManagerRemoved(offlineTableName);
      waitForEVToDisappear(offlineTableName);
    }
    if (_helixResourceManager.getSchema(tableName) != null) {
      deleteSchema(tableName);
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

    assertEquals(_serverStarters.size(), NUM_SERVERS,
        "Shared rich cluster must be started with -Dpinot.integration.sharedRichCluster.numServers=" + NUM_SERVERS);
    _brokerStarters.forEach(brokerStarter -> assertEquals(brokerStarter.getConfig()
        .getProperty(KEY_OF_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES, -1), INBOUND_BLOCK_SIZE,
        "Shared rich cluster broker must use the small inbound query data block size"));
    _serverStarters.forEach(serverStarter -> assertEquals(serverStarter.getConfig()
        .getProperty(KEY_OF_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES, -1), INBOUND_BLOCK_SIZE,
        "Shared rich cluster server must use the small inbound query data block size"));
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

  private void cleanTempDirectory()
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
