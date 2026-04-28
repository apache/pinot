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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration test that converts Avro data for 12 segments and runs queries against it.
 */
public class GrpcBrokerClusterIntegrationTest extends SharedRichClusterIntegrationTest {
  private static final String SHARED_TABLE_NAME = "grpc_broker_cluster";
  private static final String SHARED_KAFKA_TOPIC = "grpc_broker_cluster";
  private static final String TENANT_NAME = "TestTenant";
  private static final int NUM_OFFLINE_SEGMENTS = 8;
  private static final int NUM_REALTIME_SEGMENTS = 6;

  private File _classTempDir;
  private File _classSegmentDir;
  private File _classTarDir;

  @Override
  protected String getTableName() {
    return isSharedRichClusterEnabled() ? SHARED_TABLE_NAME : super.getTableName();
  }

  @Override
  protected String getKafkaTopic() {
    return isSharedRichClusterEnabled() ? SHARED_KAFKA_TOPIC : super.getKafkaTopic();
  }

  @Override
  protected String getBrokerTenant() {
    return TENANT_NAME;
  }

  @Override
  protected String getServerTenant() {
    return TENANT_NAME;
  }

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    properties.put(ControllerConf.CLUSTER_TENANT_ISOLATION_ENABLE, false);
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    brokerConf.setProperty(Broker.BROKER_REQUEST_HANDLER_TYPE, "grpc");
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    serverConf.setProperty(Server.CONFIG_OF_ENABLE_GRPC_SERVER, true);
  }

  @Override
  protected int getSharedNumBrokers() {
    return 1;
  }

  @Override
  protected int getSharedNumServers() {
    return 2;
  }

  @Override
  protected boolean shouldStartSharedKafka() {
    return true;
  }

  @Override
  protected boolean shouldStartSharedMinion() {
    return false;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    _classTempDir = getClassTempDir();
    _classSegmentDir = getClassSegmentDir();
    _classTarDir = getClassTarDir();
    TestUtils.ensureDirectoriesExistAndEmpty(_classTempDir, _classSegmentDir, _classTarDir);

    // Start Zk, Kafka and Pinot
    startHybridCluster();
    cleanTableAndSchema();
    cleanTenants();
    createBrokerTenant(TENANT_NAME, 1);
    createServerTenant(TENANT_NAME, 1, 1);
    resetKafkaTopic();

    List<File> avroFiles = getClassAvroFiles();
    List<File> offlineAvroFiles = getOfflineAvroFiles(avroFiles, NUM_OFFLINE_SEGMENTS);
    List<File> realtimeAvroFiles = getRealtimeAvroFiles(avroFiles, NUM_REALTIME_SEGMENTS);

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig offlineTableConfig = createOfflineTableConfig();
    addTableConfig(offlineTableConfig);
    addTableConfig(createRealtimeTableConfig(realtimeAvroFiles.get(0)));

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(offlineAvroFiles, offlineTableConfig, schema, 0,
        _classSegmentDir, _classTarDir);
    uploadSegments(getTableName(), _classTarDir);

    // Push data into Kafka
    pushAvroIntoKafka(realtimeAvroFiles);

    // Set up the H2 connection
    setUpH2Connection(avroFiles);

    // Initialize the query generator
    setUpQueryGenerator(avroFiles);
  }

  protected void startHybridCluster()
      throws Exception {
    startZk();
    startController();
    startBroker();
    startServers(2);
    startKafkaWithoutTopic();
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testGrpcBrokerRequestHandlerOnSelectionOnlyQuery(boolean useMultiStageQueryEngine)
    throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String tableName = getTableName();
    String query = "SELECT * FROM " + tableName + " LIMIT 1000000";
    testQuery(query);
    query = "SELECT * FROM " + tableName + " WHERE DaysSinceEpoch > 16312 LIMIT 10000000";
    testQuery(query);
    query = "SELECT ArrTime, DaysSinceEpoch, Carrier FROM " + tableName + " LIMIT 10000000";
    testQuery(query);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    Exception exception = null;
    exception = runCleanup(exception, this::cleanTableAndSchema);
    exception = runCleanup(exception, this::deleteKafkaTopicIfPresent);
    exception = runCleanup(exception, this::cleanTenants);
    exception = runCleanup(exception, this::closeH2Connection);
    exception = runCleanup(exception, this::closePinotConnections);
    exception = runCleanup(exception, this::stopServer);
    exception = runCleanup(exception, this::stopBroker);
    exception = runCleanup(exception, this::stopController);
    exception = runCleanup(exception, this::stopKafka);
    exception = runCleanup(exception, this::stopZk);
    exception = runCleanup(exception, this::deleteClassTempDir);
    if (exception != null) {
      throw exception;
    }
  }

  private File getClassTempDir() {
    return isSharedRichClusterEnabled()
        ? new File(FileUtils.getTempDirectory(), getClass().getSimpleName() + "-shared")
        : _tempDir;
  }

  private File getClassSegmentDir() {
    return isSharedRichClusterEnabled() ? new File(_classTempDir, "segmentDir") : _segmentDir;
  }

  private File getClassTarDir() {
    return isSharedRichClusterEnabled() ? new File(_classTempDir, "tarDir") : _tarDir;
  }

  private List<File> getClassAvroFiles()
      throws Exception {
    int numSegments = unpackAvroData(_classTempDir).size();

    List<File> avroFiles = new ArrayList<>(numSegments);
    for (int i = 1; i <= numSegments; i++) {
      avroFiles.add(new File(_classTempDir, "On_Time_On_Time_Performance_2014_" + i + ".avro"));
    }
    return avroFiles;
  }

  private void cleanTableAndSchema()
      throws Exception {
    if (_helixResourceManager == null) {
      return;
    }

    String tableName = getTableName();
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    if (_helixResourceManager.getTableConfig(offlineTableName) != null
        || _helixResourceManager.hasOfflineTable(tableName)) {
      dropOfflineTable(tableName);
      waitForTableDataManagerRemoved(offlineTableName);
      waitForEVToDisappear(offlineTableName);
    }

    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    if (_helixResourceManager.getTableConfig(realtimeTableName) != null
        || _helixResourceManager.hasRealtimeTable(tableName)) {
      dropRealtimeTable(tableName);
      waitForTableDataManagerRemoved(realtimeTableName);
      waitForEVToDisappear(realtimeTableName);
    }

    if (_helixResourceManager.getSchema(tableName) != null) {
      deleteSchema(tableName);
    }
  }

  private void resetKafkaTopic() {
    deleteKafkaTopicIfPresent();
    createKafkaTopic(getKafkaTopic());
  }

  private void deleteKafkaTopicIfPresent() {
    if (isKafkaTopicPresent()) {
      deleteKafkaTopic(getKafkaTopic());
    }
  }

  private boolean isKafkaTopicPresent() {
    if (_kafkaStarters == null || _kafkaStarters.isEmpty()) {
      return false;
    }
    Properties adminProps = new Properties();
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBrokerList());
    adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
    adminProps.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
    try (AdminClient adminClient = AdminClient.create(adminProps)) {
      return adminClient.listTopics().names().get(5, TimeUnit.SECONDS).contains(getKafkaTopic());
    } catch (Exception e) {
      return false;
    }
  }

  private void cleanTenants() {
    if (!isSharedRichClusterEnabled()) {
      return;
    }
    try {
      getOrCreateAdminClient().getTenantClient().deleteTenant(TENANT_NAME, "BROKER");
    } catch (Exception e) {
      // Tenant may not exist if setup failed before creation.
    }
    try {
      getOrCreateAdminClient().getTenantClient().deleteTenant(TENANT_NAME, "SERVER");
    } catch (Exception e) {
      // Tenant may not exist if setup failed before creation.
    }
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

  private void deleteClassTempDir()
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
