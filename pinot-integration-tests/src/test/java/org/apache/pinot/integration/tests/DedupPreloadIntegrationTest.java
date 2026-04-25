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

import com.google.common.base.Joiner;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.pinot.client.admin.PinotAdminClient;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.ParallelSegmentConsumptionPolicy;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.Enablement;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class DedupPreloadIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final String SHARED_TABLE_NAME_PREFIX = "dedup_preload";
  private static final String SHARED_KAFKA_TOPIC_PREFIX = "dedup-preload";

  private final String _sharedResourceSuffix = Long.toUnsignedString(RANDOM.nextLong(), Character.MAX_RADIX);

  private File _classTempDir;
  private List<File> _avroFiles;

  @Override
  protected int getSharedNumBrokers() {
    return 1;
  }

  @Override
  protected boolean shouldStartSharedKafka() {
    return true;
  }

  @Override
  protected int getSharedNumServers() {
    return 1;
  }

  @Override
  protected boolean shouldStartSharedMinion() {
    return false;
  }

  @Override
  protected String getTableName() {
    return isSharedRichClusterEnabled() ? SHARED_TABLE_NAME_PREFIX + "_" + _sharedResourceSuffix
        : super.getTableName();
  }

  @Override
  protected String getKafkaTopic() {
    return isSharedRichClusterEnabled() ? SHARED_KAFKA_TOPIC_PREFIX + "-" + _sharedResourceSuffix
        : super.getKafkaTopic();
  }

  @BeforeClass(alwaysRun = true)
  public void setUp()
      throws Exception {
    _classTempDir = getClassTempDir();
    TestUtils.ensureDirectoriesExistAndEmpty(_classTempDir);

    // Start the Pinot cluster
    startZk();
    // Start a customized controller with more frequent realtime segment validation
    startController();
    startBroker();
    startServer();
    startKafkaWithoutTopic();

    cleanRealtimeTableAndSchema();
    resetKafkaTopic();

    _avroFiles = unpackAvroData(_classTempDir);
    pushAvroIntoKafka(_avroFiles);

    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createDedupTableConfig(_avroFiles.get(0), "id", getNumKafkaPartitions());
    addTableConfig(tableConfig);

    waitForAllDocsLoaded(600_000L);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    serverConf.setProperty(Server.INSTANCE_DATA_MANAGER_CONFIG_PREFIX + ".max.segment.preload.threads", "1");
    serverConf.setProperty(Joiner.on(".")
        .join(Server.INSTANCE_DATA_MANAGER_CONFIG_PREFIX, Server.Dedup.CONFIG_PREFIX,
            Server.Dedup.DEFAULT_ENABLE_PRELOAD), "true");
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    Exception exception = null;
    exception = runCleanup(exception, this::cleanRealtimeTableAndSchema);
    exception = runCleanup(exception, this::deleteKafkaTopicIfPresent);
    exception = runCleanup(exception, this::closeH2Connection);
    exception = runCleanup(exception, this::closePinotConnections);
    exception = runCleanup(exception, this::closeAdminClient);
    if (!isSharedRichClusterEnabled()) {
      exception = runCleanup(exception, this::stopServerIfStarted);
      exception = runCleanup(exception, this::stopBrokerIfStarted);
      exception = runCleanup(exception, this::stopControllerIfStarted);
      exception = runCleanup(exception, this::stopKafkaIfStarted);
      exception = runCleanup(exception, this::stopZk);
    }
    exception = runCleanup(exception, this::deleteClassTempDir);
    if (exception != null) {
      throw exception;
    }
  }

  @Override
  protected int getRealtimeSegmentFlushSize() {
    // Create > 1 segments
    return 2;
  }

  @Override
  protected String getSchemaFileName() {
    return "dedupIngestionTestSchema.schema";
  }

  @Override
  protected String getAvroTarFileName() {
    return "dedupIngestionTestData.tar.gz";
  }

  @Override
  protected String getPartitionColumn() {
    return "id";
  }

  @Override
  protected long getCountStarResult() {
    // Three distinct records are expected with pk values of 100000, 100001, 100002
    return 5;
  }

  @Test
  public void testValues()
      throws Exception {
    assertEquals(getCurrentCountStarResult(), getCountStarResult());

    // Validate the older value persist
    for (int i = 0; i < getCountStarResult(); i++) {
      assertEquals(
          getPinotConnection().execute("SELECT name FROM " + getTableName() + " WHERE id = " + i).getResultSet(0)
              .getString(0), "" + i);
    }

    // Restart the servers and check again
    restartServers();
    waitForAllDocsLoaded(600_000L);

    // Validate the older value persist
    for (int i = 0; i < getCountStarResult(); i++) {
      assertEquals(
          getPinotConnection().execute("SELECT name FROM " + getTableName() + " WHERE id = " + i).getResultSet(0)
              .getString(0), "" + i);
    }
  }

  @Override
  protected void restartServers()
      throws Exception {
    super.restartServers();
    restoreSharedServerStarters();
  }

  @Override
  protected IngestionConfig getIngestionConfig() {
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(new StreamIngestionConfig(List.of(getStreamConfigs())));
    ingestionConfig.getStreamIngestionConfig()
        .setParallelSegmentConsumptionPolicy(ParallelSegmentConsumptionPolicy.ALLOW_DURING_BUILD_ONLY);
    ingestionConfig.getStreamIngestionConfig().setEnforceConsumptionInOrder(true);
    return ingestionConfig;
  }

  @Override
  protected TableConfig createDedupTableConfig(File sampleAvroFile, String primaryKeyColumn, int numPartitions) {
    AvroFileSchemaKafkaAvroMessageDecoder._avroFile = sampleAvroFile;
    Map<String, ColumnPartitionConfig> columnPartitionConfigMap = new HashMap<>();
    columnPartitionConfigMap.put(primaryKeyColumn, new ColumnPartitionConfig("Murmur", numPartitions));

    DedupConfig dedupConfig = new DedupConfig();
    dedupConfig.setPreload(Enablement.ENABLE);

    return new TableConfigBuilder(TableType.REALTIME).setTableName(getTableName())
        .setTimeColumnName(getTimeColumnName())
        .setFieldConfigList(getFieldConfigs())
        .setNumReplicas(getNumReplicas())
        .setSegmentVersion(getSegmentVersion())
        .setLoadMode(getLoadMode())
        .setTaskConfig(getTaskConfig())
        .setBrokerTenant(getBrokerTenant())
        .setServerTenant(getServerTenant())
        .setIngestionConfig(getIngestionConfig())
        .setNullHandlingEnabled(getNullHandlingEnabled())
        .setRoutingConfig(
            new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false))
        .setSegmentPartitionConfig(new SegmentPartitionConfig(columnPartitionConfigMap))
        .setReplicaGroupStrategyConfig(new ReplicaGroupStrategyConfig(primaryKeyColumn, 1))
        .setDedupConfig(dedupConfig)
        .build();
  }

  private File getClassTempDir() {
    return isSharedRichClusterEnabled() ? new File(_tempDir, "testData-" + _sharedResourceSuffix) : _tempDir;
  }

  private void resetKafkaTopic() {
    deleteKafkaTopicIfPresent();
    createKafkaTopic(getKafkaTopic());
  }

  private void cleanRealtimeTableAndSchema()
      throws Exception {
    if (_helixResourceManager == null) {
      return;
    }

    String tableName = getTableName();
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

  private void closeAdminClient()
      throws Exception {
    Field adminClientField = ControllerTest.class.getDeclaredField("_pinotAdminClient");
    adminClientField.setAccessible(true);
    PinotAdminClient adminClient = (PinotAdminClient) adminClientField.get(this);
    if (adminClient != null) {
      adminClient.close();
      adminClientField.set(this, null);
    }
  }

  private void stopServerIfStarted() {
    if (!_serverStarters.isEmpty()) {
      stopServer();
    }
  }

  private void stopBrokerIfStarted() {
    if (!_brokerStarters.isEmpty()) {
      stopBroker();
    }
  }

  private void stopControllerIfStarted() {
    if (_controllerStarter != null) {
      stopController();
    }
  }

  private void stopKafkaIfStarted() {
    if (_kafkaStarters != null && !_kafkaStarters.isEmpty()) {
      stopKafka();
    }
  }

  private void deleteClassTempDir()
      throws IOException {
    if (_classTempDir != null) {
      FileUtils.deleteDirectory(_classTempDir);
    }
  }

  private void restoreSharedServerStarters() {
    if (isSharedRichClusterEnabled() && _sharedRichClusterTestSuite != null && _sharedRichClusterTestSuite != this) {
      _sharedRichClusterTestSuite._serverStarters.clear();
      _sharedRichClusterTestSuite._serverStarters.addAll(_serverStarters);
      attachSharedRichCluster();
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
