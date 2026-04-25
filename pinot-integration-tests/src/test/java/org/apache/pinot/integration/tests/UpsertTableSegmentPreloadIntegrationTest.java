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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.IdealState;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.pinot.client.ExecutionStats;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.server.starter.helix.BaseServerStarter;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


public class UpsertTableSegmentPreloadIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final int NUM_SERVERS = 1;
  private static final String PRIMARY_KEY_COL = "clientId";
  private static final String SHARED_TABLE_NAME = "upsert_table_segment_preload";
  private static final String SHARED_KAFKA_TOPIC = "upsert-table-segment-preload";

  // Segment 1 contains records of pk value 100000 (partition 0)
  private static final String UPLOADED_SEGMENT_1_SUFFIX = "_10027_19736_0 %";
  // Segment 2 contains records of pk value 100001 (partition 1)
  private static final String UPLOADED_SEGMENT_2_SUFFIX = "_10072_19919_1 %";
  // Segment 3 contains records of pk value 100002 (partition 1)
  private static final String UPLOADED_SEGMENT_3_SUFFIX = "_10158_19938_2 %";

  private final String _resourceSuffix = Long.toUnsignedString(RANDOM.nextLong(), Character.MAX_RADIX);

  private File _classTempDir;
  private File _classSegmentDir;
  private File _classTarDir;

  @BeforeClass(alwaysRun = true)
  public void setUp()
      throws Exception {
    _classTempDir = getClassTempDir();
    _classSegmentDir = new File(_classTempDir, "segmentDir");
    _classTarDir = new File(_classTempDir, "tarDir");
    TestUtils.ensureDirectoriesExistAndEmpty(_classTempDir, _classSegmentDir, _classTarDir);

    // Start the Pinot cluster
    startZk();
    // Start a customized controller with more frequent realtime segment validation
    startController();
    startBroker();
    startServers(NUM_SERVERS);

    // Start Kafka and push data into Kafka
    startKafka();
    validateSharedTopology();
    cleanRealtimeTableAndSchema();
    resetKafkaTopic();

    populateTables();
  }

  protected void populateTables()
      throws Exception {
    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_classTempDir);

    // Create and upload schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createUpsertTableConfig(avroFiles.get(0), PRIMARY_KEY_COL, null, getNumKafkaPartitions());
    UpsertConfig upsertConfig = tableConfig.getUpsertConfig();
    assertNotNull(upsertConfig);
    addTableConfig(tableConfig);

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _classSegmentDir,
        _classTarDir);
    uploadSegments(getTableName(), TableType.REALTIME, _classTarDir);

    pushAvroIntoKafka(avroFiles);
    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    serverConf.setProperty(Server.INSTANCE_DATA_MANAGER_CONFIG_PREFIX + ".max.segment.preload.threads",
        "1");
    serverConf.setProperty(Joiner.on(".")
        .join(Server.INSTANCE_DATA_MANAGER_CONFIG_PREFIX, Server.Upsert.CONFIG_PREFIX,
            Server.Upsert.DEFAULT_ENABLE_SNAPSHOT), "true");
    serverConf.setProperty(Joiner.on(".")
        .join(Server.INSTANCE_DATA_MANAGER_CONFIG_PREFIX, Server.Upsert.CONFIG_PREFIX,
            Server.Upsert.DEFAULT_ENABLE_PRELOAD), "true");
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    Exception exception = null;
    exception = runCleanup(exception, this::cleanRealtimeTableAndSchema);
    exception = runCleanup(exception, this::deleteKafkaTopicIfPresent);
    exception = runCleanup(exception, this::closeH2Connection);
    exception = runCleanup(exception, this::closePinotConnections);
    exception = runCleanup(exception, this::closeAdminClientInSharedMode);
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
  protected String getTableName() {
    return isSharedRichClusterEnabled() ? SHARED_TABLE_NAME + "_" + _resourceSuffix : super.getTableName();
  }

  @Override
  protected String getKafkaTopic() {
    return isSharedRichClusterEnabled() ? SHARED_KAFKA_TOPIC + "-" + _resourceSuffix : super.getKafkaTopic();
  }

  @Override
  protected boolean shouldStartSharedKafka() {
    return true;
  }

  @Override
  protected int getSharedNumBrokers() {
    return 1;
  }

  @Override
  protected int getSharedNumServers() {
    return NUM_SERVERS;
  }

  @Override
  protected boolean shouldStartSharedMinion() {
    return false;
  }

  @Override
  protected String getSchemaFileName() {
    return "upsert_upload_segment_test.schema";
  }

  @Override
  protected String getAvroTarFileName() {
    return "upsert_upload_segment_test.tar.gz";
  }

  @Override
  protected String getPartitionColumn() {
    return PRIMARY_KEY_COL;
  }

  @Override
  protected long getCountStarResult() {
    // Three distinct records are expected with pk values of 100000, 100001, 100002
    return 3;
  }

  @Override
  protected void waitForAllDocsLoaded(long timeoutMs)
      throws Exception {
    TestUtils.waitForCondition(aVoid -> {
      try {
        return getCurrentCountStarResultWithoutUpsert() == getCountStarResultWithoutUpsert();
      } catch (Exception e) {
        return null;
      }
    }, timeoutMs, "Failed to load all documents");
    assertEquals(getCurrentCountStarResult(), getCountStarResult());
  }

  private long getCurrentCountStarResultWithoutUpsert() {
    return getPinotConnection().execute("SELECT COUNT(*) FROM " + getTableName() + " OPTION(skipUpsert=true)")
        .getResultSet(0).getLong(0);
  }

  private long getCountStarResultWithoutUpsert() {
    // 3 Avro files, each with 100 documents, one copy from streaming source, one copy from batch source
    return 600;
  }

  @Test
  public void testSegmentAssignment()
      throws Exception {
    verifyIdealState(5);

    // Run the real-time segment validation and check again
    _controllerStarter.getRealtimeSegmentValidationManager().run();
    verifyIdealState(5);
    assertEquals(getCurrentCountStarResult(), getCountStarResult());
    assertEquals(getCurrentCountStarResultWithoutUpsert(), getCountStarResultWithoutUpsert());

    waitForSnapshotCreation();

    // Restart the servers and check again
    restartServers();
    verifyIdealState(7);
    waitForAllDocsLoaded(600_000L);
  }

  @Override
  protected void restartServers()
      throws Exception {
    super.restartServers();
    restoreSharedOwnerServerStarters();
  }

  protected void waitForSnapshotCreation()
      throws Exception {
    // Pause consumption and wait until all consuming segments are committed and loaded as immutable segment
    String rawTableName = getTableName();
    getOrCreateAdminClient().getTableClient().pauseConsumption(rawTableName);
    TestUtils.waitForCondition(aVoid -> {
      ExecutionStats executionStats =
          getPinotConnection().execute("SELECT COUNT(*) FROM " + rawTableName).getExecutionStats();
      return executionStats.getNumSegmentsQueried() == 5 && executionStats.getNumConsumingSegmentsQueried() == 0;
    }, 600_000L, "Failed to pause consumption");
    // Resume consumption to trigger snapshot
    getOrCreateAdminClient().getTableClient().resumeConsumption(rawTableName, null);

    // All the immutable (committed and uploaded) segments should have snapshots generated
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(rawTableName);
    TestUtils.waitForCondition(aVoid -> {
      for (BaseServerStarter serverStarter : _serverStarters) {
        String segmentDir = serverStarter.getConfig().getProperty(Server.CONFIG_OF_INSTANCE_DATA_DIR);
        File[] files = new File(segmentDir, realtimeTableName).listFiles((dir, name) -> name.startsWith(rawTableName));
        assertNotNull(files);
        for (File file : files) {
          if (!new File(new File(file, "v3"), V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME).exists()) {
            return false;
          }
        }
      }
      return true;
    }, 600_000L, "Failed to verify snapshots");
  }

  protected void verifyIdealState(int numSegmentsExpected) {
    IdealState idealState = HelixHelper.getTableIdealState(_helixManager, getRealtimeTableName());
    Map<String, Map<String, String>> segmentAssignment = idealState.getRecord().getMapFields();
    assertEquals(segmentAssignment.size(), numSegmentsExpected);

    String serverForPartition0 = null;
    String serverForPartition1 = null;

    int maxSequenceNumber = 0;
    for (Map.Entry<String, Map<String, String>> entry : segmentAssignment.entrySet()) {
      String segmentName = entry.getKey();
      LLCSegmentName llcSegmentName = LLCSegmentName.of(segmentName);
      if (llcSegmentName != null) {
        maxSequenceNumber = Math.max(maxSequenceNumber, llcSegmentName.getSequenceNumber());
      }
    }

    for (Map.Entry<String, Map<String, String>> entry : segmentAssignment.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> instanceStateMap = entry.getValue();

      // Verify that all segments have the correct state
      assertEquals(instanceStateMap.size(), 1);
      Map.Entry<String, String> instanceIdAndState = instanceStateMap.entrySet().iterator().next();
      String state = instanceIdAndState.getValue();
      LLCSegmentName llcSegmentName = LLCSegmentName.of(segmentName);
      if (llcSegmentName != null) {
        if (llcSegmentName.getSequenceNumber() < maxSequenceNumber) {
          assertEquals(state, SegmentStateModel.ONLINE);
        } else {
          assertEquals(state, SegmentStateModel.CONSUMING);
        }
      } else {
        assertEquals(state, SegmentStateModel.ONLINE);
      }

      // Verify that all segments of the same partition are mapped to the same server
      String instanceId = instanceIdAndState.getKey();
      int partitionId = getSegmentPartitionId(segmentName);
      if (partitionId == 0) {
        if (serverForPartition0 == null) {
          serverForPartition0 = instanceId;
        } else {
          assertEquals(instanceId, serverForPartition0);
        }
      } else {
        assertEquals(partitionId, 1);
        if (serverForPartition1 == null) {
          serverForPartition1 = instanceId;
        } else {
          assertEquals(instanceId, serverForPartition1);
        }
      }
    }
  }

  private File getClassTempDir() {
    return isSharedRichClusterEnabled()
        ? new File(FileUtils.getTempDirectory(), getClass().getSimpleName() + "-" + _resourceSuffix)
        : _tempDir;
  }

  private String getRealtimeTableName() {
    return TableNameBuilder.REALTIME.tableNameWithType(getTableName());
  }

  private int getSegmentPartitionId(String segmentName) {
    if (segmentName.equals(getTableName() + UPLOADED_SEGMENT_1_SUFFIX)) {
      return 0;
    }
    if (segmentName.equals(getTableName() + UPLOADED_SEGMENT_2_SUFFIX)
        || segmentName.equals(getTableName() + UPLOADED_SEGMENT_3_SUFFIX)) {
      return 1;
    }
    return new LLCSegmentName(segmentName).getPartitionGroupId();
  }

  private void validateSharedTopology() {
    if (!isSharedRichClusterEnabled()) {
      return;
    }
    assertEquals(_brokerStarters.size(), 1, "Shared rich cluster broker count mismatch");
    assertEquals(_serverStarters.size(), NUM_SERVERS, "Shared rich cluster server count mismatch");
    assertNotNull(_controllerStarter, "Shared rich cluster controller is not started");
    assertNotNull(_kafkaStarters, "Shared rich cluster Kafka is not started");
    assertFalse(_kafkaStarters.isEmpty(), "Shared rich cluster Kafka is not started");
    assertNull(_minionStarter, "Shared rich cluster minion should not be started");
    for (BaseServerStarter serverStarter : _serverStarters) {
      PinotConfiguration serverConfig = serverStarter.getConfig();
      assertEquals(serverConfig.getProperty(
          Server.INSTANCE_DATA_MANAGER_CONFIG_PREFIX + ".max.segment.preload.threads"), "1");
      assertEquals(serverConfig.getProperty(Joiner.on(".")
          .join(Server.INSTANCE_DATA_MANAGER_CONFIG_PREFIX, Server.Upsert.CONFIG_PREFIX,
              Server.Upsert.DEFAULT_ENABLE_SNAPSHOT)), "true");
      assertEquals(serverConfig.getProperty(Joiner.on(".")
          .join(Server.INSTANCE_DATA_MANAGER_CONFIG_PREFIX, Server.Upsert.CONFIG_PREFIX,
              Server.Upsert.DEFAULT_ENABLE_PRELOAD)), "true");
    }
  }

  private void restoreSharedOwnerServerStarters() {
    if (!isSharedRichClusterEnabled() || _sharedRichClusterTestSuite == null || _sharedRichClusterTestSuite == this) {
      return;
    }
    _sharedRichClusterTestSuite._serverStarters.clear();
    _sharedRichClusterTestSuite._serverStarters.addAll(_serverStarters);
    _sharedRichClusterTestSuite._serverGrpcPort = _serverGrpcPort;
    _sharedRichClusterTestSuite._serverAdminApiPort = _serverAdminApiPort;
    _sharedRichClusterTestSuite._serverNettyPort = _serverNettyPort;
    attachSharedRichCluster();
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
    String realtimeTableName = getRealtimeTableName();
    if (_helixResourceManager.getTableConfig(realtimeTableName) != null
        || _helixResourceManager.hasRealtimeTable(tableName)) {
      dropSegmentsIfPresent(realtimeTableName);
      dropRealtimeTable(tableName);
      waitForTableDataManagerRemoved(realtimeTableName);
      waitForEVToDisappear(realtimeTableName);
    }
    if (_helixResourceManager.getSchema(tableName) != null) {
      deleteSchema(tableName);
    }
  }

  private void dropSegmentsIfPresent(String realtimeTableName)
      throws IOException {
    List<String> segments = listSegments(realtimeTableName);
    if (segments.isEmpty()) {
      return;
    }
    for (String segment : segments) {
      dropSegment(realtimeTableName, segment);
    }
    // NOTE: There is a delay to remove the segment from property store.
    TestUtils.waitForCondition((aVoid) -> {
      try {
        return listSegments(realtimeTableName).isEmpty();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, 60_000L, "Failed to drop the segments");
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

  private void closeAdminClientInSharedMode()
      throws IOException {
    if (isSharedRichClusterEnabled()) {
      getOrCreateAdminClient().close();
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
