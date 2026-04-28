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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.pinot.common.restlet.resources.RebalanceResult;
import org.apache.pinot.common.restlet.resources.RebalanceSummaryResult;
import org.apache.pinot.server.starter.helix.BaseServerStarter;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class KafkaConsumingSegmentToBeMovedSummaryIntegrationTest extends BaseRealtimeClusterIntegrationTest {
  private static final String SHARED_KAFKA_TOPIC = "kafka_consuming_segment_to_be_moved_summary";

  private File _classTempDir;
  private BaseServerStarter _extraServerStarter;
  private String _originalMaxSegmentPreprocessParallelism;
  private String _originalMaxSegmentStarTreePreprocessParallelism;
  private boolean _clusterConfigOverridesApplied;

  @Override
  @BeforeClass(alwaysRun = true)
  public void setUp()
      throws Exception {
    _classTempDir = getClassTempDir();
    TestUtils.ensureDirectoriesExistAndEmpty(_classTempDir);

    // Start the Pinot cluster
    startZk();
    startController();

    applyClusterConfigOverrides();

    startBroker();
    startServer();

    // Start Kafka
    startKafkaWithoutTopic();

    cleanRealtimeTableAndSchema();
    resetKafkaTopic();

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_classTempDir);

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createRealtimeTableConfig(avroFiles.get(0));
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    Map<String, String> streamConfig = getStreamConfigs();
    streamConfig.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_SEGMENT_SIZE, "1000000");
    streamConfig.remove(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS);
    indexingConfig.setStreamConfigs(streamConfig);
    tableConfig.setIndexingConfig(indexingConfig);
    addTableConfig(tableConfig);

    // Push data into Kafka
    pushAvroIntoKafka(avroFiles);

    // create segments and upload them to controller
    createSegmentsAndUpload(avroFiles, schema, tableConfig);

    // Set up the H2 connection
    setUpH2Connection(avroFiles);

    // Initialize the query generator
    setUpQueryGenerator(avroFiles);

    runValidationJob(600_000);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  @AfterClass(alwaysRun = true)
  @Override
  public void tearDown()
      throws Exception {
    Exception exception = null;
    exception = runCleanup(exception, this::cleanRealtimeTableAndSchema);
    exception = runCleanup(exception, this::deleteKafkaTopicIfPresent);
    exception = runCleanup(exception, this::restoreClusterConfigOverrides);
    exception = runCleanup(exception, this::closeH2Connection);
    exception = runCleanup(exception, this::closePinotConnections);
    exception = runCleanup(exception, this::stopAdditionalServerIfStarted);
    exception = runCleanup(exception, this::stopServerIfStarted);
    exception = runCleanup(exception, this::stopBrokerIfStarted);
    exception = runCleanup(exception, this::stopControllerIfStarted);
    exception = runCleanup(exception, this::stopKafkaIfStarted);
    exception = runCleanup(exception, this::stopZk);
    exception = runCleanup(exception, this::deleteClassTempDir);
    if (exception != null) {
      throw exception;
    }
  }

  @Override
  protected String getKafkaTopic() {
    return isSharedRichClusterEnabled() ? SHARED_KAFKA_TOPIC : super.getKafkaTopic();
  }

  @Test
  public void testConsumingSegmentSummary()
      throws Exception {
    try {
      RebalanceResult result = getOrCreateAdminClient().getRebalanceClient()
          .rebalanceTable(getTableName(), "REALTIME", true, false, true, false, -1);
      Assert.assertNotNull(result);
      Assert.assertNotNull(result.getRebalanceSummaryResult());
      Assert.assertNotNull(result.getRebalanceSummaryResult().getSegmentInfo());
      RebalanceSummaryResult.SegmentInfo segmentInfo = result.getRebalanceSummaryResult().getSegmentInfo();
      RebalanceSummaryResult.ConsumingSegmentToBeMovedSummary consumingSegmentToBeMovedSummary =
          segmentInfo.getConsumingSegmentToBeMovedSummary();
      Assert.assertNotNull(consumingSegmentToBeMovedSummary);
      Assert.assertEquals(consumingSegmentToBeMovedSummary.getNumConsumingSegmentsToBeMoved(), 0);
      Assert.assertEquals(consumingSegmentToBeMovedSummary.getNumServersGettingConsumingSegmentsAdded(), 0);
      Assert.assertEquals(consumingSegmentToBeMovedSummary.getServerConsumingSegmentSummary().size(),
          0);

      startAdditionalServerForRebalance();
      result = getOrCreateAdminClient().getRebalanceClient()
          .rebalanceTable(getTableName(), "REALTIME", true, false, true, false, -1);
      Assert.assertNotNull(result);
      Assert.assertNotNull(result.getRebalanceSummaryResult());
      Assert.assertNotNull(result.getRebalanceSummaryResult().getSegmentInfo());
      segmentInfo = result.getRebalanceSummaryResult().getSegmentInfo();
      consumingSegmentToBeMovedSummary = segmentInfo.getConsumingSegmentToBeMovedSummary();
      Assert.assertNotNull(consumingSegmentToBeMovedSummary);
      Assert.assertEquals(consumingSegmentToBeMovedSummary.getNumConsumingSegmentsToBeMoved(), 1);
      Assert.assertEquals(consumingSegmentToBeMovedSummary.getNumServersGettingConsumingSegmentsAdded(), 1);
      Assert.assertEquals(consumingSegmentToBeMovedSummary.getServerConsumingSegmentSummary().size(),
          1);
      Assert.assertTrue(consumingSegmentToBeMovedSummary
          .getServerConsumingSegmentSummary()
          .values()
          .stream()
          .allMatch(x -> x.getTotalOffsetsToCatchUpAcrossAllConsumingSegments() == 57801
              || x.getTotalOffsetsToCatchUpAcrossAllConsumingSegments() == 0));
      Assert.assertEquals(consumingSegmentToBeMovedSummary
          .getServerConsumingSegmentSummary()
          .values()
          .stream()
          .reduce(0L, (a, b) -> a + b.getTotalOffsetsToCatchUpAcrossAllConsumingSegments(), Long::sum), 57801);

      // set includeConsuming to false
      result = getOrCreateAdminClient().getRebalanceClient()
          .rebalanceTable(getTableName(), "REALTIME", true, false, false, false, -1);
      Assert.assertNotNull(result);
      Assert.assertNotNull(result.getRebalanceSummaryResult());
      Assert.assertNotNull(result.getRebalanceSummaryResult().getSegmentInfo());
      segmentInfo = result.getRebalanceSummaryResult().getSegmentInfo();
      consumingSegmentToBeMovedSummary = segmentInfo.getConsumingSegmentToBeMovedSummary();
      Assert.assertNotNull(consumingSegmentToBeMovedSummary);
      Assert.assertEquals(consumingSegmentToBeMovedSummary.getNumConsumingSegmentsToBeMoved(), 0);
      Assert.assertEquals(consumingSegmentToBeMovedSummary.getNumServersGettingConsumingSegmentsAdded(), 0);
      Assert.assertEquals(consumingSegmentToBeMovedSummary.getServerConsumingSegmentSummary().size(),
          0);

      makeKafkaOffsetsUnavailableForTable();
      RebalanceResult resultNoInfo = getOrCreateAdminClient().getRebalanceClient()
          .rebalanceTable(getTableName(), "REALTIME", true, false, true, false, -1);
      Assert.assertNotNull(resultNoInfo);
      Assert.assertNotNull(resultNoInfo.getRebalanceSummaryResult());
      Assert.assertNotNull(resultNoInfo.getRebalanceSummaryResult().getSegmentInfo());
      segmentInfo = resultNoInfo.getRebalanceSummaryResult().getSegmentInfo();
      consumingSegmentToBeMovedSummary = segmentInfo.getConsumingSegmentToBeMovedSummary();
      Assert.assertNotNull(consumingSegmentToBeMovedSummary);
      Assert.assertEquals(consumingSegmentToBeMovedSummary.getNumConsumingSegmentsToBeMoved(), 1);
      Assert.assertEquals(consumingSegmentToBeMovedSummary.getNumServersGettingConsumingSegmentsAdded(), 1);
      Assert.assertNotNull(consumingSegmentToBeMovedSummary.getServerConsumingSegmentSummary());
      Assert.assertNull(consumingSegmentToBeMovedSummary.getConsumingSegmentsToBeMovedWithMostOffsetsToCatchUp());
    } finally {
      if (isSharedRichClusterEnabled()) {
        stopAdditionalServerIfStarted();
      }
    }
  }

  private void applyClusterConfigOverrides() {
    HelixConfigScope scope = getClusterConfigScope();
    ConfigAccessor configAccessor = _helixManager.getConfigAccessor();
    _originalMaxSegmentPreprocessParallelism =
        configAccessor.get(scope, CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM);
    _originalMaxSegmentStarTreePreprocessParallelism =
        configAccessor.get(scope, CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM);
    try {
      // Set max segment preprocess parallelism to 8
      configAccessor.set(scope, CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM,
          Integer.toString(8));
      // Set max segment startree preprocess parallelism to 6
      configAccessor.set(scope, CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM,
          Integer.toString(6));
      _clusterConfigOverridesApplied = true;
    } catch (RuntimeException e) {
      restoreClusterConfig(configAccessor, scope, CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM,
          _originalMaxSegmentPreprocessParallelism);
      restoreClusterConfig(configAccessor, scope,
          CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM,
          _originalMaxSegmentStarTreePreprocessParallelism);
      throw e;
    }
  }

  private void restoreClusterConfigOverrides() {
    if (!_clusterConfigOverridesApplied || _helixManager == null) {
      return;
    }

    ConfigAccessor configAccessor = _helixManager.getConfigAccessor();
    HelixConfigScope scope = getClusterConfigScope();
    restoreClusterConfig(configAccessor, scope, CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM,
        _originalMaxSegmentPreprocessParallelism);
    restoreClusterConfig(configAccessor, scope,
        CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM,
        _originalMaxSegmentStarTreePreprocessParallelism);
    _clusterConfigOverridesApplied = false;
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
    return new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(getHelixClusterName())
        .build();
  }

  private File getClassTempDir() {
    return isSharedRichClusterEnabled()
        ? new File(FileUtils.getTempDirectory(), getClass().getSimpleName() + "-shared")
        : _tempDir;
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

  private void startAdditionalServerForRebalance()
      throws Exception {
    if (isSharedRichClusterEnabled()) {
      _extraServerStarter = startOneServer(_serverStarters.size());
    } else {
      startServer();
    }
  }

  private void stopAdditionalServerIfStarted() {
    if (_extraServerStarter == null) {
      return;
    }

    BaseServerStarter extraServerStarter = _extraServerStarter;
    _extraServerStarter = null;
    extraServerStarter.stop();
    TestUtils.waitForCondition(
        aVoid -> _helixResourceManager.dropInstance(extraServerStarter.getInstanceId()).isSuccessful(),
        60_000L, "Failed to drop added server");
  }

  private void makeKafkaOffsetsUnavailableForTable() {
    if (isSharedRichClusterEnabled()) {
      deleteKafkaTopicIfPresent();
    } else {
      stopKafka();
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
