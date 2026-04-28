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
package org.apache.pinot.integration.tests.realtime.ingestion;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.pinot.integration.tests.BaseRealtimeClusterIntegrationTest;
import org.apache.pinot.plugin.stream.kafka.KafkaStreamConfigProperties;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.*;


public class KafkaIncreaseDecreasePartitionsIntegrationTest extends BaseRealtimeClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaIncreaseDecreasePartitionsIntegrationTest.class);

  private static final String DEFAULT_MEETUP_TABLE_NAME = "upsertMeetupRsvp";
  private static final String DEFAULT_KAFKA_TOPIC = "meetup";
  private static final String SHARED_TABLE_NAME = "kafka_increase_decrease_partitions";
  private static final String SHARED_KAFKA_TOPIC = "kafka_increase_decrease_partitions";
  private static final int NUM_PARTITIONS = 1;

  private File _classTempDir;
  private String _previousMaxSegmentPreprocessParallelism;
  private String _previousMaxSegmentStartreePreprocessParallelism;
  private boolean _clusterConfigOverridesApplied;

  @BeforeClass
  @Override
  public void setUp()
      throws Exception {
    _classTempDir = getClassTempDir();
    TestUtils.ensureDirectoriesExistAndEmpty(_classTempDir);

    startZk();
    startKafkaWithoutTopic();
    startController();
    applyClusterConfigOverrides();
    startBroker();
    startServer();

    cleanRealtimeTableAndSchema();
    deleteKafkaTopicIfPresent();
  }

  @AfterClass(alwaysRun = true)
  @Override
  public void tearDown()
      throws Exception {
    Exception exception = null;
    exception = runCleanup(exception, this::cleanRealtimeTableAndSchema);
    exception = runCleanup(exception, this::deleteKafkaTopicIfPresent);
    exception = runCleanup(exception, this::restoreClusterConfigOverrides);
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
  protected String getTableName() {
    return isSharedRichClusterEnabled() ? SHARED_TABLE_NAME : DEFAULT_MEETUP_TABLE_NAME;
  }

  @Override
  protected String getKafkaTopic() {
    return isSharedRichClusterEnabled() ? SHARED_KAFKA_TOPIC : DEFAULT_KAFKA_TOPIC;
  }

  String createTable()
      throws IOException {
    Schema schema = createSchema("simpleMeetup_schema.json");
    schema.setSchemaName(getTableName());
    addSchema(schema);
    TableConfig tableConfig;
    try (InputStream inputStream =
        getClass().getClassLoader().getResourceAsStream("simpleMeetup_realtime_table_config.json")) {
      Assert.assertNotNull(inputStream);
      tableConfig = JsonUtils.inputStreamToObject(inputStream, TableConfig.class);
    }
    tableConfig.setTableName(getTableName());
    Map<String, String> streamConfigs = new HashMap<>(tableConfig.getIndexingConfig().getStreamConfigs());
    streamConfigs.put(
        StreamConfigProperties.constructStreamProperty("kafka", StreamConfigProperties.STREAM_TOPIC_NAME),
        getKafkaTopic());
    streamConfigs.put(KafkaStreamConfigProperties.constructStreamProperty(
        KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_BROKER_LIST), getKafkaBrokerList());
    streamConfigs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBrokerList());
    tableConfig.getIndexingConfig().setStreamConfigs(streamConfigs);
    addTableConfig(tableConfig);
    return tableConfig.getTableName();
  }

  @Test
  public void testDecreasePartitions()
      throws Exception {
    LOGGER.info("Starting testDecreasePartitions");
    LOGGER.info("Creating Kafka topic with {} partitions", NUM_PARTITIONS + 2);
    createKafkaTopic(getKafkaTopic(), NUM_PARTITIONS + 2);
    String tableName = createTable();
    waitForNumSegmentsInDesiredStateInEV(tableName, CONSUMING, NUM_PARTITIONS + 2, TableType.REALTIME);

    pauseTable(tableName);

    LOGGER.info("Deleting Kafka topic");
    deleteKafkaTopic(getKafkaTopic());
    LOGGER.info("Creating Kafka topic with {} partitions", NUM_PARTITIONS);
    createKafkaTopic(getKafkaTopic(), NUM_PARTITIONS);

    resumeTable(tableName);
    waitForNumSegmentsInDesiredStateInEV(tableName, CONSUMING, NUM_PARTITIONS, TableType.REALTIME);
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

    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    if (_helixResourceManager.getTableConfig(realtimeTableName) != null
        || _helixResourceManager.hasRealtimeTable(getTableName())) {
      dropRealtimeTable(getTableName());
      waitForTableDataManagerRemoved(realtimeTableName);
      waitForEVToDisappear(realtimeTableName);
    }
    if (_helixResourceManager.getSchema(getTableName()) != null) {
      deleteSchema(getTableName());
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

  private void applyClusterConfigOverrides() {
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(getHelixClusterName())
            .build();
    _previousMaxSegmentPreprocessParallelism = _helixManager.getConfigAccessor()
        .get(scope, CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM);
    _previousMaxSegmentStartreePreprocessParallelism = _helixManager.getConfigAccessor()
        .get(scope, CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM);
    _clusterConfigOverridesApplied = true;
    _helixManager.getConfigAccessor()
        .set(scope, CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM, Integer.toString(8));
    _helixManager.getConfigAccessor()
        .set(scope, CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM, Integer.toString(6));
  }

  private void restoreClusterConfigOverrides()
      throws Exception {
    if (!_clusterConfigOverridesApplied) {
      return;
    }
    restoreClusterConfig(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM,
        _previousMaxSegmentPreprocessParallelism);
    restoreClusterConfig(CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM,
        _previousMaxSegmentStartreePreprocessParallelism);
    _clusterConfigOverridesApplied = false;
  }

  private void restoreClusterConfig(String key, String value)
      throws Exception {
    if (value == null) {
      deleteClusterConfig(key);
    } else {
      updateClusterConfig(Map.of(key, value));
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

  @Test(enabled = false)
  public void testDictionaryBasedQueries(boolean useMultiStageQueryEngine) {
    // Do nothing
  }

  @Test(enabled = false)
  public void testGeneratedQueries(boolean useMultiStageQueryEngine) {
    // Do nothing
  }

  @Test(enabled = false)
  public void testHardcodedQueries(boolean useMultiStageQueryEngine) {
    // Do nothing
  }

  @Test(enabled = false)
  public void testInstanceShutdown() {
    // Do nothing
  }

  @Test(enabled = false)
  public void testQueriesFromQueryFile(boolean useMultiStageQueryEngine) {
    // Do nothing
  }

  @Test(enabled = false)
  public void testQueryExceptions(boolean useMultiStageQueryEngine) {
    // Do nothing
  }

  @Test(enabled = false)
  public void testHardcodedServerPartitionedSqlQueries() {
    // Do nothing
  }
}
