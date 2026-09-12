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
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.plugin.stream.kafka.KafkaStreamConfigProperties;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


/// Owns the Pinot and Kafka processes shared by the focused realtime ingestion suite.
///
/// Only the LLC test extends this class. The other suite classes use [#getSharedSuiteOwner()] so TestNG cannot
/// accidentally invoke the generic realtime query suite for every stream-specific scenario. Scenarios are sequential
/// and lease unique table, schema, topic, and filesystem names from this owner.
public abstract class SharedKafkaRealtimeIntegrationTestSuite extends BaseRealtimeClusterIntegrationTest {
  private static SharedKafkaRealtimeIntegrationTestSuite _sharedSuiteOwner;

  private final List<ScenarioLease> _scenarioLeases = new ArrayList<>();
  private boolean _startingTransactionalKafka;
  private boolean _zkStarted;
  private boolean _kafkaStarted;
  private boolean _controllerStarted;
  private boolean _brokerStarted;
  private boolean _serverStarted;

  @BeforeSuite(alwaysRun = true)
  public final void setUpSharedSuite()
      throws Throwable {
    if (_sharedSuiteOwner != null) {
      throw new IllegalStateException("Shared Kafka realtime suite already has an owner");
    }
    _sharedSuiteOwner = this;

    Throwable primaryFailure = null;
    try {
      prepareSharedSuiteFiles();
      TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

      _zkStarted = true;
      startZk();

      _startingTransactionalKafka = true;
      try {
        _kafkaStarted = true;
        startKafkaWithoutTopic();
      } finally {
        _startingTransactionalKafka = false;
      }

      _controllerStarted = true;
      startController();
      configureClusterPreprocessParallelism();

      _brokerStarted = true;
      startBroker();
      _serverStarted = true;
      startServer();
    } catch (Throwable t) {
      primaryFailure = t;
      throw t;
    } finally {
      if (primaryFailure != null) {
        Throwable cleanupFailure = tearDownSharedSuiteInternal();
        if (cleanupFailure != null) {
          primaryFailure.addSuppressed(cleanupFailure);
        }
      }
    }
  }

  protected void prepareSharedSuiteFiles()
      throws Exception {
  }

  private void configureClusterPreprocessParallelism() {
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(getHelixClusterName())
            .build();
    _helixManager.getConfigAccessor()
        .set(scope, CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM, Integer.toString(8));
    _helixManager.getConfigAccessor()
        .set(scope, CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM, Integer.toString(6));
  }

  @AfterSuite(alwaysRun = true)
  public final void tearDownSharedSuite()
      throws Throwable {
    Throwable cleanupFailure = tearDownSharedSuiteInternal();
    if (cleanupFailure != null) {
      throw cleanupFailure;
    }
  }

  private Throwable tearDownSharedSuiteInternal() {
    Throwable cleanupFailure = null;
    for (int i = _scenarioLeases.size() - 1; i >= 0; i--) {
      cleanupFailure = cleanUpScenario(_scenarioLeases.get(i), cleanupFailure);
    }

    if (_serverStarted) {
      try {
        stopServer();
        _serverStarted = false;
      } catch (Throwable t) {
        cleanupFailure = appendCleanupFailure(cleanupFailure, t);
      }
    }
    if (_brokerStarted) {
      try {
        stopBroker();
        _brokerStarted = false;
      } catch (Throwable t) {
        cleanupFailure = appendCleanupFailure(cleanupFailure, t);
      }
    }
    if (_controllerStarted) {
      try {
        stopController();
        _controllerStarted = false;
      } catch (Throwable t) {
        cleanupFailure = appendCleanupFailure(cleanupFailure, t);
      }
    }
    if (_kafkaStarted) {
      try {
        stopKafka();
        _kafkaStarted = false;
      } catch (Throwable t) {
        cleanupFailure = appendCleanupFailure(cleanupFailure, t);
      }
    }
    if (_zkStarted) {
      try {
        stopZk();
        _zkStarted = false;
      } catch (Throwable t) {
        cleanupFailure = appendCleanupFailure(cleanupFailure, t);
      }
    }
    for (ScenarioLease lease : _scenarioLeases) {
      if (lease._resetAction != null) {
        try {
          lease._resetAction.run();
          lease._resetAction = null;
        } catch (Throwable t) {
          cleanupFailure = appendCleanupFailure(cleanupFailure, t);
        }
      }
    }
    try {
      FileUtils.deleteDirectory(_tempDir);
    } catch (Throwable t) {
      cleanupFailure = appendCleanupFailure(cleanupFailure, t);
    }
    try {
      resetSharedSuiteStatics();
    } catch (Throwable t) {
      cleanupFailure = appendCleanupFailure(cleanupFailure, t);
    } finally {
      _sharedSuiteOwner = null;
    }
    return cleanupFailure;
  }

  protected void resetSharedSuiteStatics()
      throws Exception {
    AvroFileSchemaKafkaAvroMessageDecoder._avroFile = null;
  }

  static SharedKafkaRealtimeIntegrationTestSuite getSharedSuiteOwner() {
    assertNotNull(_sharedSuiteOwner, "Shared Kafka realtime suite has not been started");
    return _sharedSuiteOwner;
  }

  @Override
  protected final int getNumKafkaBrokers() {
    return DEFAULT_TRANSACTION_NUM_KAFKA_BROKERS;
  }

  @Override
  protected final boolean useKafkaTransaction() {
    // BaseClusterIntegrationTest uses this while Kafka starts to wait for the transaction coordinator. Individual
    // tables still opt into read_committed explicitly so the canonical LLC table retains its original configuration.
    return _startingTransactionalKafka;
  }

  final ScenarioLease newScenario(String tableName, String topicName)
      throws Exception {
    File scenarioDir = new File(_tempDir, tableName);
    File segmentDir = new File(scenarioDir, "segmentDir");
    File tarDir = new File(scenarioDir, "tarDir");
    TestUtils.ensureDirectoriesExistAndEmpty(scenarioDir);
    TestUtils.ensureDirectoriesExistAndEmpty(segmentDir, tarDir);
    ScenarioLease lease = new ScenarioLease(tableName, topicName, scenarioDir, segmentDir, tarDir);
    _scenarioLeases.add(lease);
    return lease;
  }

  final List<File> unpackScenarioData(ScenarioLease lease)
      throws Exception {
    return unpackAvroData(lease._scenarioDir);
  }

  final Schema addScenarioSchema(ScenarioLease lease)
      throws Exception {
    Schema schema = Schema.cloneSchemaWithName(createSchema(), lease._tableName);
    lease._schemaCreated = true;
    addSchema(schema);
    return schema;
  }

  final void createScenarioTopic(ScenarioLease lease) {
    lease._topicCreated = true;
    createKafkaTopic(lease._topicName);
  }

  final Map<String, String> getScenarioStreamConfigs(String topicName, boolean readCommitted) {
    Map<String, String> streamConfigs = super.getStreamConfigMap();
    String streamType = streamConfigs.get(StreamConfigProperties.STREAM_TYPE);
    streamConfigs.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME),
        topicName);
    if (readCommitted) {
      streamConfigs.put(KafkaStreamConfigProperties.constructStreamProperty(
              KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_ISOLATION_LEVEL),
          KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_ISOLATION_LEVEL_READ_COMMITTED);
      streamConfigs.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, Integer.toString(10 * 1024 * 1024));
    }
    return streamConfigs;
  }

  final TableConfig createScenarioTableConfig(ScenarioLease lease, File sampleAvroFile,
      Map<String, String> streamConfigs) {
    AvroFileSchemaKafkaAvroMessageDecoder._avroFile = sampleAvroFile;
    return getTableConfigBuilder(TableType.REALTIME).setTableName(lease._tableName).setIngestionConfig(null)
        .setStreamConfigs(streamConfigs).build();
  }

  final void addScenarioTable(ScenarioLease lease, TableConfig tableConfig)
      throws Exception {
    lease._tableCreated = true;
    addTableConfig(tableConfig);
    waitForAllRealtimePartitionsConsuming(TableNameBuilder.REALTIME.tableNameWithType(lease._tableName),
        getRealtimePartitionsReadyTimeoutMs());
  }

  final void waitForScenarioCount(ScenarioLease lease, long expected, long timeoutMs) {
    TestUtils.waitForCondition(aVoid -> getCurrentCountStarResult(lease._tableName) == expected, 100L, timeoutMs,
        "Failed to load " + expected + " documents for scenario table: " + lease._tableName);
    assertEquals(getCurrentCountStarResult(lease._tableName), expected);
  }

  final void waitForScenarioQueryResult(String query, long expected, long timeoutMs) {
    TestUtils.waitForCondition(aVoid -> getScenarioQueryResult(query) == expected, 100L, timeoutMs,
        "Query did not return expected value " + expected + ": " + query);
    assertEquals(getScenarioQueryResult(query), expected);
  }

  private long getScenarioQueryResult(String query) {
    ResultSetGroup resultSetGroup = getPinotConnection().execute(query);
    if (resultSetGroup.getResultSetCount() == 0) {
      return Long.MIN_VALUE;
    }
    return Long.parseLong(resultSetGroup.getResultSet(0).getString(0));
  }

  final long getDefaultScenarioCount() {
    return super.getCountStarResult();
  }

  final SegmentZKMetadata getSegmentZKMetadata(String tableNameWithType, String segmentName) {
    return _helixResourceManager.getSegmentZKMetadata(tableNameWithType, segmentName);
  }

  final void updateSegmentZKMetadata(String tableNameWithType, SegmentZKMetadata metadata) {
    _helixResourceManager.updateZkMetadata(tableNameWithType, metadata);
  }

  final boolean scenarioTableExists(String tableName) {
    return _helixResourceManager.getRealtimeTableConfig(tableName) != null;
  }

  final void startAdditionalServer()
      throws Exception {
    startServer();
  }

  final void stopSharedKafkaForFinalScenario() {
    if (_kafkaStarted) {
      stopKafka();
      _kafkaStarted = false;
    }
  }

  final void closeScenario(ScenarioLease lease, Throwable primaryFailure, CleanupAction resetAction)
      throws Exception {
    if (resetAction != null) {
      lease._resetAction = resetAction;
    }
    Throwable cleanupFailure = cleanUpScenario(lease, null);
    if (cleanupFailure != null) {
      if (primaryFailure != null) {
        primaryFailure.addSuppressed(cleanupFailure);
      } else if (cleanupFailure instanceof Error) {
        throw (Error) cleanupFailure;
      } else if (cleanupFailure instanceof Exception) {
        throw (Exception) cleanupFailure;
      } else {
        throw new RuntimeException(cleanupFailure);
      }
    }
  }

  private Throwable cleanUpScenario(ScenarioLease lease, Throwable cleanupFailure) {
    boolean tableRemoved = !lease._tableCreated;
    if (lease._tableCreated) {
      boolean dataManagerRemoved = true;
      boolean externalViewRemoved = true;
      try {
        if (_helixResourceManager.getRealtimeTableConfig(lease._tableName) != null) {
          dropRealtimeTable(lease._tableName);
        }
      } catch (Throwable t) {
        cleanupFailure = appendCleanupFailure(cleanupFailure, t);
      }

      String tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(lease._tableName);
      try {
        waitForTableDataManagerRemoved(tableNameWithType);
      } catch (Throwable t) {
        cleanupFailure = appendCleanupFailure(cleanupFailure, t);
        dataManagerRemoved = false;
      }
      try {
        waitForEVToDisappear(tableNameWithType);
      } catch (Throwable t) {
        cleanupFailure = appendCleanupFailure(cleanupFailure, t);
        externalViewRemoved = false;
      }
      try {
        tableRemoved = dataManagerRemoved && externalViewRemoved
            && _helixResourceManager.getRealtimeTableConfig(lease._tableName) == null;
      } catch (Throwable t) {
        cleanupFailure = appendCleanupFailure(cleanupFailure, t);
        tableRemoved = false;
      }
      if (tableRemoved) {
        lease._tableCreated = false;
      }
    }

    if (tableRemoved && lease._schemaCreated) {
      try {
        if (_helixResourceManager.getSchema(lease._tableName) != null) {
          deleteSchema(lease._tableName);
        }
        lease._schemaCreated = false;
      } catch (Throwable t) {
        cleanupFailure = appendCleanupFailure(cleanupFailure, t);
      }
    }
    if (tableRemoved && lease._topicCreated && !isTopicUsedByActiveScenario(lease)) {
      try {
        if (_kafkaStarted && kafkaTopicExists(lease._topicName)) {
          deleteKafkaTopic(lease._topicName);
        }
        lease._topicCreated = false;
      } catch (Throwable t) {
        cleanupFailure = appendCleanupFailure(cleanupFailure, t);
      }
    }
    if (tableRemoved) {
      try {
        FileUtils.deleteDirectory(lease._scenarioDir);
      } catch (Throwable t) {
        cleanupFailure = appendCleanupFailure(cleanupFailure, t);
      }
    }
    if (tableRemoved && lease._resetAction != null) {
      try {
        lease._resetAction.run();
        lease._resetAction = null;
      } catch (Throwable t) {
        cleanupFailure = appendCleanupFailure(cleanupFailure, t);
      }
    }
    if (!lease._tableCreated && !lease._schemaCreated && !lease._topicCreated && lease._resetAction == null) {
      _scenarioLeases.remove(lease);
    }
    return cleanupFailure;
  }

  private boolean isTopicUsedByActiveScenario(ScenarioLease topicOwner) {
    for (ScenarioLease lease : _scenarioLeases) {
      if (lease != topicOwner && lease._tableCreated && lease._topicName.equals(topicOwner._topicName)) {
        return true;
      }
    }
    return false;
  }

  private boolean kafkaTopicExists(String topicName)
      throws Exception {
    Properties adminProperties = new Properties();
    adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBrokerList());
    adminProperties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
    adminProperties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
    try (AdminClient adminClient = AdminClient.create(adminProperties)) {
      return adminClient.listTopics().names().get(5L, TimeUnit.SECONDS).contains(topicName);
    }
  }

  static Throwable appendCleanupFailure(Throwable cleanupFailure, Throwable failure) {
    if (cleanupFailure == null) {
      return failure;
    }
    cleanupFailure.addSuppressed(failure);
    return cleanupFailure;
  }

  @FunctionalInterface
  interface CleanupAction {
    void run()
        throws Throwable;
  }

  static final class ScenarioLease {
    final String _tableName;
    final String _topicName;
    final File _scenarioDir;
    final File _segmentDir;
    final File _tarDir;
    boolean _schemaCreated;
    boolean _tableCreated;
    boolean _topicCreated;
    CleanupAction _resetAction;

    private ScenarioLease(String tableName, String topicName, File scenarioDir, File segmentDir, File tarDir) {
      _tableName = tableName;
      _topicName = topicName;
      _scenarioDir = scenarioDir;
      _segmentDir = segmentDir;
      _tarDir = tarDir;
    }
  }
}
