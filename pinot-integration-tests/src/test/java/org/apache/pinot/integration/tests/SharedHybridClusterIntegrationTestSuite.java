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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.pinot.broker.broker.helix.BaseBrokerStarter;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.FilterConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

/// Owns the Pinot and Kafka processes shared by the focused hybrid integration-test suite.
///
/// The canonical hybrid class is the only TestNG-managed subclass. Focused scenarios acquire its owner without
/// inheriting and repeating the generic hybrid query tests. When a focused class is run directly,
/// [#acquireSharedSuite()] starts a temporary owner for that class.
public abstract class SharedHybridClusterIntegrationTestSuite extends BaseClusterIntegrationTestSet {
  static final String TENANT_NAME = "TestTenant";
  static final int NUM_OFFLINE_SEGMENTS = 8;
  static final int NUM_REALTIME_SEGMENTS = 6;
  static final long DEFAULT_HYBRID_COUNT = DEFAULT_COUNT_STAR_RESULT;
  static final long FILTERED_HYBRID_COUNT = 24_047L;
  static final String INGESTION_TIME_COLUMN = "millisSinceEpoch";

  private static SharedHybridClusterIntegrationTestSuite _sharedSuiteOwner;
  private static Throwable _sharedSuiteLifecycleFailure;

  private final List<HybridScenarioLease> _scenarioLeases = new ArrayList<>();
  private List<File> _sharedAvroFiles;
  private boolean _zkStarted;
  private boolean _controllerStarted;
  private boolean _brokerStarted;
  private boolean _serversStarted;
  private boolean _kafkaStarted;
  private BaseBrokerStarter _transientBroker;
  private String _transientBrokerId;
  private Integer _transientBrokerPort;
  private int _transientBrokerPortCountBeforeStart = -1;
  private boolean _transientBrokerStopped;
  private Schema _canonicalSchema;
  private boolean _canonicalSchemaRestoreNeeded;
  private boolean _canonicalRoutingRecoveryNeeded;

  @Override
  protected final String getBrokerTenant() {
    return TENANT_NAME;
  }

  @Override
  protected final String getServerTenant() {
    return TENANT_NAME;
  }

  @Override
  protected final void overrideControllerConf(Map<String, Object> properties) {
    properties.put(ControllerConf.CLUSTER_TENANT_ISOLATION_ENABLE, false);
  }

  @Override
  protected final void overrideBrokerConf(PinotConfiguration configuration) {
    configuration.setProperty(CommonConstants.Broker.CONFIG_OF_BROKER_INSTANCE_TAGS,
        TagNameUtils.getBrokerTagForTenant(TENANT_NAME));
  }

  @Override
  protected final void overrideServerConf(PinotConfiguration configuration) {
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_REALTIME_OFFHEAP_ALLOCATION, false);
  }

  @BeforeSuite(alwaysRun = true)
  public final void setUpSharedHybridSuite()
      throws Throwable {
    if (_sharedSuiteLifecycleFailure != null) {
      throw new IllegalStateException("A previous shared hybrid suite did not shut down cleanly",
          _sharedSuiteLifecycleFailure);
    }
    if (_sharedSuiteOwner != null) {
      throw new IllegalStateException("Shared hybrid suite already has an owner");
    }
    _sharedSuiteOwner = this;

    Throwable primaryFailure = null;
    try {
      TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

      _zkStarted = true;
      startZk();
      _controllerStarted = true;
      startController();
      configureClusterParallelism();

      _brokerStarted = true;
      startBroker();
      _serversStarted = true;
      startServers(2);

      _kafkaStarted = true;
      startKafkaWithoutTopic();
      createServerTenant(TENANT_NAME, 1, 1);
      _sharedAvroFiles = getAllAvroFiles();
    } catch (Throwable t) {
      primaryFailure = t;
      throw t;
    } finally {
      if (primaryFailure != null) {
        Throwable cleanupFailure;
        try {
          cleanupFailure = tearDownSharedHybridSuiteInternal();
        } catch (Throwable t) {
          cleanupFailure = t;
          recordSuiteLifecycleFailure(t);
        }
        if (cleanupFailure != null) {
          primaryFailure.addSuppressed(cleanupFailure);
        }
      }
    }
  }

  private void configureClusterParallelism() {
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(getHelixClusterName())
            .build();
    _helixManager.getConfigAccessor()
        .set(scope, CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM, Integer.toString(10));
    _helixManager.getConfigAccessor()
        .set(scope, CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM, Integer.toString(6));
    _helixManager.getConfigAccessor()
        .set(scope, CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_DOWNLOAD_PARALLELISM, Integer.toString(12));
  }

  @AfterSuite(alwaysRun = true)
  public final void tearDownSharedHybridSuite()
      throws Throwable {
    Throwable cleanupFailure;
    try {
      cleanupFailure = tearDownSharedHybridSuiteInternal();
    } catch (Throwable t) {
      cleanupFailure = t;
      recordSuiteLifecycleFailure(t);
    }
    if (cleanupFailure != null) {
      throw cleanupFailure;
    }
  }

  private Throwable tearDownSharedHybridSuiteInternal() {
    Throwable cleanupFailure = cleanUpTransientBroker(null);
    for (int i = _scenarioLeases.size() - 1; i >= 0; i--) {
      HybridScenarioLease lease = _scenarioLeases.get(i);
      lease._cleanupRequested = true;
      cleanupFailure = cleanUpScenarioSafely(lease, cleanupFailure);
    }

    cleanupFailure = releaseCanonicalQueryState(cleanupFailure);
    // Retry a transient broker cleanup that might have failed during the final test's after-method cleanup.
    cleanupFailure = cleanUpTransientBroker(cleanupFailure);

    if (_serversStarted) {
      try {
        stopServer();
        _serversStarted = false;
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

    if (!_serversStarted) {
      for (HybridScenarioLease lease : _scenarioLeases) {
        resetScenarioDecoder(lease);
      }
      AvroFileSchemaKafkaAvroMessageDecoder._avroFile = null;
    }
    if (!_zkStarted && !_controllerStarted && !_brokerStarted && !_serversStarted && !_kafkaStarted) {
      try {
        FileUtils.deleteDirectory(_tempDir);
      } catch (Throwable t) {
        cleanupFailure = appendCleanupFailure(cleanupFailure, t);
      }
    }
    _sharedAvroFiles = null;
    _canonicalSchema = null;
    _canonicalSchemaRestoreNeeded = false;
    _canonicalRoutingRecoveryNeeded = false;
    _sharedSuiteOwner = null;
    if (cleanupFailure != null) {
      recordSuiteLifecycleFailure(cleanupFailure);
    }
    return cleanupFailure;
  }

  private static void recordSuiteLifecycleFailure(Throwable failure) {
    if (_sharedSuiteLifecycleFailure == null) {
      _sharedSuiteLifecycleFailure = failure;
    } else if (_sharedSuiteLifecycleFailure != failure) {
      _sharedSuiteLifecycleFailure.addSuppressed(failure);
    }
  }

  static synchronized SharedHybridSuiteLease acquireSharedSuite()
      throws Throwable {
    if (_sharedSuiteLifecycleFailure != null) {
      throw new IllegalStateException("A previous shared hybrid suite did not shut down cleanly",
          _sharedSuiteLifecycleFailure);
    }
    if (_sharedSuiteOwner != null) {
      return new SharedHybridSuiteLease(_sharedSuiteOwner, false);
    }

    SharedHybridClusterIntegrationTestSuite owner = new DirectFocusedHybridSuiteOwner();
    owner.setUpSharedHybridSuite();
    return new SharedHybridSuiteLease(owner, true);
  }

  final HybridScenarioLease newScenario(String tableName, String topicName)
      throws Throwable {
    enforceScenarioCleanupBarrier();
    File scenarioDir = new File(_tempDir, tableName);
    File segmentDir = new File(scenarioDir, "segmentDir");
    File tarDir = new File(scenarioDir, "tarDir");
    TestUtils.ensureDirectoriesExistAndEmpty(scenarioDir, segmentDir, tarDir);
    HybridScenarioLease lease = new HybridScenarioLease(tableName, topicName, scenarioDir, segmentDir, tarDir);
    _scenarioLeases.add(lease);
    return lease;
  }

  final Schema setUpStandardScenario(HybridScenarioLease lease, String schemaFileName, long expectedCount)
      throws Exception {
    createScenarioTopic(lease);

    Schema schema = schemaFileName == null ? createSchema() : createSchema(schemaFileName);
    schema = Schema.cloneSchemaWithName(schema, lease._tableName);
    addScenarioSchema(lease, schema);

    List<File> offlineAvroFiles = getOfflineAvroFiles(_sharedAvroFiles, NUM_OFFLINE_SEGMENTS);
    List<File> realtimeAvroFiles = getRealtimeAvroFiles(_sharedAvroFiles, NUM_REALTIME_SEGMENTS);
    TableConfig offlineTableConfig = createStandardOfflineTableConfig(lease);
    TableConfig realtimeTableConfig = createStandardRealtimeTableConfig(lease, realtimeAvroFiles.get(0));
    addScenarioTables(lease, offlineTableConfig, realtimeTableConfig);
    buildAndUploadOfflineSegments(lease, offlineAvroFiles, offlineTableConfig, schema);
    pushRealtimeRecords(lease, realtimeAvroFiles);

    lease._offlineInputCount = countRecords(offlineAvroFiles);
    lease._realtimeInputCount = countRecords(realtimeAvroFiles);
    waitForScenarioCount(lease._tableName, expectedCount, 600_000L);
    return schema;
  }

  final Schema setUpIngestionConfigScenario(HybridScenarioLease lease)
      throws Exception {
    createScenarioTopic(lease);

    Schema schema = createIngestionConfigSchema(lease._tableName);
    addScenarioSchema(lease, schema);
    List<File> offlineAvroFiles = getOfflineAvroFiles(_sharedAvroFiles, NUM_OFFLINE_SEGMENTS);
    List<File> realtimeAvroFiles = getRealtimeAvroFiles(_sharedAvroFiles, NUM_REALTIME_SEGMENTS);
    Map<String, String> streamConfig = getScenarioStreamConfig(lease._topicName);
    TableConfig offlineTableConfig =
        createIngestionConfigTableConfig(lease._tableName, TableType.OFFLINE, streamConfig);
    TableConfig realtimeTableConfig =
        createIngestionConfigTableConfig(lease._tableName, TableType.REALTIME, streamConfig);
    installScenarioDecoder(lease, realtimeAvroFiles.get(0));
    addScenarioTables(lease, offlineTableConfig, realtimeTableConfig);
    buildAndUploadOfflineSegments(lease, offlineAvroFiles, offlineTableConfig, schema);
    pushRealtimeRecords(lease, realtimeAvroFiles);
    waitForScenarioCount(lease._tableName, FILTERED_HYBRID_COUNT, 600_000L);
    return schema;
  }

  private void createScenarioTopic(HybridScenarioLease lease) {
    lease._topicCreated = true;
    createKafkaTopic(lease._topicName);
  }

  private void addScenarioSchema(HybridScenarioLease lease, Schema schema)
      throws Exception {
    lease._schemaCreated = true;
    addSchema(schema);
  }

  private TableConfig createStandardOfflineTableConfig(HybridScenarioLease lease) {
    TableConfig tableConfig = createOfflineTableConfig();
    tableConfig.setTableName(TableNameBuilder.OFFLINE.tableNameWithType(lease._tableName));
    return tableConfig;
  }

  private TableConfig createStandardRealtimeTableConfig(HybridScenarioLease lease, File sampleAvroFile) {
    installScenarioDecoder(lease, sampleAvroFile);
    TableConfig tableConfig = createRealtimeTableConfig(sampleAvroFile);
    tableConfig.setTableName(TableNameBuilder.REALTIME.tableNameWithType(lease._tableName));
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    Map<String, String> streamConfig = new HashMap<>(indexingConfig.getStreamConfigs());
    String streamType = streamConfig.get(StreamConfigProperties.STREAM_TYPE);
    streamConfig.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME),
        lease._topicName);
    indexingConfig.setStreamConfigs(streamConfig);
    tableConfig.setIndexingConfig(indexingConfig);
    return tableConfig;
  }

  private TableConfig createIngestionConfigTableConfig(String tableName, TableType tableType,
      Map<String, String> streamConfig) {
    IngestionConfig ingestionConfig = createIngestionConfig(streamConfig);
    return new TableConfigBuilder(tableType).setTableName(tableName).setTimeColumnName(INGESTION_TIME_COLUMN)
        .setNumReplicas(getNumReplicas()).setSegmentVersion(getSegmentVersion()).setLoadMode(getLoadMode())
        .setTaskConfig(getTaskConfig()).setBrokerTenant(getBrokerTenant()).setServerTenant(getServerTenant())
        .setIngestionConfig(ingestionConfig).setQueryConfig(getQueryConfig()).setFieldConfigList(getFieldConfigs())
        .setNullHandlingEnabled(getNullHandlingEnabled()).setSegmentPartitionConfig(getSegmentPartitionConfig())
        .setOptimizeNoDictStatsCollection(true).build();
  }

  private static IngestionConfig createIngestionConfig(Map<String, String> streamConfig) {
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(new StreamIngestionConfig(List.of(new HashMap<>(streamConfig))));
    ingestionConfig.setFilterConfig(
        new FilterConfig("Groovy({AirlineID == 19393 || ArrDelayMinutes <= 5 }, AirlineID, ArrDelayMinutes)"));
    ingestionConfig.setTransformConfigs(List.of(
        new TransformConfig("AmPm", "Groovy({DepTime < 1200 ? \"AM\": \"PM\"}, DepTime)"),
        new TransformConfig(INGESTION_TIME_COLUMN, "fromEpochDays(DaysSinceEpoch)"),
        new TransformConfig("lowerCaseDestCityName", "lower(DestCityName)")));
    return ingestionConfig;
  }

  private static Schema createIngestionConfigSchema(String tableName) {
    return new Schema.SchemaBuilder().setSchemaName(tableName)
        .addSingleValueDimension("AirlineID", FieldSpec.DataType.LONG)
        .addSingleValueDimension("DepTime", FieldSpec.DataType.INT)
        .addSingleValueDimension("AmPm", FieldSpec.DataType.STRING)
        .addSingleValueDimension("lowerCaseDestCityName", FieldSpec.DataType.STRING)
        .addMetric("ArrDelayMinutes", FieldSpec.DataType.DOUBLE)
        .addDateTime(INGESTION_TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:DAYS").build();
  }

  private Map<String, String> getScenarioStreamConfig(String topicName) {
    Map<String, String> streamConfig = super.getStreamConfigMap();
    String streamType = streamConfig.get(StreamConfigProperties.STREAM_TYPE);
    streamConfig.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME),
        topicName);
    return streamConfig;
  }

  private void addScenarioTables(HybridScenarioLease lease, TableConfig offlineTableConfig,
      TableConfig realtimeTableConfig)
      throws Exception {
    lease._routingTableMayExist = true;
    lease._offlineTableCreated = true;
    addTableConfig(offlineTableConfig);
    lease._realtimeTableCreated = true;
    addTableConfig(realtimeTableConfig);
    waitForAllRealtimePartitionsConsuming(TableNameBuilder.REALTIME.tableNameWithType(lease._tableName), 120_000L);
  }

  private void buildAndUploadOfflineSegments(HybridScenarioLease lease, List<File> offlineAvroFiles,
      TableConfig offlineTableConfig, Schema schema)
      throws Exception {
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(offlineAvroFiles, offlineTableConfig, schema, 0,
        lease._segmentDir, lease._tarDir);
    uploadSegments(lease._tableName, lease._tarDir);
  }

  private void pushRealtimeRecords(HybridScenarioLease lease, List<File> realtimeAvroFiles)
      throws Exception {
    ClusterIntegrationTestUtils.pushAvroIntoKafka(realtimeAvroFiles, getKafkaBrokerList(), lease._topicName,
        getMaxNumKafkaMessagesPerBatch(), getKafkaMessageHeader(), getPartitionColumn(), false);
  }

  private static long countRecords(List<File> avroFiles)
      throws Exception {
    long count = 0L;
    for (File avroFile : avroFiles) {
      try (DataFileStream<GenericRecord> reader = AvroUtils.getAvroReader(avroFile)) {
        while (reader.hasNext()) {
          reader.next();
          count++;
        }
      }
    }
    return count;
  }

  final void initializeCanonicalQueryState(List<File> avroFiles, Schema schema)
      throws Exception {
    setUpH2Connection(avroFiles);
    setUpQueryGenerator(avroFiles);
    _canonicalSchema = Schema.cloneSchemaWithName(schema, schema.getSchemaName());
  }

  final List<File> getSharedAvroFiles() {
    return _sharedAvroFiles;
  }

  final Throwable releaseCanonicalQueryState(Throwable cleanupFailure) {
    if (_h2Connection != null) {
      try {
        _h2Connection.close();
        _h2Connection = null;
      } catch (Throwable t) {
        cleanupFailure = appendCleanupFailure(cleanupFailure, t);
      }
    }
    _queryGenerator = null;
    return cleanupFailure;
  }

  final void markCanonicalSchemaMutation() {
    _canonicalSchemaRestoreNeeded = true;
    _canonicalRoutingRecoveryNeeded = true;
  }

  final void clearCanonicalSchemaMutation() {
    _canonicalSchemaRestoreNeeded = false;
  }

  final void markCanonicalRoutingMutation() {
    _canonicalRoutingRecoveryNeeded = true;
  }

  @AfterMethod(alwaysRun = true)
  public final void recoverSharedClusterAfterMethod(ITestResult testResult)
      throws Throwable {
    Throwable cleanupFailure = cleanUpTransientBroker(null);
    cleanupFailure = restoreCanonicalSchemaIfNeeded(cleanupFailure);
    cleanupFailure = scrubClusterState(cleanupFailure);
    cleanupFailure = waitForCanonicalRoutingRecovery(cleanupFailure);
    if (cleanupFailure != null) {
      Throwable primaryFailure = testResult.getThrowable();
      if (primaryFailure != null) {
        primaryFailure.addSuppressed(cleanupFailure);
      } else {
        throw cleanupFailure;
      }
    }
  }

  private Throwable restoreCanonicalSchemaIfNeeded(Throwable cleanupFailure) {
    if (!_canonicalSchemaRestoreNeeded || _canonicalSchema == null || !_controllerStarted) {
      return cleanupFailure;
    }
    boolean schemaRestored = false;
    try {
      Schema currentSchema = getSchema(_canonicalSchema.getSchemaName());
      if (currentSchema == null
          || !currentSchema.toSingleLineJsonString().equals(_canonicalSchema.toSingleLineJsonString())) {
        forceUpdateSchema(_canonicalSchema);
      }
      schemaRestored = true;
    } catch (Throwable t) {
      cleanupFailure = appendCleanupFailure(cleanupFailure, t);
    }
    if (!schemaRestored) {
      return cleanupFailure;
    }

    String realtimeJob = null;
    String offlineJob = null;
    try {
      realtimeJob = reloadTableAndValidateResponse(_canonicalSchema.getSchemaName(), TableType.REALTIME, false);
    } catch (Throwable t) {
      cleanupFailure = appendCleanupFailure(cleanupFailure, t);
    }
    try {
      offlineJob = reloadTableAndValidateResponse(_canonicalSchema.getSchemaName(), TableType.OFFLINE, false);
    } catch (Throwable t) {
      cleanupFailure = appendCleanupFailure(cleanupFailure, t);
    }
    boolean realtimeReloaded = realtimeJob != null;
    boolean offlineReloaded = offlineJob != null;
    if (realtimeJob != null) {
      try {
        waitForReloadJob(realtimeJob, TableType.REALTIME);
      } catch (Throwable t) {
        realtimeReloaded = false;
        cleanupFailure = appendCleanupFailure(cleanupFailure, t);
      }
    }
    if (offlineJob != null) {
      try {
        waitForReloadJob(offlineJob, TableType.OFFLINE);
      } catch (Throwable t) {
        offlineReloaded = false;
        cleanupFailure = appendCleanupFailure(cleanupFailure, t);
      }
    }
    if (realtimeReloaded && offlineReloaded) {
      _canonicalSchemaRestoreNeeded = false;
    }
    return cleanupFailure;
  }

  private void waitForReloadJob(String reloadJob, TableType tableType) {
    TestUtils.waitForCondition(() -> isReloadJobCompleted(reloadJob), 100L, 600_000L,
        "Failed to restore the canonical " + tableType + " schema after a test failure", null);
  }

  private Throwable scrubClusterState(Throwable cleanupFailure) {
    setUseMultiStageQueryEngine(false);
    if (!_canonicalRoutingRecoveryNeeded || !_controllerStarted || _helixAdmin == null) {
      return cleanupFailure;
    }
    List<String> instances;
    try {
      instances = _helixAdmin.getInstancesInCluster(getHelixClusterName());
    } catch (Throwable t) {
      return appendCleanupFailure(cleanupFailure, t);
    }
    for (String instance : instances) {
      if (InstanceTypeUtils.isServer(instance)) {
        try {
          getOrCreateAdminClient().getInstanceClient().updateInstanceState(instance, "QUERIES_ENABLE");
        } catch (Throwable t) {
          cleanupFailure = appendCleanupFailure(cleanupFailure, t);
        }
      }
      try {
        InstanceConfig instanceConfig = _helixAdmin.getInstanceConfig(getHelixClusterName(), instance);
        instanceConfig.getRecord().setBooleanField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, false);
        _helixAdmin.setInstanceConfig(getHelixClusterName(), instance, instanceConfig);
      } catch (Throwable t) {
        cleanupFailure = appendCleanupFailure(cleanupFailure, t);
      }
    }
    return cleanupFailure;
  }

  private Throwable waitForCanonicalRoutingRecovery(Throwable cleanupFailure) {
    if (!_canonicalRoutingRecoveryNeeded || !_controllerStarted) {
      return cleanupFailure;
    }
    try {
      TestUtils.waitForCondition(() -> getCurrentCountStarResult(getTableName()) == DEFAULT_HYBRID_COUNT, 100L,
          60_000L, "Failed to restore canonical hybrid routing after a stateful test", null);
      _canonicalRoutingRecoveryNeeded = false;
    } catch (Throwable t) {
      cleanupFailure = appendCleanupFailure(cleanupFailure, t);
    }
    return cleanupFailure;
  }

  final BaseBrokerStarter startTrackedBroker(int brokerId)
      throws Exception {
    if (_transientBroker != null) {
      throw new IllegalStateException("A transient broker is already tracked");
    }
    markCanonicalRoutingMutation();
    _transientBroker = createBrokerStarter();
    _transientBrokerPortCountBeforeStart = _brokerPorts.size();
    PinotConfiguration brokerConfiguration = getBrokerConf(brokerId);
    _transientBrokerPort = _brokerPorts.get(_brokerPorts.size() - 1);
    _transientBroker.init(brokerConfiguration);
    _transientBrokerId = _transientBroker.getInstanceId();
    _transientBroker.start();
    return _transientBroker;
  }

  final void stopTrackedBrokerProcess() {
    if (_transientBroker != null && !_transientBrokerStopped) {
      _transientBroker.stop();
      _transientBrokerStopped = true;
    }
    removeTransientBrokerPort();
  }

  final void removeTrackedBrokerFromCluster()
      throws Exception {
    if (_transientBrokerId == null || !_helixAdmin.getInstancesInCluster(getHelixClusterName())
        .contains(_transientBrokerId)) {
      if (_transientBrokerStopped) {
        clearTransientBrokerTracking();
      }
      return;
    }
    getOrCreateAdminClient().getInstanceClient().updateInstanceTags(_transientBrokerId, List.of(), true);
    waitForBrokerResourceRemoval(_transientBrokerId);
    getOrCreateAdminClient().getInstanceClient().dropInstance(_transientBrokerId);
    if (_transientBrokerStopped) {
      clearTransientBrokerTracking();
    }
  }

  private void waitForBrokerResourceRemoval(String brokerId) {
    TestUtils.waitForCondition(ignored -> !isBrokerInResource(brokerId), 60_000L,
        "Failed to remove transient broker from broker resource");
  }

  private boolean isBrokerInResource(String brokerId) {
    IdealState idealState =
        _helixAdmin.getResourceIdealState(getHelixClusterName(), CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    if (idealState != null) {
      for (Map<String, String> assignment : idealState.getRecord().getMapFields().values()) {
        if (assignment.containsKey(brokerId)) {
          return true;
        }
      }
    }
    ExternalView externalView =
        _helixAdmin.getResourceExternalView(getHelixClusterName(), CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    if (externalView != null) {
      for (Map<String, String> assignment : externalView.getRecord().getMapFields().values()) {
        if (assignment.containsKey(brokerId)) {
          return true;
        }
      }
    }
    return false;
  }

  private Throwable cleanUpTransientBroker(Throwable cleanupFailure) {
    if (_transientBroker == null && _transientBrokerId == null && _transientBrokerPort == null) {
      return cleanupFailure;
    }
    try {
      stopTrackedBrokerProcess();
    } catch (Throwable t) {
      cleanupFailure = appendCleanupFailure(cleanupFailure, t);
    }
    if (_transientBrokerStopped) {
      try {
        removeTransientBrokerPort();
      } catch (Throwable t) {
        cleanupFailure = appendCleanupFailure(cleanupFailure, t);
      }
    }
    if (_transientBrokerId != null && _controllerStarted) {
      try {
        if (_helixAdmin.getInstancesInCluster(getHelixClusterName()).contains(_transientBrokerId)) {
          getOrCreateAdminClient().getInstanceClient().updateInstanceTags(_transientBrokerId, List.of(), true);
          waitForBrokerResourceRemoval(_transientBrokerId);
          getOrCreateAdminClient().getInstanceClient().dropInstance(_transientBrokerId);
        }
        if (_transientBrokerStopped) {
          clearTransientBrokerTracking();
        }
      } catch (Throwable t) {
        cleanupFailure = appendCleanupFailure(cleanupFailure, t);
      }
    } else if (_transientBrokerStopped) {
      clearTransientBrokerTracking();
    }
    return cleanupFailure;
  }

  private void removeTransientBrokerPort() {
    if (_transientBrokerPort != null) {
      _brokerPorts.remove(_transientBrokerPort);
      _transientBrokerPort = null;
    }
    if (_transientBrokerPortCountBeforeStart >= 0) {
      while (_brokerPorts.size() > _transientBrokerPortCountBeforeStart) {
        _brokerPorts.remove(_brokerPorts.size() - 1);
      }
    }
  }

  private void clearTransientBrokerTracking() {
    _transientBroker = null;
    _transientBrokerId = null;
    _transientBrokerPort = null;
    _transientBrokerPortCountBeforeStart = -1;
    _transientBrokerStopped = false;
  }

  final void closeScenario(HybridScenarioLease lease, Throwable primaryFailure)
      throws Throwable {
    lease._cleanupRequested = true;
    Throwable cleanupFailure = cleanUpScenarioSafely(lease, null);
    if (cleanupFailure != null) {
      if (primaryFailure != null) {
        primaryFailure.addSuppressed(cleanupFailure);
      } else {
        throw cleanupFailure;
      }
    }
  }

  private Throwable cleanUpScenario(HybridScenarioLease lease, Throwable cleanupFailure) {
    cleanupFailure = cleanUpTable(lease, TableType.REALTIME, cleanupFailure);
    cleanupFailure = cleanUpTable(lease, TableType.OFFLINE, cleanupFailure);

    boolean tablesRemoved = !lease._offlineTableCreated && !lease._realtimeTableCreated;
    if (tablesRemoved && lease._routingTableMayExist) {
      if (_brokerStarted) {
        try {
          waitForBrokerRoutingTableToDisappear(lease._tableName);
          lease._routingTableMayExist = false;
        } catch (Throwable t) {
          cleanupFailure = appendCleanupFailure(cleanupFailure, t);
        }
      } else {
        cleanupFailure = appendCleanupFailure(cleanupFailure,
            new IllegalStateException("Cannot confirm routing-table removal after the broker has stopped: "
                + lease._tableName));
      }
    }
    boolean routingTableRemoved = !lease._routingTableMayExist;
    if (tablesRemoved && routingTableRemoved && lease._schemaCreated) {
      try {
        if (_helixResourceManager.getSchema(lease._tableName) != null) {
          deleteSchema(lease._tableName);
        }
        lease._schemaCreated = false;
      } catch (Throwable t) {
        cleanupFailure = appendCleanupFailure(cleanupFailure, t);
      }
    }
    if (!lease._realtimeTableCreated && routingTableRemoved && lease._topicCreated) {
      try {
        if (_kafkaStarted) {
          deleteKafkaTopic(lease._topicName);
        }
        lease._topicCreated = false;
      } catch (Throwable t) {
        cleanupFailure = appendCleanupFailure(cleanupFailure, t);
      }
    }
    if (tablesRemoved && routingTableRemoved) {
      try {
        FileUtils.deleteDirectory(lease._scenarioDir);
      } catch (Throwable t) {
        cleanupFailure = appendCleanupFailure(cleanupFailure, t);
      }
      resetScenarioDecoder(lease);
    }
    if (!lease._offlineTableCreated && !lease._realtimeTableCreated && !lease._schemaCreated && !lease._topicCreated
        && !lease._routingTableMayExist && !lease._decoderStateOwned) {
      _scenarioLeases.remove(lease);
    }
    return cleanupFailure;
  }

  private Throwable cleanUpScenarioSafely(HybridScenarioLease lease, Throwable cleanupFailure) {
    try {
      return cleanUpScenario(lease, cleanupFailure);
    } catch (Throwable t) {
      return appendCleanupFailure(cleanupFailure, t);
    }
  }

  private Throwable cleanUpTable(HybridScenarioLease lease, TableType tableType, Throwable cleanupFailure) {
    boolean tableCreated = tableType == TableType.OFFLINE ? lease._offlineTableCreated : lease._realtimeTableCreated;
    if (!tableCreated) {
      return cleanupFailure;
    }
    String tableNameWithType = TableNameBuilder.forType(tableType).tableNameWithType(lease._tableName);
    boolean dataManagerRemoved = true;
    boolean externalViewRemoved = true;
    try {
      TableConfig tableConfig = tableType == TableType.OFFLINE
          ? _helixResourceManager.getOfflineTableConfig(lease._tableName)
          : _helixResourceManager.getRealtimeTableConfig(lease._tableName);
      if (tableConfig != null) {
        if (tableType == TableType.OFFLINE) {
          dropOfflineTable(lease._tableName);
        } else {
          dropRealtimeTable(lease._tableName);
        }
      }
    } catch (Throwable t) {
      cleanupFailure = appendCleanupFailure(cleanupFailure, t);
    }
    try {
      waitForTableDataManagerRemoved(tableNameWithType);
    } catch (Throwable t) {
      dataManagerRemoved = false;
      cleanupFailure = appendCleanupFailure(cleanupFailure, t);
    }
    try {
      waitForEVToDisappear(tableNameWithType);
    } catch (Throwable t) {
      externalViewRemoved = false;
      cleanupFailure = appendCleanupFailure(cleanupFailure, t);
    }
    try {
      TableConfig tableConfig = tableType == TableType.OFFLINE
          ? _helixResourceManager.getOfflineTableConfig(lease._tableName)
          : _helixResourceManager.getRealtimeTableConfig(lease._tableName);
      if (dataManagerRemoved && externalViewRemoved && tableConfig == null) {
        if (tableType == TableType.OFFLINE) {
          lease._offlineTableCreated = false;
        } else {
          lease._realtimeTableCreated = false;
        }
      }
    } catch (Throwable t) {
      cleanupFailure = appendCleanupFailure(cleanupFailure, t);
    }
    return cleanupFailure;
  }

  private void enforceScenarioCleanupBarrier()
      throws Throwable {
    if (_scenarioLeases.isEmpty()) {
      return;
    }

    Throwable retryFailure = null;
    boolean activeScenarioExists = false;
    for (HybridScenarioLease lease : List.copyOf(_scenarioLeases)) {
      if (lease._cleanupRequested) {
        retryFailure = cleanUpScenarioSafely(lease, retryFailure);
      } else {
        activeScenarioExists = true;
      }
    }
    if (_scenarioLeases.isEmpty() && retryFailure == null) {
      return;
    }

    IllegalStateException barrierFailure = new IllegalStateException(activeScenarioExists
        ? "Cannot start a hybrid scenario while another scenario is active"
        : "Cannot start a hybrid scenario because cleanup of the previous scenario is incomplete");
    if (retryFailure != null) {
      barrierFailure.addSuppressed(retryFailure);
    }
    throw barrierFailure;
  }

  private void installScenarioDecoder(HybridScenarioLease lease, File avroFile) {
    for (HybridScenarioLease existingLease : _scenarioLeases) {
      if (existingLease != lease && existingLease._decoderStateOwned) {
        throw new IllegalStateException("Another hybrid scenario still owns the static Avro decoder: "
            + existingLease._tableName);
      }
    }
    lease._decoderStateOwned = true;
    AvroFileSchemaKafkaAvroMessageDecoder._avroFile = avroFile;
  }

  private static void resetScenarioDecoder(HybridScenarioLease lease) {
    if (lease._decoderStateOwned) {
      AvroFileSchemaKafkaAvroMessageDecoder._avroFile = null;
      lease._decoderStateOwned = false;
    }
  }

  private void waitForBrokerRoutingTableToDisappear(String tableName) {
    TestUtils.waitForCondition(ignored -> {
      try {
        getDebugInfo("debug/routingTable/" + tableName);
        return false;
      } catch (Exception e) {
        String message = e.getMessage();
        return message != null && message.contains("Got error status code: 404");
      }
    }, 60_000L, "Routing table still exists after dropping hybrid scenario " + tableName);
  }

  final void waitForScenarioCount(String tableName, long expectedCount, long timeoutMs) {
    TestUtils.waitForCondition(ignored -> getCurrentCountStarResult(tableName) == expectedCount, 100L, timeoutMs,
        "Failed to load " + expectedCount + " documents for hybrid scenario " + tableName);
    Assert.assertEquals(getCurrentCountStarResult(tableName), expectedCount);
  }

  final JsonNode queryScenario(String query, boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    return postQuery(query);
  }

  final JsonNode getScenarioDebugInfo(String path)
      throws Exception {
    return getDebugInfo(path);
  }

  final Schema getScenarioSchema(String tableName) {
    return _helixResourceManager.getSchema(tableName);
  }

  final TableConfig getScenarioTableConfig(String tableName, TableType tableType) {
    return tableType == TableType.OFFLINE ? _helixResourceManager.getOfflineTableConfig(tableName)
        : _helixResourceManager.getRealtimeTableConfig(tableName);
  }

  final void resetQueryEngine() {
    setUseMultiStageQueryEngine(false);
  }

  static Throwable appendCleanupFailure(Throwable cleanupFailure, Throwable failure) {
    if (cleanupFailure == null) {
      return failure;
    }
    cleanupFailure.addSuppressed(failure);
    return cleanupFailure;
  }

  /// Borrows the TestNG suite owner or owns a temporary fixture for a directly invoked focused class.
  static final class SharedHybridSuiteLease {
    private final SharedHybridClusterIntegrationTestSuite _owner;
    private final boolean _ownsSuite;
    private boolean _closed;

    private SharedHybridSuiteLease(SharedHybridClusterIntegrationTestSuite owner, boolean ownsSuite) {
      _owner = owner;
      _ownsSuite = ownsSuite;
    }

    SharedHybridClusterIntegrationTestSuite getOwner() {
      return _owner;
    }

    void close(Throwable primaryFailure)
        throws Throwable {
      if (_closed || !_ownsSuite) {
        _closed = true;
        return;
      }
      _closed = true;
      Throwable cleanupFailure;
      try {
        cleanupFailure = _owner.tearDownSharedHybridSuiteInternal();
      } catch (Throwable t) {
        cleanupFailure = t;
        recordSuiteLifecycleFailure(t);
      }
      if (cleanupFailure != null) {
        if (primaryFailure != null) {
          primaryFailure.addSuppressed(cleanupFailure);
        } else {
          throw cleanupFailure;
        }
      }
    }
  }

  /// Concrete fixture used only when a focused class is selected directly with `-Dtest`.
  private static final class DirectFocusedHybridSuiteOwner extends SharedHybridClusterIntegrationTestSuite {
  }

  static final class HybridScenarioLease {
    final String _tableName;
    final String _topicName;
    final File _scenarioDir;
    final File _segmentDir;
    final File _tarDir;
    boolean _schemaCreated;
    boolean _offlineTableCreated;
    boolean _realtimeTableCreated;
    boolean _topicCreated;
    boolean _routingTableMayExist;
    boolean _decoderStateOwned;
    boolean _cleanupRequested;
    long _offlineInputCount;
    long _realtimeInputCount;

    private HybridScenarioLease(String tableName, String topicName, File scenarioDir, File segmentDir, File tarDir) {
      _tableName = tableName;
      _topicName = topicName;
      _scenarioDir = scenarioDir;
      _segmentDir = segmentDir;
      _tarDir = tarDir;
    }
  }
}
