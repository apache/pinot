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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.MetricValueUtils;
import org.apache.pinot.common.metrics.ValidationMetrics;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.ControllerConf.ControllerPeriodicTasksConf;
import org.apache.pinot.controller.validation.OfflineSegmentValidationManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TagOverrideConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Integration test for all {@link org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask}s.
 * The intention of these tests is not to test functionality of daemons, but simply to check that they run as expected
 * and process the tables when the controller starts.
 */
// TODO: Add tests for other ControllerPeriodicTasks (RetentionManager, RealtimeSegmentValidationManager).
public class ControllerPeriodicTasksIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final int PERIODIC_TASK_INITIAL_DELAY_SECONDS = 30;
  private static final String PERIODIC_TASK_FREQUENCY_PERIOD = "5s";
  private static final String PERIODIC_TASK_WAIT_FOR_PUSH_TIME_PERIOD = "5s";
  private static final ZNRecordSerializer RECORD_SERIALIZER = new ZNRecordSerializer();

  private static final int NUM_REPLICAS = 2;
  private static final String TENANT_NAME = "TestTenant";
  private static final int NUM_BROKERS = 1;
  private static final int NUM_OFFLINE_SERVERS = 2;
  private static final int NUM_REALTIME_SERVERS = 2;
  private static final int NUM_OFFLINE_AVRO_FILES = 8;
  private static final int NUM_REALTIME_AVRO_FILES = 6;

  private final String _sharedResourceSuffix = Long.toUnsignedString(RANDOM.nextLong(), Character.MAX_RADIX);
  private String _currentTable = DEFAULT_TABLE_NAME;
  private String _schemaFileName = DEFAULT_SCHEMA_FILE_NAME;
  private File _classTempDir;
  private File _classSegmentDir;
  private File _classTarDir;

  @Override
  protected String getTableName() {
    return scopedTableName(_currentTable);
  }

  @Override
  protected String getLogicalTableName() {
    return isSharedRichClusterEnabled() ? "controller_periodic_tasks_logical_" + _sharedResourceSuffix
        : super.getLogicalTableName();
  }

  @Override
  protected String getKafkaTopic() {
    return isSharedRichClusterEnabled() ? "controller-periodic-tasks-" + _sharedResourceSuffix
        : super.getKafkaTopic();
  }

  @Override
  protected String getSchemaFileName() {
    return _schemaFileName;
  }

  @Override
  protected int getNumReplicas() {
    return NUM_REPLICAS;
  }

  @Override
  protected String getBrokerTenant() {
    return getTenantName();
  }

  @Override
  protected String getServerTenant() {
    return getTenantName();
  }

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    properties.put(ControllerConf.CLUSTER_TENANT_ISOLATION_ENABLE, false);
    properties.put(ControllerPeriodicTasksConf.STATUS_CHECKER_INITIAL_DELAY_IN_SECONDS,
        PERIODIC_TASK_INITIAL_DELAY_SECONDS);
    properties.put(ControllerPeriodicTasksConf.STATUS_CHECKER_FREQUENCY_PERIOD, PERIODIC_TASK_FREQUENCY_PERIOD);
    properties.put(ControllerPeriodicTasksConf.SEGMENT_RELOCATOR_INITIAL_DELAY_IN_SECONDS,
        PERIODIC_TASK_INITIAL_DELAY_SECONDS);
    properties.put(ControllerPeriodicTasksConf.SEGMENT_RELOCATOR_FREQUENCY_PERIOD, PERIODIC_TASK_FREQUENCY_PERIOD);
    properties.put(ControllerPeriodicTasksConf.BROKER_RESOURCE_VALIDATION_INITIAL_DELAY_IN_SECONDS,
        PERIODIC_TASK_INITIAL_DELAY_SECONDS);
    properties.put(ControllerPeriodicTasksConf.BROKER_RESOURCE_VALIDATION_FREQUENCY_PERIOD,
        PERIODIC_TASK_FREQUENCY_PERIOD);
    properties.put(ControllerPeriodicTasksConf.OFFLINE_SEGMENT_INTERVAL_CHECKER_INITIAL_DELAY_IN_SECONDS,
        PERIODIC_TASK_INITIAL_DELAY_SECONDS);
    properties.put(ControllerPeriodicTasksConf.OFFLINE_SEGMENT_INTERVAL_CHECKER_FREQUENCY_PERIOD,
        PERIODIC_TASK_FREQUENCY_PERIOD);
    properties.put(ControllerPeriodicTasksConf.STATUS_CHECKER_WAIT_FOR_PUSH_TIME_PERIOD,
        PERIODIC_TASK_WAIT_FOR_PUSH_TIME_PERIOD);
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    _classTempDir = getClassTempDir();
    _classSegmentDir = new File(_classTempDir, "segmentDir");
    _classTarDir = new File(_classTempDir, "tarDir");
    TestUtils.ensureDirectoriesExistAndEmpty(_classTempDir, _classSegmentDir, _classTarDir);

    startZk();
    startController();
    startBrokers(NUM_BROKERS);
    startServers(NUM_OFFLINE_SERVERS + NUM_REALTIME_SERVERS);
    startKafka();

    // Create tenants
    createBrokerTenant(getTenantName(), NUM_BROKERS);
    createServerTenant(getTenantName(), NUM_OFFLINE_SERVERS, NUM_REALTIME_SERVERS);

    // Unpack the Avro files
    int numAvroFiles = unpackAvroData(_classTempDir).size();
    // Avro files has to be ordered as time series data
    List<File> avroFiles = new ArrayList<>(numAvroFiles);
    for (int i = 1; i <= numAvroFiles; i++) {
      avroFiles.add(new File(_classTempDir, "On_Time_On_Time_Performance_2014_" + i + ".avro"));
    }
    List<File> offlineAvroFiles = avroFiles.subList(0, NUM_OFFLINE_AVRO_FILES);
    List<File> realtimeAvroFiles = avroFiles.subList(numAvroFiles - NUM_REALTIME_AVRO_FILES, numAvroFiles);

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig offlineTableConfig = createOfflineTableConfig();
    addTableConfig(offlineTableConfig);
    addTableConfig(createRealtimeTableConfig(realtimeAvroFiles.get(0)));

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(offlineAvroFiles, offlineTableConfig, schema, 0, _classSegmentDir,
        _classTarDir);
    uploadSegments(getTableName(), _classTarDir);

    // Push data into Kafka
    pushAvroIntoKafka(realtimeAvroFiles);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    Exception exception = null;
    exception = runCleanup(exception, () -> cleanHybridTableAndSchema(DEFAULT_TABLE_NAME));
    exception = runCleanup(exception, this::deleteKafkaTopicIfPresent);
    exception = runCleanup(exception, this::stopServerIfDirectMode);
    exception = runCleanup(exception, this::stopBrokerIfDirectMode);
    exception = runCleanup(exception, this::stopControllerIfDirectMode);
    exception = runCleanup(exception, this::stopKafkaIfDirectMode);
    exception = runCleanup(exception, this::stopZkIfDirectMode);
    exception = runCleanup(exception, this::deleteClassTempDir);
    if (exception != null) {
      throw exception;
    }
  }

  @Test
  public void testSegmentStatusChecker()
      throws Exception {
    String emptyTable = "emptyTable";
    String disabledTable = "disabledTable";
    String tableWithOfflineSegment = "tableWithOfflineSegment";
    String upsertTable = "upsertTable";

    try {
      Schema schema = createSchema();
      _currentTable = emptyTable;
      schema.setSchemaName(getTableName());
      addSchema(schema);
      addTableConfig(createOfflineTableConfig());

      _currentTable = disabledTable;
      schema.setSchemaName(getTableName());
      addSchema(schema);
      addTableConfig(createOfflineTableConfig());
      _helixAdmin.enableResource(getHelixClusterName(), TableNameBuilder.OFFLINE.tableNameWithType(getTableName()),
          false);

      _currentTable = upsertTable;
      _schemaFileName = UpsertTableIntegrationTest.UPSERT_SCHEMA_FILE_NAME;
      setupUpsertTable();
      _schemaFileName = DEFAULT_SCHEMA_FILE_NAME;

      _currentTable = tableWithOfflineSegment;
      schema.setSchemaName(getTableName());
      addSchema(schema);
      addTableConfig(createOfflineTableConfig());
      uploadSegments(getTableName(), _classTarDir);
      // Turn one replica of a segment OFFLINE
      HelixHelper.updateIdealState(_helixManager, TableNameBuilder.OFFLINE.tableNameWithType(getTableName()),
          idealState -> {
            assertNotNull(idealState);
            Map<String, String> instanceStateMap = idealState.getRecord().getMapFields().values().iterator().next();
            instanceStateMap.entrySet().iterator().next().setValue(SegmentStateModel.OFFLINE);
            return idealState;
          }, RetryPolicies.fixedDelayRetryPolicy(2, 10));

      _currentTable = DEFAULT_TABLE_NAME;

      int numTables = 6;
      TestUtils.waitForCondition(aVoid -> {
        if (!checkGlobalGaugeValue(ControllerGauge.PERIODIC_TASK_NUM_TABLES_PROCESSED, "SegmentStatusChecker",
            numTables)) {
          return false;
        }
        if (!checkSegmentStatusCheckerMetrics(TableNameBuilder.OFFLINE.tableNameWithType(scopedTableName(emptyTable)),
            null, null, NUM_REPLICAS, 100, 0, 100)) {
          return false;
        }
        if (!checkSegmentStatusCheckerMetrics(
            TableNameBuilder.OFFLINE.tableNameWithType(scopedTableName(disabledTable)), null, null, 0, 0, 0, 0)) {
          return false;
        }

        String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
        IdealState idealState = _helixResourceManager.getTableIdealState(tableNameWithType);
        ExternalView externalView = _helixResourceManager.getTableExternalView(tableNameWithType);
        if (!checkSegmentStatusCheckerMetrics(tableNameWithType, idealState, externalView, NUM_REPLICAS, 100, 0, 100)) {
          return false;
        }
        tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(scopedTableName(tableWithOfflineSegment));
        idealState = _helixResourceManager.getTableIdealState(tableNameWithType);
        externalView = _helixResourceManager.getTableExternalView(tableNameWithType);
        //noinspection PointlessArithmeticExpression
        if (!checkSegmentStatusCheckerMetrics(tableNameWithType, idealState, externalView, NUM_REPLICAS - 1,
            100 * (NUM_REPLICAS - 1) / NUM_REPLICAS, 0, 100)) {
          return false;
        }
        tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
        idealState = _helixResourceManager.getTableIdealState(tableNameWithType);
        externalView = _helixResourceManager.getTableExternalView(tableNameWithType);
        if (!checkSegmentStatusCheckerMetrics(tableNameWithType, idealState, externalView, NUM_REPLICAS, 100, 0, 100)) {
          return false;
        }
        return checkGlobalGaugeValue(ControllerGauge.OFFLINE_TABLE_COUNT, 4) && checkGlobalGaugeValue(
            ControllerGauge.REALTIME_TABLE_COUNT, 2) && checkGlobalGaugeValue(ControllerGauge.DISABLED_TABLE_COUNT, 1)
            && checkGlobalGaugeValue(ControllerGauge.UPSERT_TABLE_COUNT, 1);
      }, 600_000, "Timed out waiting for SegmentStatusChecker");
    } finally {
      _schemaFileName = DEFAULT_SCHEMA_FILE_NAME;
      _currentTable = DEFAULT_TABLE_NAME;
      cleanOfflineTableAndSchema(emptyTable);
      cleanOfflineTableAndSchema(disabledTable);
      cleanOfflineTableAndSchema(tableWithOfflineSegment);
      cleanRealtimeTableAndSchema(upsertTable);
    }
  }

  private void setupUpsertTable()
      throws IOException {
    Schema upsertSchema = createSchema();
    upsertSchema.setSchemaName(getTableName());
    upsertSchema.getDateTimeFieldSpecs().get(0).setName(UpsertTableIntegrationTest.TIME_COL_NAME);
    addSchema(upsertSchema);
    TableConfig tableConfig =
        createCSVUpsertTableConfig(getTableName(), getKafkaTopic(), getNumKafkaPartitions(), new HashMap<>(),
            new UpsertConfig(UpsertConfig.Mode.FULL), UpsertTableIntegrationTest.PRIMARY_KEY_COL);
    tableConfig.getValidationConfig().setTimeColumnName(UpsertTableIntegrationTest.TIME_COL_NAME);
    addTableConfig(tableConfig);
  }

  private boolean checkGlobalGaugeValue(ControllerGauge gauge, long expectedValue) {
    long actualValue = MetricValueUtils.getGlobalGaugeValue(ControllerMetrics.get(), gauge);
    return isSharedRichClusterEnabled() ? actualValue >= expectedValue : actualValue == expectedValue;
  }

  private boolean checkGlobalGaugeValue(ControllerGauge gauge, String key, long expectedValue) {
    long actualValue = MetricValueUtils.getGlobalGaugeValue(ControllerMetrics.get(), key, gauge);
    return isSharedRichClusterEnabled() ? actualValue >= expectedValue : actualValue == expectedValue;
  }

  private boolean checkTableGaugeValue(ControllerGauge gauge, String tableNameWithType, long expectedValue) {
    return MetricValueUtils.getTableGaugeValue(ControllerMetrics.get(), tableNameWithType, gauge) == expectedValue;
  }

  private boolean checkSegmentStatusCheckerMetrics(String tableNameWithType, @Nullable IdealState idealState,
      @Nullable ExternalView externalView,
      long expectedNumReplicas, long expectedPercentReplicas, long expectedSegmentsInErrorState,
      long expectedPercentSegmentsAvailable) {
    if (idealState != null) {
      if (!checkTableGaugeValue(ControllerGauge.IDEALSTATE_ZNODE_SIZE, tableNameWithType,
          idealState.toString().length())) {
        return false;
      }
      if (!checkTableGaugeValue(ControllerGauge.IDEALSTATE_ZNODE_BYTE_SIZE, tableNameWithType,
          idealState.serialize(RECORD_SERIALIZER).length)) {
        return false;
      }
      if (!checkTableGaugeValue(ControllerGauge.SEGMENT_COUNT, tableNameWithType,
          idealState.getPartitionSet().size())) {
        return false;
      }

      ZkHelixPropertyStore<ZNRecord> propertyStore = _helixResourceManager.getPropertyStore();
      String segmentsPath = ZKMetadataProvider.constructPropertyStorePathForResource(tableNameWithType);
      List<String> segmentNames = propertyStore.getChildNames(segmentsPath, AccessOption.PERSISTENT);
      long expectedBytesSize = 0;
      if (segmentNames != null) {
        for (String segmentName : segmentNames) {
          expectedBytesSize += segmentName.getBytes().length;
        }
      }

      // Check if the metric matches the calculated byte size
      long bytesSize = MetricValueUtils.getTableGaugeValue(ControllerMetrics.get(), tableNameWithType,
          ControllerGauge.PROPERTYSTORE_SEGMENT_CHILDREN_BYTE_SIZE);
      if (bytesSize != expectedBytesSize) {
        return false;
      }
    }

    if (externalView != null) {
      if (!checkTableGaugeValue(ControllerGauge.EXTERNALVIEW_ZNODE_SIZE, tableNameWithType,
          externalView.toString().length())) {
        return false;
      }
      if (!checkTableGaugeValue(ControllerGauge.EXTERNALVIEW_ZNODE_BYTE_SIZE, tableNameWithType,
          externalView.serialize(RECORD_SERIALIZER).length)) {
        return false;
      }
    }

    return checkTableGaugeValue(ControllerGauge.NUMBER_OF_REPLICAS, tableNameWithType, expectedNumReplicas)
        && checkTableGaugeValue(ControllerGauge.PERCENT_OF_REPLICAS, tableNameWithType, expectedPercentReplicas)
        && checkTableGaugeValue(ControllerGauge.SEGMENTS_IN_ERROR_STATE, tableNameWithType,
        expectedSegmentsInErrorState) && checkTableGaugeValue(ControllerGauge.PERCENT_SEGMENTS_AVAILABLE,
        tableNameWithType, expectedPercentSegmentsAvailable);
  }

  @Test
  public void testSegmentRelocator()
      throws Exception {
    // Add relocation tenant config
    TableConfig realtimeTableConfig = getRealtimeTableConfig();
    realtimeTableConfig.setTenantConfig(new TenantConfig(getTenantName(), getTenantName(),
        new TagOverrideConfig(TagNameUtils.getRealtimeTagForTenant(getTenantName()),
            TagNameUtils.getOfflineTagForTenant(getTenantName()))));
    updateTableConfig(realtimeTableConfig);

    TestUtils.waitForCondition(aVoid -> {
      // Check servers for ONLINE segment and CONSUMING segments are disjoint sets
      Set<String> consumingServers = new HashSet<>();
      Set<String> completedServers = new HashSet<>();
      IdealState idealState =
          _helixResourceManager.getTableIdealState(TableNameBuilder.REALTIME.tableNameWithType(getTableName()));
      assertNotNull(idealState);
      for (Map<String, String> instanceStateMap : idealState.getRecord().getMapFields().values()) {
        for (Map.Entry<String, String> entry : instanceStateMap.entrySet()) {
          String state = entry.getValue();
          if (state.equals(SegmentStateModel.CONSUMING)) {
            consumingServers.add(entry.getKey());
          } else if (state.equals(SegmentStateModel.ONLINE)) {
            completedServers.add(entry.getKey());
          }
        }
      }
      return Collections.disjoint(consumingServers, completedServers);
    }, 600_000, "Timed out waiting for SegmentRelocator");
  }

  @Test
  public void testBrokerResourceValidationManager() {
    // Add a new broker with the same tag
    String brokerId = "Broker_localhost_" + (isSharedRichClusterEnabled()
        ? _sharedResourceSuffix.hashCode() & 0x7fffffff : 1234);
    InstanceConfig instanceConfig = InstanceConfig.toInstanceConfig(brokerId);
    instanceConfig.addTag(TagNameUtils.getBrokerTagForTenant(getTenantName()));
    String helixClusterName = getHelixClusterName();
    _helixAdmin.addInstance(helixClusterName, instanceConfig);
    try {
      Set<String> brokersAfterAdd = _helixResourceManager.getAllInstancesForBrokerTenant(getTenantName());
      assertTrue(brokersAfterAdd.contains(brokerId));

      String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
      TestUtils.waitForCondition(aVoid -> {
        IdealState idealState = HelixHelper.getBrokerIdealStates(_helixAdmin, helixClusterName);
        assertNotNull(idealState);
        return idealState.getInstanceSet(tableNameWithType).equals(brokersAfterAdd);
      }, 600_000L, "Timeout when waiting for BrokerResourceValidationManager");

      // Drop the new added broker
      _helixAdmin.dropInstance(helixClusterName, instanceConfig);
      Set<String> brokersAfterDrop = _helixResourceManager.getAllInstancesForBrokerTenant(getTenantName());
      assertFalse(brokersAfterDrop.contains(brokerId));

      TestUtils.waitForCondition(input -> {
        IdealState idealState = HelixHelper.getBrokerIdealStates(_helixAdmin, helixClusterName);
        assertNotNull(idealState);
        return idealState.getInstanceSet(tableNameWithType).equals(brokersAfterDrop);
      }, 600_000L, "Timeout when waiting for BrokerResourceValidationManager");
    } finally {
      dropInstanceIfPresent(helixClusterName, instanceConfig);
    }
  }

  /**
   * Verifies that BrokerResourceValidationManager also repairs broker resource for a logical table
   * when a new broker is added (Issue #15751).
   */
  @Test
  public void testBrokerResourceValidationManagerRepairsLogicalTable()
      throws Exception {
    // Add logical table (same broker tenant as physical table)
    Schema logicalTableSchema = createSchema();
    logicalTableSchema.setSchemaName(getLogicalTableName());
    addSchema(logicalTableSchema);
    createLogicalTable();

    String helixClusterName = getHelixClusterName();
    String logicalTableName = getLogicalTableName();
    IdealState idealState = HelixHelper.getBrokerIdealStates(_helixAdmin, helixClusterName);
    assertNotNull(idealState);
    assertTrue(idealState.getPartitionSet().contains(logicalTableName), "Broker resource should have logical table");

    // Add a new broker so logical table partition is out of sync
    String brokerId = "Broker_localhost_" + (isSharedRichClusterEnabled()
        ? (_sharedResourceSuffix.hashCode() & 0x7fffffff) + 1 : 5678);
    InstanceConfig instanceConfig = InstanceConfig.toInstanceConfig(brokerId);
    instanceConfig.addTag(TagNameUtils.getBrokerTagForTenant(getTenantName()));
    _helixAdmin.addInstance(helixClusterName, instanceConfig);
    try {
      Set<String> brokersAfterAdd = _helixResourceManager.getAllInstancesForBrokerTenant(getTenantName());
      assertTrue(brokersAfterAdd.contains(brokerId));

      // Assert logical table partition does not yet contain the new broker (periodic task will repair it)
      idealState = HelixHelper.getBrokerIdealStates(_helixAdmin, helixClusterName);
      assertNotNull(idealState);
      assertFalse(idealState.getInstanceSet(logicalTableName).contains(brokerId),
          "Logical table partition should not yet include the new broker before periodic task runs");

      // Wait for BrokerResourceValidationManager to repair both physical and logical table partitions
      String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
      TestUtils.waitForCondition(aVoid -> {
        IdealState is = HelixHelper.getBrokerIdealStates(_helixAdmin, helixClusterName);
        if (is == null) {
          return false;
        }
        return is.getInstanceSet(tableNameWithType).equals(brokersAfterAdd)
            && is.getInstanceSet(logicalTableName).equals(brokersAfterAdd);
      }, 60_000L, "Timeout when waiting for BrokerResourceValidationManager to repair logical table partition");
    } finally {
      dropInstanceIfPresent(helixClusterName, instanceConfig);
      dropLogicalTableIfPresent(logicalTableName);
      deleteSchemaIfPresent(logicalTableName);
    }
  }

  @Test
  public void testOfflineSegmentIntervalChecker() {
    OfflineSegmentValidationManager offlineSegmentValidationManager =
        _controllerStarter.getOfflineSegmentValidationManager();
    ValidationMetrics validationMetrics = offlineSegmentValidationManager.getValidationMetrics();
    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());

    // Wait until OfflineSegmentIntervalChecker gets executed
    TestUtils.waitForCondition(aVoid -> {
      return checkValidationGaugeValue(validationMetrics, tableNameWithType, "SegmentCount", NUM_OFFLINE_AVRO_FILES)
          && checkValidationGaugeValue(validationMetrics, tableNameWithType, "missingSegmentCount", 0)
          && checkValidationGaugeValue(validationMetrics, tableNameWithType, "TotalDocumentCount", 79003);
    }, 600_000, "Timed out waiting for OfflineSegmentIntervalChecker");
  }

  private boolean checkValidationGaugeValue(ValidationMetrics validationMetrics, String tableNameWithType,
      String gaugeName, long expectedValue) {
    return validationMetrics.getValueOfGauge(ValidationMetrics.makeGaugeName(tableNameWithType, gaugeName))
        == expectedValue;
  }

  private String getTenantName() {
    return isSharedRichClusterEnabled() ? TENANT_NAME + "_" + _sharedResourceSuffix : TENANT_NAME;
  }

  private String scopedTableName(String tableName) {
    return isSharedRichClusterEnabled() ? tableName + "_" + _sharedResourceSuffix : tableName;
  }

  private File getClassTempDir() {
    return isSharedRichClusterEnabled()
        ? new File(FileUtils.getTempDirectory(), getClass().getSimpleName() + "-" + _sharedResourceSuffix)
        : _tempDir;
  }

  private void cleanHybridTableAndSchema(String tableName)
      throws Exception {
    cleanOfflineTable(tableName);
    cleanRealtimeTable(tableName);
    deleteSchemaIfPresent(scopedTableName(tableName));
  }

  private void cleanOfflineTableAndSchema(String tableName)
      throws Exception {
    cleanOfflineTable(tableName);
    deleteSchemaIfPresent(scopedTableName(tableName));
  }

  private void cleanRealtimeTableAndSchema(String tableName)
      throws Exception {
    cleanRealtimeTable(tableName);
    deleteSchemaIfPresent(scopedTableName(tableName));
  }

  private void cleanOfflineTable(String tableName)
      throws Exception {
    String scopedTableName = scopedTableName(tableName);
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(scopedTableName);
    if (_helixResourceManager != null && (_helixResourceManager.getTableConfig(offlineTableName) != null
        || _helixResourceManager.hasOfflineTable(scopedTableName))) {
      dropOfflineTable(scopedTableName);
      waitForEVToDisappear(offlineTableName);
    }
  }

  private void cleanRealtimeTable(String tableName)
      throws Exception {
    String scopedTableName = scopedTableName(tableName);
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(scopedTableName);
    if (_helixResourceManager != null && (_helixResourceManager.getTableConfig(realtimeTableName) != null
        || _helixResourceManager.hasRealtimeTable(scopedTableName))) {
      dropRealtimeTable(scopedTableName);
      waitForEVToDisappear(realtimeTableName);
    }
  }

  private void deleteSchemaIfPresent(String schemaName)
      throws Exception {
    if (_helixResourceManager != null && _helixResourceManager.getSchema(schemaName) != null) {
      deleteSchema(schemaName);
    }
  }

  private void dropLogicalTableIfPresent(String logicalTableName)
      throws Exception {
    if (_helixResourceManager != null && _helixResourceManager.getLogicalTableConfig(logicalTableName) != null) {
      dropLogicalTable(logicalTableName);
    }
  }

  private void dropInstanceIfPresent(String helixClusterName, InstanceConfig instanceConfig) {
    if (_helixAdmin.getInstancesInCluster(helixClusterName).contains(instanceConfig.getInstanceName())) {
      _helixAdmin.dropInstance(helixClusterName, instanceConfig);
    }
  }

  private void deleteKafkaTopicIfPresent() {
    if (_kafkaStarters != null && !_kafkaStarters.isEmpty() && isKafkaTopicPresent()) {
      deleteKafkaTopic(getKafkaTopic());
    }
  }

  private boolean isKafkaTopicPresent() {
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

  private void stopServerIfDirectMode() {
    if (!isSharedRichClusterEnabled() && !_serverStarters.isEmpty()) {
      stopServer();
    }
  }

  private void stopBrokerIfDirectMode() {
    if (!isSharedRichClusterEnabled() && !_brokerStarters.isEmpty()) {
      stopBroker();
    }
  }

  private void stopControllerIfDirectMode() {
    if (!isSharedRichClusterEnabled() && _controllerStarter != null) {
      stopController();
    }
  }

  private void stopKafkaIfDirectMode() {
    if (!isSharedRichClusterEnabled() && _kafkaStarters != null && !_kafkaStarters.isEmpty()) {
      stopKafka();
    }
  }

  private void stopZkIfDirectMode() {
    if (!isSharedRichClusterEnabled()) {
      stopZk();
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
