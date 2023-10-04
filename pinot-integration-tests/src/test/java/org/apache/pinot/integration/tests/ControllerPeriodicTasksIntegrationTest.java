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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.MetricValueUtils;
import org.apache.pinot.common.metrics.ValidationMetrics;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.ControllerConf.ControllerPeriodicTasksConf;
import org.apache.pinot.controller.validation.OfflineSegmentIntervalChecker;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TagOverrideConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
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
public class ControllerPeriodicTasksIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final int PERIODIC_TASK_INITIAL_DELAY_SECONDS = 30;
  private static final int PERIODIC_TASK_FREQUENCY_SECONDS = 5;
  private static final String PERIODIC_TASK_FREQUENCY = "5s";
  private static final String PERIODIC_TASK_WAIT_FOR_PUSH_TIME_PERIOD = "5s";

  private static final int NUM_REPLICAS = 2;
  private static final String TENANT_NAME = "TestTenant";
  private static final int NUM_BROKERS = 1;
  private static final int NUM_OFFLINE_SERVERS = 2;
  private static final int NUM_REALTIME_SERVERS = 2;
  private static final int NUM_OFFLINE_AVRO_FILES = 8;
  private static final int NUM_REALTIME_AVRO_FILES = 6;

  private String _currentTable = DEFAULT_TABLE_NAME;

  @Override
  protected String getTableName() {
    return _currentTable;
  }

  @Override
  protected int getNumReplicas() {
    return NUM_REPLICAS;
  }

  @Override
  protected String getBrokerTenant() {
    return TENANT_NAME;
  }

  @Override
  protected String getServerTenant() {
    return TENANT_NAME;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    startZk();
    startKafka();

    Map<String, Object> properties = getDefaultControllerConfiguration();
    properties.put(ControllerConf.CLUSTER_TENANT_ISOLATION_ENABLE, false);
    properties
        .put(ControllerPeriodicTasksConf.STATUS_CHECKER_INITIAL_DELAY_IN_SECONDS, PERIODIC_TASK_INITIAL_DELAY_SECONDS);
    properties.put(ControllerPeriodicTasksConf.DEPRECATED_STATUS_CHECKER_FREQUENCY_IN_SECONDS,
        PERIODIC_TASK_FREQUENCY_SECONDS);
    properties.put(ControllerPeriodicTasksConf.DEPRECATED_REALTIME_SEGMENT_RELOCATION_INITIAL_DELAY_IN_SECONDS,
        PERIODIC_TASK_INITIAL_DELAY_SECONDS);
    properties
        .put(ControllerPeriodicTasksConf.DEPRECATED_REALTIME_SEGMENT_RELOCATOR_FREQUENCY, PERIODIC_TASK_FREQUENCY);
    properties.put(ControllerPeriodicTasksConf.BROKER_RESOURCE_VALIDATION_INITIAL_DELAY_IN_SECONDS,
        PERIODIC_TASK_INITIAL_DELAY_SECONDS);
    properties.put(ControllerPeriodicTasksConf.DEPRECATED_BROKER_RESOURCE_VALIDATION_FREQUENCY_IN_SECONDS,
        PERIODIC_TASK_FREQUENCY_SECONDS);
    properties.put(ControllerPeriodicTasksConf.OFFLINE_SEGMENT_INTERVAL_CHECKER_INITIAL_DELAY_IN_SECONDS,
        PERIODIC_TASK_INITIAL_DELAY_SECONDS);
    properties.put(ControllerPeriodicTasksConf.DEPRECATED_OFFLINE_SEGMENT_INTERVAL_CHECKER_FREQUENCY_IN_SECONDS,
        PERIODIC_TASK_FREQUENCY_SECONDS);
    properties.put(ControllerPeriodicTasksConf.STATUS_CHECKER_WAIT_FOR_PUSH_TIME_PERIOD,
        PERIODIC_TASK_WAIT_FOR_PUSH_TIME_PERIOD);

    startController(properties);
    startBrokers(NUM_BROKERS);
    startServers(NUM_OFFLINE_SERVERS + NUM_REALTIME_SERVERS);

    // Create tenants
    createBrokerTenant(TENANT_NAME, NUM_BROKERS);
    createServerTenant(TENANT_NAME, NUM_OFFLINE_SERVERS, NUM_REALTIME_SERVERS);

    // Unpack the Avro files
    int numAvroFiles = unpackAvroData(_tempDir).size();
    // Avro files has to be ordered as time series data
    List<File> avroFiles = new ArrayList<>(numAvroFiles);
    for (int i = 1; i <= numAvroFiles; i++) {
      avroFiles.add(new File(_tempDir, "On_Time_On_Time_Performance_2014_" + i + ".avro"));
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
    ClusterIntegrationTestUtils
        .buildSegmentsFromAvro(offlineAvroFiles, offlineTableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), _tarDir);

    // Push data into Kafka
    pushAvroIntoKafka(realtimeAvroFiles);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    String tableName = getTableName();
    dropOfflineTable(tableName);
    dropRealtimeTable(tableName);

    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  @Test
  public void testSegmentStatusChecker()
      throws Exception {
    String emptyTable = "emptyTable";
    String disabledTable = "disabledTable";
    String tableWithOfflineSegment = "tableWithOfflineSegment";

    Schema schema = createSchema();
    _currentTable = emptyTable;
    schema.setSchemaName(_currentTable);
    addSchema(schema);
    addTableConfig(createOfflineTableConfig());

    _currentTable = disabledTable;
    schema.setSchemaName(_currentTable);
    addSchema(schema);
    addTableConfig(createOfflineTableConfig());
    _helixAdmin.enableResource(getHelixClusterName(), TableNameBuilder.OFFLINE.tableNameWithType(disabledTable), false);

    _currentTable = tableWithOfflineSegment;
    schema.setSchemaName(_currentTable);
    addSchema(schema);
    addTableConfig(createOfflineTableConfig());
    uploadSegments(_currentTable, _tarDir);
    // Turn one replica of a segment OFFLINE
    HelixHelper.updateIdealState(_helixManager, TableNameBuilder.OFFLINE.tableNameWithType(tableWithOfflineSegment),
        idealState -> {
          assertNotNull(idealState);
          Map<String, String> instanceStateMap = idealState.getRecord().getMapFields().values().iterator().next();
          instanceStateMap.entrySet().iterator().next().setValue(SegmentStateModel.OFFLINE);
          return idealState;
        }, RetryPolicies.fixedDelayRetryPolicy(2, 10));

    _currentTable = DEFAULT_TABLE_NAME;

    int numTables = 5;
    ControllerMetrics controllerMetrics = _controllerStarter.getControllerMetrics();
    TestUtils.waitForCondition(aVoid -> {
      if (MetricValueUtils.getGlobalGaugeValue(controllerMetrics, "SegmentStatusChecker",
          ControllerGauge.PERIODIC_TASK_NUM_TABLES_PROCESSED) != numTables) {
        return false;
      }
      if (!checkSegmentStatusCheckerMetrics(controllerMetrics, TableNameBuilder.OFFLINE.tableNameWithType(emptyTable),
          null, NUM_REPLICAS, 100, 0, 100)) {
        return false;
      }
      if (!checkSegmentStatusCheckerMetrics(controllerMetrics,
          TableNameBuilder.OFFLINE.tableNameWithType(disabledTable), null, Long.MIN_VALUE, Long.MIN_VALUE,
          Long.MIN_VALUE, Long.MIN_VALUE)) {
        return false;
      }
      String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
      IdealState idealState = _helixResourceManager.getTableIdealState(tableNameWithType);
      if (!checkSegmentStatusCheckerMetrics(controllerMetrics, tableNameWithType, idealState, NUM_REPLICAS, 100, 0,
          100)) {
        return false;
      }
      tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(tableWithOfflineSegment);
      idealState = _helixResourceManager.getTableIdealState(tableNameWithType);
      //noinspection PointlessArithmeticExpression
      if (!checkSegmentStatusCheckerMetrics(controllerMetrics, tableNameWithType, idealState, NUM_REPLICAS - 1,
          100 * (NUM_REPLICAS - 1) / NUM_REPLICAS, 0, 100)) {
        return false;
      }
      tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
      idealState = _helixResourceManager.getTableIdealState(tableNameWithType);
      if (!checkSegmentStatusCheckerMetrics(controllerMetrics, tableNameWithType, idealState, NUM_REPLICAS, 100, 0,
          100)) {
        return false;
      }
      return MetricValueUtils.getGlobalGaugeValue(controllerMetrics, ControllerGauge.OFFLINE_TABLE_COUNT) == 4
          && MetricValueUtils.getGlobalGaugeValue(controllerMetrics, ControllerGauge.REALTIME_TABLE_COUNT) == 1
          && MetricValueUtils.getGlobalGaugeValue(controllerMetrics, ControllerGauge.DISABLED_TABLE_COUNT) == 1;
    }, 60_000, "Timed out waiting for SegmentStatusChecker");

    dropOfflineTable(emptyTable);
    dropOfflineTable(disabledTable);
    dropOfflineTable(tableWithOfflineSegment);
    deleteSchema(emptyTable);
    deleteSchema(disabledTable);
    deleteSchema(tableWithOfflineSegment);
  }

  private boolean checkSegmentStatusCheckerMetrics(ControllerMetrics controllerMetrics, String tableNameWithType,
      IdealState idealState, long expectedNumReplicas, long expectedPercentReplicas, long expectedSegmentsInErrorState,
      long expectedPercentSegmentsAvailable) {
    if (idealState != null) {
      if (MetricValueUtils.getTableGaugeValue(controllerMetrics, tableNameWithType,
          ControllerGauge.IDEALSTATE_ZNODE_SIZE) != idealState.toString().length()) {
        return false;
      }
      if (MetricValueUtils.getTableGaugeValue(controllerMetrics, tableNameWithType, ControllerGauge.SEGMENT_COUNT)
          != idealState.getPartitionSet().size()) {
        return false;
      }
    }
    return MetricValueUtils.getTableGaugeValue(controllerMetrics, tableNameWithType,
        ControllerGauge.NUMBER_OF_REPLICAS) == expectedNumReplicas
        && MetricValueUtils.getTableGaugeValue(controllerMetrics, tableNameWithType,
        ControllerGauge.PERCENT_OF_REPLICAS) == expectedPercentReplicas
        && MetricValueUtils.getTableGaugeValue(controllerMetrics, tableNameWithType,
        ControllerGauge.SEGMENTS_IN_ERROR_STATE) == expectedSegmentsInErrorState
        && MetricValueUtils.getTableGaugeValue(controllerMetrics, tableNameWithType,
        ControllerGauge.PERCENT_SEGMENTS_AVAILABLE) == expectedPercentSegmentsAvailable;
  }

  @Test
  public void testRealtimeSegmentRelocator()
      throws Exception {
    // Add relocation tenant config
    TableConfig realtimeTableConfig = getRealtimeTableConfig();
    realtimeTableConfig.setTenantConfig(new TenantConfig(TENANT_NAME, TENANT_NAME,
        new TagOverrideConfig(TagNameUtils.getRealtimeTagForTenant(TENANT_NAME),
            TagNameUtils.getOfflineTagForTenant(TENANT_NAME))));
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
    }, 60_000, "Timed out waiting for RealtimeSegmentRelocation");
  }

  @Test
  public void testBrokerResourceValidationManager() {
    // Add a new broker with the same tag
    String brokerId = "Broker_localhost_1234";
    InstanceConfig instanceConfig = InstanceConfig.toInstanceConfig(brokerId);
    instanceConfig.addTag(TagNameUtils.getBrokerTagForTenant(TENANT_NAME));
    String helixClusterName = getHelixClusterName();
    _helixAdmin.addInstance(helixClusterName, instanceConfig);
    Set<String> brokersAfterAdd = _helixResourceManager.getAllInstancesForBrokerTenant(TENANT_NAME);
    assertTrue(brokersAfterAdd.contains(brokerId));

    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    TestUtils.waitForCondition(aVoid -> {
      IdealState idealState = HelixHelper.getBrokerIdealStates(_helixAdmin, helixClusterName);
      assertNotNull(idealState);
      return idealState.getInstanceSet(tableNameWithType).equals(brokersAfterAdd);
    }, 60_000L, "Timeout when waiting for BrokerResourceValidationManager");

    // Drop the new added broker
    _helixAdmin.dropInstance(helixClusterName, instanceConfig);
    Set<String> brokersAfterDrop = _helixResourceManager.getAllInstancesForBrokerTenant(TENANT_NAME);
    assertFalse(brokersAfterDrop.contains(brokerId));

    TestUtils.waitForCondition(input -> {
      IdealState idealState = HelixHelper.getBrokerIdealStates(_helixAdmin, helixClusterName);
      assertNotNull(idealState);
      return idealState.getInstanceSet(tableNameWithType).equals(brokersAfterDrop);
    }, 60_000L, "Timeout when waiting for BrokerResourceValidationManager");
  }

  @Test
  public void testOfflineSegmentIntervalChecker() {
    OfflineSegmentIntervalChecker offlineSegmentIntervalChecker = _controllerStarter.getOfflineSegmentIntervalChecker();
    ValidationMetrics validationMetrics = offlineSegmentIntervalChecker.getValidationMetrics();
    String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());

    // Wait until OfflineSegmentIntervalChecker gets executed
    TestUtils.waitForCondition(aVoid -> {
      long numSegments =
          validationMetrics.getValueOfGauge(ValidationMetrics.makeGaugeName(tableNameWithType, "SegmentCount"));
      long numMissingSegments =
          validationMetrics.getValueOfGauge(ValidationMetrics.makeGaugeName(tableNameWithType, "missingSegmentCount"));
      long numTotalDocs =
          validationMetrics.getValueOfGauge(ValidationMetrics.makeGaugeName(tableNameWithType, "TotalDocumentCount"));
      return numSegments == NUM_OFFLINE_AVRO_FILES && numMissingSegments == 0 && numTotalDocs == 79003;
    }, 60_000, "Timed out waiting for OfflineSegmentIntervalChecker");
  }

  // TODO: tests for other ControllerPeriodicTasks (RetentionManager, RealtimeSegmentValidationManager)
}
