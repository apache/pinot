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
package org.apache.pinot.integration.tests.controller.periodic.tasks;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.KafkaStarterUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.common.utils.retry.RetryPolicies;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.integration.tests.BaseClusterIntegrationTestSet;
import org.apache.pinot.integration.tests.ClusterIntegrationTestUtils;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration test to check {@link org.apache.pinot.controller.helix.SegmentStatusChecker} is
 * running and verify the metrics emitted
 */
public class SegmentStatusCheckerIntegrationTest extends BaseClusterIntegrationTestSet {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentStatusCheckerIntegrationTest.class);

  private String emptyTable = "table1_OFFLINE";
  private String disabledOfflineTable = "table2_OFFLINE";
  private String basicOfflineTable = "table3_OFFLINE";
  private String errorOfflineTable = "table4_OFFLINE";
  private String realtimeTableErrorState = "table5_REALTIME";
  private String _currentTableName;
  private static final int NUM_TABLES = 5;

  private static final int SEGMENT_STATUS_CHECKER_INITIAL_DELAY_SECONDS = 60;
  private static final int SEGMENT_STATUS_CHECKER_FREQ_SECONDS = 5;

  @BeforeClass
  public void setUp() throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    startZk();

    // Set initial delay of 60 seconds for the segment status checker, to allow time for tables setup.
    // Run at 5 seconds freq in order to keep it running, in case first run happens before table setup
    ControllerConf controllerConf = getDefaultControllerConfiguration();
    controllerConf.setStatusCheckerInitialDelayInSeconds(SEGMENT_STATUS_CHECKER_INITIAL_DELAY_SECONDS);
    controllerConf.setStatusCheckerFrequencyInSeconds(SEGMENT_STATUS_CHECKER_FREQ_SECONDS);

    startController(controllerConf);
    startBroker();
    startServers(3);

    // empty table
    setupOfflineTable(emptyTable);

    // table with disabled ideal state
    setupOfflineTable(disabledOfflineTable);
    _helixAdmin.enableResource(_clusterName, disabledOfflineTable, false);

    // happy case table
    setupOfflineTableAndSegments(basicOfflineTable);

    // some segments offline
    setupOfflineTableAndSegments(errorOfflineTable);
    HelixHelper.updateIdealState(_helixManager, errorOfflineTable, new Function<IdealState, IdealState>() {
      @Nullable
      @Override
      public IdealState apply(@Nullable IdealState input) {
        List<String> segmentNames = Lists.newArrayList(input.getPartitionSet());
        Collections.sort(segmentNames);

        Map<String, String> instanceStateMap1 = input.getInstanceStateMap(segmentNames.get(0));
        for (String instance : instanceStateMap1.keySet()) {
          instanceStateMap1.put(instance, "OFFLINE");
          break;
        }
        return input;
      }
    }, RetryPolicies.fixedDelayRetryPolicy(2, 10));

    // realtime table with segments in error state
    setupRealtimeTable(realtimeTableErrorState);
  }

  private void setupOfflineTable(String table) throws Exception {
    _realtimeTableConfig = null;
    addOfflineTable(table);
    completeTableConfiguration();
  }

  private void setupOfflineTableAndSegments(String table) throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    setTableName(table);
    _realtimeTableConfig = null;
    addOfflineTable(table);
    completeTableConfiguration();
    List<File> avroFiles = unpackAvroData(_tempDir);
    ExecutorService executor = Executors.newCachedThreadPool();
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, 0, _segmentDir, _tarDir, table, false, null, null,
        null, executor);
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.MINUTES);
    uploadSegments(_tarDir);

    waitForAllDocsLoaded(600_000L);
  }

  private void setupRealtimeTable(String table) throws Exception {
    _offlineTableConfig = null;
    File schemaFile = getSchemaFile();
    Schema schema = Schema.fromFile(schemaFile);
    String schemaName = schema.getSchemaName();
    addSchema(schemaFile, schemaName);

    String timeColumnName = schema.getTimeColumnName();
    Assert.assertNotNull(timeColumnName);
    TimeUnit outgoingTimeUnit = schema.getOutgoingTimeUnit();
    Assert.assertNotNull(outgoingTimeUnit);
    String timeType = outgoingTimeUnit.toString();

    addRealtimeTable(table, useLlc(), KafkaStarterUtils.DEFAULT_KAFKA_BROKER, KafkaStarterUtils.DEFAULT_ZK_STR,
        getKafkaTopic(), getRealtimeSegmentFlushSize(), null, timeColumnName, timeType, schemaName, null, null,
        getLoadMode(), getSortedColumn(), getInvertedIndexColumns(), getBloomFilterIndexColumns(), getRawIndexColumns(),
        getTaskConfig(), getStreamConsumerFactoryClassName());
    completeTableConfiguration();
  }

  @Override
  public String getTableName() {
    return _currentTableName;
  }

  private void setTableName(String tableName) {
    _currentTableName = tableName;
  }
  /**
   * After 1 run of SegmentStatusChecker the controllerMetrics will be set for each table
   * Validate that we are seeing the expected numbers
   */
  @Test
  public void testSegmentStatusChecker() {
    ControllerMetrics controllerMetrics = _controllerStarter.getControllerMetrics();

    long millisToWait = TimeUnit.MILLISECONDS.convert(3, TimeUnit.MINUTES);
    while (controllerMetrics.getValueOfGlobalGauge(ControllerGauge.PERIODIC_TASK_NUM_TABLES_PROCESSED,
        "SegmentStatusChecker") < NUM_TABLES && millisToWait > 0) {
      try {
        Thread.sleep(1000);
        millisToWait -= 1000;
      } catch (InterruptedException e) {
        LOGGER.info("Interrupted while waiting for SegmentStatusChecker");
      }
    }

    Assert.assertEquals(controllerMetrics.getValueOfGlobalGauge(ControllerGauge.PERIODIC_TASK_NUM_TABLES_PROCESSED,
        "SegmentStatusChecker"), NUM_TABLES);

    // empty table - table1_OFFLINE
    // num replicas set from ideal state
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(emptyTable, ControllerGauge.NUMBER_OF_REPLICAS), 3);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(emptyTable, ControllerGauge.PERCENT_OF_REPLICAS), 100);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(emptyTable, ControllerGauge.PERCENT_SEGMENTS_AVAILABLE),
        100);

    // disabled table - table2_OFFLINE
    // reset to defaults
    Assert.assertEquals(
        controllerMetrics.getValueOfTableGauge(disabledOfflineTable, ControllerGauge.NUMBER_OF_REPLICAS),
        Long.MIN_VALUE);
    Assert.assertEquals(
        controllerMetrics.getValueOfTableGauge(disabledOfflineTable, ControllerGauge.PERCENT_OF_REPLICAS),
        Long.MIN_VALUE);
    Assert.assertEquals(
        controllerMetrics.getValueOfTableGauge(disabledOfflineTable, ControllerGauge.SEGMENTS_IN_ERROR_STATE),
        Long.MIN_VALUE);
    Assert.assertEquals(
        controllerMetrics.getValueOfTableGauge(disabledOfflineTable, ControllerGauge.PERCENT_SEGMENTS_AVAILABLE),
        Long.MIN_VALUE);

    // happy path table - table3_OFFLINE
    IdealState idealState = _helixResourceManager.getTableIdealState(basicOfflineTable);
    Assert.assertEquals(
        controllerMetrics.getValueOfTableGauge(basicOfflineTable, ControllerGauge.IDEALSTATE_ZNODE_SIZE),
        idealState.toString().length());
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(basicOfflineTable, ControllerGauge.SEGMENT_COUNT),
        (long) (idealState.getPartitionSet().size()));
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(basicOfflineTable, ControllerGauge.NUMBER_OF_REPLICAS),
        3);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(basicOfflineTable, ControllerGauge.PERCENT_OF_REPLICAS),
        100);
    Assert.assertEquals(
        controllerMetrics.getValueOfTableGauge(basicOfflineTable, ControllerGauge.SEGMENTS_IN_ERROR_STATE), 0);
    Assert.assertEquals(
        controllerMetrics.getValueOfTableGauge(basicOfflineTable, ControllerGauge.PERCENT_SEGMENTS_AVAILABLE), 100);

    // offline segments - table4_OFFLINE
    // 2 replicas available out of 3, percent 66
    idealState = _helixResourceManager.getTableIdealState(errorOfflineTable);
    Assert.assertEquals(
        controllerMetrics.getValueOfTableGauge(errorOfflineTable, ControllerGauge.IDEALSTATE_ZNODE_SIZE),
        idealState.toString().length());
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(errorOfflineTable, ControllerGauge.SEGMENT_COUNT),
        (long) (idealState.getPartitionSet().size()));
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(errorOfflineTable, ControllerGauge.NUMBER_OF_REPLICAS),
        2);
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(errorOfflineTable, ControllerGauge.PERCENT_OF_REPLICAS),
        66);
    Assert.assertEquals(
        controllerMetrics.getValueOfTableGauge(errorOfflineTable, ControllerGauge.SEGMENTS_IN_ERROR_STATE), 0);
    Assert.assertEquals(
        controllerMetrics.getValueOfTableGauge(errorOfflineTable, ControllerGauge.PERCENT_SEGMENTS_AVAILABLE), 100);

    // error segments - table5_REALTIME
    // no replicas available, all segments in error state
    idealState = _helixResourceManager.getTableIdealState(realtimeTableErrorState);
    Assert.assertEquals(
        controllerMetrics.getValueOfTableGauge(realtimeTableErrorState, ControllerGauge.IDEALSTATE_ZNODE_SIZE),
        idealState.toString().length());
    Assert.assertEquals(controllerMetrics.getValueOfTableGauge(realtimeTableErrorState, ControllerGauge.SEGMENT_COUNT),
        (long) (idealState.getPartitionSet().size()));
    Assert.assertEquals(
        controllerMetrics.getValueOfTableGauge(realtimeTableErrorState, ControllerGauge.NUMBER_OF_REPLICAS), 0);
    Assert.assertEquals(
        controllerMetrics.getValueOfTableGauge(realtimeTableErrorState, ControllerGauge.PERCENT_OF_REPLICAS), 0);
    Assert.assertTrue(
        controllerMetrics.getValueOfTableGauge(realtimeTableErrorState, ControllerGauge.SEGMENTS_IN_ERROR_STATE) > 0);
    Assert.assertEquals(
        controllerMetrics.getValueOfTableGauge(realtimeTableErrorState, ControllerGauge.PERCENT_SEGMENTS_AVAILABLE), 0);

    // Total metrics
    Assert.assertEquals(controllerMetrics.getValueOfGlobalGauge(ControllerGauge.OFFLINE_TABLE_COUNT), 4);
    Assert.assertEquals(controllerMetrics.getValueOfGlobalGauge(ControllerGauge.REALTIME_TABLE_COUNT), 1);
    Assert.assertEquals(controllerMetrics.getValueOfGlobalGauge(ControllerGauge.DISABLED_TABLE_COUNT), 1);
  }

  @Override
  protected boolean isUsingNewConfigFormat() {
    return true;
  }

  @AfterClass
  public void tearDown() throws Exception {
    dropRealtimeTable(realtimeTableErrorState);
    dropOfflineTable(emptyTable);
    dropOfflineTable(disabledOfflineTable);
    dropOfflineTable(basicOfflineTable);
    dropOfflineTable(errorOfflineTable);

    stopServer();
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }
}
