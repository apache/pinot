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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.ExternalView;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.MetricValueUtils;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import org.apache.pinot.controller.validation.RealtimeSegmentValidationManager;
import org.apache.pinot.integration.tests.realtime.utils.FailureInjectingTableConfig;
import org.apache.pinot.integration.tests.realtime.utils.FailureInjectingTableDataManagerProvider;
import org.apache.pinot.integration.tests.realtime.utils.PauselessRealtimeTestUtils;
import org.apache.pinot.server.starter.helix.HelixInstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.ParallelSegmentConsumptionPolicy;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


/// Verifies the segment-level validation's repair of ERROR replicas on a pauseless dedup table. Without the override,
/// it leaves them in ERROR and counts their segments as unrecoverable: a pauseless server can consume the segments
/// after a failed one without its keys, so reloading the failed segment could serve wrong rows. With the override for
/// one run, it resets only the ERROR replicas, which reload from a peer or the deep store.
///
/// The consuming transition is made to fail on one server only, so every affected segment keeps one healthy replica
/// and one ERROR replica, and the healthy replica commits it.
public class PauselessDedupErrorReplicaRepairTest extends BaseClusterIntegrationTest {
  private static final int NUM_PARTITIONS = 2;
  private static final long VALIDATION_RERUN_INTERVAL_MS = 10_000L;
  private static final int NUM_VALIDATION_RUNS_WITHOUT_OVERRIDE = 3;

  private String _realtimeTableName;
  // Segment name to the servers holding it in ERROR state, once the healthy replicas have committed every such segment
  private Map<String, Set<String>> _errorReplicas;

  @Override
  protected String getAvroTarFileName() {
    return "dedupPauselessIngestionTestData.tar.gz";
  }

  @Override
  protected int getRealtimeSegmentFlushSize() {
    return 2;
  }

  @Override
  protected String getSchemaFileName() {
    return "dedupIngestionTestSchema.schema";
  }

  @Override
  protected long getCountStarResult() {
    // Two distinct records are expected with pk values of 0, 1.
    return 2;
  }

  @Override
  protected String getPartitionColumn() {
    return "id";
  }

  @Override
  protected int getNumReplicas() {
    return 2;
  }

  @Override
  protected IngestionConfig getIngestionConfig() {
    IngestionConfig ingestionConfig = new IngestionConfig();
    Map<String, String> streamConfigs = getStreamConfigs();
    streamConfigs.put(StreamConfigProperties.PAUSELESS_SEGMENT_DOWNLOAD_TIMEOUT_SECONDS, "10");
    StreamIngestionConfig streamIngestionConfig = new StreamIngestionConfig(List.of(streamConfigs));
    streamIngestionConfig.setPauselessConsumptionEnabled(true);
    streamIngestionConfig.setParallelSegmentConsumptionPolicy(ParallelSegmentConsumptionPolicy.ALLOW_DURING_BUILD_ONLY);
    streamIngestionConfig.setEnforceConsumptionInOrder(true);
    // Intentionally no disasterRecoveryMode: repairs stay disallowed for this dedup table unless a validation run
    // overrides it
    ingestionConfig.setStreamIngestionConfig(streamIngestionConfig);
    return ingestionConfig;
  }

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    properties.put(ControllerConf.ControllerPeriodicTasksConf.ENABLE_DEEP_STORE_RETRY_UPLOAD_LLC_SEGMENT, true);
    // Recovery is triggered explicitly by the test; keep the scheduled validator from racing it.
    properties.put(ControllerConf.ControllerPeriodicTasksConf.REALTIME_SEGMENT_VALIDATION_INITIAL_DELAY_IN_SECONDS,
        3600);
  }

  @Override
  protected PinotConfiguration getServerConf(int serverId) {
    PinotConfiguration serverConf = super.getServerConf(serverId);
    serverConf.setProperty("pinot.server.instance.segment.store.uri", "file:" + _controllerConfig.getDataDir());
    serverConf.setProperty("pinot.server.instance." + HelixInstanceDataManagerConfig.UPLOAD_SEGMENT_TO_DEEP_STORE,
        "true");
    serverConf.setProperty("pinot.server.instance." + CommonConstants.Server.TABLE_DATA_MANAGER_PROVIDER_CLASS,
        FailureInjectingTableDataManagerProvider.class.getName());
    if (serverId == 1) {
      // Fail the consuming transition of the first segment of each partition on this server only, so each affected
      // segment has one ERROR replica here and one healthy replica on the other server.
      serverConf.setProperty(
          "pinot.server.instance." + FailureInjectingTableDataManagerProvider.FAILURE_CONFIG_KEY + "."
              + getTableName(), new FailureInjectingTableConfig(false, true, NUM_PARTITIONS).toJson());
    }
    return serverConf;
  }

  @BeforeClass(alwaysRun = true)
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    startZk();
    startKafka();
    startController();
    startBroker();
    startServers(2);

    List<File> avroFiles = unpackAvroData(_tempDir);
    pushAvroIntoKafka(avroFiles);

    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createDedupTableConfig(avroFiles.get(0), getPartitionColumn(), NUM_PARTITIONS);
    assertNotNull(tableConfig.getDedupConfig());
    tableConfig.getValidationConfig().setRetentionTimeUnit("DAYS");
    tableConfig.getValidationConfig().setRetentionTimeValue("100000");
    tableConfig.getValidationConfig().setPeerSegmentDownloadScheme(CommonConstants.HTTP_PROTOCOL);
    addTableConfig(tableConfig);

    // Wait for the injected failures, and for the healthy replicas to commit every segment with an ERROR replica, so
    // the validation finds them committed rather than waiting for their commit
    _realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    TestUtils.waitForCondition(aVoid -> {
      Map<String, Set<String>> errorReplicas = getErrorReplicas();
      Map<String, Map<String, String>> idealStateMaps =
          _helixResourceManager.getTableIdealState(_realtimeTableName).getRecord().getMapFields();
      Set<Integer> partitionIds = new HashSet<>();
      for (String segmentName : errorReplicas.keySet()) {
        Map<String, String> idealStateMap = idealStateMaps.get(segmentName);
        if (idealStateMap == null || idealStateMap.containsValue(SegmentStateModel.CONSUMING)) {
          return false;
        }
        partitionIds.add(new LLCSegmentName(segmentName).getPartitionGroupId());
      }
      if (partitionIds.size() < NUM_PARTITIONS) {
        return false;
      }
      _errorReplicas = errorReplicas;
      return true;
    }, 1000L, 600_000L, "Injected failures did not leave a committed segment with an ERROR replica per partition");
  }

  @Test(priority = 0)
  public void testErrorReplicasAreNotResetWithoutOverride()
      throws Exception {
    // Space the runs out as in production, so a reset sent by one run would have landed before the next check
    for (int run = 0; run < NUM_VALIDATION_RUNS_WITHOUT_OVERRIDE; run++) {
      runSegmentLevelValidation(false);
      Thread.sleep(VALIDATION_RERUN_INTERVAL_MS);
      assertEquals(getErrorReplicas(), _errorReplicas, "ERROR replicas changed after validation run: " + run);
      assertEquals(MetricValueUtils.getTableGaugeValue(_controllerStarter.getControllerMetrics(), _realtimeTableName,
              ControllerGauge.PAUSELESS_SEGMENTS_IN_UNRECOVERABLE_ERROR_COUNT), _errorReplicas.size(),
          "Unexpected number of unrecoverable segments after validation run: " + run);
    }
  }

  @Test(priority = 1)
  public void testErrorReplicasAreRepairedWithOverride()
      throws Exception {
    // Drive the segment-level validation with the override until every ERROR replica is repaired, i.e. reaches its
    // ideal state: leaving ERROR is not enough, since a reset replica goes through OFFLINE. The runs are spaced out
    // so a run does not re-trigger resets still in flight from the previous run.
    long[] lastValidationRunMs = new long[1];
    TestUtils.waitForCondition(aVoid -> {
      if (PauselessRealtimeTestUtils.isExternalViewConverged(_helixResourceManager, _realtimeTableName)) {
        return true;
      }
      long nowMs = System.currentTimeMillis();
      if (nowMs - lastValidationRunMs[0] >= VALIDATION_RERUN_INTERVAL_MS) {
        lastValidationRunMs[0] = nowMs;
        runSegmentLevelValidation(true);
      }
      return false;
    }, 1000L, 600_000L, "ERROR replicas were not repaired with the override");

    // Wait for the broker to route to the repaired server again: each routing table it builds picks one replica
    // group, alternating between them
    Set<String> routedServers = new HashSet<>();
    TestUtils.waitForCondition(aVoid -> {
      try {
        getDebugInfo("debug/routingTable/" + _realtimeTableName).get(_realtimeTableName).fieldNames()
            .forEachRemaining(routedServers::add);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return routedServers.size() == getNumReplicas();
    }, 1000L, 60_000L, "Broker does not route to both replica groups");

    // With all its replicas repaired, the lagging server catches up. Strict replica-group routing sends consecutive
    // queries to alternating replica groups, so both must return the deduplicated count.
    waitForAllDocsLoaded(600_000L);
    for (int i = 0; i < 2 * getNumReplicas(); i++) {
      assertEquals(getCurrentCountStarResult(), getCountStarResult());
    }
  }

  private Map<String, Set<String>> getErrorReplicas() {
    ExternalView externalView = _helixResourceManager.getHelixAdmin()
        .getResourceExternalView(_helixResourceManager.getHelixClusterName(), _realtimeTableName);
    Map<String, Set<String>> errorReplicas = new HashMap<>();
    if (externalView == null) {
      return errorReplicas;
    }
    for (Map.Entry<String, Map<String, String>> entry : externalView.getRecord().getMapFields().entrySet()) {
      for (Map.Entry<String, String> instanceState : entry.getValue().entrySet()) {
        if (instanceState.getValue().equals(SegmentStateModel.ERROR)) {
          errorReplicas.computeIfAbsent(entry.getKey(), k -> new HashSet<>()).add(instanceState.getKey());
        }
      }
    }
    return errorReplicas;
  }

  private void runSegmentLevelValidation(boolean repairErrorSegmentsForPartialUpsertOrDedup) {
    Properties periodicTaskProperties = new Properties();
    periodicTaskProperties.setProperty(ControllerPeriodicTask.RUN_SEGMENT_LEVEL_VALIDATION, Boolean.TRUE.toString());
    periodicTaskProperties.setProperty(
        RealtimeSegmentValidationManager.REPAIR_ERROR_SEGMENTS_FOR_PARTIAL_UPSERT_OR_DEDUP,
        Boolean.toString(repairErrorSegmentsForPartialUpsertOrDedup));
    _controllerStarter.getRealtimeSegmentValidationManager().run(periodicTaskProperties);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    // Run every step even if an earlier one fails, so a failed test does not leak the cluster into later suites
    Throwable cleanupFailure = null;
    try {
      if (_helixResourceManager != null && _helixResourceManager.getRealtimeTableConfig(getTableName()) != null) {
        dropRealtimeTable(getTableName());
      }
    } catch (Throwable t) {
      cleanupFailure = addCleanupFailure(cleanupFailure, t);
    }
    try {
      if (!_serverStarters.isEmpty()) {
        stopServer();
      }
    } catch (Throwable t) {
      cleanupFailure = addCleanupFailure(cleanupFailure, t);
    }
    try {
      if (!_brokerStarters.isEmpty()) {
        stopBroker();
      }
    } catch (Throwable t) {
      cleanupFailure = addCleanupFailure(cleanupFailure, t);
    }
    try {
      if (_controllerStarter != null) {
        stopController();
      }
    } catch (Throwable t) {
      cleanupFailure = addCleanupFailure(cleanupFailure, t);
    }
    try {
      stopKafka();
    } catch (Throwable t) {
      cleanupFailure = addCleanupFailure(cleanupFailure, t);
    }
    try {
      stopZk();
    } catch (Throwable t) {
      cleanupFailure = addCleanupFailure(cleanupFailure, t);
    }
    try {
      FileUtils.deleteDirectory(_tempDir);
    } catch (Throwable t) {
      cleanupFailure = addCleanupFailure(cleanupFailure, t);
    }
    if (cleanupFailure != null) {
      rethrowCleanupFailure(cleanupFailure);
    }
  }

  private static Throwable addCleanupFailure(Throwable cleanupFailure, Throwable failure) {
    if (cleanupFailure == null) {
      return failure;
    }
    cleanupFailure.addSuppressed(failure);
    return cleanupFailure;
  }

  private static void rethrowCleanupFailure(Throwable cleanupFailure)
      throws Exception {
    if (cleanupFailure instanceof Error) {
      throw (Error) cleanupFailure;
    }
    if (cleanupFailure instanceof Exception) {
      throw (Exception) cleanupFailure;
    }
    throw new RuntimeException(cleanupFailure);
  }
}
