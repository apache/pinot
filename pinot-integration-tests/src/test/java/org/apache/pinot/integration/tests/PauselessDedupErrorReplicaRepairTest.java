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
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.ExternalView;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
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

import static org.testng.Assert.assertNotNull;


/// Verifies that on a pauseless dedup table WITHOUT the disaster recovery override, the segment-level validation
/// repairs ERROR replicas whose segment another replica holds, by resetting only those replicas so they reload from
/// a peer or the deep store.
///
/// The consuming transition is made to fail on one server only, so every affected segment keeps one healthy replica
/// and one ERROR replica. Re-ingestion is never needed for this repair, so it must run even though re-ingestion is
/// disallowed for dedup tables. The dedup consume gate also blocks the failing server's consumers while any of its
/// replicas is in ERROR, so without this repair the lagging server never catches up.
public class PauselessDedupErrorReplicaRepairTest extends BaseClusterIntegrationTest {
  private static final int NUM_PARTITIONS = 2;
  private static final long VALIDATION_RERUN_INTERVAL_MS = 10_000L;

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
    // Intentionally no disasterRecoveryMode: repair of COMMITTING segments stays disallowed for this dedup table,
    // which must not stop the reset of ERROR replicas that have a healthy copy elsewhere.
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
  }

  @Test
  public void testErrorReplicasAreRepairedWithoutDisasterRecoveryOverride()
      throws Exception {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());

    TestUtils.waitForCondition(aVoid -> getNumErrorReplicasInEV(realtimeTableName) > 0, 600_000L,
        "Injected failures did not put any replica in ERROR state");

    // Drive the segment-level validation periodically (as it runs in production) until every ERROR replica is
    // repaired, i.e. reaches its ideal state: leaving ERROR is not enough, since a reset replica goes through OFFLINE.
    // The runs are spaced out so a pass does not re-trigger resets still in flight from the previous pass.
    long[] lastValidationRunMs = new long[1];
    TestUtils.waitForCondition(aVoid -> {
      if (PauselessRealtimeTestUtils.isExternalViewConverged(_helixResourceManager, realtimeTableName)) {
        return true;
      }
      long nowMs = System.currentTimeMillis();
      if (nowMs - lastValidationRunMs[0] >= VALIDATION_RERUN_INTERVAL_MS) {
        lastValidationRunMs[0] = nowMs;
        runSegmentLevelValidation();
      }
      return false;
    }, 1000L, 600_000L, "ERROR replicas were not repaired without the disaster recovery override");

    // With all its replicas repaired, the lagging server passes the dedup consume gate and catches up, so queries
    // return the deduplicated count from either replica group.
    waitForAllDocsLoaded(600_000L);
  }

  private int getNumErrorReplicasInEV(String realtimeTableName) {
    ExternalView externalView = _helixResourceManager.getHelixAdmin()
        .getResourceExternalView(_helixResourceManager.getHelixClusterName(), realtimeTableName);
    if (externalView == null) {
      return 0;
    }
    int numErrorReplicas = 0;
    for (Map<String, String> instanceStateMap : externalView.getRecord().getMapFields().values()) {
      for (String state : instanceStateMap.values()) {
        if (state.equals(SegmentStateModel.ERROR)) {
          numErrorReplicas++;
        }
      }
    }
    return numErrorReplicas;
  }

  private void runSegmentLevelValidation() {
    Properties periodicTaskProperties = new Properties();
    periodicTaskProperties.setProperty(ControllerPeriodicTask.RUN_SEGMENT_LEVEL_VALIDATION, Boolean.TRUE.toString());
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
