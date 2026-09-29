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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.ExternalView;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.BaseControllerStarter;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import org.apache.pinot.integration.tests.realtime.utils.FailureInjectingControllerStarter;
import org.apache.pinot.integration.tests.realtime.utils.FailureInjectingTableConfig;
import org.apache.pinot.integration.tests.realtime.utils.FailureInjectingTableDataManagerProvider;
import org.apache.pinot.server.starter.helix.HelixInstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;


/// Verifies recovery from server-side segment commit and consuming-transition failures with one shared cluster.
public class PauselessRealtimeIngestionSegmentCommitFailureTest extends BaseClusterIntegrationTest {
  private static final String REFERENCE_TABLE_NAME = DEFAULT_TABLE_NAME + "_reference";
  private static final long VALIDATION_RERUN_INTERVAL_MS = 10_000L;

  private FailureScenario _failureScenario;
  private File _sampleAvroFile;

  @Override
  protected String getTableName() {
    return _failureScenario != null ? getPauselessTableName(_failureScenario) : getNonPauselessTableName();
  }

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    properties.put(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_MANAGER_SCHEDULER_ENABLED, true);
    properties.put(ControllerConf.ControllerPeriodicTasksConf.ENABLE_DEEP_STORE_RETRY_UPLOAD_LLC_SEGMENT, true);
    // Recovery is triggered explicitly by each scenario. Keep the scheduled validator from racing it while this
    // class shares one controller across both scenarios.
    properties.put(ControllerConf.ControllerPeriodicTasksConf.REALTIME_SEGMENT_VALIDATION_INITIAL_DELAY_IN_SECONDS,
        3600);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    serverConf.setProperty("pinot.server.instance.segment.store.uri", "file:" + _controllerConfig.getDataDir());
    serverConf.setProperty("pinot.server.instance." + HelixInstanceDataManagerConfig.UPLOAD_SEGMENT_TO_DEEP_STORE,
        "true");
    serverConf.setProperty("pinot.server.instance." + CommonConstants.Server.TABLE_DATA_MANAGER_PROVIDER_CLASS,
        FailureInjectingTableDataManagerProvider.class.getName());
    for (FailureScenario failureScenario : FailureScenario.values()) {
      serverConf.setProperty(
          "pinot.server.instance." + FailureInjectingTableDataManagerProvider.FAILURE_CONFIG_KEY + "."
              + getPauselessTableName(failureScenario), failureScenario._failureConfig.toJson());
    }
  }

  @Override
  public BaseControllerStarter createControllerStarter() {
    return new FailureInjectingControllerStarter();
  }

  @BeforeClass(alwaysRun = true)
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    startZk();
    startKafka();
    startController();
    startBroker();
    startServers(getNumServersForTest());

    List<File> avroFiles = unpackAvroData(_tempDir);
    _sampleAvroFile = avroFiles.get(0);
    pushAvroIntoKafka(avroFiles);
    setupReferenceTable();
  }

  protected int getNumServersForTest() {
    return 1;
  }

  protected TableConfig createTestTableConfig(File sampleAvroFile) {
    return createRealtimeTableConfig(sampleAvroFile);
  }

  protected void configurePauselessTable(TableConfig tableConfig) {
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    Map<String, String> streamConfigs = indexingConfig.getStreamConfigs();
    indexingConfig.setStreamConfigs(null);
    assertNotNull(streamConfigs);
    streamConfigs.put(StreamConfigProperties.PAUSELESS_SEGMENT_DOWNLOAD_TIMEOUT_SECONDS, "10");
    IngestionConfig ingestionConfig = new IngestionConfig();
    StreamIngestionConfig streamIngestionConfig = new StreamIngestionConfig(List.of(streamConfigs));
    streamIngestionConfig.setPauselessConsumptionEnabled(true);
    ingestionConfig.setStreamIngestionConfig(streamIngestionConfig);
    tableConfig.setIngestionConfig(ingestionConfig);
  }

  protected boolean hasExpectedErrorSegments(String realtimeTableName, int expectedMaxFailures) {
    return getNumErrorSegmentsInEV(realtimeTableName) == expectedMaxFailures;
  }

  protected int getNumErrorSegmentsInEV(String realtimeTableName) {
    ExternalView externalView = _helixResourceManager.getHelixAdmin()
        .getResourceExternalView(_helixResourceManager.getHelixClusterName(), realtimeTableName);
    if (externalView == null) {
      return 0;
    }
    int numErrorSegments = 0;
    for (Map<String, String> instanceStateMap : externalView.getRecord().getMapFields().values()) {
      for (String state : instanceStateMap.values()) {
        if (state.equals(SegmentStateModel.ERROR)) {
          numErrorSegments++;
        }
      }
    }
    return numErrorSegments;
  }

  @Test
  public void testSegmentCommitFailure()
      throws Exception {
    runFailureScenario(FailureScenario.SEGMENT_COMMIT);
  }

  @Test
  public void testConsumingTransitionFailure()
      throws Exception {
    runFailureScenario(FailureScenario.CONSUMING_TRANSITION);
  }

  private synchronized void runFailureScenario(FailureScenario failureScenario)
      throws Exception {
    _failureScenario = failureScenario;
    Throwable testFailure = null;
    try {
      setupPauselessTable(failureScenario);
      verifyRecovery(failureScenario);
    } catch (Exception e) {
      testFailure = e;
      throw e;
    } catch (Error e) {
      testFailure = e;
      throw e;
    } finally {
      Throwable cleanupFailure;
      try {
        cleanupFailure = tearDownFailureScenario(failureScenario);
      } finally {
        _failureScenario = null;
      }
      if (cleanupFailure != null) {
        if (testFailure != null) {
          testFailure.addSuppressed(cleanupFailure);
        } else {
          rethrowCleanupFailure(cleanupFailure);
        }
      }
    }
  }

  private void setupReferenceTable()
      throws Exception {
    Schema schema = createSchema();
    schema.setSchemaName(getNonPauselessTableName());
    addSchema(schema);

    TableConfig tableConfig = createTestTableConfig(_sampleAvroFile);
    tableConfig.setTableName(TableNameBuilder.REALTIME.tableNameWithType(getNonPauselessTableName()));
    configureRetention(tableConfig);
    addTableConfig(tableConfig);
    waitForAllDocsLoaded(tableConfig.getTableName(), 600_000L);
  }

  private void setupPauselessTable(FailureScenario failureScenario)
      throws Exception {
    String tableName = getPauselessTableName(failureScenario);
    Schema schema = createSchema();
    schema.setSchemaName(tableName);
    addSchema(schema);

    TableConfig tableConfig = createTestTableConfig(_sampleAvroFile);
    tableConfig.setTableName(TableNameBuilder.REALTIME.tableNameWithType(tableName));
    configureRetention(tableConfig);
    configurePauselessTable(tableConfig);
    addTableConfig(tableConfig);

    String realtimeTableName = tableConfig.getTableName();
    TestUtils.waitForCondition(
        aVoid -> hasExpectedErrorSegments(realtimeTableName, failureScenario._expectedMaxFailures), 600_000L,
        "Segments still not in error state");
  }

  private static void configureRetention(TableConfig tableConfig) {
    tableConfig.getValidationConfig().setRetentionTimeUnit("DAYS");
    tableConfig.getValidationConfig().setRetentionTimeValue("100000");
  }

  private void verifyRecovery(FailureScenario failureScenario) {
    String pauselessTableName = TableNameBuilder.REALTIME.tableNameWithType(getPauselessTableName(failureScenario));

    List<String> erroredSegments = getSegmentsInEV(pauselessTableName, SegmentStateModel.ERROR);
    assertFalse(erroredSegments.isEmpty(), "No segments found in ERROR state, expected at least one.");

    // Segments in ERROR state are repaired by the segment-level validation (re-ingestion for COMMITTING segments,
    // reset for IN_PROGRESS segments), which runs periodically in production. Run it periodically here as well
    // instead of asserting on a single pass: the failure injection keeps producing new ERROR states past any single
    // pass — replicas hit the injected failures independently while the table is still consuming, and every reset of
    // a consuming segment creates a new segment data manager that draws from the remaining failure budget. The
    // re-runs are spaced out so that a pass does not re-trigger repairs (re-ingestion, reset) that are still in
    // flight from the previous pass, while the ERROR-empty check itself polls at the usual 1s granularity.
    long[] lastValidationRunMs = new long[1];
    TestUtils.waitForCondition(aVoid -> {
      if (getSegmentsInEV(pauselessTableName, SegmentStateModel.ERROR).isEmpty()) {
        return true;
      }
      long nowMs = System.currentTimeMillis();
      if (nowMs - lastValidationRunMs[0] >= VALIDATION_RERUN_INTERVAL_MS) {
        lastValidationRunMs[0] = nowMs;
        runSegmentLevelValidation();
      }
      return false;
    }, 1000L, 600_000L, "Some segments are still in ERROR state after repeated validation runs");

    TestUtils.waitForCondition(aVoid -> getSegmentsInEV(pauselessTableName, SegmentStateModel.OFFLINE).isEmpty(),
        30_000L, "Some segments are in OFFLINE state after resetSegments()");

    compareZKMetadataForSegments(_helixResourceManager.getSegmentsZKMetadata(pauselessTableName),
        _helixResourceManager.getSegmentsZKMetadata(
            TableNameBuilder.REALTIME.tableNameWithType(getNonPauselessTableName())));
  }

  private void runSegmentLevelValidation() {
    Properties periodicTaskProperties = new Properties();
    periodicTaskProperties.setProperty(ControllerPeriodicTask.RUN_SEGMENT_LEVEL_VALIDATION, Boolean.TRUE.toString());
    _controllerStarter.getRealtimeSegmentValidationManager().run(periodicTaskProperties);
  }

  private List<String> getSegmentsInEV(String realtimeTableName, String status) {
    ExternalView externalView = _helixResourceManager.getHelixAdmin()
        .getResourceExternalView(_helixResourceManager.getHelixClusterName(), realtimeTableName);
    if (externalView == null) {
      return List.of();
    }
    List<String> segmentsToReturn = new ArrayList<>();
    for (Map.Entry<String, Map<String, String>> entry : externalView.getRecord().getMapFields().entrySet()) {
      if (entry.getValue().containsValue(status)) {
        segmentsToReturn.add(entry.getKey());
      }
    }
    return segmentsToReturn;
  }

  private void compareZKMetadataForSegments(List<SegmentZKMetadata> segmentsZKMetadata,
      List<SegmentZKMetadata> referenceSegmentsZKMetadata) {
    Map<String, SegmentZKMetadata> segmentZKMetadataMap = getPartitionSegmentNumberToMetadataMap(segmentsZKMetadata);
    Map<String, SegmentZKMetadata> referenceSegmentZKMetadataMap =
        getPartitionSegmentNumberToMetadataMap(referenceSegmentsZKMetadata);
    segmentZKMetadataMap.forEach((segmentKey, segmentZKMetadata) ->
        assertSegmentZKMetadataSame(segmentZKMetadata, referenceSegmentZKMetadataMap.get(segmentKey)));
  }

  private void assertSegmentZKMetadataSame(SegmentZKMetadata segmentZKMetadata,
      SegmentZKMetadata referenceSegmentZKMetadata) {
    if (segmentZKMetadata.getStatus() != CommonConstants.Segment.Realtime.Status.DONE) {
      return;
    }
    assertEquals(segmentZKMetadata.getStatus(), referenceSegmentZKMetadata.getStatus());
    assertEquals(segmentZKMetadata.getStartOffset(), referenceSegmentZKMetadata.getStartOffset());
    assertEquals(segmentZKMetadata.getEndOffset(), referenceSegmentZKMetadata.getEndOffset());
    assertEquals(segmentZKMetadata.getTotalDocs(), referenceSegmentZKMetadata.getTotalDocs());
    assertEquals(segmentZKMetadata.getStartTimeMs(), referenceSegmentZKMetadata.getStartTimeMs());
    assertEquals(segmentZKMetadata.getEndTimeMs(), referenceSegmentZKMetadata.getEndTimeMs());
  }

  private Map<String, SegmentZKMetadata> getPartitionSegmentNumberToMetadataMap(
      List<SegmentZKMetadata> segmentsZKMetadata) {
    Map<String, SegmentZKMetadata> segmentZKMetadataMap = new HashMap<>();
    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
      LLCSegmentName llcSegmentName = new LLCSegmentName(segmentZKMetadata.getSegmentName());
      String segmentKey = llcSegmentName.getPartitionGroupId() + "_" + llcSegmentName.getSequenceNumber();
      segmentZKMetadataMap.put(segmentKey, segmentZKMetadata);
    }
    return segmentZKMetadataMap;
  }

  protected String getNonPauselessTableName() {
    return REFERENCE_TABLE_NAME;
  }

  private static String getPauselessTableName(FailureScenario failureScenario) {
    return DEFAULT_TABLE_NAME + "_" + failureScenario._tableNameSuffix;
  }

  private Throwable tearDownFailureScenario(FailureScenario failureScenario) {
    return cleanUpTableAndSchema(getPauselessTableName(failureScenario), null);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    Throwable cleanupFailure = null;
    for (FailureScenario failureScenario : FailureScenario.values()) {
      cleanupFailure = cleanUpTableAndSchema(getPauselessTableName(failureScenario), cleanupFailure);
    }
    cleanupFailure = cleanUpTableAndSchema(getNonPauselessTableName(), cleanupFailure);
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

  private Throwable cleanUpTableAndSchema(String rawTableName, Throwable cleanupFailure) {
    if (_helixResourceManager == null) {
      return cleanupFailure;
    }

    try {
      if (_helixResourceManager.getRealtimeTableConfig(rawTableName) != null) {
        dropRealtimeTable(rawTableName);
      }
    } catch (Throwable t) {
      cleanupFailure = addCleanupFailure(cleanupFailure, t);
    }

    String tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(rawTableName);
    boolean externalViewRemoved = false;
    try {
      waitForEVToDisappear(tableNameWithType);
      externalViewRemoved = true;
    } catch (Throwable t) {
      cleanupFailure = addCleanupFailure(cleanupFailure, t);
    }
    boolean tableDataManagerRemoved = false;
    try {
      waitForTableDataManagerRemoved(tableNameWithType);
      tableDataManagerRemoved = true;
    } catch (Throwable t) {
      cleanupFailure = addCleanupFailure(cleanupFailure, t);
    }

    boolean tableConfigRemoved = false;
    try {
      tableConfigRemoved = _helixResourceManager.getRealtimeTableConfig(rawTableName) == null;
    } catch (Throwable t) {
      cleanupFailure = addCleanupFailure(cleanupFailure, t);
    }

    if (tableConfigRemoved && externalViewRemoved && tableDataManagerRemoved) {
      try {
        if (_helixResourceManager.getSchema(rawTableName) != null) {
          deleteSchema(rawTableName);
        }
      } catch (Throwable t) {
        cleanupFailure = addCleanupFailure(cleanupFailure, t);
      }
    }
    return cleanupFailure;
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

  private enum FailureScenario {
    SEGMENT_COMMIT("segmentCommitFailure", new FailureInjectingTableConfig(true, false, 10)),
    CONSUMING_TRANSITION("consumingTransitionFailure", new FailureInjectingTableConfig(false, true, 2));

    private final String _tableNameSuffix;
    private final FailureInjectingTableConfig _failureConfig;
    private final int _expectedMaxFailures;

    FailureScenario(String tableNameSuffix, FailureInjectingTableConfig failureConfig) {
      _tableNameSuffix = tableNameSuffix;
      _failureConfig = failureConfig;
      _expectedMaxFailures = failureConfig.getMaxFailures();
    }
  }
}
