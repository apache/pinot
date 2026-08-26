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
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.util.FailureInjectionUtils;
import org.apache.pinot.integration.tests.realtime.utils.PauselessRealtimeTestUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNull;


public class PauselessRealtimeIngestionIntegrationTest extends BasePauselessRealtimeIngestionTest {
  private static final int NUM_REALTIME_SEGMENTS_IN_COMMITTING_STATE = 46;

  private File _decoderAvroFile;
  private Scenario _scenario;

  @Override
  protected String getFailurePoint() {
    return _scenario._failurePoint;
  }

  @Override
  protected int getExpectedSegmentsWithFailure() {
    return _scenario._expectedSegmentsWithFailure;
  }

  @Override
  protected int getExpectedZKMetadataWithFailure() {
    return _scenario._expectedZkMetadataWithFailure;
  }

  @Override
  protected long getCountStarResultWithFailure() {
    return _scenario._countStarResultWithFailure;
  }

  @Override
  protected String getTableName() {
    return DEFAULT_TABLE_NAME + "_" + _scenario._name;
  }

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    super.overrideControllerConf(properties);
    // Recovery is triggered explicitly by each failure scenario. Keep the scheduled validator from racing it on a
    // slow CI runner while this class shares one controller across all scenarios.
    properties.put(ControllerConf.ControllerPeriodicTasksConf.REALTIME_SEGMENT_VALIDATION_INITIAL_DELAY_IN_SECONDS,
        3600);
  }

  @Override
  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    startZk();
    startKafka();
    startController();
    startBroker();
    startServer();
    // setupNonPauselessTable() also unpacks and publishes the source records. All scenarios consume the immutable
    // topic from the smallest offset and compare their recovered metadata with this reference table.
    _scenario = Scenario.NO_FAILURE;
    try {
      setupNonPauselessTable();
    } finally {
      _scenario = null;
    }
    // The decoder stores this file in a static field and can initialize on a Helix transition thread well after the
    // table was created. Pin a class-lifetime copy outside _tempDir so scenario cleanup cannot invalidate it.
    _decoderAvroFile = File.createTempFile(getClass().getSimpleName() + "-decoder-", ".avro");
    FileUtils.copyFile(_avroFiles.get(0), _decoderAvroFile);
    _avroFiles = new ArrayList<>(_avroFiles);
    _avroFiles.set(0, _decoderAvroFile);
  }

  @Override
  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws IOException {
    Throwable cleanupFailure = tearDownScenario();
    TableCleanupResult referenceTableCleanup = dropScenarioTable(DEFAULT_TABLE_NAME_2, cleanupFailure);
    cleanupFailure = referenceTableCleanup._cleanupFailure;
    if (referenceTableCleanup._tableRemoved) {
      cleanupFailure = deleteScenarioSchema(DEFAULT_TABLE_NAME_2, cleanupFailure);
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
      if (_decoderAvroFile != null) {
        Files.deleteIfExists(_decoderAvroFile.toPath());
        _decoderAvroFile = null;
      }
    } catch (Throwable t) {
      cleanupFailure = addCleanupFailure(cleanupFailure, t);
    }
    try {
      FileUtils.deleteDirectory(_tempDir);
    } catch (Throwable t) {
      cleanupFailure = addCleanupFailure(cleanupFailure, t);
    }

    throwAsIOException(cleanupFailure);
  }

  @Test(description = "Ensure that all the segments are ingested, built and uploaded when pauseless consumption is "
      + "enabled")
  public void testSegmentAssignment()
      throws Exception {
    runScenario(Scenario.NO_FAILURE);
  }

  @Test
  public void testCommitEndMetadataFailure()
      throws Exception {
    runScenario(Scenario.COMMIT_END_METADATA_FAILURE);
  }

  @Test
  public void testIdealStateUpdateFailure()
      throws Exception {
    runScenario(Scenario.IDEAL_STATE_UPDATE_FAILURE);
  }

  @Test
  public void testNewSegmentMetadataCreationFailure()
      throws Exception {
    runScenario(Scenario.NEW_SEGMENT_METADATA_CREATION_FAILURE);
  }

  private synchronized void runScenario(Scenario scenario)
      throws Exception {
    _scenario = scenario;
    Throwable testFailure = null;
    try {
      if (_scenario._failurePoint != null) {
        injectFailure();
      }
      setupPauselessTable();
      waitForAllDocsLoaded(600_000L);

      switch (_scenario) {
        case NO_FAILURE -> testBasicSegmentAssignment();
        case COMMIT_END_METADATA_FAILURE -> verifyCommitEndMetadataFailure();
        case IDEAL_STATE_UPDATE_FAILURE -> runValidationAndVerify();
        case NEW_SEGMENT_METADATA_CREATION_FAILURE -> verifyNewSegmentMetadataCreationFailure();
        default -> throw new IllegalStateException("Unhandled scenario: " + _scenario);
      }
    } catch (Exception e) {
      testFailure = e;
      throw e;
    } catch (Error e) {
      testFailure = e;
      throw e;
    } finally {
      Throwable cleanupFailure = tearDownScenario();
      if (cleanupFailure != null) {
        if (testFailure != null) {
          testFailure.addSuppressed(cleanupFailure);
        } else {
          throwCleanupFailure(cleanupFailure);
        }
      }
    }
  }

  private Throwable tearDownScenario() {
    Scenario scenario = _scenario;
    try {
      if (scenario == null) {
        return null;
      }
      String tableName = DEFAULT_TABLE_NAME + "_" + scenario._name;
      Throwable cleanupFailure = null;
      if (_failureEnabled) {
        try {
          disableFailure();
        } catch (Throwable t) {
          cleanupFailure = t;
        }
      }
      TableCleanupResult tableCleanup = dropScenarioTable(tableName, cleanupFailure);
      cleanupFailure = tableCleanup._cleanupFailure;
      if (tableCleanup._tableRemoved) {
        cleanupFailure = deleteScenarioSchema(tableName, cleanupFailure);
      }
      return cleanupFailure;
    } finally {
      _scenario = null;
    }
  }

  private void verifyCommitEndMetadataFailure()
      throws Exception {
    String tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    PauselessRealtimeTestUtils.verifyIdealState(tableNameWithType, NUM_REALTIME_SEGMENTS, _helixManager);

    TestUtils.waitForCondition((aVoid) -> {
      List<SegmentZKMetadata> segmentZKMetadataList =
          _helixResourceManager.getSegmentsZKMetadata(tableNameWithType);
      return segmentZKMetadataList.stream()
          .filter(
              segmentZKMetadata -> segmentZKMetadata.getStatus() == CommonConstants.Segment.Realtime.Status.COMMITTING)
          .count() == NUM_REALTIME_SEGMENTS_IN_COMMITTING_STATE;
    }, 1000, 100000, "Some segments are still IN_PROGRESS");

    List<SegmentZKMetadata> segmentZKMetadataList = _helixResourceManager.getSegmentsZKMetadata(tableNameWithType);
    for (SegmentZKMetadata metadata : segmentZKMetadataList) {
      assertNull(metadata.getDownloadUrl());
    }

    runValidationAndVerify();
  }

  private void verifyNewSegmentMetadataCreationFailure()
      throws Exception {
    String tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    TestUtils.waitForCondition((aVoid) -> {
      List<SegmentZKMetadata> segmentZKMetadataList =
          _helixResourceManager.getSegmentsZKMetadata(tableNameWithType);
      return segmentZKMetadataList.stream()
          .filter(
              segmentZKMetadata -> segmentZKMetadata.getStatus() == CommonConstants.Segment.Realtime.Status.COMMITTING)
          .count() == Scenario.NEW_SEGMENT_METADATA_CREATION_FAILURE._expectedZkMetadataWithFailure;
    }, 1000, 100000, "Some segments are still IN_PROGRESS");
    runValidationAndVerify();
  }

  private TableCleanupResult dropScenarioTable(String tableName, Throwable cleanupFailure) {
    if (_helixResourceManager == null) {
      return new TableCleanupResult(cleanupFailure, false);
    }

    String tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    try {
      if (_helixResourceManager.getRealtimeTableConfig(tableName) != null) {
        dropRealtimeTable(tableName);
      }
    } catch (Throwable t) {
      cleanupFailure = addCleanupFailure(cleanupFailure, t);
    }

    boolean tableConfigRemoved = false;
    try {
      tableConfigRemoved = _helixResourceManager.getRealtimeTableConfig(tableName) == null;
    } catch (Throwable t) {
      cleanupFailure = addCleanupFailure(cleanupFailure, t);
    }

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

    return new TableCleanupResult(cleanupFailure,
        tableConfigRemoved && externalViewRemoved && tableDataManagerRemoved);
  }

  private Throwable deleteScenarioSchema(String schemaName, Throwable cleanupFailure) {
    try {
      if (_helixResourceManager != null && _helixResourceManager.getSchema(schemaName) != null) {
        deleteSchema(schemaName);
      }
    } catch (Throwable t) {
      cleanupFailure = addCleanupFailure(cleanupFailure, t);
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

  private static void throwCleanupFailure(Throwable cleanupFailure)
      throws Exception {
    if (cleanupFailure instanceof Error) {
      throw (Error) cleanupFailure;
    }
    if (cleanupFailure instanceof Exception) {
      throw (Exception) cleanupFailure;
    }
    throw new AssertionError("Failed to clean up pauseless ingestion scenario", cleanupFailure);
  }

  private static void throwAsIOException(Throwable cleanupFailure)
      throws IOException {
    if (cleanupFailure == null) {
      return;
    }
    if (cleanupFailure instanceof Error) {
      throw (Error) cleanupFailure;
    }
    if (cleanupFailure instanceof IOException) {
      throw (IOException) cleanupFailure;
    }
    throw new IOException("Failed to tear down pauseless ingestion test cluster", cleanupFailure);
  }

  private static class TableCleanupResult {
    private final Throwable _cleanupFailure;
    private final boolean _tableRemoved;

    private TableCleanupResult(Throwable cleanupFailure, boolean tableRemoved) {
      _cleanupFailure = cleanupFailure;
      _tableRemoved = tableRemoved;
    }
  }

  private enum Scenario {
    NO_FAILURE("noFailure", null, NUM_REALTIME_SEGMENTS, NUM_REALTIME_SEGMENTS, DEFAULT_COUNT_STAR_RESULT),
    COMMIT_END_METADATA_FAILURE("commitEndMetadataFailure",
        FailureInjectionUtils.FAULT_BEFORE_COMMIT_END_METADATA, NUM_REALTIME_SEGMENTS, NUM_REALTIME_SEGMENTS,
        DEFAULT_COUNT_STAR_RESULT),
    IDEAL_STATE_UPDATE_FAILURE("idealStateUpdateFailure", FailureInjectionUtils.FAULT_BEFORE_IDEAL_STATE_UPDATE,
        2, 4, 5000),
    NEW_SEGMENT_METADATA_CREATION_FAILURE("newSegmentMetadataCreationFailure",
        FailureInjectionUtils.FAULT_BEFORE_NEW_SEGMENT_METADATA_CREATION, 2, 2, 5000);

    private final String _name;
    private final String _failurePoint;
    private final int _expectedSegmentsWithFailure;
    private final int _expectedZkMetadataWithFailure;
    private final long _countStarResultWithFailure;

    Scenario(String name, String failurePoint, int expectedSegmentsWithFailure, int expectedZkMetadataWithFailure,
        long countStarResultWithFailure) {
      _name = name;
      _failurePoint = failurePoint;
      _expectedSegmentsWithFailure = expectedSegmentsWithFailure;
      _expectedZkMetadataWithFailure = expectedZkMetadataWithFailure;
      _countStarResultWithFailure = countStarResultWithFailure;
    }
  }
}
