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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.realtime.SegmentCompletionConfig;
import org.apache.pinot.server.starter.helix.HelixInstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.stream.StreamConfigProperties.SEGMENT_COMPLETION_FSM_SCHEME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;


public class PauselessRealtimeIngestionSegmentCommitFailureTest extends BaseClusterIntegrationTest {

  private static final int NUM_REALTIME_SEGMENTS = 48;
  protected static final long MAX_SEGMENT_COMPLETION_TIME_MILLIS = 300_000L; // 5 MINUTES
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PauselessRealtimeIngestionSegmentCommitFailureTest.class);
  private static final String DEFAULT_TABLE_NAME_2 = DEFAULT_TABLE_NAME + "_2";
  private List<File> _avroFiles;

  protected void overrideControllerConf(Map<String, Object> properties) {
    properties.put(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_MANAGER_SCHEDULER_ENABLED, true);
    properties.put(ControllerConf.ControllerPeriodicTasksConf.ENABLE_DEEP_STORE_RETRY_UPLOAD_LLC_SEGMENT, true);
    properties.put(SegmentCompletionConfig.FSM_SCHEME + "pauseless",
        "org.apache.pinot.controller.helix.core.realtime.PauselessSegmentCompletionFSM");
    // Set the delay more than the time we sleep before triggering RealtimeSegmentValidationManager manually, i.e.
    // MAX_SEGMENT_COMPLETION_TIME_MILLIS, to ensure that the segment level validations are performed.
    properties.put(ControllerConf.ControllerPeriodicTasksConf.REALTIME_SEGMENT_VALIDATION_INITIAL_DELAY_IN_SECONDS,
        500);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    // Set segment store uri to the one used by controller as data dir (i.e. deep store)
    try {
      LOGGER.info("Set segment.store.uri: {} for server with scheme: {}", _controllerConfig.getDataDir(),
          new URI(_controllerConfig.getDataDir()).getScheme());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    serverConf.setProperty("pinot.server.instance.segment.store.uri", "file:" + _controllerConfig.getDataDir());
    serverConf.setProperty("pinot.server.instance." + HelixInstanceDataManagerConfig.UPLOAD_SEGMENT_TO_DEEP_STORE,
        "true");
    serverConf.setProperty("pinot.server.instance." + CommonConstants.Server.TABLE_DATA_MANAGER_PROVIDER_CLASS,
        "org.apache.pinot.integration.tests.realtime.FailureInjectingTableDataManagerProvider");
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    // Start a customized controller with more frequent realtime segment validation
    startController();
    startBroker();
    startServer();

    // load data in kafka
    _avroFiles = unpackAvroData(_tempDir);
    startKafka();
    pushAvroIntoKafka(_avroFiles);

    // create schema for non-pauseless table
    Schema schema = createSchema();
    schema.setSchemaName(DEFAULT_TABLE_NAME_2);
    addSchema(schema);

    // add non-pauseless table
    TableConfig tableConfig2 = createRealtimeTableConfig(_avroFiles.get(0));
    tableConfig2.setTableName(DEFAULT_TABLE_NAME_2);
    tableConfig2.getValidationConfig().setRetentionTimeUnit("DAYS");
    tableConfig2.getValidationConfig().setRetentionTimeValue("100000");
    addTableConfig(tableConfig2);

    // Ensure that the commit protocol for all the segments have completed before injecting failure
    waitForDocsLoaded(600_000L, true, tableConfig2.getTableName());
    TestUtils.waitForCondition((aVoid) -> {
      List<SegmentZKMetadata> segmentZKMetadataList =
          _helixResourceManager.getSegmentsZKMetadata(tableConfig2.getTableName());
      return assertUrlPresent(segmentZKMetadataList);
    }, 1000, 100000, "Some segments still have missing url");

    // create schema for pauseless table
    schema.setSchemaName(DEFAULT_TABLE_NAME);
    addSchema(schema);

    // add pauseless table
    TableConfig tableConfig = createRealtimeTableConfig(_avroFiles.get(0));
    tableConfig.getValidationConfig().setRetentionTimeUnit("DAYS");
    tableConfig.getValidationConfig().setRetentionTimeValue("100000");
    tableConfig.getIndexingConfig().setPauselessConsumptionEnabled(true);
    tableConfig.getIndexingConfig().getStreamConfigs().put(SEGMENT_COMPLETION_FSM_SCHEME, "pauseless");
    addTableConfig(tableConfig);
    Thread.sleep(60000L);
    TestUtils.waitForCondition(
        (aVoid) -> atLeastOneErrorSegmentInExternalView(TableNameBuilder.REALTIME.tableNameWithType(getTableName())),
        1000, 600000, "Segments still not in error state");
  }

  @Test
  public void testSegmentAssignment()
      throws Exception {
    String tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(getTableName());

    // 1) Capture which segments went into the ERROR state
    List<String> erroredSegments = getErroredSegmentsInExternalView(tableNameWithType);
    assertFalse(erroredSegments.isEmpty(), "No segments found in ERROR state, expected at least one.");

    // Let the RealtimeSegmentValidationManager run so it can fix up segments
    Thread.sleep(MAX_SEGMENT_COMPLETION_TIME_MILLIS);
    LOGGER.info("Triggering RealtimeSegmentValidationManager to reingest errored segments");
    _controllerStarter.getRealtimeSegmentValidationManager().run();
    LOGGER.info("Finished RealtimeSegmentValidationManager to reingest errored segments");

    // Wait until there are no ERROR segments in the ExternalView
    TestUtils.waitForCondition(aVoid -> {
      List<String> errorSegmentsRemaining = getErroredSegmentsInExternalView(tableNameWithType);
      return errorSegmentsRemaining.isEmpty();
    }, 10000, 100000, "Some segments are still in ERROR state after resetSegments()");

    // Finally compare metadata across your two tables
    compareZKMetadataForSegments(_helixResourceManager.getSegmentsZKMetadata(tableNameWithType),
        _helixResourceManager.getSegmentsZKMetadata(
            TableNameBuilder.REALTIME.tableNameWithType(DEFAULT_TABLE_NAME_2)));
  }

  /**
   * Returns the list of segment names in ERROR state from the ExternalView of the given table.
   */
  private List<String> getErroredSegmentsInExternalView(String tableName) {
    ExternalView resourceEV = _helixResourceManager.getHelixAdmin()
        .getResourceExternalView(_helixResourceManager.getHelixClusterName(), tableName);
    Map<String, Map<String, String>> segmentAssignment = resourceEV.getRecord().getMapFields();
    List<String> erroredSegments = new ArrayList<>();
    for (Map.Entry<String, Map<String, String>> entry : segmentAssignment.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> serverToStateMap = entry.getValue();
      for (String state : serverToStateMap.values()) {
        if ("ERROR".equals(state)) {
          erroredSegments.add(segmentName);
          break; // No need to check other servers for this segment
        }
      }
    }
    return erroredSegments;
  }

  /**
   * Checks that all segments which were previously in ERROR state now have status == UPLOADED.
   */
  private boolean haveErroredSegmentsUploaded(
      List<SegmentZKMetadata> segmentZKMetadataList, List<String> previouslyErroredSegments) {

    // Convert to a Set for quick lookups
    Set<String> erroredSegmentNames = new HashSet<>(previouslyErroredSegments);

    for (SegmentZKMetadata metadata : segmentZKMetadataList) {
      if (erroredSegmentNames.contains(metadata.getSegmentName())) {
        // If it was previously ERROR, then we expect it to have transitioned to UPLOADED
        if (metadata.getStatus() != CommonConstants.Segment.Realtime.Status.UPLOADED) {
          return false;
        }
      }
    }
    return true;
  }


  private void compareZKMetadataForSegments(List<SegmentZKMetadata> segmentsZKMetadata,
      List<SegmentZKMetadata> segmentsZKMetadata1) {
    Map<String, SegmentZKMetadata> segmentZKMetadataMap = getPartitionSegmentNumberToMetadataMap(segmentsZKMetadata);
    Map<String, SegmentZKMetadata> segmentZKMetadataMap1 = getPartitionSegmentNumberToMetadataMap(segmentsZKMetadata1);
    segmentZKMetadataMap.forEach((segmentKey, segmentZKMetadata) -> {
      SegmentZKMetadata segmentZKMetadata1 = segmentZKMetadataMap1.get(segmentKey);
      areSegmentZkMetadataSame(segmentZKMetadata, segmentZKMetadata1);
    });
  }

  private void areSegmentZkMetadataSame(SegmentZKMetadata segmentZKMetadata, SegmentZKMetadata segmentZKMetadata1) {
    if (segmentZKMetadata.getStatus() != CommonConstants.Segment.Realtime.Status.DONE) {
      return;
    }
    assertEquals(segmentZKMetadata.getStatus(), segmentZKMetadata1.getStatus());
    assertEquals(segmentZKMetadata.getStartOffset(), segmentZKMetadata1.getStartOffset());
    assertEquals(segmentZKMetadata.getEndOffset(), segmentZKMetadata1.getEndOffset());
    assertEquals(segmentZKMetadata.getTotalDocs(), segmentZKMetadata1.getTotalDocs());
    assertEquals(segmentZKMetadata.getStartTimeMs(), segmentZKMetadata1.getStartTimeMs());
    assertEquals(segmentZKMetadata.getEndTimeMs(), segmentZKMetadata1.getEndTimeMs());
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

  @AfterClass
  public void tearDown()
      throws IOException {
    LOGGER.info("Tearing down...");
    dropRealtimeTable(getTableName());
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  private void verifyIdealState(String tableName, int numSegmentsExpected) {
    IdealState idealState = HelixHelper.getTableIdealState(_helixManager, tableName);
    Map<String, Map<String, String>> segmentAssignment = idealState.getRecord().getMapFields();
    assertEquals(segmentAssignment.size(), numSegmentsExpected);
  }

  private boolean atLeastOneErrorSegmentInExternalView(String tableName) {
    ExternalView resourceEV = _helixResourceManager.getHelixAdmin()
        .getResourceExternalView(_helixResourceManager.getHelixClusterName(), tableName);
    Map<String, Map<String, String>> segmentAssigment = resourceEV.getRecord().getMapFields();
    for (Map<String, String> serverToStateMap : segmentAssigment.values()) {
      for (String state : serverToStateMap.values()) {
        if (state.equals("ERROR")) {
          return true;
        }
      }
    }
    return false;
  }

  private void assertUploadUrlEmpty(List<SegmentZKMetadata> segmentZKMetadataList) {
    for (SegmentZKMetadata segmentZKMetadata : segmentZKMetadataList) {
      assertNull(segmentZKMetadata.getDownloadUrl());
    }
  }

  private boolean assertUrlPresent(List<SegmentZKMetadata> segmentZKMetadataList) {
    for (SegmentZKMetadata segmentZKMetadata : segmentZKMetadataList) {
      if (segmentZKMetadata.getStatus() == CommonConstants.Segment.Realtime.Status.COMMITTING
          && segmentZKMetadata.getDownloadUrl() == null) {
        LOGGER.warn("URl not found for segment: {}", segmentZKMetadata.getSegmentName());
        return false;
      }
    }
    return true;
  }
}
