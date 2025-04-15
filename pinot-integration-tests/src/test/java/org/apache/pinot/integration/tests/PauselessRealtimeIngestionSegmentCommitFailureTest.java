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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.ExternalView;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.BaseControllerStarter;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.integration.tests.realtime.utils.FailureInjectingControllerStarter;
import org.apache.pinot.integration.tests.realtime.utils.FailureInjectingPinotLLCRealtimeSegmentManager;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.integration.tests.realtime.utils.FailureInjectingRealtimeTableDataManager.MAX_NUMBER_OF_FAILURES;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;


public class PauselessRealtimeIngestionSegmentCommitFailureTest extends BaseClusterIntegrationTest {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PauselessRealtimeIngestionSegmentCommitFailureTest.class);

  private static final String DEFAULT_TABLE_NAME_2 = DEFAULT_TABLE_NAME + "_2";
  private static final long MAX_SEGMENT_COMPLETION_TIME_MILLIS = 10_000L;

  protected void overrideControllerConf(Map<String, Object> properties) {
    properties.put(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_MANAGER_SCHEDULER_ENABLED, true);
    properties.put(ControllerConf.ControllerPeriodicTasksConf.ENABLE_DEEP_STORE_RETRY_UPLOAD_LLC_SEGMENT, true);
    // Set the delay more than the time we sleep before triggering RealtimeSegmentValidationManager manually, i.e.
    // MAX_SEGMENT_COMPLETION_TIME_MILLIS, to ensure that the segment level validations are performed.
    properties.put(ControllerConf.ControllerPeriodicTasksConf.REALTIME_SEGMENT_VALIDATION_INITIAL_DELAY_IN_SECONDS,
        500);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    serverConf.setProperty("pinot.server.instance.segment.store.uri", "file:" + _controllerConfig.getDataDir());
    serverConf.setProperty("pinot.server.instance." + HelixInstanceDataManagerConfig.UPLOAD_SEGMENT_TO_DEEP_STORE,
        "true");
    serverConf.setProperty("pinot.server.instance." + CommonConstants.Server.TABLE_DATA_MANAGER_PROVIDER_CLASS,
        "org.apache.pinot.integration.tests.realtime.utils.FailureInjectingTableDataManagerProvider");
  }

  @Override
  public BaseControllerStarter createControllerStarter() {
    return new FailureInjectingControllerStarter();
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
    List<File> avroFiles = unpackAvroData(_tempDir);
    startKafka();
    pushAvroIntoKafka(avroFiles);

    setMaxSegmentCompletionTimeMillis();
    // create schema for non-pauseless table
    Schema schema = createSchema();
    schema.setSchemaName(DEFAULT_TABLE_NAME_2);
    addSchema(schema);

    // add non-pauseless table
    TableConfig tableConfig2 = createRealtimeTableConfig(avroFiles.get(0));
    tableConfig2.setTableName(DEFAULT_TABLE_NAME_2);
    tableConfig2.getValidationConfig().setRetentionTimeUnit("DAYS");
    tableConfig2.getValidationConfig().setRetentionTimeValue("100000");
    addTableConfig(tableConfig2);

    waitForDocsLoaded(600_000L, true, tableConfig2.getTableName());

    // create schema for pauseless table
    schema.setSchemaName(DEFAULT_TABLE_NAME);
    addSchema(schema);

    // add pauseless table
    TableConfig tableConfig = createRealtimeTableConfig(avroFiles.get(0));
    tableConfig.getValidationConfig().setRetentionTimeUnit("DAYS");
    tableConfig.getValidationConfig().setRetentionTimeValue("100000");

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

    addTableConfig(tableConfig);
    String realtimeTableName = tableConfig.getTableName();
    TestUtils.waitForCondition(aVoid -> getNumErrorSegmentsInEV(realtimeTableName) == MAX_NUMBER_OF_FAILURES, 10_000L,
        600_000L, "Segments still not in error state: expected " + MAX_NUMBER_OF_FAILURES + ", found: "
            + getNumErrorSegmentsInEV(realtimeTableName));
  }

  private void setMaxSegmentCompletionTimeMillis() {
    PinotLLCRealtimeSegmentManager realtimeSegmentManager = _helixResourceManager.getRealtimeSegmentManager();
    if (realtimeSegmentManager instanceof FailureInjectingPinotLLCRealtimeSegmentManager) {
      ((FailureInjectingPinotLLCRealtimeSegmentManager) realtimeSegmentManager).setMaxSegmentCompletionTimeoutMs(
          MAX_SEGMENT_COMPLETION_TIME_MILLIS);
    }
  }

  private int getNumErrorSegmentsInEV(String realtimeTableName) {
    ExternalView externalView = _helixResourceManager.getHelixAdmin()
        .getResourceExternalView(_helixResourceManager.getHelixClusterName(), realtimeTableName);
    ExternalView newExternalView = _helixResourceManager.getTableExternalView(realtimeTableName);
    if (externalView == null) {
      LOGGER.error("Found NULL EV for resource: {}!", realtimeTableName);
      return 0;
    }
    if (!externalView.equals(newExternalView)) {
      LOGGER.error("******* EV returned from two methods differ!!! ********");
    }
    int numErrorSegments = 0;
    int numNonErrorSegments = 0;
    for (Map.Entry<String, Map<String, String>> segmentToInstancesStateMap
        : externalView.getRecord().getMapFields().entrySet()) {
      String segmentName = segmentToInstancesStateMap.getKey();
      Map<String, String> instanceStateMap = segmentToInstancesStateMap.getValue();
      for (String state : instanceStateMap.values()) {
        if (state.equals(SegmentStateModel.ERROR)) {
          LOGGER.error("Segment {} found in ERROR state", segmentName);
          numErrorSegments++;
        } else {
          LOGGER.error("Segment {} found in {} state", segmentName, state);
          numNonErrorSegments++;
        }
      }
    }
    LOGGER.error("Total EV segments: {}, error segments found: {}, non-error segments found: {}, for table: {}",
        externalView.getRecord().getMapFields().size(), numErrorSegments, numNonErrorSegments, realtimeTableName);
    return numErrorSegments;
  }

  @Test
  public void testSegmentAssignment()
      throws Exception {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());

    // 1) Capture which segments went into the ERROR state
    List<String> erroredSegments = getErrorSegmentsInEV(realtimeTableName);
    assertFalse(erroredSegments.isEmpty(), "No segments found in ERROR state, expected at least one.");

    // Let the RealtimeSegmentValidationManager run so it can fix up segments
    Thread.sleep(MAX_SEGMENT_COMPLETION_TIME_MILLIS);
    _controllerStarter.getRealtimeSegmentValidationManager().run();

    // Wait until there are no ERROR segments in the ExternalView
    TestUtils.waitForCondition(aVoid -> getErrorSegmentsInEV(realtimeTableName).isEmpty(), 600_000L,
        "Some segments are still in ERROR state after resetSegments()");

    // Finally compare metadata across your two tables
    compareZKMetadataForSegments(_helixResourceManager.getSegmentsZKMetadata(realtimeTableName),
        _helixResourceManager.getSegmentsZKMetadata(TableNameBuilder.REALTIME.tableNameWithType(DEFAULT_TABLE_NAME_2)));
  }

  /**
   * Returns the list of segment names in ERROR state from the ExternalView of the given table.
   */
  private List<String> getErrorSegmentsInEV(String realtimeTableName) {
    ExternalView externalView = _helixResourceManager.getHelixAdmin()
        .getResourceExternalView(_helixResourceManager.getHelixClusterName(), realtimeTableName);
    if (externalView == null) {
      return List.of();
    }
    List<String> errorSegments = new ArrayList<>();
    for (Map.Entry<String, Map<String, String>> entry : externalView.getRecord().getMapFields().entrySet()) {
      if (entry.getValue().containsValue(SegmentStateModel.ERROR)) {
        errorSegments.add(entry.getKey());
      }
    }
    return errorSegments;
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
    dropRealtimeTable(getTableName());
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }
}
