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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.realtime.SegmentCompletionConfig;
import org.apache.pinot.controller.helix.core.util.FailureInjectionUtils;
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
import static org.testng.Assert.assertNull;


public class PauselessRealtimeIngestionCommitEndMetadataFailureTest extends BaseClusterIntegrationTest {

  private static final int NUM_REALTIME_SEGMENTS = 48;
  protected static final long MAX_SEGMENT_COMPLETION_TIME_MILLIS = 300_000L; // 5 MINUTES
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PauselessRealtimeIngestionCommitEndMetadataFailureTest.class);
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

    // inject failure in the commit protocol for the pauseless table
    _helixResourceManager.getPinotLLCRealtimeSegmentManager()
        .enableTestFault(FailureInjectionUtils.FAULT_BEFORE_COMMIT_END_METADATA);

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
    waitForAllDocsLoaded(600_000L);
  }

  @Test
  public void testSegmentAssignment()
      throws Exception {
    String tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    verifyIdealState(tableNameWithType, NUM_REALTIME_SEGMENTS);
    assertUploadUrlEmpty(_helixResourceManager.getSegmentsZKMetadata(tableNameWithType));
    // this sleep has been introduced to ensure that the RealtimeSegmentValidationManager can
    // run segment level validations. The segment is not fixed by the validation manager in case the desired time
    // can not elapsed
    Thread.sleep(MAX_SEGMENT_COMPLETION_TIME_MILLIS);
    _controllerStarter.getRealtimeSegmentValidationManager().run();
    // wait for the url to show up after running validation manager
    TestUtils.waitForCondition((aVoid) -> {
      List<SegmentZKMetadata> segmentZKMetadataList = _helixResourceManager.getSegmentsZKMetadata(tableNameWithType);
      return assertUrlPresent(segmentZKMetadataList);
    }, 1000, 100000, "Some segments still have missing url");

    compareZKMetadataForSegments(_helixResourceManager.getSegmentsZKMetadata(tableNameWithType),
        _helixResourceManager.getSegmentsZKMetadata(TableNameBuilder.REALTIME.tableNameWithType(DEFAULT_TABLE_NAME_2)));
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
