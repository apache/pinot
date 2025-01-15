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
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.PauselessConsumptionUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.realtime.SegmentCompletionConfig;
import org.apache.pinot.server.starter.helix.HelixInstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
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
import static org.testng.Assert.assertTrue;


public class PauselessRealtimeIngestionIntegrationTest extends BaseClusterIntegrationTest {

  private static final int NUM_REALTIME_SEGMENTS = 48;
  private static final Logger LOGGER = LoggerFactory.getLogger(PauselessRealtimeIngestionIntegrationTest.class);
  private List<File> _avroFiles;

  protected void overrideControllerConf(Map<String, Object> properties) {
    properties.put(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_MANAGER_SCHEDULER_ENABLED, true);
    properties.put(ControllerConf.ControllerPeriodicTasksConf.ENABLE_DEEP_STORE_RETRY_UPLOAD_LLC_SEGMENT, true);
    properties.put(SegmentCompletionConfig.FSM_SCHEME + "pauseless",
        "org.apache.pinot.controller.helix.core.realtime.PauselessSegmentCompletionFSM");
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

    _avroFiles = unpackAvroData(_tempDir);
    startKafka();
    pushAvroIntoKafka(_avroFiles);

    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createRealtimeTableConfig(_avroFiles.get(0));
    // Replace stream config from indexing config to ingestion config
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(
        new StreamIngestionConfig(List.of(tableConfig.getIndexingConfig().getStreamConfigs())));
    ingestionConfig.getStreamIngestionConfig().setPauselessConsumptionEnabled(true);
    tableConfig.getIndexingConfig().setStreamConfigs(null);
    tableConfig.setIngestionConfig(ingestionConfig);
    addTableConfig(tableConfig);

    waitForAllDocsLoaded(600_000L);
  }

  @Test(description = "Ensure that all the segments are ingested, built and uploaded when pauseless consumption is "
      + "enabled")
  public void testSegmentAssignment()
      throws Exception {
    String tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    verifyIdealState(tableNameWithType, NUM_REALTIME_SEGMENTS);
    assertTrue(PauselessConsumptionUtils.isPauselessEnabled(getRealtimeTableConfig()));
    TestUtils.waitForCondition((aVoid) -> {
      List<SegmentZKMetadata> segmentZKMetadataList = _helixResourceManager.getSegmentsZKMetadata(tableNameWithType);
      return assertNoSegmentInProhibitedStatus(segmentZKMetadataList,
          CommonConstants.Segment.Realtime.Status.COMMITTING);
    }, 1000, 100000, "Some segments have status COMMITTING");
    TestUtils.waitForCondition((aVoid) -> {
      List<SegmentZKMetadata> segmentZKMetadataList = _helixResourceManager.getSegmentsZKMetadata(tableNameWithType);
      return assertUrlPresent(segmentZKMetadataList);
    }, 1000, 100000, "Some segments still have missing url");
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

  private boolean assertUrlPresent(List<SegmentZKMetadata> segmentZKMetadataList) {
    for (SegmentZKMetadata segmentZKMetadata : segmentZKMetadataList) {
      if (segmentZKMetadata.getStatus() == CommonConstants.Segment.Realtime.Status.DONE
          && segmentZKMetadata.getDownloadUrl() == null) {
        System.out.println("URl not found for segment: " + segmentZKMetadata.getSegmentName());
        return false;
      }
    }
    return true;
  }

  private boolean assertNoSegmentInProhibitedStatus(List<SegmentZKMetadata> segmentZKMetadataList,
      CommonConstants.Segment.Realtime.Status prohibitedStatus) {
    for (SegmentZKMetadata segmentZKMetadata : segmentZKMetadataList) {
      if (segmentZKMetadata.getStatus() == prohibitedStatus) {
        return false;
      }
    }
    return true;
  }

  @Override
  protected Map<String, String> getStreamConfigs() {
    Map<String, String> streamConfigMap = getStreamConfigMap();
    streamConfigMap.put(SEGMENT_COMPLETION_FSM_SCHEME, "pauseless");
    return streamConfigMap;
  }
}
