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
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.PauselessConsumptionUtils;
import org.apache.pinot.controller.BaseControllerStarter;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.integration.tests.realtime.utils.FailureInjectingControllerStarter;
import org.apache.pinot.integration.tests.realtime.utils.FailureInjectingPinotLLCRealtimeSegmentManager;
import org.apache.pinot.integration.tests.realtime.utils.PauselessRealtimeTestUtils;
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

import static org.testng.Assert.assertTrue;


public abstract class BasePauselessRealtimeIngestionTest extends BaseClusterIntegrationTest {
  protected static final int NUM_REALTIME_SEGMENTS = 48;
  protected static final long DEFAULT_COUNT_STAR_RESULT = 115545L;
  protected static final String DEFAULT_TABLE_NAME_2 = DEFAULT_TABLE_NAME + "_2";
  private static final long MAX_SEGMENT_COMPLETION_TIME_MILLIS = 10_000;

  protected List<File> _avroFiles;
  protected boolean _failureEnabled = false;
  private static final Logger LOGGER = LoggerFactory.getLogger(BasePauselessRealtimeIngestionTest.class);

  protected abstract String getFailurePoint();

  protected abstract int getExpectedSegmentsWithFailure();

  protected abstract int getExpectedZKMetadataWithFailure();

  protected abstract long getCountStarResultWithFailure();

  @Override
  public BaseControllerStarter createControllerStarter() {
    return new FailureInjectingControllerStarter();
  }

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    properties.put(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_MANAGER_SCHEDULER_ENABLED, true);
    properties.put(ControllerConf.ControllerPeriodicTasksConf.ENABLE_DEEP_STORE_RETRY_UPLOAD_LLC_SEGMENT, true);
    properties.put(ControllerConf.ControllerPeriodicTasksConf.REALTIME_SEGMENT_VALIDATION_INITIAL_DELAY_IN_SECONDS,
        500);
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    super.overrideBrokerConf(brokerConf);
    brokerConf.setProperty(CommonConstants.Broker.USE_MSE_TO_FILL_EMPTY_RESPONSE_SCHEMA, true);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    try {
      LOGGER.info("Set segment.store.uri: {} for server with scheme: {}", _controllerConfig.getDataDir(),
          new URI(_controllerConfig.getDataDir()).getScheme());
      serverConf.setProperty("pinot.server.instance.segment.store.uri",
          "file:" + _controllerConfig.getDataDir());
      serverConf.setProperty("pinot.server.instance." + HelixInstanceDataManagerConfig.UPLOAD_SEGMENT_TO_DEEP_STORE,
          "true");
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    startZk();
    startController();
    startBroker();
    startServer();
    setMaxSegmentCompletionTimeMillis();
    setupNonPauselessTable();
    injectFailure();
    setupPauselessTable();
    waitForAllDocsLoaded(600_000L);
  }

  protected TableConfig getPauselessTableConfig() {
    TableConfig tableConfig = createRealtimeTableConfig(_avroFiles.get(0));
    tableConfig.getValidationConfig().setRetentionTimeUnit("DAYS");
    tableConfig.getValidationConfig().setRetentionTimeValue("100000");
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(
        new StreamIngestionConfig(List.of(tableConfig.getIndexingConfig().getStreamConfigs())));
    ingestionConfig.getStreamIngestionConfig().setPauselessConsumptionEnabled(true);
    tableConfig.getIndexingConfig().setStreamConfigs(null);
    tableConfig.setIngestionConfig(ingestionConfig);
    return tableConfig;
  }

  private void setupNonPauselessTable()
      throws Exception {
    _avroFiles = unpackAvroData(_tempDir);
    startKafka();
    pushAvroIntoKafka(_avroFiles);

    Schema schema = createSchema();
    schema.setSchemaName(DEFAULT_TABLE_NAME_2);
    addSchema(schema);

    TableConfig tableConfig = createRealtimeTableConfig(_avroFiles.get(0));
    tableConfig.setTableName(DEFAULT_TABLE_NAME_2);
    tableConfig.getValidationConfig().setRetentionTimeUnit("DAYS");
    tableConfig.getValidationConfig().setRetentionTimeValue("100000");
    addTableConfig(tableConfig);

    waitForDocsLoaded(600_000L, true, tableConfig.getTableName());
    TestUtils.waitForCondition((aVoid) -> {
      List<SegmentZKMetadata> segmentZKMetadataList =
          _helixResourceManager.getSegmentsZKMetadata(tableConfig.getTableName());
      return PauselessRealtimeTestUtils.assertUrlPresent(segmentZKMetadataList);
    }, 1000, 100000, "Some segments still have missing url");
  }

  private void setupPauselessTable()
      throws Exception {
    Schema schema = createSchema();
    schema.setSchemaName(DEFAULT_TABLE_NAME);
    addSchema(schema);

    TableConfig tableConfig = getPauselessTableConfig();
    addTableConfig(tableConfig);
  }

  private void setMaxSegmentCompletionTimeMillis() {
    PinotLLCRealtimeSegmentManager realtimeSegmentManager = _helixResourceManager.getRealtimeSegmentManager();
    if (realtimeSegmentManager instanceof FailureInjectingPinotLLCRealtimeSegmentManager) {
      ((FailureInjectingPinotLLCRealtimeSegmentManager) realtimeSegmentManager)
          .setMaxSegmentCompletionTimeoutMs(MAX_SEGMENT_COMPLETION_TIME_MILLIS);
    }
  }

  protected void injectFailure() {
    PinotLLCRealtimeSegmentManager realtimeSegmentManager = _helixResourceManager.getRealtimeSegmentManager();
    if (realtimeSegmentManager instanceof FailureInjectingPinotLLCRealtimeSegmentManager) {
      ((FailureInjectingPinotLLCRealtimeSegmentManager) realtimeSegmentManager).enableTestFault(getFailurePoint());
    }
    _failureEnabled = true;
  }

  protected void disableFailure() {
    _failureEnabled = false;
    PinotLLCRealtimeSegmentManager realtimeSegmentManager = _helixResourceManager.getRealtimeSegmentManager();
    if (realtimeSegmentManager instanceof FailureInjectingPinotLLCRealtimeSegmentManager) {
      ((FailureInjectingPinotLLCRealtimeSegmentManager) realtimeSegmentManager).disableTestFault(getFailurePoint());
    }
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

  protected long getCountStarResult() {
    return _failureEnabled ? getCountStarResultWithFailure() : DEFAULT_COUNT_STAR_RESULT;
  }

  protected void runValidationAndVerify()
      throws Exception {
    String tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    String tableNameWithType2 = TableNameBuilder.REALTIME.tableNameWithType(DEFAULT_TABLE_NAME_2);

    PauselessRealtimeTestUtils.verifyIdealState(tableNameWithType, getExpectedSegmentsWithFailure(), _helixManager);

    TestUtils.waitForCondition((aVoid) -> {
      List<SegmentZKMetadata> segmentZKMetadataList =
          _helixResourceManager.getSegmentsZKMetadata(tableNameWithType);
      return segmentZKMetadataList.size() == getExpectedZKMetadataWithFailure();
    }, 1000, 100000, "New Segment ZK Metadata not created");

    Thread.sleep(MAX_SEGMENT_COMPLETION_TIME_MILLIS);
    disableFailure();

    _controllerStarter.getRealtimeSegmentValidationManager().run();

    waitForAllDocsLoaded(600_000L);
    waitForDocsLoaded(600_000L, true, tableNameWithType2);

    PauselessRealtimeTestUtils.verifyIdealState(tableNameWithType, NUM_REALTIME_SEGMENTS, _helixManager);
    PauselessRealtimeTestUtils.verifyIdealState(tableNameWithType2, NUM_REALTIME_SEGMENTS, _helixManager);

    TestUtils.waitForCondition((aVoid) -> {
      List<SegmentZKMetadata> segmentZKMetadataList =
          _helixResourceManager.getSegmentsZKMetadata(tableNameWithType);
      return PauselessRealtimeTestUtils.assertUrlPresent(segmentZKMetadataList);
    }, 1000, 100000, "Some segments still have missing url");

    PauselessRealtimeTestUtils.compareZKMetadataForSegments(
        _helixResourceManager.getSegmentsZKMetadata(tableNameWithType),
        _helixResourceManager.getSegmentsZKMetadata(tableNameWithType2));
  }

  /**
   * Basic test to verify segment assignment and metadata without any failures
   */
  protected void testBasicSegmentAssignment() {
    String tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(getTableName());

    PauselessRealtimeTestUtils.verifyIdealState(tableNameWithType, NUM_REALTIME_SEGMENTS, _helixManager);
    assertTrue(PauselessConsumptionUtils.isPauselessEnabled(getRealtimeTableConfig()));

    TestUtils.waitForCondition((aVoid) -> {
      List<SegmentZKMetadata> segmentZKMetadataList = _helixResourceManager.getSegmentsZKMetadata(tableNameWithType);
      return !hasSegmentsInStatus(segmentZKMetadataList, CommonConstants.Segment.Realtime.Status.COMMITTING);
    }, 1000, 100000, "Some segments have status COMMITTING");

    TestUtils.waitForCondition((aVoid) -> {
      List<SegmentZKMetadata> segmentZKMetadataList = _helixResourceManager.getSegmentsZKMetadata(tableNameWithType);
      return PauselessRealtimeTestUtils.assertUrlPresent(segmentZKMetadataList);
    }, 1000, 100000, "Some segments still have missing url");
  }

  private boolean hasSegmentsInStatus(List<SegmentZKMetadata> segmentZKMetadataList,
      CommonConstants.Segment.Realtime.Status prohibitedStatus) {
    for (SegmentZKMetadata segmentZKMetadata : segmentZKMetadataList) {
      if (segmentZKMetadata.getStatus() == prohibitedStatus) {
        return true;
      }
    }
    return false;
  }
}
