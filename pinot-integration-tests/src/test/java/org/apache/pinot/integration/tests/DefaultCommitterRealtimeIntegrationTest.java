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
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.controller.helix.core.realtime.SegmentCompletionManager;
import org.apache.pinot.core.data.manager.realtime.LLRealtimeSegmentDataManager;
import org.apache.pinot.core.data.manager.realtime.SegmentCommitter;
import org.apache.pinot.core.data.manager.realtime.SegmentCommitterFactory;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.segment.creator.SegmentIndexCreationDriver;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.core.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.server.realtime.ControllerLeaderLocator;
import org.apache.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


/**
 * Integration test that extends RealtimeClusterIntegrationTest but uses low-level Kafka consumer.
 * TODO: Add separate module-level tests and remove the randomness of this test
 */
public class DefaultCommitterRealtimeIntegrationTest extends RealtimeClusterIntegrationTest {
  private static final String CONSUMER_DIRECTORY = "/tmp/consumer-test";
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "SegmentCommitterTest");
  private static final String AVRO_DATA = "data" + File.separator + "test_data-sv.avro";
  private final boolean _enableSplitCommit = false;
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultCommitterRealtimeIntegrationTest.class);
  private final long END_OFFSET = 500L;
  private File TEMP_FILE;
  private static final String SEGMENT_NAME = "testTable_126164076_167572854";
  private File untarredSegmentFile = new File(INDEX_DIR, SEGMENT_NAME);
  private File _realtimeSegmentUntarred;

  @BeforeClass
  @Override
  public void setUp()
      throws Exception {
    TEMP_FILE = File.createTempFile("SegmentCommitterTest", null);
    // Remove the consumer directory
    File consumerDirectory = new File(CONSUMER_DIRECTORY);
    if (consumerDirectory.exists()) {
      FileUtils.deleteDirectory(consumerDirectory);
    }

    startZk();
    startController();
    addFakeBrokerInstancesToAutoJoinHelixCluster(1, true);
    addFakeServerInstancesToAutoJoinHelixCluster(1, true);

    ControllerLeaderLocator.create(_helixManager);

    // Start Kafka
    startKafka();
    List<File> avroFiles = unpackAvroData(_tempDir);

    setUpRealtimeTable(avroFiles.get(0));
  }

  @Override
  protected boolean useLlc() {
    return true;
  }

  @Test
  public void testDefaultCommitter() {
    IndexLoadingConfig indexLoadingConfig = mock(IndexLoadingConfig.class);
    ServerMetrics serverMetrics = mock(ServerMetrics.class);
    doNothing().when(serverMetrics).addMeteredGlobalValue(ServerMeter.LLC_CONTROLLER_RESPONSE_COMMIT_SUCCESS, 1);
    ServerSegmentCompletionProtocolHandler
        protocolHandler = new ServerSegmentCompletionProtocolHandler(serverMetrics, getTableName());
    SegmentCompletionProtocol.Request.Params params = mock(SegmentCompletionProtocol.Request.Params.class);
    SegmentCompletionProtocol.Response prevResponse = mock(SegmentCompletionProtocol.Response.class);
    LLRealtimeSegmentDataManager.SegmentBuildDescriptor segmentBuildDescriptor = mock(
        LLRealtimeSegmentDataManager.SegmentBuildDescriptor.class);

    RealtimeSegmentZKMetadata metadata = _helixResourceManager.getRealtimeSegmentMetadata(getTableName()).get(0);

    when(params.getSegmentName()).thenReturn(metadata.getSegmentName());
    when(params.getInstanceId()).thenReturn("Server_localhost_0");
    when(params.getOffset()).thenReturn(END_OFFSET);
    when(segmentBuildDescriptor.getSegmentTarFilePath()).thenReturn(_realtimeSegmentUntarred + ".tar.gz");
    when(segmentBuildDescriptor.getBuildTimeMillis()).thenReturn(0L);
    Map<String, File> metadataPaths = new HashMap();
    metadataPaths.put(metadata.getSegmentName(), new File(untarredSegmentFile, "metadata.properties"));

    when(segmentBuildDescriptor.getMetadataFiles()).thenReturn(metadataPaths);
    when(segmentBuildDescriptor.getOffset()).thenReturn(END_OFFSET);
    when(segmentBuildDescriptor.getSegmentSizeBytes()).thenReturn(0L);
    when(segmentBuildDescriptor.getWaitTimeMillis()).thenReturn(0L);

    SegmentCompletionManager segmentCompletionManager = _controllerStarter.getSegmentCompletionManager();
    segmentCompletionManager.segmentConsumed(params);
    SegmentCommitterFactory
        segmentCommitterFactory = new SegmentCommitterFactory(LOGGER, indexLoadingConfig, protocolHandler);
    SegmentCommitter segmentCommitter = segmentCommitterFactory.createDefaultSegmentCommitter(params, prevResponse);
    segmentCommitter.commit(END_OFFSET, 3, segmentBuildDescriptor);
  }

  @Override
  public void testDictionaryBasedQueries()
      throws Exception {
  }

  @Override
  public void testGeneratedQueriesWithMultiValues()
      throws Exception {

  }

  @Override
  public void testHardcodedSqlQueries()
      throws Exception {
  }

  @Override
  public void testInstanceShutdown()
      throws Exception {
  }

  @Override
  public void testQueriesFromQueryFile()
      throws Exception {
  }

  @Override
  public void testSqlQueriesFromQueryFile()
      throws Exception {
  }

  @Override
  public void testQueryExceptions()
      throws Exception {
  }

  @BeforeTest
  public void buildSegment()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    // Get resource file path.
    URL resource = getClass().getClassLoader().getResource(AVRO_DATA);
    Assert.assertNotNull(resource);
    String filePath = resource.getFile();

    // Build the segment schema.
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable").addMetric("column1", FieldSpec.DataType.INT)
        .addMetric("column3", FieldSpec.DataType.INT).addSingleValueDimension("column5", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column6", FieldSpec.DataType.INT)
        .addSingleValueDimension("column7", FieldSpec.DataType.INT)
        .addSingleValueDimension("column9", FieldSpec.DataType.INT)
        .addSingleValueDimension("column11", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column12", FieldSpec.DataType.STRING).addMetric("column17", FieldSpec.DataType.INT)
        .addMetric("column18", FieldSpec.DataType.INT).addTime("daysSinceEpoch", TimeUnit.DAYS, FieldSpec.DataType.INT)
        .build();

    // Create the segment generator config.
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(schema);
    segmentGeneratorConfig.setInputFilePath(filePath);
    segmentGeneratorConfig.setTableName("testTable");
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getAbsolutePath());
    // The segment generation code in SegmentColumnarIndexCreator will throw
    // exception if start and end time in time column are not in acceptable
    // range. For this test, we first need to fix the input avro data
    // to have the time column values in allowed range. Until then, the check
    // is explicitly disabled
    segmentGeneratorConfig.setSkipTimeValueCheck(true);
    segmentGeneratorConfig
        .setInvertedIndexCreationColumns(Arrays.asList("column6", "column7", "column11", "column17", "column18"));

    // Build the index segment.
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig);
    driver.build();

    untarredSegmentFile.getParent();
    _realtimeSegmentUntarred = new File(untarredSegmentFile.getParentFile(), "testTable__1__1__19700101T0000Z");
    FileUtils.copyDirectory(untarredSegmentFile, _realtimeSegmentUntarred);

    TarGzCompressionUtils.createTarGzOfDirectory(_realtimeSegmentUntarred.getAbsolutePath());
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    untarredSegmentFile.deleteOnExit();
    _realtimeSegmentUntarred.deleteOnExit();
  }

}
