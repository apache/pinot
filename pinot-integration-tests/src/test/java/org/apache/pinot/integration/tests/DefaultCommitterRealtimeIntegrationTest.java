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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.io.Files;
import com.yammer.metrics.core.MetricsRegistry;
import java.io.File;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.core.data.manager.realtime.LLRealtimeSegmentDataManager;
import org.apache.pinot.core.data.manager.realtime.SegmentCommitter;
import org.apache.pinot.core.data.manager.realtime.SegmentCommitterFactory;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.core.data.readers.PinotSegmentUtil;
import org.apache.pinot.server.realtime.ControllerLeaderLocator;
import org.apache.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Integration test that extends RealtimeClusterIntegrationTest to test default commit but uses low-level Kafka consumer.
 */
public class DefaultCommitterRealtimeIntegrationTest extends RealtimeClusterIntegrationTest {
  private File _indexDir;
  private File _realtimeSegmentUntarred;

  private static final String TARGZ_SUFFIX = ".tar.gz";
  private static final long END_OFFSET = 500L;
  private static final String CONSUMER_DIRECTORY = "/tmp/consumer-test";
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultCommitterRealtimeIntegrationTest.class);

  @BeforeClass
  @Override
  public void setUp()
      throws Exception {
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
    buildSegment();
  }

  @Override
  protected boolean useLlc() {
    return true;
  }

  @Test
  public void testDefaultCommitter()
      throws Exception {
    ServerMetrics serverMetrics = new ServerMetrics(new MetricsRegistry());
    ServerSegmentCompletionProtocolHandler protocolHandler =
        new ServerSegmentCompletionProtocolHandler(serverMetrics, getTableName());

    SegmentCompletionProtocol.Response prevResponse = new SegmentCompletionProtocol.Response();
    LLRealtimeSegmentDataManager.SegmentBuildDescriptor segmentBuildDescriptor =
        mock(LLRealtimeSegmentDataManager.SegmentBuildDescriptor.class);

    RealtimeSegmentZKMetadata metadata = _helixResourceManager.getRealtimeSegmentMetadata(getTableName()).get(0);

    String instanceId = _helixResourceManager.getAllInstances().get(0);

    SegmentCompletionProtocol.Request.Params params = new SegmentCompletionProtocol.Request.Params();
    params.withSegmentName(metadata.getSegmentName()).withInstanceId(instanceId).withOffset(END_OFFSET);

    _realtimeSegmentUntarred = new File(_indexDir.getParentFile(), metadata.getSegmentName());
    FileUtils.copyDirectory(_indexDir, _realtimeSegmentUntarred);

    TarGzCompressionUtils.createTarGzOfDirectory(_realtimeSegmentUntarred.getAbsolutePath());

    // SegmentBuildDescriptor is currently not a static class, so we will mock this object.
    when(segmentBuildDescriptor.getSegmentTarFilePath()).thenReturn(_realtimeSegmentUntarred + TARGZ_SUFFIX);
    when(segmentBuildDescriptor.getBuildTimeMillis()).thenReturn(0L);
    when(segmentBuildDescriptor.getOffset()).thenReturn(END_OFFSET);
    when(segmentBuildDescriptor.getSegmentSizeBytes()).thenReturn(0L);
    when(segmentBuildDescriptor.getWaitTimeMillis()).thenReturn(0L);

    // Get realtime segment name
    String segmentList =
        sendGetRequest(_controllerRequestURLBuilder.forSegmentListAPIWithTableType(getTableName(), "REALTIME"));
    JsonNode realtimeSegmentsList = getSegmentsFromJsonSegmentAPI(segmentList, TableType.REALTIME.toString());
    String segmentName = realtimeSegmentsList.get(0).asText();

    // Send segmentConsumed request
    sendGetRequest("http://localhost:" + DEFAULT_CONTROLLER_PORT + "/segmentConsumed?instance=" + instanceId + "&name="
        + segmentName + "&offset=" + END_OFFSET);

    SegmentCommitterFactory segmentCommitterFactory =
        new SegmentCommitterFactory(LOGGER, mock(TableConfig.class), protocolHandler);
    SegmentCommitter segmentCommitter = segmentCommitterFactory.createDefaultSegmentCommitter(params);
    segmentCommitter.commit(END_OFFSET, 3, segmentBuildDescriptor);
  }

  public void buildSegment()
      throws Exception {
    Schema schema = _helixResourceManager.getSchema(getTableName());
    String _segmentOutputDir = Files.createTempDir().toString();
    List<GenericRow> _rows = PinotSegmentUtil.createTestData(schema, 1);
    RecordReader _recordReader = new GenericRowRecordReader(_rows, schema);

    _indexDir = PinotSegmentUtil.createSegment(schema, "segmentName", _segmentOutputDir, _recordReader);
  }

  private JsonNode getSegmentsFromJsonSegmentAPI(String json, String type)
      throws Exception {
    return JsonUtils.stringToJsonNode(json).get(0).get(type);
  }

  @Override
  public void tearDown()
      throws Exception {
    super.tearDown();
    _indexDir.deleteOnExit();
    _realtimeSegmentUntarred.deleteOnExit();
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
  public void testQueryExceptions()
      throws Exception {
  }

  @Override
  public void testSqlQueriesFromQueryFile()
      throws Exception {
  }
}
