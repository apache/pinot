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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol.ControllerResponseStatus;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.controller.BaseControllerStarter;
import org.apache.pinot.core.data.manager.realtime.SegmentCompletionUtils;
import org.apache.pinot.integration.tests.realtime.utils.FailureInjectingControllerStarter;
import org.apache.pinot.integration.tests.realtime.utils.FailureInjectingPinotLLCRealtimeSegmentManager;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.server.realtime.ControllerLeaderLocator;
import org.apache.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import org.apache.pinot.server.starter.helix.SegmentOnlineOfflineStateModelFactory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// Runs the segment completion protocol between a fake server and a real controller, with a short segment commit
/// timeout, to check what the controller FSM does with a request that arrives after its commit deadline.
///
/// The commit deadline is the FSM start plus the build time the controller gives the committer with the COMMIT
/// response. The committer asks for more build time while it builds the segment: a blocking committer before its commit
/// start, a pauseless committer after it.
public class SegmentCompletionDeadlineIntegrationTest extends BaseClusterIntegrationTest {
  private static final String PAUSELESS_TABLE = "pauselessLateExtension";
  private static final String PAUSELESS_LATE_REPORT_TABLE = "pauselessLateReport";
  private static final String PAUSELESS_LATE_COMMIT_START_TABLE = "pauselessLateCommitStart";
  private static final String PAUSELESS_RESUME_TABLE = "pauselessResumedCommitStart";
  private static final String PAUSELESS_EXPIRED_TABLE = "pauselessExpiredCommitEnd";
  private static final String PAUSELESS_REPAIRED_TABLE = "pauselessRepairedCommitEnd";
  private static final String BLOCKING_TABLE = "blockingLateExtension";
  private static final List<String> TABLES =
      List.of(PAUSELESS_TABLE, PAUSELESS_LATE_REPORT_TABLE, PAUSELESS_LATE_COMMIT_START_TABLE, PAUSELESS_RESUME_TABLE,
          PAUSELESS_EXPIRED_TABLE, PAUSELESS_REPAIRED_TABLE, BLOCKING_TABLE);
  // Not a server of the cluster: the FSM takes any reporting server as a replica
  private static final String OTHER_SERVER_INSTANCE = "Server_otherReplica_8098";
  private static final int COMMIT_TIMEOUT_SECONDS = 1;
  private static final LongMsgOffset END_OFFSET = new LongMsgOffset(100L);
  // What SegmentBuildTimeLeaseExtender asks for
  private static final int EXTRA_BUILD_TIME_SECONDS = 120;

  private String _serverInstance;
  private HelixManager _serverHelixManager;

  @Override
  protected int getNumKafkaPartitions() {
    return 1;
  }

  @Override
  public BaseControllerStarter createControllerStarter() {
    return new FailureInjectingControllerStarter();
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);
    startZk();
    startController();
    startBroker();
    startFakeServer();
    startKafka();
    for (String rawTableName : TABLES) {
      Schema schema = createSchema();
      schema.setSchemaName(rawTableName);
      addSchema(schema);
      addTableConfig(createTableConfig(rawTableName, !rawTableName.equals(BLOCKING_TABLE)));
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    for (String rawTableName : TABLES) {
      dropRealtimeTable(rawTableName);
    }
    _serverHelixManager.disconnect();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  /// A pauseless commit start moves the segment to COMMITTING and creates the next consuming segment, so no other
  /// replica can take the commit over. A build time extension from another server, or for another offset, fails
  /// without aborting the FSM. A build time extension from the committer that arrives after the commit deadline is
  /// accepted, and so is the commit end that follows it.
  @Test
  public void testPauselessLateBuildTimeExtension()
      throws Exception {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(PAUSELESS_TABLE);
    String segmentName = getConsumingSegment(realtimeTableName);
    ServerSegmentCompletionProtocolHandler protocolHandler = createProtocolHandler(realtimeTableName);

    // The server is the only replica, so its first report makes it the committer
    SegmentCompletionProtocol.Response commitResponse = protocolHandler.segmentConsumed(params(segmentName));
    long commitResponseTimeMs = System.currentTimeMillis();
    assertEquals(commitResponse.getStatus(), ControllerResponseStatus.COMMIT);
    assertEquals(protocolHandler.segmentCommitStart(params(segmentName)).getStatus(),
        ControllerResponseStatus.COMMIT_CONTINUE);

    // Before the commit deadline, an extension from another server or for another offset fails. It does not move the
    // commit deadline, so the extensions below still arrive after it.
    assertEquals(protocolHandler.extendBuildTime(
        extendBuildTimeParams(segmentName).withInstanceId(OTHER_SERVER_INSTANCE)).getStatus(),
        ControllerResponseStatus.FAILED);
    assertEquals(protocolHandler.extendBuildTime(extendBuildTimeParams(segmentName).withStreamPartitionMsgOffset(
        new LongMsgOffset(END_OFFSET.getOffset() + 1).toString())).getStatus(), ControllerResponseStatus.FAILED);

    // The server builds the segment and asks for more build time after the commit deadline, e.g. because its commit
    // start took long or the controller handled the request late. It sends the extension again, as a retry after any
    // answer other than PROCESSED or as the next periodic extension, then uploads the segment and ends the commit.
    waitPastCommitDeadline(commitResponseTimeMs, commitResponse.getBuildTimeSeconds());
    List<ControllerResponseStatus> responses = new ArrayList<>();
    responses.add(protocolHandler.extendBuildTime(extendBuildTimeParams(segmentName)).getStatus());
    responses.add(protocolHandler.extendBuildTime(extendBuildTimeParams(segmentName)).getStatus());
    responses.add(commitEnd(protocolHandler, segmentName, uploadSegmentFile(PAUSELESS_TABLE, segmentName)).getStatus());
    assertEquals(responses, List.of(ControllerResponseStatus.PROCESSED, ControllerResponseStatus.PROCESSED,
        ControllerResponseStatus.COMMIT_SUCCESS), "Responses: " + responses);

    SegmentZKMetadata segmentZKMetadata = _helixResourceManager.getSegmentZKMetadata(realtimeTableName, segmentName);
    assertNotNull(segmentZKMetadata);
    assertEquals(segmentZKMetadata.getStatus(), Status.DONE);
    assertTrue(getDeepStoreSegmentFile(PAUSELESS_TABLE, segmentName).exists());
  }

  /// After the commit start, a report from another replica that arrives after the commit deadline does not abort the
  /// FSM either: the replica keeps its own build, and the commit end of the committer still succeeds.
  @Test
  public void testPauselessLateReportFromOtherReplica()
      throws Exception {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(PAUSELESS_LATE_REPORT_TABLE);
    String segmentName = getConsumingSegment(realtimeTableName);
    ServerSegmentCompletionProtocolHandler protocolHandler = createProtocolHandler(realtimeTableName);

    SegmentCompletionProtocol.Response commitResponse = protocolHandler.segmentConsumed(params(segmentName));
    long commitResponseTimeMs = System.currentTimeMillis();
    assertEquals(commitResponse.getStatus(), ControllerResponseStatus.COMMIT);
    assertEquals(protocolHandler.segmentCommitStart(params(segmentName)).getStatus(),
        ControllerResponseStatus.COMMIT_CONTINUE);

    // Another replica reaches the end offset and reports it after the commit deadline, then the committer ends the
    // commit
    waitPastCommitDeadline(commitResponseTimeMs, commitResponse.getBuildTimeSeconds());
    List<ControllerResponseStatus> responses = new ArrayList<>();
    responses.add(
        protocolHandler.segmentConsumed(params(segmentName).withInstanceId(OTHER_SERVER_INSTANCE)).getStatus());
    File uploadedSegmentFile = uploadSegmentFile(PAUSELESS_LATE_REPORT_TABLE, segmentName);
    responses.add(commitEnd(protocolHandler, segmentName, uploadedSegmentFile).getStatus());
    assertEquals(responses, List.of(ControllerResponseStatus.KEEP, ControllerResponseStatus.COMMIT_SUCCESS),
        "Responses: " + responses);
  }

  /// Before the commit start the segment is still IN_PROGRESS, so a commit start that arrives after the commit deadline
  /// still aborts the FSM, and a committer is picked again.
  @Test
  public void testPauselessLateCommitStartAborts()
      throws Exception {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(PAUSELESS_LATE_COMMIT_START_TABLE);
    String segmentName = getConsumingSegment(realtimeTableName);
    ServerSegmentCompletionProtocolHandler protocolHandler = createProtocolHandler(realtimeTableName);

    SegmentCompletionProtocol.Response commitResponse = protocolHandler.segmentConsumed(params(segmentName));
    long commitResponseTimeMs = System.currentTimeMillis();
    assertEquals(commitResponse.getStatus(), ControllerResponseStatus.COMMIT);

    // The committer starts the commit after the commit deadline, then reports its offset again
    waitPastCommitDeadline(commitResponseTimeMs, commitResponse.getBuildTimeSeconds());
    List<ControllerResponseStatus> responses = new ArrayList<>();
    responses.add(protocolHandler.segmentCommitStart(params(segmentName)).getStatus());
    responses.add(protocolHandler.segmentConsumed(params(segmentName)).getStatus());
    assertEquals(responses, List.of(ControllerResponseStatus.HOLD, ControllerResponseStatus.COMMIT),
        "Responses: " + responses);
    SegmentZKMetadata segmentZKMetadata = _helixResourceManager.getSegmentZKMetadata(realtimeTableName, segmentName);
    assertNotNull(segmentZKMetadata);
    assertEquals(segmentZKMetadata.getStatus(), Status.IN_PROGRESS);
  }

  /// A committer whose commit start timed out on its side reports its offset again. That report still aborts the FSM
  /// after the commit start, so that the committer is picked again and its next commit start resumes the commit.
  @Test
  public void testPauselessCommitResumesAfterCommitterReportsAgain()
      throws Exception {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(PAUSELESS_RESUME_TABLE);
    String segmentName = getConsumingSegment(realtimeTableName);
    ServerSegmentCompletionProtocolHandler protocolHandler = createProtocolHandler(realtimeTableName);

    assertEquals(protocolHandler.segmentConsumed(params(segmentName)).getStatus(), ControllerResponseStatus.COMMIT);
    // The server does not get this response
    assertEquals(protocolHandler.segmentCommitStart(params(segmentName)).getStatus(),
        ControllerResponseStatus.COMMIT_CONTINUE);

    List<ControllerResponseStatus> responses = new ArrayList<>();
    responses.add(protocolHandler.segmentConsumed(params(segmentName)).getStatus());
    responses.add(protocolHandler.segmentConsumed(params(segmentName)).getStatus());
    responses.add(protocolHandler.segmentCommitStart(params(segmentName)).getStatus());
    responses.add(
        commitEnd(protocolHandler, segmentName, uploadSegmentFile(PAUSELESS_RESUME_TABLE, segmentName)).getStatus());
    assertEquals(responses, List.of(ControllerResponseStatus.HOLD, ControllerResponseStatus.COMMIT,
        ControllerResponseStatus.COMMIT_CONTINUE, ControllerResponseStatus.COMMIT_SUCCESS), "Responses: " + responses);
    SegmentZKMetadata segmentZKMetadata = _helixResourceManager.getSegmentZKMetadata(realtimeTableName, segmentName);
    assertNotNull(segmentZKMetadata);
    assertEquals(segmentZKMetadata.getStatus(), Status.DONE);
  }

  /// After the max segment completion time, the validation manager repairs a COMMITTING segment: it uploads the copy of
  /// another replica to the deep store path of the segment and marks the segment DONE. A commit end that arrives after
  /// the repair fails without replacing the repaired copy.
  @Test
  public void testPauselessCommitEndAfterRepair()
      throws Exception {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(PAUSELESS_REPAIRED_TABLE);
    String segmentName = getConsumingSegment(realtimeTableName);
    ServerSegmentCompletionProtocolHandler protocolHandler = createProtocolHandler(realtimeTableName);

    assertEquals(protocolHandler.segmentConsumed(params(segmentName)).getStatus(), ControllerResponseStatus.COMMIT);
    assertEquals(protocolHandler.segmentCommitStart(params(segmentName)).getStatus(),
        ControllerResponseStatus.COMMIT_CONTINUE);

    // The repair
    File repairedSegmentFile = getDeepStoreSegmentFile(PAUSELESS_REPAIRED_TABLE, segmentName);
    FileUtils.writeStringToFile(repairedSegmentFile, "repaired", StandardCharsets.UTF_8);
    SegmentZKMetadata segmentZKMetadata = _helixResourceManager.getSegmentZKMetadata(realtimeTableName, segmentName);
    assertNotNull(segmentZKMetadata);
    segmentZKMetadata.setStatus(Status.DONE);
    assertTrue(_helixResourceManager.updateZkMetadata(realtimeTableName, segmentZKMetadata));

    File uploadedSegmentFile = uploadSegmentFile(PAUSELESS_REPAIRED_TABLE, segmentName);
    assertEquals(commitEnd(protocolHandler, segmentName, uploadedSegmentFile).getStatus(),
        ControllerResponseStatus.FAILED);
    assertTrue(uploadedSegmentFile.exists());
    assertEquals(FileUtils.readFileToString(repairedSegmentFile, StandardCharsets.UTF_8), "repaired");
  }

  /// A blocking committer builds the segment before its commit start, while the segment is still IN_PROGRESS and
  /// another replica can commit it. A build time extension that arrives after the commit deadline still aborts the FSM.
  @Test
  public void testBlockingLateBuildTimeExtension()
      throws Exception {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(BLOCKING_TABLE);
    String segmentName = getConsumingSegment(realtimeTableName);
    ServerSegmentCompletionProtocolHandler protocolHandler = createProtocolHandler(realtimeTableName);

    SegmentCompletionProtocol.Response commitResponse = protocolHandler.segmentConsumed(params(segmentName));
    long commitResponseTimeMs = System.currentTimeMillis();
    assertEquals(commitResponse.getStatus(), ControllerResponseStatus.COMMIT);

    // The server asks for more build time after the commit deadline, retries the extension, then starts the commit.
    // The late extension aborts the FSM (HOLD). The retry and the commit start reach a new FSM that has no committer
    // yet (FAILED, HOLD), so the server goes back to reporting its offset and a committer is picked again.
    waitPastCommitDeadline(commitResponseTimeMs, commitResponse.getBuildTimeSeconds());
    List<ControllerResponseStatus> responses = new ArrayList<>();
    responses.add(protocolHandler.extendBuildTime(extendBuildTimeParams(segmentName)).getStatus());
    responses.add(protocolHandler.extendBuildTime(extendBuildTimeParams(segmentName)).getStatus());
    responses.add(protocolHandler.segmentCommitStart(params(segmentName)).getStatus());
    assertEquals(responses, List.of(ControllerResponseStatus.HOLD, ControllerResponseStatus.FAILED,
        ControllerResponseStatus.HOLD), "Responses: " + responses);
  }

  /// After the max segment completion time, the validation manager can repair a COMMITTING segment by uploading the
  /// copy of another replica to the deep store path of the segment. A pauseless commit end that arrives after that time
  /// fails before it moves its upload to that path.
  @Test
  public void testPauselessCommitEndAfterMaxSegmentCompletionTime()
      throws Exception {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(PAUSELESS_EXPIRED_TABLE);
    String segmentName = getConsumingSegment(realtimeTableName);
    ServerSegmentCompletionProtocolHandler protocolHandler = createProtocolHandler(realtimeTableName);

    assertEquals(protocolHandler.segmentConsumed(params(segmentName)).getStatus(), ControllerResponseStatus.COMMIT);
    assertEquals(protocolHandler.segmentCommitStart(params(segmentName)).getStatus(),
        ControllerResponseStatus.COMMIT_CONTINUE);

    FailureInjectingPinotLLCRealtimeSegmentManager segmentManager =
        (FailureInjectingPinotLLCRealtimeSegmentManager) _helixResourceManager.getRealtimeSegmentManager();
    segmentManager.forceExpireSegments(List.of(segmentName));
    try {
      File uploadedSegmentFile = uploadSegmentFile(PAUSELESS_EXPIRED_TABLE, segmentName);
      assertEquals(commitEnd(protocolHandler, segmentName, uploadedSegmentFile).getStatus(),
          ControllerResponseStatus.FAILED);
      assertTrue(uploadedSegmentFile.exists());
      assertFalse(getDeepStoreSegmentFile(PAUSELESS_EXPIRED_TABLE, segmentName).exists());
      SegmentZKMetadata segmentZKMetadata = _helixResourceManager.getSegmentZKMetadata(realtimeTableName, segmentName);
      assertNotNull(segmentZKMetadata);
      assertEquals(segmentZKMetadata.getStatus(), Status.COMMITTING);
    } finally {
      segmentManager.clearForceExpiredSegments();
    }
  }

  private TableConfig createTableConfig(String rawTableName, boolean pauseless) {
    Map<String, String> streamConfigMap = getStreamConfigMap();
    streamConfigMap.put(StreamConfigProperties.SEGMENT_COMMIT_TIMEOUT_SECONDS,
        Integer.toString(COMMIT_TIMEOUT_SECONDS));
    StreamIngestionConfig streamIngestionConfig = new StreamIngestionConfig(List.of(streamConfigMap));
    streamIngestionConfig.setPauselessConsumptionEnabled(pauseless);
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(streamIngestionConfig);
    return new TableConfigBuilder(TableType.REALTIME).setTableName(rawTableName).setTimeColumnName(getTimeColumnName())
        .setNumReplicas(1).setIngestionConfig(ingestionConfig).build();
  }

  /// Starts a server that only takes part in Helix, so that the test can send the segment completion requests.
  private void startFakeServer()
      throws Exception {
    _serverInstance = CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE + NetUtils.getHostAddress() + "_"
        + CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT;
    _serverHelixManager =
        HelixManagerFactory.getZKHelixManager(getHelixClusterName(), _serverInstance, InstanceType.PARTICIPANT,
            getZkUrl());
    _serverHelixManager.getStateMachineEngine()
        .registerStateModelFactory(SegmentOnlineOfflineStateModelFactory.getStateModelName(),
            new FakeServerSegmentStateModelFactory());
    _serverHelixManager.connect();
    _serverHelixManager.getClusterManagmentTool().addInstanceTag(getHelixClusterName(), _serverInstance,
        TableNameBuilder.REALTIME.tableNameWithType(TagNameUtils.DEFAULT_TENANT_NAME));
    ControllerLeaderLocator.create(_serverHelixManager);
  }

  private String getConsumingSegment(String realtimeTableName) {
    IdealState idealState = _helixResourceManager.getTableIdealState(realtimeTableName);
    assertNotNull(idealState);
    for (Map.Entry<String, Map<String, String>> entry : idealState.getRecord().getMapFields().entrySet()) {
      if (entry.getValue().containsValue(SegmentStateModel.CONSUMING)) {
        return entry.getKey();
      }
    }
    throw new AssertionError("Failed to find the consuming segment of table: " + realtimeTableName);
  }

  private static ServerSegmentCompletionProtocolHandler createProtocolHandler(String realtimeTableName) {
    return new ServerSegmentCompletionProtocolHandler(new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()),
        realtimeTableName);
  }

  private SegmentCompletionProtocol.Request.Params params(String segmentName) {
    return new SegmentCompletionProtocol.Request.Params().withInstanceId(_serverInstance).withSegmentName(segmentName)
        .withStreamPartitionMsgOffset(END_OFFSET.toString());
  }

  private SegmentCompletionProtocol.Request.Params extendBuildTimeParams(String segmentName) {
    return params(segmentName).withExtraTimeSec(EXTRA_BUILD_TIME_SECONDS);
  }

  /// The FSM started before the COMMIT response, and its commit deadline is less than one second past the build time
  /// in that response.
  private static void waitPastCommitDeadline(long commitResponseTimeMs, long buildTimeSeconds)
      throws InterruptedException {
    long waitMs = commitResponseTimeMs + (buildTimeSeconds + 1) * 1000L - System.currentTimeMillis();
    if (waitMs > 0) {
      Thread.sleep(waitMs);
    }
  }

  /// Places a segment file where the server uploads it for a split commit: a temporary name under the table directory
  /// of the deep store. The controller moves the file without reading it.
  private File uploadSegmentFile(String rawTableName, String segmentName)
      throws IOException {
    File uploadedSegmentFile = new File(new File(_controllerDataDir, rawTableName),
        SegmentCompletionUtils.generateTmpSegmentFileName(segmentName));
    FileUtils.writeStringToFile(uploadedSegmentFile, segmentName, StandardCharsets.UTF_8);
    return uploadedSegmentFile;
  }

  private File getDeepStoreSegmentFile(String rawTableName, String segmentName) {
    return new File(new File(_controllerDataDir, rawTableName), segmentName);
  }

  private SegmentCompletionProtocol.Response commitEnd(ServerSegmentCompletionProtocolHandler protocolHandler,
      String segmentName, File uploadedSegmentFile)
      throws Exception {
    SegmentCompletionProtocol.Request.Params params =
        params(segmentName).withSegmentLocation(uploadedSegmentFile.toURI().toString())
            .withSegmentSizeBytes(uploadedSegmentFile.length());
    return protocolHandler.segmentCommitEndWithMetadata(params, buildMetadataFiles(segmentName));
  }

  /// Builds a small segment with the name of the committing segment, and returns the metadata files that the server
  /// sends with the commit end.
  private Map<String, File> buildMetadataFiles(String segmentName)
      throws Exception {
    String rawTableName = "metadataFiles";
    Schema schema = new Schema.SchemaBuilder().setSchemaName(rawTableName)
        .addSingleValueDimension("value", DataType.STRING)
        .addDateTime("ts", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(rawTableName).setTimeColumnName("ts").build();
    File outDir = new File(_tempDir, "metadataFiles");
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setOutDir(outDir.getAbsolutePath());
    segmentGeneratorConfig.setSegmentName(segmentName);
    List<GenericRow> rows = new ArrayList<>();
    long now = System.currentTimeMillis();
    for (int i = 0; i < 3; i++) {
      GenericRow row = new GenericRow();
      row.putValue("value", "v" + i);
      row.putValue("ts", now + i);
      rows.add(row);
    }
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(rows));
    driver.build();
    File indexDir = new File(outDir, segmentName);
    return Map.of(V1Constants.MetadataKeys.METADATA_FILE_NAME, SegmentDirectoryPaths.findMetadataFile(indexDir),
        V1Constants.SEGMENT_CREATION_META, SegmentDirectoryPaths.findCreationMetaFile(indexDir));
  }

  public static class FakeServerSegmentStateModelFactory extends StateModelFactory<StateModel> {
    @Override
    public StateModel createNewStateModel(String resourceName, String partitionName) {
      return new FakeSegmentStateModel();
    }
  }

  /// Accepts every segment state transition without loading anything.
  @SuppressWarnings("unused")
  @StateModelInfo(states = "{'OFFLINE', 'ONLINE', 'CONSUMING', 'DROPPED'}", initialState = "OFFLINE")
  public static class FakeSegmentStateModel extends StateModel {
    @Transition(from = "OFFLINE", to = "CONSUMING")
    public void onBecomeConsumingFromOffline(Message message, NotificationContext context) {
    }

    @Transition(from = "CONSUMING", to = "ONLINE")
    public void onBecomeOnlineFromConsuming(Message message, NotificationContext context) {
    }

    @Transition(from = "CONSUMING", to = "OFFLINE")
    public void onBecomeOfflineFromConsuming(Message message, NotificationContext context) {
    }

    @Transition(from = "CONSUMING", to = "DROPPED")
    public void onBecomeDroppedFromConsuming(Message message, NotificationContext context) {
    }

    @Transition(from = "OFFLINE", to = "ONLINE")
    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
    }

    @Transition(from = "ONLINE", to = "OFFLINE")
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
    }

    @Transition(from = "OFFLINE", to = "DROPPED")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
    }

    @Transition(from = "ONLINE", to = "DROPPED")
    public void onBecomeDroppedFromOnline(Message message, NotificationContext context) {
    }

    @Transition(from = "ERROR", to = "OFFLINE")
    public void onBecomeOfflineFromError(Message message, NotificationContext context) {
    }
  }
}
