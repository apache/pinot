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
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.pinot.common.assignment.ReplicateModeAssignmentUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.restlet.resources.RebalanceConfig;
import org.apache.pinot.common.restlet.resources.RebalanceResult;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.controller.helix.core.rebalance.TableRebalancer;
import org.apache.pinot.core.data.manager.realtime.RealtimeSegmentDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import org.apache.pinot.plugin.inputformat.json.JSONMessageDecoder;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.server.starter.helix.BaseServerStarter;
import org.apache.pinot.server.starter.helix.HelixInstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Exercises real Kafka replay and segment fanout after loss of a leader's unsealed mutable data.
/// The isolated in-process cluster tests both completion protocols and both query engines; it never uses a deployed
/// cluster. Server stop has query draining disabled and no forced commit, and the segment must remain IN_PROGRESS.
public class ReplicateModeFailoverIntegrationTest extends BaseClusterIntegrationTest {
  private static final int PHASE_ROWS = 100;
  private static final long WAIT_MS = 180_000L;
  private static final String SEGMENT_STORE_SCHEME = "replicate-pauseless-test";
  private String _scenarioTable;
  private String _scenarioTopic;

  @Override
  protected int getNumKafkaPartitions() {
    return 1;
  }

  @Override
  protected String getTableName() {
    return _scenarioTable;
  }

  @Override
  protected String getKafkaTopic() {
    return _scenarioTopic;
  }

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    properties.put(ControllerConf.DATA_DIR,
        SEGMENT_STORE_SCHEME + ":" + new File(_tempDir, "deepStore").getAbsolutePath());
    properties.put(ControllerConf.ControllerPeriodicTasksConf.REPLICATE_MODE_FAILOVER_FREQUENCY_PERIOD, "1s");
    properties.put(ControllerConf.ControllerPeriodicTasksConf.REPLICATE_MODE_FAILOVER_INITIAL_DELAY_IN_SECONDS, 0L);
    properties.put("controller.realtime.replicateMode.failover.cooldownMs", 1000L);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration configuration) {
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_SHUTDOWN_ENABLE_QUERY_CHECK, false);
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_SHUTDOWN_ENABLE_RESOURCE_CHECK, false);
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_REALTIME_OFFHEAP_ALLOCATION, false);
    configuration.setProperty("pinot.server.instance.segment.store.uri",
        _controllerConfig.getDataDir());
    configuration.setProperty("pinot.server.instance." + HelixInstanceDataManagerConfig.UPLOAD_SEGMENT_TO_DEEP_STORE,
        true);
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    PinotFSFactory.register(SEGMENT_STORE_SCHEME, BlockingSegmentUploadFileSystem.class.getName(),
        new PinotConfiguration());
    startZk();
    startKafkaWithoutTopic();
    startController();
    startBroker();
    startServers(3);
  }

  @DataProvider
  public Object[][] completionModes() {
    return new Object[][]{{false}, {true}};
  }

  @DataProvider
  public Object[][] pauselessCommitStages() {
    return new Object[][]{{false, false}, {true, false}, {true, true}};
  }

  @DataProvider
  public Object[][] replicationChanges() {
    return new Object[][]{{3, 2, 0}, {2, 3, 0}, {3, 2, 1}};
  }

  @Test(dataProvider = "replicationChanges")
  public void testPauselessFanoutUsesLatestPlacementWhilePreservingHistoricalAssignments(int originalReplicas,
      int latestReplicas, int partitionId)
      throws Exception {
    _scenarioTable = "replicateModeResize_" + originalReplicas + "_" + latestReplicas + "_" + partitionId;
    _scenarioTopic = _scenarioTable;
    String table = _scenarioTable + "_REALTIME";
    int partitions = partitionId + 1;
    TableConfig config = tableConfig(true);
    config.getValidationConfig().setReplication(Integer.toString(originalReplicas));
    if (partitionId > 0) {
      config.getIngestionConfig().getStreamIngestionConfig().getStreamConfigMaps().get(0)
          .put("stream.kafka.partition.ids", Integer.toString(partitionId));
    }
    boolean tableCreated = false;
    UploadBarrier uploadBarrier = null;
    try {
      createScenarioTable(config, partitions);
      tableCreated = true;
      waitForAllRealtimePartitionsConsuming(table, WAIT_MS);
      produceRows(0, PHASE_ROWS, partitions, partitionId);
      waitForRows(PHASE_ROWS);
      IdealState initial = _helixResourceManager.getTableIdealState(table);
      String segment = initial.getRecord().getMapFields().keySet().iterator().next();
      String leader = ReplicateModeAssignmentUtils.getLeader(initial, partitionId);
      Set<String> initialCandidates = Set.copyOf(ReplicateModeAssignmentUtils.getReplicas(initial, partitionId));
      assertEquals(initialCandidates.size(), originalReplicas);
      uploadBarrier = new UploadBarrier(segment, new CountDownLatch(1), new CountDownLatch(1));
      BlockingSegmentUploadFileSystem._barrier = uploadBarrier;
      getOrCreateAdminClient().getTableClient().forceCommit(table);
      assertTrue(uploadBarrier.entered().await(WAIT_MS, TimeUnit.MILLISECONDS), "Segment upload did not start");
      TestUtils.waitForCondition(() -> {
        IdealState state = _helixResourceManager.getTableIdealState(table);
        return _helixResourceManager.getSegmentZKMetadata(table, segment).getStatus() == Status.COMMITTING
            && state.getRecord().getMapFields().size() == 2;
      }, 100L, WAIT_MS, "Pauseless successor was not published before upload", Duration.ofSeconds(10));
      String next = _helixResourceManager.getTableIdealState(table).getRecord().getMapFields().keySet().stream()
          .filter(name -> !name.equals(segment)).findFirst().orElseThrow();

      // Change the config after the successor already exists: its partition roles still describe the old RF.
      // First fanout must resolve current native placement instead of relying on either historical snapshot.
      config.getValidationConfig().setReplication(Integer.toString(latestReplicas));
      updateTableConfig(config);
      assertEquals(_helixResourceManager.getTableConfig(table).getReplication(), latestReplicas);
      produceRows(PHASE_ROWS, PHASE_ROWS * 2, partitions, partitionId);
      waitForRows(PHASE_ROWS * 2);
      assertEquals(_helixResourceManager.getTableIdealState(table).getInstanceStateMap(segment),
          Map.of(leader, SegmentStateModel.ONLINE));
      assertSingleActualConsumer(table, next, leader);

      uploadBarrier.release().countDown();
      TestUtils.waitForCondition(() -> completedOnReplicas(table, segment, latestReplicas), 100L, WAIT_MS,
          "Completed segment did not converge to the latest replica count", Duration.ofSeconds(10));
      SegmentZKMetadata done = _helixResourceManager.getSegmentZKMetadata(table, segment);
      assertEquals(done.getNumReplicas(), originalReplicas, "Creation metadata remains a historical snapshot");
      assertEquals(done.getStartOffset(), "0");
      assertEquals(done.getEndOffset(), Integer.toString(PHASE_ROWS));
      assertFalse(done.getDownloadUrl().contains(".recovery."), "The original uploader must complete normally");
      Map<String, String> completed =
          _helixResourceManager.getTableIdealState(table).getInstanceStateMap(segment);
      if (partitionId == 1) {
        // Native non-replica-group assignment changes p1 from [A,B,C] to [C,A], excluding its original leader B.
        Set<String> expected = new HashSet<>(initialCandidates);
        expected.remove(leader);
        assertEquals(completed.keySet(), expected);
      } else {
        assertTrue(completed.containsKey(leader));
      }
      TestUtils.waitForCondition(() -> !_helixResourceManager.getRealtimeSegmentManager()
              .getPendingSegmentCompletions(table).contains(segment), 100L, WAIT_MS,
          "Completed fanout remained in the native completion list", Duration.ofSeconds(10));

      // A later config edit must not reinterpret an already completed assignment as unfinished first fanout.
      config.getValidationConfig().setReplication(Integer.toString(originalReplicas));
      updateTableConfig(config);
      _helixResourceManager.getRealtimeSegmentManager().repairCompletedReplicateModeSegments(table);
      assertEquals(_helixResourceManager.getTableIdealState(table).getInstanceStateMap(segment), completed);
      assertSingleActualConsumer(table, next, leader);
      int rows = PHASE_ROWS * 2;
      assertExactRowsOnBothEngines(rows, partitions * (long) rows * (rows - 1) / 2 + (long) partitionId * rows);
    } finally {
      if (uploadBarrier != null) {
        uploadBarrier.release().countDown();
      }
      BlockingSegmentUploadFileSystem._barrier = null;
      if (tableCreated) {
        dropRealtimeTable(_scenarioTable);
        waitForTableDataManagerRemoved(table);
        waitForEVToDisappear(table);
      }
    }
  }

  @Test(dataProvider = "pauselessCommitStages")
  public void testPauselessSuccessorConsumesWhilePreviousBuildOrUploadIsBlocked(boolean blockUpload,
      boolean moveSuccessor)
      throws Exception {
    _scenarioTable = "replicateModePauseless_" + (blockUpload ? "upload" : "build")
        + (moveSuccessor ? "_handoff" : "");
    _scenarioTopic = _scenarioTable;
    String table = _scenarioTable + "_REALTIME";
    boolean tableCreated = false;
    RealtimeTableDataManager tableManager = null;
    RealtimeSegmentDataManager previous = null;
    Semaphore buildSemaphore = null;
    int heldPermits = 0;
    UploadBarrier uploadBarrier = null;
    try {
      createScenarioTable(tableConfig(true));
      tableCreated = true;
      waitForAllRealtimePartitionsConsuming(table, WAIT_MS);
      produceRows(0, PHASE_ROWS);
      waitForRows(PHASE_ROWS);
      IdealState initial = _helixResourceManager.getTableIdealState(table);
      String segment = initial.getRecord().getMapFields().keySet().iterator().next();
      String leader = ReplicateModeAssignmentUtils.getLeader(initial, 0);
      BaseServerStarter original = _serverStarters.stream().filter(server -> server.getInstanceId().equals(leader))
          .findFirst().orElseThrow();
      tableManager = (RealtimeTableDataManager) original.getServerInstance()
          .getInstanceDataManager().getTableDataManager(table);
      assertNotNull(tableManager);
      // Retain a real serving reference throughout the commit. An exact fixed range must not hold up its successor
      // merely because an older query can still be using the mutable segment.
      previous = acquireConsumer(leader, table, segment);
      assertNotNull(previous);
      if (blockUpload) {
        uploadBarrier = new UploadBarrier(segment, new CountDownLatch(1), new CountDownLatch(1));
        BlockingSegmentUploadFileSystem._barrier = uploadBarrier;
      } else {
        buildSemaphore = tableManager.getSegmentBuildSemaphore();
        assertNotNull(buildSemaphore);
        heldPermits = buildSemaphore.availablePermits();
        assertTrue(heldPermits > 0);
        assertTrue(buildSemaphore.tryAcquire(heldPermits, 5, TimeUnit.SECONDS));
      }

      getOrCreateAdminClient().getTableClient().forceCommit(table);
      TestUtils.waitForCondition(() -> {
        SegmentZKMetadata metadata = _helixResourceManager.getSegmentZKMetadata(table, segment);
        IdealState state = _helixResourceManager.getTableIdealState(table);
        return metadata.getStatus() == Status.COMMITTING && state.getRecord().getMapFields().size() == 2;
      }, 100L, WAIT_MS, "Pauseless commit did not publish its successor", Duration.ofSeconds(10));
      if (uploadBarrier != null) {
        assertTrue(uploadBarrier.entered().await(WAIT_MS, TimeUnit.MILLISECONDS), "Segment upload did not start");
      }
      SegmentZKMetadata fixed = _helixResourceManager.getSegmentZKMetadata(table, segment);
      assertEquals(fixed.getEndOffset(), Integer.toString(PHASE_ROWS));
      assertNull(fixed.getDownloadUrl());
      String next = _helixResourceManager.getTableIdealState(table).getRecord().getMapFields().keySet().stream()
          .filter(name -> !name.equals(segment)).findFirst().orElseThrow();
      assertEquals(_helixResourceManager.getSegmentZKMetadata(table, next).getStartOffset(), fixed.getEndOffset());

      produceRows(PHASE_ROWS, PHASE_ROWS * 2);
      waitForRows(PHASE_ROWS * 2);
      assertExactRowsOnBothEngines(PHASE_ROWS * 2);
      assertEquals(previous.getCurrentOffset().toString(), fixed.getEndOffset(), "Old mutable must stay frozen");
      assertTrue(previous.getReferenceCount() > 0);
      assertEquals(_helixResourceManager.getSegmentZKMetadata(table, segment).getStatus(), Status.COMMITTING,
          "New records must be queryable before the old segment can finish committing");
      assertEquals(_helixResourceManager.getTableIdealState(table).getInstanceStateMap(segment),
          Map.of(leader, SegmentStateModel.ONLINE), "Followers must wait for the completed artifact");
      assertSingleActualConsumer(table, next, leader);

      String consumingLeader = leader;
      if (moveSuccessor) {
        String replacement = ReplicateModeAssignmentUtils.getReplicas(initial, 0).stream()
            .filter(server -> !server.equals(leader)).findFirst().orElseThrow();
        // Exercise the real Server and Broker transitions of a consuming handoff while the previous range still
        // belongs to its original uploader. Local consumer permits cannot coordinate these two Servers.
        HelixHelper.updateIdealState(_helixResourceManager.getHelixZkManager(), table, state -> {
          ReplicateModeAssignmentUtils.setAssignment(state, 0, replacement,
              ReplicateModeAssignmentUtils.getReplicas(state, 0));
          state.setInstanceStateMap(next, new HashMap<>(Map.of(replacement, SegmentStateModel.CONSUMING)));
          return state;
        });
        assertSingleActualConsumer(table, next, replacement);
        produceRows(PHASE_ROWS * 2, PHASE_ROWS * 3);
        waitForRows(PHASE_ROWS * 3);
        assertExactRowsOnBothEngines(PHASE_ROWS * 3);
        assertEquals(previous.getCurrentOffset().toString(), fixed.getEndOffset());
        assertEquals(_helixResourceManager.getSegmentZKMetadata(table, segment).getStatus(), Status.COMMITTING);
        assertEquals(_helixResourceManager.getTableIdealState(table).getInstanceStateMap(segment),
            Map.of(leader, SegmentStateModel.ONLINE));
        consumingLeader = replacement;
      }

      if (uploadBarrier != null) {
        uploadBarrier.release().countDown();
      } else {
        buildSemaphore.release(heldPermits);
        heldPermits = 0;
      }
      TestUtils.waitForCondition(() -> completedOnAllReplicas(table, segment), 100L, WAIT_MS,
          "Old segment did not finish and replicate after its commit was unblocked", Duration.ofSeconds(10));
      assertFalse(_helixResourceManager.getSegmentZKMetadata(table, segment).getDownloadUrl().contains(".recovery."),
          "A healthy pauseless commit must finish without re-ingestion");
      int visibleRows = PHASE_ROWS * (moveSuccessor ? 3 : 2);
      assertTrue(consumerAtOffset(consumingLeader, table, next, visibleRows));
      tableManager.releaseSegment(previous);
      previous = null;
      produceRows(visibleRows, visibleRows + PHASE_ROWS);
      waitForRows(visibleRows + PHASE_ROWS);
      assertExactRowsOnBothEngines(visibleRows + PHASE_ROWS);
    } finally {
      if (uploadBarrier != null) {
        uploadBarrier.release().countDown();
      }
      BlockingSegmentUploadFileSystem._barrier = null;
      if (buildSemaphore != null && heldPermits > 0) {
        buildSemaphore.release(heldPermits);
      }
      if (previous != null) {
        tableManager.releaseSegment(previous);
      }
      if (tableCreated) {
        dropRealtimeTable(_scenarioTable);
        waitForTableDataManagerRemoved(table);
        waitForEVToDisappear(table);
      }
    }
  }

  @Test(dataProvider = "completionModes")
  public void testUnsealedLeaderLossReplaysSameSegmentAndReplicatesItsCommit(boolean pauseless)
      throws Exception {
    _scenarioTable = "replicateModeFailover_" + (pauseless ? "pauseless" : "blocking");
    _scenarioTopic = _scenarioTable;
    String table = _scenarioTable + "_REALTIME";
    boolean tableCreated = false;
    BaseServerStarter stoppedLeader = null;
    try {
      createScenarioTable(tableConfig(pauseless));
      tableCreated = true;
      waitForAllRealtimePartitionsConsuming(table, WAIT_MS);
      IdealState initial = _helixResourceManager.getTableIdealState(table);
      assertEquals(initial.getRecord().getMapFields().size(), 1);
      String segment = initial.getRecord().getMapFields().keySet().iterator().next();
      String leader = ReplicateModeAssignmentUtils.getLeader(initial, 0);
      assertNotNull(leader);
      assertEquals(initial.getInstanceStateMap(segment), Map.of(leader, SegmentStateModel.CONSUMING));
      assertEquals(ReplicateModeAssignmentUtils.getReplicas(initial, 0).size(), 3);
      produceRows(0, PHASE_ROWS);
      waitForRows(PHASE_ROWS);
      assertSingleActualConsumer(table, segment, leader);
      assertEquals(_helixResourceManager.getSegmentZKMetadata(table, segment).getStatus(), Status.IN_PROGRESS);

      stoppedLeader = _serverStarters.stream().filter(server -> server.getInstanceId().equals(leader))
          .findFirst().orElseThrow();
      // This stops the in-process participant and destroys its mutable data without forcing a segment commit.
      stoppedLeader.stop();
      _serverStarters.remove(stoppedLeader);
      SegmentZKMetadata afterLoss = _helixResourceManager.getSegmentZKMetadata(table, segment);
      assertEquals(afterLoss.getStatus(), Status.IN_PROGRESS, "Failure must happen before segment sealing");
      assertNull(afterLoss.getDownloadUrl());
      String startOffset = afterLoss.getStartOffset();
      TestUtils.waitForCondition(() -> {
        IdealState current = _helixResourceManager.getTableIdealState(table);
        String replacement = ReplicateModeAssignmentUtils.getLeader(current, 0);
        return replacement != null && !replacement.equals(leader)
            && current.getInstanceStateMap(segment).equals(Map.of(replacement, SegmentStateModel.CONSUMING));
      }, 100L, WAIT_MS, "Follower did not take over the original consuming segment", Duration.ofSeconds(10));
      assertEquals(_helixResourceManager.getSegmentZKMetadata(table, segment).getStartOffset(), startOffset);
      assertEquals(_helixResourceManager.getTableIdealState(table).getRecord().getMapFields().size(), 1);
      waitForRows(PHASE_ROWS);

      _serverStarters.add(startOneServer(stoppedLeader.getConfig()));
      stoppedLeader = null;
      String replacement = ReplicateModeAssignmentUtils.getLeader(_helixResourceManager.getTableIdealState(table), 0);
      assertFalse(replacement.equals(leader), "Returning Server must remain a follower");
      assertSingleActualConsumer(table, segment, replacement);
      getOrCreateAdminClient().getTableClient().forceCommit(table);
      TestUtils.waitForCondition(() -> completedOnAllReplicas(table, segment), 100L, WAIT_MS,
          "Recovered segment did not commit and load on all three replicas", Duration.ofSeconds(10));
      SegmentZKMetadata done = _helixResourceManager.getSegmentZKMetadata(table, segment);
      assertEquals(done.getStartOffset(), startOffset);
      assertEquals(done.getEndOffset(), Integer.toString(PHASE_ROWS));
      assertEquals(done.getNumReplicas(), 3);
      produceRows(PHASE_ROWS, PHASE_ROWS * 2);
      waitForRows(PHASE_ROWS * 2);
      assertExactRowsOnBothEngines(PHASE_ROWS * 2);
    } finally {
      if (stoppedLeader != null) {
        _serverStarters.add(startOneServer(stoppedLeader.getConfig()));
      }
      if (tableCreated) {
        dropRealtimeTable(_scenarioTable);
        waitForTableDataManagerRemoved(table);
        waitForEVToDisappear(table);
      }
    }
  }

  /// The semantic case also restores missing time values and skips the same oversized MV record as normal ingestion.
  @DataProvider
  public Object[][] recoveryPartitionSelections() {
    return new Object[][]{{1, 0, false}, {4, 3, false}, {1, 0, true}};
  }

  @Test(dataProvider = "recoveryPartitionSelections")
  public void testCommittingLeaderLossReingestsFixedRangeAndPreservesNextSegment(int partitionCount, int partitionId,
      boolean exerciseRowSemantics)
      throws Exception {
    _scenarioTable = "replicateModeFailover_committing_" + partitionCount + "_" + partitionId
        + (exerciseRowSemantics ? "_rowSemantics" : "");
    _scenarioTopic = _scenarioTable;
    String table = _scenarioTable + "_REALTIME";
    boolean tableCreated = false;
    BaseServerStarter stoppedLeader = null;
    Semaphore buildSemaphore = null;
    int heldPermits = 0;
    String timeoutKey = PinotLLCRealtimeSegmentManager.MAX_SEGMENT_COMPLETION_TIME_MILLIS_KEY;
    try {
      updateClusterConfig(Map.of(timeoutKey, "10000"));
      TestUtils.waitForCondition(() -> _helixResourceManager.getRealtimeSegmentManager()
              .getMaxSegmentCompletionTimeMillis() == 10_000L, 100L, WAIT_MS,
          "Native completion timeout did not update", Duration.ofSeconds(10));
      TableConfig config = tableConfig(true);
      StreamIngestionConfig streamIngestion = config.getIngestionConfig().getStreamIngestionConfig();
      streamIngestion.setDropRecordOnPartitionMismatch(true);
      Map<String, String> stream = streamIngestion.getStreamConfigMaps().get(0);
      stream.put(StreamConfigProperties.SEGMENT_COMMIT_TIMEOUT_SECONDS, "5");
      if (partitionCount > 1) {
        stream.put("stream.kafka.partition.ids", Integer.toString(partitionId));
      }
      config.getIndexingConfig().setSegmentPartitionConfig(
          new SegmentPartitionConfig(Map.of("id", new ColumnPartitionConfig("Modulo", partitionCount))));
      config.getIndexingConfig().setNullHandlingEnabled(false);
      createScenarioTable(config, partitionCount, exerciseRowSemantics);
      tableCreated = true;
      waitForAllRealtimePartitionsConsuming(table, WAIT_MS);
      produceRows(0, PHASE_ROWS, partitionCount, partitionId, exerciseRowSemantics);
      waitForRows(PHASE_ROWS - (exerciseRowSemantics ? 1 : 0));
      IdealState initial = _helixResourceManager.getTableIdealState(table);
      assertEquals(initial.getRecord().getMapFields().size(), 1);
      String segment = initial.getRecord().getMapFields().keySet().iterator().next();
      long segmentCreationTime = _helixResourceManager.getSegmentZKMetadata(table, segment).getCreationTime();
      if (exerciseRowSemantics) {
        assertRowSemanticsOnBothEngines(segmentCreationTime);
      }
      String leader = ReplicateModeAssignmentUtils.getLeader(initial, partitionId);
      BaseServerStarter original = _serverStarters.stream().filter(server -> server.getInstanceId().equals(leader))
          .findFirst().orElseThrow();
      RealtimeTableDataManager tableManager = (RealtimeTableDataManager) original.getServerInstance()
          .getInstanceDataManager().getTableDataManager(table);
      assertNotNull(tableManager);
      buildSemaphore = tableManager.getSegmentBuildSemaphore();
      assertNotNull(buildSemaphore);
      int permits = buildSemaphore.availablePermits();
      assertTrue(permits > 0);
      assertTrue(buildSemaphore.tryAcquire(permits, 5, TimeUnit.SECONDS));
      heldPermits = permits;
      getOrCreateAdminClient().getTableClient().forceCommit(table);
      TestUtils.waitForCondition(() -> {
        SegmentZKMetadata metadata = _helixResourceManager.getSegmentZKMetadata(table, segment);
        IdealState state = _helixResourceManager.getTableIdealState(table);
        return metadata.getStatus() == Status.COMMITTING && state.getRecord().getMapFields().size() == 2;
      }, 100L, WAIT_MS, "Real pauseless commit did not reach COMMITTING before build", Duration.ofSeconds(10));
      SegmentZKMetadata fixedRange = _helixResourceManager.getSegmentZKMetadata(table, segment);
      assertEquals(fixedRange.getStartOffset(), "0");
      assertEquals(fixedRange.getEndOffset(), Integer.toString(PHASE_ROWS));
      assertNull(fixedRange.getDownloadUrl());
      String next = _helixResourceManager.getTableIdealState(table).getRecord().getMapFields().keySet().stream()
          .filter(name -> !name.equals(segment)).findFirst().orElseThrow();
      assertEquals(_helixResourceManager.getSegmentZKMetadata(table, next).getStartOffset(), fixedRange.getEndOffset());

      // Interrupt the blocked real build before stopping its Server; keep all permits held through shutdown so
      // neither its consumer nor a CONSUMING->ONLINE handler can finish a local build during failure injection.
      stopBlockedConsumer(tableManager, segment);
      original.stop();
      _serverStarters.remove(original);
      stoppedLeader = original;
      buildSemaphore.release(heldPermits);
      heldPermits = 0;
      SegmentZKMetadata afterLoss = _helixResourceManager.getSegmentZKMetadata(table, segment);
      assertTrue(afterLoss.getStatus() == Status.COMMITTING || afterLoss.getStatus() == Status.DONE);
      assertTrue(afterLoss.getDownloadUrl() == null || afterLoss.getDownloadUrl().contains(".recovery."),
          "The stopped Server must not finish the blocked native build");
      TestUtils.waitForCondition(() -> {
        IdealState state = _helixResourceManager.getTableIdealState(table);
        String replacement = ReplicateModeAssignmentUtils.getLeader(state, partitionId);
        return replacement != null && !replacement.equals(leader)
            && state.getInstanceStateMap(next).equals(Map.of(replacement, SegmentStateModel.CONSUMING));
      }, 100L, WAIT_MS, "Next segment did not move to a surviving follower", Duration.ofSeconds(10));
      produceRows(PHASE_ROWS, PHASE_ROWS * 2, partitionCount, partitionId);
      TestUtils.waitForCondition(() -> _helixResourceManager.getSegmentZKMetadata(table, segment).getStatus()
          == Status.DONE, 100L, WAIT_MS, "Fixed-range replay did not publish DONE", Duration.ofSeconds(10));
      SegmentZKMetadata recovered = _helixResourceManager.getSegmentZKMetadata(table, segment);
      assertEquals(recovered.getStartOffset(), fixedRange.getStartOffset());
      assertEquals(recovered.getEndOffset(), fixedRange.getEndOffset());
      assertTrue(recovered.getDownloadUrl().contains(".recovery."), "Must exercise explicit fixed-range re-ingestion");
      _serverStarters.add(startOneServer(stoppedLeader.getConfig()));
      stoppedLeader = null;
      TestUtils.waitForCondition(() -> completedOnAllReplicas(table, segment), 100L, WAIT_MS,
          "Re-ingested range did not load on all three replicas", Duration.ofSeconds(10));
      assertEquals(_helixResourceManager.getTableIdealState(table).getRecord().getMapFields().size(), 2,
          "Recovery must preserve the existing next segment instead of creating a duplicate range");
      int producedRows = PHASE_ROWS * 2;
      int expectedRows = producedRows - (exerciseRowSemantics ? 1 : 0);
      waitForRows(expectedRows);
      long expectedSum = partitionCount * ((long) producedRows * (producedRows - 1) / 2)
          + (long) partitionId * producedRows;
      if (exerciseRowSemantics) {
        expectedSum -= PHASE_ROWS / 2;
        assertRowSemanticsOnBothEngines(segmentCreationTime);
      }
      assertExactRowsOnBothEngines(expectedRows, expectedSum);
      assertNotNull(recovered.getPartitionMetadata());
      assertEquals(recovered.getPartitionMetadata().getNumPartitions("id"), partitionCount);
      assertEquals(recovered.getPartitionMetadata().getPartitions("id"), Set.of(partitionId));
      for (boolean multiStage : List.of(false, true)) {
        setUseMultiStageQueryEngine(multiStage);
        JsonNode response = postQuery("SELECT COUNT(*) FROM " + _scenarioTable + " WHERE id = " + partitionId);
        assertTrue(response.path("exceptions").isEmpty(), response.toString());
        assertEquals(response.path("resultTable").path("rows").path(0).path(0).asLong(), 1L);
      }
    } finally {
      if (buildSemaphore != null && heldPermits > 0) {
        buildSemaphore.release(heldPermits);
      }
      try {
        if (tableCreated) {
          dropRealtimeTable(_scenarioTable);
          waitForTableDataManagerRemoved(table);
          waitForEVToDisappear(table);
        }
      } finally {
        try {
          // On a failed test, remove a stranded COMMITTING assignment before restoring the stopped participant.
          if (stoppedLeader != null) {
            _serverStarters.add(startOneServer(stoppedLeader.getConfig()));
          }
        } finally {
          deleteClusterConfig(timeoutKey);
        }
      }
    }
  }

  @Test(dataProvider = "completionModes")
  public void testManualRebalanceRedistributesReturnedServerLeadershipAndKeepsConsuming(boolean pauseless)
      throws Exception {
    _scenarioTable = "replicateModeManual_" + (pauseless ? "pauseless" : "blocking");
    _scenarioTopic = _scenarioTable;
    String table = _scenarioTable + "_REALTIME";
    TableConfig config = tableConfig(pauseless);
    BaseServerStarter stoppedLeader = null;
    boolean tableCreated = false;
    try {
      createScenarioTable(config, 3);
      tableCreated = true;
      waitForSingleConsumers(table, 3);
      produceRows(0, PHASE_ROWS * 3, 3);
      waitForRows(PHASE_ROWS * 3);
      waitForSingleConsumers(table, 3);
      IdealState initial = _helixResourceManager.getTableIdealState(table);
      Set<String> initialSegments = Set.copyOf(initial.getRecord().getMapFields().keySet());
      assertEquals(initialSegments.size(), 3);
      assertEquals(leaderCounts(initial).size(), 3);
      String returningServer = ReplicateModeAssignmentUtils.getLeader(initial, 0);
      assertNotNull(returningServer);
      stoppedLeader = _serverStarters.stream().filter(server -> server.getInstanceId().equals(returningServer))
          .findFirst().orElseThrow();
      stoppedLeader.stop();
      _serverStarters.remove(stoppedLeader);
      TestUtils.waitForCondition(() -> !returningServer.equals(
              ReplicateModeAssignmentUtils.getLeader(_helixResourceManager.getTableIdealState(table), 0)),
          100L, WAIT_MS, "Original partition leader did not fail over", Duration.ofSeconds(10));
      waitForRows(PHASE_ROWS * 3);
      _serverStarters.add(startOneServer(stoppedLeader.getConfig()));
      stoppedLeader = null;
      waitForSingleConsumers(table, 3);
      Map<String, Integer> skewedCounts = leaderCounts(_helixResourceManager.getTableIdealState(table));
      assertFalse(skewedCounts.containsKey(returningServer), "Rejoin must not automatically reclaim leadership");
      assertEquals(new HashSet<>(skewedCounts.values()), Set.of(1, 2));

      RebalanceResult result = rebalance(config, false);
      assertEquals(result.getStatus(), RebalanceResult.Status.DONE, result.toString());
      waitForSingleConsumers(table, 3);
      IdealState balanced = _helixResourceManager.getTableIdealState(table);
      Map<String, Integer> counts = leaderCounts(balanced);
      assertEquals(counts.size(), 3, "An explicit consuming rebalance must use the returned Server");
      assertEquals(new HashSet<>(counts.values()), Set.of(1));
      assertEquals(balanced.getRecord().getMapFields().keySet(), initialSegments,
          "Rebalance must retain the unfinished segment identities");
      for (String segment : initialSegments) {
        SegmentZKMetadata metadata = _helixResourceManager.getSegmentZKMetadata(table, segment);
        assertEquals(metadata.getStatus(), Status.IN_PROGRESS);
        assertEquals(metadata.getStartOffset(), "0");
      }
      produceRows(PHASE_ROWS * 3, PHASE_ROWS * 6, 3);
      waitForRows(PHASE_ROWS * 6);
      assertExactRowsOnBothEngines(PHASE_ROWS * 6);
      getOrCreateAdminClient().getTableClient().forceCommit(table);
      TestUtils.waitForCondition(() -> initialSegments.stream()
              .allMatch(segment -> completedOnAllReplicas(table, segment)), 100L, WAIT_MS,
          "Rebalanced segments did not commit and replicate to all three Servers", Duration.ofSeconds(10));
      for (String segment : initialSegments) {
        assertEquals(_helixResourceManager.getSegmentZKMetadata(table, segment).getEndOffset(),
            Integer.toString(PHASE_ROWS * 2));
      }
      waitForSingleConsumers(table, 3);
      produceRows(PHASE_ROWS * 6, PHASE_ROWS * 9, 3);
      waitForRows(PHASE_ROWS * 9);
      assertExactRowsOnBothEngines(PHASE_ROWS * 9);
    } finally {
      if (stoppedLeader != null) {
        _serverStarters.add(startOneServer(stoppedLeader.getConfig()));
      }
      if (tableCreated) {
        dropRealtimeTable(_scenarioTable);
        waitForTableDataManagerRemoved(table);
        waitForEVToDisappear(table);
      }
    }
  }

  @DataProvider
  public Object[][] rebalanceModes() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "rebalanceModes")
  public void testManualRebalanceRestartsConsumersDiscardedByAnEarlierFullReplicaAssignment(boolean downtime)
      throws Exception {
    _scenarioTable = "replicateModeRepair_" + (downtime ? "downtime" : "online");
    _scenarioTopic = _scenarioTable;
    String table = _scenarioTable + "_REALTIME";
    TableConfig config = tableConfig(false);
    config.getValidationConfig().setPeerSegmentDownloadScheme(null);
    boolean tableCreated = false;
    try {
      createScenarioTable(config);
      tableCreated = true;
      waitForAllRealtimePartitionsConsuming(table, WAIT_MS);
      produceRows(0, PHASE_ROWS);
      waitForRows(PHASE_ROWS);
      IdealState initial = _helixResourceManager.getTableIdealState(table);
      String segment = initial.getRecord().getMapFields().keySet().iterator().next();
      List<String> replicas = ReplicateModeAssignmentUtils.getReplicas(initial, 0);
      assertEquals(replicas.size(), 3);
      // Reproduce the earlier rebalance bug through its persisted assignment: all replicas consume, while role
      // metadata still names one leader. Completion admission discards each actual consumer in this state.
      HelixHelper.updateIdealState(_helixResourceManager.getHelixZkManager(), table, state -> {
        Map<String, String> consumers = new HashMap<>();
        replicas.forEach(server -> consumers.put(server, SegmentStateModel.CONSUMING));
        state.setInstanceStateMap(segment, consumers);
        return state;
      });
      TestUtils.waitForCondition(() -> {
        ExternalView view = _helixResourceManager.getTableExternalView(table);
        Map<String, String> states = view != null ? view.getStateMap(segment) : null;
        return states != null && states.size() == 3
            && states.values().stream().allMatch(SegmentStateModel.CONSUMING::equals)
            && replicas.stream().allMatch(server -> consumerAtOffset(server, table, segment, PHASE_ROWS));
      }, 100L, WAIT_MS, "Injected full-replica consumption did not become active", Duration.ofSeconds(10));
      getOrCreateAdminClient().getTableClient().forceCommit(table);
      TestUtils.waitForCondition(() -> replicas.stream().allMatch(server -> consumerDiscarded(server, table, segment)),
          100L, WAIT_MS, "Invalid multi-consumer assignment did not reach discarded consumer state",
          Duration.ofSeconds(10));
      produceRows(PHASE_ROWS, PHASE_ROWS * 2);
      assertTrue(replicas.stream().allMatch(server -> consumerAtOffset(server, table, segment, PHASE_ROWS)));
      assertEquals(_helixResourceManager.getSegmentZKMetadata(table, segment).getStatus(), Status.IN_PROGRESS);

      RebalanceResult result = rebalance(config, downtime);
      assertEquals(result.getStatus(), RebalanceResult.Status.DONE, result.toString());
      waitForSingleConsumers(table, 1);
      waitForRows(PHASE_ROWS * 2);
      assertEquals(_helixResourceManager.getTableIdealState(table).getRecord().getMapFields().keySet(),
          Set.of(segment));
      produceRows(PHASE_ROWS * 2, PHASE_ROWS * 3);
      waitForRows(PHASE_ROWS * 3);
      getOrCreateAdminClient().getTableClient().forceCommit(table);
      TestUtils.waitForCondition(() -> completedOnAllReplicas(table, segment), 100L, WAIT_MS,
          "Repaired consumer did not seal and replicate its original segment", Duration.ofSeconds(10));
      assertEquals(_helixResourceManager.getSegmentZKMetadata(table, segment).getEndOffset(),
          Integer.toString(PHASE_ROWS * 3));
      waitForSingleConsumers(table, 1);
      produceRows(PHASE_ROWS * 3, PHASE_ROWS * 4);
      waitForRows(PHASE_ROWS * 4);
      assertExactRowsOnBothEngines(PHASE_ROWS * 4);
    } finally {
      if (tableCreated) {
        dropRealtimeTable(_scenarioTable);
        waitForTableDataManagerRemoved(table);
        waitForEVToDisappear(table);
      }
    }
  }

  private RebalanceResult rebalance(TableConfig config, boolean downtime) {
    RebalanceConfig rebalance = new RebalanceConfig();
    rebalance.setDryRun(false);
    rebalance.setIncludeConsuming(true);
    rebalance.setDowntime(downtime);
    rebalance.setDisableSummary(true);
    TableRebalancer rebalancer = new TableRebalancer(_helixResourceManager.getHelixZkManager(), null,
        _controllerStarter.getControllerMetrics(), null, null, _helixResourceManager.getRealtimeSegmentManager());
    return rebalancer.rebalance(config, rebalance, null);
  }

  private static Map<String, Integer> leaderCounts(IdealState state) {
    Map<String, Integer> counts = new HashMap<>();
    ReplicateModeAssignmentUtils.getLeaders(state).values().forEach(server -> counts.merge(server, 1, Integer::sum));
    return counts;
  }

  private void waitForSingleConsumers(String table, int partitions) {
    TestUtils.waitForCondition(() -> {
      IdealState state = _helixResourceManager.getTableIdealState(table);
      ExternalView view = _helixResourceManager.getTableExternalView(table);
      if (state == null || view == null) {
        return false;
      }
      int consuming = 0;
      for (Map.Entry<String, Map<String, String>> segment : state.getRecord().getMapFields().entrySet()) {
        Map<String, String> assignment = segment.getValue();
        if (!assignment.containsValue(SegmentStateModel.CONSUMING)) {
          continue;
        }
        consuming++;
        int partition = new LLCSegmentName(segment.getKey()).getPartitionGroupId();
        if (assignment.size() != 1 || !assignment.equals(view.getStateMap(segment.getKey()))
            || !assignment.containsKey(ReplicateModeAssignmentUtils.getLeader(state, partition))
            || ReplicateModeAssignmentUtils.getReplicas(state, partition).size() != 3) {
          return false;
        }
      }
      return consuming == partitions;
    }, 100L, WAIT_MS, "Expected one active consumer per partition with matching replicate-mode roles",
        Duration.ofSeconds(10));
  }

  private boolean consumerAtOffset(String server, String table, String segment, long expectedOffset) {
    RealtimeSegmentDataManager manager = acquireConsumer(server, table, segment);
    if (manager == null) {
      return false;
    }
    try {
      return manager.getCurrentOffset().toString().equals(Long.toString(expectedOffset));
    } finally {
      releaseConsumer(server, table, manager);
    }
  }

  private boolean consumerDiscarded(String server, String table, String segment) {
    RealtimeSegmentDataManager manager = acquireConsumer(server, table, segment);
    if (manager == null) {
      return false;
    }
    try {
      Field state = RealtimeSegmentDataManager.class.getDeclaredField("_state");
      state.setAccessible(true);
      return state.get(manager) == RealtimeSegmentDataManager.State.DISCARDED;
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Could not observe the consumer's terminal state", e);
    } finally {
      releaseConsumer(server, table, manager);
    }
  }

  private RealtimeSegmentDataManager acquireConsumer(String server, String table, String segment) {
    BaseServerStarter starter = _serverStarters.stream().filter(candidate -> candidate.getInstanceId().equals(server))
        .findFirst().orElse(null);
    if (starter == null) {
      return null;
    }
    RealtimeTableDataManager tableManager =
        (RealtimeTableDataManager) starter.getServerInstance().getInstanceDataManager().getTableDataManager(table);
    if (tableManager == null) {
      return null;
    }
    SegmentDataManager manager = tableManager.acquireSegment(segment);
    if (manager instanceof RealtimeSegmentDataManager) {
      return (RealtimeSegmentDataManager) manager;
    }
    if (manager != null) {
      tableManager.releaseSegment(manager);
    }
    return null;
  }

  private void releaseConsumer(String server, String table, RealtimeSegmentDataManager manager) {
    BaseServerStarter starter = _serverStarters.stream().filter(candidate -> candidate.getInstanceId().equals(server))
        .findFirst().orElseThrow();
    starter.getServerInstance().getInstanceDataManager().getTableDataManager(table).releaseSegment(manager);
  }

  private void createScenarioTable(TableConfig config)
      throws Exception {
    createScenarioTable(config, 1);
  }

  private void createScenarioTable(TableConfig config, int partitions)
      throws Exception {
    createScenarioTable(config, partitions, false);
  }

  private void createScenarioTable(TableConfig config, int partitions, boolean exerciseRowSemantics)
      throws Exception {
    createKafkaTopic(_scenarioTopic, partitions);
    Schema.SchemaBuilder schemaBuilder = new Schema.SchemaBuilder().setSchemaName(_scenarioTable)
        .addSingleValueDimension("id", DataType.INT)
        .addDateTime("ts", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS");
    if (exerciseRowSemantics) {
      schemaBuilder.addMultiValueDimension("tags", DataType.INT);
    }
    Schema schema = schemaBuilder.build();
    addSchema(schema);
    addTableConfig(config);
  }

  private static void stopBlockedConsumer(RealtimeTableDataManager tableManager, String segment)
      throws InterruptedException {
    SegmentDataManager manager = tableManager.acquireSegment(segment);
    assertNotNull(manager);
    try {
      assertTrue(manager instanceof RealtimeSegmentDataManager);
      ((RealtimeSegmentDataManager) manager).stop();
    } finally {
      tableManager.releaseSegment(manager);
    }
  }

  private TableConfig tableConfig(boolean pauseless) {
    Map<String, String> stream = getStreamConfigMap();
    stream.put(StreamConfigProperties.constructStreamProperty("kafka", StreamConfigProperties.STREAM_DECODER_CLASS),
        JSONMessageDecoder.class.getName());
    stream.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, "10000");
    stream.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_TIME, "1h");
    StreamIngestionConfig streamIngestion = new StreamIngestionConfig(List.of(stream));
    streamIngestion.setReplicateMode(true);
    streamIngestion.setPauselessConsumptionEnabled(pauseless);
    IngestionConfig ingestion = new IngestionConfig();
    ingestion.setStreamIngestionConfig(streamIngestion);
    TableConfig config = new TableConfigBuilder(TableType.REALTIME).setTableName(_scenarioTable).setTimeColumnName("ts")
        .setNumReplicas(3).setRetentionTimeUnit("DAYS").setRetentionTimeValue("7")
        .setBrokerTenant(getBrokerTenant()).setServerTenant(getServerTenant()).setIngestionConfig(ingestion).build();
    config.getValidationConfig().setPeerSegmentDownloadScheme(CommonConstants.HTTP_PROTOCOL);
    return config;
  }

  private void produceRows(int start, int end)
      throws Exception {
    produceRows(start, end, 1);
  }

  private void produceRows(int start, int end, int partitions)
      throws Exception {
    produceRows(start, end, partitions, -1);
  }

  private void produceRows(int start, int end, int partitions, int partitionId)
      throws Exception {
    produceRows(start, end, partitions, partitionId, false);
  }

  private void produceRows(int start, int end, int partitions, int partitionId, boolean exerciseRowSemantics)
      throws Exception {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBrokerList());
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    List<Integer> oversizedTags = exerciseRowSemantics ? IntStream.range(0, 1001).boxed().toList() : List.of();
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties)) {
      for (int row = start; row < end; row++) {
        int id = partitionId >= 0 ? row * partitions + partitionId : row;
        Map<String, ?> record = exerciseRowSemantics
            ? Map.of("id", id, "tags", row == PHASE_ROWS / 2 ? oversizedTags : List.of(id))
            : Map.of("id", id, "ts", System.currentTimeMillis());
        producer.send(new ProducerRecord<>(_scenarioTopic, id % partitions, null,
            JsonUtils.objectToBytes(record))).get();
      }
      producer.flush();
    }
  }

  private void waitForRows(int expected) {
    setUseMultiStageQueryEngine(false);
    TestUtils.waitForCondition(() -> {
      JsonNode response = postQuery("SELECT COUNT(*) FROM " + _scenarioTable);
      return response.path("exceptions").isEmpty()
          && response.path("resultTable").path("rows").path(0).path(0).asLong(-1) == expected;
    }, 100L, WAIT_MS, "Expected " + expected + " rows after ingestion/replay", Duration.ofSeconds(10));
  }

  private void assertExactRowsOnBothEngines(int expected)
      throws Exception {
    assertExactRowsOnBothEngines(expected, (long) expected * (expected - 1) / 2);
  }

  private void assertExactRowsOnBothEngines(int expected, long expectedSum)
      throws Exception {
    for (boolean multiStage : List.of(false, true)) {
      setUseMultiStageQueryEngine(multiStage);
      for (int query = 0; query < 6; query++) {
        JsonNode response = postQuery("SELECT COUNT(*), DISTINCTCOUNT(id), SUM(id) FROM " + _scenarioTable);
        assertTrue(response.path("exceptions").isEmpty(), response.toString());
        JsonNode row = response.path("resultTable").path("rows").path(0);
        assertEquals(row.path(0).asLong(), expected);
        assertEquals(row.path(1).asLong(), expected);
        assertEquals(row.path(2).asLong(), expectedSum);
      }
    }
  }

  private void assertRowSemanticsOnBothEngines(long segmentCreationTime)
      throws Exception {
    for (boolean multiStage : List.of(false, true)) {
      setUseMultiStageQueryEngine(multiStage);
      JsonNode response = postQuery("SELECT COUNT(*), MIN(ts), MAX(ts) FROM " + _scenarioTable
          + " WHERE id < " + PHASE_ROWS);
      assertTrue(response.path("exceptions").isEmpty(), response.toString());
      JsonNode row = response.path("resultTable").path("rows").path(0);
      assertEquals(row.path(0).asLong(), PHASE_ROWS - 1L);
      assertEquals(row.path(1).asLong(), segmentCreationTime);
      assertEquals(row.path(2).asLong(), segmentCreationTime);
      response = postQuery("SELECT COUNT(*) FROM " + _scenarioTable + " WHERE id = " + PHASE_ROWS / 2);
      assertTrue(response.path("exceptions").isEmpty(), response.toString());
      assertEquals(response.path("resultTable").path("rows").path(0).path(0).asLong(), 0L);
    }
  }

  private void assertSingleActualConsumer(String table, String segment, String leader) {
    TestUtils.waitForCondition(() -> {
      ExternalView view = _helixResourceManager.getTableExternalView(table);
      Map<String, String> states = view != null ? view.getStateMap(segment) : null;
      return states != null && states.equals(Map.of(leader, SegmentStateModel.CONSUMING));
    }, 100L, WAIT_MS, "Consuming segment must have exactly one actual CONSUMING replica", Duration.ofSeconds(10));
  }

  private boolean completedOnAllReplicas(String table, String segment) {
    return completedOnReplicas(table, segment, 3);
  }

  private boolean completedOnReplicas(String table, String segment, int replicas) {
    if (_helixResourceManager.getSegmentZKMetadata(table, segment).getStatus() != Status.DONE) {
      return false;
    }
    IdealState idealState = _helixResourceManager.getTableIdealState(table);
    ExternalView externalView = _helixResourceManager.getTableExternalView(table);
    Map<String, String> desired = idealState.getInstanceStateMap(segment);
    Map<String, String> actual = externalView != null ? externalView.getStateMap(segment) : null;
    return desired != null && desired.size() == replicas
        && desired.values().stream().allMatch(SegmentStateModel.ONLINE::equals)
        && desired.equals(actual);
  }

  private record UploadBarrier(String segmentName, CountDownLatch entered, CountDownLatch release) {
  }

  /// Local storage with a single test-controlled upload barrier. The volatile barrier is published before commit;
  /// the uploader reads one immutable snapshot, and cleanup always releases the latch before clearing it.
  public static class BlockingSegmentUploadFileSystem extends LocalPinotFS {
    private static volatile UploadBarrier _barrier;

    @Override
    public void copyFromLocalFile(File source, URI destination)
        throws Exception {
      UploadBarrier barrier = _barrier;
      if (barrier != null
          && source.getName().equals(barrier.segmentName() + TarCompressionUtils.TAR_COMPRESSED_FILE_EXTENSION)) {
        barrier.entered().countDown();
        if (!barrier.release().await(WAIT_MS, TimeUnit.MILLISECONDS)) {
          throw new IOException("Timed out waiting to unblock upload for segment: " + barrier.segmentName());
        }
      }
      super.copyFromLocalFile(source, destination);
    }
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }
}
