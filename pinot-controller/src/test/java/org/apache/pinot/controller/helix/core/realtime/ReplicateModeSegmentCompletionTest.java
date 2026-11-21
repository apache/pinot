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
package org.apache.pinot.controller.helix.core.realtime;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.assignment.ReplicateModeAssignmentUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol.ControllerResponseStatus;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol.Request.Params;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.realtime.BlockingSegmentCompletionFSM.BlockingSegmentCompletionFSMState;
import org.apache.pinot.controller.helix.core.realtime.SegmentCompletionFSM.RecoveryStatus;
import org.apache.pinot.controller.helix.core.realtime.segment.CommittingSegmentDescriptor;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.stream.LongMsgOffsetFactory;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffsetFactory;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Exercises membership admission, preserved old commit attempts and local handoff ordering without a cluster.
@Test(singleThreaded = true)
public class ReplicateModeSegmentCompletionTest {
  private static final String TABLE = "testTable_REALTIME";
  private static final String SEGMENT = "testTable__0__0__20250228T1903Z";
  private static final String NEXT_SEGMENT = "testTable__0__1__20250228T1903Z";
  private static final String SERVER_A = "Server_a_1234";
  private static final String SERVER_B = "Server_b_1234";
  private static final String SERVER_C = "Server_c_1234";
  private static final List<String> SERVERS = List.of(SERVER_A, SERVER_B, SERVER_C);

  private PinotLLCRealtimeSegmentManager _segmentManager;
  private SegmentCompletionManager _manager;
  private SegmentZKMetadata _metadata;
  private IdealState _idealState;
  private TableConfig _tableConfig;
  private long _previousCommitTime;

  @BeforeMethod
  public void setUp() {
    _previousCommitTime = SegmentCompletionProtocol.getMaxSegmentCommitTimeMs();
    _segmentManager = mock(PinotLLCRealtimeSegmentManager.class);
    _metadata = new SegmentZKMetadata(SEGMENT);
    _metadata.setReplicateMode(true);
    _metadata.setStatus(Status.IN_PROGRESS);
    _metadata.setNumReplicas(3);
    _metadata.setStartOffset("0");
    _metadata.setCrc(1234L);
    _idealState = new IdealState(TABLE);
    assign(SERVER_A, SegmentStateModel.CONSUMING);
    _tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE).setNumReplicas(3).build();
    when(_segmentManager.getSegmentZKMetadata(anyString(), anyString(), isNull())).thenReturn(_metadata);
    when(_segmentManager.getIdealState(TABLE)).thenAnswer(invocation -> _idealState);
    when(_segmentManager.getTableConfig(TABLE)).thenAnswer(invocation -> _tableConfig);
    when(_segmentManager.getControllerVipUrl()).thenReturn("http://controller:9000");
    when(_segmentManager.getCommitTimeoutMS(TABLE)).thenReturn(120_000L);
    HelixManager helixManager = mock(HelixManager.class);
    when(helixManager.isConnected()).thenReturn(true);
    _manager = new SegmentCompletionManager(helixManager, _segmentManager, mock(ControllerMetrics.class),
        mock(LeadControllerManager.class), SegmentCompletionProtocol.getDefaultMaxSegmentCommitTimeSeconds(),
        new SegmentCompletionConfig(new PinotConfiguration())) {
      @Override
      protected boolean isLeader(String tableName) {
        return true;
      }

      @Override
      protected long getCurrentTimeMs() {
        return 1000L;
      }

      @Override
      protected StreamPartitionMsgOffsetFactory getStreamPartitionMsgOffsetFactory(LLCSegmentName name) {
        return new LongMsgOffsetFactory();
      }
    };
    doAnswer(invocation -> {
      CommittingSegmentDescriptor descriptor = invocation.getArgument(1);
      _metadata.setStatus(Status.DONE);
      _metadata.setEndOffset(descriptor.getNextOffset());
      _metadata.setDownloadUrl(descriptor.getSegmentLocation());
      return null;
    }).when(_segmentManager).commitSegmentMetadata(anyString(), any());
  }

  @AfterMethod
  public void tearDown() {
    SegmentCompletionProtocol.setMaxSegmentCommitTimeMs(_previousCommitTime);
  }

  @Test
  public void testOldInputsCannotCreateAnAttemptAfterHandoff()
      throws Exception {
    assign(SERVER_B, SegmentStateModel.CONSUMING);

    assertStaleInputsAreIgnored(SERVER_A);
    assertEquals(commitEnd(SERVER_A, "10").getStatus(), ControllerResponseStatus.FAILED);

    assertTrue(fsms().isEmpty());
    verify(_segmentManager, never()).commitSegmentFile(anyString(), any());
    verify(_segmentManager, never()).reduceSegmentSizeAndReset(any(), anyInt());
    verify(_segmentManager, never()).segmentStoppedConsuming(any(), anyString());
  }

  @Test
  public void testOldInputsCannotChangeNewLeaderAttempt()
      throws Exception {
    assign(SERVER_B, SegmentStateModel.CONSUMING);
    assertEquals(_manager.segmentConsumed(params(SERVER_B, "10")).getStatus(), ControllerResponseStatus.COMMIT);
    BlockingSegmentCompletionFSM fsm = (BlockingSegmentCompletionFSM) fsms().get(SEGMENT);
    long deadline = fsm._maxTimeAllowedToCommitMs;

    assertStaleInputsAreIgnored(SERVER_A);
    assertEquals(commitEnd(SERVER_A, "10").getStatus(), ControllerResponseStatus.FAILED);

    assertSame(fsms().get(SEGMENT), fsm);
    assertEquals(fsm._state, BlockingSegmentCompletionFSMState.COMMITTER_NOTIFIED);
    assertEquals(fsm._maxTimeAllowedToCommitMs, deadline);
    assertEquals(fsm._commitStateMap.keySet(), Set.of(SERVER_B));
    assertTrue(fsm._excludedServerStateMap.isEmpty());
    assertEquals(_manager.segmentCommitStart(params(SERVER_B, "10")).getStatus(),
        ControllerResponseStatus.COMMIT_CONTINUE);
  }

  @Test
  public void testExactPreviouslyAcceptedCommitFinishesAfterHandoffAndRetriesWithoutPublishing()
      throws Exception {
    acceptUpload(SERVER_A);
    BlockingSegmentCompletionFSM fsm = (BlockingSegmentCompletionFSM) fsms().get(SEGMENT);
    assign(SERVER_B, SegmentStateModel.CONSUMING);

    assertStaleInputsAreIgnored(SERVER_A);
    assertEquals(commitEnd(SERVER_A, "11").getStatus(), ControllerResponseStatus.FAILED);
    assertSame(fsms().get(SEGMENT), fsm);
    assertEquals(fsm._state, BlockingSegmentCompletionFSMState.COMMITTER_UPLOADING);
    assertEquals(commitEnd(SERVER_A, "10").getStatus(), ControllerResponseStatus.COMMIT_SUCCESS);
    assertTrue(fsms().isEmpty());
    String selectedArtifact = _metadata.getDownloadUrl();

    assertEquals(commitEnd(SERVER_A, "10").getStatus(), ControllerResponseStatus.COMMIT_SUCCESS);
    assertEquals(commitEnd(SERVER_A, "11").getStatus(), ControllerResponseStatus.FAILED);
    assertTrue(fsms().isEmpty());
    assertEquals(_metadata.getDownloadUrl(), selectedArtifact);
    verify(_segmentManager, times(1)).commitSegmentFile(anyString(), any());
    verify(_segmentManager, times(1)).commitSegmentMetadata(anyString(), any());
  }

  @Test
  public void testDoneRetryRequiresWinningCrcAndNeverKeepsAnUnverifiedLocalCopy()
      throws Exception {
    acceptUpload(SERVER_A);
    assertEquals(commitEnd(SERVER_A, "10").getStatus(), ControllerResponseStatus.COMMIT_SUCCESS);

    assertEquals(commitEnd(SERVER_A, "10", 5678L).getStatus(), ControllerResponseStatus.FAILED);
    Params missingMetadata = params(SERVER_A, "10");
    assertEquals(_manager.segmentCommitEnd(missingMetadata,
        CommittingSegmentDescriptor.fromSegmentCompletionReqParams(missingMetadata)).getStatus(),
        ControllerResponseStatus.FAILED);
    assertEquals(_manager.segmentConsumed(params(SERVER_A, "10")).getStatus(), ControllerResponseStatus.DISCARD);
    assertEquals(_manager.segmentCommitStart(params(SERVER_A, "10")).getStatus(), ControllerResponseStatus.DISCARD);
    assertEquals(commitEnd(SERVER_A, "10").getStatus(), ControllerResponseStatus.COMMIT_SUCCESS);
    assertTrue(fsms().isEmpty());
    verify(_segmentManager, times(1)).commitSegmentFile(anyString(), any());
    verify(_segmentManager, times(1)).commitSegmentMetadata(anyString(), any());
  }

  @Test
  public void testNewLeaderCannotExtendOrStartTheOldAcceptedAttempt()
      throws Exception {
    acceptUpload(SERVER_A);
    BlockingSegmentCompletionFSM fsm = (BlockingSegmentCompletionFSM) fsms().get(SEGMENT);
    long deadline = fsm._maxTimeAllowedToCommitMs;
    assign(SERVER_B, SegmentStateModel.CONSUMING);

    assertEquals(_manager.extendBuildTime(params(SERVER_B, "10").withExtraTimeSec(100)).getStatus(),
        ControllerResponseStatus.FAILED);
    assertEquals(_manager.segmentCommitStart(params(SERVER_B, "10")).getStatus(), ControllerResponseStatus.FAILED);
    assertEquals(fsm._maxTimeAllowedToCommitMs, deadline);
    assertEquals(fsm._state, BlockingSegmentCompletionFSMState.COMMITTER_UPLOADING);
    assertEquals(commitEnd(SERVER_A, "10").getStatus(), ControllerResponseStatus.COMMIT_SUCCESS);
  }

  @Test
  public void testAcceptedCommitSurvivesItsServerStoppedNotification()
      throws Exception {
    acceptUpload(SERVER_A);
    assertEquals(_manager.segmentStoppedConsuming(params(SERVER_A, "10")).getStatus(),
        ControllerResponseStatus.PROCESSED);
    assign(SERVER_B, SegmentStateModel.CONSUMING);

    assertEquals(commitEnd(SERVER_A, "10").getStatus(), ControllerResponseStatus.COMMIT_SUCCESS);
    assertTrue(fsms().isEmpty());
  }

  @Test
  public void testPauselessAcceptedUploaderCanExtendUntilItsSegmentOwnerChanges()
      throws Exception {
    acceptPauselessUpload();

    assertEquals(_manager.extendBuildTime(params(SERVER_A, "10").withExtraTimeSec(100)).getStatus(),
        ControllerResponseStatus.PROCESSED);
    assign(SERVER_B, SegmentStateModel.ONLINE);
    assertEquals(_manager.extendBuildTime(params(SERVER_A, "10").withExtraTimeSec(100)).getStatus(),
        ControllerResponseStatus.FAILED);
  }

  @Test
  public void testPauselessSegmentOwnerRetainsItsLeaseAfterNewConsumingLeaderMoves()
      throws Exception {
    acceptPauselessUpload();
    ReplicateModeAssignmentUtils.setAssignment(_idealState, 0, SERVER_B, SERVERS);
    _idealState.setInstanceStateMap(NEXT_SEGMENT, Map.of(SERVER_B, SegmentStateModel.CONSUMING));
    BlockingSegmentCompletionFSM fsm = (BlockingSegmentCompletionFSM) fsms().get(SEGMENT);

    assertEquals(_manager.extendBuildTime(params(SERVER_A, "10").withExtraTimeSec(100)).getStatus(),
        ControllerResponseStatus.PROCESSED);
    // Native commit-start retries while an upload is already running return HOLD, retaining that accepted attempt.
    assertEquals(_manager.segmentCommitStart(params(SERVER_A, "10")).getStatus(),
        ControllerResponseStatus.HOLD);
    assertEquals(fsm._state, BlockingSegmentCompletionFSMState.COMMITTER_UPLOADING);
    assertTrue(fsm.isAcceptedCommitter(SERVER_A, "10"));
    verify(_segmentManager, times(1)).commitSegmentMetadataToCommitting(anyString(), any());
    assertEquals(_manager.extendBuildTime(params(SERVER_A, "11").withExtraTimeSec(100)).getStatus(),
        ControllerResponseStatus.FAILED);
    assertEquals(_manager.extendBuildTime(params(SERVER_B, "10").withExtraTimeSec(100)).getStatus(),
        ControllerResponseStatus.FAILED);
    assertEquals(_manager.segmentCommitStart(params(SERVER_B, "10")).getStatus(), ControllerResponseStatus.FAILED);
    assertSame(fsms().get(SEGMENT), fsm);
    assertEquals(_idealState.getInstanceStateMap(SEGMENT), Map.of(SERVER_A, SegmentStateModel.ONLINE));
    Params noOffset = new Params().withSegmentName(SEGMENT).withInstanceId(SERVER_A);
    assertEquals(_manager.segmentStoppedConsuming(noOffset).getStatus(), ControllerResponseStatus.PROCESSED);
    assertEquals(_manager.reduceSegmentSizeAndReset(noOffset.withNumRows(100)).getStatus(),
        ControllerResponseStatus.DISCARD);
    verify(_segmentManager, never()).segmentStoppedConsuming(any(), anyString());
    assertEquals(_idealState.getInstanceStateMap(NEXT_SEGMENT), Map.of(SERVER_B, SegmentStateModel.CONSUMING));
    assertEquals(ReplicateModeAssignmentUtils.getLeader(_idealState, 0), SERVER_B);

    _metadata.setEndOffset("11");
    assertEquals(_manager.extendBuildTime(params(SERVER_A, "10").withExtraTimeSec(100)).getStatus(),
        ControllerResponseStatus.FAILED);
    assertEquals(_manager.extendBuildTime(params(SERVER_A, "11").withExtraTimeSec(100)).getStatus(),
        ControllerResponseStatus.FAILED);
    assertSame(fsms().get(SEGMENT), fsm);
  }

  private void acceptPauselessUpload()
      throws Exception {
    enablePauselessConsumption();
    doAnswer(invocation -> {
      _metadata.setStatus(Status.COMMITTING);
      _metadata.setEndOffset("10");
      _idealState.setInstanceStateMap(SEGMENT, Map.of(SERVER_A, SegmentStateModel.ONLINE));
      return null;
    }).when(_segmentManager).commitSegmentMetadataToCommitting(anyString(), any());
    acceptUpload(SERVER_A);
  }

  private void enablePauselessConsumption() {
    StreamIngestionConfig stream = new StreamIngestionConfig(List.of());
    stream.setPauselessConsumptionEnabled(true);
    IngestionConfig ingestion = new IngestionConfig();
    ingestion.setStreamIngestionConfig(stream);
    _tableConfig.setIngestionConfig(ingestion);
  }

  @DataProvider
  public Object[][] invalidPartialPauselessCommitStarts() {
    return new Object[][]{{"wrongOwner"}, {"wrongLeader"}, {"earlierOffset"}, {"laterOffset"}, {"expired"},
        {"multipleOwners"}, {"offlineOwner"}, {"missingEndOffset"}};
  }

  @Test(dataProvider = "invalidPartialPauselessCommitStarts")
  public void testPartialPauselessCommitStartPreservesMembershipAndRangeGuards(String invalidCondition)
      throws Exception {
    enablePauselessConsumption();
    _metadata.setStatus(Status.COMMITTING);
    _metadata.setEndOffset("10");
    String caller = SERVER_A;
    String offset = "10";
    switch (invalidCondition) {
      case "wrongOwner":
        caller = SERVER_B;
        break;
      case "wrongLeader":
        ReplicateModeAssignmentUtils.setAssignment(_idealState, 0, SERVER_B, SERVERS);
        break;
      case "earlierOffset":
        offset = "9";
        break;
      case "laterOffset":
        offset = "11";
        break;
      case "expired":
        when(_segmentManager.isExceededMaxSegmentCompletionTime(anyString(), anyString(), anyLong())).thenReturn(true);
        break;
      case "multipleOwners":
        _idealState.setInstanceStateMap(SEGMENT,
            Map.of(SERVER_A, SegmentStateModel.CONSUMING, SERVER_B, SegmentStateModel.CONSUMING));
        break;
      case "offlineOwner":
        _idealState.setInstanceStateMap(SEGMENT, Map.of(SERVER_A, SegmentStateModel.OFFLINE));
        break;
      case "missingEndOffset":
        _metadata.setEndOffset(null);
        break;
      default:
        throw new IllegalArgumentException(invalidCondition);
    }

    assertEquals(_manager.segmentConsumed(params(caller, offset)).getStatus(), ControllerResponseStatus.DISCARD);
    assertEquals(_manager.segmentCommitStart(params(caller, offset)).getStatus(), ControllerResponseStatus.FAILED);
    assertEquals(_manager.extendBuildTime(params(caller, offset).withExtraTimeSec(100)).getStatus(),
        ControllerResponseStatus.FAILED);
    assertEquals(commitEnd(caller, offset).getStatus(), ControllerResponseStatus.FAILED);
    Params noOffset = new Params().withSegmentName(SEGMENT).withInstanceId(caller);
    assertEquals(_manager.segmentStoppedConsuming(noOffset).getStatus(), ControllerResponseStatus.PROCESSED);
    assertEquals(_manager.reduceSegmentSizeAndReset(noOffset.withNumRows(100)).getStatus(),
        ControllerResponseStatus.DISCARD);
    assertTrue(fsms().isEmpty());
    assertEquals(_metadata.getStatus(), Status.COMMITTING);
    verify(_segmentManager, never()).commitSegmentMetadataToCommitting(anyString(), any());
    verify(_segmentManager, never()).commitSegmentFile(anyString(), any());
    verify(_segmentManager, never()).reduceSegmentSizeAndReset(any(), anyInt());
    verify(_segmentManager, never()).segmentStoppedConsuming(any(), anyString());
  }

  @Test
  public void testPauselessSegmentOwnerResumesExactRangeAfterControllerLosesItsFsm()
      throws Exception {
    acceptPauselessUpload();
    ReplicateModeAssignmentUtils.setAssignment(_idealState, 0, SERVER_B, SERVERS);
    _idealState.setInstanceStateMap(NEXT_SEGMENT, Map.of(SERVER_B, SegmentStateModel.CONSUMING));
    // A restarted Controller has persisted COMMITTING metadata, but no accepted in-memory attempt.
    fsms().clear();

    assertEquals(commitEnd(SERVER_A, "10").getStatus(), ControllerResponseStatus.FAILED);
    assertEquals(_manager.segmentConsumed(params(SERVER_B, "10")).getStatus(), ControllerResponseStatus.DISCARD);
    assertEquals(_manager.segmentConsumed(params(SERVER_A, "11")).getStatus(), ControllerResponseStatus.DISCARD);
    when(_segmentManager.isExceededMaxSegmentCompletionTime(anyString(), anyString(), anyLong())).thenReturn(true);
    assertEquals(_manager.segmentConsumed(params(SERVER_A, "10")).getStatus(), ControllerResponseStatus.DISCARD);
    assertTrue(fsms().isEmpty());

    when(_segmentManager.isExceededMaxSegmentCompletionTime(anyString(), anyString(), anyLong())).thenReturn(false);
    assertEquals(_manager.segmentConsumed(params(SERVER_A, "10")).getStatus(), ControllerResponseStatus.COMMIT);
    assertEquals(_manager.segmentCommitStart(params(SERVER_A, "10")).getStatus(),
        ControllerResponseStatus.COMMIT_CONTINUE);
    assertTrue(fsms().get(SEGMENT).isAcceptedCommitter(SERVER_A, "10"));
    assertEquals(ReplicateModeAssignmentUtils.getLeader(_idealState, 0), SERVER_B);
    verify(_segmentManager, never()).commitSegmentFile(anyString(), any());
  }

  @DataProvider
  public Object[][] commitFailures() {
    return new Object[][]{{false, false}, {false, true}, {true, false}, {true, true}};
  }

  @Test(dataProvider = "commitFailures")
  public void testReturnedCommitIoFailureAbortsAndRemovesOnlyThatAttempt(boolean pauseless, boolean metadataFailure)
      throws Exception {
    if (pauseless) {
      StreamIngestionConfig stream = new StreamIngestionConfig(List.of());
      stream.setPauselessConsumptionEnabled(true);
      IngestionConfig ingestion = new IngestionConfig();
      ingestion.setStreamIngestionConfig(stream);
      _tableConfig.setIngestionConfig(ingestion);
      doAnswer(invocation -> {
        _metadata.setStatus(Status.COMMITTING);
        _metadata.setEndOffset("10");
        assign(SERVER_A, SegmentStateModel.ONLINE);
        return null;
      }).when(_segmentManager).commitSegmentMetadataToCommitting(anyString(), any());
    }
    acceptUpload(SERVER_A);
    BlockingSegmentCompletionFSM fsm = (BlockingSegmentCompletionFSM) fsms().get(SEGMENT);
    if (!metadataFailure) {
      doThrow(new IllegalStateException("publication failed")).when(_segmentManager)
          .commitSegmentFile(anyString(), any());
    } else if (pauseless) {
      doThrow(new IllegalStateException("metadata failed")).when(_segmentManager)
          .commitSegmentMetadataToDone(anyString(), any());
    } else {
      doThrow(new IllegalStateException("metadata failed")).when(_segmentManager)
          .commitSegmentMetadata(anyString(), any());
    }

    assertEquals(commitEnd(SERVER_A, "10").getStatus(), ControllerResponseStatus.FAILED);

    assertEquals(fsm._state, BlockingSegmentCompletionFSMState.ABORTED);
    assertEquals(fsm.getRecoveryStatus(), RecoveryStatus.NO_ACTIVE_COMMIT);
    assertEquals(_manager.getRecoveryStatus(SEGMENT), RecoveryStatus.NO_ACTIVE_COMMIT);
    assertTrue(fsms().isEmpty());
    assertEquals(_metadata.getStatus(), pauseless ? Status.COMMITTING : Status.IN_PROGRESS);
  }

  @Test
  public void testAcceptedCommitDoesNotHoldHandoffGateDuringPublication()
      throws Exception {
    acceptUpload(SERVER_A);
    CountDownLatch publicationEntered = new CountDownLatch(1);
    CountDownLatch releasePublication = new CountDownLatch(1);
    doAnswer(invocation -> {
      publicationEntered.countDown();
      assertTrue(releasePublication.await(5, TimeUnit.SECONDS));
      return null;
    }).when(_segmentManager).commitSegmentFile(anyString(), any());
    ExecutorService executor = Executors.newFixedThreadPool(3);
    try {
      Future<SegmentCompletionProtocol.Response> response = executor.submit(() -> commitEnd(SERVER_A, "10"));
      assertTrue(publicationEntered.await(5, TimeUnit.SECONDS));
      assertEquals(_manager.getRecoveryStatus(SEGMENT), RecoveryStatus.COMMIT_IN_PROGRESS);
      AtomicReference<Thread> pollingThread = new AtomicReference<>();
      Future<SegmentCompletionProtocol.Response> polling = executor.submit(() -> {
        pollingThread.set(Thread.currentThread());
        return _manager.segmentConsumed(params(SERVER_A, "10"));
      });
      TestUtils.waitForCondition(ignored -> pollingThread.get() != null
          && pollingThread.get().getState() == Thread.State.BLOCKED, 5000L,
          "Protocol polling did not reach the commit monitor");
      Future<?> handoff = executor.submit(() -> _manager.withSegmentCompletionLock(SEGMENT, () -> {
        assign(SERVER_B, SegmentStateModel.CONSUMING);
        return null;
      }));
      handoff.get(5, TimeUnit.SECONDS);
      releasePublication.countDown();
      assertEquals(response.get(5, TimeUnit.SECONDS).getStatus(), ControllerResponseStatus.COMMIT_SUCCESS);
      assertEquals(polling.get(5, TimeUnit.SECONDS).getStatus(), ControllerResponseStatus.DISCARD);
      assertTrue(fsms().isEmpty());
    } finally {
      releasePublication.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testLocalArtifactDownloadUrlRetainsImmutableAttemptLeaf()
      throws Exception {
    acceptUpload(SERVER_A);
    String artifact = SEGMENT + ".attempt-123";
    doAnswer(invocation -> {
      CommittingSegmentDescriptor descriptor = invocation.getArgument(1);
      descriptor.setSegmentLocation("file:///tmp/" + artifact);
      return null;
    }).when(_segmentManager).commitSegmentFile(anyString(), any());

    assertEquals(commitEnd(SERVER_A, "10").getStatus(), ControllerResponseStatus.COMMIT_SUCCESS);

    assertTrue(_metadata.getDownloadUrl().endsWith("/" + artifact));
  }

  @Test
  public void testHandoffWaitsUntilAdmissionAndFsmMutationFinish()
      throws Exception {
    CountDownLatch admissionEntered = new CountDownLatch(1);
    CountDownLatch releaseAdmission = new CountDownLatch(1);
    CountDownLatch handoffAttempted = new CountDownLatch(1);
    CountDownLatch handoffFinished = new CountDownLatch(1);
    when(_segmentManager.getIdealState(TABLE)).thenAnswer(invocation -> {
      admissionEntered.countDown();
      assertTrue(releaseAdmission.await(5, TimeUnit.SECONDS));
      return _idealState;
    });
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<SegmentCompletionProtocol.Response> response = executor.submit(
          () -> _manager.segmentConsumed(params(SERVER_A, "10")));
      assertTrue(admissionEntered.await(5, TimeUnit.SECONDS));
      Future<?> handoff = executor.submit(() -> {
        handoffAttempted.countDown();
        _manager.withSegmentCompletionLock(SEGMENT, () -> {
          assign(SERVER_B, SegmentStateModel.CONSUMING);
          handoffFinished.countDown();
          return null;
        });
      });
      assertTrue(handoffAttempted.await(5, TimeUnit.SECONDS));
      assertEquals(handoffFinished.getCount(), 1L);
      releaseAdmission.countDown();
      assertEquals(response.get(5, TimeUnit.SECONDS).getStatus(), ControllerResponseStatus.COMMIT);
      handoff.get(5, TimeUnit.SECONDS);
      assertEquals(_manager.segmentConsumed(params(SERVER_A, "10")).getStatus(), ControllerResponseStatus.DISCARD);
    } finally {
      releaseAdmission.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testCleanupDoesNotRemoveAReplacementFsm()
      throws Exception {
    SegmentCompletionFSM oldFsm = mock(SegmentCompletionFSM.class);
    SegmentCompletionFSM replacement = mock(SegmentCompletionFSM.class);
    when(oldFsm.getStreamPartitionMsgOffsetFactory()).thenReturn(new LongMsgOffsetFactory());
    when(oldFsm.isDone()).thenReturn(true);
    when(oldFsm.segmentConsumed(anyString(), any(), anyString())).thenAnswer(invocation -> {
      fsms().put(SEGMENT, replacement);
      return new SegmentCompletionProtocol.Response(
          new SegmentCompletionProtocol.Response.Params().withStatus(ControllerResponseStatus.HOLD));
    });
    fsms().put(SEGMENT, oldFsm);

    _manager.segmentConsumed(params(SERVER_A, "10"));

    assertSame(fsms().get(SEGMENT), replacement);
  }

  private void assertStaleInputsAreIgnored(String server) {
    assertEquals(_manager.segmentConsumed(params(server, "10")).getStatus(), ControllerResponseStatus.DISCARD);
    assertEquals(_manager.segmentCommitStart(params(server, "10")).getStatus(), ControllerResponseStatus.FAILED);
    assertEquals(_manager.extendBuildTime(params(server, "10").withExtraTimeSec(100)).getStatus(),
        ControllerResponseStatus.FAILED);
    assertEquals(_manager.reduceSegmentSizeAndReset(params(server, "10").withNumRows(100)).getStatus(),
        ControllerResponseStatus.DISCARD);
    assertEquals(_manager.segmentStoppedConsuming(params(server, "10")).getStatus(),
        ControllerResponseStatus.PROCESSED);
  }

  private void acceptUpload(String server) {
    assertEquals(_manager.segmentConsumed(params(server, "10")).getStatus(), ControllerResponseStatus.COMMIT);
    assertEquals(_manager.segmentCommitStart(params(server, "10")).getStatus(),
        ControllerResponseStatus.COMMIT_CONTINUE);
  }

  private SegmentCompletionProtocol.Response commitEnd(String server, String offset) {
    return commitEnd(server, offset, 1234L);
  }

  private SegmentCompletionProtocol.Response commitEnd(String server, String offset, long crc) {
    Params params = params(server, offset).withSegmentLocation("http://controller:9000/segments/attempt");
    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getCrc()).thenReturn(crc);
    return _manager.segmentCommitEnd(params,
        CommittingSegmentDescriptor.fromSegmentCompletionReqParamsAndMetadata(params, segmentMetadata));
  }

  private void assign(String server, String state) {
    ReplicateModeAssignmentUtils.setAssignment(_idealState, 0, server, SERVERS);
    _idealState.setInstanceStateMap(SEGMENT, Map.of(server, state));
  }

  private static Params params(String server, String offset) {
    return new Params().withSegmentName(SEGMENT).withInstanceId(server).withStreamPartitionMsgOffset(offset)
        .withReason(SegmentCompletionProtocol.REASON_ROW_LIMIT);
  }

  @SuppressWarnings("unchecked")
  private Map<String, SegmentCompletionFSM> fsms()
      throws ReflectiveOperationException {
    Field field = SegmentCompletionManager.class.getDeclaredField("_fsmMap");
    field.setAccessible(true);
    return (Map<String, SegmentCompletionFSM>) field.get(_manager);
  }
}
