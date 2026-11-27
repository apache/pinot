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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.helix.HelixManager;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.realtime.BlockingSegmentCompletionFSM.BlockingSegmentCompletionFSMState;
import org.apache.pinot.controller.helix.core.realtime.SegmentCompletionFSM.RecoveryStatus;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.stream.LongMsgOffsetFactory;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Verifies read-only recovery observations using isolated FSMs, including polling while the commit monitor is held.
public class SegmentCompletionRecoveryStatusTest {
  private static final String SEGMENT_NAME = "testTable__0__0__20250228T1903Z";
  private static final LLCSegmentName SEGMENT = new LLCSegmentName(SEGMENT_NAME);
  private static final long DEADLINE_MS = 1000L;
  private long _previousMaxCommitTimeMs;

  @BeforeMethod
  public void rememberProtocolTimeout() {
    _previousMaxCommitTimeMs = SegmentCompletionProtocol.getMaxSegmentCommitTimeMs();
  }

  @AfterMethod(alwaysRun = true)
  public void restoreProtocolTimeout() {
    SegmentCompletionProtocol.setMaxSegmentCommitTimeMs(_previousMaxCommitTimeMs);
  }

  @DataProvider
  public Object[][] states() {
    List<Object[]> states = new ArrayList<>();
    for (boolean pauseless : List.of(false, true)) {
      for (BlockingSegmentCompletionFSMState state : BlockingSegmentCompletionFSMState.values()) {
        states.add(new Object[]{pauseless, state});
      }
    }
    return states.toArray(Object[][]::new);
  }

  @Test(dataProvider = "states")
  public void testDeadlineObservationDoesNotAdvanceState(boolean pauseless, BlockingSegmentCompletionFSMState state) {
    SegmentCompletionManager completionManager = mock(SegmentCompletionManager.class);
    BlockingSegmentCompletionFSM fsm = newFsm(pauseless, completionManager);
    fsm._state = state;
    fsm._maxTimeAllowedToCommitMs = DEADLINE_MS;
    when(completionManager.getCurrentTimeMs()).thenReturn(DEADLINE_MS);
    boolean acceptedAttempt = state == BlockingSegmentCompletionFSMState.COMMITTER_DECIDED
        || state == BlockingSegmentCompletionFSMState.COMMITTER_NOTIFIED
        || state == BlockingSegmentCompletionFSMState.COMMITTER_UPLOADING;
    RecoveryStatus atDeadline = acceptedAttempt || state == BlockingSegmentCompletionFSMState.COMMITTING
        ? RecoveryStatus.COMMIT_IN_PROGRESS
        : RecoveryStatus.NO_ACTIVE_COMMIT;

    assertEquals(fsm.getRecoveryStatus(), atDeadline);
    when(completionManager.getCurrentTimeMs()).thenReturn(DEADLINE_MS + 1);
    RecoveryStatus afterDeadline;
    if (state == BlockingSegmentCompletionFSMState.COMMITTING) {
      afterDeadline = RecoveryStatus.COMMIT_IN_PROGRESS;
    } else {
      afterDeadline = acceptedAttempt ? RecoveryStatus.COMMIT_EXPIRED : RecoveryStatus.NO_ACTIVE_COMMIT;
    }
    assertEquals(fsm.getRecoveryStatus(), afterDeadline);
    assertEquals(fsm.getRecoveryStatus(), afterDeadline);
    assertEquals(fsm._state, state);
    assertEquals(fsm._maxTimeAllowedToCommitMs, DEADLINE_MS);
  }

  @Test
  public void testObservationDoesNotWaitForCommitMonitor()
      throws Exception {
    BlockingSegmentCompletionFSM fsm = newFsm(false, mock(SegmentCompletionManager.class));
    fsm._state = BlockingSegmentCompletionFSMState.COMMITTING;
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      synchronized (fsm) {
        Future<RecoveryStatus> observation = executor.submit(fsm::getRecoveryStatus);
        assertEquals(observation.get(5, TimeUnit.SECONDS), RecoveryStatus.COMMIT_IN_PROGRESS);
      }
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testAbsentFsmDoesNotReadMetadataOrCreateOne()
      throws Exception {
    PinotLLCRealtimeSegmentManager segmentManager = mock(PinotLLCRealtimeSegmentManager.class);
    HelixManager helixManager = mock(HelixManager.class);
    ControllerMetrics metrics = mock(ControllerMetrics.class);
    LeadControllerManager leadControllerManager = mock(LeadControllerManager.class);
    SegmentCompletionManager manager = new SegmentCompletionManager(helixManager, segmentManager, metrics,
        leadControllerManager, 30, new SegmentCompletionConfig(new PinotConfiguration()));

    assertEquals(manager.getRecoveryStatus(SEGMENT_NAME), RecoveryStatus.NO_ACTIVE_COMMIT);
    assertTrue(fsms(manager).isEmpty());
    verifyNoInteractions(segmentManager, helixManager, metrics, leadControllerManager);
  }

  @Test
  public void testExistingFsmObservationDoesNotRemoveIt()
      throws Exception {
    PinotLLCRealtimeSegmentManager segmentManager = mock(PinotLLCRealtimeSegmentManager.class);
    SegmentCompletionManager manager = new SegmentCompletionManager(mock(HelixManager.class), segmentManager,
        mock(ControllerMetrics.class), mock(LeadControllerManager.class), 30,
        new SegmentCompletionConfig(new PinotConfiguration()));
    SegmentCompletionFSM fsm = mock(SegmentCompletionFSM.class);
    when(fsm.getRecoveryStatus()).thenReturn(RecoveryStatus.COMMIT_EXPIRED);
    fsms(manager).put(SEGMENT_NAME, fsm);

    assertEquals(manager.getRecoveryStatus(SEGMENT_NAME), RecoveryStatus.COMMIT_EXPIRED);
    assertSame(fsms(manager).get(SEGMENT_NAME), fsm);
    verify(fsm).getRecoveryStatus();
    verifyNoInteractions(segmentManager);
  }

  @Test
  public void testCustomFsmDefaultsToUnknown() {
    SegmentCompletionFSM customFsm = mock(SegmentCompletionFSM.class, CALLS_REAL_METHODS);
    assertEquals(customFsm.getRecoveryStatus(), RecoveryStatus.UNKNOWN);
  }

  private static BlockingSegmentCompletionFSM newFsm(boolean pauseless, SegmentCompletionManager completionManager) {
    when(completionManager.getStreamPartitionMsgOffsetFactory(SEGMENT)).thenReturn(new LongMsgOffsetFactory());
    SegmentZKMetadata metadata = new SegmentZKMetadata(SEGMENT_NAME);
    metadata.setStatus(Status.IN_PROGRESS);
    metadata.setNumReplicas(3);
    PinotLLCRealtimeSegmentManager segmentManager = mock(PinotLLCRealtimeSegmentManager.class);
    return pauseless
        ? new PauselessSegmentCompletionFSM(segmentManager, completionManager, SEGMENT, metadata)
        : new BlockingSegmentCompletionFSM(segmentManager, completionManager, SEGMENT, metadata);
  }

  @SuppressWarnings("unchecked")
  private static Map<String, SegmentCompletionFSM> fsms(SegmentCompletionManager manager)
      throws Exception {
    Field field = SegmentCompletionManager.class.getDeclaredField("_fsmMap");
    field.setAccessible(true);
    return (Map<String, SegmentCompletionFSM>) field.get(manager);
  }
}
