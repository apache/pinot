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
package org.apache.pinot.segment.local.utils.tablestate;

import java.util.Map;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ERROR;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.OFFLINE;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Tests readiness against changing assignments and the receiving server's current state. Each test owns its fixture
/// and confines all record mutations to the invoking thread.
public class TableStateUtilsTest {
  private static final String TABLE = "readiness_REALTIME";
  private static final String SERVER = "Server_receiving_1234";
  private static final String OTHER_SERVER = "Server_other_1234";
  private static final String SESSION = "test-session";

  @Test
  public void testRequiresCurrentConsumerAssignment() {
    Assignment fixture = new Assignment();
    when(fixture._accessor.getProperty(fixture._keys.idealStates(TABLE))).thenReturn(null);
    assertFalse(fixture.isReady());
    // Preserve the legacy table-wide check's behavior when no IdealState is available.
    assertTrue(TableStateUtils.isAllSegmentsLoaded(fixture._manager, TABLE));

    when(fixture._accessor.getProperty(fixture._keys.idealStates(TABLE))).thenReturn(fixture._idealState);
    fixture._idealState.getRecord().getMapFields().remove(fixture._consumer.getSegmentName());
    assertFalse(fixture.isReady());
    fixture._idealState.setInstanceStateMap(fixture._consumer.getSegmentName(), Map.of(OTHER_SERVER, CONSUMING));
    assertFalse(fixture.isReady());
    fixture.assign(fixture._consumer.getSegmentName(), OFFLINE);
    assertFalse(fixture.isReady());
    fixture.assign(fixture._consumer.getSegmentName(), ERROR);
    assertFalse(fixture.isReady());
    fixture.assign(fixture._consumer.getSegmentName(), CONSUMING);
    assertTrue(fixture.isReady());
    fixture.assign(fixture._consumer.getSegmentName(), ONLINE);
    assertTrue(fixture.isReady(), "The consumer's own ONLINE transition must not block its consumption");
  }

  @Test
  public void testRequiresEveryAssignedPredecessor() {
    Assignment fixture = new Assignment();
    String n0 = segment(1, 0);
    String n1 = segment(1, 1);
    String n2 = segment(1, 2);
    fixture.assign(n0, ONLINE);
    fixture.assign(n1, ONLINE);
    fixture.assign(n2, ONLINE);
    fixture._currentState.setState(n0, ONLINE);
    fixture._currentState.setState(n2, ONLINE);
    assertFalse(fixture.isReady(), "Loading the immediate predecessor does not cover an older missing segment");
    fixture._currentState.setState(n1, OFFLINE);
    assertFalse(fixture.isReady());
    fixture._currentState.setState(n1, ERROR);
    assertFalse(fixture.isReady());
    fixture._currentState.setState(n1, CONSUMING);
    assertFalse(fixture.isReady());
    fixture._currentState.setState(n1, ONLINE);
    assertTrue(fixture.isReady());
    assertTrue(TableStateUtils.isAllSegmentsLoaded(fixture._manager, TABLE));
  }

  @Test
  public void testScopesPrerequisitesToEarlierOnlineSegmentsOnThisServer() {
    Assignment fixture = new Assignment();
    String predecessor = segment(1, 1);
    fixture.assign(predecessor, ONLINE);
    fixture._currentState.setState(predecessor, ONLINE);
    fixture.assign(segment(2, 0), ONLINE);
    fixture.assign(segment(1, 4), ONLINE);
    fixture.assign(fixture._consumer.getSegmentName(), ONLINE);
    fixture.assign(segment(1, 2), CONSUMING);
    fixture._idealState.setInstanceStateMap(segment(1, 0), Map.of(OTHER_SERVER, ONLINE));
    assertTrue(TableStateUtils.isReadyToConsume(fixture._manager, TABLE, fixture._consumer, segmentName -> {
      assertEquals(segmentName, predecessor, "Only required segments should be considered for local readiness");
      return false;
    }));
    assertFalse(TableStateUtils.isAllSegmentsLoaded(fixture._manager, TABLE),
        "The legacy table-wide check still includes other partitions and successors");
  }

  @Test
  public void testRequiresUploadedSegments() {
    Assignment fixture = new Assignment();
    String uploadedSegment = "uploadedSegment";
    fixture.assign(uploadedSegment, ONLINE);
    assertFalse(fixture.isReady(), "A non-LLC segment may hold base rows for this partition");
    fixture._currentState.setState(uploadedSegment, ONLINE);
    assertTrue(fixture.isReady());
  }

  @Test
  public void testLocallyReadyPredecessorDoesNotHideOtherMissingSegments() {
    Assignment fixture = new Assignment();
    String n1 = segment(1, 1);
    String n2 = segment(1, 2);
    fixture.assign(n1, ONLINE);
    fixture.assign(n2, ONLINE);
    assertFalse(TableStateUtils.isReadyToConsume(fixture._manager, TABLE, fixture._consumer, n2::equals));
    fixture._currentState.setState(n1, ONLINE);
    assertTrue(TableStateUtils.isReadyToConsume(fixture._manager, TABLE, fixture._consumer, n2::equals),
        "A predecessor that finished locally need not wait for its ONLINE transition");
    assertFalse(fixture.isReady(), "Without local readiness, N2 must be ONLINE in current state");
    fixture._idealState.getRecord().getMapFields().remove(fixture._consumer.getSegmentName());
    assertFalse(TableStateUtils.isReadyToConsume(fixture._manager, TABLE, fixture._consumer, segmentName -> true),
        "Local readiness cannot bypass a missing consumer assignment");
  }

  @Test
  public void testRechecksDroppedAndReassignedSegments() {
    Assignment fixture = new Assignment();
    String predecessor = segment(1, 1);
    fixture.assign(predecessor, ONLINE);
    assertFalse(fixture.isReady());
    fixture._idealState.getRecord().getMapFields().remove(predecessor);
    assertTrue(fixture.isReady(), "A dropped predecessor must not block forever");
    fixture.assign(predecessor, ONLINE);
    assertFalse(fixture.isReady(), "A newly assigned predecessor must be checked again");
    fixture._idealState.setInstanceStateMap(predecessor, Map.of(OTHER_SERVER, ONLINE));
    assertTrue(fixture.isReady());
    fixture._idealState.setInstanceStateMap(fixture._consumer.getSegmentName(), Map.of(OTHER_SERVER, CONSUMING));
    assertFalse(fixture.isReady());
  }

  @Test
  public void testRequiresLiveInstanceAndCurrentStateForUnloadedPredecessor() {
    Assignment fixture = new Assignment();
    String predecessor = segment(1, 1);
    fixture.assign(predecessor, ONLINE);
    when(fixture._accessor.getProperty(fixture._keys.liveInstance(SERVER))).thenReturn(null);
    assertFalse(fixture.isReady());
    when(fixture._accessor.getProperty(fixture._keys.liveInstance(SERVER))).thenReturn(fixture._liveInstance);
    when(fixture._accessor.getProperty(fixture._keys.currentState(SERVER, SESSION, TABLE))).thenReturn(null);
    assertFalse(fixture.isReady());
    when(fixture._accessor.getProperty(fixture._keys.currentState(SERVER, SESSION, TABLE)))
        .thenReturn(fixture._currentState);
    assertFalse(fixture.isReady());
    fixture._currentState.setState(predecessor, ONLINE);
    assertTrue(fixture.isReady());
  }

  private static String segment(int partitionId, int sequenceNumber) {
    return new LLCSegmentName(TABLE, partitionId, sequenceNumber, 0).getSegmentName();
  }

  /// Supplies real mutable Helix records through a mocked accessor; accessed only by the invoking test thread.
  private static class Assignment {
    private final LLCSegmentName _consumer = new LLCSegmentName(TABLE, 1, 3, 0);
    private final HelixManager _manager = mock(HelixManager.class);
    private final HelixDataAccessor _accessor = mock(HelixDataAccessor.class);
    private final PropertyKey.Builder _keys = new PropertyKey.Builder("test-cluster");
    private final IdealState _idealState = new IdealState(TABLE);
    private final CurrentState _currentState = new CurrentState(TABLE);
    private final LiveInstance _liveInstance = mock(LiveInstance.class);

    Assignment() {
      when(_manager.getInstanceName()).thenReturn(SERVER);
      when(_manager.getHelixDataAccessor()).thenReturn(_accessor);
      when(_accessor.keyBuilder()).thenReturn(_keys);
      when(_accessor.getProperty(_keys.idealStates(TABLE))).thenReturn(_idealState);
      when(_accessor.getProperty(_keys.liveInstance(SERVER))).thenReturn(_liveInstance);
      when(_liveInstance.getEphemeralOwner()).thenReturn(SESSION);
      when(_accessor.getProperty(_keys.currentState(SERVER, SESSION, TABLE))).thenReturn(_currentState);
      assign(_consumer.getSegmentName(), CONSUMING);
    }

    void assign(String segmentName, String state) {
      _idealState.setInstanceStateMap(segmentName, Map.of(SERVER, state));
    }

    boolean isReady() {
      return TableStateUtils.isReadyToConsume(_manager, TABLE, _consumer, segmentName -> false);
    }
  }
}
