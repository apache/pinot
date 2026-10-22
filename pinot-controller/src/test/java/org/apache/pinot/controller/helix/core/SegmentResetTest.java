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
package org.apache.pinot.controller.helix.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.StateModelDefinition;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class SegmentResetTest {
  private static final String SESSION_ID = "session_1";

  private final String _tableName = "myTable_OFFLINE";
  private final String _segmentName = "segment_1";
  private final String _instance1 = "instance_1";
  private final String _instance2 = "instance_2";

  private class MockPinotHelixResourceManager extends PinotHelixResourceManager {

    private final Set<String> _instanceToFail;

    public MockPinotHelixResourceManager(ControllerConf controllerConf) {
      super(controllerConf);
      _instanceToFail = new HashSet<>();
    }

    @Override
    public IdealState getTableIdealState(String tableNameWithType) {
      IdealState idealState = Mockito.mock(IdealState.class);
      when(idealState.getInstanceSet(_segmentName)).thenReturn(new HashSet<>(Arrays.asList(_instance1, _instance2)));
      when(idealState.getPartitionSet()).thenReturn(new HashSet<>(List.of(_segmentName)));
      return idealState;
    }

    @Override
    public ExternalView getTableExternalView(String tableNameWithType) {
      ExternalView externalView = Mockito.mock(ExternalView.class);
      Map<String, String> segmentStateMap = new HashMap<>();
      segmentStateMap.put(_instance1, CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE);
      segmentStateMap.put(_instance2, CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE);
      when(externalView.getStateMap(_segmentName)).thenReturn(segmentStateMap);
      return externalView;
    }

    @Override
    public void resetPartitionAllState(String instanceName, String resourceName, Set<String> resetPartitionNames) {
      if (_instanceToFail.contains(instanceName)) {
        throw new RuntimeException("Test: Fail reset for " + instanceName);
      }
    }

    public void addInstanceToFail(String instanceName) {
      _instanceToFail.add(instanceName);
    }
  }

  @Test
  public void testResetSegmentOneFailureOthersStillInvoked() {
    ControllerConf cfg = new ControllerConf();
    cfg.setZkStr("localhost:2181");
    cfg.setHelixClusterName("cluster01");
    MockPinotHelixResourceManager pinotHelixManager = new MockPinotHelixResourceManager(cfg);
    pinotHelixManager.addInstanceToFail(_instance1);

    RuntimeException runtimeException = Assert.expectThrows(RuntimeException.class,
        () -> pinotHelixManager.resetSegment(_tableName, _segmentName, null));
    Assert.assertEquals(runtimeException.getMessage(),
        "Reset segment failed for table: myTable_OFFLINE, segment: segment_1, instances: [instance_1]");

    runtimeException =
        Assert.expectThrows(RuntimeException.class, () -> pinotHelixManager.resetSegments(_tableName, null, false));
    Assert.assertEquals(runtimeException.getMessage(),
        "Reset segment failed for table: myTable_OFFLINE, instances: [instance_1]");
  }

  @Test
  public void testResetSegmentNoFailure() {
    ControllerConf cfg = new ControllerConf();
    cfg.setZkStr("localhost:2181");
    cfg.setHelixClusterName("cluster01");
    MockPinotHelixResourceManager pinotHelixManager = new MockPinotHelixResourceManager(cfg);

    pinotHelixManager.resetSegment(_tableName, _segmentName, null);
    pinotHelixManager.resetSegments(_tableName, null, false);
  }

  @Test
  public void testResetErrorReplicasResetsOnlyReplicasStillInError() {
    IdealState idealState = new IdealState(_tableName);
    ExternalView externalView = new ExternalView(_tableName);
    for (String segment : List.of("segment_1", "segment_2", "segment_3", "segment_4")) {
      idealState.setPartitionState(segment, _instance1, SegmentStateModel.ONLINE);
      idealState.setPartitionState(segment, _instance2, SegmentStateModel.ONLINE);
    }
    externalView.setState("segment_1", _instance1, SegmentStateModel.ERROR);
    externalView.setState("segment_1", _instance2, SegmentStateModel.ONLINE);
    // An ERROR replica on an instance the ideal state no longer assigns the segment to
    externalView.setState("segment_1", "instance_3", SegmentStateModel.ERROR);
    externalView.setState("segment_2", _instance1, SegmentStateModel.ERROR);
    externalView.setState("segment_2", _instance2, SegmentStateModel.ERROR);
    // Recovered after the caller saw it in ERROR
    externalView.setState("segment_3", _instance1, SegmentStateModel.ONLINE);
    externalView.setState("segment_3", _instance2, SegmentStateModel.ONLINE);
    // In ERROR but not requested
    externalView.setState("segment_4", _instance1, SegmentStateModel.ERROR);
    ErrorReplicaResetHelixResourceManager pinotHelixManager =
        new ErrorReplicaResetHelixResourceManager(idealState, externalView);

    // segment_5 is in neither the ideal state nor the external view, e.g. deleted after the caller looked
    Set<String> failedSegments = pinotHelixManager.resetErrorReplicas(_tableName,
        Set.of("segment_1", "segment_2", "segment_3", "segment_5"));

    assertEquals(failedSegments, Set.of());
    assertEquals(pinotHelixManager._resetSegmentsPerInstance,
        Map.of(_instance1, Set.of("segment_1", "segment_2"), _instance2, Set.of("segment_2")));
  }

  @Test
  public void testResetErrorReplicasReturnsSegmentsOfFailedInstance() {
    IdealState idealState = new IdealState(_tableName);
    ExternalView externalView = new ExternalView(_tableName);
    for (String segment : List.of("segment_1", "segment_2")) {
      idealState.setPartitionState(segment, _instance1, SegmentStateModel.ONLINE);
      idealState.setPartitionState(segment, _instance2, SegmentStateModel.ONLINE);
    }
    externalView.setState("segment_1", _instance1, SegmentStateModel.ERROR);
    externalView.setState("segment_1", _instance2, SegmentStateModel.ERROR);
    externalView.setState("segment_2", _instance1, SegmentStateModel.ONLINE);
    externalView.setState("segment_2", _instance2, SegmentStateModel.ERROR);
    ErrorReplicaResetHelixResourceManager pinotHelixManager =
        new ErrorReplicaResetHelixResourceManager(idealState, externalView);
    pinotHelixManager._instancesToFail.add(_instance1);

    Set<String> failedSegments = pinotHelixManager.resetErrorReplicas(_tableName, Set.of("segment_1", "segment_2"));

    // The other instance is still reset
    assertEquals(failedSegments, Set.of("segment_1"));
    assertEquals(pinotHelixManager._resetSegmentsPerInstance,
        Map.of(_instance2, Set.of("segment_1", "segment_2")));
  }

  /// Serves the given ideal state and external view, and records the resets per instance instead of sending them.
  private static class ErrorReplicaResetHelixResourceManager extends PinotHelixResourceManager {
    private final IdealState _idealState;
    private final ExternalView _externalView;
    private final Set<String> _instancesToFail = new HashSet<>();
    private final Map<String, Set<String>> _resetSegmentsPerInstance = new HashMap<>();

    ErrorReplicaResetHelixResourceManager(IdealState idealState, ExternalView externalView) {
      super(createControllerConf());
      _idealState = idealState;
      _externalView = externalView;
    }

    private static ControllerConf createControllerConf() {
      ControllerConf controllerConf = new ControllerConf();
      controllerConf.setZkStr("localhost:2181");
      controllerConf.setHelixClusterName("cluster01");
      return controllerConf;
    }

    @Override
    public IdealState getTableIdealState(String tableNameWithType) {
      return _idealState;
    }

    @Override
    public ExternalView getTableExternalView(String tableNameWithType) {
      return _externalView;
    }

    @Override
    void resetPartitions(String instanceName, String resourceName, Set<String> resetPartitionNames,
        boolean errorStateOnly) {
      assertTrue(errorStateOnly);
      if (_instancesToFail.contains(instanceName)) {
        throw new RuntimeException("Test: Fail reset for " + instanceName);
      }
      _resetSegmentsPerInstance.put(instanceName, resetPartitionNames);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testResetPartitionsErrorStateOnlySendsResetsFromErrorForPartitionsInError() {
    CurrentState currentState = new CurrentState(_tableName);
    currentState.setState("segment_1", SegmentStateModel.ERROR);
    // Recovered after the external view was read
    currentState.setState("segment_2", SegmentStateModel.ONLINE);
    // A reset from an earlier pass is still pending
    currentState.setState("segment_3", SegmentStateModel.ERROR);
    Message pendingMessage = new Message(Message.MessageType.STATE_TRANSITION, "pending");
    pendingMessage.setTgtSessionId(SESSION_ID);
    pendingMessage.setResourceName(_tableName);
    pendingMessage.setPartitionName("segment_3");
    HelixDataAccessor accessor = mockHelixDataAccessor(currentState, List.of(pendingMessage));
    when(accessor.setChildren(anyList(), anyList())).thenReturn(new boolean[]{true});

    new HelixDataAccessorResourceManager(accessor).resetPartitions(_instance1, _tableName,
        Set.of("segment_1", "segment_2", "segment_3"), true);

    ArgumentCaptor<List<Message>> messagesCaptor = ArgumentCaptor.forClass(List.class);
    verify(accessor).setChildren(anyList(), messagesCaptor.capture());
    List<Message> messages = messagesCaptor.getValue();
    assertEquals(messages.size(), 1);
    assertEquals(messages.get(0).getPartitionName(), "segment_1");
    // Sent from ERROR, so the instance discards it if the segment leaves ERROR before processing it
    assertEquals(messages.get(0).getFromState(), SegmentStateModel.ERROR);
    assertEquals(messages.get(0).getToState(), SegmentStateModel.OFFLINE);
  }

  @Test
  public void testResetPartitionsFailsWhenResetMessageIsNotWritten() {
    CurrentState currentState = new CurrentState(_tableName);
    currentState.setState("segment_1", SegmentStateModel.ERROR);
    HelixDataAccessor accessor = mockHelixDataAccessor(currentState, List.of());
    when(accessor.setChildren(anyList(), anyList())).thenReturn(new boolean[]{false});

    RuntimeException exception = expectThrows(RuntimeException.class,
        () -> new HelixDataAccessorResourceManager(accessor).resetPartitions(_instance1, _tableName,
            Set.of("segment_1"), true));
    assertTrue(exception.getMessage().contains("segment_1"), exception.getMessage());
  }

  private HelixDataAccessor mockHelixDataAccessor(CurrentState currentState, List<Message> messages) {
    LiveInstance liveInstance = mock(LiveInstance.class);
    when(liveInstance.getEphemeralOwner()).thenReturn(SESSION_ID);
    IdealState idealState = new IdealState(_tableName);
    idealState.setStateModelDefRef("SegmentOnlineOfflineStateModel");
    StateModelDefinition stateModelDefinition = mock(StateModelDefinition.class);
    when(stateModelDefinition.getInitialState()).thenReturn(SegmentStateModel.OFFLINE);
    HelixDataAccessor accessor = mock(HelixDataAccessor.class);
    when(accessor.keyBuilder()).thenReturn(new PropertyKey.Builder("cluster01"));
    when(accessor.getProperty(any(PropertyKey.class))).thenAnswer(invocation -> {
      PropertyKey key = invocation.getArgument(0);
      switch (key.getType()) {
        case LIVEINSTANCES:
          return liveInstance;
        case IDEALSTATES:
          return idealState;
        case STATEMODELDEFS:
          return stateModelDefinition;
        case CURRENTSTATES:
          return currentState;
        default:
          throw new IllegalArgumentException("Unexpected property key: " + key);
      }
    });
    when(accessor.getChildValues(any(PropertyKey.class), anyBoolean())).thenReturn(new ArrayList<>(messages));
    return accessor;
  }

  /// Sends the resets through the given Helix data accessor.
  private static class HelixDataAccessorResourceManager extends PinotHelixResourceManager {
    private final HelixManager _helixManager;

    HelixDataAccessorResourceManager(HelixDataAccessor accessor) {
      super(ErrorReplicaResetHelixResourceManager.createControllerConf());
      _helixManager = mock(HelixManager.class);
      when(_helixManager.getHelixDataAccessor()).thenReturn(accessor);
    }

    @Override
    public HelixManager getHelixZkManager() {
      return _helixManager;
    }
  }
}
