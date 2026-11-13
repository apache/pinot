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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.UploadedRealtimeSegmentName;
import org.apache.pinot.segment.local.utils.tablestate.TableStateUtils.ReadinessSnapshot;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ERROR;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.OFFLINE;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
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
    assertTrue(fixture.isReady());
    assertFalse(TableStateUtils.isAllSegmentsLoaded(fixture._manager, TABLE),
        "The legacy table-wide check still includes other partitions and successors");
  }

  @DataProvider
  public Object[][] unresolvedPartitionMetadata() {
    return new Object[][]{
        {null},
        {Map.of()},
        {Map.of("primaryKey", new ColumnPartitionMetadata("Murmur", 8, Set.of(2, 3), null))},
        {Map.of("primaryKey", new ColumnPartitionMetadata("Murmur", 8, Set.of(2), null),
            "otherColumn", new ColumnPartitionMetadata("Murmur", 8, Set.of(3), null))}
    };
  }

  @Test(dataProvider = "unresolvedPartitionMetadata")
  public void testRequiresSegmentsWithUnknownPartition(Map<String, ColumnPartitionMetadata> columns) {
    Assignment fixture = new Assignment();
    String segmentName = "customSegmentName";
    fixture.assign(segmentName, ONLINE);
    if (columns != null) {
      fixture.metadata(segmentName, columns);
    }
    assertFalse(fixture.isReady(), "Unresolved metadata may hold base rows for this consumer");
    ReadinessSnapshot snapshot =
        TableStateUtils.getReadinessSnapshot(fixture._manager, TABLE, name -> false);
    assertFalse(snapshot.isReadyToConsume(fixture._consumer, name -> false));
    assertTrue(snapshot.isReadyToConsume(fixture._consumer, segmentName::equals));
    fixture._currentState.setState(segmentName, ONLINE);
    assertTrue(fixture.isReady());
    assertTrue(TableStateUtils.getReadinessSnapshot(fixture._manager, TABLE, name -> false)
        .isReadyToConsume(fixture._consumer, name -> false));
  }

  @DataProvider
  public Object[][] uploadedSegmentStates() {
    return new Object[][]{{null}, {OFFLINE}, {ERROR}, {ONLINE}};
  }

  @Test(dataProvider = "uploadedSegmentStates")
  public void testUploadedSegmentsAreScopedToConsumerPartition(String state) {
    Assignment fixture = new Assignment();
    String samePartition = new UploadedRealtimeSegmentName(TABLE, 1, 0, "uploaded", "base").getSegmentName();
    String otherPartition = new UploadedRealtimeSegmentName(TABLE, 2, 0, "uploaded", "base").getSegmentName();
    fixture.assign(otherPartition, ONLINE);
    if (state != null) {
      fixture._currentState.setState(otherPartition, state);
    }
    assertTrue(fixture.isReady(), "An uploaded segment in another partition must not hold up this consumer");

    fixture.assign(samePartition, ONLINE);
    if (state != null) {
      fixture._currentState.setState(samePartition, state);
    }
    assertEquals(fixture.isReady(), ONLINE.equals(state),
        "An uploaded segment in this partition must still finish loading");
    fixture._currentState.setState(samePartition, ONLINE);
    assertTrue(fixture.isReady(), "Readiness must depend only on the required partition's uploaded segment");
  }

  @Test(dataProvider = "uploadedSegmentStates")
  public void testCustomSegmentsAreScopedByPartitionMetadata(String state) {
    Assignment fixture = new Assignment();
    String segmentName = "customSegmentName";
    fixture.assign(segmentName, ONLINE);
    SegmentZKMetadata metadata = fixture.metadata(segmentName, partitionColumns(2));
    if (state != null) {
      fixture._currentState.setState(segmentName, state);
    }
    assertTrue(fixture.isReady(), "A custom-named segment in another partition must not block consumption");
    assertTrue(TableStateUtils.getReadinessSnapshot(fixture._manager, TABLE, name -> false)
        .isReadyToConsume(fixture._consumer, name -> false));

    metadata.setPartitionMetadata(new SegmentPartitionMetadata(partitionColumns(1)));
    assertEquals(fixture.isReady(), ONLINE.equals(state));
    ReadinessSnapshot snapshot =
        TableStateUtils.getReadinessSnapshot(fixture._manager, TABLE, name -> false);
    assertEquals(snapshot.isReadyToConsume(fixture._consumer, name -> false), ONLINE.equals(state));
    assertTrue(snapshot.isReadyToConsume(fixture._consumer, segmentName::equals),
        "Local completion must still satisfy custom-named history");
  }

  @Test
  public void testNamedAndUnassignedSegmentsDoNotReadMetadata() {
    Assignment fixture = new Assignment();
    String predecessor = segment(1, 1);
    fixture.assign(predecessor, ONLINE);
    fixture._currentState.setState(predecessor, ONLINE);
    fixture.assign(new UploadedRealtimeSegmentName(TABLE, 2, 0, "uploaded", "base").getSegmentName(), ONLINE);
    fixture.assign(segment(2, 0), ONLINE);
    fixture.assign("offlineCustomSegment", OFFLINE);
    fixture.assign("consumingCustomSegment", CONSUMING);
    fixture._idealState.setInstanceStateMap("remoteCustomSegment", Map.of(OTHER_SERVER, ONLINE));
    assertTrue(fixture.isReady());
    assertTrue(TableStateUtils.getReadinessSnapshot(fixture._manager, TABLE, name -> false)
        .isReadyToConsume(fixture._consumer, name -> false));
    verifyNoInteractions(fixture._propertyStore);
  }

  @Test
  public void testOnlineCustomSegmentsDoNotReadMetadata() {
    Assignment fixture = new Assignment();
    String segmentName = "customSegmentName";
    fixture.assign(segmentName, ONLINE);
    fixture._currentState.setState(segmentName, ONLINE);
    assertTrue(fixture.isReady());
    assertTrue(TableStateUtils.getReadinessSnapshot(fixture._manager, TABLE, name -> false)
        .isReadyToConsume(fixture._consumer, name -> false));
    verifyNoInteractions(fixture._propertyStore);
  }

  @Test
  public void testSkippedMetadataDoesNotCacheLocalReadiness() {
    Assignment fixture = new Assignment();
    String segmentName = "customSegmentName";
    fixture.assign(segmentName, ONLINE);
    fixture._currentState.setState(segmentName, OFFLINE);
    fixture.metadata(segmentName, partitionColumns(2));
    Set<String> locallyReady = new HashSet<>(Set.of(segmentName));
    ReadinessSnapshot snapshot =
        TableStateUtils.getReadinessSnapshot(fixture._manager, TABLE, locallyReady::contains);
    assertTrue(snapshot.isReadyToConsume(fixture._consumer, locallyReady::contains));
    assertTrue(TableStateUtils.isReadyToConsume(fixture._manager, TABLE, fixture._consumer, locallyReady::contains));
    verifyNoInteractions(fixture._propertyStore);

    // Replacing the local manager can invalidate its initialized metadata before CurrentState changes.
    locallyReady.clear();
    clearInvocations(fixture._accessor, fixture._propertyStore);
    assertFalse(snapshot.isReadyToConsume(fixture._consumer, locallyReady::contains),
        "A skipped lookup leaves the partition unknown; local readiness must be checked again");
    locallyReady.add(segmentName);
    assertTrue(snapshot.isReadyToConsume(fixture._consumer, locallyReady::contains));
    locallyReady.clear();
    assertFalse(snapshot.isReadyToConsume(fixture._consumer, locallyReady::contains));
    verifyNoInteractions(fixture._accessor, fixture._propertyStore);

    assertTrue(fixture.isReady(), "A fresh lookup must resolve the now-unloaded segment to its other partition");
    ReadinessSnapshot refreshed =
        TableStateUtils.getReadinessSnapshot(fixture._manager, TABLE, name -> false);
    assertTrue(refreshed.isReadyToConsume(fixture._consumer, locallyReady::contains));
  }

  @Test
  public void testSnapshotFreezesPartitionMetadataUntilFreshRead() {
    Assignment fixture = new Assignment();
    LLCSegmentName otherConsumer = new LLCSegmentName(TABLE, 2, 3, 0);
    fixture.assign(otherConsumer.getSegmentName(), CONSUMING);
    String segmentName = "customSegmentName";
    fixture.assign(segmentName, ONLINE);
    SegmentZKMetadata metadata = fixture.metadata(segmentName, partitionColumns(2));
    ReadinessSnapshot snapshot =
        TableStateUtils.getReadinessSnapshot(fixture._manager, TABLE, name -> false);
    verify(fixture._propertyStore).get(ZKMetadataProvider.constructPropertyStorePathForSegment(TABLE, segmentName),
        null, AccessOption.PERSISTENT);
    verifyNoMoreInteractions(fixture._propertyStore);

    // The mocked store returns this same mutable record. A snapshot must retain the resolved id, not the record.
    metadata.setPartitionMetadata(new SegmentPartitionMetadata(partitionColumns(1)));
    clearInvocations(fixture._accessor, fixture._propertyStore);
    assertTrue(snapshot.isReadyToConsume(fixture._consumer, name -> false));
    assertFalse(snapshot.isReadyToConsume(otherConsumer, name -> false));
    assertTrue(snapshot.isReadyToConsume(fixture._consumer, name -> false));
    verifyNoInteractions(fixture._accessor, fixture._propertyStore);

    assertFalse(fixture.isReady(), "Fresh confirmation must observe a changed partition for the same segment name");
    ReadinessSnapshot refreshed =
        TableStateUtils.getReadinessSnapshot(fixture._manager, TABLE, name -> false);
    assertFalse(refreshed.isReadyToConsume(fixture._consumer, name -> false));
    assertTrue(refreshed.isReadyToConsume(otherConsumer, name -> false));
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

  @Test
  public void testReadinessSnapshotCopiesAssignmentAndCurrentState() {
    Assignment fixture = new Assignment();
    String predecessor = segment(1, 1);
    fixture.assign(predecessor, ONLINE);
    ReadinessSnapshot missingHistory =
        TableStateUtils.getReadinessSnapshot(fixture._manager, TABLE, name -> false);
    assertFalse(missingHistory.isReadyToConsume(fixture._consumer, segmentName -> false));

    fixture._currentState.setState(predecessor, ONLINE);
    ReadinessSnapshot loadedHistory =
        TableStateUtils.getReadinessSnapshot(fixture._manager, TABLE, name -> false);
    assertTrue(loadedHistory.isReadyToConsume(fixture._consumer, segmentName -> false));
    assertFalse(missingHistory.isReadyToConsume(fixture._consumer, segmentName -> false),
        "Updating the source CurrentState must not change an earlier snapshot");

    fixture.assign(segment(1, 0), ONLINE);
    assertFalse(fixture.isReady(), "Fresh evaluation must include newly assigned history");
    fixture.assign(fixture._consumer.getSegmentName(), OFFLINE);
    clearInvocations(fixture._accessor);
    assertTrue(loadedHistory.isReadyToConsume(fixture._consumer, segmentName -> false),
        "Updating the source IdealState must not change a snapshot; a cached positive needs fresh confirmation");
    verifyNoInteractions(fixture._accessor);
  }

  @Test(dataProvider = "uploadedSegmentStates")
  public void testReadinessSnapshotMatchesFreshPartitionChecks(String state) {
    Assignment fixture = new Assignment();
    String uploaded = new UploadedRealtimeSegmentName(TABLE, 1, 0, "uploaded", "base").getSegmentName();
    String unrelated = new UploadedRealtimeSegmentName(TABLE, 2, 0, "uploaded", "base").getSegmentName();
    fixture.assign(uploaded, ONLINE);
    fixture.assign(unrelated, ONLINE);
    fixture._currentState.setState(unrelated, ERROR);
    fixture.assign(segment(2, 0), ONLINE);
    fixture.assign(segment(1, 4), ONLINE);
    fixture._idealState.setInstanceStateMap(segment(1, 0), Map.of(OTHER_SERVER, ONLINE));
    if (state != null) {
      fixture._currentState.setState(uploaded, state);
    }
    ReadinessSnapshot snapshot =
        TableStateUtils.getReadinessSnapshot(fixture._manager, TABLE, name -> false);
    assertEquals(snapshot.isReadyToConsume(fixture._consumer, segmentName -> false), ONLINE.equals(state));
    assertEquals(snapshot.isReadyToConsume(fixture._consumer, segmentName -> false, true), ONLINE.equals(state));
    assertEquals(snapshot.isReadyToConsume(fixture._consumer, segmentName -> false), fixture.isReady());
  }

  @Test
  public void testReadinessSnapshotReevaluatesLocalReadinessWithoutReads() {
    Assignment fixture = new Assignment();
    String n1 = segment(1, 1);
    String n2 = segment(1, 2);
    fixture.assign(n1, ONLINE);
    fixture.assign(n2, ONLINE);
    ReadinessSnapshot snapshot =
        TableStateUtils.getReadinessSnapshot(fixture._manager, TABLE, name -> false);
    Set<String> locallyReady = new HashSet<>(Set.of(n2));
    clearInvocations(fixture._accessor);
    assertFalse(snapshot.isReadyToConsume(fixture._consumer, locallyReady::contains));
    locallyReady.add(n1);
    assertTrue(snapshot.isReadyToConsume(fixture._consumer, locallyReady::contains));
    locallyReady.remove(n2);
    assertFalse(snapshot.isReadyToConsume(fixture._consumer, locallyReady::contains));
    verifyNoInteractions(fixture._accessor);
  }

  @Test
  public void testReadinessSnapshotHandlesUnavailableRecords() {
    Assignment fixture = new Assignment();
    when(fixture._accessor.getProperty(fixture._keys.idealStates(TABLE))).thenReturn(null);
    assertFalse(TableStateUtils.getReadinessSnapshot(fixture._manager, TABLE, name -> false)
        .isReadyToConsume(fixture._consumer, segmentName -> true));
    when(fixture._accessor.getProperty(fixture._keys.idealStates(TABLE))).thenReturn(fixture._idealState);
    String predecessor = segment(1, 1);
    fixture.assign(predecessor, ONLINE);
    when(fixture._accessor.getProperty(fixture._keys.liveInstance(SERVER))).thenReturn(null);
    assertFalse(TableStateUtils.getReadinessSnapshot(fixture._manager, TABLE, name -> false)
        .isReadyToConsume(fixture._consumer, segmentName -> false));
    when(fixture._accessor.getProperty(fixture._keys.liveInstance(SERVER))).thenReturn(fixture._liveInstance);
    when(fixture._accessor.getProperty(fixture._keys.currentState(SERVER, SESSION, TABLE))).thenReturn(null);
    ReadinessSnapshot missingCurrentState =
        TableStateUtils.getReadinessSnapshot(fixture._manager, TABLE, name -> false);
    assertFalse(missingCurrentState.isReadyToConsume(fixture._consumer, segmentName -> false));
    assertTrue(missingCurrentState.isReadyToConsume(fixture._consumer, predecessor::equals),
        "Locally completed history needs no CurrentState, matching fresh evaluation");
    assertEquals(missingCurrentState.isReadyToConsume(fixture._consumer, predecessor::equals),
        TableStateUtils.isReadyToConsume(fixture._manager, TABLE, fixture._consumer, predecessor::equals));
  }

  @Test
  public void testFreshAndSnapshotReadStateOnce() {
    Assignment fixture = new Assignment();
    clearInvocations(fixture._accessor);
    assertTrue(fixture.isReady());
    verify(fixture._accessor).getProperty(fixture._keys.idealStates(TABLE));
    verify(fixture._accessor).getProperty(fixture._keys.liveInstance(SERVER));
    verify(fixture._accessor).getProperty(fixture._keys.currentState(SERVER, SESSION, TABLE));

    clearInvocations(fixture._accessor);
    ReadinessSnapshot snapshot =
        TableStateUtils.getReadinessSnapshot(fixture._manager, TABLE, name -> false);
    verify(fixture._accessor).getProperty(fixture._keys.idealStates(TABLE));
    verify(fixture._accessor).getProperty(fixture._keys.liveInstance(SERVER));
    verify(fixture._accessor).getProperty(fixture._keys.currentState(SERVER, SESSION, TABLE));
    clearInvocations(fixture._accessor);
    assertTrue(snapshot.isReadyToConsume(fixture._consumer, segmentName -> false));
    verifyNoInteractions(fixture._accessor);
  }

  private static Map<String, ColumnPartitionMetadata> partitionColumns(int partitionId) {
    return Map.of("primaryKey", new ColumnPartitionMetadata("Murmur", 8, Set.of(partitionId), null));
  }

  private static String segment(int partitionId, int sequenceNumber) {
    return new LLCSegmentName(TABLE, partitionId, sequenceNumber, 0).getSegmentName();
  }

  /// Supplies real mutable Helix records through a mocked accessor; accessed only by the invoking test thread.
  private static class Assignment {
    private final LLCSegmentName _consumer = new LLCSegmentName(TABLE, 1, 3, 0);
    private final HelixManager _manager = mock(HelixManager.class);
    private final HelixDataAccessor _accessor = mock(HelixDataAccessor.class);
    @SuppressWarnings("unchecked")
    private final ZkHelixPropertyStore<ZNRecord> _propertyStore = mock(ZkHelixPropertyStore.class);
    private final PropertyKey.Builder _keys = new PropertyKey.Builder("test-cluster");
    private final IdealState _idealState = new IdealState(TABLE);
    private final CurrentState _currentState = new CurrentState(TABLE);
    private final LiveInstance _liveInstance = mock(LiveInstance.class);

    Assignment() {
      when(_manager.getInstanceName()).thenReturn(SERVER);
      when(_manager.getHelixDataAccessor()).thenReturn(_accessor);
      when(_manager.getHelixPropertyStore()).thenReturn(_propertyStore);
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

    SegmentZKMetadata metadata(String segmentName, Map<String, ColumnPartitionMetadata> columns) {
      SegmentZKMetadata metadata = new SegmentZKMetadata(segmentName);
      if (!columns.isEmpty()) {
        metadata.setPartitionMetadata(new SegmentPartitionMetadata(columns));
      }
      when(_propertyStore.get(ZKMetadataProvider.constructPropertyStorePathForSegment(TABLE, segmentName), null,
          AccessOption.PERSISTENT)).thenReturn(metadata.toZNRecord());
      return metadata;
    }

    boolean isReady() {
      return TableStateUtils.isReadyToConsume(_manager, TABLE, _consumer, segmentName -> false);
    }
  }
}
