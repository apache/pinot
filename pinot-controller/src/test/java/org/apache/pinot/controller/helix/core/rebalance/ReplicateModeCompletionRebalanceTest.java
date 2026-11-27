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
package org.apache.pinot.controller.helix.core.rebalance;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.assignment.ReplicateModeAssignmentUtils;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.helix.core.assignment.segment.TableAssignment;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Exercises completion retirement against version-checked storage so stale fanout cannot undo a successful rebalance.
/// Fixtures are isolated per method; the atomic reference models the IdealState CAS without background threads.
public class ReplicateModeCompletionRebalanceTest {
  private static final String TABLE = "testTable_REALTIME";
  private static final String SEGMENT = "testTable__0__0__20261118T120000Z";
  private static final String PATH = new PropertyKey.Builder("testCluster").idealStates(TABLE).getPath();
  private static final Map<String, String> COMPLETED_PLACEMENT = Map.of("tier_x", ONLINE, "tier_y", ONLINE);
  private AtomicReference<IdealState> _state;
  private Set<String> _pending;
  private BaseDataAccessor<ZNRecord> _baseAccessor;
  private PinotLLCRealtimeSegmentManager _segmentManager;
  private TableRebalancer _rebalancer;
  private TableAssignment _target;

  @BeforeMethod
  @SuppressWarnings("unchecked")
  public void setUp() {
    HelixManager helix = mock(HelixManager.class);
    HelixDataAccessor accessor = mock(HelixDataAccessor.class);
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    _baseAccessor = mock(BaseDataAccessor.class);
    _segmentManager = mock(PinotLLCRealtimeSegmentManager.class);
    when(helix.getHelixDataAccessor()).thenReturn(accessor);
    when(helix.getHelixPropertyStore()).thenReturn(propertyStore);
    when(accessor.keyBuilder()).thenReturn(new PropertyKey.Builder("testCluster"));
    when(accessor.getBaseDataAccessor()).thenReturn(_baseAccessor);
    IdealState initial = new IdealState(TABLE);
    initial.setInstanceStateMap(SEGMENT, COMPLETED_PLACEMENT);
    ReplicateModeAssignmentUtils.setAssignment(initial, 0, "a", List.of("a", "b", "c"));
    initial.getRecord().setVersion(7);
    _state = new AtomicReference<>(initial);
    when(accessor.getProperty(any(PropertyKey.class))).thenAnswer(invocation -> {
      IdealState observed = _state.get();
      IdealState snapshot = HelixHelper.cloneIdealState(observed);
      // Native reads include ZK stats, while the deep-copy helper intentionally omits them.
      snapshot.getRecord().setVersion(observed.getRecord().getVersion());
      return snapshot;
    });
    when(_baseAccessor.set(eq(PATH), any(ZNRecord.class), anyInt(), eq(AccessOption.PERSISTENT)))
        .thenAnswer(invocation -> {
          IdealState previous = _state.get();
          int expectedVersion = invocation.getArgument(2);
          if (previous.getRecord().getVersion() != expectedVersion) {
            return false;
          }
          ZNRecord written = new ZNRecord(invocation.getArgument(1, ZNRecord.class));
          written.setVersion(expectedVersion + 1);
          return _state.compareAndSet(previous, new IdealState(written));
        });
    _pending = new HashSet<>();
    when(_segmentManager.getPendingSegmentCompletions(TABLE)).thenAnswer(invocation -> List.copyOf(_pending));
    doAnswer(invocation -> {
      _pending.removeAll(invocation.getArgument(1));
      return null;
    }).when(_segmentManager).completeReplicateModeSegmentCompletions(eq(TABLE), any());
    SegmentZKMetadata metadata = new SegmentZKMetadata(SEGMENT);
    metadata.setReplicateMode(true);
    metadata.setStatus(Status.DONE);
    when(propertyStore.get(eq(ZKMetadataProvider.constructPropertyStorePathForSegment(TABLE, SEGMENT)), isNull(),
        eq(AccessOption.PERSISTENT))).thenReturn(metadata.toZNRecord());
    _target = new TableAssignment(Map.of(SEGMENT, COMPLETED_PLACEMENT), Map.of());
    _rebalancer = new TableRebalancer(helix, null, null, null, null, _segmentManager);
  }

  @DataProvider
  public Object[][] completionRecords() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "completionRecords")
  public void testStaleFanoutCannotOverwriteCompletedPlacement(boolean pending) {
    if (pending) {
      _pending.add(SEGMENT);
    }
    IdealState known = HelixHelper.cloneIdealState(_state.get());
    IdealState staleRepair = staleRepair(known);
    _rebalancer.completeRebalancedReplicateModeSegments(TABLE, _target, known);
    assertTrue(_pending.isEmpty());
    assertEquals(_state.get().getRecord().getVersion(), 8);
    // The fanout callback had read version 7 before completion retirement, including when another caller already
    // removed the list entry. An equal-data fence must still advance the version to reject that in-flight write.
    assertFalse(_baseAccessor.set(PATH, staleRepair.getRecord(), 7, AccessOption.PERSISTENT));
    assertEquals(_state.get().getInstanceStateMap(SEGMENT), COMPLETED_PLACEMENT);
  }

  @Test
  public void testChangedPlacementPreventsRebalanceSuccess() {
    IdealState known = HelixHelper.cloneIdealState(_state.get());
    IdealState staleRepair = staleRepair(known);
    assertTrue(_baseAccessor.set(PATH, staleRepair.getRecord(), 7, AccessOption.PERSISTENT));
    assertThrows(IllegalStateException.class,
        () -> _rebalancer.completeRebalancedReplicateModeSegments(TABLE, _target, known));
    assertEquals(_state.get().getRecord().getVersion(), 8);
    assertEquals(_state.get().getInstanceStateMap(SEGMENT), staleRepair.getInstanceStateMap(SEGMENT));
  }

  @Test
  public void testUnretiredCompletionPreventsRebalanceSuccess() {
    _pending.add(SEGMENT);
    doNothing().when(_segmentManager).completeReplicateModeSegmentCompletions(eq(TABLE), any());
    assertThrows(IllegalStateException.class,
        () -> _rebalancer.completeRebalancedReplicateModeSegments(TABLE, _target, _state.get()));
    assertEquals(_pending, Set.of(SEGMENT));
    assertEquals(_state.get().getRecord().getVersion(), 7);
    verify(_baseAccessor, never()).set(eq(PATH), any(ZNRecord.class), anyInt(), eq(AccessOption.PERSISTENT));
  }

  private static IdealState staleRepair(IdealState observed) {
    IdealState repair = HelixHelper.cloneIdealState(observed);
    repair.setInstanceStateMap(SEGMENT, Map.of("a", ONLINE, "b", ONLINE, "c", ONLINE));
    return repair;
  }
}
