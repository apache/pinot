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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.exception.ZkBadVersionException;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.PartitionAssignmentUtils;
import org.apache.pinot.common.restlet.resources.BatchConfig;
import org.apache.pinot.common.restlet.resources.RebalanceConfig;
import org.apache.pinot.common.restlet.resources.RebalanceResult;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignment;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentFactory;
import org.apache.pinot.controller.helix.core.assignment.segment.TableAssignment;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.utils.Enablement;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.MockedStatic;
import org.slf4j.Logger;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentUtils.getInstanceStateMap;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Exercises force commit through the rebalance loop with deterministic IdealState write conflicts.
/// Each test owns a synchronous fixture; no mutable rebalance state is shared between invocations.
public class TableRebalancerForceCommitTest {
  private static final String TABLE_NAME = "forceCommit_REALTIME";
  private static final List<String> SOURCE_INSTANCES = List.of("oldServer0", "oldServer1");
  private static final List<String> TARGET_INSTANCES = List.of("newServer0", "newServer1");

  @DataProvider
  public Object[][] rebalanceSteps() {
    return new Object[][]{{0, 0, 0}, {0, 3, 0}, {1, 0, 0}, {1, 1, 1}};
  }

  @Test(dataProvider = "rebalanceSteps")
  public void testForceCommitOnceAcrossRetriesAndMovementSteps(int minAvailableReplicas, int conflicts,
      int successfulWritesBeforeConflict) throws Exception {
    RebalanceFixture fixture = new RebalanceFixture(0);
    fixture._config.setMinAvailableReplicas(minAvailableReplicas);
    fixture._conflictsRemaining = conflicts;
    fixture._successfulWritesBeforeConflict = successfulWritesBeforeConflict;
    fixture.assertSuccess(0);
    assertEquals(fixture._commitsAtWrites.size(), minAvailableReplicas == 1 ? 2 : 1);
  }

  @Test
  public void testForceCommitNewPartitionSelectedAfterConflict() throws Exception {
    RebalanceFixture fixture = new RebalanceFixture(0, 1);
    fixture._config.setBatchSizePerServer(1);
    fixture._beforeWrite = () -> fixture.movePartition(0, TARGET_INSTANCES);
    fixture.assertSuccess(0, 1);
    assertEquals(fixture._writeAttempts, 2);
  }

  @Test
  public void testRememberDeferredPartitionAfterConflict() throws Exception {
    RebalanceFixture fixture = new RebalanceFixture(1);
    fixture._config.setBatchSizePerServer(1);
    PartitionAssignmentUtils.setInstances(fixture._state, 0, SOURCE_INSTANCES);
    // After partition 1 commits, the competing write adds partition 0, which the next batch moves first.
    fixture._beforeWrite = () -> {
      fixture.addPartition(0);
      fixture.advanceVersion();
    };
    fixture.assertSuccess(1, 0);
    assertEquals(fixture._commitsAtWrites, List.of(2, 2));
  }

  @Test
  public void testCommitPublicationRacesWithAssignmentWrite() throws Exception {
    RebalanceFixture fixture = new RebalanceFixture(0);
    // The wait observes DONE metadata; publication of ONLINE + successor in IdealState is still pending.
    fixture._onCommit = segments -> {
      fixture._beforeWrite = () -> fixture.publishCommit(segments, null);
    };
    fixture.assertSuccess(0);
    assertEquals(fixture._writeAttempts, 2);
  }

  @Test
  public void testForceCommitNewPartitionSelectedDuringInnerReplan() throws Exception {
    RebalanceFixture fixture = new RebalanceFixture(1);
    fixture._config.setBatchSizePerServer(1);
    PartitionAssignmentUtils.setInstances(fixture._state, 0, SOURCE_INSTANCES);
    fixture._onCommit = segments -> {
      fixture.publishCommit(segments, null);
      if (fixture._commits.size() == 1) {
        fixture.addPartition(0);
      }
    };
    fixture.assertSuccess(1, 0);
    // Both partitions commit before the first assignment write, and partition 1 stays remembered afterward.
    assertEquals(fixture._commitsAtWrites, List.of(2, 2));
  }

  @Test
  public void testForceCommitFailureAbortsBeforeMovingSegments() throws Exception {
    RebalanceFixture fixture = new RebalanceFixture(0);
    IdealState initialState = copy(fixture._state);
    fixture._onCommit = segments -> {
      throw new IllegalStateException("Injected force commit failure");
    };
    RebalanceResult result = fixture.run();
    assertEquals(result.getStatus(), RebalanceResult.Status.FAILED);
    assertTrue(result.getDescription().contains("Injected force commit failure"));
    assertEquals(fixture._commits, List.of(Set.of(segmentName(0, 11))));
    assertEquals(fixture._writeAttempts, 0);
    assertEquals(fixture._state.getRecord().getMapFields(), initialState.getRecord().getMapFields());
  }

  @Test
  public void testForceCommitDisabled() throws Exception {
    RebalanceFixture fixture = new RebalanceFixture(0);
    fixture._config.setForceCommit(false);
    fixture.assertSuccess();
  }

  @Test
  public void testRememberForceCommitAfterConcurrentMovement() throws Exception {
    RebalanceFixture fixture = new RebalanceFixture(0);
    fixture._config.setMinAvailableReplicas(1);
    fixture._beforeWrite = () -> fixture.movePartition(0,
        List.of(SOURCE_INSTANCES.getFirst(), TARGET_INSTANCES.getFirst()));
    fixture.assertSuccess(0);
    assertEquals(fixture._writeAttempts, 2);
  }

  @Test
  public void testRememberForceCommitAfterNonStrictSuccessorRelocation() throws Exception {
    RebalanceFixture fixture = new RebalanceFixture(0);
    fixture._tableConfig.setRoutingConfig(null);
    fixture._state.getRecord().getListFields().clear();
    fixture._onCommit = segments -> {
      // The original stays ONLINE on A; its non-pinned successor consumes on B. Recalculation then targets C.
      fixture.publishCommit(segments, TARGET_INSTANCES);
      fixture._targetInstances = List.of("reassignedServer0", "reassignedServer1");
    };
    fixture.assertSuccess(0);
    assertEquals(fixture._commitsAtWrites, List.of(1));
  }

  /// In-memory IdealState store with hooks for commit completion and one competing write.
  /// Each fixture is confined to one test thread.
  private static class RebalanceFixture {
    private final RebalanceConfig _config = new RebalanceConfig();
    private final TableConfig _tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setNumReplicas(2).setRoutingConfig(new RoutingConfig(null, null,
            RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false)).build();
    private IdealState _state = new IdealState(TABLE_NAME);
    private List<String> _targetInstances = TARGET_INSTANCES;
    private final Set<Integer> _partitions = new HashSet<>();
    private final List<Set<String>> _commits = new ArrayList<>();
    private final List<Integer> _commitsAtWrites = new ArrayList<>();
    private int _writeAttempts;
    private int _conflictsRemaining;
    private int _successfulWritesBeforeConflict;
    private Runnable _beforeWrite;
    private Consumer<Set<String>> _onCommit = segments -> publishCommit(segments, null);

    RebalanceFixture(int... partitions) {
      _state.setReplicas("2");
      Arrays.stream(partitions).forEach(this::addPartition);
      _config.setDryRun(false);
      _config.setDisableSummary(true);
      _config.setIncludeConsuming(true);
      _config.setForceCommit(true);
      _config.setMinAvailableReplicas(0);
    }

    void addPartition(int partition) {
      _partitions.add(partition);
      _state.getRecord().setMapField(segmentName(partition, 11), getInstanceStateMap(SOURCE_INSTANCES, CONSUMING));
      PartitionAssignmentUtils.setInstances(_state, partition, SOURCE_INSTANCES);
    }

    void advanceVersion() {
      _state.getRecord().setVersion(_state.getRecord().getVersion() + 1);
    }

    void movePartition(int partition, List<String> instances) {
      _state.getRecord().getMapFields().replaceAll((segment, states) ->
          new LLCSegmentName(segment).getPartitionGroupId() == partition
              ? getInstanceStateMap(instances, states.values().iterator().next()) : states);
      PartitionAssignmentUtils.setInstances(_state, partition, instances);
      advanceVersion();
    }

    void publishCommit(Set<String> segments, List<String> successorInstances) {
      for (String segment : segments) {
        LLCSegmentName name = new LLCSegmentName(segment);
        List<String> instances = new ArrayList<>(_state.getRecord().getMapField(segment).keySet());
        _state.getRecord().setMapField(segment, getInstanceStateMap(instances, ONLINE));
        _state.getRecord().setMapField(segmentName(name.getPartitionGroupId(), name.getSequenceNumber() + 1),
            getInstanceStateMap(successorInstances != null ? successorInstances : instances, CONSUMING));
      }
      advanceVersion();
    }

    boolean writeAssignment(ZNRecord record, int expectedVersion) {
      _writeAttempts++;
      assertTrue(_writeAttempts <= 20, "Rebalance did not make progress after 20 writes");
      assertEquals(expectedVersion, _state.getRecord().getVersion(), "Assignment used an outdated IdealState");
      if (_beforeWrite != null) {
        Runnable beforeWrite = _beforeWrite;
        _beforeWrite = null;
        beforeWrite.run();
      }
      if (_conflictsRemaining > 0 && _commitsAtWrites.size() >= _successfulWritesBeforeConflict) {
        assertEquals(_commitsAtWrites.size(), _successfulWritesBeforeConflict);
        _conflictsRemaining--;
        advanceVersion();
      }
      if (expectedVersion != _state.getRecord().getVersion()) {
        throw new ZkBadVersionException("Injected concurrent IdealState write");
      }
      _state = copy(new IdealState(record));
      advanceVersion();
      _commitsAtWrites.add(_commits.size());
      return true;
    }

    void assertSuccess(int... committedPartitions) throws Exception {
      RebalanceResult result = run();
      assertEquals(result.getStatus(), RebalanceResult.Status.DONE, result.getDescription());
      assertEquals(_conflictsRemaining, 0);
      assertEquals(_commits, Arrays.stream(committedPartitions).mapToObj(id -> Set.of(segmentName(id, 11))).toList());
      for (int partition : _partitions) {
        assertEquals(_state.getRecord().getMapField(segmentName(partition, _config.isForceCommit() ? 12 : 11)),
            getInstanceStateMap(_targetInstances, CONSUMING));
      }
    }

    RebalanceResult run() throws Exception {
      HelixManager manager = mock(HelixManager.class);
      HelixDataAccessor accessor = mock(HelixDataAccessor.class);
      @SuppressWarnings("unchecked")
      BaseDataAccessor<ZNRecord> baseAccessor = mock(BaseDataAccessor.class);
      PropertyKey.Builder keys = new PropertyKey.Builder("testCluster");
      when(manager.getHelixDataAccessor()).thenReturn(accessor);
      when(accessor.keyBuilder()).thenReturn(keys);
      when(accessor.getBaseDataAccessor()).thenReturn(baseAccessor);
      when(accessor.getProperty(keys.idealStates(TABLE_NAME))).thenAnswer(invocation -> copy(_state));
      when(accessor.getProperty(keys.externalView(TABLE_NAME)))
          .thenAnswer(invocation -> new ExternalView(copy(_state).getRecord()));
      when(baseAccessor.set(eq(keys.idealStates(TABLE_NAME).getPath()), any(ZNRecord.class), anyInt(),
          eq(AccessOption.PERSISTENT))).thenAnswer(invocation ->
          writeAssignment(invocation.getArgument(1), invocation.getArgument(2)));
      PinotLLCRealtimeSegmentManager segmentManager = mock(PinotLLCRealtimeSegmentManager.class);
      when(segmentManager.forceCommit(eq(TABLE_NAME), isNull(), any(String.class), any(BatchConfig.class)))
          .thenAnswer(invocation -> {
            Set<String> segments = new HashSet<>(Arrays.asList(invocation.<String>getArgument(2).split(",")));
            // Match the real API's filtering of names that are no longer CONSUMING in IdealState.
            segments.removeIf(segment -> !_state.getRecord().getMapFields().containsKey(segment)
                || !_state.getRecord().getMapField(segment).containsValue(CONSUMING));
            _commits.add(segments);
            return segments;
          });
      doAnswer(invocation -> {
        _onCommit.accept(invocation.getArgument(1));
        return null;
      }).when(segmentManager).waitUntilSegmentsForceCommitted(eq(TABLE_NAME), any(), any());
      SegmentAssignment assignment = mock(SegmentAssignment.class);
      when(assignment.rebalanceTable(any(), any(), any(), any(), any())).thenAnswer(invocation -> {
        IdealState state = invocation.getArgument(0);
        Map<String, Map<String, String>> target = new TreeMap<>();
        state.getRecord().getMapFields().forEach((segment, states) ->
            target.put(segment, getInstanceStateMap(_targetInstances, states.values().iterator().next())));
        Map<Integer, List<String>> targetPartitions = new TreeMap<>();
        PartitionAssignmentUtils.getPartitionAssignment(state).keySet()
            .forEach(partition -> targetPartitions.put(partition, _targetInstances));
        return new TableAssignment(target, targetPartitions);
      });
      TableRebalancer rebalancer = new TableRebalancer(manager, new NoOpTableRebalanceObserver(), null, null, null,
          segmentManager) {
        @Override
        public Pair<Map<InstancePartitionsType, InstancePartitions>, Boolean> getInstancePartitionsMap(
            TableConfig config, boolean reassignInstances, boolean bootstrap, boolean dryRun,
            Enablement minimizeDataMovement, Logger logger) {
          return Pair.of(Map.of(), true);
        }
      };
      try (MockedStatic<SegmentAssignmentFactory> factory = mockStatic(SegmentAssignmentFactory.class)) {
        factory.when(() -> SegmentAssignmentFactory.getSegmentAssignment(manager, _tableConfig)).thenReturn(assignment);
        return rebalancer.rebalance(_tableConfig, _config, null);
      }
    }
  }

  private static String segmentName(int partition, int sequence) {
    return new LLCSegmentName("forceCommit", partition, sequence, 0).getSegmentName();
  }

  private static IdealState copy(IdealState state) {
    ZNRecord record = new ZNRecord(state.getRecord());
    // ZNRecord's copy constructor shares nested collections; persisted reads/writes must not share them.
    state.getRecord().getMapFields().forEach((segment, states) -> record.setMapField(segment, new TreeMap<>(states)));
    state.getRecord().getListFields().forEach((key, values) -> record.setListField(key, new ArrayList<>(values)));
    return new IdealState(record);
  }
}
