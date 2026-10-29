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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
/// Each test invocation owns its mutable state, so invocations do not share rebalance state.
public class TableRebalancerForceCommitTest {
  private static final String TABLE_NAME = "forceCommit_REALTIME";
  private static final List<String> SOURCE_INSTANCES = List.of("oldServer0", "oldServer1");
  private static final List<String> TARGET_INSTANCES = List.of("newServer0", "newServer1");

  private enum ConcurrentChange {
    VERSION_ONLY,
    MOVE_FIRST_PARTITION,
    ADD_EARLIER_SEGMENT
  }

  @DataProvider
  public Object[][] rebalanceSteps() {
    return new Object[][]{{0, 0, 1, 0}, {0, 1, 1, 0}, {0, 3, 1, 0}, {1, 0, 2, 0}, {1, 1, 2, 0}, {1, 1, 2, 1}};
  }

  @Test(dataProvider = "rebalanceSteps")
  public void testForceCommitForEachMovementStep(int minAvailableReplicas, int numVersionConflicts,
      int expectedCommitCount, int successfulWritesBeforeConflict)
      throws Exception {
    assertForceCommits(minAvailableReplicas, numVersionConflicts, expectedCommitCount, ConcurrentChange.VERSION_ONLY,
        successfulWritesBeforeConflict);
  }

  @Test
  public void testForceCommitNewPartitionSelectedAfterConflict()
      throws Exception {
    // The first partition is moved by a concurrent write. The retry selects the next partition, which still needs
    // its own force commit even though the rebalancer has not successfully written an assignment yet.
    assertForceCommits(0, 1, 2, ConcurrentChange.MOVE_FIRST_PARTITION, 0);
  }

  @Test
  public void testRememberForceCommitWhileAnotherPartitionMoves()
      throws Exception {
    // Partition 0 starts empty. After partition 1 is force committed, a concurrent write adds partition 0's segment,
    // so batching moves partition 0 first. That successful write must not forget partition 1's force commit.
    assertForceCommits(0, 1, 2, ConcurrentChange.ADD_EARLIER_SEGMENT, 0);
  }

  private void assertForceCommits(int minAvailableReplicas, int numVersionConflicts, int expectedCommitCount,
      ConcurrentChange concurrentChange, int successfulWritesBeforeConflict)
      throws Exception {
    HelixManager helixManager = mock(HelixManager.class);
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    @SuppressWarnings("unchecked")
    BaseDataAccessor<ZNRecord> baseDataAccessor = mock(BaseDataAccessor.class);
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder("testCluster");
    when(helixManager.getHelixDataAccessor()).thenReturn(dataAccessor);
    when(dataAccessor.keyBuilder()).thenReturn(keyBuilder);
    when(dataAccessor.getBaseDataAccessor()).thenReturn(baseDataAccessor);

    boolean addEarlierSegment = concurrentChange == ConcurrentChange.ADD_EARLIER_SEGMENT;
    boolean moveFirstPartition = concurrentChange == ConcurrentChange.MOVE_FIRST_PARTITION;
    int numPartitions = concurrentChange == ConcurrentChange.VERSION_ONLY ? 1 : 2;
    IdealState initialState = new IdealState(TABLE_NAME);
    initialState.setReplicas("2");
    if (addEarlierSegment) {
      PartitionAssignmentUtils.setInstances(initialState, 0, SOURCE_INSTANCES);
    }
    for (int partitionId = addEarlierSegment ? 1 : 0; partitionId < numPartitions; partitionId++) {
      String originalSegment = new LLCSegmentName("forceCommit", partitionId, 11, 0).getSegmentName();
      initialState.getRecord().setMapField(originalSegment, instanceStates(SOURCE_INSTANCES, CONSUMING));
      PartitionAssignmentUtils.setInstances(initialState, partitionId, SOURCE_INSTANCES);
    }
    AtomicReference<IdealState> storedState = new AtomicReference<>(initialState);
    when(dataAccessor.getProperty(keyBuilder.idealStates(TABLE_NAME)))
        .thenAnswer(invocation -> copy(storedState.get()));
    when(dataAccessor.getProperty(keyBuilder.externalView(TABLE_NAME)))
        .thenAnswer(invocation -> new ExternalView(new ZNRecord(storedState.get().getRecord())));

    AtomicInteger conflictsInjected = new AtomicInteger();
    AtomicInteger successfulWrites = new AtomicInteger();
    when(baseDataAccessor.set(eq(keyBuilder.idealStates(TABLE_NAME).getPath()), any(ZNRecord.class), anyInt(),
        eq(AccessOption.PERSISTENT))).thenAnswer(invocation -> {
          if (conflictsInjected.get() < numVersionConflicts
              && successfulWrites.get() >= successfulWritesBeforeConflict) {
            // A concurrent metadata write wins after force commit, before the partition can move.
            conflictsInjected.incrementAndGet();
            IdealState state = storedState.get();
            if (moveFirstPartition) {
              state.getRecord().getMapFields().replaceAll((segment, states) ->
                  new LLCSegmentName(segment).getPartitionGroupId() == 0
                      ? instanceStates(TARGET_INSTANCES, states.values().iterator().next()) : states);
              PartitionAssignmentUtils.setInstances(state, 0, TARGET_INSTANCES);
            } else if (addEarlierSegment) {
              String segment = new LLCSegmentName("forceCommit", 0, 11, 0).getSegmentName();
              state.getRecord().setMapField(segment, instanceStates(SOURCE_INSTANCES, CONSUMING));
              PartitionAssignmentUtils.setInstances(state, 0, SOURCE_INSTANCES);
            }
            storedState.get().getRecord().setVersion(storedState.get().getRecord().getVersion() + 1);
            throw new ZkBadVersionException("Injected concurrent IdealState write");
          }
          ZNRecord record = new ZNRecord(invocation.<ZNRecord>getArgument(1));
          record.setVersion(invocation.<Integer>getArgument(2) + 1);
          storedState.set(new IdealState(record));
          successfulWrites.incrementAndGet();
          return true;
        });

    PinotLLCRealtimeSegmentManager segmentManager = mock(PinotLLCRealtimeSegmentManager.class);
    List<Set<String>> forceCommittedSegments = new ArrayList<>();
    when(segmentManager.forceCommit(eq(TABLE_NAME), isNull(), any(String.class), any(BatchConfig.class)))
        .thenAnswer(invocation -> {
          Set<String> segments = Set.copyOf(Arrays.asList(invocation.<String>getArgument(2).split(",")));
          forceCommittedSegments.add(segments);
          return segments;
        });
    doAnswer(invocation -> {
      IdealState state = copy(storedState.get());
      for (String segment : invocation.<Set<String>>getArgument(1)) {
        LLCSegmentName name = new LLCSegmentName(segment);
        List<String> currentInstances = PartitionAssignmentUtils.getInstances(state, name.getPartitionGroupId());
        String successor = new LLCSegmentName(name.getTableName(), name.getPartitionGroupId(),
            name.getSequenceNumber() + 1, 0).getSegmentName();
        state.getRecord().setMapField(segment, instanceStates(currentInstances, ONLINE));
        // Strict replica-group successors remain on the partition's current instances until it moves.
        state.getRecord().setMapField(successor, instanceStates(currentInstances, CONSUMING));
      }
      state.getRecord().setVersion(state.getRecord().getVersion() + 1);
      storedState.set(state);
      return null;
    }).when(segmentManager).waitUntilSegmentsForceCommitted(eq(TABLE_NAME), any(), any());

    TableConfig tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME)
        .setNumReplicas(2).setRoutingConfig(new RoutingConfig(null, null,
            RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false)).build();
    SegmentAssignment segmentAssignment = mock(SegmentAssignment.class);
    when(segmentAssignment.rebalanceTable(any(), any(), any(), any(), any())).thenAnswer(invocation -> {
      IdealState state = invocation.getArgument(0);
      Map<String, Map<String, String>> target = new TreeMap<>();
      state.getRecord().getMapFields().forEach((segment, states) ->
          target.put(segment, instanceStates(TARGET_INSTANCES, states.values().iterator().next())));
      Map<Integer, List<String>> targetPartitions = new TreeMap<>();
      PartitionAssignmentUtils.getPartitionAssignment(state).keySet()
          .forEach(partitionId -> targetPartitions.put(partitionId, TARGET_INSTANCES));
      return new TableAssignment(target, targetPartitions);
    });

    try (MockedStatic<SegmentAssignmentFactory> factory = mockStatic(SegmentAssignmentFactory.class)) {
      factory.when(() -> SegmentAssignmentFactory.getSegmentAssignment(helixManager, tableConfig))
          .thenReturn(segmentAssignment);
      AtomicInteger forceCommitStarts = new AtomicInteger();
      List<Integer> reportedCommitCounts = new ArrayList<>();
      TableRebalanceObserver observer = new NoOpTableRebalanceObserver() {
        @Override
        public void onTrigger(Trigger trigger, Map<String, Map<String, String>> initialAssignment,
            Map<String, Map<String, String>> targetAssignment, RebalanceContext context) {
          if (trigger == Trigger.FORCE_COMMIT_START_TRIGGER) {
            forceCommitStarts.incrementAndGet();
          } else if (trigger == Trigger.FORCE_COMMIT_END_TRIGGER) {
            reportedCommitCounts.add(context.getNumSegmentsForceCommitted());
          }
        }
      };
      TableRebalancer rebalancer = new TableRebalancer(helixManager, observer, null, null, null, segmentManager) {
        @Override
        public Pair<Map<InstancePartitionsType, InstancePartitions>, Boolean> getInstancePartitionsMap(
            TableConfig config, boolean reassignInstances, boolean bootstrap, boolean dryRun,
            Enablement minimizeDataMovement, Logger logger) {
          return Pair.of(Map.of(), true);
        }
      };
      RebalanceConfig rebalanceConfig = new RebalanceConfig();
      rebalanceConfig.setDryRun(false);
      rebalanceConfig.setDisableSummary(true);
      rebalanceConfig.setIncludeConsuming(true);
      rebalanceConfig.setForceCommit(true);
      rebalanceConfig.setMinAvailableReplicas(minAvailableReplicas);
      if (numPartitions > 1) {
        rebalanceConfig.setBatchSizePerServer(1);
      }
      RebalanceResult result = rebalancer.rebalance(tableConfig, rebalanceConfig, null);

      assertEquals(result.getStatus(), RebalanceResult.Status.DONE);
      assertEquals(conflictsInjected.get(), numVersionConflicts);
      assertEquals(forceCommittedSegments.size(), expectedCommitCount);
      assertEquals(forceCommitStarts.get(), reportedCommitCounts.size());
      assertEquals(reportedCommitCounts.stream().mapToInt(Integer::intValue).sum(), expectedCommitCount);
      reportedCommitCounts.forEach(count -> assertTrue(count > 0));
      for (int i = 0; i < expectedCommitCount; i++) {
        int partitionId = 0;
        if (addEarlierSegment) {
          partitionId = 1 - i;
        } else if (moveFirstPartition) {
          partitionId = i;
        }
        int sequenceNumber = numPartitions > 1 ? 11 : 11 + i;
        assertEquals(forceCommittedSegments.get(i),
            Set.of(new LLCSegmentName("forceCommit", partitionId, sequenceNumber, 0).getSegmentName()));
      }
      int finalSequenceNumber = numPartitions > 1 ? 12 : 11 + expectedCommitCount;
      for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
        String successor = new LLCSegmentName("forceCommit", partitionId, finalSequenceNumber, 0).getSegmentName();
        assertEquals(storedState.get().getRecord().getMapField(successor),
            instanceStates(TARGET_INSTANCES, CONSUMING));
      }
    }
  }

  private static Map<String, String> instanceStates(List<String> instances, String state) {
    Map<String, String> states = new TreeMap<>();
    instances.forEach(instance -> states.put(instance, state));
    return states;
  }

  private static IdealState copy(IdealState state) {
    ZNRecord record = new ZNRecord(state.getRecord());
    record.setVersion(state.getRecord().getVersion());
    return new IdealState(record);
  }
}
