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
  private static final List<String> REASSIGNED_INSTANCES = List.of("reassignedServer0", "reassignedServer1");

  private enum ConcurrentChange {
    VERSION_ONLY,
    MOVE_FIRST_PARTITION,
    MOVE_TO_INTERMEDIATE,
    ADD_EARLIER_SEGMENT
  }

  private enum CommitBehavior {
    IMMEDIATE,
    PUBLISH_AT_ASSIGNMENT_WRITE,
    ADD_EARLIER_SEGMENT,
    RELOCATE_SUCCESSOR_AND_REASSIGN,
    FAIL
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

  @Test
  public void testCommitPublicationRacesWithAssignmentWrite()
      throws Exception {
    // DONE metadata can be observed before the original becomes ONLINE and its successor is added to IdealState.
    assertForceCommits(0, 0, 1, ConcurrentChange.VERSION_ONLY, 0,
        CommitBehavior.PUBLISH_AT_ASSIGNMENT_WRITE, true);
  }

  @Test
  public void testForceCommitNewPartitionSelectedDuringInnerReplan()
      throws Exception {
    // Completing partition 1 adds a consuming segment for the previously empty partition 0. Partition 0 becomes
    // the next batch and must be committed before any assignment is written; partition 1's commit remains valid.
    assertForceCommits(0, 0, 2, ConcurrentChange.VERSION_ONLY, 0, CommitBehavior.ADD_EARLIER_SEGMENT, true);
  }

  @Test
  public void testForceCommitFailureAbortsBeforeMovingSegments()
      throws Exception {
    assertForceCommits(0, 0, 1, ConcurrentChange.VERSION_ONLY, 0, CommitBehavior.FAIL, true);
  }

  @Test
  public void testForceCommitDisabled()
      throws Exception {
    assertForceCommits(0, 0, 0, ConcurrentChange.VERSION_ONLY, 0, CommitBehavior.IMMEDIATE, false);
  }

  @Test
  public void testForceCommitAfterConcurrentMovement()
      throws Exception {
    // Another rebalance moves the partition to intermediate instances after the first force commit. Moving it
    // again from those instances requires a fresh force commit even though our own previous write was rejected.
    assertForceCommits(1, 1, 2, ConcurrentChange.MOVE_TO_INTERMEDIATE, 0);
  }

  @Test
  public void testForceCommitAfterNonStrictSuccessorRelocation()
      throws Exception {
    // A non-pinned successor starts on the current target instances. If instance reassignment changes the target
    // again during the full recalculation, that successor needs a force commit before its own movement.
    assertForceCommits(0, 0, 2, ConcurrentChange.VERSION_ONLY, 0,
        CommitBehavior.RELOCATE_SUCCESSOR_AND_REASSIGN, true);
  }

  private void assertForceCommits(int minAvailableReplicas, int numVersionConflicts, int expectedCommitCount,
      ConcurrentChange concurrentChange, int successfulWritesBeforeConflict)
      throws Exception {
    assertForceCommits(minAvailableReplicas, numVersionConflicts, expectedCommitCount, concurrentChange,
        successfulWritesBeforeConflict, CommitBehavior.IMMEDIATE, true);
  }

  private void assertForceCommits(int minAvailableReplicas, int numVersionConflicts, int expectedCommitCount,
      ConcurrentChange concurrentChange, int successfulWritesBeforeConflict, CommitBehavior commitBehavior,
      boolean forceCommitEnabled)
      throws Exception {
    HelixManager helixManager = mock(HelixManager.class);
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    @SuppressWarnings("unchecked")
    BaseDataAccessor<ZNRecord> baseDataAccessor = mock(BaseDataAccessor.class);
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder("testCluster");
    when(helixManager.getHelixDataAccessor()).thenReturn(dataAccessor);
    when(dataAccessor.keyBuilder()).thenReturn(keyBuilder);
    when(dataAccessor.getBaseDataAccessor()).thenReturn(baseDataAccessor);

    boolean addEarlierSegment = concurrentChange == ConcurrentChange.ADD_EARLIER_SEGMENT
        || commitBehavior == CommitBehavior.ADD_EARLIER_SEGMENT;
    boolean moveFirstPartition = concurrentChange == ConcurrentChange.MOVE_FIRST_PARTITION;
    boolean relocateSuccessor = commitBehavior == CommitBehavior.RELOCATE_SUCCESSOR_AND_REASSIGN;
    AtomicReference<List<String>> targetInstances = new AtomicReference<>(TARGET_INSTANCES);
    List<List<String>> recalculatedTargets = new ArrayList<>();
    int numPartitions = addEarlierSegment || moveFirstPartition ? 2 : 1;
    IdealState initialState = new IdealState(TABLE_NAME);
    initialState.setReplicas("2");
    if (addEarlierSegment) {
      PartitionAssignmentUtils.setInstances(initialState, 0, SOURCE_INSTANCES);
    }
    for (int partitionId = addEarlierSegment ? 1 : 0; partitionId < numPartitions; partitionId++) {
      String originalSegment = new LLCSegmentName("forceCommit", partitionId, 11, 0).getSegmentName();
      initialState.getRecord().setMapField(originalSegment, instanceStates(SOURCE_INSTANCES, CONSUMING));
      if (!relocateSuccessor) {
        PartitionAssignmentUtils.setInstances(initialState, partitionId, SOURCE_INSTANCES);
      }
    }
    AtomicReference<IdealState> storedState = new AtomicReference<>(initialState);
    when(dataAccessor.getProperty(keyBuilder.idealStates(TABLE_NAME)))
        .thenAnswer(invocation -> copy(storedState.get()));
    when(dataAccessor.getProperty(keyBuilder.externalView(TABLE_NAME)))
        .thenAnswer(invocation -> new ExternalView(copy(storedState.get()).getRecord()));

    AtomicInteger conflictsInjected = new AtomicInteger();
    AtomicInteger successfulWrites = new AtomicInteger();
    AtomicInteger writeAttempts = new AtomicInteger();
    AtomicInteger delayedPublications = new AtomicInteger();
    AtomicInteger commitsBeforeFirstWrite = new AtomicInteger();
    AtomicReference<Set<String>> unpublishedSegments = new AtomicReference<>();
    List<Set<String>> forceCommittedSegments = new ArrayList<>();
    List<Set<String>> forceCommittedInstances = new ArrayList<>();
    when(baseDataAccessor.set(eq(keyBuilder.idealStates(TABLE_NAME).getPath()), any(ZNRecord.class), anyInt(),
        eq(AccessOption.PERSISTENT))).thenAnswer(invocation -> {
          assertTrue(writeAttempts.incrementAndGet() <= 20, "Rebalance did not make progress after 20 writes");
          assertEquals(invocation.<Integer>getArgument(2).intValue(), storedState.get().getRecord().getVersion(),
              "Rebalance attempted to persist an assignment based on an outdated IdealState version");
          Set<String> segmentsToPublish = unpublishedSegments.getAndSet(null);
          if (segmentsToPublish != null) {
            storedState.set(publishCommit(storedState.get(), segmentsToPublish));
            delayedPublications.incrementAndGet();
            throw new ZkBadVersionException("Commit published its IdealState update before rebalance");
          }
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
            } else if (concurrentChange == ConcurrentChange.MOVE_TO_INTERMEDIATE) {
              List<String> intermediateInstances = List.of(SOURCE_INSTANCES.getFirst(), TARGET_INSTANCES.getFirst());
              state.getRecord().getMapFields().replaceAll((segment, states) ->
                  instanceStates(intermediateInstances, states.values().iterator().next()));
              PartitionAssignmentUtils.setInstances(state, 0, intermediateInstances);
            } else if (concurrentChange == ConcurrentChange.ADD_EARLIER_SEGMENT) {
              String segment = new LLCSegmentName("forceCommit", 0, 11, 0).getSegmentName();
              state.getRecord().setMapField(segment, instanceStates(SOURCE_INSTANCES, CONSUMING));
              PartitionAssignmentUtils.setInstances(state, 0, SOURCE_INSTANCES);
            }
            storedState.get().getRecord().setVersion(storedState.get().getRecord().getVersion() + 1);
            throw new ZkBadVersionException("Injected concurrent IdealState write");
          }
          ZNRecord record = copy(new IdealState(invocation.<ZNRecord>getArgument(1))).getRecord();
          record.setVersion(invocation.<Integer>getArgument(2) + 1);
          storedState.set(new IdealState(record));
          if (successfulWrites.get() == 0) {
            commitsBeforeFirstWrite.set(forceCommittedSegments.size());
          }
          successfulWrites.incrementAndGet();
          return true;
        });

    PinotLLCRealtimeSegmentManager segmentManager = mock(PinotLLCRealtimeSegmentManager.class);
    when(segmentManager.forceCommit(eq(TABLE_NAME), isNull(), any(String.class), any(BatchConfig.class)))
        .thenAnswer(invocation -> {
          Set<String> segments = new HashSet<>(Arrays.asList(invocation.<String>getArgument(2).split(",")));
          // The real API filters out requested segments that no longer have a CONSUMING replica in IdealState.
          segments.removeIf(segment -> {
            Map<String, String> states = storedState.get().getRecord().getMapField(segment);
            return states == null || !states.containsValue(CONSUMING);
          });
          forceCommittedSegments.add(segments);
          if (relocateSuccessor) {
            assertEquals(segments.size(), 1);
            forceCommittedInstances.add(Set.copyOf(
                storedState.get().getRecord().getMapField(segments.iterator().next()).keySet()));
          }
          return segments;
        });
    doAnswer(invocation -> {
      if (commitBehavior == CommitBehavior.FAIL) {
        throw new IllegalStateException("Injected force commit failure");
      }
      Set<String> segments = invocation.getArgument(1);
      if (commitBehavior == CommitBehavior.PUBLISH_AT_ASSIGNMENT_WRITE) {
        unpublishedSegments.set(Set.copyOf(segments));
      } else {
        IdealState state = publishCommit(storedState.get(), segments, relocateSuccessor ? targetInstances.get() : null);
        if (commitBehavior == CommitBehavior.ADD_EARLIER_SEGMENT && forceCommittedSegments.size() == 1) {
          String segment = new LLCSegmentName("forceCommit", 0, 11, 0).getSegmentName();
          state.getRecord().setMapField(segment, instanceStates(SOURCE_INSTANCES, CONSUMING));
        }
        storedState.set(state);
        if (relocateSuccessor) {
          // The commit uses the existing instance partitions; the following full recalculation sees new instances.
          targetInstances.set(REASSIGNED_INSTANCES);
        }
      }
      return null;
    }).when(segmentManager).waitUntilSegmentsForceCommitted(eq(TABLE_NAME), any(), any());

    TableConfigBuilder tableConfigBuilder =
        new TableConfigBuilder(TableType.REALTIME).setTableName(TABLE_NAME).setNumReplicas(2);
    if (!relocateSuccessor) {
      tableConfigBuilder.setRoutingConfig(new RoutingConfig(null, null,
          RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, false));
    }
    TableConfig tableConfig = tableConfigBuilder.build();
    SegmentAssignment segmentAssignment = mock(SegmentAssignment.class);
    when(segmentAssignment.rebalanceTable(any(), any(), any(), any(), any())).thenAnswer(invocation -> {
      IdealState state = invocation.getArgument(0);
      List<String> instances = targetInstances.get();
      recalculatedTargets.add(instances);
      Map<String, Map<String, String>> target = new TreeMap<>();
      state.getRecord().getMapFields().forEach((segment, states) ->
          target.put(segment, instanceStates(instances, states.values().iterator().next())));
      Map<Integer, List<String>> targetPartitions = new TreeMap<>();
      PartitionAssignmentUtils.getPartitionAssignment(state).keySet()
          .forEach(partitionId -> targetPartitions.put(partitionId, instances));
      return new TableAssignment(target, targetPartitions);
    });

    try (MockedStatic<SegmentAssignmentFactory> factory = mockStatic(SegmentAssignmentFactory.class)) {
      factory.when(() -> SegmentAssignmentFactory.getSegmentAssignment(helixManager, tableConfig))
          .thenReturn(segmentAssignment);
      AtomicInteger forceCommitStarts = new AtomicInteger();
      List<String> observerErrors = new ArrayList<>();
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

        @Override
        public void onError(String errorMsg) {
          observerErrors.add(errorMsg);
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
      rebalanceConfig.setForceCommit(forceCommitEnabled);
      rebalanceConfig.setMinAvailableReplicas(minAvailableReplicas);
      if (numPartitions > 1) {
        rebalanceConfig.setBatchSizePerServer(1);
      }
      RebalanceResult result = rebalancer.rebalance(tableConfig, rebalanceConfig, null);

      if (commitBehavior == CommitBehavior.FAIL) {
        assertEquals(result.getStatus(), RebalanceResult.Status.FAILED);
        assertEquals(forceCommittedSegments.size(), 1);
        assertEquals(writeAttempts.get(), 0);
        assertEquals(storedState.get().getRecord().getMapFields(), initialState.getRecord().getMapFields());
        assertEquals(forceCommitStarts.get(), 1);
        assertTrue(reportedCommitCounts.isEmpty());
        assertEquals(observerErrors.size(), 1);
        assertTrue(observerErrors.getFirst().contains("Injected force commit failure"));
        return;
      }
      assertEquals(result.getStatus(), RebalanceResult.Status.DONE);
      assertTrue(observerErrors.isEmpty());
      assertEquals(conflictsInjected.get(), numVersionConflicts);
      assertEquals(delayedPublications.get(),
          commitBehavior == CommitBehavior.PUBLISH_AT_ASSIGNMENT_WRITE ? 1 : 0);
      if (commitBehavior == CommitBehavior.ADD_EARLIER_SEGMENT) {
        assertEquals(commitsBeforeFirstWrite.get(), 2);
      }
      if (relocateSuccessor) {
        assertEquals(forceCommittedInstances, List.of(Set.copyOf(SOURCE_INSTANCES), Set.copyOf(TARGET_INSTANCES)));
        assertEquals(recalculatedTargets, List.of(TARGET_INSTANCES, REASSIGNED_INSTANCES, REASSIGNED_INSTANCES));
        assertEquals(commitsBeforeFirstWrite.get(), 2);
      }
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
            instanceStates(targetInstances.get(), CONSUMING));
      }
    }
  }

  private static Map<String, String> instanceStates(List<String> instances, String state) {
    Map<String, String> states = new TreeMap<>();
    instances.forEach(instance -> states.put(instance, state));
    return states;
  }

  private static IdealState publishCommit(IdealState previousState, Set<String> segments) {
    return publishCommit(previousState, segments, null);
  }

  private static IdealState publishCommit(IdealState previousState, Set<String> segments,
      List<String> successorInstances) {
    IdealState state = copy(previousState);
    for (String segment : segments) {
      LLCSegmentName name = new LLCSegmentName(segment);
      List<String> currentInstances = new ArrayList<>(state.getRecord().getMapField(segment).keySet());
      String successor = new LLCSegmentName(name.getTableName(), name.getPartitionGroupId(),
          name.getSequenceNumber() + 1, 0).getSegmentName();
      state.getRecord().setMapField(segment, instanceStates(currentInstances, ONLINE));
      // Pinned successors remain on the current instances; non-pinned successors use the latest instance partitions.
      state.getRecord().setMapField(successor,
          instanceStates(successorInstances != null ? successorInstances : currentInstances, CONSUMING));
    }
    state.getRecord().setVersion(state.getRecord().getVersion() + 1);
    return state;
  }

  private static IdealState copy(IdealState state) {
    ZNRecord record = new ZNRecord(state.getRecord());
    // ZNRecord's copy constructor shares nested collections; persisted reads/writes must not share them.
    state.getRecord().getMapFields().forEach((segment, states) -> record.setMapField(segment, new TreeMap<>(states)));
    state.getRecord().getListFields().forEach((key, values) -> record.setListField(key, new ArrayList<>(values)));
    return new IdealState(record);
  }
}
