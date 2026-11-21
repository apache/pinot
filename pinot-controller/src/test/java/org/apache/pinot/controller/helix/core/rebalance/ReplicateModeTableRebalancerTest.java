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

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.assignment.PartitionAssignmentUtils;
import org.apache.pinot.common.assignment.ReplicateModeAssignmentUtils;
import org.apache.pinot.controller.helix.core.assignment.segment.TableAssignment;
import org.apache.pinot.controller.helix.core.assignment.segment.TableAssignment.ReplicateModePartitionAssignment;
import org.slf4j.LoggerFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

/// Verifies replicate-mode rebalance stages retain one consumer and persist the matching full replica roles.
public class ReplicateModeTableRebalancerTest {
  private static final String TABLE = "table_REALTIME";
  private static final String SEGMENT = "table__0__1__20250228T1903Z";
  private static final String PREVIOUS = "table__0__0__20250228T1903Z";
  private static final String HISTORY = "historical";
  private static final List<String> ABC = List.of("a", "b", "c");
  private static final TableRebalancer.DataLossRiskAssessor NO_RISK = new TableRebalancer.NoOpRiskAssessor();

  @DataProvider
  public Object[][] completionTargets() {
    return new Object[][]{{List.of("b")}, {List.of("b", "c")}};
  }

  @Test(dataProvider = "completionTargets")
  public void testCompletedSegmentKeepsOwnerUntilTargetIsReady(List<String> instances) {
    Map<String, String> current = online(List.of("a"));
    Map<String, String> target = online(instances);
    Map<String, String> intermediate = new TreeMap<>(current);
    intermediate.putAll(target);
    assertEquals(TableRebalancer.getNextSegmentAssignment(current, target, Map.of("a", CONSUMING)), intermediate);
    assertEquals(TableRebalancer.getNextSegmentAssignment(intermediate, target, Map.of("a", ONLINE)), intermediate);
    assertEquals(TableRebalancer.getNextSegmentAssignment(intermediate, target,
        Map.of("a", ONLINE, "b", ONLINE)), target);
    assertEquals(current, Map.of("a", ONLINE));
  }

  @Test
  public void testCompletedSegmentCanExpandOnItsServingOwner() {
    assertEquals(TableRebalancer.getNextSegmentAssignment(Map.of("a", ONLINE), online(ABC),
        Map.of("a", CONSUMING)), online(ABC));
    assertEquals(TableRebalancer.getNextSegmentAssignment(online(ABC), online(List.of("a", "b")),
        Map.of("a", ONLINE)), online(List.of("a", "b")));
  }

  @DataProvider
  public Object[][] stagingModes() {
    return new Object[][]{{false, false, -1}, {false, true, -1}, {true, false, -1}, {true, true, -1},
        {false, false, 1}, {false, true, 1}, {true, false, 1}, {true, true, 1}};
  }

  @Test(dataProvider = "stagingModes")
  public void testSingletonHandoffKeepsHistoricalAvailability(boolean strict, boolean lowDisk, int batch) {
    Map<String, Map<String, String>> current = Map.of(
        HISTORY, online(ABC), SEGMENT, Map.of("a", CONSUMING));
    TableAssignment target = target(Map.of(HISTORY, online(List.of("b", "c", "d")),
        SEGMENT, Map.of("b", CONSUMING)), Map.of(), "b", ABC, Set.of(SEGMENT));
    TableAssignment next = TableRebalancer.getNextAssignment(current, Map.of(), target, 1,
        strict, lowDisk, batch, NO_RISK);
    assertEquals(next.getSegmentAssignment().get(SEGMENT), Map.of("b", CONSUMING));
    assertTrue(next.getSegmentAssignment().get(HISTORY).keySet().stream().anyMatch(ABC::contains));
    assertEquals(next.getReplicateModePartitionAssignment(), target.getReplicateModePartitionAssignment());
    assertEquals(next.getReplicateModeSegments(), Set.of(SEGMENT));
    IdealState idealState = idealState(current, "a", ABC);
    TableRebalancer.setAssignment(idealState, next.getSegmentAssignment(), next.getPartitionAssignment(), target);
    assertEquals(ReplicateModeAssignmentUtils.getLeader(idealState, 0), "b");
    assertEquals(ReplicateModeAssignmentUtils.getReplicas(idealState, 0), ABC);
    assertEquals(idealState.getReplicas(), "3");
    assertTrue(ReplicateModeAssignmentUtils.getPendingRestarts(idealState).isEmpty());
  }

  @Test
  public void testCommittingOwnerStaysOnlineWhileSuccessorChangesLeader() {
    Map<String, Map<String, String>> current = Map.of(PREVIOUS, Map.of("a", ONLINE),
        SEGMENT, Map.of("a", CONSUMING));
    TableAssignment target = target(Map.of(PREVIOUS, Map.of("a", ONLINE), SEGMENT, Map.of("b", CONSUMING)),
        Map.of(), "b", ABC, Set.of(PREVIOUS, SEGMENT));
    TableAssignment next = TableRebalancer.getNextAssignment(current, Map.of(), target, 0, true, false, 1, NO_RISK);
    IdealState idealState = idealState(current, "a", ABC);
    TableRebalancer.setAssignment(idealState, next.getSegmentAssignment(), Map.of(), target);
    assertEquals(idealState.getInstanceStateMap(PREVIOUS), Map.of("a", ONLINE));
    assertEquals(idealState.getInstanceStateMap(SEGMENT), Map.of("b", CONSUMING));
    assertEquals(ReplicateModeAssignmentUtils.getLeader(idealState, 0), "b");
    assertTrue(ReplicateModeAssignmentUtils.getPendingRestarts(idealState).isEmpty());
    assertFalse(TableRebalancer.hasCommittingOwnerOutsideCandidates(target));
  }

  @Test
  public void testOutOfPoolCommittingOwnerCannotBeReportedAsBalanced() {
    List<String> candidates = List.of("b", "c", "d");
    TableAssignment target = target(Map.of(PREVIOUS, Map.of("a", ONLINE), SEGMENT, Map.of("b", CONSUMING)),
        Map.of(), "b", candidates, Set.of(PREVIOUS, SEGMENT));
    assertTrue(TableRebalancer.hasCommittingOwnerOutsideCandidates(target));
    IdealState idealState = idealState(Map.of(PREVIOUS, Map.of("a", ONLINE), SEGMENT, Map.of("a", CONSUMING)),
        "a", ABC);
    TableRebalancer.setAssignment(idealState, target.getSegmentAssignment(), Map.of(), target);
    // New candidates must still be persisted: if the old owner fails, fixed-range recovery needs this new pool.
    assertEquals(ReplicateModeAssignmentUtils.getReplicas(idealState, 0), candidates);
    assertEquals(idealState.getInstanceStateMap(PREVIOUS), Map.of("a", ONLINE));
    assertEquals(idealState.getInstanceStateMap(SEGMENT), Map.of("b", CONSUMING));
  }

  @Test
  public void testCandidateOnlyChangeIsPersistedWithoutChangingConsumer() {
    Map<String, Map<String, String>> assignment = Map.of(SEGMENT, Map.of("a", CONSUMING));
    IdealState idealState = idealState(assignment, "a", ABC);
    idealState.getRecord().setSimpleField("unrelated", "value");
    idealState.getRecord().setListField("unrelated", List.of("value"));
    ReplicateModeAssignmentUtils.setAssignment(idealState, 99, "z", List.of("z"));
    TableAssignment target = target(assignment, Map.of(), "a", List.of("a", "b", "d"), Set.of(SEGMENT));
    assertFalse(TableRebalancer.isReplicateModeAssignmentApplied(idealState, target));
    TableRebalancer.setAssignment(idealState, assignment, Map.of(), target);
    assertTrue(TableRebalancer.isReplicateModeAssignmentApplied(idealState, target));
    assertEquals(idealState.getInstanceStateMap(SEGMENT), Map.of("a", CONSUMING));
    assertEquals(idealState.getReplicas(), "3");
    assertEquals(ReplicateModeAssignmentUtils.getLeader(idealState, 99), "z");
    assertEquals(idealState.getRecord().getSimpleField("unrelated"), "value");
    assertEquals(idealState.getRecord().getListField("unrelated"), List.of("value"));
    assertTrue(ReplicateModeAssignmentUtils.getPendingRestarts(idealState).isEmpty());
  }

  @DataProvider
  public Object[][] invalidPriorConsumers() {
    return new Object[][]{
        {Map.of("a", CONSUMING, "b", CONSUMING, "c", CONSUMING), "a"},
        {Map.of("a", CONSUMING), "b"}
    };
  }

  @Test(dataProvider = "invalidPriorConsumers")
  public void testRepairPersistsRestartForRetainedDiscardedConsumer(Map<String, String> previous, String oldLeader) {
    IdealState idealState = idealState(Map.of(SEGMENT, previous), oldLeader, ABC);
    TableAssignment target = target(Map.of(SEGMENT, Map.of("a", CONSUMING)), Map.of(), "a", ABC, Set.of(SEGMENT));
    TableRebalancer.setAssignment(idealState, target.getSegmentAssignment(), Map.of(), target);
    assertEquals(idealState.getInstanceStateMap(SEGMENT), Map.of("a", CONSUMING));
    assertEquals(ReplicateModeAssignmentUtils.getPendingRestarts(idealState), Map.of(SEGMENT, "a"));
    assertEquals(ReplicateModeAssignmentUtils.getLeader(idealState, 0), "a");
    assertEquals(idealState.getReplicas(), "3");
  }

  @Test
  public void testNewConsumerDoesNotRequireResetOfAnUnassignedServer() {
    IdealState idealState = idealState(Map.of(SEGMENT, Map.of("a", CONSUMING)), "c", ABC);
    TableAssignment target = target(Map.of(SEGMENT, Map.of("b", CONSUMING)), Map.of(), "b", ABC, Set.of(SEGMENT));
    TableRebalancer.setAssignment(idealState, target.getSegmentAssignment(), Map.of(), target);
    assertTrue(ReplicateModeAssignmentUtils.getPendingRestarts(idealState).isEmpty());
  }

  @Test(dataProvider = "invalidPriorConsumers")
  public void testForceCommitDoesNotWaitForKnownDiscardedConsumers(Map<String, String> previous, String oldLeader) {
    IdealState idealState = idealState(Map.of(SEGMENT, previous), oldLeader, ABC);
    TableAssignment target = target(Map.of(SEGMENT, Map.of("b", CONSUMING)), Map.of(), "b", ABC, Set.of(SEGMENT));
    Set<String> forceCommit = new HashSet<>(Set.of(SEGMENT, HISTORY));
    TableRebalancer.excludeDiscardedReplicateModeConsumers(forceCommit, idealState, target);
    assertEquals(forceCommit, Set.of(HISTORY));
    ReplicateModeAssignmentUtils.setAssignment(idealState, 0, "a", ABC);
    idealState.setInstanceStateMap(SEGMENT, Map.of("a", CONSUMING));
    forceCommit.add(SEGMENT);
    TableRebalancer.excludeDiscardedReplicateModeConsumers(forceCommit, idealState, target);
    assertEquals(forceCommit, Set.of(SEGMENT, HISTORY));
  }

  @Test
  public void testPinnedStagesKeepConsumerInsideActualReplicaCandidates() {
    List<String> finalCandidates = List.of("d", "e", "f");
    Map<String, Map<String, String>> current = Map.of(HISTORY, online(ABC), SEGMENT, Map.of("a", CONSUMING));
    Map<Integer, List<String>> currentPartitions = Map.of(0, ABC);
    TableAssignment target = target(Map.of(HISTORY, online(finalCandidates), SEGMENT, Map.of("f", CONSUMING)),
        Map.of(0, finalCandidates), "f", finalCandidates, Set.of(SEGMENT));
    IdealState idealState = idealState(current, "a", ABC);
    for (int stage = 0; stage < 3 && !current.equals(target.getSegmentAssignment()); stage++) {
      TableAssignment next = TableRebalancer.getNextAssignment(current, currentPartitions, target, 1,
          true, false, 1, NO_RISK);
      assertEquals(next.getSegmentAssignment().get(SEGMENT).size(), 1);
      String owner = next.getSegmentAssignment().get(SEGMENT).keySet().iterator().next();
      assertTrue(next.getPartitionAssignment().get(0).contains(owner));
      TableRebalancer.setAssignment(idealState, next.getSegmentAssignment(), next.getPartitionAssignment(), target);
      assertEquals(ReplicateModeAssignmentUtils.getLeader(idealState, 0), owner);
      assertEquals(ReplicateModeAssignmentUtils.getReplicas(idealState, 0), next.getPartitionAssignment().get(0));
      assertEquals(idealState.getReplicas(), "3");
      current = next.getSegmentAssignment();
      currentPartitions = next.getPartitionAssignment();
    }
    assertEquals(current, target.getSegmentAssignment());
    assertEquals(PartitionAssignmentUtils.getInstances(idealState, 0), finalCandidates);
    assertTrue(TableRebalancer.isReplicateModeAssignmentApplied(idealState, target));
  }

  @Test
  public void testPinnedCommittingStageRepairsMissingRecordedLeader() {
    List<String> finalCandidates = List.of("d", "e", "f");
    Map<String, Map<String, String>> current = Map.of(HISTORY, online(ABC), PREVIOUS, Map.of("a", ONLINE));
    TableAssignment target = target(Map.of(HISTORY, online(finalCandidates), PREVIOUS, Map.of("a", ONLINE)),
        Map.of(0, finalCandidates), "f", finalCandidates, Set.of(PREVIOUS));
    TableAssignment next = TableRebalancer.getNextAssignment(current, Map.of(0, ABC), target, 1,
        true, false, 1, NO_RISK);
    List<String> intermediateCandidates = List.copyOf(next.getPartitionAssignment().get(0));
    assertFalse(intermediateCandidates.contains("f"));
    IdealState idealState = new IdealState(TABLE);
    idealState.getRecord().setMapFields(new TreeMap<>(current));
    TableRebalancer.setAssignment(idealState, next.getSegmentAssignment(), Map.of(0, intermediateCandidates), target);
    assertEquals(idealState.getInstanceStateMap(PREVIOUS), Map.of("a", ONLINE));
    assertEquals(ReplicateModeAssignmentUtils.getLeader(idealState, 0), intermediateCandidates.get(0));
    assertEquals(ReplicateModeAssignmentUtils.getReplicas(idealState, 0), intermediateCandidates);
  }

  @Test
  public void testBatchLimitDefersCleanSingletonWithoutAddingAnotherConsumer() {
    Map<String, Map<String, String>> current = Map.of(HISTORY, online(List.of("a", "c", "d")),
        SEGMENT, Map.of("a", CONSUMING));
    TableAssignment target = target(Map.of(HISTORY, online(List.of("b", "c", "d")), SEGMENT, Map.of("b", CONSUMING)),
        Map.of(), "b", ABC, Set.of(SEGMENT));
    TableAssignment first = TableRebalancer.getNextAssignment(current, Map.of(), target, 1, false, false, 1, NO_RISK);
    assertEquals(first.getSegmentAssignment().get(SEGMENT), Map.of("a", CONSUMING));
    IdealState idealState = idealState(current, "a", ABC);
    TableRebalancer.setAssignment(idealState, first.getSegmentAssignment(), Map.of(), target);
    assertEquals(ReplicateModeAssignmentUtils.getLeader(idealState, 0), "a");
    assertFalse(TableRebalancer.isReplicateModeAssignmentApplied(idealState, target));
    TableAssignment second = TableRebalancer.getNextAssignment(first.getSegmentAssignment(), Map.of(),
        target, 1, false, false, 1, NO_RISK);
    assertEquals(second.getSegmentAssignment().get(SEGMENT), Map.of("b", CONSUMING));
  }

  @Test
  public void testRoleCarrierDisablesMapOnlyConcurrentAssignmentShortcut() {
    TableAssignment target = target(Map.of(SEGMENT, Map.of("b", CONSUMING)), Map.of(), "b", ABC, Set.of(SEGMENT));
    assertNull(TableRebalancer.getTargetAssignmentForNewIdealState(
        Map.of(SEGMENT, Map.of("a", CONSUMING)), Map.of(), Map.of(), target, List.of(SEGMENT), false, false,
        LoggerFactory.getLogger(ReplicateModeTableRebalancerTest.class)));
  }

  @Test
  public void testDefaultAvailabilityAppliesToCompletedReplicasRatherThanSingleConsumer() {
    Map<String, Map<String, String>> current = Map.of(HISTORY, online(ABC), SEGMENT, Map.of("a", CONSUMING));
    Map<String, Map<String, String>> target = Map.of(HISTORY, online(List.of("b", "c", "d")),
        SEGMENT, Map.of("b", CONSUMING));
    assertEquals(TableRebalancer.getMinAvailableReplicas(List.of(SEGMENT), current, target, 1, Set.of(SEGMENT),
        LoggerFactory.getLogger(ReplicateModeTableRebalancerTest.class)), 0);
    assertEquals(TableRebalancer.getMinAvailableReplicas(List.of(HISTORY, SEGMENT), current, target, 2, Set.of(SEGMENT),
        LoggerFactory.getLogger(ReplicateModeTableRebalancerTest.class)), 2);
    assertThrows(IllegalStateException.class, () -> TableRebalancer.getMinAvailableReplicas(List.of(SEGMENT), current,
        target, 1, Set.of(), LoggerFactory.getLogger(ReplicateModeTableRebalancerTest.class)));
  }

  @Test
  public void testRoleCarrierTakesImmutableSnapshots() {
    List<String> replicas = new ArrayList<>(List.of("c", "a", "b"));
    Map<Integer, ReplicateModePartitionAssignment> roles = new TreeMap<>();
    roles.put(0, new ReplicateModePartitionAssignment("a", replicas));
    Set<String> segments = new HashSet<>(Set.of(SEGMENT));
    TableAssignment target = new TableAssignment(Map.of(SEGMENT, Map.of("a", CONSUMING)), Map.of(),
        new Object2IntOpenHashMap<>(), roles, segments);
    replicas.clear();
    roles.clear();
    segments.clear();
    assertEquals(target.getReplicateModePartitionAssignment().get(0).replicas(), ABC);
    assertEquals(target.getReplicateModeSegments(), Set.of(SEGMENT));
    assertThrows(UnsupportedOperationException.class, () -> target.getReplicateModePartitionAssignment().clear());
    assertThrows(UnsupportedOperationException.class,
        () -> target.getReplicateModePartitionAssignment().get(0).replicas().clear());
  }

  private static TableAssignment target(Map<String, Map<String, String>> segments,
      Map<Integer, List<String>> partitions, String leader, List<String> replicas, Set<String> unfinished) {
    Object2IntOpenHashMap<String> partitionIds = new Object2IntOpenHashMap<>();
    segments.keySet().forEach(segment -> partitionIds.put(segment, 0));
    return new TableAssignment(segments, partitions, partitionIds,
        Map.of(0, new ReplicateModePartitionAssignment(leader, replicas)), unfinished);
  }

  private static IdealState idealState(Map<String, Map<String, String>> assignment, String leader,
      List<String> replicas) {
    IdealState idealState = new IdealState(TABLE);
    idealState.getRecord().setMapFields(new TreeMap<>(assignment));
    ReplicateModeAssignmentUtils.setAssignment(idealState, 0, leader, replicas);
    return idealState;
  }

  private static Map<String, String> online(List<String> instances) {
    Map<String, String> states = new TreeMap<>();
    instances.forEach(instance -> states.put(instance, ONLINE));
    return states;
  }
}
