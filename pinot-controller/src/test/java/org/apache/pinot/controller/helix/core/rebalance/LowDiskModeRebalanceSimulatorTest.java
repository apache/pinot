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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.pinot.common.restlet.resources.RebalanceConfig;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentUtils;
import org.apache.pinot.controller.util.TableSizeReader;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Drives the real [TableRebalancer#getNextAssignment] step by step, the way [TableRebalancer] does, and checks the
/// three things low disk mode promises:
///
/// 1. the rebalance always reaches the target assignment, i.e. deferring segment moves never wedges it;
/// 2. no server is pushed above `max(bytes it held that this rebalance did not place, bytes the target places on it)`,
///    except where the disk utilization pre-check says up front that it will be;
/// 3. under strict replica group routing, segments sharing a current and target instance pair always move together.
///
/// Segment sizes are the ones [TableSizeReader] reports. Where a scenario supplies none, every segment weighs one
/// byte, which turns the budget into a bound on the number of segments hosted.
///
/// [#main] runs randomized sweeps over a far wider space than the tests do, and reports how often the budget had to be
/// given up and how many steps it took. That is for comparing two versions of the assignment logic by hand, not for
/// CI, so it is deliberately not a test.
public class LowDiskModeRebalanceSimulatorTest {
  private static final String ONLINE = "ONLINE";
  private static final int MAX_STEPS = 200;
  private static final long MIB = 1024L * 1024;
  private static final TableRebalancer.PartitionIdFetcher DUMMY_PARTITION_FETCHER = segmentName -> 0;
  /// Reads the partition id back out of a segment name, so that batching groups segments the way it would in a
  /// cluster rather than treating the whole table as one partition
  private static final TableRebalancer.PartitionIdFetcher LLC_PARTITION_FETCHER = segmentName -> {
    LLCSegmentName llcSegmentName = LLCSegmentName.of(segmentName);
    return llcSegmentName == null ? 0 : llcSegmentName.getPartitionGroupId();
  };
  private static final TableRebalancer.DataLossRiskAssessor NO_DATA_LOSS_RISK =
      new TableRebalancer.NoOpRiskAssessor();

  // ---------------------------------------------------------------------------------------------------------------
  // The three guarantees
  // ---------------------------------------------------------------------------------------------------------------

  /// The budget defers segment moves, so the thing to rule out is that it defers them forever.
  @Test
  public void testRebalanceAlwaysReachesTheTargetAssignment() {
    for (Scenario scenario : scenarios()) {
      SimResult result = simulate(scenario);
      assertTrue(result.reachedTarget(), scenario._name + ": " + result._outcome + result.report());
    }
  }

  /// The bound, and the pre-check that reports where it cannot be held.
  ///
  /// When no progress at all is possible within the budget, the rebalance gives it up for a step rather than stalling,
  /// so the bound is not absolute. What is absolute is that the pre-check replay names exactly the servers that go
  /// over, before any segment moves: an operator told nothing is entitled to a rebalance that stays within the disk
  /// every server started with.
  @Test
  public void testSequenceStaysWithinBudgetUnlessThePreCheckSaysOtherwise() {
    for (Scenario scenario : scenarios()) {
      SimResult result = simulate(scenario);
      if (scenario._injectAtStep > 0) {
        // The pre-check runs before the rebalance, so it cannot know about segments that appear while it runs. Those
        // are credited to the anchor when first seen, which has to keep the rebalance inside the budget on its own
        assertEquals(result._serversOverBudget, Set.of(),
            scenario._name + ": went outside the budget while segments were being added" + result.report());
        continue;
      }
      Set<String> reported = TableRebalancer.getServersForcedOverDiskBudget(scenario._currentAssignment,
          scenario._targetAssignment, scenario._minAvailableReplicas, scenario._enableStrictReplicaGroup,
          scenario._batchSizePerServer, toTableSizeDetails(scenario._segmentSizeBytes),
          LoggerFactory.getLogger(getClass())).keySet();
      // Asserting what the pre-check reports, and not only that it agrees with the rebalance, is what makes this fail
      // when a change starts giving the budget up on a shape that used to hold: the equality on its own passes a
      // rebalance that goes over and says so.
      if (scenario._withinBudget) {
        assertEquals(reported, Set.of(),
            scenario._name + ": the pre-check says the budget cannot be held for a shape that it should"
                + result.report());
      } else {
        assertTrue(!reported.isEmpty(), scenario._name + ": expected the pre-check to report a server, so that the "
            + "case where it does is covered. If a change made this shape hold, clear its withinBudget flag"
            + result.report());
      }
      assertEquals(result._serversOverBudget, reported,
          scenario._name + ": the servers that went over the budget are not the ones the pre-check named"
              + result.report());
    }
  }

  /// Strict replica group routing needs every segment of a partition on the same instances at all times, so the budget
  /// has to charge a whole group of segments at once. Charging one segment at a time fits the first few segments of a
  /// partition and not the rest, which splits the partition across replica groups.
  @Test
  public void testStrictReplicaGroupMovesGroupsTogether() {
    for (Scenario scenario : scenarios()) {
      if (scenario._enableStrictReplicaGroup) {
        SimResult result = simulate(scenario);
        assertEquals(result._groupSplits, Set.of(),
            scenario._name + ": split a group of segments across instances" + result.report());
      }
    }
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Scenarios
  // ---------------------------------------------------------------------------------------------------------------

  /// The shapes worth holding down: the overlapping server sets that per-segment sequencing could not bound, a pure
  /// scale-out as a control, both routing modes, batching, uneven segment sizes, segments arriving mid-rebalance, and
  /// the assignment a randomized search found hardest.
  private static List<Scenario> scenarios() {
    Random random = new Random(11);
    List<Scenario> scenarios = new ArrayList<>();
    List<String> threeOld = List.of("host1", "host2", "host3");
    List<String> threeNew = List.of("host2", "host3", "host4");
    List<String> sixOld = servers(0, 6);
    List<String> sixNew = servers(3, 6);

    for (boolean strict : List.of(false, true)) {
      String suffix = strict ? " [strict]" : "";
      // The original shape: every server in the overlap both sheds and takes on segments
      scenarios.add(new Scenario("3 -> 3 servers, 2 in overlap" + suffix, roundRobin(6, 2, threeOld, 0),
          roundRobin(6, 2, threeNew, 0), 1, strict, RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, Map.of(), 0));
      // Same, with uneven segment sizes, which is what a budget counting segments cannot see
      Map<String, Map<String, String>> skewed = roundRobin(24, 2, sixOld, 0);
      scenarios.add(new Scenario("6 -> 6 servers, 3 in overlap, uneven sizes" + suffix, skewed,
          roundRobin(24, 2, sixNew, 0), 1, strict, RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER,
          skewedSizes(skewed.keySet(), random), 0));
      // Replication 3 against a minimum of 1, so a segment has to gain two instances at once
      Map<String, Map<String, String>> deep = roundRobin(18, 3, sixOld, 0);
      scenarios.add(new Scenario("6 -> 6 servers, replication 3, uneven sizes" + suffix, deep,
          roundRobin(18, 3, sixNew, 0), 1, strict, RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER,
          skewedSizes(deep.keySet(), random), 0));
      // Batching, which defers whole partitions for reasons of its own
      Map<String, Map<String, String>> batched = roundRobin(24, 2, sixOld, 0);
      scenarios.add(new Scenario("6 -> 6 servers, batchSizePerServer 2" + suffix, batched,
          roundRobin(24, 2, sixNew, 0), 1, strict, 2, skewedSizes(batched.keySet(), random), 0));
      // A control: no server in the old set gains anything, so the budget is never binding
      scenarios.add(new Scenario("4 -> 8 servers, pure scale-out" + suffix, roundRobin(24, 2, servers(0, 4), 0),
          roundRobin(24, 2, servers(0, 8), 0), 1, strict, RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, Map.of(), 0));
    }

    scenarios.addAll(replicaGroupScenarios(random));
    scenarios.add(hardestKnownStrictScenario());
    scenarios.add(knownToNeedTheBudgetGivenUpScenario());
    scenarios.addAll(midRebalanceUploadScenarios());
    return scenarios;
  }

  /// The one assignment known to need the budget given up, found by [#sweepRandomGroupStructures]. Included so that
  /// the pre-check is checked against a rebalance that does go over, and not only against ones that do not.
  ///
  /// `host05` starts on 2041 MiB and the target places 1523 MiB on it, so its ceiling is what it started with. It is
  /// pinned with eight segments it cannot drop without going below three available replicas, and a step arrives where
  /// it is the only place left to put a segment and has nothing spare. The rebalance gives the budget up for that step
  /// and `host05` ends up 31 MiB over, 2% above its ceiling.
  private static Scenario knownToNeedTheBudgetGivenUpScenario() {
    // Groups of segments sharing one current and one target instance set, then the size of every segment in MiB
    String[][] groups = {
        {"host00,host01,host02,host03", "host02,host04,host05,host06", "0,1,2,3,4,5,6,7,8,9"},
        {"host00,host02,host06", "host00,host03,host05,host06", "10,11,12"},
        {"host00,host01,host03,host06", "host01,host02,host03,host05", "13,14,15"},
        {"host02,host04,host05,host06", "host00,host01,host03,host04", "16,17,18,19,20,21,22,23"},
        {"host01,host02,host03,host04", "host00,host02,host05,host06", "24,25,26,27,28,29,30,31,32"}
    };
    long[] sizesInMib = {
        31, 12, 17, 9, 16, 14, 431, 664, 23, 23, 23, 20, 12, 12, 28, 9, 29, 1162, 768, 14, 15, 31, 8, 14, 21, 9, 14,
        25, 15, 25, 18, 24, 28
    };
    Map<String, Map<String, String>> current = new TreeMap<>();
    Map<String, Map<String, String>> target = new TreeMap<>();
    Map<String, Long> sizes = new TreeMap<>();
    for (String[] group : groups) {
      for (String index : group[2].split(",")) {
        String segment = String.format("segment%03d", Integer.parseInt(index));
        current.put(segment, instanceStateMap(group[0].split(",")));
        target.put(segment, instanceStateMap(group[1].split(",")));
        sizes.put(segment, sizesInMib[Integer.parseInt(index)] * MIB);
      }
    }
    Scenario scenario = new Scenario("assignment known to need the budget given up", current, target, 3, false, 1,
        sizes, 0);
    scenario._withinBudget = false;
    return scenario;
  }

  /// Replica group assignment, where the placement is structured rather than balanced: every segment of a partition
  /// sits on one server per replica group, so a partition's segments always share a current and target instance pair.
  /// That is the shape strict replica group routing is built for, and it moves partitions between the servers of a
  /// replica group rather than spreading them over all servers the way a balanced assignment does.
  private static List<Scenario> replicaGroupScenarios(Random random) {
    List<List<String>> twoBySmall = List.of(List.of("host00", "host01"), List.of("host02", "host03"));
    List<List<String>> twoByGrown =
        List.of(List.of("host00", "host01", "host04"), List.of("host02", "host03", "host05"));
    List<List<String>> twoByRotated = List.of(List.of("host01", "host04"), List.of("host03", "host05"));
    List<List<String>> threeBySmall =
        List.of(List.of("host00", "host01"), List.of("host02", "host03"), List.of("host04", "host05"));
    List<List<String>> threeByGrown = List.of(List.of("host00", "host01", "host06"),
        List.of("host02", "host03", "host07"), List.of("host04", "host05", "host08"));

    List<Scenario> scenarios = new ArrayList<>();
    for (boolean strict : List.of(false, true)) {
      String suffix = strict ? " [strict]" : "";
      scenarios.add(replicaGroupScenario("replica groups grow 2 -> 3 servers each" + suffix, twoBySmall, twoByGrown,
          strict, RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, random));
      scenarios.add(replicaGroupScenario("replica groups, one server replaced in each" + suffix, twoBySmall,
          twoByRotated, strict, RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, random));
    }
    scenarios.add(replicaGroupScenario("replica groups shrink 3 -> 2 servers each [strict]", twoByGrown, twoBySmall,
        true, RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, random));
    scenarios.add(replicaGroupScenario("three replica groups grow 2 -> 3 servers each [strict]", threeBySmall,
        threeByGrown, true, RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, random));
    // Batching groups segments by partition, which only means anything once partition ids are real
    scenarios.add(replicaGroupScenario("replica groups grow, batchSizePerServer 2 [strict]", twoBySmall, twoByGrown,
        true, 2, random));
    return scenarios;
  }

  private static Scenario replicaGroupScenario(String name, List<List<String>> currentReplicaGroups,
      List<List<String>> targetReplicaGroups, boolean strict, int batchSizePerServer, Random random) {
    Map<String, Map<String, String>> current = replicaGroupAssignment(9, 3, currentReplicaGroups);
    Map<String, Map<String, String>> target = replicaGroupAssignment(9, 3, targetReplicaGroups);
    Scenario scenario = new Scenario(name, current, target, 1, strict, batchSizePerServer,
        skewedSizes(current.keySet(), random), 0);
    scenario._partitionIdFetcher = LLC_PARTITION_FETCHER;
    return scenario;
  }

  /// Assigns `numPartitions` partitions of `segmentsPerPartition` segments over `replicaGroups`, one server per
  /// replica group per partition, the shape `ReplicaGroupSegmentAssignmentStrategy` produces. Segments are named so
  /// that their partition id can be read back, which is what batching groups them by.
  private static Map<String, Map<String, String>> replicaGroupAssignment(int numPartitions, int segmentsPerPartition,
      List<List<String>> replicaGroups) {
    Map<String, Map<String, String>> assignment = new TreeMap<>();
    for (int partition = 0; partition < numPartitions; partition++) {
      List<String> instances = new ArrayList<>(replicaGroups.size());
      for (List<String> replicaGroup : replicaGroups) {
        instances.add(replicaGroup.get(partition % replicaGroup.size()));
      }
      for (int sequence = 0; sequence < segmentsPerPartition; sequence++) {
        assignment.put(String.format("myTable__%d__%d__20240101T000000Z", partition, sequence),
            SegmentAssignmentUtils.getInstanceStateMap(instances, ONLINE));
      }
    }
    return assignment;
  }

  /// The assignment a randomized search over group structures found hardest: `host02` has 318 MiB free under its own
  /// ceiling while the group it has to take on is six segments totalling roughly 1.2 GiB, and strict replica group
  /// routing needs all of them to move at once. Choosing the instances to add without regard for the budget left every
  /// group blocked here, which gave the budget up and pushed `host02` 36% over.
  private static Scenario hardestKnownStrictScenario() {
    // Groups of segments sharing one current and one target instance set, and the size of each segment in MiB
    String[][] groups = {
        {"host02,host07", "host00,host01", "0,1,2"},
        {"host00,host01", "host02,host04", "3,4,5,6,7"},
        {"host04", "host00,host06", "8,9,10,11,12,13"},
        {"host04,host05", "host00,host02", "14,15,16,17,18,19"}
    };
    long[] sizesInMib = {1099, 8, 1135, 351, 27, 23, 895, 12, 8, 9, 20, 14, 30, 14, 23, 1152, 26, 16, 17, 18};

    Map<String, Map<String, String>> current = new TreeMap<>();
    Map<String, Map<String, String>> target = new TreeMap<>();
    Map<String, Long> sizes = new TreeMap<>();
    for (String[] group : groups) {
      for (String index : group[2].split(",")) {
        String segment = String.format("segment%03d", Integer.parseInt(index));
        current.put(segment, instanceStateMap(group[0].split(",")));
        target.put(segment, instanceStateMap(group[1].split(",")));
        sizes.put(segment, sizesInMib[Integer.parseInt(index)] * MIB);
      }
    }
    return new Scenario("hardest known strict replica group assignment", current, target, 1, true,
        RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, sizes, 0);
  }

  /// Segments can be uploaded, or start consuming, while a rebalance runs. They are absent from the anchor the budget
  /// was built on, so they are credited to it when first seen: the ceiling of a server whose net change is a loss is
  /// pinned to what it started with, and would otherwise have the headroom the rebalance needs eaten by them.
  ///
  /// Both placements are covered. Strict replica group assignment overrides a new segment onto its partition's
  /// existing placement to keep the partition collocated, which mid-rebalance is the placement being moved away from,
  /// so the target assignment naming somewhere else is the normal case there rather than the exception.
  private static List<Scenario> midRebalanceUploadScenarios() {
    List<Scenario> scenarios = new ArrayList<>();
    for (boolean targetAgrees : List.of(true, false)) {
      Map<String, Map<String, String>> current = roundRobin(12, 2, List.of("host1", "host2", "host3"), 0);
      Map<String, Map<String, String>> target = roundRobin(12, 2, List.of("host2", "host3", "host4"), 0);
      Map<String, Long> sizes = new TreeMap<>();
      current.keySet().forEach(segment -> sizes.put(segment, 100 * MIB));

      Scenario scenario = new Scenario(
          "3 -> 3 servers, segments uploaded mid-rebalance, target " + (targetAgrees ? "agrees" : "names elsewhere"),
          current, target, 1, false, RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, sizes, 3);
      for (int i = 0; i < 4; i++) {
        String segment = "uploaded" + i;
        scenario._injectedCurrent.put(segment, instanceStateMap("host1", "host2"));
        scenario._injectedTarget.put(segment,
            targetAgrees ? instanceStateMap("host1", "host2") : instanceStateMap("host3", "host4"));
        sizes.put(segment, 400 * MIB);
      }
      scenarios.add(scenario);
    }
    return scenarios;
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Simulator
  // ---------------------------------------------------------------------------------------------------------------

  /// Runs `scenario` to the target assignment, or until it stops making progress, tracking for every server the most
  /// it ever hosts and whether the budget had to be given up to get there.
  private static SimResult simulate(Scenario scenario) {
    Map<String, Map<String, String>> current = deepCopy(scenario._currentAssignment);
    Map<String, Map<String, String>> target = deepCopy(scenario._targetAssignment);
    Map<String, Long> sizes = new TreeMap<>(scenario._segmentSizeBytes);
    TableRebalancer.DiskUsageBudget budget =
        TableRebalancer.DiskUsageBudget.create(current, toTableSizeDetails(scenario._segmentSizeBytes));
    Object2IntOpenHashMap<String> segmentPartitionIdMap = new Object2IntOpenHashMap<>();

    SimResult result = new SimResult(scenario);
    result._anchor = hostedBytes(current, sizes);
    result._peak = new TreeMap<>(result._anchor);

    for (int step = 1; step <= MAX_STEPS; step++) {
      if (scenario._injectAtStep == step) {
        current.putAll(scenario._injectedCurrent);
        target.putAll(scenario._injectedTarget);
        // A segment this rebalance did not place raises the anchor of whoever is holding it, so that it neither eats
        // the headroom the rebalance needs nor lets the rebalance raise its own ceiling
        scenario._injectedCurrent.forEach((segment, instanceStateMap) -> instanceStateMap.keySet()
            .forEach(instance -> result._anchor.merge(instance, sizes.get(segment), Long::sum)));
      }

      Map<String, Long> allowed = budget.forStep(current, target).getRemainingBytes();
      Map<String, Map<String, String>> next;
      try {
        next = TableRebalancer.getNextAssignment(current, target, scenario._minAvailableReplicas,
            scenario._enableStrictReplicaGroup, true, scenario._batchSizePerServer, segmentPartitionIdMap,
            scenario._partitionIdFetcher, NO_DATA_LOSS_RISK, budget);
      } catch (Exception e) {
        result._outcome = "threw " + e;
        break;
      }
      if (next.equals(current)) {
        result._outcome = "could not make progress";
        break;
      }

      // A server assigned more bytes than it was allowed means the budget was given up for this step
      bytesAdded(current, next, sizes).forEach((server, addedBytes) -> {
        if (addedBytes > allowed.getOrDefault(server, 0L)) {
          result._serversOverBudget.add(server);
        }
      });
      recordGroupSplits(scenario, current, target, next, step, result);

      current = next;
      hostedBytes(current, sizes).forEach((server, bytes) -> result._peak.merge(server, bytes, Math::max));
      result._steps = step;
      if (current.equals(target)) {
        result._outcome = "reached the target assignment";
        break;
      }
      if (step == MAX_STEPS) {
        result._outcome = "hit the step limit";
      }
    }
    result._target = hostedBytes(target, sizes);
    return result;
  }

  private static Map<String, Long> bytesAdded(Map<String, Map<String, String>> current,
      Map<String, Map<String, String>> next, Map<String, Long> sizes) {
    Map<String, Long> added = new TreeMap<>();
    for (Map.Entry<String, Map<String, String>> entry : next.entrySet()) {
      Map<String, String> currentInstanceStateMap = current.get(entry.getKey());
      for (String instance : entry.getValue().keySet()) {
        if (currentInstanceStateMap == null || !currentInstanceStateMap.containsKey(instance)) {
          added.merge(instance, sizes.getOrDefault(entry.getKey(), 1L), Long::sum);
        }
      }
    }
    return added;
  }

  /// Under strict replica group routing every segment sharing a current and target instance pair has to be assigned
  /// the same instances, or a partition ends up split across replica groups.
  private static void recordGroupSplits(Scenario scenario, Map<String, Map<String, String>> current,
      Map<String, Map<String, String>> target, Map<String, Map<String, String>> next, int step, SimResult result) {
    if (!scenario._enableStrictReplicaGroup) {
      return;
    }
    // Keyed the way the rebalance groups segments: by their current and target instances and by partition. Batching
    // moves one partition at a time, so two partitions sharing a pair of instance sets are free to move in different
    // steps - what must never happen is segments of the same partition being assigned different instances.
    Map<List<Object>, Set<String>> groupToNextInstances = new HashMap<>();
    for (String segment : current.keySet()) {
      List<Object> group = List.of(current.get(segment).keySet(), target.get(segment).keySet(),
          scenario._partitionIdFetcher.fetch(segment));
      Set<String> nextInstances = next.get(segment).keySet();
      Set<String> alreadyAssigned = groupToNextInstances.putIfAbsent(group, nextInstances);
      if (alreadyAssigned != null && !alreadyAssigned.equals(nextInstances)) {
        result._groupSplits.add(String.format("step %d: partition %s of %s -> %s was split between %s and %s", step,
            group.get(2), group.get(0), group.get(1), alreadyAssigned, nextInstances));
      }
    }
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Randomized sweeps, for comparing two versions of the assignment logic by hand. Not tests.
  // ---------------------------------------------------------------------------------------------------------------

  public static void main(String[] args) {
    sweepServerSetShapes();
    sweepRandomGroupStructures(new Random(2027), 30_000);
  }

  /// Every combination of old and new server set sizes, overlap, replication and minimum available replicas, with and
  /// without strict replica group routing and batching, over unevenly sized segments.
  private static void sweepServerSetShapes() {
    Random random = new Random(7);
    List<SimResult> results = new ArrayList<>();
    for (int numOldServers = 3; numOldServers <= 8; numOldServers++) {
      for (int numNewServers = 3; numNewServers <= 8; numNewServers++) {
        for (int shift = 1; shift < numOldServers; shift++) {
          for (int replication = 2; replication <= Math.min(3, Math.min(numOldServers, numNewServers)); replication++) {
            for (int minAvailableReplicas = 1; minAvailableReplicas < replication; minAvailableReplicas++) {
              for (boolean strict : List.of(false, true)) {
                for (int batchSizePerServer : List.of(RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, 2)) {
                  int numSegments = 12 * replication;
                  Map<String, Map<String, String>> current =
                      roundRobin(numSegments, replication, servers(0, numOldServers), 0);
                  results.add(simulate(new Scenario(
                      String.format("old=%d new=%d shift=%d replication=%d minAvail=%d strict=%s batch=%d",
                          numOldServers, numNewServers, shift, replication, minAvailableReplicas, strict,
                          batchSizePerServer), current,
                      roundRobin(numSegments, replication, servers(shift, numNewServers), 0), minAvailableReplicas,
                      strict, batchSizePerServer, skewedSizes(current.keySet(), random), 0)));
                }
              }
            }
          }
        }
      }
    }
    report("Server set shapes", results);
  }

  /// Random current and target instance sets, group sizes, replica counts and size distributions. A rebalance the
  /// budget cannot carry out is a rare structure, so this reaches shapes the fixed scenarios do not.
  private static void sweepRandomGroupStructures(Random random, int numTrials) {
    List<SimResult> results = new ArrayList<>();
    for (int trial = 0; trial < numTrials; trial++) {
      int numServers = 3 + random.nextInt(8);
      int replication = 2 + random.nextInt(3);
      int minAvailableReplicas = 1 + random.nextInt(replication - 1);
      List<String> allServers = servers(0, numServers);
      Map<String, Map<String, String>> current = new TreeMap<>();
      Map<String, Map<String, String>> target = new TreeMap<>();
      int segmentId = 0;
      int numGroups = 2 + random.nextInt(7);
      for (int group = 0; group < numGroups; group++) {
        List<String> currentInstances = pickServers(allServers,
            minAvailableReplicas + random.nextInt(replication - minAvailableReplicas + 1), random);
        List<String> targetInstances = pickServers(allServers, replication, random);
        int numSegmentsInGroup = 1 + random.nextInt(10);
        for (int i = 0; i < numSegmentsInGroup; i++) {
          String segment = String.format("segment%04d", segmentId++);
          current.put(segment, SegmentAssignmentUtils.getInstanceStateMap(currentInstances, ONLINE));
          target.put(segment, SegmentAssignmentUtils.getInstanceStateMap(targetInstances, ONLINE));
        }
      }
      // Only run what the rebalance itself would accept
      if (TableRebalancer.getMinAvailableReplicas(current, target,
          SegmentAssignmentUtils.getSegmentsToMove(current, target), minAvailableReplicas,
          LoggerFactory.getLogger(LowDiskModeRebalanceSimulatorTest.class)) != minAvailableReplicas) {
        continue;
      }
      results.add(simulate(new Scenario("trial " + trial, current, target, minAvailableReplicas, random.nextBoolean(),
          List.of(RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, 1, 2, 20).get(random.nextInt(4)),
          skewedSizes(current.keySet(), random), 0)));
    }
    report("Random group structures", results);
  }

  /// Prints how often the budget had to be given up, how far over it went and how many steps it took, which is what
  /// there is to compare between two versions of the assignment logic.
  private static void report(String label, List<SimResult> results) {
    int gaveUpBudget = 0;
    int notReached = 0;
    int splits = 0;
    long totalSteps = 0;
    int maxSteps = 0;
    double worstAmplification = 1;
    SimResult worst = null;
    for (SimResult result : results) {
      if (!result._serversOverBudget.isEmpty()) {
        gaveUpBudget++;
        if (result.amplification() > worstAmplification) {
          worstAmplification = result.amplification();
          worst = result;
        }
      }
      notReached += result.reachedTarget() ? 0 : 1;
      splits += result._groupSplits.size();
      totalSteps += result._steps;
      maxSteps = Math.max(maxSteps, result._steps);
    }
    System.out.printf("%n%s, over %d scenarios%n", label, results.size());
    System.out.printf("  gave up the budget in    : %d%n", gaveUpBudget);
    System.out.printf("  worst amplification      : %.2fx%n", worstAmplification);
    System.out.printf("  split a group of segments: %d%n", splits);
    System.out.printf("  did not reach the target : %d%n", notReached);
    System.out.printf("  steps                    : %.1f mean, %d max%n", (double) totalSteps / results.size(),
        maxSteps);
    if (worst != null) {
      System.out.println(worst.report());
    }
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Model and helpers
  // ---------------------------------------------------------------------------------------------------------------

  private static class Scenario {
    final String _name;
    final Map<String, Map<String, String>> _currentAssignment;
    final Map<String, Map<String, String>> _targetAssignment;
    final int _minAvailableReplicas;
    final boolean _enableStrictReplicaGroup;
    final int _batchSizePerServer;
    final Map<String, Long> _segmentSizeBytes;
    /// Step at which to add segments this rebalance did not place, 0 for none
    final int _injectAtStep;
    /// Whether the budget can be held for this shape. False for the one shape known to need it given up, which is
    /// what exercises the pre-check reporting a server rather than reporting nothing.
    boolean _withinBudget = true;
    /// How the rebalance reads partition ids, which batching groups segments by. Replica group scenarios name their
    /// segments so that the real fetcher resolves them, the rest have no meaningful partition.
    TableRebalancer.PartitionIdFetcher _partitionIdFetcher = DUMMY_PARTITION_FETCHER;
    final Map<String, Map<String, String>> _injectedCurrent = new TreeMap<>();
    final Map<String, Map<String, String>> _injectedTarget = new TreeMap<>();

    Scenario(String name, Map<String, Map<String, String>> currentAssignment,
        Map<String, Map<String, String>> targetAssignment, int minAvailableReplicas, boolean enableStrictReplicaGroup,
        int batchSizePerServer, Map<String, Long> segmentSizeBytes, int injectAtStep) {
      _name = name;
      _currentAssignment = currentAssignment;
      _targetAssignment = targetAssignment;
      _minAvailableReplicas = minAvailableReplicas;
      _enableStrictReplicaGroup = enableStrictReplicaGroup;
      _batchSizePerServer = batchSizePerServer;
      _segmentSizeBytes = segmentSizeBytes;
      _injectAtStep = injectAtStep;
    }
  }

  private static class SimResult {
    final Scenario _scenario;
    final Set<String> _serversOverBudget = new TreeSet<>();
    final Set<String> _groupSplits = new TreeSet<>();
    Map<String, Long> _anchor = new TreeMap<>();
    Map<String, Long> _target = new TreeMap<>();
    Map<String, Long> _peak = new TreeMap<>();
    int _steps;
    String _outcome = "did not finish";

    SimResult(Scenario scenario) {
      _scenario = scenario;
    }

    boolean reachedTarget() {
      return "reached the target assignment".equals(_outcome);
    }

    private long bound(String server) {
      return Math.max(_anchor.getOrDefault(server, 0L), _target.getOrDefault(server, 0L));
    }

    /// The most any server held, as a multiple of what it was allowed to hold.
    double amplification() {
      double worst = 1;
      for (String server : _peak.keySet()) {
        long bound = bound(server);
        if (bound > 0) {
          worst = Math.max(worst, (double) _peak.get(server) / bound);
        }
      }
      return worst;
    }

    /// Rendered only when an assertion fails, or for the worst scenario of a sweep.
    String report() {
      StringBuilder sb = new StringBuilder("\n  ").append(_scenario._name).append(" — ").append(_outcome)
          .append(" after ").append(_steps).append(" steps\n");
      sb.append(String.format("  %-8s %10s %10s %10s %10s%n", "server", "anchor", "target", "bound", "peak"));
      for (String server : union(_anchor.keySet(), _peak.keySet())) {
        long bound = bound(server);
        long peak = _peak.getOrDefault(server, 0L);
        sb.append(String.format("  %-8s %10s %10s %10s %10s%s%n", server, mib(_anchor.getOrDefault(server, 0L)),
            mib(_target.getOrDefault(server, 0L)), mib(bound), mib(peak), peak > bound ? "   OVER" : ""));
      }
      if (!_serversOverBudget.isEmpty()) {
        sb.append("  budget given up for: ").append(_serversOverBudget).append('\n');
      }
      _groupSplits.forEach(split -> sb.append("  ").append(split).append('\n'));
      return sb.toString();
    }
  }

  private static List<String> servers(int startIndex, int count) {
    List<String> servers = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      servers.add(String.format("host%02d", startIndex + i));
    }
    return servers;
  }

  private static List<String> pickServers(List<String> allServers, int count, Random random) {
    List<String> shuffled = new ArrayList<>(allServers);
    for (int i = shuffled.size() - 1; i > 0; i--) {
      shuffled.set(i, shuffled.set(random.nextInt(i + 1), shuffled.get(i)));
    }
    List<String> picked = new ArrayList<>(shuffled.subList(0, Math.min(count, shuffled.size())));
    picked.sort(null);
    return picked;
  }

  /// Round-robins `numSegments` segments with `replication` replicas over `servers`, the shape
  /// `BalanceNumSegmentAssignmentStrategy` produces.
  private static Map<String, Map<String, String>> roundRobin(int numSegments, int replication, List<String> servers,
      int offset) {
    Map<String, Map<String, String>> assignment = new TreeMap<>();
    int cursor = offset;
    for (int i = 0; i < numSegments; i++) {
      List<String> instances = new ArrayList<>(replication);
      for (int r = 0; r < replication; r++) {
        instances.add(servers.get(cursor++ % servers.size()));
      }
      assignment.put(String.format("segment%03d", i), SegmentAssignmentUtils.getInstanceStateMap(instances, ONLINE));
    }
    return assignment;
  }

  /// Most segments small and a few an order of magnitude larger, which is what a table with mixed pushes and varying
  /// retention looks like, and what a budget counting segments rather than bytes cannot see.
  private static Map<String, Long> skewedSizes(Collection<String> segments, Random random) {
    Map<String, Long> segmentSizeBytes = new TreeMap<>();
    for (String segment : segments) {
      segmentSizeBytes.put(segment,
          random.nextInt(10) < 8 ? (8 + random.nextInt(24)) * MIB : (320 + random.nextInt(960)) * MIB);
    }
    return segmentSizeBytes;
  }

  private static Map<String, String> instanceStateMap(String... instances) {
    return SegmentAssignmentUtils.getInstanceStateMap(List.of(instances), ONLINE);
  }

  private static Map<String, Map<String, String>> deepCopy(Map<String, Map<String, String>> assignment) {
    Map<String, Map<String, String>> copy = new TreeMap<>();
    assignment.forEach((segment, instanceStateMap) -> copy.put(segment, new TreeMap<>(instanceStateMap)));
    return copy;
  }

  private static Map<String, Long> hostedBytes(Map<String, Map<String, String>> assignment, Map<String, Long> sizes) {
    Map<String, Long> bytes = new TreeMap<>();
    assignment.forEach((segment, instanceStateMap) -> instanceStateMap.keySet()
        .forEach(instance -> bytes.merge(instance, sizes.getOrDefault(segment, 1L), Long::sum)));
    return bytes;
  }

  /// The sizes as [TableSizeReader] reports them, so the real extraction path is exercised. `null` where the scenario
  /// supplies none, which makes every segment weigh one byte.
  private static TableSizeReader.TableSubTypeSizeDetails toTableSizeDetails(Map<String, Long> segmentSizeBytes) {
    if (segmentSizeBytes.isEmpty()) {
      return null;
    }
    TableSizeReader.TableSubTypeSizeDetails tableSizeDetails = new TableSizeReader.TableSubTypeSizeDetails();
    segmentSizeBytes.forEach((segment, sizeBytes) -> {
      TableSizeReader.SegmentSizeDetails segmentSizeDetails = new TableSizeReader.SegmentSizeDetails();
      segmentSizeDetails._maxReportedSizePerReplicaInBytes = sizeBytes;
      tableSizeDetails._segments.put(segment, segmentSizeDetails);
    });
    return tableSizeDetails;
  }

  private static Set<String> union(Set<String> a, Set<String> b) {
    Set<String> union = new TreeSet<>(a);
    union.addAll(b);
    return union;
  }

  private static String mib(long bytes) {
    return bytes >= MIB ? (bytes / MIB) + "M" : Long.toString(bytes);
  }
}
