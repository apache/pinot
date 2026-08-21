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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.pinot.common.restlet.resources.RebalanceConfig;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentUtils;
import org.apache.pinot.controller.util.TableSizeReader;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Step-by-step simulator for `lowDiskMode` rebalance, used to demonstrate that a server can still see a **net
/// increase** in the number of hosted segments (i.e. disk usage) while the rebalance is in flight.
///
/// The simulator drives the real [TableRebalancer#getNextAssignment] in a loop, exactly the way
/// [TableRebalancer] does, and after each step accounts, per server:
///  * `before`  - segments hosted before the step
///  * `+adds`   - segments the step assigns to the server (a download)
///  * `-drops`  - segments the step removes from the server (a deletion)
///  * `after`   - segments hosted once the step converges
///
/// `lowDiskMode` is supposed to guarantee that no server ever needs more disk than it started with (or than the
/// target asks it to hold), so the invariant checked here is:
///
///     usage(server, any point in time) <= max(initialUsage(server), targetUsage(server))
///
/// Two peaks are tracked against that bound:
///  * `maxAfter`     - the steady-state peak, i.e. the assignment the controller actually publishes. Exceeding the
///                     bound here is a *persistent* over-allocation that lasts for at least one full step.
///  * `maxInStep`    - the worst-case peak within a step (`before + adds`). `getNextSingleSegmentAssignment` notes
///                     that "even if segment addition and drop happen in the same step, there is no guarantee that
///                     server process the segment drop before the segment addition", so this is reachable.
///
/// All segments are treated as equal-sized, so "segment count" is a proxy for disk.
public class LowDiskModeRebalanceSimulatorTest {
  private static final String ONLINE = "ONLINE";
  private static final int MAX_STEPS = 50;
  private static final TableRebalancer.PartitionIdFetcher DUMMY_PARTITION_FETCHER = segmentName -> 0;
  private static final TableRebalancer.DataLossRiskAssessor NO_DATA_LOSS_RISK =
      new TableRebalancer.NoOpRiskAssessor();

  // ---------------------------------------------------------------------------------------------------------------
  // Scenarios
  // ---------------------------------------------------------------------------------------------------------------

  /// The minimal toy example: 3 servers -> 3 servers with 2 servers in the overlap, replication 2,
  /// `minAvailableReplicas = 1`. Every overlap server both offloads and onloads segments.
  ///
  /// `hostC` starts with 4 segments and ends with 4 segments, but transiently holds 6.
  @Test
  public void testOverlappingServerSetsRotation() {
    List<String> oldServers = List.of("hostA", "hostB", "hostC");
    List<String> newServers = List.of("hostB", "hostC", "hostD");
    Scenario scenario =
        new Scenario("3 servers -> 3 servers, 2 in overlap (replication 2, minAvailableReplicas 1)",
            roundRobin(6, 2, oldServers, 0), roundRobin(6, 2, newServers, 0), 1, false,
            RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER);
    SimResult result = simulate(scenario);
    System.out.println(result.report());
    assertFalse(result.hasSteadyStateViolation(),
        "lowDiskMode must not let any server hold more than max(initial, target) segments");
    assertFalse(result.hasInStepViolation(),
        "lowDiskMode must not let any server download while it still has segments to drop");
  }

  /// Same shape, with strict replica-group enabled, to show the problem is not specific to the non-strict path.
  @Test
  public void testOverlappingServerSetsRotationStrictReplicaGroup() {
    List<String> oldServers = List.of("hostA", "hostB", "hostC");
    List<String> newServers = List.of("hostB", "hostC", "hostD");
    Scenario scenario = new Scenario("Same, strictReplicaGroup = true", roundRobin(6, 2, oldServers, 0),
        roundRobin(6, 2, newServers, 0), 1, true, RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER);
    SimResult result = simulate(scenario);
    System.out.println(result.report());
    assertFalse(result.hasSteadyStateViolation(),
        "lowDiskMode must not let any server hold more than max(initial, target) segments");
    assertFalse(result.hasInStepViolation(),
        "lowDiskMode must not let any server download while it still has segments to drop");
    assertEquals(result._groupSplits, Set.of());
  }

  /// The disk budget is charged as segments are assigned, so charging it per segment could fit the first segments of a
  /// partition and not the rest, splitting the partition across replica groups. Sweeps the grid with strict replica
  /// group routing and skewed segment sizes - which is what makes a group only partially fit - and checks that every
  /// group of segments sharing the same current and target instances is always moved as a whole.
  @Test
  public void testStrictReplicaGroupMovesGroupsTogether() {
    Random random = new Random(11);
    List<SimResult> violations = new ArrayList<>();
    List<SimResult> all = new ArrayList<>();
    List<String> splits = new ArrayList<>();
    for (int numOldServers = 3; numOldServers <= 8; numOldServers++) {
      for (int numNewServers = 3; numNewServers <= 8; numNewServers++) {
        for (int shift = 1; shift < numOldServers; shift++) {
          for (int replication = 2; replication <= Math.min(3, Math.min(numOldServers, numNewServers));
              replication++) {
            int numSegments = 12 * replication;
            List<String> oldServers = servers(0, numOldServers);
            List<String> newServers = servers(shift, numNewServers);
            Map<String, Map<String, String>> current = roundRobin(numSegments, replication, oldServers, 0);
            String name = String.format("strict old=%d new=%d overlap=%d replication=%d segments=%d", numOldServers,
                numNewServers, overlap(oldServers, newServers), replication, numSegments);
            SimResult result = simulate(new Scenario(name, current,
                roundRobin(numSegments, replication, newServers, 0), 1, true,
                RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, skewedSizes(current.keySet(), random)));
            all.add(result);
            if (result.hasSteadyStateViolation()) {
              violations.add(result);
            }
            result._groupSplits.forEach(split -> splits.add(name + " | " + split));
          }
        }
      }
    }
    violations.sort((a, b) -> Double.compare(b.worstAmplification(), a.worstAmplification()));
    System.out.println();
    System.out.println(summarize("Strict replica group sweep with skewed segment sizes", all, violations));
    if (!splits.isEmpty()) {
      System.out.printf("%d strict replica group splits, first 5:%n", splits.size());
      splits.subList(0, Math.min(5, splits.size())).forEach(split -> System.out.println("  " + split));
    }
    assertEquals(splits, List.of(), "strict replica group requires every group of segments to be moved together");
  }

  /// A larger, more realistic rotation: 6 servers -> 6 servers with 3 in the overlap, replication 2, 24 segments.
  /// Shows the over-allocation scales with the table rather than being a rounding artifact - the worst server ends
  /// up holding double what it started with.
  ///
  /// Note that not every overlapping rotation trips this; whether it does depends on how the round-robin lands.
  /// [#testSweepOverlappingServerSets] enumerates the space.
  @Test
  public void testLargerRotationOverlap() {
    List<String> oldServers = List.of("host1", "host2", "host3", "host4", "host5", "host6");
    List<String> newServers = List.of("host4", "host5", "host6", "host7", "host8", "host9");
    Scenario scenario =
        new Scenario("6 servers -> 6 servers, 3 in overlap (replication 2, minAvailableReplicas 1)",
            roundRobin(24, 2, oldServers, 0), roundRobin(24, 2, newServers, 0), 1, false,
            RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER);
    SimResult result = simulate(scenario);
    System.out.println(result.report());
    assertFalse(result.hasSteadyStateViolation(),
        "lowDiskMode must not let any server hold more than max(initial, target) segments");
    assertFalse(result.hasInStepViolation(),
        "lowDiskMode must not let any server download while it still has segments to drop");
  }

  /// Control case: a pure scale-out where no server in the old set gains anything. `lowDiskMode` holds here, which
  /// is why the existing coverage in [TableRebalancerTest] does not catch the problem above.
  @Test
  public void testPureScaleOutIsClean() {
    List<String> oldServers = List.of("host1", "host2", "host3", "host4");
    List<String> newServers = List.of("host1", "host2", "host3", "host4", "host5", "host6", "host7", "host8");
    Scenario scenario = new Scenario("4 servers -> 8 servers, pure scale-out (control case)",
        roundRobin(24, 2, oldServers, 0), roundRobin(24, 2, newServers, 0), 1, false,
        RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER);
    SimResult result = simulate(scenario);
    System.out.println(result.report());
    assertFalse(result.hasSteadyStateViolation(), "pure scale-out should not over-allocate any old server");
  }

  /// The shape that over-allocated the most under a segment count based budget: two servers each ended up holding
  /// 23 segments against a bound of 18, because the budget was relaxed to break a stall that was not real - both
  /// servers had free space at the time. Under the byte budget it stays within the bound and is never relaxed.
  @Test
  public void testPreviouslyWorstCaseStaysWithinBound() {
    Scenario scenario =
        new Scenario("8 servers -> 6 servers, 6 in overlap (replication 3, minAvailableReplicas 2), 36 segments",
            roundRobin(36, 3, servers(0, 8), 0), roundRobin(36, 3, servers(2, 6), 0), 2, false,
            RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER);
    SimResult result = simulate(scenario);
    System.out.println(result.report());
    assertFalse(result.hasSteadyStateViolation(), "no server may hold more than max(initial, target)");
    assertFalse(result.hasInStepViolation(), "no server may exceed max(initial, target) mid-step either");
    assertTrue(result.relaxedSteps().isEmpty(), "the budget should not have to be relaxed for this shape");
  }

  /// Sweeps a grid of overlapping old/new server sets and reports the worst amplification found, so the blast
  /// radius of the edge case can be sized.
  @Test
  public void testSweepOverlappingServerSets() {
    List<SimResult> violations = new ArrayList<>();
    List<SimResult> all = new ArrayList<>();
    for (int numOldServers = 3; numOldServers <= 8; numOldServers++) {
      for (int numNewServers = 3; numNewServers <= 8; numNewServers++) {
        for (int shift = 1; shift < numOldServers; shift++) {
          for (int replication = 1; replication <= Math.min(3, Math.min(numOldServers, numNewServers));
              replication++) {
            for (int minAvailableReplicas = 1; minAvailableReplicas < replication + 1; minAvailableReplicas++) {
              if (minAvailableReplicas >= replication && replication > 1) {
                // A rebalance cannot make progress when minAvailableReplicas == replication
                continue;
              }
              int numSegments = 12 * replication;
              List<String> oldServers = servers(0, numOldServers);
              List<String> newServers = servers(shift, numNewServers);
              String name = String.format(
                  "old=%d new=%d overlap=%d replication=%d minAvailableReplicas=%d segments=%d", numOldServers,
                  numNewServers, overlap(oldServers, newServers), replication, minAvailableReplicas, numSegments);
              Scenario scenario = new Scenario(name, roundRobin(numSegments, replication, oldServers, 0),
                  roundRobin(numSegments, replication, newServers, 0), minAvailableReplicas, false,
                  RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER);
              SimResult result = simulate(scenario);
              all.add(result);
              if (result.hasSteadyStateViolation()) {
                violations.add(result);
              }
            }
          }
        }
      }
    }
    violations.sort((a, b) -> Double.compare(b.worstAmplification(), a.worstAmplification()));
    System.out.println();
    System.out.println(summarize("Sweep", all, violations));
    System.out.println("Worst 10 by amplification (peak hosted / bound):");
    for (SimResult result : violations.subList(0, Math.min(10, violations.size()))) {
      System.out.printf("  %-88s worst server %s: bound %s, peak %s (%.2fx, +%s)%n", result._scenario._name,
          result.worstServer(), formatUnits(result.bound(result.worstServer()), result._scenario),
          formatUnits(result._maxAfter.get(result.worstServer()), result._scenario), result.worstAmplification(),
          formatUnits(result.worstExcess(), result._scenario));
    }
    System.out.println();
  }

  /// Aggregate metrics for a batch of scenarios. Counting violating scenarios alone is misleading - a one-segment
  /// overshoot is not the same problem as a 2x overshoot - so report the magnitude and the convergence cost too.
  private static String summarize(String label, List<SimResult> all, List<SimResult> violations) {
    long totalExcess = 0;
    long worstExcess = 0;
    double worstAmplification = 1.0;
    int totalSteps = 0;
    int maxSteps = 0;
    int numStuck = 0;
    int numRelaxed = 0;
    int numViolatingWithoutRelax = 0;
    for (SimResult result : all) {
      if (!result.relaxedSteps().isEmpty()) {
        numRelaxed++;
      } else if (result.hasSteadyStateViolation()) {
        numViolatingWithoutRelax++;
      }
      totalExcess += result.worstExcess();
      worstExcess = Math.max(worstExcess, result.worstExcess());
      worstAmplification = Math.max(worstAmplification, result.worstAmplification());
      totalSteps += result._steps.size();
      maxSteps = Math.max(maxSteps, result._steps.size());
      if (result._terminated.startsWith("STUCK") || result._terminated.startsWith("hit MAX_STEPS")) {
        numStuck++;
      }
    }
    return String.format("%s over %d scenarios:%n"
            + "  violating scenarios : %d (%.1f%%)%n"
            + "  worst amplification : %.2fx%n"
            + "  worst excess        : %d units over the bound%n"
            + "  total excess        : %d units summed over all scenarios%n"
            + "  steps               : %.1f mean, %d max%n"
            + "  did not converge    : %d%n"
            + "  budget relaxed in    : %d (no progress was possible within the budget)%n"
            + "  violating, no relax  : %d  <-- unexplained by the relaxation%n", label, all.size(),
        violations.size(), 100.0 * violations.size() / all.size(), worstAmplification, worstExcess, totalExcess,
        (double) totalSteps / all.size(), maxSteps, numStuck, numRelaxed, numViolatingWithoutRelax);
  }

  /// Same grid as [#testSweepOverlappingServerSets], but with a skewed per-segment size distribution, so the budget
  /// is bounding bytes rather than segment counts. This is the case a count based budget cannot get right: it lets a
  /// server take on large segments because it dropped small ones.
  @Test
  public void testSweepWithSkewedSegmentSizes() {
    List<SimResult> violations = new ArrayList<>();
    List<SimResult> all = new ArrayList<>();
    Random random = new Random(7);
    for (int numOldServers = 3; numOldServers <= 8; numOldServers++) {
      for (int numNewServers = 3; numNewServers <= 8; numNewServers++) {
        for (int shift = 1; shift < numOldServers; shift++) {
          for (int replication = 1; replication <= Math.min(3, Math.min(numOldServers, numNewServers));
              replication++) {
            for (int minAvailableReplicas = 1; minAvailableReplicas < replication + 1; minAvailableReplicas++) {
              if (minAvailableReplicas >= replication && replication > 1) {
                continue;
              }
              int numSegments = 12 * replication;
              List<String> oldServers = servers(0, numOldServers);
              List<String> newServers = servers(shift, numNewServers);
              Map<String, Map<String, String>> current = roundRobin(numSegments, replication, oldServers, 0);
              String name = String.format(
                  "old=%d new=%d overlap=%d replication=%d minAvailableReplicas=%d segments=%d", numOldServers,
                  numNewServers, overlap(oldServers, newServers), replication, minAvailableReplicas, numSegments);
              SimResult result = simulate(new Scenario(name, current,
                  roundRobin(numSegments, replication, newServers, 0), minAvailableReplicas, false,
                  RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, skewedSizes(current.keySet(), random)));
              all.add(result);
              if (result.hasSteadyStateViolation()) {
                violations.add(result);
              }
            }
          }
        }
      }
    }
    violations.sort((a, b) -> Double.compare(b.worstAmplification(), a.worstAmplification()));
    System.out.println();
    System.out.println(summarize("Sweep with skewed segment sizes", all, violations));
    for (SimResult result : violations.subList(0, Math.min(10, violations.size()))) {
      System.out.printf("  %-88s worst server %s: bound %s, peak %s (%.2fx, +%s)%n", result._scenario._name,
          result.worstServer(), formatUnits(result.bound(result.worstServer()), result._scenario),
          formatUnits(result._maxAfter.get(result.worstServer()), result._scenario), result.worstAmplification(),
          formatUnits(result.worstExcess(), result._scenario));
    }
    System.out.println();
  }

  /// Randomized search over non-uniform current assignments (the state a table is in after an interrupted
  /// rebalance, a replication change, or realtime segments that were created against the new instance
  /// partitions) looking for the largest amplification.
  @Test
  public void testRandomSearch() {
    Random random = new Random(42);
    List<SimResult> violations = new ArrayList<>();
    List<SimResult> all = new ArrayList<>();
    for (int trial = 0; trial < 400; trial++) {
      int numOldServers = 3 + random.nextInt(6);
      int numNewServers = 3 + random.nextInt(6);
      int shift = 1 + random.nextInt(numOldServers);
      int replication = 1 + random.nextInt(3);
      int minAvailableReplicas = 1 + random.nextInt(Math.max(1, replication - 1));
      int numSegments = 6 + random.nextInt(30);
      List<String> oldServers = servers(0, numOldServers);
      List<String> newServers = servers(shift, numNewServers);
      Map<String, Map<String, String>> current =
          roundRobin(numSegments, Math.min(replication, numOldServers), oldServers, random.nextInt(numOldServers));
      Map<String, Map<String, String>> target =
          roundRobin(numSegments, Math.min(replication, numNewServers), newServers, random.nextInt(numNewServers));
      String name = String.format("trial %d: old=%d new=%d overlap=%d replication=%d minAvail=%d segments=%d", trial,
          numOldServers, numNewServers, overlap(oldServers, newServers), replication, minAvailableReplicas,
          numSegments);
      SimResult result = simulate(new Scenario(name, current, target, minAvailableReplicas, false,
          RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER));
      all.add(result);
      if (result.hasSteadyStateViolation()) {
        violations.add(result);
      }
    }
    violations.sort((a, b) -> Double.compare(b.worstAmplification(), a.worstAmplification()));
    System.out.println();
    System.out.println(summarize("Random search", all, violations));
    if (!violations.isEmpty()) {
      System.out.println("Worst offender:");
      System.out.println(violations.get(0).report());
    }
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Simulator
  // ---------------------------------------------------------------------------------------------------------------

  private static SimResult simulate(Scenario scenario) {
    SimResult result = new SimResult(scenario);
    Map<String, Map<String, String>> current = deepCopy(scenario._currentAssignment);
    Object2IntOpenHashMap<String> segmentPartitionIdMap = new Object2IntOpenHashMap<>();

    // The budget the controller uses, anchored to the assignment the rebalance starts from
    TableRebalancer.DiskUsageBudget diskUsageBudget = TableRebalancer.DiskUsageBudget.create(current,
        scenario._segmentSizeBytes.isEmpty() ? null : toTableSizeDetails(scenario._segmentSizeBytes));

    result._initial = hostedBytes(current, scenario);
    result._target = hostedBytes(scenario._targetAssignment, scenario);
    result._maxAfter = new TreeMap<>(result._initial);
    result._maxInStep = new TreeMap<>(result._initial);
    for (String server : result._target.keySet()) {
      result._maxAfter.putIfAbsent(server, 0L);
      result._maxInStep.putIfAbsent(server, 0L);
    }

    for (int step = 1; step <= MAX_STEPS; step++) {
      Map<String, Map<String, String>> next;
      try {
        next = TableRebalancer.getNextAssignment(current, scenario._targetAssignment, scenario._minAvailableReplicas,
            scenario._enableStrictReplicaGroup, true /* lowDiskMode */, scenario._batchSizePerServer,
            segmentPartitionIdMap, DUMMY_PARTITION_FETCHER, NO_DATA_LOSS_RISK, diskUsageBudget);
      } catch (Exception e) {
        result._terminated = "step " + step + " threw " + e;
        break;
      }
      if (next.equals(current)) {
        result._terminated = next.equals(scenario._targetAssignment) ? "converged" : "STUCK (no progress possible)";
        break;
      }

      Step stepInfo = new Step(step);
      Map<String, Long> before = hostedBytes(current, scenario);
      Map<String, Long> after = hostedBytes(next, scenario);
      for (String segment : current.keySet()) {
        Set<String> currentInstances = current.get(segment).keySet();
        Set<String> nextInstances = next.get(segment).keySet();
        Set<String> targetInstances = scenario._targetAssignment.get(segment).keySet();
        long sizeBytes = scenario.sizeOf(segment);
        for (String instance : nextInstances) {
          if (!currentInstances.contains(instance)) {
            stepInfo._adds.merge(instance, sizeBytes, Long::sum);
            stepInfo._addedSegments.computeIfAbsent(instance, k -> new TreeSet<>()).add(segment);
          } else if (!targetInstances.contains(instance)) {
            // The instance is not in the target for this segment, yet the step keeps the segment on it. This only
            // happens because dropping it would break the minAvailableReplicas requirement, i.e. the replacement
            // replica has not been created yet. The disk it occupies is not accounted for anywhere.
            stepInfo._heldForAvailability.computeIfAbsent(instance, k -> new TreeSet<>()).add(segment);
          }
        }
        for (String instance : currentInstances) {
          if (!nextInstances.contains(instance)) {
            stepInfo._drops.merge(instance, sizeBytes, Long::sum);
          }
        }
      }
      // Ask the real budget what each server was allowed to take on at the start of this step. A server that was
      // assigned more than that means TableRebalancer could not make progress within the budget and relaxed it.
      Map<String, Long> allowedBytes =
          diskUsageBudget.forStep(current, scenario._targetAssignment).getRemainingBytes();
      stepInfo._adds.forEach((server, addedBytes) -> {
        if (addedBytes > allowedBytes.getOrDefault(server, 0L)) {
          stepInfo._deferralRelaxedFor.add(server);
        }
      });
      for (String server : union(result._maxAfter.keySet(), after.keySet())) {
        long beforeBytes = before.getOrDefault(server, 0L);
        long afterBytes = after.getOrDefault(server, 0L);
        long inStepPeak = beforeBytes + stepInfo._adds.getOrDefault(server, 0L);
        stepInfo._before.put(server, beforeBytes);
        stepInfo._after.put(server, afterBytes);
        result._maxAfter.merge(server, afterBytes, Math::max);
        result._maxInStep.merge(server, inStepPeak, Math::max);
      }
      // Strict replica group routing requires every segment that shares the same current and target instances to get
      // the same next assignment, otherwise a partition ends up split across replica groups.
      if (scenario._enableStrictReplicaGroup) {
        Map<List<Set<String>>, Set<String>> groupToNextInstances = new HashMap<>();
        for (String segment : current.keySet()) {
          List<Set<String>> group =
              List.of(current.get(segment).keySet(), scenario._targetAssignment.get(segment).keySet());
          Set<String> nextInstances = next.get(segment).keySet();
          Set<String> alreadyAssigned = groupToNextInstances.putIfAbsent(group, nextInstances);
          if (alreadyAssigned != null && !alreadyAssigned.equals(nextInstances)) {
            result._groupSplits.add(
                String.format("step %d: segments with current %s and target %s were split between %s and %s",
                    stepInfo._index, group.get(0), group.get(1), alreadyAssigned, nextInstances));
          }
        }
      }
      result._steps.add(stepInfo);

      current = next;
      if (current.equals(scenario._targetAssignment)) {
        result._terminated = "converged";
        break;
      }
      if (step == MAX_STEPS) {
        result._terminated = "hit MAX_STEPS";
      }
    }
    return result;
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Assignment builders
  // ---------------------------------------------------------------------------------------------------------------

  private static List<String> servers(int startIndex, int count) {
    List<String> servers = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      servers.add(String.format("host%02d", startIndex + i));
    }
    return servers;
  }

  private static int overlap(List<String> oldServers, List<String> newServers) {
    Set<String> intersection = new TreeSet<>(oldServers);
    intersection.retainAll(newServers);
    return intersection.size();
  }

  /// Round-robins `numSegments` segments with `replication` replicas over `servers`, starting at `offset`. This is
  /// the same shape `BalanceNumSegmentAssignmentStrategy` produces.
  private static Map<String, Map<String, String>> roundRobin(int numSegments, int replication, List<String> servers,
      int offset) {
    Map<String, Map<String, String>> assignment = new TreeMap<>();
    int numServers = servers.size();
    int cursor = offset;
    for (int i = 0; i < numSegments; i++) {
      List<String> instances = new ArrayList<>(replication);
      for (int r = 0; r < replication; r++) {
        instances.add(servers.get(cursor % numServers));
        cursor++;
      }
      assignment.put(String.format("segment%03d", i), SegmentAssignmentUtils.getInstanceStateMap(instances, ONLINE));
    }
    return assignment;
  }

  private static Map<String, Map<String, String>> deepCopy(Map<String, Map<String, String>> assignment) {
    Map<String, Map<String, String>> copy = new TreeMap<>();
    assignment.forEach((segment, instanceStateMap) -> copy.put(segment, new TreeMap<>(instanceStateMap)));
    return copy;
  }

  /// Bytes each server hosts under `assignment`. With no per-segment sizes every segment weighs one, so this is the
  /// hosted segment count and the accounting is identical to a count based budget.
  private static Map<String, Long> hostedBytes(Map<String, Map<String, String>> assignment, Scenario scenario) {
    Map<String, Long> bytes = new TreeMap<>();
    for (Map.Entry<String, Map<String, String>> entry : assignment.entrySet()) {
      long sizeBytes = scenario.sizeOf(entry.getKey());
      for (String instance : entry.getValue().keySet()) {
        bytes.merge(instance, sizeBytes, Long::sum);
      }
    }
    return bytes;
  }

  /// Renders the accounting unit: raw segment counts when no sizes were supplied, MiB otherwise.
  private static String formatUnits(long units, Scenario scenario) {
    return scenario._segmentSizeBytes.isEmpty() ? Long.toString(units)
        : (units / (1024 * 1024)) + "M";
  }

  /// The per-segment sizes as [TableSizeReader] would report them, so the real extraction path is exercised.
  private static TableSizeReader.TableSubTypeSizeDetails toTableSizeDetails(Map<String, Long> segmentSizeBytes) {
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

  // ---------------------------------------------------------------------------------------------------------------
  // Model
  // ---------------------------------------------------------------------------------------------------------------

  private static class Scenario {
    final String _name;
    final Map<String, Map<String, String>> _currentAssignment;
    final Map<String, Map<String, String>> _targetAssignment;
    final int _minAvailableReplicas;
    final boolean _enableStrictReplicaGroup;
    final int _batchSizePerServer;
    /// Per-segment size in bytes. Empty means every segment counts as one byte, i.e. the budget degenerates to
    /// bounding segment counts.
    final Map<String, Long> _segmentSizeBytes;

    Scenario(String name, Map<String, Map<String, String>> currentAssignment,
        Map<String, Map<String, String>> targetAssignment, int minAvailableReplicas,
        boolean enableStrictReplicaGroup, int batchSizePerServer) {
      this(name, currentAssignment, targetAssignment, minAvailableReplicas, enableStrictReplicaGroup,
          batchSizePerServer, Map.of());
    }

    Scenario(String name, Map<String, Map<String, String>> currentAssignment,
        Map<String, Map<String, String>> targetAssignment, int minAvailableReplicas,
        boolean enableStrictReplicaGroup, int batchSizePerServer, Map<String, Long> segmentSizeBytes) {
      _name = name;
      _currentAssignment = currentAssignment;
      _targetAssignment = targetAssignment;
      _minAvailableReplicas = minAvailableReplicas;
      _enableStrictReplicaGroup = enableStrictReplicaGroup;
      _batchSizePerServer = batchSizePerServer;
      _segmentSizeBytes = segmentSizeBytes;
    }

    long sizeOf(String segment) {
      return _segmentSizeBytes.getOrDefault(segment, 1L);
    }
  }

  /// Builds a skewed size distribution over `segments`: most segments are small and a few are an order of magnitude
  /// larger, which is what a table with mixed offline pushes and varying retention actually looks like. Segment count
  /// based budgeting cannot see this skew, which is the whole reason for going byte based.
  private static Map<String, Long> skewedSizes(Collection<String> segments, Random random) {
    Map<String, Long> segmentSizeBytes = new TreeMap<>();
    for (String segment : segments) {
      // 80% small, 20% between 10x and 40x larger
      long sizeBytes = random.nextInt(10) < 8
          ? 8L * 1024 * 1024 + random.nextInt(24) * 1024L * 1024
          : 320L * 1024 * 1024 + random.nextInt(960) * 1024L * 1024;
      segmentSizeBytes.put(segment, sizeBytes);
    }
    return segmentSizeBytes;
  }

  private static class Step {
    final int _index;
    final Map<String, Long> _before = new TreeMap<>();
    final Map<String, Long> _after = new TreeMap<>();
    final Map<String, Long> _adds = new HashMap<>();
    final Map<String, Long> _drops = new HashMap<>();
    /// Server -> segments this step newly assigns to it.
    final Map<String, Set<String>> _addedSegments = new HashMap<>();
    /// Server -> segments this step keeps on it even though the target does not place them there, because dropping
    /// them would break the minAvailableReplicas requirement.
    final Map<String, Set<String>> _heldForAvailability = new HashMap<>();
    /// Servers that had segments to drop at the start of this step and were still assigned new segments, i.e. the
    /// servers for which TableRebalancer gave up on the deferral in order to make progress.
    final Set<String> _deferralRelaxedFor = new TreeSet<>();

    Step(int index) {
      _index = index;
    }
  }

  private static class SimResult {
    final Scenario _scenario;
    final List<Step> _steps = new ArrayList<>();
    Map<String, Long> _initial = new TreeMap<>();
    Map<String, Long> _target = new TreeMap<>();
    Map<String, Long> _maxAfter = new TreeMap<>();
    Map<String, Long> _maxInStep = new TreeMap<>();
    /// Strict replica group violations: groups of segments that were not moved together.
    final Set<String> _groupSplits = new TreeSet<>();
    String _terminated = "did not terminate";

    SimResult(Scenario scenario) {
      _scenario = scenario;
    }

    long bound(String server) {
      return Math.max(_initial.getOrDefault(server, 0L), _target.getOrDefault(server, 0L));
    }

    boolean hasSteadyStateViolation() {
      return _maxAfter.keySet().stream().anyMatch(server -> _maxAfter.get(server) > bound(server));
    }

    boolean hasInStepViolation() {
      return _maxInStep.keySet().stream().anyMatch(server -> _maxInStep.get(server) > bound(server));
    }

    String worstServer() {
      return _maxAfter.keySet().stream()
          .max((a, b) -> Double.compare(amplification(a), amplification(b)))
          .orElseThrow();
    }

    double amplification(String server) {
      long bound = bound(server);
      return bound == 0 ? _maxAfter.get(server) : (double) _maxAfter.get(server) / bound;
    }

    double worstAmplification() {
      return amplification(worstServer());
    }

    /// Steps in which TableRebalancer gave up on deferring the adds in order to make progress.
    List<Step> relaxedSteps() {
      return _steps.stream().filter(step -> !step._deferralRelaxedFor.isEmpty()).toList();
    }

    /// The most any single server holds above its bound, in accounting units.
    long worstExcess() {
      return _maxAfter.keySet().stream().mapToLong(server -> _maxAfter.get(server) - bound(server)).max().orElse(0L);
    }

    String report() {
      StringBuilder sb = new StringBuilder();
      sb.append("\n").append("=".repeat(118)).append('\n');
      sb.append(_scenario._name).append('\n');
      sb.append("=".repeat(118)).append('\n');
      sb.append(String.format("steps: %d, outcome: %s%n%n", _steps.size(), _terminated));

      Set<String> servers = union(_maxAfter.keySet(), _initial.keySet());
      sb.append("Per-step hosted segment counts (before -> after, +downloads/-deletes):\n");
      sb.append(String.format("  %-6s", "server"));
      for (Step step : _steps) {
        sb.append(String.format("%-18s", "step " + step._index));
      }
      sb.append('\n');
      for (String server : servers) {
        sb.append(String.format("  %-6s", server));
        for (Step step : _steps) {
          long adds = step._adds.getOrDefault(server, 0L);
          long drops = step._drops.getOrDefault(server, 0L);
          sb.append(String.format("%-22s", String.format("%s->%s %s%s",
              formatUnits(step._before.getOrDefault(server, 0L), _scenario),
              formatUnits(step._after.getOrDefault(server, 0L), _scenario),
              adds > 0 ? "+" + formatUnits(adds, _scenario) : "",
              drops > 0 ? "-" + formatUnits(drops, _scenario) : "")));
        }
        sb.append('\n');
      }

      sb.append("\nPer-server summary:\n");
      sb.append(String.format("  %-8s %9s %9s %9s %10s %10s   %s%n", "server", "initial", "target", "bound",
          "maxAfter", "maxInStep", "verdict"));
      for (String server : servers) {
        long bound = bound(server);
        long maxAfter = _maxAfter.getOrDefault(server, 0L);
        long maxInStep = _maxInStep.getOrDefault(server, 0L);
        String verdict;
        if (maxAfter > bound) {
          verdict = String.format("NET INCREASE: holds %s vs bound %s (+%s, %.0f%% over)",
              formatUnits(maxAfter, _scenario), formatUnits(bound, _scenario),
              formatUnits(maxAfter - bound, _scenario), 100.0 * (maxAfter - bound) / Math.max(1, bound));
        } else if (maxInStep > bound) {
          verdict = String.format("TRANSIENT: may hold %s vs bound %s within a step",
              formatUnits(maxInStep, _scenario), formatUnits(bound, _scenario));
        } else {
          verdict = "ok";
        }
        sb.append(String.format("  %-8s %9s %9s %9s %10s %10s   %s%n", server,
            formatUnits(_initial.getOrDefault(server, 0L), _scenario),
            formatUnits(_target.getOrDefault(server, 0L), _scenario), formatUnits(bound, _scenario),
            formatUnits(maxAfter, _scenario), formatUnits(maxInStep, _scenario), verdict));
      }
      sb.append(String.format("%nsteady-state violation: %s, within-step violation: %s%n", hasSteadyStateViolation(),
          hasInStepViolation()));
      for (Step step : relaxedSteps()) {
        sb.append(String.format("  step %d: budget relaxed for %s (no progress was possible within it)%n",
            step._index, step._deferralRelaxedFor));
        // Was the stall physically necessary, or only an artifact of blocking every server that has a pending drop?
        // Show how many free slots each server had under its own bound at the start of the stalled step.
        long totalHeadroom = 0;
        StringBuilder headroomDetail = new StringBuilder();
        for (String server : _maxAfter.keySet()) {
          long headroom = bound(server) - step._before.getOrDefault(server, 0L);
          if (headroom > 0) {
            totalHeadroom += headroom;
            headroomDetail.append(String.format(" %s=%s", server, formatUnits(headroom, _scenario)));
          }
        }
        sb.append(String.format("    free space under each server's own bound at that point:%s (total %s)%n",
            headroomDetail, formatUnits(totalHeadroom, _scenario)));
      }
      for (String groupSplit : _groupSplits) {
        sb.append("  STRICT REPLICA GROUP SPLIT: ").append(groupSplit).append('\n');
      }
      if (hasSteadyStateViolation()) {
        sb.append(explainWorstServer());
      }
      return sb.toString();
    }

    /// Explains the peak for the worst server: the segments it is simultaneously downloading, and the segments it is
    /// still holding only to satisfy minAvailableReplicas.
    String explainWorstServer() {
      String server = worstServer();
      Step peakStep = null;
      for (Step step : _steps) {
        if (peakStep == null || step._after.getOrDefault(server, 0L) > peakStep._after.getOrDefault(server, 0L)) {
          peakStep = step;
        }
      }
      if (peakStep == null) {
        return "";
      }
      Set<String> added = peakStep._addedSegments.getOrDefault(server, new TreeSet<>());
      Set<String> held = peakStep._heldForAvailability.getOrDefault(server, new TreeSet<>());
      StringBuilder sb = new StringBuilder();
      sb.append(String.format("%nWhy %s peaks at %s in step %d:%n", server,
          formatUnits(peakStep._after.get(server), _scenario), peakStep._index));
      sb.append(String.format("  * %d segment(s) downloaded onto %s, because for each of them every current replica "
              + "is already in the target (nothing left to drop), so lowDiskMode allows the add: %s%n", added.size(),
          server, abbreviate(added)));
      sb.append(String.format("  * %d segment(s) still pinned on %s even though the target does not place them "
              + "there, because dropping them would break minAvailableReplicas=%d: %s%n", held.size(), server,
          _scenario._minAvailableReplicas, abbreviate(held)));
      sb.append("  lowDiskMode only sequences drop-before-add *per segment*. Nothing sequences it per server, so a\n"
          + "  server in the overlap of the old and new server sets pays for both sets at once.\n");
      return sb.toString();
    }

    private static String abbreviate(Set<String> segments) {
      if (segments.size() <= 6) {
        return segments.toString();
      }
      List<String> head = new ArrayList<>(segments).subList(0, 6);
      return head + " ... (" + segments.size() + " total)";
    }
  }

  /// Runs every scenario and prints the reports, for use outside of the TestNG runner.
  public static void main(String[] args) {
    LowDiskModeRebalanceSimulatorTest simulator = new LowDiskModeRebalanceSimulatorTest();
    Map<String, Runnable> scenarios = new LinkedHashMap<>();
    scenarios.put("overlapping-rotation", simulator::testOverlappingServerSetsRotation);
    scenarios.put("overlapping-rotation-strict", simulator::testOverlappingServerSetsRotationStrictReplicaGroup);
    scenarios.put("larger-rotation", simulator::testLargerRotationOverlap);
    scenarios.put("pure-scale-out-control", simulator::testPureScaleOutIsClean);
    scenarios.put("sweep", simulator::testSweepOverlappingServerSets);
    scenarios.put("random-search", simulator::testRandomSearch);
    scenarios.forEach((name, runnable) -> {
      System.out.println("\n\n##### " + name + " #####");
      runnable.run();
    });
  }
}
