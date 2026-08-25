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
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.restlet.resources.RebalanceConfig;
import org.apache.pinot.controller.helix.core.assignment.segment.SegmentAssignmentUtils;
import org.apache.pinot.controller.util.TableSizeReader;
import org.slf4j.LoggerFactory;
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

  /// Exercises the escape hatch: when nothing can move within the budget, the rebalance relaxes it for a step rather
  /// than stalling, and the replay the pre-check uses reports exactly the servers that go over.
  ///
  /// hostA and hostB both start at their ceiling and each has to take on a segment from the other, while the segments
  /// that would free up their space cannot move at all - single replica, single target instance, so no addition is
  /// ever attempted for them and `minAvailableReplicas = 1` forbids dropping the only replica.
  ///
  /// NOTE: this is not a state a real rebalance reaches. `TableRebalancer#getMinAvailableReplicas` rejects
  /// `minAvailableReplicas = 1` when a segment being moved has a single target replica, so a rebalance would fail on
  /// the config before getting here. It is kept because it is the only known way to drive the relaxation, and it
  /// pins down what the replay reports when it happens.
  @Test
  public void testRelaxingTheBudgetIsReportedByTheReplay() {
    Map<String, Map<String, String>> currentAssignment = new TreeMap<>();
    // Each needs a second replica on the other host, which is already at its ceiling
    currentAssignment.put("needsReplicaOnB", instanceStateMap("hostA"));
    currentAssignment.put("needsReplicaOnA", instanceStateMap("hostB"));
    // These would free up the space, but cannot be moved while keeping 1 replica available
    currentAssignment.put("stuckOnA", instanceStateMap("hostA"));
    currentAssignment.put("stuckOnB", instanceStateMap("hostB"));

    Map<String, Map<String, String>> targetAssignment = new TreeMap<>();
    targetAssignment.put("needsReplicaOnB", instanceStateMap("hostA", "hostB"));
    targetAssignment.put("needsReplicaOnA", instanceStateMap("hostB", "hostA"));
    targetAssignment.put("stuckOnA", instanceStateMap("hostC"));
    targetAssignment.put("stuckOnB", instanceStateMap("hostC"));

    SimResult result = simulate(new Scenario("Cannot stay within the disk each server starts with", currentAssignment,
        targetAssignment, 1, false, RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER));
    System.out.println(result.report());
    // hostA and hostB each start with 2 segments and the target places 2 on them, so neither may ever hold 3
    assertTrue(result.hasSteadyStateViolation(), "expected the rebalance to be forced over the bound");
    assertEquals(result._maxAfter.get("hostA"), (Long) 3L);
    assertEquals(result._maxAfter.get("hostB"), (Long) 3L);

    // The pre-check must report exactly those two servers, before anything is moved
    Map<String, Long> reported = TableRebalancer.getServersForcedOverDiskBudget(currentAssignment, targetAssignment, 1,
        false, RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, null, LoggerFactory.getLogger(getClass()));
    System.out.println("Pre-check reports servers forced over their disk budget: " + reported);
    assertEquals(reported, Map.of("hostA", 1L, "hostB", 1L));
  }

  private static Map<String, String> instanceStateMap(String... instances) {
    return SegmentAssignmentUtils.getInstanceStateMap(List.of(instances), ONLINE);
  }

  /// The disk budget holds a total per pair of current and target instances, used to pick which instance to add. It
  /// is tempting to charge a strict replica group move by looking that total up, but the two are different totals when
  /// batching is enabled: `updateNextAssignmentForPartitionIdStrictReplicaGroup` is then called once per partition,
  /// while the step-wide total spans every partition sharing the pair. Looking it up would charge the whole pair for
  /// each partition, over-charging by the number of partitions that share it.
  ///
  /// Several partitions sharing one pair of instances is the normal shape for a replica group table, where partitions
  /// per replica group is exactly that multiple.
  @Test
  public void testStrictReplicaGroupChargesPerPartitionNotPerInstancePair() {
    long mib = 1024L * 1024;
    Map<String, Map<String, String>> current = new TreeMap<>();
    Map<String, Map<String, String>> target = new TreeMap<>();
    Map<String, Long> sizes = new TreeMap<>();
    for (int partition = 0; partition < 2; partition++) {
      for (int sequence = 0; sequence < 2; sequence++) {
        String segment = "myTable__" + partition + "__" + sequence + "__20240101T0000Z";
        current.put(segment, instanceStateMap("hostA", "hostB"));
        target.put(segment, instanceStateMap("hostC", "hostD"));
        sizes.put(segment, 100 * mib);
      }
    }
    TableRebalancer.StepDiskBudget stepBudget =
        TableRebalancer.DiskUsageBudget.create(current, toTableSizeDetails(sizes)).forStep(current, target);

    // All four segments share one pair of instances, so the step-wide map holds a single total covering both partitions
    Map<Pair<Set<String>, Set<String>>, Long> stepWide = stepBudget.getInstancePairToBytes(current, target);
    assertEquals(stepWide.size(), 1);
    long stepWideBytes = stepWide.values().iterator().next();

    // What one call is handed once batching has grouped by pair and then by partition
    Map<String, Map<String, String>> onePartition = new TreeMap<>();
    current.forEach((segment, instanceStateMap) -> {
      if (segment.startsWith("myTable__0__")) {
        onePartition.put(segment, instanceStateMap);
      }
    });
    long onePartitionBytes = stepBudget.getInstancePairToBytes(onePartition, target).values().iterator().next();

    System.out.printf("%n  step-wide total for the pair: %s, what one call is handed: %s%n",
        formatMib(stepWideBytes), formatMib(onePartitionBytes));
    assertEquals(onePartitionBytes, 200 * mib);
    assertEquals(stepWideBytes, 2 * onePartitionBytes,
        "the step-wide total spans both partitions, so charging it per partition would double count");
  }

  /// Where a segment added mid-rebalance costs headroom, and where it does not.
  ///
  /// The new segment is credited to the anchor of the servers hosting it, so `ceiling` becomes
  /// `max(initial + upload, target)`. That covers the upload whenever the anchor side of the max wins - which it
  /// always does on a server whose net change is a loss. On a server that is growing, the target side wins and the
  /// credit is swallowed by the max, so the upload eats headroom.
  ///
  /// It only costs anything at all when the target assignment does not place the segment where it landed. When it
  /// does, the target rises by the same amount and the cost cancels out wherever it fell. That is the case strict
  /// replica group assignment cannot give us: `BaseStrictRealtimeSegmentAssignment` overrides a new segment onto its
  /// partition's existing placement to keep the partition collocated, which mid-rebalance is the placement the
  /// rebalance is moving away from.
  @Test
  public void testWhereMidRebalanceUploadsCostHeadroom() {
    long mib = 1024L * 1024;
    System.out.printf("%n  %-34s %12s %12s %10s%n", "case", "control", "with upload", "cost");
    for (boolean netLoss : List.of(true, false)) {
      for (boolean targetAgrees : List.of(true, false)) {
        long control = remainingOnFocus(netLoss, null, 0, mib);
        long withUpload = remainingOnFocus(netLoss, targetAgrees ? "focus" : "elsewhere", 300 * mib, mib);
        System.out.printf("  %-34s %12s %12s %10s%n",
            (netLoss ? "net loss, " : "net gain,  ") + (targetAgrees ? "target agrees" : "target elsewhere"),
            formatMib(control), formatMib(withUpload), formatMib(control - withUpload));
        if (netLoss || targetAgrees) {
          assertEquals(withUpload, control, "this placement must not cost the rebalance any headroom");
        } else {
          // The one combination that costs anything: on a growing server the target side of the max wins, so the
          // credit for the new segment is swallowed, and the target does not carry it either. The cost is exactly the
          // segment that appeared, which bounds it - pinned so that a change widening it fails here
          assertEquals(control - withUpload, 300 * mib,
              "the cost must stay bounded by the size of the segment that appeared");
        }
      }
    }
  }

  /// Headroom on `focus` in a mid-rebalance state, optionally with a segment that appeared after the anchor was taken.
  /// `netLoss` picks whether `focus` is shedding bytes overall or taking them on.
  private static long remainingOnFocus(boolean netLoss, String uploadedOn, long uploadSizeBytes, long mib) {
    Map<String, Map<String, String>> initialAssignment = new TreeMap<>();
    Map<String, Map<String, String>> current = new TreeMap<>();
    Map<String, Map<String, String>> target = new TreeMap<>();
    Map<String, Long> sizes = new TreeMap<>();

    if (netLoss) {
      // focus starts on 1000M and the target places 600M, so the anchor side of the max wins
      addSegment(initialAssignment, current, target, sizes, "kept", List.of("focus"), List.of("focus"),
          List.of("focus"), 500 * mib);
      addSegment(initialAssignment, current, target, sizes, "dropped", List.of("focus"), List.of("other"),
          List.of("other"), 500 * mib);
      addSegment(initialAssignment, current, target, sizes, "arriving", List.of("other"), List.of("other"),
          List.of("focus"), 100 * mib);
    } else {
      // focus starts on 100M and the target places 1000M, so the target side of the max wins
      addSegment(initialAssignment, current, target, sizes, "kept", List.of("focus"), List.of("focus"),
          List.of("focus"), 100 * mib);
      addSegment(initialAssignment, current, target, sizes, "arriving", List.of("other"), List.of("other"),
          List.of("focus"), 900 * mib);
    }
    if (uploadedOn != null) {
      // Appeared after the anchor was taken, so it is absent from initialAssignment
      current.put("uploaded", instanceStateMap("focus"));
      target.put("uploaded", instanceStateMap(uploadedOn));
      sizes.put("uploaded", uploadSizeBytes);
    }
    return TableRebalancer.DiskUsageBudget.create(initialAssignment, toTableSizeDetails(sizes))
        .forStep(current, target).getRemainingBytes().getOrDefault("focus", 0L);
  }

  private static void addSegment(Map<String, Map<String, String>> initialAssignment,
      Map<String, Map<String, String>> current, Map<String, Map<String, String>> target, Map<String, Long> sizes,
      String segment, List<String> initialInstances, List<String> currentInstances, List<String> targetInstances,
      long sizeBytes) {
    initialAssignment.put(segment, SegmentAssignmentUtils.getInstanceStateMap(initialInstances, ONLINE));
    current.put(segment, SegmentAssignmentUtils.getInstanceStateMap(currentInstances, ONLINE));
    target.put(segment, SegmentAssignmentUtils.getInstanceStateMap(targetInstances, ONLINE));
    sizes.put(segment, sizeBytes);
  }

  /// A segment uploaded onto a server that the target assignment also places it on is not automatically harmless.
  ///
  /// The ceiling is `max(frozen initial bytes, target bytes)`. Where a server's net change is a gain, the upload
  /// raises the target side of that max by as much as it raises what the server hosts, so the headroom is untouched.
  /// Where the net change is a loss the ceiling is pinned to the frozen initial bytes, which the upload cannot raise,
  /// so an upload accounted for there would consume headroom outright. The budget therefore accounts only for the
  /// segments the rebalance started with, which keeps the headroom of a net-loss server intact.
  ///
  /// `host0` below hosts 1000M and the target places 600M on it, a net loss of 400M. It has to drop a 500M segment and
  /// take on a 100M one, so once the drop lands it has 500M of headroom for a 100M arrival.
  @Test
  public void testUploadOntoNetLossServerEatsHeadroom() {
    long mib = 1024L * 1024;
    System.out.printf("%n  %-10s %10s %10s %10s %10s %10s   %s%n", "upload", "ceiling", "hosted", "remaining",
        "needs", "margin", "verdict");
    long marginWithoutUpload = -1;
    for (long uploadMib : List.of(0L, 100L, 200L, 400L, 700L)) {
      // The state after host0 has dropped its outgoing segment, which is where its headroom opens up
      Map<String, Map<String, String>> current = new TreeMap<>();
      Map<String, Map<String, String>> target = new TreeMap<>();
      Map<String, Long> sizes = new TreeMap<>();
      Map<String, Map<String, String>> initialAssignment = new TreeMap<>();

      // kept by host0
      initialAssignment.put("kept", instanceStateMap("host0", "host1"));
      current.put("kept", instanceStateMap("host0", "host1"));
      target.put("kept", instanceStateMap("host0", "host1"));
      sizes.put("kept", 500 * mib);
      // dropped by host0 — present when the rebalance started, already gone in this state
      initialAssignment.put("dropped", instanceStateMap("host0", "host1"));
      current.put("dropped", instanceStateMap("host1"));
      target.put("dropped", instanceStateMap("host1", "host2"));
      sizes.put("dropped", 500 * mib);
      // still to arrive on host0
      initialAssignment.put("arriving", instanceStateMap("host1", "host2"));
      current.put("arriving", instanceStateMap("host1"));
      target.put("arriving", instanceStateMap("host0", "host1"));
      sizes.put("arriving", 100 * mib);

      if (uploadMib > 0) {
        // Uploaded after the rebalance started, so absent from the anchor, and placed on host0 by both assignments
        current.put("uploaded", instanceStateMap("host0"));
        target.put("uploaded", instanceStateMap("host0"));
        sizes.put("uploaded", uploadMib * mib);
      }

      TableRebalancer.DiskUsageBudget budget =
          TableRebalancer.DiskUsageBudget.create(initialAssignment, toTableSizeDetails(sizes));
      long remaining = budget.forStep(current, target).getRemainingBytes().getOrDefault("host0", 0L);
      long hosted = hostedBytesOf(current, sizes).getOrDefault("host0", 0L);
      // The ceiling the budget is actually working to, which the credit for the uploaded segment raises
      long ceiling = hosted + remaining;
      long needs = 100 * mib;
      long margin = remaining - needs;
      if (uploadMib == 0) {
        marginWithoutUpload = margin;
      }
      System.out.printf("  %-10s %10s %10s %10s %10s %10s   %s%n", uploadMib == 0 ? "none" : uploadMib + "M",
          formatMib(ceiling), formatMib(hosted), formatMib(remaining), formatMib(needs), formatMib(margin),
          margin < 0 ? "BLOCKED" : margin == 0 ? "exact fit, no slack" : "fits");
      if (uploadMib > 0) {
        assertEquals(margin, marginWithoutUpload,
            uploadMib + "M upload must not consume headroom the rebalance needs");
      }
    }
    System.out.println("\n  The margin that absorbs group granularity is unaffected by the upload, because the"
        + " budget accounts\n  only for the segments the rebalance started with.");
  }

  /// Segments can be added to a table while a rebalance is running - an uploaded segment, or a new consuming segment.
  /// The budget is anchored once, before the first step, so it has never heard of them: they are absent from
  /// `initialServerBytes` and from the per-segment sizes, while they do count towards what a server hosts now and
  /// towards the recalculated target. Drives that directly, holding the budget across the injection the way
  /// `doRebalance` does.
  @Test
  public void testSegmentsAddedDuringTheRebalance() {
    List<String> oldServers = List.of("host1", "host2", "host3");
    List<String> newServers = List.of("host2", "host3", "host4");
    Map<String, Map<String, String>> current = roundRobin(12, 2, oldServers, 0);
    Map<String, Map<String, String>> target = roundRobin(12, 2, newServers, 0);
    Map<String, Long> sizes = new TreeMap<>();
    current.keySet().forEach(segment -> sizes.put(segment, 100L * 1024 * 1024));

    // The budget the controller builds once, before the first step
    TableRebalancer.DiskUsageBudget diskUsageBudget =
        TableRebalancer.DiskUsageBudget.create(current, toTableSizeDetails(sizes));
    Map<String, Long> frozenCeiling = new TreeMap<>();
    Map<String, Long> initialBytes = hostedBytesOf(current, sizes);
    hostedBytesOf(target, sizes).forEach(
        (server, bytes) -> frozenCeiling.put(server, Math.max(initialBytes.getOrDefault(server, 0L), bytes)));
    initialBytes.forEach((server, bytes) -> frozenCeiling.merge(server, bytes, Math::max));

    Map<String, Long> peak = new TreeMap<>(initialBytes);
    int numRelaxedSteps = 0;
    boolean converged = false;
    for (int step = 1; step <= 40; step++) {
      // Segments uploaded part way through, placed by the instance partitions in force at upload time - the old
      // servers - while the recalculated target places them on the new servers. The overlap servers therefore host
      // them without the target assignment accounting for them
      if (step == 3) {
        for (int i = 0; i < 4; i++) {
          String uploaded = "uploaded" + i;
          current.put(uploaded, SegmentAssignmentUtils.getInstanceStateMap(List.of("host1", "host2"), ONLINE));
          target.put(uploaded, SegmentAssignmentUtils.getInstanceStateMap(List.of("host3", "host4"), ONLINE));
          sizes.put(uploaded, 400L * 1024 * 1024);
        }
        System.out.println("  step 3: 4 uploaded segments of 400M landed on host1/host2, target says host3/host4");
      }
      Map<String, Long> allowed = diskUsageBudget.forStep(current, target).getRemainingBytes();
      // A new segment is credited to the anchor once, so asking again must give the same answer. If it were credited
      // per call, the rebalance could raise its own ceiling simply by taking another step
      assertEquals(diskUsageBudget.forStep(current, target).getRemainingBytes(), allowed,
          "crediting a new segment to the anchor must be idempotent");
      Map<String, Map<String, String>> next =
          TableRebalancer.getNextAssignment(current, target, 1, false, true,
              RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, new Object2IntOpenHashMap<>(), DUMMY_PARTITION_FETCHER,
              NO_DATA_LOSS_RISK, diskUsageBudget);
      if (next.equals(current)) {
        break;
      }
      // Measured against what the budget accounts for: segments added while the rebalance runs are outside it, so
      // the bytes they bring are not the rebalance going over its budget
      Map<String, Long> added = new TreeMap<>();
      for (Map.Entry<String, Map<String, String>> entry : next.entrySet()) {
        Map<String, String> currentInstanceStateMap = current.get(entry.getKey());
        long budgetedBytes = diskUsageBudget.getSegmentSizeBytes(entry.getKey());
        for (String instance : entry.getValue().keySet()) {
          if (currentInstanceStateMap == null || !currentInstanceStateMap.containsKey(instance)) {
            added.merge(instance, budgetedBytes, Long::sum);
          }
        }
      }
      for (Map.Entry<String, Long> entry : added.entrySet()) {
        if (entry.getValue() > allowed.getOrDefault(entry.getKey(), 0L)) {
          numRelaxedSteps++;
          System.out.printf("  step %d: budget relaxed - %s took %s but was allowed %s%n", step, entry.getKey(),
              formatMib(entry.getValue()), formatMib(allowed.getOrDefault(entry.getKey(), 0L)));
          break;
        }
      }
      current = next;
      hostedBytesOf(current, sizes).forEach((server, bytes) -> peak.merge(server, bytes, Math::max));
      if (current.equals(target)) {
        converged = true;
        break;
      }
    }

    System.out.printf("%nconverged=%s, steps where the rebalance exceeded its budget=%d%n", converged,
        numRelaxedSteps);
    System.out.println("  peak below counts every segment on the server, including the uploaded ones the budget does"
        + " not account for");
    Map<String, Long> finalBytes = hostedBytesOf(target, sizes);
    System.out.printf("  %-8s %10s %10s %12s %10s%n", "server", "initial", "final", "frozenCeil", "peak");
    for (String server : peak.keySet()) {
      System.out.printf("  %-8s %10s %10s %12s %10s%s%n", server,
          formatMib(initialBytes.getOrDefault(server, 0L)), formatMib(finalBytes.getOrDefault(server, 0L)),
          formatMib(frozenCeiling.getOrDefault(server, 0L)), formatMib(peak.get(server)),
          peak.get(server) > Math.max(frozenCeiling.getOrDefault(server, 0L),
              finalBytes.getOrDefault(server, 0L)) ? "   above the ceiling, carrying uploaded segments" : "");
    }
    assertTrue(converged, "the rebalance must still converge when segments are added while it runs");
    assertEquals(numRelaxedSteps, 0,
        "segments added while the rebalance runs must not push it outside the budget it started with");
  }

  private static Map<String, Long> hostedBytesOf(Map<String, Map<String, String>> assignment,
      Map<String, Long> sizes) {
    Map<String, Long> bytes = new TreeMap<>();
    assignment.forEach((segment, instanceStateMap) -> instanceStateMap.keySet()
        .forEach(instance -> bytes.merge(instance, sizes.get(segment), Long::sum)));
    return bytes;
  }

  private static String formatMib(long bytes) {
    return (bytes / (1024 * 1024)) + "M";
  }

  /// Searches broadly for rebalances the disk budget cannot carry out, i.e. the ones the disk utilization pre-check
  /// has to report. Randomizes everything that feeds the budget: strict and non strict routing, batching on and off,
  /// the minimum available replicas, how many replicas a group currently sits at, group sizes, and four different
  /// segment size distributions including one where a single segment dominates its whole group.
  ///
  /// The property asserted is not that a rebalance can always stay within the budget - it cannot, and relaxing the
  /// budget rather than stalling is the deliberate behaviour - but that the pre-check replay reports exactly the
  /// servers that end up over. A relaxation the pre-check does not predict would be a silent over-allocation.
  @Test
  public void testSearchForRebalancesTheBudgetCannotCarryOut() {
    Random random = new Random(2027);
    List<SimResult> relaxed = new ArrayList<>();
    List<String> mismatches = new ArrayList<>();
    int numTrials = 30000;
    int numRun = 0;
    for (int trial = 0; trial < numTrials; trial++) {
      int numServers = 3 + random.nextInt(8);
      int replication = 2 + random.nextInt(3);
      int minAvailableReplicas = 1 + random.nextInt(replication - 1);
      int numGroups = 2 + random.nextInt(7);
      boolean enableStrictReplicaGroup = random.nextBoolean();
      int batchSizePerServer = List.of(RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, 1, 2, 5, 20)
          .get(random.nextInt(5));
      int sizeRegime = random.nextInt(4);
      List<String> allServers = servers(0, numServers);

      Map<String, Map<String, String>> current = new TreeMap<>();
      Map<String, Map<String, String>> target = new TreeMap<>();
      Map<String, Long> sizes = new TreeMap<>();
      int segmentId = 0;
      for (int group = 0; group < numGroups; group++) {
        // A group that already sits at the minimum available replicas cannot drop anything, which is what forces it
        // to depend on an addition landing somewhere
        int numCurrentInstances = minAvailableReplicas + random.nextInt(replication - minAvailableReplicas + 1);
        List<String> currentInstances = pickServers(allServers, numCurrentInstances, random);
        List<String> targetInstances = pickServers(allServers, replication, random);
        int numSegmentsInGroup = 1 + random.nextInt(10);
        List<String> groupSegments = new ArrayList<>();
        for (int i = 0; i < numSegmentsInGroup; i++) {
          String segment = String.format("segment%04d", segmentId++);
          groupSegments.add(segment);
          current.put(segment, SegmentAssignmentUtils.getInstanceStateMap(currentInstances, ONLINE));
          target.put(segment, SegmentAssignmentUtils.getInstanceStateMap(targetInstances, ONLINE));
        }
        sizes.putAll(sizesForRegime(groupSegments, sizeRegime, random));
      }
      List<String> segmentsToMove = SegmentAssignmentUtils.getSegmentsToMove(current, target);
      if (segmentsToMove.isEmpty() || TableRebalancer.getMinAvailableReplicas(current, target, segmentsToMove,
          minAvailableReplicas, LoggerFactory.getLogger(getClass())) != minAvailableReplicas) {
        continue;
      }
      numRun++;

      String name = String.format(
          "trial %d: servers=%d replication=%d minAvail=%d groups=%d strict=%s batch=%d sizes=%d", trial, numServers,
          replication, minAvailableReplicas, numGroups, enableStrictReplicaGroup, batchSizePerServer, sizeRegime);
      SimResult result = simulate(new Scenario(name, current, target, minAvailableReplicas, enableStrictReplicaGroup,
          batchSizePerServer, sizes));
      Set<String> observed = new TreeSet<>();
      result.relaxedSteps().forEach(step -> observed.addAll(step._deferralRelaxedFor));
      if (observed.isEmpty()) {
        continue;
      }
      relaxed.add(result);
      // The pre-check has to predict it, from the same starting point, before anything moves
      Map<String, Long> reported = TableRebalancer.getServersForcedOverDiskBudget(current, target,
          minAvailableReplicas, enableStrictReplicaGroup, batchSizePerServer, toTableSizeDetails(sizes),
          LoggerFactory.getLogger(getClass()));
      if (!reported.keySet().equals(observed)) {
        mismatches.add(name + ": rebalance went over on " + observed + " but the pre-check reported " + reported);
      }
    }

    System.out.printf("%nSearched %d of %d generated scenarios (the rest were configs the rebalance would reject)%n",
        numRun, numTrials);
    System.out.printf("  rebalances the budget could not carry out: %d%n", relaxed.size());
    if (!relaxed.isEmpty()) {
      relaxed.sort((a, b) -> Double.compare(b.worstAmplification(), a.worstAmplification()));
      System.out.printf("  worst amplification among them: %.2fx%n", relaxed.get(0).worstAmplification());
      for (SimResult result : relaxed.subList(0, Math.min(8, relaxed.size()))) {
        System.out.printf("    %-92s worst %s: bound %s, peak %s%n", result._scenario._name, result.worstServer(),
            formatUnits(result.bound(result.worstServer()), result._scenario),
            formatUnits(result._maxAfter.get(result.worstServer()), result._scenario));
      }
      System.out.println(relaxed.get(0).report());
      dumpScenario(relaxed.get(0)._scenario);
    }
    assertEquals(mismatches, List.of(), "the pre-check must report every rebalance that goes over the disk budget");
  }

  /// Enumerates every small rebalance exhaustively, rather than sampling. A rebalance the budget cannot carry out is
  /// a rare structure, so random generation is the wrong instrument for it: if a minimal one exists it should show up
  /// among 4 servers, 3 groups sitting at the minimum available replicas, and a handful of group sizes.
  @Test
  public void testExhaustiveSmallSearchForRebalancesTheBudgetCannotCarryOut() {
    List<String> allServers = servers(0, 4);
    List<Long> groupSizes = List.of(1L, 3L, 10L, 40L);
    List<SimResult> relaxed = new ArrayList<>();
    List<String> mismatches = new ArrayList<>();
    int numRun = 0;
    for (int replication : List.of(2, 3)) {
      // Every (single current instance, target instance set) a group can have
      List<List<List<String>>> groupShapes = new ArrayList<>();
      for (String currentInstance : allServers) {
        for (List<String> targetInstances : combinations(allServers, replication)) {
          groupShapes.add(List.of(List.of(currentInstance), targetInstances));
        }
      }
      int numShapes = groupShapes.size();
      for (int a = 0; a < numShapes; a++) {
        for (int b = a; b < numShapes; b++) {
          for (int c = b; c < numShapes; c++) {
            for (long sizeA : groupSizes) {
              for (long sizeB : groupSizes) {
                for (long sizeC : groupSizes) {
                  List<List<List<String>>> shapes = List.of(groupShapes.get(a), groupShapes.get(b),
                      groupShapes.get(c));
                  List<Long> sizesPerGroup = List.of(sizeA, sizeB, sizeC);
                  Map<String, Map<String, String>> current = new TreeMap<>();
                  Map<String, Map<String, String>> target = new TreeMap<>();
                  Map<String, Long> sizes = new TreeMap<>();
                  for (int group = 0; group < 3; group++) {
                    String segment = "segment" + group;
                    current.put(segment,
                        SegmentAssignmentUtils.getInstanceStateMap(shapes.get(group).get(0), ONLINE));
                    target.put(segment,
                        SegmentAssignmentUtils.getInstanceStateMap(shapes.get(group).get(1), ONLINE));
                    sizes.put(segment, sizesPerGroup.get(group));
                  }
                  List<String> segmentsToMove = SegmentAssignmentUtils.getSegmentsToMove(current, target);
                  if (segmentsToMove.isEmpty() || TableRebalancer.getMinAvailableReplicas(current, target,
                      segmentsToMove, 1, LoggerFactory.getLogger(getClass())) != 1) {
                    continue;
                  }
                  for (boolean enableStrictReplicaGroup : List.of(false, true)) {
                    numRun++;
                    String name = String.format("replication=%d shapes=%s sizes=%s strict=%s", replication, shapes,
                        sizesPerGroup, enableStrictReplicaGroup);
                    SimResult result = simulate(new Scenario(name, current, target, 1, enableStrictReplicaGroup,
                        RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, sizes));
                    Set<String> observed = new TreeSet<>();
                    result.relaxedSteps().forEach(step -> observed.addAll(step._deferralRelaxedFor));
                    if (observed.isEmpty()) {
                      continue;
                    }
                    relaxed.add(result);
                    Map<String, Long> reported = TableRebalancer.getServersForcedOverDiskBudget(current, target, 1,
                        enableStrictReplicaGroup, RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER,
                        toTableSizeDetails(sizes), LoggerFactory.getLogger(getClass()));
                    if (!reported.keySet().equals(observed)) {
                      mismatches.add(name + ": went over on " + observed + " but pre-check reported " + reported);
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    System.out.printf("%nExhaustively ran %d small rebalances%n", numRun);
    System.out.printf("  rebalances the budget could not carry out: %d%n", relaxed.size());
    if (!relaxed.isEmpty()) {
      relaxed.sort((a, b) -> Double.compare(b.worstAmplification(), a.worstAmplification()));
      System.out.println(relaxed.get(0).report());
      dumpScenario(relaxed.get(0)._scenario);
    }
    assertEquals(mismatches, List.of(), "the pre-check must report every rebalance that goes over the disk budget");
  }

  /// All the subsets of `items` of exactly `size`, in a stable order.
  private static List<List<String>> combinations(List<String> items, int size) {
    List<List<String>> combinations = new ArrayList<>();
    if (size == 0) {
      combinations.add(List.of());
      return combinations;
    }
    for (int i = 0; i <= items.size() - size; i++) {
      for (List<String> rest : combinations(items.subList(i + 1, items.size()), size - 1)) {
        List<String> combination = new ArrayList<>();
        combination.add(items.get(i));
        combination.addAll(rest);
        combinations.add(combination);
      }
    }
    return combinations;
  }

  /// Segment sizes under one of several distributions, so that the search is not limited to one shape of skew.
  private static Map<String, Long> sizesForRegime(List<String> segments, int sizeRegime, Random random) {
    Map<String, Long> sizes = new TreeMap<>();
    long mib = 1024L * 1024;
    switch (sizeRegime) {
      case 0 -> segments.forEach(segment -> sizes.put(segment, 64 * mib));
      case 1 -> segments.forEach(segment -> sizes.put(segment,
          random.nextInt(10) < 8 ? (8 + random.nextInt(24)) * mib : (320 + random.nextInt(960)) * mib));
      case 2 -> {
        // One segment dominates the group, so the group only fits where there is a lot of room
        segments.forEach(segment -> sizes.put(segment, (4 + random.nextInt(12)) * mib));
        sizes.put(segments.get(random.nextInt(segments.size())), (1024 + random.nextInt(3072)) * mib);
      }
      default -> segments.forEach(
          segment -> sizes.put(segment, (long) Math.pow(2, 3 + random.nextInt(9)) * mib));
    }
    return sizes;
  }

  /// Regression test for a strict replica group rebalance that used to be pushed over the disk budget, found by
  /// [#testStrictReplicaGroupRandomGroupStructureSearch].
  ///
  /// Before the instances to add were picked with the budget in mind, this stalled at a step where three of the four
  /// groups still had an instance in their target assignment with room for them - `host01` had exactly the 2242 MiB
  /// that one group needed, `host04` had room for another and `host00` for the third - but the instances were picked
  /// purely on the number of segments to offload, which landed on `host00` (2186 MiB free, 56 MiB short), `host02`
  /// (318 MiB free) and `host02` again. All three were rejected by the budget, none of the instances that fit was
  /// tried, and with nothing able to move the budget was relaxed and `host02` went 36% over the disk it started with.
  ///
  /// A correct sequence existed the whole time; only the choice of instance was blind to the budget.
  @Test
  public void testStrictReplicaGroupStaysWithinTheDiskBudget() {
    Map<String, Map<String, String>> current = new TreeMap<>();
    Map<String, Map<String, String>> target = new TreeMap<>();
    Map<String, Long> sizes = new TreeMap<>();
    current.put("segment000", instanceStateMap("host02", "host07"));
    current.put("segment001", instanceStateMap("host02", "host07"));
    current.put("segment002", instanceStateMap("host02", "host07"));
    current.put("segment003", instanceStateMap("host00", "host01"));
    current.put("segment004", instanceStateMap("host00", "host01"));
    current.put("segment005", instanceStateMap("host00", "host01"));
    current.put("segment006", instanceStateMap("host00", "host01"));
    current.put("segment007", instanceStateMap("host00", "host01"));
    current.put("segment008", instanceStateMap("host04"));
    current.put("segment009", instanceStateMap("host04"));
    current.put("segment010", instanceStateMap("host04"));
    current.put("segment011", instanceStateMap("host04"));
    current.put("segment012", instanceStateMap("host04"));
    current.put("segment013", instanceStateMap("host04"));
    current.put("segment014", instanceStateMap("host04", "host05"));
    current.put("segment015", instanceStateMap("host04", "host05"));
    current.put("segment016", instanceStateMap("host04", "host05"));
    current.put("segment017", instanceStateMap("host04", "host05"));
    current.put("segment018", instanceStateMap("host04", "host05"));
    current.put("segment019", instanceStateMap("host04", "host05"));
    target.put("segment000", instanceStateMap("host00", "host01"));
    target.put("segment001", instanceStateMap("host00", "host01"));
    target.put("segment002", instanceStateMap("host00", "host01"));
    target.put("segment003", instanceStateMap("host02", "host04"));
    target.put("segment004", instanceStateMap("host02", "host04"));
    target.put("segment005", instanceStateMap("host02", "host04"));
    target.put("segment006", instanceStateMap("host02", "host04"));
    target.put("segment007", instanceStateMap("host02", "host04"));
    target.put("segment008", instanceStateMap("host00", "host06"));
    target.put("segment009", instanceStateMap("host00", "host06"));
    target.put("segment010", instanceStateMap("host00", "host06"));
    target.put("segment011", instanceStateMap("host00", "host06"));
    target.put("segment012", instanceStateMap("host00", "host06"));
    target.put("segment013", instanceStateMap("host00", "host06"));
    target.put("segment014", instanceStateMap("host00", "host02"));
    target.put("segment015", instanceStateMap("host00", "host02"));
    target.put("segment016", instanceStateMap("host00", "host02"));
    target.put("segment017", instanceStateMap("host00", "host02"));
    target.put("segment018", instanceStateMap("host00", "host02"));
    target.put("segment019", instanceStateMap("host00", "host02"));
    sizes.put("segment000", 1152385024L);
    sizes.put("segment001", 8388608L);
    sizes.put("segment002", 1190133760L);
    sizes.put("segment003", 368050176L);
    sizes.put("segment004", 28311552L);
    sizes.put("segment005", 24117248L);
    sizes.put("segment006", 938475520L);
    sizes.put("segment007", 12582912L);
    sizes.put("segment008", 8388608L);
    sizes.put("segment009", 9437184L);
    sizes.put("segment010", 20971520L);
    sizes.put("segment011", 14680064L);
    sizes.put("segment012", 31457280L);
    sizes.put("segment013", 14680064L);
    sizes.put("segment014", 24117248L);
    sizes.put("segment015", 1207959552L);
    sizes.put("segment016", 27262976L);
    sizes.put("segment017", 16777216L);
    sizes.put("segment018", 17825792L);
    sizes.put("segment019", 18874368L);

    // The config the rebalance would actually run with, so this is a state a real rebalance can be asked to carry out
    assertEquals(TableRebalancer.getMinAvailableReplicas(current, target,
        SegmentAssignmentUtils.getSegmentsToMove(current, target), 1, LoggerFactory.getLogger(getClass())), 1);

    SimResult result = simulate(new Scenario("Strict replica group within the disk budget", current, target, 1, true,
        RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, sizes));
    System.out.println(result.report());
    assertFalse(result.hasSteadyStateViolation(), "no server may hold more than max(initial, target)");
    assertFalse(result.hasInStepViolation(), "no server may exceed max(initial, target) mid-step either");
    assertTrue(result.relaxedSteps().isEmpty(), "the budget should not have to be relaxed for this assignment");
    assertEquals(result._groupSplits, Set.of(), "groups must still be moved together");

    // And the pre-check must agree that it is safe
    assertEquals(TableRebalancer.getServersForcedOverDiskBudget(current, target, 1, true,
        RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, toTableSizeDetails(sizes),
        LoggerFactory.getLogger(getClass())), Map.of());
  }

  /// The argument that the disk budget can never be the only thing blocking a step does not carry over to strict
  /// replica group routing: there a whole group of segments has to move at once, so a group can be blocked while the
  /// server it would move to still has room, which cannot happen when segments are charged one at a time. The sweeps
  /// above only build groups by round robin, which makes them regular and equally sized, so they say little about it.
  ///
  /// Searches randomized group structures - random current and target instance sets, groups of differing sizes,
  /// segments sitting at differing replica counts, skewed segment sizes - for a strict replica group rebalance the
  /// budget cannot carry out.
  @Test
  public void testStrictReplicaGroupRandomGroupStructureSearch() {
    Random random = new Random(101);
    List<SimResult> relaxed = new ArrayList<>();
    List<SimResult> violations = new ArrayList<>();
    List<String> splits = new ArrayList<>();
    List<SimResult> all = new ArrayList<>();
    int numSkippedAsIllegal = 0;
    for (int trial = 0; trial < 20000; trial++) {
      int numServers = 3 + random.nextInt(6);
      int replication = 2 + random.nextInt(2);
      int numGroups = 2 + random.nextInt(5);
      List<String> allServers = servers(0, numServers);
      Map<String, Map<String, String>> current = new TreeMap<>();
      Map<String, Map<String, String>> target = new TreeMap<>();
      int segmentId = 0;
      for (int group = 0; group < numGroups; group++) {
        // Segments of a group share one current instance set and one target instance set. Let the current set be
        // smaller than the replication so that groups part way through a move are covered too.
        List<String> currentInstances = pickServers(allServers, 1 + random.nextInt(replication), random);
        List<String> targetInstances = pickServers(allServers, replication, random);
        int numSegmentsInGroup = 1 + random.nextInt(6);
        for (int i = 0; i < numSegmentsInGroup; i++) {
          String segment = String.format("segment%03d", segmentId++);
          current.put(segment, SegmentAssignmentUtils.getInstanceStateMap(currentInstances, ONLINE));
          target.put(segment, SegmentAssignmentUtils.getInstanceStateMap(targetInstances, ONLINE));
        }
      }
      // Only run what the rebalance itself would accept
      if (TableRebalancer.getMinAvailableReplicas(current, target,
          SegmentAssignmentUtils.getSegmentsToMove(current, target), 1, LoggerFactory.getLogger(getClass()))
          != 1) {
        numSkippedAsIllegal++;
        continue;
      }
      String name = String.format("trial %d: servers=%d replication=%d groups=%d segments=%d", trial, numServers,
          replication, numGroups, current.size());
      SimResult result = simulate(new Scenario(name, current, target, 1, true,
          RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, skewedSizes(current.keySet(), random)));
      all.add(result);
      if (!result.relaxedSteps().isEmpty()) {
        relaxed.add(result);
      }
      if (result.hasSteadyStateViolation()) {
        violations.add(result);
      }
      result._groupSplits.forEach(split -> splits.add(name + " | " + split));
    }
    violations.sort((a, b) -> Double.compare(b.worstAmplification(), a.worstAmplification()));
    System.out.println();
    System.out.println(summarize("Strict replica group random group structures", all, violations));
    assertEquals(relaxed.size(), 0, "the budget should not have to be relaxed for any of these assignments");
    System.out.printf("  skipped as illegal minAvailableReplicas: %d%n", numSkippedAsIllegal);
    if (!relaxed.isEmpty()) {
      System.out.printf("%nFOUND %d scenarios where the budget had to be relaxed. Worst:%n", relaxed.size());
      SimResult worst = violations.isEmpty() ? relaxed.get(0) : violations.get(0);
      System.out.println(worst.report());
      dumpScenario(worst._scenario);
    }
    assertEquals(splits, List.of(), "strict replica group requires every group of segments to be moved together");
  }

  /// Prints a scenario as Java source, so a case found by the random search can be pinned as a fixed test.
  private static void dumpScenario(Scenario scenario) {
    System.out.println("---- scenario as source ----");
    scenario._currentAssignment.forEach((segment, instanceStateMap) -> System.out.printf(
        "    current.put(\"%s\", instanceStateMap(%s));%n", segment,
        instanceStateMap.keySet().stream().map(i -> '"' + i + '"').collect(Collectors.joining(", "))));
    scenario._targetAssignment.forEach((segment, instanceStateMap) -> System.out.printf(
        "    target.put(\"%s\", instanceStateMap(%s));%n", segment,
        instanceStateMap.keySet().stream().map(i -> '"' + i + '"').collect(Collectors.joining(", "))));
    scenario._segmentSizeBytes.forEach(
        (segment, sizeBytes) -> System.out.printf("    sizes.put(\"%s\", %dL);%n", segment, sizeBytes));
    System.out.println("---- end ----");
  }

  /// Picks `count` distinct servers at random, in a stable order so the assignment is deterministic per seed.
  private static List<String> pickServers(List<String> allServers, int count, Random random) {
    List<String> shuffled = new ArrayList<>(allServers);
    for (int i = shuffled.size() - 1; i > 0; i--) {
      int j = random.nextInt(i + 1);
      shuffled.set(i, shuffled.set(j, shuffled.get(i)));
    }
    List<String> picked = new ArrayList<>(shuffled.subList(0, Math.min(count, shuffled.size())));
    picked.sort(null);
    return picked;
  }

  /// Sweeps the grid with `batchSizePerServer` set, which is the combination the sweeps above do not cover: batching
  /// can defer a partition for reasons of its own, and the disk budget can defer the rest, so between them a step can
  /// end up making no progress at all.
  @Test
  public void testSweepWithBatchSizePerServer() {
    Random random = new Random(31);
    List<SimResult> violations = new ArrayList<>();
    List<SimResult> all = new ArrayList<>();
    for (int numOldServers = 3; numOldServers <= 7; numOldServers++) {
      for (int shift = 1; shift < numOldServers; shift++) {
        for (int replication = 2; replication <= 3; replication++) {
          for (int batchSizePerServer : List.of(1, 2, 5)) {
            for (boolean enableStrictReplicaGroup : List.of(false, true)) {
              int numSegments = 12 * replication;
              List<String> oldServers = servers(0, numOldServers);
              List<String> newServers = servers(shift, numOldServers);
              Map<String, Map<String, String>> current = roundRobin(numSegments, replication, oldServers, 0);
              String name = String.format("old=%d shift=%d replication=%d batchSizePerServer=%d strict=%s",
                  numOldServers, shift, replication, batchSizePerServer, enableStrictReplicaGroup);
              all.add(simulate(new Scenario(name, current, roundRobin(numSegments, replication, newServers, 0), 1,
                  enableStrictReplicaGroup, batchSizePerServer, skewedSizes(current.keySet(), random))));
            }
          }
        }
      }
    }
    List<String> splits = new ArrayList<>();
    for (SimResult result : all) {
      if (result.hasSteadyStateViolation()) {
        violations.add(result);
      }
      result._groupSplits.forEach(split -> splits.add(result._scenario._name + " | " + split));
    }
    violations.sort((a, b) -> Double.compare(b.worstAmplification(), a.worstAmplification()));
    System.out.println();
    System.out.println(summarize("Sweep with batchSizePerServer and skewed sizes", all, violations));
    for (SimResult result : violations.subList(0, Math.min(5, violations.size()))) {
      System.out.println(result.report());
    }
    assertEquals(splits, List.of(), "strict replica group requires every group of segments to be moved together");
  }

  /// The pre-check replay must agree with what the rebalance actually does: for every scenario, the servers it
  /// reports as forced over their disk have to be exactly the ones the simulation observes going over.
  @Test
  public void testDiskBudgetReplayMatchesTheRebalance() {
    Random random = new Random(23);
    int numChecked = 0;
    for (int numOldServers = 3; numOldServers <= 7; numOldServers++) {
      for (int shift = 1; shift < numOldServers; shift++) {
        for (int replication = 2; replication <= 3; replication++) {
          for (boolean enableStrictReplicaGroup : List.of(false, true)) {
            int numSegments = 12 * replication;
            List<String> oldServers = servers(0, numOldServers);
            List<String> newServers = servers(shift, numOldServers);
            Map<String, Map<String, String>> current = roundRobin(numSegments, replication, oldServers, 0);
            Map<String, Map<String, String>> target = roundRobin(numSegments, replication, newServers, 0);
            Map<String, Long> segmentSizeBytes = skewedSizes(current.keySet(), random);
            String name = String.format("old=%d new=%d replication=%d strict=%s", numOldServers, numOldServers,
                replication, enableStrictReplicaGroup);
            SimResult result = simulate(new Scenario(name, current, target, 1, enableStrictReplicaGroup,
                RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER, segmentSizeBytes));

            Map<String, Long> reported = TableRebalancer.getServersForcedOverDiskBudget(current, target, 1,
                enableStrictReplicaGroup, RebalanceConfig.DISABLE_BATCH_SIZE_PER_SERVER,
                toTableSizeDetails(segmentSizeBytes), LoggerFactory.getLogger(getClass()));
            Set<String> observed = new TreeSet<>();
            result.relaxedSteps().forEach(step -> observed.addAll(step._deferralRelaxedFor));
            assertEquals(reported.keySet(), observed, name + ": pre-check replay disagrees with the rebalance");
            numChecked++;
          }
        }
      }
    }
    System.out.printf("%nDisk budget replay agreed with the rebalance on all %d scenarios%n%n", numChecked);
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
