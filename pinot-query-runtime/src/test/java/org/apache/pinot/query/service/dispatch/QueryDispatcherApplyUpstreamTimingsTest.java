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
package org.apache.pinot.query.service.dispatch;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.physical.FragmentType;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.runtime.operator.BaseMailboxReceiveOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * Unit tests for upstream timing extraction via {@link AdaptiveRoutingStageClassification#classify} and
 * {@link QueryDispatcher#extractMaxTimingsPerInstance}.
 *
 * <p>Each test constructs a {@link QueryDispatcher.QueryResult} with hand-crafted
 * {@code UPSTREAM_SERVER_RESPONSE_TIMES_MS} stats and a matching {@link DispatchableSubPlan},
 * then asserts that {@link ServerRoutingStatsManager#recordStatsUponResponseArrival} is (or is not)
 * called with the expected arguments.
 *
 * <p>Two kinds of stages are consulted: direct pure-leaf receivers ({@code stagesReceivingFromLeaves},
 * including stage 0 for 2-stage queries), and SINGLETON leaf stages themselves
 * ({@code singletonLeafStageIds}). All other stages — including receivers of SINGLETON leaf stages —
 * are excluded to prevent SINGLETON cascade contamination.
 * See {@link AdaptiveRoutingStageClassification} for details.
 */
public class QueryDispatcherApplyUpstreamTimingsTest {

  private static final long REQUEST_ID = 42L;
  private static final int MAILBOX_PORT = 8442;
  private static final ResultTable EMPTY_RESULT =
      new ResultTable(new DataSchema(new String[]{}, new DataSchema.ColumnDataType[]{}), List.of());

  /**
   * Mirrors the 3-step sequence in QueryDispatcher's finally block: classify -> extract -> record.
   * Returns the classification so tests can inspect _trackedServers.
   */
  private static AdaptiveRoutingStageClassification applyUpstreamTimingsFromStats(QueryDispatcher.QueryResult result,
      DispatchableSubPlan plan, ServerRoutingStatsManager statsManager, long requestId,
      Set<String> recordedInstanceIds) {
    return applyUpstreamTimingsFromStats(result, plan, statsManager, requestId, recordedInstanceIds,
        QueryDispatcher.CancelOutcome.NONE);
  }

  private static AdaptiveRoutingStageClassification applyUpstreamTimingsFromStats(QueryDispatcher.QueryResult result,
      DispatchableSubPlan plan, ServerRoutingStatsManager statsManager, long requestId,
      Set<String> recordedInstanceIds, QueryDispatcher.CancelOutcome cancelOutcome) {
    AdaptiveRoutingStageClassification classification = AdaptiveRoutingStageClassification.classify(plan);
    Map<String, Long> maxTimings = QueryDispatcher.extractMaxTimingsPerInstance(
        result, classification, requestId, cancelOutcome);
    for (Map.Entry<String, Long> entry : maxTimings.entrySet()) {
      if (recordedInstanceIds.add(entry.getKey())) {
        statsManager.recordStatsUponResponseArrival(requestId, entry.getKey(), entry.getValue());
      }
    }
    return classification;
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /**
   * Builds a QueryResult whose stage-1 stats contain a MAILBOX_RECEIVE operator with
   * UPSTREAM_SERVER_RESPONSE_TIMES_MS set to {@code encoded}.
   */
  private static QueryDispatcher.QueryResult resultWithStage1Timing(String encoded) {
    StatMap<BaseMailboxReceiveOperator.StatKey> receiveStats =
        new StatMap<>(BaseMailboxReceiveOperator.StatKey.class);
    receiveStats.merge(BaseMailboxReceiveOperator.StatKey.UPSTREAM_SERVER_RESPONSE_TIMES_MS, encoded);

    MultiStageQueryStats.StageStats.Closed stage1 = new MultiStageQueryStats.StageStats.Closed(
        List.of(MultiStageOperator.Type.MAILBOX_RECEIVE), List.of(receiveStats));

    MultiStageQueryStats mqStats = MultiStageQueryStats.emptyStats(0);
    mqStats.mergeUpstream(1, stage1);
    return new QueryDispatcher.QueryResult(EMPTY_RESULT, mqStats, 0L);
  }

  private static DispatchablePlanFragment leafFragment(int receiverStageId, QueryServerInstance... servers) {
    return leafFragmentWithStageId(0, receiverStageId, servers);
  }

  private static DispatchablePlanFragment leafFragmentWithStageId(int stageId, int receiverStageId,
      QueryServerInstance... servers) {
    DispatchablePlanFragment fragment = mock(DispatchablePlanFragment.class);
    Map.Entry<QueryServerInstance, List<Integer>>[] entries = new Map.Entry[servers.length];
    for (int i = 0; i < servers.length; i++) {
      entries[i] = Map.entry(servers[i], List.of(i));
    }
    when(fragment.getServerInstanceToWorkerIdMap()).thenReturn(Map.ofEntries(entries));
    when(fragment.getFragmentType()).thenReturn(FragmentType.LEAF);

    MailboxSendNode sendNode = mock(MailboxSendNode.class);
    when(sendNode.getReceiverStageIds()).thenReturn(List.of(receiverStageId));
    PlanFragment planFragment = mock(PlanFragment.class);
    when(planFragment.getFragmentId()).thenReturn(stageId);
    when(planFragment.getFragmentRoot()).thenReturn(sendNode);
    when(fragment.getPlanFragment()).thenReturn(planFragment);
    return fragment;
  }

  private static DispatchablePlanFragment nonLeafFragment(QueryServerInstance... servers) {
    return nonLeafFragmentWithReceivers(List.of(), servers);
  }

  private static DispatchablePlanFragment nonLeafFragmentWithReceivers(List<Integer> receiverStageIds,
      QueryServerInstance... servers) {
    DispatchablePlanFragment fragment = mock(DispatchablePlanFragment.class);
    Map.Entry<QueryServerInstance, List<Integer>>[] entries = new Map.Entry[servers.length];
    for (int i = 0; i < servers.length; i++) {
      entries[i] = Map.entry(servers[i], List.of(i));
    }
    when(fragment.getServerInstanceToWorkerIdMap()).thenReturn(Map.ofEntries(entries));
    when(fragment.getFragmentType()).thenReturn(FragmentType.INTERMEDIATE);

    if (!receiverStageIds.isEmpty()) {
      MailboxSendNode sendNode = mock(MailboxSendNode.class);
      when(sendNode.getReceiverStageIds()).thenReturn(receiverStageIds);
      PlanFragment planFragment = mock(PlanFragment.class);
      when(planFragment.getFragmentRoot()).thenReturn(sendNode);
      when(fragment.getPlanFragment()).thenReturn(planFragment);
    } else {
      when(fragment.getPlanFragment()).thenReturn(null);
    }
    return fragment;
  }

  /**
   * Wraps one or more fragments into a {@link DispatchableSubPlan} mock.
   */
  private static DispatchableSubPlan planWith(DispatchablePlanFragment... fragments) {
    DispatchableSubPlan plan = mock(DispatchableSubPlan.class);
    TreeSet<DispatchablePlanFragment> fragmentSet = new TreeSet<>(Comparator.comparingInt(System::identityHashCode));
    fragmentSet.addAll(Arrays.asList(fragments));
    when(plan.getQueryStagesWithoutRoot()).thenReturn(fragmentSet);
    return plan;
  }

  // ---------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------

  @Test
  public void testRecordsRealLatencyForKnownIndirectSender() {
    // Stage-1 stats encode that host-a took 600ms. The plan knows host-a as "instance-a" via a
    // leaf fragment that sends to Stage 1.
    QueryServerInstance server = new QueryServerInstance("instance-a", "host-a", 9000, MAILBOX_PORT);
    String encoded = AdaptiveRoutingUpstreamTimings.senderKey("host-a", MAILBOX_PORT) + "=600";

    ServerRoutingStatsManager stats = mock(ServerRoutingStatsManager.class);
    Set<String> recorded = new HashSet<>();

    applyUpstreamTimingsFromStats(
        resultWithStage1Timing(encoded), planWith(leafFragment(1, server)), stats, REQUEST_ID, recorded);

    verify(stats).recordStatsUponResponseArrival(REQUEST_ID, "instance-a", 600L);
    Assert.assertTrue(recorded.contains("instance-a"), "instance-a should be added to recordedInstanceIds");
  }

  @Test
  public void testSkipsUnknownSenderKey() {
    // Stage stats reference a host that isn't in the plan — should not be recorded.
    QueryServerInstance server = new QueryServerInstance("instance-a", "host-a", 9000, MAILBOX_PORT);
    String encoded = AdaptiveRoutingUpstreamTimings.senderKey("host-unknown", MAILBOX_PORT) + "=300";

    ServerRoutingStatsManager stats = mock(ServerRoutingStatsManager.class);

    applyUpstreamTimingsFromStats(
        resultWithStage1Timing(encoded), planWith(leafFragment(1, server)), stats, REQUEST_ID, new HashSet<>());

    verify(stats, never()).recordStatsUponResponseArrival(REQUEST_ID, "instance-a", 300L);
  }


  @Test
  public void testMultipleStagesAndSendersAllProcessed() {
    // Stage 1 receives from leaf-A (host-a); stage 2 receives from leaf-B (host-b). Both recorded.
    QueryServerInstance serverA = new QueryServerInstance("instance-a", "host-a", 9000, MAILBOX_PORT);
    QueryServerInstance serverB = new QueryServerInstance("instance-b", "host-b", 9000, MAILBOX_PORT);

    StatMap<BaseMailboxReceiveOperator.StatKey> s1Stats =
        new StatMap<>(BaseMailboxReceiveOperator.StatKey.class);
    s1Stats.merge(BaseMailboxReceiveOperator.StatKey.UPSTREAM_SERVER_RESPONSE_TIMES_MS,
        AdaptiveRoutingUpstreamTimings.senderKey("host-a", MAILBOX_PORT) + "=500");

    StatMap<BaseMailboxReceiveOperator.StatKey> s2Stats =
        new StatMap<>(BaseMailboxReceiveOperator.StatKey.class);
    s2Stats.merge(BaseMailboxReceiveOperator.StatKey.UPSTREAM_SERVER_RESPONSE_TIMES_MS,
        AdaptiveRoutingUpstreamTimings.senderKey("host-b", MAILBOX_PORT) + "=250");

    MultiStageQueryStats mqStats = MultiStageQueryStats.emptyStats(0);
    mqStats.mergeUpstream(1, new MultiStageQueryStats.StageStats.Closed(
        List.of(MultiStageOperator.Type.MAILBOX_RECEIVE), List.of(s1Stats)));
    mqStats.mergeUpstream(2, new MultiStageQueryStats.StageStats.Closed(
        List.of(MultiStageOperator.Type.MAILBOX_RECEIVE), List.of(s2Stats)));

    QueryDispatcher.QueryResult result = new QueryDispatcher.QueryResult(EMPTY_RESULT, mqStats, 0L);
    ServerRoutingStatsManager stats = mock(ServerRoutingStatsManager.class);
    Set<String> recorded = new HashSet<>();

    // Two leaf fragments: leaf-A sends to Stage 1, leaf-B sends to Stage 2.
    applyUpstreamTimingsFromStats(result,
        planWith(leafFragment(1, serverA), leafFragment(2, serverB)), stats, REQUEST_ID, recorded);

    verify(stats).recordStatsUponResponseArrival(REQUEST_ID, "instance-a", 500L);
    verify(stats).recordStatsUponResponseArrival(REQUEST_ID, "instance-b", 250L);
    Assert.assertTrue(recorded.containsAll(List.of("instance-a", "instance-b")));
  }

  @Test
  public void testIntermediateStageTimingsIgnored() {
    // Stage 1 (leaf receiver) has accurate per-leaf timings for all 3 servers.
    // Stage 0 (broker) has no timing data (it receives from the intermediate, not from leaves).
    //
    // The plan has a leaf fragment (Stage 2) that sends to Stage 1 as its receiver.
    // Stage 1 is consulted because it is a pure-leaf receiver.
    QueryServerInstance serverA = new QueryServerInstance("instance-a", "host-a", 9000, MAILBOX_PORT);
    QueryServerInstance serverB = new QueryServerInstance("instance-b", "host-b", 9000, MAILBOX_PORT);
    QueryServerInstance serverC = new QueryServerInstance("instance-c", "host-c", 9000, MAILBOX_PORT);

    // Stage 0 (broker reduce): no timing data — it receives from the intermediate, not leaves.
    StatMap<BaseMailboxReceiveOperator.StatKey> stage0ReceiveStats =
        new StatMap<>(BaseMailboxReceiveOperator.StatKey.class);

    // Stage 1 (leaf receiver): true per-leaf timings: fast servers at 50ms, slow at 600ms
    StatMap<BaseMailboxReceiveOperator.StatKey> stage1ReceiveStats =
        new StatMap<>(BaseMailboxReceiveOperator.StatKey.class);
    stage1ReceiveStats.merge(BaseMailboxReceiveOperator.StatKey.UPSTREAM_SERVER_RESPONSE_TIMES_MS,
        AdaptiveRoutingUpstreamTimings.senderKey("host-a", MAILBOX_PORT) + "=50;"
        + AdaptiveRoutingUpstreamTimings.senderKey("host-b", MAILBOX_PORT) + "=50;"
        + AdaptiveRoutingUpstreamTimings.senderKey("host-c", MAILBOX_PORT) + "=600");
    MultiStageQueryStats.StageStats.Closed stage1Closed = new MultiStageQueryStats.StageStats.Closed(
        List.of(MultiStageOperator.Type.MAILBOX_RECEIVE), List.of(stage1ReceiveStats));

    MultiStageQueryStats mqStats = MultiStageQueryStats.emptyStats(0);
    mqStats.getCurrentStats().addLastOperator(MultiStageOperator.Type.MAILBOX_RECEIVE, stage0ReceiveStats);
    mqStats.mergeUpstream(1, stage1Closed);
    QueryDispatcher.QueryResult result = new QueryDispatcher.QueryResult(EMPTY_RESULT, mqStats, 0L);

    // Plan: leaf fragment (Stage 2) sends to Stage 1; non-leaf fragment (Stage 1) holds the servers.
    // Both fragments list servers A/B/C so senderKeyToInstanceId is fully populated.
    ServerRoutingStatsManager stats = mock(ServerRoutingStatsManager.class);
    Set<String> recorded = new HashSet<>();

    applyUpstreamTimingsFromStats(result,
        planWith(leafFragment(1, serverA, serverB, serverC), nonLeafFragment(serverA, serverB, serverC)),
        stats, REQUEST_ID, recorded);

    // Accurate leaf timings from Stage 1 must be recorded.
    verify(stats).recordStatsUponResponseArrival(REQUEST_ID, "instance-a", 50L);
    verify(stats).recordStatsUponResponseArrival(REQUEST_ID, "instance-b", 50L);
    verify(stats).recordStatsUponResponseArrival(REQUEST_ID, "instance-c", 600L);
  }

  @Test
  public void testSameServerInMultipleLeafStagesTakesMax() {
    // Server A participates in two leaf stages: Stage 3 (50ms) and Stage 5 (300ms).
    // Both leaf fragments send to different receivers (Stage 2 and Stage 4 respectively).
    // The maximum of the two leaf observations (300ms) should be recorded.
    QueryServerInstance serverA = new QueryServerInstance("instance-a", "host-a", 9000, MAILBOX_PORT);

    StatMap<BaseMailboxReceiveOperator.StatKey> stage2Stats =
        new StatMap<>(BaseMailboxReceiveOperator.StatKey.class);
    stage2Stats.merge(BaseMailboxReceiveOperator.StatKey.UPSTREAM_SERVER_RESPONSE_TIMES_MS,
        AdaptiveRoutingUpstreamTimings.senderKey("host-a", MAILBOX_PORT) + "=50");

    StatMap<BaseMailboxReceiveOperator.StatKey> stage4Stats =
        new StatMap<>(BaseMailboxReceiveOperator.StatKey.class);
    stage4Stats.merge(BaseMailboxReceiveOperator.StatKey.UPSTREAM_SERVER_RESPONSE_TIMES_MS,
        AdaptiveRoutingUpstreamTimings.senderKey("host-a", MAILBOX_PORT) + "=300");

    MultiStageQueryStats mqStats = MultiStageQueryStats.emptyStats(0);
    mqStats.mergeUpstream(2, new MultiStageQueryStats.StageStats.Closed(
        List.of(MultiStageOperator.Type.MAILBOX_RECEIVE), List.of(stage2Stats)));
    mqStats.mergeUpstream(4, new MultiStageQueryStats.StageStats.Closed(
        List.of(MultiStageOperator.Type.MAILBOX_RECEIVE), List.of(stage4Stats)));

    QueryDispatcher.QueryResult result = new QueryDispatcher.QueryResult(EMPTY_RESULT, mqStats, 0L);
    ServerRoutingStatsManager stats = mock(ServerRoutingStatsManager.class);
    Set<String> recorded = new HashSet<>();

    // Two leaf fragments for server A: one sends to Stage 2, another sends to Stage 4.
    applyUpstreamTimingsFromStats(result,
        planWith(leafFragment(2, serverA), leafFragment(4, serverA)), stats, REQUEST_ID, recorded);

    // The max of the two leaf observations (300ms) should be the single recorded value.
    verify(stats).recordStatsUponResponseArrival(REQUEST_ID, "instance-a", 300L);
  }

  private static DispatchablePlanFragment singletonLeafFragment(int leafStageId, boolean timingTrusted,
      QueryServerInstance... servers) {
    return singletonLeafFragment(leafStageId, timingTrusted, List.of(), servers);
  }

  private static DispatchablePlanFragment singletonLeafFragment(int leafStageId, boolean timingTrusted,
      List<Integer> receiverStageIds, QueryServerInstance... servers) {
    DispatchablePlanFragment fragment = mock(DispatchablePlanFragment.class);
    Map.Entry<QueryServerInstance, List<Integer>>[] entries = new Map.Entry[servers.length];
    for (int i = 0; i < servers.length; i++) {
      entries[i] = Map.entry(servers[i], List.of(i));
    }
    when(fragment.getServerInstanceToWorkerIdMap()).thenReturn(Map.ofEntries(entries));
    when(fragment.getFragmentType()).thenReturn(
        timingTrusted ? FragmentType.SINGLETON_LEAF : FragmentType.INTERMEDIATE);

    MailboxSendNode sendNode = mock(MailboxSendNode.class);
    when(sendNode.getReceiverStageIds()).thenReturn(receiverStageIds);
    PlanFragment planFragment = mock(PlanFragment.class);
    when(planFragment.getFragmentId()).thenReturn(leafStageId);
    when(planFragment.getFragmentRoot()).thenReturn(sendNode);
    when(fragment.getPlanFragment()).thenReturn(planFragment);
    return fragment;
  }

  @Test
  public void testMixedLeafWithSingletonReceiveIsSkipped() {
    // A SINGLETON leaf fragment (dim table lookup-join). Its receiver stage 1 is NOT trusted
    // (SINGLETON receivers are contaminated). Its servers are NOT tracked.
    QueryServerInstance server = new QueryServerInstance("instance-a", "host-a", 9000, MAILBOX_PORT);
    String encoded = AdaptiveRoutingUpstreamTimings.senderKey("host-a", MAILBOX_PORT) + "=800";

    ServerRoutingStatsManager stats = mock(ServerRoutingStatsManager.class);
    Set<String> recorded = new HashSet<>();

    // SINGLETON leaf (stageId=2), not timing-trusted (sender is not a leaf in this test).
    AdaptiveRoutingStageClassification classification = applyUpstreamTimingsFromStats(
        resultWithStage1Timing(encoded),
        planWith(singletonLeafFragment(2, false, server)),
        stats, REQUEST_ID, recorded);

    // Stage 1 (receiver) is not consulted — the inflated 800ms timing is never recorded.
    verify(stats, never()).recordStatsUponResponseArrival(REQUEST_ID, "instance-a", 800L);
    verify(stats, never()).recordStatsUponResponseArrival(REQUEST_ID, "instance-a", -1L);
    Assert.assertFalse(classification._trackedServers.contains("instance-a"),
        "instance-a must NOT be in trackedServers so the finally block records -1");
  }


  @Test
  public void testPureLeavesStillRecordedWhenMixedLeafAlsoPresent() {
    // When a query has both a pure leaf (fact table) and a SINGLETON leaf (dim lookup),
    // the pure leaf timing is still recorded correctly. The SINGLETON leaf is silently skipped.
    QueryServerInstance server = new QueryServerInstance("instance-a", "host-a", 9000, MAILBOX_PORT);

    // Stage 1 receives from the pure leaf (trusted receiver).
    // Stage 2 receives from the SINGLETON leaf (not trusted).
    StatMap<BaseMailboxReceiveOperator.StatKey> stage1Stats =
        new StatMap<>(BaseMailboxReceiveOperator.StatKey.class);
    stage1Stats.merge(BaseMailboxReceiveOperator.StatKey.UPSTREAM_SERVER_RESPONSE_TIMES_MS,
        AdaptiveRoutingUpstreamTimings.senderKey("host-a", MAILBOX_PORT) + "=90");

    StatMap<BaseMailboxReceiveOperator.StatKey> stage2Stats =
        new StatMap<>(BaseMailboxReceiveOperator.StatKey.class);
    stage2Stats.merge(BaseMailboxReceiveOperator.StatKey.UPSTREAM_SERVER_RESPONSE_TIMES_MS,
        AdaptiveRoutingUpstreamTimings.senderKey("host-a", MAILBOX_PORT) + "=950");

    MultiStageQueryStats mqStats = MultiStageQueryStats.emptyStats(0);
    mqStats.mergeUpstream(1, new MultiStageQueryStats.StageStats.Closed(
        List.of(MultiStageOperator.Type.MAILBOX_RECEIVE), List.of(stage1Stats)));
    mqStats.mergeUpstream(2, new MultiStageQueryStats.StageStats.Closed(
        List.of(MultiStageOperator.Type.MAILBOX_RECEIVE), List.of(stage2Stats)));

    QueryDispatcher.QueryResult result = new QueryDispatcher.QueryResult(EMPTY_RESULT, mqStats, 0L);
    ServerRoutingStatsManager stats = mock(ServerRoutingStatsManager.class);
    Set<String> recorded = new HashSet<>();

    // Pure leaf sends to stage 1 (trusted). SINGLETON leaf (stage 3) has no trusted receivers.
    applyUpstreamTimingsFromStats(result,
        planWith(leafFragment(1, server), singletonLeafFragment(3, false, server)),
        stats, REQUEST_ID, recorded);

    // Pure leaf timing (90ms) recorded, NOT the inflated SINGLETON timing (950ms).
    verify(stats).recordStatsUponResponseArrival(REQUEST_ID, "instance-a", 90L);
    verify(stats, never()).recordStatsUponResponseArrival(REQUEST_ID, "instance-a", 950L);
  }

  @Test
  public void testSingletonLeafStageItselfIsConsultedWhenSenderIsLeaf() {
    QueryServerInstance impacted = new QueryServerInstance("impacted", "host-impacted", 9000, MAILBOX_PORT);
    QueryServerInstance normal = new QueryServerInstance("normal", "host-normal", 9000, MAILBOX_PORT);

    // Stage 4 (the SINGLETON leaf stage) receives from stage 5 (a pure leaf scan stage).
    StatMap<BaseMailboxReceiveOperator.StatKey> stage4Stats =
        new StatMap<>(BaseMailboxReceiveOperator.StatKey.class);
    stage4Stats.merge(BaseMailboxReceiveOperator.StatKey.UPSTREAM_SERVER_RESPONSE_TIMES_MS,
        AdaptiveRoutingUpstreamTimings.senderKey("host-impacted", MAILBOX_PORT) + "=911;"
        + AdaptiveRoutingUpstreamTimings.senderKey("host-normal", MAILBOX_PORT) + "=2");

    MultiStageQueryStats mqStats = MultiStageQueryStats.emptyStats(0);
    mqStats.mergeUpstream(4, new MultiStageQueryStats.StageStats.Closed(
        List.of(MultiStageOperator.Type.MAILBOX_RECEIVE), List.of(stage4Stats)));
    QueryDispatcher.QueryResult result = new QueryDispatcher.QueryResult(EMPTY_RESULT, mqStats, 0L);

    // SINGLETON leaf at stageId=4, marked TIMING_TRUSTED (sender stage 5 is a leaf).
    // Stage 5 is a pure leaf running on the same servers.
    DispatchablePlanFragment singletonLeaf = singletonLeafFragment(4, true, impacted, normal);
    DispatchablePlanFragment senderLeaf = leafFragmentWithStageId(5, 4, impacted, normal);
    DispatchablePlanFragment intermediate = nonLeafFragment(impacted, normal);

    ServerRoutingStatsManager stats = mock(ServerRoutingStatsManager.class);
    Set<String> recorded = new HashSet<>();

    applyUpstreamTimingsFromStats(
        result, planWith(singletonLeaf, senderLeaf, intermediate), stats, REQUEST_ID, recorded);

    // Stage 4 is TIMING_TRUSTED -> its stats are consulted.
    verify(stats).recordStatsUponResponseArrival(REQUEST_ID, "impacted", 911L);
    verify(stats).recordStatsUponResponseArrival(REQUEST_ID, "normal", 2L);
    Assert.assertTrue(recorded.containsAll(List.of("impacted", "normal")));
  }


  /**
   * Covers the relay/intermediate-server contamination case (e.g. a SINGLETON relay stage-1 server
   * like {@code 055f50e85c4876db1} that waits for a slow upstream and therefore has an inflated
   * wall-clock elapsed time).  The intermediate server must NOT be in {@code _trackedServers} so
   * the caller's finally block records it at -1 (not at wall-clock).
   *
   * <p>Topology: leaf (stage 2) sends non-SINGLETON to stage 1 (intermediate relay). Stage 1's
   * UPSTREAM_SERVER_RESPONSE_TIMES_MS gives accurate leaf timings. The relay server runs only the
   * intermediate stage ({@code nonLeafFragment}) and must NOT have a real latency recorded for it.
   */
  @Test
  public void testIntermediateRelayServerNotInTrackedServers() {
    QueryServerInstance leafServer = new QueryServerInstance("leaf-instance", "host-leaf", 9000, MAILBOX_PORT);
    QueryServerInstance relayServer = new QueryServerInstance("relay-instance", "host-relay", 9000, MAILBOX_PORT);

    // Stage 1 (intermediate relay) records leaf server timing accurately.
    String encoded = AdaptiveRoutingUpstreamTimings.senderKey("host-leaf", MAILBOX_PORT) + "=80";

    ServerRoutingStatsManager stats = mock(ServerRoutingStatsManager.class);
    Set<String> recorded = new HashSet<>();

    // leaf fragment sends to stage 1; relay server is in a non-leaf intermediate fragment.
    AdaptiveRoutingStageClassification classification = applyUpstreamTimingsFromStats(
        resultWithStage1Timing(encoded),
        planWith(leafFragment(1, leafServer), nonLeafFragment(relayServer)),
        stats, REQUEST_ID, recorded);

    // Leaf server gets its real latency recorded.
    verify(stats).recordStatsUponResponseArrival(REQUEST_ID, "leaf-instance", 80L);
    // Relay server is NOT recorded by applyUpstreamTimingsFromStats — the caller's finally block
    // will handle it (not in trackedServers, so it records -1).
    verify(stats, never()).recordStatsUponResponseArrival(REQUEST_ID, "relay-instance", -1L);
    Assert.assertFalse(recorded.contains("relay-instance"),
        "relay-instance must NOT be in recordedInstanceIds (handled by caller)");
    Assert.assertFalse(classification._trackedServers.contains("relay-instance"),
        "relay-instance must NOT be in trackedServers so the finally block records -1");
  }

  @Test
  public void testSingletonLeafReceivingFromIntermediateNotConsulted() {
    QueryServerInstance serverA = new QueryServerInstance("server-a", "host-a", 9000, MAILBOX_PORT);
    QueryServerInstance serverB = new QueryServerInstance("server-b", "host-b", 9000, MAILBOX_PORT);

    // Stage 2 (SINGLETON leaf) records timing for its stage-3 upstream senders.
    StatMap<BaseMailboxReceiveOperator.StatKey> stage2Stats =
        new StatMap<>(BaseMailboxReceiveOperator.StatKey.class);
    stage2Stats.merge(BaseMailboxReceiveOperator.StatKey.UPSTREAM_SERVER_RESPONSE_TIMES_MS,
        AdaptiveRoutingUpstreamTimings.senderKey("host-a", MAILBOX_PORT) + "=600;"
        + AdaptiveRoutingUpstreamTimings.senderKey("host-b", MAILBOX_PORT) + "=10");

    MultiStageQueryStats mqStats = MultiStageQueryStats.emptyStats(0);
    mqStats.mergeUpstream(2, new MultiStageQueryStats.StageStats.Closed(
        List.of(MultiStageOperator.Type.MAILBOX_RECEIVE), List.of(stage2Stats)));
    QueryDispatcher.QueryResult result = new QueryDispatcher.QueryResult(EMPTY_RESULT, mqStats, 0L);

    // Stage 2 is a SINGLETON leaf. NOT timing-trusted (sender stage 3 is intermediate, not a leaf).
    DispatchablePlanFragment singletonLeaf = singletonLeafFragment(2, false, serverA, serverB);
    DispatchablePlanFragment intermediate = nonLeafFragment(serverA, serverB);

    ServerRoutingStatsManager stats = mock(ServerRoutingStatsManager.class);
    Set<String> recorded = new HashSet<>();

    AdaptiveRoutingStageClassification classification = applyUpstreamTimingsFromStats(
        result, planWith(singletonLeaf, intermediate), stats, REQUEST_ID, recorded);

    // Stage 2 is NOT consulted (not timing-trusted) -> contaminated timings never recorded.
    verify(stats, never()).recordStatsUponResponseArrival(REQUEST_ID, "server-a", 600L);
    verify(stats, never()).recordStatsUponResponseArrival(REQUEST_ID, "server-b", 10L);
    verify(stats, never()).recordStatsUponResponseArrival(REQUEST_ID, "server-a", -1L);
    verify(stats, never()).recordStatsUponResponseArrival(REQUEST_ID, "server-b", -1L);
    Assert.assertFalse(classification._trackedServers.contains("server-a"));
    Assert.assertFalse(classification._trackedServers.contains("server-b"));
  }

  // ---------------------------------------------------------------------------
  // Cancel stats tests
  // ---------------------------------------------------------------------------

  @Test
  public void testCancelStatsProvideTimingsForStagesMissingFromResult() {
    // Stage 1 timings come from the normal query result. Stage 2 timings come only from cancelStats
    // (the broker didn't see stage 2 during normal reduce because the query was cancelled early).
    QueryServerInstance serverA = new QueryServerInstance("instance-a", "host-a", 9000, MAILBOX_PORT);
    QueryServerInstance serverB = new QueryServerInstance("instance-b", "host-b", 9000, MAILBOX_PORT);

    // Normal result has stage 1 timing for server A.
    StatMap<BaseMailboxReceiveOperator.StatKey> stage1Stats =
        new StatMap<>(BaseMailboxReceiveOperator.StatKey.class);
    stage1Stats.merge(BaseMailboxReceiveOperator.StatKey.UPSTREAM_SERVER_RESPONSE_TIMES_MS,
        AdaptiveRoutingUpstreamTimings.senderKey("host-a", MAILBOX_PORT) + "=100");

    MultiStageQueryStats mqStats = MultiStageQueryStats.emptyStats(0);
    mqStats.mergeUpstream(1, new MultiStageQueryStats.StageStats.Closed(
        List.of(MultiStageOperator.Type.MAILBOX_RECEIVE), List.of(stage1Stats)));
    QueryDispatcher.QueryResult result = new QueryDispatcher.QueryResult(EMPTY_RESULT, mqStats, 0L);

    // Cancel stats have stage 2 timing for server B (gathered during cancel).
    StatMap<BaseMailboxReceiveOperator.StatKey> cancelStage2Stats =
        new StatMap<>(BaseMailboxReceiveOperator.StatKey.class);
    cancelStage2Stats.merge(BaseMailboxReceiveOperator.StatKey.UPSTREAM_SERVER_RESPONSE_TIMES_MS,
        AdaptiveRoutingUpstreamTimings.senderKey("host-b", MAILBOX_PORT) + "=450");
    MultiStageQueryStats cancelStats = MultiStageQueryStats.emptyStats(0);
    cancelStats.mergeUpstream(2, new MultiStageQueryStats.StageStats.Closed(
        List.of(MultiStageOperator.Type.MAILBOX_RECEIVE), List.of(cancelStage2Stats)));

    // Plan: leaf-A sends to stage 1, leaf-B sends to stage 2.
    ServerRoutingStatsManager stats = mock(ServerRoutingStatsManager.class);
    Set<String> recorded = new HashSet<>();

    applyUpstreamTimingsFromStats(result,
        planWith(leafFragment(1, serverA), leafFragment(2, serverB)),
        stats, REQUEST_ID, recorded, new QueryDispatcher.CancelOutcome(cancelStats, Set.of()));

    verify(stats).recordStatsUponResponseArrival(REQUEST_ID, "instance-a", 100L);
    verify(stats).recordStatsUponResponseArrival(REQUEST_ID, "instance-b", 450L);
    Assert.assertTrue(recorded.containsAll(List.of("instance-a", "instance-b")));
  }

  @Test
  public void testCancelStatsMergedWithResultTakingMax() {
    // Same server appears in both the normal result (stage 1) and cancelStats (stage 2).
    // extractMaxTimingsPerInstance should take the max across both sources.
    QueryServerInstance server = new QueryServerInstance("instance-a", "host-a", 9000, MAILBOX_PORT);

    // Normal result: stage 1 reports server at 200ms.
    StatMap<BaseMailboxReceiveOperator.StatKey> stage1Stats =
        new StatMap<>(BaseMailboxReceiveOperator.StatKey.class);
    stage1Stats.merge(BaseMailboxReceiveOperator.StatKey.UPSTREAM_SERVER_RESPONSE_TIMES_MS,
        AdaptiveRoutingUpstreamTimings.senderKey("host-a", MAILBOX_PORT) + "=200");

    MultiStageQueryStats mqStats = MultiStageQueryStats.emptyStats(0);
    mqStats.mergeUpstream(1, new MultiStageQueryStats.StageStats.Closed(
        List.of(MultiStageOperator.Type.MAILBOX_RECEIVE), List.of(stage1Stats)));
    QueryDispatcher.QueryResult result = new QueryDispatcher.QueryResult(EMPTY_RESULT, mqStats, 0L);

    // Cancel stats: stage 2 reports same server at 700ms.
    StatMap<BaseMailboxReceiveOperator.StatKey> cancelStage2Stats =
        new StatMap<>(BaseMailboxReceiveOperator.StatKey.class);
    cancelStage2Stats.merge(BaseMailboxReceiveOperator.StatKey.UPSTREAM_SERVER_RESPONSE_TIMES_MS,
        AdaptiveRoutingUpstreamTimings.senderKey("host-a", MAILBOX_PORT) + "=700");
    MultiStageQueryStats cancelStats = MultiStageQueryStats.emptyStats(0);
    cancelStats.mergeUpstream(2, new MultiStageQueryStats.StageStats.Closed(
        List.of(MultiStageOperator.Type.MAILBOX_RECEIVE), List.of(cancelStage2Stats)));

    // Plan: leaf sends to both stage 1 and stage 2.
    ServerRoutingStatsManager stats = mock(ServerRoutingStatsManager.class);
    Set<String> recorded = new HashSet<>();

    applyUpstreamTimingsFromStats(result,
        planWith(leafFragment(1, server), leafFragment(2, server)),
        stats, REQUEST_ID, recorded, new QueryDispatcher.CancelOutcome(cancelStats, Set.of()));

    // Max of 200 and 700 -> 700ms.
    verify(stats).recordStatsUponResponseArrival(REQUEST_ID, "instance-a", 700L);
  }

  @Test
  public void testCancelStatsUntrustedStagesSkipped() {
    // Cancel stats contain timings for a non-trusted stage (intermediate). These should be ignored.
    QueryServerInstance server = new QueryServerInstance("instance-a", "host-a", 9000, MAILBOX_PORT);

    // Empty normal result (no upstream stage stats).
    MultiStageQueryStats mqStats = MultiStageQueryStats.emptyStats(0);
    QueryDispatcher.QueryResult result = new QueryDispatcher.QueryResult(EMPTY_RESULT, mqStats, 0L);

    // Cancel stats have stage 1 timing, but stage 1 is NOT a trusted stage (no leaf sends to it).
    StatMap<BaseMailboxReceiveOperator.StatKey> cancelStage1Stats =
        new StatMap<>(BaseMailboxReceiveOperator.StatKey.class);
    cancelStage1Stats.merge(BaseMailboxReceiveOperator.StatKey.UPSTREAM_SERVER_RESPONSE_TIMES_MS,
        AdaptiveRoutingUpstreamTimings.senderKey("host-a", MAILBOX_PORT) + "=500");
    MultiStageQueryStats cancelStats = MultiStageQueryStats.emptyStats(0);
    cancelStats.mergeUpstream(1, new MultiStageQueryStats.StageStats.Closed(
        List.of(MultiStageOperator.Type.MAILBOX_RECEIVE), List.of(cancelStage1Stats)));

    // Plan: only a non-leaf fragment (no leaf sends to stage 1).
    ServerRoutingStatsManager stats = mock(ServerRoutingStatsManager.class);
    Set<String> recorded = new HashSet<>();

    applyUpstreamTimingsFromStats(result,
        planWith(nonLeafFragment(server)),
        stats, REQUEST_ID, recorded, new QueryDispatcher.CancelOutcome(cancelStats, Set.of()));

    verify(stats, never()).recordStatsUponResponseArrival(REQUEST_ID, "instance-a", 500L);
    Assert.assertTrue(recorded.isEmpty());
  }
}
