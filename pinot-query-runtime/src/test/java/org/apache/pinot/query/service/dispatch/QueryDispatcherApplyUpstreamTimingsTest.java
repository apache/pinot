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
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
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
    AdaptiveRoutingStageClassification classification = AdaptiveRoutingStageClassification.classify(plan);
    Map<String, Long> maxTimings = QueryDispatcher.extractMaxTimingsPerInstance(result, classification, requestId);
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

  /**
   * Builds a leaf {@link DispatchablePlanFragment} mock: has a non-null table name (leaf stage)
   * and a plan root that advertises {@code receiverStageId} as its receiver.
   */
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
    when(fragment.getTableName()).thenReturn("testTable");

    MailboxSendNode sendNode = mock(MailboxSendNode.class);
    when(sendNode.getStageId()).thenReturn(stageId);
    when(sendNode.getReceiverStageIds()).thenReturn(List.of(receiverStageId));
    PlanFragment planFragment = mock(PlanFragment.class);
    when(planFragment.getFragmentRoot()).thenReturn(sendNode);
    when(fragment.getPlanFragment()).thenReturn(planFragment);
    return fragment;
  }

  /**
   * Builds a non-leaf {@link DispatchablePlanFragment} mock: has a null table name (intermediate stage).
   * Used to populate the senderKey -> instanceId lookup without contributing leaf-stage receiver info.
   */
  private static DispatchablePlanFragment nonLeafFragment(QueryServerInstance... servers) {
    DispatchablePlanFragment fragment = mock(DispatchablePlanFragment.class);
    Map.Entry<QueryServerInstance, List<Integer>>[] entries = new Map.Entry[servers.length];
    for (int i = 0; i < servers.length; i++) {
      entries[i] = Map.entry(servers[i], List.of(i));
    }
    when(fragment.getServerInstanceToWorkerIdMap()).thenReturn(Map.ofEntries(entries));
    when(fragment.getTableName()).thenReturn(null);
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
    // Stage 0 (broker) has inflated timings for all 3 servers (in their Stage 1 intermediate role).
    // Stage 1 (intermediate) has accurate leaf timings for all 3 servers (in their Stage 2 leaf role).
    //
    // The plan has a leaf fragment (Stage 2) that sends to Stage 1 as its receiver.
    // Stage 1 is consulted because it is a pure-leaf receiver (in stagesReceivingFromLeaves).
    // Stage 0 is excluded because it is not a leaf receiver (the leaf sends to stage 1, not stage 0)
    // and is not in singletonLeafStageIds.
    QueryServerInstance serverA = new QueryServerInstance("instance-a", "host-a", 9000, MAILBOX_PORT);
    QueryServerInstance serverB = new QueryServerInstance("instance-b", "host-b", 9000, MAILBOX_PORT);
    QueryServerInstance serverC = new QueryServerInstance("instance-c", "host-c", 9000, MAILBOX_PORT);

    // Stage 0 (broker reduce): all 3 servers reported as ~600ms (inflated, each waited for slow server-c)
    StatMap<BaseMailboxReceiveOperator.StatKey> stage0ReceiveStats =
        new StatMap<>(BaseMailboxReceiveOperator.StatKey.class);
    stage0ReceiveStats.merge(BaseMailboxReceiveOperator.StatKey.UPSTREAM_SERVER_RESPONSE_TIMES_MS,
        AdaptiveRoutingUpstreamTimings.senderKey("host-a", MAILBOX_PORT) + "=600;"
        + AdaptiveRoutingUpstreamTimings.senderKey("host-b", MAILBOX_PORT) + "=600;"
        + AdaptiveRoutingUpstreamTimings.senderKey("host-c", MAILBOX_PORT) + "=600");

    // Stage 1 (intermediate receivers): true per-leaf timings: fast servers at 50ms, slow at 600ms
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

    // Accurate leaf timings must be recorded for fast servers.
    verify(stats).recordStatsUponResponseArrival(REQUEST_ID, "instance-a", 50L);
    verify(stats).recordStatsUponResponseArrival(REQUEST_ID, "instance-b", 50L);
    verify(stats).recordStatsUponResponseArrival(REQUEST_ID, "instance-c", 600L);
    // Inflated Stage 0 observations must never reach the stats manager.
    verify(stats, never()).recordStatsUponResponseArrival(REQUEST_ID, "instance-a", 600L);
    verify(stats, never()).recordStatsUponResponseArrival(REQUEST_ID, "instance-b", 600L);
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

  /**
   * Builds a "mixed" leaf fragment: {@code getTableName()} is non-null (scans a dim table), but the plan
   * subtree also contains a {@link MailboxReceiveNode} with the given {@code receiveDistribution}. This
   * mirrors the lookup-join pattern where a stage both receives all upstream data via a SINGLETON exchange
   * and scans an inline dimension table.
   *
   * @param leafStageId  the stage ID of this leaf fragment itself (used as the SINGLETON leaf stage ID
   *                     when {@code receiveDistribution} is SINGLETON)
   * @param receiverStageId  the stage ID that this leaf sends its output to
   */
  private static DispatchablePlanFragment mixedLeafFragment(int leafStageId, int receiverStageId,
      RelDistribution.Type receiveDistribution, QueryServerInstance... servers) {
    return mixedLeafFragment(leafStageId, receiverStageId, receiveDistribution, 98, servers);
  }

  private static DispatchablePlanFragment mixedLeafFragment(int leafStageId, int receiverStageId,
      RelDistribution.Type receiveDistribution, int senderStageId, QueryServerInstance... servers) {
    DispatchablePlanFragment fragment = mock(DispatchablePlanFragment.class);
    Map.Entry<QueryServerInstance, List<Integer>>[] entries = new Map.Entry[servers.length];
    for (int i = 0; i < servers.length; i++) {
      entries[i] = Map.entry(servers[i], List.of(i));
    }
    when(fragment.getServerInstanceToWorkerIdMap()).thenReturn(Map.ofEntries(entries));
    when(fragment.getTableName()).thenReturn("dimTable");

    // A real MailboxReceiveNode with the given distribution type, representing the upstream receive.
    MailboxReceiveNode receiveNode = new MailboxReceiveNode(
        /*stageId=*/99, new DataSchema(new String[]{}, new DataSchema.ColumnDataType[]{}),
        senderStageId, PinotRelExchangeType.getDefaultExchangeType(),
        receiveDistribution, null, null, false, false, null);

    // A mock MailboxSendNode whose subtree contains the receive node.
    MailboxSendNode sendNode = mock(MailboxSendNode.class);
    when(sendNode.getStageId()).thenReturn(leafStageId);
    when(sendNode.getReceiverStageIds()).thenReturn(List.of(receiverStageId));
    when(sendNode.getInputs()).thenReturn(List.of(receiveNode));

    PlanFragment planFragment = mock(PlanFragment.class);
    when(planFragment.getFragmentRoot()).thenReturn(sendNode);
    when(fragment.getPlanFragment()).thenReturn(planFragment);
    return fragment;
  }

  @Test
  public void testMixedLeafWithSingletonReceiveIsSkipped() {
    // A fragment that has getTableName() != null (dim table scan) AND a SINGLETON MailboxReceiveNode
    // (lookup-join pattern). Stage 1 is not consulted (the 800ms timing is never recorded).
    // The SINGLETON stage's server is NOT in _trackedServers, so the caller's finally block records
    // it at -1 (not at wall-clock) to avoid cascade contamination.
    QueryServerInstance server = new QueryServerInstance("instance-a", "host-a", 9000, MAILBOX_PORT);
    String encoded = AdaptiveRoutingUpstreamTimings.senderKey("host-a", MAILBOX_PORT) + "=800";

    ServerRoutingStatsManager stats = mock(ServerRoutingStatsManager.class);
    Set<String> recorded = new HashSet<>();

    // Stage 1 has timing, but the only "leaf" fragment is a mixed stage (leafStageId=2) with
    // SINGLETON receive that sends to stage 1.
    AdaptiveRoutingStageClassification classification = applyUpstreamTimingsFromStats(
        resultWithStage1Timing(encoded),
        planWith(mixedLeafFragment(2, 1, RelDistribution.Type.SINGLETON, server)),
        stats, REQUEST_ID, recorded);

    // Stage 1 (receiver) is not consulted — the inflated 800ms timing is never recorded.
    verify(stats, never()).recordStatsUponResponseArrival(REQUEST_ID, "instance-a", 800L);
    // No -1 pre-recording inside applyUpstreamTimingsFromStats — that's the caller's job.
    verify(stats, never()).recordStatsUponResponseArrival(REQUEST_ID, "instance-a", -1L);
    // But the server is NOT in trackedServers, so the caller's fallback will record -1.
    Assert.assertFalse(classification._trackedServers.contains("instance-a"),
        "instance-a must NOT be in trackedServers so the finally block records -1");
  }


  @Test
  public void testPureLeavesStillRecordedWhenMixedLeafAlsoPresent() {
    // When a query has both a pure leaf (fact table) and a mixed leaf (inline dim with SINGLETON receive),
    // the pure leaf timing is still recorded correctly. The mixed leaf is silently skipped.
    QueryServerInstance server = new QueryServerInstance("instance-a", "host-a", 9000, MAILBOX_PORT);

    // Stage 1 receives from the pure leaf (will be in stagesReceivingFromLeaves).
    // Stage 2 receives from the mixed leaf (must be excluded).
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

    applyUpstreamTimingsFromStats(result,
        planWith(leafFragment(1, server), mixedLeafFragment(3, 2, RelDistribution.Type.SINGLETON, server)),
        stats, REQUEST_ID, recorded);

    // Pure leaf timing (90ms) recorded, NOT the inflated mixed-leaf timing (950ms).
    verify(stats).recordStatsUponResponseArrival(REQUEST_ID, "instance-a", 90L);
    verify(stats, never()).recordStatsUponResponseArrival(REQUEST_ID, "instance-a", 950L);
  }

  /**
   * Models the 10-stage canary case where all leaf stages are "mixed" SINGLETON lookup-join stages,
   * so {@code stagesReceivingFromLeaves} is empty. The slow server is identified by its slow network
   * sends: stage-4 SINGLETON workers at remote servers record the slow server's stage-5 data as
   * arriving late. Stage 4 (the SINGLETON leaf stage itself) is in {@code singletonLeafStageIds}
   * and is directly consulted, giving clean per-server upstream-send timings.
   */
  @Test
  public void testSingletonLeafStageItselfIsConsultedWhenSenderIsLeaf() {
    QueryServerInstance impacted = new QueryServerInstance("impacted", "host-impacted", 9000, MAILBOX_PORT);
    QueryServerInstance normal = new QueryServerInstance("normal", "host-normal", 9000, MAILBOX_PORT);

    // Stage 4 (the SINGLETON leaf stage) receives from stage 5 (a pure leaf scan stage).
    // It records stage-5 leaf senders' timings: the impacted server's data took 911ms to arrive;
    // the normal server's data arrived in 2ms.
    StatMap<BaseMailboxReceiveOperator.StatKey> stage4Stats =
        new StatMap<>(BaseMailboxReceiveOperator.StatKey.class);
    stage4Stats.merge(BaseMailboxReceiveOperator.StatKey.UPSTREAM_SERVER_RESPONSE_TIMES_MS,
        AdaptiveRoutingUpstreamTimings.senderKey("host-impacted", MAILBOX_PORT) + "=911;"
        + AdaptiveRoutingUpstreamTimings.senderKey("host-normal", MAILBOX_PORT) + "=2");

    MultiStageQueryStats mqStats = MultiStageQueryStats.emptyStats(0);
    mqStats.mergeUpstream(4, new MultiStageQueryStats.StageStats.Closed(
        List.of(MultiStageOperator.Type.MAILBOX_RECEIVE), List.of(stage4Stats)));
    QueryDispatcher.QueryResult result = new QueryDispatcher.QueryResult(EMPTY_RESULT, mqStats, 0L);

    // SINGLETON mixed leaf at stageId=4, sends to receiver stage 3, receives from stage 5 (a leaf).
    // singletonLeafStageIds = {4}; stagesReceivingFromLeaves = {}.
    // Stage 5 is a pure leaf (fact table scan) running on the same servers.
    DispatchablePlanFragment mixedLeaf =
        mixedLeafFragment(4, 3, RelDistribution.Type.SINGLETON, /*senderStageId=*/5, impacted, normal);
    DispatchablePlanFragment senderLeaf = leafFragmentWithStageId(5, 4, impacted, normal);
    DispatchablePlanFragment intermediate = nonLeafFragment(impacted, normal);

    ServerRoutingStatsManager stats = mock(ServerRoutingStatsManager.class);
    Set<String> recorded = new HashSet<>();

    applyUpstreamTimingsFromStats(
        result, planWith(mixedLeaf, senderLeaf, intermediate), stats, REQUEST_ID, recorded);

    // Stage 4 is in singletonLeafStageIds (sender stage 5 is a leaf) -> consulted.
    // Correct per-server send timings recorded.
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

  /**
   * Verifies that a SINGLETON leaf stage whose sender is an intermediate (non-leaf) stage is NOT
   * consulted. This prevents cascade-contaminated timings from being attributed to intermediate
   * servers. Example: stage 2 is a SINGLETON leaf (dim join) that receives from stage 3 (an
   * intermediate hash-distribution stage running on multiple servers). If stage 3 servers wait for
   * a slow upstream, their delivery to stage 2 is delayed. Reading stage 2's timing would wrongly
   * attribute that cascade delay to those intermediate servers.
   */
  @Test
  public void testSingletonLeafReceivingFromIntermediateNotConsulted() {
    QueryServerInstance serverA = new QueryServerInstance("server-a", "host-a", 9000, MAILBOX_PORT);
    QueryServerInstance serverB = new QueryServerInstance("server-b", "host-b", 9000, MAILBOX_PORT);

    // Stage 2 (SINGLETON leaf) records timing for its stage-3 upstream senders. These timings
    // are cascade-contaminated: serverA took 600ms because it waited for a slow upstream.
    StatMap<BaseMailboxReceiveOperator.StatKey> stage2Stats =
        new StatMap<>(BaseMailboxReceiveOperator.StatKey.class);
    stage2Stats.merge(BaseMailboxReceiveOperator.StatKey.UPSTREAM_SERVER_RESPONSE_TIMES_MS,
        AdaptiveRoutingUpstreamTimings.senderKey("host-a", MAILBOX_PORT) + "=600;"
        + AdaptiveRoutingUpstreamTimings.senderKey("host-b", MAILBOX_PORT) + "=10");

    MultiStageQueryStats mqStats = MultiStageQueryStats.emptyStats(0);
    mqStats.mergeUpstream(2, new MultiStageQueryStats.StageStats.Closed(
        List.of(MultiStageOperator.Type.MAILBOX_RECEIVE), List.of(stage2Stats)));
    QueryDispatcher.QueryResult result = new QueryDispatcher.QueryResult(EMPTY_RESULT, mqStats, 0L);

    // Stage 2 is a SINGLETON leaf that receives from stage 3, but stage 3 is NOT a leaf
    // (it's an intermediate stage). So stage 2 must NOT be consulted.
    DispatchablePlanFragment singletonLeaf =
        mixedLeafFragment(2, 1, RelDistribution.Type.SINGLETON, /*senderStageId=*/3, serverA, serverB);
    DispatchablePlanFragment intermediate = nonLeafFragment(serverA, serverB);

    ServerRoutingStatsManager stats = mock(ServerRoutingStatsManager.class);
    Set<String> recorded = new HashSet<>();

    AdaptiveRoutingStageClassification classification = applyUpstreamTimingsFromStats(
        result, planWith(singletonLeaf, intermediate), stats, REQUEST_ID, recorded);

    // Stage 2 is NOT consulted (sender stage 3 is not a leaf) -> contaminated timings never recorded.
    verify(stats, never()).recordStatsUponResponseArrival(REQUEST_ID, "server-a", 600L);
    verify(stats, never()).recordStatsUponResponseArrival(REQUEST_ID, "server-b", 10L);
    // No -1 pre-recording inside applyUpstreamTimingsFromStats. Caller handles via trackedServers check.
    verify(stats, never()).recordStatsUponResponseArrival(REQUEST_ID, "server-a", -1L);
    verify(stats, never()).recordStatsUponResponseArrival(REQUEST_ID, "server-b", -1L);
    // Neither server is in trackedServers, so the caller's fallback will suppress them.
    Assert.assertFalse(classification._trackedServers.contains("server-a"));
    Assert.assertFalse(classification._trackedServers.contains("server-b"));
  }
}
