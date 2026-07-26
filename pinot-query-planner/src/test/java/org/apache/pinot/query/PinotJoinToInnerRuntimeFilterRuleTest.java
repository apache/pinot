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
package org.apache.pinot.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.core.routing.MockRoutingManagerFactory;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.RuntimeFilterNode;
import org.apache.pinot.query.planner.serde.PlanNodeDeserializer;
import org.apache.pinot.query.planner.serde.PlanNodeSerializer;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Planner tests for {@link org.apache.pinot.calcite.rel.rules.PinotJoinToInnerRuntimeFilterRule} — the
 * additive INNER-join probe-side runtime filter. Validates when the rule fires, the resulting plan
 * shape (join + both exchanges preserved, an extra pipeline-breaker edge added), serde round-trips, and
 * the negative cases (default off, off-hint, outer join, non-leaf probe).
 */
public class PinotJoinToInnerRuntimeFilterRuleTest extends QueryEnvironmentTestBase {

  private static List<PlanNode> collectAllNodes(DispatchableSubPlan plan) {
    List<PlanNode> all = new ArrayList<>();
    for (DispatchablePlanFragment fragment : plan.getQueryStages()) {
      PlanFragment planFragment = fragment.getPlanFragment();
      if (planFragment != null && planFragment.getFragmentRoot() != null) {
        collect(planFragment.getFragmentRoot(), all);
      }
    }
    return all;
  }

  private static void collect(PlanNode node, List<PlanNode> out) {
    out.add(node);
    for (PlanNode input : node.getInputs()) {
      collect(input, out);
    }
  }

  private static RuntimeFilterNode findRuntimeFilter(DispatchableSubPlan plan) {
    for (PlanNode node : collectAllNodes(plan)) {
      if (node instanceof RuntimeFilterNode) {
        return (RuntimeFilterNode) node;
      }
    }
    return null;
  }

  private static boolean hasJoin(DispatchableSubPlan plan) {
    return collectAllNodes(plan).stream().anyMatch(n -> n instanceof JoinNode);
  }

  @Test
  public void testRuleFiresWithHintAndPreservesJoinAndPipelineBreaker() {
    DispatchableSubPlan plan = _queryEnvironment.planQuery(
        "SELECT /*+ joinOptions(runtime_filter='in') */ a.col1, b.col2 FROM a JOIN b ON a.col3 = b.col3");
    RuntimeFilterNode runtimeFilter = findRuntimeFilter(plan);
    assertNotNull(runtimeFilter, "expected a RuntimeFilterNode for an INNER join with runtime_filter hint");
    // The real hash join must still be present (the filter is additive, not a replacement).
    assertTrue(hasJoin(plan), "the inner join must be preserved");
    // input[1] must be a PIPELINE_BREAKER receive carrying the build keys.
    assertEquals(runtimeFilter.getInputs().size(), 2);
    PlanNode buildInput = runtimeFilter.getInputs().get(1);
    assertTrue(buildInput instanceof MailboxReceiveNode, "RuntimeFilterNode input[1] must be a MailboxReceiveNode");
    assertEquals(((MailboxReceiveNode) buildInput).getExchangeType(), PinotRelExchangeType.PIPELINE_BREAKER);
    // Single equi-key: one probe key, one build key.
    assertEquals(runtimeFilter.getProbeKeys().size(), 1);
    assertEquals(runtimeFilter.getBuildKeys().size(), 1);
    assertEquals(runtimeFilter.getType(), RuntimeFilterNode.Type.IN);
    // Pin the probe-key -> leaf-select-list alignment by VALUE (not just count): the probe key must point
    // at the join-key column (col3) in the probe output schema, NOT blindly at position 0 (the query
    // selects col1, joins on col3). The build key is the projected key at position 0.
    assertEquals(runtimeFilter.getDataSchema().getColumnName(runtimeFilter.getProbeKeys().get(0)), "col3");
    assertEquals(runtimeFilter.getBuildKeys().get(0).intValue(), 0);
  }

  @Test
  public void testAutoHintYieldsAutoType() {
    DispatchableSubPlan plan = _queryEnvironment.planQuery(
        "SELECT /*+ joinOptions(runtime_filter='auto') */ a.col1, b.col2 FROM a JOIN b ON a.col3 = b.col3");
    RuntimeFilterNode runtimeFilter = findRuntimeFilter(plan);
    assertNotNull(runtimeFilter);
    assertEquals(runtimeFilter.getType(), RuntimeFilterNode.Type.AUTO);
  }

  @Test
  public void testBloomHintYieldsBloomType() {
    DispatchableSubPlan plan = _queryEnvironment.planQuery(
        "SELECT /*+ joinOptions(runtime_filter='bloom') */ a.col1, b.col2 FROM a JOIN b ON a.col3 = b.col3");
    RuntimeFilterNode runtimeFilter = findRuntimeFilter(plan);
    assertNotNull(runtimeFilter);
    assertEquals(runtimeFilter.getType(), RuntimeFilterNode.Type.BLOOM);
  }

  @Test
  public void testSerdeRoundTripWithRuntimeFilter() {
    DispatchableSubPlan plan = _queryEnvironment.planQuery(
        "SELECT /*+ joinOptions(runtime_filter='bloom') */ a.col1, b.col2 FROM a JOIN b ON a.col3 = b.col3");
    // Sanity: the plan really contains the new node, so this exercises its serde.
    assertNotNull(findRuntimeFilter(plan));
    for (DispatchablePlanFragment fragment : plan.getQueryStages()) {
      PlanNode stagePlan = fragment.getPlanFragment().getFragmentRoot();
      PlanNode roundTripped = PlanNodeDeserializer.process(PlanNodeSerializer.process(stagePlan));
      assertEquals(roundTripped, stagePlan);
    }
  }

  @Test
  public void testNonColocatedUsesBroadcastPipelineBreaker() {
    // Default (non-colocated) path: the build keys are BROADCAST to the probe leaf. The colocated
    // (is_colocated_by_join_keys) path emits a SINGLETON pre-partitioned pipeline breaker exactly like
    // PinotJoinToDynamicBroadcastRule; exercising it end-to-end requires real table colocation (matching
    // sender/receiver worker counts), so it is covered by the shared dynamic-broadcast machinery.
    DispatchableSubPlan plan = _queryEnvironment.planQuery(
        "SELECT /*+ joinOptions(runtime_filter='in') */ a.col1, b.col2 FROM a JOIN b ON a.col3 = b.col3");
    MailboxReceiveNode pb = (MailboxReceiveNode) findRuntimeFilter(plan).getInputs().get(1);
    assertEquals(pb.getExchangeType(), PinotRelExchangeType.PIPELINE_BREAKER);
    assertEquals(pb.getDistributionType(), RelDistribution.Type.BROADCAST_DISTRIBUTED);
  }

  @Test
  public void testMultiKeyUsesRuntimeFilter() {
    DispatchableSubPlan plan = _queryEnvironment.planQuery(
        "SELECT /*+ joinOptions(runtime_filter='in') */ a.col1, b.col2 FROM a JOIN b "
            + "ON a.col3 = b.col3 AND a.col7 = b.col7");
    RuntimeFilterNode runtimeFilter = findRuntimeFilter(plan);
    assertNotNull(runtimeFilter, "multi-key INNER join should still get a runtime filter (exact IN per key)");
    assertEquals(runtimeFilter.getProbeKeys().size(), 2);
    assertEquals(runtimeFilter.getBuildKeys().size(), 2);
    assertTrue(hasJoin(plan));
  }

  @Test
  public void testNoRuntimeFilterByDefault() {
    DispatchableSubPlan plan =
        _queryEnvironment.planQuery("SELECT a.col1, b.col2 FROM a JOIN b ON a.col3 = b.col3");
    assertNull(findRuntimeFilter(plan));
  }

  @Test
  public void testOffHintDisablesRuntimeFilter() {
    DispatchableSubPlan plan = _queryEnvironment.planQuery(
        "SELECT /*+ joinOptions(runtime_filter='off') */ a.col1, b.col2 FROM a JOIN b ON a.col3 = b.col3");
    assertNull(findRuntimeFilter(plan));
  }

  @Test
  public void testNoRuntimeFilterForLeftJoin() {
    DispatchableSubPlan plan = _queryEnvironment.planQuery(
        "SELECT /*+ joinOptions(runtime_filter='in') */ a.col1, b.col2 FROM a LEFT JOIN b ON a.col3 = b.col3");
    assertNull(findRuntimeFilter(plan), "runtime filter must only apply to INNER joins");
  }

  @Test
  public void testNoRuntimeFilterForNonLeafProbe() {
    // The probe (left) side is a GROUP BY subquery (not leaf-pushable), so the filter cannot be pushed.
    DispatchableSubPlan plan = _queryEnvironment.planQuery(
        "SELECT /*+ joinOptions(runtime_filter='in') */ x.col3, b.col2 "
            + "FROM (SELECT col3 FROM a GROUP BY col3) x JOIN b ON x.col3 = b.col3");
    assertNull(findRuntimeFilter(plan), "runtime filter must not be pushed past a non-leaf (aggregate) probe");
  }

  @Test
  public void testClusterFlagEnablesRuntimeFilterWithoutHint() {
    QueryEnvironment flagEnabledEnv = runtimeFilterEnabledEnvironment();
    DispatchableSubPlan plan =
        flagEnabledEnv.planQuery("SELECT a.col1, b.col2 FROM a JOIN b ON a.col3 = b.col3");
    RuntimeFilterNode runtimeFilter = findRuntimeFilter(plan);
    assertNotNull(runtimeFilter, "cluster flag should enable the runtime filter without a hint");
    // No explicit hint -> AUTO (tiered) is the default mode.
    assertEquals(runtimeFilter.getType(), RuntimeFilterNode.Type.AUTO);
  }

  @Test
  public void testQueryOptionCanDisableEvenWithFlagOn() {
    QueryEnvironment flagEnabledEnv = runtimeFilterEnabledEnvironment();
    DispatchableSubPlan plan = flagEnabledEnv.planQuery(
        "SET runtimeFilterJoin = 'off'; SELECT a.col1, b.col2 FROM a JOIN b ON a.col3 = b.col3");
    assertNull(findRuntimeFilter(plan), "query option 'off' should disable the runtime filter even with flag on");
  }

  /**
   * Builds a {@link QueryEnvironment} identical to the base one but with the runtime-filter-join cluster
   * default enabled.
   */
  private static QueryEnvironment runtimeFilterEnabledEnvironment() {
    MockRoutingManagerFactory factory = new MockRoutingManagerFactory(1, 2);
    for (Map.Entry<String, Schema> entry : TABLE_SCHEMAS.entrySet()) {
      factory.registerTable(entry.getValue(), entry.getKey());
    }
    for (Map.Entry<String, List<String>> entry : SERVER1_SEGMENTS.entrySet()) {
      for (String segment : entry.getValue()) {
        factory.registerSegment(1, entry.getKey(), segment);
      }
    }
    for (Map.Entry<String, List<String>> entry : SERVER2_SEGMENTS.entrySet()) {
      for (String segment : entry.getValue()) {
        factory.registerSegment(2, entry.getKey(), segment);
      }
    }
    RoutingManager routingManager = factory.buildRoutingManager(null);
    TableCache tableCache = factory.buildTableCache();
    return new QueryEnvironment(QueryEnvironment.configBuilder()
        .requestId(-1L)
        .database(CommonConstants.DEFAULT_DATABASE)
        .tableCache(tableCache)
        .workerManager(new WorkerManager("Broker_localhost", "localhost", 3, routingManager))
        .defaultEnableRuntimeFilterJoin(true)
        .build());
  }
}
