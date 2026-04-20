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
package org.apache.pinot.query.planner.physical.v2;

import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.core.routing.MockRoutingManagerFactory;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.apache.pinot.query.routing.WorkerManager;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class LookupJoinPlanTest {

  private QueryEnvironment _queryEnvironment;

  @BeforeClass
  public void setUp() {
    // Set up the query environment with V2 optimizer enabled
    MockRoutingManagerFactory factory = new MockRoutingManagerFactory(1, 2);
    for (Map.Entry<String, Schema> entry : QueryEnvironmentTestBase.TABLE_SCHEMAS.entrySet()) {
      factory.registerTable(entry.getValue(), entry.getKey());
    }
    for (Map.Entry<String, List<String>> entry : QueryEnvironmentTestBase.SERVER1_SEGMENTS.entrySet()) {
      for (String segment : entry.getValue()) {
        factory.registerSegment(1, entry.getKey(), segment);
      }
    }
    for (Map.Entry<String, List<String>> entry : QueryEnvironmentTestBase.SERVER2_SEGMENTS.entrySet()) {
      for (String segment : entry.getValue()) {
        factory.registerSegment(2, entry.getKey(), segment);
      }
    }
    RoutingManager routingManager = factory.buildRoutingManager(null);
    TableCache tableCache = factory.buildTableCache();
    WorkerManager workerManager = new WorkerManager("Broker_localhost", "localhost", 3, routingManager);

    _queryEnvironment = new QueryEnvironment(
        QueryEnvironment.configBuilder()
            .requestId(-1L)
            .database(CommonConstants.DEFAULT_DATABASE)
            .tableCache(tableCache)
            .workerManager(workerManager)
            .defaultUsePhysicalOptimizer(true)
            .defaultInferPartitionHint(true)
            .build());
  }

  @Test
  public void testLookupJoinPlanStructure() {
    // Plan a lookup join query with the lookup hint
    String query = "SELECT /*+ joinOptions(join_strategy='lookup') */ a.col1, b.col2 FROM a_REALTIME a "
        + "JOIN b_REALTIME b ON a.col1 = b.col1";
    DispatchableSubPlan plan = _queryEnvironment.planQuery(query);

    assertNotNull(plan);

    // Find the join node and verify its structure
    JoinNode joinNode = null;
    for (DispatchablePlanFragment fragment : plan.getQueryStageMap().values()) {
      PlanNode root = fragment.getPlanFragment().getFragmentRoot();
      joinNode = findJoinNode(root);
      if (joinNode != null) {
        break;
      }
    }
    assertNotNull(joinNode, "Should find a JoinNode in the plan");

    // Verify the join strategy is LOOKUP
    assertEquals(joinNode.getJoinStrategy(), JoinNode.JoinStrategy.LOOKUP,
        "Join strategy should be LOOKUP");

    // JoinNode inputs: index 0 = left, index 1 = right
    assertTrue(joinNode.getInputs().size() >= 2, "Join should have at least 2 inputs");
    PlanNode leftInput = joinNode.getInputs().get(0);
    PlanNode rightInput = joinNode.getInputs().get(1);

    // Verify the left input is a MailboxReceiveNode (from IDENTITY exchange — each server processes
    // its own fact data locally, no SINGLETON bottleneck)
    assertTrue(leftInput instanceof MailboxReceiveNode,
        "Left input should be a MailboxReceiveNode (from IDENTITY exchange)");
    boolean foundTableScanInRight = false;
    boolean foundMailboxInRight = false;
    PlanNode current = rightInput;
    while (current != null) {
      if (current instanceof TableScanNode) {
        foundTableScanInRight = true;
        break;
      }
      if (current instanceof MailboxReceiveNode) {
        foundMailboxInRight = true;
        break;
      }
      // Continue down the right input chain (Project, Filter, etc.)
      if (current.getInputs().isEmpty()) {
        break;
      }
      current = current.getInputs().get(0);
    }
    assertTrue(foundTableScanInRight,
        "Right input chain should contain a TableScanNode (dim table local to join)");
    assertFalse(foundMailboxInRight,
        "Right input chain should NOT contain a MailboxReceiveNode (dim table not from remote exchange)");
  }

  @Test
  public void testNonLookupJoinPlanStructure() {
    // Plan the same join WITHOUT the lookup hint for comparison
    String query = "SELECT a.col1, b.col2 FROM a_REALTIME a JOIN b_REALTIME b ON a.col1 = b.col1";
    DispatchableSubPlan plan = _queryEnvironment.planQuery(query);

    assertNotNull(plan);

    // Find the join node and verify its structure
    JoinNode joinNode = null;
    for (DispatchablePlanFragment fragment : plan.getQueryStageMap().values()) {
      PlanNode root = fragment.getPlanFragment().getFragmentRoot();
      joinNode = findJoinNode(root);
      if (joinNode != null) {
        break;
      }
    }
    assertNotNull(joinNode, "Should find a JoinNode in the plan");

    // Verify the join strategy is HASH (non-lookup)
    assertEquals(joinNode.getJoinStrategy(), JoinNode.JoinStrategy.HASH,
        "Join strategy should be HASH for non-lookup join");

    // JoinNode inputs: index 0 = left, index 1 = right
    assertTrue(joinNode.getInputs().size() >= 2, "Join should have at least 2 inputs");

    // Verify both inputs are MailboxReceiveNodes (from exchanges)
    assertTrue(joinNode.getInputs().get(0) instanceof MailboxReceiveNode,
        "Left input should be a MailboxReceiveNode (from exchange)");
    assertTrue(joinNode.getInputs().get(1) instanceof MailboxReceiveNode,
        "Right input should be a MailboxReceiveNode (from exchange)");
  }

  /**
   * Verify EXPLAIN output shows the dim table in the same stage as the join (no exchange between them).
   * This is the V2 equivalent of QueryCompilationTest.testJoinPushTransitivePredicateLookupJoin().
   */
  @Test
  public void testLookupJoinExplainPlan() {
    String query = "EXPLAIN PLAN FOR "
        + "SELECT /*+ joinOptions(join_strategy='lookup') */ a.col1, b.col2 "
        + "FROM a_REALTIME a JOIN b_REALTIME b ON a.col1 = b.col1";
    long requestId = new Random().nextLong();
    String explain = _queryEnvironment.explainQuery(query, requestId);

    // Expected plan structure:
    //   PhysicalExchange(SINGLETON_EXCHANGE)           ← root
    //     PhysicalProject(...)
    //       PhysicalExchange(IDENTITY_EXCHANGE)         ← above lookup join (isolation)
    //         PhysicalJoin(...)                          ← the lookup join
    //           PhysicalExchange(IDENTITY_EXCHANGE)      ← left (leaf boundary)
    //             PhysicalProject(...)
    //               PhysicalTableScan(a_REALTIME)
    //           PhysicalExchange(LOOKUP_LOCAL_EXCHANGE)  ← right (dim table, no split)
    //             PhysicalProject(...)
    //               PhysicalTableScan(b_REALTIME)
    String[] lines = explain.split("\n");

    // Find the PhysicalJoin line and verify its children
    int joinLineIdx = -1;
    for (int i = 0; i < lines.length; i++) {
      if (lines[i].contains("PhysicalJoin")) {
        joinLineIdx = i;
        break;
      }
    }
    assertTrue(joinLineIdx >= 0, "EXPLAIN should contain PhysicalJoin. Output:\n" + explain);

    // The line ABOVE the join should be IDENTITY_EXCHANGE (above isolation)
    boolean foundAboveIdentity = false;
    for (int i = joinLineIdx - 1; i >= 0; i--) {
      if (lines[i].contains("Exchange")) {
        assertTrue(lines[i].contains("IDENTITY_EXCHANGE"),
            "Exchange above the join should be IDENTITY_EXCHANGE (isolation), found: " + lines[i].trim()
                + "\nEXPLAIN output:\n" + explain);
        foundAboveIdentity = true;
        break;
      }
    }
    assertTrue(foundAboveIdentity, "Should find an exchange above the join. Output:\n" + explain);

    // The join's first child exchange should be IDENTITY_EXCHANGE (left, leaf boundary)
    // The join's second child exchange should be LOOKUP_LOCAL_EXCHANGE (right, dim table)
    int joinIndent = lines[joinLineIdx].indexOf("PhysicalJoin");
    int childExchangeCount = 0;
    String[] expectedChildExchanges = {"IDENTITY_EXCHANGE", "LOOKUP_LOCAL_EXCHANGE"};
    for (int i = joinLineIdx + 1; i < lines.length; i++) {
      String trimmed = lines[i].trim();
      if (trimmed.startsWith("PhysicalExchange")) {
        // Check indent — must be a direct child of the join (one indent level deeper)
        int indent = lines[i].indexOf("PhysicalExchange");
        if (indent > joinIndent && childExchangeCount < 2) {
          assertTrue(trimmed.contains(expectedChildExchanges[childExchangeCount]),
              "Join child exchange #" + childExchangeCount + " should be "
                  + expectedChildExchanges[childExchangeCount] + ", found: " + trimmed
                  + "\nEXPLAIN output:\n" + explain);
          childExchangeCount++;
        }
      }
    }
    assertEquals(childExchangeCount, 2,
        "Join should have exactly 2 child exchanges. EXPLAIN output:\n" + explain);
  }

  /**
   * When a hash join feeds into a lookup join (hash+lookup against same dim table),
   * the hash join must be in a separate stage — not absorbed into the lookup join's leaf fragment.
   */
  @Test
  public void testLookupJoinAfterHashJoin() {
    // hash_join(a, b) → lookup_join(result, b)
    String query = "SELECT /*+ joinOptions(join_strategy='lookup') */ "
        + "t.col1, d.col2 FROM "
        + "(SELECT /*+ joinOptions(join_strategy='hash') */ a.col1, b.col2 "
        + " FROM a_REALTIME a JOIN b_REALTIME b ON a.col1 = b.col1) t "
        + "JOIN b_REALTIME d ON t.col1 = d.col1";
    DispatchableSubPlan plan = _queryEnvironment.planQuery(query);
    assertNotNull(plan);

    // Find the lookup join node
    JoinNode lookupJoin = null;
    for (DispatchablePlanFragment fragment : plan.getQueryStageMap().values()) {
      PlanNode root = fragment.getPlanFragment().getFragmentRoot();
      lookupJoin = findLookupJoinNode(root);
      if (lookupJoin != null) {
        break;
      }
    }
    assertNotNull(lookupJoin, "Should find a lookup JoinNode");
    assertEquals(lookupJoin.getJoinStrategy(), JoinNode.JoinStrategy.LOOKUP);

    // Left input must be a MailboxReceiveNode (stage boundary separating the hash join)
    assertTrue(lookupJoin.getInputs().get(0) instanceof MailboxReceiveNode,
        "Left input of lookup join must be MailboxReceiveNode "
            + "(hash join should be in a separate stage)");
  }

  /**
   * When a lookup join's result feeds into another join (e.g., LEFT JOIN active_subs),
   * the lookup join must be in its own isolated fragment. The outer join must NOT be absorbed
   * into the lookup join's leaf stage.
   */
  @Test
  public void testJoinAboveLookupJoin() {
    // lookup_join(a, b) → regular_join(result, a)
    // The outer join should be in a separate stage from the lookup join.
    String query = "SELECT t.col1, c.col2 FROM "
        + "(SELECT /*+ joinOptions(join_strategy='lookup') */ a.col1, b.col2 "
        + " FROM a_REALTIME a JOIN b_REALTIME b ON a.col1 = b.col1) t "
        + "JOIN a_REALTIME c ON t.col1 = c.col1";
    DispatchableSubPlan plan = _queryEnvironment.planQuery(query);
    assertNotNull(plan);

    // Find the NON-lookup join (the outer join)
    JoinNode outerJoin = null;
    for (DispatchablePlanFragment fragment : plan.getQueryStageMap().values()) {
      PlanNode root = fragment.getPlanFragment().getFragmentRoot();
      outerJoin = findNonLookupJoinNode(root);
      if (outerJoin != null) {
        break;
      }
    }
    assertNotNull(outerJoin, "Should find a non-lookup JoinNode (the outer join)");
    assertEquals(outerJoin.getJoinStrategy(), JoinNode.JoinStrategy.HASH);

    // The outer join's inputs should both be MailboxReceiveNodes — it should NOT
    // be in the same fragment as the lookup join
    assertTrue(outerJoin.getInputs().get(0) instanceof MailboxReceiveNode,
        "Left input of outer join must be MailboxReceiveNode "
            + "(lookup join result should be in a separate stage)");
  }

  /**
   * Verify that the lookup join fragment is properly structured: has a table name (from the dim
   * table registered via LOOKUP_LOCAL_EXCHANGE), right input is a TableScanNode (in same fragment),
   * and left input is a MailboxReceiveNode (separate fragment).
   */
  @Test
  public void testLookupJoinFragmentIsolation() {
    String query = "SELECT /*+ joinOptions(join_strategy='lookup') */ a.col1, b.col2 FROM a_REALTIME a "
        + "JOIN b_REALTIME b ON a.col1 = b.col1";
    DispatchableSubPlan plan = _queryEnvironment.planQuery(query);
    assertNotNull(plan);

    // Find the fragment containing the lookup join
    for (DispatchablePlanFragment fragment : plan.getQueryStageMap().values()) {
      PlanNode root = fragment.getPlanFragment().getFragmentRoot();
      JoinNode join = findLookupJoinNode(root);
      if (join != null) {
        // Fragment should have a table name set (from dim table registered via LOOKUP_LOCAL_EXCHANGE)
        String tableName = fragment.getTableName();
        assertNotNull(tableName, "Lookup join fragment should have a table name (dim table)");
        // The join's right input should be a TableScanNode (not MailboxReceive)
        assertTrue(join.getInputs().get(1) instanceof TableScanNode
                || hasTableScanChild(join.getInputs().get(1)),
            "Right input should contain TableScanNode in same fragment");
        // The join's left input should be a MailboxReceiveNode (from exchange)
        assertTrue(join.getInputs().get(0) instanceof MailboxReceiveNode,
            "Left input should be MailboxReceiveNode (separate fragment)");
        return;
      }
    }
    fail("Should find a fragment containing a lookup JoinNode");
  }

  /**
   * Verify that a non-lookup join is completely unaffected by LookupJoinRule.
   * Both inputs should be MailboxReceiveNodes (standard exchange behavior).
   */
  @Test
  public void testNonLookupJoinUnaffected() {
    // Same query as testNonLookupJoinPlanStructure but explicitly checking no LOOKUP_LOCAL_EXCHANGE
    String query = "SELECT a.col1, b.col2 FROM a_REALTIME a JOIN b_REALTIME b ON a.col1 = b.col1";
    DispatchableSubPlan plan = _queryEnvironment.planQuery(query);
    assertNotNull(plan);

    // No fragment should have a lookup join
    for (DispatchablePlanFragment fragment : plan.getQueryStageMap().values()) {
      PlanNode root = fragment.getPlanFragment().getFragmentRoot();
      assertNull(findLookupJoinNode(root),
          "Non-lookup query should not contain any lookup JoinNode");
    }
  }

  private boolean hasTableScanChild(PlanNode node) {
    if (node instanceof TableScanNode) {
      return true;
    }
    for (PlanNode input : node.getInputs()) {
      if (hasTableScanChild(input)) {
        return true;
      }
    }
    return false;
  }

  private JoinNode findNonLookupJoinNode(PlanNode node) {
    if (node instanceof JoinNode && ((JoinNode) node).getJoinStrategy() != JoinNode.JoinStrategy.LOOKUP) {
      return (JoinNode) node;
    }
    for (PlanNode input : node.getInputs()) {
      JoinNode found = findNonLookupJoinNode(input);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  private JoinNode findJoinNode(PlanNode node) {
    if (node instanceof JoinNode) {
      return (JoinNode) node;
    }
    for (PlanNode input : node.getInputs()) {
      JoinNode found = findJoinNode(input);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  private JoinNode findLookupJoinNode(PlanNode node) {
    if (node instanceof JoinNode && ((JoinNode) node).getJoinStrategy() == JoinNode.JoinStrategy.LOOKUP) {
      return (JoinNode) node;
    }
    for (PlanNode input : node.getInputs()) {
      JoinNode found = findLookupJoinNode(input);
      if (found != null) {
        return found;
      }
    }
    return null;
  }
}
