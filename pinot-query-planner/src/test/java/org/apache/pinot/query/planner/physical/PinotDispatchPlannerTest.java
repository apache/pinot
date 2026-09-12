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
package org.apache.pinot.query.planner.physical;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class PinotDispatchPlannerTest extends QueryEnvironmentTestBase {

  @Test
  public void testHasNonEmptyReplicatedLeafAllNull() {
    DispatchablePlanMetadata metadata = new DispatchablePlanMetadata();
    assertFalse(PinotDispatchPlanner.hasNonEmptyReplicatedLeaf(Map.of(0, metadata)));
  }

  @Test
  public void testHasNonEmptyReplicatedLeafEmptyLists() {
    DispatchablePlanMetadata metadata = new DispatchablePlanMetadata();
    metadata.setReplicatedSegments(Map.of("t", List.of()));
    assertFalse(PinotDispatchPlanner.hasNonEmptyReplicatedLeaf(Map.of(0, metadata)));
  }

  @Test
  public void testHasNonEmptyReplicatedLeafNonEmpty() {
    DispatchablePlanMetadata metadata = new DispatchablePlanMetadata();
    metadata.setReplicatedSegments(Map.of("t", List.of("seg1")));
    assertTrue(PinotDispatchPlanner.hasNonEmptyReplicatedLeaf(Map.of(0, metadata)));
  }

  @Test
  public void testHasNonEmptyReplicatedLeafMixed() {
    DispatchablePlanMetadata metadataNullSegments = new DispatchablePlanMetadata();
    DispatchablePlanMetadata metadataNonEmpty = new DispatchablePlanMetadata();
    metadataNonEmpty.setReplicatedSegments(Map.of("t", List.of("seg1")));
    Map<Integer, DispatchablePlanMetadata> metadataMap = Map.of(0, metadataNullSegments, 1, metadataNonEmpty);
    assertTrue(PinotDispatchPlanner.hasNonEmptyReplicatedLeaf(metadataMap));
  }

  @Test
  public void testHasNonEmptyReplicatedLeafEmptyMap() {
    assertFalse(PinotDispatchPlanner.hasNonEmptyReplicatedLeaf(Map.of()));
  }

  @Test
  public void testRewriteReduceStageSetAllNodeStageIdsToZero() {
    DispatchableSubPlan subPlan = _queryEnvironment.planQuery("SELECT COUNT(*) FROM a WHERE ts < 0");
    Map<Integer, DispatchablePlanFragment> fragmentMap = new HashMap<>(subPlan.getQueryStageMap());
    PinotDispatchPlanner.rewriteReduceStageForEmptyLeaves(fragmentMap);
    PlanNode root = fragmentMap.get(0).getPlanFragment().getFragmentRoot();
    assertAllStageIdsAreZero(root);
  }

  @Test
  public void testRewriteReduceStageWithEmptyWorkerMetadataListIsNoOp() {
    DispatchableSubPlan subPlan = _queryEnvironment.planQuery("SELECT COUNT(*) FROM a WHERE ts < 0");
    Map<Integer, DispatchablePlanFragment> fragmentMap = new HashMap<>(subPlan.getQueryStageMap());
    fragmentMap.get(0).getWorkerMetadataList().clear();
    PinotDispatchPlanner.rewriteReduceStageForEmptyLeaves(fragmentMap);
    assertTrue(fragmentMap.get(0).getWorkerMetadataList().isEmpty());
  }

  @Test
  public void testRewriteReduceStageStripsMailboxInfos() {
    DispatchableSubPlan subPlan = _queryEnvironment.planQuery("SELECT COUNT(*) FROM a WHERE ts < 0 LIMIT 1");
    Map<Integer, DispatchablePlanFragment> fragmentMap = new HashMap<>(subPlan.getQueryStageMap());
    assertTrue(fragmentMap.get(0).getWorkerMetadataList().get(0).getMailboxInfosMap().containsKey(1));

    PinotDispatchPlanner.rewriteReduceStageForEmptyLeaves(fragmentMap);
    assertTrue(fragmentMap.get(0).getWorkerMetadataList().get(0).getMailboxInfosMap().isEmpty());
  }

  @Test
  public void testRewriteReduceStageWithJoinInlinesAllBranches() {
    DispatchableSubPlan subPlan =
        _queryEnvironment.planQuery("SELECT COUNT(*) FROM a JOIN b ON a.col1 = b.col1 WHERE a.ts < 0");
    Map<Integer, DispatchablePlanFragment> fragmentMap = new HashMap<>(subPlan.getQueryStageMap());
    PinotDispatchPlanner.rewriteReduceStageForEmptyLeaves(fragmentMap);
    PlanNode root = fragmentMap.get(0).getPlanFragment().getFragmentRoot();
    assertAllStageIdsAreZero(root);
  }

  /// `WorkerManager` walks the plan twice (leaves first, then intermediate stages), and with a spool the same
  /// `PlanFragment` is a child of every receiver that reads it. Sharing one visited set across both passes would leave
  /// a spooled intermediate fragment with no workers: the leaf pass visits it and skips it as a non-leaf, then the
  /// intermediate pass skips it as visited.
  @Test
  public void testSpooledIntermediateStageGetsWorkers() {
    DispatchableSubPlan subPlan = _queryEnvironment.planQuery("SET useSpools=true; "
        + "WITH mySpool AS (SELECT col1, SUM(col3) AS s FROM a GROUP BY col1) "
        + "SELECT 1 FROM mySpool AS a1 JOIN b ON a1.col1 = b.col1 JOIN mySpool AS a2 ON a2.col1 = b.col1");

    // The spool is only useful if some fragment really is read by more than one receiver.
    assertTrue(hasMultiReceiverSend(subPlan), "Query did not produce a spool: " + subPlan.getQueryStageMap().keySet());
    for (Map.Entry<Integer, DispatchablePlanFragment> entry : subPlan.getQueryStageMap().entrySet()) {
      assertFalse(entry.getValue().getWorkerMetadataList().isEmpty(), "No worker for stage: " + entry.getKey());
    }
  }

  private static boolean hasMultiReceiverSend(DispatchableSubPlan subPlan) {
    for (DispatchablePlanFragment fragment : subPlan.getQueryStageMap().values()) {
      PlanNode root = fragment.getPlanFragment().getFragmentRoot();
      if (root instanceof MailboxSendNode && ((MailboxSendNode) root).isMultiSend()) {
        return true;
      }
    }
    return false;
  }

  private static void assertAllStageIdsAreZero(PlanNode node) {
    assertEquals(node.getStageId(), 0);
    for (PlanNode input : node.getInputs()) {
      assertAllStageIdsAreZero(input);
    }
  }
}
