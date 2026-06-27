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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.runtime.PairList;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class DispatchableSubPlanTest {

  @Test
  public void testIsAllLeafStagesEmptyTrue() {
    DispatchableSubPlan plan = new DispatchableSubPlan(
        PairList.of(0, "col1"), Map.of(), Set.of("testTable"),
        Map.of(), 5, true);
    assertTrue(plan.isAllLeafStagesEmpty());
  }

  @Test
  public void testIsAllLeafStagesEmptyFalse() {
    DispatchableSubPlan plan = new DispatchableSubPlan(
        PairList.of(0, "col1"), Map.of(), Set.of("testTable"),
        Map.of(), 3, false);
    assertFalse(plan.isAllLeafStagesEmpty());
  }

  @Test
  public void testIsAllLeafStagesEmptyDefaultFalse() {
    // 5-arg constructor defaults to false
    DispatchableSubPlan plan = new DispatchableSubPlan(
        PairList.of(0, "col1"), Map.of(), Set.of("testTable"),
        Map.of(), 5);
    assertFalse(plan.isAllLeafStagesEmpty());
  }

  @Test
  public void testIsAllLeafStagesEmptyNoTables() {
    // Constant expression query (no tables) — flag can still be false
    DispatchableSubPlan plan = new DispatchableSubPlan(
        PairList.of(0, "col1"), Map.of(), Set.of(),
        Map.of(), 0, false);
    assertFalse(plan.isAllLeafStagesEmpty());
  }

  @Test
  public void testCopyWithRootPreservesFragmentId() {
    ValueNode oldRoot = new ValueNode(0, new DataSchema(new String[0], new ColumnDataType[0]),
        PlanNode.NodeHint.EMPTY, List.of(), List.of());
    PlanFragment fragment = new PlanFragment(0, oldRoot, List.of());
    DispatchablePlanFragment original = new DispatchablePlanFragment(fragment);

    ValueNode newRoot = new ValueNode(0, new DataSchema(new String[0], new ColumnDataType[0]),
        PlanNode.NodeHint.EMPTY, List.of(), List.of());
    DispatchablePlanFragment copy = DispatchablePlanFragment.copyWithRoot(original, newRoot);

    org.testng.Assert.assertEquals(copy.getPlanFragment().getFragmentId(), 0);
    assertSame(copy.getPlanFragment().getFragmentRoot(), newRoot);
    assertSame(original.getPlanFragment().getFragmentRoot(), oldRoot);
  }
}
