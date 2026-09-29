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

import java.util.Map;
import java.util.Set;
import org.apache.calcite.runtime.PairList;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.context.PlannerContext;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class DispatchablePlanContextPruningTest {

  @Test
  public void testNoLeafStagesRecorded() {
    DispatchablePlanContext context = createMinimalContext();
    assertFalse(context.isAllNonReplicatedLeafStagesEmpty(), "No leaf stages → false");
  }

  @Test
  public void testOneLeafFullyPruned() {
    DispatchablePlanContext context = createMinimalContext();
    context.recordLeafStageAssigned();
    context.recordLeafStageEmpty();
    assertTrue(context.isAllNonReplicatedLeafStagesEmpty(), "1 assigned, 1 pruned → true");
  }

  @Test
  public void testTwoLeavesJoinBothPruned() {
    DispatchablePlanContext context = createMinimalContext();
    context.recordLeafStageAssigned();
    context.recordLeafStageEmpty();
    context.recordLeafStageAssigned();
    context.recordLeafStageEmpty();
    assertTrue(context.isAllNonReplicatedLeafStagesEmpty(), "2 assigned, 2 pruned → true");
  }

  @Test
  public void testTwoLeavesOnlyOnePruned() {
    DispatchablePlanContext context = createMinimalContext();
    context.recordLeafStageAssigned();
    context.recordLeafStageEmpty();
    context.recordLeafStageAssigned();
    // second leaf NOT pruned
    assertFalse(context.isAllNonReplicatedLeafStagesEmpty(), "2 assigned, 1 pruned → false");
  }

  @Test
  public void testOneLeafNotPruned() {
    DispatchablePlanContext context = createMinimalContext();
    context.recordLeafStageAssigned();
    // not pruned
    assertFalse(context.isAllNonReplicatedLeafStagesEmpty(), "1 assigned, 0 pruned → false");
  }

  private static DispatchablePlanContext createMinimalContext() {
    PlannerContext plannerContext = Mockito.mock(PlannerContext.class);
    Mockito.when(plannerContext.getOptions()).thenReturn(Map.of());
    QueryEnvironment.Config envConfig = Mockito.mock(QueryEnvironment.Config.class);
    Mockito.when(plannerContext.getEnvConfig()).thenReturn(envConfig);
    return new DispatchablePlanContext(null, 0, plannerContext,
        PairList.of(0, "col"), Set.of("table"));
  }
}
