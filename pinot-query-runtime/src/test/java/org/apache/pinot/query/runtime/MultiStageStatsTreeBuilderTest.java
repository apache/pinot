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
package org.apache.pinot.query.runtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MultiStageStatsTreeBuilderTest {
  private static PlanNode createMinimalPlanNode(int stageId) {
    // Minimal ValueNode with empty schema and no literals
    DataSchema emptySchema = new DataSchema(new String[0], new DataSchema.ColumnDataType[0]);
    return new ValueNode(stageId, emptySchema, null, Collections.emptyList(), Collections.emptyList());
  }

  private static DispatchablePlanFragment createDispatchableFragmentForStage(int stageId) {
    PlanNode root = createMinimalPlanNode(stageId);
    PlanFragment fragment = new PlanFragment(stageId, root, Collections.emptyList());
    DispatchablePlanFragment dispatchable = new DispatchablePlanFragment(fragment);
    // Set optional table name for better coverage
    dispatchable.setTableName("testTable_OFFLINE");
    return dispatchable;
  }

  @Test
  public void jsonStatsByStageReturnsPlaceholderWhenStageStatsIsNull() {
    Map<Integer, DispatchablePlanFragment> fragments = new HashMap<>();
    fragments.put(0, createDispatchableFragmentForStage(0));

    List<MultiStageQueryStats.StageStats> stats = new ArrayList<>();
    // Intentionally leave index 0 as null by having an empty list

    MultiStageStatsTreeBuilder builder = new MultiStageStatsTreeBuilder(fragments, stats);
    ObjectNode node = builder.jsonStatsByStage(0);

    Assert.assertEquals("EMPTY_MAILBOX_SEND", node.get("type").asText());
    Assert.assertEquals(0, node.get("stage").asInt());
    Assert.assertTrue(node.get("description").asText().contains("No stats available"));
  }

  @Test
  public void jsonStatsByStageReturnsPlaceholderWhenStageStatsIsEmpty() {
    Map<Integer, DispatchablePlanFragment> fragments = new HashMap<>();
    fragments.put(0, createDispatchableFragmentForStage(0));

    // Create an explicit empty Closed stats for stage 0
    MultiStageQueryStats.StageStats.Closed emptyClosed =
        new MultiStageQueryStats.StageStats.Closed(Collections.emptyList(), Collections.emptyList());
    List<MultiStageQueryStats.StageStats> stats = new ArrayList<>();
    stats.add(emptyClosed);

    MultiStageStatsTreeBuilder builder = new MultiStageStatsTreeBuilder(fragments, stats);
    JsonNode node = builder.jsonStatsByStage(0);

    Assert.assertEquals("EMPTY_MAILBOX_SEND", node.get("type").asText());
    Assert.assertEquals(0, node.get("stage").asInt());
    Assert.assertTrue(node.get("description").asText().contains("No stats available"));
  }
}
