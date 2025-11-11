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
package org.apache.pinot.broker.requesthandler;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.response.broker.BrokerResponseNativeV2;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.physical.DispatchableSubPlan;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.query.runtime.operator.LeafOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class MultiStageBrokerRequestHandlerTest {

  @Test
  public void testFillOldBrokerResponseStatsWithMixedStageStats()
      throws Exception {
    MultiStageBrokerRequestHandler handler = Mockito.mock(MultiStageBrokerRequestHandler.class);
    Method method = MultiStageBrokerRequestHandler.class.getDeclaredMethod("fillOldBrokerResponseStats",
        BrokerResponseNativeV2.class, List.class, DispatchableSubPlan.class);
    method.setAccessible(true);

    BrokerResponseNativeV2 brokerResponse = new BrokerResponseNativeV2();
    List<MultiStageQueryStats.StageStats.Closed> stageStats = new ArrayList<>();
    stageStats.add(new MultiStageQueryStats.StageStats.Closed(Collections.emptyList(), Collections.emptyList()));
    stageStats.add(createLeafStageStats(42L, 17L));

    Map<Integer, DispatchablePlanFragment> stageMap = new HashMap<>();
    stageMap.put(0, createFragment(0, "testTable_OFFLINE"));
    stageMap.put(1, createFragment(1, null));
    DispatchableSubPlan subPlan = Mockito.mock(DispatchableSubPlan.class);
    when(subPlan.getQueryStageMap()).thenReturn(stageMap);

    method.invoke(handler, brokerResponse, stageStats, subPlan);

    ObjectNode json = brokerResponse.getStageStats();
    assertNotNull(json);
    assertEquals(json.get("type").asText(), "EMPTY_MAILBOX_SEND");
    assertEquals(json.get("stage").asInt(), 0);
    assertEquals(json.get("table").asText(), "testTable_OFFLINE");

    assertEquals(brokerResponse.getNumDocsScanned(), 42L);
    assertEquals(brokerResponse.getMaxRowsInOperator(), 17L);
  }

  private static MultiStageQueryStats.StageStats.Closed createLeafStageStats(long numDocsScanned, long emittedRows) {
    StatMap<LeafOperator.StatKey> statMap = new StatMap<>(LeafOperator.StatKey.class);
    statMap.merge(LeafOperator.StatKey.NUM_DOCS_SCANNED, numDocsScanned);
    statMap.merge(LeafOperator.StatKey.EMITTED_ROWS, emittedRows);
    return new MultiStageQueryStats.StageStats.Closed(
        Collections.singletonList(MultiStageOperator.Type.LEAF), Collections.singletonList(statMap));
  }

  private static DispatchablePlanFragment createFragment(int stageId, String tableName) {
    DataSchema emptySchema = new DataSchema(new String[0], new DataSchema.ColumnDataType[0]);
    PlanNode root = new ValueNode(stageId, emptySchema, null, Collections.emptyList(), Collections.emptyList());
    PlanFragment fragment = new PlanFragment(stageId, root, Collections.emptyList());
    DispatchablePlanFragment dispatchable = new DispatchablePlanFragment(fragment);
    if (tableName != null) {
      dispatchable.setTableName(tableName);
    }
    return dispatchable;
  }
}
