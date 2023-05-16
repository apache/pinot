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
package org.apache.pinot.query.planner.plannode;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.proto.Plan;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.planner.DispatchablePlanFragment;
import org.apache.pinot.query.planner.DispatchableSubPlan;
import org.apache.pinot.query.planner.serde.ProtoProperties;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SerDeUtilsTest extends QueryEnvironmentTestBase {

  @Test(dataProvider = "testQueryDataProvider")
  public void testQueryStagePlanSerDe(String query)
      throws Exception {

    DispatchableSubPlan dispatchableSubPlan = _queryEnvironment.planQuery(query);
    for (DispatchablePlanFragment dispatchablePlanFragment : dispatchableSubPlan.getQueryStageList()) {
      PlanNode stageNode = dispatchablePlanFragment.getPlanFragment().getFragmentRoot();
      Plan.StageNode serializedStageNode = StageNodeSerDeUtils.serializeStageNode((AbstractPlanNode) stageNode);
      PlanNode deserializedStageNode = StageNodeSerDeUtils.deserializeStageNode(serializedStageNode);
      Assert.assertTrue(isObjectEqual(stageNode, deserializedStageNode));
      Assert.assertEquals(deserializedStageNode.getPlanFragmentId(), stageNode.getPlanFragmentId());
      Assert.assertEquals(deserializedStageNode.getDataSchema(), stageNode.getDataSchema());
      Assert.assertEquals(deserializedStageNode.getInputs().size(), stageNode.getInputs().size());
    }
  }

  @SuppressWarnings({"rawtypes"})
  private boolean isObjectEqual(Object left, Object right)
      throws IllegalAccessException {
    Class<?> clazz = left.getClass();
    while (Object.class != clazz) {
      for (Field field : clazz.getDeclaredFields()) {
        if (field.isAnnotationPresent(ProtoProperties.class)) {
          field.setAccessible(true);
          Object l = field.get(left);
          Object r = field.get(right);
          if (l instanceof List) {
            if (((List) l).size() != ((List) r).size()) {
              return false;
            }
            for (int i = 0; i < ((List) l).size(); i++) {
              if (!isObjectEqual(((List) l).get(i), ((List) r).get(i))) {
                return false;
              }
            }
          } else if (l instanceof Map) {
            if (((Map) l).size() != ((Map) r).size()) {
              return false;
            }
            for (Object key : ((Map) l).keySet()) {
              if (!isObjectEqual(((Map) l).get(key), ((Map) r).get(key))) {
                return false;
              }
            }
          } else {
            if (l == null && r != null || l != null && r == null) {
              return false;
            }
            if (!(l == null && r == null || l != null && l.equals(r) || isObjectEqual(l, r))) {
              return false;
            }
          }
        }
      }
      clazz = clazz.getSuperclass();
    }
    return true;
  }
}
