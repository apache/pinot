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
package org.apache.pinot.query.planner.physical.v2.validation;

import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.PRelNodeTreeValidator;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.utils.CommonConstants;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class LiteModeJoinValidationTest {

  private static PhysicalPlannerContext buildContext(boolean liteModeJoinsEnabled, boolean useLiteMode) {
    return new PhysicalPlannerContext(
        null,
        "localhost",
        8000,
        1L,
        "Broker_localhost",
        Map.of(),
        useLiteMode,
        false,
        false,
        CommonConstants.Broker.DEFAULT_LITE_MODE_LEAF_STAGE_LIMIT,
        KeySelector.DEFAULT_HASH_ALGORITHM,
        CommonConstants.Broker.DEFAULT_LITE_MODE_LEAF_STAGE_FAN_OUT_ADJUSTED_LIMIT,
        liteModeJoinsEnabled);
  }

  private static PRelNode makeJoinPlan() {
    PRelNode node = Mockito.mock(PRelNode.class);
    Join join = Mockito.mock(Join.class);
    Mockito.doReturn(join).when(node).unwrap();
    Mockito.doReturn(List.of()).when(node).getPRelInputs();
    return node;
  }

  private static PRelNode makeNoJoinPlan() {
    PRelNode node = Mockito.mock(PRelNode.class);
    RelNode rel = Mockito.mock(RelNode.class);
    Mockito.doReturn(rel).when(node).unwrap();
    Mockito.doReturn(List.of()).when(node).getPRelInputs();
    return node;
  }

  @Test
  public void testJoinEnabledByDefaultInLiteMode() {
    PhysicalPlannerContext ctx = buildContext(CommonConstants.Broker.DEFAULT_LITE_MODE_ENABLE_JOINS, true);
    PRelNode plan = makeJoinPlan();
    PRelNodeTreeValidator.validate(plan, ctx);
  }

  @Test
  public void testJoinNotAllowedWhenEnabledInLiteMode() {
    PhysicalPlannerContext ctx = buildContext(false, true);
    PRelNode plan = makeJoinPlan();
    try {
      PRelNodeTreeValidator.validate(plan, ctx);
      Assert.fail("Expected QUERY_PLANNING error due to joins disabled in lite mode");
    } catch (QueryException qe) {
      Assert.assertEquals(qe.getErrorCode(), QueryErrorCode.QUERY_PLANNING);
      Assert.assertTrue(qe.getMessage().contains("Joins are disabled in lite mode"), qe.getMessage());
    }
  }

  @Test
  public void testNoJoinPassesEvenWhenDisabled() {
    PhysicalPlannerContext ctx = buildContext(false, true);
    PRelNode plan = makeNoJoinPlan();
    PRelNodeTreeValidator.validate(plan, ctx);
  }
}
