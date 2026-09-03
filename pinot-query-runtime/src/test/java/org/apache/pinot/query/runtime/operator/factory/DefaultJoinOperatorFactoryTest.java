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
package org.apache.pinot.query.runtime.operator.factory;

import java.util.List;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.runtime.operator.HashJoinOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OperatorTestUtil;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class DefaultJoinOperatorFactoryTest {

  /// Enriched joins were removed. If a plan from an older-version broker still carries an EnrichedJoinNode, the
  /// runtime must reject it explicitly (fail-loud) rather than silently executing a degraded join.
  @Test
  @SuppressWarnings("deprecation")
  public void createEnrichedJoinOperatorThrows() {
    DefaultJoinOperatorFactory factory = new DefaultJoinOperatorFactory();
    assertThrows(UnsupportedOperationException.class,
        () -> factory.createEnrichedJoinOperator(null, null, null, null, null, null));
  }

  @Test(dataProvider = "semiAndAntiJoinTypes")
  public void createHashJoinUsesRightInputSchema(JoinRelType joinType) {
    DataSchema leftSchema = new DataSchema(new String[]{"left_key"},
        new ColumnDataType[]{ColumnDataType.INT});
    DataSchema rightSchema = new DataSchema(new String[]{"right_key"},
        new ColumnDataType[]{ColumnDataType.INT});
    MultiStageOperator operator = createHashJoin(leftSchema, rightSchema, joinType);

    assertTrue(operator instanceof HashJoinOperator);
  }

  @Test(dataProvider = "semiAndAntiJoinTypes")
  public void createHashJoinRejectsRawVariantRightKey(JoinRelType joinType) {
    DataSchema leftSchema = new DataSchema(new String[]{"left_key"},
        new ColumnDataType[]{ColumnDataType.INT});
    DataSchema rightSchema = new DataSchema(new String[]{"right_key"},
        new ColumnDataType[]{ColumnDataType.VARIANT});

    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
        () -> createHashJoin(leftSchema, rightSchema, joinType));
    assertTrue(exception.getMessage().contains("Raw VARIANT values do not support JOIN keys"));
  }

  @Test
  public void createLookupJoinRejectsRawVariantKeys() {
    DataSchema typedSchema = new DataSchema(new String[]{"key"}, new ColumnDataType[]{ColumnDataType.INT});
    DataSchema variantSchema =
        new DataSchema(new String[]{"key"}, new ColumnDataType[]{ColumnDataType.VARIANT});

    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
        () -> createJoin(variantSchema, typedSchema, JoinRelType.INNER, JoinNode.JoinStrategy.LOOKUP));
    assertTrue(exception.getMessage().contains("Raw VARIANT values do not support JOIN keys"));

    exception = expectThrows(IllegalArgumentException.class,
        () -> createJoin(typedSchema, variantSchema, JoinRelType.INNER, JoinNode.JoinStrategy.LOOKUP));
    assertTrue(exception.getMessage().contains("Raw VARIANT values do not support JOIN keys"));
  }

  @Test
  public void createAsofJoinRejectsRawVariantMatchKeys() {
    DataSchema leftSchema =
        new DataSchema(new String[]{"key", "match"}, new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.VARIANT});
    DataSchema rightSchema =
        new DataSchema(new String[]{"key", "match"}, new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG});
    RexExpression matchCondition =
        new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, "GREATER_THAN",
            List.of(new RexExpression.InputRef(1), new RexExpression.InputRef(3)));

    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
        () -> createJoin(leftSchema, rightSchema, JoinRelType.ASOF, JoinNode.JoinStrategy.ASOF, matchCondition));
    assertTrue(exception.getMessage().contains("Raw VARIANT values do not support ASOF JOIN match keys"));

    DataSchema typedLeftSchema =
        new DataSchema(new String[]{"key", "match"}, new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG});
    DataSchema variantRightSchema =
        new DataSchema(new String[]{"key", "match"}, new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.VARIANT});
    exception = expectThrows(IllegalArgumentException.class,
        () -> createJoin(typedLeftSchema, variantRightSchema, JoinRelType.ASOF, JoinNode.JoinStrategy.ASOF,
            matchCondition));
    assertTrue(exception.getMessage().contains("Raw VARIANT values do not support ASOF JOIN match keys"));
  }

  @DataProvider
  private static Object[][] semiAndAntiJoinTypes() {
    return new Object[][]{{JoinRelType.SEMI}, {JoinRelType.ANTI}};
  }

  private static MultiStageOperator createHashJoin(DataSchema leftSchema, DataSchema rightSchema,
      JoinRelType joinType) {
    return createJoin(leftSchema, rightSchema, joinType, JoinNode.JoinStrategy.HASH);
  }

  private static MultiStageOperator createJoin(DataSchema leftSchema, DataSchema rightSchema, JoinRelType joinType,
      JoinNode.JoinStrategy joinStrategy) {
    return createJoin(leftSchema, rightSchema, joinType, joinStrategy, null);
  }

  private static MultiStageOperator createJoin(DataSchema leftSchema, DataSchema rightSchema, JoinRelType joinType,
      JoinNode.JoinStrategy joinStrategy, RexExpression matchCondition) {
    PlanNode leftPlanNode = mock(PlanNode.class);
    when(leftPlanNode.getDataSchema()).thenReturn(leftSchema);
    PlanNode rightPlanNode = mock(PlanNode.class);
    when(rightPlanNode.getDataSchema()).thenReturn(rightSchema);
    JoinNode joinNode =
        new JoinNode(-1, leftSchema, PlanNode.NodeHint.EMPTY, List.of(), joinType, List.of(0), List.of(0), List.of(),
            joinStrategy, matchCondition);
    return new DefaultJoinOperatorFactory().createJoinOperator(OperatorTestUtil.getTracingContext(),
        mock(MultiStageOperator.class), leftPlanNode, mock(MultiStageOperator.class), rightPlanNode, joinNode);
  }
}
