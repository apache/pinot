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
package org.apache.pinot.query.runtime.operator;

import java.util.List;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.mockito.Mock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


// TODO: Add more tests.
public class NonEquiJoinOperatorTest {
  private AutoCloseable _mocks;
  @Mock
  private MultiStageOperator _leftInput;
  @Mock
  private MultiStageOperator _rightInput;
  @Mock
  private VirtualServerAddress _serverAddress;

  @BeforeMethod
  public void setUp() {
    _mocks = openMocks(this);
    when(_serverAddress.toString()).thenReturn(new VirtualServerAddress("mock", 80, 0).toString());
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void shouldHandleCrossJoin() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    when(_leftInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    when(_rightInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_col2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    NonEquiJoinOperator operator = getOperator(leftSchema, resultSchema, JoinRelType.INNER, List.of());
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows.size(), 6);
    assertEquals(resultRows.get(0), new Object[]{1, "Aa", 2, "Aa"});
    assertEquals(resultRows.get(1), new Object[]{1, "Aa", 2, "BB"});
    assertEquals(resultRows.get(2), new Object[]{1, "Aa", 3, "BB"});
    assertEquals(resultRows.get(3), new Object[]{2, "BB", 2, "Aa"});
    assertEquals(resultRows.get(4), new Object[]{2, "BB", 2, "BB"});
    assertEquals(resultRows.get(5), new Object[]{2, "BB", 3, "BB"});
  }

  @Test
  public void shouldHandleNonEquiJoinOnString() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    when(_leftInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    when(_rightInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RexExpression> functionOperands = List.of(new RexExpression.InputRef(1), new RexExpression.InputRef(3));
    List<RexExpression> nonEquiConditions =
        List.of(new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, SqlKind.NOT_EQUALS.name(), functionOperands));
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_col2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    NonEquiJoinOperator operator = getOperator(leftSchema, resultSchema, JoinRelType.INNER, nonEquiConditions);
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{1, "Aa", 2, "BB"});
    assertEquals(resultRows.get(1), new Object[]{1, "Aa", 3, "BB"});
    assertEquals(resultRows.get(2), new Object[]{2, "BB", 2, "Aa"});
  }

  @Test
  public void shouldHandleNonEquiJoinOnInt() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    when(_leftInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    when(_rightInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{1, "BB"}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RexExpression> functionOperands = List.of(new RexExpression.InputRef(0), new RexExpression.InputRef(2));
    List<RexExpression> nonEquiConditions =
        List.of(new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, SqlKind.NOT_EQUALS.name(), functionOperands));
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    NonEquiJoinOperator operator = getOperator(leftSchema, resultSchema, JoinRelType.INNER, nonEquiConditions);
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{1, "Aa", 2, "Aa"});
    assertEquals(resultRows.get(1), new Object[]{2, "BB", 1, "BB"});
  }

  private NonEquiJoinOperator getOperator(DataSchema leftSchema, DataSchema resultSchema, JoinRelType joinType,
      List<RexExpression> nonEquiConditions, PlanNode.NodeHint nodeHint) {
    return new NonEquiJoinOperator(OperatorTestUtil.getTracingContext(), _leftInput, leftSchema, _rightInput,
        new JoinNode(-1, resultSchema, nodeHint, List.of(), joinType, List.of(), List.of(), nonEquiConditions,
            JoinNode.JoinStrategy.HASH));
  }

  @Test
  public void shouldHandleRightJoinWithNonEquiCondition() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    when(_leftInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    when(_rightInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{5, "CC"}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    // Condition: left.int_col < right.int_col
    List<RexExpression> functionOperands = List.of(new RexExpression.InputRef(0), new RexExpression.InputRef(2));
    List<RexExpression> nonEquiConditions =
        List.of(new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, SqlKind.LESS_THAN.name(), functionOperands));
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_col2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    NonEquiJoinOperator operator = getOperator(leftSchema, resultSchema, JoinRelType.RIGHT, nonEquiConditions);
    // First block: joined rows
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    // left(1) < right(2): match -> (1, "Aa", 2, "Aa")
    // left(2) < right(2): no match (2 < 2 is false)
    // left(1) < right(5): match -> (1, "Aa", 5, "CC")
    // left(2) < right(5): match -> (2, "BB", 5, "CC")
    // right(2) matched by left(1), right(5) matched by left(1) and left(2)
    // So no unmatched right rows
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{1, "Aa", 2, "Aa"});
    assertEquals(resultRows.get(1), new Object[]{1, "Aa", 5, "CC"});
    assertEquals(resultRows.get(2), new Object[]{2, "BB", 5, "CC"});

    assertTrue(operator.nextBlock().isSuccess());
  }

  @Test
  public void shouldHandleRightJoinWithUnmatchedRightRows() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    when(_leftInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(leftSchema, new Object[]{5, "Aa"}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    when(_rightInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "BB"}, new Object[]{10, "CC"}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    // Condition: left.int_col < right.int_col
    List<RexExpression> functionOperands = List.of(new RexExpression.InputRef(0), new RexExpression.InputRef(2));
    List<RexExpression> nonEquiConditions =
        List.of(new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, SqlKind.LESS_THAN.name(), functionOperands));
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_col2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    NonEquiJoinOperator operator = getOperator(leftSchema, resultSchema, JoinRelType.RIGHT, nonEquiConditions);
    // First block: joined rows + unmatched left (none for RIGHT)
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    // left(5) < right(2): no (5 < 2 false)
    // left(5) < right(10): yes -> (5, "Aa", 10, "CC")
    // right(2) is unmatched
    assertEquals(resultRows.size(), 1);
    assertEquals(resultRows.get(0), new Object[]{5, "Aa", 10, "CC"});
    // Second block: unmatched right rows
    MseBlock secondBlock = operator.nextBlock();
    List<Object[]> unmatchedRightRows = ((MseBlock.Data) secondBlock).asRowHeap().getRows();
    assertEquals(unmatchedRightRows.size(), 1);
    assertEquals(unmatchedRightRows.get(0), new Object[]{null, null, 2, "BB"});

    assertTrue(operator.nextBlock().isSuccess());
  }

  @Test
  public void shouldHandleFullOuterJoinWithNonEquiCondition() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    when(_leftInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{10, "BB"}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    when(_rightInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{5, "CC"}, new Object[]{0, "DD"}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    // Condition: left.int_col < right.int_col
    List<RexExpression> functionOperands = List.of(new RexExpression.InputRef(0), new RexExpression.InputRef(2));
    List<RexExpression> nonEquiConditions =
        List.of(new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, SqlKind.LESS_THAN.name(), functionOperands));
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_col2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    NonEquiJoinOperator operator = getOperator(leftSchema, resultSchema, JoinRelType.FULL, nonEquiConditions);
    // First block: joined rows + unmatched left rows
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    // left(1) < right(5): yes -> (1, "Aa", 5, "CC")
    // left(1) < right(0): no
    // left(10) < right(5): no
    // left(10) < right(0): no
    // left(10) has no match -> emitted with nulls on right: (10, "BB", null, null)
    // right(5) matched by left(1), right(0) has no match
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{1, "Aa", 5, "CC"});
    assertEquals(resultRows.get(1), new Object[]{10, "BB", null, null});
    // Second block: unmatched right rows
    MseBlock secondBlock = operator.nextBlock();
    List<Object[]> unmatchedRightRows = ((MseBlock.Data) secondBlock).asRowHeap().getRows();
    assertEquals(unmatchedRightRows.size(), 1);
    assertEquals(unmatchedRightRows.get(0), new Object[]{null, null, 0, "DD"});

    assertTrue(operator.nextBlock().isSuccess());
  }

  private NonEquiJoinOperator getOperator(DataSchema leftSchema, DataSchema resultSchema, JoinRelType joinType,
      List<RexExpression> nonEquiConditions) {
    return getOperator(leftSchema, resultSchema, joinType, nonEquiConditions, PlanNode.NodeHint.EMPTY);
  }
}
