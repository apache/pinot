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
import java.util.Map;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockTestUtils;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.spi.exception.QException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


// TODO: Add more inequi join tests.
public class HashJoinOperatorTest {
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
  public void shouldHandleHashJoinKeyCollisionInnerJoin() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    when(_leftInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    when(_rightInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_col2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    HashJoinOperator operator =
        getOperator(leftSchema, resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of());
    List<Object[]> resultRows = operator.nextBlock().getContainer();
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{1, "Aa", 2, "Aa"});
    assertEquals(resultRows.get(1), new Object[]{2, "BB", 2, "BB"});
    assertEquals(resultRows.get(2), new Object[]{2, "BB", 3, "BB"});
  }

  @Test
  public void shouldHandleInnerJoinOnInt() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    when(_leftInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    when(_rightInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_col2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    HashJoinOperator operator =
        getOperator(leftSchema, resultSchema, JoinRelType.INNER, List.of(0), List.of(0), List.of());
    List<Object[]> resultRows = operator.nextBlock().getContainer();
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{2, "BB", 2, "Aa"});
    assertEquals(resultRows.get(1), new Object[]{2, "BB", 2, "BB"});
  }

  @Test
  public void shouldHandleJoinOnEmptySelector() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    when(_leftInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    when(_rightInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_col2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    HashJoinOperator operator =
        getOperator(leftSchema, resultSchema, JoinRelType.INNER, List.of(), List.of(), List.of());
    List<Object[]> resultRows = operator.nextBlock().getContainer();
    assertEquals(resultRows.size(), 6);
    assertEquals(resultRows.get(0), new Object[]{1, "Aa", 2, "Aa"});
    assertEquals(resultRows.get(1), new Object[]{1, "Aa", 2, "BB"});
    assertEquals(resultRows.get(2), new Object[]{1, "Aa", 3, "BB"});
    assertEquals(resultRows.get(3), new Object[]{2, "BB", 2, "Aa"});
    assertEquals(resultRows.get(4), new Object[]{2, "BB", 2, "BB"});
    assertEquals(resultRows.get(5), new Object[]{2, "BB", 3, "BB"});
  }

  @Test
  public void shouldHandleLeftJoin() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    when(_leftInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "CC"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    when(_rightInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    HashJoinOperator operator =
        getOperator(leftSchema, resultSchema, JoinRelType.LEFT, List.of(1), List.of(1), List.of());
    List<Object[]> resultRows = operator.nextBlock().getContainer();
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{1, "Aa", 2, "Aa"});
    assertEquals(resultRows.get(1), new Object[]{2, "CC", null, null});
  }

  @Test
  public void shouldPassLeftTableEOS() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    when(_leftInput.nextBlock()).thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    when(_rightInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{1, "BB"}, new Object[]{1, "CC"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    HashJoinOperator operator =
        getOperator(leftSchema, resultSchema, JoinRelType.INNER, List.of(0), List.of(0), List.of());
    TransferableBlock block = operator.nextBlock();
    assertTrue(block.isEndOfStreamBlock());
  }

  @Test
  public void shouldHandleLeftJoinOneToN() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    when(_leftInput.nextBlock()).thenReturn(OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    when(_rightInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{1, "BB"}, new Object[]{1, "CC"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    HashJoinOperator operator =
        getOperator(leftSchema, resultSchema, JoinRelType.LEFT, List.of(0), List.of(0), List.of());
    List<Object[]> resultRows = operator.nextBlock().getContainer();
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{1, "Aa", 1, "BB"});
    assertEquals(resultRows.get(1), new Object[]{1, "Aa", 1, "CC"});
  }

  @Test
  public void shouldPassRightTableEOS() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    when(_leftInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{1, "BB"}, new Object[]{1, "CC"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    when(_rightInput.nextBlock()).thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    HashJoinOperator operator =
        getOperator(leftSchema, resultSchema, JoinRelType.INNER, List.of(0), List.of(0), List.of());
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock());
  }

  @Test
  public void shouldHandleInequiJoinOnString() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    when(_leftInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    when(_rightInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    List<RexExpression> functionOperands = List.of(new RexExpression.InputRef(1), new RexExpression.InputRef(3));
    List<RexExpression> nonEquiConditions =
        List.of(new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, SqlKind.NOT_EQUALS.name(), functionOperands));
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_col2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    HashJoinOperator operator =
        getOperator(leftSchema, resultSchema, JoinRelType.INNER, List.of(), List.of(), nonEquiConditions);
    List<Object[]> resultRows = operator.nextBlock().getContainer();
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{1, "Aa", 2, "BB"});
    assertEquals(resultRows.get(1), new Object[]{1, "Aa", 3, "BB"});
    assertEquals(resultRows.get(2), new Object[]{2, "BB", 2, "Aa"});
  }

  @Test
  public void shouldHandleInequiJoinOnInt() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    when(_leftInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    when(_rightInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{1, "BB"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    List<RexExpression> functionOperands = List.of(new RexExpression.InputRef(0), new RexExpression.InputRef(2));
    List<RexExpression> nonEquiConditions =
        List.of(new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, SqlKind.NOT_EQUALS.name(), functionOperands));
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    HashJoinOperator operator =
        getOperator(leftSchema, resultSchema, JoinRelType.INNER, List.of(), List.of(), nonEquiConditions);
    List<Object[]> resultRows = operator.nextBlock().getContainer();
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{1, "Aa", 2, "Aa"});
    assertEquals(resultRows.get(1), new Object[]{2, "BB", 1, "BB"});
  }

  @Test
  public void shouldHandleRightJoin() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    when(_leftInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    when(_rightInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema = new DataSchema(new String[]{"foo", "bar", "foo", "bar"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
    });
    HashJoinOperator operator =
        getOperator(leftSchema, resultSchema, JoinRelType.RIGHT, List.of(0), List.of(0), List.of());
    List<Object[]> resultRows1 = operator.nextBlock().getContainer();
    assertEquals(resultRows1.size(), 2);
    assertEquals(resultRows1.get(0), new Object[]{2, "BB", 2, "Aa"});
    assertEquals(resultRows1.get(1), new Object[]{2, "BB", 2, "BB"});
    // Second block should be non-matched broadcast rows
    List<Object[]> resultRows2 = operator.nextBlock().getContainer();
    assertEquals(resultRows2.size(), 1);
    assertEquals(resultRows2.get(0), new Object[]{null, null, 3, "BB"});
    // Third block is EOS block.
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock());
  }

  @Test
  public void shouldHandleSemiJoin() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    when(_leftInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}, new Object[]{4, "CC"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    when(_rightInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema = new DataSchema(new String[]{"foo", "bar"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    HashJoinOperator operator =
        getOperator(leftSchema, resultSchema, JoinRelType.SEMI, List.of(1), List.of(1), List.of());
    List<Object[]> resultRows = operator.nextBlock().getContainer();
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{1, "Aa"});
    assertEquals(resultRows.get(1), new Object[]{2, "BB"});
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock());
  }

  @Test
  public void shouldHandleFullJoin() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    when(_leftInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}, new Object[]{4, "CC"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    when(_rightInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema = new DataSchema(new String[]{"foo", "bar", "foo", "bar"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
    });
    HashJoinOperator operator =
        getOperator(leftSchema, resultSchema, JoinRelType.FULL, List.of(0), List.of(0), List.of());
    List<Object[]> resultRows1 = operator.nextBlock().getContainer();
    assertEquals(resultRows1.size(), 4);
    assertEquals(resultRows1.get(0), new Object[]{1, "Aa", null, null});
    assertEquals(resultRows1.get(1), new Object[]{2, "BB", 2, "Aa"});
    assertEquals(resultRows1.get(2), new Object[]{2, "BB", 2, "BB"});
    assertEquals(resultRows1.get(3), new Object[]{4, "CC", null, null});
    // Second block should be non-matched broadcast rows
    List<Object[]> resultRows2 = operator.nextBlock().getContainer();
    assertEquals(resultRows2.size(), 1);
    assertEquals(resultRows2.get(0), new Object[]{null, null, 3, "BB"});
    // Third block is EOS block.
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock());
  }

  @Test
  public void shouldHandleAntiJoin() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    when(_leftInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}, new Object[]{4, "CC"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    when(_rightInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema = new DataSchema(new String[]{"foo", "bar"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    HashJoinOperator operator =
        getOperator(leftSchema, resultSchema, JoinRelType.ANTI, List.of(1), List.of(1), List.of());
    List<Object[]> resultRows = operator.nextBlock().getContainer();
    assertEquals(resultRows.size(), 1);
    assertEquals(resultRows.get(0), new Object[]{4, "CC"});
    assertTrue(operator.nextBlock().isSuccessfulEndOfStreamBlock());
  }

  @Test
  public void shouldPropagateRightTableError() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    when(_leftInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{1, "BB"}, new Object[]{1, "CC"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    when(_rightInput.nextBlock()).thenReturn(
        TransferableBlockUtils.getErrorTransferableBlock(new Exception("testInnerJoinRightError")));
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    HashJoinOperator operator =
        getOperator(leftSchema, resultSchema, JoinRelType.INNER, List.of(0), List.of(0), List.of());
    TransferableBlock block = operator.nextBlock();
    assertTrue(block.isErrorBlock());
    assertTrue(block.getExceptions().get(QException.UNKNOWN_ERROR_CODE).contains("testInnerJoinRightError"));
  }

  @Test
  public void shouldPropagateLeftTableError() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    when(_rightInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{1, "BB"}, new Object[]{1, "CC"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    when(_leftInput.nextBlock()).thenReturn(
        TransferableBlockUtils.getErrorTransferableBlock(new Exception("testInnerJoinLeftError")));
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    HashJoinOperator operator =
        getOperator(leftSchema, resultSchema, JoinRelType.INNER, List.of(0), List.of(0), List.of());
    TransferableBlock block = operator.nextBlock();
    assertTrue(block.isErrorBlock());
    assertTrue(block.getExceptions().get(QException.UNKNOWN_ERROR_CODE).contains("testInnerJoinLeftError"));
  }

  @Test
  public void shouldPropagateRightInputJoinLimitError() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    when(_leftInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    when(_rightInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    PlanNode.NodeHint nodeHint = new PlanNode.NodeHint(Map.of(PinotHintOptions.JOIN_HINT_OPTIONS,
        Map.of(PinotHintOptions.JoinHintOptions.JOIN_OVERFLOW_MODE, "THROW",
            PinotHintOptions.JoinHintOptions.MAX_ROWS_IN_JOIN, "1")));
    HashJoinOperator operator =
        getOperator(leftSchema, resultSchema, JoinRelType.INNER, List.of(0), List.of(0), List.of(), nodeHint);
    TransferableBlock block = operator.nextBlock();
    assertTrue(block.isErrorBlock());
    assertTrue(block.getExceptions().get(QException.SERVER_RESOURCE_LIMIT_EXCEEDED_ERROR_CODE)
        .contains("reached number of rows limit"));
    assertTrue(block.getExceptions().get(QException.SERVER_RESOURCE_LIMIT_EXCEEDED_ERROR_CODE)
        .contains("Cannot build in memory hash table"));
  }

  @Test
  public void shouldHandleJoinWithPartialResultsWhenHitDataRowsLimitOnRightInput() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    when(_leftInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    when(_rightInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    PlanNode.NodeHint nodeHint = new PlanNode.NodeHint(Map.of(PinotHintOptions.JOIN_HINT_OPTIONS,
        Map.of(PinotHintOptions.JoinHintOptions.JOIN_OVERFLOW_MODE, "BREAK",
            PinotHintOptions.JoinHintOptions.MAX_ROWS_IN_JOIN, "1")));
    HashJoinOperator operator =
        getOperator(leftSchema, resultSchema, JoinRelType.INNER, List.of(0), List.of(0), List.of(), nodeHint);
    List<Object[]> resultRows1 = operator.nextBlock().getContainer();
    Mockito.verify(_rightInput).earlyTerminate();
    assertEquals(resultRows1.size(), 1);
    TransferableBlock block2 = operator.nextBlock();
    assertTrue(block2.isSuccessfulEndOfStreamBlock());
    StatMap<HashJoinOperator.StatKey> statMap = OperatorTestUtil.getStatMap(HashJoinOperator.StatKey.class, block2);
    assertTrue(statMap.getBoolean(HashJoinOperator.StatKey.MAX_ROWS_IN_JOIN_REACHED),
        "Max rows in join should be reached");
  }

  @Test
  public void shouldPropagateLeftInputJoinLimitError() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    when(_leftInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    when(_rightInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}, new Object[]{2, "BB"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    PlanNode.NodeHint nodeHint = new PlanNode.NodeHint(Map.of(PinotHintOptions.JOIN_HINT_OPTIONS,
        Map.of(PinotHintOptions.JoinHintOptions.JOIN_OVERFLOW_MODE, "THROW",
            PinotHintOptions.JoinHintOptions.MAX_ROWS_IN_JOIN, "2")));
    HashJoinOperator operator =
        getOperator(leftSchema, resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of(), nodeHint);
    TransferableBlock block = operator.nextBlock();
    assertTrue(block.isErrorBlock());
    assertTrue(block.getExceptions().get(QException.SERVER_RESOURCE_LIMIT_EXCEEDED_ERROR_CODE)
        .contains("reached number of rows limit"));
    assertTrue(block.getExceptions().get(QException.SERVER_RESOURCE_LIMIT_EXCEEDED_ERROR_CODE)
        .contains("Cannot process join"));
  }

  @Test
  public void shouldHandleJoinWithPartialResultsWhenHitDataRowsLimitOnLeftInput() {
    DataSchema leftSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    DataSchema rightSchema = new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
        ColumnDataType.INT, ColumnDataType.STRING
    });
    when(_leftInput.nextBlock()).thenReturn(
            OperatorTestUtil.block(leftSchema, new Object[]{1, "Aa"}, new Object[]{2, "Aa"}, new Object[]{3, "Aa"}))
        .thenReturn(OperatorTestUtil.block(leftSchema, new Object[]{4, "Aa"}, new Object[]{5, "Aa"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    when(_rightInput.nextBlock()).thenReturn(OperatorTestUtil.block(rightSchema, new Object[]{2, "Aa"}))
        .thenReturn(TransferableBlockTestUtils.getEndOfStreamTransferableBlock(0));
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    PlanNode.NodeHint nodeHint = new PlanNode.NodeHint(Map.of(PinotHintOptions.JOIN_HINT_OPTIONS,
        Map.of(PinotHintOptions.JoinHintOptions.JOIN_OVERFLOW_MODE, "BREAK",
            PinotHintOptions.JoinHintOptions.MAX_ROWS_IN_JOIN, "2")));
    HashJoinOperator operator =
        getOperator(leftSchema, resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of(), nodeHint);
    List<Object[]> resultRows = operator.nextBlock().getContainer();
    Mockito.verify(_leftInput).earlyTerminate();
    assertEquals(resultRows.size(), 2);
    TransferableBlock block2 = operator.nextBlock();
    assertTrue(block2.isSuccessfulEndOfStreamBlock());
    StatMap<HashJoinOperator.StatKey> statMap = OperatorTestUtil.getStatMap(HashJoinOperator.StatKey.class, block2);
    assertTrue(statMap.getBoolean(HashJoinOperator.StatKey.MAX_ROWS_IN_JOIN_REACHED),
        "Max rows in join should be reached");
  }

  private HashJoinOperator getOperator(DataSchema leftSchema, DataSchema resultSchema, JoinRelType joinType,
      List<Integer> leftKeys, List<Integer> rightKeys, List<RexExpression> nonEquiConditions,
      PlanNode.NodeHint nodeHint) {
    return new HashJoinOperator(OperatorTestUtil.getTracingContext(), _leftInput, leftSchema, _rightInput,
        new JoinNode(-1, resultSchema, nodeHint, List.of(), joinType, leftKeys, rightKeys, nonEquiConditions,
            JoinNode.JoinStrategy.HASH));
  }

  private HashJoinOperator getOperator(DataSchema leftSchema, DataSchema resultSchema, JoinRelType joinType,
      List<Integer> leftKeys, List<Integer> rightKeys, List<RexExpression> nonEquiConditions) {
    return getOperator(leftSchema, resultSchema, joinType, leftKeys, rightKeys, nonEquiConditions,
        PlanNode.NodeHint.EMPTY);
  }
}
