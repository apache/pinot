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
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class HashJoinOperatorTest {
  private AutoCloseable _mocks;
  private MultiStageOperator _leftInput;
  private MultiStageOperator _rightInput;
  @Mock
  private VirtualServerAddress _serverAddress;

  private static final DataSchema DEFAULT_CHILD_SCHEMA = new DataSchema(new String[]{"int_col", "string_col"},
      new ColumnDataType[] {ColumnDataType.INT, ColumnDataType.STRING});
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
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING});
    HashJoinOperator operator = getOperator(resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of());
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{1, "Aa", 2, "Aa"});
    assertEquals(resultRows.get(1), new Object[]{2, "BB", 2, "BB"});
    assertEquals(resultRows.get(2), new Object[]{2, "BB", 3, "BB"});
  }

  @Test
  public void shouldHandleInnerJoinOnInt() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();
    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING});
    HashJoinOperator operator = getOperator(resultSchema, JoinRelType.INNER, List.of(0), List.of(0), List.of());
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{2, "BB", 2, "Aa"});
    assertEquals(resultRows.get(1), new Object[]{2, "BB", 2, "BB"});
  }

  @Test
  public void shouldHandleLeftJoin() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "CC")
        .buildWithEos();
    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING});
    HashJoinOperator operator = getOperator(resultSchema, JoinRelType.LEFT, List.of(1), List.of(1), List.of());
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{1, "Aa", 2, "Aa"});
    assertEquals(resultRows.get(1), new Object[]{2, "CC", null, null});
  }

  @Test
  public void shouldPassLeftTableEOS() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA).buildWithEos();
    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "BB")
        .addRow(1, "CC")
        .addRow(3, "BB")
        .buildWithEos();
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    HashJoinOperator operator = getOperator(resultSchema, JoinRelType.INNER, List.of(0), List.of(0), List.of());
    MseBlock block = operator.nextBlock();
    assertTrue(block.isEos());
  }

  @Test
  public void shouldHandleLeftJoinOneToN() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "Aa")
        .buildWithEos();
    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "BB")
        .addRow(1, "CC")
        .addRow(3, "BB")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING});
    HashJoinOperator operator = getOperator(resultSchema, JoinRelType.LEFT, List.of(0), List.of(0), List.of());
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{1, "Aa", 1, "BB"});
    assertEquals(resultRows.get(1), new Object[]{1, "Aa", 1, "CC"});
  }

  @Test
  public void shouldPassRightTableEOS() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "BB")
        .addRow(1, "CC")
        .addRow(3, "BB")
        .buildWithEos();
    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA).buildWithEos();
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    HashJoinOperator operator = getOperator(resultSchema, JoinRelType.INNER, List.of(0), List.of(0), List.of());
    assertTrue(operator.nextBlock().isSuccess());
  }

  @Test
  public void shouldHandleRightJoin() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();
    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(new String[]{"foo", "bar", "foo", "bar"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING});
    HashJoinOperator operator = getOperator(resultSchema, JoinRelType.RIGHT, List.of(0), List.of(0), List.of());
    List<Object[]> resultRows1 = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows1.size(), 2);
    assertEquals(resultRows1.get(0), new Object[]{2, "BB", 2, "Aa"});
    assertEquals(resultRows1.get(1), new Object[]{2, "BB", 2, "BB"});
    // Second block should be non-matched broadcast rows
    List<Object[]> resultRows2 = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows2.size(), 1);
    assertEquals(resultRows2.get(0), new Object[]{null, null, 3, "BB"});
    // Third block is EOS block.
    assertTrue(operator.nextBlock().isSuccess());
  }

  @Test
  public void shouldHandleSemiJoin() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .addRow(4, "CC")
        .buildWithEos();
    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(new String[]{"foo", "bar"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING});
    HashJoinOperator operator = getOperator(resultSchema, JoinRelType.SEMI, List.of(1), List.of(1), List.of());
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{1, "Aa"});
    assertEquals(resultRows.get(1), new Object[]{2, "BB"});
    assertTrue(operator.nextBlock().isSuccess());
  }

  @Test
  public void shouldHandleFullJoin() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .addRow(4, "CC")
        .buildWithEos();
    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(new String[]{"foo", "bar", "foo", "bar"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING});
    HashJoinOperator operator = getOperator(resultSchema, JoinRelType.FULL, List.of(0), List.of(0), List.of());
    List<Object[]> resultRows1 = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows1.size(), 4);
    assertEquals(resultRows1.get(0), new Object[]{1, "Aa", null, null});
    assertEquals(resultRows1.get(1), new Object[]{2, "BB", 2, "Aa"});
    assertEquals(resultRows1.get(2), new Object[]{2, "BB", 2, "BB"});
    assertEquals(resultRows1.get(3), new Object[]{4, "CC", null, null});
    // Second block should be non-matched broadcast rows
    List<Object[]> resultRows2 = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows2.size(), 1);
    assertEquals(resultRows2.get(0), new Object[]{null, null, 3, "BB"});
    // Third block is EOS block.
    assertTrue(operator.nextBlock().isSuccess());
  }

  @Test
  public void shouldHandleAntiJoin() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .addRow(4, "CC")
        .buildWithEos();
    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(new String[]{"foo", "bar"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING});
    HashJoinOperator operator = getOperator(resultSchema, JoinRelType.ANTI, List.of(1), List.of(1), List.of());
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows.size(), 1);
    assertEquals(resultRows.get(0), new Object[]{4, "CC"});
    assertTrue(operator.nextBlock().isSuccess());
  }

  @Test
  public void shouldPropagateRightTableError() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "BB")
        .addRow(1, "CC")
        .addRow(3, "BB")
        .buildWithEos();
    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .buildWithError(ErrorMseBlock.fromException(new Exception("testInnerJoinRightError")));
    DataSchema resultSchema = new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING});
    HashJoinOperator operator = getOperator(resultSchema, JoinRelType.INNER, List.of(0), List.of(0), List.of());
    MseBlock block = operator.nextBlock();
    assertTrue(block.isError());
    assertTrue(((ErrorMseBlock) block).getErrorMessages()
        .get(QueryErrorCode.UNKNOWN).contains("testInnerJoinRightError"));
  }

  @Test
  public void shouldPropagateLeftTableError() {
    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "BB")
        .addRow(1, "CC")
        .addRow(3, "BB")
        .buildWithEos();
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .buildWithError(ErrorMseBlock.fromException(new Exception("testInnerJoinLeftError")));
    DataSchema resultSchema = new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING});
    HashJoinOperator operator = getOperator(resultSchema, JoinRelType.INNER, List.of(0), List.of(0), List.of());
    MseBlock block = operator.nextBlock();
    assertTrue(block.isError());
    assertTrue(((ErrorMseBlock) block).getErrorMessages()
        .get(QueryErrorCode.UNKNOWN).contains("testInnerJoinLeftError"));
  }

  @Test
  public void shouldPropagateRightInputJoinLimitError() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();
    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();
    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    PlanNode.NodeHint nodeHint = new PlanNode.NodeHint(Map.of(PinotHintOptions.JOIN_HINT_OPTIONS,
        Map.of(PinotHintOptions.JoinHintOptions.JOIN_OVERFLOW_MODE, "THROW",
            PinotHintOptions.JoinHintOptions.MAX_ROWS_IN_JOIN, "1")));
    HashJoinOperator operator =
        getOperator(resultSchema, JoinRelType.INNER, List.of(0), List.of(0), List.of(), nodeHint);
    MseBlock block = operator.nextBlock();
    assertTrue(block.isError());
    assertTrue(((ErrorMseBlock) block).getErrorMessages().get(QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED)
        .contains("reached number of rows limit"));
    assertTrue(((ErrorMseBlock) block).getErrorMessages().get(QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED)
        .contains("Cannot build in memory hash table"));
  }

  @Test
  public void shouldHandleJoinWithPartialResultsWhenHitDataRowsLimitOnRightInput() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();
    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .spied()
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"},
            new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING});
    PlanNode.NodeHint nodeHint = new PlanNode.NodeHint(Map.of(PinotHintOptions.JOIN_HINT_OPTIONS,
        Map.of(PinotHintOptions.JoinHintOptions.JOIN_OVERFLOW_MODE, "BREAK",
            PinotHintOptions.JoinHintOptions.MAX_ROWS_IN_JOIN, "1")));
    HashJoinOperator operator =
        getOperator(resultSchema, JoinRelType.INNER, List.of(0), List.of(0), List.of(), nodeHint);
    List<Object[]> resultRows1 = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    Mockito.verify(_rightInput).earlyTerminate();
    assertEquals(resultRows1.size(), 1);
    MseBlock block2 = operator.nextBlock();
    assertTrue(block2.isSuccess());


    StatMap<HashJoinOperator.StatKey> statMap =
        OperatorTestUtil.getStatMap(HashJoinOperator.StatKey.class, operator.calculateStats());
    assertTrue(statMap.getBoolean(HashJoinOperator.StatKey.MAX_ROWS_IN_JOIN_REACHED),
        "Max rows in join should be reached");
  }

  @Test
  public void shouldPropagateLeftInputJoinLimitError() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();
    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING});
    PlanNode.NodeHint nodeHint = new PlanNode.NodeHint(Map.of(PinotHintOptions.JOIN_HINT_OPTIONS,
        Map.of(PinotHintOptions.JoinHintOptions.JOIN_OVERFLOW_MODE, "THROW",
            PinotHintOptions.JoinHintOptions.MAX_ROWS_IN_JOIN, "2")));
    HashJoinOperator operator =
        getOperator(resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of(), nodeHint);
    MseBlock block = operator.nextBlock();
    assertTrue(block.isError());
    assertTrue(((ErrorMseBlock) block).getErrorMessages().get(QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED)
        .contains("reached number of rows limit"));
    assertTrue(((ErrorMseBlock) block).getErrorMessages().get(QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED)
        .contains("Cannot process join"));
  }

  @Test
  public void shouldHandleJoinWithPartialResultsWhenHitDataRowsLimitOnLeftInput() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .spied()
        .addRow(1, "Aa")
        .addRow(2, "Aa")
        .addRow(3, "Aa")
        .finishBlock()
        .addRow(4, "Aa")
        .addRow(5, "Aa")
        .buildWithEos();
    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .spied()
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();

    DataSchema resultSchema =
        new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"}, new ColumnDataType[]{
            ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING
        });
    PlanNode.NodeHint nodeHint = new PlanNode.NodeHint(Map.of(PinotHintOptions.JOIN_HINT_OPTIONS,
        Map.of(PinotHintOptions.JoinHintOptions.JOIN_OVERFLOW_MODE, "BREAK",
            PinotHintOptions.JoinHintOptions.MAX_ROWS_IN_JOIN, "2")));
    HashJoinOperator operator =
        getOperator(resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of(), nodeHint);

    // When
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then
    Mockito.verify(_leftInput).earlyTerminate();
    assertEquals(resultRows.size(), 2);
    MseBlock block2 = operator.nextBlock();
    assertTrue(block2.isSuccess());
    StatMap<HashJoinOperator.StatKey> statMap =
        OperatorTestUtil.getStatMap(HashJoinOperator.StatKey.class, operator.calculateStats());
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

  private HashJoinOperator getOperator(DataSchema resultSchema, JoinRelType joinType,
      List<Integer> leftKeys, List<Integer> rightKeys, List<RexExpression> nonEquiConditions,
      PlanNode.NodeHint nodeHint) {
    return new HashJoinOperator(OperatorTestUtil.getTracingContext(), _leftInput, DEFAULT_CHILD_SCHEMA, _rightInput,
        new JoinNode(-1, resultSchema, nodeHint, List.of(), joinType, leftKeys, rightKeys, nonEquiConditions,
            JoinNode.JoinStrategy.HASH));
  }

  private HashJoinOperator getOperator(DataSchema resultSchema, JoinRelType joinType,
      List<Integer> leftKeys, List<Integer> rightKeys, List<RexExpression> nonEquiConditions) {
    return getOperator(DEFAULT_CHILD_SCHEMA, resultSchema, joinType, leftKeys, rightKeys, nonEquiConditions,
        PlanNode.NodeHint.EMPTY);
  }
}
