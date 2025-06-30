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
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;


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

  @Test
  public void shouldHandleHashJoinKeyCollisionLeftJoinWithNulls() {
    // Test LEFT join with both hash collision AND null values
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
            .addRow(1, "Aa")    // Hash collision string
            .addRow(2, "BB")    // Hash collision string
            .addRow(3, null)    // Null key
            .addRow(4, "CC")    // Non-collision string
            .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
            .addRow(2, "Aa")    // Hash collision match
            .addRow(2, "BB")    // Hash collision match
            .addRow(3, null)    // Null key - should NOT match left null
            .addRow(5, "DD")    // No match in left
            .buildWithEos();

    DataSchema resultSchema = new DataSchema(
            new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
            new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING});

    HashJoinOperator operator = getOperator(resultSchema, JoinRelType.LEFT, List.of(1), List.of(1), List.of());
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    assertEquals(resultRows.size(), 4);
    assertEquals(resultRows.get(0), new Object[]{1, "Aa", 2, "Aa"});     // Hash collision match
    assertEquals(resultRows.get(1), new Object[]{2, "BB", 2, "BB"});     // Hash collision match
    assertEquals(resultRows.get(2), new Object[]{3, null, null, null});  // Left null preserved, no match
    assertEquals(resultRows.get(3), new Object[]{4, "CC", null, null});  // Left unmatched preserved
  }

  @Test
  public void shouldHandleRightJoinWithNulls() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
            .addRow(1, "Aa")
            .addRow(2, null)
            .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
            .addRow(2, "Aa")
            .addRow(3, null)
            .addRow(4, "BB")
            .buildWithEos();

    DataSchema resultSchema = new DataSchema(
            new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
            new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING});

    HashJoinOperator operator = getOperator(resultSchema, JoinRelType.RIGHT, List.of(1), List.of(1), List.of());

    // First block: only non-null match
    List<Object[]> resultRows1 = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(1, resultRows1.size());
    assertTrue(containsRow(resultRows1, new Object[]{1, "Aa", 2, "Aa"}));

    // Second block: unmatched right rows
    List<Object[]> resultRows2 = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(2, resultRows2.size());
    assertTrue(containsRow(resultRows2, new Object[]{null, null, 3, null}));
    assertTrue(containsRow(resultRows2, new Object[]{null, null, 4, "BB"}));

    // Third block should be EOS
    assertTrue(operator.nextBlock().isSuccess());
  }

  @Test
  public void shouldHandleFullJoinWithNulls() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
            .addRow(1, "Aa")
            .addRow(2, null)
            .addRow(4, "CC")
            .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
            .addRow(2, "Aa")
            .addRow(2, null)
            .addRow(3, "BB")
            .buildWithEos();

    DataSchema resultSchema = new DataSchema(
            new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
            new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING});

    HashJoinOperator operator = getOperator(resultSchema, JoinRelType.FULL, List.of(1), List.of(1), List.of());

    // First block
    List<Object[]> resultRows1 = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(3, resultRows1.size());

    assertTrue(containsRow(resultRows1, new Object[]{1, "Aa", 2, "Aa"}));   // Match
    assertTrue(containsRow(resultRows1, new Object[]{2, null, null, null})); // Left null unmatched
    assertTrue(containsRow(resultRows1, new Object[]{4, "CC", null, null})); // Left unmatched

    // Second block
    List<Object[]> resultRows2 = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(2, resultRows2.size());

    assertTrue(containsRow(resultRows2, new Object[]{null, null, 2, null})); // Right null unmatched
    assertTrue(containsRow(resultRows2, new Object[]{null, null, 3, "BB"})); // Right unmatched
  }

  private boolean containsRow(List<Object[]> rows, Object[] expectedRow) {
    for (Object[] row : rows) {
      if (java.util.Arrays.equals(row, expectedRow)) {
        return true;
      }
    }
    return false;
  }


  @Test
  public void shouldHandleSemiJoinWithNulls() {
    // Test SEMI join - should not match null keys
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
            .addRow(1, "Aa")
            .addRow(2, null)    // Null key
            .addRow(4, "CC")    // No match in right
            .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
            .addRow(2, "Aa")    // Match for left row 1
            .addRow(3, null)    // Null - should NOT match left null
            .addRow(5, "BB")    // No match in left
            .buildWithEos();

    DataSchema resultSchema = new DataSchema(
            new String[]{"int_col1", "string_col1"},
            new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING});

    HashJoinOperator operator = getOperator(resultSchema, JoinRelType.SEMI, List.of(1), List.of(1), List.of());
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    assertEquals(resultRows.size(), 1);
    assertEquals(resultRows.get(0), new Object[]{1, "Aa"}); // Only non-null match
  }

  @Test
  public void shouldHandleAntiJoinWithNulls() {
    // Test ANTI join - null keys should be preserved (not matched)
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
            .addRow(1, "Aa")    // Has match in right
            .addRow(2, null)    // Null key - no match
            .addRow(4, "CC")    // No match in right
            .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
            .addRow(2, "Aa")    // Match for left row 1
            .addRow(3, null)    // Null - should NOT match left null
            .addRow(5, "BB")    // No match in left
            .buildWithEos();

    DataSchema resultSchema = new DataSchema(
            new String[]{"int_col1", "string_col1"},
            new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING});

    HashJoinOperator operator = getOperator(resultSchema, JoinRelType.ANTI, List.of(1), List.of(1), List.of());
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{2, null}); // Left null preserved (no match)
    assertEquals(resultRows.get(1), new Object[]{4, "CC"}); // Left unmatched preserved
  }

  @Test
  public void shouldHandleCompositeKeyWithNullValues() {
    // Test composite key join (multi-column) with null values
    // This should expose the bug in isNullKey method where it checks for Object[] instead of Key

    DataSchema compositeSchema = new DataSchema(
            new String[]{"int_col", "string_col", "double_col"},
            new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.DOUBLE});

    _leftInput = new BlockListMultiStageOperator.Builder(compositeSchema)
            .addRow(1, "Aa", 1.0)      // Normal row
            .addRow(2, null, 2.0)      // Null in second key component
            .addRow(3, "Cc", null)     // Null in third key component
            .addRow(4, "Dd", 4.0)      // Normal row
            .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(compositeSchema)
            .addRow(1, "Aa", 1.0)      // Match for first left row
            .addRow(2, null, 2.0)      // Should NOT match left null (SQL standard)
            .addRow(3, "Cc", null)     // Should NOT match left null (SQL standard)
            .addRow(5, "Ee", 5.0)      // No match in left
            .buildWithEos();

    DataSchema resultSchema = new DataSchema(
            new String[]{"int_col1", "string_col1", "double_col1", "int_col2", "string_col2", "double_col2"},
            new ColumnDataType[]{
                    ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.DOUBLE,
                    ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.DOUBLE
            });

    // Composite key join on columns 1 and 2 (string_col and double_col)
    HashJoinOperator operator = getOperator(compositeSchema, resultSchema, JoinRelType.LEFT,
            List.of(1, 2), List.of(1, 2), List.of(), PlanNode.NodeHint.EMPTY);

    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Expected behavior per SQL standard:
    // - Row 1: (1, "Aa", 1.0) should match (1, "Aa", 1.0)
    // - Row 2: (2, null, 2.0) should NOT match (2, null, 2.0) -> left preserved with nulls
    // - Row 3: (3, "Cc", null) should NOT match (3, "Cc", null) -> left preserved with nulls
    // - Row 4: (4, "Dd", 4.0) has no match -> left preserved with nulls

    assertEquals(resultRows.size(), 4);
    assertEquals(resultRows.get(0), new Object[]{1, "Aa", 1.0, 1, "Aa", 1.0});      // Match
    assertEquals(resultRows.get(1), new Object[]{2, null, 2.0, null, null, null});  // Left null preserved
    assertEquals(resultRows.get(2), new Object[]{3, "Cc", null, null, null, null}); // Left null preserved
    assertEquals(resultRows.get(3), new Object[]{4, "Dd", 4.0, null, null, null});  // Left unmatched preserved
  }

  @Test
  public void shouldHandleCompositeKeyInnerJoinWithNulls() {
    // Test that composite keys with nulls are properly excluded from INNER join

    DataSchema compositeSchema = new DataSchema(
            new String[]{"int_col", "string_col", "double_col"},
            new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.DOUBLE});

    _leftInput = new BlockListMultiStageOperator.Builder(compositeSchema)
            .addRow(1, "Aa", 1.0)      // Should match
            .addRow(2, null, 2.0)      // Should be excluded (null key)
            .addRow(3, "Cc", 3.0)      // No match in right
            .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(compositeSchema)
            .addRow(1, "Aa", 1.0)      // Match
            .addRow(2, null, 2.0)      // Should be excluded (null key)
            .addRow(4, "Dd", 4.0)      // No match in left
            .buildWithEos();

    DataSchema resultSchema = new DataSchema(
            new String[]{"int_col1", "string_col1", "double_col1", "int_col2", "string_col2", "double_col2"},
            new ColumnDataType[]{
                    ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.DOUBLE,
                    ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.DOUBLE
            });

    // Composite key join on columns 1 and 2 (string_col and double_col)
    HashJoinOperator operator = getOperator(compositeSchema, resultSchema, JoinRelType.INNER,
            List.of(1, 2), List.of(1, 2), List.of(), PlanNode.NodeHint.EMPTY);

    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    // Only the non-null key match should be returned
    assertEquals(resultRows.size(), 1);
    assertArrayEquals(resultRows.get(0), new Object[]{1, "Aa", 1.0, 1, "Aa", 1.0});
  }

  @Test
  public void shouldHandleCompositeKeySemiJoinWithNulls() {
    // Test that SEMI join properly handles composite keys with nulls

    DataSchema compositeSchema = new DataSchema(
            new String[]{"int_col", "string_col", "double_col"},
            new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.DOUBLE});

    _leftInput = new BlockListMultiStageOperator.Builder(compositeSchema)
            .addRow(1, "Aa", 1.0)      // Should match
            .addRow(2, null, 2.0)      // Should be excluded (null key)
            .addRow(3, "Cc", 3.0)      // No match in right
            .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(compositeSchema)
            .addRow(1, "Aa", 1.0)      // Match
            .addRow(2, null, 2.0)      // Should be excluded (null key)
            .addRow(4, "Dd", 4.0)      // No match in left
            .buildWithEos();

    DataSchema resultSchema = new DataSchema(
            new String[]{"int_col1", "string_col1", "double_col1"},
            new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.DOUBLE});

    // Composite key join on columns 1 and 2 (string_col and double_col)
    HashJoinOperator operator = getOperator(resultSchema, JoinRelType.SEMI,
            List.of(1, 2), List.of(1, 2), List.of());

    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Only left rows with non-null keys that have matches should be returned
    assertEquals(resultRows.size(), 1);
    assertEquals(resultRows.get(0), new Object[]{1, "Aa", 1.0});
  }

  @Test
  public void shouldHandleCompositeKeyAntiJoinWithNulls() {
    // Test that ANTI join properly handles composite keys with nulls
    // Per SQL standard, rows with null keys should be included in ANTI join result

    DataSchema compositeSchema = new DataSchema(
            new String[]{"int_col", "string_col", "double_col"},
            new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.DOUBLE});

    _leftInput = new BlockListMultiStageOperator.Builder(compositeSchema)
            .addRow(1, "Aa", 1.0)      // Has match in right
            .addRow(2, null, 2.0)      // Null key - should be included
            .addRow(3, "Cc", 3.0)      // No match in right
            .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(compositeSchema)
            .addRow(1, "Aa", 1.0)      // Match for left row 1
            .addRow(2, null, 2.0)      // Null key - should not match left null
            .addRow(4, "Dd", 4.0)      // No match in left
            .buildWithEos();

    DataSchema resultSchema = new DataSchema(
            new String[]{"int_col1", "string_col1", "double_col1"},
            new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.DOUBLE});

    // Composite key join on columns 1 and 2 (string_col and double_col)
    HashJoinOperator operator = getOperator(resultSchema, JoinRelType.ANTI,
            List.of(1, 2), List.of(1, 2), List.of());

    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Left rows with null keys and unmatched non-null keys should be returned
    assertEquals(resultRows.size(), 2);
    assertTrue(containsRow(resultRows, new Object[]{2, null, 2.0}));  // Null key preserved
    assertTrue(containsRow(resultRows, new Object[]{3, "Cc", 3.0}));  // Unmatched preserved
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
