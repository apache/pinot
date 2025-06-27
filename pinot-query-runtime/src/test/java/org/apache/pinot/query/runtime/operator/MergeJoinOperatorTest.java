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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
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


public class MergeJoinOperatorTest {
  private AutoCloseable _mocks;
  private MultiStageOperator _leftInput;
  private MultiStageOperator _rightInput;
  @Mock
  private VirtualServerAddress _serverAddress;

  private static final DataSchema DEFAULT_CHILD_SCHEMA = new DataSchema(new String[]{"int_col", "string_col"},
      new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});

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
  public void shouldHandleLeftSideEmpty() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
        });
    MergeJoinOperator operator = getOperator(resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of(),
        List.of(new RelFieldCollation(0)));
    MseBlock block;
    block = operator.nextBlock();
    assertTrue(block.isSuccess());
  }

  @Test
  public void shouldHandleRightSideEmpty() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
        });
    MergeJoinOperator operator = getOperator(resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of(),
        List.of(new RelFieldCollation(0)));
    MseBlock block;
    block = operator.nextBlock();
    assertTrue(block.isSuccess());
  }

  @Test
  public void shouldHandleBothSideEmpty() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
        });
    MergeJoinOperator operator = getOperator(resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of(),
        List.of(new RelFieldCollation(0)));
    MseBlock block;
    block = operator.nextBlock();
    assertTrue(block.isSuccess());
  }

  @Test
  public void shouldHandleMergeJoinDupKeyInnerJoin() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
        });
    MergeJoinOperator operator = getOperator(resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of(),
        List.of(new RelFieldCollation(0)));
    List<Object[]> resultRows = new ArrayList<>();
    MseBlock block;
    block = operator.nextBlock();
    while (block.isData()) {
      resultRows.addAll(((MseBlock.Data) block).asRowHeap().getRows());
      block = operator.nextBlock();
    }
    assertEquals(resultRows.size(), 4);
    assertEquals(resultRows.get(0), new Object[]{1, "Aa", 2, "Aa"});
    assertEquals(resultRows.get(1), new Object[]{2, "Aa", 2, "Aa"});
    assertEquals(resultRows.get(2), new Object[]{2, "BB", 2, "BB"});
    assertEquals(resultRows.get(3), new Object[]{2, "BB", 3, "BB"});
  }

  @Test
  public void shouldHandleMergeJoinUniqueKeyInnerJoin() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .addRow(1, "BC")
        .addRow(2, "BD")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(3, "BB")
        .addRow(3, "BD")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
        });
    MergeJoinOperator operator = getOperator(resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of(),
        List.of(new RelFieldCollation(0)));
    List<Object[]> resultRows = new ArrayList<>();
    MseBlock block;
    block = operator.nextBlock();
    while (block.isData()) {
      resultRows.addAll(((MseBlock.Data) block).asRowHeap().getRows());
      block = operator.nextBlock();
    }
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{1, "Aa", 2, "Aa"});
    assertEquals(resultRows.get(1), new Object[]{2, "BB", 3, "BB"});
    assertEquals(resultRows.get(2), new Object[]{2, "BD", 3, "BD"});
  }

  @Test
  public void shouldHandleMergeJoinRightEmptyTwoInputBlockInnerJoin() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addBlock(new Object[]{1, "Aa"},
            new Object[]{2, "BB"})
        .addBlock(new Object[]{1, "BC"},
            new Object[]{2, "BD"})
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
        });
    MergeJoinOperator operator = getOperator(resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of(),
        List.of(new RelFieldCollation(0)));
    MseBlock block;
    block = operator.nextBlock();
    assertTrue(block.isSuccess());
  }

  @Test
  public void shouldHandleMergeJoinLeftEmptyTwoInputBlockInnerJoin() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addBlock(new Object[]{1, "Aa"},
            new Object[]{2, "BB"})
        .addBlock(new Object[]{1, "BC"},
            new Object[]{2, "BD"})
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
        });
    MergeJoinOperator operator = getOperator(resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of(),
        List.of(new RelFieldCollation(0)));
    MseBlock block;
    block = operator.nextBlock();
    assertTrue(block.isSuccess());
  }

  @Test
  public void shouldHandleMergeJoinUniqueKeyTwoInputBlockInnerJoin() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addBlock(new Object[]{1, "Aa"},
            new Object[]{2, "BB"})
        .addBlock(new Object[]{1, "BC"},
            new Object[]{2, "BD"})
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(3, "BB")
        .addRow(3, "BD")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
        });
    MergeJoinOperator operator = getOperator(resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of(),
        List.of(new RelFieldCollation(0)));
    List<Object[]> resultRows = new ArrayList<>();
    MseBlock block;
    block = operator.nextBlock();
    while (block.isData()) {
      resultRows.addAll(((MseBlock.Data) block).asRowHeap().getRows());
      block = operator.nextBlock();
    }
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{1, "Aa", 2, "Aa"});
    assertEquals(resultRows.get(1), new Object[]{2, "BB", 3, "BB"});
    assertEquals(resultRows.get(2), new Object[]{2, "BD", 3, "BD"});
  }

  @Test
  public void shouldHandleMergeJoinUniqueKeyLastDoesNotMatchTwoInputBlockInnerJoin() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addBlock(new Object[]{1, "Aa"},
            new Object[]{2, "BB"})
        .addBlock(new Object[]{1, "BC"})
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(3, "BB")
        .addRow(3, "BD")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
        });
    MergeJoinOperator operator = getOperator(resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of(),
        List.of(new RelFieldCollation(0)));
    List<Object[]> resultRows = new ArrayList<>();
    MseBlock block;
    block = operator.nextBlock();
    while (block.isData()) {
      resultRows.addAll(((MseBlock.Data) block).asRowHeap().getRows());
      block = operator.nextBlock();
    }
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{1, "Aa", 2, "Aa"});
    assertEquals(resultRows.get(1), new Object[]{2, "BB", 3, "BB"});
  }

  @Test
  public void shouldHandleMergeJoinDupKeySpanTwoInputBlockInnerJoin() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addBlock(new Object[]{1, "Aa"},
            new Object[]{2, "BB"})
        .addBlock(new Object[]{3, "BB"},
            new Object[]{2, "BD"})
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(3, "BB")
        .addRow(3, "BD")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
        });
    MergeJoinOperator operator = getOperator(resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of(),
        List.of(new RelFieldCollation(0)));
    List<Object[]> resultRows = new ArrayList<>();
    MseBlock block;
    block = operator.nextBlock();
    while (block.isData()) {
      resultRows.addAll(((MseBlock.Data) block).asRowHeap().getRows());
      block = operator.nextBlock();
    }
    assertEquals(resultRows.size(), 4);
    assertEquals(resultRows.get(0), new Object[]{1, "Aa", 2, "Aa"});
    assertEquals(resultRows.get(1), new Object[]{2, "BB", 3, "BB"});
    assertEquals(resultRows.get(2), new Object[]{3, "BB", 3, "BB"});
    assertEquals(resultRows.get(3), new Object[]{2, "BD", 3, "BD"});
  }

  @Test
  public void shouldHandleMergeJoinDupKeyBothSideSpanTwoInputBlockInnerJoin() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addBlock(new Object[]{1, "Aa"},
            new Object[]{2, "BB"})
        .addBlock(new Object[]{3, "BB"},
            new Object[]{2, "BD"})
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addBlock(new Object[]{2, "Aa"},
            new Object[]{2, "BB"})
        .addBlock(new Object[]{3, "BB"},
            new Object[]{3, "BD"})
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
        });
    MergeJoinOperator operator = getOperator(resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of(),
        List.of(new RelFieldCollation(0)));
    List<Object[]> resultRows = new ArrayList<>();
    MseBlock block;
    block = operator.nextBlock();
    while (block.isData()) {
      resultRows.addAll(((MseBlock.Data) block).asRowHeap().getRows());
      block = operator.nextBlock();
    }
    assertEquals(resultRows.size(), 6);
    assertEquals(resultRows.get(0), new Object[]{1, "Aa", 2, "Aa"});
    assertEquals(resultRows.get(1), new Object[]{2, "BB", 2, "BB"});
    assertEquals(resultRows.get(2), new Object[]{2, "BB", 3, "BB"});
    assertEquals(resultRows.get(3), new Object[]{3, "BB", 2, "BB"});
    assertEquals(resultRows.get(4), new Object[]{3, "BB", 3, "BB"});
    assertEquals(resultRows.get(5), new Object[]{2, "BD", 3, "BD"});
  }

  @Test
  public void shouldHandleCompositeKeyInnerJoin() {
    // Test composite key join (multi-column) with null values
    // This should expose the bug in isNullKey method where it checks for Object[] instead of Key

    DataSchema compositeSchema = new DataSchema(
        new String[]{"int_col", "string_col", "double_col"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.DOUBLE
        });

    _leftInput = new BlockListMultiStageOperator.Builder(compositeSchema)
        .addRow(1, "Aa", 1.0)
        .addRow(2, "Cc", 3.0)
        .addRow(4, "Dd", 4.0)
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(compositeSchema)
        .addRow(1, "Aa", 1.0)
        .addRow(2, "Bb", 2.0)
        .addRow(3, "Cc", 3.0)
        .addRow(5, "Ee", 5.0)
        .buildWithEos();

    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "double_col1", "int_col2", "string_col2", "double_col2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE,
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE
        });

    // Composite key join on columns 1 and 2 (string_col and double_col)
    MergeJoinOperator operator = getOperator(compositeSchema, resultSchema, JoinRelType.INNER,
        List.of(1, 2), List.of(1, 2), List.of(), PlanNode.NodeHint.EMPTY,
        List.of(new RelFieldCollation(0), new RelFieldCollation(1)));

    List<Object[]> resultRows = new ArrayList<>();
    MseBlock block;
    block = operator.nextBlock();
    while (block.isData()) {
      resultRows.addAll(((MseBlock.Data) block).asRowHeap().getRows());
      block = operator.nextBlock();
    }
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{1, "Aa", 1.0, 1, "Aa", 1.0});
    assertEquals(resultRows.get(1), new Object[]{2, "Cc", 3.0, 3, "Cc", 3.0});
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
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
        });
    MergeJoinOperator operator = getOperator(resultSchema, JoinRelType.INNER, List.of(0), List.of(0), List.of(),
        List.of(new RelFieldCollation(0)));
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
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
        });
    MergeJoinOperator operator = getOperator(resultSchema, JoinRelType.INNER, List.of(0), List.of(0), List.of(),
        List.of(new RelFieldCollation(0)));
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
        new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"},
            new DataSchema.ColumnDataType[]{
                DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT,
                DataSchema.ColumnDataType.STRING
            });
    PlanNode.NodeHint nodeHint = new PlanNode.NodeHint(Map.of(PinotHintOptions.JOIN_HINT_OPTIONS,
        Map.of(PinotHintOptions.JoinHintOptions.JOIN_OVERFLOW_MODE, "THROW",
            PinotHintOptions.JoinHintOptions.MAX_ROWS_IN_JOIN, "1")));
    MergeJoinOperator operator =
        getOperator(DEFAULT_CHILD_SCHEMA, resultSchema, JoinRelType.INNER, List.of(0), List.of(0), List.of(), nodeHint,
            List.of(new RelFieldCollation(0)));
    MseBlock block = operator.nextBlock();
    assertTrue(block.isError());
    assertTrue(((ErrorMseBlock) block).getErrorMessages().get(QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED)
        .contains("reached number of rows limit"));
    assertTrue(((ErrorMseBlock) block).getErrorMessages().get(QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED)
        .contains("Cannot process join"));
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
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
        });
    PlanNode.NodeHint nodeHint = new PlanNode.NodeHint(Map.of(PinotHintOptions.JOIN_HINT_OPTIONS,
        Map.of(PinotHintOptions.JoinHintOptions.JOIN_OVERFLOW_MODE, "BREAK",
            PinotHintOptions.JoinHintOptions.MAX_ROWS_IN_JOIN, "1")));
    MergeJoinOperator operator =
        getOperator(DEFAULT_CHILD_SCHEMA, resultSchema, JoinRelType.INNER, List.of(0), List.of(0), List.of(), nodeHint,
            List.of(new RelFieldCollation(0)));
    List<Object[]> resultRows1 = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    Mockito.verify(_rightInput).earlyTerminate();
    assertEquals(resultRows1.size(), 1);
    MseBlock block2 = operator.nextBlock();
    assertTrue(block2.isSuccess());
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
        new DataSchema(new String[]{"int_col1", "string_col1", "int_co2", "string_col2"},
            new DataSchema.ColumnDataType[]{
                DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT,
                DataSchema.ColumnDataType.STRING
            });
    PlanNode.NodeHint nodeHint = new PlanNode.NodeHint(Map.of(PinotHintOptions.JOIN_HINT_OPTIONS,
        Map.of(PinotHintOptions.JoinHintOptions.JOIN_OVERFLOW_MODE, "BREAK",
            PinotHintOptions.JoinHintOptions.MAX_ROWS_IN_JOIN, "2")));
    MergeJoinOperator operator =
        getOperator(DEFAULT_CHILD_SCHEMA, resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of(), nodeHint,
            List.of(new RelFieldCollation(1)));

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
  public void shouldHandleCompositeKeyInnerJoinWithNulls() {
    // Test that composite keys with nulls are properly excluded from INNER join

    DataSchema compositeSchema = new DataSchema(
        new String[]{"int_col", "string_col", "double_col"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.DOUBLE
        });

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
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE,
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE
        });

    // Composite key join on columns 1 and 2 (string_col and double_col)
    MergeJoinOperator operator = getOperator(compositeSchema, resultSchema, JoinRelType.INNER,
        List.of(1, 2), List.of(1, 2), List.of(), PlanNode.NodeHint.EMPTY,
        List.of(new RelFieldCollation(1), new RelFieldCollation(2)));

    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    // Only the non-null key match should be returned
    assertEquals(resultRows.size(), 1);
    assertArrayEquals(resultRows.get(0), new Object[]{1, "Aa", 1.0, 1, "Aa", 1.0});
  }

  @Test
  public void shouldHandleCompositeKeyInnerJoinWithNullsDupKeyTwoBlocks() {
    // Test that composite keys with nulls are properly excluded from INNER join

    DataSchema compositeSchema = new DataSchema(
        new String[]{"int_col", "string_col", "double_col"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.DOUBLE
        });

    _leftInput = new BlockListMultiStageOperator.Builder(compositeSchema)
        .addBlock(new Object[]{1, "Aa", 1.0})
        .addBlock(new Object[]{2, "Aa", 1.0}, new Object[]{3, "Cc", 3.0}, new Object[]{2, null, 2.0})
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(compositeSchema)
        .addBlock(new Object[]{1, "Aa", 1.0})
        .addBlock(new Object[]{2, "Aa", 1.0}, new Object[]{4, "Dd", 4.0}, new Object[]{2, null, 2.0})
        .buildWithEos();

    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "double_col1", "int_col2", "string_col2", "double_col2"},
        new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE,
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE
        });

    // Composite key join on columns 1 and 2 (string_col and double_col)
    MergeJoinOperator operator = getOperator(compositeSchema, resultSchema, JoinRelType.INNER,
        List.of(1, 2), List.of(1, 2), List.of(), PlanNode.NodeHint.EMPTY,
        List.of(new RelFieldCollation(1), new RelFieldCollation(2)));

    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    // Only the non-null key match should be returned
    assertEquals(resultRows.size(), 4);
    assertArrayEquals(resultRows.get(0), new Object[]{1, "Aa", 1.0, 1, "Aa", 1.0});
    assertArrayEquals(resultRows.get(1), new Object[]{1, "Aa", 1.0, 2, "Aa", 1.0});
    assertArrayEquals(resultRows.get(2), new Object[]{2, "Aa", 1.0, 1, "Aa", 1.0});
    assertArrayEquals(resultRows.get(3), new Object[]{2, "Aa", 1.0, 2, "Aa", 1.0});
  }

  // TODO: test null handling enabled vs disabled, and non-equi conditions!

  private MergeJoinOperator getOperator(DataSchema leftSchema, DataSchema resultSchema, JoinRelType joinType,
      List<Integer> leftKeys, List<Integer> rightKeys, List<RexExpression> nonEquiConditions,
      PlanNode.NodeHint nodeHint, List<RelFieldCollation> collations) {
    return new MergeJoinOperator(OperatorTestUtil.getTracingContext(), _leftInput, leftSchema, _rightInput,
        new JoinNode(-1, resultSchema, nodeHint, List.of(), joinType, leftKeys, rightKeys, nonEquiConditions,
            JoinNode.JoinStrategy.MERGE, null, collations));
  }

  private MergeJoinOperator getOperator(DataSchema resultSchema, JoinRelType joinType,
      List<Integer> leftKeys, List<Integer> rightKeys, List<RexExpression> nonEquiConditions,
      List<RelFieldCollation> collations) {
    return getOperator(DEFAULT_CHILD_SCHEMA, resultSchema, joinType, leftKeys, rightKeys, nonEquiConditions,
        PlanNode.NodeHint.EMPTY, collations);
  }
}
