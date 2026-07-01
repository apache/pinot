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
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.mockito.Mock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Unit coverage for [RepeatOperator]'s per-set row expansion. Output rows have the shape
/// `[input columns..., group-key copies..., $groupingId]`: the original input columns must stay UNTOUCHED
/// (aggregation arguments may reference a grouping column), the appended key copies carry the group-key values
/// with NULL where the column is rolled up in the set, and `$groupingId` is the set's ordinal in the plan's set
/// list. The expansion is streamed — one grouping set per output block — so tests collect blocks until EOS.
public class RepeatOperatorTest {
  private AutoCloseable _mocks;
  @Mock
  private MultiStageOperator _input;

  @BeforeMethod
  public void setUp() {
    _mocks = openMocks(this);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  /// Input [d1, d2, cnt]; union group keys are d1 (idx 0) and d2 (idx 1); result appends the two key copies and
  /// $groupingId: [d1, d2, cnt, key0, key1, $groupingId].
  private static final DataSchema INPUT_SCHEMA = new DataSchema(new String[]{"d1", "d2", "cnt"},
      new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.INT});
  private static final DataSchema RESULT_SCHEMA = new DataSchema(
      new String[]{"d1", "d2", "cnt", "$groupingSetKey0", "$groupingSetKey1", "$groupingId"},
      new ColumnDataType[]{
          ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.STRING,
          ColumnDataType.STRING, ColumnDataType.INT
      });

  /// Drains the operator until EOS, returning all emitted rows in block order (one block per grouping set).
  private static List<Object[]> collectRows(RepeatOperator operator) {
    List<Object[]> rows = new ArrayList<>();
    MseBlock block = operator.nextBlock();
    while (!block.isEos()) {
      rows.addAll(((MseBlock.Data) block).asRowHeap().getRows());
      block = operator.nextBlock();
    }
    return rows;
  }

  @Test
  public void shouldExpandRollupAndNullRolledUpKeyCopies() {
    /// ROLLUP(d1, d2) => sets {d1,d2}, {d1}, {} — member union-column indexes (0,1), (0), (); ordinal = position.
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(INPUT_SCHEMA, new Object[]{"a", "x", 5}),
        SuccessMseBlock.INSTANCE);
    RepeatOperator operator = new RepeatOperator(OperatorTestUtil.getTracingContext(), _input, new int[]{0, 1},
        List.of(List.of(0, 1), List.of(0), List.of()), RESULT_SCHEMA);

    List<Object[]> rows = collectRows(operator);
    assertEquals(rows.size(), 3); /// one input row x three sets

    /// {d1,d2} (ordinal 0): both key copies carry the values.
    assertEquals(rows.get(0), new Object[]{"a", "x", 5, "a", "x", 0});
    /// {d1} (ordinal 1): the d2 key copy is NULLed — but the ORIGINAL d2 column must stay untouched, because
    /// aggregation arguments may reference it (e.g. SUM(d2) under ROLLUP(d1, d2)).
    assertEquals(rows.get(1), new Object[]{"a", "x", 5, "a", null, 1});
    /// {} (ordinal 2): both key copies NULLed, originals untouched.
    assertEquals(rows.get(2), new Object[]{"a", "x", 5, null, null, 2});
  }

  @Test
  public void shouldNullTheCorrectKeyCopyForCube() {
    /// CUBE(d1, d2) => the {d2}-only set (ordinal 2) rolls up d1, which a wrong member/column mapping would get
    /// backwards.
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(INPUT_SCHEMA, new Object[]{"a", "x", 5}),
        SuccessMseBlock.INSTANCE);
    RepeatOperator operator = new RepeatOperator(OperatorTestUtil.getTracingContext(), _input, new int[]{0, 1},
        List.of(List.of(0, 1), List.of(0), List.of(1), List.of()), RESULT_SCHEMA);

    List<Object[]> rows = collectRows(operator);
    assertEquals(rows.size(), 4);
    /// {d2} (ordinal 2): the d1 key copy NULLed, the d2 key copy kept.
    assertEquals(rows.get(2), new Object[]{"a", "x", 5, null, "x", 2});
  }

  @Test
  public void shouldHandleNonContiguousSetAndMultipleRows() {
    /// Three union columns, a single explicit set {c0, c2} (member indexes 0 and 2); only the c1 key copy is NULLed.
    DataSchema inputSchema = new DataSchema(new String[]{"c0", "c1", "c2", "cnt"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.INT});
    DataSchema resultSchema = new DataSchema(
        new String[]{"c0", "c1", "c2", "cnt", "$groupingSetKey0", "$groupingSetKey1", "$groupingSetKey2",
            "$groupingId"},
        new ColumnDataType[]{
            ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.INT,
            ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.INT
        });
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inputSchema,
        new Object[]{"a", "m", "x", 1}, new Object[]{"b", "n", "y", 2}), SuccessMseBlock.INSTANCE);
    RepeatOperator operator = new RepeatOperator(OperatorTestUtil.getTracingContext(), _input, new int[]{0, 1, 2},
        List.of(List.of(0, 2)), resultSchema);

    List<Object[]> rows = collectRows(operator);
    assertEquals(rows.size(), 2); /// two input rows x one set
    assertEquals(rows.get(0), new Object[]{"a", "m", "x", 1, "a", null, "x", 0});
    assertEquals(rows.get(1), new Object[]{"b", "n", "y", 2, "b", null, "y", 0});
  }

  @Test
  public void shouldExpandAcrossMultipleInputBlocks() {
    /// The operator streams one set per output block and only pulls the next input block once every set of the
    /// current block has been emitted. Two input blocks x two sets => four output blocks in (block, set) order,
    /// exercising the _currentRows/_currentSet reset-and-repull state machine.
    when(_input.nextBlock()).thenReturn(
        OperatorTestUtil.block(INPUT_SCHEMA, new Object[]{"a", "x", 1}),
        OperatorTestUtil.block(INPUT_SCHEMA, new Object[]{"b", "y", 2}),
        SuccessMseBlock.INSTANCE);
    RepeatOperator operator = new RepeatOperator(OperatorTestUtil.getTracingContext(), _input, new int[]{0, 1},
        List.of(List.of(0, 1), List.of()), RESULT_SCHEMA);

    /// Block 0, set 0: first input row, both keys kept.
    assertEquals(((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows().get(0),
        new Object[]{"a", "x", 1, "a", "x", 0});
    /// Block 0, set 1: first input row, grand total (keys NULLed).
    assertEquals(((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows().get(0),
        new Object[]{"a", "x", 1, null, null, 1});
    /// Block 1, set 0: SECOND input block pulled only now.
    assertEquals(((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows().get(0),
        new Object[]{"b", "y", 2, "b", "y", 0});
    /// Block 1, set 1.
    assertEquals(((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows().get(0),
        new Object[]{"b", "y", 2, null, null, 1});
    assertTrue(operator.nextBlock().isEos());
  }

  @Test
  public void shouldEmitOneBlockPerGroupingSet() {
    /// The expansion is streamed one set per output block, bounding the transient materialization by the input
    /// block size: 2 input rows x 2 sets arrive as two 2-row blocks (set-major), not one 4-row block.
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(INPUT_SCHEMA,
        new Object[]{"a", "x", 1}, new Object[]{"b", "y", 2}), SuccessMseBlock.INSTANCE);
    RepeatOperator operator = new RepeatOperator(OperatorTestUtil.getTracingContext(), _input, new int[]{0, 1},
        List.of(List.of(0, 1), List.of()), RESULT_SCHEMA);

    List<Object[]> block0 = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(block0.size(), 2);
    assertEquals(block0.get(0), new Object[]{"a", "x", 1, "a", "x", 0});
    assertEquals(block0.get(1), new Object[]{"b", "y", 2, "b", "y", 0});
    List<Object[]> block1 = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(block1.size(), 2);
    assertEquals(block1.get(0), new Object[]{"a", "x", 1, null, null, 1});
    assertEquals(block1.get(1), new Object[]{"b", "y", 2, null, null, 1});
    assertTrue(operator.nextBlock().isEos());
  }

  @Test
  public void shouldPropagateUpstreamErrorBlock() {
    when(_input.nextBlock()).thenReturn(ErrorMseBlock.fromException(new Exception("boom!")));
    RepeatOperator operator = new RepeatOperator(OperatorTestUtil.getTracingContext(), _input, new int[]{0, 1},
        List.of(List.of(0, 1), List.of()), RESULT_SCHEMA);

    MseBlock block = operator.nextBlock();
    assertTrue(block.isError(), "upstream error must pass through unexpanded");
    assertTrue(((ErrorMseBlock) block).getErrorMessages().values().toString().contains("boom!"));
  }

  @Test
  public void shouldPropagateUpstreamEos() {
    when(_input.nextBlock()).thenReturn(SuccessMseBlock.INSTANCE);
    RepeatOperator operator = new RepeatOperator(OperatorTestUtil.getTracingContext(), _input, new int[]{0, 1},
        List.of(List.of(0, 1), List.of()), RESULT_SCHEMA);

    assertTrue(operator.nextBlock().isEos(), "EOS must pass through with no expansion");
  }

  @Test
  public void shouldExpandEmptyDataBlock() {
    /// An empty data block (e.g. a filtered-out segment) expands to empty per-set blocks, not an error.
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(INPUT_SCHEMA), SuccessMseBlock.INSTANCE);
    RepeatOperator operator = new RepeatOperator(OperatorTestUtil.getTracingContext(), _input, new int[]{0, 1},
        List.of(List.of(0, 1), List.of()), RESULT_SCHEMA);

    assertEquals(collectRows(operator).size(), 0);
  }

  @Test
  public void shouldHandleMoreThanThirtyTwoUnionColumns() {
    /// The retired bitmask encoding computed (1 << numUnionColumns) - 1, which overflows past 31 columns; the
    /// ordinal encoding has no such limit. 34 union columns, ROLLUP-style sets (all columns) and (): the
    /// grand-total set must NULL all 34 key copies and both ordinals must be emitted.
    int numColumns = 34;
    int numResultColumns = numColumns + 1 + numColumns + 1;
    String[] columnNames = new String[numResultColumns];
    ColumnDataType[] resultTypes = new ColumnDataType[numResultColumns];
    ColumnDataType[] inputTypes = new ColumnDataType[numColumns + 1];
    Object[] inputRow = new Object[numColumns + 1];
    int[] unionGroupKeyIds = new int[numColumns];
    List<Integer> allColumns = new ArrayList<>(numColumns);
    for (int i = 0; i < numColumns; i++) {
      columnNames[i] = "c" + i;
      inputTypes[i] = ColumnDataType.STRING;
      resultTypes[i] = ColumnDataType.STRING;
      inputRow[i] = "v" + i;
      unionGroupKeyIds[i] = i;
      allColumns.add(i);
      columnNames[numColumns + 1 + i] = "$groupingSetKey" + i;
      resultTypes[numColumns + 1 + i] = ColumnDataType.STRING;
    }
    columnNames[numColumns] = "cnt";
    inputTypes[numColumns] = ColumnDataType.INT;
    resultTypes[numColumns] = ColumnDataType.INT;
    inputRow[numColumns] = 7;
    columnNames[numResultColumns - 1] = "$groupingId";
    resultTypes[numResultColumns - 1] = ColumnDataType.INT;
    String[] inputNames = new String[numColumns + 1];
    System.arraycopy(columnNames, 0, inputNames, 0, numColumns + 1);
    DataSchema inputSchema = new DataSchema(inputNames, inputTypes);
    DataSchema resultSchema = new DataSchema(columnNames, resultTypes);

    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inputSchema, inputRow), SuccessMseBlock.INSTANCE);
    RepeatOperator operator = new RepeatOperator(OperatorTestUtil.getTracingContext(), _input, unionGroupKeyIds,
        List.of(allColumns, List.of()), resultSchema);

    List<Object[]> rows = collectRows(operator);
    assertEquals(rows.size(), 2);
    /// Set (all columns), ordinal 0: every key copy carries the value.
    for (int i = 0; i < numColumns; i++) {
      assertEquals(rows.get(0)[numColumns + 1 + i], "v" + i);
    }
    assertEquals(rows.get(0)[numResultColumns - 1], 0);
    /// Grand-total set (), ordinal 1: every key copy NULLed — including the ones past the 32-bit boundary — while
    /// the original columns stay untouched.
    for (int i = 0; i < numColumns; i++) {
      assertNull(rows.get(1)[numColumns + 1 + i]);
      assertEquals(rows.get(1)[i], "v" + i);
    }
    assertEquals(rows.get(1)[numColumns], 7);
    assertEquals(rows.get(1)[numResultColumns - 1], 1);
  }
}
