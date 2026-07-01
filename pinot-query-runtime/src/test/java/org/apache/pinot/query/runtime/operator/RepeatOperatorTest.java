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
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.mockito.Mock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


/// Unit coverage for [RepeatOperator]'s per-set row expansion: it must NULL the rolled-up union columns and append the
/// `$groupingId` discriminator (the rolled-up complement of each grouping set's participation mask).
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

  /// Input [d1, d2, cnt]; union group keys are d1 (idx 0) and d2 (idx 1); result appends $groupingId.
  private static final DataSchema INPUT_SCHEMA = new DataSchema(new String[]{"d1", "d2", "cnt"},
      new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.INT});
  private static final DataSchema RESULT_SCHEMA = new DataSchema(new String[]{"d1", "d2", "cnt", "$groupingId"},
      new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.INT});

  @Test
  public void shouldExpandRollupAndNullRolledUpColumns() {
    /// ROLLUP(d1, d2) => sets {d1,d2}, {d1}, {} — member union-column indexes (0,1), (0), (); ordinal = position.
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(INPUT_SCHEMA, new Object[]{"a", "x", 5}));
    RepeatOperator operator = new RepeatOperator(OperatorTestUtil.getTracingContext(), _input, new int[]{0, 1},
        List.of(List.of(0, 1), List.of(0), List.of()), RESULT_SCHEMA);

    List<Object[]> rows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(rows.size(), 3); /// one input row x three sets

    /// {d1,d2} (ordinal 0): nothing rolled up, both keys kept.
    assertEquals(rows.get(0), new Object[]{"a", "x", 5, 0});
    /// {d1} (ordinal 1): d2 rolled up -> NULLed.
    assertEquals(rows.get(1)[0], "a");
    assertNull(rows.get(1)[1]);
    assertEquals(rows.get(1)[3], 1);
    /// {} (ordinal 2): both rolled up -> both keys NULLed.
    assertNull(rows.get(2)[0]);
    assertNull(rows.get(2)[1]);
    assertEquals(rows.get(2)[3], 2);
    /// cnt (non-key column) is never touched.
    assertEquals(rows.get(2)[2], 5);
  }

  @Test
  public void shouldNullTheCorrectKeyForCube() {
    /// CUBE(d1, d2) => the {d2}-only set (ordinal 2) rolls up d1, which a wrong member/column mapping would get
    /// backwards.
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(INPUT_SCHEMA, new Object[]{"a", "x", 5}));
    RepeatOperator operator = new RepeatOperator(OperatorTestUtil.getTracingContext(), _input, new int[]{0, 1},
        List.of(List.of(0, 1), List.of(0), List.of(1), List.of()), RESULT_SCHEMA);

    List<Object[]> rows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(rows.size(), 4);
    /// {d2} (ordinal 2): d1 NULLed, d2 kept.
    Object[] d2Only = rows.get(2);
    assertNull(d2Only[0]);
    assertEquals(d2Only[1], "x");
    assertEquals(d2Only[3], 2);
  }

  @Test
  public void shouldHandleNonContiguousSetAndMultipleRows() {
    /// Three union columns, a single explicit set {c0, c2} (member indexes 0 and 2); only c1 must be NULLed.
    DataSchema inputSchema = new DataSchema(new String[]{"c0", "c1", "c2", "cnt"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.INT});
    DataSchema resultSchema = new DataSchema(new String[]{"c0", "c1", "c2", "cnt", "$groupingId"},
        new ColumnDataType[]{
            ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.INT
        });
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inputSchema,
        new Object[]{"a", "m", "x", 1}, new Object[]{"b", "n", "y", 2}));
    RepeatOperator operator = new RepeatOperator(OperatorTestUtil.getTracingContext(), _input, new int[]{0, 1, 2},
        List.of(List.of(0, 2)), resultSchema);

    List<Object[]> rows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(rows.size(), 2); /// two input rows x one set
    /// The single set has ordinal 0; only c1 (idx 1) NULLed, c0/c2 kept.
    assertEquals(rows.get(0), new Object[]{"a", null, "x", 1, 0});
    assertEquals(rows.get(1), new Object[]{"b", null, "y", 2, 0});
  }

  @Test
  public void shouldHandleMoreThanThirtyTwoUnionColumns() {
    /// The retired bitmask encoding computed (1 << numUnionColumns) - 1, which overflows past 31 columns; the
    /// ordinal encoding has no such limit. 34 union columns, ROLLUP-style sets (all columns) and (): the
    /// grand-total set must NULL all 34 keys and both ordinals must be emitted.
    int numColumns = 34;
    String[] columnNames = new String[numColumns + 2];
    ColumnDataType[] inputTypes = new ColumnDataType[numColumns + 1];
    Object[] inputRow = new Object[numColumns + 1];
    int[] unionGroupKeyIds = new int[numColumns];
    List<Integer> allColumns = new ArrayList<>(numColumns);
    for (int i = 0; i < numColumns; i++) {
      columnNames[i] = "c" + i;
      inputTypes[i] = ColumnDataType.STRING;
      inputRow[i] = "v" + i;
      unionGroupKeyIds[i] = i;
      allColumns.add(i);
    }
    columnNames[numColumns] = "cnt";
    inputTypes[numColumns] = ColumnDataType.INT;
    inputRow[numColumns] = 7;
    columnNames[numColumns + 1] = "$groupingId";
    DataSchema inputSchema = new DataSchema(Arrays.copyOf(columnNames, numColumns + 1), inputTypes);
    ColumnDataType[] resultTypes = Arrays.copyOf(inputTypes, numColumns + 2);
    resultTypes[numColumns + 1] = ColumnDataType.INT;
    DataSchema resultSchema = new DataSchema(columnNames, resultTypes);

    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(inputSchema, inputRow));
    RepeatOperator operator = new RepeatOperator(OperatorTestUtil.getTracingContext(), _input, unionGroupKeyIds,
        List.of(allColumns, List.of()), resultSchema);

    List<Object[]> rows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(rows.size(), 2);
    /// Set (all columns), ordinal 0: every key kept.
    for (int i = 0; i < numColumns; i++) {
      assertEquals(rows.get(0)[i], "v" + i);
    }
    assertEquals(rows.get(0)[numColumns + 1], 0);
    /// Grand-total set (), ordinal 1: every key NULLed, including the ones past the 32-bit boundary.
    for (int i = 0; i < numColumns; i++) {
      assertNull(rows.get(1)[i]);
    }
    assertEquals(rows.get(1)[numColumns], 7);
    assertEquals(rows.get(1)[numColumns + 1], 1);
  }
}
