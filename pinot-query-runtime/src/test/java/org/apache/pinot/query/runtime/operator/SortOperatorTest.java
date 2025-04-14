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
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.mockito.Mock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.INT;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.STRING;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class SortOperatorTest {
  private AutoCloseable _mocks;
  @Mock
  private MultiStageOperator _input;
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
  public void shouldHandleUpstreamErrorBlock() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    when(_input.nextBlock()).thenReturn(ErrorMseBlock.fromException(new Exception("foo!")));
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations);

    // When:
    MseBlock block = operator.nextBlock();

    // Then:
    assertTrue(block.isError(), "expected error block to propagate");
  }

  @Test
  public void shouldCreateEmptyBlockOnUpstreamEOS() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    when(_input.nextBlock()).thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations);

    // When:
    MseBlock block = operator.nextBlock();

    // Then:
    assertTrue(block.isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldConsumeAndSortInputOneBlockWithTwoRows() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{2}, new Object[]{1}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{1});
    assertEquals(resultRows.get(1), new Object[]{2});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldConsumeAndSkipSortInputOneBlockWithTwoRowsInputSorted() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    _input = mock(SortedMailboxReceiveOperator.class);
    // Purposefully setting input as unsorted order for validation but 'isInputSorted' should only be true if actually
    // sorted
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{2}, new Object[]{1}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{2});
    assertEquals(resultRows.get(1), new Object[]{1});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldConsumeAndSortOnNonZeroIdxCollation() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"ignored", "sort"}, new DataSchema.ColumnDataType[]{INT, INT});
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{1, 2}, new Object[]{2, 1}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(1, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{2, 1});
    assertEquals(resultRows.get(1), new Object[]{1, 2});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldConsumeAndSortInputOneBlockWithTwoRowsNonNumeric() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{STRING});
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{"b"}, new Object[]{"a"}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{"a"});
    assertEquals(resultRows.get(1), new Object[]{"b"});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldConsumeAndSortDescending() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{2}, new Object[]{1}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.DESCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{2});
    assertEquals(resultRows.get(1), new Object[]{1});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldOffsetSortInputOneBlockWithThreeRows() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{2}, new Object[]{1}, new Object[]{3}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations, 10, 1);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{2});
    assertEquals(resultRows.get(1), new Object[]{3});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldOffsetSortInputOneBlockWithThreeRowsInputSorted() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    _input = mock(SortedMailboxReceiveOperator.class);
    // Set input rows as sorted since input is expected to be sorted
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{1}, new Object[]{2}, new Object[]{3}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations, 10, 1);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{2});
    assertEquals(resultRows.get(1), new Object[]{3});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldOffsetLimitSortInputOneBlockWithThreeRows() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{2}, new Object[]{1}, new Object[]{3}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations, 1, 1);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 1);
    assertEquals(resultRows.get(0), new Object[]{2});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldOffsetLimitSortInputOneBlockWithThreeRowsInputSorted() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    _input = mock(SortedMailboxReceiveOperator.class);
    // Set input rows as sorted since input is expected to be sorted
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{1}, new Object[]{2}, new Object[]{3}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations, 1, 1);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 1);
    assertEquals(resultRows.get(0), new Object[]{2});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldRespectDefaultLimit() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{2}, new Object[]{1}, new Object[]{3}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = new SortOperator(OperatorTestUtil.getTracingContext(), _input,
        new SortNode(-1, schema, PlanNode.NodeHint.EMPTY, List.of(), collations, -1, 0), 10, 1);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 1, "expected 1 element even though fetch is 2 because of max limit");
    assertEquals(resultRows.get(0), new Object[]{1});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldFetchAllWithNegativeFetch() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{2}, new Object[]{1}, new Object[]{3}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations, -1, 0);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{1});
    assertEquals(resultRows.get(1), new Object[]{2});
    assertEquals(resultRows.get(2), new Object[]{3});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldConsumeAndSortTwoInputBlocksWithOneRowEach() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{2})).thenReturn(block(schema, new Object[]{1}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{1});
    assertEquals(resultRows.get(1), new Object[]{2});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldConsumeAndSortTwoInputBlocksWithOneRowEachInputSorted() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    _input = mock(SortedMailboxReceiveOperator.class);
    // Set input rows as sorted since input is expected to be sorted
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{1})).thenReturn(block(schema, new Object[]{2}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{1});
    assertEquals(resultRows.get(1), new Object[]{2});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldBreakTiesUsingSecondCollationKey() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"first", "second"}, new DataSchema.ColumnDataType[]{INT, INT});
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{1, 2}, new Object[]{1, 1}, new Object[]{1, 3}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST),
        new RelFieldCollation(1, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{1, 1});
    assertEquals(resultRows.get(1), new Object[]{1, 2});
    assertEquals(resultRows.get(2), new Object[]{1, 3});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldBreakTiesUsingSecondCollationKeyWithDifferentDirection() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"first", "second"}, new DataSchema.ColumnDataType[]{INT, INT});
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{1, 2}, new Object[]{1, 1}, new Object[]{1, 3}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST),
        new RelFieldCollation(1, Direction.DESCENDING, NullDirection.FIRST));
    SortOperator operator = getOperator(schema, collations);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{1, 3});
    assertEquals(resultRows.get(1), new Object[]{1, 2});
    assertEquals(resultRows.get(2), new Object[]{1, 1});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldHaveNullAtLast() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{2}, new Object[]{1}, new Object[]{null}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{1});
    assertEquals(resultRows.get(1), new Object[]{2});
    assertEquals(resultRows.get(2), new Object[]{null});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldHaveNullAtFirst() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{2}, new Object[]{1}, new Object[]{null}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.FIRST));
    SortOperator operator = getOperator(schema, collations);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{null});
    assertEquals(resultRows.get(1), new Object[]{1});
    assertEquals(resultRows.get(2), new Object[]{2});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldHaveNullAtLastWhenUnspecified() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{2}, new Object[]{1}, new Object[]{null}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations =
        List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.UNSPECIFIED));
    SortOperator operator = getOperator(schema, collations);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{1});
    assertEquals(resultRows.get(1), new Object[]{2});
    assertEquals(resultRows.get(2), new Object[]{null});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  @Test
  public void shouldHandleMultipleCollationKeysWithNulls() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"first", "second"}, new DataSchema.ColumnDataType[]{INT, INT});
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{1, 1}, new Object[]{1, null}, new Object[]{null, 1}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.FIRST),
        new RelFieldCollation(1, Direction.DESCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{null, 1});
    assertEquals(resultRows.get(1), new Object[]{1, 1});
    assertEquals(resultRows.get(2), new Object[]{1, null});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  private SortOperator getOperator(DataSchema schema, List<RelFieldCollation> collations, int fetch, int offset) {
    return new SortOperator(OperatorTestUtil.getTracingContext(), _input,
        new SortNode(-1, schema, PlanNode.NodeHint.EMPTY, List.of(), collations, fetch, offset));
  }

  private SortOperator getOperator(DataSchema schema, List<RelFieldCollation> collations) {
    return getOperator(schema, collations, 10, 0);
  }

  private static RowHeapDataBlock block(DataSchema schema, Object[]... rows) {
    return new RowHeapDataBlock(List.of(rows), schema);
  }
}
