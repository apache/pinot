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
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.LONG;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.STRING;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
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
  public void shouldApplyOffsetToSortedResult() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
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
  public void shouldApplyOffsetAndLimitToSortedResult() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
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
    SortOperator operator = SortOperator.create(OperatorTestUtil.getTracingContext(), _input,
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

  @Test
  public void shouldPreservePrecision() {
    // Given:
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{LONG});
    // Insert 3 consecutive large numbers that are represented by the same double value if converted to double due to
    // precision loss.
    long largeValue = 1L << 60;
    //noinspection ConstantValue
    assert (double) largeValue == (double) (largeValue + 1) && (double) largeValue == (double) (largeValue + 2);
    when(_input.nextBlock())
        .thenReturn(block(schema, new Object[]{largeValue + 2}, new Object[]{largeValue}, new Object[]{largeValue + 1}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0));
    SortOperator operator = getOperator(schema, collations);

    // When:
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    // Then:
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{largeValue});
    assertEquals(resultRows.get(1), new Object[]{largeValue + 1});
    assertEquals(resultRows.get(2), new Object[]{largeValue + 2});
    assertTrue(operator.nextBlock().isSuccess(), "expected EOS block to propagate");
  }

  // --------------------------------------------------------------------------------------------------------------
  // Implementation selection. Which of the three SortOperator implementations runs decides the peak memory, so each
  // branch of the factory is pinned here rather than inferred from the output rows, which are identical either way.
  // --------------------------------------------------------------------------------------------------------------

  @Test
  public void shouldUseTopNWhenFetchBoundsTheResult() {
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));

    SortOperator operator = getOperator(schema, collations, 10, 0);

    assertTrue(operator instanceof TopNSortOperator, "a fetch bounds the result, so a bounded heap is enough");
    assertEquals(operator.toExplainString(), "SORT_TOP_N");
  }

  @Test
  public void shouldUseFullSortWhenNothingBoundsTheResult() {
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));

    // No fetch, and the production broker response limit is Integer.MAX_VALUE, so no bound exists.
    SortOperator operator = getOperator(schema, collations, -1, 0);

    assertTrue(operator instanceof FullSortOperator, "without a bound every row has to be buffered and sorted");
    assertEquals(operator.toExplainString(), "SORT_FULL");
  }

  /// A SortedMailboxReceiveOperator input gets no special treatment: it is sorted again. Skipping the sort would save
  /// one pass, but only for plans built by a broker that predates PinotSortExchangeNodeInsertRule dropping
  /// sort-on-receiver, and the type test that used to enable it never checked that the input was ordered on *this*
  /// collation. Re-sorting an already sorted stream is correct, so the cheaper invariant wins: SORT_LIMIT is chosen
  /// only when there is no collation to honor.
  @Test
  public void shouldSortAgainWhenInputIsASortedMailboxReceive() {
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    _input = mock(SortedMailboxReceiveOperator.class);
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{2}, new Object[]{1}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));

    SortOperator operator = getOperator(schema, collations, 10, 0);

    assertTrue(operator instanceof TopNSortOperator, "a collation is honored regardless of the input operator type");
    assertBlockRows(operator.nextBlock(), new Object[]{1}, new Object[]{2});
  }

  @Test
  public void shouldUseSortedInputWhenThereIsNoCollation() {
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});

    SortOperator operator = getOperator(schema, List.of(), 10, 0);

    assertTrue(operator instanceof LimitSortOperator, "a plain LIMIT needs no ordering at all");
  }

  // --------------------------------------------------------------------------------------------------------------
  // Streaming. The result is handed back in bounded blocks rather than one block holding everything, so that a large
  // result is neither serialized nor materialized in one piece.
  // --------------------------------------------------------------------------------------------------------------

  /// A sorted input must not be buffered at all: the consumer sees rows from the first input block before the input
  /// has reached EOS. This is what makes SORT_LIMIT the only implementation that does not break the pipeline.
  @Test
  public void shouldStreamWithoutWaitingForEos() {
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{1}, new Object[]{2}))
        .thenReturn(block(schema, new Object[]{3})).thenReturn(SuccessMseBlock.INSTANCE);
    SortOperator operator = getOperator(schema, List.of(), 10, 0);

    assertEquals(((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows().size(), 2);
    assertEquals(((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows().size(), 1);
    assertTrue(operator.nextBlock().isSuccess());
  }

  /// The offset can be consumed part way through a block and can span several blocks, and the fetch can be reached
  /// part way through a block. Both are applied without buffering the rows that are kept.
  @Test
  public void shouldApplyOffsetAndFetchAcrossStreamedBlocks() {
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{1}, new Object[]{2}))
        .thenReturn(block(schema, new Object[]{3}, new Object[]{4}, new Object[]{5}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    // OFFSET 3 swallows the whole first block and one row of the second; LIMIT 1 then stops after a single row.
    SortOperator operator = getOperator(schema, List.of(), 1, 3);

    List<Object[]> rows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();

    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0), new Object[]{4});
    assertTrue(operator.nextBlock().isSuccess(), "input must be early terminated once the fetch is satisfied");
  }

  /// A buffering implementation still hands its result back in slices. Without this a `LIMIT 1000000` produces one
  /// block holding every row, which is expensive to serialize and forces the consumer to materialize all of it.
  @Test
  public void shouldSplitTopNResultIntoBoundedBlocks() {
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    Object[][] rows = new Object[5][];
    for (int i = 0; i < 5; i++) {
      rows[i] = new Object[]{5 - i};
    }
    when(_input.nextBlock()).thenReturn(block(schema, rows)).thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = SortOperator.create(OperatorTestUtil.getTracingContext(), _input,
        new SortNode(-1, schema, PlanNode.NodeHint.EMPTY, List.of(), collations, 10, 0), 10, Integer.MAX_VALUE, 2);

    // Sorted output is 1,2,3,4,5, handed back as 2 + 2 + 1 rows.
    assertBlockRows(operator.nextBlock(), new Object[]{1}, new Object[]{2});
    assertBlockRows(operator.nextBlock(), new Object[]{3}, new Object[]{4});
    assertBlockRows(operator.nextBlock(), new Object[]{5});
    assertTrue(operator.nextBlock().isSuccess());
  }

  @Test
  public void shouldSplitFullSortResultIntoBoundedBlocks() {
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{3}, new Object[]{1}, new Object[]{2}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = SortOperator.create(OperatorTestUtil.getTracingContext(), _input,
        new SortNode(-1, schema, PlanNode.NodeHint.EMPTY, List.of(), collations, -1, 0), 10, Integer.MAX_VALUE, 2);

    assertTrue(operator instanceof FullSortOperator);
    assertBlockRows(operator.nextBlock(), new Object[]{1}, new Object[]{2});
    assertBlockRows(operator.nextBlock(), new Object[]{3});
    assertTrue(operator.nextBlock().isSuccess());
  }

  /// After a terminal block every further call must return that same block, whichever implementation ran.
  @Test
  public void shouldKeepReturningTheSameEosBlock() {
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    when(_input.nextBlock()).thenReturn(block(schema, new Object[]{1})).thenReturn(SuccessMseBlock.INSTANCE);
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));
    SortOperator operator = getOperator(schema, collations);

    assertTrue(operator.nextBlock().isData());
    MseBlock eos = operator.nextBlock();
    assertTrue(eos.isSuccess());
    assertSame(operator.nextBlock(), eos);
  }

  /// `requireSort` distinguishes the implementations that pay for a sort from the one that does not, so it must be
  /// false exactly when SORT_LIMIT ran.
  @Test
  public void shouldReportRequireSortPerImplementation() {
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST));

    assertTrue(getOperator(schema, collations, 10, 0).copyStatMaps()
        .getBoolean(SortOperator.StatKey.REQUIRE_SORT));
    assertTrue(getOperator(schema, collations, -1, 0).copyStatMaps()
        .getBoolean(SortOperator.StatKey.REQUIRE_SORT));
    assertFalse(getOperator(schema, List.of(), 10, 0).copyStatMaps()
        .getBoolean(SortOperator.StatKey.REQUIRE_SORT));
  }

  private SortOperator getOperator(DataSchema schema, List<RelFieldCollation> collations, int fetch, int offset) {
    return SortOperator.create(OperatorTestUtil.getTracingContext(), _input,
        new SortNode(-1, schema, PlanNode.NodeHint.EMPTY, List.of(), collations, fetch, offset));
  }

  private SortOperator getOperator(DataSchema schema, List<RelFieldCollation> collations) {
    return getOperator(schema, collations, 10, 0);
  }

  private static void assertBlockRows(MseBlock block, Object[]... expected) {
    List<Object[]> rows = ((MseBlock.Data) block).asRowHeap().getRows();
    assertEquals(rows.size(), expected.length);
    for (int i = 0; i < expected.length; i++) {
      assertEquals(rows.get(i), expected[i]);
    }
  }

  private static RowHeapDataBlock block(DataSchema schema, Object[]... rows) {
    return new RowHeapDataBlock(List.of(rows), schema);
  }
}
