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

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.INT;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.STRING;


public class SortOperatorTest {

  private AutoCloseable _mocks;

  @Mock
  private MultiStageOperator _input;

  @Mock
  private VirtualServerAddress _serverAddress;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
    Mockito.when(_serverAddress.toString()).thenReturn(new VirtualServerAddress("mock", 80, 0).toString());
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void shouldHandleUpstreamErrorBlock() {
    // Given:
    List<RexExpression> collation = collation(0);
    List<Direction> directions = ImmutableList.of(Direction.ASCENDING);
    List<NullDirection> nullDirections = ImmutableList.of(NullDirection.LAST);
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    SortOperator op =
        new SortOperator(OperatorTestUtil.getDefaultContext(), _input, collation, directions, nullDirections, 10, 0,
            schema, false);

    Mockito.when(_input.nextBlock())
        .thenReturn(TransferableBlockUtils.getErrorTransferableBlock(new Exception("foo!")));

    // When:
    TransferableBlock block = op.nextBlock();

    // Then:
    Assert.assertTrue(block.isErrorBlock(), "expected error block to propagate");
  }

  @Test
  public void shouldCreateEmptyBlockOnUpstreamEOS() {
    // Given:
    List<RexExpression> collation = collation(0);
    List<Direction> directions = ImmutableList.of(Direction.ASCENDING);
    List<NullDirection> nullDirections = ImmutableList.of(NullDirection.LAST);
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    SortOperator op =
        new SortOperator(OperatorTestUtil.getDefaultContext(), _input, collation, directions, nullDirections, 10, 0,
            schema, false);

    Mockito.when(_input.nextBlock()).thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    // When:
    TransferableBlock block = op.nextBlock();

    // Then:
    Assert.assertTrue(block.isEndOfStreamBlock(), "expected EOS block to propagate");
  }

  @Test
  public void shouldConsumeAndSortInputOneBlockWithTwoRows() {
    // Given:
    List<RexExpression> collation = collation(0);
    List<Direction> directions = ImmutableList.of(Direction.ASCENDING);
    List<NullDirection> nullDirections = ImmutableList.of(NullDirection.LAST);
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    SortOperator op =
        new SortOperator(OperatorTestUtil.getDefaultContext(), _input, collation, directions, nullDirections, 10, 0,
            schema, false);

    Mockito.when(_input.nextBlock()).thenReturn(block(schema, new Object[]{2}, new Object[]{1}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    // When:
    TransferableBlock block = op.nextBlock(); // construct
    TransferableBlock block2 = op.nextBlock(); // eos

    // Then:
    Assert.assertEquals(block.getNumRows(), 2);
    Assert.assertEquals(block.getContainer().get(0), new Object[]{1});
    Assert.assertEquals(block.getContainer().get(1), new Object[]{2});
    Assert.assertTrue(block2.isEndOfStreamBlock(), "expected EOS block to propagate");
  }

  @Test
  public void shouldConsumeAndSkipSortInputOneBlockWithTwoRowsInputSorted() {
    // Given:
    List<RexExpression> collation = collation(0);
    List<Direction> directions = ImmutableList.of(Direction.ASCENDING);
    List<NullDirection> nullDirections = ImmutableList.of(NullDirection.LAST);
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    SortOperator op =
        new SortOperator(OperatorTestUtil.getDefaultContext(), _input, collation, directions, nullDirections, 10, 0,
            schema, true);

    // Purposefully setting input as unsorted order for validation but 'isInputSorted' should only be true if actually
    // sorted
    Mockito.when(_input.nextBlock()).thenReturn(block(schema, new Object[]{2}, new Object[]{1}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    // When:
    TransferableBlock block = op.nextBlock(); // construct
    TransferableBlock block2 = op.nextBlock(); // eos

    // Then:
    Assert.assertEquals(block.getNumRows(), 2);
    Assert.assertEquals(block.getContainer().get(0), new Object[]{2});
    Assert.assertEquals(block.getContainer().get(1), new Object[]{1});
    Assert.assertTrue(block2.isEndOfStreamBlock(), "expected EOS block to propagate");
  }

  @Test
  public void shouldConsumeAndSortOnNonZeroIdxCollation() {
    // Given:
    List<RexExpression> collation = collation(1);
    List<Direction> directions = ImmutableList.of(Direction.ASCENDING);
    List<NullDirection> nullDirections = ImmutableList.of(NullDirection.LAST);
    DataSchema schema = new DataSchema(new String[]{"ignored", "sort"}, new DataSchema.ColumnDataType[]{INT, INT});
    SortOperator op =
        new SortOperator(OperatorTestUtil.getDefaultContext(), _input, collation, directions, nullDirections, 10, 0,
            schema, false);

    Mockito.when(_input.nextBlock()).thenReturn(block(schema, new Object[]{1, 2}, new Object[]{2, 1}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    // When:
    TransferableBlock block = op.nextBlock(); // construct
    TransferableBlock block2 = op.nextBlock(); // eos

    // Then:
    Assert.assertEquals(block.getNumRows(), 2);
    Assert.assertEquals(block.getContainer().get(0), new Object[]{2, 1});
    Assert.assertEquals(block.getContainer().get(1), new Object[]{1, 2});
    Assert.assertTrue(block2.isEndOfStreamBlock(), "expected EOS block to propagate");
  }

  @Test
  public void shouldConsumeAndSortInputOneBlockWithTwoRowsNonNumeric() {
    // Given:
    List<RexExpression> collation = collation(0);
    List<Direction> directions = ImmutableList.of(Direction.ASCENDING);
    List<NullDirection> nullDirections = ImmutableList.of(NullDirection.LAST);
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{STRING});
    SortOperator op =
        new SortOperator(OperatorTestUtil.getDefaultContext(), _input, collation, directions, nullDirections, 10, 0,
            schema, false);

    Mockito.when(_input.nextBlock()).thenReturn(block(schema, new Object[]{"b"}, new Object[]{"a"}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    // When:
    TransferableBlock block = op.nextBlock(); // construct
    TransferableBlock block2 = op.nextBlock(); // eos

    // Then:
    Assert.assertEquals(block.getNumRows(), 2);
    Assert.assertEquals(block.getContainer().get(0), new Object[]{"a"});
    Assert.assertEquals(block.getContainer().get(1), new Object[]{"b"});
    Assert.assertTrue(block2.isEndOfStreamBlock(), "expected EOS block to propagate");
  }

  @Test
  public void shouldConsumeAndSortDescending() {
    // Given:
    List<RexExpression> collation = collation(0);
    List<Direction> directions = ImmutableList.of(Direction.DESCENDING);
    List<NullDirection> nullDirections = ImmutableList.of(NullDirection.LAST);
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    SortOperator op =
        new SortOperator(OperatorTestUtil.getDefaultContext(), _input, collation, directions, nullDirections, 10, 0,
            schema, false);

    Mockito.when(_input.nextBlock()).thenReturn(block(schema, new Object[]{2}, new Object[]{1}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    // When:
    TransferableBlock block = op.nextBlock(); // construct
    TransferableBlock block2 = op.nextBlock(); // eos

    // Then:
    Assert.assertEquals(block.getNumRows(), 2);
    Assert.assertEquals(block.getContainer().get(0), new Object[]{2});
    Assert.assertEquals(block.getContainer().get(1), new Object[]{1});
    Assert.assertTrue(block2.isEndOfStreamBlock(), "expected EOS block to propagate");
  }

  @Test
  public void shouldOffsetSortInputOneBlockWithThreeRows() {
    // Given:
    List<RexExpression> collation = collation(0);
    List<Direction> directions = ImmutableList.of(Direction.ASCENDING);
    List<NullDirection> nullDirections = ImmutableList.of(NullDirection.LAST);
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    SortOperator op =
        new SortOperator(OperatorTestUtil.getDefaultContext(), _input, collation, directions, nullDirections, 10, 1,
            schema, false);

    Mockito.when(_input.nextBlock()).thenReturn(block(schema, new Object[]{2}, new Object[]{1}, new Object[]{3}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    // When:
    TransferableBlock block = op.nextBlock(); // construct
    TransferableBlock block2 = op.nextBlock(); // eos

    // Then:
    Assert.assertEquals(block.getNumRows(), 2);
    Assert.assertEquals(block.getContainer().get(0), new Object[]{2});
    Assert.assertEquals(block.getContainer().get(1), new Object[]{3});
    Assert.assertTrue(block2.isEndOfStreamBlock(), "expected EOS block to propagate");
  }

  @Test
  public void shouldOffsetSortInputOneBlockWithThreeRowsInputSorted() {
    // Given:
    List<RexExpression> collation = collation(0);
    List<Direction> directions = ImmutableList.of(Direction.ASCENDING);
    List<NullDirection> nullDirections = ImmutableList.of(NullDirection.LAST);
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    SortOperator op =
        new SortOperator(OperatorTestUtil.getDefaultContext(), _input, collation, directions, nullDirections, 10, 1,
            schema, true);

    // Set input rows as sorted since input is expected to be sorted
    Mockito.when(_input.nextBlock()).thenReturn(block(schema, new Object[]{1}, new Object[]{2}, new Object[]{3}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    // When:
    TransferableBlock block = op.nextBlock(); // construct
    TransferableBlock block2 = op.nextBlock(); // eos

    // Then:
    Assert.assertEquals(block.getNumRows(), 2);
    Assert.assertEquals(block.getContainer().get(0), new Object[]{2});
    Assert.assertEquals(block.getContainer().get(1), new Object[]{3});
    Assert.assertTrue(block2.isEndOfStreamBlock(), "expected EOS block to propagate");
  }

  @Test
  public void shouldOffsetLimitSortInputOneBlockWithThreeRows() {
    // Given:
    List<RexExpression> collation = collation(0);
    List<Direction> directions = ImmutableList.of(Direction.ASCENDING);
    List<NullDirection> nullDirections = ImmutableList.of(NullDirection.LAST);
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    SortOperator op =
        new SortOperator(OperatorTestUtil.getDefaultContext(), _input, collation, directions, nullDirections, 1, 1,
            schema, false);

    Mockito.when(_input.nextBlock()).thenReturn(block(schema, new Object[]{2}, new Object[]{1}, new Object[]{3}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    // When:
    TransferableBlock block = op.nextBlock(); // construct
    TransferableBlock block2 = op.nextBlock(); // eos

    // Then:
    Assert.assertEquals(block.getNumRows(), 1);
    Assert.assertEquals(block.getContainer().get(0), new Object[]{2});
    Assert.assertTrue(block2.isEndOfStreamBlock(), "expected EOS block to propagate");
  }

  @Test
  public void shouldOffsetLimitSortInputOneBlockWithThreeRowsInputSorted() {
    // Given:
    List<RexExpression> collation = collation(0);
    List<Direction> directions = ImmutableList.of(Direction.ASCENDING);
    List<NullDirection> nullDirections = ImmutableList.of(NullDirection.LAST);
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    SortOperator op =
        new SortOperator(OperatorTestUtil.getDefaultContext(), _input, collation, directions, nullDirections, 1, 1,
            schema, true);

    // Set input rows as sorted since input is expected to be sorted
    Mockito.when(_input.nextBlock()).thenReturn(block(schema, new Object[]{1}, new Object[]{2}, new Object[]{3}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    // When:
    TransferableBlock block = op.nextBlock(); // construct
    TransferableBlock block2 = op.nextBlock(); // eos

    // Then:
    Assert.assertEquals(block.getNumRows(), 1);
    Assert.assertEquals(block.getContainer().get(0), new Object[]{2});
    Assert.assertTrue(block2.isEndOfStreamBlock(), "expected EOS block to propagate");
  }

  @Test
  public void shouldRespectDefaultLimit() {
    // Given:
    List<RexExpression> collation = collation(0);
    List<Direction> directions = ImmutableList.of(Direction.ASCENDING);
    List<NullDirection> nullDirections = ImmutableList.of(NullDirection.LAST);
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    SortOperator op =
        new SortOperator(OperatorTestUtil.getDefaultContext(), _input, collation, directions, nullDirections, 0, 0,
            schema, false, 10, 1);

    Mockito.when(_input.nextBlock()).thenReturn(block(schema, new Object[]{2}, new Object[]{1}, new Object[]{3}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    // When:
    TransferableBlock block = op.nextBlock(); // construct
    TransferableBlock block2 = op.nextBlock(); // eos

    // Then:
    Assert.assertEquals(block.getNumRows(), 1, "expected 1 element even though fetch is 2 because of max limit");
    Assert.assertEquals(block.getContainer().get(0), new Object[]{1});
    Assert.assertTrue(block2.isEndOfStreamBlock(), "expected EOS block to propagate");
  }

  @Test
  public void shouldFetchAllWithNegativeFetch() {
    // Given:
    List<RexExpression> collation = collation(0);
    List<Direction> directions = ImmutableList.of(Direction.ASCENDING);
    List<NullDirection> nullDirections = ImmutableList.of(NullDirection.LAST);
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    SortOperator op =
        new SortOperator(OperatorTestUtil.getDefaultContext(), _input, collation, directions, nullDirections, -1, 0,
            schema, false);

    Mockito.when(_input.nextBlock()).thenReturn(block(schema, new Object[]{2}, new Object[]{1}, new Object[]{3}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    // When:
    TransferableBlock block = op.nextBlock(); // construct
    TransferableBlock block2 = op.nextBlock(); // eos

    // Then:
    Assert.assertEquals(block.getNumRows(), 3);
    Assert.assertTrue(block2.isEndOfStreamBlock(), "expected EOS block to propagate");
  }

  @Test
  public void shouldConsumeAndSortTwoInputBlocksWithOneRowEach() {
    // Given:
    List<RexExpression> collation = collation(0);
    List<Direction> directions = ImmutableList.of(Direction.ASCENDING);
    List<NullDirection> nullDirections = ImmutableList.of(NullDirection.LAST);
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    SortOperator op =
        new SortOperator(OperatorTestUtil.getDefaultContext(), _input, collation, directions, nullDirections, 10, 0,
            schema, false);

    Mockito.when(_input.nextBlock()).thenReturn(block(schema, new Object[]{2}))
        .thenReturn(block(schema, new Object[]{1}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    // When:
    TransferableBlock block = op.nextBlock(); // construct
    TransferableBlock block2 = op.nextBlock(); // eos

    // Then:
    Assert.assertEquals(block.getNumRows(), 2);
    Assert.assertEquals(block.getContainer().get(0), new Object[]{1});
    Assert.assertEquals(block.getContainer().get(1), new Object[]{2});
    Assert.assertTrue(block2.isEndOfStreamBlock(), "expected EOS block to propagate");
  }

  @Test
  public void shouldConsumeAndSortTwoInputBlocksWithOneRowEachInputSorted() {
    // Given:
    List<RexExpression> collation = collation(0);
    List<Direction> directions = ImmutableList.of(Direction.ASCENDING);
    List<NullDirection> nullDirections = ImmutableList.of(NullDirection.LAST);
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    SortOperator op =
        new SortOperator(OperatorTestUtil.getDefaultContext(), _input, collation, directions, nullDirections, 10, 0,
            schema, true);

    // Set input rows as sorted since input is expected to be sorted
    Mockito.when(_input.nextBlock()).thenReturn(block(schema, new Object[]{1}))
        .thenReturn(block(schema, new Object[]{2}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    // When:
    TransferableBlock block = op.nextBlock(); // construct
    TransferableBlock block2 = op.nextBlock(); // eos

    // Then:
    Assert.assertEquals(block.getNumRows(), 2);
    Assert.assertEquals(block.getContainer().get(0), new Object[]{1});
    Assert.assertEquals(block.getContainer().get(1), new Object[]{2});
    Assert.assertTrue(block2.isEndOfStreamBlock(), "expected EOS block to propagate");
  }

  @Test
  public void shouldBreakTiesUsingSecondCollationKey() {
    // Given:
    List<RexExpression> collation = collation(0, 1);
    List<Direction> directions = ImmutableList.of(Direction.ASCENDING, Direction.ASCENDING);
    List<NullDirection> nullDirections = ImmutableList.of(NullDirection.LAST, NullDirection.LAST);
    DataSchema schema = new DataSchema(new String[]{"first", "second"}, new DataSchema.ColumnDataType[]{INT, INT});
    SortOperator op =
        new SortOperator(OperatorTestUtil.getDefaultContext(), _input, collation, directions, nullDirections, 10, 0,
            schema, false);

    Mockito.when(_input.nextBlock())
        .thenReturn(block(schema, new Object[]{1, 2}, new Object[]{1, 1}, new Object[]{1, 3}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    // When:
    TransferableBlock block = op.nextBlock(); // construct
    TransferableBlock block2 = op.nextBlock(); // eos

    // Then:
    Assert.assertEquals(block.getNumRows(), 3);
    Assert.assertEquals(block.getContainer().get(0), new Object[]{1, 1});
    Assert.assertEquals(block.getContainer().get(1), new Object[]{1, 2});
    Assert.assertEquals(block.getContainer().get(2), new Object[]{1, 3});
    Assert.assertTrue(block2.isEndOfStreamBlock(), "expected EOS block to propagate");
  }

  @Test
  public void shouldBreakTiesUsingSecondCollationKeyWithDifferentDirection() {
    // Given:
    List<RexExpression> collation = collation(0, 1);
    List<Direction> directions = ImmutableList.of(Direction.ASCENDING, Direction.DESCENDING);
    List<NullDirection> nullDirections = ImmutableList.of(NullDirection.LAST, NullDirection.FIRST);
    DataSchema schema = new DataSchema(new String[]{"first", "second"}, new DataSchema.ColumnDataType[]{INT, INT});
    SortOperator op =
        new SortOperator(OperatorTestUtil.getDefaultContext(), _input, collation, directions, nullDirections, 10, 0,
            schema, false);

    Mockito.when(_input.nextBlock())
        .thenReturn(block(schema, new Object[]{1, 2}, new Object[]{1, 1}, new Object[]{1, 3}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    // When:
    TransferableBlock block = op.nextBlock(); // construct
    TransferableBlock block2 = op.nextBlock(); // eos

    // Then:
    Assert.assertEquals(block.getNumRows(), 3);
    Assert.assertEquals(block.getContainer().get(0), new Object[]{1, 3});
    Assert.assertEquals(block.getContainer().get(1), new Object[]{1, 2});
    Assert.assertEquals(block.getContainer().get(2), new Object[]{1, 1});
    Assert.assertTrue(block2.isEndOfStreamBlock(), "expected EOS block to propagate");
  }

  @Test
  public void shouldHandleNoOpUpstreamBlockWhileConstructing() {
    // Given:
    List<RexExpression> collation = collation(0);
    List<Direction> directions = ImmutableList.of(Direction.ASCENDING);
    List<NullDirection> nullDirections = ImmutableList.of(NullDirection.LAST);
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    SortOperator op =
        new SortOperator(OperatorTestUtil.getDefaultContext(), _input, collation, directions, nullDirections, 10, 0,
            schema, false);

    Mockito.when(_input.nextBlock()).thenReturn(block(schema, new Object[]{2}))
        .thenReturn(block(schema, new Object[]{1}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    // When:
    TransferableBlock block = op.nextBlock(); // construct
    TransferableBlock block2 = op.nextBlock(); // eos

    // Then:
    Assert.assertEquals(block.getNumRows(), 2);
    Assert.assertEquals(block.getContainer().get(0), new Object[]{1});
    Assert.assertEquals(block.getContainer().get(1), new Object[]{2});
    Assert.assertTrue(block2.isEndOfStreamBlock(), "expected EOS block to propagate");
  }

  @Test
  public void shouldHandleNoOpUpstreamBlockWhileConstructingInputSorted() {
    // Given:
    List<RexExpression> collation = collation(0);
    List<Direction> directions = ImmutableList.of(Direction.ASCENDING);
    List<NullDirection> nullDirections = ImmutableList.of(NullDirection.LAST);
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    SortOperator op =
        new SortOperator(OperatorTestUtil.getDefaultContext(), _input, collation, directions, nullDirections, 10, 0,
            schema, true);

    // Set input rows as sorted since input is expected to be sorted
    Mockito.when(_input.nextBlock()).thenReturn(block(schema, new Object[]{1}))
        .thenReturn(block(schema, new Object[]{2}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    // When:
    TransferableBlock block = op.nextBlock(); // construct
    TransferableBlock block2 = op.nextBlock(); // eos

    // Then:
    Assert.assertEquals(block.getNumRows(), 2);
    Assert.assertEquals(block.getContainer().get(0), new Object[]{1});
    Assert.assertEquals(block.getContainer().get(1), new Object[]{2});
    Assert.assertTrue(block2.isEndOfStreamBlock(), "expected EOS block to propagate");
  }

  @Test
  public void shouldHaveNullAtLast() {
    // Given:
    List<RexExpression> collation = collation(0);
    List<Direction> directions = ImmutableList.of(Direction.ASCENDING);
    List<NullDirection> nullDirections = ImmutableList.of(NullDirection.LAST);
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    SortOperator op =
        new SortOperator(OperatorTestUtil.getDefaultContext(), _input, collation, directions, nullDirections, 10, 0,
            schema, false);

    Mockito.when(_input.nextBlock()).thenReturn(block(schema, new Object[]{2}, new Object[]{1}, new Object[]{null}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    // When:
    TransferableBlock block = op.nextBlock(); // construct
    TransferableBlock block2 = op.nextBlock(); // eos

    // Then:
    Assert.assertEquals(block.getNumRows(), 3);
    Assert.assertEquals(block.getContainer().get(0), new Object[]{1});
    Assert.assertEquals(block.getContainer().get(1), new Object[]{2});
    Assert.assertEquals(block.getContainer().get(2), new Object[]{null});
    Assert.assertTrue(block2.isEndOfStreamBlock(), "expected EOS block to propagate");
  }

  @Test
  public void shouldHaveNullAtFirst() {
    // Given:
    List<RexExpression> collation = collation(0);
    List<Direction> directions = ImmutableList.of(Direction.ASCENDING);
    List<NullDirection> nullDirections = ImmutableList.of(NullDirection.FIRST);
    DataSchema schema = new DataSchema(new String[]{"sort"}, new DataSchema.ColumnDataType[]{INT});
    SortOperator op =
        new SortOperator(OperatorTestUtil.getDefaultContext(), _input, collation, directions, nullDirections, 10, 0,
            schema, false);

    Mockito.when(_input.nextBlock()).thenReturn(block(schema, new Object[]{2}, new Object[]{1}, new Object[]{null}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    // When:
    TransferableBlock block = op.nextBlock(); // construct
    TransferableBlock block2 = op.nextBlock(); // eos

    // Then:
    Assert.assertEquals(block.getNumRows(), 3);
    Assert.assertEquals(block.getContainer().get(0), new Object[]{null});
    Assert.assertEquals(block.getContainer().get(1), new Object[]{1});
    Assert.assertEquals(block.getContainer().get(2), new Object[]{2});
    Assert.assertTrue(block2.isEndOfStreamBlock(), "expected EOS block to propagate");
  }

  @Test
  public void shouldHandleMultipleCollationKeysWithNulls() {
    // Given:
    List<RexExpression> collation = collation(0, 1);
    List<Direction> directions = ImmutableList.of(Direction.ASCENDING, Direction.DESCENDING);
    List<NullDirection> nullDirections = ImmutableList.of(NullDirection.FIRST, NullDirection.LAST);
    DataSchema schema = new DataSchema(new String[]{"first", "second"}, new DataSchema.ColumnDataType[]{INT, INT});
    SortOperator op =
        new SortOperator(OperatorTestUtil.getDefaultContext(), _input, collation, directions, nullDirections, 10, 0,
            schema, false);

    Mockito.when(_input.nextBlock())
        .thenReturn(block(schema, new Object[]{1, 1}, new Object[]{1, null}, new Object[]{null, 1}))
        .thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    // When:
    TransferableBlock block = op.nextBlock(); // construct
    TransferableBlock block2 = op.nextBlock(); // eos

    // Then:
    Assert.assertEquals(block.getNumRows(), 3);
    Assert.assertEquals(block.getContainer().get(0), new Object[]{null, 1});
    Assert.assertEquals(block.getContainer().get(1), new Object[]{1, 1});
    Assert.assertEquals(block.getContainer().get(2), new Object[]{1, null});
    Assert.assertTrue(block2.isEndOfStreamBlock(), "expected EOS block to propagate");
  }

  private static List<RexExpression> collation(int... indexes) {
    return Arrays.stream(indexes).mapToObj(RexExpression.InputRef::new).collect(Collectors.toList());
  }

  private static TransferableBlock block(DataSchema schema, Object[]... rows) {
    return new TransferableBlock(Arrays.asList(rows), schema, DataBlock.Type.ROW);
  }
}
