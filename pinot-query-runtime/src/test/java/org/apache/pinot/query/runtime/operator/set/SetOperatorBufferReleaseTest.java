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
package org.apache.pinot.query.runtime.operator.set;

import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.operator.BlockListMultiStageOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OperatorTestUtil;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// The set-operator half of the release invariant asserted by
/// [org.apache.pinot.query.runtime.operator.OperatorBufferReleaseTest]: every operator must drop the row state it is
/// holding on end of stream, on error and on cancellation alike.
///
/// These live in their own class because `hasBufferedState()` is `protected` and the overrides that matter here are
/// declared in this package.
///
/// The [BinarySetOperator] subclasses all buffer the whole right child in `_rightRowSet`, and [UnionOperator] buffers
/// every distinct row it has seen — neither was released on any path before.
public class SetOperatorBufferReleaseTest {
  private static final DataSchema SCHEMA =
      new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{
          ColumnDataType.INT, ColumnDataType.STRING
      });
  private static final RuntimeException ERROR = new RuntimeException("boom");

  @Test
  public void shouldReleaseRightRowSetOnCancelForEveryBinarySetOperator() {
    for (BinarySetOperatorFactory factory : binarySetOperators()) {
      // The right child completes and the left child produces a block, so the operator is mid-flight.
      MultiStageOperator right = twoRowOperator();
      BinarySetOperator operator = factory.create(mixedLeftOperator(), right);
      String name = operator.getClass().getSimpleName();

      assertTrue(operator.nextBlock().isData(), name + " should emit a data block");
      assertTrue(operator.hasBufferedState(), name + " should still hold the right row set mid-flight");

      operator.cancel(ERROR);

      assertFalse(operator.hasBufferedState(), name + " should release the right row set on cancel");
    }
  }

  @Test
  public void shouldReleaseRightRowSetOnCloseForEveryBinarySetOperator() {
    for (BinarySetOperatorFactory factory : binarySetOperators()) {
      MultiStageOperator right = twoRowOperator();
      BinarySetOperator operator = factory.create(mixedLeftOperator(), right);
      String name = operator.getClass().getSimpleName();

      assertTrue(operator.nextBlock().isData());
      assertTrue(operator.hasBufferedState());

      operator.close();

      assertFalse(operator.hasBufferedState(), name + " should release the right row set on close");
      // Terminating twice, by the other path, must be safe.
      operator.cancel(ERROR);
      assertFalse(operator.hasBufferedState(), name + " should stay released after a second termination");
    }
  }

  /// When the left child fails, the right row set is released in band with the error block rather than waiting for
  /// teardown.
  @Test
  public void shouldReleaseRightRowSetWhenTheLeftChildFails() {
    for (BinarySetOperatorFactory factory : binarySetOperators()) {
      MultiStageOperator right = twoRowOperator();
      MultiStageOperator left = mock(MultiStageOperator.class);
      when(left.nextBlock()).thenReturn(ErrorMseBlock.fromException(ERROR));
      BinarySetOperator operator = factory.create(left, right);
      String name = operator.getClass().getSimpleName();

      assertTrue(operator.nextBlock().isError(), name + " should propagate the error");

      assertFalse(operator.hasBufferedState(), name + " should release the right row set on the error path");
      operator.close();
      assertFalse(operator.hasBufferedState());
    }
  }

  /// The right child failing leaves the right row set partially built and no result will ever be produced from it.
  @Test
  public void shouldReleasePartialRightRowSetWhenTheRightChildFails() {
    for (BinarySetOperatorFactory factory : binarySetOperators()) {
      MultiStageOperator right = mock(MultiStageOperator.class);
      when(right.nextBlock()).thenReturn(OperatorTestUtil.block(SCHEMA, new Object[]{1, "AA"}))
          .thenReturn(ErrorMseBlock.fromException(ERROR));
      BinarySetOperator operator = factory.create(mixedLeftOperator(), right);
      String name = operator.getClass().getSimpleName();

      assertTrue(operator.nextBlock().isError(), name + " should propagate the error");

      assertFalse(operator.hasBufferedState(), name + " should release the partial right row set");
    }
  }

  /// The emitted rows come from the left child, so releasing the right row set must not disturb a block already
  /// handed downstream.
  @Test
  public void shouldNotEmptyTheEmittedBlockOnClose() {
    MultiStageOperator right = twoRowOperator();
    MultiStageOperator left = new BlockListMultiStageOperator.Builder(SCHEMA).addRow(1, "AA").buildWithEos();
    IntersectOperator operator =
        new IntersectOperator(OperatorTestUtil.getTracingContext(), List.of(left, right), SCHEMA);
    List<Object[]> rows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(rows.size(), 1);

    operator.close();

    assertEquals(rows.size(), 1, "the emitted block must survive the operator being closed");
    assertEquals(rows.get(0), new Object[]{1, "AA"});
  }

  @Test
  public void shouldReleaseUnionSeenRecordsOnCancel() {
    MultiStageOperator first = mock(MultiStageOperator.class);
    when(first.nextBlock()).thenReturn(OperatorTestUtil.block(SCHEMA, new Object[]{1, "AA"}));
    MultiStageOperator second = mock(MultiStageOperator.class);
    UnionOperator operator = new UnionOperator(OperatorTestUtil.getTracingContext(), List.of(first, second), SCHEMA);

    assertTrue(operator.nextBlock().isData());
    assertTrue(operator.hasBufferedState(), "the seen records should be tracked before termination");

    operator.cancel(ERROR);

    assertFalse(operator.hasBufferedState());
  }

  @Test
  public void shouldReleaseUnionSeenRecordsOnError() {
    MultiStageOperator first = mock(MultiStageOperator.class);
    when(first.nextBlock()).thenReturn(OperatorTestUtil.block(SCHEMA, new Object[]{1, "AA"}))
        .thenReturn(ErrorMseBlock.fromException(ERROR));
    MultiStageOperator second = mock(MultiStageOperator.class);
    UnionOperator operator = new UnionOperator(OperatorTestUtil.getTracingContext(), List.of(first, second), SCHEMA);

    assertTrue(operator.nextBlock().isData());
    assertTrue(operator.hasBufferedState());
    assertTrue(operator.nextBlock().isError());

    assertFalse(operator.hasBufferedState(), "the seen records should be released on the error path");
    operator.close();
    assertFalse(operator.hasBufferedState());
  }

  /// Draining every input successfully must release too — the success path is the common one.
  @Test
  public void shouldReleaseUnionSeenRecordsOnEndOfStream() {
    MultiStageOperator first = new BlockListMultiStageOperator.Builder(SCHEMA).addRow(1, "AA").buildWithEos();
    MultiStageOperator second = new BlockListMultiStageOperator.Builder(SCHEMA).addRow(2, "BB").buildWithEos();
    UnionOperator operator = new UnionOperator(OperatorTestUtil.getTracingContext(), List.of(first, second), SCHEMA);

    MseBlock block = operator.nextBlock();
    while (block.isData()) {
      block = operator.nextBlock();
    }

    assertTrue(block.isSuccess());
    assertFalse(operator.hasBufferedState(), "the seen records should be released once every input is drained");
  }

  /// The emitted blocks reference the left rows directly, so releasing must not touch them.
  @Test
  public void shouldNotEmptyTheBlockEmittedByUnion() {
    MultiStageOperator first = new BlockListMultiStageOperator.Builder(SCHEMA).addRow(1, "AA").buildWithEos();
    MultiStageOperator second = new BlockListMultiStageOperator.Builder(SCHEMA).addRow(2, "BB").buildWithEos();
    UnionOperator operator = new UnionOperator(OperatorTestUtil.getTracingContext(), List.of(first, second), SCHEMA);
    List<Object[]> rows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(rows.size(), 1);

    operator.close();

    assertEquals(rows.size(), 1, "the emitted block must survive the operator being closed");
    assertEquals(rows.get(0), new Object[]{1, "AA"});
  }

  private static MultiStageOperator twoRowOperator() {
    // Two rows so that the set is still non-empty after the left child has matched one of them.
    return new BlockListMultiStageOperator.Builder(SCHEMA).addRow(1, "AA").addRow(2, "BB").buildWithEos();
  }

  /// One row the right child also has and one it does not, so that every flavour emits a data block: INTERSECT and
  /// INTERSECT ALL keep the matching row, MINUS and MINUS ALL keep the other one.
  private static MultiStageOperator mixedLeftOperator() {
    return new BlockListMultiStageOperator.Builder(SCHEMA).addRow(1, "AA").addRow(3, "CC").buildWithEos();
  }

  private static List<BinarySetOperatorFactory> binarySetOperators() {
    return List.of(
        (left, right) -> new IntersectOperator(OperatorTestUtil.getTracingContext(), List.of(left, right), SCHEMA),
        (left, right) -> new IntersectAllOperator(OperatorTestUtil.getTracingContext(), List.of(left, right), SCHEMA),
        (left, right) -> new MinusOperator(OperatorTestUtil.getTracingContext(), List.of(left, right), SCHEMA),
        (left, right) -> new MinusAllOperator(OperatorTestUtil.getTracingContext(), List.of(left, right), SCHEMA));
  }

  @FunctionalInterface
  private interface BinarySetOperatorFactory {
    BinarySetOperator create(MultiStageOperator left, MultiStageOperator right);
  }
}
