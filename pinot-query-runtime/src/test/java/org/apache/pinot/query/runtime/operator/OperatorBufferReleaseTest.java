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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.operator.set.IntersectOperator;
import org.apache.pinot.query.runtime.operator.set.UnionOperator;
import org.mockito.Mock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.INT;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.STRING;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Asserts the invariant that a [MultiStageOperator] releases the row and hash buffers it accumulated on *every*
/// termination path — not only when it produces its end-of-stream block, but also when the query errors out and when
/// it is cancelled.
///
/// While an operator stays reachable its un-released buffers are live references, so no GC can reclaim them. Under
/// heap pressure that turns a single failing stage into a self-reinforcing cascade, which is why the release has to
/// happen in the operator rather than depend on nothing else holding the operator tree.
///
/// The buffers are private state, so these tests read them reflectively rather than widening the production API.
public class OperatorBufferReleaseTest {
  private static final DataSchema SCHEMA =
      new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{INT, STRING});
  private static final DataSchema JOIN_RESULT_SCHEMA =
      new DataSchema(new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
          new ColumnDataType[]{INT, STRING, INT, STRING});

  private AutoCloseable _mocks;
  @Mock
  private MultiStageOperator _input;
  @Mock
  private MultiStageOperator _leftInput;
  @Mock
  private MultiStageOperator _rightInput;

  @BeforeMethod
  public void setUp() {
    _mocks = openMocks(this);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void sortOperatorReleasesPriorityQueueOnError() {
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(SCHEMA, new Object[]{2, "b"}, new Object[]{1, "a"}))
        .thenReturn(ErrorMseBlock.fromException(new RuntimeException("boom")));
    SortOperator operator = sortOperator(_input);

    assertTrue(operator.nextBlock().isError());
    assertFalse(isEmpty(field(operator, "_priorityQueue")), "rows should still be buffered before termination");

    operator.cancel(new RuntimeException("boom"));

    assertTrue(isEmpty(field(operator, "_priorityQueue")), "priority queue should be released on cancel");
    assertNull(field(operator, "_rows"));
  }

  @Test
  public void sortOperatorReleasesRowsOnError() {
    // No collations => the operator buffers into _rows instead of the priority queue.
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(SCHEMA, new Object[]{2, "b"}, new Object[]{1, "a"}))
        .thenReturn(ErrorMseBlock.fromException(new RuntimeException("boom")));
    SortOperator operator = sortOperator(_input, List.of());

    assertTrue(operator.nextBlock().isError());
    assertNotNull(field(operator, "_rows"), "rows should still be buffered before termination");

    operator.close();

    assertNull(field(operator, "_rows"), "buffered rows should be released on close");
  }

  /// [SortOperator] used to override `cancel()` with an empty body, so a cancel never reached the operators feeding
  /// it. Releasing state must not come at the cost of that recursion.
  @Test
  public void sortOperatorCancelReachesChildren() {
    when(_input.nextBlock()).thenReturn(SuccessMseBlock.INSTANCE);
    SortOperator operator = sortOperator(_input);
    RuntimeException error = new RuntimeException("boom");

    operator.cancel(error);

    verify(_input, times(1)).cancel(error);
  }

  /// The block [SortOperator] emits is a sublist view of `_rows`, and a block handed to a local mailbox can still be
  /// read after this op chain has been closed. Releasing must therefore drop the reference, never empty the list.
  @Test
  public void sortOperatorCloseDoesNotEmptyTheEmittedBlock() {
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(SCHEMA, new Object[]{2, "b"}, new Object[]{1, "a"}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    SortOperator operator = sortOperator(_input, List.of());
    List<Object[]> rows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(rows.size(), 2);

    operator.close();

    assertEquals(rows.size(), 2, "the emitted block must survive the operator being closed");
    assertEquals(rows.get(0), new Object[]{2, "b"});
  }

  /// The right side is materialized, the join starts emitting, and then the op chain is cancelled mid-flight — the
  /// case where nothing on the success path ever gets to run.
  @Test
  public void hashJoinOperatorReleasesRightTableOnCancel() {
    when(_rightInput.nextBlock()).thenReturn(OperatorTestUtil.block(SCHEMA, new Object[]{1, "a"}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    when(_leftInput.nextBlock()).thenReturn(OperatorTestUtil.block(SCHEMA, new Object[]{1, "a"}));
    HashJoinOperator operator = hashJoinOperator();

    assertTrue(operator.nextBlock().isData());
    assertNotNull(field(operator, "_rightTable"), "the right table should still be held mid-join");

    operator.cancel(new RuntimeException("boom"));

    assertNull(field(operator, "_rightTable"));
    assertNull(field(operator, "_matchedRightRows"));
    assertNull(field(operator, "_nullKeyRightRows"));
  }

  /// When the right side itself fails, the partially built right table never reaches the success path.
  @Test
  public void hashJoinOperatorReleasesPartialRightTableOnError() {
    when(_rightInput.nextBlock()).thenReturn(OperatorTestUtil.block(SCHEMA, new Object[]{1, "a"}))
        .thenReturn(ErrorMseBlock.fromException(new RuntimeException("boom")));
    HashJoinOperator operator = hashJoinOperator();

    assertTrue(operator.nextBlock().isError());
    assertNotNull(field(operator, "_rightTable"), "the partial right table is still held when the error propagates");

    operator.cancel(new RuntimeException("boom"));

    assertNull(field(operator, "_rightTable"));
    assertNull(field(operator, "_matchedRightRows"));
    assertNull(field(operator, "_nullKeyRightRows"));
  }

  @Test
  public void nonEquiJoinOperatorReleasesRightTableOnCancel() {
    when(_rightInput.nextBlock()).thenReturn(OperatorTestUtil.block(SCHEMA, new Object[]{1, "a"}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    when(_leftInput.nextBlock()).thenReturn(OperatorTestUtil.block(SCHEMA, new Object[]{0, "z"}));
    NonEquiJoinOperator operator = nonEquiJoinOperator();

    assertTrue(operator.nextBlock().isData());
    assertFalse(isEmpty(field(operator, "_rightTable")), "the right table should still be held mid-join");

    operator.cancel(new RuntimeException("boom"));

    assertTrue(isEmpty(field(operator, "_rightTable")));
    assertNull(field(operator, "_matchedRightRows"));
  }

  @Test
  public void nonEquiJoinOperatorReleasesPartialRightTableOnError() {
    when(_rightInput.nextBlock()).thenReturn(OperatorTestUtil.block(SCHEMA, new Object[]{1, "a"}))
        .thenReturn(ErrorMseBlock.fromException(new RuntimeException("boom")));
    NonEquiJoinOperator operator = nonEquiJoinOperator();

    assertTrue(operator.nextBlock().isError());
    assertFalse(isEmpty(field(operator, "_rightTable")),
        "the partial right table is still held when the error propagates");

    operator.close();

    assertTrue(isEmpty(field(operator, "_rightTable")));
  }

  @Test
  public void aggregateOperatorReleasesGroupByExecutorOnError() {
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(SCHEMA, new Object[]{1, "a"}, new Object[]{2, "b"}))
        .thenReturn(ErrorMseBlock.fromException(new RuntimeException("boom")));
    AggregateOperator operator = aggregateOperator(List.of(0));

    assertNotNull(field(operator, "_groupByExecutor"));

    assertTrue(operator.nextBlock().isError());

    // The upstream failed, so the group-by hash maps are dead weight from the moment the error block is produced.
    assertNull(field(operator, "_groupByExecutor"), "group-by executor should be released on the error path");
    operator.close();
    assertNull(field(operator, "_groupByExecutor"));
  }

  @Test
  public void aggregateOperatorReleasesAggregationExecutorOnError() {
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(SCHEMA, new Object[]{1, "a"}))
        .thenReturn(ErrorMseBlock.fromException(new RuntimeException("boom")));
    AggregateOperator operator = aggregateOperator(List.of());

    assertNotNull(field(operator, "_aggregationExecutor"));

    assertTrue(operator.nextBlock().isError());

    assertNull(field(operator, "_aggregationExecutor"), "aggregation executor should be released on the error path");
  }

  @Test
  public void repeatOperatorReleasesCurrentRowsOnCancel() {
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(SCHEMA, new Object[]{1, "a"}, new Object[]{2, "b"}));
    // Two grouping sets, so the operator still holds the input block after emitting the first expansion.
    RepeatOperator operator = new RepeatOperator(OperatorTestUtil.getTracingContext(), _input, new int[]{0},
        List.of(List.of(0), List.of()), repeatResultSchema());

    assertTrue(operator.nextBlock().isData());
    assertNotNull(field(operator, "_currentRows"), "the input block should still be held before termination");

    operator.cancel(new RuntimeException("boom"));

    assertNull(field(operator, "_currentRows"));
  }

  @Test
  public void intersectOperatorReleasesRightRowSetOnCancel() {
    MultiStageOperator right = mock(MultiStageOperator.class);
    MultiStageOperator left = mock(MultiStageOperator.class);
    // Two right rows, one of which the left side matches, so the set is still populated when the cancel lands.
    when(right.nextBlock()).thenReturn(OperatorTestUtil.block(SCHEMA, new Object[]{1, "a"}, new Object[]{2, "b"}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    when(left.nextBlock()).thenReturn(OperatorTestUtil.block(SCHEMA, new Object[]{1, "a"}));
    IntersectOperator operator =
        new IntersectOperator(OperatorTestUtil.getTracingContext(), List.of(left, right), SCHEMA);

    assertTrue(operator.nextBlock().isData());
    assertFalse(isEmpty(field(operator, "_rightRowSet")), "the right row set should still be held mid-intersect");

    operator.cancel(new RuntimeException("boom"));

    assertTrue(isEmpty(field(operator, "_rightRowSet")));
  }

  @Test
  public void intersectOperatorReleasesRightRowSetOnError() {
    MultiStageOperator right = mock(MultiStageOperator.class);
    MultiStageOperator left = mock(MultiStageOperator.class);
    when(right.nextBlock()).thenReturn(OperatorTestUtil.block(SCHEMA, new Object[]{1, "a"}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    when(left.nextBlock()).thenReturn(ErrorMseBlock.fromException(new RuntimeException("boom")));
    IntersectOperator operator =
        new IntersectOperator(OperatorTestUtil.getTracingContext(), List.of(left, right), SCHEMA);

    assertTrue(operator.nextBlock().isError());
    operator.cancel(new RuntimeException("boom"));

    assertTrue(isEmpty(field(operator, "_rightRowSet")));
  }

  @Test
  public void unionOperatorReleasesSeenRecordsOnError() {
    MultiStageOperator first = mock(MultiStageOperator.class);
    MultiStageOperator second = mock(MultiStageOperator.class);
    when(first.nextBlock()).thenReturn(OperatorTestUtil.block(SCHEMA, new Object[]{1, "a"}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    when(second.nextBlock()).thenReturn(ErrorMseBlock.fromException(new RuntimeException("boom")));
    UnionOperator operator = new UnionOperator(OperatorTestUtil.getTracingContext(), List.of(first, second), SCHEMA);

    assertTrue(operator.nextBlock().isData());
    assertFalse(isEmpty(field(operator, "_seenRecords")), "seen records should be tracked before termination");
    assertTrue(operator.nextBlock().isError());

    operator.cancel(new RuntimeException("boom"));

    assertTrue(isEmpty(field(operator, "_seenRecords")));
  }

  private HashJoinOperator hashJoinOperator() {
    return new HashJoinOperator(OperatorTestUtil.getTracingContext(), _leftInput, SCHEMA, _rightInput,
        new JoinNode(-1, JOIN_RESULT_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), JoinRelType.FULL, List.of(0),
            List.of(0), List.of(), JoinNode.JoinStrategy.HASH));
  }

  private NonEquiJoinOperator nonEquiJoinOperator() {
    // Condition: left.int_col < right.int_col
    List<RexExpression> nonEquiConditions = List.of(
        new RexExpression.FunctionCall(ColumnDataType.BOOLEAN, SqlKind.LESS_THAN.name(),
            List.of(new RexExpression.InputRef(0), new RexExpression.InputRef(2))));
    return new NonEquiJoinOperator(OperatorTestUtil.getTracingContext(), _leftInput, SCHEMA, _rightInput,
        new JoinNode(-1, JOIN_RESULT_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), JoinRelType.FULL, List.of(),
            List.of(), nonEquiConditions, JoinNode.JoinStrategy.HASH));
  }

  private SortOperator sortOperator(MultiStageOperator input) {
    return sortOperator(input, List.of(new RelFieldCollation(0, Direction.ASCENDING, NullDirection.LAST)));
  }

  private SortOperator sortOperator(MultiStageOperator input, List<RelFieldCollation> collations) {
    return new SortOperator(OperatorTestUtil.getTracingContext(), input,
        new SortNode(-1, SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), collations, 10, 0));
  }

  private AggregateOperator aggregateOperator(List<Integer> groupKeys) {
    RexExpression.FunctionCall countStar =
        new RexExpression.FunctionCall(ColumnDataType.LONG, SqlKind.COUNT.name(), List.of());
    return new AggregateOperator(OperatorTestUtil.getTracingContext(), _input,
        new AggregateNode(-1, aggregateResultSchema(groupKeys), PlanNode.NodeHint.EMPTY, List.of(), List.of(countStar),
            List.of(-1), groupKeys, AggType.DIRECT, false, null, 0));
  }

  private static DataSchema aggregateResultSchema(List<Integer> groupKeys) {
    return groupKeys.isEmpty() ? new DataSchema(new String[]{"count"}, new ColumnDataType[]{ColumnDataType.LONG})
        : new DataSchema(new String[]{"int_col", "count"}, new ColumnDataType[]{INT, ColumnDataType.LONG});
  }

  /// Input schema plus one group-key copy column plus the `$groupingId` discriminator.
  private static DataSchema repeatResultSchema() {
    return new DataSchema(new String[]{"int_col", "string_col", "int_col_key", "$groupingId"},
        new ColumnDataType[]{INT, STRING, INT, INT});
  }

  private static Object field(Object operator, String name) {
    try {
      return FieldUtils.readField(operator, name, true);
    } catch (IllegalAccessException e) {
      throw new AssertionError("Cannot read field " + name + " of " + operator.getClass().getSimpleName(), e);
    }
  }

  private static boolean isEmpty(Object collection) {
    assertNotNull(collection, "expected a collection, not null");
    if (collection instanceof Collection) {
      return ((Collection<?>) collection).isEmpty();
    }
    if (collection instanceof Map) {
      return ((Map<?, ?>) collection).isEmpty();
    }
    throw new AssertionError("Not a collection: " + collection.getClass());
  }
}
