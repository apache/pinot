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
import java.util.function.Supplier;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.common.datatable.StatMap;
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
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.BOOLEAN;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.INT;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.LONG;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.STRING;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Asserts the invariant that a [MultiStageOperator] releases the row and hash state it is holding on *every*
/// termination path — not only when it produces its end-of-stream block, but also when the query errors out and when
/// it is cancelled.
///
/// While an operator stays reachable its un-released buffers are live references, so no GC can reclaim them. That is
/// why the release has to happen in the operator itself rather than depend on nothing else holding the operator tree.
///
/// This is deliberately one cross-operator suite rather than a case bolted onto each operator's own test class: the
/// invariant is a property of [MultiStageOperator], the interesting failure is one operator quietly not having it,
/// and [#shouldReleaseByEitherPathForEveryOperator()] can only be written in one place. Each case drives an operator
/// to the point where it holds state, asserts [MultiStageOperator#hasBufferedState] so the scenario cannot go
/// vacuous, terminates it, and asserts the state is gone.
///
/// Set operators live in their own package and are covered by
/// [org.apache.pinot.query.runtime.operator.set.SetOperatorBufferReleaseTest].
public class OperatorBufferReleaseTest {
  private static final DataSchema SCHEMA =
      new DataSchema(new String[]{"int_col", "string_col"}, new ColumnDataType[]{INT, STRING});
  private static final DataSchema JOIN_RESULT_SCHEMA =
      new DataSchema(new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
          new ColumnDataType[]{INT, STRING, INT, STRING});
  private static final RuntimeException ERROR = new RuntimeException("boom");

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

  // ---------------------------------------------------------------------------------------------------------------
  // The base-class contract
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  public void shouldReleaseFromBothCloseAndCancel() {
    RecordingOperator closed = new RecordingOperator();
    closed.close();
    assertEquals(closed._releaseCount, 1, "close() must release");

    RecordingOperator cancelled = new RecordingOperator();
    cancelled.cancel(ERROR);
    assertEquals(cancelled._releaseCount, 1, "cancel() must release");
  }

  /// close() may run more than once, and it runs after cancel() on the error path, so releasing has to be idempotent.
  @Test
  public void shouldTolerateRepeatedTermination() {
    RecordingOperator operator = new RecordingOperator();
    operator.cancel(ERROR);
    operator.close();
    operator.close();
    assertEquals(operator._releaseCount, 3);
  }

  /// Children are torn down first, so an operator may still reach into its inputs while releasing.
  @Test
  public void shouldReleaseAfterClosingChildren() {
    RecordingOperator operator = new RecordingOperator(_input);
    operator.close();
    verify(_input, times(1)).close();
    assertTrue(operator._childrenWereClosedFirst);
  }

  /// Releasing is best-effort cleanup: a throwing implementation must not abort the rest of the teardown. An
  /// `AssertionError` is the interesting case, since tests run with assertions enabled.
  @Test
  public void shouldSurviveAThrowingRelease() {
    MultiStageOperator throwsException = new RecordingOperator(_input) {
      @Override
      protected void releaseBuffers() {
        throw new RuntimeException("release failed");
      }
    };
    throwsException.close();
    throwsException.cancel(ERROR);

    MultiStageOperator throwsAssertionError = new RecordingOperator(_leftInput) {
      @Override
      protected void releaseBuffers() {
        throw new AssertionError("release failed");
      }
    };
    throwsAssertionError.close();
    throwsAssertionError.cancel(ERROR);

    verify(_input, times(1)).close();
    verify(_input, times(1)).cancel(any());
    verify(_leftInput, times(1)).close();
    verify(_leftInput, times(1)).cancel(any());
  }

  // ---------------------------------------------------------------------------------------------------------------
  // SortOperator
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  public void shouldReleaseSortPriorityQueueOnError() {
    givenInputProducesThenFails();
    SortOperator operator = sortOperator(_input);

    assertTrue(operator.nextBlock().isError());
    assertTrue(operator.hasBufferedState(), "the sort is still holding its heap when the error propagates");

    operator.cancel(ERROR);

    assertFalse(operator.hasBufferedState());
  }

  @Test
  public void shouldReleaseSortRowsOnError() {
    // No collations => the operator buffers into _rows instead of the priority queue.
    givenInputProducesThenFails();
    SortOperator operator = sortOperator(_input, List.of());

    assertTrue(operator.nextBlock().isError());
    assertTrue(operator.hasBufferedState(), "the sort is still holding its rows when the error propagates");

    operator.close();

    assertFalse(operator.hasBufferedState());
  }

  /// [SortOperator] used to override `cancel()` with an empty body, so a cancel never reached the operators feeding
  /// it. Releasing state must not come at the cost of that recursion.
  @Test
  public void shouldPropagateSortCancelToChildren() {
    when(_input.nextBlock()).thenReturn(SuccessMseBlock.INSTANCE);
    SortOperator operator = sortOperator(_input);

    operator.cancel(ERROR);

    verify(_input, times(1)).cancel(ERROR);
  }

  /// The block [SortOperator] emits is a sublist view of `_rows`, and a block handed to a local mailbox can still be
  /// read after this op chain has been closed. Releasing must therefore drop the reference, never empty the list.
  @Test
  public void shouldNotEmptyTheBlockEmittedBySort() {
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(SCHEMA, new Object[]{2, "b"}, new Object[]{1, "a"}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    SortOperator operator = sortOperator(_input, List.of());
    List<Object[]> rows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(rows.size(), 2);

    operator.close();

    assertEquals(rows.size(), 2, "the emitted block must survive the operator being closed");
    assertEquals(rows.get(0), new Object[]{2, "b"});
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Joins
  // ---------------------------------------------------------------------------------------------------------------

  /// The right side is materialized, the join starts emitting, and then the op chain is cancelled mid-flight — the
  /// case where nothing on the success path ever gets to run.
  @Test
  public void shouldReleaseHashJoinRightTableOnCancel() {
    givenRightBuiltThenLeftKeepsFlowing();
    HashJoinOperator operator = hashJoinOperator();

    assertTrue(operator.nextBlock().isData());
    assertTrue(operator.hasBufferedState(), "the right table is still held mid-join");

    operator.cancel(ERROR);

    assertFalse(operator.hasBufferedState());
  }

  /// When the right side itself fails, the partially built right table never reaches the success path.
  @Test
  public void shouldReleaseHashJoinPartialRightTableOnError() {
    givenRightSideFails();
    HashJoinOperator operator = hashJoinOperator();

    assertTrue(operator.nextBlock().isError());
    assertTrue(operator.hasBufferedState(), "the partial right table is still held when the error propagates");

    operator.close();

    assertFalse(operator.hasBufferedState());
  }

  @Test
  public void shouldReleaseNonEquiJoinRightTableOnCancel() {
    givenRightBuiltThenLeftKeepsFlowing();
    NonEquiJoinOperator operator = nonEquiJoinOperator();

    assertTrue(operator.nextBlock().isData());
    assertTrue(operator.hasBufferedState(), "the right table is still held mid-join");

    operator.cancel(ERROR);

    assertFalse(operator.hasBufferedState());
  }

  @Test
  public void shouldReleaseNonEquiJoinPartialRightTableOnError() {
    givenRightSideFails();
    NonEquiJoinOperator operator = nonEquiJoinOperator();

    assertTrue(operator.nextBlock().isError());
    assertTrue(operator.hasBufferedState(), "the partial right table is still held when the error propagates");

    operator.close();

    assertFalse(operator.hasBufferedState());
  }

  /// The emitted rows are copies, so releasing the right table must not disturb a block already sent downstream.
  @Test
  public void shouldNotEmptyTheBlockEmittedByNonEquiJoin() {
    when(_rightInput.nextBlock()).thenReturn(OperatorTestUtil.block(SCHEMA, new Object[]{5, "r"}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    when(_leftInput.nextBlock()).thenReturn(OperatorTestUtil.block(SCHEMA, new Object[]{1, "l"}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    NonEquiJoinOperator operator = nonEquiJoinOperator();
    List<Object[]> rows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(rows.size(), 1);

    operator.close();

    assertEquals(rows.size(), 1, "the emitted block must survive the operator being closed");
    assertEquals(rows.get(0), new Object[]{1, "l", 5, "r"});
  }

  @Test
  public void shouldReleaseAsofJoinRightTableOnCancel() {
    givenRightBuiltThenLeftKeepsFlowing();
    AsofJoinOperator operator = asofJoinOperator();

    assertTrue(operator.nextBlock().isData());
    assertTrue(operator.hasBufferedState(), "the right table is still held mid-join");

    operator.cancel(ERROR);

    assertFalse(operator.hasBufferedState());
  }

  @Test
  public void shouldReleaseAsofJoinPartialRightTableOnError() {
    givenRightSideFails();
    AsofJoinOperator operator = asofJoinOperator();

    assertTrue(operator.nextBlock().isError());
    assertTrue(operator.hasBufferedState(), "the partial right table is still held when the error propagates");

    operator.close();

    assertFalse(operator.hasBufferedState());
  }

  // ---------------------------------------------------------------------------------------------------------------
  // AggregateOperator and RepeatOperator
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  public void shouldReleaseGroupByExecutorOnError() {
    givenInputProducesThenFails();
    AggregateOperator operator = aggregateOperator(List.of(0));
    assertTrue(operator.hasBufferedState());

    assertTrue(operator.nextBlock().isError());

    // The upstream failed, so the group-by hash maps are dead weight from the moment the error block is produced.
    assertFalse(operator.hasBufferedState(), "the group-by executor should be released on the error path");
    operator.close();
    assertFalse(operator.hasBufferedState());
  }

  @Test
  public void shouldReleaseAggregationExecutorOnError() {
    givenInputProducesThenFails();
    AggregateOperator operator = aggregateOperator(List.of());
    assertTrue(operator.hasBufferedState());

    assertTrue(operator.nextBlock().isError());

    assertFalse(operator.hasBufferedState(), "the aggregation executor should be released on the error path");
    operator.cancel(ERROR);
    assertFalse(operator.hasBufferedState());
  }

  /// Releasing the executors must not change which of the two the operator thinks it is: a group-by that has been
  /// closed still has to behave like a group-by, not silently turn into a plain aggregation.
  @Test
  public void shouldKeepAggregateModeAfterRelease() {
    givenInputProducesThenFails();
    AggregateOperator groupBy = aggregateOperator(List.of(0));
    groupBy.close();
    assertTrue(groupBy.nextBlock().isEos(), "a released group-by must not be re-dispatched as an aggregation");

    givenInputProducesThenFails();
    AggregateOperator aggregation = aggregateOperator(List.of());
    aggregation.close();
    assertTrue(aggregation.nextBlock().isEos());
  }

  @Test
  public void shouldReleaseRepeatCurrentRowsOnCancel() {
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(SCHEMA, new Object[]{1, "a"}, new Object[]{2, "b"}));
    RepeatOperator operator = repeatOperator();

    assertTrue(operator.nextBlock().isData());
    assertTrue(operator.hasBufferedState(), "the input block is still held between grouping sets");

    operator.cancel(ERROR);

    assertFalse(operator.hasBufferedState());
  }

  /// `_currentRows == null` means "pull the next input block", so releasing it must also mark the operator finished —
  /// otherwise a released operator would go back to an input that has already been closed.
  @Test
  public void shouldNotPullFromTheInputAfterRepeatIsReleased() {
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(SCHEMA, new Object[]{1, "a"}));
    RepeatOperator operator = repeatOperator();
    assertTrue(operator.nextBlock().isData());
    reset(_input);

    operator.close();

    assertTrue(operator.nextBlock().isEos(), "a released operator must report end of stream");
    verify(_input, times(0)).nextBlock();
    assertFalse(operator.hasBufferedState());
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Cross-operator sweep
  // ---------------------------------------------------------------------------------------------------------------

  /// close() and cancel() must be interchangeable and repeatable for every operator that buffers: whichever one runs,
  /// and however often, the operator ends up holding nothing. Guards against an operator that releases on only one
  /// of the two paths, which is the exact defect this change set fixes.
  @Test
  public void shouldReleaseByEitherPathForEveryOperator() {
    for (boolean cancelFirst : new boolean[]{true, false}) {
      assertReleasedByEitherPath(cancelFirst, this::sortHoldingRows);
      assertReleasedByEitherPath(cancelFirst, this::sortHoldingPriorityQueue);
      assertReleasedByEitherPath(cancelFirst, this::hashJoinHoldingRightTable);
      assertReleasedByEitherPath(cancelFirst, this::nonEquiJoinHoldingRightTable);
      assertReleasedByEitherPath(cancelFirst, this::asofJoinHoldingRightTable);
      assertReleasedByEitherPath(cancelFirst, this::aggregateHoldingGroupByExecutor);
      assertReleasedByEitherPath(cancelFirst, this::repeatHoldingCurrentRows);
    }
  }

  private void assertReleasedByEitherPath(boolean cancelFirst, Supplier<MultiStageOperator> factory) {
    MultiStageOperator operator = factory.get();
    String name = operator.getClass().getSimpleName();
    assertTrue(operator.hasBufferedState(), name + " should be holding state before termination");

    terminate(operator, cancelFirst);
    assertFalse(operator.hasBufferedState(),
        name + " should have released after " + (cancelFirst ? "cancel" : "close"));

    // Terminating again, by the other path, must be safe and must leave it released.
    terminate(operator, !cancelFirst);
    assertFalse(operator.hasBufferedState(), name + " should stay released after a second termination");
  }

  private static void terminate(MultiStageOperator operator, boolean cancel) {
    if (cancel) {
      operator.cancel(ERROR);
    } else {
      operator.close();
    }
  }

  private MultiStageOperator sortHoldingRows() {
    resetMocks();
    givenInputProducesThenFails();
    SortOperator operator = sortOperator(_input, List.of());
    operator.nextBlock();
    return operator;
  }

  private MultiStageOperator sortHoldingPriorityQueue() {
    resetMocks();
    givenInputProducesThenFails();
    SortOperator operator = sortOperator(_input);
    operator.nextBlock();
    return operator;
  }

  private MultiStageOperator hashJoinHoldingRightTable() {
    resetMocks();
    givenRightBuiltThenLeftKeepsFlowing();
    HashJoinOperator operator = hashJoinOperator();
    operator.nextBlock();
    return operator;
  }

  private MultiStageOperator nonEquiJoinHoldingRightTable() {
    resetMocks();
    givenRightBuiltThenLeftKeepsFlowing();
    NonEquiJoinOperator operator = nonEquiJoinOperator();
    operator.nextBlock();
    return operator;
  }

  private MultiStageOperator asofJoinHoldingRightTable() {
    resetMocks();
    givenRightBuiltThenLeftKeepsFlowing();
    AsofJoinOperator operator = asofJoinOperator();
    operator.nextBlock();
    return operator;
  }

  private MultiStageOperator aggregateHoldingGroupByExecutor() {
    resetMocks();
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(SCHEMA, new Object[]{1, "a"}));
    return aggregateOperator(List.of(0));
  }

  private MultiStageOperator repeatHoldingCurrentRows() {
    resetMocks();
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(SCHEMA, new Object[]{1, "a"}));
    RepeatOperator operator = repeatOperator();
    operator.nextBlock();
    return operator;
  }

  private void resetMocks() {
    reset(_input, _leftInput, _rightInput);
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Fixtures
  // ---------------------------------------------------------------------------------------------------------------

  private void givenInputProducesThenFails() {
    when(_input.nextBlock()).thenReturn(OperatorTestUtil.block(SCHEMA, new Object[]{2, "b"}, new Object[]{1, "a"}))
        .thenReturn(ErrorMseBlock.fromException(ERROR));
  }

  /// Right side completes, left side keeps producing, so the join is mid-flight when we terminate it.
  private void givenRightBuiltThenLeftKeepsFlowing() {
    when(_rightInput.nextBlock()).thenReturn(OperatorTestUtil.block(SCHEMA, new Object[]{1, "a"}))
        .thenReturn(SuccessMseBlock.INSTANCE);
    when(_leftInput.nextBlock()).thenReturn(OperatorTestUtil.block(SCHEMA, new Object[]{0, "z"}));
  }

  /// Right side produces one block and then fails, so the right table is only partially built.
  private void givenRightSideFails() {
    when(_rightInput.nextBlock()).thenReturn(OperatorTestUtil.block(SCHEMA, new Object[]{1, "a"}))
        .thenReturn(ErrorMseBlock.fromException(ERROR));
  }

  private HashJoinOperator hashJoinOperator() {
    return new HashJoinOperator(OperatorTestUtil.getTracingContext(), _leftInput, SCHEMA, _rightInput,
        new JoinNode(-1, JOIN_RESULT_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), JoinRelType.FULL, List.of(0),
            List.of(0), List.of(), JoinNode.JoinStrategy.HASH));
  }

  private NonEquiJoinOperator nonEquiJoinOperator() {
    // Condition: left.int_col < right.int_col
    List<RexExpression> nonEquiConditions = List.of(
        new RexExpression.FunctionCall(BOOLEAN, SqlKind.LESS_THAN.name(),
            List.of(new RexExpression.InputRef(0), new RexExpression.InputRef(2))));
    return new NonEquiJoinOperator(OperatorTestUtil.getTracingContext(), _leftInput, SCHEMA, _rightInput,
        new JoinNode(-1, JOIN_RESULT_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), JoinRelType.FULL, List.of(),
            List.of(), nonEquiConditions, JoinNode.JoinStrategy.HASH));
  }

  private AsofJoinOperator asofJoinOperator() {
    // Joined on int_col, MATCH_CONDITION on string_col.
    RexExpression matchCondition = new RexExpression.FunctionCall(BOOLEAN, "LESS_THAN_OR_EQUAL",
        List.of(new RexExpression.InputRef(1), new RexExpression.InputRef(3)));
    return new AsofJoinOperator(OperatorTestUtil.getTracingContext(), _leftInput, SCHEMA, _rightInput,
        new JoinNode(-1, JOIN_RESULT_SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), JoinRelType.LEFT_ASOF, List.of(0),
            List.of(0), List.of(), JoinNode.JoinStrategy.ASOF, matchCondition));
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
        new RexExpression.FunctionCall(LONG, SqlKind.COUNT.name(), List.of());
    DataSchema resultSchema = groupKeys.isEmpty()
        ? new DataSchema(new String[]{"count"}, new ColumnDataType[]{LONG})
        : new DataSchema(new String[]{"int_col", "count"}, new ColumnDataType[]{INT, LONG});
    return new AggregateOperator(OperatorTestUtil.getTracingContext(), _input,
        new AggregateNode(-1, resultSchema, PlanNode.NodeHint.EMPTY, List.of(), List.of(countStar), List.of(-1),
            groupKeys, AggType.DIRECT, false, null, 0));
  }

  /// Two grouping sets, so the operator still holds the input block after emitting the first expansion.
  private RepeatOperator repeatOperator() {
    DataSchema resultSchema = new DataSchema(new String[]{"int_col", "string_col", "int_col_key", "$groupingId"},
        new ColumnDataType[]{INT, STRING, INT, INT});
    return new RepeatOperator(OperatorTestUtil.getTracingContext(), _input, new int[]{0},
        List.of(List.of(0), List.of()), resultSchema);
  }

  /// Minimal operator that records how the base class drove it.
  private static class RecordingOperator extends MultiStageOperator {
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordingOperator.class);

    private final List<MultiStageOperator> _children;
    private final StatMap<SortOperator.StatKey> _statMap = new StatMap<>(SortOperator.StatKey.class);
    private int _releaseCount;
    private boolean _childrenWereClosedFirst;

    RecordingOperator(MultiStageOperator... children) {
      this(OperatorTestUtil.getTracingContext(), children);
    }

    private RecordingOperator(OpChainExecutionContext context, MultiStageOperator... children) {
      super(context);
      _children = List.of(children);
    }

    @Override
    protected void releaseBuffers() {
      _releaseCount++;
      for (MultiStageOperator child : _children) {
        verify(child, atLeastOnce()).close();
      }
      _childrenWereClosedFirst = true;
    }

    @Override
    public List<MultiStageOperator> getChildOperators() {
      return _children;
    }

    @Override
    protected MseBlock getNextBlock() {
      return SuccessMseBlock.INSTANCE;
    }

    @Override
    public void registerExecution(long time, int numRows, long memoryUsedBytes, long gcTimeMs) {
    }

    @Override
    public Type getOperatorType() {
      return Type.SORT_OR_LIMIT;
    }

    @Override
    public StatMap<SortOperator.StatKey> copyStatMaps() {
      return new StatMap<>(_statMap);
    }

    @Override
    protected Logger logger() {
      return LOGGER;
    }

    @Override
    public String toExplainString() {
      return "RECORDING";
    }
  }
}
