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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions.JoinHintOptions;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.operator.join.JoinedRowView;
import org.apache.pinot.query.runtime.operator.operands.TransformOperand;
import org.apache.pinot.query.runtime.operator.operands.TransformOperandFactory;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner.JoinOverFlowMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code SortedMergeJoinOperator} implements a streaming sorted merge join.
 * <p>Unlike {@link HashJoinOperator}, it does not materialize the right side into an in-memory hash table. Instead it
 * assumes both the left and right inputs are already sorted in ascending order on their respective join keys and
 * advances two cursors in lock-step (a two-pointer merge). Only one block per side is held in memory at a time, plus a
 * small buffer for the run of right rows that share the current join key (needed to support one-to-many and
 * many-to-many matches).
 * <p>This makes memory usage proportional to the largest single-key run on the right side rather than the entire right
 * input, which is the key advantage for pre-sorted, pre-partitioned data layouts.
 * <p>Preconditions (enforced by the planner, assumed here):
 * <ul>
 *   <li>Both inputs are sorted ascending on the join keys (left keys for the left input, right keys for the right
 *       input).</li>
 *   <li>The join is an equi-join (non-empty join keys). Non-equi conditions are still applied as residual filters.</li>
 * </ul>
 * <p>Rows whose join key contains a {@code null} value never match (per SQL semantics) and are skipped; for LEFT joins
 * such left rows are emitted with {@code null} padding on the right.
 * <p>Only INNER and LEFT joins are currently supported.
 */
public class SortedMergeJoinOperator extends MultiStageOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SortedMergeJoinOperator.class);
  private static final String EXPLAIN_NAME = "SORTED_MERGE_JOIN";
  private static final String MERGE_LOOP_SCOPE = "SortedMergeJoinOperator#mergeLoop";
  private static final String EMIT_MATCHED_KEY_SCOPE = "SortedMergeJoinOperator#emitMatchedKey";
  private static final String BUFFER_RIGHT_RUN_SCOPE = "SortedMergeJoinOperator#bufferRightRun";
  private static final Set<JoinRelType> SUPPORTED_JOIN_TYPES = Set.of(JoinRelType.INNER, JoinRelType.LEFT);
  // Target number of output rows per emitted block. A single equi-key run may overshoot this; that is acceptable.
  private static final int TARGET_BLOCK_SIZE_ROWS = 1024;
  protected static final int DEFAULT_MAX_ROWS_IN_JOIN = 1024 * 1024; // 2^20, around 1MM rows
  protected static final JoinOverFlowMode DEFAULT_JOIN_OVERFLOW_MODE = JoinOverFlowMode.THROW;

  private final Cursor _leftCursor;
  private final Cursor _rightCursor;
  private final JoinRelType _joinType;
  private final boolean _needUnmatchedLeftRows;
  private final int[] _leftKeyIds;
  private final int[] _rightKeyIds;
  // Stored type of each join key column, resolved once so key comparison dispatches on a concrete type instead of a
  // raw Comparable (avoids cross-type ClassCastExceptions and boxing-heavy generic compares on the hot path).
  private final ColumnDataType[] _keyStoredTypes;
  private final DataSchema _resultSchema;
  private final int _leftColumnSize;
  private final int _resultColumnSize;
  private final List<TransformOperand> _nonEquiEvaluators;
  private final boolean _hasNonEquiConditions;
  private final int _maxRowsInJoin;
  private final JoinOverFlowMode _joinOverflowMode;
  private final StatMap<StatKey> _statMap = new StatMap<>(StatKey.class);
  // Reused buffer holding the run of right rows that share the current join key.
  private final List<Object[]> _rightRun = new ArrayList<>();

  private long _numEmittedRows;
  // Monotonically increasing count of input rows examined. Used as the tick for
  // checkTerminationAndSampleUsagePeriodically, which only samples when the counter is a multiple of 8192. The number
  // of rows accumulated in the current output block cannot serve as that tick: it is capped at TARGET_BLOCK_SIZE_ROWS,
  // so it would sample on every call while the block is empty and then never again once it is not.
  private int _numRowsProcessed;
  @Nullable
  private MseBlock.Eos _eos;

  public SortedMergeJoinOperator(OpChainExecutionContext context, MultiStageOperator leftInput, DataSchema leftSchema,
      MultiStageOperator rightInput, JoinNode node) {
    super(context);
    _joinType = node.getJoinType();
    Preconditions.checkState(SUPPORTED_JOIN_TYPES.contains(_joinType),
        "Join type: %s is not supported for sorted merge join", _joinType);
    List<Integer> leftKeys = node.getLeftKeys();
    List<Integer> rightKeys = node.getRightKeys();
    Preconditions.checkState(!leftKeys.isEmpty(), "Sorted merge join operator requires join keys");
    Preconditions.checkState(leftKeys.size() == rightKeys.size(),
        "Left and right join keys must have the same size, got: %s and %s", leftKeys.size(), rightKeys.size());
    _leftCursor = new Cursor(leftInput);
    _rightCursor = new Cursor(rightInput);
    _needUnmatchedLeftRows = (_joinType == JoinRelType.LEFT);
    _leftKeyIds = toIntArray(leftKeys);
    _rightKeyIds = toIntArray(rightKeys);
    _keyStoredTypes = new ColumnDataType[_leftKeyIds.length];
    for (int i = 0; i < _leftKeyIds.length; i++) {
      // Left and right join key columns share a common type after planner type coercion, so the left column's stored
      // type is representative of both sides.
      _keyStoredTypes[i] = leftSchema.getColumnDataType(_leftKeyIds[i]).getStoredType();
    }
    _leftColumnSize = leftSchema.size();
    _resultSchema = node.getDataSchema();
    _resultColumnSize = _resultSchema.size();
    List<RexExpression> nonEquiConditions = node.getNonEquiConditions();
    _nonEquiEvaluators = new ArrayList<>(nonEquiConditions.size());
    for (RexExpression nonEquiCondition : nonEquiConditions) {
      _nonEquiEvaluators.add(TransformOperandFactory.getTransformOperand(nonEquiCondition, _resultSchema));
    }
    _hasNonEquiConditions = !_nonEquiEvaluators.isEmpty();
    Map<String, String> metadata = context.getOpChainMetadata();
    PlanNode.NodeHint nodeHint = node.getNodeHint();
    _maxRowsInJoin = getMaxRowsInJoin(metadata, nodeHint);
    _joinOverflowMode = getJoinOverflowMode(metadata, nodeHint);
  }

  private static int[] toIntArray(List<Integer> list) {
    int[] array = new int[list.size()];
    for (int i = 0; i < list.size(); i++) {
      array[i] = list.get(i);
    }
    return array;
  }

  @Override
  public void registerExecution(long time, int numRows, long memoryUsedBytes, long gcTimeMs) {
    _statMap.merge(StatKey.EXECUTION_TIME_MS, time);
    _statMap.merge(StatKey.EMITTED_ROWS, numRows);
    _statMap.merge(StatKey.ALLOCATED_MEMORY_BYTES, memoryUsedBytes);
    _statMap.merge(StatKey.GC_TIME_MS, gcTimeMs);
  }

  @Override
  public Type getOperatorType() {
    return Type.SORTED_MERGE_JOIN;
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return List.of(_leftCursor.getInput(), _rightCursor.getInput());
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  public StatMap<StatKey> copyStatMaps() {
    return new StatMap<>(_statMap);
  }

  @Override
  protected MseBlock getNextBlock() {
    if (_eos != null) {
      return _eos;
    }
    // If a downstream operator (e.g. a LIMIT) has signalled early termination, stop producing promptly.
    // Returning EOS here lets the consumer's drain loop exit instead of pulling the full cross-product of the
    // already-buffered inputs.
    if (_isEarlyTerminated) {
      _eos = SuccessMseBlock.INSTANCE;
      return _eos;
    }
    List<Object[]> rows = new ArrayList<>(Math.min(TARGET_BLOCK_SIZE_ROWS, 64));
    while (rows.size() < TARGET_BLOCK_SIZE_ROWS) {
      boolean leftHasRow = _leftCursor.advanceToNextRow();
      if (_leftCursor.isError()) {
        _eos = _leftCursor.getEos();
        return _eos;
      }
      boolean rightHasRow = _rightCursor.advanceToNextRow();
      if (_rightCursor.isError()) {
        _eos = _rightCursor.getEos();
        return _eos;
      }

      if (!leftHasRow) {
        // Left input is exhausted. INNER and LEFT joins emit nothing for unmatched right rows.
        _eos = SuccessMseBlock.INSTANCE;
        break;
      }
      if (!rightHasRow) {
        // Right input is exhausted. Remaining left rows are unmatched.
        if (_needUnmatchedLeftRows) {
          Object[] leftRow = _leftCursor.peek();
          if (!addRow(rows, joinRow(leftRow, null))) {
            break;
          }
          _leftCursor.consume();
        } else {
          _eos = SuccessMseBlock.INSTANCE;
          break;
        }
        continue;
      }

      Object[] leftRow = _leftCursor.peek();
      Object[] rightRow = _rightCursor.peek();
      // Null join keys never match per SQL semantics.
      if (hasNullKey(leftRow, _leftKeyIds)) {
        if (_needUnmatchedLeftRows && !addRow(rows, joinRow(leftRow, null))) {
          break;
        }
        _leftCursor.consume();
        continue;
      }
      if (hasNullKey(rightRow, _rightKeyIds)) {
        _rightCursor.consume();
        continue;
      }

      int cmp = compareKeys(leftRow, _leftKeyIds, rightRow, _rightKeyIds);
      if (cmp < 0) {
        if (_needUnmatchedLeftRows && !addRow(rows, joinRow(leftRow, null))) {
          break;
        }
        _leftCursor.consume();
      } else if (cmp > 0) {
        _rightCursor.consume();
      } else {
        if (_hasNonEquiConditions) {
          emitMatchedKeyWithFilter(rows);
        } else {
          emitMatchedKeyEquiOnly(rows);
        }
        if (_eos != null) {
          break;
        }
      }
      checkTerminationAndSampleUsagePeriodically(++_numRowsProcessed, MERGE_LOOP_SCOPE);
    }

    if (!rows.isEmpty()) {
      return new RowHeapDataBlock(rows, _resultSchema);
    }
    // No rows produced in this pass: the loop only exits with empty rows once an EOS has been reached.
    if (_eos == null) {
      _eos = SuccessMseBlock.INSTANCE;
    }
    return _eos;
  }

  /**
   * Buffers the run of right rows that share the current join key (reusing {@link #_rightRun} across keys).
   * <p>This buffer is the operator's peak memory: a single hot join key with a very large right-side run would
   * otherwise grow it without bound before a single output row is produced. It is therefore capped by the same
   * {@code maxRowsInJoin} budget the emit path uses, honouring {@code joinOverflowMode}: THROW aborts the query,
   * BREAK truncates the run and early-terminates. Note the budget is applied here to <em>buffered</em> right rows and
   * in {@link #addRow} to <em>emitted</em> rows, matching how {@code BaseJoinOperator} treats its own right table.
   * <p>Returns {@code false} if the operator terminated while buffering.
   */
  private boolean bufferRightRun(Object[] anchor) {
    List<Object[]> rightRun = _rightRun;
    rightRun.clear();
    while (_rightCursor.advanceToNextRow()) {
      Object[] rightRow = _rightCursor.peek();
      if (compareKeys(anchor, _leftKeyIds, rightRow, _rightKeyIds) != 0) {
        break;
      }
      if (rightRun.size() >= _maxRowsInJoin) {
        if (_joinOverflowMode == JoinOverFlowMode.THROW) {
          _statMap.merge(StatKey.MAX_ROWS_IN_JOIN, (long) rightRun.size());
          throwForJoinRowLimitExceeded(
              "Cannot process sorted merge join, reached number of rows limit while buffering the right rows for a "
                  + "single join key: " + _maxRowsInJoin);
        }
        // BREAK mode: stop buffering and early-terminate both inputs (propagates to children).
        _statMap.merge(StatKey.MAX_ROWS_IN_JOIN_REACHED, true);
        _statMap.merge(StatKey.MAX_ROWS_IN_JOIN, (long) rightRun.size());
        earlyTerminate();
        _eos = SuccessMseBlock.INSTANCE;
        return false;
      }
      rightRun.add(rightRow);
      _rightCursor.consume();
      checkTerminationAndSampleUsagePeriodically(++_numRowsProcessed, BUFFER_RIGHT_RUN_SCOPE);
    }
    return true;
  }

  /**
   * Equi-only fast path: no residual filter, emit joined rows directly without lazy view allocation.
   */
  private void emitMatchedKeyEquiOnly(List<Object[]> rows) {
    Object[] anchor = _leftCursor.peek();
    if (!bufferRightRun(anchor)) {
      return;
    }
    List<Object[]> rightRun = _rightRun;

    while (_leftCursor.advanceToNextRow()) {
      Object[] leftRow = _leftCursor.peek();
      if (compareKeys(anchor, _leftKeyIds, leftRow, _leftKeyIds) != 0) {
        break;
      }
      for (Object[] rightRow : rightRun) {
        if (!addRow(rows, joinRow(leftRow, rightRow))) {
          return;
        }
      }
      _leftCursor.consume();
      checkTerminationAndSampleUsagePeriodically(++_numRowsProcessed, EMIT_MATCHED_KEY_SCOPE);
    }
  }

  /**
   * Non-equi path: evaluate residual predicates via lazy view before materializing the joined row.
   */
  private void emitMatchedKeyWithFilter(List<Object[]> rows) {
    Object[] anchor = _leftCursor.peek();
    if (!bufferRightRun(anchor)) {
      return;
    }
    List<Object[]> rightRun = _rightRun;

    while (_leftCursor.advanceToNextRow()) {
      Object[] leftRow = _leftCursor.peek();
      if (compareKeys(anchor, _leftKeyIds, leftRow, _leftKeyIds) != 0) {
        break;
      }
      boolean matched = false;
      for (Object[] rightRow : rightRun) {
        List<Object> resultRowView = joinRowView(leftRow, rightRow);
        if (matchNonEquiConditions(resultRowView)) {
          if (!addRow(rows, resultRowView.toArray())) {
            return;
          }
          matched = true;
        }
      }
      if (!matched && _needUnmatchedLeftRows && !addRow(rows, joinRow(leftRow, null))) {
        return;
      }
      _leftCursor.consume();
      checkTerminationAndSampleUsagePeriodically(++_numRowsProcessed, EMIT_MATCHED_KEY_SCOPE);
    }
  }

  /**
   * Appends a joined row to the current output block, applying the {@code maxRowsInJoin} budget. Returns {@code false}
   * if the row was refused because the budget was exhausted in BREAK mode, in which case the operator has already been
   * terminated and callers must stop producing; THROW mode raises instead of returning.
   */
  private boolean addRow(List<Object[]> rows, Object[] row) {
    if (_numEmittedRows >= _maxRowsInJoin) {
      if (_joinOverflowMode == JoinOverFlowMode.THROW) {
        _statMap.merge(StatKey.MAX_ROWS_IN_JOIN, _numEmittedRows);
        throwForJoinRowLimitExceeded(
            "Cannot process sorted merge join, reached number of rows limit: " + _maxRowsInJoin);
      } else {
        // BREAK mode: stop emitting and early-terminate both inputs (propagates to children).
        _statMap.merge(StatKey.MAX_ROWS_IN_JOIN_REACHED, true);
        _statMap.merge(StatKey.MAX_ROWS_IN_JOIN, _numEmittedRows);
        earlyTerminate();
        _eos = SuccessMseBlock.INSTANCE;
        return false;
      }
    }
    rows.add(row);
    _numEmittedRows++;
    return true;
  }

  private static boolean hasNullKey(Object[] row, int[] keyIds) {
    for (int keyId : keyIds) {
      if (row[keyId] == null) {
        return true;
      }
    }
    return false;
  }

  private int compareKeys(Object[] row1, int[] keyIds1, Object[] row2, int[] keyIds2) {
    for (int i = 0; i < keyIds1.length; i++) {
      int result = compareValue(row1[keyIds1[i]], row2[keyIds2[i]], _keyStoredTypes[i]);
      if (result != 0) {
        return result;
      }
    }
    return 0;
  }

  /**
   * Compares two join key values using nulls-last ordering, matching the {@code NullDirection.LAST} collation that
   * {@code PinotJoinExchangeNodeInsertRule} requests for both join inputs. Null keys never match (handled by the
   * caller), but they must still compare consistently: a null-key row sorts after every non-null key, so a run scan
   * that reaches one terminates the run rather than dereferencing it.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static int compareValue(@Nullable Object value1, @Nullable Object value2, ColumnDataType storedType) {
    if (value1 == null) {
      return value2 == null ? 0 : 1;
    }
    if (value2 == null) {
      return -1;
    }
    switch (storedType) {
      case INT:
        return Integer.compare((Integer) value1, (Integer) value2);
      case LONG:
        return Long.compare((Long) value1, (Long) value2);
      case FLOAT:
        return Float.compare((Float) value1, (Float) value2);
      case DOUBLE:
        return Double.compare((Double) value1, (Double) value2);
      default:
        // STRING, BIG_DECIMAL, BYTES (ByteArray), etc. are all Comparable in their stored representation.
        return ((Comparable) value1).compareTo(value2);
    }
  }

  private Object[] joinRow(Object[] leftRow, @Nullable Object[] rightRow) {
    Object[] resultRow = new Object[_resultColumnSize];
    System.arraycopy(leftRow, 0, resultRow, 0, leftRow.length);
    if (rightRow != null) {
      System.arraycopy(rightRow, 0, resultRow, _leftColumnSize, rightRow.length);
    }
    return resultRow;
  }

  private List<Object> joinRowView(@Nullable Object[] leftRow, @Nullable Object[] rightRow) {
    return JoinedRowView.of(leftRow, rightRow, _resultColumnSize, _leftColumnSize);
  }

  private boolean matchNonEquiConditions(List<Object> row) {
    if (_nonEquiEvaluators.isEmpty()) {
      return true;
    }
    for (TransformOperand evaluator : _nonEquiEvaluators) {
      if (!BooleanUtils.isTrueInternalValue(evaluator.apply(row))) {
        return false;
      }
    }
    return true;
  }

  private boolean needUnmatchedLeftRows() {
    return _joinType == JoinRelType.LEFT;
  }

  private static int getMaxRowsInJoin(Map<String, String> opChainMetadata, @Nullable PlanNode.NodeHint nodeHint) {
    if (nodeHint != null) {
      Map<String, String> joinOptions = nodeHint.getHintOptions().get(PinotHintOptions.JOIN_HINT_OPTIONS);
      if (joinOptions != null) {
        String maxRowsInJoinStr = joinOptions.get(JoinHintOptions.MAX_ROWS_IN_JOIN);
        if (maxRowsInJoinStr != null) {
          return Integer.parseInt(maxRowsInJoinStr);
        }
      }
    }
    Integer maxRowsInJoin = QueryOptionsUtils.getMaxRowsInJoin(opChainMetadata);
    return maxRowsInJoin != null ? maxRowsInJoin : DEFAULT_MAX_ROWS_IN_JOIN;
  }

  private static JoinOverFlowMode getJoinOverflowMode(Map<String, String> contextMetadata,
      @Nullable PlanNode.NodeHint nodeHint) {
    if (nodeHint != null) {
      Map<String, String> joinOptions = nodeHint.getHintOptions().get(PinotHintOptions.JOIN_HINT_OPTIONS);
      if (joinOptions != null) {
        String joinOverflowModeStr = joinOptions.get(JoinHintOptions.JOIN_OVERFLOW_MODE);
        if (joinOverflowModeStr != null) {
          return JoinOverFlowMode.valueOf(joinOverflowModeStr);
        }
      }
    }
    JoinOverFlowMode joinOverflowMode = QueryOptionsUtils.getJoinOverflowMode(contextMetadata);
    return joinOverflowMode != null ? joinOverflowMode : DEFAULT_JOIN_OVERFLOW_MODE;
  }

  private static void throwForJoinRowLimitExceeded(String reason) {
    throw QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED.asException(
        reason
            + ".\nConsider increasing the limit for the maximum number of rows in a join either via:\n"
            + "  - The query option '" + QueryOptionKey.MAX_ROWS_IN_JOIN + "'\n"
            + "  - The hint '" + JoinHintOptions.MAX_ROWS_IN_JOIN + "' in the '" + PinotHintOptions.JOIN_HINT_OPTIONS
            + "'\n"
            + "Alternatively, if partial results are acceptable, the join overflow mode can be set to '"
            + JoinOverFlowMode.BREAK.name() + "' either via:\n"
            + "  - The query option '" + QueryOptionKey.JOIN_OVERFLOW_MODE + "'\n"
            + "  - The hint '" + JoinHintOptions.JOIN_OVERFLOW_MODE + "' in the '"
            + PinotHintOptions.JOIN_HINT_OPTIONS + "'\n");
  }

  /**
   * A lazy, block-at-a-time cursor over a {@link MultiStageOperator} input. Only one data block is held in memory at a
   * time. Use {@link #advanceToNextRow()} to ensure a current row is available, {@link #peek()} to read it without
   * consuming, and {@link #consume()} to move to the next row.
   */
  private final class Cursor {
    private final MultiStageOperator _input;
    private List<Object[]> _rows = List.of();
    private int _index;
    @Nullable
    private MseBlock.Eos _eosBlock;

    Cursor(MultiStageOperator input) {
      _input = input;
    }

    MultiStageOperator getInput() {
      return _input;
    }

    /**
     * Ensures a current row is available, fetching subsequent blocks as needed. Returns {@code false} when the input
     * is exhausted (an EOS block was encountered), in which case {@link #getEos()} is populated.
     */
    boolean advanceToNextRow() {
      while (_index >= _rows.size()) {
        if (_eosBlock != null) {
          return false;
        }
        MseBlock block = _input.nextBlock();
        if (block.isData()) {
          _rows = ((MseBlock.Data) block).asRowHeap().getRows();
          _index = 0;
        } else {
          _eosBlock = (MseBlock.Eos) block;
          _rows = List.of();
          _index = 0;
          return false;
        }
      }
      return true;
    }

    Object[] peek() {
      return _rows.get(_index);
    }

    void consume() {
      _index++;
    }

    boolean isError() {
      return _eosBlock != null && _eosBlock.isError();
    }

    @Nullable
    MseBlock.Eos getEos() {
      return _eosBlock;
    }
  }

  public enum StatKey implements StatMap.Key {
    EXECUTION_TIME_MS(StatMap.Type.LONG) {
      @Override
      public boolean includeDefaultInJson() {
        return true;
      }
    },
    EMITTED_ROWS(StatMap.Type.LONG) {
      @Override
      public boolean includeDefaultInJson() {
        return true;
      }
    },
    MAX_ROWS_IN_JOIN_REACHED(StatMap.Type.BOOLEAN),
    /**
     * The max number of joined rows emitted by this operator.
     */
    MAX_ROWS_IN_JOIN(StatMap.Type.LONG) {
      @Override
      public long merge(long value1, long value2) {
        return Math.max(value1, value2);
      }
    },
    /**
     * Allocated memory in bytes for this operator or its children in the same stage.
     */
    ALLOCATED_MEMORY_BYTES(StatMap.Type.LONG),
    /**
     * Time spent on GC while this operator or its children in the same stage were running.
     */
    GC_TIME_MS(StatMap.Type.LONG);

    private final StatMap.Type _type;

    StatKey(StatMap.Type type) {
      _type = type;
    }

    @Override
    public StatMap.Type getType() {
      return _type;
    }
  }
}
