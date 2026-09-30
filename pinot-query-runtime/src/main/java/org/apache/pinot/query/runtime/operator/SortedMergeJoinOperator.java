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
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.operator.join.JoinedRowView;
import org.apache.pinot.query.runtime.operator.operands.TransformOperand;
import org.apache.pinot.query.runtime.operator.operands.TransformOperandFactory;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner.JoinOverFlowMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// The `SortedMergeJoinOperator` implements a streaming sorted merge join.
///
/// Unlike [HashJoinOperator], it does not materialize the right side into an in-memory hash table. Instead it
/// assumes both the left and right inputs are already sorted in ascending order on their respective join keys and
/// advances two cursors in lock-step (a two-pointer merge). Only one block per side is held in memory at a time, plus a
/// small buffer for the run of right rows that share the current join key (needed to support one-to-many and
/// many-to-many matches). Duplicate matches are emitted in bounded blocks, so a downstream LIMIT can stop within
/// a single join key. This operator is driven by one consumer thread and is not thread-safe.
///
/// This makes memory usage proportional to the largest single-key run on the right side rather than the entire right
/// input, which is the key advantage for pre-sorted, pre-partitioned data layouts.
///
/// Preconditions (enforced by the planner, assumed here):
///   - Both inputs are sorted ascending on the join keys (left keys for the left input, right keys for the right
///       input).
///   - The join is an equi-join (non-empty join keys). Non-equi conditions are still applied as residual filters.
///
/// Rows whose join key contains a `null` value never match (per SQL semantics) and are skipped; for LEFT joins
/// such left rows are emitted with `null` padding on the right.
///
/// Only INNER and LEFT joins are currently supported.
public class SortedMergeJoinOperator extends MultiStageOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SortedMergeJoinOperator.class);
  private static final String EXPLAIN_NAME = "SORTED_MERGE_JOIN";
  private static final String MERGE_LOOP_SCOPE = "SortedMergeJoinOperator#mergeLoop";
  private static final String EMIT_MATCHED_KEY_SCOPE = "SortedMergeJoinOperator#emitMatchedKey";
  private static final String BUFFER_RIGHT_RUN_SCOPE = "SortedMergeJoinOperator#bufferRightRun";
  private static final Set<JoinRelType> SUPPORTED_JOIN_TYPES = Set.of(JoinRelType.INNER, JoinRelType.LEFT);
  // A duplicate-key cross product resumes across output blocks instead of materializing the complete run.
  private static final int TARGET_BLOCK_SIZE_ROWS = 1024;

  private final Cursor _leftCursor;
  private final Cursor _rightCursor;
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
  private List<Object[]> _rightRun = new ArrayList<>();

  @Nullable
  private Object[] _rightRunAnchor;
  private int _rightRunIndex;
  private boolean _leftRowMatched;
  // Monotonically increasing count of examined input rows and candidate pairs. Used as the tick for
  // checkTerminationAndSampleUsagePeriodically, which only samples when the counter is a multiple of 8192. The number
  // of rows accumulated in the current output block cannot serve as that tick: it is capped at TARGET_BLOCK_SIZE_ROWS,
  // so it would sample on every call while the block is empty and then never again once it is not.
  private int _numRowsProcessed;
  @Nullable
  private MseBlock.Eos _eos;

  public SortedMergeJoinOperator(OpChainExecutionContext context, MultiStageOperator leftInput, DataSchema leftSchema,
      MultiStageOperator rightInput, JoinNode node) {
    super(context);
    JoinRelType joinType = node.getJoinType();
    Preconditions.checkState(SUPPORTED_JOIN_TYPES.contains(joinType),
        "Join type: %s is not supported for sorted merge join", joinType);
    List<Integer> leftKeys = node.getLeftKeys();
    List<Integer> rightKeys = node.getRightKeys();
    Preconditions.checkState(!leftKeys.isEmpty(), "Sorted merge join operator requires join keys");
    Preconditions.checkState(leftKeys.size() == rightKeys.size(),
        "Left and right join keys must have the same size, got: %s and %s", leftKeys.size(), rightKeys.size());
    _leftCursor = new Cursor(leftInput);
    _rightCursor = new Cursor(rightInput);
    _needUnmatchedLeftRows = joinType == JoinRelType.LEFT;
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
    _maxRowsInJoin = BaseJoinOperator.getMaxRowsInJoin(metadata, node.getNodeHint());
    Preconditions.checkArgument(_maxRowsInJoin > 0, "maxRowsInJoin must be positive");
    _joinOverflowMode = BaseJoinOperator.getJoinOverflowMode(metadata, node.getNodeHint());
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

  /// INNER and LEFT merge joins preserve the left key order, including after residual filtering and null padding.
  @Override
  public boolean isSortedOn(List<RelFieldCollation> collations) {
    if (collations.isEmpty() || collations.size() > _leftKeyIds.length) {
      return false;
    }
    for (int i = 0; i < collations.size(); i++) {
      RelFieldCollation collation = collations.get(i);
      if (collation.getFieldIndex() != _leftKeyIds[i]
          || collation.getDirection() != RelFieldCollation.Direction.ASCENDING
          || collation.nullDirection != RelFieldCollation.NullDirection.LAST) {
        return false;
      }
    }
    return true;
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
  protected void releaseBuffers() {
    _rightRun = new ArrayList<>();
    _rightRunAnchor = null;
    _leftCursor.releaseRows();
    _rightCursor.releaseRows();
  }

  @Override
  protected boolean hasBufferedState() {
    return !_rightRun.isEmpty() || _rightRunAnchor != null
        || _leftCursor.hasBufferedRows() || _rightCursor.hasBufferedRows();
  }

  @Override
  protected MseBlock getNextBlock() {
    try {
      MseBlock block = getNextBlockInternal();
      if (_eos != null) {
        releaseBuffers();
      }
      return block;
    } catch (RuntimeException e) {
      releaseBuffers();
      throw e;
    }
  }

  private MseBlock getNextBlockInternal() {
    if (_eos != null) {
      return _eos;
    }
    if (_isEarlyTerminated) {
      _eos = SuccessMseBlock.INSTANCE;
      return _eos;
    }
    int blockSize = Math.min(TARGET_BLOCK_SIZE_ROWS, _maxRowsInJoin);
    List<Object[]> rows = new ArrayList<>(Math.min(blockSize, 64));
    while (rows.size() < blockSize) {
      boolean leftHasRow = _leftCursor.advanceToNextRow();
      if (_leftCursor.isError()) {
        _eos = _leftCursor.getEos();
        return _eos;
      }
      if (!leftHasRow) {
        _eos = SuccessMseBlock.INSTANCE;
        break;
      }
      Object[] leftRow = _leftCursor.peek();
      if (_rightRunAnchor != null) {
        if (compareKeys(_rightRunAnchor, _leftKeyIds, leftRow, _leftKeyIds) == 0) {
          emitMatchedKey(rows, leftRow, blockSize);
          continue;
        }
        _rightRun.clear();
        _rightRunAnchor = null;
      }
      boolean rightHasRow = _rightCursor.advanceToNextRow();
      if (_rightCursor.isError()) {
        _eos = _rightCursor.getEos();
        return _eos;
      }
      if (!rightHasRow) {
        if (_needUnmatchedLeftRows) {
          rows.add(joinRow(leftRow, null));
          _leftCursor.consume();
          checkTerminationAndSampleUsagePeriodically(++_numRowsProcessed, MERGE_LOOP_SCOPE);
          continue;
        }
        _eos = SuccessMseBlock.INSTANCE;
        break;
      }
      Object[] rightRow = _rightCursor.peek();
      // Null join keys never match per SQL semantics.
      if (hasNullKey(leftRow, _leftKeyIds)) {
        if (_needUnmatchedLeftRows) {
          rows.add(joinRow(leftRow, null));
        }
        _leftCursor.consume();
      } else if (hasNullKey(rightRow, _rightKeyIds)) {
        _rightCursor.consume();
      } else {
        int cmp = compareKeys(leftRow, _leftKeyIds, rightRow, _rightKeyIds);
        if (cmp < 0) {
          if (_needUnmatchedLeftRows) {
            rows.add(joinRow(leftRow, null));
          }
          _leftCursor.consume();
        } else if (cmp > 0) {
          _rightCursor.consume();
        } else if (!bufferRightRun(leftRow)) {
          break;
        }
      }
      checkTerminationAndSampleUsagePeriodically(++_numRowsProcessed, MERGE_LOOP_SCOPE);
    }
    if (!rows.isEmpty()) {
      return new RowHeapDataBlock(rows, _resultSchema);
    }
    return _eos;
  }

  /// Buffers the right rows for one join key. Unlike a hash join, the resource limit applies to this buffered run,
  /// rather than the entire right input. Output is separately emitted in blocks bounded by `maxRowsInJoin`, so the
  /// total streamed result may exceed that budget without retaining it in memory.
  private boolean bufferRightRun(Object[] anchor) {
    while (_rightCursor.advanceToNextRow()) {
      Object[] rightRow = _rightCursor.peek();
      if (compareKeys(anchor, _leftKeyIds, rightRow, _rightKeyIds) != 0) {
        break;
      }
      if (_rightRun.size() >= _maxRowsInJoin) {
        _statMap.merge(StatKey.MAX_ROWS_IN_JOIN, (long) _rightRun.size());
        if (_joinOverflowMode == JoinOverFlowMode.THROW) {
          BaseJoinOperator.throwForJoinRowLimitExceeded(
              "Cannot process sorted merge join, reached number of rows limit while buffering the right rows for a "
                  + "single join key: " + _maxRowsInJoin);
        }
        _statMap.merge(StatKey.MAX_ROWS_IN_JOIN_REACHED, true);
        earlyTerminate();
        _eos = SuccessMseBlock.INSTANCE;
        return false;
      }
      _rightRun.add(rightRow);
      _rightCursor.consume();
      checkTerminationAndSampleUsagePeriodically(++_numRowsProcessed, BUFFER_RIGHT_RUN_SCOPE);
    }
    if (_rightCursor.isError()) {
      _eos = _rightCursor.getEos();
      return false;
    }
    _statMap.merge(StatKey.MAX_ROWS_IN_JOIN, (long) _rightRun.size());
    _rightRunAnchor = anchor;
    return true;
  }

  /// Resumes the duplicate-key cross product for the current left row. The match flag survives output block
  /// boundaries, so a LEFT join only emits null padding after every residual predicate candidate was rejected.
  private void emitMatchedKey(List<Object[]> rows, Object[] leftRow, int blockSize) {
    while (_rightRunIndex < _rightRun.size() && rows.size() < blockSize) {
      Object[] rightRow = _rightRun.get(_rightRunIndex++);
      checkTerminationAndSampleUsagePeriodically(++_numRowsProcessed, EMIT_MATCHED_KEY_SCOPE);
      if (!_hasNonEquiConditions || matchNonEquiConditions(
          JoinedRowView.of(leftRow, rightRow, _resultColumnSize, _leftColumnSize))) {
        rows.add(joinRow(leftRow, rightRow));
        _leftRowMatched = true;
      }
    }
    if (_rightRunIndex == _rightRun.size()) {
      if (!_leftRowMatched && _needUnmatchedLeftRows) {
        rows.add(joinRow(leftRow, null));
      }
      _leftCursor.consume();
      _rightRunIndex = 0;
      _leftRowMatched = false;
    }
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

  /// Compares two join key values using nulls-last ordering, matching the `NullDirection.LAST` collation that
  /// `PinotJoinExchangeNodeInsertRule` requests for both join inputs. Null keys never match (handled by the
  /// caller), but they must still compare consistently: a null-key row sorts after every non-null key, so a run scan
  /// that reaches one terminates the run rather than dereferencing it.
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

  private boolean matchNonEquiConditions(List<Object> row) {
    for (TransformOperand evaluator : _nonEquiEvaluators) {
      if (!BooleanUtils.isTrueInternalValue(evaluator.apply(row))) {
        return false;
      }
    }
    return true;
  }

  /// A lazy, block-at-a-time cursor over a [MultiStageOperator] input. Only one data block is held in memory at a
  /// time. Use [#advanceToNextRow()] to ensure a current row is available, [#peek()] to read it without
  /// consuming, and [#consume()] to move to the next row.
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

    /// Ensures a current row is available, fetching subsequent blocks as needed. Returns `false` when the input
    /// is exhausted (an EOS block was encountered), in which case [#getEos()] is populated.
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

    void releaseRows() {
      _rows = List.of();
      _index = 0;
    }

    boolean hasBufferedRows() {
      return !_rows.isEmpty();
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
    /// The maximum number of right rows buffered for a single join key.
    MAX_ROWS_IN_JOIN(StatMap.Type.LONG) {
      @Override
      public long merge(long value1, long value2) {
        return Math.max(value1, value2);
      }
    },
    /// Allocated memory in bytes for this operator or its children in the same stage.
    ALLOCATED_MEMORY_BYTES(StatMap.Type.LONG),
    /// Time spent on GC while this operator or its children in the same stage were running.
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
