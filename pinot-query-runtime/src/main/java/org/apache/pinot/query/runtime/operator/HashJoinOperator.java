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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.planner.plannode.AbstractPlanNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.operands.TransformOperand;
import org.apache.pinot.query.runtime.operator.operands.TransformOperandFactory;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.query.runtime.plan.StageMetadata;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner.JoinOverFlowMode;


/**
 * This basic {@code BroadcastJoinOperator} implement a basic broadcast join algorithm.
 * This algorithm assumes that the broadcast table has to fit in memory since we are not supporting any spilling.
 *
 * For left join, inner join, right join and full join,
 * <p>It takes the right table as the broadcast side and materialize a hash table. Then for each of the left table row,
 * it looks up for the corresponding row(s) from the hash table and create a joint row.
 *
 * <p>For each of the data block received from the left table, it will generate a joint data block.
 * We currently support left join, inner join, right join and full join.
 * The output is in the format of [left_row, right_row]
 */
// TODO: Move inequi out of hashjoin. (https://github.com/apache/pinot/issues/9728)
// TODO: Support memory size based resource limit.
public class HashJoinOperator extends MultiStageOperator {
  private static final String EXPLAIN_NAME = "HASH_JOIN";
  private static final int INITIAL_HEURISTIC_SIZE = 16;
  private static final int DEFAULT_MAX_ROWS_IN_JOIN = 1024 * 1024; // 2^20, around 1MM rows
  private static final JoinOverFlowMode DEFAULT_JOIN_OVERFLOW_MODE = JoinOverFlowMode.THROW;

  private static final Set<JoinRelType> SUPPORTED_JOIN_TYPES =
      ImmutableSet.of(JoinRelType.INNER, JoinRelType.LEFT, JoinRelType.RIGHT, JoinRelType.FULL, JoinRelType.SEMI,
          JoinRelType.ANTI);

  private final HashMap<Key, ArrayList<Object[]>> _broadcastRightTable;

  // Used to track matched right rows.
  // Only used for right join and full join to output non-matched right rows.
  // TODO: Replace hashset with rolling bit map.
  private final HashMap<Key, HashSet<Integer>> _matchedRightRows;

  private final MultiStageOperator _leftTableOperator;
  private final MultiStageOperator _rightTableOperator;
  private final JoinRelType _joinType;
  private final DataSchema _resultSchema;
  private final int _leftColumnSize;
  private final int _resultColumnSize;
  private final List<TransformOperand> _joinClauseEvaluators;
  private boolean _isHashTableBuilt;

  // Used by non-inner join.
  // Needed to indicate we have finished processing all results after returning last block.
  // TODO: Remove this special handling by fixing data block EOS abstraction or operator's invariant.
  private boolean _isTerminated;
  private TransferableBlock _upstreamErrorBlock;
  private final KeySelector<Object[], Object[]> _leftKeySelector;
  private final KeySelector<Object[], Object[]> _rightKeySelector;

  // Below are specific parameters to protect the hash table from growing too large.
  // Once the hash table reaches the limit, we will throw exception or break the right table build process.
  /**
   * Max rows allowed to build the right table hash collection.
   */
  private final int _maxRowsInHashTable;
  /**
   * Mode when join overflow happens, supported values: THROW or BREAK.
   *   THROW(default): Break right table build process, and throw exception, no JOIN with left table performed.
   *   BREAK: Break right table build process, continue to perform JOIN operation, results might be partial.
   */
  private final JoinOverFlowMode _joinOverflowMode;

  private int _currentRowsInHashTable = 0;
  private ProcessingException _resourceLimitExceededException = null;

  public HashJoinOperator(OpChainExecutionContext context, MultiStageOperator leftTableOperator,
      MultiStageOperator rightTableOperator, DataSchema leftSchema, JoinNode node) {
    super(context);
    Preconditions.checkState(SUPPORTED_JOIN_TYPES.contains(node.getJoinRelType()),
        "Join type: " + node.getJoinRelType() + " is not supported!");
    _joinType = node.getJoinRelType();
    _leftKeySelector = node.getJoinKeys().getLeftJoinKeySelector();
    _rightKeySelector = node.getJoinKeys().getRightJoinKeySelector();
    Preconditions.checkState(_leftKeySelector != null, "LeftKeySelector for join cannot be null");
    Preconditions.checkState(_rightKeySelector != null, "RightKeySelector for join cannot be null");
    _leftColumnSize = leftSchema.size();
    Preconditions.checkState(_leftColumnSize > 0, "leftColumnSize has to be greater than zero:" + _leftColumnSize);
    _resultSchema = node.getDataSchema();
    _resultColumnSize = _resultSchema.size();
    Preconditions.checkState(_resultColumnSize >= _leftColumnSize,
        "Result column size" + _leftColumnSize + " has to be greater than or equal to left column size:"
            + _leftColumnSize);
    _leftTableOperator = leftTableOperator;
    _rightTableOperator = rightTableOperator;
    _joinClauseEvaluators = new ArrayList<>(node.getJoinClauses().size());
    for (RexExpression joinClause : node.getJoinClauses()) {
      _joinClauseEvaluators.add(TransformOperandFactory.getTransformOperand(joinClause, _resultSchema));
    }
    _isHashTableBuilt = false;
    _broadcastRightTable = new HashMap<>();
    if (needUnmatchedRightRows()) {
      _matchedRightRows = new HashMap<>();
    } else {
      _matchedRightRows = null;
    }
    StageMetadata stageMetadata = context.getStageMetadata();
    Map<String, String> customProperties =
        stageMetadata != null ? stageMetadata.getCustomProperties() : Collections.emptyMap();
    _maxRowsInHashTable = getMaxRowInJoin(customProperties, node.getJoinHints());
    _joinOverflowMode = getJoinOverflowMode(customProperties, node.getJoinHints());
  }

  private int getMaxRowInJoin(Map<String, String> customProperties, @Nullable AbstractPlanNode.NodeHint nodeHint) {
    if (nodeHint != null) {
      Map<String, String> joinOptions = nodeHint._hintOptions.get(PinotHintOptions.JOIN_HINT_OPTIONS);
      if (joinOptions != null) {
        String maxRowsInJoinStr = joinOptions.get(PinotHintOptions.JoinHintOptions.MAX_ROWS_IN_JOIN);
        if (maxRowsInJoinStr != null) {
          return Integer.parseInt(maxRowsInJoinStr);
        }
      }
    }
    Integer maxRowsInJoin = QueryOptionsUtils.getMaxRowsInJoin(customProperties);
    return maxRowsInJoin != null ? maxRowsInJoin : DEFAULT_MAX_ROWS_IN_JOIN;
  }

  private JoinOverFlowMode getJoinOverflowMode(Map<String, String> customProperties,
      @Nullable AbstractPlanNode.NodeHint nodeHint) {
    if (nodeHint != null) {
      Map<String, String> joinOptions = nodeHint._hintOptions.get(PinotHintOptions.JOIN_HINT_OPTIONS);
      if (joinOptions != null) {
        String joinOverflowModeStr = joinOptions.get(PinotHintOptions.JoinHintOptions.JOIN_OVERFLOW_MODE);
        if (joinOverflowModeStr != null) {
          return JoinOverFlowMode.valueOf(joinOverflowModeStr);
        }
      }
    }
    JoinOverFlowMode joinOverflowMode = QueryOptionsUtils.getJoinOverflowMode(customProperties);
    return joinOverflowMode != null ? joinOverflowMode : DEFAULT_JOIN_OVERFLOW_MODE;
  }

  // TODO: Separate left and right table operator.
  @Override
  public List<MultiStageOperator> getChildOperators() {
    return ImmutableList.of(_leftTableOperator, _rightTableOperator);
  }

  @Nullable
  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    try {
      if (_isTerminated) {
        return setPartialResultExceptionToBlock(TransferableBlockUtils.getEndOfStreamTransferableBlock());
      }
      if (!_isHashTableBuilt) {
        // Build JOIN hash table
        buildBroadcastHashTable();
      }
      if (_upstreamErrorBlock != null) {
        return _upstreamErrorBlock;
      }
      TransferableBlock leftBlock = _leftTableOperator.nextBlock();
      // JOIN each left block with the right block.
      return setPartialResultExceptionToBlock(buildJoinedDataBlock(leftBlock));
    } catch (Exception e) {
      return TransferableBlockUtils.getErrorTransferableBlock(e);
    }
  }

  private void buildBroadcastHashTable()
      throws ProcessingException {
    TransferableBlock rightBlock = _rightTableOperator.nextBlock();
    while (!TransferableBlockUtils.isEndOfStream(rightBlock)) {
      List<Object[]> container = rightBlock.getContainer();
      // Row based overflow check.
      if (container.size() + _currentRowsInHashTable > _maxRowsInHashTable) {
        _resourceLimitExceededException =
            new ProcessingException(QueryException.SERVER_RESOURCE_LIMIT_EXCEEDED_ERROR_CODE);
        _resourceLimitExceededException.setMessage(
            "Cannot build in memory hash table for join operator, reach number of rows limit: " + _maxRowsInHashTable);
        if (_joinOverflowMode == JoinOverFlowMode.THROW) {
          throw _resourceLimitExceededException;
        } else {
          // Just fill up the buffer.
          int remainingRows = _maxRowsInHashTable - _currentRowsInHashTable;
          container = container.subList(0, remainingRows);
        }
      }
      // put all the rows into corresponding hash collections keyed by the key selector function.
      for (Object[] row : container) {
        ArrayList<Object[]> hashCollection =
            _broadcastRightTable.computeIfAbsent(new Key(_rightKeySelector.getKey(row)),
                k -> new ArrayList<>(INITIAL_HEURISTIC_SIZE));
        int size = hashCollection.size();
        if ((size & size - 1) == 0 && size < _maxRowsInHashTable && size < Integer.MAX_VALUE / 2) { // is power of 2
          hashCollection.ensureCapacity(Math.min(size << 1, _maxRowsInHashTable));
        }
        hashCollection.add(row);
      }
      _currentRowsInHashTable += container.size();
      if (_currentRowsInHashTable == _maxRowsInHashTable) {
        // Early terminate right table operator.
        _rightTableOperator.close();
        break;
      }
      rightBlock = _rightTableOperator.nextBlock();
    }
    if (rightBlock.isErrorBlock()) {
      _upstreamErrorBlock = rightBlock;
    } else {
      _isHashTableBuilt = true;
    }
  }

  private TransferableBlock buildJoinedDataBlock(TransferableBlock leftBlock) {
    if (leftBlock.isErrorBlock()) {
      _upstreamErrorBlock = leftBlock;
      return _upstreamErrorBlock;
    }
    if (leftBlock.isSuccessfulEndOfStreamBlock()) {
      if (!needUnmatchedRightRows()) {
        return leftBlock;
      }
      // TODO: Moved to a different function.
      // Return remaining non-matched rows for non-inner join.
      List<Object[]> returnRows = new ArrayList<>();
      for (Map.Entry<Key, ArrayList<Object[]>> entry : _broadcastRightTable.entrySet()) {
        Set<Integer> matchedIdx = _matchedRightRows.getOrDefault(entry.getKey(), new HashSet<>());
        List<Object[]> rightRows = entry.getValue();
        if (rightRows.size() == matchedIdx.size()) {
          continue;
        }
        for (int i = 0; i < rightRows.size(); i++) {
          if (!matchedIdx.contains(i)) {
            returnRows.add(joinRow(null, rightRows.get(i)));
          }
        }
      }
      _isTerminated = true;
      return new TransferableBlock(returnRows, _resultSchema, DataBlock.Type.ROW);
    }
    List<Object[]> rows;
    switch (_joinType) {
      case SEMI: {
        rows = buildJoinedDataBlockSemi(leftBlock);
        break;
      }
      case ANTI: {
        rows = buildJoinedDataBlockAnti(leftBlock);
        break;
      }
      default: { // INNER, LEFT, RIGHT, FULL
        rows = buildJoinedDataBlockDefault(leftBlock);
        break;
      }
    }
    // TODO: Rows can be empty here. Consider fetching another left block instead of returning empty block.
    return new TransferableBlock(rows, _resultSchema, DataBlock.Type.ROW);
  }

  private TransferableBlock setPartialResultExceptionToBlock(TransferableBlock block) {
    if (_resourceLimitExceededException != null) {
      block.addException(_resourceLimitExceededException);
    }
    return block;
  }

  private List<Object[]> buildJoinedDataBlockSemi(TransferableBlock leftBlock) {
    List<Object[]> container = leftBlock.getContainer();
    List<Object[]> rows = new ArrayList<>(container.size());

    for (Object[] leftRow : container) {
      Key key = new Key(_leftKeySelector.getKey(leftRow));
      // SEMI-JOIN only checks existence of the key
      if (_broadcastRightTable.containsKey(key)) {
        rows.add(joinRow(leftRow, null));
      }
    }

    return rows;
  }

  private List<Object[]> buildJoinedDataBlockDefault(TransferableBlock leftBlock) {
    List<Object[]> container = leftBlock.getContainer();
    ArrayList<Object[]> rows = new ArrayList<>(container.size());

    for (Object[] leftRow : container) {
      Key key = new Key(_leftKeySelector.getKey(leftRow));
      // NOTE: Empty key selector will always give same hash code.
      List<Object[]> matchedRightRows = _broadcastRightTable.getOrDefault(key, null);
      if (matchedRightRows == null) {
        if (needUnmatchedLeftRows()) {
          rows.add(joinRow(leftRow, null));
        }
        continue;
      }
      boolean hasMatchForLeftRow = false;
      rows.ensureCapacity(rows.size() + matchedRightRows.size());
      for (int i = 0; i < matchedRightRows.size(); i++) {
        Object[] rightRow = matchedRightRows.get(i);
        // TODO: Optimize this to avoid unnecessary object copy.
        Object[] resultRow = joinRow(leftRow, rightRow);
        if (_joinClauseEvaluators.isEmpty() || _joinClauseEvaluators.stream().allMatch(evaluator -> {
          Object result = evaluator.apply(resultRow);
          return result != null && (int) result == 1;
        })) {
          rows.add(resultRow);
          hasMatchForLeftRow = true;
          if (_matchedRightRows != null) {
            HashSet<Integer> matchedRows = _matchedRightRows.computeIfAbsent(key, k -> new HashSet<>());
            matchedRows.add(i);
          }
        }
      }
      if (!hasMatchForLeftRow && needUnmatchedLeftRows()) {
        rows.add(joinRow(leftRow, null));
      }
    }

    return rows;
  }

  private List<Object[]> buildJoinedDataBlockAnti(TransferableBlock leftBlock) {
    List<Object[]> container = leftBlock.getContainer();
    List<Object[]> rows = new ArrayList<>(container.size());

    for (Object[] leftRow : container) {
      Key key = new Key(_leftKeySelector.getKey(leftRow));
      // ANTI-JOIN only checks non-existence of the key
      if (!_broadcastRightTable.containsKey(key)) {
        rows.add(joinRow(leftRow, null));
      }
    }
    return rows;
  }

  private Object[] joinRow(@Nullable Object[] leftRow, @Nullable Object[] rightRow) {
    Object[] resultRow = new Object[_resultColumnSize];
    int idx = 0;
    if (leftRow != null) {
      for (Object obj : leftRow) {
        resultRow[idx++] = obj;
      }
    }
    // This is needed since left row can be null and we need to advance the idx to the beginning of right row.
    idx = _leftColumnSize;
    if (rightRow != null) {
      for (Object obj : rightRow) {
        resultRow[idx++] = obj;
      }
    }
    return resultRow;
  }

  private boolean needUnmatchedRightRows() {
    return _joinType == JoinRelType.RIGHT || _joinType == JoinRelType.FULL;
  }

  private boolean needUnmatchedLeftRows() {
    return _joinType == JoinRelType.LEFT || _joinType == JoinRelType.FULL;
  }
}
