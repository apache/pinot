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
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.planner.partitioning.KeySelectorFactory;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.operands.TransformOperand;
import org.apache.pinot.query.runtime.operator.operands.TransformOperandFactory;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner.JoinOverFlowMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
  private static final Logger LOGGER = LoggerFactory.getLogger(HashJoinOperator.class);
  private static final String EXPLAIN_NAME = "HASH_JOIN";
  private static final int INITIAL_HEURISTIC_SIZE = 16;
  private static final int DEFAULT_MAX_ROWS_IN_JOIN = 1024 * 1024; // 2^20, around 1MM rows
  private static final JoinOverFlowMode DEFAULT_JOIN_OVERFLOW_MODE = JoinOverFlowMode.THROW;

  private static final Set<JoinRelType> SUPPORTED_JOIN_TYPES =
      Set.of(JoinRelType.INNER, JoinRelType.LEFT, JoinRelType.RIGHT, JoinRelType.FULL, JoinRelType.SEMI,
          JoinRelType.ANTI);

  private final Map<Object, ArrayList<Object[]>> _broadcastRightTable;

  // Used to track matched right rows.
  // Only used for right join and full join to output non-matched right rows.
  private final Map<Object, BitSet> _matchedRightRows;

  private final MultiStageOperator _leftInput;
  private final MultiStageOperator _rightInput;
  private final JoinRelType _joinType;
  private final KeySelector<?> _leftKeySelector;
  private final KeySelector<?> _rightKeySelector;
  private final DataSchema _resultSchema;
  private final int _leftColumnSize;
  private final int _resultColumnSize;
  private final List<TransformOperand> _nonEquiEvaluators;
  private final StatMap<StatKey> _statMap = new StatMap<>(StatKey.class);

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

  private boolean _isHashTableBuilt;
  private int _currentRowsInHashTable;
  private TransferableBlock _upstreamErrorBlock;
  private MultiStageQueryStats _leftSideStats;
  private MultiStageQueryStats _rightSideStats;
  // Used by non-inner join.
  // Needed to indicate we have finished processing all results after returning last block.
  private boolean _isTerminated;

  public HashJoinOperator(OpChainExecutionContext context, MultiStageOperator leftInput, DataSchema leftSchema,
      MultiStageOperator rightInput, JoinNode node) {
    super(context);
    Preconditions.checkState(SUPPORTED_JOIN_TYPES.contains(node.getJoinType()),
        "Join type: " + node.getJoinType() + " is not supported!");
    _joinType = node.getJoinType();
    _leftKeySelector = KeySelectorFactory.getKeySelector(node.getLeftKeys());
    _rightKeySelector = KeySelectorFactory.getKeySelector(node.getRightKeys());
    _leftColumnSize = leftSchema.size();
    _resultSchema = node.getDataSchema();
    _resultColumnSize = _resultSchema.size();
    Preconditions.checkState(_resultColumnSize >= _leftColumnSize,
        "Result column size: %s has to be greater than or equal to left column size: %s", _resultColumnSize,
        _leftColumnSize);
    _leftInput = leftInput;
    _rightInput = rightInput;
    List<RexExpression> nonEquiConditions = node.getNonEquiConditions();
    _nonEquiEvaluators = new ArrayList<>(nonEquiConditions.size());
    for (RexExpression nonEquiCondition : nonEquiConditions) {
      _nonEquiEvaluators.add(TransformOperandFactory.getTransformOperand(nonEquiCondition, _resultSchema));
    }
    _broadcastRightTable = new HashMap<>();
    if (needUnmatchedRightRows()) {
      _matchedRightRows = new HashMap<>();
    } else {
      _matchedRightRows = null;
    }
    Map<String, String> metadata = context.getOpChainMetadata();
    PlanNode.NodeHint nodeHint = node.getNodeHint();
    _maxRowsInHashTable = getMaxRowInJoin(metadata, nodeHint);
    _joinOverflowMode = getJoinOverflowMode(metadata, nodeHint);
  }

  @Override
  public void registerExecution(long time, int numRows) {
    _statMap.merge(StatKey.EXECUTION_TIME_MS, time);
    _statMap.merge(StatKey.EMITTED_ROWS, numRows);
  }

  @Override
  public Type getOperatorType() {
    return Type.HASH_JOIN;
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  private int getMaxRowInJoin(Map<String, String> opChainMetadata, @Nullable PlanNode.NodeHint nodeHint) {
    if (nodeHint != null) {
      Map<String, String> joinOptions = nodeHint.getHintOptions().get(PinotHintOptions.JOIN_HINT_OPTIONS);
      if (joinOptions != null) {
        String maxRowsInJoinStr = joinOptions.get(PinotHintOptions.JoinHintOptions.MAX_ROWS_IN_JOIN);
        if (maxRowsInJoinStr != null) {
          return Integer.parseInt(maxRowsInJoinStr);
        }
      }
    }
    Integer maxRowsInJoin = QueryOptionsUtils.getMaxRowsInJoin(opChainMetadata);
    return maxRowsInJoin != null ? maxRowsInJoin : DEFAULT_MAX_ROWS_IN_JOIN;
  }

  private JoinOverFlowMode getJoinOverflowMode(Map<String, String> contextMetadata,
      @Nullable PlanNode.NodeHint nodeHint) {
    if (nodeHint != null) {
      Map<String, String> joinOptions = nodeHint.getHintOptions().get(PinotHintOptions.JOIN_HINT_OPTIONS);
      if (joinOptions != null) {
        String joinOverflowModeStr = joinOptions.get(PinotHintOptions.JoinHintOptions.JOIN_OVERFLOW_MODE);
        if (joinOverflowModeStr != null) {
          return JoinOverFlowMode.valueOf(joinOverflowModeStr);
        }
      }
    }
    JoinOverFlowMode joinOverflowMode = QueryOptionsUtils.getJoinOverflowMode(contextMetadata);
    return joinOverflowMode != null ? joinOverflowMode : DEFAULT_JOIN_OVERFLOW_MODE;
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return List.of(_leftInput, _rightInput);
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock()
      throws ProcessingException {
    if (!_isHashTableBuilt) {
      // Build JOIN hash table
      buildBroadcastHashTable();
    }
    if (_upstreamErrorBlock != null) {
      return _upstreamErrorBlock;
    }
    return buildJoinedDataBlock();
  }

  private void buildBroadcastHashTable()
      throws ProcessingException {
    long startTime = System.currentTimeMillis();
    TransferableBlock rightBlock = _rightInput.nextBlock();
    while (!TransferableBlockUtils.isEndOfStream(rightBlock)) {
      List<Object[]> container = rightBlock.getContainer();
      // Row based overflow check.
      if (container.size() + _currentRowsInHashTable > _maxRowsInHashTable) {
        if (_joinOverflowMode == JoinOverFlowMode.THROW) {
          ProcessingException resourceLimitExceededException =
              new ProcessingException(QueryException.SERVER_RESOURCE_LIMIT_EXCEEDED_ERROR_CODE);
          resourceLimitExceededException.setMessage(
              "Cannot build in memory hash table for join operator, reach number of rows limit: " + _maxRowsInHashTable
                  + ". Consider increasing the limit for the maximum number of rows in a join either via the query "
                  + "option '" + CommonConstants.Broker.Request.QueryOptionKey.MAX_ROWS_IN_JOIN + "' or the '"
                  + PinotHintOptions.JoinHintOptions.MAX_ROWS_IN_JOIN + "' hint in the '"
                  + PinotHintOptions.JOIN_HINT_OPTIONS + "'. Alternatively, if partial results are acceptable, the join"
                  + " overflow mode can be set to '" + JoinOverFlowMode.BREAK.name() + "' either via the query option '"
                  + CommonConstants.Broker.Request.QueryOptionKey.JOIN_OVERFLOW_MODE + "' or the '"
                  + PinotHintOptions.JoinHintOptions.JOIN_OVERFLOW_MODE + "' hint in the '"
                  + PinotHintOptions.JOIN_HINT_OPTIONS + "'. Furthermore, if there is a large disparity in the size of "
                  + "the two tables being joined, use the smaller table as the right input instead of the left.");
          throw resourceLimitExceededException;
        } else {
          // Just fill up the buffer.
          int remainingRows = _maxRowsInHashTable - _currentRowsInHashTable;
          container = container.subList(0, remainingRows);
          _statMap.merge(StatKey.MAX_ROWS_IN_JOIN_REACHED, true);
          // setting only the rightTableOperator to be early terminated and awaits EOS block next.
          _rightInput.earlyTerminate();
        }
      }
      // put all the rows into corresponding hash collections keyed by the key selector function.
      for (Object[] row : container) {
        ArrayList<Object[]> hashCollection = _broadcastRightTable.computeIfAbsent(_rightKeySelector.getKey(row),
            k -> new ArrayList<>(INITIAL_HEURISTIC_SIZE));
        int size = hashCollection.size();
        if ((size & size - 1) == 0 && size < _maxRowsInHashTable && size < Integer.MAX_VALUE / 2) { // is power of 2
          hashCollection.ensureCapacity(Math.min(size << 1, _maxRowsInHashTable));
        }
        hashCollection.add(row);
      }
      _currentRowsInHashTable += container.size();
      rightBlock = _rightInput.nextBlock();
    }
    if (rightBlock.isErrorBlock()) {
      _upstreamErrorBlock = rightBlock;
    } else {
      _isHashTableBuilt = true;
      _rightSideStats = rightBlock.getQueryStats();
      assert _rightSideStats != null;
    }
    _statMap.merge(StatKey.TIME_BUILDING_HASH_TABLE_MS, System.currentTimeMillis() - startTime);
  }

  private TransferableBlock buildJoinedDataBlock() {
    if (_isTerminated) {
      assert _leftSideStats != null;
      return TransferableBlockUtils.getEndOfStreamTransferableBlock(_leftSideStats);
    }

    // Keep reading the input blocks until we find a match row or all blocks are processed.
    // TODO: Consider batching the rows to improve performance.
    while (true) {
      TransferableBlock leftBlock = _leftInput.nextBlock();
      if (leftBlock.isErrorBlock()) {
        return leftBlock;
      }
      if (leftBlock.isSuccessfulEndOfStreamBlock()) {
        assert _rightSideStats != null;
        _leftSideStats = leftBlock.getQueryStats();
        assert _leftSideStats != null;
        _leftSideStats.mergeInOrder(_rightSideStats, getOperatorType(), _statMap);
        if (needUnmatchedRightRows()) {
          List<Object[]> rows = buildNonMatchRightRows();
          if (!rows.isEmpty()) {
            _isTerminated = true;
            return new TransferableBlock(rows, _resultSchema, DataBlock.Type.ROW);
          }
        }
        return TransferableBlockUtils.getEndOfStreamTransferableBlock(_leftSideStats);
      }
      assert leftBlock.isDataBlock();
      List<Object[]> rows = buildJoinedRows(leftBlock);
      if (!rows.isEmpty()) {
        return new TransferableBlock(rows, _resultSchema, DataBlock.Type.ROW);
      }
    }
  }

  private List<Object[]> buildJoinedRows(TransferableBlock leftBlock) {
    switch (_joinType) {
      case SEMI:
        return buildJoinedDataBlockSemi(leftBlock);
      case ANTI:
        return buildJoinedDataBlockAnti(leftBlock);
      default: { // INNER, LEFT, RIGHT, FULL
        return buildJoinedDataBlockDefault(leftBlock);
      }
    }
  }

  private List<Object[]> buildJoinedDataBlockSemi(TransferableBlock leftBlock) {
    List<Object[]> container = leftBlock.getContainer();
    List<Object[]> rows = new ArrayList<>(container.size());

    for (Object[] leftRow : container) {
      Object key = _leftKeySelector.getKey(leftRow);
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
      Object key = _leftKeySelector.getKey(leftRow);
      // NOTE: Empty key selector will always give same hash code.
      List<Object[]> rightRows = _broadcastRightTable.get(key);
      if (rightRows == null) {
        if (needUnmatchedLeftRows()) {
          rows.add(joinRow(leftRow, null));
        }
        continue;
      }
      boolean hasMatchForLeftRow = false;
      int numRightRows = rightRows.size();
      rows.ensureCapacity(rows.size() + numRightRows);
      for (int i = 0; i < numRightRows; i++) {
        Object[] rightRow = rightRows.get(i);
        // TODO: Optimize this to avoid unnecessary object copy.
        Object[] resultRow = joinRow(leftRow, rightRow);
        if (_nonEquiEvaluators.isEmpty() || _nonEquiEvaluators.stream()
            .allMatch(evaluator -> BooleanUtils.isTrueInternalValue(evaluator.apply(resultRow)))) {
          rows.add(resultRow);
          hasMatchForLeftRow = true;
          if (_matchedRightRows != null) {
            _matchedRightRows.computeIfAbsent(key, k -> new BitSet(numRightRows)).set(i);
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
      Object key = _leftKeySelector.getKey(leftRow);
      // ANTI-JOIN only checks non-existence of the key
      if (!_broadcastRightTable.containsKey(key)) {
        rows.add(joinRow(leftRow, null));
      }
    }
    return rows;
  }

  private List<Object[]> buildNonMatchRightRows() {
    List<Object[]> rows = new ArrayList<>();
    for (Map.Entry<Object, ArrayList<Object[]>> entry : _broadcastRightTable.entrySet()) {
      List<Object[]> rightRows = entry.getValue();
      BitSet matchedIndices = _matchedRightRows.get(entry.getKey());
      if (matchedIndices == null) {
        for (Object[] rightRow : rightRows) {
          rows.add(joinRow(null, rightRow));
        }
      } else {
        int numRightRows = rightRows.size();
        int unmatchedIndex = 0;
        while ((unmatchedIndex = matchedIndices.nextClearBit(unmatchedIndex)) < numRightRows) {
          rows.add(joinRow(null, rightRows.get(unmatchedIndex++)));
        }
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

  public enum StatKey implements StatMap.Key {
    //@formatter:off
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
     * How long (CPU time) has been spent on building the hash table.
     */
    TIME_BUILDING_HASH_TABLE_MS(StatMap.Type.LONG);
    //@formatter:on

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
