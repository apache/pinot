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
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.planner.partitioning.KeySelectorFactory;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.exception.QException;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner.JoinOverFlowMode;


/**
 * This {@code HashJoinOperator} join algorithm with join keys. Right table is materialized into a hash table.
 */
// TODO: Support memory size based resource limit.
public class HashJoinOperator extends BaseJoinOperator {
  private static final String EXPLAIN_NAME = "HASH_JOIN";
  private static final int INITIAL_HEURISTIC_SIZE = 16;

  private final KeySelector<?> _leftKeySelector;
  private final KeySelector<?> _rightKeySelector;
  private final Map<Object, ArrayList<Object[]>> _rightTable;
  // Track matched right rows for right join and full join to output non-matched right rows.
  // TODO: Revisit whether we should use IntList or RoaringBitmap for smaller memory footprint.
  private final Map<Object, BitSet> _matchedRightRows;

  public HashJoinOperator(OpChainExecutionContext context, MultiStageOperator leftInput, DataSchema leftSchema,
      MultiStageOperator rightInput, JoinNode node) {
    super(context, leftInput, leftSchema, rightInput, node);
    Preconditions.checkState(!node.getLeftKeys().isEmpty(), "Hash join operator requires join keys");
    _leftKeySelector = KeySelectorFactory.getKeySelector(node.getLeftKeys());
    _rightKeySelector = KeySelectorFactory.getKeySelector(node.getRightKeys());
    _rightTable = new HashMap<>();
    _matchedRightRows = needUnmatchedRightRows() ? new HashMap<>() : null;
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected void buildRightTable()
      throws ProcessingException {
    LOGGER.trace("Building hash table for join operator");
    long startTime = System.currentTimeMillis();
    int numRowsInHashTable = 0;
    TransferableBlock rightBlock = _rightInput.nextBlock();
    while (!TransferableBlockUtils.isEndOfStream(rightBlock)) {
      List<Object[]> container = rightBlock.getContainer();
      // Row based overflow check.
      if (container.size() + numRowsInHashTable > _maxRowsInJoin) {
        if (_joinOverflowMode == JoinOverFlowMode.THROW) {
          throwProcessingExceptionForJoinRowLimitExceeded(
              "Cannot build in memory hash table for join operator, reached number of rows limit: " + _maxRowsInJoin);
        } else {
          // Just fill up the buffer.
          int remainingRows = _maxRowsInJoin - numRowsInHashTable;
          container = container.subList(0, remainingRows);
          _statMap.merge(StatKey.MAX_ROWS_IN_JOIN_REACHED, true);
          // setting only the rightTableOperator to be early terminated and awaits EOS block next.
          _rightInput.earlyTerminate();
        }
      }
      // put all the rows into corresponding hash collections keyed by the key selector function.
      for (Object[] row : container) {
        ArrayList<Object[]> hashCollection =
            _rightTable.computeIfAbsent(_rightKeySelector.getKey(row), k -> new ArrayList<>(INITIAL_HEURISTIC_SIZE));
        int size = hashCollection.size();
        if ((size & size - 1) == 0 && size < _maxRowsInJoin && size < Integer.MAX_VALUE / 2) { // is power of 2
          hashCollection.ensureCapacity(Math.min(size << 1, _maxRowsInJoin));
        }
        hashCollection.add(row);
      }
      numRowsInHashTable += container.size();
      sampleAndCheckInterruption();
      rightBlock = _rightInput.nextBlock();
    }
    if (rightBlock.isErrorBlock()) {
      _upstreamErrorBlock = rightBlock;
    } else {
      _isRightTableBuilt = true;
      _rightSideStats = rightBlock.getQueryStats();
      assert _rightSideStats != null;
    }
    _statMap.merge(StatKey.TIME_BUILDING_HASH_TABLE_MS, System.currentTimeMillis() - startTime);
    LOGGER.trace("Finished building hash table for join operator");
  }

  @Override
  protected List<Object[]> buildJoinedRows(TransferableBlock leftBlock)
      throws ProcessingException {
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

  private List<Object[]> buildJoinedDataBlockDefault(TransferableBlock leftBlock)
      throws ProcessingException {
    List<Object[]> container = leftBlock.getContainer();
    ArrayList<Object[]> rows = new ArrayList<>(container.size());

    for (Object[] leftRow : container) {
      Object key = _leftKeySelector.getKey(leftRow);
      // NOTE: Empty key selector will always give same hash code.
      List<Object[]> rightRows = _rightTable.get(key);
      if (rightRows == null) {
        if (needUnmatchedLeftRows()) {
          if (isMaxRowsLimitReached(rows.size())) {
            break;
          }
          rows.add(joinRow(leftRow, null));
        }
        continue;
      }
      boolean hasMatchForLeftRow = false;
      int numRightRows = rightRows.size();
      rows.ensureCapacity(rows.size() + numRightRows);
      boolean maxRowsLimitReached = false;
      for (int i = 0; i < numRightRows; i++) {
        Object[] rightRow = rightRows.get(i);
        // TODO: Optimize this to avoid unnecessary object copy.
        Object[] resultRow = joinRow(leftRow, rightRow);
        if (_nonEquiEvaluators.isEmpty() || _nonEquiEvaluators.stream()
            .allMatch(evaluator -> BooleanUtils.isTrueInternalValue(evaluator.apply(resultRow)))) {
          if (isMaxRowsLimitReached(rows.size())) {
            maxRowsLimitReached = true;
            break;
          }
          rows.add(resultRow);
          hasMatchForLeftRow = true;
          if (_matchedRightRows != null) {
            _matchedRightRows.computeIfAbsent(key, k -> new BitSet(numRightRows)).set(i);
          }
        }
      }
      if (maxRowsLimitReached) {
        break;
      }
      if (!hasMatchForLeftRow && needUnmatchedLeftRows()) {
        if (isMaxRowsLimitReached(rows.size())) {
          break;
        }
        rows.add(joinRow(leftRow, null));
      }
    }

    return rows;
  }

  private List<Object[]> buildJoinedDataBlockSemi(TransferableBlock leftBlock) {
    List<Object[]> container = leftBlock.getContainer();
    List<Object[]> rows = new ArrayList<>(container.size());

    for (Object[] leftRow : container) {
      Object key = _leftKeySelector.getKey(leftRow);
      // SEMI-JOIN only checks existence of the key
      if (_rightTable.containsKey(key)) {
        rows.add(leftRow);
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
      if (!_rightTable.containsKey(key)) {
        rows.add(leftRow);
      }
    }

    return rows;
  }

  @Override
  protected List<Object[]> buildNonMatchRightRows() {
    List<Object[]> rows = new ArrayList<>();
    for (Map.Entry<Object, ArrayList<Object[]>> entry : _rightTable.entrySet()) {
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
}
