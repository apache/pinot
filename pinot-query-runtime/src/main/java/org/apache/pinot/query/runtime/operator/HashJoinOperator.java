


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

import com.clearspring.analytics.util.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.planner.stage.JoinNode;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.operands.FilterOperand;


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
public class HashJoinOperator extends BaseOperator<TransferableBlock> {
  private class BroadcastRows {
    public BroadcastRows(List<Object[]> rows, boolean hasMatch) {
      _rows = rows;
      _hasMatch = hasMatch;
    }

    public List<Object[]> _rows;
    public boolean _hasMatch = false;
  };

  private static final String EXPLAIN_NAME = "HASH_JOIN";
  private static final Set<JoinRelType> SUPPORTED_JOIN_TYPES =
      ImmutableSet.of(JoinRelType.INNER, JoinRelType.LEFT, JoinRelType.RIGHT, JoinRelType.FULL);
  private final HashMap<Key, BroadcastRows> _broadcastHashTable;
  private final Operator<TransferableBlock> _leftTableOperator;
  private final Operator<TransferableBlock> _rightTableOperator;
  private final JoinRelType _joinType;
  private final DataSchema _resultSchema;
  private final int _leftRowSize;
  private final int _resultRowSize;
  private final List<FilterOperand> _joinClauseEvaluators;
  private boolean _isHashTableBuilt;

  // Used only by full join.
  // Needed for full join to indicate we have finished processing all results after returning last block.
  // TODO: Remove this special handling by fixing data block EOS abstraction or operator's invariant.
  private boolean _isTerminated;
  private TransferableBlock _upstreamErrorBlock;
  private KeySelector<Object[], Object[]> _leftKeySelector;
  private KeySelector<Object[], Object[]> _rightKeySelector;

  public HashJoinOperator(Operator<TransferableBlock> leftTableOperator, Operator<TransferableBlock> rightTableOperator,
      DataSchema leftSchema, JoinNode node) {
    Preconditions.checkState(SUPPORTED_JOIN_TYPES.contains(node.getJoinRelType()),
        "Join type: " + node.getJoinRelType() + " is not supported!");
    _joinType = node.getJoinRelType();
    _leftKeySelector = node.getJoinKeys().getLeftJoinKeySelector();
    _rightKeySelector = node.getJoinKeys().getRightJoinKeySelector();
    Preconditions.checkState(_leftKeySelector != null, "LeftKeySelector for join cannot be null");
    Preconditions.checkState(_rightKeySelector != null, "RightKeySelector for join cannot be null");
    _leftRowSize = leftSchema.size();
    Preconditions.checkState(_leftRowSize > 0, "leftRowSize has to be greater than zero:" + _leftRowSize);
    _resultSchema = node.getDataSchema();
    _resultRowSize = _resultSchema.size();
    Preconditions.checkState(_resultRowSize > _leftRowSize,
        "Result row size" + _leftRowSize + " has to be greater than left row size:" + _leftRowSize);
    _leftTableOperator = leftTableOperator;
    _rightTableOperator = rightTableOperator;
    _joinClauseEvaluators = new ArrayList<>(node.getJoinClauses().size());
    for (RexExpression joinClause : node.getJoinClauses()) {
      _joinClauseEvaluators.add(FilterOperand.toFilterOperand(joinClause, _resultSchema));
    }
    _isHashTableBuilt = false;
    _broadcastHashTable = new HashMap<>();
    _upstreamErrorBlock = null;
  }

  @Override
  public List<Operator> getChildOperators() {
    // WorkerExecutor doesn't use getChildOperators, returns null here.
    return null;
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
        return TransferableBlockUtils.getEndOfStreamTransferableBlock();
      }
      if (!_isHashTableBuilt) {
        // Build JOIN hash table
        buildBroadcastHashTable();
      }
      if (_upstreamErrorBlock != null) {
        return _upstreamErrorBlock;
      } else if (!_isHashTableBuilt) {
        return TransferableBlockUtils.getNoOpTransferableBlock();
      }
      // JOIN each left block with the right block.
      return buildJoinedDataBlock(_leftTableOperator.nextBlock());
    } catch (Exception e) {
      return TransferableBlockUtils.getErrorTransferableBlock(e);
    }
  }

  private void buildBroadcastHashTable() {
    TransferableBlock rightBlock = _rightTableOperator.nextBlock();
    if (rightBlock.isErrorBlock()) {
      _upstreamErrorBlock = rightBlock;
      return;
    }

    if (TransferableBlockUtils.isEndOfStream(rightBlock)) {
      _isHashTableBuilt = true;
      return;
    } else if (TransferableBlockUtils.isNoOpBlock(rightBlock)) {
      return;
    }

    List<Object[]> container = rightBlock.getContainer();
    // put all the rows into corresponding hash collections keyed by the key selector function.
    for (Object[] row : container) {
      BroadcastRows hashCollection = _broadcastHashTable.computeIfAbsent(new Key(_rightKeySelector.getKey(row)),
          k -> new BroadcastRows(new ArrayList<>(), false));
      hashCollection._rows.add(row);
    }
  }

  private TransferableBlock buildJoinedDataBlock(TransferableBlock leftBlock)
      throws Exception {
    if (leftBlock.isErrorBlock()) {
      _upstreamErrorBlock = leftBlock;
      return _upstreamErrorBlock;
    }
    if (leftBlock.isNoOpBlock() || (leftBlock.isEOSBlock() && (_joinType != JoinRelType.FULL
        && _joinType != JoinRelType.RIGHT))) {
      return leftBlock;
    }
    if (leftBlock.isEOSBlock()) {
      // Return remaining non-matched rows for full join in broadcast table
      List<Object[]> returnRows = new ArrayList<>();
      for (BroadcastRows rows : _broadcastHashTable.values()) {
        if (!rows._hasMatch) {
          for (Object[] notMatchedRow : rows._rows) {
            Object[] resultRow = joinRow(null, notMatchedRow);
            if (_joinClauseEvaluators.isEmpty() || _joinClauseEvaluators.stream()
                .allMatch(evaluator -> evaluator.apply(resultRow))) {
              returnRows.add(resultRow);
            }
          }
        }
      }
      _isTerminated = true;
      return new TransferableBlock(returnRows, _resultSchema, DataBlock.Type.ROW);
    }
    List<Object[]> rows = new ArrayList<>();
    List<Object[]> container = leftBlock.isEndOfStreamBlock() ? new ArrayList<>() : leftBlock.getContainer();
    for (Object[] leftRow : container) {
      // NOTE: Empty key selector will always give same hash code.
      BroadcastRows broadcastRows = _broadcastHashTable.getOrDefault(new Key(_leftKeySelector.getKey(leftRow)), null);
      if (broadcastRows == null) {
        switch (_joinType) {
          case LEFT:
            // intentional fall through
          case FULL:
            rows.add(joinRow(leftRow, null));
            break;
          case RIGHT:
            // intentional fall through
          case INNER:
            break;
          default:
            Preconditions.checkState(false, "Join type shouldn't be supported:" + _joinType);
            break;
        }
        continue;
      }
      broadcastRows._hasMatch = true;
      for (Object[] rightRow : broadcastRows._rows) {
        // TODO: Optimize this to avoid unnecessary object copy.
        Object[] resultRow = joinRow(leftRow, rightRow);
        if (_joinClauseEvaluators.isEmpty() || _joinClauseEvaluators.stream()
            .allMatch(evaluator -> evaluator.apply(resultRow))) {
          rows.add(resultRow);
        }
      }
    }
    return new TransferableBlock(rows, _resultSchema, DataBlock.Type.ROW);
  }

  private Object[] joinRow(@Nullable Object[] leftRow, @Nullable Object[] rightRow) {
    Object[] resultRow = new Object[_resultRowSize];
    int idx = 0;
    if (leftRow != null) {
      for (Object obj : leftRow) {
        resultRow[idx++] = obj;
      }
    }
    // This is needed since left row can be null and we need to advance the idx to the beginning of right row.
    idx = _leftRowSize;
    if (rightRow != null) {
      for (Object obj : rightRow) {
        resultRow[idx++] = obj;
      }
    }
    return resultRow;
  }
}