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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
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
 * For left join and inner join,
 * <p>It takes the right table as the broadcast side and materialize a hash table. Then for each of the left table row,
 * it looks up for the corresponding row(s) from the hash table and create a joint row.
 *
 * <p>For each of the data block received from the left table, it will generate a joint data block.
 *
 * For right join,
 * We broadcast the left table and probe the hash table using right table.
 *
 * We currently support left join, inner join and right join.
 * The output is in the format of [left_row, right_row]
 */
// TODO: Move inequi out of hashjoin. (https://github.com/apache/pinot/issues/9728)
public class HashJoinOperator extends BaseOperator<TransferableBlock> {
  private static class JoinResolver{
    public static JoinResolver create(JoinRelType joinType,
        Operator<TransferableBlock> leftTableOperator, Operator<TransferableBlock> rightTableOperator,
        KeySelector leftKeySelector, KeySelector rightKeySelector){
      JoinResolver resolver = new JoinResolver();
      switch (joinType){
        case LEFT:
        case INNER:
          resolver._broadcastOperator = rightTableOperator;
          resolver._probeOperator = leftTableOperator;
          resolver._probeKeySelector = leftKeySelector;
          resolver._broadcastKeySelector = rightKeySelector;
          resolver._getLeftRow = (Object[] probeRow, Object[] broadcastRow) -> probeRow;
          resolver._getRightRow = (Object[] probeRow, Object[] broadcastRow) -> broadcastRow;
          break;
        case RIGHT:
           resolver._broadcastOperator = leftTableOperator;
          resolver._probeOperator = rightTableOperator;
          resolver._probeKeySelector = rightKeySelector;
          resolver._broadcastKeySelector = leftKeySelector;
          resolver._getLeftRow = (Object[] probeRow, Object[] broadcastRow) -> broadcastRow;
          resolver._getRightRow = (Object[] probeRow, Object[] broadcastRow) -> probeRow;
          break;
        default:
           Preconditions.checkState(false, "Join type shouldn't be supported:" + joinType);
           break;
      }
      return resolver;
    }
    public Operator<TransferableBlock> _broadcastOperator;
    public Operator<TransferableBlock> _probeOperator;
    public KeySelector<Object[], Object[]> _broadcastKeySelector;
    public KeySelector<Object[], Object[]> _probeKeySelector;
    public BiFunction<Object[], Object[], Object[]> _getLeftRow;
    public BiFunction<Object[], Object[], Object[]> _getRightRow;
  }
  private static final String EXPLAIN_NAME = "HASH_JOIN";
  private static final Set<JoinRelType> SUPPORTED_JOIN_TYPES =
      ImmutableSet.of(JoinRelType.INNER, JoinRelType.LEFT, JoinRelType.RIGHT);
  private final HashMap<Key, List<Object[]>> _broadcastHashTable;
  private final JoinRelType _joinType;
  private final DataSchema _resultSchema;
  private final int _leftRowSize;
  private final int _resultRowSize;
  private final List<FilterOperand> _joinClauseEvaluators;
  private boolean _isHashTableBuilt;
  private TransferableBlock _upstreamErrorBlock;

  private JoinResolver _joinResolver;

  public HashJoinOperator(Operator<TransferableBlock> leftTableOperator, Operator<TransferableBlock> rightTableOperator,
      DataSchema leftSchema, JoinNode node) {
    _joinType = node.getJoinRelType();
    Preconditions.checkState(SUPPORTED_JOIN_TYPES.contains(node.getJoinRelType()),
        "Join type: " + _joinType + " is not supported!");
    KeySelector<Object[], Object[]> leftKeySelector = node.getJoinKeys().getLeftJoinKeySelector();
    Preconditions.checkState(leftKeySelector != null, "LeftKeySelector for join cannot be null");
    KeySelector<Object[], Object[]> rightKeySelector = node.getJoinKeys().getRightJoinKeySelector();
    Preconditions.checkState(rightKeySelector != null, "RightKeySelector for join cannot be null");
    _leftRowSize = leftSchema.size();
    Preconditions.checkState(_leftRowSize > 0, "leftRowSize has to be greater than zero:" + _leftRowSize);
    _resultSchema = node.getDataSchema();
    _resultRowSize = _resultSchema.size();
    Preconditions.checkState(_resultRowSize > _leftRowSize,
        "Result row size" + _leftRowSize + " has to be greater than left row size:" + _leftRowSize);

    _joinClauseEvaluators = new ArrayList<>(node.getJoinClauses().size());
    for (RexExpression joinClause : node.getJoinClauses()) {
      _joinClauseEvaluators.add(FilterOperand.toFilterOperand(joinClause, _resultSchema));
    }
    _isHashTableBuilt = false;
    _broadcastHashTable = new HashMap<>();
    _upstreamErrorBlock = null;
    _joinResolver = JoinResolver.create(_joinType, leftTableOperator, rightTableOperator, leftKeySelector,
        rightKeySelector);
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
      return buildJoinedDataBlock(_joinResolver._probeOperator.nextBlock());
    } catch (Exception e) {
      return TransferableBlockUtils.getErrorTransferableBlock(e);
    }
  }

  private void buildBroadcastHashTable() {
    TransferableBlock rightBlock = _joinResolver._broadcastOperator.nextBlock();
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
      List<Object[]> hashCollection =
          _broadcastHashTable.computeIfAbsent(new Key(_joinResolver._broadcastKeySelector.getKey(row)), k -> new ArrayList<>());
      hashCollection.add(row);
    }
  }

  private TransferableBlock buildJoinedDataBlock(TransferableBlock probeBlock)
      throws Exception {
    if (probeBlock.isErrorBlock()) {
      _upstreamErrorBlock = probeBlock;
      return _upstreamErrorBlock;
    } else if (TransferableBlockUtils.isNoOpBlock(probeBlock) || TransferableBlockUtils.isEndOfStream(probeBlock)) {
      return probeBlock;
    }
    List<Object[]> rows = new ArrayList<>();
    List<Object[]> container = probeBlock.isEndOfStreamBlock() ? new ArrayList<>() : probeBlock.getContainer();
    for (Object[] probeRow : container) {
      // NOTE: Empty key selector will always give same hash code.
      List<Object[]> hashCollection =
          _broadcastHashTable.getOrDefault(new Key(_joinResolver._probeKeySelector.getKey(probeRow)), Collections.emptyList());
      // If it is a left join and right table is empty, we return left rows.
      if (hashCollection.isEmpty() && (_joinType == JoinRelType.LEFT || _joinType == JoinRelType.RIGHT)) {
        rows.add(joinRow(probeRow, null));
      } else {
        // If it is other type of join.
        for (Object[] broadcastRow : hashCollection) {
          // TODO: Optimize this to avoid unnecessary object copy.
          Object[] resultRow = joinRow(probeRow, broadcastRow);
          if (_joinClauseEvaluators.isEmpty() || _joinClauseEvaluators.stream()
              .allMatch(evaluator -> evaluator.apply(resultRow))) {
            rows.add(resultRow);
          }
        }
      }
    }
    return new TransferableBlock(rows, _resultSchema, DataBlock.Type.ROW);
  }

  private Object[] joinRow(Object[] probeRow, @Nullable Object[] broadcastRow) {
    Object[] resultRow = new Object[_resultRowSize];
    int idx = 0;
    if (_joinResolver._getLeftRow.apply(probeRow, broadcastRow) != null) {
      for (Object obj : _joinResolver._getLeftRow.apply(probeRow, broadcastRow)) {
        resultRow[idx++] = obj;
      }
    }
    // This is needed since left row can be null and we need to advance the idx to the beginning of right row.
    idx = _leftRowSize;
    if (_joinResolver._getRightRow.apply(probeRow, broadcastRow) != null) {
      for (Object obj : _joinResolver._getRightRow.apply(probeRow, broadcastRow)) {
        resultRow[idx++] = obj;
      }
    }
    return resultRow;
  }
}
