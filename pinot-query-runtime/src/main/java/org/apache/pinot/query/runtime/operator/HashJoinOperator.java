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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.common.datablock.BaseDataBlock;
import org.apache.pinot.core.common.datablock.DataBlockUtils;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.planner.stage.JoinNode;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;


/**
 * This basic {@code BroadcastJoinOperator} implement a basic broadcast join algorithm.
 *
 * <p>It takes the right table as the broadcast side and materialize a hash table. Then for each of the left table row,
 * it looks up for the corresponding row(s) from the hash table and create a joint row.
 *
 * <p>For each of the data block received from the left table, it will generate a joint data block.
 */
public class HashJoinOperator extends BaseOperator<TransferableBlock> {
  private static final String EXPLAIN_NAME = "BROADCAST_JOIN";

  private final HashMap<Integer, List<Object[]>> _broadcastHashTable;
  private final BaseOperator<TransferableBlock> _leftTableOperator;
  private final BaseOperator<TransferableBlock> _rightTableOperator;
  private final JoinRelType _joinType;
  private final DataSchema _resultSchema;
  private final DataSchema _leftTableSchema;
  private final DataSchema _rightTableSchema;
  private final int _resultRowSize;
  private boolean _isHashTableBuilt;
  private TransferableBlock _upstreamErrorBlock;
  private KeySelector<Object[], Object[]> _leftKeySelector;
  private KeySelector<Object[], Object[]> _rightKeySelector;

  public HashJoinOperator(BaseOperator<TransferableBlock> leftTableOperator, DataSchema leftSchema,
      BaseOperator<TransferableBlock> rightTableOperator, DataSchema rightSchema, DataSchema outputSchema,
      List<JoinNode.JoinClause> criteria, JoinRelType joinType) {
    _leftKeySelector = criteria.get(0).getLeftJoinKeySelector();
    _rightKeySelector = criteria.get(0).getRightJoinKeySelector();
    _leftTableOperator = leftTableOperator;
    _rightTableOperator = rightTableOperator;
    _resultSchema = outputSchema;
    _leftTableSchema = leftSchema;
    _rightTableSchema = rightSchema;
    _joinType = joinType;
    _resultRowSize = _resultSchema.size();
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
    // Build JOIN hash table
    buildBroadcastHashTable();
    if (_upstreamErrorBlock != null) {
      return _upstreamErrorBlock;
    }
    // JOIN each left block with the right block.
    try {
      return buildJoinedDataBlock(_leftTableOperator.nextBlock());
    } catch (Exception e) {
      return TransferableBlockUtils.getErrorTransferableBlock(e);
    }
  }

  private void buildBroadcastHashTable() {
    if (!_isHashTableBuilt) {
      TransferableBlock rightBlock = _rightTableOperator.nextBlock();
      while (!TransferableBlockUtils.isEndOfStream(rightBlock)) {
        List<Object[]> container = rightBlock.getContainer();
        // put all the rows into corresponding hash collections keyed by the key selector function.
        for (Object[] row : container) {
          List<Object[]> hashCollection =
              _broadcastHashTable.computeIfAbsent(_rightKeySelector.computeHash(row), k -> new ArrayList<>());
          hashCollection.add(row);
        }
        rightBlock = _rightTableOperator.nextBlock();
      }
      if (rightBlock.isErrorBlock()) {
        _upstreamErrorBlock = rightBlock;
      }
      _isHashTableBuilt = true;
    }
  }

  private TransferableBlock buildJoinedDataBlock(TransferableBlock leftBlock)
      throws Exception {
    if (!TransferableBlockUtils.isEndOfStream(leftBlock)) {
      List<Object[]> rows = new ArrayList<>();
      List<Object[]> container = leftBlock.getContainer();
      for (Object[] leftRow : container) {
        List<Object[]> hashCollection = _broadcastHashTable.getOrDefault(
            _leftKeySelector.computeHash(leftRow), Collections.emptyList());
        for (Object[] rightRow : hashCollection) {
          rows.add(joinRow(leftRow, rightRow));
        }
      }
      return new TransferableBlock(rows, _resultSchema, BaseDataBlock.Type.ROW);
    } else if (leftBlock.isErrorBlock()) {
      _upstreamErrorBlock = leftBlock;
      return _upstreamErrorBlock;
    } else {
      return new TransferableBlock(DataBlockUtils.getEndOfStreamDataBlock(_resultSchema));
    }
  }

  private Object[] joinRow(Object[] leftRow, Object[] rightRow) {
    Object[] resultRow = new Object[_resultRowSize];
    int idx = 0;
    for (Object obj : leftRow) {
      resultRow[idx++] = obj;
    }
    if (_joinType != JoinRelType.SEMI) {
      for (Object obj : rightRow) {
        resultRow[idx++] = obj;
      }
    }
    return resultRow;
  }
}
