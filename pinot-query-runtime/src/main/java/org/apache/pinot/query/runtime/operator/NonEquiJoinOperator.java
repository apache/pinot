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
import java.util.List;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner.JoinOverFlowMode;


/**
 * The {@code NonEquiJoinOperator} implements the join algorithm without join keys. Right table is materialized into a
 * list.
 */
public class NonEquiJoinOperator extends BaseJoinOperator {
  private static final String EXPLAIN_NAME = "NON_EQUI_JOIN";

  private final List<Object[]> _rightTable;
  // Track matched right rows for right join and full join to output non-matched right rows.
  // TODO: Revisit whether we should use IntList or RoaringBitmap for smaller memory footprint.
  private BitSet _matchedRightRows;

  public NonEquiJoinOperator(OpChainExecutionContext context, MultiStageOperator leftInput, DataSchema leftSchema,
      MultiStageOperator rightInput, JoinNode node) {
    super(context, leftInput, leftSchema, rightInput, node);
    Preconditions.checkState(node.getLeftKeys().isEmpty(), "Non-equi join operator cannot have join keys");
    Preconditions.checkState(_joinType != JoinRelType.SEMI && _joinType != JoinRelType.ANTI,
        "Non-equi join operator does not support semi or anti join");
    _rightTable = new ArrayList<>();
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected void buildRightTable() {
    LOGGER.trace("Building right table for join operator");
    long startTime = System.currentTimeMillis();
    MseBlock rightBlock = _rightInput.nextBlock();
    while (rightBlock.isData()) {
      List<Object[]> rows = ((MseBlock.Data) rightBlock).asRowHeap().getRows();
      int numRowsInRightTable = _rightTable.size();
      // Row based overflow check.
      if (rows.size() + numRowsInRightTable > _maxRowsInJoin) {
        if (_joinOverflowMode == JoinOverFlowMode.THROW) {
          throwForJoinRowLimitExceeded(
              "Cannot build in memory right table for join operator, reached number of rows limit: " + _maxRowsInJoin);
        } else {
          // Just fill up the buffer.
          int remainingRows = _maxRowsInJoin - numRowsInRightTable;
          rows = rows.subList(0, remainingRows);
          _statMap.merge(StatKey.MAX_ROWS_IN_JOIN_REACHED, true);
          // setting only the rightTableOperator to be early terminated and awaits EOS block next.
          _rightInput.earlyTerminate();
        }
      }
      _rightTable.addAll(rows);
      sampleAndCheckInterruption();
      rightBlock = _rightInput.nextBlock();
    }
    MseBlock.Eos eosBlock = (MseBlock.Eos) rightBlock;
    if (eosBlock.isError()) {
      _eos = eosBlock;
    } else {
      _isRightTableBuilt = true;
      if (needUnmatchedRightRows()) {
        _matchedRightRows = new BitSet(_rightTable.size());
      }
    }
    _statMap.merge(StatKey.TIME_BUILDING_HASH_TABLE_MS, System.currentTimeMillis() - startTime);
    LOGGER.trace("Finished building right table for join operator");
  }

  @Override
  protected List<Object[]> buildJoinedRows(MseBlock.Data leftBlock) {
    ArrayList<Object[]> rows = new ArrayList<>();
    for (Object[] leftRow : leftBlock.asRowHeap().getRows()) {
      // NOTE: Empty key selector will always give same hash code.
      boolean hasMatchForLeftRow = false;
      int numRightRows = _rightTable.size();
      boolean maxRowsLimitReached = false;
      for (int i = 0; i < numRightRows; i++) {
        Object[] rightRow = _rightTable.get(i);
        // TODO: Optimize this to avoid unnecessary object copy.
        Object[] resultRow = joinRow(leftRow, rightRow);
        if (matchNonEquiConditions(resultRow)) {
          if (isMaxRowsLimitReached(rows.size())) {
            maxRowsLimitReached = true;
            break;
          }
          rows.add(resultRow);
          hasMatchForLeftRow = true;
          if (_matchedRightRows != null) {
            _matchedRightRows.set(i);
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

  @Override
  protected List<Object[]> buildNonMatchRightRows() {
    int numRightRows = _rightTable.size();
    int numMatchedRightRows = _matchedRightRows.cardinality();
    if (numMatchedRightRows == numRightRows) {
      return List.of();
    }
    List<Object[]> rows = new ArrayList<>(numRightRows - numMatchedRightRows);
    int unmatchedIndex = 0;
    while ((unmatchedIndex = _matchedRightRows.nextClearBit(unmatchedIndex)) < numRightRows) {
      rows.add(joinRow(null, _rightTable.get(unmatchedIndex++)));
    }
    return rows;
  }
}
