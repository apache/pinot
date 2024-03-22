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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.ExplainPlanRows;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.segment.spi.IndexSegment;


/**
 * Set operator, which supports UNION, INTERSECT and EXCEPT.
 * This has two child operators, and the left child operator is the one that is used to construct the result.
 * The right child operator is used to construct a set of rows that are used to filter the left child operator.
 * The right child operator is consumed in a blocking manner, and the left child operator is consumed in a non-blocking
 * UnionOperator: The right child operator is consumed in a blocking manner.
 */
public abstract class SetOperator<K extends Enum<K> & StatMap.Key> extends MultiStageOperator<K> {
  protected final Set<Record> _rightRowSet;

  private final List<MultiStageOperator<?>> _upstreamOperators;
  private final MultiStageOperator<?> _leftChildOperator;
  private final MultiStageOperator<?> _rightChildOperator;

  private final DataSchema _dataSchema;

  private boolean _isRightSetBuilt;
  private boolean _isTerminated;
  private TransferableBlock _upstreamErrorBlock;

  public SetOperator(OpChainExecutionContext opChainExecutionContext, List<MultiStageOperator<?>> upstreamOperators,
      DataSchema dataSchema) {
    super(opChainExecutionContext);
    _dataSchema = dataSchema;
    _upstreamOperators = upstreamOperators;
    _leftChildOperator = getChildOperators().get(0);
    _rightChildOperator = getChildOperators().get(1);
    _rightRowSet = new HashSet<>();
    _isRightSetBuilt = false;
    _isTerminated = false;
  }

  @Override
  public List<MultiStageOperator<?>> getChildOperators() {
    return _upstreamOperators;
  }

  @Override
  public void prepareForExplainPlan(ExplainPlanRows explainPlanRows) {
    super.prepareForExplainPlan(explainPlanRows);
  }

  @Override
  public void explainPlan(ExplainPlanRows explainPlanRows, int[] globalId, int parentId) {
    super.explainPlan(explainPlanRows, globalId, parentId);
  }

  @Override
  public IndexSegment getIndexSegment() {
    return super.getIndexSegment();
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return super.getExecutionStatistics();
  }

  @Override
  protected TransferableBlock getNextBlock() {
    if (_isTerminated) {
      return TransferableBlockUtils.getEndOfStreamTransferableBlock();
    }
    if (!_isRightSetBuilt) {
      // construct a SET with all the right side rows.
      constructRightBlockSet();
    }
    if (_upstreamErrorBlock != null) {
      return _upstreamErrorBlock;
    }
    // UNION each left block with the constructed right block set.
    TransferableBlock leftBlock = _leftChildOperator.nextBlock();
    return constructResultBlockSet(leftBlock);
  }

  protected void constructRightBlockSet() {
    TransferableBlock block = _rightChildOperator.nextBlock();
    while (!block.isEndOfStreamBlock()) {
      if (block.getType() != DataBlock.Type.METADATA) {
        for (Object[] row : block.getContainer()) {
          _rightRowSet.add(new Record(row));
        }
      }
      block = _rightChildOperator.nextBlock();
    }
    if (block.isErrorBlock()) {
      _upstreamErrorBlock = block;
    } else {
      _isRightSetBuilt = true;
    }
  }

  protected TransferableBlock constructResultBlockSet(TransferableBlock leftBlock) {
    List<Object[]> rows = new ArrayList<>();
    // TODO: Other operators keep the first erroneous block, while this keep the last.
    //  We should decide what is what we want to do and be consistent with that.
    if (_upstreamErrorBlock != null || leftBlock.isErrorBlock()) {
      _upstreamErrorBlock = leftBlock;
      return _upstreamErrorBlock;
    }
    if (leftBlock.isSuccessfulEndOfStreamBlock()) {
      return TransferableBlockUtils.getEndOfStreamTransferableBlock();
    }
    for (Object[] row : leftBlock.getContainer()) {
      if (handleRowMatched(row)) {
        rows.add(row);
      }
    }
    return new TransferableBlock(rows, _dataSchema, DataBlock.Type.ROW);
  }

  /**
   * Returns true if the row is matched.
   * Also updates the right row set based on the Operator.
   * @param row
   * @return true if the row is matched.
   */
  protected abstract boolean handleRowMatched(Object[] row);
}
