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
package org.apache.pinot.query.runtime.operator.set;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;


public abstract class BinarySetOperator extends SetOperator {

  protected final MultiStageOperator _leftChildOperator;
  protected final MultiStageOperator _rightChildOperator;
  protected final Multiset<Record> _rightRowSet;
  private MseBlock.Eos _eos;
  private boolean _isRightChildOperatorProcessed;

  public BinarySetOperator(OpChainExecutionContext opChainExecutionContext,
      List<MultiStageOperator> inputOperators,
      DataSchema dataSchema) {
    super(opChainExecutionContext, inputOperators, dataSchema);
    Preconditions.checkArgument(inputOperators.size() == 2, "Binary set operator should have 2 inputs");
    _leftChildOperator = inputOperators.get(0);
    _rightChildOperator = inputOperators.get(1);
    _rightRowSet = HashMultiset.create();
  }

  /**
   * Processes the right child operator and builds the set of rows that can be used to filter the left child.
   *
   * @return either a data block containing rows or an EoS block, never {@code null}.
   */
  protected MseBlock processRightOperator() {
    MseBlock block = _rightChildOperator.nextBlock();
    while (block.isData()) {
      MseBlock.Data dataBlock = (MseBlock.Data) block;
      for (Object[] row : dataBlock.asRowHeap().getRows()) {
        _rightRowSet.add(new Record(row));
      }
      checkTerminationAndSampleUsage();
      block = _rightChildOperator.nextBlock();
    }
    assert block.isEos();
    return block;
  }

  /**
   * Processes the left child operator and returns blocks of rows that match the criteria defined by the set operation.
   *
   * @return block containing matched rows or EoS, never {@code null}.
   */
  protected MseBlock processLeftOperator() {
    // Keep reading the input blocks until we find a match row or all blocks are processed.
    // TODO: Consider batching the rows to improve performance.
    while (true) {
      MseBlock leftBlock = _leftChildOperator.nextBlock();
      if (leftBlock.isEos()) {
        return leftBlock;
      }
      MseBlock.Data dataBlock = (MseBlock.Data) leftBlock;
      List<Object[]> rows = new ArrayList<>();
      for (Object[] row : dataBlock.asRowHeap().getRows()) {
        if (handleRowMatched(row)) {
          rows.add(row);
        }
      }
      checkTerminationAndSampleUsage();
      if (!rows.isEmpty()) {
        return new RowHeapDataBlock(rows, _dataSchema);
      }
    }
  }

  @Override
  protected MseBlock getNextBlock() {
    if (_eos != null) {
      return _eos;
    }

    if (!_isRightChildOperatorProcessed) {
      MseBlock mseBlock = processRightOperator();

      if (mseBlock.isData()) {
        return mseBlock;
      } else if (mseBlock.isError()) {
        _eos = (MseBlock.Eos) mseBlock;
        return _eos;
      } else if (mseBlock.isSuccess()) {
        // If it's a regular EOS block, we continue to process the left child operator.
        _isRightChildOperatorProcessed = true;
      }
    }

    MseBlock mseBlock = processLeftOperator();
    if (mseBlock.isEos()) {
      _eos = (MseBlock.Eos) mseBlock;
      return _eos;
    } else {
      return mseBlock;
    }
  }

  /**
   * Returns true if the row matches the criteria defined by the set operation.
   * <p>
   * Also updates the right row set based on the operator.
   *
   * @param row the row from the left operator to be checked for matching.
   * @return true if the row is matched.
   */
  protected abstract boolean handleRowMatched(Object[] row);
}
