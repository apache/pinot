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

import java.io.IOException;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.common.datablock.BaseDataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;


public class SortOperator extends BaseOperator<TransferableBlock> {
  private static final String EXPLAIN_NAME = "SORT";
  private final Operator<TransferableBlock> _upstreamOperator;
  private final int _fetch;
  private final int _offset;
  private final DataSchema _dataSchema;
  private final PriorityQueue<Object[]> _rows;
  private final int _numRowsToKeep;

  private boolean _isSortedBlockConstructed;
  private TransferableBlock _upstreamErrorBlock;

  public SortOperator(Operator<TransferableBlock> upstreamOperator, List<RexExpression> collationKeys,
      List<RelFieldCollation.Direction> collationDirections, int fetch, int offset, DataSchema dataSchema) {
    _upstreamOperator = upstreamOperator;
    _fetch = fetch;
    _offset = offset;
    _dataSchema = dataSchema;
    _upstreamErrorBlock = null;
    _isSortedBlockConstructed = false;
    _numRowsToKeep = _fetch > 0 ? Math.min(SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY,
        _fetch + (Math.max(_offset, 0))) : SelectionOperatorUtils.MAX_ROW_HOLDER_INITIAL_CAPACITY;
    _rows = new PriorityQueue<>(_numRowsToKeep,
        new SortComparator(collationKeys, collationDirections, dataSchema, false));
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
      consumeInputBlocks();
      return produceSortedBlock();
    } catch (Exception e) {
      return TransferableBlockUtils.getErrorTransferableBlock(e);
    }
  }

  private TransferableBlock produceSortedBlock()
      throws IOException {
    if (_upstreamErrorBlock != null) {
      return _upstreamErrorBlock;
    }
    if (!_isSortedBlockConstructed) {
      LinkedList<Object[]> rows = new LinkedList<>();
      while (_rows.size() > _offset) {
        Object[] row = _rows.poll();
          rows.addFirst(row);
      }
      _isSortedBlockConstructed = true;
      if (rows.size() == 0) {
        return TransferableBlockUtils.getEndOfStreamTransferableBlock(_dataSchema);
      } else {
        return new TransferableBlock(rows, _dataSchema, BaseDataBlock.Type.ROW);
      }
    } else {
      return TransferableBlockUtils.getEndOfStreamTransferableBlock(_dataSchema);
    }
  }

  private void consumeInputBlocks() {
    if (!_isSortedBlockConstructed) {
      TransferableBlock block = _upstreamOperator.nextBlock();
      while (!TransferableBlockUtils.isEndOfStream(block)) {
        BaseDataBlock dataBlock = block.getDataBlock();
        int numRows = dataBlock.getNumberOfRows();
        for (int rowId = 0; rowId < numRows; rowId++) {
          Object[] row = SelectionOperatorUtils.extractRowFromDataTable(dataBlock, rowId);
          SelectionOperatorUtils.addToPriorityQueue(row, _rows, _numRowsToKeep);
        }
        block = _upstreamOperator.nextBlock();
      }
      // setting upstream error block
      if (block.isErrorBlock()) {
        _upstreamErrorBlock = block;
      }
    }
  }

  private static class SortComparator implements Comparator<Object[]> {
    private final int _size;
    private final int[] _valueIndices;
    private final int[] _multipliers;
    private final boolean[] _useDoubleComparison;

    public SortComparator(List<RexExpression> collationKeys, List<RelFieldCollation.Direction> collationDirections,
        DataSchema dataSchema, boolean isNullHandlingEnabled) {
      DataSchema.ColumnDataType[] columnDataTypes = dataSchema.getColumnDataTypes();
      _size = collationKeys.size();
      _valueIndices = new int[_size];
      _multipliers = new int[_size];
      _useDoubleComparison = new boolean[_size];
      for (int i = 0; i < _size; i++) {
        _valueIndices[i] = ((RexExpression.InputRef) collationKeys.get(i)).getIndex();
        _multipliers[i] = collationDirections.get(i).isDescending() ? 1 : -1;
        _useDoubleComparison[i] = columnDataTypes[_valueIndices[i]].isNumber();
      }
    }

    @Override
    public int compare(Object[] o1, Object[] o2) {
      for (int i = 0; i < _size; i++) {
        int index = _valueIndices[i];
        Object v1 = o1[index];
        Object v2 = o2[index];
        int result;
        if (_useDoubleComparison[i]) {
          result = Double.compare(((Number) v1).doubleValue(), ((Number) v2).doubleValue());
        } else {
          //noinspection unchecked
          result = ((Comparable) v1).compareTo(v2);
        }
        if (result != 0) {
          return result * _multipliers[i];
        }
      }
      return 0;
    }
  }
}
