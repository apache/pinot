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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.datablock.BaseDataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.spi.data.FieldSpec;


/**
 *
 * AggregateOperator is used to aggregate values over a set of group by keys.
 * Output data will be in the format of [group by key, aggregate result1, ... aggregate resultN]
 * Currently, we only support SUM/COUNT/MIN/MAX aggregation.
 *
 * When the list of aggregation calls is empty, this class is used to calculate distinct result based on group by keys.
 * In this case, the input can be any type.
 *
 * If the list of aggregation calls is not empty, the input of aggregation has to be a number.
 *
 * Note: This class performs aggregation over the double value of input.
 * If the input is single value, the output type will be input type. Otherwise, the output type will be double.
 *
 * For inequi join, the join key has to be numeric.
 */
public class AggregateOperator extends BaseOperator<TransferableBlock> {
  private static final String EXPLAIN_NAME = "AGGREGATE_OPERATOR";

  private Operator<TransferableBlock> _inputOperator;
  // TODO: Deal with the case where _aggCalls is empty but we have groupSet setup, which means this is a Distinct call.
  private List<RexExpression> _aggCalls;
  private List<RexExpression> _groupSet;

  private final int[] _aggregationFunctionInputRefs;
  private final Object[] _aggregationFunctionLiterals;
  private final DataSchema _resultSchema;
  private final Map<Key, Object>[] _groupByResultHolders;
  private final Map<Key, Object[]> _groupByKeyHolder;
  private TransferableBlock _upstreamErrorBlock;
  private boolean _isCumulativeBlockConstructed;

  // TODO: refactor Pinot Reducer code to support the intermediate stage agg operator.
  // aggCalls has to be a list of FunctionCall and cannot be null
  // groupSet has to be a list of InputRef and cannot be null
  // TODO: Add these two checks when we confirm we can handle error in upstream ctor call.
  public AggregateOperator(Operator<TransferableBlock> inputOperator, DataSchema dataSchema,
      List<RexExpression> aggCalls, List<RexExpression> groupSet) {
    _inputOperator = inputOperator;
    _aggCalls = aggCalls;
    _groupSet = groupSet;
    _upstreamErrorBlock = null;

    _aggregationFunctionInputRefs = new int[_aggCalls.size()];
    _aggregationFunctionLiterals = new Object[_aggCalls.size()];
    _groupByResultHolders = new Map[_aggCalls.size()];
    _groupByKeyHolder = new HashMap<Key, Object[]>();
    for (int i = 0; i < aggCalls.size(); i++) {
      // agg function operand should either be a InputRef or a Literal
      RexExpression rexExpression = toAggregationFunctionOperand(aggCalls.get(i));
      if (rexExpression instanceof RexExpression.InputRef) {
        _aggregationFunctionInputRefs[i] = ((RexExpression.InputRef) rexExpression).getIndex();
      } else {
        _aggregationFunctionInputRefs[i] = -1;
        _aggregationFunctionLiterals[i] = ((RexExpression.Literal) rexExpression).getValue();
      }
      _groupByResultHolders[i] = new HashMap<Key, Object>();
    }
    _resultSchema = dataSchema;

    _isCumulativeBlockConstructed = false;
  }

  private RexExpression toAggregationFunctionOperand(RexExpression rexExpression) {
    List<RexExpression> functionOperands = ((RexExpression.FunctionCall) rexExpression).getFunctionOperands();
    Preconditions.checkState(functionOperands.size() < 2);
    return functionOperands.size() > 0 ? functionOperands.get(0) : new RexExpression.Literal(FieldSpec.DataType.INT, 1);
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
      return produceAggregatedBlock();
    } catch (Exception e) {
      return TransferableBlockUtils.getErrorTransferableBlock(e);
    }
  }

  private TransferableBlock produceAggregatedBlock()
      throws IOException {
    if (_upstreamErrorBlock != null) {
      return _upstreamErrorBlock;
    }
    if (!_isCumulativeBlockConstructed) {
      List<Object[]> rows = new ArrayList<>(_groupByKeyHolder.size());
      for (Map.Entry<Key, Object[]> e : _groupByKeyHolder.entrySet()) {
        Object[] row = new Object[_aggCalls.size() + _groupSet.size()];
        Object[] keyElements = e.getValue();
        for (int i = 0; i < keyElements.length; i++) {
          row[i] = keyElements[i];
        }
        for (int i = 0; i < _groupByResultHolders.length; i++) {
          row[i + _groupSet.size()] = _groupByResultHolders[i].get(e.getKey());
        }
        rows.add(row);
      }
      _isCumulativeBlockConstructed = true;
      if (rows.size() == 0) {
        return TransferableBlockUtils.getEndOfStreamTransferableBlock(_resultSchema);
      } else {
        return new TransferableBlock(rows, _resultSchema, BaseDataBlock.Type.ROW);
      }
    } else {
      return TransferableBlockUtils.getEndOfStreamTransferableBlock(_resultSchema);
    }
  }

  private void consumeInputBlocks() {
    if (!_isCumulativeBlockConstructed) {
      TransferableBlock block = _inputOperator.nextBlock();
      while (!TransferableBlockUtils.isEndOfStream(block)) {
        BaseDataBlock dataBlock = block.getDataBlock();
        int numRows = dataBlock.getNumberOfRows();
        for (int rowId = 0; rowId < numRows; rowId++) {
          Object[] row = SelectionOperatorUtils.extractRowFromDataTable(dataBlock, rowId);
          Key key = extraRowKey(row, _groupSet);
          _groupByKeyHolder.put(key, key.getValues());
          for (int i = 0; i < _aggCalls.size(); i++) {
            Object currentRes = _groupByResultHolders[i].get(key);
            // TODO: fix that single agg result (original type) has different type from multiple agg results (double).
            if (currentRes == null) {
              _groupByResultHolders[i].put(key, _aggregationFunctionInputRefs[i] == -1 ? _aggregationFunctionLiterals[i]
                  : row[_aggregationFunctionInputRefs[i]]);
            } else {
              _groupByResultHolders[i].put(key, merge(_aggCalls.get(i), currentRes,
                  _aggregationFunctionInputRefs[i] == -1 ? _aggregationFunctionLiterals[i]
                      : row[_aggregationFunctionInputRefs[i]]));
            }
          }
        }
        block = _inputOperator.nextBlock();
      }
      // setting upstream error block
      if (block.isErrorBlock()) {
        _upstreamErrorBlock = block;
      }
    }
  }

  private Object merge(RexExpression aggCall, Object left, Object right) {
    Preconditions.checkState(aggCall instanceof RexExpression.FunctionCall);
    switch (((RexExpression.FunctionCall) aggCall).getFunctionName()) {
      case "SUM":
      case "$SUM":
      case "$SUM0":
        return ((Number) left).doubleValue() + ((Number) right).doubleValue();
      case "MIN":
      case "$MIN":
      case "$MIN0":
        return Math.min(((Number) left).doubleValue(), ((Number) right).doubleValue());
      case "MAX":
      case "$MAX":
      case "$MAX0":
        return Math.max(((Number) left).doubleValue(), ((Number) right).doubleValue());
      // COUNT(*) doesn't need to parse right object.
      case "COUNT":
        return ((Number) left).doubleValue() + 1;
      default:
        throw new IllegalStateException(
            "Unexpected value: " + ((RexExpression.FunctionCall) aggCall).getFunctionName());
    }
  }

  private static Key extraRowKey(Object[] row, List<RexExpression> groupSet) {
    Object[] keyElements = new Object[groupSet.size()];
    for (int i = 0; i < groupSet.size(); i++) {
      keyElements[i] = row[((RexExpression.InputRef) groupSet.get(i)).getIndex()];
    }
    return new Key(keyElements);
  }
}
