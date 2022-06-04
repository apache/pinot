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
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.CountAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.MaxAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.MinAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.SumAggregationFunction;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.blocks.BaseDataBlock;
import org.apache.pinot.query.runtime.blocks.DataBlockBuilder;
import org.apache.pinot.query.runtime.blocks.DataBlockUtils;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;


/**
 *
 */
public class AggregateOperator extends BaseOperator<TransferableBlock> {
  private static final String EXPLAIN_NAME = "AGGREGATE_OPERATOR";

  private BaseOperator<TransferableBlock> _inputOperator;
  private List<RexExpression> _aggCalls;
  private List<RexExpression> _groupSet;

  private final AggregationFunction[] _aggregationFunctions;
  private final Map<Integer, Object>[] _groupByResultHolders;
  private final Map<Integer, Object[]> _groupByKeyHolder;

  private DataSchema _dataSchema;
  private boolean _isCumulativeBlockConstructed;

  // TODO: refactor Pinot Reducer code to support the intermediate stage agg operator.
  public AggregateOperator(BaseOperator<TransferableBlock> inputOperator, List<RexExpression> aggCalls,
      List<RexExpression> groupSet) {
    _inputOperator = inputOperator;
    _aggCalls = aggCalls;
    _groupSet = groupSet;

    _aggregationFunctions = new AggregationFunction[_aggCalls.size()];
    _groupByResultHolders = new Map[_aggCalls.size()];
    _groupByKeyHolder = new HashMap<Integer, Object[]>();
    for (int i = 0; i < aggCalls.size(); i++) {
      _aggregationFunctions[i] = (toAggregationFunction(aggCalls.get(i)));
      _groupByResultHolders[i] = new HashMap<Integer, Object>();
    }

    _isCumulativeBlockConstructed = false;
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
      cumulateAggregationBlocks();
      return new TransferableBlock(toResultBlock());
    } catch (Exception e) {
      return DataBlockUtils.getErrorTransferableBlock(e);
    }
  }

  private BaseDataBlock toResultBlock()
      throws IOException {
    if (!_isCumulativeBlockConstructed) {
      List<Object[]> rows = new ArrayList<>(_groupByKeyHolder.size());
      for (Map.Entry<Integer, Object[]> e : _groupByKeyHolder.entrySet()) {
        Object[] row = new Object[_aggregationFunctions.length + _groupSet.size()];
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
        return DataBlockUtils.getEmptyDataBlock(_dataSchema);
      } else {
        return DataBlockBuilder.buildFromRows(rows, _dataSchema);
      }
    } else {
      return DataBlockUtils.getEndOfStreamDataBlock();
    }
  }

  private void cumulateAggregationBlocks() {
    TransferableBlock block = _inputOperator.nextBlock();
    while (!DataBlockUtils.isEndOfStream(block)) {
      BaseDataBlock dataBlock = block.getDataBlock();
      if (_dataSchema == null) {
        _dataSchema = dataBlock.getDataSchema();
      }
      int numRows = dataBlock.getNumberOfRows();
      for (int rowId = 0; rowId < numRows; rowId++) {
        Object[] row = SelectionOperatorUtils.extractRowFromDataTable(dataBlock, rowId);
        Key key = extraRowKey(row, _groupSet);
        int keyHashCode = key.hashCode();
        _groupByKeyHolder.put(keyHashCode, key.getValues());
        for (int i = 0; i < _aggregationFunctions.length; i++) {
          Object currentRes = _groupByResultHolders[i].get(keyHashCode);
          if (currentRes == null) {
            _groupByResultHolders[i].put(keyHashCode, row[i + _groupSet.size()]);
          } else {
            _groupByResultHolders[i].put(keyHashCode,
                merge(_aggCalls.get(i), currentRes, row[i + _groupSet.size()]));
          }
        }
      }
      block = _inputOperator.nextBlock();
    }
  }

  private AggregationFunction toAggregationFunction(RexExpression aggCall) {
    Preconditions.checkState(aggCall instanceof RexExpression.FunctionCall);
    switch (((RexExpression.FunctionCall) aggCall).getFunctionName()) {
      case "$SUM":
      case "$SUM0":
        return new SumAggregationFunction(
            ExpressionContext.forIdentifier(
                ((RexExpression.FunctionCall) aggCall).getFunctionOperands().get(0).toString()));
      case "$COUNT":
        return new CountAggregationFunction();
      case "$MIN":
      case "$MIN0":
        return new MinAggregationFunction(
            ExpressionContext.forIdentifier(
                ((RexExpression.FunctionCall) aggCall).getFunctionOperands().get(0).toString()));
      case "$MAX":
      case "$MAX0":
        return new MaxAggregationFunction(
            ExpressionContext.forIdentifier(
                ((RexExpression.FunctionCall) aggCall).getFunctionOperands().get(0).toString()));
      default:
        throw new IllegalStateException(
            "Unexpected value: " + ((RexExpression.FunctionCall) aggCall).getFunctionName());
    }
  }

  private Object merge(RexExpression aggCall, Object left, Object right) {
    Preconditions.checkState(aggCall instanceof RexExpression.FunctionCall);
    switch (((RexExpression.FunctionCall) aggCall).getFunctionName()) {
      case "$SUM":
      case "$SUM0":
        return (double) left + (double) right;
      case "$COUNT":
        return (int) left + (int) right;
      case "$MIN":
      case "$MIN0":
        return Math.min((double) left, (double) right);
      case "$MAX":
      case "$MAX0":
        return Math.max((double) left, (double) right);
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
