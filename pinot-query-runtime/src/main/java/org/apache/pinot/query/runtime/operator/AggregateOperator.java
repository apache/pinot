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
import javax.annotation.RegEx;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.commons.lang3.ArrayUtils;
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
 * This class is not thread safe.
 *
 * AggregateOperator is used to aggregate values over a set of group by keys.
 * Output data will be in the format of [group by key, aggregate result1, ... aggregate resultN]
 * Currently, we only support SUM/COUNT/MIN/MAX aggregation.
 *
 * When the list of aggregation calls is empty, this class is used to calculate distinct result based on group by keys.
 * In this case, the input can be any type.
 *
 * If the list of aggregation calls is not empty, the input of aggregation has to be a number.
 * Note: This class performs aggregation over the double value of input.
 */
public class AggregateOperator extends BaseOperator<TransferableBlock> {
  private static final String EXPLAIN_NAME = "AGGREGATE_OPERATOR";
  private Operator<TransferableBlock> _inputOperator;
  // TODO: Deal with the case where _aggCalls is empty but we have groupSet setup, which means this is a Distinct call.
  private List<RexExpression.FunctionCall> _aggCalls;
  private List<RexExpression.InputRef> _groupSet;
  private final DataSchema _resultSchema;

  // Mapping from group by key to list of aggregation results.
  private final Map<Key, Object[]> _groupByResultHolders = new HashMap<>();
  private TransferableBlock _upstreamErrorBlock = null;
  private boolean _isCumulativeBlockConstructed = false;

  private static Object getAggOperand(@Nullable RexExpression op, Object[] row){
    if(op == null){
      return 1;
    }
    if(op instanceof RexExpression.InputRef) {
      return row[((RexExpression.InputRef) op).getIndex()];
    }
    return ((RexExpression.Literal) op).getValue();
  }

  // TODO: refactor Pinot Reducer code to support the intermediate stage agg operator.
  // aggCalls has to be a list of FunctionCall and cannot be null
  // groupSet has to be a list of InputRef and cannot be null
  public AggregateOperator(Operator<TransferableBlock> inputOperator, DataSchema dataSchema,
      List<RexExpression> aggCalls, List<RexExpression> groupSet) {
    _inputOperator = inputOperator;
    Preconditions.checkState(_aggCalls != null
        && _aggCalls.isEmpty() && _groupSet !=null &&_groupSet.isEmpty());
    _aggCalls = new ArrayList<>(aggCalls.size());
    for(RexExpression exp: aggCalls){
      Preconditions.checkState(exp instanceof RexExpression.FunctionCall);
      _aggCalls.add((RexExpression.FunctionCall) exp);
    }
    _groupSet = new ArrayList<>(groupSet.size());
    for(RexExpression exp: groupSet){
      Preconditions.checkState(exp instanceof RexExpression.InputRef);
      _groupSet.add((RexExpression.InputRef) exp);
    }
    _resultSchema = dataSchema;
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
  protected TransferableBlock getNextBlock() throws IllegalStateException {
    if(_isCumulativeBlockConstructed){
      return TransferableBlockUtils.getEndOfStreamTransferableBlock(_resultSchema);
    }
    try {
      consumeInputBlocks();
    } catch (Exception e){
      return TransferableBlockUtils.getErrorTransferableBlock(e);
    }
    if (_upstreamErrorBlock != null) {
      return _upstreamErrorBlock;
    }
    return produceAggregatedBlock();
  }

  private TransferableBlock produceAggregatedBlock() {
    _isCumulativeBlockConstructed = true;
    List<Object[]> rows = new ArrayList<>(_groupByResultHolders.size());
    // Each row is written in the format of [group by keys, aggregate result1, ...aggregate resultN]
    for (Map.Entry<Key, Object[]> e : _groupByResultHolders.entrySet()) {
      Object[] keyElements = e.getKey().getValues();
      Object[] groupByResult = e.getValue();
      Object[] row = ArrayUtils.addAll(keyElements, groupByResult);
      rows.add(row);
    }
    if (rows.size() == 0) {
      return TransferableBlockUtils.getEndOfStreamTransferableBlock(_resultSchema);
    }
    return new TransferableBlock(rows, _resultSchema, BaseDataBlock.Type.ROW);
  }

  private void consumeInputBlocks() throws IllegalStateException {
    TransferableBlock block = _inputOperator.nextBlock();
    while (!TransferableBlockUtils.isEndOfStream(block)) {
      BaseDataBlock dataBlock = block.getDataBlock();
      int numRows = dataBlock.getNumberOfRows();
      for (int rowId = 0; rowId < numRows; rowId++) {
          Object[] row = SelectionOperatorUtils.extractRowFromDataTable(dataBlock, rowId);
          Key key = extraRowKey(row, _groupSet);
          Object[] aggResults = _groupByResultHolders.computeIfAbsent(key, k-> new Object[_aggCalls.size()]);
          for (int i = 0; i < _aggCalls.size(); i++) {
            List<RexExpression> functionOperands = _aggCalls.get(i).getFunctionOperands();
            Preconditions.checkState(functionOperands.size() < 2);
            RexExpression op = functionOperands.isEmpty()? null: functionOperands.get(0);
            Object input = getAggOperand(op, row);
            if (aggResults[i] == null) {
              aggResults[i] = input;
            } else {
              Preconditions.checkState(aggResults[i] instanceof Number && input instanceof Number);
              aggResults[i] = merge(_aggCalls.get(i).getFunctionName(), (Number) aggResults[i], (Number) input);
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

  private Object merge(String functionName, Number left, Number right) {
    switch (functionName) {
      case "SUM":
      case "$SUM":
      case "$SUM0":
        return left.doubleValue() + right.doubleValue();
      case "MIN":
      case "$MIN":
      case "$MIN0":
        return Math.min(left.doubleValue(), right.doubleValue());
      case "MAX":
      case "$MAX":
      case "$MAX0":
        return Math.max(left.doubleValue(), right.doubleValue());
      // COUNT(*) doesn't need to parse right object.
      case "COUNT":
        return left.doubleValue() + 1;
      default:
        throw new IllegalStateException("Unexpected functionName in aggregateOperator: " + functionName);
    }
  }

  private static Key extraRowKey(Object[] row, List<RexExpression.InputRef> groupSet) {
    Object[] keyElements = new Object[groupSet.size()];
    for (int i = 0; i < groupSet.size(); i++) {
      keyElements[i] = row[groupSet.get(i).getIndex()];
    }
    return new Key(keyElements);
  }
}
