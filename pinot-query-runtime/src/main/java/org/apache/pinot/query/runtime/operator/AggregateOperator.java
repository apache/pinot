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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.DataBlockUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.RoaringBitmap;


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
 */
public class AggregateOperator extends BaseOperator<TransferableBlock> {

  interface Merger extends BiFunction<Object, Object, Object> {
  }

  private static final String EXPLAIN_NAME = "AGGREGATE_OPERATOR";
  private static final Map<String, Merger> MERGERS = ImmutableMap
      .<String, Merger>builder()
      .put("SUM", AggregateOperator::mergeSum)
      .put("$SUM", AggregateOperator::mergeSum)
      .put("$SUM0", AggregateOperator::mergeSum)
      .put("MIN", AggregateOperator::mergeMin)
      .put("$MIN", AggregateOperator::mergeMin)
      .put("$MIN0", AggregateOperator::mergeMin)
      .put("MAX", AggregateOperator::mergeMax)
      .put("$MAX", AggregateOperator::mergeMax)
      .put("$MAX0", AggregateOperator::mergeMax)
      .put("COUNT", AggregateOperator::mergeCount)
      .build();

  private final Map<String, Merger> _mergers;
  private final Operator<TransferableBlock> _inputOperator;
  // TODO: Deal with the case where _aggCalls is empty but we have groupSet setup, which means this is a Distinct call.
  private final List<RexExpression.FunctionCall> _aggCalls;
  private final List<RexExpression> _groupSet;

  private final DataSchema _resultSchema;
  private final Holder[] _holders;
  private final Map<Key, Object[]> _groupByKeyHolder;
  private TransferableBlock _upstreamErrorBlock;

  private boolean _readyToConstruct;
  private boolean _hasReturnedAggregateBlock;

  // TODO: refactor Pinot Reducer code to support the intermediate stage agg operator.
  // aggCalls has to be a list of FunctionCall and cannot be null
  // groupSet has to be a list of InputRef and cannot be null
  // TODO: Add these two checks when we confirm we can handle error in upstream ctor call.
  public AggregateOperator(Operator<TransferableBlock> inputOperator, DataSchema dataSchema,
      List<RexExpression> aggCalls, List<RexExpression> groupSet) {
    this(inputOperator, dataSchema, aggCalls, groupSet, MERGERS);
  }

  @VisibleForTesting
  AggregateOperator(Operator<TransferableBlock> inputOperator, DataSchema dataSchema,
      List<RexExpression> aggCalls, List<RexExpression> groupSet, Map<String, Merger> mergers) {
    _inputOperator = inputOperator;
    _groupSet = groupSet;
    _upstreamErrorBlock = null;
    _mergers = mergers;

    // we expect all agg calls to be aggregate function calls
    _aggCalls = aggCalls.stream()
        .map(RexExpression.FunctionCall.class::cast)
        .collect(Collectors.toList());

    _holders = new Holder[_aggCalls.size()];
    for (int i = 0; i < _aggCalls.size(); i++) {
      RexExpression.FunctionCall agg = _aggCalls.get(i);
      _holders[i] = new Holder(agg, mergers);
    }

    _groupByKeyHolder = new HashMap<>();
    _resultSchema = dataSchema;
    _readyToConstruct = false;
    _hasReturnedAggregateBlock = false;
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
      if (!_readyToConstruct && !consumeInputBlocks()) {
        return TransferableBlockUtils.getNoOpTransferableBlock();
      }

      if (_upstreamErrorBlock != null) {
        return _upstreamErrorBlock;
      }

      if (!_hasReturnedAggregateBlock) {
        return produceAggregatedBlock();
      } else {
        return TransferableBlockUtils.getEndOfStreamTransferableBlock();
      }
    } catch (Exception e) {
      return TransferableBlockUtils.getErrorTransferableBlock(e);
    }
  }

  private TransferableBlock produceAggregatedBlock() {
    List<Object[]> rows = new ArrayList<>(_groupByKeyHolder.size());
    for (Map.Entry<Key, Object[]> e : _groupByKeyHolder.entrySet()) {
      Object[] row = new Object[_aggCalls.size() + _groupSet.size()];
      Object[] keyElements = e.getValue();
      System.arraycopy(keyElements, 0, row, 0, keyElements.length);
      for (int i = 0; i < _holders.length; i++) {
        row[i + _groupSet.size()] = _holders[i]._result.get(e.getKey());
      }
      rows.add(row);
    }
    _hasReturnedAggregateBlock = true;
    if (rows.size() == 0) {
      return TransferableBlockUtils.getEndOfStreamTransferableBlock();
    } else {
      return new TransferableBlock(rows, _resultSchema, DataBlock.Type.ROW);
    }
  }

  /**
   * @return whether or not the operator is ready to move on (EOS or ERROR)
   */
  private boolean consumeInputBlocks() {
    TransferableBlock block = _inputOperator.nextBlock();
    // setting upstream error block
    if (block.isErrorBlock()) {
      _upstreamErrorBlock = block;
      return true;
    } else if (block.isEndOfStreamBlock()) {
      _readyToConstruct = true;
      return true;
    }

    DataBlock dataBlock = block.getDataBlock();
    int numRows = dataBlock.getNumberOfRows();
    if (numRows > 0) {
      RoaringBitmap[] nullBitmaps = DataBlockUtils.extractNullBitmaps(dataBlock);
      for (int rowId = 0; rowId < numRows; rowId++) {
        Object[] row = DataBlockUtils.extractRowFromDataBlock(dataBlock, rowId,
            dataBlock.getDataSchema().getColumnDataTypes(), nullBitmaps);
        Key key = extraRowKey(row, _groupSet);
        _groupByKeyHolder.put(key, key.getValues());
        for (int i = 0; i < _aggCalls.size(); i++) {
          Map<Key, Object> keys = _holders[i]._result;

          // TODO: fix that single agg result (original type) has different type from multiple agg results (double).
          Object currentRes = keys.get(key);
          if (currentRes == null) {
            keys.put(key, _holders[i].getValue(row));
          } else {
            Merger merger = _mergers.get(_aggCalls.get(i).getFunctionName());
            Object mergedResult = merger.apply(currentRes, _holders[i].getValue(row));
            _holders[i]._result.put(key, mergedResult);
          }
        }
      }
    }
    return false;
  }

  private static Object mergeSum(Object left, Object right) {
    return ((Number) left).doubleValue() + ((Number) right).doubleValue();
  }

  private static Object mergeMin(Object left, Object right) {
    return Math.min(((Number) left).doubleValue(), ((Number) right).doubleValue());
  }

  private static Object mergeMax(Object left, Object right) {
    return Math.max(((Number) left).doubleValue(), ((Number) right).doubleValue());
  }

  private static Object mergeCount(Object left, Object ignored) {
    // TODO: COUNT(*) doesn't need to parse right object until we support NULL
    return ((Number) left).doubleValue() + 1;
  }

  private static Key extraRowKey(Object[] row, List<RexExpression> groupSet) {
    Object[] keyElements = new Object[groupSet.size()];
    for (int i = 0; i < groupSet.size(); i++) {
      keyElements[i] = row[((RexExpression.InputRef) groupSet.get(i)).getIndex()];
    }
    return new Key(keyElements);
  }

  private static class Holder {
    final int _inputRef;
    final Object _literal;
    final Map<Key, Object> _result = new HashMap<>();

    Holder(RexExpression.FunctionCall aggCall, Map<String, Merger> mergers) {
      if (!mergers.containsKey(aggCall.getFunctionName())) {
        throw new IllegalStateException("Unexpected value: " + aggCall.getFunctionName());
      }

      // agg function operand should either be a InputRef or a Literal
      RexExpression rexExpression = toAggregationFunctionOperand(aggCall);
      if (rexExpression instanceof RexExpression.InputRef) {
        _inputRef = ((RexExpression.InputRef) rexExpression).getIndex();
        _literal = null;
      } else {
        _inputRef = -1;
        _literal = ((RexExpression.Literal) rexExpression).getValue();
      }
    }

    private RexExpression toAggregationFunctionOperand(RexExpression.FunctionCall rexExpression) {
      List<RexExpression> functionOperands = rexExpression.getFunctionOperands();
      Preconditions.checkState(functionOperands.size() < 2, "aggregate functions cannot have more than one operand");
      return functionOperands.size() > 0
          ? functionOperands.get(0)
          : new RexExpression.Literal(FieldSpec.DataType.INT, 1);
    }

    Object getValue(Object[] row) {
      return _inputRef == -1 ? _literal : row[_inputRef];
    }
  }
}
