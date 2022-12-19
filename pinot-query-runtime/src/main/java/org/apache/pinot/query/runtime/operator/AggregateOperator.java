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
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.operator.BaseOperator;
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
 */
public class AggregateOperator extends BaseOperator<TransferableBlock> {
  private static final String EXPLAIN_NAME = "AGGREGATE_OPERATOR";

  private final Operator<TransferableBlock> _inputOperator;
  // TODO: Deal with the case where _aggCalls is empty but we have groupSet setup, which means this is a Distinct call.
  private final List<RexExpression.FunctionCall> _aggCalls;
  private final List<RexExpression> _groupSet;

  private final DataSchema _resultSchema;
  private final Accumulator[] _accumulators;
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
    this(inputOperator, dataSchema, aggCalls, groupSet, AggregateOperator.Accumulator.MERGERS);
  }

  @VisibleForTesting
  AggregateOperator(Operator<TransferableBlock> inputOperator, DataSchema dataSchema,
      List<RexExpression> aggCalls, List<RexExpression> groupSet, Map<String, Merger> mergers) {
    _inputOperator = inputOperator;
    _groupSet = groupSet;
    _upstreamErrorBlock = null;

    // we expect all agg calls to be aggregate function calls
    _aggCalls = aggCalls.stream()
        .map(RexExpression.FunctionCall.class::cast)
        .collect(Collectors.toList());

    _accumulators = new Accumulator[_aggCalls.size()];
    for (int i = 0; i < _aggCalls.size(); i++) {
      RexExpression.FunctionCall agg = _aggCalls.get(i);
      String functionName = agg.getFunctionName();
      if (!mergers.containsKey(functionName)) {
        throw new IllegalStateException("Unexpected value: " + functionName);
      }
      _accumulators[i] = new Accumulator(agg, mergers.get(functionName));
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
      for (int i = 0; i < _accumulators.length; i++) {
        row[i + _groupSet.size()] = _accumulators[i]._results.get(e.getKey());
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
    while (!block.isNoOpBlock()) {
      // setting upstream error block
      if (block.isErrorBlock()) {
        _upstreamErrorBlock = block;
        return true;
      } else if (block.isEndOfStreamBlock()) {
        _readyToConstruct = true;
        return true;
      }

      List<Object[]> container = block.getContainer();
      for (Object[] row : container) {
        Key key = extraRowKey(row, _groupSet);
        _groupByKeyHolder.put(key, key.getValues());
        for (int i = 0; i < _aggCalls.size(); i++) {
          _accumulators[i].accumulate(key, row);
        }
      }
      block = _inputOperator.nextBlock();
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

  private static Boolean mergeBoolAnd(Object left, Object right) {
    return ((Boolean) left) && ((Boolean) right);
  }

  private static Boolean mergeBoolOr(Object left, Object right) {
    return ((Boolean) left) || ((Boolean) right);
  }

  private static Key extraRowKey(Object[] row, List<RexExpression> groupSet) {
    Object[] keyElements = new Object[groupSet.size()];
    for (int i = 0; i < groupSet.size(); i++) {
      keyElements[i] = row[((RexExpression.InputRef) groupSet.get(i)).getIndex()];
    }
    return new Key(keyElements);
  }

  interface Merger extends BiFunction<Object, Object, Object> {
  }

  private static class Accumulator {

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
        .put("BOOL_AND", AggregateOperator::mergeBoolAnd)
        .put("$BOOL_AND", AggregateOperator::mergeBoolAnd)
        .put("$BOOL_AND0", AggregateOperator::mergeBoolAnd)
        .put("BOOL_OR", AggregateOperator::mergeBoolOr)
        .put("$BOOL_OR", AggregateOperator::mergeBoolOr)
        .put("$BOOL_OR0", AggregateOperator::mergeBoolOr)
        .build();

    final int _inputRef;
    final Object _literal;
    final Map<Key, Object> _results = new HashMap<>();
    final Merger _merger;

    Accumulator(RexExpression.FunctionCall aggCall, Merger merger) {
      _merger = merger;
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

    void accumulate(Key key, Object[] row) {
      Map<Key, Object> keys = _results;

      // TODO: fix that single agg result (original type) has different type from multiple agg results (double).
      Object currentRes = keys.get(key);
      Object value = _inputRef == -1 ? _literal : row[_inputRef];

      if (currentRes == null) {
        keys.put(key, value);
      } else {
        Object mergedResult = _merger.apply(currentRes, value);
        _results.put(key, mergedResult);
      }
    }

    private RexExpression toAggregationFunctionOperand(RexExpression.FunctionCall rexExpression) {
      List<RexExpression> functionOperands = rexExpression.getFunctionOperands();
      Preconditions.checkState(functionOperands.size() < 2, "aggregate functions cannot have more than one operand");
      return functionOperands.size() > 0
          ? functionOperands.get(0)
          : new RexExpression.Literal(FieldSpec.DataType.INT, 1);
    }
  }
}
