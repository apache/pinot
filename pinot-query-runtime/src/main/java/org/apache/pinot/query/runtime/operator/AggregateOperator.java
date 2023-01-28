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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.segment.local.customobject.PinotFourthMoment;
import org.apache.pinot.spi.data.FieldSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
public class AggregateOperator extends MultiStageOperator {
  private static final String EXPLAIN_NAME = "AGGREGATE_OPERATOR";
  private static final Logger LOGGER = LoggerFactory.getLogger(AggregateOperator.class);

  private final MultiStageOperator _inputOperator;

  // TODO: Deal with the case where _aggCalls is empty but we have groupSet setup, which means this is a Distinct call.
  private final List<RexExpression.FunctionCall> _aggCalls;
  private final List<RexExpression> _groupSet;

  private final DataSchema _resultSchema;
  private final Accumulator[] _accumulators;
  private final Map<Key, Object[]> _groupByKeyHolder;
  private TransferableBlock _upstreamErrorBlock;

  private boolean _readyToConstruct;
  private boolean _hasReturnedAggregateBlock;

  // TODO: Move to OperatorContext class.
  private OperatorStats _operatorStats;

  // TODO: refactor Pinot Reducer code to support the intermediate stage agg operator.
  // aggCalls has to be a list of FunctionCall and cannot be null
  // groupSet has to be a list of InputRef and cannot be null
  // TODO: Add these two checks when we confirm we can handle error in upstream ctor call.
  public AggregateOperator(MultiStageOperator inputOperator, DataSchema dataSchema, List<RexExpression> aggCalls,
      List<RexExpression> groupSet, DataSchema inputSchema, long requestId, int stageId) {
    this(inputOperator, dataSchema, aggCalls, groupSet, inputSchema, AggregateOperator.Accumulator.MERGERS, requestId,
        stageId);
  }

  @VisibleForTesting
  AggregateOperator(MultiStageOperator inputOperator, DataSchema dataSchema, List<RexExpression> aggCalls,
      List<RexExpression> groupSet, DataSchema inputSchema,
      Map<String, Function<DataSchema.ColumnDataType, Merger>> mergers, long requestId, int stageId) {
    _inputOperator = inputOperator;
    _groupSet = groupSet;
    _upstreamErrorBlock = null;

    // we expect all agg calls to be aggregate function calls
    _aggCalls = aggCalls.stream().map(RexExpression.FunctionCall.class::cast).collect(Collectors.toList());

    _accumulators = new Accumulator[_aggCalls.size()];
    for (int i = 0; i < _aggCalls.size(); i++) {
      RexExpression.FunctionCall agg = _aggCalls.get(i);
      String functionName = agg.getFunctionName();
      if (!mergers.containsKey(functionName)) {
        throw new IllegalStateException("Unexpected value: " + functionName);
      }
      _accumulators[i] = new Accumulator(agg, mergers, functionName, inputSchema);
    }

    _groupByKeyHolder = new HashMap<>();
    _resultSchema = dataSchema;
    _readyToConstruct = false;
    _hasReturnedAggregateBlock = false;
    _operatorStats = new OperatorStats(requestId, stageId, EXPLAIN_NAME);
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return ImmutableList.of(_inputOperator);
  }

  @Nullable
  @Override
  public String toExplainString() {
    // TODO: move to close call;
    _inputOperator.toExplainString();
    LOGGER.debug(_operatorStats.toString());
    return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    _operatorStats.startTimer();
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
        // TODO: Move to close call.
        return TransferableBlockUtils.getEndOfStreamTransferableBlock();
      }
    } catch (Exception e) {
      return TransferableBlockUtils.getErrorTransferableBlock(e);
    } finally {
      _operatorStats.endTimer();
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
      if (_groupSet.size() == 0) {
        return constructEmptyAggResultBlock();
      } else {
        return TransferableBlockUtils.getEndOfStreamTransferableBlock();
      }
    } else {
      _operatorStats.recordOutput(1, rows.size());
      return new TransferableBlock(rows, _resultSchema, DataBlock.Type.ROW);
    }
  }

  /**
   * @return an empty agg result block for non-group-by aggregation.
   */
  private TransferableBlock constructEmptyAggResultBlock() {
    Object[] row = new Object[_aggCalls.size()];
    for (int i = 0; i < _accumulators.length; i++) {
      row[i] = _accumulators[i]._merger.initialize(null, _accumulators[i]._dataType);
    }
    return new TransferableBlock(Collections.singletonList(row), _resultSchema, DataBlock.Type.ROW);
  }

  /**
   * @return whether or not the operator is ready to move on (EOS or ERROR)
   */
  private boolean consumeInputBlocks() {
    _operatorStats.endTimer();
    TransferableBlock block = _inputOperator.nextBlock();
    _operatorStats.startTimer();
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
      _operatorStats.recordInput(1, container.size());
      _operatorStats.endTimer();
      block = _inputOperator.nextBlock();
      _operatorStats.startTimer();
    }
    return false;
  }

  private static Key extraRowKey(Object[] row, List<RexExpression> groupSet) {
    Object[] keyElements = new Object[groupSet.size()];
    for (int i = 0; i < groupSet.size(); i++) {
      keyElements[i] = row[((RexExpression.InputRef) groupSet.get(i)).getIndex()];
    }
    return new Key(keyElements);
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

  // NOTE: the below two classes are needed depending on where the
  // fourth moment is being executed - if the leaf stage gets a
  // fourth moment pushed down to it, it will return a PinotFourthMoment
  // as the result of the aggregation. If it is not possible (e.g. the
  // input to the aggregate requires the result of a JOIN - such as
  // FOURTHMOMENT(t1.a + t2.a)) then the input to the aggregate in the
  // intermediate stage is a numeric.

  private static class MergeFourthMomentNumeric implements Merger {

    @Override
    public Object merge(Object left, Object right) {
      ((PinotFourthMoment) left).increment(((Number) right).doubleValue());
      return left;
    }

    @Override
    public Object initialize(Object other, DataSchema.ColumnDataType dataType) {
      PinotFourthMoment moment = new PinotFourthMoment();
      moment.increment(((Number) other).doubleValue());
      return moment;
    }
  }

  private static class MergeFourthMomentObject implements Merger {

    @Override
    public Object merge(Object left, Object right) {
      PinotFourthMoment agg = (PinotFourthMoment) left;
      agg.combine((PinotFourthMoment) right);
      return agg;
    }
  }

  private static class MergeCountDistinctScalars implements Merger {
    @SuppressWarnings("unchecked")
    @Override
    public Object merge(Object agg, Object value) {
      // TODO: this casts everything to `Set<?>` instead of using the primitive version (e.g. IntSet)
      ((Set<Object>) agg).add(value);
      return agg;
    }

    @Override
    public Object initialize(Object other, DataSchema.ColumnDataType dataType) {
      ObjectOpenHashSet<Object> set = new ObjectOpenHashSet<>();
      set.add(other);
      return set;
    }
  }

  private static class MergeCountDistinctSets implements Merger {

    @SuppressWarnings("unchecked")
    @Override
    public Object merge(Object agg, Object value) {
      // TODO: this casts everything to `Set<?>` instead of using the primitive version (e.g. IntSet)
      ((Set<Object>) agg).addAll((Set<Object>) value);
      return agg;
    }
  }

  interface Merger {
    /**
     * Initializes the merger based on the first input
     */
    default Object initialize(Object other, DataSchema.ColumnDataType dataType) {
      return other == null ? dataType.getNullPlaceholder() : other;
    }

    /**
     * Merges the existing aggregate (the result of {@link #initialize(Object, DataSchema.ColumnDataType)}) with
     * the new value coming in (which may be an aggregate in and of itself).
     */
    Object merge(Object agg, Object value);
  }

  private static class Accumulator {
    private static final Map<String, Function<DataSchema.ColumnDataType, Merger>> MERGERS =
        ImmutableMap.<String, Function<DataSchema.ColumnDataType, Merger>>builder()
            .put("SUM", cdt -> AggregateOperator::mergeSum).put("$SUM", cdt -> AggregateOperator::mergeSum)
            .put("$SUM0", cdt -> AggregateOperator::mergeSum).put("MIN", cdt -> AggregateOperator::mergeMin)
            .put("$MIN", cdt -> AggregateOperator::mergeMin).put("$MIN0", cdt -> AggregateOperator::mergeMin)
            .put("MAX", cdt -> AggregateOperator::mergeMax).put("$MAX", cdt -> AggregateOperator::mergeMax)
            .put("$MAX0", cdt -> AggregateOperator::mergeMax).put("COUNT", cdt -> AggregateOperator::mergeCount)
            .put("BOOL_AND", cdt -> AggregateOperator::mergeBoolAnd)
            .put("$BOOL_AND", cdt -> AggregateOperator::mergeBoolAnd)
            .put("$BOOL_AND0", cdt -> AggregateOperator::mergeBoolAnd)
            .put("BOOL_OR", cdt -> AggregateOperator::mergeBoolOr)
            .put("$BOOL_OR", cdt -> AggregateOperator::mergeBoolOr)
            .put("$BOOL_OR0", cdt -> AggregateOperator::mergeBoolOr)
            .put("FOURTHMOMENT",
                cdt -> cdt == DataSchema.ColumnDataType.OBJECT ? new MergeFourthMomentObject()
                    : new MergeFourthMomentNumeric())
            .put("$FOURTHMOMENT",
                cdt -> cdt == DataSchema.ColumnDataType.OBJECT ? new MergeFourthMomentObject()
                    : new MergeFourthMomentNumeric())
            .put("$FOURTHMOMENT0",
                cdt -> cdt == DataSchema.ColumnDataType.OBJECT ? new MergeFourthMomentObject()
                    : new MergeFourthMomentNumeric())
            .put("DISTINCTCOUNT", cdt -> cdt == DataSchema.ColumnDataType.OBJECT
                ? new MergeCountDistinctSets() : new MergeCountDistinctScalars())
            .put("$DISTINCTCOUNT", cdt -> cdt == DataSchema.ColumnDataType.OBJECT
                ? new MergeCountDistinctSets() : new MergeCountDistinctScalars())
            .put("$DISTINCTCOUNT0", cdt -> cdt == DataSchema.ColumnDataType.OBJECT
                ? new MergeCountDistinctSets() : new MergeCountDistinctScalars())
            .build();

    final int _inputRef;
    final Object _literal;
    final Map<Key, Object> _results = new HashMap<>();
    final Merger _merger;
    final DataSchema.ColumnDataType _dataType;

    Accumulator(RexExpression.FunctionCall aggCall, Map<String, Function<DataSchema.ColumnDataType, Merger>> merger,
        String functionName, DataSchema inputSchema) {
      // agg function operand should either be a InputRef or a Literal
      RexExpression rexExpression = toAggregationFunctionOperand(aggCall);
      if (rexExpression instanceof RexExpression.InputRef) {
        _inputRef = ((RexExpression.InputRef) rexExpression).getIndex();
        _literal = null;
        _dataType = inputSchema.getColumnDataType(_inputRef);
      } else {
        _inputRef = -1;
        _literal = ((RexExpression.Literal) rexExpression).getValue();
        _dataType = DataSchema.ColumnDataType.fromDataType(rexExpression.getDataType(), true);
      }
      _merger = merger.get(functionName).apply(_dataType);
    }

    void accumulate(Key key, Object[] row) {
      // TODO: fix that single agg result (original type) has different type from multiple agg results (double).
      Object currentRes = _results.get(key);
      Object value = _inputRef == -1 ? _literal : row[_inputRef];

      if (currentRes == null) {
        _results.put(key, _merger.initialize(value, _dataType));
      } else {
        Object mergedResult = _merger.merge(currentRes, value);
        _results.put(key, mergedResult);
      }
    }

    private RexExpression toAggregationFunctionOperand(RexExpression.FunctionCall rexExpression) {
      List<RexExpression> functionOperands = rexExpression.getFunctionOperands();
      Preconditions.checkState(functionOperands.size() < 2, "aggregate functions cannot have more than one operand");
      return functionOperands.size() > 0 ? functionOperands.get(0)
          : new RexExpression.Literal(FieldSpec.DataType.INT, 1);
    }
  }
}
