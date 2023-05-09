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
import org.apache.pinot.query.runtime.operator.utils.AggregationUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.segment.local.customobject.PinotFourthMoment;
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
  private final AggregateAccumulator[] _accumulators;
  private final Map<Key, Object[]> _groupByKeyHolder;
  private TransferableBlock _upstreamErrorBlock;

  private boolean _readyToConstruct;
  private boolean _hasReturnedAggregateBlock;

  // TODO: refactor Pinot Reducer code to support the intermediate stage agg operator.
  // aggCalls has to be a list of FunctionCall and cannot be null
  // groupSet has to be a list of InputRef and cannot be null
  // TODO: Add these two checks when we confirm we can handle error in upstream ctor call.
  public AggregateOperator(OpChainExecutionContext context, MultiStageOperator inputOperator, DataSchema dataSchema,
      List<RexExpression> aggCalls, List<RexExpression> groupSet, DataSchema inputSchema) {
    this(context, inputOperator, dataSchema, aggCalls, groupSet, inputSchema,
        AggregateOperator.AggregateAccumulator.AGG_MERGERS);
  }

  @VisibleForTesting
  AggregateOperator(OpChainExecutionContext context, MultiStageOperator inputOperator, DataSchema dataSchema,
      List<RexExpression> aggCalls, List<RexExpression> groupSet, DataSchema inputSchema,
      Map<String, Function<DataSchema.ColumnDataType, AggregationUtils.Merger>> mergers) {
    super(context);
    _inputOperator = inputOperator;
    _groupSet = groupSet;
    _upstreamErrorBlock = null;

    // we expect all agg calls to be aggregate function calls
    _aggCalls = aggCalls.stream().map(RexExpression.FunctionCall.class::cast).collect(Collectors.toList());

    _accumulators = new AggregateAccumulator[_aggCalls.size()];
    for (int i = 0; i < _aggCalls.size(); i++) {
      RexExpression.FunctionCall agg = _aggCalls.get(i);
      String functionName = agg.getFunctionName();
      if (!mergers.containsKey(functionName)) {
        throw new IllegalStateException("Unexpected value: " + functionName);
      }
      _accumulators[i] = new AggregateAccumulator(agg, mergers, functionName, inputSchema);
    }

    _groupByKeyHolder = new HashMap<>();
    _resultSchema = dataSchema;
    _readyToConstruct = false;
    _hasReturnedAggregateBlock = false;
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return ImmutableList.of(_inputOperator);
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
        // TODO: Move to close call.
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
        row[i + _groupSet.size()] = _accumulators[i].getResults().get(e.getKey());
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
      return new TransferableBlock(rows, _resultSchema, DataBlock.Type.ROW);
    }
  }

  /**
   * @return an empty agg result block for non-group-by aggregation.
   */
  private TransferableBlock constructEmptyAggResultBlock() {
    Object[] row = new Object[_aggCalls.size()];
    for (int i = 0; i < _accumulators.length; i++) {
      row[i] = _accumulators[i].getMerger().initialize(null, _accumulators[i].getDataType());
    }
    return new TransferableBlock(Collections.singletonList(row), _resultSchema, DataBlock.Type.ROW);
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
        Key key = AggregationUtils.extractRowKey(row, _groupSet);
        _groupByKeyHolder.put(key, key.getValues());
        for (int i = 0; i < _aggCalls.size(); i++) {
          _accumulators[i].accumulate(key, row);
        }
      }
      block = _inputOperator.nextBlock();
    }
    return false;
  }

  // NOTE: the below two classes are needed depending on where the
  // fourth moment is being executed - if the leaf stage gets a
  // fourth moment pushed down to it, it will return a PinotFourthMoment
  // as the result of the aggregation. If it is not possible (e.g. the
  // input to the aggregate requires the result of a JOIN - such as
  // FOURTHMOMENT(t1.a + t2.a)) then the input to the aggregate in the
  // intermediate stage is a numeric.

  private static class MergeFourthMomentNumeric implements AggregationUtils.Merger {

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

  private static class MergeFourthMomentObject implements AggregationUtils.Merger {

    @Override
    public Object merge(Object left, Object right) {
      PinotFourthMoment agg = (PinotFourthMoment) left;
      agg.combine((PinotFourthMoment) right);
      return agg;
    }
  }

  private static class MergeCountDistinctScalars implements AggregationUtils.Merger {
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

  private static class MergeCountDistinctSets implements AggregationUtils.Merger {

    @SuppressWarnings("unchecked")
    @Override
    public Object merge(Object agg, Object value) {
      // TODO: this casts everything to `Set<?>` instead of using the primitive version (e.g. IntSet)
      ((Set<Object>) agg).addAll((Set<Object>) value);
      return agg;
    }
  }

  private static class AggregateAccumulator extends AggregationUtils.Accumulator {
    private static final Map<String, Function<DataSchema.ColumnDataType, AggregationUtils.Merger>> AGG_MERGERS =
        ImmutableMap.<String, Function<DataSchema.ColumnDataType, AggregationUtils.Merger>>builder()
            .putAll(AggregationUtils.Accumulator.MERGERS)
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

    AggregateAccumulator(RexExpression.FunctionCall aggCall, Map<String,
        Function<DataSchema.ColumnDataType, AggregationUtils.Merger>> merger, String functionName,
        DataSchema inputSchema) {
      super(aggCall, merger, functionName, inputSchema);
    }
  }
}
