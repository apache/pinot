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
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.WindowNode;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.utils.AggregationUtils;
import org.apache.pinot.query.runtime.operator.utils.TypeUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The WindowAggregateOperator is used to compute window function aggregations over a set of optional
 * PARTITION BY keys, ORDER BY keys and a FRAME clause. The output data will include the projected
 * columns and in addition will add the aggregation columns to the output data.
 * [input columns, aggregate result1, ... aggregate resultN]
 *
 * The window functions supported today are:
 * Aggregation: SUM/COUNT/MIN/MAX/AVG/BOOL_OR/BOOL_AND aggregations [RANGE window type only]
 * Ranking: ROW_NUMBER [ROWS window type only], RANK, DENSE_RANK [RANGE window type only] ranking functions
 * Value: [none]
 *
 * Unlike the AggregateOperator which will output one row per group, the WindowAggregateOperator
 * will output as many rows as input rows.
 *
 * For queries using an 'ORDER BY' clause within the 'OVER()', this WindowAggregateOperator expects that the incoming
 * keys are already ordered based on the 'ORDER BY' keys. No ordering is performed in this operator. The planner
 * should handle adding a 'SortExchange' to do the ordering prior to pipelining the data to the upstream operators
 * wherever ordering is required.
 *
 * Note: This class performs aggregation over the double value of input.
 * If the input is single value, the output type will be input type. Otherwise, the output type will be double.
 *
 * TODO:
 *     1. Add support for additional rank window functions
 *     2. Add support for value window functions
 *     3. Add support for custom frames (including ROWS support)
 *     4. Add support for null direction handling (even for PARTITION BY only queries with custom null direction)
 *     5. Add support for multiple window groups (each WindowAggregateOperator should still work on a single group)
 */
public class WindowAggregateOperator extends MultiStageOperator<MultiStageOperator.BaseStatKeys> {
  private static final String EXPLAIN_NAME = "WINDOW";
  private static final Logger LOGGER = LoggerFactory.getLogger(WindowAggregateOperator.class);

  // List of window functions which can only be applied as ROWS window frame type
  private static final Set<String> ROWS_ONLY_FUNCTION_NAMES = ImmutableSet.of("ROW_NUMBER");
  // List of ranking window functions whose output depends on the ordering of input rows and not on the actual values
  private static final Set<String> RANKING_FUNCTION_NAMES = ImmutableSet.of("RANK", "DENSE_RANK");

  private final MultiStageOperator<?> _inputOperator;
  private final List<RexExpression> _groupSet;
  private final OrderSetInfo _orderSetInfo;
  private final WindowFrame _windowFrame;
  private final List<RexExpression.FunctionCall> _aggCalls;
  private final List<RexExpression> _constants;
  private final DataSchema _resultSchema;
  private final WindowAggregateAccumulator[] _windowAccumulators;
  private final Map<Key, List<Object[]>> _partitionRows;
  private final boolean _isPartitionByOnly;

  private int _numRows;
  private boolean _hasReturnedWindowAggregateBlock;

  public WindowAggregateOperator(OpChainExecutionContext context, MultiStageOperator<?> inputOperator,
      List<RexExpression> groupSet, List<RexExpression> orderSet, List<RelFieldCollation.Direction> orderSetDirection,
      List<RelFieldCollation.NullDirection> orderSetNullDirection, List<RexExpression> aggCalls, int lowerBound,
      int upperBound, WindowNode.WindowFrameType windowFrameType, List<RexExpression> constants,
      DataSchema resultSchema, DataSchema inputSchema) {
    this(context, inputOperator, groupSet, orderSet, orderSetDirection, orderSetNullDirection, aggCalls, lowerBound,
        upperBound, windowFrameType, constants, resultSchema, inputSchema, WindowAggregateAccumulator.WIN_AGG_MERGERS);
  }

  @VisibleForTesting
  public WindowAggregateOperator(OpChainExecutionContext context, MultiStageOperator<?> inputOperator,
      List<RexExpression> groupSet, List<RexExpression> orderSet, List<RelFieldCollation.Direction> orderSetDirection,
      List<RelFieldCollation.NullDirection> orderSetNullDirection, List<RexExpression> aggCalls, int lowerBound,
      int upperBound, WindowNode.WindowFrameType windowFrameType, List<RexExpression> constants,
      DataSchema resultSchema, DataSchema inputSchema,
      Map<String, Function<ColumnDataType, AggregationUtils.Merger>> mergers) {
    super(context);

    _inputOperator = inputOperator;
    _groupSet = groupSet;
    _isPartitionByOnly = isPartitionByOnlyQuery(groupSet, orderSet);
    _orderSetInfo = new OrderSetInfo(orderSet, orderSetDirection, orderSetNullDirection, _isPartitionByOnly);
    _windowFrame = new WindowFrame(lowerBound, upperBound, windowFrameType);

    Preconditions.checkState(_windowFrame.isUnboundedPreceding(),
        "Only default frame is supported, lowerBound must be UNBOUNDED PRECEDING");
    Preconditions.checkState(_windowFrame.isUnboundedFollowing() || _windowFrame.isUpperBoundCurrentRow(),
        "Only default frame is supported, upperBound must be UNBOUNDED FOLLOWING or CURRENT ROW");

    // we expect all agg calls to be aggregate function calls
    _aggCalls = aggCalls.stream().map(RexExpression.FunctionCall.class::cast).collect(Collectors.toList());
    _constants = constants;
    _resultSchema = resultSchema;

    _windowAccumulators = new WindowAggregateAccumulator[_aggCalls.size()];
    int aggCallsSize = _aggCalls.size();
    for (int i = 0; i < aggCallsSize; i++) {
      RexExpression.FunctionCall agg = _aggCalls.get(i);
      String functionName = agg.getFunctionName();
      validateAggregationCalls(functionName, mergers);
      _windowAccumulators[i] = new WindowAggregateAccumulator(agg, mergers, functionName, inputSchema, _orderSetInfo);
    }

    _partitionRows = new HashMap<>();

    _numRows = 0;
    _hasReturnedWindowAggregateBlock = false;
  }

  @Override
  public Class<BaseStatKeys> getStatKeyClass() {
    return BaseStatKeys.class;
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  public List<MultiStageOperator<?>> getChildOperators() {
    return ImmutableList.of(_inputOperator);
  }

  @Nullable
  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock() {
    if (_hasReturnedWindowAggregateBlock) {
      return TransferableBlockUtils.getEndOfStreamTransferableBlock();
    }
    TransferableBlock finalBlock = consumeInputBlocks();
    if (finalBlock.isErrorBlock()) {
      return finalBlock;
    }
    return produceWindowAggregatedBlock();
  }

  private void validateAggregationCalls(String functionName,
      Map<String, Function<ColumnDataType, AggregationUtils.Merger>> mergers) {
    if (!mergers.containsKey(functionName)) {
      throw new IllegalStateException("Unexpected aggregation function name: " + functionName);
    }

    if (ROWS_ONLY_FUNCTION_NAMES.contains(functionName)) {
      Preconditions.checkState(
          _windowFrame.getWindowFrameType() == WindowNode.WindowFrameType.ROWS && _windowFrame.isUpperBoundCurrentRow(),
          String.format("%s must be of ROW frame type and have CURRENT ROW as the upper bound", functionName));
    } else {
      Preconditions.checkState(_windowFrame.getWindowFrameType() == WindowNode.WindowFrameType.RANGE,
          String.format("Only RANGE type frames are supported at present for function: %s", functionName));
    }
  }

  private boolean isPartitionByOnlyQuery(List<RexExpression> groupSet, List<RexExpression> orderSet) {
    if (CollectionUtils.isEmpty(orderSet)) {
      return true;
    }

    if (CollectionUtils.isEmpty(groupSet) || (groupSet.size() != orderSet.size())) {
      return false;
    }

    Set<Integer> partitionByInputRefIndexes = new HashSet<>();
    Set<Integer> orderByInputRefIndexes = new HashSet<>();
    int groupSetSize = groupSet.size();
    for (int i = 0; i < groupSetSize; i++) {
      partitionByInputRefIndexes.add(((RexExpression.InputRef) groupSet.get(i)).getIndex());
      orderByInputRefIndexes.add(((RexExpression.InputRef) orderSet.get(i)).getIndex());
    }

    return partitionByInputRefIndexes.equals(orderByInputRefIndexes);
  }

  private TransferableBlock produceWindowAggregatedBlock() {
    Key emptyOrderKey = AggregationUtils.extractEmptyKey();
    ColumnDataType[] resultStoredTypes = _resultSchema.getStoredColumnDataTypes();
    List<Object[]> rows = new ArrayList<>(_numRows);
    if (_windowFrame.getWindowFrameType() == WindowNode.WindowFrameType.RANGE) {
      // All aggregation window functions only support RANGE type today (SUM/AVG/MIN/MAX/COUNT/BOOL_AND/BOOL_OR)
      // RANK and DENSE_RANK ranking window functions also only support RANGE type today
      for (Map.Entry<Key, List<Object[]>> e : _partitionRows.entrySet()) {
        Key partitionKey = e.getKey();
        List<Object[]> rowList = e.getValue();
        for (Object[] existingRow : rowList) {
          Object[] row = new Object[existingRow.length + _aggCalls.size()];
          Key orderKey = (_isPartitionByOnly && CollectionUtils.isEmpty(_orderSetInfo.getOrderSet())) ? emptyOrderKey
              : AggregationUtils.extractRowKey(existingRow, _orderSetInfo.getOrderSet());
          System.arraycopy(existingRow, 0, row, 0, existingRow.length);
          for (int i = 0; i < _windowAccumulators.length; i++) {
            row[i + existingRow.length] = _windowAccumulators[i].getRangeResultForKeys(partitionKey, orderKey);
          }
          // Convert the results from Accumulator to the desired type
          TypeUtils.convertRow(row, resultStoredTypes);
          rows.add(row);
        }
      }
    } else {
      // Only ROW_NUMBER() window function is supported as ROWS type today
      Key previousPartitionKey = null;
      Object[] previousRowValues = new Object[_windowAccumulators.length];
      for (int i = 0; i < _windowAccumulators.length; i++) {
        previousRowValues[i] = null;
      }
      for (Map.Entry<Key, List<Object[]>> e : _partitionRows.entrySet()) {
        Key partitionKey = e.getKey();
        List<Object[]> rowList = e.getValue();
        for (Object[] existingRow : rowList) {
          Object[] row = new Object[existingRow.length + _aggCalls.size()];
          System.arraycopy(existingRow, 0, row, 0, existingRow.length);
          for (int i = 0; i < _windowAccumulators.length; i++) {
            row[i + existingRow.length] =
                _windowAccumulators[i].computeRowResultForCurrentRow(partitionKey, previousPartitionKey, row,
                    previousRowValues[i]);
            previousRowValues[i] = row[i + existingRow.length];
          }
          // Convert the results from Accumulator to the desired type
          TypeUtils.convertRow(row, resultStoredTypes);
          rows.add(row);
          previousPartitionKey = partitionKey;
        }
      }
    }
    _hasReturnedWindowAggregateBlock = true;
    if (rows.size() == 0) {
      return TransferableBlockUtils.getEndOfStreamTransferableBlock();
    } else {
      return new TransferableBlock(rows, _resultSchema, DataBlock.Type.ROW);
    }
  }

  /**
   * @return the final block, which must be either an end of stream or an error.
   */
  private TransferableBlock consumeInputBlocks() {
    Key emptyOrderKey = AggregationUtils.extractEmptyKey();
    TransferableBlock block = _inputOperator.nextBlock();
    while (!TransferableBlockUtils.isEndOfStream(block)) {
      List<Object[]> container = block.getContainer();
      if (_windowFrame.getWindowFrameType() == WindowNode.WindowFrameType.RANGE) {
        // Only need to accumulate the aggregate function values for RANGE type. ROW type can be calculated as
        // we output the rows since the aggregation value depends on the neighboring rows.
        for (Object[] row : container) {
          _numRows++;
          // TODO: Revisit null direction handling for all query types
          Key key = AggregationUtils.extractRowKey(row, _groupSet);
          _partitionRows.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
          // Only need to accumulate the aggregate function values for RANGE type. ROW type can be calculated as
          // we output the rows since the aggregation value depends on the neighboring rows.
          Key orderKey = (_isPartitionByOnly && CollectionUtils.isEmpty(_orderSetInfo.getOrderSet())) ? emptyOrderKey
              : AggregationUtils.extractRowKey(row, _orderSetInfo.getOrderSet());
          int aggCallsSize = _aggCalls.size();
          for (int i = 0; i < aggCallsSize; i++) {
            _windowAccumulators[i].accumulateRangeResults(key, orderKey, row);
          }
        }
      } else {
        for (Object[] row : container) {
          _numRows++;
          // TODO: Revisit null direction handling for all query types
          Key key = AggregationUtils.extractRowKey(row, _groupSet);
          _partitionRows.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
        }
      }
      block = _inputOperator.nextBlock();
    }
    return block;
  }

  /**
   * Contains all the ORDER BY key related information such as the keys, direction, and null direction
   */
  private static class OrderSetInfo {
    // List of order keys
    final List<RexExpression> _orderSet;
    // List of order direction for each key
    final List<RelFieldCollation.Direction> _orderSetDirection;
    // List of null direction for each key
    final List<RelFieldCollation.NullDirection> _orderSetNullDirection;
    // Set to 'true' if this is a partition by only query
    final boolean _isPartitionByOnly;

    OrderSetInfo(List<RexExpression> orderSet, List<RelFieldCollation.Direction> orderSetDirection,
        List<RelFieldCollation.NullDirection> orderSetNullDirection, boolean isPartitionByOnly) {
      _orderSet = orderSet;
      _orderSetDirection = orderSetDirection;
      _orderSetNullDirection = orderSetNullDirection;
      _isPartitionByOnly = isPartitionByOnly;
    }

    List<RexExpression> getOrderSet() {
      return _orderSet;
    }

    List<RelFieldCollation.Direction> getOrderSetDirection() {
      return _orderSetDirection;
    }

    List<RelFieldCollation.NullDirection> getOrderSetNullDirection() {
      return _orderSetNullDirection;
    }

    boolean isPartitionByOnly() {
      return _isPartitionByOnly;
    }
  }

  /**
   * Defines the Frame to be used for the window query. The 'lowerBound' and 'upperBound' indicate the frame
   * boundaries to be used. Whereas, 'isRows' is used to differentiate between RANGE and ROWS type frames.
   */
  private static class WindowFrame {
    // The lower bound of the frame. Set to Integer.MIN_VALUE if UNBOUNDED PRECEDING
    final int _lowerBound;
    // The lower bound of the frame. Set to Integer.MAX_VALUE if UNBOUNDED FOLLOWING. Set to 0 if CURRENT ROW
    final int _upperBound;
    // Enum to denote the FRAME type, can be either ROW or RANGE types
    final WindowNode.WindowFrameType _windowFrameType;

    WindowFrame(int lowerBound, int upperBound, WindowNode.WindowFrameType windowFrameType) {
      _lowerBound = lowerBound;
      _upperBound = upperBound;
      _windowFrameType = windowFrameType;
    }

    boolean isUnboundedPreceding() {
      return _lowerBound == Integer.MIN_VALUE;
    }

    boolean isUnboundedFollowing() {
      return _upperBound == Integer.MAX_VALUE;
    }

    boolean isUpperBoundCurrentRow() {
      return _upperBound == 0;
    }

    WindowNode.WindowFrameType getWindowFrameType() {
      return _windowFrameType;
    }

    int getLowerBound() {
      return _lowerBound;
    }

    int getUpperBound() {
      return _upperBound;
    }
  }

  private static class MergeRowNumber implements AggregationUtils.Merger {

    @Override
    public Long init(@Nullable Object value, ColumnDataType dataType) {
      return 1L;
    }

    @Override
    public Long merge(Object agg, @Nullable Object value) {
      return (long) agg + 1;
    }
  }

  private static class MergeRank implements AggregationUtils.Merger {

    @Override
    public Long init(Object other, ColumnDataType dataType) {
      return 1L;
    }

    @Override
    public Long merge(Object left, Object right) {
      // RANK always increase by the number of duplicate entries seen for the given ORDER BY key.
      return ((Number) left).longValue() + ((Number) right).longValue();
    }
  }

  private static class MergeDenseRank implements AggregationUtils.Merger {

    @Override
    public Long init(Object other, ColumnDataType dataType) {
      return 1L;
    }

    @Override
    public Long merge(Object left, Object right) {
      long rightValueInLong = ((Number) right).longValue();
      // DENSE_RANK always increase the rank by 1, irrespective of the number of duplicate ORDER BY keys seen
      return (rightValueInLong == 0L) ? ((Number) left).longValue() : ((Number) left).longValue() + 1L;
    }
  }

  private static class WindowAggregateAccumulator extends AggregationUtils.Accumulator {
    private static final Map<String, Function<ColumnDataType, AggregationUtils.Merger>> WIN_AGG_MERGERS =
        ImmutableMap.<String, Function<ColumnDataType, AggregationUtils.Merger>>builder()
            .putAll(AggregationUtils.Accumulator.MERGERS).put("ROW_NUMBER", cdt -> new MergeRowNumber())
            .put("RANK", cdt -> new MergeRank()).put("DENSE_RANK", cdt -> new MergeDenseRank()).build();

    private final boolean _isPartitionByOnly;
    private final boolean _isRankingWindowFunction;

    // Fields needed only for RANGE frame type queries (ORDER BY)
    private final Map<Key, OrderKeyResult> _orderByResults = new HashMap<>();

    WindowAggregateAccumulator(RexExpression.FunctionCall aggCall,
        Map<String, Function<ColumnDataType, AggregationUtils.Merger>> merger, String functionName,
        DataSchema inputSchema, OrderSetInfo orderSetInfo) {
      super(aggCall, merger, functionName, inputSchema);
      _isPartitionByOnly = CollectionUtils.isEmpty(orderSetInfo.getOrderSet()) || orderSetInfo.isPartitionByOnly();
      _isRankingWindowFunction = RANKING_FUNCTION_NAMES.contains(functionName);
    }

    /**
     * For ROW type queries the aggregation function value depends on the order of the rows rather than on the actual
     * keys. For such queries compute the current row value based on the previous row and previous partition key.
     * This should only be called for ROW type queries.
     */
    public Object computeRowResultForCurrentRow(Key currentPartitionKey, Key previousPartitionKey, Object[] row,
        Object previousRowOutputValue) {
      Object value = _inputRef == -1 ? _literal : row[_inputRef];
      if (previousPartitionKey == null || !currentPartitionKey.equals(previousPartitionKey)) {
        return _merger.init(currentPartitionKey, _dataType);
      } else {
        return _merger.merge(previousRowOutputValue, value);
      }
    }

    /**
     * For RANGE type queries, accumulate the function values for each PARTITION BY key and ORDER BY key based on
     * the current row. Should only be called for RANGE type queries where the aggregation values are tied to the
     * RANGE key and not to the row ordering. This should only be called for RANGE type queries.
     */
    public void accumulateRangeResults(Key key, Key orderKey, Object[] row) {
      // Ranking functions don't use the row value, thus cannot reuse the AggregationUtils accumulate function for them
      if (_isPartitionByOnly && !_isRankingWindowFunction) {
        accumulate(key, row);
        return;
      }

      // TODO: fix that single agg result (original type) has different type from multiple agg results (double).
      Key previousOrderKeyIfPresent =
          _orderByResults.get(key) == null ? null : _orderByResults.get(key).getPreviousOrderByKey();
      Object currentRes = previousOrderKeyIfPresent == null ? null
          : _orderByResults.get(key).getOrderByResults().get(previousOrderKeyIfPresent);
      Object value = _inputRef == -1 ? _literal : row[_inputRef];

      // The ranking functions do not depend on the actual value of the data, but are calculated based on the
      // position of the data ordered by the ORDER BY key. Thus they need to be handled differently and require setting
      // whether the rank has changed or not and if changed then by how much.
      _orderByResults.putIfAbsent(key, new OrderKeyResult());
      if (currentRes == null) {
        value = _isRankingWindowFunction ? 0 : value;
        _orderByResults.get(key).addOrderByResult(orderKey, _merger.init(value, _dataType));
      } else {
        Object mergedResult;
        if (orderKey.equals(previousOrderKeyIfPresent)) {
          value = _isRankingWindowFunction ? 0 : value;
          mergedResult = _merger.merge(currentRes, value);
        } else {
          Object previousValue = _orderByResults.get(key).getOrderByResults().get(previousOrderKeyIfPresent);
          value = _isRankingWindowFunction ? _orderByResults.get(key).getCountOfDuplicateOrderByKeys() : value;
          mergedResult = _merger.merge(previousValue, value);
        }
        _orderByResults.get(key).addOrderByResult(orderKey, mergedResult);
      }
    }

    public Object getRangeResultForKeys(Key key, Key orderKey) {
      if (_isPartitionByOnly && !_isRankingWindowFunction) {
        return _results.get(key);
      } else {
        return _orderByResults.get(key).getOrderByResults().get(orderKey);
      }
    }

    public Map<Key, OrderKeyResult> getRangeOrderByResults() {
      return _orderByResults;
    }

    static class OrderKeyResult {
      final Map<Key, Object> _orderByResults;
      Key _previousOrderByKey;
      // Store the counts of duplicate ORDER BY keys seen for this PARTITION BY key for calculating RANK/DENSE_RANK
      long _countOfDuplicateOrderByKeys;

      OrderKeyResult() {
        _orderByResults = new HashMap<>();
        _previousOrderByKey = null;
        _countOfDuplicateOrderByKeys = 0;
      }

      public void addOrderByResult(Key orderByKey, Object value) {
        // We expect to get the rows in order based on the ORDER BY key so it is safe to blindly assign the
        // current key as the previous key
        _orderByResults.put(orderByKey, value);
        _countOfDuplicateOrderByKeys =
            (_previousOrderByKey != null && _previousOrderByKey.equals(orderByKey)) ? _countOfDuplicateOrderByKeys + 1
                : 1;
        _previousOrderByKey = orderByKey;
      }

      public Map<Key, Object> getOrderByResults() {
        return _orderByResults;
      }

      public Key getPreviousOrderByKey() {
        return _previousOrderByKey;
      }

      public long getCountOfDuplicateOrderByKeys() {
        return _countOfDuplicateOrderByKeys;
      }
    }
  }
}
