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
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The WindowAggregateOperator is used to compute window function aggregations over a set of optional
 * PARTITION BY keys, ORDER BY keys and a FRAME clause. The output data will include the projected
 * columns and in addition will add the aggregation columns to the output data.
 * [input columns, aggregate result1, ... aggregate resultN]
 *
 * The window functions supported today are SUM/COUNT/MIN/MAX aggregations. Window functions also include
 * other types of functions such as rank and value functions.
 *
 * Unlike the AggregateOperator which will output one row per group, the WindowAggregateOperator
 * will output as many rows as input rows.
 *
 * TODO:
 *     1. Add support for OVER() clause with ORDER BY only or PARTITION BY ORDER BY
 *     2. Add support for rank window functions
 *     3. Add support for value window functions
 *     4. Add support for custom frames
 */
public class WindowAggregateOperator extends MultiStageOperator {
  private static final String EXPLAIN_NAME = "WINDOW";
  private static final Logger LOGGER = LoggerFactory.getLogger(WindowAggregateOperator.class);

  private final MultiStageOperator _inputOperator;
  private final List<RexExpression> _groupSet;
  private final OrderSetInfo _orderSetInfo;
  private final WindowFrame _windowFrame;
  private final List<RexExpression.FunctionCall> _aggCalls;
  private final List<RexExpression> _constants;
  private final DataSchema _resultSchema;
  private final WindowAccumulator[] _windowAccumulators;
  private final Map<Key, List<Object[]>> _partitionRows;

  private TransferableBlock _upstreamErrorBlock;

  private int _numRows;
  private boolean _readyToConstruct;
  private boolean _hasReturnedWindowAggregateBlock;

  public WindowAggregateOperator(MultiStageOperator inputOperator, List<RexExpression> groupSet,
      List<RexExpression> orderSet, List<RelFieldCollation.Direction> orderSetDirection,
      List<RelFieldCollation.NullDirection> orderSetNullDirection, List<RexExpression> aggCalls, int lowerBound,
      int upperBound, boolean isRows, List<RexExpression> constants, DataSchema resultSchema, DataSchema inputSchema,
      long requestId, int stageId, VirtualServerAddress virtualServerAddress) {
    this(inputOperator, groupSet, orderSet, orderSetDirection, orderSetNullDirection, aggCalls, lowerBound,
        upperBound, isRows, constants, resultSchema, inputSchema, WindowAccumulator.WINDOW_MERGERS,
        requestId, stageId, virtualServerAddress);
  }

  @VisibleForTesting
  public WindowAggregateOperator(MultiStageOperator inputOperator, List<RexExpression> groupSet,
      List<RexExpression> orderSet, List<RelFieldCollation.Direction> orderSetDirection,
      List<RelFieldCollation.NullDirection> orderSetNullDirection, List<RexExpression> aggCalls, int lowerBound,
      int upperBound, boolean isRows, List<RexExpression> constants, DataSchema resultSchema, DataSchema inputSchema,
      Map<String, Function<DataSchema.ColumnDataType, WindowMerger>> mergers, long requestId, int stageId,
      VirtualServerAddress virtualServerAddress) {
    super(requestId, stageId, virtualServerAddress);

    boolean isPartitionByOnly = isPartitionByOnlyQuery(groupSet, orderSet, orderSetDirection, orderSetNullDirection);
    // TODO: add support for ORDER BY in the OVER() clause
    Preconditions.checkState(orderSet == null || orderSet.isEmpty() || isPartitionByOnly,
        "Order by is not yet supported in window functions");

    _inputOperator = inputOperator;
    _groupSet = groupSet;
    _orderSetInfo = new OrderSetInfo(orderSet, orderSetDirection, orderSetNullDirection);
    _windowFrame = new WindowFrame(lowerBound, upperBound, isRows);

    // TODO: add support for custom frames, and for ORDER BY default frame (upperBound => currentRow)
    Preconditions.checkState(!_windowFrame.isRows(), "Only RANGE type frames are supported at present");
    Preconditions.checkState(_windowFrame.isUnboundedPreceding(),
        "Only default frame is supported, lowerBound must be UNBOUNDED PRECEDING");
    Preconditions.checkState(_windowFrame.isUnboundedFollowing(),
        "Only default frame is supported, upperBound must be UNBOUNDED FOLLOWING since order by is not present");

    // we expect all agg calls to be aggregate function calls
    _aggCalls = aggCalls.stream().map(RexExpression.FunctionCall.class::cast).collect(Collectors.toList());
    _constants = constants;
    _resultSchema = resultSchema;

    // TODO: Not all window functions (e.g. ROW_NUMBER, LAG, etc) need aggregations. Such functions should be handled
    //       differently.
    _windowAccumulators = new WindowAccumulator[_aggCalls.size()];
    for (int i = 0; i < _aggCalls.size(); i++) {
      RexExpression.FunctionCall agg = _aggCalls.get(i);
      String functionName = agg.getFunctionName();
      if (!mergers.containsKey(functionName)) {
        throw new IllegalStateException("Unexpected value: " + functionName);
      }
      _windowAccumulators[i] = new WindowAccumulator(agg, mergers, functionName, inputSchema);
    }

    _partitionRows = new HashMap<>();

    _numRows = 0;
    _readyToConstruct = false;
    _hasReturnedWindowAggregateBlock = false;
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

      if (!_hasReturnedWindowAggregateBlock) {
        return produceWindowAggregateBlock();
      } else {
        // TODO: Move to close call.
        return TransferableBlockUtils.getEndOfStreamTransferableBlock();
      }
    } catch (Exception e) {
      return TransferableBlockUtils.getErrorTransferableBlock(e);
    }
  }

  private boolean isPartitionByOnlyQuery(List<RexExpression> groupSet, List<RexExpression> orderSet,
      List<RelFieldCollation.Direction> orderSetDirection,
      List<RelFieldCollation.NullDirection> orderSetNullDirection) {
    if (orderSet == null || orderSet.isEmpty()) {
      return true;
    }

    if (groupSet == null || (groupSet.size() != orderSet.size())) {
      return false;
    }

    Set<Integer> partitionByInputRefIndexes = new HashSet<>();
    Set<Integer> orderByInputRefIndexes = new HashSet<>();
    for (int i = 0; i < groupSet.size(); i++) {
      partitionByInputRefIndexes.add(((RexExpression.InputRef) groupSet.get(0)).getIndex());
      orderByInputRefIndexes.add(((RexExpression.InputRef) orderSet.get(0)).getIndex());
    }

    boolean isPartitionByOnly = partitionByInputRefIndexes.equals(orderByInputRefIndexes);
    if (isPartitionByOnly) {
      // Check the direction and null direction to ensure default ordering on the order by keys, which are:
      // Direction: ASC
      // Null Direction: LAST
      for (int i = 0; i < orderSet.size(); i++) {
        if (orderSetDirection.get(i) == RelFieldCollation.Direction.DESCENDING
            || orderSetNullDirection.get(i) == RelFieldCollation.NullDirection.FIRST) {
          isPartitionByOnly = false;
          break;
        }
      }
    }
    return isPartitionByOnly;
  }

  private TransferableBlock produceWindowAggregateBlock() {
    List<Object[]> rows = new ArrayList<>(_numRows);
    for (Map.Entry<Key, List<Object[]>> e : _partitionRows.entrySet()) {
      Key partitionKey = e.getKey();
      List<Object[]> rowList = e.getValue();
      for (Object[] existingRow : rowList) {
        Object[] row = new Object[existingRow.length + _aggCalls.size()];
        System.arraycopy(existingRow, 0, row, 0, existingRow.length);
        for (int i = 0; i < _windowAccumulators.length; i++) {
          row[i + existingRow.length] = _windowAccumulators[i]._results.get(partitionKey);
        }
        rows.add(row);
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
        _numRows++;
        // TODO: Revisit the aggregation logic once ORDER BY inside OVER() support is added
        Key key = extractRowKey(row, _groupSet);
        _partitionRows.putIfAbsent(key, new ArrayList<>());
        _partitionRows.get(key).add(row);
        for (int i = 0; i < _aggCalls.size(); i++) {
          _windowAccumulators[i].accumulate(key, row);
        }
      }
      block = _inputOperator.nextBlock();
    }
    return false;
  }

  private static Key extractRowKey(Object[] row, List<RexExpression> groupSet) {
    Object[] keyElements = new Object[groupSet.size()];
    for (int i = 0; i < groupSet.size(); i++) {
      keyElements[i] = row[((RexExpression.InputRef) groupSet.get(i)).getIndex()];
    }
    return new Key(keyElements);
  }

  private static class OrderSetInfo {
    final List<RexExpression> _orderSet;
    final List<RelFieldCollation.Direction> _orderSetDirection;
    final List<RelFieldCollation.NullDirection> _orderSetNullDirection;

    OrderSetInfo(List<RexExpression> orderSet, List<RelFieldCollation.Direction> orderSetDirection,
        List<RelFieldCollation.NullDirection> orderSetNullDirection) {
      _orderSet = orderSet;
      _orderSetDirection = orderSetDirection;
      _orderSetNullDirection = orderSetNullDirection;
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
  }

  private static class WindowFrame {
    final int _lowerBound;
    final int _upperBound;
    final boolean _isRows;

    WindowFrame(int lowerBound, int upperBound, boolean isRows) {
      _lowerBound = lowerBound;
      _upperBound = upperBound;
      _isRows = isRows;
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

    boolean isRows() {
      return _isRows;
    }

    int getLowerBound() {
      return _lowerBound;
    }

    int getUpperBound() {
      return _upperBound;
    }
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

  private static class MergeCount implements WindowMerger {

    @Override
    public Object initialize(Object other, DataSchema.ColumnDataType dataType) {
      return other == null ? 0 : 1d;
    }

    @Override
    public Object merge(Object left, Object ignored) {
      // TODO: COUNT(*) doesn't need to parse right object until we support NULL
      return ((Number) left).doubleValue() + 1;
    }
  }

  interface WindowMerger {
    /**
     * Initializes the merger based on the first input
     */
    default Object initialize(Object other, DataSchema.ColumnDataType dataType) {
      return other == null ? ((Number) dataType.getNullPlaceholder()).doubleValue() : ((Number) other).doubleValue();
    }

    /**
     * Merges the existing aggregate (the result of {@link #initialize(Object, DataSchema.ColumnDataType)}) with
     * the new value coming in (which may be an aggregate in and of itself).
     */
    Object merge(Object agg, Object value);
  }

  private static class WindowAccumulator {
    private static final Map<String, Function<DataSchema.ColumnDataType, WindowMerger>> WINDOW_MERGERS =
        ImmutableMap.<String, Function<DataSchema.ColumnDataType, WindowMerger>>builder()
            .put("SUM", cdt -> WindowAggregateOperator::mergeSum)
            .put("$SUM", cdt -> WindowAggregateOperator::mergeSum)
            .put("$SUM0", cdt -> WindowAggregateOperator::mergeSum)
            .put("MIN", cdt -> WindowAggregateOperator::mergeMin)
            .put("$MIN", cdt -> WindowAggregateOperator::mergeMin)
            .put("$MIN0", cdt -> WindowAggregateOperator::mergeMin)
            .put("MAX", cdt -> WindowAggregateOperator::mergeMax)
            .put("$MAX", cdt -> WindowAggregateOperator::mergeMax)
            .put("$MAX0", cdt -> WindowAggregateOperator::mergeMax)
            .put("COUNT", cdt -> new MergeCount())
            .build();

    final int _inputRef;
    final Object _literal;
    final Map<Key, Object> _results = new HashMap<>();
    final WindowMerger _windowMerger;
    final DataSchema.ColumnDataType _dataType;

    WindowAccumulator(RexExpression.FunctionCall aggCall, Map<String,
        Function<DataSchema.ColumnDataType, WindowMerger>> merger, String functionName, DataSchema inputSchema) {
      // The aggregate function operand should either be a InputRef or a Literal
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
      _windowMerger = merger.get(functionName).apply(_dataType);
    }

    void accumulate(Key key, Object[] row) {
      Object currentRes = _results.get(key);
      Object value = _inputRef == -1 ? _literal : row[_inputRef];

      if (currentRes == null) {
        _results.put(key, _windowMerger.initialize(value, _dataType));
      } else {
        Object mergedResult = _windowMerger.merge(currentRes, value);
        _results.put(key, mergedResult);
      }
    }

    private RexExpression toAggregationFunctionOperand(RexExpression.FunctionCall rexExpression) {
      List<RexExpression> functionOperands = rexExpression.getFunctionOperands();
      Preconditions.checkState(functionOperands.size() < 2,
          "Aggregate window functions cannot have more than one operand");
      return functionOperands.size() > 0 ? functionOperands.get(0)
          : new RexExpression.Literal(FieldSpec.DataType.INT, 1);
    }
  }
}
