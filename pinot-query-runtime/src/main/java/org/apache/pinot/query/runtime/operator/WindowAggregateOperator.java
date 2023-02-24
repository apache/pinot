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
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.utils.AggregationUtils;
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
 * Note: This class performs aggregation over the double value of input.
 * If the input is single value, the output type will be input type. Otherwise, the output type will be double.
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
  private final AggregationUtils.Accumulator[] _windowAccumulators;
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
        upperBound, isRows, constants, resultSchema, inputSchema, AggregationUtils.Accumulator.MERGERS,
        requestId, stageId, virtualServerAddress);
  }

  @VisibleForTesting
  public WindowAggregateOperator(MultiStageOperator inputOperator, List<RexExpression> groupSet,
      List<RexExpression> orderSet, List<RelFieldCollation.Direction> orderSetDirection,
      List<RelFieldCollation.NullDirection> orderSetNullDirection, List<RexExpression> aggCalls, int lowerBound,
      int upperBound, boolean isRows, List<RexExpression> constants, DataSchema resultSchema, DataSchema inputSchema,
      Map<String, Function<DataSchema.ColumnDataType, AggregationUtils.Merger>> mergers, long requestId, int stageId,
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
    Preconditions.checkState(_windowFrame.isUnboundedFollowing()
            || (_windowFrame.isUpperBoundCurrentRow() && isPartitionByOnly),
        "Only default frame is supported, upperBound must be UNBOUNDED FOLLOWING or CURRENT ROW");

    // we expect all agg calls to be aggregate function calls
    _aggCalls = aggCalls.stream().map(RexExpression.FunctionCall.class::cast).collect(Collectors.toList());
    _constants = constants;
    _resultSchema = resultSchema;

    // TODO: Not all window functions (e.g. ROW_NUMBER, LAG, etc) need aggregations. Such functions should be handled
    //       differently.
    _windowAccumulators = new AggregationUtils.Accumulator[_aggCalls.size()];
    for (int i = 0; i < _aggCalls.size(); i++) {
      RexExpression.FunctionCall agg = _aggCalls.get(i);
      String functionName = agg.getFunctionName();
      if (!mergers.containsKey(functionName)) {
        throw new IllegalStateException("Unexpected value: " + functionName);
      }
      _windowAccumulators[i] = new AggregationUtils.Accumulator(agg, mergers, functionName, inputSchema);
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
      LOGGER.error("Caught exception while executing WindowAggregationOperator, returning an error block", e);
      return TransferableBlockUtils.getErrorTransferableBlock(e);
    }
  }

  private boolean isPartitionByOnlyQuery(List<RexExpression> groupSet, List<RexExpression> orderSet,
      List<RelFieldCollation.Direction> orderSetDirection,
      List<RelFieldCollation.NullDirection> orderSetNullDirection) {
    if (CollectionUtils.isEmpty(orderSet)) {
      return true;
    }

    if (CollectionUtils.isEmpty(groupSet) || (groupSet.size() != orderSet.size())) {
      return false;
    }

    Set<Integer> partitionByInputRefIndexes = new HashSet<>();
    Set<Integer> orderByInputRefIndexes = new HashSet<>();
    for (int i = 0; i < groupSet.size(); i++) {
      partitionByInputRefIndexes.add(((RexExpression.InputRef) groupSet.get(i)).getIndex());
      orderByInputRefIndexes.add(((RexExpression.InputRef) orderSet.get(i)).getIndex());
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
          row[i + existingRow.length] = _windowAccumulators[i].getResults().get(partitionKey);
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
        Key key = AggregationUtils.extractRowKey(row, _groupSet);
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

  /**
   * Defines the Frame to be used for the window query. The 'lowerBound' and 'upperBound' indicate the frame
   * boundaries to be used. Whereas, 'isRows' is used to differentiate between RANGE and ROWS type frames.
   */
  private static class WindowFrame {
    // The lower bound of the frame. Set to Integer.MIN_VALUE if UNBOUNDED PRECEDING
    final int _lowerBound;
    // The lower bound of the frame. Set to Integer.MAX_VALUE if UNBOUNDED FOLLOWING. Set to 0 if CURRENT ROW
    final int _upperBound;
    // Set to 'true' for ROWS type frames, otherwise set to 'false'
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
}
