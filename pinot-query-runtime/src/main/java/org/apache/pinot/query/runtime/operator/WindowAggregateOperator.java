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
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.response.ProcessingException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.AbstractPlanNode;
import org.apache.pinot.query.planner.plannode.WindowNode;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.utils.AggregationUtils;
import org.apache.pinot.query.runtime.operator.utils.TypeUtils;
import org.apache.pinot.query.runtime.operator.window.WindowFunction;
import org.apache.pinot.query.runtime.operator.window.WindowFunctionFactory;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner.WindowOverFlowMode;
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
public class WindowAggregateOperator extends MultiStageOperator {
  private static final String EXPLAIN_NAME = "WINDOW";
  private static final Logger LOGGER = LoggerFactory.getLogger(WindowAggregateOperator.class);
  private static final int DEFAULT_MAX_ROWS_IN_WINDOW = 1024 * 1024; // 2^20, around 1MM rows
  private static final WindowOverFlowMode DEFAULT_WINDOW_OVERFLOW_MODE = WindowOverFlowMode.THROW;

  // List of window functions which can only be applied as ROWS window frame type
  public static final Set<String> ROWS_ONLY_FUNCTION_NAMES = ImmutableSet.of("ROW_NUMBER");
  // List of ranking window functions whose output depends on the ordering of input rows and not on the actual values
  public static final Set<String> RANKING_FUNCTION_NAMES = ImmutableSet.of("RANK", "DENSE_RANK");

  private final MultiStageOperator _inputOperator;
  private final List<RexExpression> _groupSet;
  private final OrderSetInfo _orderSetInfo;
  private final WindowFrame _windowFrame;
  private final List<RexExpression.FunctionCall> _aggCalls;
  private final List<RexExpression> _constants;
  private final DataSchema _resultSchema;
  private final WindowFunction[] _windowFunctions;
  private final Map<Key, List<Object[]>> _partitionRows;
  private final boolean _isPartitionByOnly;

  // Below are specific parameters to protect the window cache from growing too large.
  // Once the window cache reaches the limit, we will throw exception or break the cache build process.
  /**
   * Max rows allowed to build the right table hash collection.
   */
  private final int _maxRowsInWindowCache;
  /**
   * Mode when window overflow happens, supported values: THROW or BREAK.
   * THROW(default): Break window cache build process, and throw exception, no WINDOW operation performed.
   * BREAK: Break window cache build process, continue to perform WINDOW operation, results might be partial or wrong.
   */
  private final WindowOverFlowMode _windowOverflowMode;

  private int _numRows;
  private boolean _hasReturnedWindowAggregateBlock;
  @Nullable
  private TransferableBlock _eosBlock = null;
  private final StatMap<StatKey> _statMap = new StatMap<>(StatKey.class);

  @VisibleForTesting
  public WindowAggregateOperator(OpChainExecutionContext context, MultiStageOperator inputOperator,
      List<RexExpression> groupSet, List<RexExpression> orderSet, List<RelFieldCollation.Direction> orderSetDirection,
      List<RelFieldCollation.NullDirection> orderSetNullDirection, List<RexExpression> aggCalls, int lowerBound,
      int upperBound, WindowNode.WindowFrameType windowFrameType, List<RexExpression> constants,
      DataSchema resultSchema, DataSchema inputSchema, AbstractPlanNode.NodeHint hints) {
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

    _windowFunctions = new WindowFunction[_aggCalls.size()];
    int aggCallsSize = _aggCalls.size();
    for (int i = 0; i < aggCallsSize; i++) {
      RexExpression.FunctionCall agg = _aggCalls.get(i);
      String functionName = agg.getFunctionName();
      validateAggregationCalls(functionName);
      _windowFunctions[i] = WindowFunctionFactory.construnctWindowFunction(agg, inputSchema, _orderSetInfo);
    }

    _partitionRows = new HashMap<>();

    _numRows = 0;
    _hasReturnedWindowAggregateBlock = false;
    Map<String, String> metadata = context.getOpChainMetadata();
    _maxRowsInWindowCache = getMaxRowInWindow(metadata, hints);
    _windowOverflowMode = getWindowOverflowMode(metadata, hints);
  }

  @Override
  public void registerExecution(long time, int numRows) {
    _statMap.merge(StatKey.EXECUTION_TIME_MS, time);
    _statMap.merge(StatKey.EMITTED_ROWS, numRows);
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  private int getMaxRowInWindow(Map<String, String> opChainMetadata, @Nullable AbstractPlanNode.NodeHint nodeHint) {
    if (nodeHint != null) {
      Map<String, String> windowOptions = nodeHint._hintOptions.get(PinotHintOptions.WINDOW_HINT_OPTIONS);
      if (windowOptions != null) {
        String maxRowsInWindowStr = windowOptions.get(PinotHintOptions.WindowHintOptions.MAX_ROWS_IN_WINDOW);
        if (maxRowsInWindowStr != null) {
          return Integer.parseInt(maxRowsInWindowStr);
        }
      }
    }
    Integer maxRowsInWindow = QueryOptionsUtils.getMaxRowsInWindow(opChainMetadata);
    return maxRowsInWindow != null ? maxRowsInWindow : DEFAULT_MAX_ROWS_IN_WINDOW;
  }

  private WindowOverFlowMode getWindowOverflowMode(Map<String, String> contextMetadata,
      @Nullable AbstractPlanNode.NodeHint nodeHint) {
    if (nodeHint != null) {
      Map<String, String> windowOptions = nodeHint._hintOptions.get(PinotHintOptions.WINDOW_HINT_OPTIONS);
      if (windowOptions != null) {
        String windowOverflowModeStr = windowOptions.get(PinotHintOptions.WindowHintOptions.WINDOW_OVERFLOW_MODE);
        if (windowOverflowModeStr != null) {
          return WindowOverFlowMode.valueOf(windowOverflowModeStr);
        }
      }
    }
    WindowOverFlowMode windowOverflowMode =
        QueryOptionsUtils.getWindowOverflowMode(contextMetadata);
    return windowOverflowMode != null ? windowOverflowMode : DEFAULT_WINDOW_OVERFLOW_MODE;
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return ImmutableList.of(_inputOperator);
  }

  @Override
  public Type getOperatorType() {
    return Type.WINDOW;
  }

  @Nullable
  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected TransferableBlock getNextBlock()
      throws ProcessingException {
    if (_hasReturnedWindowAggregateBlock) {
      return _eosBlock;
    }
    return computeBlocks();
  }

  private void validateAggregationCalls(String functionName) {
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

  /**
   * @return the final block, which must be either an end of stream or an error.
   */
  private TransferableBlock computeBlocks() throws ProcessingException {
    TransferableBlock block = _inputOperator.nextBlock();
    while (!TransferableBlockUtils.isEndOfStream(block)) {
      List<Object[]> container = block.getContainer();
      int containerSize = container.size();
      if (_numRows + containerSize > _maxRowsInWindowCache) {
        if (_windowOverflowMode == WindowOverFlowMode.THROW) {
          ProcessingException resourceLimitExceededException =
              new ProcessingException(QueryException.SERVER_RESOURCE_LIMIT_EXCEEDED_ERROR_CODE);
          resourceLimitExceededException.setMessage(
              "Cannot build in memory window cache for WINDOW operator, reach number of rows limit: "
                  + _maxRowsInWindowCache);
          throw resourceLimitExceededException;
        } else {
          // Just fill up the buffer.
          int remainingRows = _maxRowsInWindowCache - _numRows;
          container = container.subList(0, remainingRows);
          _statMap.merge(StatKey.MAX_ROWS_IN_WINDOW_REACHED, true);
          // setting the inputOperator to be early terminated and awaits EOS block next.
          _inputOperator.earlyTerminate();
        }
      }
      for (Object[] row : container) {
        // TODO: Revisit null direction handling for all query types
        Key key = AggregationUtils.extractRowKey(row, _groupSet);
        _partitionRows.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
      }
      _numRows += containerSize;
      block = _inputOperator.nextBlock();
    }
    // Early termination if the block is an error block
    if (block.isErrorBlock()) {
      return block;
    }
    _eosBlock = updateEosBlock(block, _statMap);

    ColumnDataType[] resultStoredTypes = _resultSchema.getStoredColumnDataTypes();
    List<Object[]> rows = new ArrayList<>(_numRows);
    for (Map.Entry<Key, List<Object[]>> e : _partitionRows.entrySet()) {
      List<Object[]> rowList = e.getValue();

      // Each window function will return a list of results for each row in the input set
      List<List<Object>> windowFunctionResults = new ArrayList<>();
      for (WindowFunction windowFunction : _windowFunctions) {
        List<Object> processRows = windowFunction.processRows(rowList);
        assert processRows.size() == rowList.size();
        windowFunctionResults.add(processRows);
      }

      for (int rowId = 0; rowId < rowList.size(); rowId++) {
        Object[] existingRow = rowList.get(rowId);
        Object[] row = new Object[existingRow.length + _aggCalls.size()];
        System.arraycopy(existingRow, 0, row, 0, existingRow.length);
        for (int i = 0; i < _windowFunctions.length; i++) {
          row[i + existingRow.length] = windowFunctionResults.get(i).get(rowId);
        }
        // Convert the results from WindowFunction to the desired type
        TypeUtils.convertRow(row, resultStoredTypes);
        rows.add(row);
      }
    }

    _hasReturnedWindowAggregateBlock = true;
    if (rows.isEmpty()) {
      return _eosBlock;
    } else {
      return new TransferableBlock(rows, _resultSchema, DataBlock.Type.ROW);
    }
  }

  /**
   * Contains all the ORDER BY key related information such as the keys, direction, and null direction
   */
  public static class OrderSetInfo {
    // List of order keys
    public final List<RexExpression> _orderSet;
    // List of order direction for each key
    public final List<RelFieldCollation.Direction> _orderSetDirection;
    // List of null direction for each key
    public final List<RelFieldCollation.NullDirection> _orderSetNullDirection;
    // Set to 'true' if this is a partition by only query
    public final boolean _isPartitionByOnly;

    public OrderSetInfo(List<RexExpression> orderSet, List<RelFieldCollation.Direction> orderSetDirection,
        List<RelFieldCollation.NullDirection> orderSetNullDirection, boolean isPartitionByOnly) {
      _orderSet = orderSet;
      _orderSetDirection = orderSetDirection;
      _orderSetNullDirection = orderSetNullDirection;
      _isPartitionByOnly = isPartitionByOnly;
    }

    public List<RexExpression> getOrderSet() {
      return _orderSet;
    }

    public List<RelFieldCollation.Direction> getOrderSetDirection() {
      return _orderSetDirection;
    }

    public List<RelFieldCollation.NullDirection> getOrderSetNullDirection() {
      return _orderSetNullDirection;
    }

    public boolean isPartitionByOnly() {
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

  public enum StatKey implements StatMap.Key {
    EXECUTION_TIME_MS(StatMap.Type.LONG) {
      @Override
      public boolean includeDefaultInJson() {
        return true;
      }
    },
    EMITTED_ROWS(StatMap.Type.LONG) {
      @Override
      public boolean includeDefaultInJson() {
        return true;
      }
    },
    MAX_ROWS_IN_WINDOW_REACHED(StatMap.Type.BOOLEAN);
    private final StatMap.Type _type;

    StatKey(StatMap.Type type) {
      _type = type;
    }

    @Override
    public StatMap.Type getType() {
      return _type;
    }
  }
}
