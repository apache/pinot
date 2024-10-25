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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.rel.RelFieldCollation;
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
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.WindowNode;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.operator.utils.AggregationUtils;
import org.apache.pinot.query.runtime.operator.utils.TypeUtils;
import org.apache.pinot.query.runtime.operator.window.WindowFrame;
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
  public static final Set<String> ROWS_ONLY_FUNCTION_NAMES = Set.of("ROW_NUMBER");
  // List of ranking window functions whose output depends on the ordering of input rows and not on the actual values
  public static final Set<String> RANKING_FUNCTION_NAMES = Set.of("RANK", "DENSE_RANK");

  private final MultiStageOperator _input;
  private final DataSchema _resultSchema;
  private final int[] _keys;
  private final WindowFrame _windowFrame;
  private final WindowFunction[] _windowFunctions;
  private final Map<Key, List<Object[]>> _partitionRows = new HashMap<>();
  private final StatMap<StatKey> _statMap = new StatMap<>(StatKey.class);

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
  private TransferableBlock _eosBlock;

  public WindowAggregateOperator(OpChainExecutionContext context, MultiStageOperator input, DataSchema inputSchema,
      WindowNode node) {
    super(context);

    _input = input;
    _resultSchema = node.getDataSchema();
    List<Integer> keys = node.getKeys();
    int numKeys = keys.size();
    _keys = new int[numKeys];
    for (int i = 0; i < numKeys; i++) {
      _keys[i] = keys.get(i);
    }
    _windowFrame = new WindowFrame(node.getWindowFrameType(), node.getLowerBound(), node.getUpperBound());
    Preconditions.checkState(
        _windowFrame.isRowType() || ((_windowFrame.isUnboundedPreceding() || _windowFrame.isLowerBoundCurrentRow()) && (
            _windowFrame.isUnboundedFollowing() || _windowFrame.isUpperBoundCurrentRow())),
        "RANGE window frame with offset PRECEDING / FOLLOWING is not supported");
    Preconditions.checkState(_windowFrame.getLowerBound() <= _windowFrame.getUpperBound(),
        "Window frame lower bound can't be greater than upper bound");
    List<RelFieldCollation> collations = node.getCollations();
    List<RexExpression.FunctionCall> aggCalls = node.getAggCalls();
    int numAggCalls = aggCalls.size();
    _windowFunctions = new WindowFunction[numAggCalls];
    for (int i = 0; i < numAggCalls; i++) {
      RexExpression.FunctionCall aggCall = aggCalls.get(i);
      _windowFunctions[i] =
          WindowFunctionFactory.constructWindowFunction(aggCall, inputSchema, collations, _windowFrame);
    }

    Map<String, String> metadata = context.getOpChainMetadata();
    PlanNode.NodeHint nodeHint = node.getNodeHint();
    _maxRowsInWindowCache = getMaxRowInWindow(metadata, nodeHint);
    _windowOverflowMode = getWindowOverflowMode(metadata, nodeHint);
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

  private int getMaxRowInWindow(Map<String, String> opChainMetadata, PlanNode.NodeHint nodeHint) {
    Map<String, String> windowOptions = nodeHint.getHintOptions().get(PinotHintOptions.WINDOW_HINT_OPTIONS);
    if (windowOptions != null) {
      String maxRowsInWindowStr = windowOptions.get(PinotHintOptions.WindowHintOptions.MAX_ROWS_IN_WINDOW);
      if (maxRowsInWindowStr != null) {
        return Integer.parseInt(maxRowsInWindowStr);
      }
    }
    Integer maxRowsInWindow = QueryOptionsUtils.getMaxRowsInWindow(opChainMetadata);
    return maxRowsInWindow != null ? maxRowsInWindow : DEFAULT_MAX_ROWS_IN_WINDOW;
  }

  private WindowOverFlowMode getWindowOverflowMode(Map<String, String> contextMetadata, PlanNode.NodeHint nodeHint) {
    Map<String, String> windowOptions = nodeHint.getHintOptions().get(PinotHintOptions.WINDOW_HINT_OPTIONS);
    if (windowOptions != null) {
      String windowOverflowModeStr = windowOptions.get(PinotHintOptions.WindowHintOptions.WINDOW_OVERFLOW_MODE);
      if (windowOverflowModeStr != null) {
        return WindowOverFlowMode.valueOf(windowOverflowModeStr);
      }
    }
    WindowOverFlowMode windowOverflowMode = QueryOptionsUtils.getWindowOverflowMode(contextMetadata);
    return windowOverflowMode != null ? windowOverflowMode : DEFAULT_WINDOW_OVERFLOW_MODE;
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return List.of(_input);
  }

  @Override
  public Type getOperatorType() {
    return Type.WINDOW;
  }

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

  /**
   * @return the final block, which must be either an end of stream or an error.
   */
  private TransferableBlock computeBlocks()
      throws ProcessingException {
    TransferableBlock block = _input.nextBlock();
    while (block.isDataBlock()) {
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
          _input.earlyTerminate();
        }
      }
      for (Object[] row : container) {
        // TODO: Revisit null direction handling for all query types
        Key key = AggregationUtils.extractRowKey(row, _keys);
        _partitionRows.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
      }
      _numRows += containerSize;
      sampleAndCheckInterruption();
      block = _input.nextBlock();
    }
    // Early termination if the block is an error block
    if (block.isErrorBlock()) {
      return block;
    }
    assert block.isSuccessfulEndOfStreamBlock();
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
        Object[] row = new Object[existingRow.length + _windowFunctions.length];
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

  public enum StatKey implements StatMap.Key {
    //@formatter:off
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
    //@formatter:on

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
