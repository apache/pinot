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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionFactory;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.aggregation.function.CountAggregationFunction;
import org.apache.pinot.core.util.GroupByUtils;
import org.apache.pinot.query.parser.CalciteRexExpressionParser;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.CommonConstants.Server;


/// The state and the group-by flow that [AggregateOperator] and [MvAggregateOperator] share: the aggregation
/// functions and their filter arguments, the group trim and groups-limit settings, the stats, the consumption of the
/// input into a [MultistageGroupByExecutor], and the output block built from that executor.
///
/// A subclass creates the executor in its constructor, and implements [#consumeInput()] and
/// [#produceAggregatedBlock()]. The base drives them from [#getNextBlock()]: it consumes the whole input, builds one
/// output block, then drops the executor.
///
/// Like every [MultiStageOperator], an instance is not thread-safe: one op chain thread drives it.
abstract class BaseAggregateOperator extends MultiStageOperator {
  private static final CountAggregationFunction COUNT_STAR_AGG_FUNCTION =
      new CountAggregationFunction(List.of(ExpressionContext.forIdentifier("*")), false);

  protected final MultiStageOperator _input;
  protected final DataSchema _resultSchema;
  protected final AggregationFunction<?, ?>[] _aggFunctions;
  protected final int[] _filterArgIds;
  protected final int _maxFilterArgId;
  @Nullable
  protected MultistageGroupByExecutor _groupByExecutor;

  @Nullable
  protected MseBlock.Eos _eosBlock;
  protected final StatMap<AggregateOperator.StatKey> _statMap = new StatMap<>(AggregateOperator.StatKey.class);

  protected final boolean _errorOnNumGroupsLimit;

  // trimming - related members
  protected final int _groupTrimSize;
  @Nullable
  protected final List<RelFieldCollation> _collations;

  protected BaseAggregateOperator(OpChainExecutionContext context, MultiStageOperator input,
      DataSchema resultSchema, List<RexExpression.FunctionCall> aggCalls, List<Integer> filterArgs,
      PlanNode.NodeHint nodeHint, int limit, List<RelFieldCollation> collations) {
    super(context);
    _resultSchema = resultSchema;
    _aggFunctions = getAggFunctions(aggCalls);
    int numFunctions = _aggFunctions.length;

    // Process the filter argument indices
    int[] filterArgIds = new int[numFunctions];
    int maxFilterArgId = -1;
    for (int i = 0; i < numFunctions; i++) {
      filterArgIds[i] = filterArgs.get(i);
      maxFilterArgId = Math.max(maxFilterArgId, filterArgIds[i]);
    }
    _filterArgIds = filterArgIds;
    _maxFilterArgId = maxFilterArgId;
    _input = input;

    int groupTrimSize = Integer.MAX_VALUE;
    List<RelFieldCollation> trimCollations = null;
    int minGroupTrimSize = getMinGroupTrimSize(nodeHint, context.getOpChainMetadata());
    if (limit > 0 && minGroupTrimSize > 0) {
      // Trim is enabled
      if (collations.isEmpty()) {
        // TODO: Keeping only 'LIMIT' groups can cause inaccurate result because the groups are randomly selected
        //       without ordering. Consider ordering on group-by columns if no ordering is specified.
        groupTrimSize = limit;
      } else {
        groupTrimSize = GroupByUtils.getTableCapacity(limit, minGroupTrimSize);
        if (groupTrimSize < Integer.MAX_VALUE) {
          trimCollations = collations;
        }
      }
    }
    _groupTrimSize = groupTrimSize;
    _collations = trimCollations;

    _errorOnNumGroupsLimit = getErrorOnNumGroupsLimit(nodeHint, context.getOpChainMetadata());
  }

  private static int getMinGroupTrimSize(PlanNode.NodeHint nodeHint, Map<String, String> opChainMetadata) {
    String option = getOption(nodeHint, PinotHintOptions.AggregateOptions.MSE_MIN_GROUP_TRIM_SIZE);
    if (option != null) {
      return Integer.parseInt(option);
    }
    Integer minGroupTrimSize = QueryOptionsUtils.getMSEMinGroupTrimSize(opChainMetadata);
    return minGroupTrimSize != null ? minGroupTrimSize : Server.DEFAULT_MSE_MIN_GROUP_TRIM_SIZE;
  }

  private static boolean getErrorOnNumGroupsLimit(PlanNode.NodeHint nodeHint, Map<String, String> opChainMetadata) {
    String option = getOption(nodeHint, PinotHintOptions.AggregateOptions.ERROR_ON_NUM_GROUPS_LIMIT);
    return option != null ? Boolean.parseBoolean(option) : QueryOptionsUtils.getErrorOnNumGroupsLimit(opChainMetadata);
  }

  @Nullable
  private static String getOption(PlanNode.NodeHint nodeHint, String key) {
    Map<String, String> options = nodeHint.getHintOptions().get(PinotHintOptions.AGGREGATE_HINT_OPTIONS);
    return options != null ? options.get(key) : null;
  }

  /// Consumes the whole input.
  ///
  /// @return the last block, which must always be either an error or the end of the stream
  protected abstract MseBlock.Eos consumeInput();

  /// Builds the output block from what [#consumeInput()] accumulated.
  protected abstract MseBlock produceAggregatedBlock();

  @Override
  public void registerExecution(long time, int numRows, long memoryUsedBytes, long gcTimeMs) {
    _statMap.merge(AggregateOperator.StatKey.EXECUTION_TIME_MS, time);
    _statMap.merge(AggregateOperator.StatKey.EMITTED_ROWS, numRows);
    _statMap.merge(AggregateOperator.StatKey.ALLOCATED_MEMORY_BYTES, memoryUsedBytes);
    _statMap.merge(AggregateOperator.StatKey.GC_TIME_MS, gcTimeMs);
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return List.of(_input);
  }

  @Override
  protected MseBlock getNextBlock() {
    if (_eosBlock != null) {
      return _eosBlock;
    }
    MseBlock.Eos finalBlock = consumeInput();
    _eosBlock = finalBlock;

    if (finalBlock.isError()) {
      // The upstream failed, so no result will ever be produced from what we accumulated: drop it right away instead
      // of waiting for close()/cancel().
      releaseBuffers();
      return finalBlock;
    }
    MseBlock mseBlock = produceAggregatedBlock();
    releaseBuffers();
    return mseBlock;
  }

  /// Drops the group-by executor, and with it the group-by hash map and the aggregate result holders it owns. Marks
  /// the operator finished at the same time, so that a later [#getNextBlock()] returns the cached end of stream
  /// instead of trying to consume the input again with a dropped executor.
  @Override
  protected void releaseBuffers() {
    _groupByExecutor = null;
    if (_eosBlock == null) {
      _eosBlock = SuccessMseBlock.INSTANCE;
    }
  }

  @Override
  protected boolean hasBufferedState() {
    return _groupByExecutor != null;
  }

  @Override
  public StatMap<AggregateOperator.StatKey> copyStatMaps() {
    return new StatMap<>(_statMap);
  }

  /// Consumes the input blocks into the group-by executor.
  ///
  /// @return the last block, which must always be either an error or the end of the stream
  protected MseBlock.Eos consumeGroupBy() {
    assert _groupByExecutor != null;
    MseBlock block = _input.nextBlock();
    while (block.isData()) {
      _groupByExecutor.processBlock((MseBlock.Data) block);
      checkTerminationAndSampleUsage();
      block = _input.nextBlock();
    }
    return (MseBlock.Eos) block;
  }

  /// Builds the output block of the group-by executor: trims the groups and records the group stats.
  protected MseBlock produceGroupByBlock() {
    assert _groupByExecutor != null;
    List<Object[]> rows;
    if (_collations != null) {
      rows = _groupByExecutor.getResult(_collations, _groupTrimSize);
    } else {
      rows = _groupByExecutor.getResult(_groupTrimSize);
    }
    recordApproximatedResult();

    // Record stat before we check for limit so we can propagate to query response
    _statMap.merge(AggregateOperator.StatKey.NUM_GROUPS, _groupByExecutor.getNumGroups());

    if (rows.isEmpty()) {
      return _eosBlock;
    } else {
      MseBlock dataBlock = new RowHeapDataBlock(rows, _resultSchema, _aggFunctions);
      if (_groupByExecutor.getRowsProcessed() > _groupTrimSize) {
        _statMap.merge(AggregateOperator.StatKey.GROUPS_TRIMMED, true);
      }

      if (_groupByExecutor.isNumGroupsLimitReached()) {
        if (_errorOnNumGroupsLimit) {
          throw QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED.asException(
              "NUM_GROUPS_LIMIT has been reached at: " + _operatorId);
        } else {
          _statMap.merge(AggregateOperator.StatKey.NUM_GROUPS_LIMIT_REACHED, true);
          _input.earlyTerminate();
        }
      }
      if (_groupByExecutor.getNumGroups() >= _groupByExecutor.getNumGroupsWarningLimit()) {
        logger().warn("numGroups reached warning limit: {} (actual: {})",
            _groupByExecutor.getNumGroupsWarningLimit(), _groupByExecutor.getNumGroups());
        _statMap.merge(AggregateOperator.StatKey.NUM_GROUPS_WARNING_LIMIT_REACHED, true);
      }
      return dataBlock;
    }
  }

  /// Records a function that returned an approximate final result while the executor extracted the results, see
  /// [AggregationFunction#hasApproximatedResult()].
  protected void recordApproximatedResult() {
    if (AggregationFunctionUtils.hasApproximatedResult(_aggFunctions)) {
      _statMap.merge(AggregateOperator.StatKey.APPROXIMATED_RESULT, true);
    }
  }

  /// Builds the aggregation functions of `aggCalls`. An input reference becomes the `$<colId>` identifier.
  static AggregationFunction<?, ?>[] getAggFunctions(List<RexExpression.FunctionCall> aggCalls) {
    int numFunctions = aggCalls.size();
    AggregationFunction<?, ?>[] aggFunctions = new AggregationFunction[numFunctions];
    for (int i = 0; i < numFunctions; i++) {
      aggFunctions[i] = getAggFunction(aggCalls.get(i));
    }
    return aggFunctions;
  }

  private static AggregationFunction<?, ?> getAggFunction(RexExpression.FunctionCall functionCall) {
    String functionName = functionCall.getFunctionName();
    List<RexExpression> operands = functionCall.getFunctionOperands();
    int numArguments = operands.size();
    if (numArguments == 0) {
      Preconditions.checkState(functionName.equals("COUNT"),
          "Aggregate function without argument must be COUNT, got: %s", functionName);
      return COUNT_STAR_AGG_FUNCTION;
    }
    List<ExpressionContext> arguments = new ArrayList<>(numArguments);
    for (RexExpression operand : operands) {
      if (operand instanceof RexExpression.InputRef) {
        RexExpression.InputRef inputRef = (RexExpression.InputRef) operand;
        arguments.add(ExpressionContext.forIdentifier(fromColIdToIdentifier(inputRef.getIndex())));
      } else {
        assert operand instanceof RexExpression.Literal;
        arguments.add(
            ExpressionContext.forLiteral(CalciteRexExpressionParser.toLiteral((RexExpression.Literal) operand)));
      }
    }
    return AggregationFunctionFactory.getAggregationFunction(
        new FunctionContext(FunctionContext.Type.AGGREGATION, functionName, arguments), true);
  }

  private static String fromColIdToIdentifier(int colId) {
    return "$" + colId;
  }

  /// Returns the column id of an identifier built by [#getAggFunctions(List)].
  static int fromIdentifierToColId(String identifier) {
    Preconditions.checkArgument(identifier.charAt(0) == '$', "Got identifier not representing column index: %s",
        identifier);
    return Integer.parseInt(identifier.substring(1));
  }

  /// Returns the group keys as an array of column ids.
  static int[] getGroupKeyIds(List<Integer> groupKeys) {
    int numKeys = groupKeys.size();
    int[] groupKeyIds = new int[numKeys];
    for (int i = 0; i < numKeys; i++) {
      groupKeyIds[i] = groupKeys.get(i);
    }
    return groupKeyIds;
  }

  @VisibleForTesting
  int getGroupTrimSize() {
    return _groupTrimSize;
  }
}
