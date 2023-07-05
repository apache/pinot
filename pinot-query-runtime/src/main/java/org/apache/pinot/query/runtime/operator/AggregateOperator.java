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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionFactory;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.data.FieldSpec;


/**
 *
 * AggregateOperator is used to aggregate values over a set of group by keys.
 * Output data will be in the format of [group by key, aggregate result1, ... aggregate resultN]
 * Currently, we only support the following aggregation functions:
 * 1. SUM
 * 2. COUNT
 * 3. MIN
 * 4. MAX
 * 5. DistinctCount and Count(Distinct)
 * 6. AVG
 * 7. FourthMoment
 * 8. BoolAnd and BoolOr
 *
 * When the list of aggregation calls is empty, this class is used to calculate distinct result based on group by keys.
 * In this case, the input can be any type.
 *
 * If the list of aggregation calls is not empty, the input of aggregation has to be a number.
 * Note: This class performs aggregation over the double value of input.
 * If the input is single value, the output type will be input type. Otherwise, the output type will be double.
 */
// TODO(Sonam): Rename to AggregateOperator when merging Planner support.
public class AggregateOperator extends MultiStageOperator {
  private static final String EXPLAIN_NAME = "AGGREGATE_OPERATOR";

  private final MultiStageOperator _inputOperator;
  private final DataSchema _resultSchema;
  private final DataSchema _inputSchema;

  // Map that maintains the mapping between columnName and the column ordinal index. It is used to fetch the required
  // column value from row-based container and fetch the input datatype for the column.
  private final HashMap<String, Integer> _colNameToIndexMap;

  private TransferableBlock _upstreamErrorBlock;
  private boolean _readyToConstruct;
  private boolean _hasReturnedAggregateBlock;

  private final boolean _isGroupByAggregation;
  private MultistageAggregationExecutor _aggregationExecutor;
  private MultistageGroupByExecutor _groupByExecutor;

  // TODO: refactor Pinot Reducer code to support the intermediate stage agg operator.
  // aggCalls has to be a list of FunctionCall and cannot be null
  // groupSet has to be a list of InputRef and cannot be null
  // TODO: Add these two checks when we confirm we can handle error in upstream ctor call.

  @VisibleForTesting
  public AggregateOperator(OpChainExecutionContext context, MultiStageOperator inputOperator,
      DataSchema resultSchema, DataSchema inputSchema, List<RexExpression> aggCalls, List<RexExpression> groupSet,
      PinotHintOptions.InternalAggregateOptions.AggType aggType) {
    super(context);
    _inputOperator = inputOperator;
    _resultSchema = resultSchema;
    _inputSchema = inputSchema;

    _upstreamErrorBlock = null;
    _readyToConstruct = false;
    _hasReturnedAggregateBlock = false;
    _colNameToIndexMap = new HashMap<>();

    // Convert groupSet to ExpressionContext that our aggregation functions understand.
    List<ExpressionContext> groupByExpr = getGroupSet(groupSet);
    List<FunctionContext> functionContexts = getFunctionContexts(aggCalls);
    AggregationFunction[] aggFunctions = new AggregationFunction[functionContexts.size()];

    for (int i = 0; i < functionContexts.size(); i++) {
      aggFunctions[i] = AggregationFunctionFactory.getAggregationFunction(functionContexts.get(i), true);
    }

    // Initialize the appropriate executor.
    if (!groupSet.isEmpty()) {
      _isGroupByAggregation = true;
      _groupByExecutor = new MultistageGroupByExecutor(groupByExpr, aggFunctions, aggType, _colNameToIndexMap);
    } else {
      _isGroupByAggregation = false;
      _aggregationExecutor = new MultistageAggregationExecutor(aggFunctions, aggType, _colNameToIndexMap);
    }
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
    List<Object[]> rows = _isGroupByAggregation ? _groupByExecutor.getResult() : _aggregationExecutor.getResult();

    _hasReturnedAggregateBlock = true;
    if (rows.size() == 0) {
      if (!_isGroupByAggregation) {
        Object[] row = _aggregationExecutor.constructEmptyAggResultRow();
        return new TransferableBlock(Collections.singletonList(row), _resultSchema, DataBlock.Type.ROW);
      } else {
        return TransferableBlockUtils.getEndOfStreamTransferableBlock();
      }
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

      if (_isGroupByAggregation) {
        _groupByExecutor.processBlock(block, _inputSchema);
      } else {
        _aggregationExecutor.processBlock(block, _inputSchema);
      }

      block = _inputOperator.nextBlock();
    }
    return false;
  }

  private List<FunctionContext> getFunctionContexts(List<RexExpression> aggCalls) {
    List<RexExpression.FunctionCall> aggFunctionCalls =
        aggCalls.stream().map(RexExpression.FunctionCall.class::cast).collect(Collectors.toList());
    List<FunctionContext> functionContexts = new ArrayList<>();
    for (RexExpression.FunctionCall functionCall : aggFunctionCalls) {
      FunctionContext funcContext = convertRexExpressionsToFunctionContext(functionCall);
      functionContexts.add(funcContext);
    }
    return functionContexts;
  }

  private FunctionContext convertRexExpressionsToFunctionContext(RexExpression.FunctionCall aggFunctionCall) {
    // Extract details from RexExpression aggFunctionCall.
    String functionName = aggFunctionCall.getFunctionName();
    List<RexExpression> functionOperands = aggFunctionCall.getFunctionOperands();

    List<ExpressionContext> aggArguments = new ArrayList<>();
    for (RexExpression operand : functionOperands) {
      ExpressionContext exprContext = convertRexExpressionToExpressionContext(operand);
      aggArguments.add(exprContext);
    }

    if (aggArguments.isEmpty()) {
      // This can only be true for COUNT aggregation functions.
      // The literal value here does not matter. We create a dummy literal here just so that the count aggregation
      // has some column to process.
      ExpressionContext literalExpr = ExpressionContext.forLiteralContext(FieldSpec.DataType.LONG, 1L);
      aggArguments.add(literalExpr);
    }

    FunctionContext functionContext = new FunctionContext(FunctionContext.Type.AGGREGATION, functionName, aggArguments);
    return functionContext;
  }

  private List<ExpressionContext> getGroupSet(List<RexExpression> groupBySetRexExpr) {
    List<ExpressionContext> groupByExprContext = new ArrayList<>();
    for (RexExpression groupByRexExpr : groupBySetRexExpr) {
      ExpressionContext exprContext = convertRexExpressionToExpressionContext(groupByRexExpr);
      groupByExprContext.add(exprContext);
    }

    return groupByExprContext;
  }

  private ExpressionContext convertRexExpressionToExpressionContext(RexExpression rexExpr) {
    ExpressionContext exprContext;

    // This is used only for aggregation arguments and groupby columns. The rexExpression can never be a function type.
    switch (rexExpr.getKind()) {
      case INPUT_REF: {
        RexExpression.InputRef inputRef = (RexExpression.InputRef) rexExpr;
        int identifierIndex = inputRef.getIndex();
        String columnName = _inputSchema.getColumnName(identifierIndex);
        // Calcite generates unique column names for aggregation functions. For example, select avg(col1), sum(col1)
        // will generate names $f0 and $f1 for avg and sum respectively. We use a map to store the name -> index
        // mapping to extract the required column value from row-based container and fetch the input datatype for the
        // column.
        _colNameToIndexMap.put(columnName, identifierIndex);
        exprContext = ExpressionContext.forIdentifier(columnName);
        break;
      }
      case LITERAL: {
        RexExpression.Literal literalRexExp = (RexExpression.Literal) rexExpr;
        Object value = literalRexExp.getValue();
        exprContext = ExpressionContext.forLiteralContext(literalRexExp.getDataType(), value);
        break;
      }
      default:
        throw new IllegalStateException("Aggregation Function operands or GroupBy columns cannot be a function.");
    }

    return exprContext;
  }
}
