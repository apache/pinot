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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionFactory;
import org.apache.pinot.query.planner.logical.LiteralHintUtils;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.AbstractPlanNode;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.operator.block.DataBlockValSet;
import org.apache.pinot.query.runtime.operator.block.FilteredDataBlockValSet;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.query.runtime.plan.StageMetadata;
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
public class AggregateOperator extends MultiStageOperator {
  private static final String EXPLAIN_NAME = "AGGREGATE_OPERATOR";

  private final MultiStageOperator _inputOperator;
  private final DataSchema _resultSchema;
  private final DataSchema _inputSchema;
  private final AggType _aggType;

  // Map that maintains the mapping between columnName and the column ordinal index. It is used to fetch the required
  // column value from row-based container and fetch the input datatype for the column.
  private final Map<String, Integer> _colNameToIndexMap;
  private final Map<Integer, Map<Integer, Literal>> _aggCallSignatureMap;

  private boolean _hasReturnedAggregateBlock;

  private final boolean _isGroupByAggregation;
  private MultistageAggregationExecutor _aggregationExecutor;
  private MultistageGroupByExecutor _groupByExecutor;

  @VisibleForTesting
  public AggregateOperator(OpChainExecutionContext context, MultiStageOperator inputOperator, DataSchema resultSchema,
      DataSchema inputSchema, List<RexExpression> aggCalls, List<RexExpression> groupSet, AggType aggType,
      @Nullable List<Integer> filterArgIndices, @Nullable AbstractPlanNode.NodeHint nodeHint) {
    super(context);
    _inputOperator = inputOperator;
    _resultSchema = resultSchema;
    _inputSchema = inputSchema;
    _aggType = aggType;
    // filter arg index array
    int[] filterArgIndexArray;
    if (filterArgIndices == null || filterArgIndices.size() == 0) {
      filterArgIndexArray = null;
    } else {
      filterArgIndexArray = filterArgIndices.stream().mapToInt(Integer::intValue).toArray();
    }
    // filter operand and literal hints
    if (nodeHint != null && nodeHint._hintOptions != null
        && nodeHint._hintOptions.get(PinotHintOptions.INTERNAL_AGG_OPTIONS) != null) {
      _aggCallSignatureMap = LiteralHintUtils.hintStringToLiteralMap(
          nodeHint._hintOptions.get(PinotHintOptions.INTERNAL_AGG_OPTIONS)
              .get(PinotHintOptions.InternalAggregateOptions.AGG_CALL_SIGNATURE));
    } else {
      _aggCallSignatureMap = Collections.emptyMap();
    }

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
      StageMetadata stageMetadata = context.getStageMetadata();
      Map<String, String> customProperties =
          stageMetadata != null ? stageMetadata.getCustomProperties() : Collections.emptyMap();
      _groupByExecutor =
          new MultistageGroupByExecutor(groupByExpr, aggFunctions, filterArgIndexArray, aggType, _colNameToIndexMap,
              _resultSchema, customProperties, nodeHint);
    } else {
      _isGroupByAggregation = false;
      _aggregationExecutor =
          new MultistageAggregationExecutor(aggFunctions, filterArgIndexArray, aggType, _colNameToIndexMap,
              _resultSchema);
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
      TransferableBlock finalBlock;
      if (_isGroupByAggregation) {
        finalBlock = consumeGroupBy();
      } else {
        finalBlock = consumeAggregation();
      }

      // setting upstream error block
      if (finalBlock.isErrorBlock()) {
        return finalBlock;
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
    _hasReturnedAggregateBlock = true;
    if (_isGroupByAggregation) {
      List<Object[]> rows = _groupByExecutor.getResult();
      if (rows.isEmpty()) {
        return TransferableBlockUtils.getEndOfStreamTransferableBlock();
      } else {
        TransferableBlock dataBlock = new TransferableBlock(rows, _resultSchema, DataBlock.Type.ROW);
        if (_groupByExecutor.isNumGroupsLimitReached()) {
          dataBlock.addException(QueryException.SERVER_RESOURCE_LIMIT_EXCEEDED_ERROR_CODE,
              String.format("Reached numGroupsLimit of: %d for group-by, ignoring the extra groups",
                  _groupByExecutor.getNumGroupsLimit()));
        }
        return dataBlock;
      }
    } else {
      return new TransferableBlock(_aggregationExecutor.getResult(), _resultSchema, DataBlock.Type.ROW);
    }
  }

  /**
   * Consumes the input blocks as a group by
   * @return the last block, which must always be either an error or the end of the stream
   */
  private TransferableBlock consumeGroupBy() {
    TransferableBlock block = _inputOperator.nextBlock();
    while (block.isDataBlock()) {
      _groupByExecutor.processBlock(block, _inputSchema);
      block = _inputOperator.nextBlock();
    }
    return block;
  }

  /**
   * Consumes the input blocks as an aggregation
   * @return the last block, which must always be either an error or the end of the stream
   */
  private TransferableBlock consumeAggregation() {
    TransferableBlock block = _inputOperator.nextBlock();
    while (block.isDataBlock()) {
      _aggregationExecutor.processBlock(block, _inputSchema);
      block = _inputOperator.nextBlock();
    }
    return block;
  }

  private List<FunctionContext> getFunctionContexts(List<RexExpression> aggCalls) {
    List<RexExpression.FunctionCall> aggFunctionCalls =
        aggCalls.stream().map(RexExpression.FunctionCall.class::cast).collect(Collectors.toList());
    List<FunctionContext> functionContexts = new ArrayList<>();
    for (int aggIdx = 0; aggIdx < aggFunctionCalls.size(); aggIdx++) {
      RexExpression.FunctionCall functionCall = aggFunctionCalls.get(aggIdx);
      FunctionContext funcContext = convertRexExpressionsToFunctionContext(aggIdx, functionCall);
      functionContexts.add(funcContext);
    }
    return functionContexts;
  }

  private FunctionContext convertRexExpressionsToFunctionContext(int aggIdx,
      RexExpression.FunctionCall aggFunctionCall) {
    // Extract details from RexExpression aggFunctionCall.
    String functionName = aggFunctionCall.getFunctionName();
    List<RexExpression> functionOperands = aggFunctionCall.getFunctionOperands();

    List<ExpressionContext> aggArguments = new ArrayList<>();
    for (int argIdx = 0; argIdx < functionOperands.size(); argIdx++) {
      RexExpression operand = functionOperands.get(argIdx);
      ExpressionContext exprContext = convertRexExpressionToExpressionContext(aggIdx, argIdx, operand);
      aggArguments.add(exprContext);
    }

    // add additional arguments for aggFunctionCall
    if (_aggType.isInputIntermediateFormat()) {
      rewriteAggArgumentForIntermediateInput(aggArguments, aggIdx);
    }
    // This can only be true for COUNT aggregation functions on intermediate stage.
    // The literal value here does not matter. We create a dummy literal here just so that the count aggregation
    // has some column to process.
    if (aggArguments.isEmpty()) {
      aggArguments.add(ExpressionContext.forLiteralContext(FieldSpec.DataType.STRING, "__PLACEHOLDER__"));
    }

    return new FunctionContext(FunctionContext.Type.AGGREGATION, functionName, aggArguments);
  }

  private void rewriteAggArgumentForIntermediateInput(List<ExpressionContext> aggArguments, int aggIdx) {
    Map<Integer, Literal> aggCallSignature = _aggCallSignatureMap.get(aggIdx);
    if (aggCallSignature != null && !aggCallSignature.isEmpty()) {
      int argListSize = aggCallSignature.get(-1).getIntValue();
      for (int argIdx = 1; argIdx < argListSize; argIdx++) {
        Literal aggIdxLiteral = aggCallSignature.get(argIdx);
        if (aggIdxLiteral != null) {
          aggArguments.add(ExpressionContext.forLiteralContext(aggIdxLiteral));
        } else {
          aggArguments.add(ExpressionContext.forIdentifier("__PLACEHOLDER__"));
        }
      }
    }
  }

  private List<ExpressionContext> getGroupSet(List<RexExpression> groupBySetRexExpr) {
    List<ExpressionContext> groupByExprContext = new ArrayList<>();
    for (RexExpression groupByRexExpr : groupBySetRexExpr) {
      ExpressionContext exprContext = convertRexExpressionToExpressionContext(-1, -1, groupByRexExpr);
      groupByExprContext.add(exprContext);
    }

    return groupByExprContext;
  }

  private ExpressionContext convertRexExpressionToExpressionContext(int aggIdx, int argIdx, RexExpression rexExpr) {
    ExpressionContext exprContext;
    if (_aggCallSignatureMap.get(aggIdx) != null && _aggCallSignatureMap.get(aggIdx).get(argIdx) != null) {
      return ExpressionContext.forLiteralContext(_aggCallSignatureMap.get(aggIdx).get(argIdx));
    }

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
        exprContext = ExpressionContext.forLiteralContext(literalRexExp.getDataType().toDataType(), value);
        break;
      }
      default:
        throw new IllegalStateException("Aggregation Function operands or GroupBy columns cannot be a function.");
    }

    return exprContext;
  }

  // TODO: If the previous block is not mailbox received, this method is not efficient.  Then getDataBlock() will
  //  convert the unserialized format to serialized format of BaseDataBlock. Then it will convert it back to column
  //  value primitive type.
  static Map<ExpressionContext, BlockValSet> getBlockValSetMap(AggregationFunction aggFunction, TransferableBlock block,
      DataSchema inputDataSchema, Map<String, Integer> colNameToIndexMap, int filterArgIdx) {
    List<ExpressionContext> expressions = aggFunction.getInputExpressions();
    int numExpressions = expressions.size();
    if (numExpressions == 0) {
      return Collections.emptyMap();
    }

    Map<ExpressionContext, BlockValSet> blockValSetMap = new HashMap<>();
    for (ExpressionContext expression : expressions) {
      if (expression.getType().equals(ExpressionContext.Type.IDENTIFIER) && !"__PLACEHOLDER__".equals(
          expression.getIdentifier())) {
        int index = colNameToIndexMap.get(expression.getIdentifier());
        ColumnDataType dataType = inputDataSchema.getColumnDataType(index);
        Preconditions.checkState(block.getType().equals(DataBlock.Type.ROW), "Datablock type is not ROW");
        if (filterArgIdx == -1) {
          blockValSetMap.put(expression, new DataBlockValSet(dataType, block.getDataBlock(), index));
        } else {
          blockValSetMap.put(expression,
              new FilteredDataBlockValSet(dataType, block.getDataBlock(), index, filterArgIdx));
        }
      }
    }
    return blockValSetMap;
  }

  static int computeBlockNumRows(TransferableBlock block, int filterArgIdx) {
    if (filterArgIdx == -1) {
      return block.getNumRows();
    } else {
      int rowCount = 0;
      for (int rowId = 0; rowId < block.getNumRows(); rowId++) {
        // NOTE: The value of filterArgIdx is 0 (FALSE) or 1 (TRUE), so we can directly add them up
        rowCount += block.getDataBlock().getInt(rowId, filterArgIdx);
      }
      return rowCount;
    }
  }

  static Object extractValueFromRow(AggregationFunction aggregationFunction, Object[] row,
      Map<String, Integer> colNameToIndexMap) {
    List<ExpressionContext> expressions = aggregationFunction.getInputExpressions();
    ExpressionContext expr = expressions.get(0);
    ExpressionContext.Type exprType = expr.getType();
    if (exprType == ExpressionContext.Type.IDENTIFIER) {
      return row[colNameToIndexMap.get(expr.getIdentifier())];
    }
    Preconditions.checkState(exprType == ExpressionContext.Type.LITERAL, "Unsupported expression type: %s", exprType);
    return expr.getLiteral().getValue();
  }
}
