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
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.rel.hint.PinotHintOptions;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.docvalsets.DataBlockValSet;
import org.apache.pinot.core.operator.docvalsets.FilteredDataBlockValSet;
import org.apache.pinot.core.operator.docvalsets.FilteredRowBasedBlockValSet;
import org.apache.pinot.core.operator.docvalsets.RowBasedBlockValSet;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionFactory;
import org.apache.pinot.core.util.DataBlockExtractUtils;
import org.apache.pinot.query.planner.logical.LiteralHintUtils;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.AbstractPlanNode;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.RoaringBitmap;


/**
 * AggregateOperator is used to aggregate values over a set of group by keys.
 * Output data will be in the format of [group by key, aggregate result1, ... aggregate resultN]
 * When the list of aggregation calls is empty, this class is used to calculate distinct result based on group by keys.
 */
public class AggregateOperator extends MultiStageOperator {
  private static final String EXPLAIN_NAME = "AGGREGATE_OPERATOR";

  private final MultiStageOperator _inputOperator;
  private final DataSchema _resultSchema;
  private final DataSchema _inputSchema;
  private final AggType _aggType;

  // Map that maintains the mapping between columnName and the column ordinal index. It is used to fetch the required
  // column value from row-based container and fetch the input datatype for the column.
  // TODO: Pass the column index instead of converting back and forth
  private final Map<String, Integer> _colNameToIndexMap;

  private final Map<Integer, Map<Integer, Literal>> _aggCallSignatureMap;

  private MultistageAggregationExecutor _aggregationExecutor;
  private MultistageGroupByExecutor _groupByExecutor;
  private boolean _hasReturnedAggregateBlock;

  public AggregateOperator(OpChainExecutionContext context, MultiStageOperator inputOperator, DataSchema resultSchema,
      DataSchema inputSchema, List<RexExpression> aggCalls, List<RexExpression> groupSet, AggType aggType,
      List<Integer> filterArgIndices, @Nullable AbstractPlanNode.NodeHint nodeHint) {
    super(context);
    _inputOperator = inputOperator;
    _resultSchema = resultSchema;
    _inputSchema = inputSchema;
    _aggType = aggType;

    // Process literal hints
    if (nodeHint != null && nodeHint._hintOptions != null
        && nodeHint._hintOptions.get(PinotHintOptions.INTERNAL_AGG_OPTIONS) != null) {
      _aggCallSignatureMap = LiteralHintUtils.hintStringToLiteralMap(
          nodeHint._hintOptions.get(PinotHintOptions.INTERNAL_AGG_OPTIONS)
              .get(PinotHintOptions.InternalAggregateOptions.AGG_CALL_SIGNATURE));
    } else {
      _aggCallSignatureMap = Collections.emptyMap();
    }

    // Initialize the aggregation functions
    _colNameToIndexMap = new HashMap<>();
    List<FunctionContext> functionContexts = getFunctionContexts(aggCalls);
    int numFunctions = functionContexts.size();
    AggregationFunction<?, ?>[] aggFunctions = new AggregationFunction[numFunctions];
    for (int i = 0; i < numFunctions; i++) {
      aggFunctions[i] = AggregationFunctionFactory.getAggregationFunction(functionContexts.get(i), true);
    }

    // Process the filter argument indices
    int[] filterArgIds = new int[numFunctions];
    int maxFilterArgId = -1;
    for (int i = 0; i < numFunctions; i++) {
      filterArgIds[i] = filterArgIndices.get(i);
      maxFilterArgId = Math.max(maxFilterArgId, filterArgIds[i]);
    }

    // Initialize the appropriate executor.
    if (groupSet.isEmpty()) {
      _aggregationExecutor =
          new MultistageAggregationExecutor(aggFunctions, filterArgIds, maxFilterArgId, aggType, _colNameToIndexMap,
              _resultSchema);
      _groupByExecutor = null;
    } else {
      _groupByExecutor =
          new MultistageGroupByExecutor(getGroupKeyIds(groupSet), aggFunctions, filterArgIds, maxFilterArgId, aggType,
              _colNameToIndexMap, _resultSchema, context.getOpChainMetadata(), nodeHint);
      _aggregationExecutor = null;
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
      TransferableBlock finalBlock = _aggregationExecutor != null ? consumeAggregation() : consumeGroupBy();

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
    if (_aggregationExecutor != null) {
      return new TransferableBlock(_aggregationExecutor.getResult(), _resultSchema, DataBlock.Type.ROW);
    } else {
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
    }
  }

  /**
   * Consumes the input blocks as a group by
   * @return the last block, which must always be either an error or the end of the stream
   */
  private TransferableBlock consumeGroupBy() {
    TransferableBlock block = _inputOperator.nextBlock();
    while (block.isDataBlock()) {
      _groupByExecutor.processBlock(block);
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
      _aggregationExecutor.processBlock(block);
      block = _inputOperator.nextBlock();
    }
    return block;
  }

  private List<FunctionContext> getFunctionContexts(List<RexExpression> aggCalls) {
    int numFunctions = aggCalls.size();
    List<FunctionContext> functionContexts = new ArrayList<>(numFunctions);
    for (int i = 0; i < numFunctions; i++) {
      RexExpression.FunctionCall functionCall = (RexExpression.FunctionCall) aggCalls.get(i);
      FunctionContext funcContext = convertRexExpressionsToFunctionContext(i, functionCall);
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

  private int[] getGroupKeyIds(List<RexExpression> groupSet) {
    int numKeys = groupSet.size();
    int[] groupKeyIds = new int[numKeys];
    for (int i = 0; i < numKeys; i++) {
      RexExpression rexExp = groupSet.get(i);
      Preconditions.checkState(rexExp.getKind() == SqlKind.INPUT_REF, "Group key must be an input reference, got: %s",
          rexExp.getKind());
      groupKeyIds[i] = ((RexExpression.InputRef) rexExp).getIndex();
    }
    return groupKeyIds;
  }

  static RoaringBitmap getMatchedBitmap(TransferableBlock block, int filterArgId) {
    Preconditions.checkArgument(filterArgId >= 0, "Got negative filter argument id: %s", filterArgId);
    RoaringBitmap matchedBitmap = new RoaringBitmap();
    if (block.isContainerConstructed()) {
      List<Object[]> rows = block.getContainer();
      int numRows = rows.size();
      for (int rowId = 0; rowId < numRows; rowId++) {
        if ((int) rows.get(rowId)[filterArgId] == 1) {
          matchedBitmap.add(rowId);
        }
      }
    } else {
      DataBlock dataBlock = block.getDataBlock();
      int numRows = dataBlock.getNumberOfRows();
      for (int rowId = 0; rowId < numRows; rowId++) {
        if (dataBlock.getInt(rowId, filterArgId) == 1) {
          matchedBitmap.add(rowId);
        }
      }
    }
    return matchedBitmap;
  }

  static Map<ExpressionContext, BlockValSet> getBlockValSetMap(AggregationFunction<?, ?> aggFunction,
      TransferableBlock block, Map<String, Integer> colNameToIndexMap) {
    List<ExpressionContext> expressions = aggFunction.getInputExpressions();
    int numExpressions = expressions.size();
    if (numExpressions == 0) {
      return Collections.emptyMap();
    }
    Map<ExpressionContext, BlockValSet> blockValSetMap = new HashMap<>();
    if (block.isContainerConstructed()) {
      List<Object[]> rows = block.getContainer();
      for (ExpressionContext expression : expressions) {
        if (expression.getType() == ExpressionContext.Type.IDENTIFIER && !"__PLACEHOLDER__".equals(
            expression.getIdentifier())) {
          int colId = colNameToIndexMap.get(expression.getIdentifier());
          blockValSetMap.put(expression,
              new RowBasedBlockValSet(block.getDataSchema().getColumnDataType(colId), rows, colId, true));
        }
      }
    } else {
      DataBlock dataBlock = block.getDataBlock();
      for (ExpressionContext expression : expressions) {
        if (expression.getType() == ExpressionContext.Type.IDENTIFIER && !"__PLACEHOLDER__".equals(
            expression.getIdentifier())) {
          int colId = colNameToIndexMap.get(expression.getIdentifier());
          blockValSetMap.put(expression,
              new DataBlockValSet(block.getDataSchema().getColumnDataType(colId), dataBlock, colId));
        }
      }
    }
    return blockValSetMap;
  }

  static Map<ExpressionContext, BlockValSet> getFilteredBlockValSetMap(AggregationFunction<?, ?> aggFunction,
      TransferableBlock block, Map<String, Integer> colNameToIndexMap, int numMatchedRows,
      RoaringBitmap matchedBitmap) {
    List<ExpressionContext> expressions = aggFunction.getInputExpressions();
    int numExpressions = expressions.size();
    if (numExpressions == 0) {
      return Collections.emptyMap();
    }
    Map<ExpressionContext, BlockValSet> blockValSetMap = new HashMap<>();
    if (block.isContainerConstructed()) {
      List<Object[]> rows = block.getContainer();
      for (ExpressionContext expression : expressions) {
        if (expression.getType() == ExpressionContext.Type.IDENTIFIER && !"__PLACEHOLDER__".equals(
            expression.getIdentifier())) {
          int colId = colNameToIndexMap.get(expression.getIdentifier());
          blockValSetMap.put(expression,
              new FilteredRowBasedBlockValSet(block.getDataSchema().getColumnDataType(colId), rows, colId,
                  numMatchedRows, matchedBitmap, true));
        }
      }
    } else {
      DataBlock dataBlock = block.getDataBlock();
      for (ExpressionContext expression : expressions) {
        if (expression.getType() == ExpressionContext.Type.IDENTIFIER && !"__PLACEHOLDER__".equals(
            expression.getIdentifier())) {
          int colId = colNameToIndexMap.get(expression.getIdentifier());
          blockValSetMap.put(expression,
              new FilteredDataBlockValSet(block.getDataSchema().getColumnDataType(colId), dataBlock, colId,
                  numMatchedRows, matchedBitmap));
        }
      }
    }
    return blockValSetMap;
  }

  static Object[] getIntermediateResults(AggregationFunction<?, ?> aggregationFunction, TransferableBlock block,
      Map<String, Integer> colNameToIndexMap) {
    ExpressionContext exp = aggregationFunction.getInputExpressions().get(0);
    Preconditions.checkState(exp.getType() == ExpressionContext.Type.IDENTIFIER, "Expected identifier, got: %s",
        exp.getType());
    Integer index = colNameToIndexMap.get(exp.getIdentifier());
    Preconditions.checkState(index != null, "Failed to find index for column: %s", exp.getIdentifier());
    int colId = index;
    int numValues = block.getNumRows();
    if (block.isContainerConstructed()) {
      Object[] values = new Object[numValues];
      List<Object[]> rows = block.getContainer();
      for (int i = 0; i < numValues; i++) {
        values[i] = rows.get(i)[colId];
      }
      return values;
    } else {
      return DataBlockExtractUtils.extractColumn(block.getDataBlock(), colId);
    }
  }
}
