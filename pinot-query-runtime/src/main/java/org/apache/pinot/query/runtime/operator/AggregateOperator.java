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
import org.apache.pinot.common.datatable.DataTable;
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
import org.apache.pinot.core.query.aggregation.function.CountAggregationFunction;
import org.apache.pinot.core.util.DataBlockExtractUtils;
import org.apache.pinot.query.planner.logical.LiteralHintUtils;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.AbstractPlanNode;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * AggregateOperator is used to aggregate values over a set of group by keys.
 * Output data will be in the format of [group by key, aggregate result1, ... aggregate resultN]
 * When the list of aggregation calls is empty, this class is used to calculate distinct result based on group by keys.
 */
public class AggregateOperator extends MultiStageOperator<MultiStageOperator.BaseStatKeys> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AggregateOperator.class);
  private static final String EXPLAIN_NAME = "AGGREGATE_OPERATOR";
  private static final CountAggregationFunction COUNT_STAR_AGG_FUNCTION =
      new CountAggregationFunction(Collections.singletonList(ExpressionContext.forIdentifier("*")), false);
  private static final ExpressionContext PLACEHOLDER_IDENTIFIER = ExpressionContext.forIdentifier("__PLACEHOLDER__");

  private final MultiStageOperator<?> _inputOperator;
  private final DataSchema _resultSchema;
  private final AggType _aggType;
  private final MultistageAggregationExecutor _aggregationExecutor;
  private final MultistageGroupByExecutor _groupByExecutor;

  private boolean _hasConstructedAggregateBlock;

  public AggregateOperator(OpChainExecutionContext context, MultiStageOperator<?> inputOperator,
      DataSchema resultSchema, List<RexExpression> aggCalls, List<RexExpression> groupSet, AggType aggType,
      List<Integer> filterArgIndices, @Nullable AbstractPlanNode.NodeHint nodeHint) {
    super(context);
    _inputOperator = inputOperator;
    _resultSchema = resultSchema;
    _aggType = aggType;

    // Process literal hints
    Map<Integer, Map<Integer, Literal>> literalArgumentsMap = null;
    if (nodeHint != null) {
      Map<String, String> aggOptions = nodeHint._hintOptions.get(PinotHintOptions.INTERNAL_AGG_OPTIONS);
      if (aggOptions != null) {
        literalArgumentsMap = LiteralHintUtils.hintStringToLiteralMap(
            aggOptions.get(PinotHintOptions.InternalAggregateOptions.AGG_CALL_SIGNATURE));
      }
    }
    if (literalArgumentsMap == null) {
      literalArgumentsMap = Collections.emptyMap();
    }

    // Initialize the aggregation functions
    AggregationFunction<?, ?>[] aggFunctions = getAggFunctions(aggCalls, literalArgumentsMap);

    // Process the filter argument indices
    int numFunctions = aggFunctions.length;
    int[] filterArgIds = new int[numFunctions];
    int maxFilterArgId = -1;
    for (int i = 0; i < numFunctions; i++) {
      filterArgIds[i] = filterArgIndices.get(i);
      maxFilterArgId = Math.max(maxFilterArgId, filterArgIds[i]);
    }

    // Initialize the appropriate executor.
    if (groupSet.isEmpty()) {
      _aggregationExecutor =
          new MultistageAggregationExecutor(aggFunctions, filterArgIds, maxFilterArgId, aggType, _resultSchema);
      _groupByExecutor = null;
    } else {
      _groupByExecutor =
          new MultistageGroupByExecutor(getGroupKeyIds(groupSet), aggFunctions, filterArgIds, maxFilterArgId, aggType,
              _resultSchema, context.getOpChainMetadata(), nodeHint);
      _aggregationExecutor = null;
    }
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
    if (_hasConstructedAggregateBlock) {
      return TransferableBlockUtils.getEndOfStreamTransferableBlock();
    }
    TransferableBlock finalBlock = _aggregationExecutor != null ? consumeAggregation() : consumeGroupBy();
    // returning upstream error block if finalBlock contains error.
    if (finalBlock.isErrorBlock()) {
      return finalBlock;
    }
    return produceAggregatedBlock();
  }

  private TransferableBlock produceAggregatedBlock() {
    _hasConstructedAggregateBlock = true;
    if (_aggregationExecutor != null) {
      return new TransferableBlock(_aggregationExecutor.getResult(), _resultSchema, DataBlock.Type.ROW);
    } else {
      List<Object[]> rows = _groupByExecutor.getResult();
      if (rows.isEmpty()) {
        return TransferableBlockUtils.getEndOfStreamTransferableBlock();
      } else {
        TransferableBlock dataBlock = new TransferableBlock(rows, _resultSchema, DataBlock.Type.ROW);
        if (_groupByExecutor.isNumGroupsLimitReached()) {
          OperatorStats operatorStats = _opChainStats.getOperatorStats(_context, _operatorId);
          operatorStats.recordSingleStat(DataTable.MetadataKey.NUM_GROUPS_LIMIT_REACHED.getName(), "true");
          _inputOperator.earlyTerminate();
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

  private AggregationFunction<?, ?>[] getAggFunctions(List<RexExpression> aggCalls,
      Map<Integer, Map<Integer, Literal>> literalArgumentsMap) {
    int numFunctions = aggCalls.size();
    AggregationFunction<?, ?>[] aggFunctions = new AggregationFunction[numFunctions];
    if (!_aggType.isInputIntermediateFormat()) {
      for (int i = 0; i < numFunctions; i++) {
        Map<Integer, Literal> literalArguments = literalArgumentsMap.getOrDefault(i, Collections.emptyMap());
        aggFunctions[i] = getAggFunctionForRawInput((RexExpression.FunctionCall) aggCalls.get(i), literalArguments);
      }
    } else {
      for (int i = 0; i < numFunctions; i++) {
        Map<Integer, Literal> literalArguments = literalArgumentsMap.getOrDefault(i, Collections.emptyMap());
        aggFunctions[i] =
            getAggFunctionForIntermediateInput((RexExpression.FunctionCall) aggCalls.get(i), literalArguments);
      }
    }
    return aggFunctions;
  }

  private AggregationFunction<?, ?> getAggFunctionForRawInput(RexExpression.FunctionCall functionCall,
      Map<Integer, Literal> literalArguments) {
    String functionName = functionCall.getFunctionName();
    List<RexExpression> operands = functionCall.getFunctionOperands();
    int numArguments = operands.size();
    if (numArguments == 0) {
      Preconditions.checkState(functionName.equals("COUNT"),
          "Aggregate function without argument must be COUNT, got: %s", functionName);
      return COUNT_STAR_AGG_FUNCTION;
    }
    List<ExpressionContext> arguments = new ArrayList<>(numArguments);
    for (int i = 0; i < numArguments; i++) {
      Literal literalArgument = literalArguments.get(i);
      if (literalArgument != null) {
        arguments.add(ExpressionContext.forLiteralContext(literalArgument));
      } else {
        RexExpression operand = operands.get(i);
        switch (operand.getKind()) {
          case INPUT_REF:
            RexExpression.InputRef inputRef = (RexExpression.InputRef) operand;
            arguments.add(ExpressionContext.forIdentifier(fromColIdToIdentifier(inputRef.getIndex())));
            break;
          case LITERAL:
            RexExpression.Literal literalRexExp = (RexExpression.Literal) operand;
            arguments.add(ExpressionContext.forLiteralContext(literalRexExp.getDataType().toDataType(),
                literalRexExp.getValue()));
            break;
          default:
            throw new IllegalStateException("Illegal aggregation function operand type: " + operand.getKind());
        }
      }
    }

    return AggregationFunctionFactory.getAggregationFunction(
        new FunctionContext(FunctionContext.Type.AGGREGATION, functionName, arguments), true);
  }

  private static AggregationFunction<?, ?> getAggFunctionForIntermediateInput(RexExpression.FunctionCall functionCall,
      Map<Integer, Literal> literalArguments) {
    String functionName = functionCall.getFunctionName();
    List<RexExpression> operands = functionCall.getFunctionOperands();
    int numArguments = operands.size();
    Preconditions.checkState(numArguments == 1, "Intermediate aggregate must have 1 argument, got: %s", numArguments);
    RexExpression operand = operands.get(0);
    Preconditions.checkState(operand.getKind() == SqlKind.INPUT_REF,
        "Intermediate aggregate argument must be an input reference, got: %s", operand.getKind());
    // We might need to append extra arguments extracted from the hint to match the signature of the aggregation
    Literal numArgumentsLiteral = literalArguments.get(-1);
    if (numArgumentsLiteral == null) {
      return AggregationFunctionFactory.getAggregationFunction(
          new FunctionContext(FunctionContext.Type.AGGREGATION, functionName, Collections.singletonList(
              ExpressionContext.forIdentifier(fromColIdToIdentifier(((RexExpression.InputRef) operand).getIndex())))),
          true);
    } else {
      int numExpectedArguments = numArgumentsLiteral.getIntValue();
      List<ExpressionContext> arguments = new ArrayList<>(numExpectedArguments);
      arguments.add(
          ExpressionContext.forIdentifier(fromColIdToIdentifier(((RexExpression.InputRef) operand).getIndex())));
      for (int i = 1; i < numExpectedArguments; i++) {
        Literal literalArgument = literalArguments.get(i);
        if (literalArgument != null) {
          arguments.add(ExpressionContext.forLiteralContext(literalArgument));
        } else {
          arguments.add(PLACEHOLDER_IDENTIFIER);
        }
      }
      return AggregationFunctionFactory.getAggregationFunction(
          new FunctionContext(FunctionContext.Type.AGGREGATION, functionName, arguments), true);
    }
  }

  private static String fromColIdToIdentifier(int colId) {
    return "$" + colId;
  }

  private static int fromIdentifierToColId(String identifier) {
    Preconditions.checkArgument(identifier.charAt(0) == '$', "Got identifier not representing column index: %s",
        identifier);
    return Integer.parseInt(identifier.substring(1));
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
      TransferableBlock block) {
    List<ExpressionContext> expressions = aggFunction.getInputExpressions();
    int numExpressions = expressions.size();
    if (numExpressions == 0) {
      return Collections.emptyMap();
    }
    DataSchema dataSchema = block.getDataSchema();
    Map<ExpressionContext, BlockValSet> blockValSetMap = new HashMap<>();
    if (block.isContainerConstructed()) {
      List<Object[]> rows = block.getContainer();
      for (ExpressionContext expression : expressions) {
        String identifier = expression.getIdentifier();
        if (identifier != null) {
          int colId = fromIdentifierToColId(identifier);
          blockValSetMap.put(expression,
              new RowBasedBlockValSet(dataSchema.getColumnDataType(colId), rows, colId, true));
        }
      }
    } else {
      DataBlock dataBlock = block.getDataBlock();
      for (ExpressionContext expression : expressions) {
        String identifier = expression.getIdentifier();
        if (identifier != null) {
          int colId = fromIdentifierToColId(identifier);
          blockValSetMap.put(expression, new DataBlockValSet(dataSchema.getColumnDataType(colId), dataBlock, colId));
        }
      }
    }
    return blockValSetMap;
  }

  static Map<ExpressionContext, BlockValSet> getFilteredBlockValSetMap(AggregationFunction<?, ?> aggFunction,
      TransferableBlock block, int numMatchedRows, RoaringBitmap matchedBitmap) {
    List<ExpressionContext> expressions = aggFunction.getInputExpressions();
    int numExpressions = expressions.size();
    if (numExpressions == 0) {
      return Collections.emptyMap();
    }
    DataSchema dataSchema = block.getDataSchema();
    Map<ExpressionContext, BlockValSet> blockValSetMap = new HashMap<>();
    if (block.isContainerConstructed()) {
      List<Object[]> rows = block.getContainer();
      for (ExpressionContext expression : expressions) {
        String identifier = expression.getIdentifier();
        if (identifier != null) {
          int colId = fromIdentifierToColId(identifier);
          blockValSetMap.put(expression,
              new FilteredRowBasedBlockValSet(dataSchema.getColumnDataType(colId), rows, colId, numMatchedRows,
                  matchedBitmap, true));
        }
      }
    } else {
      DataBlock dataBlock = block.getDataBlock();
      for (ExpressionContext expression : expressions) {
        String identifier = expression.getIdentifier();
        if (identifier != null) {
          int colId = fromIdentifierToColId(identifier);
          blockValSetMap.put(expression,
              new FilteredDataBlockValSet(block.getDataSchema().getColumnDataType(colId), dataBlock, colId,
                  numMatchedRows, matchedBitmap));
        }
      }
    }
    return blockValSetMap;
  }

  static Object[] getIntermediateResults(AggregationFunction<?, ?> aggFunctions, TransferableBlock block) {
    ExpressionContext firstArgument = aggFunctions.getInputExpressions().get(0);
    Preconditions.checkState(firstArgument.getType() == ExpressionContext.Type.IDENTIFIER,
        "Expected the first argument to be IDENTIFIER, got: %s", firstArgument.getType());
    int colId = fromIdentifierToColId(firstArgument.getIdentifier());
    int numRows = block.getNumRows();
    if (block.isContainerConstructed()) {
      Object[] values = new Object[numRows];
      List<Object[]> rows = block.getContainer();
      for (int rowId = 0; rowId < numRows; rowId++) {
        values[rowId] = rows.get(rowId)[colId];
      }
      return values;
    } else {
      return DataBlockExtractUtils.extractColumn(block.getDataBlock(), colId);
    }
  }
}
