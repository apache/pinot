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
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datatable.StatMap;
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
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.AbstractPlanNode;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * AggregateOperator is used to aggregate values over a set of group by keys.
 * Output data will be in the format of [group by key, aggregate result1, ... aggregate resultN]
 * When the list of aggregation calls is empty, this class is used to calculate distinct result based on group by keys.
 */
public class AggregateOperator extends MultiStageOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(AggregateOperator.class);
  private static final String EXPLAIN_NAME = "AGGREGATE_OPERATOR";
  private static final CountAggregationFunction COUNT_STAR_AGG_FUNCTION =
      new CountAggregationFunction(Collections.singletonList(ExpressionContext.forIdentifier("*")), false);

  private final MultiStageOperator _inputOperator;
  private final DataSchema _resultSchema;
  private final MultistageAggregationExecutor _aggregationExecutor;
  private final MultistageGroupByExecutor _groupByExecutor;
  @Nullable
  private TransferableBlock _eosBlock;
  private final StatMap<StatKey> _statMap = new StatMap<>(StatKey.class);

  private boolean _hasConstructedAggregateBlock;

  public AggregateOperator(OpChainExecutionContext context, MultiStageOperator inputOperator, DataSchema resultSchema,
      List<RexExpression> aggCalls, List<RexExpression> groupSet, AggType aggType, List<Integer> filterArgIndices,
      @Nullable AbstractPlanNode.NodeHint nodeHint) {
    super(context);
    _inputOperator = inputOperator;
    _resultSchema = resultSchema;

    // Initialize the aggregation functions
    AggregationFunction<?, ?>[] aggFunctions = getAggFunctions(aggCalls);

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
  public void registerExecution(long time, int numRows) {
    _statMap.merge(StatKey.EXECUTION_TIME_MS, time);
    _statMap.merge(StatKey.EMITTED_ROWS, numRows);
  }

  @Override
  public Type getOperatorType() {
    return Type.AGGREGATE;
  }

  @Override
  protected Logger logger() {
    return LOGGER;
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
    if (_hasConstructedAggregateBlock) {
      assert _eosBlock != null;
      return _eosBlock;
    }
    TransferableBlock finalBlock = _aggregationExecutor != null ? consumeAggregation() : consumeGroupBy();
    // returning upstream error block if finalBlock contains error.
    if (finalBlock.isErrorBlock()) {
      return finalBlock;
    }
    assert finalBlock.isSuccessfulEndOfStreamBlock() : "Final block must be EOS block";
    _eosBlock = updateEosBlock(finalBlock, _statMap);
    return produceAggregatedBlock(finalBlock);
  }

  private TransferableBlock produceAggregatedBlock(TransferableBlock finalBlock) {
    _hasConstructedAggregateBlock = true;
    if (_aggregationExecutor != null) {
      return new TransferableBlock(_aggregationExecutor.getResult(), _resultSchema, DataBlock.Type.ROW);
    } else {
      List<Object[]> rows = _groupByExecutor.getResult();
      if (rows.isEmpty()) {
        return _eosBlock;
      } else {
        TransferableBlock dataBlock = new TransferableBlock(rows, _resultSchema, DataBlock.Type.ROW);
        if (_groupByExecutor.isNumGroupsLimitReached()) {
          _statMap.merge(StatKey.NUM_GROUPS_LIMIT_REACHED, true);
          _inputOperator.earlyTerminate();
        }
        return dataBlock;
      }
    }
  }

  /**
   * Consumes the input blocks as a group by
   *
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
   *
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

  private AggregationFunction<?, ?>[] getAggFunctions(List<RexExpression> aggCalls) {
    int numFunctions = aggCalls.size();
    AggregationFunction<?, ?>[] aggFunctions = new AggregationFunction[numFunctions];
    for (int i = 0; i < numFunctions; i++) {
      aggFunctions[i] = getAggFunction((RexExpression.FunctionCall) aggCalls.get(i));
    }
    return aggFunctions;
  }

  private AggregationFunction<?, ?> getAggFunction(RexExpression.FunctionCall functionCall) {
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
        RexExpression.Literal literal = (RexExpression.Literal) operand;
        DataType dataType = literal.getDataType().toDataType();
        Object value = literal.getValue();
        // TODO: Fix BOOLEAN literal to directly store true/false
        if (dataType == DataType.BOOLEAN) {
          value = BooleanUtils.fromNonNullInternalValue(value);
        }
        arguments.add(ExpressionContext.forLiteralContext(dataType, value));
      }
    }
    return AggregationFunctionFactory.getAggregationFunction(
        new FunctionContext(FunctionContext.Type.AGGREGATION, functionName, arguments), true);
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
      groupKeyIds[i] = ((RexExpression.InputRef) groupSet.get(i)).getIndex();
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
    NUM_GROUPS_LIMIT_REACHED(StatMap.Type.BOOLEAN);

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
