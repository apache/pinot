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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.docvalsets.DataBlockValSet;
import org.apache.pinot.core.operator.docvalsets.FilteredDataBlockValSet;
import org.apache.pinot.core.operator.docvalsets.FilteredRowBasedBlockValSet;
import org.apache.pinot.core.operator.docvalsets.RowBasedBlockValSet;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionFactory;
import org.apache.pinot.core.query.aggregation.function.CountAggregationFunction;
import org.apache.pinot.core.util.DataBlockExtractUtils;
import org.apache.pinot.core.util.GroupByUtils;
import org.apache.pinot.query.parser.CalciteRexExpressionParser;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.operator.utils.SortUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * AggregateOperator is used to aggregate values over a (potentially empty) set of group by keys in V2/MSQE.
 * Output data will be in the format of [group by key, aggregate result1, ... aggregate resultN]
 * When the list of aggregation calls is empty, this class is used to calculate distinct result based on group by keys.
 */
public class AggregateOperator extends MultiStageOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(AggregateOperator.class);
  private static final String EXPLAIN_NAME = "AGGREGATE_OPERATOR";
  private static final CountAggregationFunction COUNT_STAR_AGG_FUNCTION =
      new CountAggregationFunction(Collections.singletonList(ExpressionContext.forIdentifier("*")), false);

  private final MultiStageOperator _input;
  private final DataSchema _resultSchema;
  private final AggregationFunction<?, ?>[] _aggFunctions;
  private final MultistageAggregationExecutor _aggregationExecutor;
  private final MultistageGroupByExecutor _groupByExecutor;

  @Nullable
  private MseBlock.Eos _eosBlock;
  private final StatMap<StatKey> _statMap = new StatMap<>(StatKey.class);

  private final boolean _errorOnNumGroupsLimit;

  // trimming - related members
  private final int _groupTrimSize;
  /**
   * Comparator is used in priority queue, and the order is reversed so that peek() returns the smallest row to be
   * compared with other rows.
   */
  @Nullable
  private final Comparator<Object[]> _comparator;

  public AggregateOperator(OpChainExecutionContext context, MultiStageOperator input, AggregateNode node) {
    super(context);
    _input = input;
    _resultSchema = node.getDataSchema();
    _aggFunctions = getAggFunctions(node.getAggCalls());
    int numFunctions = _aggFunctions.length;

    // Process the filter argument indices
    List<Integer> filterArgs = node.getFilterArgs();
    int[] filterArgIds = new int[numFunctions];
    int maxFilterArgId = -1;
    for (int i = 0; i < numFunctions; i++) {
      filterArgIds[i] = filterArgs.get(i);
      maxFilterArgId = Math.max(maxFilterArgId, filterArgIds[i]);
    }

    List<Integer> groupKeys = node.getGroupKeys();

    int groupTrimSize = Integer.MAX_VALUE;
    Comparator<Object[]> comparator = null;
    int limit = node.getLimit();
    int minGroupTrimSize = getMinGroupTrimSize(node.getNodeHint(), context.getOpChainMetadata());
    if (limit > 0 && minGroupTrimSize > 0) {
      // Trim is enabled
      List<RelFieldCollation> collations = node.getCollations();
      if (collations.isEmpty()) {
        // TODO: Keeping only 'LIMIT' groups can cause inaccurate result because the groups are randomly selected
        //       without ordering. Consider ordering on group-by columns if no ordering is specified.
        groupTrimSize = limit;
      } else {
        groupTrimSize = GroupByUtils.getTableCapacity(limit, minGroupTrimSize);
        if (groupTrimSize < Integer.MAX_VALUE) {
          comparator = new SortUtils.SortComparator(_resultSchema, collations, true);
        }
      }
    }
    _groupTrimSize = groupTrimSize;
    _comparator = comparator;

    _errorOnNumGroupsLimit = getErrorOnNumGroupsLimit(node.getNodeHint(), context.getOpChainMetadata());

    // Initialize the appropriate executor.
    AggregateNode.AggType aggType = node.getAggType();
    // TODO: Allow leaf return final result for non-group-by queries
    boolean leafReturnFinalResult = node.isLeafReturnFinalResult();
    if (groupKeys.isEmpty()) {
      _aggregationExecutor =
          new MultistageAggregationExecutor(_aggFunctions, filterArgIds, maxFilterArgId, aggType, _resultSchema);
      _groupByExecutor = null;
    } else {
      _groupByExecutor =
          new MultistageGroupByExecutor(getGroupKeyIds(groupKeys), _aggFunctions, filterArgIds, maxFilterArgId, aggType,
              leafReturnFinalResult, _resultSchema, context.getOpChainMetadata(), node.getNodeHint());
      _aggregationExecutor = null;
    }
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
    return List.of(_input);
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected MseBlock getNextBlock() {
    if (_eosBlock != null) {
      return _eosBlock;
    }
    MseBlock.Eos finalBlock = _aggregationExecutor != null ? consumeAggregation() : consumeGroupBy();
    _eosBlock = finalBlock;

    if (finalBlock.isError()) {
      return finalBlock;
    }
    return produceAggregatedBlock();
  }

  private MseBlock produceAggregatedBlock() {
    if (_aggregationExecutor != null) {
      return new RowHeapDataBlock(_aggregationExecutor.getResult(), _resultSchema, _aggFunctions);
    } else {
      List<Object[]> rows;
      if (_comparator != null) {
        rows = _groupByExecutor.getResult(_comparator, _groupTrimSize);
      } else {
        rows = _groupByExecutor.getResult(_groupTrimSize);
      }

      if (rows.isEmpty()) {
        return _eosBlock;
      } else {
        MseBlock dataBlock = new RowHeapDataBlock(rows, _resultSchema, _aggFunctions);
        if (_groupByExecutor.isNumGroupsLimitReached()) {
          if (_errorOnNumGroupsLimit) {
            _input.earlyTerminate();
            throw new RuntimeException("NUM_GROUPS_LIMIT has been reached at " + _operatorId);
          } else {
            _statMap.merge(StatKey.NUM_GROUPS_LIMIT_REACHED, true);
            _input.earlyTerminate();
          }
        }
        return dataBlock;
      }
    }
  }

  @Override
  protected StatMap<?> copyStatMaps() {
    return new StatMap<>(_statMap);
  }

  /**
   * Consumes the input blocks as a group by
   *
   * @return the last block, which must always be either an error or the end of the stream
   */
  private MseBlock.Eos consumeGroupBy() {
    MseBlock block = _input.nextBlock();
    while (block.isData()) {
      _groupByExecutor.processBlock((MseBlock.Data) block);
      sampleAndCheckInterruption();
      block = _input.nextBlock();
    }
    return (MseBlock.Eos) block;
  }

  /**
   * Consumes the input blocks as an aggregation
   *
   * @return the last block, which must always be either an error or the end of the stream
   */
  private MseBlock.Eos consumeAggregation() {
    MseBlock block = _input.nextBlock();
    while (block.isData()) {
      _aggregationExecutor.processBlock((MseBlock.Data) block);
      sampleAndCheckInterruption();
      block = _input.nextBlock();
    }
    return (MseBlock.Eos) block;
  }

  private AggregationFunction<?, ?>[] getAggFunctions(List<RexExpression.FunctionCall> aggCalls) {
    int numFunctions = aggCalls.size();
    AggregationFunction<?, ?>[] aggFunctions = new AggregationFunction[numFunctions];
    for (int i = 0; i < numFunctions; i++) {
      aggFunctions[i] = getAggFunction(aggCalls.get(i));
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

  private static int fromIdentifierToColId(String identifier) {
    Preconditions.checkArgument(identifier.charAt(0) == '$', "Got identifier not representing column index: %s",
        identifier);
    return Integer.parseInt(identifier.substring(1));
  }

  private int[] getGroupKeyIds(List<Integer> groupKeys) {
    int numKeys = groupKeys.size();
    int[] groupKeyIds = new int[numKeys];
    for (int i = 0; i < numKeys; i++) {
      groupKeyIds[i] = groupKeys.get(i);
    }
    return groupKeyIds;
  }

  static RoaringBitmap getMatchedBitmap(MseBlock.Data block, int filterArgId) {
    Preconditions.checkArgument(filterArgId >= 0, "Got negative filter argument id: %s", filterArgId);
    RoaringBitmap matchedBitmap = new RoaringBitmap();
    if (block.isRowHeap()) {
      List<Object[]> rows = block.asRowHeap().getRows();
      int numRows = rows.size();
      for (int rowId = 0; rowId < numRows; rowId++) {
        if ((int) rows.get(rowId)[filterArgId] == 1) {
          matchedBitmap.add(rowId);
        }
      }
    } else {
      DataBlock dataBlock = block.asSerialized().getDataBlock();
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
      MseBlock.Data block) {
    List<ExpressionContext> expressions = aggFunction.getInputExpressions();
    int numExpressions = expressions.size();
    if (numExpressions == 0) {
      return Collections.emptyMap();
    }
    DataSchema dataSchema = block.getDataSchema();
    assert dataSchema != null;
    Map<ExpressionContext, BlockValSet> blockValSetMap = new HashMap<>();
    if (block.isRowHeap()) {
      List<Object[]> rows = block.asRowHeap().getRows();
      for (ExpressionContext expression : expressions) {
        String identifier = expression.getIdentifier();
        if (identifier != null) {
          int colId = fromIdentifierToColId(identifier);
          blockValSetMap.put(expression,
              new RowBasedBlockValSet(dataSchema.getColumnDataType(colId), rows, colId, true));
        }
      }
    } else {
      DataBlock dataBlock = block.asSerialized().getDataBlock();
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
      MseBlock.Data block, int numMatchedRows, RoaringBitmap matchedBitmap) {
    List<ExpressionContext> expressions = aggFunction.getInputExpressions();
    int numExpressions = expressions.size();
    if (numExpressions == 0) {
      return Collections.emptyMap();
    }
    DataSchema dataSchema = block.getDataSchema();
    assert dataSchema != null;
    Map<ExpressionContext, BlockValSet> blockValSetMap = new HashMap<>();
    if (block.isRowHeap()) {
      List<Object[]> rows = block.asRowHeap().getRows();
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
      DataBlock dataBlock = block.asSerialized().getDataBlock();
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

  static Object[] getIntermediateResults(AggregationFunction<?, ?> aggFunction, MseBlock.Data block) {
    ExpressionContext firstArgument = aggFunction.getInputExpressions().get(0);
    Preconditions.checkState(firstArgument.getType() == ExpressionContext.Type.IDENTIFIER,
        "Expected the first argument to be IDENTIFIER, got: %s", firstArgument.getType());
    int colId = fromIdentifierToColId(firstArgument.getIdentifier());
    int numRows = block.getNumRows();
    if (block.isRowHeap()) {
      Object[] values = new Object[numRows];
      List<Object[]> rows = block.asRowHeap().getRows();
      for (int rowId = 0; rowId < numRows; rowId++) {
        values[rowId] = rows.get(rowId)[colId];
      }
      return values;
    } else {
      return DataBlockExtractUtils.extractAggResult(block.asSerialized().getDataBlock(), colId, aggFunction);
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
    NUM_GROUPS_LIMIT_REACHED(StatMap.Type.BOOLEAN);
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

  @VisibleForTesting
  int getGroupTrimSize() {
    return _groupTrimSize;
  }
}
