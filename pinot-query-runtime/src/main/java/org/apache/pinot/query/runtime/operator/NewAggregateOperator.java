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
import javax.annotation.Nullable;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.IntermediateStageBlockValSet;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.AggFunctionQueryContext;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionFactory;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;


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
 * 6.AVG
 * 7. FourthMoment
 * 8. BoolAnd and BoolOr
 *
 * When the list of aggregation calls is empty, this class is used to calculate distinct result based on group by keys.
 * In this case, the input can be any type.
 *
 * If the list of aggregation calls is not empty, the input of aggregation has to be a number.
 *
 * Note: This class performs aggregation over the double value of input.
 * If the input is single value, the output type will be input type. Otherwise, the output type will be double.
 */
// TODO: Rename to AggregateOperator when merging Planner support.
public class NewAggregateOperator extends MultiStageOperator {
  private static final String EXPLAIN_NAME = "AGGREGATE_OPERATOR";

  private final MultiStageOperator _inputOperator;
  private final DataSchema _resultSchema;

  // Aggregation containers
  private final AggregationFunction[] _aggregationFunctions;
  private final AggregationResultHolder[] _aggregationResultHolders;

  // Group By containers
  private final List<ExpressionContext> _groupSet;
  private final GroupByResultHolder[] _groupByResultHolders;
  // Mapping from the group by row-key to the values in the row.
  private final Map<Key, Object[]> _groupByKeyHolder;
  // groupId and groupIdMap are used to create a 0-based index for group-by keys instead of using the hash value
  // directly - similar to GroupByKeyGenerator. This is useful when we invoke the aggregation functions because they
  // use the group by key indexes to store results.
  private int _groupId = 0;
  private Map<Integer, Integer> _groupIdMap;

  private TransferableBlock _upstreamErrorBlock;
  private boolean _readyToConstruct;
  private boolean _hasReturnedAggregateBlock;

  // Denotes whether this aggregation operator should merge intermediate results.
  private boolean _isMergeAggregation;

  // TODO: refactor Pinot Reducer code to support the intermediate stage agg operator.
  // aggCalls has to be a list of FunctionCall and cannot be null
  // groupSet has to be a list of InputRef and cannot be null
  // TODO: Add these two checks when we confirm we can handle error in upstream ctor call.

  @VisibleForTesting
  public NewAggregateOperator(OpChainExecutionContext context, MultiStageOperator inputOperator,
      DataSchema resultSchema, List<FunctionContext> functionContexts, List<ExpressionContext> groupSet,
      boolean isMergeAggregation, boolean isSingleStageAggregation) {
    super(context);
    _inputOperator = inputOperator;
    _resultSchema = resultSchema;

    _groupSet = groupSet;
    _groupByKeyHolder = new HashMap<>();
    _groupIdMap = new HashMap<>();
    _aggregationFunctions = new AggregationFunction[functionContexts.size()];
    _aggregationResultHolders = new AggregationResultHolder[functionContexts.size()];
    _groupByResultHolders = new GroupByResultHolder[functionContexts.size()];
    for (int i = 0; i < _aggregationFunctions.length; i++) {
      _aggregationFunctions[i] = AggregationFunctionFactory.getAggregationFunction(functionContexts.get(i),
          new AggFunctionQueryContext(true));
      _aggregationResultHolders[i] = _aggregationFunctions[i].createAggregationResultHolder();
      _groupByResultHolders[i] = _aggregationFunctions[i].createGroupByResultHolder(
          InstancePlanMakerImplV2.DEFAULT_MAX_INITIAL_RESULT_HOLDER_CAPACITY,
          InstancePlanMakerImplV2.DEFAULT_NUM_GROUPS_LIMIT);
    }

    _upstreamErrorBlock = null;
    _readyToConstruct = false;
    _hasReturnedAggregateBlock = false;
    _isMergeAggregation = isMergeAggregation && !isSingleStageAggregation;
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
    List<Object[]> rows = _groupSet.isEmpty() ? collectAggregationResultRows() : collectGroupByResultRows();

    _hasReturnedAggregateBlock = true;
    if (rows.size() == 0) {
      if (_groupSet.size() == 0) {
        return constructEmptyAggResultBlock();
      } else {
        return TransferableBlockUtils.getEndOfStreamTransferableBlock();
      }
    } else {
      return new TransferableBlock(rows, _resultSchema, DataBlock.Type.ROW);
    }
  }

  private List<Object[]> collectAggregationResultRows() {
    List<Object[]> rows = new ArrayList<>();

    Object[] row = new Object[_aggregationFunctions.length];
    for (int i = 0; i < _aggregationFunctions.length; i++) {
      AggregationFunction aggregationFunction = _aggregationFunctions[i];
      row[i] = aggregationFunction.extractAggregationResult(_aggregationResultHolders[i]);
    }
    rows.add(row);
    return rows;
  }

  private List<Object[]> collectGroupByResultRows() {
    List<Object[]> rows = new ArrayList<>();
    for (Map.Entry<Key, Object[]> e : _groupByKeyHolder.entrySet()) {
      Object[] row = new Object[_aggregationFunctions.length + _groupSet.size()];
      Object[] keyElements = e.getValue();
      System.arraycopy(keyElements, 0, row, 0, keyElements.length);
      for (int i = 0; i < _aggregationFunctions.length; i++) {
        row[i + _groupSet.size()] = _aggregationFunctions[i].extractGroupByResult(_groupByResultHolders[i],
            _groupIdMap.get(e.getKey().hashCode()));
      }
      rows.add(row);
    }
    return rows;
  }

  /**
   * @return an empty agg result block for non-group-by aggregation.
   */
  private TransferableBlock constructEmptyAggResultBlock() {
    Object[] row = new Object[_aggregationFunctions.length];
    for (int i = 0; i < _aggregationFunctions.length; i++) {
      AggregationFunction aggFunction = _aggregationFunctions[i];
      row[i] = aggFunction.extractAggregationResult(aggFunction.createAggregationResultHolder());
    }
    return new TransferableBlock(Collections.singletonList(row), _resultSchema, DataBlock.Type.ROW);
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
      if (_isMergeAggregation) {
        mergeIntermediateValues(container);
      } else {
        aggregateValues(container);
      }

      block = _inputOperator.nextBlock();
    }
    return false;
  }

  private void mergeIntermediateValues(List<Object[]> container) {
    if (_groupSet.isEmpty()) {
      performMergeAggregation(container);
    } else {
      performMergeGroupBy(container);
    }
  }

  private void performMergeAggregation(List<Object[]> container) {
    // Simple aggregation function.
    for (int i = 0; i < _aggregationFunctions.length; i++) {
      List<ExpressionContext> expressions = _aggregationFunctions[i].getInputExpressions();
      for (Object[] row : container) {
        Object intermediateResultToMerge = extractIntermediateValue(row, expressions);
        _aggregationFunctions[i].mergeAndUpdateResultHolder(intermediateResultToMerge,
            _aggregationResultHolders[i]);
      }
    }
  }

  private void performMergeGroupBy(List<Object[]> container) {
    // Create group by keys for each row.
    int[] intKeys = generateGroupByKeys(container);

    for (int i = 0; i < _aggregationFunctions.length; i++) {
      GroupByResultHolder groupByResultHolder = _groupByResultHolders[i];
      groupByResultHolder.ensureCapacity(_groupIdMap.size());
      List<ExpressionContext> expressions = _aggregationFunctions[i].getInputExpressions();

      for (int j = 0; j < container.size(); j++) {
        Object[] row = container.get(j);
        Object intermediateResultToMerge = extractIntermediateValue(row, expressions);
        _aggregationFunctions[i].mergeAndUpdateResultHolder(intermediateResultToMerge, groupByResultHolder,
            intKeys[j]);
      }
    }
  }

  Object extractIntermediateValue(Object[] row, List<ExpressionContext> expressions) {
    // TODO: Add support to handle aggregation functions where:
    //       1. The identifier need not be the first argument
    //       2. There are more than one identifiers.
    Preconditions.checkState(expressions.size() <= 1);
    Preconditions.checkState(!expressions.isEmpty());
    ExpressionContext expr = expressions.get(0);

    Object result = expr.getType().equals(ExpressionContext.Type.IDENTIFIER) ? row[expr.getIdentifierIndex()]
        : expr.getLiteral().getValue();
    return result;
  }

  public void aggregateValues(List<Object[]> container) {
    // Convert row to columnar representation
    Map<Integer, List<Object>> columnValuesMap = new HashMap<>();
    for (Object[] row : container) {
      for (int i = 0; i < row.length; i++) {
        if (!columnValuesMap.containsKey(i)) {
          columnValuesMap.put(i, new ArrayList<>());
        }

        columnValuesMap.get(i).add(row[i]);
      }
    }

    if (_groupSet.isEmpty()) {
      performSimpleAggregation(container.size(), columnValuesMap);
    } else {
      int[] intKeys = generateGroupByKeys(container);
      performGroupByAggregation(container.size(), columnValuesMap, intKeys);
    }
  }

  private void performSimpleAggregation(int length, Map<Integer, List<Object>> columnValuesMap) {
    for (int i = 0; i < _aggregationFunctions.length; i++) {
      AggregationFunction aggregationFunction = _aggregationFunctions[i];
      aggregationFunction.aggregate(length, _aggregationResultHolders[i],
          getBlockValSetMap(aggregationFunction, columnValuesMap));
    }
  }

  private void performGroupByAggregation(int length, Map<Integer, List<Object>> columnValuesMap, int[] intKeys) {
    for (int i = 0; i < _aggregationFunctions.length; i++) {
      AggregationFunction aggregationFunction = _aggregationFunctions[i];
      Map<ExpressionContext, BlockValSet> blockValSetMap = getBlockValSetMap(aggregationFunction, columnValuesMap);
      GroupByResultHolder groupByResultHolder = _groupByResultHolders[i];
      groupByResultHolder.ensureCapacity(_groupIdMap.size());
      Preconditions.checkState(intKeys.length == length, "Length don't match.");

      aggregationFunction.aggregateGroupBySV(length, intKeys, groupByResultHolder, blockValSetMap);
    }
  }

  private int[] generateGroupByKeys(List<Object[]> rows) {
    int[] rowKeys = new int[rows.size()];

    for (int i = 0; i < rows.size(); i++) {
      Object[] row = rows.get(i);

      Object[] keyElements = new Object[_groupSet.size()];
      for (int j = 0; j < _groupSet.size(); j++) {
        keyElements[j] = row[_groupSet.get(j).getIdentifierIndex()];
      }

      Key rowKey = new Key(keyElements);
      _groupByKeyHolder.put(rowKey, rowKey.getValues());
      if (!_groupIdMap.containsKey(rowKey.hashCode())) {
        _groupIdMap.put(rowKey.hashCode(), _groupId);
        ++_groupId;
      }
      rowKeys[i] = _groupIdMap.get(rowKey.hashCode());
    }

    return rowKeys;
  }

  private Map<ExpressionContext, BlockValSet> getBlockValSetMap(AggregationFunction aggregationFunction,
      Map<Integer, List<Object>> values) {
    List<ExpressionContext> expressions = aggregationFunction.getInputExpressions();
    int numExpressions = expressions.size();
    if (numExpressions == 0) {
      return Collections.emptyMap();
    }

    Preconditions.checkState(numExpressions == 1, "Cannot handle more than one identifier in aggregation function.");
    ExpressionContext expression = expressions.get(0);
    Preconditions.checkState(expression.getType().equals(ExpressionContext.Type.IDENTIFIER));
    List<Object> val = values.get(expression.getIdentifierIndex());

    return Collections.singletonMap(expression,
        new IntermediateStageBlockValSet(expression.getIdentifierDataType(), val));
  }
}
