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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.IntermediateStageBlockValSet;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;


/**
 * Class that executes the group by aggregations for the multistage AggregateOperator.
 */
public class MultistageGroupByExecutor {
  private final AggType _aggType;
  // The identifier operands for the aggregation function only store the column name. This map contains mapping
  // between column name to their index which is used in v2 engine.
  private final Map<String, Integer> _colNameToIndexMap;

  private final List<ExpressionContext> _groupSet;
  private final AggregationFunction[] _aggFunctions;

  // Group By Result holders for each mode
  private final GroupByResultHolder[] _aggregateResultHolders;
  private final Map<Integer, Object[]> _mergeResultHolder;
  private final List<Object[]> _finalResultHolder;

  // Mapping from the row-key to a zero based integer index. This is used when we invoke the v1 aggregation functions
  // because they use the zero based integer indexes to store results.
  private final Map<Key, Integer> _groupKeyToIdMap;

  public MultistageGroupByExecutor(List<ExpressionContext> groupByExpr, AggregationFunction[] aggFunctions,
      AggType aggType, Map<String, Integer> colNameToIndexMap) {
    _aggType = aggType;
    _colNameToIndexMap = colNameToIndexMap;
    _groupSet = groupByExpr;
    _aggFunctions = aggFunctions;

    _aggregateResultHolders = new GroupByResultHolder[_aggFunctions.length];
    _mergeResultHolder = new HashMap<>();
    _finalResultHolder = new ArrayList<>();

    _groupKeyToIdMap = new HashMap<>();

    for (int i = 0; i < _aggFunctions.length; i++) {
      _aggregateResultHolders[i] =
          _aggFunctions[i].createGroupByResultHolder(InstancePlanMakerImplV2.DEFAULT_MAX_INITIAL_RESULT_HOLDER_CAPACITY,
              InstancePlanMakerImplV2.DEFAULT_NUM_GROUPS_LIMIT);
    }
  }

  /**
   * Performs group-by aggregation for the data in the block.
   */
  public void processBlock(TransferableBlock block, DataSchema inputDataSchema) {
    if (!_aggType.isInputIntermediateFormat()) {
      processAggregate(block, inputDataSchema);
    } else if (_aggType.isOutputIntermediateFormat()) {
      processMerge(block);
    } else {
      collectResult(block);
    }
  }

  /**
   * Fetches the result.
   */
  public List<Object[]> getResult() {
    if (_aggType == AggType.FINAL) {
      return extractFinalGroupByResult();
    }

    List<Object[]> rows = new ArrayList<>(_groupKeyToIdMap.size());
    int numKeys = _groupSet.size();
    int numFunctions = _aggFunctions.length;
    int numColumns = numKeys + numFunctions;
    for (Map.Entry<Key, Integer> entry : _groupKeyToIdMap.entrySet()) {
      Object[] row = new Object[numColumns];
      Object[] keyValues = entry.getKey().getValues();
      System.arraycopy(keyValues, 0, row, 0, numKeys);
      int groupId = entry.getValue();
      for (int i = 0; i < numFunctions; i++) {
        AggregationFunction func = _aggFunctions[i];
        int index = numKeys + i;
        if (!_aggType.isInputIntermediateFormat()) {
          Object intermediateResult = func.extractGroupByResult(_aggregateResultHolders[i], groupId);
          if (_aggType.isOutputIntermediateFormat()) {
            row[index] = intermediateResult;
          } else {
            Object finalResult = func.extractFinalResult(intermediateResult);
            row[index] = finalResult == null ? null : func.getFinalResultColumnType().convert(finalResult);
          }
        } else {
          assert _aggType == AggType.INTERMEDIATE;
          row[index] = _mergeResultHolder.get(groupId)[i];
        }
      }
      rows.add(row);
    }
    return rows;
  }

  private List<Object[]> extractFinalGroupByResult() {
    List<Object[]> rows = new ArrayList<>(_finalResultHolder.size());
    int numKeys = _groupSet.size();
    int numFunctions = _aggFunctions.length;
    int numColumns = numKeys + numFunctions;
    for (Object[] resultRow : _finalResultHolder) {
      Object[] row = new Object[numColumns];
      System.arraycopy(resultRow, 0, row, 0, numKeys);
      for (int i = 0; i < numFunctions; i++) {
        AggregationFunction func = _aggFunctions[i];
        int index = numKeys + i;
        Object finalResult = func.extractFinalResult(resultRow[index]);
        row[index] = finalResult == null ? null : func.getFinalResultColumnType().convert(finalResult);
      }
      rows.add(row);
    }
    return rows;
  }

  private void processAggregate(TransferableBlock block, DataSchema inputDataSchema) {
    int[] intKeys = generateGroupByKeys(block.getContainer());

    for (int i = 0; i < _aggFunctions.length; i++) {
      AggregationFunction aggregationFunction = _aggFunctions[i];
      Map<ExpressionContext, BlockValSet> blockValSetMap =
          getBlockValSetMap(aggregationFunction, block, inputDataSchema);
      GroupByResultHolder groupByResultHolder = _aggregateResultHolders[i];
      groupByResultHolder.ensureCapacity(_groupKeyToIdMap.size());
      aggregationFunction.aggregateGroupBySV(block.getNumRows(), intKeys, groupByResultHolder, blockValSetMap);
    }
  }

  private void processMerge(TransferableBlock block) {
    List<Object[]> container = block.getContainer();
    int[] intKeys = generateGroupByKeys(container);

    for (int i = 0; i < _aggFunctions.length; i++) {
      for (int j = 0; j < container.size(); j++) {
        Object[] row = container.get(j);
        int rowKey = intKeys[j];
        if (!_mergeResultHolder.containsKey(rowKey)) {
          _mergeResultHolder.put(rowKey, new Object[_aggFunctions.length]);
        }
        Object intermediateResultToMerge = extractValueFromRow(_aggFunctions[i], row);

        // Not all V1 aggregation functions have null-handling. So handle null values and call merge only if necessary.
        if (intermediateResultToMerge == null) {
          continue;
        }
        Object mergedIntermediateResult = _mergeResultHolder.get(rowKey)[i];
        if (mergedIntermediateResult == null) {
          _mergeResultHolder.get(rowKey)[i] = intermediateResultToMerge;
          continue;
        }

        _mergeResultHolder.get(rowKey)[i] = _aggFunctions[i].merge(mergedIntermediateResult, intermediateResultToMerge);
      }
    }
  }

  private void collectResult(TransferableBlock block) {
    List<Object[]> container = block.getContainer();
    for (Object[] row : container) {
      assert row.length == _groupSet.size() + _aggFunctions.length;
      Object[] resultRow = new Object[row.length];
      System.arraycopy(row, 0, resultRow, 0, _groupSet.size());

      for (int i = 0; i < _aggFunctions.length; i++) {
        int index = _groupSet.size() + i;
        resultRow[index] = extractValueFromRow(_aggFunctions[i], row);
      }

      _finalResultHolder.add(resultRow);
    }
  }

  /**
   * Creates the group by key for each row. Converts the key into a 0-index based int value that can be used by
   * GroupByAggregationResultHolders used in v1 aggregations.
   * <p>
   * Returns the int key for each row.
   */
  private int[] generateGroupByKeys(List<Object[]> rows) {
    int numRows = rows.size();
    int[] rowIntKeys = new int[numRows];
    int numKeys = _groupSet.size();
    for (int i = 0; i < numRows; i++) {
      Object[] row = rows.get(i);
      Object[] keyValues = new Object[numKeys];
      for (int j = 0; j < numKeys; j++) {
        keyValues[j] = row[_colNameToIndexMap.get(_groupSet.get(j).getIdentifier())];
      }
      Key rowKey = new Key(keyValues);
      rowIntKeys[i] = _groupKeyToIdMap.computeIfAbsent(rowKey, k -> _groupKeyToIdMap.size());
    }
    return rowIntKeys;
  }

  private Map<ExpressionContext, BlockValSet> getBlockValSetMap(AggregationFunction aggFunction,
      TransferableBlock block, DataSchema inputDataSchema) {
    List<ExpressionContext> expressions = aggFunction.getInputExpressions();
    int numExpressions = expressions.size();
    if (numExpressions == 0) {
      return Collections.emptyMap();
    }

    Preconditions.checkState(numExpressions == 1, "Cannot handle more than one identifier in aggregation function.");
    ExpressionContext expression = expressions.get(0);
    Preconditions.checkState(expression.getType().equals(ExpressionContext.Type.IDENTIFIER));
    int index = _colNameToIndexMap.get(expression.getIdentifier());

    DataSchema.ColumnDataType dataType = inputDataSchema.getColumnDataType(index);
    Preconditions.checkState(block.getType().equals(DataBlock.Type.ROW), "Datablock type is not ROW");
    // TODO: If the previous block is not mailbox received, this method is not efficient.  Then getDataBlock() will
    //  convert the unserialized format to serialized format of BaseDataBlock. Then it will convert it back to column
    //  value primitive type.
    return Collections.singletonMap(expression,
        new IntermediateStageBlockValSet(dataType, block.getDataBlock(), index));
  }

  private Object extractValueFromRow(AggregationFunction aggregationFunction, Object[] row) {
    // TODO: Add support to handle aggregation functions where:
    //       1. The identifier need not be the first argument
    //       2. There are more than one identifiers.
    List<ExpressionContext> expressions = aggregationFunction.getInputExpressions();
    Preconditions.checkState(expressions.size() == 1, "Only support single expression, got: %s", expressions.size());
    ExpressionContext expr = expressions.get(0);
    ExpressionContext.Type exprType = expr.getType();
    if (exprType == ExpressionContext.Type.IDENTIFIER) {
      return row[_colNameToIndexMap.get(expr.getIdentifier())];
    }
    Preconditions.checkState(exprType == ExpressionContext.Type.LITERAL, "Unsupported expression type: %s", exprType);
    return expr.getLiteral().getValue();
  }
}
