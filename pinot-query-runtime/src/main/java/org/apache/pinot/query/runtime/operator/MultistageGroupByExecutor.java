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
import org.apache.pinot.segment.spi.AggregationFunctionType;


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
  private int _groupId = 0;
  private Map<Key, Integer> _groupKeyToIdMap;

  // Mapping from the group by row-key to the values in the row which form the key. Used to fetch the actual row
  // values when populating the result.
  private final Map<Key, Object[]> _groupByKeyHolder;

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
    _groupByKeyHolder = new HashMap<>();

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
    List<Object[]> rows = new ArrayList<>();

    if (_aggType.equals(AggType.FINAL)) {
      return extractFinalGroupByResult();
    }

    // If the mode is MERGE or AGGREGATE, the groupby keys are already collected in _groupByKeyHolder by virtue of
    // generating the row keys.
    for (Map.Entry<Key, Object[]> e : _groupByKeyHolder.entrySet()) {
      int numCols = _groupSet.size() + _aggFunctions.length;
      Object[] row = new Object[numCols];
      Object[] keyElements = e.getValue();
      System.arraycopy(keyElements, 0, row, 0, keyElements.length);

      for (int i = 0; i < _aggFunctions.length; i++) {
        int index = i + _groupSet.size();
        int groupId = _groupKeyToIdMap.get(e.getKey());
        if (_aggType.equals(AggType.INTERMEDIATE)) {
          Object value = _mergeResultHolder.get(groupId)[i];
          row[index] = convertObjectToReturnType(_aggFunctions[i].getType(), value);
        } else {
          Object value = _aggFunctions[i].extractGroupByResult(_aggregateResultHolders[i], groupId);
          row[index] = convertObjectToReturnType(_aggFunctions[i].getType(), value);
        }
      }

      rows.add(row);
    }

    return rows;
  }

  private List<Object[]> extractFinalGroupByResult() {
    List<Object[]> rows = new ArrayList<>();
    for (Object[] resultRow : _finalResultHolder) {
      int numCols = _groupSet.size() + _aggFunctions.length;
      Object[] row = new Object[numCols];
      System.arraycopy(resultRow, 0, row, 0, _groupSet.size());

      for (int i = 0; i < _aggFunctions.length; i++) {
        int aggIdx = i + _groupSet.size();
        Comparable result = _aggFunctions[i].extractFinalResult(resultRow[aggIdx]);
        row[aggIdx] = result == null ? null : _aggFunctions[i].getFinalResultColumnType().convert(result);
      }

      rows.add(row);
    }
    return rows;
  }

  private Object convertObjectToReturnType(AggregationFunctionType aggFuncType, Object value) {
    // For bool_and and bool_or aggregation functions, the return type for aggregate and merge modes are set as
    // boolean. However, the v1 bool_and and bool_or function uses Integer as the intermediate type.
    boolean boolAndOrAgg =
        aggFuncType.equals(AggregationFunctionType.BOOLAND) || aggFuncType.equals(AggregationFunctionType.BOOLOR);
    if (boolAndOrAgg && value instanceof Integer) {
      Boolean boolVal = ((Number) value).intValue() > 0 ? true : false;
      return boolVal;
    }
    return value;
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
        Object mergedIntermediateResult = _mergeResultHolder.get(rowKey)[i];

        // Not all V1 aggregation functions have null-handling. So handle null values and call merge only if necessary.
        if (intermediateResultToMerge == null) {
          continue;
        }
        if (mergedIntermediateResult == null) {
          _mergeResultHolder.get(rowKey)[i] = intermediateResultToMerge;
          continue;
        }

        _mergeResultHolder.get(rowKey)[i] = _aggFunctions[i].merge(intermediateResultToMerge, mergedIntermediateResult);
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
    int[] rowKeys = new int[rows.size()];
    int numGroups = _groupSet.size();

    for (int i = 0; i < rows.size(); i++) {
      Object[] row = rows.get(i);

      Object[] keyElements = new Object[numGroups];
      for (int j = 0; j < numGroups; j++) {
        String colName = _groupSet.get(j).getIdentifier();
        int colIndex = _colNameToIndexMap.get(colName);
        keyElements[j] = row[colIndex];
      }

      Key rowKey = new Key(keyElements);
      _groupByKeyHolder.put(rowKey, rowKey.getValues());
      if (!_groupKeyToIdMap.containsKey(rowKey)) {
        _groupKeyToIdMap.put(rowKey, _groupId);
        ++_groupId;
      }
      rowKeys[i] = _groupKeyToIdMap.get(rowKey);
    }

    return rowKeys;
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

  Object extractValueFromRow(AggregationFunction aggregationFunction, Object[] row) {
    // TODO: Add support to handle aggregation functions where:
    //       1. The identifier need not be the first argument
    //       2. There are more than one identifiers.
    List<ExpressionContext> expressions = aggregationFunction.getInputExpressions();
    Preconditions.checkState(expressions.size() == 1);
    ExpressionContext expr = expressions.get(0);
    ExpressionContext.Type exprType = expr.getType();

    if (exprType.equals(ExpressionContext.Type.IDENTIFIER)) {
      String colName = expr.getIdentifier();
      int colIndex = _colNameToIndexMap.get(colName);
      Object value = row[colIndex];

      // Boolean aggregation functions like BOOL_AND and BOOL_OR have return types set to Boolean. However, their
      // intermediateResultType is Integer. To handle this case convert Boolean objects to Integer objects.
      boolean boolAndOrAgg =
          aggregationFunction.getType().equals(AggregationFunctionType.BOOLAND) || aggregationFunction.getType()
              .equals(AggregationFunctionType.BOOLOR);
      if (boolAndOrAgg && value instanceof Boolean) {
        Integer intVal = ((Boolean) value).booleanValue() ? 1 : 0;
        return intVal;
      }

      return value;
    }

    Preconditions.checkState(exprType.equals(ExpressionContext.Type.LITERAL), "Invalid expression type");
    return expr.getLiteral().getValue();
  }
}
