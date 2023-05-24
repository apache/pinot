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
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.IntermediateStageBlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/**
 * Class that executes all aggregation functions (without group-bys) for the multistage AggregateOperator.
 */
public class MultistageAggregationExecutor {
  private final AggregateOperator.Mode _mode;
  // The identifier operands for the aggregation function only store the column name. This map contains mapping
  // from column name to their index.
  private final Map<String, Integer> _colNameToIndexMap;

  private final AggregationFunction[] _aggFunctions;

  // Result holders for each mode.
  private final AggregationResultHolder[] _aggregateResultHolder;
  private final Object[] _mergeResultHolder;
  private final Object[] _finalResultHolder;

  public MultistageAggregationExecutor(AggregationFunction[] aggFunctions, AggregateOperator.Mode mode,
      Map<String, Integer> colNameToIndexMap) {
    _aggFunctions = aggFunctions;
    _mode = mode;
    _colNameToIndexMap = colNameToIndexMap;

    _aggregateResultHolder = new AggregationResultHolder[aggFunctions.length];
    _mergeResultHolder = new Object[aggFunctions.length];
    _finalResultHolder = new Object[aggFunctions.length];

    for (int i = 0; i < _aggFunctions.length; i++) {
      _aggregateResultHolder[i] = _aggFunctions[i].createAggregationResultHolder();
    }
  }

  /**
   * Performs aggregation for the data in the block.
   */
  public void processBlock(TransferableBlock block, DataSchema inputDataSchema) {
    if (_mode.equals(AggregateOperator.Mode.AGGREGATE)) {
      processAggregate(block, inputDataSchema);
    } else if (_mode.equals(AggregateOperator.Mode.MERGE)) {
      processMerge(block);
    } else if (_mode.equals(AggregateOperator.Mode.EXTRACT_RESULT)) {
      collectResult(block);
    }
  }

  /**
   * @return an empty agg result block for non-group-by aggregation.
   */
  public Object[] constructEmptyAggResultRow() {
    Object[] row = new Object[_aggFunctions.length];
    for (int i = 0; i < _aggFunctions.length; i++) {
      AggregationFunction aggFunction = _aggFunctions[i];
      row[i] = aggFunction.extractAggregationResult(aggFunction.createAggregationResultHolder());
    }
    return row;
  }

  /**
   * Fetches the result.
   */
  public List<Object[]> getResult() {
    List<Object[]> rows = new ArrayList<>();
    Object[] row = new Object[_aggFunctions.length];

    for (int i = 0; i < _aggFunctions.length; i++) {
      AggregationFunction aggFunction = _aggFunctions[i];
      if (_mode.equals(AggregateOperator.Mode.MERGE)) {
        Object value = _mergeResultHolder[i];
        row[i] = convertObjectToReturnType(_aggFunctions[i].getType(), value);
      } else if (_mode.equals(AggregateOperator.Mode.AGGREGATE)) {
        Object value = aggFunction.extractAggregationResult(_aggregateResultHolder[i]);
        row[i] = convertObjectToReturnType(_aggFunctions[i].getType(), value);
      } else {
        assert _mode.equals(AggregateOperator.Mode.EXTRACT_RESULT);
        Comparable result = aggFunction.extractFinalResult(_finalResultHolder[i]);
        row[i] = result == null ? null : aggFunction.getFinalResultColumnType().convert(result);
      }
    }
    rows.add(row);
    return rows;
  }

  private Object convertObjectToReturnType(AggregationFunctionType aggType, Object value) {
    // For bool_and and bool_or aggregation functions, the return type for aggregate and merge modes are set as
    // boolean. However, the v1 bool_and and bool_or function uses Integer as the intermediate type.
    boolean boolAndOrAgg =
        aggType.equals(AggregationFunctionType.BOOLAND) || aggType.equals(AggregationFunctionType.BOOLOR);
    if (boolAndOrAgg && value instanceof Integer) {
      Boolean boolVal = ((Number) value).intValue() > 0 ? true : false;
      return boolVal;
    }
    return value;
  }

  private void processAggregate(TransferableBlock block, DataSchema inputDataSchema) {
    for (int i = 0; i < _aggFunctions.length; i++) {
      AggregationFunction aggregationFunction = _aggFunctions[i];
      Map<ExpressionContext, BlockValSet> blockValSetMap =
          getBlockValSetMap(aggregationFunction, block, inputDataSchema);
      aggregationFunction.aggregate(block.getNumRows(), _aggregateResultHolder[i], blockValSetMap);
    }
  }

  private void processMerge(TransferableBlock block) {
    List<Object[]> container = block.getContainer();

    for (int i = 0; i < _aggFunctions.length; i++) {
      for (Object[] row : container) {
        Object intermediateResultToMerge = extractValueFromRow(_aggFunctions[i], row);
        Object mergedIntermediateResult = _mergeResultHolder[i];

        // Not all V1 aggregation functions have null-handling logic. Handle null values before calling merge.
        if (intermediateResultToMerge == null) {
          continue;
        }
        if (mergedIntermediateResult == null) {
          _mergeResultHolder[i] = intermediateResultToMerge;
          continue;
        }

        _mergeResultHolder[i] = _aggFunctions[i].merge(intermediateResultToMerge, mergedIntermediateResult);
      }
    }
  }

  private void collectResult(TransferableBlock block) {
    List<Object[]> container = block.getContainer();
    assert container.size() == 1;
    Object[] row = container.get(0);
    for (int i = 0; i < _aggFunctions.length; i++) {
      _finalResultHolder[i] = extractValueFromRow(_aggFunctions[i], row);
    }
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
