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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.operator.utils.TypeUtils;
import org.roaringbitmap.RoaringBitmap;


/**
 * Class that executes all aggregation functions (without group-bys) for the multistage AggregateOperator.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class MultistageAggregationExecutor {
  private final AggregationFunction[] _aggFunctions;
  private final int[] _filterArgIds;
  private final int _maxFilterArgId;
  private final AggType _aggType;
  private final DataSchema _resultSchema;

  // Result holders for each mode.
  private final AggregationResultHolder[] _aggregateResultHolder;
  private final Object[] _mergeResultHolder;

  public MultistageAggregationExecutor(AggregationFunction[] aggFunctions, int[] filterArgIds, int maxFilterArgId,
      AggType aggType, DataSchema resultSchema) {
    _aggFunctions = aggFunctions;
    _filterArgIds = filterArgIds;
    _maxFilterArgId = maxFilterArgId;
    _aggType = aggType;
    _resultSchema = resultSchema;

    int numFunctions = aggFunctions.length;
    if (!aggType.isInputIntermediateFormat()) {
      _aggregateResultHolder = new AggregationResultHolder[numFunctions];
      for (int i = 0; i < numFunctions; i++) {
        _aggregateResultHolder[i] = aggFunctions[i].createAggregationResultHolder();
      }
      _mergeResultHolder = null;
    } else {
      _mergeResultHolder = new Object[numFunctions];
      _aggregateResultHolder = null;
    }
  }

  /**
   * Performs aggregation for the data in the block.
   */
  public void processBlock(TransferableBlock block) {
    if (!_aggType.isInputIntermediateFormat()) {
      processAggregate(block);
    } else {
      processMerge(block);
    }
  }

  /**
   * Fetches the result.
   */
  public List<Object[]> getResult() {
    Object[] row = new Object[_aggFunctions.length];
    for (int i = 0; i < _aggFunctions.length; i++) {
      AggregationFunction aggFunction = _aggFunctions[i];
      Object value;
      switch (_aggType) {
        case LEAF:
          value = aggFunction.extractAggregationResult(_aggregateResultHolder[i]);
          break;
        case INTERMEDIATE:
          value = _mergeResultHolder[i];
          break;
        case FINAL:
          value = aggFunction.extractFinalResult(_mergeResultHolder[i]);
          break;
        case DIRECT:
          Object intermediate = aggFunction.extractAggregationResult(_aggregateResultHolder[i]);
          value = aggFunction.extractFinalResult(intermediate);
          break;
        default:
          throw new UnsupportedOperationException("Unsupported aggTyp: " + _aggType);
      }
      row[i] = value;
    }
    // Convert the results from AggregationFunction to the desired type
    TypeUtils.convertRow(row, _resultSchema.getStoredColumnDataTypes());
    return Collections.singletonList(row);
  }

  private void processAggregate(TransferableBlock block) {
    if (_maxFilterArgId < 0) {
      // No filter for any aggregation function
      for (int i = 0; i < _aggFunctions.length; i++) {
        AggregationFunction aggFunction = _aggFunctions[i];
        Map<ExpressionContext, BlockValSet> blockValSetMap = AggregateOperator.getBlockValSetMap(aggFunction, block);
        aggFunction.aggregate(block.getNumRows(), _aggregateResultHolder[i], blockValSetMap);
      }
    } else {
      // Some aggregation functions have filter, cache the matching rows
      RoaringBitmap[] matchedBitmaps = new RoaringBitmap[_maxFilterArgId + 1];
      int[] numMatchedRowsArray = new int[_maxFilterArgId + 1];
      for (int i = 0; i < _aggFunctions.length; i++) {
        AggregationFunction aggFunction = _aggFunctions[i];
        int filterArgId = _filterArgIds[i];
        if (filterArgId < 0) {
          // No filter for this aggregation function
          Map<ExpressionContext, BlockValSet> blockValSetMap = AggregateOperator.getBlockValSetMap(aggFunction, block);
          aggFunction.aggregate(block.getNumRows(), _aggregateResultHolder[i], blockValSetMap);
        } else {
          // Need to filter the block before aggregation
          RoaringBitmap matchedBitmap = matchedBitmaps[filterArgId];
          if (matchedBitmap == null) {
            matchedBitmap = AggregateOperator.getMatchedBitmap(block, filterArgId);
            matchedBitmaps[filterArgId] = matchedBitmap;
            numMatchedRowsArray[filterArgId] = matchedBitmap.getCardinality();
          }
          int numMatchedRows = numMatchedRowsArray[filterArgId];
          Map<ExpressionContext, BlockValSet> blockValSetMap =
              AggregateOperator.getFilteredBlockValSetMap(aggFunction, block, numMatchedRows, matchedBitmap);
          aggFunction.aggregate(numMatchedRows, _aggregateResultHolder[i], blockValSetMap);
        }
      }
    }
  }

  private void processMerge(TransferableBlock block) {
    for (int i = 0; i < _aggFunctions.length; i++) {
      AggregationFunction aggFunction = _aggFunctions[i];
      Object[] intermediateResults = AggregateOperator.getIntermediateResults(aggFunction, block);
      for (Object intermediateResult : intermediateResults) {
        // Not all V1 aggregation functions have null-handling logic. Handle null values before calling merge.
        // TODO: Fix it
        if (intermediateResult == null) {
          continue;
        }
        if (_mergeResultHolder[i] == null) {
          _mergeResultHolder[i] = intermediateResult;
        } else {
          _mergeResultHolder[i] = aggFunction.merge(_mergeResultHolder[i], intermediateResult);
        }
      }
    }
  }
}
