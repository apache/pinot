/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.query.aggregation;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.operator.blocks.TransformBlock;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionFactory;
import java.util.ArrayList;
import java.util.List;


/**
 * Implementation of AggregationExecutor interface, to perform
 * aggregations.
 */
public class DefaultAggregationExecutor implements AggregationExecutor {
  private final int _numAggrFunc;
  private final AggregationFunctionContext[] _aggrFuncContextArray;

  // Array of result holders, one for each aggregation.
  private final AggregationResultHolder[] _resultHolderArray;

  boolean _inited = false;
  boolean _finished = false;

  public DefaultAggregationExecutor(AggregationFunctionContext[] aggrFuncContextArray) {
    Preconditions.checkNotNull(aggrFuncContextArray);
    Preconditions.checkArgument(aggrFuncContextArray.length > 0);

    _numAggrFunc = aggrFuncContextArray.length;
    _aggrFuncContextArray = aggrFuncContextArray;
    _resultHolderArray = new AggregationResultHolder[_numAggrFunc];
  }

  @Override
  public void init() {
    if (_inited) {
      return;
    }

    for (int i = 0; i < _numAggrFunc; i++) {
      _resultHolderArray[i] = _aggrFuncContextArray[i].getAggregationFunction().createAggregationResultHolder();
    }
    _inited = true;
  }

  /**
   * {@inheritDoc}
   * Perform aggregation on a given docIdSet.
   * Asserts that 'init' has be called before calling this method.
   *
   * @param transformBlock Block upon which to perform aggregation.
   */
  @Override
  public void aggregate(TransformBlock transformBlock) {
    Preconditions.checkState(_inited,
        "Method 'aggregate' cannot be called before 'init' for class " + getClass().getName());

    for (int i = 0; i < _numAggrFunc; i++) {
      aggregateColumn(transformBlock, _aggrFuncContextArray[i], _resultHolderArray[i]);
    }
  }

  /**
   * Helper method to perform aggregation for a given column.
   *
   * @param aggrFuncContext aggregation function context.
   * @param resultHolder result holder.
   */
  @SuppressWarnings("ConstantConditions")
  private void aggregateColumn(TransformBlock transformBlock, AggregationFunctionContext aggrFuncContext,
      AggregationResultHolder resultHolder) {
    AggregationFunction aggregationFunction = aggrFuncContext.getAggregationFunction();
    String[] aggregationColumns = aggrFuncContext.getAggregationColumns();
    Preconditions.checkState(aggregationColumns.length == 1);
    int length = transformBlock.getNumDocs();

    if (!aggregationFunction.getName().equals(AggregationFunctionFactory.AggregationFunctionType.COUNT.getName())) {
      BlockValSet blockValSet = transformBlock.getBlockValueSet(aggregationColumns[0]);
      aggregationFunction.aggregate(length, resultHolder, blockValSet);
    } else {
      aggregationFunction.aggregate(length, resultHolder);
    }
  }

  @Override
  public void finish() {
    Preconditions.checkState(_inited,
        "Method 'finish' cannot be called before 'init' for class " + getClass().getName());

    _finished = true;
  }

  @Override
  public List<Object> getResult() {
    Preconditions.checkState(_finished,
        "Method 'getResult' cannot be called before 'finish' for class " + getClass().getName());

    List<Object> aggregationResults = new ArrayList<>(_numAggrFunc);

    for (int i = 0; i < _numAggrFunc; i++) {
      AggregationFunction aggregationFunction = _aggrFuncContextArray[i].getAggregationFunction();
      aggregationResults.add(aggregationFunction.extractAggregationResult(_resultHolderArray[i]));
    }

    return aggregationResults;
  }
}
