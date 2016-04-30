/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.operator.aggregation;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.utils.primitive.MutableLongValue;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.aggregation.function.AggregationFunction;
import com.linkedin.pinot.core.operator.aggregation.function.AggregationFunctionFactory;
import com.linkedin.pinot.core.operator.groupby.AggregationFunctionContext;
import com.linkedin.pinot.core.operator.groupby.ResultHolderFactory;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;
import com.linkedin.pinot.core.query.aggregation.function.AvgAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.MinMaxRangeAggregationFunction;
import com.linkedin.pinot.core.query.utils.Pair;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Implementation of AggregationExecutor interface, to perform
 * aggregations.
 */
public class DefaultAggregationExecutor implements AggregationExecutor {

  private final IndexSegment _indexSegment;
  private final List<AggregationInfo> _aggregationInfoList;
  private List<AggregationFunctionContext> _aggrFuncContextList;

  // Map from column name to int[] that stores dictionary id's for the column values.
  private Map<String, int[]> _columnToDictArrayMap;

  // Map from column name to double[] that stores actual column values.
  private Map<String, double[]> _columnToValueArrayMap;

  // Array of result holders, one for each aggregation.
  private AggregationResultHolder[] _resultHolderArray;

  boolean _inited = false;
  boolean _finished = false;

  public DefaultAggregationExecutor(IndexSegment indexSegment, List<AggregationInfo> aggregationInfoList) {
    _indexSegment = indexSegment;
    _aggregationInfoList = aggregationInfoList;
  }

  /**
   * {@inheritDoc}
   * Must be called before the first call to 'process'.
   */
  @Override
  public void init() {
    _aggrFuncContextList = new ArrayList<AggregationFunctionContext>(_aggregationInfoList.size());
    _columnToDictArrayMap = new HashMap<String, int[]>();
    _columnToValueArrayMap = new HashMap<String, double[]>();

    for (AggregationInfo aggregationInfo : _aggregationInfoList) {
      final String[] columns = aggregationInfo.getAggregationParams().get("column").trim().split(",");

      for (String column : columns) {
        if (!_columnToDictArrayMap.containsKey(column)) {
          _columnToDictArrayMap.put(column, new int[DocIdSetPlanNode.MAX_DOC_PER_CALL]);
          _columnToValueArrayMap.put(column, new double[DocIdSetPlanNode.MAX_DOC_PER_CALL]);
        }
      }

      AggregationFunctionContext aggregationFunctionContext =
          new AggregationFunctionContext(_indexSegment, aggregationInfo.getAggregationType(), columns);
      _aggrFuncContextList.add(aggregationFunctionContext);
    }

    _resultHolderArray = new AggregationResultHolder[_aggrFuncContextList.size()];
    for (int i = 0; i < _aggrFuncContextList.size(); i++) {
      AggregationFunctionContext aggregationFunctionContext = _aggrFuncContextList.get(i);
      AggregationFunction aggregationFunction = aggregationFunctionContext.getAggregationFunction();
      _resultHolderArray[i] = ResultHolderFactory.getAggregationResultHolder(aggregationFunction);
    }
    _inited = true;
  }

  /**
   * {@inheritDoc}
   * Perform aggregation on a given docIdSet.
   * Asserts that 'init' has be called before calling this method.
   *
   * @param docIdSet
   * @param startIndex
   * @param length
   */
  @Override
  public void aggregate(int[] docIdSet, int startIndex, int length) {
    Preconditions
        .checkState(_inited, "Method 'aggregate' cannot be called before 'init' for class " + getClass().getName());

    fetchColumnValues(docIdSet, startIndex, length);
    for (int i = 0; i < _aggrFuncContextList.size(); i++) {
      AggregationFunctionContext aggregationFunctionContext = _aggrFuncContextList.get(i);

      for (String column : aggregationFunctionContext.getAggregationColumns()) {
        double[] valuesToAggregate = _columnToValueArrayMap.get(column);

        AggregationFunction function = aggregationFunctionContext.getAggregationFunction();
        function.aggregate(length, _resultHolderArray[i], valuesToAggregate);
      }
    }
  }

  /**
   * {@inheritDoc}
   * Must be called after all calls to 'process' are done, and before getResult()
   * can be called.
   */
  @Override
  public void finish() {
    _finished = true;
  }

  /**
   * {@inheritDoc}
   * Asserts that 'finish' has been called before calling getResult().
   * @return
   */
  @Override
  public List<Serializable> getResult() {
    Preconditions
        .checkState(_finished, "Method 'getResult' cannot be called before 'finish' for class " + getClass().getName());

    List<Serializable> aggregationResults = new ArrayList<Serializable>(_aggregationInfoList.size());

    for (int i = 0; i < _aggrFuncContextList.size(); i++) {
      AggregationFunctionContext aggregationFunctionContext = _aggrFuncContextList.get(i);
      Serializable result = getAggregationResult(_resultHolderArray[i],
          aggregationFunctionContext.getAggregationFunction().getResultDataType());
      aggregationResults.add(result);
    }
    return aggregationResults;
  }

  /**
   * Helper method to get the aggregation result.
   *
   * @param resultHolder
   * @param resultDataType
   * @return
   */
  private Serializable getAggregationResult(AggregationResultHolder resultHolder,
      AggregationFunction.ResultDataType resultDataType) {

    Serializable result;
    switch (resultDataType) {
      case LONG:
        result = new MutableLongValue((long) resultHolder.getDoubleResult());
        break;

      case DOUBLE:
        result = resultHolder.getDoubleResult();
        break;

      case AVERAGE_PAIR:
        Pair<Double, Long> doubleLongPair = (Pair<Double, Long>) resultHolder.getResult();
        result = new AvgAggregationFunction.AvgPair(doubleLongPair.getFirst(), doubleLongPair.getSecond());
        break;

      case MINMAXRANGE_PAIR:
        Pair<Double, Double> doubleDoublePair = (Pair<Double, Double>) resultHolder.getResult();
        result = new MinMaxRangeAggregationFunction.MinMaxRangePair(doubleDoublePair.getFirst(),
            doubleDoublePair.getSecond());
        break;

      default:
        throw new RuntimeException(
            "Unsupported result data type " + resultDataType + " in class " + getClass().getName());
    }
    return result;
  }

  /**
   * Fetch column dictIds and values for the given docIdSet for all aggregation columns.
   *
   * @param docIdSet
   * @param startIndex
   * @param length
   */
  private void fetchColumnValues(int[] docIdSet, int startIndex, int length) {
    Set<String> columnsLoaded = new HashSet();

    for (AggregationFunctionContext aggrFuncContext : _aggrFuncContextList) {
      if (!aggrFuncContext.getFunctionName().equalsIgnoreCase(AggregationFunctionFactory.COUNT_AGGREGATION_FUNCTION)) {
        String[] aggrColumns = aggrFuncContext.getAggregationColumns();

        for (int i = 0; i < aggrColumns.length; i++) {
          String aggrColumn = aggrColumns[i];

          if (!columnsLoaded.contains(aggrColumn)) {
            int[] dictIdArray = _columnToDictArrayMap.get(aggrColumn);
            BlockValSet blockValSet = aggrFuncContext.getBlockValSet(i);

            blockValSet.readIntValues(docIdSet, startIndex, length, dictIdArray, startIndex);
            columnsLoaded.add(aggrColumn);

            Dictionary dictionary = aggrFuncContext.getDictionary(i);
            double[] valueArray = _columnToValueArrayMap.get(aggrColumn);
            dictionary.readDoubleValues(dictIdArray, startIndex, length, valueArray, startIndex);
          }
        }
      }
    }
  }
}
