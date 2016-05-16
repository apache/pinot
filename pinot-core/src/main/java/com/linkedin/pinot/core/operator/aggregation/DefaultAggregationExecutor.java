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

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.utils.primitive.MutableLongValue;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.aggregation.function.AggregationFunction;
import com.linkedin.pinot.core.operator.aggregation.function.AggregationFunctionFactory;
import com.linkedin.pinot.core.operator.aggregation.function.DistinctCountHLLAggregationFunction;
import com.linkedin.pinot.core.operator.aggregation.function.PercentileestAggregationFunction;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;
import com.linkedin.pinot.core.query.aggregation.function.AvgAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.MinMaxRangeAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.quantile.digest.QuantileDigest;
import com.linkedin.pinot.core.query.utils.Pair;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
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
  private static final String COLUMN_STAR = "*";

  private final IndexSegment _indexSegment;
  private final List<AggregationInfo> _aggregationInfoList;
  private List<AggregationFunctionContext> _aggrFuncContextList;

  // Reusable set to record loaded columns.
  private Set<String> _columnsLoaded;

  // Map from column name to int[] that stores dictionary id's for the column values.
  private Map<String, int[]> _columnToDictArrayMap;

  // Map from column name to double[] that stores actual column values.
  private Map<String, double[]> _columnToValueArrayMap;

  // Array of hash codes of the actual column values used for distinct count.
  private double[] _hashCodeArray;

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
    if (_inited) {
      return;
    }

    _aggrFuncContextList = new ArrayList<>(_aggregationInfoList.size());
    _columnsLoaded = new HashSet<>();
    _columnToDictArrayMap = new HashMap<>();
    _columnToValueArrayMap = new HashMap<>();

    for (AggregationInfo aggregationInfo : _aggregationInfoList) {
      final String[] columns = aggregationInfo.getAggregationParams().get("column").trim().split(",");

      for (String column : columns) {
        if (!column.equals(COLUMN_STAR) && !_columnToDictArrayMap.containsKey(column)) {
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

    fetchColumnDictIds(docIdSet, startIndex, length);
    fetchColumnValues(startIndex, length);

    for (int i = 0; i < _aggrFuncContextList.size(); i++) {
      aggregateColumn(_aggrFuncContextList.get(i), _resultHolderArray[i], length);
    }
  }

  /**
   * Helper method to perform aggregation for a given column.
   *
   * @param aggrFuncContext
   * @param resultHolder
   * @param length
   */
  private void aggregateColumn(AggregationFunctionContext aggrFuncContext, AggregationResultHolder resultHolder,
      int length) {
    AggregationFunction function = aggrFuncContext.getAggregationFunction();
    String aggrFuncName = aggrFuncContext.getFunctionName();
    String[] aggrColumns = aggrFuncContext.getAggregationColumns();

    Preconditions.checkState(aggrColumns.length == 1);
    String aggrColumn = aggrColumns[0];

    switch (aggrFuncName) {
      case AggregationFunctionFactory.COUNT_AGGREGATION_FUNCTION:
        function.aggregate(length, resultHolder);
        return;

      case AggregationFunctionFactory.DISTINCTCOUNT_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.DISTINCTCOUNTHLL_AGGREGATION_FUNCTION:
        fetchColumnValueHashCodes(aggrColumn, aggrFuncContext.getDictionary(0), length);
        function.aggregate(length, resultHolder, _hashCodeArray);
        return;

      default:
        function.aggregate(length, resultHolder, _columnToValueArrayMap.get(aggrColumn));
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

    switch (resultDataType) {
      case LONG:
        return new MutableLongValue((long) resultHolder.getDoubleResult());

      case DOUBLE:
        return resultHolder.getDoubleResult();

      case AVERAGE_PAIR:
        Pair<Double, Long> doubleLongPair = (Pair<Double, Long>) resultHolder.getResult();
        if (doubleLongPair == null) {
          return new AvgAggregationFunction.AvgPair(0.0, 0L);
        } else {
          return new AvgAggregationFunction.AvgPair(doubleLongPair.getFirst(), doubleLongPair.getSecond());
        }

      case MINMAXRANGE_PAIR:
        Pair<Double, Double> doubleDoublePair = (Pair<Double, Double>) resultHolder.getResult();
        if (doubleDoublePair == null) {
          return new MinMaxRangeAggregationFunction.MinMaxRangePair(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
        } else {
          return new MinMaxRangeAggregationFunction.MinMaxRangePair(doubleDoublePair.getFirst(),
              doubleDoublePair.getSecond());
        }

      case DISTINCTCOUNT_SET:
        IntOpenHashSet intOpenHashSet = (IntOpenHashSet) resultHolder.getResult();
        if (intOpenHashSet == null) {
          return new IntOpenHashSet();
        } else {
          return intOpenHashSet;
        }

      case DISTINCTCOUNTHLL_HYPERLOGLOG:
        HyperLogLog hyperLogLog = (HyperLogLog) resultHolder.getResult();
        if (hyperLogLog == null) {
          return new HyperLogLog(DistinctCountHLLAggregationFunction.DEFAULT_BIT_SIZE);
        } else {
          return hyperLogLog;
        }

      case PERCENTILE_LIST:
        DoubleArrayList doubleArrayList = (DoubleArrayList) resultHolder.getResult();
        if (doubleArrayList == null) {
          return new DoubleArrayList();
        } else {
          return doubleArrayList;
        }

      case PERCENTILEEST_QUANTILEDIGEST:
        QuantileDigest quantileDigest = (QuantileDigest) resultHolder.getResult();
        if (quantileDigest == null) {
          return new QuantileDigest(PercentileestAggregationFunction.DEFAULT_MAX_ERROR);
        } else {
          return quantileDigest;
        }

      default:
        throw new RuntimeException(
            "Unsupported result data type " + resultDataType + " in class " + getClass().getName());
    }
  }

  /**
   * Fetch dictId's for the given docIdSet for all aggregation columns except count.
   *
   * @param docIdSet
   * @param startIndex
   * @param length
   */
  private void fetchColumnDictIds(int[] docIdSet, int startIndex, int length) {
    _columnsLoaded.clear();
    for (AggregationFunctionContext aggrFuncContext : _aggrFuncContextList) {
      String aggrFuncName = aggrFuncContext.getFunctionName();
      if (!aggrFuncName.equals(AggregationFunctionFactory.COUNT_AGGREGATION_FUNCTION)) {
        String[] aggrColumns = aggrFuncContext.getAggregationColumns();
        for (int i = 0; i < aggrColumns.length; i++) {
          String aggrColumn = aggrColumns[i];

          if (!_columnsLoaded.contains(aggrColumn)) {
            int[] dictIdArray = _columnToDictArrayMap.get(aggrColumn);
            BlockValSet blockValSet = aggrFuncContext.getBlockValSet(i);
            blockValSet.readIntValues(docIdSet, startIndex, length, dictIdArray, startIndex);
            _columnsLoaded.add(aggrColumn);
          }
        }
      }
    }
  }

  /**
   * Fetch values for all aggregation columns except count and distinctcount according to the dictId's that fetched in
   * method fetchColumnDictIds.
   *
   * @param startIndex
   * @param length
   */
  private void fetchColumnValues(int startIndex, int length) {
    _columnsLoaded.clear();
    for (AggregationFunctionContext aggrFuncContext : _aggrFuncContextList) {
      String aggrFuncName = aggrFuncContext.getFunctionName();
      if (!aggrFuncName.equals(AggregationFunctionFactory.COUNT_AGGREGATION_FUNCTION)
          && !aggrFuncName.equals(AggregationFunctionFactory.DISTINCTCOUNT_AGGREGATION_FUNCTION)
          && !aggrFuncName.equals(AggregationFunctionFactory.DISTINCTCOUNTHLL_AGGREGATION_FUNCTION)) {
        String[] aggrColumns = aggrFuncContext.getAggregationColumns();
        for (int i = 0; i < aggrColumns.length; i++) {
          String aggrColumn = aggrColumns[i];

          if (!_columnsLoaded.contains(aggrColumn)) {
            Dictionary dictionary = aggrFuncContext.getDictionary(i);
            double[] valueArray = _columnToValueArrayMap.get(aggrColumn);
            dictionary.readDoubleValues(_columnToDictArrayMap.get(aggrColumn), startIndex, length, valueArray,
                startIndex);
            _columnsLoaded.add(aggrColumn);
          }
        }
      }
    }
  }

  /**
   * Fetch value hashcodes for the given aggregation column and dictionary. This method is called on distinctcount
   * aggregation function.
   *
   * @param aggrColumn
   * @param dictionary
   * @param length
   */
  private void fetchColumnValueHashCodes(String aggrColumn, Dictionary dictionary, int length) {
    int[] dictIdArray = _columnToDictArrayMap.get(aggrColumn);
    if (_hashCodeArray == null) {
      _hashCodeArray = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }

    for (int i = 0; i < length; i++) {
      int dictId = dictIdArray[i];
      if (dictId == Dictionary.NULL_VALUE_INDEX) {
        _hashCodeArray[i] = Integer.MIN_VALUE;
      } else {
        _hashCodeArray[i] = dictionary.get(dictId).hashCode();
      }
    }
  }
}
