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
import com.linkedin.pinot.core.common.DataFetcher;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.aggregation.function.AggregationFunction;
import com.linkedin.pinot.core.operator.aggregation.function.AggregationFunctionFactory;
import com.linkedin.pinot.core.operator.aggregation.function.DistinctCountHLLAggregationFunction;
import com.linkedin.pinot.core.operator.aggregation.function.PercentileestAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.AvgAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.MinMaxRangeAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.quantile.digest.QuantileDigest;
import com.linkedin.pinot.core.query.utils.Pair;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


/**
 * Implementation of AggregationExecutor interface, to perform
 * aggregations.
 */
public class DefaultAggregationExecutor implements AggregationExecutor {
  private final SingleValueBlockCache _singleValueBlockCache;
  private final int _numAggrFunc;
  private final AggregationFunctionContext[] _aggrFuncContextArray;

  // Array of result holders, one for each aggregation.
  private final AggregationResultHolder[] _resultHolderArray;

  boolean _inited = false;
  boolean _finished = false;

  public DefaultAggregationExecutor(IndexSegment indexSegment, List<AggregationInfo> aggregationInfoList) {
    Preconditions.checkNotNull(indexSegment);
    Preconditions.checkNotNull(aggregationInfoList);
    Preconditions.checkArgument(aggregationInfoList.size() > 0);

    _singleValueBlockCache = new SingleValueBlockCache(new DataFetcher(indexSegment));
    _numAggrFunc = aggregationInfoList.size();
    _aggrFuncContextArray = new AggregationFunctionContext[_numAggrFunc];
    for (int i = 0; i < _numAggrFunc; i++) {
      AggregationInfo aggregationInfo = aggregationInfoList.get(i);
      String[] columns = aggregationInfo.getAggregationParams().get("column").trim().split(",");
      _aggrFuncContextArray[i] = new AggregationFunctionContext(aggregationInfo.getAggregationType(), columns);
    }
    _resultHolderArray = new AggregationResultHolder[_numAggrFunc];
  }

  /**
   * {@inheritDoc}
   * Must be called before the first call to 'aggregate'.
   */
  @Override
  public void init() {
    if (_inited) {
      return;
    }

    for (int i = 0; i < _numAggrFunc; i++) {
      AggregationFunction aggregationFunction = _aggrFuncContextArray[i].getAggregationFunction();
      _resultHolderArray[i] = ResultHolderFactory.getAggregationResultHolder(aggregationFunction);
    }
    _inited = true;
  }

  /**
   * {@inheritDoc}
   * Perform aggregation on a given docIdSet.
   * Asserts that 'init' has be called before calling this method.
   *
   * @param docIdSet block doc id set.
   * @param startIndex start index of the block.
   * @param length length of the block.
   */
  @Override
  public void aggregate(int[] docIdSet, int startIndex, int length) {
    Preconditions
        .checkState(_inited, "Method 'aggregate' cannot be called before 'init' for class " + getClass().getName());

    _singleValueBlockCache.initNewBlock(docIdSet, startIndex, length);

    for (int i = 0; i < _numAggrFunc; i++) {
      aggregateColumn(_aggrFuncContextArray[i], _resultHolderArray[i], length);
    }
  }

  /**
   * Helper method to perform aggregation for a given column.
   *
   * @param aggrFuncContext aggregation function context.
   * @param resultHolder result holder.
   * @param length length of the block.
   */
  private void aggregateColumn(AggregationFunctionContext aggrFuncContext, AggregationResultHolder resultHolder,
      int length) {
    AggregationFunction aggregationFunction = aggrFuncContext.getAggregationFunction();
    String[] aggrColumns = aggrFuncContext.getAggregationColumns();
    String aggrFuncName = aggregationFunction.getName();

    Preconditions.checkState(aggrColumns.length == 1);
    String aggrColumn = aggrColumns[0];

    switch (aggrFuncName) {
      case AggregationFunctionFactory.COUNT_AGGREGATION_FUNCTION:
        aggregationFunction.aggregate(length, resultHolder);
        break;

      case AggregationFunctionFactory.DISTINCTCOUNT_AGGREGATION_FUNCTION:
      case AggregationFunctionFactory.DISTINCTCOUNTHLL_AGGREGATION_FUNCTION:
        aggregationFunction.aggregate(length, resultHolder,
            _singleValueBlockCache.getHashCodeArrayForColumn(aggrColumn));
        break;

      default:
        aggregationFunction.aggregate(length, resultHolder,
            _singleValueBlockCache.getDoubleValueArrayForColumn(aggrColumn));
        break;
    }
  }

  /**
   * {@inheritDoc}
   * Must be called after all calls to 'process' are done, and before getResult() can be called.
   */
  @Override
  public void finish() {
    Preconditions
        .checkState(_inited, "Method 'finish' cannot be called before 'init' for class " + getClass().getName());

    _finished = true;
  }

  /**
   * {@inheritDoc}
   * Asserts that 'finish' has been called before calling getResult().
   *
   * @return list of aggregation results.
   */
  @Override
  public List<Serializable> getResult() {
    Preconditions
        .checkState(_finished, "Method 'getResult' cannot be called before 'finish' for class " + getClass().getName());

    List<Serializable> aggregationResults = new ArrayList<>(_numAggrFunc);

    for (int i = 0; i < _numAggrFunc; i++) {
      AggregationFunction aggregationFunction = _aggrFuncContextArray[i].getAggregationFunction();
      Serializable result = getAggregationResult(_resultHolderArray[i], aggregationFunction.getResultDataType());
      aggregationResults.add(result);
    }

    return aggregationResults;
  }

  /**
   * Helper method to get the aggregation result.
   *
   * @param resultHolder result holder.
   * @param resultDataType result data type.
   * @return aggregation result.
   */
  private Serializable getAggregationResult(AggregationResultHolder resultHolder,
      AggregationFunction.ResultDataType resultDataType) {

    switch (resultDataType) {
      case LONG:
        return new MutableLongValue((long) resultHolder.getDoubleResult());

      case DOUBLE:
        return resultHolder.getDoubleResult();

      case AVERAGE_PAIR:
        Pair<Double, Long> doubleLongPair = resultHolder.getResult();
        if (doubleLongPair == null) {
          return new AvgAggregationFunction.AvgPair(0.0, 0L);
        } else {
          return new AvgAggregationFunction.AvgPair(doubleLongPair.getFirst(), doubleLongPair.getSecond());
        }

      case MINMAXRANGE_PAIR:
        Pair<Double, Double> doubleDoublePair = resultHolder.getResult();
        if (doubleDoublePair == null) {
          return new MinMaxRangeAggregationFunction.MinMaxRangePair(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
        } else {
          return new MinMaxRangeAggregationFunction.MinMaxRangePair(doubleDoublePair.getFirst(),
              doubleDoublePair.getSecond());
        }

      case DISTINCTCOUNT_SET:
        IntOpenHashSet intOpenHashSet = resultHolder.getResult();
        if (intOpenHashSet == null) {
          return new IntOpenHashSet();
        } else {
          return intOpenHashSet;
        }

      case DISTINCTCOUNTHLL_HYPERLOGLOG:
        HyperLogLog hyperLogLog = resultHolder.getResult();
        if (hyperLogLog == null) {
          return new HyperLogLog(DistinctCountHLLAggregationFunction.DEFAULT_BIT_SIZE);
        } else {
          return hyperLogLog;
        }

      case PERCENTILE_LIST:
        DoubleArrayList doubleArrayList = resultHolder.getResult();
        if (doubleArrayList == null) {
          return new DoubleArrayList();
        } else {
          return doubleArrayList;
        }

      case PERCENTILEEST_QUANTILEDIGEST:
        QuantileDigest quantileDigest = resultHolder.getResult();
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
}
