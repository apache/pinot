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
package com.linkedin.pinot.core.operator.aggregation.groupby;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.linkedin.pinot.common.utils.primitive.MutableLongValue;
import com.linkedin.pinot.core.operator.aggregation.function.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.AvgAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.MinMaxRangeAggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.quantile.digest.QuantileDigest;
import com.linkedin.pinot.core.query.utils.Pair;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.io.Serializable;
import java.util.Iterator;


/**
 * This class holds the result of aggregation group by queries.
 * It provides an iterator over group-by keys, and provides a method
 * to get the aggregation result for the given group-by key.
 */
public class AggregationGroupByResult {

  private final GroupKeyGenerator _groupKeyGenerator;
  private final GroupByResultHolder _resultHolder[];
  private AggregationFunction.ResultDataType _resultDataType[];
  Pair<String, Serializable> _retPair;

  public AggregationGroupByResult(GroupKeyGenerator keyGenerator, GroupByResultHolder resultHolder[],
      AggregationFunction.ResultDataType resultDataType[]) {

    _groupKeyGenerator = keyGenerator;
    _resultHolder = resultHolder;
    _resultDataType = resultDataType;
    _retPair = new Pair<>(null, null);
  }

  /**
   * Returns an iterator for group-by keys.
   * @return
   */
  public Iterator<GroupKeyGenerator.GroupKey> getGroupKeyIterator() {
    return _groupKeyGenerator.getUniqueGroupKeys();
  }

  /**
   *
   * Given a group-by key and an index into the result holder array, returns
   * the corresponding aggregation result.
   *
   * Clients are expected to order the resultHolder array in the same order in which
   * the aggregation functions appear in the brokerRequest. This way they can simply
   * pass the index of aggregation function and get the result. This is purely a
   * runtime optimization, so that:
   * - Same groupKey can be re-used for multiple aggregation functions.
   * - Avoid any name based association (and lookup) between aggregation function
   *   and its result in this class.
   *
   * @param groupKey
   * @param index
   * @return
   */
  public Serializable getResultForKey(GroupKeyGenerator.GroupKey groupKey, int index) {
    int groupId = groupKey.getFirst();

    switch (_resultDataType[index]) {
      case LONG:
        return new MutableLongValue((long) _resultHolder[index].getDoubleResult(groupId));

      case DOUBLE:
        return _resultHolder[index].getDoubleResult(groupId);

      case AVERAGE_PAIR:
        Pair<Double, Long> doubleLongPair = _resultHolder[index].getResult(groupId);
        return new AvgAggregationFunction.AvgPair(doubleLongPair.getFirst(), doubleLongPair.getSecond());

      case MINMAXRANGE_PAIR:
        Pair<Double, Double> doubleDoublePair = _resultHolder[index].getResult(groupId);
        return new MinMaxRangeAggregationFunction.MinMaxRangePair(doubleDoublePair.getFirst(),
            doubleDoublePair.getSecond());

      case DISTINCTCOUNT_SET:
        return (IntOpenHashSet) _resultHolder[index].getResult(groupId);

      case DISTINCTCOUNTHLL_HYPERLOGLOG:
      case HLL_PREAGGREGATED:
        return (HyperLogLog) _resultHolder[index].getResult(groupId);

      case PERCENTILE_LIST:
        return (DoubleArrayList) _resultHolder[index].getResult(groupId);

      case PERCENTILEEST_QUANTILEDIGEST:
        return (QuantileDigest) _resultHolder[index].getResult(groupId);

      default:
        throw new RuntimeException(
            "Unsupported result data type " + _resultDataType[index] + " in class " + getClass().getName());
    }
  }
}
