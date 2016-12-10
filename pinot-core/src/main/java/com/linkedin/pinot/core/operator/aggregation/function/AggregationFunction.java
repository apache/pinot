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
package com.linkedin.pinot.core.operator.aggregation.function;

import com.linkedin.pinot.core.operator.aggregation.AggregationResultHolder;
import com.linkedin.pinot.core.operator.aggregation.groupby.GroupByResultHolder;
import java.util.List;


/**
 * Interface for Aggregation functions.
 */
public interface AggregationFunction {

  enum ResultDataType {
    LONG,
    DOUBLE,
    AVERAGE_PAIR,
    MINMAXRANGE_PAIR,
    DISTINCTCOUNT_SET,
    DISTINCTCOUNTHLL_HYPERLOGLOG,
    HLL_PREAGGREGATED,
    PERCENTILE_LIST,
    PERCENTILEEST_QUANTILEDIGEST
  }

  void accept(AggregationFunctionVisitorBase visitor);

  /**
   * Performs aggregation on the input array of values.
   *
   * @param length
   * @param resultHolder
   * @param valueArray
   */
  void aggregate(int length, AggregationResultHolder resultHolder, Object... valueArray);

  /**
   * Perform a group-by aggregation on the given set of values, and the group key to which
   * each index corresponds to. This method is for single-valued group by column(s) case, where
   * each docId has only one group key. This mapping is passed in via the docIdToGroupKey parameter.
   *
   * @param length
   * @param docIdToGroupKey
   * @param resultHolder
   * @param valueArray
   */
  void aggregateGroupBySV(int length, int[] docIdToGroupKey, GroupByResultHolder resultHolder, Object... valueArray);

  /**
   * Perform a group-by aggregation on the given set of values, and the group key to which
   * each index corresponds to. This method is for multi-valued group by column(s) case, where
   * each docId can have multiple group keys. This mapping is passed in via the
   * docIdToGroupKeys parameter.
   *
   * @param length
   * @param docIdToGroupKeys
   * @param resultHolder
   * @param valueArray
   */
  void aggregateGroupByMV(int length, int[][] docIdToGroupKeys, GroupByResultHolder resultHolder, Object... valueArray);

  /**
   * Reduce the aggregation. For example, in case of avg/range being performed
   * for blocks of docIds, the aggregate() function may be maintaining a pair of values
   * (eg, sum and total count). Use the reduce interface to compute the final aggregation
   * value from intermediate data.
   *
   * @return
   */
  Double reduce(List<Object> pairs);

  /**
   * Return the function specific default value (e.g. 0.0 for sum) for the aggregation function.
   * @return
   */
  double getDefaultValue();

  /**
   * Return the data type for the result.
   * @return
   */
  ResultDataType getResultDataType();

  /**
   * Returns the name of the aggregation function.
   * @return
   */
  String getName();
}
