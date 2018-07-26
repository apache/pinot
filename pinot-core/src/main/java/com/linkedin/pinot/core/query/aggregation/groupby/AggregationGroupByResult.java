/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.query.aggregation.groupby;

import com.linkedin.pinot.core.query.aggregation.function.AggregationFunction;
import java.util.Iterator;


/**
 * This class holds the result of aggregation group by queries.
 * It provides an iterator over group-by keys, and provides a method
 * to get the aggregation result for the given group-by key.
 */
public class AggregationGroupByResult {
  private final GroupKeyGenerator _groupKeyGenerator;
  private final AggregationFunction[] _aggregationFunctions;
  private final GroupByResultHolder[] _resultHolders;

  public AggregationGroupByResult(GroupKeyGenerator groupKeyGenerator, AggregationFunction[] aggregationFunctions,
      GroupByResultHolder[] resultHolders) {
    _groupKeyGenerator = groupKeyGenerator;
    _aggregationFunctions = aggregationFunctions;
    _resultHolders = resultHolders;
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
  public Object getResultForKey(GroupKeyGenerator.GroupKey groupKey, int index) {
    return _aggregationFunctions[index].extractGroupByResult(_resultHolders[index], groupKey._groupId);
  }
}
